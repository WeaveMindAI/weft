//! Storage keys + the identity wall, as pure functions (Layer 1).
//!
//! A key IS the tenant-local path of a file and encodes its scope:
//!   `exec/<color>/<id>`       execution scratch (swept unless kept)
//!   `project/<project_id>/<id>`  per-project persistent
//!   `shared/<name>/<id>`      tenant-shared by agreed name
//!
//! The wall: a verified caller identity + a key resolve to
//! allowed/denied with NO policy configuration. The caller can only
//! ever reach prefixes it is proven to own (its own color, its own
//! project) or has opted into by naming (shared). The service is the
//! only thing that touches disks, so these functions ARE the wall.

use weft_core::storage::StorageScope;

/// Verified caller identity, as resolved by the broker from the
/// presented token (TokenReview + namespace/project/color lookups).
/// The box never trusts a self-claimed value; everything in here was
/// checked against the DB by the broker.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CallerAuth {
    /// A worker pod of `project_id` in `tenant`, currently driving
    /// `color` (verified: the color's owning pod is the caller).
    /// `color` is None when the worker presented no color claim
    /// (then execution-scoped keys are unreachable).
    Worker {
        tenant: String,
        project_id: String,
        color: Option<String>,
    },
    /// The dispatcher (cluster control plane). Used only by the
    /// admin surface (mint, sweep, usage, wipe); the v1 file verbs
    /// reject it so the data path stays worker-only.
    ControlPlane,
}

/// A parsed storage key: scope wall + file id.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum KeyScope {
    Exec { color: String },
    Project { project_id: String },
    Shared { name: String },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ParsedKey {
    pub scope: KeyScope,
    pub id: String,
}

impl KeyScope {
    /// The on-wire scope tag (`exec`/`project`/`shared`).
    fn tag(&self) -> &'static str {
        match self {
            KeyScope::Exec { .. } => "exec",
            KeyScope::Project { .. } => "project",
            KeyScope::Shared { .. } => "shared",
        }
    }

    /// The owner segment (color / project id / shared name).
    fn owner(&self) -> &str {
        match self {
            KeyScope::Exec { color } => color,
            KeyScope::Project { project_id } => project_id,
            KeyScope::Shared { name } => name,
        }
    }
}

impl ParsedKey {
    /// Render the canonical `<scope>/<owner>/<id>` string, the exact
    /// inverse of `parse_key`. The store's index and on-disk paths are
    /// keyed by this string; a `ParsedKey` is the proof that the string
    /// passed the wall's grammar, so every store key-method takes a
    /// `&ParsedKey` and renders here rather than trusting a raw `&str`.
    pub fn to_key(&self) -> String {
        format!("{}/{}/{}", self.scope.tag(), self.scope.owner(), self.id)
    }
}

impl std::fmt::Display for ParsedKey {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.to_key())
    }
}

/// A path segment that is safe inside keys and on-disk paths: no
/// separators, no traversal, no empties. Colors are UUIDs, project
/// ids are UUIDs, ids are UUIDs; shared names are user-chosen and
/// the reason this check exists.
fn valid_segment(s: &str) -> bool {
    !s.is_empty()
        && s.len() <= 128
        && s.chars().all(|c| c.is_ascii_alphanumeric() || c == '-' || c == '_' || c == '.')
        && s != "."
        && s != ".."
}

/// Parse + validate a key. Errors name the exact fault; nothing is
/// normalized or guessed.
pub fn parse_key(key: &str) -> Result<ParsedKey, String> {
    let parts: Vec<&str> = key.split('/').collect();
    let [scope_tag, owner, id] = parts.as_slice() else {
        return Err(format!(
            "malformed storage key '{key}': expected <scope>/<owner>/<id> (3 segments)"
        ));
    };
    if !valid_segment(owner) {
        return Err(format!("malformed storage key '{key}': bad owner segment"));
    }
    if !valid_segment(id) {
        return Err(format!("malformed storage key '{key}': bad id segment"));
    }
    let scope = match *scope_tag {
        "exec" => KeyScope::Exec { color: owner.to_string() },
        "project" => KeyScope::Project { project_id: owner.to_string() },
        "shared" => KeyScope::Shared { name: owner.to_string() },
        other => {
            return Err(format!(
                "malformed storage key '{key}': unknown scope '{other}' (exec|project|shared)"
            ))
        }
    };
    Ok(ParsedKey { scope, id: id.to_string() })
}

/// Validate that `prefix` is a scope-anchored owner prefix, i.e.
/// `<exec|project|shared>/<owner>/` with a trailing slash and a valid
/// owner segment. This is the ONLY shape `wipe_prefix` may delete: a
/// raw `starts_with` match on an unanchored string (empty, or `exec`
/// without the slash) would wipe the whole box or cross owner
/// boundaries (one color's `exec/c1` also matching `exec/c1abc`).
/// Validating here keeps "wipe a prefix that isn't a real scope
/// boundary" out of the box entirely.
pub fn validate_wipe_prefix(prefix: &str) -> Result<(), String> {
    let stripped = prefix
        .strip_suffix('/')
        .ok_or_else(|| format!("wipe prefix '{prefix}' must end in '/' (a scope boundary)"))?;
    let parts: Vec<&str> = stripped.split('/').collect();
    let [scope_tag, owner] = parts.as_slice() else {
        return Err(format!(
            "wipe prefix '{prefix}' must be <scope>/<owner>/ (exec|project|shared)"
        ));
    };
    if !matches!(*scope_tag, "exec" | "project" | "shared") {
        return Err(format!(
            "wipe prefix '{prefix}': unknown scope '{scope_tag}' (exec|project|shared)"
        ));
    }
    if !valid_segment(owner) {
        return Err(format!("wipe prefix '{prefix}': bad owner segment"));
    }
    Ok(())
}

/// Build the `ParsedKey` for a fresh put under `scope` by `caller`.
/// Errors when the caller can't own the scope (no color claim for
/// Execution scope, control-plane writes) or any segment is not the
/// wall's grammar.
///
/// EVERY segment (the owner: color / project id / shared name; and the
/// id) is run through `valid_segment` here, so a `ParsedKey` this mints
/// is one `parse_key` would also accept: the "a ParsedKey is the proof a
/// key passed the grammar" invariant holds by CONSTRUCTION on both
/// construction paths, not by the accident that the broker happens to
/// supply UUIDs. A bad owner (a malformed token claim) fails loud rather
/// than minting a key the store could never look up.
pub fn key_for_put(caller: &CallerAuth, scope: &StorageScope, id: &str) -> Result<ParsedKey, String> {
    let CallerAuth::Worker { project_id, color, .. } = caller else {
        return Err("only workers store files; the control plane has no put".into());
    };
    let owned = |label: &str, seg: &str| -> Result<String, String> {
        if valid_segment(seg) {
            Ok(seg.to_string())
        } else {
            Err(format!("invalid {label} segment '{seg}' for a storage key"))
        }
    };
    let key_scope = match scope {
        StorageScope::Execution => {
            let color = color.as_deref().ok_or(
                "execution-scoped put requires a verified execution color and the caller \
                 presented none",
            )?;
            KeyScope::Exec { color: owned("color", color)? }
        }
        StorageScope::Project => KeyScope::Project { project_id: owned("project", project_id)? },
        StorageScope::Shared { name } => KeyScope::Shared { name: owned("shared-space name", name)? },
    };
    Ok(ParsedKey { scope: key_scope, id: owned("id", id)? })
}

/// The list prefix for `scope` as seen by `caller`. Same ownership
/// rules as `key_for_put`.
pub fn prefix_for_list(caller: &CallerAuth, scope: &StorageScope) -> Result<String, String> {
    let CallerAuth::Worker { project_id, color, .. } = caller else {
        return Err("only workers list scoped files; the control plane uses the admin surface".into());
    };
    match scope {
        StorageScope::Execution => {
            let color = color.as_deref().ok_or(
                "execution-scoped list requires a verified execution color and the caller \
                 presented none",
            )?;
            Ok(format!("exec/{color}/"))
        }
        StorageScope::Project => Ok(format!("project/{project_id}/")),
        StorageScope::Shared { name } => {
            if !valid_segment(name) {
                return Err(format!(
                    "invalid shared-space name '{name}': ascii alphanumerics, '-', '_', '.' only"
                ));
            }
            Ok(format!("shared/{name}/"))
        }
    }
}

/// Can `caller` touch the file at `key` (get/delete/keep/presign)?
/// The key's OWN scope decides; deny reasons are specific.
pub fn check_key_access(caller: &CallerAuth, parsed: &KeyScope) -> Result<(), String> {
    let CallerAuth::Worker { project_id, color, .. } = caller else {
        // Admin verbs run on dedicated routes; a control-plane call
        // landing on the worker data path is a caller bug.
        return Err("control-plane callers use the admin surface, not the data path".into());
    };
    match parsed {
        KeyScope::Exec { color: key_color } => match color {
            Some(c) if c == key_color => Ok(()),
            Some(_) => Err(
                "denied: execution-scoped file belongs to a different execution (colors are \
                 walled per run; use Project scope for files that outlive a run)"
                    .into(),
            ),
            None => Err("denied: caller presented no verified execution color".into()),
        },
        KeyScope::Project { project_id: key_project } => {
            if key_project == project_id {
                Ok(())
            } else {
                Err("denied: project-scoped file belongs to a different project".into())
            }
        }
        // Naming a shared key IS the opt-in (the grant table records
        // it for audit/listing; it never denies a worker of the
        // tenant, since the box itself is tenant-walled).
        KeyScope::Shared { .. } => Ok(()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn worker(color: Option<&str>) -> CallerAuth {
        CallerAuth::Worker {
            tenant: "t1".into(),
            project_id: "p1".into(),
            color: color.map(String::from),
        }
    }

    #[test]
    fn to_key_is_the_exact_inverse_of_parse() {
        for k in ["exec/c1/f1", "project/p1/f2", "shared/team/f3"] {
            assert_eq!(parse_key(k).unwrap().to_key(), k, "round-trip {k}");
        }
    }

    #[test]
    fn parse_round_trips_each_scope() {
        assert_eq!(
            parse_key("exec/c1/f1").unwrap(),
            ParsedKey { scope: KeyScope::Exec { color: "c1".into() }, id: "f1".into() }
        );
        assert_eq!(
            parse_key("project/p1/f2").unwrap().scope,
            KeyScope::Project { project_id: "p1".into() }
        );
        assert_eq!(
            parse_key("shared/team/f3").unwrap().scope,
            KeyScope::Shared { name: "team".into() }
        );
    }

    #[test]
    fn parse_rejects_malformed() {
        for bad in [
            "",
            "exec/c1",
            "exec/c1/f1/extra",
            "bogus/c1/f1",
            "exec//f1",
            "exec/../f1",
            "shared/na me/f1",
            "exec/c1/",
        ] {
            assert!(parse_key(bad).is_err(), "should reject {bad:?}");
        }
    }

    #[test]
    fn put_builds_caller_owned_prefixes_only() {
        let w = worker(Some("c1"));
        assert_eq!(key_for_put(&w, &StorageScope::Execution, "id").unwrap().to_key(), "exec/c1/id");
        assert_eq!(key_for_put(&w, &StorageScope::Project, "id").unwrap().to_key(), "project/p1/id");
        assert_eq!(
            key_for_put(&w, &StorageScope::Shared { name: "team".into() }, "id").unwrap().to_key(),
            "shared/team/id"
        );
        // No color claim -> no exec writes.
        assert!(key_for_put(&worker(None), &StorageScope::Execution, "id").is_err());
        // Control plane never puts.
        assert!(key_for_put(&CallerAuth::ControlPlane, &StorageScope::Project, "id").is_err());
        // EVERY segment is validated, so key_for_put can only mint a
        // ParsedKey that parse_key would also accept (the invariant).
        // A bad shared name, id, or owner is rejected.
        assert!(key_for_put(&w, &StorageScope::Shared { name: "a/b".into() }, "id").is_err());
        assert!(key_for_put(&w, &StorageScope::Execution, "bad/id").is_err());
        assert!(key_for_put(&w, &StorageScope::Execution, "..").is_err());
        assert!(key_for_put(&worker(Some("c/d")), &StorageScope::Execution, "id").is_err());
    }

    #[test]
    fn key_for_put_output_always_round_trips_through_parse_key() {
        // The construction invariant: whatever key_for_put mints, parse_key
        // accepts and renders back identically (the two paths agree).
        let w = worker(Some("0188-c0102"));
        for scope in [
            StorageScope::Execution,
            StorageScope::Project,
            StorageScope::Shared { name: "team.alpha".into() },
        ] {
            let pk = key_for_put(&w, &scope, "9f3a-id").expect("clean parts");
            let rendered = pk.to_key();
            assert_eq!(parse_key(&rendered).unwrap(), pk, "round-trip {rendered}");
        }
    }

    #[test]
    fn access_walls_per_scope() {
        let w = worker(Some("c1"));
        // Own color: allowed. Another color: denied.
        assert!(check_key_access(&w, &KeyScope::Exec { color: "c1".into() }).is_ok());
        assert!(check_key_access(&w, &KeyScope::Exec { color: "c2".into() }).is_err());
        // Own project: allowed. Another project: denied.
        assert!(check_key_access(&w, &KeyScope::Project { project_id: "p1".into() }).is_ok());
        assert!(check_key_access(&w, &KeyScope::Project { project_id: "p2".into() }).is_err());
        // Shared: naming is the opt-in.
        assert!(check_key_access(&w, &KeyScope::Shared { name: "x".into() }).is_ok());
        // No color claim cannot reach ANY exec key.
        assert!(check_key_access(&worker(None), &KeyScope::Exec { color: "c1".into() }).is_err());
        // Control plane is rejected on the data path.
        assert!(check_key_access(
            &CallerAuth::ControlPlane,
            &KeyScope::Project { project_id: "p1".into() }
        )
        .is_err());
    }
}
