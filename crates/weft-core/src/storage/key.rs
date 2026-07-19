//! Storage keys + the identity wall, as pure functions (Layer 1).
//!
//! A key IS the fully-qualified path of a file. The FIRST segment is
//! the owning tenant; the rest encodes the scope:
//!   `<tenant>/exec/<color>/<id>`        execution scratch (swept unless kept)
//!   `<tenant>/project/<project_id>/<id>`   per-project persistent
//!   `<tenant>/shared/<name>/<id>`       tenant-shared by agreed name
//!
//! The wall: a verified caller identity + a key resolve to
//! allowed/denied with NO policy configuration. The runtime-storage
//! bucket is SHARED across every tenant (one bucket, keys namespaced by
//! the tenant prefix), so the tenant segment is the outer wall: a caller
//! can only ever reach keys under ITS OWN broker-verified tenant, and
//! within that, only prefixes it is proven to own (its own color, its
//! own project) or has opted into by naming (shared). The broker is the
//! only thing that signs bucket requests, so these functions ARE the wall.

use super::StorageScope;

/// Verified caller identity, as resolved by the broker from the
/// presented token (TokenReview + namespace/project/color lookups).
/// Nothing here is self-claimed; everything was checked against the DB
/// by the broker.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CallerAuth {
    /// A caller acting within `tenant`/`project_id`: a worker pod of that
    /// project (verified via its token), or the dispatcher acting for the
    /// tenant's editor session on the admin upload surface (the dispatcher
    /// vouches for the tenant, the broker re-checks the key against it).
    /// `color` is the execution being driven (verified: the color's owning
    /// pod is the caller); None when no color claim was presented (then
    /// execution-scoped keys are unreachable).
    Worker {
        tenant: String,
        project_id: String,
        color: Option<String>,
    },
    /// The dispatcher (cluster control plane). Used only by the
    /// admin surface (presign, sweep, usage, wipe); the worker file
    /// verbs reject it so the data path stays worker-only.
    ControlPlane,
}

/// A parsed storage key: scope wall + file id.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum KeyScope {
    Exec { color: String },
    Project { project_id: String },
    Shared { name: String },
    /// `asset/<project_id>/<sha256>`: a project ASSET, the published copy of a
    /// file the project's source references via a media `@file` ref. The id is
    /// the content hash, so existence == "this exact content is uploaded".
    /// Sync-managed derived state: created/deleted only by the pre-build asset
    /// sync (through the control-plane surface); workers of the project READ
    /// it like project scope but may not write it.
    Asset { project_id: String },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ParsedKey {
    /// The owning tenant: the key's first segment, and the outer wall
    /// on a shared pod. Always the broker-verified caller tenant on the
    /// construction paths; validated to MATCH it on the parse path.
    pub tenant: String,
    pub scope: KeyScope,
    pub id: String,
}

/// The on-wire scope tags, the SINGLE source of truth for "is this
/// segment a scope tag." Every site that needs to recognize a tag
/// (`parse_key`, `validate_wipe_prefix`, the dispatcher's CLI-key
/// prefixers) goes through `is_scope_tag` / `KeyScope::from_tag` so a new
/// or renamed scope is changed in exactly one place. Keep this in sync with
/// the `KeyScope` variants + `KeyScope::tag`.
// SYNC: SCOPE_TAGS <-> weavemind/website/src/lib/graph/runtime-files.ts Tier
pub const SCOPE_TAGS: [&str; 4] = ["exec", "project", "shared", "asset"];

/// Is `s` a TENANT-LESS runtime storage key (`<scope>/<owner>/<id>`, every
/// segment in the wall's grammar)? The short address a human writes in source
/// (`@asset("project/<id>/<file>", Image)`) and the form `weft files` shows;
/// the build's resolution re-anchors it to the acting tenant. Distinguishes a
/// storage address from an ordinary project path by the scope-tag first
/// segment plus exact 3-segment shape.
pub fn is_scope_key(s: &str) -> bool {
    let parts: Vec<&str> = s.split('/').collect();
    matches!(parts.as_slice(),
        [tag, owner, id] if is_scope_tag(tag) && valid_segment(owner) && valid_segment(id))
}

/// Is `s` a known on-wire scope tag? The one predicate every "looks like a
/// scope key" check consults (so the tag set never forks across files).
pub fn is_scope_tag(s: &str) -> bool {
    SCOPE_TAGS.contains(&s)
}

impl KeyScope {
    /// Build the scope for a `(tag, owner)` pair, or None if `tag` is not
    /// a known scope tag. The canonical tag -> variant mapping; `parse_key`
    /// routes through here so the grammar and `SCOPE_TAGS` cannot drift.
    fn from_tag(tag: &str, owner: &str) -> Option<Self> {
        match tag {
            "exec" => Some(KeyScope::Exec { color: owner.to_string() }),
            "project" => Some(KeyScope::Project { project_id: owner.to_string() }),
            "shared" => Some(KeyScope::Shared { name: owner.to_string() }),
            "asset" => Some(KeyScope::Asset { project_id: owner.to_string() }),
            _ => None,
        }
    }

    /// The on-wire scope tag (`exec`/`project`/`shared`/`asset`).
    fn tag(&self) -> &'static str {
        match self {
            KeyScope::Exec { .. } => "exec",
            KeyScope::Project { .. } => "project",
            KeyScope::Shared { .. } => "shared",
            KeyScope::Asset { .. } => "asset",
        }
    }

    /// The owner segment (color / project id / shared name).
    fn owner(&self) -> &str {
        match self {
            KeyScope::Exec { color } => color,
            KeyScope::Project { project_id } => project_id,
            KeyScope::Shared { name } => name,
            KeyScope::Asset { project_id } => project_id,
        }
    }
}

impl ParsedKey {
    /// Render the canonical `<tenant>/<scope>/<owner>/<id>` string, the
    /// exact inverse of `parse_key`. The runtime-file row + the bucket
    /// object are keyed by this string under the `runtime/` prefix; a
    /// `ParsedKey` is the proof that the string passed the wall's grammar,
    /// so every store key-method takes a `&ParsedKey` and renders here
    /// rather than trusting a raw `&str`.
    pub fn to_key(&self) -> String {
        format!("{}/{}/{}/{}", self.tenant, self.scope.tag(), self.scope.owner(), self.id)
    }

    /// The tenant prefix `<tenant>/` that ranges EVERY key this tenant
    /// owns (across all scopes). The per-tenant usage accounting +
    /// `weft files ls` range over this. Fallible: the `tenant` is a path
    /// segment (it becomes the outer bucket-list prefix), so it MUST pass the
    /// same grammar as every other segment. Without this an admin request with
    /// a blank or `..` tenant would produce prefix `/` or `../`, ranging the
    /// whole bucket across every tenant. Callers surface the error as a 400.
    pub fn tenant_prefix(tenant: &str) -> Result<String, String> {
        if valid_segment(tenant) {
            Ok(format!("{tenant}/"))
        } else {
            Err(format!("invalid tenant segment '{tenant}' for a storage list prefix"))
        }
    }

    /// The `<tenant>/project/<project_id>/` prefix covering one project's
    /// persistent runtime files: the range the project reclaimer wipes.
    pub fn project_prefix(tenant: &str, project: &str) -> Result<String, String> {
        Self::owned_prefix(tenant, "project", project)
    }

    /// The `<tenant>/asset/<project_id>/` prefix covering one project's
    /// published assets: the range the pre-build sync diffs against and the
    /// project reclaimer wipes.
    pub fn asset_prefix(tenant: &str, project: &str) -> Result<String, String> {
        Self::owned_prefix(tenant, "asset", project)
    }

    /// A validated `<tenant>/<tag>/<owner>/` prefix: both segments pass the
    /// key grammar, so a built prefix can never range outside the tenant.
    fn owned_prefix(tenant: &str, tag: &str, owner: &str) -> Result<String, String> {
        if !valid_segment(tenant) {
            return Err(format!("invalid tenant segment '{tenant}' for a {tag} prefix"));
        }
        if !valid_segment(owner) {
            return Err(format!("invalid owner segment '{owner}' for a {tag} prefix"));
        }
        Ok(format!("{tenant}/{tag}/{owner}/"))
    }
}

impl std::fmt::Display for ParsedKey {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.to_key())
    }
}

/// A path segment that is safe inside a bucket key: no
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
/// normalized or guessed. Validates the GRAMMAR (4 segments, every
/// segment safe); the tenant-OWNERSHIP check (key tenant == the
/// broker-verified caller tenant) is the wall's job in `check_key_access`
/// / `key_for_put` / `prefix_for_list`, where the verified caller is in
/// hand. A signed capability or a control-plane admin verb has no caller
/// identity to match against, so they parse here and trust the segment.
pub fn parse_key(key: &str) -> Result<ParsedKey, String> {
    let parts: Vec<&str> = key.split('/').collect();
    let [tenant, scope_tag, owner, id] = parts.as_slice() else {
        return Err(format!(
            "malformed storage key '{key}': expected <tenant>/<scope>/<owner>/<id> (4 segments)"
        ));
    };
    if !valid_segment(tenant) {
        return Err(format!("malformed storage key '{key}': bad tenant segment"));
    }
    if !valid_segment(owner) {
        return Err(format!("malformed storage key '{key}': bad owner segment"));
    }
    if !valid_segment(id) {
        return Err(format!("malformed storage key '{key}': bad id segment"));
    }
    let scope = KeyScope::from_tag(scope_tag, owner).ok_or_else(|| {
        format!(
            "malformed storage key '{key}': unknown scope '{scope_tag}' ({})",
            SCOPE_TAGS.join("|")
        )
    })?;
    Ok(ParsedKey { tenant: tenant.to_string(), scope, id: id.to_string() })
}

/// Validate that `prefix` is one of the two scope-anchored boundaries
/// `wipe_prefix` may delete, each ending in `/`:
///   - `<tenant>/<scope>/<owner>/` (any tag in [`SCOPE_TAGS`]) : one owner's space
///     (the dispatcher's `weft rm` / `weft clean <color>` / project-delete).
///   - `<tenant>/` : the WHOLE tenant (a tenant-delete wiping every
///     object under the tenant's prefix).
/// Both are real prefix boundaries. A raw `starts_with` on an unanchored
/// string (empty, or `exec` without a slash) would wipe across tenants or
/// across owner boundaries (one color's `t/exec/c1` also matching
/// `t/exec/c1abc`); validating the trailing slash + the segment grammar
/// here keeps that out of the data path entirely. It does NOT allow a bare
/// `<tenant>/<scope>/` (no owner): that would let a caller wipe every
/// owner under one scope, which no verb wants.
pub fn validate_wipe_prefix(prefix: &str) -> Result<(), String> {
    let stripped = prefix
        .strip_suffix('/')
        .ok_or_else(|| format!("wipe prefix '{prefix}' must end in '/' (a scope boundary)"))?;
    let parts: Vec<&str> = stripped.split('/').collect();
    match parts.as_slice() {
        // Whole-tenant boundary.
        [tenant] => {
            if !valid_segment(tenant) {
                return Err(format!("wipe prefix '{prefix}': bad tenant segment"));
            }
            Ok(())
        }
        // Owner boundary within a tenant.
        [tenant, scope_tag, owner] => {
            if !valid_segment(tenant) {
                return Err(format!("wipe prefix '{prefix}': bad tenant segment"));
            }
            if !is_scope_tag(scope_tag) {
                return Err(format!(
                    "wipe prefix '{prefix}': unknown scope '{scope_tag}' ({})",
                    SCOPE_TAGS.join("|")
                ));
            }
            if !valid_segment(owner) {
                return Err(format!("wipe prefix '{prefix}': bad owner segment"));
            }
            Ok(())
        }
        _ => Err(format!(
            "wipe prefix '{prefix}' must be <tenant>/ or <tenant>/<scope>/<owner>/ ({})",
            SCOPE_TAGS.join("|")
        )),
    }
}

/// Validate + resolve the OWNED `(tenant, scope_tag, owner)` triple a worker
/// caller may address under `scope`. THE one place the wall's construction rules
/// live, shared by `key_for_put` and `prefix_for_list` so neither can forget a
/// check (an earlier `prefix_for_list` validated the tenant + shared name but NOT
/// the color / project id, so a malformed owner could produce a list prefix that
/// escaped the intended owner boundary; routing both through here makes the two
/// paths validate identically by construction).
///
/// Every returned segment (tenant AND owner) has passed `valid_segment`, so a key
/// or prefix built from this triple is always one `parse_key` would also accept:
/// the "a ParsedKey is the proof a key passed the grammar" invariant holds by
/// CONSTRUCTION, not by the accident that the broker happens to supply UUIDs.
/// Errors when the caller is not a worker, an Execution scope carries no color, or
/// any segment is not the wall's grammar.
fn owned_scope_segments<'a>(
    caller: &'a CallerAuth,
    scope: &'a StorageScope,
) -> Result<(String, &'static str, String), String> {
    let CallerAuth::Worker { tenant, project_id, color } = caller else {
        return Err("only workers address scoped files; the control plane uses the admin surface".into());
    };
    let owned = |label: &str, seg: &str| -> Result<String, String> {
        if valid_segment(seg) {
            Ok(seg.to_string())
        } else {
            Err(format!("invalid {label} segment '{seg}' for a storage key"))
        }
    };
    // The tenant comes from the broker verdict, so it is real, but it is ALSO a
    // path segment, so it must pass the same grammar as every other segment (a
    // tenant id with a '/' or '..' must fail loud, never mint a key the store could
    // not look up).
    let tenant = owned("tenant", tenant)?;
    let (tag, owner) = match scope {
        StorageScope::Execution => {
            let color = color.as_deref().ok_or(
                "execution-scoped access requires a verified execution color and the caller \
                 presented none",
            )?;
            ("exec", owned("color", color)?)
        }
        StorageScope::Project => ("project", owned("project", project_id)?),
        StorageScope::Shared { name } => ("shared", owned("shared-space name", name)?),
        // Assets are keyed like project scope (owner = the caller's project).
        // WHO may put here is route policy, not grammar: the worker data path
        // refuses asset-scope writes (assets are sync-managed), the
        // control-plane surface allows them; both build keys through this.
        StorageScope::Asset => ("asset", owned("project", project_id)?),
    };
    Ok((tenant, tag, owner))
}

/// Build the `ParsedKey` for a fresh put under `scope` by `caller`.
/// Errors when the caller can't own the scope (no color claim for
/// Execution scope, control-plane writes) or any segment is not the
/// wall's grammar. Every segment (including the `id`) is validated via
/// `owned_scope_segments` + the explicit `id` check below.
pub fn key_for_put(caller: &CallerAuth, scope: &StorageScope, id: &str) -> Result<ParsedKey, String> {
    let (tenant, tag, owner) = owned_scope_segments(caller, scope)?;
    let id = if valid_segment(id) {
        id.to_string()
    } else {
        return Err(format!("invalid id segment '{id}' for a storage key"));
    };
    // `tag` came from `owned_scope_segments`, always a known scope tag.
    let key_scope = KeyScope::from_tag(tag, &owner)
        .expect("owned_scope_segments only yields exec/project/shared tags");
    Ok(ParsedKey { tenant, scope: key_scope, id })
}

/// The list prefix for `scope` as seen by `caller`, tenant-anchored.
/// Same ownership + grammar rules as `key_for_put` (both route through
/// `owned_scope_segments`). The leading `<tenant>/` is the outer wall: a list
/// never sees another tenant's keys in the shared bucket.
pub fn prefix_for_list(caller: &CallerAuth, scope: &StorageScope) -> Result<String, String> {
    let (tenant, tag, owner) = owned_scope_segments(caller, scope)?;
    Ok(format!("{tenant}/{tag}/{owner}/"))
}

/// The prefix covering one execution's files (`<tenant>/exec/<color>/`), for
/// control-plane sweeps that act on a color with no caller identity in hand.
/// Both segments are validated and the scope tag is rendered through the one
/// grammar (`KeyScope::tag`), so no caller ever hand-builds an exec prefix that
/// could drift from `SCOPE_TAGS` or smuggle a separator through an unvalidated
/// segment.
pub fn exec_prefix(tenant: &str, color: &str) -> Result<String, String> {
    if !valid_segment(tenant) {
        return Err(format!("invalid tenant segment '{tenant}' for an exec prefix"));
    }
    if !valid_segment(color) {
        return Err(format!("invalid color segment '{color}' for an exec prefix"));
    }
    let tag = KeyScope::Exec { color: color.to_string() }.tag();
    Ok(format!("{tenant}/{tag}/{color}/"))
}

/// Can `caller` touch the file at `key` (get/delete/keep/presign)?
/// The TENANT segment is the outer wall (the shared bucket holds many
/// tenants' keys under one prefix space), then the key's own scope
/// decides. Deny reasons are specific.
pub fn check_key_access(caller: &CallerAuth, parsed: &ParsedKey) -> Result<(), String> {
    let CallerAuth::Worker { tenant, project_id, color } = caller else {
        // Admin verbs run on dedicated routes; a control-plane call
        // landing on the worker data path is a caller bug.
        return Err("control-plane callers use the admin surface, not the data path".into());
    };
    // Tenant wall FIRST: the bucket is shared, so a worker must never
    // reach a key under a different tenant's prefix. The caller tenant is
    // the broker verdict; the key tenant is the first path segment. This
    // is the load-bearing isolation check on the shared bucket.
    if &parsed.tenant != tenant {
        return Err(format!(
            "denied: file belongs to tenant '{}', not the caller's tenant '{tenant}'",
            parsed.tenant
        ));
    }
    match &parsed.scope {
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
        // Assets read like project scope: any worker of the owning project may
        // fetch them (the compiled config references them by key). Writes never
        // reach here from workers (the data-path upload verbs refuse the Asset
        // scope; assets are sync-managed).
        KeyScope::Asset { project_id: key_project } => {
            if key_project == project_id {
                Ok(())
            } else {
                Err("denied: asset belongs to a different project".into())
            }
        }
        // Naming a shared key IS the opt-in (the grant table records it
        // for audit/listing; it never denies a worker of the tenant).
        // Safe because the tenant wall above already confirmed the caller
        // owns this key's tenant, so a shared space is only ever reachable
        // by workers of the SAME tenant.
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

    /// A parsed key under tenant `t1` for the wall tests (the tenant
    /// segment matches `worker()`'s tenant unless a test says otherwise).
    fn pk(tenant: &str, scope: KeyScope) -> ParsedKey {
        ParsedKey { tenant: tenant.into(), scope, id: "f".into() }
    }

    #[test]
    fn is_scope_key_accepts_tenant_less_keys_only() {
        for ok in ["exec/c1/f1", "project/p1/f2", "shared/team/f3", "asset/p1/f4"] {
            assert!(is_scope_key(ok), "{ok}");
        }
        for no in [
            "t1/project/p1/f2",  // tenant-anchored (4 segments)
            "assets/pic.png",    // ordinary project path
            "project/p1",        // missing id
            "project//f",        // empty owner
            "banana/p1/f",       // unknown scope tag
            "https://ex.com/a",  // URL
        ] {
            assert!(!is_scope_key(no), "{no}");
        }
    }

    #[test]
    fn to_key_is_the_exact_inverse_of_parse() {
        for k in ["t1/exec/c1/f1", "t1/project/p1/f2", "t1/shared/team/f3", "t1/asset/p1/f4"] {
            assert_eq!(parse_key(k).unwrap().to_key(), k, "round-trip {k}");
        }
    }

    /// The asset scope's grammar: parses like project scope (owner = project),
    /// a 64-hex sha256 passes as the id segment (the content-hash identity the
    /// sync relies on), and the wall admits the OWNING project's workers
    /// (reads) while denying every other project and the control plane on the
    /// data path, exactly like project scope.
    #[test]
    fn asset_scope_parses_walls_and_takes_hash_ids() {
        let sha = "a".repeat(64);
        let key = format!("t1/asset/p1/{sha}");
        let parsed = parse_key(&key).unwrap();
        assert_eq!(parsed.scope, KeyScope::Asset { project_id: "p1".into() });
        assert_eq!(parsed.id, sha);
        assert_eq!(parsed.to_key(), key);

        // Same-project worker may read; another project's worker may not; the
        // control plane uses the admin surface, not the data path.
        let asset = pk("t1", KeyScope::Asset { project_id: "p1".into() });
        assert!(check_key_access(&worker(None), &asset).is_ok());
        assert!(check_key_access(&worker(Some("c1")), &asset).is_ok());
        let other = CallerAuth::Worker {
            tenant: "t1".into(),
            project_id: "p2".into(),
            color: None,
        };
        assert!(check_key_access(&other, &asset).is_err());
        assert!(check_key_access(&CallerAuth::ControlPlane, &asset).is_err());
        // The tenant wall holds first: a worker of another tenant is denied.
        let foreign = pk("t2", KeyScope::Asset { project_id: "p1".into() });
        assert!(check_key_access(&worker(None), &foreign).is_err());

        // key_for_put builds the asset key from the caller's own project (the
        // route layer decides WHO may put; the grammar just builds).
        let put = key_for_put(&worker(None), &StorageScope::Asset, &sha).unwrap();
        assert_eq!(put.to_key(), key);
        // And the list prefix ranges exactly the project's assets.
        assert_eq!(
            prefix_for_list(&worker(None), &StorageScope::Asset).unwrap(),
            "t1/asset/p1/"
        );
    }

    #[test]
    fn parse_round_trips_each_scope() {
        assert_eq!(
            parse_key("t1/exec/c1/f1").unwrap(),
            ParsedKey {
                tenant: "t1".into(),
                scope: KeyScope::Exec { color: "c1".into() },
                id: "f1".into()
            }
        );
        assert_eq!(
            parse_key("t1/project/p1/f2").unwrap().scope,
            KeyScope::Project { project_id: "p1".into() }
        );
        assert_eq!(
            parse_key("t1/shared/team/f3").unwrap().scope,
            KeyScope::Shared { name: "team".into() }
        );
        // The tenant segment is carried through verbatim.
        assert_eq!(parse_key("TenantMixedCase/exec/c1/f1").unwrap().tenant, "TenantMixedCase");
    }

    #[test]
    fn parse_rejects_malformed() {
        for bad in [
            "",
            "t1/exec/c1",      // 3 segments (the old grammar)
            "t1/exec/c1/f1/x", // 5 segments
            "t1/bogus/c1/f1",
            "t1/exec//f1",
            "t1/exec/../f1",
            "t1/shared/na me/f1",
            "t1/exec/c1/",
            "/exec/c1/f1", // empty tenant
        ] {
            assert!(parse_key(bad).is_err(), "should reject {bad:?}");
        }
    }

    #[test]
    fn put_builds_caller_owned_prefixes_only() {
        let w = worker(Some("c1"));
        // The tenant (t1, from the verdict) is the first segment.
        assert_eq!(key_for_put(&w, &StorageScope::Execution, "id").unwrap().to_key(), "t1/exec/c1/id");
        assert_eq!(key_for_put(&w, &StorageScope::Project, "id").unwrap().to_key(), "t1/project/p1/id");
        assert_eq!(
            key_for_put(&w, &StorageScope::Shared { name: "team".into() }, "id").unwrap().to_key(),
            "t1/shared/team/id"
        );
        // No color claim -> no exec writes.
        assert!(key_for_put(&worker(None), &StorageScope::Execution, "id").is_err());
        // Control plane never puts.
        assert!(key_for_put(&CallerAuth::ControlPlane, &StorageScope::Project, "id").is_err());
        // EVERY segment is validated, so key_for_put can only mint a
        // ParsedKey that parse_key would also accept (the invariant).
        // A bad shared name, id, owner, OR tenant is rejected.
        assert!(key_for_put(&w, &StorageScope::Shared { name: "a/b".into() }, "id").is_err());
        assert!(key_for_put(&w, &StorageScope::Execution, "bad/id").is_err());
        assert!(key_for_put(&w, &StorageScope::Execution, "..").is_err());
        assert!(key_for_put(&worker(Some("c/d")), &StorageScope::Execution, "id").is_err());
        let bad_tenant = CallerAuth::Worker {
            tenant: "a/b".into(),
            project_id: "p1".into(),
            color: Some("c1".into()),
        };
        assert!(key_for_put(&bad_tenant, &StorageScope::Project, "id").is_err());
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
    fn list_prefixes_are_tenant_anchored() {
        let w = worker(Some("c1"));
        assert_eq!(prefix_for_list(&w, &StorageScope::Execution).unwrap(), "t1/exec/c1/");
        assert_eq!(prefix_for_list(&w, &StorageScope::Project).unwrap(), "t1/project/p1/");
        assert_eq!(
            prefix_for_list(&w, &StorageScope::Shared { name: "team".into() }).unwrap(),
            "t1/shared/team/"
        );
    }

    #[test]
    fn access_walls_per_tenant_then_scope() {
        let w = worker(Some("c1")); // tenant t1
                                    // Own color under own tenant: allowed.
        assert!(check_key_access(&w, &pk("t1", KeyScope::Exec { color: "c1".into() })).is_ok());
        // Another color: denied.
        assert!(check_key_access(&w, &pk("t1", KeyScope::Exec { color: "c2".into() })).is_err());
        // Own project: allowed. Another project: denied.
        assert!(check_key_access(&w, &pk("t1", KeyScope::Project { project_id: "p1".into() })).is_ok());
        assert!(check_key_access(&w, &pk("t1", KeyScope::Project { project_id: "p2".into() })).is_err());
        // Shared under own tenant: naming is the opt-in.
        assert!(check_key_access(&w, &pk("t1", KeyScope::Shared { name: "x".into() })).is_ok());
        // No color claim cannot reach ANY exec key.
        assert!(check_key_access(&worker(None), &pk("t1", KeyScope::Exec { color: "c1".into() })).is_err());
        // Control plane is rejected on the data path.
        assert!(check_key_access(
            &CallerAuth::ControlPlane,
            &pk("t1", KeyScope::Project { project_id: "p1".into() })
        )
        .is_err());
    }

    /// THE load-bearing isolation proof on the shared bucket: a worker of one
    /// tenant can reach NOTHING under another tenant's prefix, even a key
    /// whose inner scope it would otherwise own (same project id, same
    /// color, or a shared name). The tenant wall is checked first.
    #[test]
    fn access_denies_cross_tenant_even_when_inner_scope_matches() {
        let w = worker(Some("c1")); // tenant t1, project p1, color c1
                                    // Another tenant's exec key with the SAME color: denied by tenant.
        assert!(check_key_access(&w, &pk("t2", KeyScope::Exec { color: "c1".into() })).is_err());
        // Another tenant's project key with the SAME project id: denied.
        assert!(check_key_access(&w, &pk("t2", KeyScope::Project { project_id: "p1".into() })).is_err());
        // Another tenant's shared space (naming is no opt-in across tenants).
        assert!(check_key_access(&w, &pk("t2", KeyScope::Shared { name: "x".into() })).is_err());
        // And the error names the tenant mismatch, not a scope mismatch.
        let err = check_key_access(&w, &pk("t2", KeyScope::Project { project_id: "p1".into() }))
            .unwrap_err();
        assert!(err.contains("tenant"), "{err}");
    }

    #[test]
    fn validate_wipe_prefix_accepts_tenant_and_owner_boundaries() {
        // Whole tenant.
        assert!(validate_wipe_prefix("t1/").is_ok());
        // Owner boundary within a tenant.
        assert!(validate_wipe_prefix("t1/exec/c1/").is_ok());
        assert!(validate_wipe_prefix("t1/project/p1/").is_ok());
        assert!(validate_wipe_prefix("t1/shared/team/").is_ok());
        // Rejected: no trailing slash, empty, a bare scope (no owner),
        // a bad scope tag, traversal, and the OLD slashless tenant shapes.
        for bad in [
            "t1",
            "",
            "/",
            "t1/exec/",     // scope without owner: would wipe all colors
            "t1/bogus/c1/",
            "t1/exec/../",
            "exec/c1/",     // the old (tenant-less) owner boundary
        ] {
            assert!(validate_wipe_prefix(bad).is_err(), "should reject {bad:?}");
        }
    }

    /// A worker with a MALFORMED owner segment (a color/project that contains a
    /// slash or `..`) must be rejected by BOTH construction paths, not just
    /// key_for_put. Before the shared `owned_scope_segments`, prefix_for_list
    /// skipped the color/project check, so a malformed owner produced a list
    /// prefix that escaped the owner boundary.
    #[test]
    fn prefix_for_list_rejects_malformed_owner_like_key_for_put() {
        // Malformed color.
        let bad_color = worker(Some("../shared/team"));
        assert!(prefix_for_list(&bad_color, &StorageScope::Execution).is_err());
        assert!(key_for_put(&bad_color, &StorageScope::Execution, "f").is_err());
        // Malformed project id.
        let bad_project = CallerAuth::Worker {
            tenant: "t1".into(),
            project_id: "..".into(),
            color: None,
        };
        assert!(prefix_for_list(&bad_project, &StorageScope::Project).is_err());
        assert!(key_for_put(&bad_project, &StorageScope::Project, "f").is_err());
        // Malformed tenant.
        let bad_tenant = CallerAuth::Worker {
            tenant: "a/b".into(),
            project_id: "p1".into(),
            color: Some("c1".into()),
        };
        assert!(prefix_for_list(&bad_tenant, &StorageScope::Execution).is_err());
        assert!(key_for_put(&bad_tenant, &StorageScope::Execution, "f").is_err());
        // A shared name with a slash.
        let bad_shared = StorageScope::Shared { name: "a/b".into() };
        assert!(prefix_for_list(&worker(None), &bad_shared).is_err());
        assert!(key_for_put(&worker(None), &bad_shared, "f").is_err());
    }

    /// A clean worker still produces the expected owner-anchored prefixes (the fix
    /// must not have narrowed the happy path).
    #[test]
    fn prefix_for_list_happy_path_unchanged() {
        assert_eq!(
            prefix_for_list(&worker(Some("c1")), &StorageScope::Execution).unwrap(),
            "t1/exec/c1/"
        );
        assert_eq!(
            prefix_for_list(&worker(None), &StorageScope::Project).unwrap(),
            "t1/project/p1/"
        );
        assert_eq!(
            prefix_for_list(&worker(None), &StorageScope::Shared { name: "team".into() }).unwrap(),
            "t1/shared/team/"
        );
    }

    /// The control plane has no worker owner: both construction paths reject it.
    #[test]
    fn construction_paths_reject_control_plane() {
        assert!(prefix_for_list(&CallerAuth::ControlPlane, &StorageScope::Project).is_err());
        assert!(key_for_put(&CallerAuth::ControlPlane, &StorageScope::Project, "f").is_err());
    }

    /// `tenant_prefix` is a bucket-list prefix, so a blank / traversal / slashed
    /// tenant must be rejected (else prefix `/` or `../` ranges the whole bucket).
    #[test]
    fn tenant_prefix_validates_the_tenant_segment() {
        assert_eq!(ParsedKey::tenant_prefix("alice").unwrap(), "alice/");
        for bad in ["", "..", ".", "a/b", "a b"] {
            assert!(ParsedKey::tenant_prefix(bad).is_err(), "should reject {bad:?}");
        }
    }

    /// `asset_prefix` is the range the pre-build sync diffs + the project
    /// reclaimer wipes; both segments are validated so it can never range
    /// outside the tenant.
    #[test]
    fn asset_prefix_builds_and_walls() {
        assert_eq!(ParsedKey::asset_prefix("alice", "p1").unwrap(), "alice/asset/p1/");
        // A traversal / slashed / spaced segment on either side is rejected.
        for (t, p) in [("..", "p"), ("alice", ".."), ("a/b", "p"), ("alice", "a b"), ("alice", "")] {
            assert!(ParsedKey::asset_prefix(t, p).is_err(), "should reject {t:?}/{p:?}");
        }
    }

    /// The wall is total for the control plane on the data path: a control-plane
    /// caller is denied every scope (exec/project/shared), not just project.
    #[test]
    fn check_key_access_denies_control_plane_for_every_scope() {
        for scope in [
            KeyScope::Exec { color: "c1".into() },
            KeyScope::Project { project_id: "p1".into() },
            KeyScope::Shared { name: "team".into() },
        ] {
            assert!(
                check_key_access(&CallerAuth::ControlPlane, &pk("t1", scope.clone())).is_err(),
                "control plane must be denied on the data path for {scope:?}"
            );
        }
    }
}
