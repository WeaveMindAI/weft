//! Storage keys + the identity wall, as pure functions (Layer 1).
//!
//! A key IS the fully-qualified path of a file. The FIRST segment is
//! the owning tenant; the rest encodes the scope:
//!   `<tenant>/exec/<execution_id>/<id>`        execution scratch (swept unless kept)
//!   `<tenant>/project/<project_id>/<id>`   per-project persistent
//!   `<tenant>/shared/<name>/<id>`       tenant-shared by agreed name
//!   `<tenant>/member/<project_id>/<member>/<id>`  one member's, in one project
//!
//! The wall: a verified caller identity + a key resolve to
//! allowed/denied with NO policy configuration. The runtime-storage
//! bucket is SHARED across every tenant (one bucket, keys namespaced by
//! the tenant prefix), so the tenant segment is the outer wall: a caller
//! can only ever reach keys under ITS OWN broker-verified tenant, and
//! within that, only prefixes it is proven to own (its own execution, its
//! own project) or has opted into by naming (shared). The broker is the
//! only thing that signs bucket requests, so these functions ARE the wall.

use super::StorageScope;

/// Verified caller identity, as resolved by the broker from the
/// presented token (which names the tenant and project), then, for a
/// claimed execution, that execution's row (its project and owner).
/// Nothing here is self-claimed; everything was checked against the DB
/// by the broker.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CallerAuth {
    /// A caller acting within `tenant`/`project_id`: a worker of that
    /// project (verified via its token), or the dispatcher acting for the
    /// tenant's editor session on the admin upload surface (the dispatcher
    /// vouches for the tenant, the broker re-checks the key against it).
    /// `execution_id` is the execution being driven (verified: the execution's owning
    /// process is the caller); None when no execution claim was presented (then
    /// execution-scoped keys are unreachable).
    Worker {
        tenant: String,
        project_id: String,
        execution_id: Option<String>,
        /// Who the run behind `execution_id` is for (verified with it); what a
        /// member-scoped handle with no member named falls back to.
        member: Option<String>,
    },
    /// The dispatcher (install control plane). Used only by the
    /// admin surface (presign, sweep, usage, wipe); the worker file
    /// verbs reject it so the data path stays worker-only.
    ControlPlane,
}

/// A parsed storage key: scope wall + file id.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum KeyScope {
    Exec { execution_id: String },
    Project { project_id: String },
    Shared { name: String },
    /// `asset/<project_id>/<sha256>`: a project ASSET, the published copy of a
    /// file the project's source references via a media `@file` ref. The id is
    /// the content hash, so existence == "this exact content is uploaded".
    /// Sync-managed derived state: created/deleted only by the pre-build asset
    /// sync (through the control-plane surface); workers of the project READ
    /// it like project scope but may not write it.
    Asset { project_id: String },
    /// `member/<project_id>/<member>/<id>`: one member's files in one
    /// project. The only scope whose owner is two segments: the project is
    /// the wall (a member id is only a name inside one project), the member
    /// is whose.
    Member { project_id: String, member: String },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ParsedKey {
    /// The owning tenant: the key's first segment, and the outer wall
    /// on a shared process. Always the broker-verified caller tenant on the
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
pub const SCOPE_TAGS: [&str; 5] = ["exec", "project", "shared", "asset", "member"];

/// Is `s` a TENANT-LESS runtime storage key (`<scope>/<owner>/<id>`, every
/// segment in the wall's grammar)? The short address a human writes in source
/// (`@asset("project/<id>/<file>", Image)`) and the form `weft files` shows;
/// the build's resolution re-anchors it to the acting tenant. Distinguishes a
/// storage address from an ordinary project path by the scope-tag first
/// segment plus its exact shape: 3 segments, or 4 for member scope
/// (`member/<project>/<member>/<id>`).
pub fn is_scope_key(s: &str) -> bool {
    let parts: Vec<&str> = s.split('/').collect();
    match parts.as_slice() {
        ["member", project, member, id] => valid_segment(project) && valid_segment(member) && valid_segment(id),
        [tag, owner, id] => *tag != "member" && is_scope_tag(tag) && valid_segment(owner) && valid_segment(id),
        _ => false,
    }
}

/// Is `s` a known on-wire scope tag? The one predicate every "looks like a
/// scope key" check consults (so the tag set never forks across files).
pub fn is_scope_tag(s: &str) -> bool {
    SCOPE_TAGS.contains(&s)
}

/// Render a TENANT-LESS scope key (`<scope>/<owner>/<id>`), the exact
/// inverse of `is_scope_key`.
///
/// The counterpart of `ParsedKey::to_key` for the short form, and the
/// only sanctioned way to build one. Both segments pass the wall's
/// grammar, so a key can never carry a `/` or a `..` out of a value
/// that came off the wire: the version tree builds asset keys from a
/// client-supplied manifest, and one bad entry there used to produce a
/// key the storage surface refuses, which poisoned that project's whole
/// asset-reference publish from then on.
pub fn scope_key(scope: &KeyScope, id: &str) -> Result<String, String> {
    for owner in scope.owner_segments() {
        if !valid_segment(owner) {
            return Err(format!("'{owner}' is not a valid key segment"));
        }
    }
    if !valid_segment(id) {
        return Err(format!("'{id}' is not a valid key segment"));
    }
    Ok(format!("{}/{}/{}", scope.tag(), scope.owner_path(), id))
}

impl KeyScope {
    /// Build the scope for a `(tag, owner)` pair, or None if `tag` is not
    /// a known scope tag. The canonical tag -> variant mapping; `parse_key`
    /// routes through here so the grammar and `SCOPE_TAGS` cannot drift.
    /// The member scope is not built here: its owner is two segments, and
    /// `parse_key` / `owned_scope` build it from both.
    fn from_tag(tag: &str, owner: &str) -> Option<Self> {
        match tag {
            "exec" => Some(KeyScope::Exec { execution_id: owner.to_string() }),
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
            KeyScope::Member { .. } => "member",
        }
    }

    /// The owner segments (execution / project id / shared name; project id
    /// and member for the member scope).
    fn owner_segments(&self) -> Vec<&str> {
        match self {
            KeyScope::Exec { execution_id } => vec![execution_id],
            KeyScope::Project { project_id } => vec![project_id],
            KeyScope::Shared { name } => vec![name],
            KeyScope::Asset { project_id } => vec![project_id],
            KeyScope::Member { project_id, member } => vec![project_id, member],
        }
    }

    /// The owner as it sits in a key, segments joined by `/`.
    fn owner_path(&self) -> String {
        self.owner_segments().join("/")
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
        format!("{}/{}/{}/{}", self.tenant, self.scope.tag(), self.scope.owner_path(), self.id)
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

    /// The `<tenant>/member/<project_id>/` prefix covering every member's
    /// files in one project: what the project reclaimer wipes with the
    /// project, and what `forget` of a member narrows to one member.
    pub fn members_prefix(tenant: &str, project: &str) -> Result<String, String> {
        Self::owned_prefix(tenant, "member", project)
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
/// separators, no traversal, no empties. Executions are UUIDs, project
/// ids are UUIDs, ids are UUIDs; shared names are user-chosen and
/// the reason this check exists.
// SYNC: valid_segment <-> packages/weft-graph/src/run-spec.ts MEMBER_ID_PATTERN
pub(crate) fn valid_segment(s: &str) -> bool {
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
    if let [tenant, "member", project, member, id] = parts.as_slice() {
        for (label, segment) in [("tenant", tenant), ("project", project), ("member", member), ("id", id)] {
            if !valid_segment(segment) {
                return Err(format!("malformed storage key '{key}': bad {label} segment"));
            }
        }
        return Ok(ParsedKey {
            tenant: tenant.to_string(),
            scope: KeyScope::Member { project_id: project.to_string(), member: member.to_string() },
            id: id.to_string(),
        });
    }
    let [tenant, scope_tag, owner, id] = parts.as_slice() else {
        return Err(format!(
            "malformed storage key '{key}': expected <tenant>/<scope>/<owner>/<id> (4 segments; \
             5 for <tenant>/member/<project>/<member>/<id>)"
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
///     (the dispatcher's `weft rm` / `weft clean <execution_id>` / project-delete).
///   - `<tenant>/` : the WHOLE tenant (a tenant-delete wiping every
///     object under the tenant's prefix).
/// Both are real prefix boundaries. A raw `starts_with` on an unanchored
/// string (empty, or `exec` without a slash) would wipe across tenants or
/// across owner boundaries (one execution's `t/exec/c1` also matching
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
        // One member's space within a project's members.
        [tenant, "member", project, member] => {
            for (label, segment) in [("tenant", tenant), ("project", project), ("member", member)] {
                if !valid_segment(segment) {
                    return Err(format!("wipe prefix '{prefix}': bad {label} segment"));
                }
            }
            Ok(())
        }
        // Owner boundary within a tenant (for the member scope: every
        // member of one project).
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
            "wipe prefix '{prefix}' must be <tenant>/, <tenant>/<scope>/<owner>/ ({}), or \
             <tenant>/member/<project>/<member>/",
            SCOPE_TAGS.join("|")
        )),
    }
}

/// Validate + resolve the OWNED `(tenant, scope)` a worker caller may address
/// under `scope`. THE one place the wall's construction rules live, shared by
/// `key_for_put` and `prefix_for_list` so neither can forget a check (an
/// earlier `prefix_for_list` validated the tenant + shared name but NOT the
/// execution / project id, so a malformed owner could produce a list prefix that
/// escaped the intended owner boundary; routing both through here makes the
/// two paths validate identically by construction).
///
/// Every returned segment has passed `valid_segment`, so a key or prefix
/// built from it is always one `parse_key` would also accept: the "a
/// ParsedKey is the proof a key passed the grammar" invariant holds by
/// CONSTRUCTION, not by the accident that the broker happens to supply UUIDs.
/// Errors when the caller is not a worker, an Execution scope carries no
/// execution, a Member scope names nobody in a run for nobody, or any segment is
/// not the wall's grammar.
fn owned_scope(caller: &CallerAuth, scope: &StorageScope) -> Result<(String, KeyScope), String> {
    let CallerAuth::Worker { tenant, project_id, execution_id, member } = caller else {
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
    let scope = match scope {
        StorageScope::Execution => {
            let execution_id = execution_id.as_deref().ok_or(
                "execution-scoped access requires a verified execution and the caller \
                 presented none",
            )?;
            KeyScope::Exec { execution_id: owned("execution_id", execution_id)? }
        }
        StorageScope::Project => KeyScope::Project { project_id: owned("project", project_id)? },
        StorageScope::Shared { name } => KeyScope::Shared { name: owned("shared-space name", name)? },
        // Assets are keyed like project scope (owner = the caller's project).
        // WHO may put here is route policy, not grammar: the worker data path
        // refuses asset-scope writes (assets are sync-managed), the
        // control-plane surface allows them; both build keys through this.
        StorageScope::Asset => KeyScope::Asset { project_id: owned("project", project_id)? },
        // The member named, or the run's own; always inside the caller's
        // project, which is the wall.
        StorageScope::Member { of } => {
            let member = match of {
                Some(of) => of.as_str().to_string(),
                None => member.clone().ok_or(
                    "member-scoped storage with no member named needs a run for a member, and \
                     this run is for nobody; name one with .of(member)",
                )?,
            };
            KeyScope::Member { project_id: owned("project", project_id)?, member: owned("member", &member)? }
        }
    };
    Ok((tenant, scope))
}

/// Build the `ParsedKey` for a fresh put under `scope` by `caller`.
/// Errors when the caller can't own the scope (no execution claim for
/// Execution scope, control-plane writes) or any segment is not the
/// wall's grammar. Every segment (including the `id`) is validated via
/// `owned_scope` + the explicit `id` check below.
pub fn key_for_put(caller: &CallerAuth, scope: &StorageScope, id: &str) -> Result<ParsedKey, String> {
    let (tenant, scope) = owned_scope(caller, scope)?;
    if !valid_segment(id) {
        return Err(format!("invalid id segment '{id}' for a storage key"));
    }
    Ok(ParsedKey { tenant, scope, id: id.to_string() })
}

/// The list prefix for `scope` as seen by `caller`, tenant-anchored.
/// Same ownership + grammar rules as `key_for_put` (both route through
/// `owned_scope`). The leading `<tenant>/` is the outer wall: a list
/// never sees another tenant's keys in the shared bucket.
pub fn prefix_for_list(caller: &CallerAuth, scope: &StorageScope) -> Result<String, String> {
    let (tenant, scope) = owned_scope(caller, scope)?;
    Ok(format!("{tenant}/{}/{}/", scope.tag(), scope.owner_path()))
}

/// The prefix covering one execution's files (`<tenant>/exec/<execution_id>/`), for
/// control-plane sweeps that act on an execution with no caller identity in hand.
/// Both segments are validated and the scope tag is rendered through the one
/// grammar (`KeyScope::tag`), so no caller ever hand-builds an exec prefix that
/// could drift from `SCOPE_TAGS` or smuggle a separator through an unvalidated
/// segment.
pub fn exec_prefix(tenant: &str, execution_id: &str) -> Result<String, String> {
    if !valid_segment(tenant) {
        return Err(format!("invalid tenant segment '{tenant}' for an exec prefix"));
    }
    if !valid_segment(execution_id) {
        return Err(format!("invalid execution segment '{execution_id}' for an exec prefix"));
    }
    let tag = KeyScope::Exec { execution_id: execution_id.to_string() }.tag();
    Ok(format!("{tenant}/{tag}/{execution_id}/"))
}

/// Can `caller` touch the file at `key` (get/delete/keep/presign)?
/// The TENANT segment is the outer wall (the shared bucket holds many
/// tenants' keys under one prefix space), then the key's own scope
/// decides. Deny reasons are specific.
pub fn check_key_access(caller: &CallerAuth, parsed: &ParsedKey) -> Result<(), String> {
    let CallerAuth::Worker { tenant, project_id, execution_id, .. } = caller else {
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
        KeyScope::Exec { execution_id: key_execution_id } => match execution_id {
            Some(c) if c == key_execution_id => Ok(()),
            Some(_) => Err(
                "denied: execution-scoped file belongs to a different execution (executions are \
                 walled per run; use Project scope for files that outlive a run)"
                    .into(),
            ),
            None => Err("denied: caller presented no verified execution".into()),
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
        // Any member of the caller's own project: inside its project the
        // program may reach every member's files; another project's never,
        // even under the same member id.
        KeyScope::Member { project_id: key_project, .. } => {
            if key_project == project_id {
                Ok(())
            } else {
                Err("denied: member-scoped file belongs to a different project".into())
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn worker(execution_id: Option<&str>) -> CallerAuth {
        CallerAuth::Worker {
            tenant: "t1".into(),
            project_id: "p1".into(),
            execution_id: execution_id.map(String::from),
            member: None,
        }
    }

    /// A parsed key under tenant `t1` for the wall tests (the tenant
    /// segment matches `worker()`'s tenant unless a test says otherwise).
    fn pk(tenant: &str, scope: KeyScope) -> ParsedKey {
        ParsedKey { tenant: tenant.into(), scope, id: "f".into() }
    }

    #[test]
    fn a_member_key_is_walled_by_its_project() {
        let key = parse_key("t1/member/p1/ada/f").expect("five segments");
        assert_eq!(key.scope, KeyScope::Member { project_id: "p1".into(), member: "ada".into() });
        assert_eq!(key.to_key(), "t1/member/p1/ada/f");
        assert!(check_key_access(&worker(None), &key).is_ok(), "any member of the caller's project");
        let other_project = parse_key("t1/member/p2/ada/f").unwrap();
        assert!(check_key_access(&worker(None), &other_project).is_err(), "same member id, other project");
        assert!(parse_key("t1/member/p1/../f").is_err());
        assert!(parse_key("t1/member/p1/f").is_err(), "a member key names its member");
    }

    #[test]
    fn a_member_handle_falls_back_to_the_runs_member() {
        let ada = CallerAuth::Worker {
            tenant: "t1".into(),
            project_id: "p1".into(),
            execution_id: Some("c1".into()),
            member: Some("ada".into()),
        };
        let own = key_for_put(&ada, &StorageScope::Member { of: None }, "f").unwrap();
        assert_eq!(own.to_key(), "t1/member/p1/ada/f");
        let bob = crate::member::MemberId::new("bob").unwrap();
        let named = key_for_put(&ada, &StorageScope::Member { of: Some(bob) }, "f").unwrap();
        assert_eq!(named.to_key(), "t1/member/p1/bob/f");
        let nobody = worker(Some("c1"));
        assert!(key_for_put(&nobody, &StorageScope::Member { of: None }, "f").unwrap_err().contains("for nobody"));
        assert_eq!(prefix_for_list(&ada, &StorageScope::Member { of: None }).unwrap(), "t1/member/p1/ada/");
        assert!(validate_wipe_prefix("t1/member/p1/ada/").is_ok());
        assert!(validate_wipe_prefix("t1/member/p1/").is_ok());
    }

    #[test]
    fn is_scope_key_accepts_tenant_less_keys_only() {
        for ok in ["exec/c1/f1", "project/p1/f2", "shared/team/f3", "asset/p1/f4", "member/p1/ada/f5"] {
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
            execution_id: None,
            member: None,
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
                scope: KeyScope::Exec { execution_id: "c1".into() },
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
        // No execution claim -> no exec writes.
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
            execution_id: Some("c1".into()),
            member: None,
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
                                    // Own execution under own tenant: allowed.
        assert!(check_key_access(&w, &pk("t1", KeyScope::Exec { execution_id: "c1".into() })).is_ok());
        // Another execution: denied.
        assert!(check_key_access(&w, &pk("t1", KeyScope::Exec { execution_id: "c2".into() })).is_err());
        // Own project: allowed. Another project: denied.
        assert!(check_key_access(&w, &pk("t1", KeyScope::Project { project_id: "p1".into() })).is_ok());
        assert!(check_key_access(&w, &pk("t1", KeyScope::Project { project_id: "p2".into() })).is_err());
        // Shared under own tenant: naming is the opt-in.
        assert!(check_key_access(&w, &pk("t1", KeyScope::Shared { name: "x".into() })).is_ok());
        // No execution claim cannot reach ANY exec key.
        assert!(check_key_access(&worker(None), &pk("t1", KeyScope::Exec { execution_id: "c1".into() })).is_err());
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
    /// execution, or a shared name). The tenant wall is checked first.
    #[test]
    fn access_denies_cross_tenant_even_when_inner_scope_matches() {
        let w = worker(Some("c1")); // tenant t1, project p1, execution c1
                                    // Another tenant's exec key with the SAME execution: denied by tenant.
        assert!(check_key_access(&w, &pk("t2", KeyScope::Exec { execution_id: "c1".into() })).is_err());
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
            "t1/exec/",     // scope without owner: would wipe all executions
            "t1/bogus/c1/",
            "t1/exec/../",
            "exec/c1/",     // the old (tenant-less) owner boundary
        ] {
            assert!(validate_wipe_prefix(bad).is_err(), "should reject {bad:?}");
        }
    }

    /// A worker with a MALFORMED owner segment (an execution/project that contains a
    /// slash or `..`) must be rejected by BOTH construction paths, not just
    /// key_for_put. Before the shared `owned_scope_segments`, prefix_for_list
    /// skipped the execution/project check, so a malformed owner produced a list
    /// prefix that escaped the owner boundary.
    #[test]
    fn prefix_for_list_rejects_malformed_owner_like_key_for_put() {
        // Malformed execution.
        let bad_execution_id = worker(Some("../shared/team"));
        assert!(prefix_for_list(&bad_execution_id, &StorageScope::Execution).is_err());
        assert!(key_for_put(&bad_execution_id, &StorageScope::Execution, "f").is_err());
        // Malformed project id.
        let bad_project = CallerAuth::Worker {
            tenant: "t1".into(),
            project_id: "..".into(),
            execution_id: None,
            member: None,
        };
        assert!(prefix_for_list(&bad_project, &StorageScope::Project).is_err());
        assert!(key_for_put(&bad_project, &StorageScope::Project, "f").is_err());
        // Malformed tenant.
        let bad_tenant = CallerAuth::Worker {
            tenant: "a/b".into(),
            project_id: "p1".into(),
            execution_id: Some("c1".into()),
            member: None,
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
            KeyScope::Exec { execution_id: "c1".into() },
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
