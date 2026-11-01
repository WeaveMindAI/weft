//! Storage keys + the tenant wall, as pure functions (Layer 1).
//!
//! A key IS the fully-qualified path of a file. The FIRST segment is
//! the owning tenant; the rest encodes the scope, which says how long the
//! file lives:
//!   `<tenant>/run/<run_id>/<id>`        deleted with its run's record
//!   `<tenant>/project/<project_id>/<id>`   deleted with its project
//!   `<tenant>/shared/<name>/<id>`       a space's, deleted by hand
//!   `<tenant>/instance/<project_id>/<instance>/<id>`  deleted by hand or with its project
//!   `<tenant>/asset/<sha256>`           the tenant's content-addressed files
//! A deadline (a keep with a time) deletes a file in any scope when it
//! arrives, whichever comes first.
//!
//! The wall: the runtime-storage bucket is SHARED across every tenant
//! (one bucket, keys namespaced by the tenant prefix), so a caller can
//! only ever reach keys under ITS OWN broker-verified tenant. Inside it,
//! any run of any project reads any file. A caller WRITES and LISTS under
//! the spaces it owns (its own run, its own project, an instance of it,
//! a shared space it names), which is where its files are put, not who
//! reads them. The broker is the only thing that signs bucket requests,
//! so these functions ARE the wall.

use super::StorageScope;

/// Verified caller identity, as resolved by the broker from the
/// presented token (which names the tenant and project), then, for a
/// claimed run, that run's row (its project and owner).
/// Nothing here is self-claimed; everything was checked against the DB
/// by the broker.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CallerAuth {
    /// A caller acting within `tenant`/`project_id`: a worker of that
    /// project (verified via its token), or the dispatcher acting for the
    /// tenant's editor session on the admin upload surface (the dispatcher
    /// vouches for the tenant, the broker re-checks the key against it).
    /// `run_id` is the run being driven (verified: the run's owning
    /// process is the caller); None when no run claim was presented (then
    /// it can put no run-scoped file, having no run to put it under).
    Worker {
        tenant: String,
        project_id: String,
        run_id: Option<String>,
        /// Who the run behind `run_id` is for (verified with it); what a
        /// instance-scoped handle with no instance named falls back to.
        instance: Option<String>,
    },
    /// The dispatcher acting for `tenant` on the tenant's asset plane
    /// (the admin upload surface: a version snapshot, a pre-build asset
    /// sync, the install's standard-library preload). The dispatcher
    /// vouches for the tenant; the wall then admits the tenant's
    /// content-addressed assets and nothing else, so no project, run
    /// or shared file is reachable through it.
    Tenant { tenant: String },
    /// The dispatcher acting for `tenant` as it copies the tenant's files
    /// in from another install (`weft migrate files`), on the admin upload
    /// surface. Each file keeps the key it had there (under this tenant),
    /// so the caller names every key itself: the wall admits any of the
    /// tenant's keys and nothing of another tenant's, and no key is ever
    /// minted for it.
    Copying { tenant: String },
    /// The dispatcher (install control plane). Used only by the
    /// admin surface (presign, sweep, usage, wipe); the worker file
    /// verbs reject it so the data path stays worker-only.
    ControlPlane,
}

/// A parsed storage key: scope wall + file id.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum KeyScope {
    Run { run_id: String },
    Project { project_id: String },
    Shared { name: String },
    /// `asset/<sha256>`: one of the TENANT's content-addressed files, the
    /// stored copy of a file some project of the tenant holds (a version's
    /// file, a media `@asset` the source references). No owner segment: the
    /// id is the content hash, so one content is one file per tenant
    /// whichever projects name it, and existence == "this exact content is
    /// stored". Kept alive by the projects that reference it (the broker's
    /// `asset_reference` rows), written only through the admin upload
    /// surface; any worker of the tenant may READ it, none may write it.
    Asset,
    /// `instance/<project_id>/<instance>/<id>`: one instance's files in one
    /// project. The only scope whose owner is two segments: the project
    /// bounds where a worker puts and lists (an instance id is only a name
    /// inside one project), and the files go with it; the instance is whose.
    Instance { project_id: String, instance: String },
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
pub const SCOPE_TAGS: [&str; 5] = ["run", "project", "shared", "asset", "instance"];

/// Is `s` a TENANT-LESS runtime storage key (`<scope>/<owner>/<id>`, every
/// segment in the wall's grammar)? The short address a human writes in source
/// (`@asset("project/<id>/<file>", Image)`) and the form `weft files` shows;
/// the build's resolution re-anchors it to the acting tenant. Distinguishes a
/// storage address from an ordinary project path by the scope-tag first
/// segment plus its exact shape: 3 segments, 4 for instance scope
/// (`instance/<project>/<instance>/<id>`), and 2 for the asset scope, whose
/// id must be a content hash (`asset/<sha256>`), so a project folder named
/// `asset` is never mistaken for one.
// SYNC: is_scope_key <-> packages/weft-graph/src/webview/lib/utils/storage-key.ts isScopeKey
pub fn is_scope_key(s: &str) -> bool {
    let parts: Vec<&str> = s.split('/').collect();
    match parts.as_slice() {
        ["asset", id] => super::is_content_hash(id),
        ["instance", project, instance, id] => valid_segment(project) && valid_segment(instance) && valid_segment(id),
        [tag, owner, id] => !matches!(*tag, "instance" | "asset") && is_scope_tag(tag) && valid_segment(owner) && valid_segment(id),
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
    if matches!(scope, KeyScope::Asset) && !super::is_content_hash(id) {
        return Err(format!("'{id}' is not a content hash, and an asset is named by its content's sha256"));
    }
    Ok(format!("{}/{}", scope.path(), id))
}

impl KeyScope {
    /// The scope a writer names to store a file under this one; an
    /// instance segment that is no instance id is refused, naming it.
    pub fn storage_scope(&self) -> Result<StorageScope, String> {
        Ok(match self {
            KeyScope::Run { .. } => StorageScope::Run,
            KeyScope::Project { .. } => StorageScope::Project,
            KeyScope::Shared { name } => StorageScope::Shared { name: name.clone() },
            KeyScope::Asset => StorageScope::Asset,
            KeyScope::Instance { instance, .. } => StorageScope::Instance { of: Some(crate::instance::InstanceId::new(instance)?) },
        })
    }

    /// Build the scope for a `(tag, owner)` pair, or None if `tag` is not
    /// a known scope tag. The canonical tag -> variant mapping; `parse_key`
    /// routes through here so the grammar and `SCOPE_TAGS` cannot drift.
    /// The instance and asset scopes are not built here: the one's owner
    /// is two segments and the other has none, and `parse_key` /
    /// `owned_scope` build them by shape.
    fn from_tag(tag: &str, owner: &str) -> Option<Self> {
        match tag {
            "run" => Some(KeyScope::Run { run_id: owner.to_string() }),
            "project" => Some(KeyScope::Project { project_id: owner.to_string() }),
            "shared" => Some(KeyScope::Shared { name: owner.to_string() }),
            _ => None,
        }
    }

    /// The on-wire scope tag (`run`/`project`/`shared`/`asset`).
    fn tag(&self) -> &'static str {
        match self {
            KeyScope::Run { .. } => "run",
            KeyScope::Project { .. } => "project",
            KeyScope::Shared { .. } => "shared",
            KeyScope::Asset => "asset",
            KeyScope::Instance { .. } => "instance",
        }
    }

    /// The owner segments (run / project id / shared name; project id
    /// and instance for the instance scope; none for the tenant's assets).
    fn owner_segments(&self) -> Vec<&str> {
        match self {
            KeyScope::Run { run_id } => vec![run_id],
            KeyScope::Project { project_id } => vec![project_id],
            KeyScope::Shared { name } => vec![name],
            KeyScope::Asset => vec![],
            KeyScope::Instance { project_id, instance } => vec![project_id, instance],
        }
    }

    /// The scope as it sits in a key: the tag, then its owner segments,
    /// joined by `/`.
    fn path(&self) -> String {
        std::iter::once(self.tag()).chain(self.owner_segments()).collect::<Vec<_>>().join("/")
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
        format!("{}/{}/{}", self.tenant, self.scope.path(), self.id)
    }

    /// The tenant's asset named by `hash`: `<tenant>/asset/<sha256>`.
    /// Fallible: the tenant must pass the segment grammar and the hash must
    /// be a content hash, so the key is always one `parse_key` accepts.
    pub fn asset(tenant: &str, hash: &str) -> Result<Self, String> {
        if !valid_segment(tenant) {
            return Err(format!("invalid tenant segment '{tenant}' for an asset key"));
        }
        if !super::is_content_hash(hash) {
            return Err(format!("'{hash}' is not a content hash, and an asset is named by its content's sha256"));
        }
        Ok(ParsedKey { tenant: tenant.to_string(), scope: KeyScope::Asset, id: hash.to_string() })
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

    /// The prefixes holding every file that goes with a project when it is
    /// removed: its own files (`<tenant>/project/<project_id>/`) and every
    /// instance's (`<tenant>/instance/<project_id>/`), since an instance id
    /// is only a name inside its project. Its runs' files go with their
    /// records, and shared files are a space's, so neither is here.
    pub fn removed_with_project(tenant: &str, project: &str) -> Result<[String; 2], String> {
        Ok([Self::project_prefix(tenant, project)?, Self::owned_prefix(tenant, "instance", project)?])
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
/// separators, no traversal, no empties. Runs are UUIDs, project
/// ids are UUIDs, ids are UUIDs; shared names are user-chosen and
/// the reason this check exists.
// SYNC: valid_segment <-> packages/weft-graph/src/run-spec.ts INSTANCE_ID_PATTERN, crates/weft-core/src/instance.rs InstanceId::new
pub fn valid_segment(s: &str) -> bool {
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
    if let [tenant, "asset", id] = parts.as_slice() {
        return ParsedKey::asset(tenant, id).map_err(|e| format!("malformed storage key '{key}': {e}"));
    }
    if let [tenant, "instance", project, instance, id] = parts.as_slice() {
        for (label, segment) in [("tenant", tenant), ("project", project), ("instance", instance), ("id", id)] {
            if !valid_segment(segment) {
                return Err(format!("malformed storage key '{key}': bad {label} segment"));
            }
        }
        return Ok(ParsedKey {
            tenant: tenant.to_string(),
            scope: KeyScope::Instance { project_id: project.to_string(), instance: instance.to_string() },
            id: id.to_string(),
        });
    }
    let [tenant, scope_tag, owner, id] = parts.as_slice() else {
        return Err(format!(
            "malformed storage key '{key}': expected <tenant>/<scope>/<owner>/<id> (4 segments; \
             5 for <tenant>/instance/<project>/<instance>/<id>, 3 for <tenant>/asset/<sha256>)"
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
///     (a run's files once its record went, a removed project's and its
///     instances' files, `weft files rm --prefix`).
///   - `<tenant>/` : the WHOLE tenant (a tenant-delete wiping every
///     object under the tenant's prefix).
/// Both are real prefix boundaries. A raw `starts_with` on an unanchored
/// string (empty, or `run` without a slash) would wipe across tenants or
/// across owner boundaries (one run's `t/run/c1` also matching
/// `t/run/c1abc`); validating the trailing slash + the segment grammar
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
        // One instance's space within a project's instances.
        [tenant, "instance", project, instance] => {
            for (label, segment) in [("tenant", tenant), ("project", project), ("instance", instance)] {
                if !valid_segment(segment) {
                    return Err(format!("wipe prefix '{prefix}': bad {label} segment"));
                }
            }
            Ok(())
        }
        // Owner boundary within a tenant (for the instance scope: every
        // instance of one project).
        [tenant, scope_tag, owner] => {
            if !valid_segment(tenant) {
                return Err(format!("wipe prefix '{prefix}': bad tenant segment"));
            }
            // The tenant's assets have no owner to bound a wipe by: they go
            // when nothing references them, or with the whole tenant.
            if !is_scope_tag(scope_tag) || *scope_tag == "asset" {
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
             <tenant>/instance/<project>/<instance>/",
            SCOPE_TAGS.join("|")
        )),
    }
}

/// Validate + resolve the OWNED `(tenant, scope)` a worker caller may address
/// under `scope`. THE one place the wall's construction rules live, shared by
/// `key_for_put` and `prefix_for_list` so neither can forget a check (an
/// earlier `prefix_for_list` validated the tenant + shared name but NOT the
/// run / project id, so a malformed owner could produce a list prefix that
/// escaped the intended owner boundary; routing both through here makes the
/// two paths validate identically by construction).
///
/// Every returned segment has passed `valid_segment`, so a key or prefix
/// built from it is always one `parse_key` would also accept: the "a
/// ParsedKey is the proof a key passed the grammar" invariant holds by
/// CONSTRUCTION, not by the accident that the broker happens to supply UUIDs.
/// Errors when the caller is not a worker, a Run scope carries no
/// run, an Instance scope names no instance in a run for no instance, or any segment is
/// not the wall's grammar.
fn owned_scope(caller: &CallerAuth, scope: &StorageScope) -> Result<(String, KeyScope), String> {
    let (tenant, project_id, run_id, instance) = match caller {
        CallerAuth::Worker { tenant, project_id, run_id, instance } => (tenant, project_id, run_id, instance),
        CallerAuth::Tenant { tenant } => {
            if !matches!(scope, StorageScope::Asset) {
                return Err("a caller acting for the tenant reaches only the tenant's assets".into());
            }
            if !valid_segment(tenant) {
                return Err(format!("invalid tenant segment '{tenant}' for a storage key"));
            }
            return Ok((tenant.clone(), KeyScope::Asset));
        }
        CallerAuth::ControlPlane => {
            return Err("only workers address scoped files; the control plane uses the admin surface".into());
        }
        CallerAuth::Copying { .. } => {
            return Err("a copied file names its own key; none is minted for it".into());
        }
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
        StorageScope::Run => {
            let run_id = run_id.as_deref().ok_or(
                "run-scoped access requires a verified run and the caller \
                 presented none",
            )?;
            KeyScope::Run { run_id: owned("run_id", run_id)? }
        }
        StorageScope::Project => KeyScope::Project { project_id: owned("project", project_id)? },
        StorageScope::Shared { name } => KeyScope::Shared { name: owned("shared-space name", name)? },
        // The tenant's assets have no owner. WHO may put here is route
        // policy, not grammar: the worker data path refuses asset-scope
        // writes, the admin upload surface (a `Tenant` caller) makes them.
        StorageScope::Asset => KeyScope::Asset,
        // The instance named, or the run's own; always inside the caller's
        // project, which is the wall.
        StorageScope::Instance { of } => {
            let instance = match of {
                Some(of) => of.as_str().to_string(),
                None => instance.clone().ok_or(
                    "instance-scoped storage with no instance named needs a run for an instance, and \
                     this run is for no instance; name one with .of(instance)",
                )?,
            };
            KeyScope::Instance { project_id: owned("project", project_id)?, instance: owned("instance", &instance)? }
        }
    };
    Ok((tenant, scope))
}

/// Build the `ParsedKey` for a fresh put under `scope` by `caller`.
/// Errors when the caller can't own the scope (no run claim for
/// Run scope, control-plane writes) or any segment is not the
/// wall's grammar. Every segment (including the `id`) is validated via
/// `owned_scope` + the explicit `id` check below.
pub fn key_for_put(caller: &CallerAuth, scope: &StorageScope, id: &str) -> Result<ParsedKey, String> {
    let (tenant, scope) = owned_scope(caller, scope)?;
    if matches!(scope, KeyScope::Asset) {
        return ParsedKey::asset(&tenant, id);
    }
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
    Ok(format!("{tenant}/{}/", scope.path()))
}

/// The prefix covering one run's files (`<tenant>/run/<run_id>/`), for
/// control-plane sweeps that act on a run with no caller identity in hand.
/// Both segments are validated and the scope tag is rendered through the one
/// grammar (`KeyScope::tag`), so no caller ever hand-builds an run prefix that
/// could drift from `SCOPE_TAGS` or smuggle a separator through an unvalidated
/// segment.
pub fn run_prefix(tenant: &str, run_id: &str) -> Result<String, String> {
    if !valid_segment(tenant) {
        return Err(format!("invalid tenant segment '{tenant}' for an run prefix"));
    }
    if !valid_segment(run_id) {
        return Err(format!("invalid run segment '{run_id}' for an run prefix"));
    }
    let tag = KeyScope::Run { run_id: run_id.to_string() }.tag();
    Ok(format!("{tenant}/{tag}/{run_id}/"))
}

/// Can `caller` touch the file at `key` (get/delete/keep/presign)?
/// The TENANT segment is the one wall (the shared bucket holds many
/// tenants' keys under one prefix space): inside its tenant, any run of
/// any project reads any file, whatever its scope. A scope says how long
/// a file lives, never who may read it. Deny reasons are specific.
// SYNC: check_key_access <-> extension-vscode/src/storedFilePicker.ts storedFilePickerRows
pub fn check_key_access(caller: &CallerAuth, parsed: &ParsedKey) -> Result<(), String> {
    let tenant = match caller {
        CallerAuth::Worker { tenant, .. } => tenant,
        // Acting for a tenant reaches that tenant's assets, nothing else.
        CallerAuth::Tenant { tenant } => {
            if &parsed.tenant != tenant {
                return Err(format!(
                    "denied: file belongs to tenant '{}', not the acting tenant '{tenant}'",
                    parsed.tenant
                ));
            }
            return match parsed.scope {
                KeyScope::Asset => Ok(()),
                _ => Err("denied: a caller acting for the tenant reaches only the tenant's assets".into()),
            };
        }
        CallerAuth::Copying { tenant } => {
            return match &parsed.tenant == tenant {
                true => Ok(()),
                false => Err(format!("denied: file belongs to tenant '{}', not the copying tenant '{tenant}'", parsed.tenant)),
            };
        }
        // Admin verbs run on dedicated routes; a control-plane call
        // landing on the worker data path is a caller bug.
        CallerAuth::ControlPlane => {
            return Err("control-plane callers use the admin surface, not the data path".into());
        }
    };
    // The bucket is shared, so a worker must never reach a key under a
    // different tenant's prefix. The caller tenant is the broker verdict;
    // the key tenant is the first path segment.
    if &parsed.tenant != tenant {
        return Err(format!(
            "denied: file belongs to tenant '{}', not the caller's tenant '{tenant}'",
            parsed.tenant
        ));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn worker(run_id: Option<&str>) -> CallerAuth {
        CallerAuth::Worker {
            tenant: "t1".into(),
            project_id: "p1".into(),
            run_id: run_id.map(String::from),
            instance: None,
        }
    }

    /// A parsed key under tenant `t1` for the wall tests (the tenant
    /// segment matches `worker()`'s tenant unless a test says otherwise).
    fn pk(tenant: &str, scope: KeyScope) -> ParsedKey {
        ParsedKey { tenant: tenant.into(), scope, id: "f".into() }
    }

    #[test]
    fn an_instance_key_names_its_project_and_instance() {
        let key = parse_key("t1/instance/p1/ada/f").expect("five segments");
        assert_eq!(key.scope, KeyScope::Instance { project_id: "p1".into(), instance: "ada".into() });
        assert_eq!(key.to_key(), "t1/instance/p1/ada/f");
        assert!(parse_key("t1/instance/p1/../f").is_err());
        assert!(parse_key("t1/instance/p1/f").is_err(), "an instance key names its instance");
    }

    #[test]
    fn a_instance_handle_falls_back_to_the_runs_instance() {
        let ada = CallerAuth::Worker {
            tenant: "t1".into(),
            project_id: "p1".into(),
            run_id: Some("c1".into()),
            instance: Some("ada".into()),
        };
        let own = key_for_put(&ada, &StorageScope::Instance { of: None }, "f").unwrap();
        assert_eq!(own.to_key(), "t1/instance/p1/ada/f");
        let bob = crate::instance::InstanceId::new("bob").unwrap();
        let named = key_for_put(&ada, &StorageScope::Instance { of: Some(bob) }, "f").unwrap();
        assert_eq!(named.to_key(), "t1/instance/p1/bob/f");
        let nobody = worker(Some("c1"));
        assert!(key_for_put(&nobody, &StorageScope::Instance { of: None }, "f").unwrap_err().contains("for no instance"));
        assert_eq!(prefix_for_list(&ada, &StorageScope::Instance { of: None }).unwrap(), "t1/instance/p1/ada/");
        assert!(validate_wipe_prefix("t1/instance/p1/ada/").is_ok());
        assert!(validate_wipe_prefix("t1/instance/p1/").is_ok());
    }

    #[test]
    fn is_scope_key_accepts_tenant_less_keys_only() {
        let sha = "a".repeat(64);
        for ok in ["run/c1/f1", "project/p1/f2", "shared/team/f3", &format!("asset/{sha}"), "instance/p1/ada/f5"] {
            assert!(is_scope_key(ok), "{ok}");
        }
        for no in [
            "asset/logo.png",    // a project folder called `asset`
            "asset/p1/f4",       // the old per-project asset shape
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
        let asset = format!("t1/asset/{}", "a".repeat(64));
        for k in ["t1/run/c1/f1", "t1/project/p1/f2", "t1/shared/team/f3", &asset] {
            assert_eq!(parse_key(k).unwrap().to_key(), k, "round-trip {k}");
        }
    }

    /// The asset scope's grammar: `<tenant>/asset/<sha256>`, no owner, the id
    /// a content hash. The wall admits every worker of the tenant (reads) and
    /// a caller acting for the tenant, and nobody of another tenant.
    #[test]
    fn asset_scope_is_tenant_wide_and_content_addressed() {
        let sha = "a".repeat(64);
        let key = format!("t1/asset/{sha}");
        let parsed = parse_key(&key).unwrap();
        assert_eq!(parsed.scope, KeyScope::Asset);
        assert_eq!(parsed.id, sha);
        assert_eq!(parsed.to_key(), key);
        assert_eq!(scope_key(&KeyScope::Asset, &sha).unwrap(), format!("asset/{sha}"));
        for bad in ["t1/asset/p1/".to_string() + &sha, "t1/asset/not-a-hash".into(), format!("../asset/{sha}")] {
            assert!(parse_key(&bad).is_err(), "should reject {bad:?}");
        }
        assert!(scope_key(&KeyScope::Asset, "f").is_err());

        // Any project of the tenant may read it; the control plane uses the
        // admin surface, not the data path.
        let asset = pk("t1", KeyScope::Asset);
        assert!(check_key_access(&worker(None), &asset).is_ok());
        let other_project = CallerAuth::Worker {
            tenant: "t1".into(),
            project_id: "p2".into(),
            run_id: None,
            instance: None,
        };
        assert!(check_key_access(&other_project, &asset).is_ok());
        assert!(check_key_access(&CallerAuth::ControlPlane, &asset).is_err());
        let copying = CallerAuth::Copying { tenant: "t1".into() };
        assert!(check_key_access(&copying, &pk("t1", KeyScope::Project { project_id: "p".into() })).is_ok(), "a copy reaches every key of its tenant");
        assert!(check_key_access(&copying, &pk("t2", KeyScope::Project { project_id: "p".into() })).is_err(), "and none of another's");
        assert!(key_for_put(&copying, &StorageScope::Project, "id").is_err(), "and is minted no key");
        // The tenant wall holds: another tenant's worker, or a caller acting
        // for another tenant, is denied the same content.
        let foreign = pk("t2", KeyScope::Asset);
        assert!(check_key_access(&worker(None), &foreign).is_err());
        let acting = CallerAuth::Tenant { tenant: "t1".into() };
        assert!(check_key_access(&acting, &asset).is_ok());
        assert!(check_key_access(&acting, &foreign).is_err());
        // Acting for the tenant reaches its assets and nothing else.
        assert!(check_key_access(&acting, &pk("t1", KeyScope::Project { project_id: "p1".into() })).is_err());
        assert!(key_for_put(&acting, &StorageScope::Project, "f").is_err());

        // A put builds the tenant's key whoever of the tenant asks, and refuses
        // an id that is not a content hash.
        assert_eq!(key_for_put(&acting, &StorageScope::Asset, &sha).unwrap().to_key(), key);
        assert_eq!(key_for_put(&worker(None), &StorageScope::Asset, &sha).unwrap().to_key(), key);
        assert!(key_for_put(&acting, &StorageScope::Asset, "f").is_err());
        assert_eq!(prefix_for_list(&acting, &StorageScope::Asset).unwrap(), "t1/asset/");
        // No owner to bound a wipe by.
        assert!(validate_wipe_prefix(&format!("t1/asset/{sha}/")).is_err());
    }

    #[test]
    fn parse_round_trips_each_scope() {
        assert_eq!(
            parse_key("t1/run/c1/f1").unwrap(),
            ParsedKey {
                tenant: "t1".into(),
                scope: KeyScope::Run { run_id: "c1".into() },
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
        assert_eq!(parse_key("TenantMixedCase/run/c1/f1").unwrap().tenant, "TenantMixedCase");
    }

    #[test]
    fn parse_rejects_malformed() {
        for bad in [
            "",
            "t1/run/c1",      // 3 segments (the old grammar)
            "t1/run/c1/f1/x", // 5 segments
            "t1/bogus/c1/f1",
            "t1/run//f1",
            "t1/run/../f1",
            "t1/shared/na me/f1",
            "t1/run/c1/",
            "/run/c1/f1", // empty tenant
        ] {
            assert!(parse_key(bad).is_err(), "should reject {bad:?}");
        }
    }

    #[test]
    fn put_builds_caller_owned_prefixes_only() {
        let w = worker(Some("c1"));
        // The tenant (t1, from the verdict) is the first segment.
        assert_eq!(key_for_put(&w, &StorageScope::Run, "id").unwrap().to_key(), "t1/run/c1/id");
        assert_eq!(key_for_put(&w, &StorageScope::Project, "id").unwrap().to_key(), "t1/project/p1/id");
        assert_eq!(
            key_for_put(&w, &StorageScope::Shared { name: "team".into() }, "id").unwrap().to_key(),
            "t1/shared/team/id"
        );
        // No run claim -> no run writes.
        assert!(key_for_put(&worker(None), &StorageScope::Run, "id").is_err());
        // Control plane never puts.
        assert!(key_for_put(&CallerAuth::ControlPlane, &StorageScope::Project, "id").is_err());
        // EVERY segment is validated, so key_for_put can only mint a
        // ParsedKey that parse_key would also accept (the invariant).
        // A bad shared name, id, owner, OR tenant is rejected.
        assert!(key_for_put(&w, &StorageScope::Shared { name: "a/b".into() }, "id").is_err());
        assert!(key_for_put(&w, &StorageScope::Run, "bad/id").is_err());
        assert!(key_for_put(&w, &StorageScope::Run, "..").is_err());
        assert!(key_for_put(&worker(Some("c/d")), &StorageScope::Run, "id").is_err());
        let bad_tenant = CallerAuth::Worker {
            tenant: "a/b".into(),
            project_id: "p1".into(),
            run_id: Some("c1".into()),
            instance: None,
        };
        assert!(key_for_put(&bad_tenant, &StorageScope::Project, "id").is_err());
    }

    #[test]
    fn key_for_put_output_always_round_trips_through_parse_key() {
        // The construction invariant: whatever key_for_put mints, parse_key
        // accepts and renders back identically (the two paths agree).
        let w = worker(Some("0188-c0102"));
        for scope in [
            StorageScope::Run,
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
        assert_eq!(prefix_for_list(&w, &StorageScope::Run).unwrap(), "t1/run/c1/");
        assert_eq!(prefix_for_list(&w, &StorageScope::Project).unwrap(), "t1/project/p1/");
        assert_eq!(
            prefix_for_list(&w, &StorageScope::Shared { name: "team".into() }).unwrap(),
            "t1/shared/team/"
        );
    }

    /// Inside its tenant, a worker reads any file whatever its scope: its
    /// own run's or another run's, its project's or another project's,
    /// any instance's of any project, a shared space's, with or without a
    /// run of its own. A scope says how long a file lives, never who reads
    /// it. The control plane is still kept off the data path.
    #[test]
    fn a_worker_reads_every_file_of_its_tenant() {
        for caller in [worker(Some("c1")), worker(None)] {
            for scope in [
                KeyScope::Run { run_id: "c1".into() },
                KeyScope::Run { run_id: "c2".into() },
                KeyScope::Project { project_id: "p1".into() },
                KeyScope::Project { project_id: "p2".into() },
                KeyScope::Instance { project_id: "p1".into(), instance: "ada".into() },
                KeyScope::Instance { project_id: "p2".into(), instance: "bob".into() },
                KeyScope::Shared { name: "x".into() },
                KeyScope::Asset,
            ] {
                assert!(check_key_access(&caller, &pk("t1", scope.clone())).is_ok(), "{caller:?} reads {scope:?}");
            }
        }
        assert!(check_key_access(
            &CallerAuth::ControlPlane,
            &pk("t1", KeyScope::Project { project_id: "p1".into() })
        )
        .is_err());
    }

    /// THE load-bearing isolation proof on the shared bucket: a worker of one
    /// tenant can reach NOTHING under another tenant's prefix, even a key
    /// whose inner scope it would otherwise own (same project id, same
    /// run, or a shared name). The tenant wall is checked first.
    #[test]
    fn access_denies_cross_tenant_even_when_inner_scope_matches() {
        let w = worker(Some("c1")); // tenant t1, project p1, run c1
                                    // Another tenant's run key with the SAME run: denied by tenant.
        assert!(check_key_access(&w, &pk("t2", KeyScope::Run { run_id: "c1".into() })).is_err());
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
    fn a_removed_project_takes_its_own_and_its_instances_files_and_nothing_else() {
        let prefixes = ParsedKey::removed_with_project("t1", "p1").unwrap();
        let goes = |key: &str| prefixes.iter().any(|p| key.starts_with(p.as_str()));
        assert!(goes("t1/project/p1/f0"));
        assert!(goes("t1/instance/p1/ada/f0"));
        assert!(!goes("t1/project/p10/f0"), "another project");
        assert!(!goes("t1/instance/p2/ada/f0"), "another project's instance");
        assert!(!goes("t1/shared/team/f0"), "a shared space");
        assert!(!goes("t2/project/p1/f0"), "another tenant");
        for prefix in &prefixes {
            assert!(validate_wipe_prefix(prefix).is_ok(), "{prefix}");
        }
    }

    #[test]
    fn validate_wipe_prefix_accepts_tenant_and_owner_boundaries() {
        // Whole tenant.
        assert!(validate_wipe_prefix("t1/").is_ok());
        // Owner boundary within a tenant.
        assert!(validate_wipe_prefix("t1/run/c1/").is_ok());
        assert!(validate_wipe_prefix("t1/project/p1/").is_ok());
        assert!(validate_wipe_prefix("t1/shared/team/").is_ok());
        // Rejected: no trailing slash, empty, a bare scope (no owner),
        // a bad scope tag, traversal, and the OLD slashless tenant shapes.
        for bad in [
            "t1",
            "",
            "/",
            "t1/run/",     // scope without owner: would wipe all runs
            "t1/bogus/c1/",
            "t1/run/../",
            "run/c1/",     // the old (tenant-less) owner boundary
        ] {
            assert!(validate_wipe_prefix(bad).is_err(), "should reject {bad:?}");
        }
    }

    /// A worker with a MALFORMED owner segment (a run/project that contains a
    /// slash or `..`) must be rejected by BOTH construction paths, not just
    /// key_for_put: both go through `owned_scope`, so a malformed owner
    /// can never produce a list prefix that escapes the owner boundary.
    #[test]
    fn prefix_for_list_rejects_malformed_owner_like_key_for_put() {
        // Malformed run.
        let bad_run_id = worker(Some("../shared/team"));
        assert!(prefix_for_list(&bad_run_id, &StorageScope::Run).is_err());
        assert!(key_for_put(&bad_run_id, &StorageScope::Run, "f").is_err());
        // Malformed project id.
        let bad_project = CallerAuth::Worker {
            tenant: "t1".into(),
            project_id: "..".into(),
            run_id: None,
            instance: None,
        };
        assert!(prefix_for_list(&bad_project, &StorageScope::Project).is_err());
        assert!(key_for_put(&bad_project, &StorageScope::Project, "f").is_err());
        // Malformed tenant.
        let bad_tenant = CallerAuth::Worker {
            tenant: "a/b".into(),
            project_id: "p1".into(),
            run_id: Some("c1".into()),
            instance: None,
        };
        assert!(prefix_for_list(&bad_tenant, &StorageScope::Run).is_err());
        assert!(key_for_put(&bad_tenant, &StorageScope::Run, "f").is_err());
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
            prefix_for_list(&worker(Some("c1")), &StorageScope::Run).unwrap(),
            "t1/run/c1/"
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

    /// The wall is total for the control plane on the data path: a control-plane
    /// caller is denied every scope (run/project/shared), not just project.
    #[test]
    fn check_key_access_denies_control_plane_for_every_scope() {
        for scope in [
            KeyScope::Run { run_id: "c1".into() },
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
