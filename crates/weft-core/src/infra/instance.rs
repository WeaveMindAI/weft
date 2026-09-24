//! Which install a process belongs to, when one cluster holds several.
//!
//! A cluster normally holds one weft install: the one `weft daemon start`
//! brings up, living in `weft-system` and `weft-db`, with every worker in
//! `wft-shared-workers` or a `wft-project-...` namespace. A NAMED install
//! (a test cell, say) sits beside it in the same cluster with names of its
//! own for every one of those, so its dispatcher, its database and its
//! pools never see the other install's. What the installs share is only
//! what the cluster itself provides: the node, the front door's gateway,
//! the object store container, and the images.
//!
//! Every name that differs between installs is answered here and nowhere
//! else. A process learns its install from `WEFT_INSTANCE` (unset is the
//! default install), which the daemon writes into the dispatcher and
//! broker manifests along with every namespace they render.

/// The variable a process reads its install's name from. Unset or empty
/// is the default install.
// SYNC: INSTANCE_ENV <-> crates/weft-cli/src/commands/daemon.rs (the
//       manifest substitution), deploy/k8s/dispatcher.yaml,
//       deploy/k8s/broker.yaml,
//       crates/weft-dispatcher/src/supervisor_pool.rs (the supervisor pod env),
//       crates/weft-infra-supervisor/src/main.rs (read at startup)
pub const INSTANCE_ENV: &str = "WEFT_INSTANCE";

/// The longest install name: the project namespace carries it next to a
/// 12-character tenant and a 12-character project, inside the 63
/// characters a namespace name may hold.
pub const MAX_INSTANCE_NAME: usize = 12;

/// One install's identity. The default install keeps the names every
/// existing cluster already uses.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct Instance {
    name: Option<String>,
}

impl Instance {
    /// The install a cluster holds when nobody named one.
    pub fn default_install() -> Self {
        Self { name: None }
    }

    /// A named install. The name becomes part of namespace and object
    /// names, so it is refused unless it is 1 to [`MAX_INSTANCE_NAME`]
    /// lowercase letters and digits starting with a letter.
    pub fn named(name: &str) -> Result<Self, String> {
        let ok = !name.is_empty()
            && name.len() <= MAX_INSTANCE_NAME
            && name.starts_with(|c: char| c.is_ascii_lowercase())
            && name.chars().all(|c| c.is_ascii_lowercase() || c.is_ascii_digit());
        if !ok {
            return Err(format!(
                "'{name}' cannot name an install: it goes into namespace names, so it must be \
                 1 to {MAX_INSTANCE_NAME} lowercase letters and digits, starting with a letter"
            ));
        }
        let named = Self { name: Some(name.to_string()) };
        // Removing a named install deletes every namespace starting with
        // its prefixes, so no default-install namespace may start with one.
        let default = Self::default_install();
        let taken = default
            .namespaces_and_prefixes()
            .into_iter()
            .find(|d| named.namespace_prefixes().iter().any(|p| d.starts_with(p.as_str())));
        if let Some(taken) = taken {
            return Err(format!(
                "'{name}' cannot name an install: the default install's namespace or namespace \
                 prefix '{taken}' starts with this install's prefix, so removing this install \
                 would remove it; pick another name"
            ));
        }
        Ok(named)
    }

    /// Every namespace name, or namespace-name prefix, this install owns.
    pub fn namespaces_and_prefixes(&self) -> [String; 4] {
        [
            self.system_namespace(),
            self.db_namespace(),
            self.shared_worker_namespace(),
            self.project_namespace_prefix(),
        ]
    }

    /// The prefixes that start every namespace a NAMED install owns
    /// (`weft-<name>-`, `wft-<name>-`); empty for the default install,
    /// whose names carry no prefix of their own.
    fn namespace_prefixes(&self) -> Vec<String> {
        match &self.name {
            None => Vec::new(),
            Some(n) => vec![format!("weft-{n}-"), self.tenant_prefix()],
        }
    }

    /// The install a raw `WEFT_INSTANCE` value names. Pure, so the rule
    /// is tested without the process environment.
    pub fn parse(raw: Option<&str>) -> Result<Self, String> {
        match raw.map(str::trim).filter(|r| !r.is_empty()) {
            None => Ok(Self::default_install()),
            Some(name) => Self::named(name).map_err(|e| format!("{INSTANCE_ENV}: {e}")),
        }
    }

    /// This process's install, from [`INSTANCE_ENV`].
    pub fn from_env() -> Result<Self, String> {
        Self::parse(std::env::var(INSTANCE_ENV).ok().as_deref())
    }

    /// The install's name, `None` for the default install.
    pub fn name(&self) -> Option<&str> {
        self.name.as_deref()
    }

    /// Where the dispatcher and the pooled listener and supervisor pods
    /// run.
    pub fn system_namespace(&self) -> String {
        self.weft_namespace("system")
    }

    /// Where the install's Postgres and broker run.
    pub fn db_namespace(&self) -> String {
        self.weft_namespace("db")
    }

    /// The one namespace every no-infra project's worker shares.
    pub fn shared_worker_namespace(&self) -> String {
        format!("{}shared-workers", self.tenant_prefix())
    }

    /// What every per-project namespace name starts with; the tenant and
    /// project follow it.
    pub fn project_namespace_prefix(&self) -> String {
        format!("{}project-", self.tenant_prefix())
    }

    /// A cluster-wide object's name (a ClusterRoleBinding, a
    /// PersistentVolume): `base` for the default install, `base-<name>`
    /// for a named one, so two installs never apply over each other's.
    pub fn cluster_object(&self, base: &str) -> String {
        match &self.name {
            None => base.to_string(),
            Some(n) => format!("{base}-{n}"),
        }
    }

    fn weft_namespace(&self, role: &str) -> String {
        match &self.name {
            None => format!("weft-{role}"),
            Some(n) => format!("weft-{n}-{role}"),
        }
    }

    /// `wft-` for the default install, `wft-<name>-` for a named one: the
    /// start of every namespace the dispatcher creates for tenants.
    pub fn tenant_prefix(&self) -> String {
        match &self.name {
            None => "wft-".to_string(),
            Some(n) => format!("wft-{n}-"),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_default_install_keeps_the_names_every_cluster_already_has() {
        let i = Instance::default_install();
        assert_eq!(i.system_namespace(), "weft-system");
        assert_eq!(i.db_namespace(), "weft-db");
        assert_eq!(i.shared_worker_namespace(), "wft-shared-workers");
        assert_eq!(i.project_namespace_prefix(), "wft-project-");
        assert_eq!(i.cluster_object("weft-dispatcher"), "weft-dispatcher");
        assert_eq!(Instance::parse(None), Ok(i.clone()));
        assert_eq!(Instance::parse(Some(" ")), Ok(i));
    }

    #[test]
    fn a_named_install_owns_a_name_for_everything() {
        let i = Instance::parse(Some("cell7")).unwrap();
        assert_eq!(i.system_namespace(), "weft-cell7-system");
        assert_eq!(i.db_namespace(), "weft-cell7-db");
        assert_eq!(i.shared_worker_namespace(), "wft-cell7-shared-workers");
        assert_eq!(i.project_namespace_prefix(), "wft-cell7-project-");
        assert_eq!(i.cluster_object("weft-dispatcher"), "weft-dispatcher-cell7");
    }

    #[test]
    fn a_name_that_cannot_live_in_a_namespace_is_refused() {
        for bad in ["Cell", "7cell", "cell-a", "abcdefghijklm", "cell_a"] {
            let err = Instance::parse(Some(bad)).unwrap_err();
            assert!(err.contains(INSTANCE_ENV) && err.contains(bad), "{err}");
        }
    }

    #[test]
    fn a_name_whose_prefix_starts_a_default_install_namespace_is_refused() {
        for bad in ["project", "shared"] {
            let err = Instance::named(bad).unwrap_err();
            assert!(err.contains("default install"), "{err}");
        }
        assert!(Instance::named("cell3").is_ok());
    }

    #[test]
    fn a_named_install_never_selects_a_default_install_namespace() {
        let default = Instance::default_install().namespaces_and_prefixes();
        for name in ["cell3", "a", "p", "s", "shar", "proj", "system", "db"] {
            let Ok(i) = Instance::named(name) else { continue };
            for ns in &default {
                for p in i.namespace_prefixes() {
                    assert!(!ns.starts_with(&p), "'{name}' prefix {p} selects {ns}");
                }
            }
        }
    }

    #[test]
    fn the_longest_name_still_fits_a_project_namespace() {
        let i = Instance::named(&"a".repeat(MAX_INSTANCE_NAME)).unwrap();
        // The dispatcher appends a 12-char tenant, `--`, a 12-char project.
        let longest = format!("{}{}--{}", i.project_namespace_prefix(), "t".repeat(12), "p".repeat(12));
        assert!(longest.len() <= 63, "{} chars: {longest}", longest.len());
    }
}
