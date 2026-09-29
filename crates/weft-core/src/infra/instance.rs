//! Which install a process belongs to, when one machine holds several.
//!
//! A machine normally holds one weft install: the one `weft daemon start`
//! brings up. A NAMED install (a test cell, say) sits beside it with names
//! of its own for everything it creates on the machine or in a cloud
//! project (its database, its worker and infra containers, its services),
//! so the two never touch each other's. Every such name starts with
//! [`Instance::resource_prefix`], answered here and nowhere else.

/// The longest install name: it goes into container and service names
/// next to a unit or project suffix.
pub const MAX_INSTANCE_NAME: usize = 12;

/// One install's identity. The default install has no name.
#[derive(Debug, Clone, PartialEq, Eq, Default, serde::Serialize, serde::Deserialize)]
#[serde(transparent)]
pub struct Instance {
    name: Option<String>,
}

impl Instance {
    /// The install a machine holds when nobody named one.
    pub fn default_install() -> Self {
        Self { name: None }
    }

    /// A named install. The name becomes part of resource names, so it is
    /// refused unless it is 1 to [`MAX_INSTANCE_NAME`] lowercase letters
    /// and digits starting with a letter.
    pub fn named(name: &str) -> Result<Self, String> {
        let ok = !name.is_empty()
            && name.len() <= MAX_INSTANCE_NAME
            && name.starts_with(|c: char| c.is_ascii_lowercase())
            && name.chars().all(|c| c.is_ascii_lowercase() || c.is_ascii_digit())
            // The default install's label value.
            && name != "default";
        if !ok {
            return Err(format!(
                "'{name}' cannot name an install: it goes into resource names, so it must be \
                 1 to {MAX_INSTANCE_NAME} lowercase letters and digits, starting with a letter, \
                 and not `default`"
            ));
        }
        Ok(Self { name: Some(name.to_string()) })
    }

    /// The install this process acts on: the one [`INSTANCE_ENV`] names,
    /// the default install when it is unset or blank.
    pub fn from_env() -> Result<Self, String> {
        match std::env::var(INSTANCE_ENV).ok().filter(|v| !v.trim().is_empty()) {
            Some(name) => Self::named(name.trim()),
            None => Ok(Self::default_install()),
        }
    }

    /// Where this install keeps its files on this machine: the root of
    /// weft's files for the default install, `installs/<name>` under it
    /// for a named one.
    pub fn dir(&self) -> std::path::PathBuf {
        match &self.name {
            None => data_dir(),
            Some(n) => data_dir().join("installs").join(n),
        }
    }

    /// The install's name, `None` for the default install.
    pub fn name(&self) -> Option<&str> {
        self.name.as_deref()
    }

    /// What every resource this install creates is named after: `weft`
    /// for the default install, `weft-<name>` for a named one. A named
    /// install's prefix never starts another install's, because the
    /// default one stops at `weft` and a name holds no `-`.
    pub fn resource_prefix(&self) -> String {
        match &self.name {
            None => "weft".to_string(),
            Some(n) => format!("weft-{n}"),
        }
    }

    /// The value of the `weft-install` label every resource this install
    /// creates carries (a container label, a cloud resource label), which
    /// is how a cleanup finds exactly this install's resources.
    pub fn label_value(&self) -> &str {
        self.name.as_deref().unwrap_or("default")
    }
}

/// The root of weft's files on this machine.
// SYNC: data_dir <-> setup.sh (~/.local/share/weft), scripts/lib/weft-cleanup.sh
//       (weft_state_dir)
pub fn data_dir() -> std::path::PathBuf {
    let home = std::env::var_os("HOME").map(std::path::PathBuf::from).unwrap_or_default();
    home.join(".local/share/weft")
}

/// The environment variable naming the install a command acts on (unset:
/// the default install).
// SYNC: INSTANCE_ENV <-> crates/weft-cli/src/commands/daemon.rs (Install::from_env),
//       crates/weft-e2e/src/cell.rs (read by Instance::from_env)
pub const INSTANCE_ENV: &str = "WEFT_INSTANCE";

/// The label key naming the install a resource belongs to.
// SYNC: INSTALL_LABEL <-> crates/weft-platform-local (docker labels),
//       crates/weft-platform-gcp (resource labels),
//       scripts/lib/weft-cleanup.sh (weft_install_label; setup.sh --purge
//       and scripts/scrub-old-install.sh read it from there)
pub const INSTALL_LABEL: &str = "weft-install";

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_default_and_a_named_install_name_their_resources_apart() {
        let d = Instance::default_install();
        let c = Instance::named("cell7").unwrap();
        assert_eq!(d.resource_prefix(), "weft");
        assert_eq!(c.resource_prefix(), "weft-cell7");
        assert_eq!(d.label_value(), "default");
        assert_eq!(c.label_value(), "cell7");
    }

    #[test]
    fn a_name_that_cannot_live_in_a_resource_name_is_refused() {
        for bad in ["Cell", "7cell", "cell-a", "abcdefghijklm", "cell_a", "", "default"] {
            assert!(Instance::named(bad).is_err(), "{bad}");
        }
    }

    #[test]
    fn an_instance_is_its_name_on_the_wire() {
        assert_eq!(serde_json::to_value(Instance::default_install()).unwrap(), serde_json::Value::Null);
        assert_eq!(serde_json::to_value(Instance::named("c1").unwrap()).unwrap(), serde_json::json!("c1"));
    }
}
