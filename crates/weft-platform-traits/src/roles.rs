//! Weft's own roles, where each one runs, and how to reach it.
//!
//! One binary (`weft-runtime`) carries every role. A setting per role
//! says where it runs: inside the one process of a local install (the
//! machine), as a service of its own that scales to zero, or, for the
//! holder, as a pool of copies weft sizes itself. Locally every role is
//! on the machine. The code of a role never asks where it runs; it asks
//! [`RoleAddresses`] for another role's address and calls it, and the
//! same call works either way.

use serde::{Deserialize, Serialize};

// SYNC: CoreRole <-> deploy/terraform/gcp/serverless.tf (local.serverless_roles)
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CoreRole {
    /// Routing, lifecycle, the management API and the public door.
    Dispatcher,
    /// The scoped door to Postgres for everything that is not the
    /// dispatcher.
    Broker,
    /// Processes signals' fires, and wakes the ones that wake at times.
    Listener,
    /// Holds the connections some signals keep open to the outside
    /// between fires (a socket, a stream, a subscription). The listener's
    /// own code, run where something stays up; nothing calls it.
    Holder,
    /// Owns projects' infrastructure.
    Supervisor,
}

impl CoreRole {
    pub const ALL: [CoreRole; 5] = [Self::Dispatcher, Self::Broker, Self::Listener, Self::Holder, Self::Supervisor];

    pub fn as_str(self) -> &'static str {
        match self {
            Self::Dispatcher => "dispatcher",
            Self::Broker => "broker",
            Self::Listener => "listener",
            Self::Holder => "holder",
            Self::Supervisor => "supervisor",
        }
    }

    pub fn parse(raw: &str) -> Result<Self, String> {
        Self::ALL
            .into_iter()
            .find(|r| r.as_str() == raw)
            .ok_or_else(|| format!("'{raw}' is not a role; the roles are dispatcher, broker, listener, holder, supervisor"))
    }

    /// The path prefix the role's internal endpoints answer under on the
    /// machine's process, which carries several roles. The dispatcher has
    /// none: on the machine it serves only its public API, and work
    /// reaches it through the database's notifications, never an internal
    /// call. The holder has none anywhere: nothing calls it.
    pub fn internal_prefix(self) -> Option<&'static str> {
        match self {
            Self::Dispatcher | Self::Holder => None,
            Self::Broker => Some("/broker"),
            Self::Listener => Some("/listener"),
            Self::Supervisor => Some("/supervisor"),
        }
    }
}

impl std::fmt::Display for CoreRole {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

/// Where one role runs.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Placement {
    /// Inside the one process of a local install.
    #[default]
    Machine,
    /// A service of its own that scales to zero when idle, called at its
    /// own address.
    Serverless,
    /// Copies that stay up and call out, never called, as many as weft
    /// asks for and none when nothing needs them (a Cloud Run worker
    /// pool): only the holder runs there.
    Pool,
}

impl Placement {
    /// Where a process running a role placed here stands.
    pub fn vantage(self) -> Vantage {
        match self {
            Self::Machine => Vantage::Machine,
            Self::Serverless | Self::Pool => Vantage::Private,
        }
    }
}

/// Where a caller stands, which decides the address it reaches a role at:
/// the machine's own loopback is only the machine's.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Vantage {
    /// The local install's own process.
    Machine,
    /// Anywhere else on the install's private network: a role in a
    /// process of its own, a worker, an infra container.
    Private,
    /// Outside the private network (a cloud's queue delivering a wake):
    /// a role's own public address, or a local install's public port.
    Public,
}

impl Vantage {
    pub const ALL: [Vantage; 3] = [Self::Machine, Self::Private, Self::Public];
}

/// Every role's placement. Absent roles are on the machine.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RolePlacement {
    #[serde(default)]
    pub dispatcher: Placement,
    #[serde(default)]
    pub broker: Placement,
    #[serde(default)]
    pub listener: Placement,
    #[serde(default)]
    pub holder: Placement,
    #[serde(default)]
    pub supervisor: Placement,
}

impl RolePlacement {
    pub fn of(&self, role: CoreRole) -> Placement {
        match role {
            CoreRole::Dispatcher => self.dispatcher,
            CoreRole::Broker => self.broker,
            CoreRole::Listener => self.listener,
            CoreRole::Holder => self.holder,
            CoreRole::Supervisor => self.supervisor,
        }
    }

    pub fn set(&mut self, role: CoreRole, placement: Placement) {
        match role {
            CoreRole::Dispatcher => self.dispatcher = placement,
            CoreRole::Broker => self.broker = placement,
            CoreRole::Listener => self.listener = placement,
            CoreRole::Holder => self.holder = placement,
            CoreRole::Supervisor => self.supervisor = placement,
        }
    }

    /// The roles placed on the machine.
    pub fn on_machine(&self) -> Vec<CoreRole> {
        CoreRole::ALL.into_iter().filter(|r| self.of(*r) == Placement::Machine).collect()
    }
}

/// The internal base URL of every role a caller can reach, as a caller at
/// one [`Vantage`] reaches it (`InstallConfig::role_addresses`). A role on
/// the machine answers at the machine's internal address under its
/// prefix; a role in a service of its own at that service's address. The
/// holder has none: nothing calls it.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RoleAddresses {
    /// `None` while the dispatcher is on the machine: it has no internal
    /// routes there ([`CoreRole::internal_prefix`]). In a process of its
    /// own it has one, its tick.
    pub dispatcher: Option<String>,
    pub broker: String,
    pub listener: String,
    pub supervisor: String,
}

impl RoleAddresses {
    /// The role's internal address; refused for the dispatcher on the
    /// machine and for the holder, which have none.
    pub fn of(&self, role: CoreRole) -> anyhow::Result<&str> {
        match role {
            CoreRole::Dispatcher => self
                .dispatcher
                .as_deref()
                .ok_or_else(|| anyhow::anyhow!("the dispatcher is on the machine, where it has no internal address to call")),
            CoreRole::Holder => anyhow::bail!("the holder has no address: it calls out and nothing calls it"),
            CoreRole::Broker => Ok(&self.broker),
            CoreRole::Listener => Ok(&self.listener),
            CoreRole::Supervisor => Ok(&self.supervisor),
        }
    }
}

/// The path a role's tick answers at, under its internal address.
// SYNC: TICK_PATH <-> deploy/terraform/gcp/builds.tf (the push endpoint)
pub const TICK_PATH: &str = "/_weft/tick";

/// Where the internal routes are reached on a local install's public
/// port, for a caller outside the private network.
pub const INTERNAL_DOOR: &str = "/_internal";

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn placement_defaults_to_the_machine_and_round_trips() {
        let p: RolePlacement = serde_json::from_value(serde_json::json!({ "listener": "serverless" })).unwrap();
        assert_eq!(p.of(CoreRole::Listener), Placement::Serverless);
        assert_eq!(p.of(CoreRole::Broker), Placement::Machine);
        assert_eq!(p.on_machine(), vec![CoreRole::Dispatcher, CoreRole::Broker, CoreRole::Holder, CoreRole::Supervisor]);
        let v = serde_json::to_value(&p).unwrap();
        assert_eq!(serde_json::from_value::<RolePlacement>(v).unwrap(), p);
        assert!(serde_json::from_value::<RolePlacement>(serde_json::json!({ "relay": "machine" })).is_err());
    }

    #[test]
    fn roles_parse_by_name_and_sit_under_their_prefix() {
        for r in CoreRole::ALL {
            assert_eq!(CoreRole::parse(r.as_str()).unwrap(), r);
        }
        assert!(CoreRole::parse("gateway").is_err());
        assert_eq!(CoreRole::Broker.internal_prefix(), Some("/broker"));
        assert_eq!(CoreRole::Dispatcher.internal_prefix(), None, "nothing calls the machine's dispatcher internally");
        assert_eq!(CoreRole::Holder.internal_prefix(), None, "nothing calls the holder");
    }
}
