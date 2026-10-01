//! Weft's own roles, where each one runs, and how to reach it.
//!
//! One binary (`weft-runtime`) carries every role. A setting per role
//! says where it runs: inside the always-on process on the install's
//! machine, as a service of its own that scales to zero, or alone on a
//! machine of its own that stays up. Locally every role is on the machine
//! (the one local process). The code of a role
//! never asks where it runs; it asks [`RoleAddresses`] for another role's
//! address and calls it, and the same call works either way.

use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CoreRole {
    /// Routing, lifecycle, the management API and the public door.
    Dispatcher,
    /// The scoped door to Postgres for everything that is not the
    /// dispatcher.
    Broker,
    /// Holds signals and processes their fires.
    Listener,
    /// Owns projects' infrastructure.
    Supervisor,
}

impl CoreRole {
    pub const ALL: [CoreRole; 4] = [Self::Dispatcher, Self::Broker, Self::Listener, Self::Supervisor];

    pub fn as_str(self) -> &'static str {
        match self {
            Self::Dispatcher => "dispatcher",
            Self::Broker => "broker",
            Self::Listener => "listener",
            Self::Supervisor => "supervisor",
        }
    }

    pub fn parse(raw: &str) -> Result<Self, String> {
        Self::ALL
            .into_iter()
            .find(|r| r.as_str() == raw)
            .ok_or_else(|| format!("'{raw}' is not a role; the roles are dispatcher, broker, listener, supervisor"))
    }

    /// The path prefix the role's internal endpoints answer under on the
    /// machine's process, which carries several roles. The dispatcher has
    /// none: on the machine it serves only its public API, and work
    /// reaches it through the database's notifications, never an internal
    /// call.
    pub fn internal_prefix(self) -> Option<&'static str> {
        match self {
            Self::Dispatcher => None,
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
    /// Inside the always-on process on the install's machine: no cold
    /// start, costs nothing beyond the machine.
    #[default]
    Machine,
    /// A service of its own that scales to zero when idle.
    Serverless,
    /// Alone on a machine of its own that stays up: for a role that holds
    /// connections between calls (the listener's sockets and streams) and
    /// must not share the install machine's load.
    OwnMachine,
}

impl Placement {
    /// Where a process running a role placed here stands.
    pub fn vantage(self) -> Vantage {
        match self {
            Self::Machine => Vantage::Machine,
            Self::Serverless | Self::OwnMachine => Vantage::Private,
        }
    }
}

/// Where a caller stands, which decides the address it reaches a role at:
/// the machine's own loopback is only the machine's.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Vantage {
    /// A process on the install's machine.
    Machine,
    /// Anywhere else on the install's private network: a role in a
    /// process of its own, a worker, an infra container.
    Private,
    /// Outside the private network (a cloud's queue delivering a wake),
    /// through the machine's public port.
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
    pub supervisor: Placement,
}

impl RolePlacement {
    pub fn of(&self, role: CoreRole) -> Placement {
        match role {
            CoreRole::Dispatcher => self.dispatcher,
            CoreRole::Broker => self.broker,
            CoreRole::Listener => self.listener,
            CoreRole::Supervisor => self.supervisor,
        }
    }

    pub fn set(&mut self, role: CoreRole, placement: Placement) {
        match role {
            CoreRole::Dispatcher => self.dispatcher = placement,
            CoreRole::Broker => self.broker = placement,
            CoreRole::Listener => self.listener = placement,
            CoreRole::Supervisor => self.supervisor = placement,
        }
    }

    /// The roles placed on the machine.
    pub fn on_machine(&self) -> Vec<CoreRole> {
        CoreRole::ALL.into_iter().filter(|r| self.of(*r) == Placement::Machine).collect()
    }
}

/// The internal base URL of every role, as a caller at one [`Vantage`]
/// reaches it (`InstallConfig::role_addresses`). A role on the machine
/// answers at the machine's internal address under its prefix; a role in
/// a process of its own at that process's address.
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
    /// machine, which has none.
    pub fn of(&self, role: CoreRole) -> anyhow::Result<&str> {
        match role {
            CoreRole::Dispatcher => self
                .dispatcher
                .as_deref()
                .ok_or_else(|| anyhow::anyhow!("the dispatcher is on the machine, where it has no internal address to call")),
            CoreRole::Broker => Ok(&self.broker),
            CoreRole::Listener => Ok(&self.listener),
            CoreRole::Supervisor => Ok(&self.supervisor),
        }
    }
}

/// Tell a role that work waits for it.
///
/// A role on the machine hears the database's own notification of the
/// write that queued the work, so a kick is nothing for it. A role that
/// scales to zero hears nothing while it is at zero: kicking it calls its
/// tick, which drains its loops. Whoever queues work for another role
/// kicks it right after the write commits.
pub trait Kick: Send + Sync {
    fn kick(&self, role: CoreRole);
}

/// The path a role's tick answers at, under its internal address.
// SYNC: TICK_PATH <-> crates/weft-runtime/src/server.rs (the route)
pub const TICK_PATH: &str = "/_weft/tick";

/// Where the internal routes are reached on the machine's public port,
/// for a caller outside the private network.
pub const INTERNAL_DOOR: &str = "/_internal";

#[cfg(any(test, feature = "test-helpers"))]
pub mod fake {
    use super::*;

    /// Records every kick.
    #[derive(Default)]
    pub struct FakeKick {
        kicked: parking_lot::Mutex<Vec<CoreRole>>,
    }

    impl FakeKick {
        pub fn kicked(&self) -> Vec<CoreRole> {
            self.kicked.lock().clone()
        }
    }

    impl Kick for FakeKick {
        fn kick(&self, role: CoreRole) {
            self.kicked.lock().push(role);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn placement_defaults_to_the_machine_and_round_trips() {
        let p: RolePlacement = serde_json::from_value(serde_json::json!({ "listener": "serverless" })).unwrap();
        assert_eq!(p.of(CoreRole::Listener), Placement::Serverless);
        assert_eq!(p.of(CoreRole::Broker), Placement::Machine);
        assert_eq!(p.on_machine(), vec![CoreRole::Dispatcher, CoreRole::Broker, CoreRole::Supervisor]);
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
    }
}
