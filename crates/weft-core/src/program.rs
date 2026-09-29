//! What a program may ask of its own project, beyond its own run: start,
//! stop or read an infra copy, activate or take down triggers, read and
//! change what a member provides, list or forget a member's connections,
//! list every member weft holds anything for, read what calls cost, clean
//! runs, and revoke member tokens. The ctx builders (`ctx.infra(..)`,
//! `ctx.trigger(..)`, `ctx.triggers()`, `ctx.values()`,
//! `ctx.connections()`, `ctx.members()`, `ctx.costs()`, `ctx.runs()`,
//! `ctx.tokens()`) each build one [`ProgramCall`]; the runtime carries it
//! to the dispatcher as one durable task and hands back its answer.
//!
//! Every call is scoped to the calling run's project: the broker takes the
//! project from the run, never from the call. Inside the project any member
//! may be named from any run, because the program is the author's own code;
//! member limits are enforced at the outside doors (a member token, the
//! `Weft-Member` header), not here.
//!
//! A call that takes something down carries a [`DeactivateSpec`] (what
//! happens to parked and running work) and a [`StopSelf`]: the calling run
//! is never among the runs a take-down reaches, and `StopSelf::Include`
//! stops it too, after the take-down is queued.

use serde::{Deserialize, Serialize};

use crate::activation::ActivationScope;
use crate::member::MemberId;
use crate::running_policy::{DeactivateSpec, RunningPolicy};
use crate::tag::StopSelf;

/// One call a program makes on its project.
// SYNC: ProgramCall <-> crates/weft-dispatcher/src/task_kinds/program_call.rs (the executor's match)
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "call", rename_all = "snake_case")]
pub enum ProgramCall {
    /// Bring up a copy of an infra node: the shared one, or a member's
    /// copy of a node that exists once per member. Answers once the start
    /// is queued; `ctx.infra(..).start()` then waits on
    /// [`ProgramCall::InfraStatus`] until it runs.
    InfraStart { node: String, member: Option<MemberId> },
    /// Scale a copy down, keeping its disk.
    InfraStop { node: String, member: Option<MemberId>, spec: DeactivateSpec },
    /// Delete a copy and its disks, keeping the ones its node lists in
    /// `keepOnTerminate` unless `disks` says the owner is going for good.
    InfraTerminate { node: String, member: Option<MemberId>, spec: DeactivateSpec, disks: crate::infra::TerminateDisks },
    /// One copy's state, or none when it was never started (or was
    /// terminated).
    InfraStatus { node: String, member: Option<MemberId> },
    /// Every copy of a node that exists: the shared one and each
    /// member's.
    InfraCopies { node: String },
    /// Activate triggers: the ones `scope` names, or every trigger of its
    /// owner.
    TriggerActivate { scope: ActivationScope },
    /// Take triggers down.
    TriggerDeactivate { scope: ActivationScope, spec: DeactivateSpec },
    /// A member's connections.
    ConnectionsList { member: MemberId },
    /// Forget every connection of a member (the values naming them go
    /// with them).
    ConnectionsForget { member: MemberId },
    /// What a member provides for the program's `@member_filled` fields.
    ValuesGet { member: MemberId },
    /// Change what a member provides: `set` given, `clear` forgotten, all
    /// or none, re-arming the member's live triggers that read them.
    ValuesChange {
        member: MemberId,
        #[serde(default)]
        set: Vec<crate::run_spec::MemberValueInput>,
        #[serde(default)]
        clear: Vec<crate::run_spec::MemberFieldRef>,
    },
    /// Forget everything a member provides (what wiping a member does),
    /// re-arming their live triggers that read any of it.
    ValuesForget { member: MemberId },
    /// Every member weft holds anything for in this project, with what it
    /// holds ([`MemberHoldings`]).
    MembersList,
    /// What calls cost, filtered.
    CostsList { filter: CostFilter },
    /// Delete runs matching `filter`. Runs still going follow `running`:
    /// `cancel` stops them (their rows go with the next clean), `wait`
    /// leaves them running.
    RunsClean { filter: RunFilter, running: RunningPolicy },
    /// Revoke member tokens: every token of `member`, or the one `id`.
    TokensRevoke { member: MemberId, id: Option<uuid::Uuid> },
}

impl ProgramCall {
    /// Whether the call can take the calling run down with it, so it
    /// carries a [`StopSelf`] that matters.
    pub fn takes_down(&self) -> bool {
        matches!(
            self,
            ProgramCall::InfraStop { .. }
                | ProgramCall::InfraTerminate { .. }
                | ProgramCall::TriggerDeactivate { .. }
                | ProgramCall::RunsClean { .. }
        )
    }

    /// The name the call is journaled under (`ctx.run`), so a replayed
    /// run gets the recorded answer instead of asking again.
    pub fn journal_name(&self) -> &'static str {
        match self {
            ProgramCall::InfraStart { .. } => "weft.infra.start",
            ProgramCall::InfraStop { .. } => "weft.infra.stop",
            ProgramCall::InfraTerminate { .. } => "weft.infra.terminate",
            ProgramCall::InfraStatus { .. } => "weft.infra.status",
            ProgramCall::InfraCopies { .. } => "weft.infra.copies",
            ProgramCall::TriggerActivate { .. } => "weft.triggers.activate",
            ProgramCall::TriggerDeactivate { .. } => "weft.triggers.deactivate",
            ProgramCall::ConnectionsList { .. } => "weft.connections.list",
            ProgramCall::ConnectionsForget { .. } => "weft.connections.forget",
            ProgramCall::ValuesGet { .. } => "weft.values.get",
            ProgramCall::ValuesChange { .. } => "weft.values.change",
            ProgramCall::ValuesForget { .. } => "weft.values.forget",
            ProgramCall::MembersList => "weft.members.list",
            ProgramCall::CostsList { .. } => "weft.costs.list",
            ProgramCall::RunsClean { .. } => "weft.runs.clean",
            ProgramCall::TokensRevoke { .. } => "weft.tokens.revoke",
        }
    }
}

/// The durable task that carries a [`ProgramCall`]: who asked, what
/// happens to the asker when the call takes it down, and the call.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ProgramCallPayload {
    /// The calling run (its execution).
    pub by: uuid::Uuid,
    pub stop_self: StopSelf,
    pub call: ProgramCall,
}

/// What a call answered, and whether the asker is being stopped by it
/// (`StopSelf::Include` on a take-down): that run then waits for its own
/// cancel and never returns from the call.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ProgramCallOutcome {
    pub value: serde_json::Value,
    #[serde(default)]
    pub stops_asker: bool,
}

/// One copy of an infra node, as `ctx.infra(..).status()` and
/// `.copies()` answer it.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct InfraCopy {
    /// The node, spelled the way the program writes it.
    pub node: String,
    /// Whose copy: `None` for the shared one.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub member: Option<MemberId>,
    /// `Provisioning` also while a start is queued but not yet picked
    /// up, `Stopping` while a stop is.
    pub status: crate::infra::InfraNodeStatus,
    /// Why it failed, when it did.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub failure: Option<String>,
}

impl InfraCopy {
    /// Whether the copy answers now.
    pub fn is_running(&self) -> bool {
        self.status == crate::infra::InfraNodeStatus::Running
    }
}

/// Fires of one member's trigger that wait, parked, because the member
/// has not given (or gave an invalid) value the fire needs. They are not
/// retried on a timer: the member's next change of values routes them
/// again, and so does activating the trigger.
// SYNC: WaitingFires <-> packages/weft-graph/src/protocol.ts ActivationEntry.waiting
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct WaitingFires {
    pub fires: u32,
    /// Why, naming the field (`member 'ada' at 'answer': 'key' is not
    /// filled`): the refusal of the fire parked last.
    pub reason: String,
}

/// What `ProgramCall::InfraStart` answers: a start queued, one already
/// under way or done, or none yet for a passing reason (a build in
/// progress, a trigger of the copy mid-activation), which
/// `ctx.infra(..).start()` asks again at its next look.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "answer", rename_all = "snake_case")]
pub enum InfraStartAnswer {
    Started,
    AlreadyRunning,
    AlreadyStarting,
    Waiting { reason: String },
}

impl InfraStartAnswer {
    /// Whether a start is under way: every answer but `Waiting`.
    pub fn queued(&self) -> bool {
        !matches!(self, InfraStartAnswer::Waiting { .. })
    }
}

/// What `ProgramCall::ConnectionsForget` answers: how many connections
/// went.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct ConnectionsForgotten {
    pub forgotten: u64,
}

/// What `ProgramCall::TokensRevoke` answers: how many tokens went.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct TokensRevoked {
    pub revoked: u64,
}

/// One member's activation of a trigger that exists once per member.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MemberTrigger {
    /// The trigger, spelled the way the program writes it.
    pub trigger: String,
    pub mode: crate::activation::ActivationMode,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub waiting: Option<WaitingFires>,
}

/// Everything weft holds for one member of the project, as
/// `ctx.members().list()` answers it: counts and states, never a value or
/// a secret.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MemberHoldings {
    pub member: MemberId,
    /// Values the member gave for `@member_filled` fields.
    pub values: u32,
    /// Connections the member made in this project.
    pub connections: u32,
    /// Member tokens acting as them that still work.
    pub tokens: u32,
    /// Their copies of `@per_member` infra nodes.
    pub copies: Vec<InfraCopy>,
    /// Their activations of per-member triggers.
    pub triggers: Vec<MemberTrigger>,
}

impl MemberHoldings {
    fn empty(member: MemberId) -> Self {
        Self { member, values: 0, connections: 0, tokens: 0, copies: Vec::new(), triggers: Vec::new() }
    }

    /// One entry per member that appears in any of the sources, sorted by
    /// member. `values`, `connections` and `tokens` are counts per member;
    /// a copy without a member (the shared one) belongs to nobody here.
    pub fn merge(
        values: Vec<(MemberId, u32)>,
        connections: Vec<(MemberId, u32)>,
        tokens: Vec<(MemberId, u32)>,
        copies: Vec<InfraCopy>,
        triggers: Vec<(MemberId, MemberTrigger)>,
    ) -> Vec<MemberHoldings> {
        let mut by_member = std::collections::BTreeMap::<MemberId, MemberHoldings>::new();
        fn at(by_member: &mut std::collections::BTreeMap<MemberId, MemberHoldings>, member: MemberId) -> &mut MemberHoldings {
            by_member.entry(member.clone()).or_insert_with(|| MemberHoldings::empty(member))
        }
        for (member, n) in values {
            at(&mut by_member, member).values += n;
        }
        for (member, n) in connections {
            at(&mut by_member, member).connections += n;
        }
        for (member, n) in tokens {
            at(&mut by_member, member).tokens += n;
        }
        for copy in copies {
            if let Some(member) = copy.member.clone() {
                at(&mut by_member, member).copies.push(copy);
            }
        }
        for (member, trigger) in triggers {
            at(&mut by_member, member).triggers.push(trigger);
        }
        by_member.into_values().collect()
    }
}

/// Which runs a clean (or a listing) reaches. Every filter narrows; none
/// set is every run of the project.
// SYNC: RunFilter <-> crates/weft-cli/src/commands/executions.rs (clean's flags)
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct RunFilter {
    /// Runs for this member.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub member: Option<MemberId>,
    /// Runs that ended this way: `completed`, `failed`, `cancelled`, or
    /// `running` for the ones still going.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub status: Option<String>,
    /// Runs started by this node (the trigger that fired, or the node a
    /// run was started from), spelled the way the program writes it.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub node: Option<String>,
    /// Runs carrying this tag.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub tag: Option<String>,
    /// Runs started at least this many seconds ago.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub older_than_secs: Option<u64>,
}

/// What a clean did.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct CleanOutcome {
    /// Runs deleted.
    pub deleted: u64,
    /// Runs still going that were cancelled (`running: cancel`); their
    /// rows go with the next clean.
    pub cancelled: u64,
    /// Runs still going that were left alone (`running: wait`).
    pub left_running: u64,
}

/// Whose money a call spent, as a cost filter names it.
// SYNC: PaidBy <-> crates/weft-core/src/access/mod.rs CredentialOwner
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PaidBy {
    /// The runtime's own credential (its credit).
    Platform,
    /// The author's own connection.
    Author,
    /// A member's own connection (any member, or the one `member`
    /// narrows to).
    Member,
}

impl PaidBy {
    /// Whether a call spent on `owner`'s credential is one of these.
    pub fn covers(&self, owner: &crate::CredentialOwner) -> bool {
        matches!(
            (self, owner),
            (PaidBy::Platform, crate::CredentialOwner::Platform)
                | (PaidBy::Author, crate::CredentialOwner::Author)
                | (PaidBy::Member, crate::CredentialOwner::Member(_))
        )
    }
}

/// Which cost records `ctx.costs()` reads. Every filter narrows.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct CostFilter {
    /// Costs of runs for this member.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub member: Option<MemberId>,
    /// Costs booked by this node, spelled the way the program writes it.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub node: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub service: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub paid_by: Option<PaidBy>,
    /// Costs of one run (`ctx.execution_id` for the calling run's own).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub run: Option<uuid::Uuid>,
    /// Costs booked at or after this unix second.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub since_unix: Option<u64>,
}

/// One cost record, as `ctx.costs().list()` answers it. Weft reports the
/// price it measured; what anybody is billed is the program's business.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct CostRecord {
    pub run: uuid::Uuid,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub member: Option<MemberId>,
    pub node: String,
    pub service: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub model: Option<String>,
    /// `None` when the service's price could not be measured.
    pub amount_usd: Option<f64>,
    pub paid_by: crate::CredentialOwner,
    pub at_unix: u64,
}

/// A member token a program minted. The value is shown here once; the
/// server keeps only its hash.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MintedMemberToken {
    /// Revoke it by this id.
    pub id: uuid::Uuid,
    /// The token itself, for the member's browser or extension to
    /// present in the `Weft-Member-Token` header (or as a signal token).
    pub token: String,
    pub expires_at_unix: u64,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::running_policy::DeactivationMode;

    fn spec() -> DeactivateSpec {
        DeactivateSpec {
            mode: DeactivationMode::Wipe,
            grace_minutes: 0,
            running_policy: RunningPolicy::Cancel,
            drain_timeout_secs: None,
        }
    }

    #[test]
    fn paid_by_covers_its_own_kind_of_credential() {
        use crate::CredentialOwner;
        let ada = CredentialOwner::Member(MemberId::new("ada").unwrap());
        assert!(PaidBy::Member.covers(&ada));
        assert!(!PaidBy::Member.covers(&CredentialOwner::Author));
        assert!(PaidBy::Author.covers(&CredentialOwner::Author));
        assert!(PaidBy::Platform.covers(&CredentialOwner::Platform));
        assert!(!PaidBy::Platform.covers(&ada));
    }

    #[test]
    fn a_call_round_trips_under_its_tag() {
        let call = ProgramCall::InfraStop {
            node: "bridge".into(),
            member: Some(MemberId::new("ada").unwrap()),
            spec: spec(),
        };
        let wire = serde_json::to_value(&call).unwrap();
        assert_eq!(wire["call"], "infra_stop");
        assert_eq!(wire["member"], "ada");
        let back: ProgramCall = serde_json::from_value(wire).unwrap();
        assert_eq!(back, call);
    }

    #[test]
    fn the_members_listing_round_trips_under_its_tag() {
        let wire = serde_json::to_value(ProgramCall::MembersList).unwrap();
        assert_eq!(wire, serde_json::json!({ "call": "members_list" }));
        let back: ProgramCall = serde_json::from_value(wire).unwrap();
        assert_eq!(back, ProgramCall::MembersList);
        assert!(!ProgramCall::MembersList.takes_down());
    }

    fn member(id: &str) -> MemberId {
        MemberId::new(id).unwrap()
    }

    #[test]
    fn holdings_merge_one_entry_per_member_from_every_source() {
        let copy = |who: Option<&str>, status: crate::infra::InfraNodeStatus| InfraCopy {
            node: "bridge".into(),
            member: who.map(member),
            status,
            failure: None,
        };
        let waiting = MemberTrigger {
            trigger: "inbox".into(),
            mode: crate::activation::ActivationMode::Active,
            waiting: Some(WaitingFires { fires: 2, reason: "member 'cyd' at 'answer': 'key' is not filled".into() }),
        };
        let merged = MemberHoldings::merge(
            vec![(member("ada"), 3)],
            vec![(member("bob"), 1)],
            vec![(member("ada"), 2)],
            // The shared copy belongs to nobody in this listing.
            vec![copy(None, crate::infra::InfraNodeStatus::Running), copy(Some("bob"), crate::infra::InfraNodeStatus::Stopped)],
            vec![(member("cyd"), waiting.clone())],
        );
        let members: Vec<&str> = merged.iter().map(|m| m.member.as_str()).collect();
        assert_eq!(members, ["ada", "bob", "cyd"]);
        assert_eq!((merged[0].values, merged[0].connections, merged[0].tokens), (3, 0, 2));
        assert!(merged[0].copies.is_empty() && merged[0].triggers.is_empty());
        assert_eq!(merged[1].connections, 1);
        assert_eq!(merged[1].copies, vec![copy(Some("bob"), crate::infra::InfraNodeStatus::Stopped)]);
        assert_eq!(merged[2].triggers, vec![waiting]);
        assert_eq!((merged[2].values, merged[2].connections, merged[2].tokens), (0, 0, 0));
    }

    #[test]
    fn holdings_of_nobody_merge_to_nothing() {
        assert!(MemberHoldings::merge(Vec::new(), Vec::new(), Vec::new(), Vec::new(), Vec::new()).is_empty());
    }

    #[test]
    fn only_take_downs_can_stop_the_asker() {
        assert!(ProgramCall::InfraTerminate {
            node: "x".into(),
            member: None,
            spec: spec(),
            disks: crate::infra::TerminateDisks::KeepListed,
        }.takes_down());
        assert!(!ProgramCall::InfraStart { node: "x".into(), member: None }.takes_down());
        assert!(!ProgramCall::ConnectionsList { member: MemberId::new("a").unwrap() }.takes_down());
    }

    #[test]
    fn an_empty_filter_says_nothing_on_the_wire() {
        assert_eq!(serde_json::to_value(RunFilter::default()).unwrap(), serde_json::json!({}));
        assert_eq!(serde_json::to_value(CostFilter::default()).unwrap(), serde_json::json!({}));
    }
}
