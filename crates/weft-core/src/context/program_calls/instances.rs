//! `ctx.connections()`, `ctx.instances()`, `ctx.values()` and
//! `ctx.tokens()`: a project's instances, and the owner's tokens.

use crate::instance_door::ValuesChanged;
use crate::program::instances::InstanceCall;
use crate::program::settings::SettingsCall;
use crate::program::{ConnectionsForgotten, InstanceHoldings, MintedInstanceToken, TokensRevoked};
use crate::signal_token::TokenSummary;
use serde_json::Value;

use crate::error::{WeftError, WeftResult};
use crate::program::ProgramCall;
use crate::tag::StopSelf;

use super::{required_instance, ProjectCalls};

impl<'a> ProjectCalls<'a> {
    /// What an instance provides for the program's `@instance_filled`
    /// fields: `ctx.values().instance(id)`.
    pub fn values(self) -> ValueCalls<'a> {
        ValueCalls { at: self }
    }

    /// An instance's connections: `ctx.connections().instance(id)`.
    pub fn connections(self) -> ConnectionCalls<'a> {
        ConnectionCalls { at: self }
    }

    /// The project's instances: `ctx.instances().list()`.
    pub fn instances(self) -> InstanceCalls<'a> {
        InstanceCalls { at: self }
    }

    /// Tokens: `mint_for_instance` hands a browser or extension a
    /// credential that acts inside one instance of this project; `list`
    /// and `revoke` reach every signal token of the project's owner.
    pub fn tokens(self) -> TokenCalls<'a> {
        TokenCalls { at: self }
    }
}

/// See [`RunContext::connections`].
pub struct ConnectionCalls<'a> {
    at: ProjectCalls<'a>,
}

impl<'a> ConnectionCalls<'a> {
    /// One instance's connections.
    pub fn instance(self, id: impl Into<String>) -> InstanceConnections<'a> {
        InstanceConnections { at: self.at, instance: id.into() }
    }
}

/// One instance's connections.
pub struct InstanceConnections<'a> {
    at: ProjectCalls<'a>,
    instance: String,
}

impl InstanceConnections<'_> {
    /// Every connection the instance has in this project.
    pub async fn list(self) -> WeftResult<Vec<crate::access::wire::GrantSummary>> {
        let call = ProgramCall::Instances(InstanceCall::ConnectionsList { instance: required_instance(&self.instance)? });
        self.at.call(call, StopSelf::Keep).await
    }

    /// Forget every connection the instance has in this project; the
    /// values naming them go with them. Answers how many went.
    pub async fn forget(self) -> WeftResult<u64> {
        let call = ProgramCall::Instances(InstanceCall::ConnectionsForget { instance: required_instance(&self.instance)? });
        let answer: ConnectionsForgotten = self.at.call(call, StopSelf::Keep).await?;
        Ok(answer.forgotten)
    }
}

/// See [`RunContext::instances`].
pub struct InstanceCalls<'a> {
    at: ProjectCalls<'a>,
}

impl InstanceCalls<'_> {
    /// Every instance weft holds anything for in this project (a value,
    /// a connection, an infra copy, an instance token, a trigger
    /// activation), one entry each with what it holds: counts and states,
    /// never a value or a secret. A trigger's fires waiting on a value the
    /// instance has not given show under that trigger, with the reason.
    pub async fn list(self) -> WeftResult<Vec<InstanceHoldings>> {
        self.at.call(ProgramCall::Instances(InstanceCall::List), StopSelf::Keep).await
    }
}

/// See [`RunContext::values`].
pub struct ValueCalls<'a> {
    at: ProjectCalls<'a>,
}

impl<'a> ValueCalls<'a> {
    /// One instance's values.
    pub fn instance(self, id: impl Into<String>) -> InstanceValueCalls<'a> {
        InstanceValueCalls { at: self.at, instance: id.into(), set: Vec::new(), clear: Vec::new() }
    }
}

/// One instance's values: read them with [`Self::get`], or chain
/// [`Self::set`] and [`Self::clear`] and make the change with
/// [`Self::apply`].
pub struct InstanceValueCalls<'a> {
    at: ProjectCalls<'a>,
    instance: String,
    set: Vec<crate::run_spec::InstanceValueInput>,
    clear: Vec<crate::run_spec::InstanceFieldRef>,
}

impl InstanceValueCalls<'_> {
    /// What the instance gives, by node (named the way the program writes
    /// it) and field. A connection reads as its `{id, identity}` handle.
    pub async fn get(self) -> WeftResult<crate::instance::InstanceValues> {
        let call = ProgramCall::Instances(InstanceCall::ValuesGet { instance: required_instance(&self.instance)? });
        self.at.call(call, StopSelf::Keep).await
    }

    /// Give `value` for `field` of the node `node`, a field the program
    /// writes `@instance_filled`. For a connection field the value is one
    /// of the instance's connections: `json!({ "id": <connection id> })`.
    pub fn set(mut self, node: impl Into<String>, field: impl Into<String>, value: Value) -> Self {
        self.set.push(crate::run_spec::InstanceValueInput { node: node.into(), field: field.into(), value });
        self
    }

    /// Forget everything the instance provides, in one change (what
    /// wiping an instance does); its live triggers reading any of it are set up
    /// again. Answers the triggers set up again.
    pub async fn forget(self) -> WeftResult<Vec<String>> {
        let call = ProgramCall::Instances(InstanceCall::ValuesForget { instance: required_instance(&self.instance)? });
        let answer: ValuesChanged = self.at.call(call, StopSelf::Keep).await?;
        Ok(answer.rearmed)
    }

    /// Forget the instance's value for `field` of `node`: the field then
    /// falls back as if never given.
    pub fn clear(mut self, node: impl Into<String>, field: impl Into<String>) -> Self {
        self.clear.push(crate::run_spec::InstanceFieldRef { node: node.into(), field: field.into() });
        self
    }

    /// Make the change, all or none: each value is held to its field and
    /// to the node's rules first, and a live trigger of the instance that
    /// reads one is set up again with the new values before this returns.
    /// Answers the triggers set up again.
    pub async fn apply(self) -> WeftResult<Vec<String>> {
        let call = ProgramCall::Instances(InstanceCall::ValuesChange { instance: required_instance(&self.instance)?, set: self.set, clear: self.clear });
        let answer: ValuesChanged = self.at.call(call, StopSelf::Keep).await?;
        Ok(answer.rearmed)
    }
}

/// See [`RunContext::tokens`].
pub struct TokenCalls<'a> {
    at: ProjectCalls<'a>,
}

impl<'a> TokenCalls<'a> {
    /// A token that acts inside one instance of this project, `instance`,
    /// for a browser or extension: it starts that instance's runs
    /// (through the project's live routes, in the `Weft-Instance-Token`
    /// header), answers its waits, reads its own copies' displays and
    /// manages its connections, and nothing else. `expires_in` is required (a year at
    /// most). The value is in the answer once and never again; the
    /// runtime keeps only its hash.
    ///
    /// The token's id comes from where the mint is made (the run, the
    /// firing and its program-call number), so a body that mints again
    /// under the same number (one resumed without its state recording
    /// the mint) replaces the token (a new value, since the old one cannot
    /// be read back, and the old value stops working), never leaves a
    /// second beside it.
    pub async fn mint_for_instance(self, instance: impl Into<String>, expires_in: std::time::Duration) -> WeftResult<MintedInstanceToken> {
        let instance = required_instance(&instance.into())?;
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map_err(|e| WeftError::NodeFailed(format!("the clock is before 1970: {e}")))?
            .as_secs();
        crate::signal_token::expiry_at(now, expires_in.as_secs()).map_err(WeftError::Input)?;
        self.at.ctx.handle.mint_instance_token(&instance, expires_in.as_secs(), true, self.at.project).await
    }

    /// Every signal token of the project's owner (`weft token ls`): what
    /// each reaches and when it stops working, never its value.
    pub async fn list(self) -> WeftResult<Vec<TokenSummary>> {
        self.at.call(ProgramCall::Settings(SettingsCall::SignalTokensList), StopSelf::Keep).await
    }

    /// Revoke the signal token `id` of the project's owner (`weft token
    /// revoke`). Answers whether one went; an operator key is a person's
    /// to revoke, never a program's.
    pub async fn revoke(self, id: uuid::Uuid) -> WeftResult<bool> {
        let call = ProgramCall::Settings(SettingsCall::SignalTokenRevoke { id });
        let answer: TokensRevoked = self.at.call(call, StopSelf::Keep).await?;
        Ok(answer.revoked > 0)
    }

    /// One instance's tokens.
    pub fn instance(self, id: impl Into<String>) -> InstanceTokens<'a> {
        InstanceTokens { at: self.at, instance: id.into() }
    }
}

/// One instance's tokens.
pub struct InstanceTokens<'a> {
    at: ProjectCalls<'a>,
    instance: String,
}

impl InstanceTokens<'_> {
    /// Revoke every token of the instance in this project. Answers how many
    /// went.
    pub async fn revoke(self) -> WeftResult<u64> {
        let call = ProgramCall::Instances(InstanceCall::TokensRevoke { instance: required_instance(&self.instance)?, id: None });
        let answer: TokensRevoked = self.at.call(call, StopSelf::Keep).await?;
        Ok(answer.revoked)
    }
}

