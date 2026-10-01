//! Telling a role that work waits for it.
//!
//! A role on the machine hears the database's own notification of the
//! write that queued the work, so kicking it is nothing. A role that
//! scales to zero hears nothing while it is at zero, so a kick calls its
//! tick (`weft_platform_traits::roles::TICK_PATH`) and returns at once;
//! the call finishes on its own.

use std::collections::BTreeMap;
use std::sync::Arc;

use weft_platform_traits::config::InstallConfig;
use weft_platform_traits::roles::TICK_PATH;
use weft_platform_traits::{CoreRole, IdentityTokens, Kick, Placement, RoleAddresses};

pub struct RoleKick {
    /// The tick address of every serverless role, and its audience.
    ticks: BTreeMap<CoreRole, (String, String)>,
    tokens: Arc<dyn IdentityTokens>,
    http: reqwest::Client,
}

impl RoleKick {
    /// `addresses` are every role's, as this process reaches them.
    pub fn new(config: &InstallConfig, addresses: &RoleAddresses, tokens: Arc<dyn IdentityTokens>) -> Self {
        let ticks = CoreRole::ALL
            .into_iter()
            .filter(|r| config.roles.of(*r) == Placement::Serverless)
            .map(|r| {
                let base = addresses.of(r).expect("a serverless role has an internal address").to_string();
                (r, (format!("{base}{TICK_PATH}"), base))
            })
            .collect();
        Self { ticks, tokens, http: reqwest::Client::new() }
    }
}

impl Kick for RoleKick {
    fn kick(&self, role: CoreRole) {
        let Some((url, audience)) = self.ticks.get(&role).cloned() else { return };
        let tokens = self.tokens.clone();
        let http = self.http.clone();
        tokio::spawn(async move {
            let sent = async {
                let token = tokens.token_for(&audience).await?;
                http.post(&url).bearer_auth(token).send().await?.error_for_status()?;
                anyhow::Ok(())
            };
            // A lost kick costs a delay, never work: the role's own next
            // wake drains what waits.
            if let Err(e) = sent.await {
                tracing::warn!(target: "weft_runtime::kick", %role, error = %format!("{e:#}"), "could not kick a role; its next wake picks the work up");
            }
        });
    }
}
