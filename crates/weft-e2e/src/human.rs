//! Human-in-the-loop, driven with no browser.
//!
//! A `HumanQuery` node suspends the execution and registers a `form` signal.
//! The rig plays the human: it enumerates pending forms via an api token,
//! submits an answer to `POST /signal/{token}`, and the suspended execution
//! resumes with that answer. A `HumanTrigger` form is the entry variant (a fresh
//! execution per submission) and uses the same submit path.
//!
//! This module is a thin, intent-named layer over [`crate::signal`]: discover a
//! form, answer it. The resume itself is asserted through the normal replay
//! (a `node_resumed` carrying the answer, then the node completing).

use std::time::Duration;

use anyhow::{Context, Result};
use serde_json::Value;

use crate::client::{poll_until, Dispatcher};
use crate::signal::{self, DiscoveredSignal, SignalScope};

/// Poll until a form registered by trigger node `node_id` in `project_id`
/// appears, then return it. Used when a project has several forms (an entry
/// trigger plus a mid-flow query) and the rig must wait for and answer a
/// specific one. Project-scoped so two projects running the same fixture (same
/// node name) never cross.
pub async fn wait_for_form_by_node(
    disp: &Dispatcher,
    project_id: &uuid::Uuid,
    node_id: &str,
) -> Result<DiscoveredSignal> {
    let node_id = node_id.to_string();
    // Open the project-scoped discovery handle ONCE and reuse it across every
    // poll iteration (minting a fresh token per poll would litter the token
    // table). `signal_for_node` returns Ok(None) for "not registered yet" (the
    // poll retries) and Err for ambiguous / enumeration failure (propagates
    // immediately so a real bug is not masked as a timeout).
    let scope = SignalScope::open(disp, project_id).await?;
    poll_until(
        &format!("a form to register for trigger node '{node_id}' in project {project_id}"),
        Duration::from_secs(60),
        Duration::from_millis(400),
        || {
            let scope = &scope;
            let node_id = node_id.as_str();
            async move { scope.signal_for_node(node_id).await }
        },
    )
    .await
}

/// Answer a discovered form: submit `answer` (a JSON object keyed by the form's
/// field keys) to the form's signal token. The suspended execution resumes with
/// this value.
pub async fn answer_form(
    disp: &Dispatcher,
    form: &DiscoveredSignal,
    answer: &Value,
) -> Result<()> {
    let token = form
        .token()
        .context("discovered form has no signal token to submit to")?;
    signal::fire_token(disp, token, answer).await
}
