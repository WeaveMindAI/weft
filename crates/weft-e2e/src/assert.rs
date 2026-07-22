//! Intent assertions over a [`SettledRun`].
//!
//! These read the run's replay event log and express checks in the language of
//! "what the program did", not JSON spelunking: `completed()`,
//! `output_of("debug")`, `node_skipped("branch")`, `loop_iterations("my_loop")`.
//! Each returns a `Result` so a test can `?` them; a failed check is an `Err`
//! carrying enough of the replay to see why, never a silent pass.
//!
//! Field-name knowledge (which event carries which value) lives in [`crate::event`]
//! and is SYNC'd by string against the dispatcher's DispatcherEvent. These
//! helpers stay above that, reading through the [`Replay`] accessors.

use anyhow::{bail, Result};
use serde_json::Value;

use crate::event::Replay;
use crate::run::SettledRun;

impl SettledRun {
    /// Assert the execution completed successfully. On failure, surfaces the
    /// terminal event's error/reason so the cause is visible.
    pub fn completed(&self) -> Result<&Self> {
        match self.status.as_str() {
            "completed" => Ok(self),
            "failed" => bail!(
                "execution {} FAILED: {}",
                self.color,
                self.failure_message().unwrap_or_else(|| "<no error text>".into())
            ),
            "cancelled" => bail!(
                "execution {} was CANCELLED: {}",
                self.color,
                self.cancel_reason().unwrap_or_else(|| "<no reason>".into())
            ),
            other => bail!("execution {} has unexpected status {other}", self.color),
        }
    }

    /// Assert the execution failed AND its error text contains `needle`. Used by
    /// negative tests (a node that must fail loud). `needle` must be non-empty: a
    /// negative test has to name a DISCRIMINATING fragment of the expected error,
    /// since `contains("")` is always true and would assert nothing.
    pub fn failed_with(&self, needle: &str) -> Result<&Self> {
        anyhow::ensure!(
            !needle.is_empty(),
            "failed_with needs a non-empty needle; an empty one matches any error \
             and asserts nothing. Pass a discriminating fragment of the expected error."
        );
        if self.status != "failed" {
            bail!(
                "expected execution {} to FAIL, but status is {}",
                self.color,
                self.status
            );
        }
        let msg = self.failure_message().unwrap_or_default();
        if !msg.contains(needle) {
            bail!(
                "execution {} failed but error did not contain '{needle}': {msg}",
                self.color
            );
        }
        Ok(self)
    }

    /// The execution-level outputs (`execution_completed.outputs`), or null if
    /// the run did not complete.
    pub fn completed_outputs(&self) -> Value {
        self.replay
            .first_kind("execution_completed")
            .and_then(|e| e.field("outputs").cloned())
            .unwrap_or(Value::Null)
    }

    /// The `error` text of `execution_failed`, if the run failed.
    pub fn failure_message(&self) -> Option<String> {
        self.replay
            .first_kind("execution_failed")
            .and_then(|e| e.str_field("error"))
            .map(str::to_string)
    }

    /// The `reason` of `execution_cancelled`, if the run was cancelled.
    pub fn cancel_reason(&self) -> Option<String> {
        self.replay
            .first_kind("execution_cancelled")
            .and_then(|e| e.str_field("reason"))
            .map(str::to_string)
    }

    /// The output value a node produced on its FIRST firing
    /// (`node_completed.output` for the root frame). Returns `None` if the node
    /// never completed (it was skipped, failed, or did not run). For a node that
    /// fires multiple times (a loop body), use [`SettledRun::node_outputs`].
    pub fn output_of(&self, node: &str) -> Option<Value> {
        self.replay
            .by_kind("node_completed")
            .find(|e| e.is_node(node))
            .and_then(|e| e.field("output").cloned())
    }

    /// Every output value a node produced across all its firings, in event
    /// order (one per `node_completed`). For loop bodies and repeated fires.
    pub fn node_outputs(&self, node: &str) -> Vec<Value> {
        self.replay
            .by_kind("node_completed")
            .filter(|e| e.is_node(node))
            .filter_map(|e| e.field("output").cloned())
            .collect()
    }

    /// Assert a node produced `expected` on its first firing.
    pub fn assert_output(&self, node: &str, expected: &Value) -> Result<&Self> {
        match self.output_of(node) {
            Some(got) if &got == expected => Ok(self),
            Some(got) => bail!(
                "node '{node}' output mismatch\n  expected: {expected}\n  got:      {got}"
            ),
            None => bail!(
                "node '{node}' never completed (skipped/failed/absent); cannot assert output. \
                 Replay node events: {:?}",
                self.replay.for_node(node).map(|e| e.kind()).collect::<Vec<_>>()
            ),
        }
    }

    /// The assembled INPUT a node received on its first firing
    /// (`node_started.input`). For sink nodes like Debug (which consume an input
    /// and emit nothing), this is how the rig asserts "the node received value
    /// X": Debug has no output, so `output_of` is empty, but its input carries
    /// the delivered value. Returns `None` if the node never started.
    pub fn input_of(&self, node: &str) -> Option<Value> {
        self.replay
            .by_kind("node_started")
            .find(|e| e.is_node(node))
            .and_then(|e| e.field("input").cloned())
    }

    /// Assert a node received `expected` as the value on input port `port` on
    /// its first firing. The sink-node counterpart to [`SettledRun::assert_output`].
    pub fn assert_input(&self, node: &str, port: &str, expected: &Value) -> Result<&Self> {
        let input = self.input_of(node).ok_or_else(|| {
            anyhow::anyhow!(
                "node '{node}' never started; cannot assert its input. Its events: {:?}",
                self.replay.for_node(node).map(|e| e.kind()).collect::<Vec<_>>()
            )
        })?;
        let got = input.get(port);
        match got {
            Some(v) if v == expected => Ok(self),
            Some(v) => bail!(
                "node '{node}' input port '{port}' mismatch\n  expected: {expected}\n  got:      {v}"
            ),
            None => bail!(
                "node '{node}' input has no port '{port}'; full input was: {input}"
            ),
        }
    }

    /// True if the node was skipped (null-propagation: a required input closed).
    pub fn node_skipped(&self, node: &str) -> bool {
        self.replay
            .by_kind("node_skipped")
            .any(|e| e.is_node(node))
    }

    /// True if the node ran to completion at least once.
    pub fn node_completed(&self, node: &str) -> bool {
        self.replay
            .by_kind("node_completed")
            .any(|e| e.is_node(node))
    }

    /// True if the node was started at least once.
    pub fn node_started(&self, node: &str) -> bool {
        self.replay
            .by_kind("node_started")
            .any(|e| e.is_node(node))
    }

    /// Assert a node was SKIPPED via null-propagation: a `node_skipped` event
    /// fired and the node did NOT complete (it produced no output). The engine
    /// still ships a `node_started` (with the closed port listed) as part of its
    /// normal lifecycle even for a skip, so the honest contract is "skipped and
    /// did not complete", NOT "never started". This is the core branch-off-on-null
    /// shape: the required input closed, so the node yields no value downstream.
    pub fn assert_skipped(&self, node: &str) -> Result<&Self> {
        if !self.node_skipped(node) {
            bail!(
                "expected node '{node}' to be skipped, but no node_skipped event for it. \
                 Its events: {:?}",
                self.replay.for_node(node).map(|e| e.kind()).collect::<Vec<_>>()
            );
        }
        if self.node_completed(node) {
            bail!("node '{node}' was skipped AND completed; expected a skip with no output");
        }
        Ok(self)
    }

    /// Number of iterations a loop launched (count of `loop_iteration_launched`
    /// for `group_id`). The honest "how many times did the body run" measure.
    pub fn loop_iterations(&self, group_id: &str) -> usize {
        self.replay
            .by_kind("loop_iteration_launched")
            .filter(|e| e.str_field("group_id") == Some(group_id))
            .count()
    }

    /// Assert a loop launched exactly `n` iterations.
    pub fn assert_loop_iterations(&self, group_id: &str, n: usize) -> Result<&Self> {
        let got = self.loop_iterations(group_id);
        if got != n {
            bail!(
                "loop '{group_id}' iteration count mismatch: expected {n}, got {got}. \
                 loop_instantiated: {:?}",
                self.replay
                    .by_kind("loop_instantiated")
                    .map(|e| e.field("iter_count").cloned().unwrap_or(Value::Null))
                    .collect::<Vec<_>>()
            );
        }
        Ok(self)
    }

    /// Every cost the execution recorded, as `(service, amount_usd)` in order.
    /// Each is a provider meter's measurement of a real call; the execution's
    /// money trail. A record whose amount is `null` (the meter could not
    /// resolve the figure, an honest unknown) is skipped here.
    /// Every resolved cost record: `(service, origin, amount_usd)`. Origin is
    /// the wire string of whose key the call spent (`"user-provided"` or
    /// `"runtime"`). Records with a null amount (an honest unknown) are
    /// not in this list.
    pub fn costs(&self) -> Vec<(String, String, f64)> {
        self.replay
            .by_kind("cost_reported")
            .filter_map(|e| {
                Some((
                    e.str_field("service")?.to_string(),
                    e.str_field("origin")?.to_string(),
                    e.0.get("amount_usd").and_then(Value::as_f64)?,
                ))
            })
            .collect()
    }

    /// Assert the execution recorded exactly one cost for `service`, resolved
    /// to a real positive amount AND spent on the expected key (`origin` is
    /// `"user-provided"` or `"runtime"`): the call was made, it rode the
    /// key the test set up (no silent fall-through to the other one), and the
    /// meter read a real figure off the real response. (This is measurement,
    /// not billing: on this path the cost is recorded, not charged.)
    ///
    /// A cost record journals AFTER the terminal event by design (the meter
    /// resolves detached from the node, possibly via a provider follow-up
    /// query), so this waits for the record to land before judging it. The
    /// deadline covers the resolve's own internal bounds (the follow-up
    /// client's request timeout, the record's bounded enqueue retries, the
    /// task fold); a record still absent past it is a real failure.
    pub async fn assert_measured(&mut self, service: &str, origin: &str) -> Result<&Self> {
        const COST_TRAIL_DEADLINE: std::time::Duration = std::time::Duration::from_secs(60);
        self.refresh_replay_until(
            &format!("the '{service}' call's cost record to land on the journal"),
            COST_TRAIL_DEADLINE,
            |replay| {
                replay
                    .by_kind("cost_reported")
                    .any(|e| e.str_field("service") == Some(service))
            },
        )
        .await?;
        let costs = self.costs();
        let for_service: Vec<(&str, f64)> = costs
            .iter()
            .filter(|(s, _, _)| s == service)
            .map(|(_, o, amount)| (o.as_str(), *amount))
            .collect();
        match for_service.as_slice() {
            [(o, amount)] if *amount > 0.0 && *o == origin => Ok(self),
            [(o, _)] if *o != origin => bail!(
                "the '{service}' call spent on the '{o}' key, expected '{origin}': the call \
                 did not ride the key the test set up"
            ),
            [(_, amount)] => bail!(
                "the '{service}' call recorded a cost of ${amount}: the call happened but its \
                 cost never resolved to a real figure"
            ),
            // The refresh above guarantees a record for the service EXISTS,
            // so reaching here means its amount is null: the meter honestly
            // could not resolve the figure. That is a final answer, not
            // something to wait longer for.
            [] => bail!(
                "the '{service}' call's cost record landed with an UNKNOWN amount (the meter \
                 could not resolve the figure); resolved costs seen: {costs:?}"
            ),
            many => bail!("expected one '{service}' cost, got {}: {many:?}", many.len()),
        }
    }

    /// Borrow the raw replay for assertions the typed helpers don't cover yet.
    /// Escape hatch, not the default path: prefer adding a named helper above
    /// when a check recurs, so tests read as intent.
    pub fn replay(&self) -> &Replay {
        &self.replay
    }
}
