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

    /// Compare two journalled values the way WEFT's type system does,
    /// which for numbers is by VALUE, not by JSON spelling. A weft
    /// `Number` is one type (the engine's own zero for it is the
    /// integer `0`, and its runtime check accepts either spelling), so
    /// `0` and `0.0` are the same value; which one reaches the journal
    /// only records whether the producing node happened to be Rust
    /// (f64) or Python (int). Asserting on that accident makes a test
    /// fail when a fixture swaps producers without changing its
    /// result, which is exactly what it must not do.
    ///
    /// The bridge is deliberately narrow: two INTEGERS still compare
    /// exactly (no f64 round-trip, so nothing above 2^53 can collapse
    /// into a false match); only an integer-vs-float pair converts.
    /// Everything else (strings, bools, shapes, key sets, ordering) is
    /// still exact.
    fn same_weft_value(a: &Value, b: &Value) -> bool {
        match (a, b) {
            (Value::Number(x), Value::Number(y)) => {
                if x.is_f64() || y.is_f64() {
                    match (x.as_f64(), y.as_f64()) {
                        (Some(xf), Some(yf)) => xf == yf,
                        _ => x == y,
                    }
                } else {
                    x == y
                }
            }
            (Value::Array(xs), Value::Array(ys)) => {
                xs.len() == ys.len()
                    && xs.iter().zip(ys).all(|(x, y)| Self::same_weft_value(x, y))
            }
            (Value::Object(xs), Value::Object(ys)) => {
                xs.len() == ys.len()
                    && xs.iter().all(|(k, x)| {
                        ys.get(k).is_some_and(|y| Self::same_weft_value(x, y))
                    })
            }
            _ => a == b,
        }
    }

    /// Assert a node produced `expected` on its first firing.
    pub fn assert_output(&self, node: &str, expected: &Value) -> Result<&Self> {
        match self.output_of(node) {
            Some(got) if Self::same_weft_value(&got, expected) => Ok(self),
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
            Some(v) if Self::same_weft_value(v, expected) => Ok(self),
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

    /// Assert a node RAN to completion at least once. The counterpart of
    /// [`Self::assert_skipped`]: a branch behind an optional output port
    /// (toolCalls, a fan port) is silently SKIPPED when the port never
    /// pulses, and the run still settles as completed, so a test that
    /// only reads the branch's sink would fail with a confusing empty
    /// value. Assert the branch actually ran first.
    pub fn assert_completed(&self, node: &str) -> Result<&Self> {
        if !self.node_completed(node) {
            bail!(
                "expected node '{node}' to run to completion, but it did not. \
                 Its events: {:?}",
                self.replay.for_node(node).map(|e| e.kind()).collect::<Vec<_>>()
            );
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
                    .map(|e| e.field("iter_cap").cloned().unwrap_or(Value::Null))
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
    /// the wire string of whose key the call spent (`"their-own"` or
    /// `"ours"`). Records with a null amount (an honest unknown) are
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
    /// `"their-own"` or `"ours"`): the call was made, it rode the
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
    /// `count` is how many calls the graph made against the service:
    /// every one must land its own resolved record on the right key.
    pub async fn assert_measured(
        &mut self,
        service: &str,
        origin: &str,
        count: usize,
    ) -> Result<&Self> {
        const COST_TRAIL_DEADLINE: std::time::Duration = std::time::Duration::from_secs(60);
        self.refresh_replay_until(
            &format!("all {count} '{service}' cost records to land on the journal"),
            COST_TRAIL_DEADLINE,
            |replay| {
                replay
                    .by_kind("cost_reported")
                    .filter(|e| e.str_field("service") == Some(service))
                    .count()
                    >= count
            },
        )
        .await?;
        let costs = self.costs();
        let for_service: Vec<(&str, f64)> = costs
            .iter()
            .filter(|(s, _, _)| s == service)
            .map(|(_, o, amount)| (o.as_str(), *amount))
            .collect();
        // The refresh above counted RECORDS; `costs()` keeps only the
        // resolved ones (a null amount drops out), so a shortfall here
        // means a record landed with an UNKNOWN amount: the meter
        // honestly could not resolve the figure. That is a final
        // answer, not something to wait longer for.
        if for_service.len() != count {
            bail!(
                "expected {count} resolved '{service}' cost(s), got {} (an UNRESOLVED record \
                 is not counted); costs seen: {costs:?}",
                for_service.len()
            );
        }
        for (o, amount) in &for_service {
            if *o != origin {
                bail!(
                    "a '{service}' call spent on the '{o}' key, expected '{origin}': the call \
                     did not ride the key the test set up"
                );
            }
            if *amount <= 0.0 {
                bail!(
                    "a '{service}' call recorded a cost of ${amount}: the call happened but \
                     its cost never resolved to a real figure"
                );
            }
        }
        Ok(self)
    }

    /// Borrow the raw replay for assertions the typed helpers don't cover yet.
    /// Escape hatch, not the default path: prefer adding a named helper above
    /// when a check recurs, so tests read as intent.
    pub fn replay(&self) -> &Replay {
        &self.replay
    }
}

#[cfg(test)]
mod same_weft_value_tests {
    use super::SettledRun;
    use serde_json::json;

    /// A weft Number is one type whatever its JSON spelling, so an
    /// assertion must not break when a fixture swaps a Rust producer
    /// (emits `0.0`) for a Python one (emits `0`), or the other way.
    #[test]
    fn integer_and_float_spellings_of_one_number_match() {
        assert!(SettledRun::same_weft_value(&json!(0), &json!(0.0)));
        assert!(SettledRun::same_weft_value(&json!([0, 2, 4]), &json!([0.0, 2.0, 4.0])));
        assert!(SettledRun::same_weft_value(
            &json!({"a": [1], "b": {"c": 2}}),
            &json!({"a": [1.0], "b": {"c": 2.0}})
        ));
    }

    /// The bridge is narrow on purpose: it must not turn a real
    /// mismatch into a pass, and two INTEGERS never round-trip through
    /// f64 (so nothing above 2^53 can collapse into a false match).
    #[test]
    fn it_never_hides_a_real_difference() {
        assert!(!SettledRun::same_weft_value(&json!(1), &json!(2)));
        assert!(!SettledRun::same_weft_value(&json!(0), &json!("0")));
        assert!(!SettledRun::same_weft_value(&json!([1, 2]), &json!([1, 2, 3])));
        assert!(!SettledRun::same_weft_value(&json!({"a": 1}), &json!({"a": 1, "b": 2})));
        assert!(!SettledRun::same_weft_value(&json!(null), &json!(0)));
        // Distinct integers that share one f64: exact-compared, so they differ.
        assert!(!SettledRun::same_weft_value(
            &json!(9_007_199_254_740_993_u64),
            &json!(9_007_199_254_740_992_u64)
        ));
    }
}
