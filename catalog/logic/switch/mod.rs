//! Switch: pick one branch by testing a value.
//!
//! Each case names an output port, and the case's KIND is the test the
//! value has to pass. At run time exactly one port emits `true` and
//! every other one closes, so wiring a case's port into a node's
//! `_should_flow` is what turns that branch on and leaves the others
//! unrun.
//!
//! Tests: `equals`, `in`, `contains`, `matches` (a regular expression),
//! `gt` / `gte` / `lt` / `lte`, `between` for a range, and `otherwise`,
//! which takes anything and so goes last.
//!
//! The ports come from the `cases` config through the `portsFromConfig`
//! mechanism, and the compiler checks the shape of every test, so what
//! reaches this body is a list it has already approved.

use async_trait::async_trait;
use serde::Deserialize;
use serde_json::Value;

use weft::node::NodeOutput;
use weft::{RunContext, Node, NodeManifest, WeftResult};

use super::compare::same;

#[derive(NodeManifest)]
pub struct SwitchNode;

#[cfg(feature = "node-tests")]
mod tests;

/// One branch: the port it drives, and the test the value must pass.
#[derive(Deserialize)]
struct Case {
    port: String,
    #[serde(flatten)]
    test: Test,
}

/// A case's test. The variant IS the entry's `kind`.
// SYNC: the kinds here <-> catalog/logic/switch/metadata.json portsFromConfig.specs
#[derive(Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
enum Test {
    Equals { value: Value },
    In { value: Vec<Value> },
    Contains { value: Value },
    Matches { value: String },
    Gt { value: f64 },
    Gte { value: f64 },
    Lt { value: f64 },
    Lte { value: f64 },
    Between { min: f64, max: f64 },
    Otherwise,
}

impl Test {
    /// Does `value` pass this test? A test that cannot apply to the
    /// value it was handed (a `gt` on a string, `matches` on a number)
    /// simply does not match: the case is not this one.
    fn holds(&self, value: &Value) -> WeftResult<bool> {
        Ok(match self {
            Self::Equals { value: expected } => same(value, expected),
            Self::In { value: options } => options.iter().any(|option| same(value, option)),
            Self::Contains { value: item } => match value {
                Value::Array(items) => items.iter().any(|held| same(held, item)),
                Value::String(text) => item.as_str().is_some_and(|needle| text.contains(needle)),
                _ => false,
            },
            Self::Matches { value: pattern } => match regex::Regex::new(pattern) {
                Ok(re) => value.as_str().is_some_and(|text| re.is_match(text)),
                Err(e) => weft::input_bail!("a `matches` case is not a regex: {e}"),
            },
            Self::Gt { value: n } => value.as_f64().is_some_and(|v| v > *n),
            Self::Gte { value: n } => value.as_f64().is_some_and(|v| v >= *n),
            Self::Lt { value: n } => value.as_f64().is_some_and(|v| v < *n),
            Self::Lte { value: n } => value.as_f64().is_some_and(|v| v <= *n),
            Self::Between { min, max } => value.as_f64().is_some_and(|v| v >= *min && v <= *max),
            Self::Otherwise => true,
        })
    }
}

#[async_trait]
impl Node for SwitchNode {
    #[cfg(feature = "node-tests")]
    fn tests(&self) -> Vec<weft::NodeTest> {
        tests::tests()
    }

    async fn run(&self, ctx: RunContext) -> WeftResult<()> {
        let value: Value = ctx.inputs.get("value")?;
        // In written order, which is the matching order, so `otherwise`
        // being last (the compiler holds it there) is what makes it the
        // catch-all.
        let cases: Vec<Case> = ctx.inputs.get("cases")?;

        // The first case whose test holds wins, and `otherwise` holds by
        // definition. Everything else closes: a branch that was not taken
        // must say "nothing is coming" rather than stay silent, or the
        // nodes behind it wait forever.
        let mut taken = None;
        for case in &cases {
            if case.test.holds(&value)? {
                taken = Some(case);
                break;
            }
        }

        let mut output = NodeOutput::new();
        if let Some(taken) = taken {
            output = output.set(&taken.port, Value::Bool(true));
        }
        ctx.pulse_downstream(output).await
    }
}
