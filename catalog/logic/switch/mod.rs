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
use serde_json::Value;

use weft::node::NodeOutput;
use weft::{node_bail, ExecutionContext, Node, NodeManifest, WeftResult};

#[derive(NodeManifest)]
pub struct SwitchNode;

#[cfg(feature = "node-tests")]
mod tests;

/// One branch: the port it drives, and the test the value must pass.
struct Case {
    port: String,
    test: Test,
}

/// A case's test, already read out of the entry. The variant IS the
/// entry's `kind`.
enum Test {
    Equals(Value),
    In(Vec<Value>),
    Contains(Value),
    Matches(regex::Regex),
    Gt(f64),
    Gte(f64),
    Lt(f64),
    Lte(f64),
    Between { min: f64, max: f64 },
    Otherwise,
}

impl Test {
    /// Read the test an entry's `kind` names. Errors only on a shape the
    /// compiler would have refused, so a failure here means the project
    /// was built by something that skipped it.
    // SYNC: the kinds here <-> catalog/logic/switch/metadata.json portsFromConfig.specs
    fn read(kind: &str, entry: &Value) -> WeftResult<Self> {
        let number = |key: &str| -> WeftResult<f64> {
            match entry.get(key).and_then(Value::as_f64) {
                Some(n) => Ok(n),
                None => node_bail!("a `{kind}` case needs a number in `{key}`"),
            }
        };
        let value = |key: &str| -> WeftResult<&Value> {
            match entry.get(key) {
                Some(v) => Ok(v),
                None => node_bail!("a `{kind}` case needs a `{key}`"),
            }
        };
        Ok(match kind {
            "equals" => Self::Equals(value("value")?.clone()),
            "in" => match value("value")?.as_array() {
                Some(items) => Self::In(items.clone()),
                None => node_bail!("an `in` case needs a list of values in `value`"),
            },
            "contains" => Self::Contains(value("value")?.clone()),
            "matches" => match value("value")?.as_str().map(regex::Regex::new) {
                Some(Ok(re)) => Self::Matches(re),
                Some(Err(e)) => node_bail!("a `matches` case is not a regex: {e}"),
                None => node_bail!("a `matches` case needs a regex string in `value`"),
            },
            "gt" => Self::Gt(number("value")?),
            "gte" => Self::Gte(number("value")?),
            "lt" => Self::Lt(number("value")?),
            "lte" => Self::Lte(number("value")?),
            "between" => Self::Between { min: number("min")?, max: number("max")? },
            "otherwise" => Self::Otherwise,
            other => node_bail!("`{other}` is not a case kind Switch knows"),
        })
    }

    /// Does `value` pass this test? A test that cannot apply to the
    /// value it was handed (a `gt` on a string, `matches` on a number)
    /// simply does not match: the case is not this one.
    fn holds(&self, value: &Value) -> bool {
        // JSON equality separates `5` from `5.0` (integer vs float
        // representation), which a case author never means: numbers
        // compare by VALUE, everything else structurally. Two integers
        // compare exactly, never through f64: above 2^53 an f64 cannot
        // tell adjacent integers apart, and message-platform ids are
        // exactly that large.
        fn same(a: &Value, b: &Value) -> bool {
            match (a, b) {
                (Value::Number(x), Value::Number(y)) => match (x.as_i64(), y.as_i64()) {
                    (Some(i), Some(j)) => i == j,
                    _ => match (x.as_u64(), y.as_u64()) {
                        (Some(i), Some(j)) => i == j,
                        _ => x.as_f64() == y.as_f64(),
                    },
                },
                _ => a == b,
            }
        }
        match self {
            Self::Equals(expected) => same(value, expected),
            Self::In(options) => options.iter().any(|option| same(value, option)),
            Self::Contains(item) => match value {
                Value::Array(items) => items.iter().any(|held| same(held, item)),
                Value::String(text) => {
                    item.as_str().is_some_and(|needle| text.contains(needle))
                }
                _ => false,
            },
            Self::Matches(re) => value.as_str().is_some_and(|text| re.is_match(text)),
            Self::Gt(n) => value.as_f64().is_some_and(|v| v > *n),
            Self::Gte(n) => value.as_f64().is_some_and(|v| v >= *n),
            Self::Lt(n) => value.as_f64().is_some_and(|v| v < *n),
            Self::Lte(n) => value.as_f64().is_some_and(|v| v <= *n),
            Self::Between { min, max } => {
                value.as_f64().is_some_and(|v| v >= *min && v <= *max)
            }
            Self::Otherwise => true,
        }
    }
}

/// Read the `cases` config into the branches, in written order. The
/// order is the matching order, so `otherwise` being last is what makes
/// it the catch-all; the compiler enforces that and this reads what it
/// approved.
fn read_cases(raw: &Value) -> WeftResult<Vec<Case>> {
    let Some(entries) = raw.as_array() else {
        node_bail!("`cases` must be a list of branches");
    };
    let mut cases = Vec::with_capacity(entries.len());
    for entry in entries {
        let Some(port) = entry.get("port").and_then(Value::as_str) else {
            node_bail!("every case needs a `port` naming the output it drives");
        };
        let Some(kind) = entry.get("kind").and_then(Value::as_str) else {
            node_bail!("every case needs a `kind` naming its test");
        };
        cases.push(Case { port: port.to_string(), test: Test::read(kind, entry)? });
    }
    Ok(cases)
}

#[async_trait]
impl Node for SwitchNode {
    #[cfg(feature = "node-tests")]
    fn tests(&self) -> Vec<weft::NodeTest> {
        tests::tests()
    }

    async fn run(&self, ctx: ExecutionContext) -> WeftResult<()> {
        let value: Value = ctx.inputs.get("value")?;
        let cases = read_cases(&ctx.inputs.get("cases")?)?;

        // The first case whose test holds wins, and `otherwise` holds by
        // definition. Everything else closes: a branch that was not taken
        // must say "nothing is coming" rather than stay silent, or the
        // nodes behind it wait forever.
        let taken = cases.iter().find(|case| case.test.holds(&value));

        let mut output = NodeOutput::new();
        if let Some(taken) = taken {
            output = output.set(&taken.port, Value::Bool(true));
        }
        ctx.pulse_downstream(output).await
    }
}
