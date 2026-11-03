//! ElevenLabsRunTemplate: run a published ElevenLabs Flows template.
//! The template's inputs, and the outputs the program wants, are the
//! node's own ports, each named by its id in the template:
//!
//! ```text
//! still = ElevenLabsRunTemplate(prompt: String, reference: Image) -> (product_still: Image) {
//!   account: elevenlabs.access
//!   template: "tmpl_abc123"
//! }
//! ```
//!
//! Before anything is paid, the node reads the template and checks its
//! ports against the version it is about to run: every input the
//! template takes needs a port whose type fits, and every port needs an
//! input or an output of the template to belong to. Every mismatch is
//! named in one refusal, as the program mistake it is. The read happens
//! inside the submit, which runs once: a run resumed after its wait finds
//! the job in its state and never reads the template again.

use std::collections::{BTreeMap, VecDeque};

use async_trait::async_trait;
use serde_json::{json, Map, Value};

use weft::access::client::get_json;
use weft::node::NodeOutput;
use weft::storage::FileHandle;
use weft::{
    node_error, Access, RunContext, Node, NodeErrExt, NodeManifest, WeftError, WeftPrimitive,
    WeftResult, WeftType,
};

use super::elevenlabs::API;
use super::flows::{finish, store_output, submit, Job, References};

#[derive(NodeManifest)]
pub struct ElevenLabsRunTemplateNode;

#[cfg(feature = "node-tests")]
mod tests;

/// The node's own settings: a template input of one of these names can
/// have no port.
const SETTINGS: [&str; 3] = ["account", "template", "version"];

/// How many published versions one template read lists, newest first:
/// the most ElevenLabs answers.
const VERSIONS: u32 = 10;

/// Seconds between two looks at a running template. A template can
/// make videos, and ElevenLabs asks for no more than one look every 10
/// seconds on a video.
const POLL_SECS: u64 = 10;

/// What a port that received nothing reads as.
const NOTHING: &Value = &Value::Null;

#[async_trait]
impl Node for ElevenLabsRunTemplateNode {
    #[cfg(feature = "node-tests")]
    fn tests(&self) -> Vec<weft::NodeTest> {
        tests::tests()
    }

    async fn run(&self, ctx: RunContext) -> WeftResult<()> {
        let account: Access = ctx.inputs.get("account")?;
        let template = template_id(ctx.inputs.get("template")?)?;
        let version: Option<String> = ctx.inputs.opt("version")?;

        // The ports the program created, and every data output.
        let declared = ctx.declared_inputs();
        let inputs: BTreeMap<String, WeftType> = ctx
            .inputs
            .created()
            .map(|name| {
                let ty = declared.get(name).node_err(format!("the created input `{name}` has no declared type"))?;
                Ok((name.clone(), ty.clone()))
            })
            .collect::<WeftResult<_>>()?;
        let outputs: BTreeMap<String, WeftType> = ctx.declared_outputs().into_iter().collect();

        let http = ctx.client(&account).await?;
        let refs = References::new(&ctx, &http);
        let route = format!("{API}/flows/templates/{template}");
        let mut job = ctx.keep(Job::default()).await?;
        let submitted = submit(
            &mut job,
            &refs,
            &format!("{route}/runs"),
            &format!("start a run of the ElevenLabs template {template}"),
            "ElevenLabs answered no run id when the template run was started",
            || async {
                let read = get_json(
                    &http,
                    &format!("{route}?versions_per_template={VERSIONS}"),
                    &format!("read the ElevenLabs template {template}"),
                )
                .await?;
                let chosen = pick_version(&template, &read, version.as_deref())?;
                let template_inputs = ports(chosen, "inputs")?;
                let problems = [
                    input_mismatches(template_inputs, &inputs),
                    output_mismatches(ports(chosen, "outputs")?, &outputs),
                ]
                .concat();
                if !problems.is_empty() {
                    return Err(WeftError::Input(format!(
                        "the ports of this node do not match version {} of the ElevenLabs template \
                         {template}, so nothing was run: {}",
                        chosen["version_id"].as_str().unwrap_or_default(),
                        problems.join("; ")
                    )));
                }
                // The run names the version that was just checked, pinned
                // or not, so one published between the read and the start
                // cannot run with ports nobody checked.
                Ok(json!({
                    "inputs": bind_inputs(&ctx, &refs, template_inputs).await?,
                    "version_id": chosen["version_id"],
                }))
            },
        )
        .await?;
        let run_id = &submitted.id;
        let run =
            finish(&ctx, &mut job, &account, &http, &format!("{route}/runs/{run_id}"), POLL_SECS, &submitted).await?;
        match run["status"].as_str().unwrap_or_default() {
            "completed" => {
                let mut emitted = NodeOutput::new();
                for port in outputs.keys() {
                    let value = output_value(&ctx, port, &run["outputs"][port.as_str()]).await?;
                    emitted = emitted.set(port.as_str(), value);
                }
                ctx.pulse_downstream(emitted).await
            }
            "failed" => Err(node_error(format!(
                "the run {run_id} of the ElevenLabs template {template} failed: {}",
                failed_outputs(&run["outputs"])
            ))),
            other => weft::node_bail!(
                "elevenlabs answered an unexpected status '{other}' for the run {run_id} of the template {template}"
            ),
        }
    }
}

/// `id` when it is one clean URL segment, the shape ElevenLabs gives
/// template ids (`tmpl_abc123`). It goes into the route, so anything
/// else could reach another one.
pub fn template_id(id: String) -> WeftResult<String> {
    let clean = !id.is_empty()
        && !id.starts_with('.')
        && id.chars().all(|c| c.is_ascii_alphanumeric() || matches!(c, '-' | '_' | '.'));
    if clean {
        Ok(id)
    } else {
        Err(WeftError::Input(format!("'{id}' is not an ElevenLabs template id (they look like tmpl_abc123)")))
    }
}

/// The version to check and run: the pinned one when there is one,
/// else the one ElevenLabs runs when none is (flagged `is_latest`, or
/// the newest listed when none is flagged). Both refusals are the
/// program's to fix.
pub fn pick_version<'a>(template_id: &str, template: &'a Value, pinned: Option<&str>) -> WeftResult<&'a Value> {
    let versions = template["versions"]
        .as_array()
        .node_err(format!("elevenlabs answered the template {template_id} with no version list"))?;
    let Some(newest) = versions.first() else {
        return Err(WeftError::Input(format!(
            "the ElevenLabs template {template_id} has no version the API can run. ElevenLabs leaves out a \
             published version whose graph uses a model it does not serve through the API, or whose saved \
             snapshot is gone; publish a version built on models the API serves"
        )));
    };
    let Some(pinned) = pinned else {
        return Ok(versions.iter().find(|v| v["is_latest"] == true).unwrap_or(newest));
    };
    versions.iter().find(|v| v["version_id"] == pinned).ok_or_else(|| {
        let listed: Vec<&str> = versions.iter().filter_map(|v| v["version_id"].as_str()).collect();
        let older = if template["has_more_versions"] == true {
            format!(
                ". Those are its {VERSIONS} newest published versions: ElevenLabs lists no older one, so an \
                 older one cannot be checked before it runs"
            )
        } else {
            String::new()
        };
        WeftError::Input(format!(
            "the ElevenLabs template {template_id} has no runnable version {pinned}; pin one of {}{older}",
            listed.join(", ")
        ))
    })
}

/// The ports on one `side` ("inputs" or "outputs") of a template
/// version.
fn ports<'a>(version: &'a Value, side: &str) -> WeftResult<&'a [Value]> {
    version[side]
        .as_array()
        .map(Vec::as_slice)
        .node_err(format!("elevenlabs answered a template version with no {side} list"))
}

/// Every way the node's input ports and the template version's inputs
/// disagree: a template input with no port, one no port can carry, a
/// port whose declared type does not fit, a port the template does not
/// take. Empty when they agree.
pub fn input_mismatches(template: &[Value], ports: &BTreeMap<String, WeftType>) -> Vec<String> {
    let mut problems = Vec::new();
    for input in template {
        let id = port_id(input);
        let schema = &input["content_schema"];
        if SETTINGS.contains(&id) {
            problems.push(format!(
                "the template input {} has the name of this node's own `{id}` setting, so no port can carry \
                 it; rename it in the template",
                named(input)
            ));
            continue;
        }
        let expected = match weft_type(schema) {
            Ok(expected) => expected,
            Err(why) => {
                problems.push(format!("the template input {} is {why}", named(input)));
                continue;
            }
        };
        match ports.get(id) {
            None => problems.push(format!(
                "the template input {} has no port: declare `{id}: {expected}`",
                named(input)
            )),
            Some(declared) if !WeftType::is_compatible(declared, &expected) => problems.push(format!(
                "the port `{id}` is declared `{declared}`, which does not fit the template's {} input: \
                 declare `{id}: {expected}`",
                kind(schema)
            )),
            Some(_) => {}
        }
    }
    let ids: Vec<&str> = template.iter().map(port_id).collect();
    for name in ports.keys().filter(|name| !ids.contains(&name.as_str())) {
        problems.push(format!(
            "the port `{name}` is not an input of the template ({})",
            listing("inputs", template)
        ));
    }
    problems
}

/// Every way the node's output ports and the template version's
/// outputs disagree: a port the template does not produce, one whose
/// declared type the output does not fit, an output the node cannot
/// read. The template's outputs nobody declared are simply not read.
pub fn output_mismatches(template: &[Value], ports: &BTreeMap<String, WeftType>) -> Vec<String> {
    let mut problems = Vec::new();
    for (name, declared) in ports {
        let Some(output) = template.iter().find(|o| port_id(o) == name.as_str()) else {
            problems.push(format!(
                "the port `{name}` is not an output of the template ({})",
                listing("outputs", template)
            ));
            continue;
        };
        let schema = &output["content_schema"];
        match output_type(schema) {
            Err(why) => problems.push(format!("the template output {} is {why}", named(output))),
            Ok(expected) if !WeftType::is_compatible(&expected, declared) => problems.push(format!(
                "the port `{name}` is declared `{declared}`, which the template's {} output does not fit: \
                 declare `{name}: {expected}`",
                kind(schema)
            )),
            Ok(_) => {}
        }
    }
    problems
}

/// The weft type a template port carries, by its schema's kind: text
/// and a voice id are a String, a number or an integer a Number, a list
/// the List of its items' type. An object has none: a run cannot be
/// given one through the API yet.
pub fn weft_type(schema: &Value) -> Result<WeftType, String> {
    let primitive = match kind(schema) {
        "string" | "voice" => WeftPrimitive::String,
        "number" | "integer" => WeftPrimitive::Number,
        "boolean" => WeftPrimitive::Boolean,
        "image" => WeftPrimitive::Image,
        "video" => WeftPrimitive::Video,
        "audio" => WeftPrimitive::Audio,
        "array" => {
            return weft_type(&schema["items"])
                .map(WeftType::list)
                .map_err(|why| format!("a list whose items are {why}"))
        }
        "object" => return Err("an object, which a run cannot be given through the API yet".to_string()),
        other => return Err(format!("of a kind this node does not know ('{other}')")),
    };
    Ok(WeftType::primitive(primitive))
}

/// The weft type a template output carries, or why the node cannot
/// read it: ElevenLabs hands back no list or object output yet.
pub fn output_type(schema: &Value) -> Result<WeftType, String> {
    match kind(schema) {
        "image" | "video" | "audio" | "string" | "number" | "integer" | "boolean" => weft_type(schema),
        "array" => Err("a list, which ElevenLabs does not hand back through the API yet".to_string()),
        "object" => Err("an object, which ElevenLabs does not hand back through the API yet".to_string()),
        other => Err(format!("of a kind this node cannot read ('{other}')")),
    }
}

/// What the template input `port` takes for the value its port carries,
/// by its schema's kind: text as itself, a voice id as a voice
/// reference, a number or a boolean as JSON, a list item by item, and
/// each file as whatever `file` answers for it (the reference it goes
/// as).
pub fn input_value(
    port: &str,
    schema: &Value,
    value: &Value,
    file: &mut dyn FnMut(FileHandle) -> WeftResult<Value>,
) -> WeftResult<Value> {
    let wrong = |takes: &str| WeftError::Input(format!("`{port}` takes {takes}, and got {value}"));
    match kind(schema) {
        "string" => value.as_str().map(|_| value.clone()).ok_or_else(|| wrong("text")),
        "voice" => value
            .as_str()
            .map(|id| json!({ "type": "voice", "voice_id": id }))
            .ok_or_else(|| wrong("a voice id")),
        "number" => value.is_number().then(|| value.clone()).ok_or_else(|| wrong("a number")),
        "integer" => whole(value).ok_or_else(|| wrong("a whole number")),
        "boolean" => value.is_boolean().then(|| value.clone()).ok_or_else(|| wrong("true or false")),
        "image" | "video" | "audio" => file(FileHandle::from_value(value)?),
        "array" => {
            let items = value.as_array().ok_or_else(|| wrong("a list"))?;
            let mut sent = Vec::with_capacity(items.len());
            for (i, item) in items.iter().enumerate() {
                sent.push(input_value(&format!("{port}[{i}]"), &schema["items"], item, &mut *file)?);
            }
            Ok(Value::Array(sent))
        }
        other => Err(WeftError::Input(format!(
            "`{port}` is a template input of a kind this node cannot send ('{other}')"
        ))),
    }
}

/// `value` as the whole number it is: a wire carries `3.0` where a
/// program wrote `3`, and an integer input takes `3`. `None` for a
/// fraction, or past what a float holds exactly.
fn whole(value: &Value) -> Option<Value> {
    if value.is_i64() || value.is_u64() {
        return Some(value.clone());
    }
    let f = value.as_f64()?;
    (f.fract() == 0.0 && f.abs() <= 9_007_199_254_740_992.0).then(|| json!(f as i64))
}

/// The `inputs` a run sends: each template input's port value, the way
/// its kind takes it. Every value is checked before the first file goes
/// up, so a bad one leaves nothing behind in the workspace.
async fn bind_inputs(ctx: &RunContext, refs: &References<'_>, template: &[Value]) -> WeftResult<Value> {
    let mut values = Vec::with_capacity(template.len());
    for input in template {
        let id = port_id(input);
        let value = ctx.inputs.raw(id).unwrap_or(NOTHING);
        if value.is_null() {
            return Err(WeftError::Input(format!(
                "nothing arrived on `{id}`, and the template takes it on every run: drop the `?` on the \
                 port, so a run without it skips this node"
            )));
        }
        values.push((id, &input["content_schema"], value));
    }
    // The first walk checks every value and gathers its files in order;
    // the second meets the same files in the same order and sends each
    // as the reference it became.
    let mut files = Vec::new();
    for (id, schema, value) in &values {
        input_value(id, schema, value, &mut |f: FileHandle| -> WeftResult<Value> {
            files.push(f);
            Ok(Value::Null)
        })?;
    }
    let mut references = VecDeque::from(refs.of_each(&files).await?);
    let mut sent = Map::new();
    for (id, schema, value) in &values {
        let bound = input_value(id, schema, value, &mut |_: FileHandle| -> WeftResult<Value> {
            references.pop_front().node_err("a file met on the second walk was not met on the first")
        })?;
        sent.insert(id.to_string(), bound);
    }
    Ok(Value::Object(sent))
}

/// What one output of a completed run emits on its port: an image, a
/// video or an audio stored as a file, a text, number or boolean as its
/// content.
async fn output_value(ctx: &RunContext, port: &str, output: &Value) -> WeftResult<Value> {
    match output["type"].as_str() {
        Some(kind @ ("image" | "video" | "audio")) => store_output(ctx, kind, output).await,
        Some("string" | "number" | "integer" | "boolean") if !output["content"].is_null() => {
            Ok(output["content"].clone())
        }
        Some(kind) => weft::node_bail!("elevenlabs answered the {kind} output `{port}` with nothing to read: {output}"),
        None => weft::node_bail!("elevenlabs answered the completed run with no output `{port}`"),
    }
}

/// Every failed output of a run, as `<port>: <reason>: <message>`.
pub fn failed_outputs(outputs: &Value) -> String {
    let failed: Vec<String> = outputs
        .as_object()
        .into_iter()
        .flatten()
        .filter(|(_, output)| output["status"] == "failed")
        .map(|(port, output)| {
            format!(
                "{port}: {}: {}",
                output["failure_reason"].as_str().unwrap_or("no reason given"),
                output["error_message"].as_str().unwrap_or("no message")
            )
        })
        .collect();
    if failed.is_empty() {
        "no output says why".to_string()
    } else {
        failed.join("; ")
    }
}

/// A template port's id.
fn port_id(port: &Value) -> &str {
    port["id"].as_str().unwrap_or_default()
}

/// The kind a content schema states (`string`, `image`, `array`, ...).
fn kind(schema: &Value) -> &str {
    schema["type"].as_str().unwrap_or_default()
}

/// A template port as a refusal names it: its id, and the title the
/// template gives it when there is one.
fn named(port: &Value) -> String {
    let id = port_id(port);
    match port["content_schema"]["title"].as_str() {
        Some(title) if !title.is_empty() => format!("`{id}` ({title})"),
        _ => format!("`{id}`"),
    }
}

/// The template's ports on one side, each as its declaration
/// (`reference: Image`, or its bare kind when no weft type carries it).
fn listing(side: &str, ports: &[Value]) -> String {
    if ports.is_empty() {
        return format!("it has no {side}");
    }
    let each: Vec<String> = ports
        .iter()
        .map(|port| {
            let schema = &port["content_schema"];
            match weft_type(schema) {
                Ok(ty) => format!("{}: {ty}", port_id(port)),
                Err(_) => format!("{}: {}", port_id(port), kind(schema)),
            }
        })
        .collect();
    format!("its {side}: {}", each.join(", "))
}
