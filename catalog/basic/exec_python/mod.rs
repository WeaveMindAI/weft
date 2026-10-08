//! ExecPython: run a user-supplied Python snippet in a `python3`
//! process beside the worker, from a pool the worker keeps.
//!
//! Wiring:
//!
//! - The catalog metadata declares no inputs/outputs. Users write
//!   the ports inline on the invocation:
//!       `foo = ExecPython(a: Number) -> (b: String) { code: "..." }`.
//!   The compiler's `canAddInputPorts` / `canAddOutputPorts`
//!   feature (handled by `merge_ports` in enrich.rs) materializes
//!   those ports onto the node definition before execution.
//!
//! - The `code` config field carries the Python source. Whatever
//!   the user writes there is wrapped in a zero-arg closure whose
//!   locals include every DECLARED input port by name: one that
//!   received nothing is bound to `None`, so a script reads an
//!   optional input with a plain `if problem is None`. `return
//!   <dict>` in the user code supplies the output pulses.
//!
//! - The script runs in a Python process of its own, one of a few the
//!   worker holds for its runs to share (`ctx.shared`), as many as
//!   the worker has CPUs, started the first time a script needs one:
//!   scripts run side by side. Each script's own names start fresh (its
//!   own scope), but the process is shared on purpose: a module a script
//!   imported stays imported for the next, and so does whatever it set on
//!   a module, the environment, the working directory and files it
//!   wrote. Inputs go to it as JSON and the answer comes
//!   back as JSON: strings to `str`, numbers to `int`/`float`, nulls to
//!   `None`, arrays to `list`, objects to `dict`, and back. No custom
//!   class leaks across. What a script prints goes to the worker's log.
//!
//! - A file arrives UNWRAPPED on a port DECLARED as one (the same
//!   question the way out asks, so a dict that happens to carry a
//!   marker key on a `JsonDict` port stays exactly as it is). On a
//!   wire a file is the marker
//!   `{"__weft_image__": {key, filename, mimeType, sizeBytes}}` (one
//!   sentinel per kind); the engine adds a `url` minted for this
//!   firing before the node runs. The script gets the inside of the
//!   marker as a plain dict, so `photo["url"]` is the download link
//!   and `photo["filename"]` the name, with no sentinel to unwrap.
//!   The kind is not lost: a file returned on a file-typed output is
//!   wrapped back into its marker from its mime type, and the engine
//!   strips the link on the way out as it does for every node.
//!
//! - The return value must be a dict keyed by declared output port
//!   name. A missing key means "no pulse on that port": the port is
//!   left out of the node's output entirely, nothing travels down
//!   it, and the normal skip propagation kicks in. A key set to
//!   `None` is the same on a port whose type does not take `Null`
//!   (`{"weather": None, "reason": "..."}` closes one branch and
//!   opens the other); on a `T | Null` port it sends a real null,
//!   which flows as data. `return {}` emits nothing at all.
//!
//! - Failures come in two kinds. An exception the script raises while
//!   it runs (a network call it makes failing, a `raise` of its own)
//!   carries its type, message and traceback; with the optional
//!   `error` output wired that message comes out there and every
//!   other output closes, unwired it fails the run. A mistake in the
//!   program itself fails the run either way: code that does not
//!   compile (an input error), and an answer that is not a dict of
//!   declared outputs holding values their types accept (a type
//!   error: no return, a key that is no output, `error` among the
//!   keys, a string on a number port). `error` is weft's, so a script
//!   that wants to fail on purpose raises.
//!
//! - A cancelled run stops its script at once: its process is killed
//!   (a script inside one long C call, a `time.sleep`, a blocking read,
//!   included), and the next script that needs one starts a fresh one.
//!   The same happens when the engine drops the node body.
//!
//! Isolation: the worker the node runs in IS the isolation boundary;
//! the Python executes there with the same access that worker already
//! has, and is not sandboxed further (a process of its own keeps a
//! script that crashes Python from taking the worker with it, nothing
//! more). Running a project therefore runs
//! its ExecPython code with that worker's privileges, the same trust
//! model as running the project's own program.

use async_trait::async_trait;
use serde_json::Value;
use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use tokio::io::{AsyncBufReadExt, AsyncWriteExt};

use weft::node::NodeOutput;
use weft::storage::media::{media_slots, substitute_media};
use weft::weft_type::FileKind;
use weft::context::ERROR_PORT;
use weft::{node_error, ExecutionContext, Node, NodeManifest, StoredFile, WeftError, WeftResult, WeftType};

#[derive(NodeManifest)]
pub struct ExecPythonNode;

#[cfg(feature = "node-tests")]
mod tests;

#[async_trait]
impl Node for ExecPythonNode {
    #[cfg(feature = "node-tests")]
    fn tests(&self) -> Vec<weft::NodeTest> {
        tests::tests()
    }

    async fn run(&self, ctx: ExecutionContext) -> WeftResult<()> {
        // An exception the script raises is a failure of the step,
        // which the runtime puts on `error` when it is wired. A mistake
        // in the program itself (the code does not compile, the answer
        // is not a dict of declared outputs of the right types) is an
        // input or type error, which fails the run whether or not
        // `error` is wired.
        let code: String = ctx.inputs.get("code")?;

        // Bind every DECLARED data input under its name in the Python
        // namespace. The declared set is the user's inline port list
        // (the node's own settings, `code`, never bind as variables);
        // a port with nothing in the bag (a skipped upstream, an
        // optional input nobody wired) binds as `None`, so the script
        // never meets an undefined name. Stored files bind unwrapped.
        let settings: std::collections::HashSet<&str> = ctx.inputs.declared().map(|(k, _)| k.as_str()).collect();
        let names: std::collections::BTreeSet<&String> = ctx
            .declared_inputs()
            .keys()
            .filter(|name| !settings.contains(name.as_str()))
            .chain(ctx.inputs.custom().map(|(name, _)| name))
            .collect();
        let inputs: Vec<(String, Value)> = names
            .into_iter()
            .map(|name| {
                let ty = ctx.declared_inputs().get(name);
                let value = ctx.inputs.raw(name).map(|v| unwrap_files(v, ty)).unwrap_or(Value::Null);
                (name.clone(), value)
            })
            .collect();

        let interpreters = ctx.shared("exec_python", |()| async { Ok(Interpreters::new()) }).await?;
        let result = run_python(&interpreters, &code, inputs, &ctx.cancellation()).await?;

        // Check the whole answer before anything goes out, so a wrong
        // key or type refuses the firing as the program mistake it is
        // (the engine's own refusal of an emission is a failure `error`
        // would catch).
        ctx.pulse_downstream(answer_output(&ctx.data_outputs(), result)?).await
    }
}

/// The script's answer as the firing's output. A missing key produces
/// no pulse; `None` is a null on a port whose type takes `Null` and no
/// pulse on any other, so `{"x": None}` skips a plain port x. A file handed back on a file-typed port
/// goes out as the marker the wire expects. A key that is not a declared
/// data output, `error` included (weft fills it with the script's
/// failure; a script fails on purpose by raising), and a value its port
/// does not accept are type errors naming the port.
fn answer_output(outputs: &HashMap<String, WeftType>, result: Vec<(String, Value)>) -> WeftResult<NodeOutput> {
    let mut out = NodeOutput::new();
    for (port, value) in result {
        if port == ERROR_PORT {
            return Err(WeftError::Type(format!(
                "the script returned '{ERROR_PORT}', which is where weft puts the script's own failure; \
                 to fail on purpose, raise an exception (its message comes out on '{ERROR_PORT}' when that is wired)"
            )));
        }
        let Some(ty) = outputs.get(&port) else {
            let mut declared: Vec<&str> = outputs.keys().map(String::as_str).collect();
            declared.sort_unstable();
            return Err(WeftError::Type(format!(
                "the script returned '{port}', which is not an output of this node (its outputs: {}); \
                 declare it in the node's header, `-> ({port}: T)`, or drop the key",
                if declared.is_empty() { "none".to_string() } else { declared.join(", ") }
            )));
        };
        // `None` sends a real null where the port takes one (`T | Null`),
        // and sends nothing anywhere else.
        if matches!(value, Value::Null) && !ty.accepts_runtime_value(&value) {
            continue;
        }
        let value = if ty.references_file() { wrap_files(&value, ty)? } else { value };
        if !ty.accepts_runtime_value(&value) {
            let why = ty.validate_value(&value).err().unwrap_or_else(|| format!("expected {ty}"));
            return Err(WeftError::Type(format!(
                "the script returned {} on '{port}', which takes {ty}: {why}",
                weft::truncate_user_string(&value.to_string(), 120)
            )));
        }
        out = out.set(port, value);
    }
    Ok(out)
}

/// The inside of every stored-file marker sitting where the port's
/// DECLARED type says a file goes: `{"__weft_image__": {...}}` becomes
/// `{...}`. The type is the question, exactly as on the way out
/// ([`wrap_files`]), so a dict that merely happens to carry a marker
/// key on a port declared `JsonDict` flows whole. A port with no file
/// anywhere in its type is copied untouched.
fn unwrap_files(value: &Value, ty: Option<&WeftType>) -> Value {
    let Some(ty) = ty.filter(|ty| ty.references_file()) else {
        return value.clone();
    };
    let replacements = media_slots(value, ty)
        .into_iter()
        .filter_map(|slot| {
            let obj = slot.as_object()?;
            let kind = FileKind::from_marker_obj(obj)?;
            let inner = obj.get(kind.marker_key())?.clone();
            Some((slot.to_string(), inner))
        })
        .collect();
    substitute_media(value, ty, &replacements)
}

/// The inverse of [`unwrap_files`] on an output typed as a file: every
/// media slot of `ty` holding a bare file dict (the shape the script
/// received, either handle) is wrapped back into the marker its mime
/// type says. A slot already carrying a marker passes; anything else
/// on a file slot is refused by name, so a script cannot put a string
/// where a picture goes.
fn wrap_files(value: &Value, ty: &WeftType) -> WeftResult<Value> {
    let mut replacements = std::collections::HashMap::new();
    for slot in media_slots(value, ty) {
        let Some(obj) = slot.as_object() else {
            return Err(WeftError::Type(format!(
                "the script returned {} on a file port; return the dict the file arrived as",
                weft::truncate_user_string(&slot.to_string(), 120)
            )));
        };
        if FileKind::from_marker_obj(obj).is_some() {
            continue;
        }
        // A file that lives at an external URL carries `url` where a
        // stored file carries `key` (see `FileHandle::Url`), so it is
        // not a `StoredFile` and never will be. The script was handed
        // that payload and handed it straight back, so wrap it by the
        // mime it declares rather than refusing a file this node
        // unwrapped itself.
        if obj.contains_key("url") {
            let mime = obj
                .get("mimeType")
                .and_then(Value::as_str)
                .unwrap_or("application/octet-stream");
            replacements.insert(
                slot.to_string(),
                WeftType::file_marker(FileKind::from_mime(mime), slot.clone()),
            );
            continue;
        }
        let file: StoredFile = serde_json::from_value(slot.clone()).map_err(|e| {
            WeftError::Type(format!("the script returned a dict on a file port that is not a stored file ({e}); return the dict the file arrived as"))
        })?;
        replacements.insert(slot.to_string(), file.to_value());
    }
    Ok(substitute_media(value, ty, &replacements))
}

/// The script's process: what it reads, one request a line on the stdin
/// it keeps for that, and what it answers, one a line on the stdout it
/// keeps for that. A script never touches either: what it prints goes to
/// stderr, the worker's log, and what it reads from stdin is empty, so an
/// `input()` cannot eat the next request. Each
/// request is `{"names", "body", "args"}`: the input names, the script
/// indented under the function it becomes, and the inputs in that order.
const SERVER: &str = r#"
import json, os, sys, traceback
_answers = os.fdopen(os.dup(1), "w", buffering=1)
os.dup2(2, 1)
_requests = os.fdopen(os.dup(0), "r")
os.dup2(os.open(os.devnull, os.O_RDONLY), 0)
sys.stdin = open(os.devnull)

def _described(e):
    tb = "".join(traceback.format_exception(type(e), e, e.__traceback__)).rstrip()
    return f"{type(e).__name__}: {e}\n{tb}"

def _plain(v):
    if v is None or isinstance(v, (bool, str, int)):
        return v
    if isinstance(v, float):
        if v != v or v in (float("inf"), float("-inf")):
            raise TypeError(f"{v}, a float JSON cannot carry")
        return v
    if isinstance(v, list):
        return [_plain(x) for x in v]
    if isinstance(v, dict):
        out = {}
        for k, x in v.items():
            if not isinstance(k, str):
                raise TypeError(f"a dict with a `{type(k).__name__}` key, which has no value on a port (its keys must be strings)")
            out[k] = _plain(x)
        return out
    raise TypeError(f"a `{type(v).__name__}`, which has no value on a port (return None, bool, int, float, str, list or dict)")

def _answer(request):
    source = "def __weft_user_fn(" + ", ".join(request["names"]) + "):\n" + request["body"] + "\n"
    scope = {}
    try:
        exec(compile(source, "<script>", "exec"), scope)
    except BaseException as e:
        return {"compile": _described(e)}
    try:
        ret = scope["__weft_user_fn"](*request["args"])
    except BaseException as e:
        return {"raised": _described(e)}
    if ret is None:
        return {"type": "none"}
    if not isinstance(ret, dict):
        return {"type": "not_dict", "got": type(ret).__name__}
    out = []
    for k, v in ret.items():
        if not isinstance(k, str):
            return {"type": "key", "got": type(k).__name__}
        try:
            out.append([k, _plain(v)])
        except TypeError as e:
            return {"type": "value", "key": k, "why": str(e)}
    return {"ok": out}

for line in _requests:
    _answers.write(json.dumps(_answer(json.loads(line))) + "\n")
"#;

/// The script's answer, as its process wrote it.
#[derive(serde::Deserialize)]
#[serde(untagged)]
enum Answer {
    Ok {
        ok: Vec<(String, Value)>,
    },
    Compile {
        compile: String,
    },
    Raised {
        raised: String,
    },
    Type {
        #[serde(rename = "type")]
        kind: String,
        #[serde(default)]
        got: Option<String>,
        #[serde(default)]
        key: Option<String>,
        #[serde(default)]
        why: Option<String>,
    },
}

/// The Python processes a worker keeps for its scripts (see the module
/// doc): idle ones waiting, and room for as many as the worker has CPUs.
pub(crate) struct Interpreters {
    idle: Mutex<Vec<Interpreter>>,
    room: Arc<tokio::sync::Semaphore>,
}

/// One Python process serving [`SERVER`].
struct Interpreter {
    child: tokio::process::Child,
    stdin: tokio::process::ChildStdin,
    answers: tokio::io::Lines<tokio::io::BufReader<tokio::process::ChildStdout>>,
}

impl Interpreters {
    pub(crate) fn new() -> Self {
        let cpus = std::thread::available_parallelism().map(usize::from).unwrap_or(1);
        Self { idle: Mutex::new(Vec::new()), room: Arc::new(tokio::sync::Semaphore::new(cpus)) }
    }

    fn start() -> WeftResult<Interpreter> {
        let mut child = tokio::process::Command::new("python3")
            .args(["-u", "-c", SERVER])
            .stdin(std::process::Stdio::piped())
            .stdout(std::process::Stdio::piped())
            .stderr(std::process::Stdio::inherit())
            .kill_on_drop(true)
            .spawn()
            .map_err(|e| node_error(format!("could not start Python (`python3`): {e}")))?;
        let stdin = child.stdin.take().expect("stdin is piped");
        let stdout = child.stdout.take().expect("stdout is piped");
        Ok(Interpreter { child, stdin, answers: tokio::io::BufReader::new(stdout).lines() })
    }
}

/// Execute `code` with the given input bindings and return the raw
/// key-value pairs the script returned. Each way it can go wrong has
/// its own kind: code that does not compile is an input error, an
/// answer that is not a dict of plain values is a type error (both are
/// the program's own mistakes), and an exception the script raises while
/// it runs is a node failure, the one kind `error` catches. A cancel
/// kills the script's process.
pub(crate) async fn run_python(
    interpreters: &Interpreters,
    code: &str,
    inputs: Vec<(String, Value)>,
    cancel: &weft::CancellationFlag,
) -> WeftResult<Vec<(String, Value)>> {
    let _room = tokio::select! {
        biased;
        err = cancel.cancelled_err() => return Err(err),
        room = interpreters.room.clone().acquire_owned() => room.map_err(|_| node_error("the Python processes were closed".to_string()))?,
    };
    let mut interpreter = loop {
        let idle = interpreters.idle.lock().expect("interpreters poisoned").pop();
        match idle {
            // A process that exited since it answered (a thread of the last
            // script ended it) serves nobody: it is dropped, never handed
            // to the next script.
            Some(mut interpreter) => {
                if matches!(interpreter.child.try_wait(), Ok(None)) {
                    break interpreter;
                }
            }
            None => break Interpreters::start()?,
        }
    };
    let (names, args): (Vec<String>, Vec<Value>) = inputs.into_iter().unzip();
    let request = serde_json::json!({ "names": names, "body": indent_block(code, "    "), "args": args });
    let mut line = serde_json::to_string(&request).map_err(|e| node_error(format!("the script's inputs as JSON: {e}")))?;
    line.push('\n');
    let asked = async {
        interpreter.stdin.write_all(line.as_bytes()).await?;
        interpreter.stdin.flush().await?;
        interpreter.answers.next_line().await
    };
    let answered = tokio::select! {
        biased;
        // The process goes with the run: dropping it kills it.
        err = cancel.cancelled_err() => return Err(err),
        answered = asked => answered,
    };
    let line = match answered {
        Ok(Some(line)) => line,
        Ok(None) | Err(_) => {
            let status = interpreter.child.wait().await.map(|s| s.to_string()).unwrap_or_else(|e| e.to_string());
            return Err(node_error(format!("the script's Python process stopped before answering ({status})")));
        }
    };
    let answer: Answer = serde_json::from_str(&line)
        .map_err(|e| node_error(format!("the script's Python process answered '{}': {e}", weft::truncate_user_string(&line, 200))))?;
    // The process is well: it serves the next script.
    interpreters.idle.lock().expect("interpreters poisoned").push(interpreter);
    match answer {
        Answer::Ok { ok } => Ok(ok),
        Answer::Compile { compile } => Err(WeftError::Input(format!("the code does not compile: {compile}"))),
        Answer::Raised { raised } => Err(node_error(format!("the script raised {raised}"))),
        Answer::Type { kind, got, key, why } => Err(WeftError::Type(match kind.as_str() {
            "none" => "the script ended without returning a dict; end it with `return {...}` keyed by output port \
                       (`return {}` emits nothing)"
                .to_string(),
            "not_dict" => format!("the script returned {}; it must return a dict keyed by output port", got.unwrap_or_default()),
            "key" => format!("the script returned a dict with a {} key; its keys are output port names", got.unwrap_or_default()),
            _ => format!("the script returned '{}' as {}", key.unwrap_or_default(), why.unwrap_or_default()),
        })),
    }
}

/// Indent every line of `s` with `prefix`. Used so the user's code
/// nests correctly under `def __weft_user_fn(...):`.
fn indent_block(s: &str, prefix: &str) -> String {
    if s.is_empty() {
        return format!("{prefix}pass");
    }
    s.lines().map(|line| format!("{prefix}{line}")).collect::<Vec<_>>().join("\n")
}
