//! ExecPython: run a user-supplied Python snippet inside the
//! worker using an embedded CPython interpreter (PyO3).
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
//! - Inputs are converted serde_json → Python via `json_to_py`. A
//!   recursive walk over the JSON tree keeps types straightforward:
//!   strings to `str`, numbers to `int`/`float`, nulls to `None`,
//!   arrays to `list`, objects to `dict`. No custom classes leak
//!   across the boundary; we round-trip through JSON twice per
//!   call but the shape is simple and predictable.
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
//! - A cancelled run stops its script. Python offers one way to stop
//!   code running on another thread: `PyThreadState_SetAsyncExc`
//!   raises an exception in that thread at its next bytecode. The
//!   script's thread records its id in an [`Interrupt`] while it runs,
//!   and the guard the node body holds raises `KeyboardInterrupt` there
//!   when the body ends early (the run was cancelled, or the engine
//!   dropped the body). A script inside one long C call (a `time.sleep`,
//!   a blocking socket read) stops when that call returns, the earliest
//!   moment Python checks. `KeyboardInterrupt` is outside `Exception`,
//!   so a script's `except Exception` does not swallow it.
//!
//! Isolation: the worker the node runs in IS the isolation boundary;
//! the Python executes there with the same access that worker already
//! has, and is not sandboxed further. Running a project therefore runs
//! its ExecPython code with that worker's privileges, the same trust
//! model as running the project's own program.

use async_trait::async_trait;
use pyo3::prelude::*;
use pyo3::types::{PyBool, PyDict, PyFloat, PyList, PyString};
use pyo3::ToPyObject;
use serde_json::{Map, Number, Value};
use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;

use weft::node::NodeOutput;
use weft::storage::media::{media_slots, substitute_media};
use weft::weft_type::FileKind;
use weft::context::ERROR_PORT;
use weft::{node_error, ExecutionContext, Node, NodeErrExt, NodeManifest, StoredFile, WeftError, WeftResult, WeftType};

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

        // PyO3 needs the GIL which it acquires on whatever sync
        // thread we call from. Hop off the async executor for the
        // blocking call so we don't stall other node invocations.
        // `_stop` interrupts the script when this body ends before it
        // does (see the module doc).
        let interrupt = Arc::new(Interrupt::default());
        let _stop = StopOnDrop(interrupt.clone());
        let task = tokio::task::spawn_blocking(move || run_python(&code, inputs, &interrupt));
        let cancel = ctx.cancellation();
        let result = tokio::select! {
            err = cancel.cancelled_err() => return Err(err),
            joined = task => joined.node_err("ExecPython blocking task panicked")??,
        };

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

/// Execute `code` with the given input bindings and return the raw
/// key-value pairs the script returned. Each way it can go wrong has
/// its own kind: code that does not compile is an input error, an
/// answer that is not a dict of plain values is a type error (both are
/// the program's own mistakes), and an exception the script raises while
/// it runs is a node failure, the one kind `error` catches.
fn run_python(code: &str, inputs: Vec<(String, Value)>, interrupt: &Interrupt) -> WeftResult<Vec<(String, Value)>> {
    Python::with_gil(|py| -> WeftResult<Vec<(String, Value)>> {
        // Build the wrapper source once per call. Wrapping in a
        // function lets the user write `return {...}` naturally.
        // The signature lists every input port so Python's scope
        // rules do the right thing (closures, shadowing, etc).
        let param_names: Vec<String> =
            inputs.iter().map(|(k, _)| k.clone()).collect();
        let wrapper_source = format!(
            "def __weft_user_fn({params}):\n{body}\n",
            params = param_names.join(", "),
            body = indent_block(code, "    "),
        );

        // Running the wrapper only defines the function, so what fails
        // here is the code not compiling (a syntax or indentation error).
        let globals = PyDict::new_bound(py);
        py.run_bound(&wrapper_source, Some(&globals), None)
            .map_err(|err| WeftError::Input(format!("the code does not compile: {}", python_error(py, &err))))?;
        let user_fn = globals
            .get_item("__weft_user_fn")
            .map_err(|err| node_error(format!("locating __weft_user_fn: {}", python_error(py, &err))))?
            .node_err("internal: ExecPython wrapper did not define __weft_user_fn")?;

        // Convert each input into a Python value and call the
        // wrapper as a positional-arg tuple matching the signature.
        let args = PyList::empty_bound(py);
        for (_, v) in &inputs {
            let py_val = json_to_py(py, v)
                .map_err(|err| node_error(format!("converting an input to Python: {}", python_error(py, &err))))?;
            args.append(py_val)
                .map_err(|err| node_error(format!("building the argument list: {}", python_error(py, &err))))?;
        }
        let ret = interrupt
            .run(py, || user_fn.call1(args.to_tuple()))?
            .map_err(|err| node_error(format!("the script raised {}", python_error(py, &err))))?;

        // Falling off the end, a bare `return` and `return None` are one
        // thing to Python, and none of them says which ports get what.
        if ret.is_none() {
            return Err(WeftError::Type(
                "the script ended without returning a dict; end it with `return {...}` keyed by \
                 output port (`return {}` emits nothing)"
                    .into(),
            ));
        }

        let dict = ret.downcast::<PyDict>().map_err(|_| {
            WeftError::Type(format!(
                "the script returned {}; it must return a dict keyed by output port",
                python_type_name(&ret)
            ))
        })?;

        let mut out: Vec<(String, Value)> = Vec::new();
        for (k, v) in dict.iter() {
            let key: String = k.extract().map_err(|_| {
                WeftError::Type(format!(
                    "the script returned a dict with a {} key; its keys are output port names",
                    python_type_name(&k)
                ))
            })?;
            let json_val = py_to_json(py, &v).map_err(|err| {
                WeftError::Type(format!("the script returned '{key}' as {}", err.value_bound(py)))
            })?;
            out.push((key, json_val));
        }
        Ok(out)
    })
}

/// The thread a script runs on, for stopping it from another thread
/// (see the module doc). `thread` is the Python thread id while the
/// script runs and 0 otherwise; it is only written with the GIL held,
/// so a reader holding the GIL sees the script either running or done.
#[derive(Default)]
struct Interrupt {
    thread: AtomicU64,
    stopped: AtomicBool,
}

impl Interrupt {
    /// Run the script's call on this thread, stoppable by [`Self::stop_now`].
    /// `Err(Cancelled)` when the stop came first or interrupted it.
    fn run<T>(&self, py: Python<'_>, call: impl FnOnce() -> PyResult<T>) -> WeftResult<PyResult<T>> {
        let ident: u64 = py
            .import_bound("threading")
            .and_then(|threading| threading.call_method0("get_ident"))
            .and_then(|ident| ident.extract())
            .map_err(|err| node_error(format!("reading the script's thread id: {}", python_error(py, &err))))?;
        // Written before `stopped` is read (see `StopOnDrop`).
        self.thread.store(ident, Ordering::SeqCst);
        if self.stopped.load(Ordering::SeqCst) {
            self.thread.store(0, Ordering::SeqCst);
            return Err(WeftError::Cancelled);
        }
        let out = call();
        self.thread.store(0, Ordering::SeqCst);
        // A stop that landed after the script's last bytecode is still
        // pending on this thread's state, and the next script this
        // pooled thread runs would raise it: clear it.
        // SAFETY: the GIL is held (`py`); a null exception clears.
        unsafe { pyo3::ffi::PyThreadState_SetAsyncExc(ident as std::os::raw::c_long, std::ptr::null_mut()) };
        if self.stopped.load(Ordering::SeqCst) {
            return Err(WeftError::Cancelled);
        }
        Ok(out)
    }

    /// Stop the script: one that has not started never starts, one that
    /// runs raises `KeyboardInterrupt` at its next bytecode. Takes the
    /// GIL, so it waits for the script's thread to hand it over (Python
    /// does every few milliseconds): never call it on the async executor.
    fn stop_now(&self) {
        self.stopped.store(true, Ordering::SeqCst);
        Python::with_gil(|_py| {
            let ident = self.thread.load(Ordering::SeqCst);
            if ident != 0 {
                // SAFETY: the GIL is held, and `PyExc_KeyboardInterrupt`
                // is a static the interpreter owns. The id is Python's
                // unsigned thread id, which this binding takes as signed.
                unsafe {
                    pyo3::ffi::PyThreadState_SetAsyncExc(
                        ident as std::os::raw::c_long,
                        pyo3::ffi::PyExc_KeyboardInterrupt,
                    )
                };
            }
        });
    }
}

/// Stops the script when the node body ends, whichever way it ends: a
/// script that already finished is untouched.
struct StopOnDrop(Arc<Interrupt>);

impl Drop for StopOnDrop {
    fn drop(&mut self) {
        // Written before `thread` is read, and the script's thread writes
        // `thread` before reading this: whichever runs second sees the
        // other, so a script that has not started yet never starts.
        self.0.stopped.store(true, Ordering::SeqCst);
        if self.0.thread.load(Ordering::SeqCst) == 0 {
            return;
        }
        let interrupt = self.0.clone();
        tokio::task::spawn_blocking(move || interrupt.stop_now());
    }
}

/// A Python object's type name, for a message.
fn python_type_name(obj: &Bound<'_, PyAny>) -> String {
    obj.get_type().name().map(|n| n.to_string()).unwrap_or_else(|_| "<unknown>".to_string())
}

/// Indent every line of `s` with `prefix`. Used so the user's code
/// nests correctly under `def __weft_user_fn(...):`.
fn indent_block(s: &str, prefix: &str) -> String {
    if s.is_empty() {
        return format!("{prefix}pass");
    }
    s.lines()
        .map(|line| format!("{prefix}{line}"))
        .collect::<Vec<_>>()
        .join("\n")
}

/// A Python exception as a person debugging their script needs it: the
/// exception's type and message, then the traceback with line numbers
/// when there is one.
fn python_error(py: Python<'_>, err: &PyErr) -> String {
    let traceback = err
        .traceback_bound(py)
        .and_then(|tb| tb.format().ok())
        .unwrap_or_default();
    let kind = err.get_type_bound(py).name().map(|n| n.to_string()).unwrap_or_else(|_| "Exception".to_string());
    let summary = format!("{kind}: {}", err.value_bound(py));
    if traceback.trim().is_empty() {
        summary
    } else {
        format!("{summary}\n{}", traceback.trim_end())
    }
}

/// Convert a serde_json Value to a Python object. Types:
/// - Null → None
/// - Bool → bool
/// - Number → int if it's an exact int, else float
/// - String → str
/// - Array → list of converted items
/// - Object → dict of (String key → converted value)
fn json_to_py<'py>(py: Python<'py>, v: &Value) -> PyResult<Bound<'py, PyAny>> {
    match v {
        Value::Null => Ok(py.None().into_bound(py)),
        Value::Bool(b) => Ok(PyBool::new_bound(py, *b).to_owned().into_any()),
        Value::Number(n) => {
            if let Some(i) = n.as_i64() {
                Ok(i.to_object(py).into_bound(py))
            } else if let Some(u) = n.as_u64() {
                Ok(u.to_object(py).into_bound(py))
            } else if let Some(f) = n.as_f64() {
                Ok(PyFloat::new_bound(py, f).into_any())
            } else {
                // serde_json's Number should always be one of the
                // above. Kept for completeness.
                Ok(py.None().into_bound(py))
            }
        }
        Value::String(s) => Ok(PyString::new_bound(py, s.as_str()).into_any()),
        Value::Array(items) => {
            let list = PyList::empty_bound(py);
            for item in items {
                list.append(json_to_py(py, item)?)?;
            }
            Ok(list.into_any())
        }
        Value::Object(obj) => {
            let dict = PyDict::new_bound(py);
            for (k, val) in obj {
                dict.set_item(k, json_to_py(py, val)?)?;
            }
            Ok(dict.into_any())
        }
    }
}

/// Convert a Python object back to serde_json. Supported: None, bool,
/// int, float, str, list, dict. Anything else (sets, custom classes,
/// bytes, tuples) raises `PyTypeError`: a downstream port expecting a
/// Dict surfaces a structured failure instead of receiving a stringified
/// `repr()` that pretends to be data.
fn py_to_json(py: Python<'_>, obj: &Bound<'_, PyAny>) -> PyResult<Value> {
    if obj.is_none() {
        return Ok(Value::Null);
    }
    if let Ok(b) = obj.extract::<bool>() {
        return Ok(Value::Bool(b));
    }
    if let Ok(i) = obj.extract::<i64>() {
        return Ok(Value::Number(i.into()));
    }
    if let Ok(u) = obj.extract::<u64>() {
        return Ok(Value::Number(u.into()));
    }
    if let Ok(f) = obj.extract::<f64>() {
        return Number::from_f64(f)
            .map(Value::Number)
            .ok_or_else(|| pyo3::exceptions::PyValueError::new_err(format!("{f}, a float JSON cannot carry")));
    }
    if let Ok(s) = obj.extract::<String>() {
        return Ok(Value::String(s));
    }
    if let Ok(list) = obj.downcast::<PyList>() {
        let mut out = Vec::with_capacity(list.len());
        for item in list.iter() {
            out.push(py_to_json(py, &item)?);
        }
        return Ok(Value::Array(out));
    }
    if let Ok(dict) = obj.downcast::<PyDict>() {
        let mut map = Map::new();
        for (k, v) in dict.iter() {
            let key: String = k.extract()?;
            map.insert(key, py_to_json(py, &v)?);
        }
        return Ok(Value::Object(map));
    }
    // Unsupported Python type: refuse to silently downgrade to a
    // stringified `repr()`. A user wiring a downstream port expecting
    // a Dict gets a structured failure instead of a `"<set {...}>"`
    // string that pretends to be data.
    let type_name = python_type_name(obj);
    Err(pyo3::exceptions::PyTypeError::new_err(format!(
        "a `{type_name}`, which has no value on a port (return None, bool, int, float, str, list or dict)"
    )))
}
