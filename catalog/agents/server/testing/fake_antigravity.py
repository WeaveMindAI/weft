"""A stand-in for `antigravity-sdk antigravity_bridge.py`: weft's bridge to Google
Antigravity's SDK, speaking the bridge's protocol (one JSON object per line).

Every line it writes is shaped as the real bridge wrote it on google-antigravity
0.1.23, driven against a stand-in of Gemini's API (no account,
`testing/gemini_standin.py`): the `start` answer and its two refusals (a
conversation the harness does not have, and one it already has), a turn's
steps (the person's message, the model's streamed message, its thinking, a
tool call's steps, a failure), the `pre_tool` ask before every tool, the
program's `lookup` and the program's `view_file` (which took the built-in's
place) through the `tool` ask, the built-in `run_command` (allowed, or refused
with a note) and `create_file` (`write_to_file` to the model), the question
of `ask_question`, the post-tool `tool_done` lines (twice for a program tool
that replaced a built-in, the second carrying the call's summary, as the
real one does), and a turn failed on a key Gemini refuses. Ids are fresh;
steps no reader looks at (the ones that only repeat a step unchanged) are
left out. What the model does is picked by keyword of the message:

  "lookup"   the program's `lookup`
  "readit"   the program's `view_file`
  "runit"    `run_command` `echo hi`
  "writeit"  `create_file` of `new.txt`
  "askit"    `ask_question`: one question, two options
  "think"    thinks, then answers
  "fail"     Gemini refuses the key
  anything else: "hello from antigravity", in three pieces

Conversations are `<saveDir>/<id>.db`, as the harness keeps them, so a new
process resumes one and a missing one is refused. `$FAKE_LOG` gets `{"argv",
"env"}` at start (`HOME` and the login variables) and every line it was sent.
"""

import json
import os
import sys
import uuid

ENV = ("HOME", "GEMINI_API_KEY", "GOOGLE_API_KEY", "GOOGLE_GENAI_USE_VERTEXAI", "GOOGLE_CLOUD_PROJECT")
STARTED = "Created At: 2026-11-05T12:48:52+01:00\nCompleted At: 2026-11-05T12:48:52+01:00\n"
KEY_REFUSED = ('Agent execution terminated due to error. ("agent executor error: Error 400, Message: API key not valid. Please pass '
               'a valid API key., Status: INVALID_ARGUMENT, Details: []: Error 400, Message: API key not valid. Please pass a valid '
               'API key., Status: INVALID_ARGUMENT, Details: []")')

state = {"conversation": None, "index": 0, "asks": 0, "folder": None}


def out(message):
    sys.stdout.write(json.dumps(message) + "\n")
    sys.stdout.flush()


def logged(entry):
    with open(os.environ["FAKE_LOG"], "a") as f:
        f.write(json.dumps(entry) + "\n")


def read():
    line = sys.stdin.readline()
    if not line:
        sys.exit(0)
    message = json.loads(line)
    logged(message)
    return message


def ask(kind, request):
    """Ask the adapter, and read until its answer comes."""
    state["asks"] += 1
    ask_id = f"ask-{state['asks']}"
    out({"type": "ask", "ask": ask_id, "kind": kind, "request": request})
    while True:
        message = read()
        if message.get("type") == "answer" and message.get("ask") == ask_id:
            return message["response"]
        raise RuntimeError(f"the fake waits on {ask_id} and was sent {message}")


def step(kind, source, target, status, content="", delta="", thinking="", thinking_delta="", tool_calls=(), error="",
         step_id=None, index=None):
    conversation = state["conversation"]
    index = state["index"] if index is None else index
    out({"type": "step", "step": {
        "id": step_id or f"{conversation}:{index}", "step_index": index, "trajectory_id": conversation, "parent_trajectory_id": "",
        "depth": 0, "type": kind, "source": source, "target": target, "status": status, "content": content, "content_delta": delta,
        "thinking": thinking, "thinking_delta": thinking_delta, "tool_calls": list(tool_calls), "error": error,
        "is_complete_response": source == "MODEL" and status == "DONE" and bool(content) and target == "TARGET_USER", "http_code": 0}})


def next_step():
    state["index"] += 1


def say(text):
    """The model's message, written whole."""
    step("TEXT_RESPONSE", "MODEL", "TARGET_USER", "ACTIVE", text, text)
    step("TEXT_RESPONSE", "MODEL", "TARGET_USER", "DONE", text)
    next_step()


def pre_tool(name, args):
    call_id = str(uuid.uuid4())
    answer = ask("pre_tool", {"id": call_id, "stepId": f"{state['conversation']}:{state['index'] + 1}", "name": name, "args": args})
    return call_id, answer


def refused(answer):
    """The model's message after a tool the hook refused."""
    why = answer.get("message") or ""
    step("TEXT_RESPONSE", "MODEL", "TARGET_USER", "ERROR", f'{why} ("denied by pre-tool hook: {why}")', error=f'{why} ("denied by pre-tool hook: {why}")')
    next_step()
    say(f'tool said: {json.dumps({"error": "Created At: 2026-11-05T12:58:03+01:00" + chr(10) + "Encountered error in step execution: denied by pre-tool hook: " + why})}')


def program_tool(name, args, replaced):
    call_id, answer = pre_tool(name, args)
    step("TOOL_CALL", "MODEL", "TARGET_ENVIRONMENT", "ACTIVE", "a step", "a step")
    step("TOOL_CALL", "MODEL", "TARGET_ENVIRONMENT", "ACTIVE", step_id=call_id, index=1,
         tool_calls=[{"name": name, "args": args, "id": call_id}])
    result = ask("tool", {"id": call_id, "name": name, "args": args})
    output = json.dumps({"result": result["output"]}) if not result.get("isError") else None
    stepped = f"{state['conversation']}:{state['index'] + 1}"
    out({"type": "tool_done", "result": {"id": call_id, "stepId": stepped, "name": name, "result": output,
                                         "error": None if output is not None else result["output"]}})
    if replaced:
        out({"type": "tool_done", "result": {"id": call_id, "stepId": stepped, "name": name, "result": "a step", "error": None}})
    step("TOOL_CALL", "MODEL", "TARGET_ENVIRONMENT", "DONE", "a step")
    next_step()
    body = f"{STARTED}Tool response for {name}:\n```json\n{output}\n```" if output is not None else result["output"]
    say(f'tool said: {json.dumps({"output" if output is not None else "error": body})}')


def built_in(name, args, step_tool, done_args, result):
    call_id, answer = pre_tool(name, args)
    if not answer.get("allow"):
        refused(answer)
        return
    step_id = f"{state['conversation']}:{state['index']}"
    running, done = done_args
    step("TOOL_CALL", "MODEL", "TARGET_ENVIRONMENT", "ACTIVE", "a step", "a step",
         tool_calls=[{"name": step_tool, "args": running, "id": step_id, "step_id": step_id}])
    step("TOOL_CALL", "MODEL", "TARGET_ENVIRONMENT", "DONE", "a step",
         tool_calls=[{"name": step_tool, "args": done, "id": step_id, "step_id": step_id}])
    out({"type": "tool_done", "result": {"id": call_id, "stepId": f"{state['conversation']}:{state['index'] + 1}", "name": name,
                                         "result": result, "error": None}})
    next_step()
    say(f'tool said: {json.dumps({"output": STARTED + "done"})}')


def question():
    call_id, answer = pre_tool("ask_question", {"questions": [{"is_multi_select": False, "options": ["red", "blue"], "question": "Which color?"}]})
    step("TEXT_RESPONSE", "MODEL", "TARGET_ENVIRONMENT", "ACTIVE", "a step", "a step")
    answer = ask("question", {"questions": [{"question": "Which color?", "options": [{"id": "1", "text": "red"}, {"id": "2", "text": "blue"}],
                                             "isMultiSelect": False}]})
    texts = {"1": "red", "2": "blue"}
    responses = answer.get("responses") or []
    said = ", ".join([texts[i] for r in responses for i in r.get("selectedOptionIds") or []] +
                     [r["freeformResponse"] for r in responses if r.get("freeformResponse")])
    # The harness's own result of its question tool (seen against Gemini's
    # real API, gemini-3.1-flash-lite): the answer reaches the model in a step.
    out({"type": "tool_done", "result": {"id": call_id, "stepId": f"{state['conversation']}:{state['index'] + 1}", "name": "ask_question",
                                         "result": "Question to user", "error": None}})
    next_step()
    say(f'tool said: {json.dumps({"output": STARTED + "A1: " + said})}')


def turn(rid, message):
    step("TEXT_RESPONSE", "USER", "UNKNOWN", "DONE", message, message)
    next_step()
    out({"id": rid, "result": {}})
    folder = state["folder"]
    if "fail" in message:
        step("TEXT_RESPONSE", "SYSTEM", "TARGET_USER", "ERROR", KEY_REFUSED, error=KEY_REFUSED)
        out({"type": "ended", "error": KEY_REFUSED})
        return
    if "lookup" in message:
        program_tool("lookup", {"word": "x"}, False)
    elif "readit" in message:
        program_tool("view_file", {"AbsolutePath": os.path.join(folder, "a.txt")}, True)
    elif "runit" in message:
        built_in("run_command", {"CommandLine": "echo hi", "Cwd": folder, "WaitMsBeforeAsync": 5000}, "run_command",
                 ({"command_line": "echo hi", "working_dir": folder},
                  {"command_line": "echo hi", "working_dir": folder, "exit_code": 0, "combined_output": "hi\r\n"}), "hi\r\n")
    elif "writeit" in message:
        target = os.path.join(folder, "new.txt")
        built_in("create_file", {"CodeContent": "new\n", "Description": "a new file", "Overwrite": False, "TargetFile": target},
                 "edit_file", ({"file_path": target}, {"file_path": target}), "a step")
    elif "askit" in message:
        question()
    elif "think" in message:
        step("THINKING", "MODEL", "TARGET_USER", "ACTIVE", thinking="let me think", thinking_delta="let me think")
        step("TEXT_RESPONSE", "MODEL", "TARGET_USER", "ACTIVE", "thought and answered", "thought and answered", thinking="let me think")
        step("TEXT_RESPONSE", "MODEL", "TARGET_USER", "DONE", "thought and answered", thinking="let me think")
        next_step()
    else:
        for text, delta in (("hello", "hello"), ("hello from", " from"), ("hello from antigravity", " antigravity")):
            step("TEXT_RESPONSE", "MODEL", "TARGET_USER", "ACTIVE", text, delta)
        step("TEXT_RESPONSE", "MODEL", "TARGET_USER", "DONE", "hello from antigravity")
        next_step()
    out({"type": "ended", "error": None})


def start(m):
    path = os.path.join(m["saveDir"], f"{m['conversation']}.db")
    if m["resume"] and not os.path.exists(path):
        raise RuntimeError(f'RuntimeError: Failed to initialize conversation at ws://localhost:58526/. Stderr: Failed to create agent: '
                           f'conversation "{m["conversation"]}" not found in "{m["saveDir"]}" (cannot resume)\n')
    if not m["resume"] and os.path.exists(path):
        raise RuntimeError(f'RuntimeError: Failed to initialize conversation at ws://localhost:58052/. Stderr: Failed to create agent: '
                           f'conversation "{m["conversation"]}" already exists in "{m["saveDir"]}"\n')
    os.makedirs(m["saveDir"], exist_ok=True)
    index = 0
    if m["resume"]:
        with open(path) as f:
            index = json.load(f)["index"]
    state.update(conversation=m["conversation"], index=index, folder=m["workDir"], path=path)
    with open(path, "w") as f:
        json.dump({"index": index}, f)
    return {"conversation": m["conversation"]}


def handle(m):
    try:
        if m["type"] == "start":
            out({"id": m["id"], "result": start(m)})
        elif m["type"] == "prompt":
            turn(m["id"], m["text"])
            with open(state["path"], "w") as f:
                json.dump({"index": state["index"]}, f)
        else:
            raise RuntimeError(f"RuntimeError: the bridge has no request {m['type']!r}")
    except RuntimeError as e:
        out({"id": m["id"], "error": {"message": str(e)}})


logged({"argv": sys.argv[1:], "env": {k: os.environ[k] for k in ENV if k in os.environ}})
while True:
    handle(read())
