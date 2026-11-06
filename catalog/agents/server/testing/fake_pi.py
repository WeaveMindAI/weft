"""A stand-in for `pi --mode rpc`: Pi's own protocol, one JSON object per
line, records tagged by `type`.

Every line it writes is a line real Pi 1.5.0 wrote, driven by this server's
adapter and weft's extension (`pi_weft.ts`), first against a fake model and
then against a real one (OpenRouter's `google/gemini-2.5-flash-lite`: a
plain reply, the program's `lookup` and a `read` of the program's in the
built-in's place, a `bash` allowed once), kept field for field with fresh
ids and only the fields that are noise to every reader trimmed (token
usage, timestamps, durations, the system prompt's text): the
`get_state` and `prompt` responses, a run's events (`agent_start` ...
`agent_settled`, the `message_update`s of streamed text and tool calls), a
permission dialog and its refusal, the ask tool's dialog, a tool of the
extension's (the program's, under its plain name), a model call refused 401, and the warning Pi prints when
`--session-id` names no session of this folder. What a model does is picked
by keyword of the turn's message:

  "run"      `bash`: under `ask` and `accept_edits` the extension asks first
             (a `select` dialog), as it does for a command
  "write"    `write` of `a.txt` in the folder: asks under `ask` only
  "ask"      the ask tool: one `input` dialog; "two questions" makes the
             model call it twice in one message, so two dialogs are open at
             once (Pi runs one message's tool calls together)
  "tool"     a tool named by the next word (`lookup` when none): one of the
             program's, which weft's extension registers (the tools its
             loader, the `-e` file, hands it), is forwarded to the route
             as the extension forwards it, one `tools/call` and no
             deadline; any other name is Pi's own built-in
  "fail"     the model call is refused 401
  "think"    the answer comes with its reasoning

The permission rules themselves are the extension's (tested on the real file
by `test_pi.py`); this stand-in asks where the extension would for these
tools in the folder, and remembers "from now on" in the session, as the
extension's session entry does. Sessions are kept per folder under the
home, so a new process resumes them and a folder of its own starts empty.

`$FAKE_LOG` gets `{"argv": ..., "env": ...}` at start, every line it was
sent, and every call it forwarded to the route (`{"route": name}`).
"""

import json
import os
import sys
import urllib.request
import uuid

HOME = os.environ["PI_CODING_AGENT_DIR"]
CWD = os.getcwd()
ARGV = sys.argv[1:]


def flag(name, default=None):
    return ARGV[ARGV.index(name) + 1] if name in ARGV else default


PERMISSION = flag("--weft-permission", "ask")
TOOLS = flag("--tools")
OFFERED = TOOLS.split(",") if TOOLS else ["read", "bash", "edit", "write", "ask_user"]
SESSION_ID = flag("--session-id")
SESSIONS = os.path.join(HOME, "sessions", "--" + CWD.strip("/").replace("/", "-") + "--")
SESSION_FILE = os.path.join(SESSIONS, f"{SESSION_ID}.json")
MODEL = {"id": flag("--model"), "name": flag("--model"), "api": "openai-completions", "provider": flag("--provider"),
         "reasoning": False, "input": ["text"], "contextWindow": 128000, "maxTokens": 16384}


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


def load_session():
    try:
        with open(SESSION_FILE) as f:
            return json.load(f)
    except FileNotFoundError:
        sys.stderr.write(f"Warning: No project session found with id '{SESSION_ID}'; creating a new session with that id.\n")
        sys.stderr.flush()
        return {"messages": [], "allowed": []}


SESSION = load_session()


def save_session():
    os.makedirs(SESSIONS, exist_ok=True)
    with open(SESSION_FILE, "w") as f:
        json.dump(SESSION, f)


def respond(rid, command, data=None, error=None):
    if error is not None:
        out({"id": rid, "type": "response", "command": command, "success": False, "error": error})
    else:
        out({"id": rid, "type": "response", "command": command, "success": True, "data": data or {}})


def assistant(content, stop="stop", error=None):
    message = {"role": "assistant", "content": content, "api": "openai-completions", "provider": MODEL["provider"],
               "model": MODEL["id"], "stopReason": stop}
    if error is not None:
        message["errorMessage"] = error
    return message


def say(text, thinking=None):
    content = []
    if thinking:
        content.append({"type": "thinking", "thinking": thinking})
        out({"type": "message_update", "assistantMessageEvent": {"type": "thinking_delta", "contentIndex": 0, "delta": thinking}})
    out({"type": "message_start", "message": assistant([], stop="pending")})
    out({"type": "message_update", "assistantMessageEvent": {"type": "text_start", "contentIndex": len(content)}})
    for piece in (text[:5], text[5:]):
        if piece:
            out({"type": "message_update", "assistantMessageEvent": {"type": "text_delta", "contentIndex": len(content), "delta": piece}})
    out({"type": "message_update", "assistantMessageEvent": {"type": "text_end", "contentIndex": len(content), "content": text}})
    content.append({"type": "text", "text": text})
    message = assistant(content)
    out({"type": "message_end", "message": message})
    out({"type": "turn_end", "message": message, "toolResults": []})
    SESSION["messages"].append(message)


def call_tools(calls):
    """One assistant message calling `calls` [(name, args)]: it, and their ids."""
    content = [{"type": "toolCall", "id": "call_" + uuid.uuid4().hex[:8], "name": name, "arguments": args} for name, args in calls]
    out({"type": "message_start", "message": assistant(content, stop="pending")})
    for i, part in enumerate(content):
        out({"type": "message_update", "assistantMessageEvent": {"type": "toolcall_start", "contentIndex": i, "id": part["id"], "toolName": part["name"]}})
        out({"type": "message_update", "assistantMessageEvent": {"type": "toolcall_delta", "contentIndex": i, "delta": json.dumps(part["arguments"], separators=(",", ":"))}})
        out({"type": "message_update", "assistantMessageEvent": {"type": "toolcall_end", "contentIndex": i, "toolCall": part}})
    message = assistant(content, stop="toolUse")
    out({"type": "message_end", "message": message})
    SESSION["messages"].append(message)
    save_session()
    for part in content:
        out({"type": "tool_execution_start", "toolCallId": part["id"], "toolName": part["name"], "args": part["arguments"]})
    return message, [part["id"] for part in content]


def tool_end(call_id, name, args, text, is_error=False, details=None, structured=None):
    """A tool call's end and its result message; real `bash` gives back
    `structuredContent` where an extension's tool gives back `details`."""
    result = {"content": [{"type": "text", "text": text}]}
    result.update({"structuredContent": structured} if structured is not None else {"details": details or {}})
    out({"type": "tool_execution_end", "toolCallId": call_id, "toolName": name, "result": result, "isError": is_error})
    message = {"role": "toolResult", "toolCallId": call_id, "toolName": name, "content": result["content"], "isError": is_error}
    out({"type": "message_start", "message": message})
    out({"type": "message_end", "message": message})
    SESSION["messages"].append(message)
    return message


def next_turn(message, results):
    """The end of a model call that used tools, and the start of the one reading their results."""
    out({"type": "turn_end", "message": message, "toolResults": results})
    out({"type": "turn_start"})


def dialog(method, asked, **more):
    """Ask the client, as the extension does through `ctx.ui`, and wait for its answer."""
    rid = str(uuid.uuid4())
    request = {"type": "extension_ui_request", "id": rid, "method": method, "title": json.dumps(asked, separators=(",", ":")), **more}
    out(request)
    return rid


def answer_of(rid):
    """Wait for the client's answer to dialog `rid` (Pi takes no command while a run waits on one)."""
    while True:
        message = read()
        if message.get("type") == "extension_ui_response" and message.get("id") == rid:
            return message


def gate(call_id, tool, args):
    """The extension's `tool_call` handler: None to run the tool, else the reason it is blocked."""
    if PERMISSION in ("bypass", "read_only") or tool in SESSION["allowed"]:
        return None
    if tool == "write" and PERMISSION == "accept_edits":
        return None
    asked = {"weft": "permission", "toolCallId": call_id, "tool": tool, "input": args}
    if tool == "write":
        asked["path"] = os.path.join(CWD, args["path"])
    answer = answer_of(dialog("select", asked, options=["allow_once", "allow_always", "reject"]))
    if answer.get("cancelled"):
        return "No answer came: the turn ended before the person answered."
    decided = json.loads(answer["value"])
    if decided["choice"] == "allow_always":
        SESSION["allowed"].append(tool)
        out({"type": "entry_appended", "entry": {"type": "custom", "customType": "weft-allowed", "data": {"tool": tool},
                                                 "id": uuid.uuid4().hex[:8]}})
    if decided["choice"] in ("allow_once", "allow_always"):
        return None
    note = decided.get("note", "").strip()
    return f"The person refused this {tool} call" + (f": {note}" if note else ".")


def extension_tools():
    """What weft's extension was made from: the loader `-e` names ends in `make({...});`."""
    with open(flag("-e"), encoding="utf-8") as f:
        text = f.read()
    return json.loads(text[text.index("make(") + len("make("):text.rindex(");")])


def program_tool(name, arguments):
    """Call the program's tool `name` as weft's extension does: one
    `tools/call` to the route, with no deadline."""
    listed = extension_tools()
    logged({"route": name})
    body = {"jsonrpc": "2.0", "id": "weft-1", "method": "tools/call", "params": {"name": name, "arguments": arguments}}
    request = urllib.request.Request(listed["url"], data=json.dumps(body).encode(), method="POST",
                                     headers={"Content-Type": "application/json", "Accept": "application/json"})
    with urllib.request.urlopen(request, timeout=None) as answer:
        return json.loads(answer.read())["result"]


def run(text):
    out({"type": "agent_start"})
    out({"type": "turn_start"})
    # Every run starts with the system prompt, as a message of its own:
    # all of it the first time, then only what changed (here, trimmed).
    system = {"role": "system", "content": "", "sections": {}, "toolsAdded": OFFERED}
    out({"type": "message_start", "message": system})
    out({"type": "message_end", "message": system})
    user = {"role": "user", "content": [{"type": "text", "text": text}]}
    out({"type": "message_start", "message": user})
    out({"type": "message_end", "message": user})
    SESSION["messages"].append(user)
    # Pi writes each message to the session as it comes.
    save_session()
    reply = "done"
    if "fail" in text:
        error = '401: {"message":"Incorrect API key provided: k-wrong.","type":"invalid_request_error","code":"invalid_api_key"}'
        message = assistant([], stop="error", error=error)
        out({"type": "message_start", "message": message})
        out({"type": "message_end", "message": message})
        out({"type": "turn_end", "message": message, "toolResults": []})
        out({"type": "agent_end", "messages": []})
        out({"type": "agent_settled", "aborted": False})
        save_session()
        return
    if "run" in text and "bash" in OFFERED:
        args = {"command": "echo hello-from-fake"}
        called, (call_id,) = call_tools([("bash", args)])
        blocked = gate(call_id, "bash", args)
        if blocked:
            results = [tool_end(call_id, "bash", args, blocked, is_error=True)]
            reply = f"refused: {blocked}"
        else:
            out({"type": "tool_execution_update", "toolCallId": call_id, "toolName": "bash", "args": args, "partialResult": {"content": []}})
            out({"type": "tool_execution_update", "toolCallId": call_id, "toolName": "bash", "args": args,
                 "partialResult": {"content": [{"type": "text", "text": "hello-from-fake\n"}], "details": {}}})
            results = [tool_end(call_id, "bash", args, "hello-from-fake\n", structured={
                "output": "hello-from-fake\n", "truncated": False, "exit_code": 0, "wall_time_seconds": 0})]
            reply = "ran: hello-from-fake"
        next_turn(called, results)
    elif "write" in text and "write" in OFFERED:
        args = {"path": "a.txt", "content": "hi\n"}
        called, (call_id,) = call_tools([("write", args)])
        blocked = gate(call_id, "write", args)
        if blocked:
            results = [tool_end(call_id, "write", args, blocked, is_error=True)]
        else:
            with open(os.path.join(CWD, "a.txt"), "w") as f:
                f.write(args["content"])
            results = [tool_end(call_id, "write", args, "Successfully wrote to a.txt")]
        next_turn(called, results)
    elif "ask" in text or "two questions" in text:
        asked = [("ask_user", {"question": "Which colour?", "options": ["red", "blue"]})]
        if "two questions" in text:
            asked.append(("ask_user", {"question": "Which size?", "options": ["S", "L"], "multiSelect": True}))
        called, ids = call_tools(asked)
        # One message's tool calls run together: every dialog is open before any answer.
        rids = [dialog("input", {"weft": "question", "toolCallId": call_id, "question": args["question"], "options": args["options"],
                                 "multiSelect": bool(args.get("multiSelect"))}, placeholder="")
                for call_id, (_, args) in zip(ids, asked)]
        answers = {}
        while len(answers) < len(rids):
            message = read()
            if message.get("type") == "extension_ui_response" and message.get("id") in rids:
                answers[message["id"]] = message.get("value", "")
        results = [tool_end(call_id, "ask_user", {}, answers[rid], details={"answer": answers[rid]}) for call_id, rid in zip(ids, rids)]
        reply = " / ".join(answers[rid] for rid in rids)
        next_turn(called, results)
    elif "tool" in text:
        words = text.split()
        name = words[words.index("tool") + 1] if words.index("tool") + 1 < len(words) else "lookup"
        called, (call_id,) = call_tools([(name, {})])
        if name in [t["name"] for t in extension_tools()["tools"]]:
            result = program_tool(name, {})
            output = result["content"][0]["text"]
            results = [tool_end(call_id, name, {}, output, is_error=result.get("isError", False))]
        else:
            output = f"built-in {name}"
            results = [tool_end(call_id, name, {}, output)]
        reply = f"looked up: {output}"
        next_turn(called, results)
    say(reply if "think" not in text else "thought", thinking="Let me think." if "think" in text else None)
    out({"type": "agent_end", "messages": []})
    out({"type": "agent_settled", "aborted": False})
    save_session()


def state():
    return {"model": MODEL, "thinkingLevel": flag("--thinking", "off"), "isStreaming": False, "isCompacting": False,
            "steeringMode": "one-at-a-time", "followUpMode": "one-at-a-time", "sessionFile": SESSION_FILE,
            "sessionId": SESSION_ID, "autoCompactionEnabled": True, "messageCount": len(SESSION["messages"]), "pendingMessageCount": 0}


def main():
    logged({"argv": ARGV, "env": {k: v for k, v in os.environ.items() if k.startswith("PI_") or k.endswith("_API_KEY")}})
    while True:
        message = read()
        kind, rid = message.get("type"), message.get("id")
        if kind == "get_state":
            respond(rid, "get_state", state())
        elif kind == "prompt":
            respond(rid, "prompt", {"disposition": "started"})
            run(message["message"])
        else:
            respond(rid, kind, error=f"Unknown command: {kind}")


main()
