"""A stand-in for `node cline_bridge.mjs <the SDK>`: weft's bridge to Cline's
SDK, speaking the bridge's protocol (one JSON object per line).

Every line it writes is shaped as the real bridge wrote it on Cline 3.0.74
(`@cline/sdk` 0.2.0): against OpenRouter for a plain reply, a call of the
program's `lookup` and an `ask_question`, and against a stand-in of an
OpenAI-compatible chat endpoint (no account) for the rest: the program's
`read_files` in the built-in's place, an approval before `run_commands`
allowed and refused, `editor` creating a file, reasoning, and a turn failed
by a refused key. Ids and paths are fresh; `usage` and `iteration_*`
events, which no reader looks at, are left out. What the model does is
picked by keyword of the message:

  "lookup"   the program's `lookup`, by its route
  "readit"   `read_files`: the program's when the program has a tool of that
             name (no approval), else Cline's own, after an approval
  "runit"    `run_commands` `echo hi`, after an approval unless every call is
             approved; refused, Cline says so as the call's result
  "editit"   `editor` creating `new.txt`, after an approval
  "askit"    `ask_question`, one question of two options
  "think"    reasoning, then the answer
  "fail"     the turn fails: the key is refused
  "shadowed" `lookup` runs and ends well without its ask: what a Cline whose
             own tool held the program's tool's name would write (3.0.74
             never does; the adapter fails the turn on it)

Sessions are kept under Cline's data folder (`$CLINE_DATA_DIR`, else
`$CLINE_DIR/data`, else `~/.cline/data`), so a new process resumes one and a
missing one is refused as the bridge refuses it. `$FAKE_LOG` gets
`{"argv", "env"}` at start and every line it was sent.
"""

import json
import os
import sys

DATA = os.environ.get("CLINE_DATA_DIR") or os.path.join(os.environ.get("CLINE_DIR") or os.path.expanduser("~/.cline"), "data")
SESSIONS = os.path.join(DATA, "sessions")

state = {"session": None, "start": None, "asks": 0, "calls": 0}


def out(message):
    sys.stdout.write(json.dumps(message, separators=(",", ":")) + "\n")
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


def event(kind, **fields):
    out({"type": "event", "event": {"type": kind, **fields}})


def ask(kind, request):
    """Ask this side, and read until its answer comes (answering what else it sends)."""
    state["asks"] += 1
    ask_id = f"ask-{state['asks']}"
    out({"type": "ask", "ask": ask_id, "kind": kind, "request": request})
    while True:
        message = read()
        if message.get("type") == "answer" and message.get("ask") == ask_id:
            return message["response"]
        handle(message)


def session_path(session_id):
    return os.path.join(SESSIONS, session_id, "messages.json")


def start(m):
    path = session_path(m["session"])
    if m["resume"] and not os.path.exists(path):
        raise RuntimeError(f"Cline has no conversation {m['session']} any more")
    os.makedirs(os.path.dirname(path), exist_ok=True)
    if not m["resume"]:
        with open(path, "w") as f:
            json.dump([], f)
    state["session"], state["start"] = m["session"], m
    provider = os.environ.get("CLINE_PROVIDER") or "cline"
    return {"session": m["session"], "provider": provider, "model": m["model"] or "anthropic/claude-sonnet-5.5"}


def text(words):
    accumulated = ""
    for piece in (words[:5], words[5:]):
        accumulated += piece
        event("content_start", contentType="text", text=piece, accumulated=accumulated)
    event("content_end", contentType="text", text=words)


def tool_names():
    return {t["name"] for t in state["start"]["tools"]}


def call(name, tool_input, run):
    """One call as Cline makes it: the approval first unless a policy approves
    it (the program's tools, `ask_question`, or every call), then the call and
    its result (`run` gives it: (output, error))."""
    state["calls"] += 1
    call_id = f"call_{name}_{state['calls']}"
    ours = name in tool_names()
    if not ours and name != "ask_question" and not state["start"]["autoApprove"]:
        answer = ask("approval", {"toolCallId": call_id, "toolName": name, "input": tool_input})
        if not answer["approved"]:
            event("content_start", contentType="tool", toolName=name, toolCallId=call_id, input=tool_input)
            why = {"error": f"{answer.get('reason') or ''} -- NOT a tool or system failure. Clarify with user before proceeding."}
            event("content_end", contentType="tool", toolName=name, toolCallId=call_id, output=why, error=json.dumps(why))
            return why["error"]
    event("content_start", contentType="tool", toolName=name, toolCallId=call_id, input=tool_input)
    if ours:
        result = ask("tool", {"toolCallId": call_id, "name": name, "input": tool_input})
        output, error = (None, result["output"]) if result.get("isError") else (result["output"], None)
    else:
        output, error = run(call_id)
    if error is not None:
        failed = {"error": error}
        event("content_end", contentType="tool", toolName=name, toolCallId=call_id, output=failed, error=json.dumps(failed), durationMs=2)
        return error
    event("content_end", contentType="tool", toolName=name, toolCallId=call_id, output=output, durationMs=2)
    return output if isinstance(output, str) else json.dumps(output)


def question(call_id):
    request = {"toolCallId": call_id, "question": "Which color?", "options": ["red", "blue"]}
    answer = ask("question", request)
    if "refused" in answer:
        return None, answer["refused"]
    return answer["answer"], None


def run_commands(call_id):
    event("content_update", contentType="tool", toolName="run_commands", toolCallId=call_id,
          update={"stream": "stdout", "chunk": "hi\n", "commandIndex": 0, "query": "echo hi"})
    return [{"query": "echo hi", "result": "hi\n", "success": True}], None


def turn(rid, message):
    if "fail" in message:
        why = "Incorrect API key provided: k. You can find your API key at https://platform.openai.com/account/api-keys."
        event("error", error={"message": why}, errorClass="auth", recoverable=False, iteration=1)
        out({"id": rid, "result": {"finishReason": "error", "text": why}})
        return
    said = None
    if "shadowed" in message:
        event("content_start", contentType="tool", toolName="lookup", toolCallId="call_lookup_0", input={})
        event("content_end", contentType="tool", toolName="lookup", toolCallId="call_lookup_0", output="from a plugin", durationMs=1)
        said = "from a plugin"
    elif "lookup" in message:
        said = call("lookup", {}, None)
    elif "readit" in message:
        said = call("read_files", {"files": [{"path": "a.txt"}]}, lambda call_id: ("cline's own file", None))
    elif "runit" in message:
        said = call("run_commands", {"commands": ["echo hi"]}, run_commands)
    elif "editit" in message:
        said = call("editor", {"path": "new.txt", "new_text": "hi"},
                    lambda call_id: ({"query": "edit:new.txt", "result": "File created successfully", "success": True}, None))
    elif "askit" in message:
        said = call("ask_question", {"question": "Which color?", "options": ["red", "blue"]}, question)
    elif "think" in message:
        event("content_start", contentType="reasoning", reasoning="let me think", redacted=False)
        text("thought and answered")
        event("content_end", contentType="reasoning", reasoning="let me think")
        out({"id": rid, "result": {"finishReason": "completed", "text": "thought and answered"}})
        return
    reply = "hello from cline" if said is None else f"tool said: {said}"
    text(reply)
    event("done", reason="completed", text=reply, iterations=2)
    out({"id": rid, "result": {"finishReason": "completed", "text": reply}})


def handle(m):
    try:
        if m["type"] == "start":
            out({"id": m["id"], "result": start(m)})
        elif m["type"] == "prompt":
            turn(m["id"], m["text"])
        else:
            raise RuntimeError(f"the bridge has no request {json.dumps(m['type'])}")
    except RuntimeError as e:
        out({"id": m["id"], "error": {"message": str(e)}})


logged({"argv": sys.argv[1:], "env": {k: v for k, v in os.environ.items() if k.startswith("CLINE")}})
while True:
    handle(read())
