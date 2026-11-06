"""A stand-in for `kimi-code-sdk kimi_bridge.mjs`: weft's bridge to Kimi Code's
SDK, speaking the bridge's protocol (one JSON object per line).

Every line it writes is shaped as the real bridge wrote it on Kimi Code 2.5.1,
driven against a stand-in of Moonshot's chat endpoint (no account): the
`start` and `tools` answers, a turn's events (`turn.started`,
`assistant.delta`, `thinking.delta`, `tool.call.started`, `tool.progress`,
`tool.result`, `turn.ended`), the approval Kimi asks before a command and
before a program tool, its AskUserQuestion, a call of the program's tool by
its plain name, one replacing the built-in `Read`, a refused command, a turn
failed for a missing key, and the errors of a session that already exists
and of one that does not. Ids, times and paths are fresh; events no reader
looks at (`agent.status.updated`, `turn.step.*`, `tool.call.delta`, ...)
are left out. What the model does is picked by keyword of the message:

  "lookup"   the program's `lookup`: under `manual` Kimi asks to approve it,
             then calls it
  "readit"   the program's `Read`, which took the built-in's place (no approval:
             Kimi approves `Read` by name)
  "runit"    `Bash` `echo hi`, after an approval under `manual`; refused,
             Kimi says so as the call's result
  "askit"    AskUserQuestion, one question of two options
  "think"    reasoning, then the answer
  "subagent" a sub-agent calls the program's `lookup`: its ask comes, its
             events never do (the bridge forwards the main agent's alone)
  "fail"     the turn fails: the key's variable is not set
  "broke"    the turn fails on Moonshot's own refusal, a key whose account
             has no balance left (as Moonshot's real API answered, account
             ids scrubbed)

Sessions are kept under `$KIMI_CODE_HOME/sessions`, with the tools
registered on them, so a new process resumes one and a missing one is
refused. `$FAKE_LOG` gets `{"argv", "env"}` at start and every line it was
sent.
"""

import json
import os
import sys
import time

HOME = os.environ.get("KIMI_CODE_HOME") or os.path.expanduser("~/.kimi-code")
SESSIONS = os.path.join(HOME, "sessions")
# Kimi 2.5.1's main agent's tools, as its registry lists them.
BUILT_IN = ["Agent", "AgentSwarm", "AskUserQuestion", "Bash", "CreateGoal", "CronCreate", "CronDelete", "CronList", "Edit",
            "ExitPlanMode", "FetchURL", "GetGoal", "Glob", "Grep", "Read", "ReadMediaFile", "select_tools", "SetGoalBudget",
            "Skill", "TaskList", "TaskOutput", "TaskStop", "TodoList", "UpdateGoal", "WaitFor", "Write"]

state = {"session": None, "turn": -1, "asks": 0}


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


def event(kind, **fields):
    out({"type": "event", "event": {"type": kind, "time": int(time.time() * 1000), "agentId": "main",
                                     "turnId": state["turn"], **fields, "sessionId": state["session"]["id"]}})


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
    return os.path.join(SESSIONS, f"{session_id}.json")


def save():
    os.makedirs(SESSIONS, exist_ok=True)
    with open(session_path(state["session"]["id"]), "w") as f:
        json.dump(state["session"], f)


def start(m):
    path = session_path(m["session"])
    if m["resume"]:
        if not os.path.exists(path):
            raise RuntimeError(f'Session "{m["session"]}" was not found')
        with open(path) as f:
            state["session"] = json.load(f)
    else:
        if os.path.exists(path):
            raise RuntimeError(f'Session "{m["session"]}" already exists')
        state["session"] = {"id": m["session"], "turn": -1}
    state["session"]["tools"] = [t["name"] for t in m["tools"]]
    state["session"]["setup"] = {k: m[k] for k in ("workDir", "model", "thinking", "permission", "plan", "additionalDirs")}
    state["turn"] = state["session"]["turn"]
    save()
    return {"session": m["session"]}


def tools():
    user = state["session"]["tools"]
    listed = [{"name": n, "source": "builtin"} for n in BUILT_IN if n not in user] + [{"name": n, "source": "user"} for n in user]
    return {"tools": sorted(listed, key=lambda t: t["name"].lower())}


def text(words):
    for piece in (words[:5], words[5:]):
        event("assistant.delta", delta=piece)


def call(name, args, approval=None, run=None):
    """One tool call as Kimi makes it: the approval first when one is asked,
    then the call, then its result (`run` gives it, or the program's route)."""
    call_id = f"call_{name}_{state['turn']}"
    # Only the `manual` mode asks before these (`yolo` lets an ordinary
    # command go, `auto` never asks).
    if approval is not None and state["session"]["setup"]["permission"] == "manual":
        answer = ask("approval", {"turnId": state["turn"], "toolCallId": call_id, "toolName": name, "action": approval[0],
                                  "display": approval[1], "sessionId": state["session"]["id"], "agentId": "main"})
        if answer["decision"] != "approved":
            event("tool.call.started", toolCallId=call_id, name=name, args=args, description=approval[0], display=approval[1])
            reason = f" Reason: {answer['feedback']}" if answer.get("feedback") else ""
            output = f'Tool "{name}" was not run because the user rejected the approval request.{reason}'
            event("tool.result", toolCallId=call_id, output=output, isError=True)
            return output
    event("tool.call.started", toolCallId=call_id, name=name, args=args,
          **({"description": approval[0], "display": approval[1]} if approval else {}))
    failed = False
    if run is not None:
        output = run(call_id)
    else:
        result = ask("tool", {"toolCallId": call_id, "name": name, "args": args})
        output, failed = result["output"], bool(result.get("isError"))
    event("tool.result", toolCallId=call_id, output=output, **({"isError": True} if failed else {}))
    return output


def question(call_id):
    args = {"questions": [{"question": "Which color?", "header": "Color", "options": [{"label": "red", "description": "warm"},
                                                                                        {"label": "blue", "description": "cold"}],
                           "multiSelect": False}]}
    answer = ask("question", {"turnId": state["turn"], "toolCallId": call_id, **args, "sessionId": state["session"]["id"], "agentId": "main"})
    return json.dumps({"answers": (answer or {}).get("answers", {})}, separators=(",", ":"))


def turn(rid, message):
    state["turn"] += 1
    state["session"]["turn"] = state["turn"]
    save()
    event("turn.prompt", input=[{"type": "text", "text": message}], origin={"kind": "user"}, promptId=f"msg_{state['turn']}")
    event("turn.started", promptId=f"msg_{state['turn']}", origin={"kind": "user"}, prompt=message)
    out({"id": rid, "result": {}})
    if "fail" in message:
        error = {"code": "config.invalid", "message": 'Provider "moonshot" declares api_key_env = "KIMI_API_KEY" in config.toml, '
                 "but the environment variable is not set or is empty.", "name": "Error2", "retryable": False}
        event("turn.ended", reason="failed", error=error, durationMs=7, interruptReason="error")
        return
    if "broke" in message:
        refused = ("Your account org-0000 <ak-0000> is suspended due to insufficient balance, please recharge your account or "
                   "check your plan and billing details")
        event("turn.step.interrupted", step=1, reason="error", message=f"[provider.api_error] {refused}")
        event("turn.ended", reason="failed", error={"code": "provider.api_error", "message": refused,
                                                    "name": "APIProviderQuotaExhaustedError",
                                                    "details": {"statusCode": 429, "requestId": None, "traceId": None, "diagnostics": {}},
                                                    "retryable": False}, durationMs=344)
        return
    if "lookup" in message:
        said = call("lookup", {"word": "x"}, approval=("Approve lookup", {"kind": "generic", "summary": "Approve lookup", "detail": {"word": "x"}}))
        text(f"tool said: {said}")
    elif "readit" in message:
        text(f"tool said: {call('Read', {'path': 'a.txt'})}")
    elif "runit" in message:
        display = {"kind": "command", "command": "echo hi", "cwd": state["session"]["setup"]["workDir"], "description": "say hi", "language": "bash"}

        def run(call_id):
            event("tool.progress", toolCallId=call_id, update={"kind": "stdout", "text": "hi\n"})
            return "hi\n"
        text(f"tool said: {call('Bash', {'command': 'echo hi', 'description': 'say hi'}, approval=('Running: echo hi', display), run=run)}")
    elif "askit" in message:
        text(f"tool said: {call('AskUserQuestion', {'questions': []}, run=question)}")
    elif "subagent" in message:
        result = ask("tool", {"toolCallId": "call_lookup_sub", "name": "lookup", "args": {"word": "y"}})
        text(f"the sub-agent got: {result['output']}")
    elif "think" in message:
        event("thinking.delta", delta="let me think")
        text("thought and answered")
    else:
        text("hello from kimi")
    event("turn.ended", reason="completed", durationMs=64)


def handle(m):
    try:
        if m["type"] == "start":
            out({"id": m["id"], "result": start(m)})
        elif m["type"] == "tools":
            out({"id": m["id"], "result": tools()})
        elif m["type"] == "prompt":
            turn(m["id"], m["text"])
        else:
            raise RuntimeError(f"the bridge has no request {m['type']!r}")
    except RuntimeError as e:
        out({"id": m["id"], "error": {"message": str(e)}})


logged({"argv": sys.argv[1:], "env": {k: v for k, v in os.environ.items() if k.startswith("KIMI")}})
while True:
    handle(read())
