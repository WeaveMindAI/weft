"""A stand-in for `vibe-app-server --agent-socket PATH`: Mistral Vibe's app
server, JSON-RPC one line each way, replayed from what Vibe 3.0.4 really
wrote (recorded against a local scripted model, no account): the same
requests, the same notifications in the same order, the same shapes,
trimmed to the fields Vibe always sends. It holds none of Vibe's logic
beyond that.

At its start it connects to the agent socket (`agent/initialize`), as
Vibe does, and takes the tools it declares; each call of one is a
`tools/call` on a connection of its own, held until the socket answers.

Each turn's message says what its model does, by keyword, in this order:

  "call:<name>"  calls the tool <name>: one the socket declared (its
                 history names it `weft.<name>`), else a built-in it runs
                 itself (`file_system.<name>`)
  "ask"          asks with its `ask_user_question` tool (a `user_input`
                 callback)
  "run"          runs `touch made.txt` with `bash`, asking first (an
                 `approval` callback) unless the agent is `auto-approve`
  "write"        writes made.txt with `write_file`, asking first under
                 `ask`
  "think"        reasons first, then streams its reply in two pieces
  "limit"        stops at its limit (`stopReason` "limit")
  "fail"         ends the turn failed, as a bad key does
  "throttled"    Mistral refuses every attempt with a 429 (a key whose plan
                 allows no requests): Vibe says it retries (`turn/retrying`),
                 then fails the turn once its retry budget is spent, as
                 3.0.4 did against Mistral's real API
  "crash"        exits in the middle of the turn

and replies with what each step got, one line each ("done" for none).

Refusals as Vibe's: `session/start` refuses a declared tool named like
one of its provided tools left on (`disabledTools`), `turn/start` one
named like a core tool. A session lives in `vibe-sessions/<id>.json`
beside `$FAKE_LOG`, so a stand-in started again resumes it and an unknown
id is refused; a resumed session keeps the agent it was left on.

`$FAKE_LOG` gets the start (`{"start": {args, env}}`), every message it
was sent, and every socket exchange (`{"socket": ...}`), one JSON line each.
"""

import json
import os
import socket
import sys
import time
import uuid

# Its core tools, which no client tool may be named like, and the provided
# ones, which one may once they are turned off (3.0.4).
CORE = {"bash", "edit", "read_file", "run_typescript", "search_tool_functions", "skill", "wait", "write_file"}
PROVIDED = {"ask_user_question", "cron", "todo", "unified_harness_scratchpad"}
AGENTS = {"ask": "Requires approval for tool executions", "accept-edits": "Auto-approves file edits only",
          "auto-approve": "Auto-approves all tool executions"}

LOG = os.environ["FAKE_LOG"]
SESSIONS = os.path.join(os.path.dirname(LOG), "vibe-sessions")
SOCKET = sys.argv[sys.argv.index("--agent-socket") + 1]
EVENT = [0]
NEXT = [0]
PENDING = {}
STATE = {"session": None, "agent": None, "tools": [], "config": {}}


def logged(entry):
    with open(LOG, "a") as f:
        f.write(json.dumps(entry) + "\n")


def write(message):
    sys.stdout.write(json.dumps(message) + "\n")
    sys.stdout.flush()


def now():
    return int(time.time() * 1000)


def socket_request(method, params):
    """One request on the agent socket, on a connection of its own, as Vibe makes it."""
    NEXT[0] += 1
    request = {"jsonrpc": "2.0", "id": NEXT[0], "method": method, "params": params}
    with socket.socket(socket.AF_UNIX, socket.SOCK_STREAM) as s:
        s.connect(SOCKET)
        s.sendall((json.dumps(request) + "\n").encode())
        reply = s.makefile().readline()
    logged({"socket": request, "reply": json.loads(reply)})
    return json.loads(reply)


def read():
    line = sys.stdin.readline()
    if not line:
        sys.exit(0)
    message = json.loads(line)
    logged(message)
    return message


def notify(method, params):
    write({"jsonrpc": "2.0", "method": method, "params": params})


def event(method, turn_id=None, **params):
    EVENT[0] += 1
    body = {"eventId": EVENT[0], "sessionId": STATE["session"], "emittedAt": now(), **params}
    if turn_id is not None:
        body["turnId"] = turn_id
    notify(method, body)


def entry_base(turn_id, entry_id, kind):
    return {"id": entry_id, "sessionId": STATE["session"], "turnId": turn_id, "inputEntryId": f"user-{turn_id}-1",
            "createdAt": now(), "updatedAt": now(), "generationStatus": "in_progress", "relatedEntryId": None, "type": kind}


def completed():
    return [{"op": "replace", "path": "/generationStatus", "value": "completed"},
            {"op": "replace", "path": "/updatedAt", "value": now()},
            {"op": "replace", "path": "/outcome", "value": {"type": "committed"}}]


def session_state():
    return {"format": "vibe.public-session-state/v1", "eventId": EVENT[0], "session": {
        "id": STATE["session"], "rootSessionId": STATE["session"], "parentSessionId": None, "title": None, "preview": "",
        "status": {"type": "idle"}, "createdAt": now(), "updatedAt": now(), "cwd": STATE["config"].get("cwd"),
        "workspaceRoots": STATE["config"].get("workspaceRoots", []), "worktree": None, "model": None, "reasoningEffort": None,
        "agent": {"name": STATE["agent"], "displayName": STATE["agent"].title(), "description": AGENTS[STATE["agent"]],
                  "safety": "neutral", "agentType": "agent"},
        "tokenUsage": None, "contextUsage": None, "harness": "unified", "loops": []},
        "history": [], "turns": [], "activeCallbacks": [], "childSessions": [], "backgroundProcesses": [],
        "turnQueue": {"items": [], "paused": False, "maxItems": 32}, "retrying": None}


def save():
    os.makedirs(SESSIONS, exist_ok=True)
    with open(os.path.join(SESSIONS, f"{STATE['session']}.json"), "w") as f:
        json.dump({"agent": STATE["agent"]}, f)


def runtime():
    return {"runtime": {"config": {"activeModel": {"name": "mock-model", "alias": "mock",
                                                   "thinking": STATE.get("effort") or "off"}},
                        "activeAgent": {"name": STATE["agent"], "displayName": STATE["agent"].title(),
                                        "description": AGENTS[STATE["agent"]], "safety": "neutral", "agentType": "agent"},
                        "bypassToolPermissions": STATE["agent"] == "auto-approve"}}


def error(rid, code, message, data=None):
    write({"jsonrpc": "2.0", "id": rid, "error": {"code": code, "message": message, "data": data}})


def answer(rid, result):
    write({"jsonrpc": "2.0", "id": rid, "result": result})


def wait_for(rid):
    """Read until the answer to our request `rid` (a `callback/call`), and the `callback/result` after it."""
    while True:
        message = read()
        if message.get("id") == rid and "method" not in message:
            continue
        if message.get("method") == "callback/result":
            answer(message["id"], {"lastEventId": EVENT[0], "accepted": True})
            return message["params"]["result"]


def callback(turn_id, callback_id, title, detail, related):
    entry = dict(entry_base(turn_id, callback_id, "callback"), relatedEntryId=related, callbackId=callback_id, title=title,
                 detail=detail, state={"status": "open"})
    event("history/entryAdded", turn_id, entry=entry)
    NEXT[0] += 1
    rid = NEXT[0]
    write({"jsonrpc": "2.0", "id": rid, "method": "callback/call", "params": {"callback": entry}})
    result = wait_for(rid)
    event("history/entryUpdated", turn_id, entryId=callback_id,
          patch=[{"op": "replace", "path": "/generationStatus", "value": "completed"},
                 {"op": "replace", "path": "/state", "value": {"status": "answered", "output": result.get("output")}}])
    return result["output"]


def effect(turn_id, tool_name, kind, tool_input, display):
    action = str(uuid.uuid4())
    entry_id = f"effect-{action}"
    event("history/entryAdded", turn_id, entry=dict(entry_base(turn_id, entry_id, "effect"), title=tool_name, detail={
        "toolName": tool_name, "display": display, "kind": kind, "input": tool_input}, state={"status": "running", "outputText": ""}))
    return action, entry_id


def effect_done(turn_id, entry_id, state_patch, failed=False):
    patch = [{"op": "replace", "path": "/generationStatus", "value": "completed"},
             {"op": "replace", "path": "/updatedAt", "value": now()}]
    patch += [{"op": "add", "path": f"/state/{k}", "value": v} for k, v in state_patch.items()]
    patch.append({"op": "replace", "path": "/state/status", "value": "failed" if failed else "completed"})
    event("history/entryUpdated", turn_id, entryId=entry_id, patch=patch)


def call_tool(turn_id, name):
    declared = [t for t in STATE["tools"] if t["name"] == name]
    if not declared:
        _, entry_id = effect(turn_id, f"file_system.{name}", "tool", {"q": "x"},
                             {"summary": name, "verb": "Running", "message": name})
        effect_done(turn_id, entry_id, {"output": {"stdout": "", "stderr": "", "output": f"built-in {name} ran", "truncated": False}})
        return f"{name}: built-in {name} ran"
    tool_name = f"{declared[0]['namespace']}.{name}"
    action, entry_id = effect(turn_id, tool_name, "tool", {"q": "x"},
                              {"summary": tool_name, "content": None, "suffix": "", "verb": "Running", "message": tool_name})
    reply = socket_request("tools/call", {"callbackId": f"client_tool:{action}", "name": tool_name, "input": {"q": "x"},
                                          "toolCallId": f"call_{action[:8]}", "caller": {
                                              "sessionId": STATE["session"], "rootSessionId": STATE["session"],
                                              "parentSessionId": None, "name": None, "agentType": None}})
    result = reply.get("result") or {}
    if result.get("error") or "error" in reply:
        message = (result.get("error") or reply.get("error"))["message"]
        effect_done(turn_id, entry_id, {"error": {"message": message, "code": "tool_failed", "details": None}}, failed=True)
        return f"{name} failed: {message}"
    output = result.get("output")
    patch = [{"op": "replace", "path": "/generationStatus", "value": "completed"},
             {"op": "add", "path": "/state/output", "value": {"type": "success", "content": [{"type": "text", "text": output}]}},
             {"op": "append", "path": "/state/outputText", "value": output},
             {"op": "replace", "path": "/state/status", "value": "completed"}]
    event("history/entryUpdated", turn_id, entryId=entry_id, patch=patch)
    return f"{name}: {output}"


def ask(turn_id):
    _, entry_id = effect(turn_id, "ui.ask_user_question", "tool", {"questions": [{"question": "Red or blue?"}]},
                         {"summary": "Asking", "verb": "Asking", "message": "Red or blue?"})
    call_id = f"call_{uuid.uuid4().hex[:8]}"
    output = callback(turn_id, f"user-input-{call_id}", "User input required", {
        "kind": "user_input", "request": {"questions": [
            {"question": "Red or blue?", "header": "Color", "options": [{"label": "Red", "description": ""},
                                                                         {"label": "Blue", "description": ""}],
             "multiSelect": False, "hideOther": False},
            {"question": "Which sizes?", "header": "Sizes", "options": [{"label": "S", "description": ""},
                                                                         {"label": "M", "description": ""}],
             "multiSelect": True, "hideOther": True}], "footerNote": None}, "relatedEntryId": call_id}, call_id)
    effect_done(turn_id, entry_id, {"output": output["result"]})
    return "ask: " + json.dumps(output["result"])


def gated(turn_id, tool, kind, tool_input, display, asks):
    """A tool that asks first when `asks`: the decision, or None when it ran without asking."""
    _, entry_id = effect(turn_id, f"file_system.{tool}", kind, tool_input, display)
    decision = None
    if asks:
        call_id = f"call_{uuid.uuid4().hex[:8]}"
        output = callback(turn_id, f"approval-{call_id}", f"Approve file_system.{tool}", {
            "kind": "approval", "effect": {"toolName": f"file_system.{tool}", "display": display, "kind": kind, "input": tool_input},
            "requiredPermissions": [], "choices": ["approve", "approve_for_session", "approve_permanently", "deny", "cancel_turn"],
            "pathScopeChoices": [], "relatedEntryId": entry_id, "reason": None}, entry_id)
        decision = output
        if output["decision"]["type"] == "deny":
            effect_done(turn_id, entry_id, {"error": {"message": "Tool execution denied by approval callback", "code": "tool_denied",
                                                      "details": None}}, failed=True)
            return decision, False
    return decision, entry_id


def run(turn_id):
    decision, entry_id = gated(turn_id, "bash", "shell", {"command": "touch made.txt"},
                               {"summary": "bash: touch made.txt", "verb": "Running", "message": "touch made.txt"},
                               STATE["agent"] != "auto-approve")
    if entry_id:
        open(os.path.join(STATE["config"]["cwd"], "made.txt"), "a").close()
        effect_done(turn_id, entry_id, {"output": {"stdout": "", "stderr": "", "output": "", "truncated": False}})
    return "run: " + (json.dumps(decision) if decision is not None else "auto")


def write_file(turn_id):
    path = os.path.join(STATE["config"]["cwd"], "made.txt")
    existed = os.path.exists(path)
    decision, entry_id = gated(turn_id, "write_file", "file_write", {"filePath": "made.txt", "content": "hello"},
                               {"summary": "Writing made.txt", "verb": "Creating", "message": "made.txt"}, STATE["agent"] == "ask")
    if entry_id:
        with open(path, "w") as f:
            f.write("hello")
        effect_done(turn_id, entry_id, {"output": {"filePath": path, "content": "hello", "fileExisted": existed,
                                                   "previousContent": None}})
    return "write: " + ("denied" if not entry_id else "done")


def say(turn_id, text, pieces=None):
    entry_id = f"assistant-{uuid.uuid4()}"
    pieces = pieces or [text]
    event("history/entryAdded", turn_id, entry=dict(entry_base(turn_id, entry_id, "message"), role="assistant",
                                                    content=[{"type": "text", "text": pieces[0]}], source="harness",
                                                    userDisplayContent=None, outcome=None))
    for piece in pieces[1:]:
        event("history/entryUpdated", turn_id, entryId=entry_id, patch=[{"op": "append", "path": "/content/0/text", "value": piece}])
    event("history/entryUpdated", turn_id, entryId=entry_id, patch=completed())


def turn_record(turn_id, status, error_=None, stop=None):
    return {"id": turn_id, "sessionId": STATE["session"], "status": status, "inputEntryId": f"user-{turn_id}-1",
            "startedAt": now(), "completedAt": None if status == "in_progress" else now(), "error": error_, "stopReason": stop,
            "queueItemId": None}


def turn(rid, text):
    names = [t["name"] for t in STATE["tools"]]
    clash = sorted(set(names) & CORE)
    if clash:
        error(rid, "invalid_params", f"direct tool name \"{clash[0]}\" conflicts with a built-in tool")
        return
    turn_id = f"turn-{uuid.uuid4().hex}"
    answer(rid, {"lastEventId": EVENT[0], "turn": turn_record(turn_id, "in_progress")})
    event("history/entryAdded", turn_id, entry=dict(entry_base(turn_id, f"user-{turn_id}-1", "message"), role="user",
                                                    content=[{"type": "text", "text": text}], source="turn_start",
                                                    generationStatus="completed", inputEntryId=None))
    event("turn/started", turn=turn_record(turn_id, "in_progress"))
    save()
    lines = []
    for word in text.split():
        if word.startswith("call:"):
            lines.append(call_tool(turn_id, word[len("call:"):]))
        elif word == "ask":
            lines.append(ask(turn_id))
        elif word == "run":
            lines.append(run(turn_id))
        elif word == "write":
            lines.append(write_file(turn_id))
        elif word == "think":
            reasoning = f"reasoning-{uuid.uuid4()}-0"
            event("history/entryAdded", turn_id, entry=dict(entry_base(turn_id, reasoning, "reasoning"), text="let me think",
                                                            summary=[], outcome=None))
            event("history/entryUpdated", turn_id, entryId=reasoning, patch=completed())
            say(turn_id, "Hello", ["Hel", "lo"])
            event("turn/completed", turn=turn_record(turn_id, "completed"))
            return
        elif word == "crash":
            sys.exit(3)
        elif word == "fail":
            event("turn/completed", turn=turn_record(turn_id, "failed", {
                "message": "Invalid API key (from env var MISTRAL_API_KEY). Please check your API key and try again.",
                "code": "invalid_api_key", "details": {"httpStatus": 401, "provider": "mistral", "reason": "http_unauthorized"}}))
            return
        elif word == "throttled":
            for _ in range(2):
                # Sent bare: no event id, no time.
                notify("turn/retrying", {"sessionId": STATE["session"], "category": "rate_limited", "detail": "HTTP 429"})
            event("turn/completed", turn=turn_record(turn_id, "failed", {
                "message": 'API error occurred: Status 429. Body: {"object":"error","message":"Rate limit exceeded",'
                           '"type":"rate_limited","param":null,"code":"1300","raw_status_code":429}',
                "code": "rate_limit", "details": {"correlationId": "01a1ac15-0000", "exceptionType": "SDKError", "httpStatus": 429,
                                                  "model": "mistral-vibe-cli-latest", "provider": "mistral", "purpose": "agent"}}))
            return
        elif word == "limit":
            say(turn_id, "partway")
            event("turn/completed", turn=turn_record(turn_id, "completed", stop="limit"))
            return
    say(turn_id, "\n".join(lines) or "done")
    event("session/updated", patch=[{"op": "replace", "path": "/status/type", "value": "idle"}])
    event("turn/completed", turn=turn_record(turn_id, "completed"))


def main():
    logged({"start": {"args": sys.argv[1:], "env": dict(os.environ)}})
    hello = socket_request("agent/initialize", {"protocolVersion": 2, "vibeVersion": "3.0.4"})
    if "error" in hello:
        print(f"Error: the agent socket refused the handshake: {hello['error']['message']}", file=sys.stderr)
        sys.exit(1)
    STATE["tools"] = hello["result"]["tools"]
    while True:
        message = read()
        method, rid, params = message.get("method"), message.get("id"), message.get("params") or {}
        if method == "initialize":
            answer(rid, {"serverInfo": {"name": "vibe-app-server", "version": "3.0.4"}, "processId": os.getpid()})
        elif method == "initialized" or rid is None:
            continue
        elif method in ("session/start", "session/resume"):
            STATE["config"] = params.get("agentConfig") or {}
            on = sorted({t["name"] for t in STATE["tools"]} & (PROVIDED - set(STATE["config"].get("disabledTools") or [])))
            if on:
                error(rid, "invalid_params", "Invalid request parameters", {"errorCount": 1, "issues": [
                    {"path": [], "message": f"Value error, duplicate direct tool name '{on[0]}'"}]})
                continue
            if method == "session/resume":
                path = os.path.join(SESSIONS, f"{params['sessionId']}.json")
                if not os.path.exists(path):
                    error(rid, "not_found", f"Session not found: {params['sessionId']}")
                    continue
                STATE["session"] = params["sessionId"]
                with open(path) as f:
                    # A resumed session keeps the agent it was left on.
                    STATE["agent"] = json.load(f)["agent"]
            else:
                STATE["session"] = str(uuid.uuid4())
                STATE["agent"] = STATE["config"].get("agent") or "accept-edits"
            answer(rid, {"lastEventId": EVENT[0], "state": session_state()})
        elif method == "session/agent/update":
            STATE["agent"] = params["agentName"]
            if os.path.exists(os.path.join(SESSIONS, f"{STATE['session']}.json")):
                # Saved from its first turn on, as Vibe saves a session.
                save()
            answer(rid, runtime())
        elif method == "config/model/write":
            STATE["effort"] = params.get("reasoningEffort")
            answer(rid, runtime())
        elif method == "turn/start":
            turn(rid, params["message"][0]["text"])
        else:
            error(rid, "not_found", f"Method not found: {method}")


main()
