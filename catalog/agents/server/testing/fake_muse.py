"""A stand-in for `muse serve`: the Muse Session Protocol (MSP), JSON-RPC
over stdio, one object per line.

Its handshake, sessions, plain turn, resume, unknown session and login
failures are the lines real `muse serve` 1.4.14 wrote (the research
transcript `muse.transcript.jsonl`: `t2` lines 32-64 for the handshake, the
session and a turn; `t3` lines 66-92 for the resume and `sessionNotFound`;
`t1` line 17 for `authRequired`; a rejected `META_API_KEY`'s stderr line from
a run of `muse serve --provider meta` with a bogus key), kept field for
field with their ids made fresh. Real Muse with no login runs no model, so
what a model does is composed from the schema the binary exports (`muse
schema generate-json-schema`: `ApprovalRequestParams`, `UserInputRequestParams`,
`Item`), by keyword of the turn's message:

  "run"      a shell command needs approval (`approval/request`); "pipe" makes
             it a two-stage pipeline, approved stage by stage
  "ask"      the ask tool (`userInput/request`)
  "tool"     calls the program's `lookup` tool over the session's MCP server
  "fail"     the turn fails `authRequired` (no login); with `META_API_KEY`
             set to "rejected", the stderr line a rejected key prints at
             startup comes first, as it does from real Muse

Like real Muse, it starts the session's MCP server (initialize, tools/list)
at `session/start` and again at `session/resume`, and keeps its sessions on
disk (beside `$FAKE_LOG`), so a new process resumes them; a turn left waiting
on an approval is still the session's active turn there, and its request is
sent again on the resume, as the schema says the host does.

`$FAKE_LOG` gets every message it was sent, one JSON line each, and every
MCP call it made (`{"mcp": method}`).
"""

import json
import os
import sys
import urllib.request
import uuid

STATE = os.path.join(os.path.dirname(os.environ["FAKE_LOG"]), "muse-sessions.json")
SESSION = {"id": None, "mcp": None, "approval": "onRequest"}
NEXT = [1000]


def out(message):
    message.setdefault("jsonrpc", "2.0")
    sys.stdout.write(json.dumps(message) + "\n")
    sys.stdout.flush()


def logged(entry):
    with open(os.environ["FAKE_LOG"], "a") as f:
        f.write(json.dumps(entry) + "\n")


def read():
    line = sys.stdin.readline()
    if not line:
        # t2 lines 63-64: what a host says when its stdin closes.
        if SESSION["id"]:
            out({"method": "session/closed", "params": {"reason": "hostShutdown", "sessionId": SESSION["id"]}})
        sys.stderr.write("muse serve: exit reason recorded: stdin_closed\n")
        sys.exit(0)
    message = json.loads(line)
    logged(message)
    return message


def sessions():
    try:
        with open(STATE) as f:
            return json.load(f)
    except FileNotFoundError:
        return {}


def keep(sid, **fields):
    state = sessions()
    state.setdefault(sid, {}).update(fields)
    with open(STATE, "w") as f:
        json.dump(state, f)


def session(sid, workspace, active=None):
    # t2 line 39's `session`.
    return {"activeTurnId": active, "approvalMode": {"lastCommandId": None, "mode": SESSION["approval"], "source": "startup"},
            "createdByClient": {"name": "weft", "version": "1"}, "forkedFrom": None, "kind": "root", "modelId": None,
            "path": f"/home/u/.local/share/muse/sessions/2026/11/03/{sid}/session.jsonl", "providerId": "muse",
            "sessionId": sid, "sideFrom": None, "status": "idle", "turnCount": 0, "workspaceRoot": workspace}


def mcp(method, params=None, rid=None):
    """One MCP message to the session's server (streamable HTTP, JSON answers)."""
    logged({"mcp": method})
    body = {"jsonrpc": "2.0", "method": method, "params": params or {}}
    if rid is not None:
        body["id"] = rid
    request = urllib.request.Request(SESSION["mcp"], data=json.dumps(body).encode(), method="POST",
                                     headers={"Content-Type": "application/json", "Accept": "application/json, text/event-stream"})
    with urllib.request.urlopen(request, timeout=60) as answer:
        raw = answer.read()
    return json.loads(raw) if raw else None


def start_mcp(config):
    server = ((config or {}).get("mcpServers") or {}).get("weft")
    SESSION["mcp"] = server["url"] if server and server.get("transport") == "streamableHttp" else None
    if SESSION["mcp"]:
        # What real Muse sent the stdio server it started (research mcp.log).
        mcp("initialize", {"protocolVersion": "2025-06-18", "capabilities": {}, "clientInfo": {"name": "tbh", "version": "0.1.0"}}, 1)
        mcp("notifications/initialized")
        mcp("tools/list", {}, 2)


def ask(method, params):
    """A server request; the client's receipt, skipping anything else on the way."""
    NEXT[0] += 1
    rid = NEXT[0]
    out({"id": rid, "method": method, "params": params})
    while True:
        message = read()
        if message.get("id") == rid and "method" not in message:
            return message


def command(method):
    """The client's next command of `method`, answered as accepted; others on the way are logged and acked."""
    while True:
        message = read()
        if message.get("method") == method:
            return message
        if "id" in message and "method" in message:
            out({"id": message["id"], "result": {"commandId": (message.get("params") or {}).get("commandId"), "status": "accepted"}})


def item(method, sid, turn_id, body):
    out({"method": method, "params": {"item": dict(body, turnId=turn_id), "sessionId": sid}})


def approve(sid, turn_id, tool_item, stages):
    """One shell approval, stage by stage, as `ApprovalRequestParams` describes it."""
    approval = str(uuid.uuid4())
    command_text = " | ".join(stages)
    decided = []
    for index in range(len(stages)):
        params = {"approvalId": approval, "sessionId": sid, "turnId": turn_id, "itemId": tool_item, "taskId": "task-1",
                  "toolCallId": "call_1", "toolName": "shell", "rawArgs": json.dumps({"command": command_text}),
                  "subject": {"kind": "shell", "command": command_text, "stages": [{"argv": s.split()} for s in stages],
                              "workspaceRoot": "/w"},
                  "availableChoices": [
                      {"choiceId": "c-once", "decision": "approved", "label": "Allow once", "scope": "once"},
                      {"choiceId": "c-session", "decision": "approvedForSession", "label": "Allow for this session", "scope": "session"},
                      {"choiceId": "c-deny", "decision": "denied", "label": "Deny", "scope": "once", "acceptsFeedback": True}],
                  "currentRequirementId": {"approvalId": approval, "sourceIndex": index},
                  "judgeEscalated": False, "protectedWrite": False, "viewCursor": "v4:x:1"}
        keep(sid, activeTurnId=turn_id, pending=params)
        receipt = ask("approval/request", params)
        assert receipt["result"] == {}, receipt
        decide = command("approval/decide")
        p = decide["params"]
        assert p["requirementId"] == params["currentRequirementId"], p
        last = index == len(stages) - 1
        out({"id": decide["id"], "result": {"approvalId": approval, "commandId": p["commandId"], "status": "accepted", "terminal": last}})
        choice = next(c for c in params["availableChoices"] if c["choiceId"] == p["choiceId"])
        decided.append(choice["decision"] + (f" ({p['feedback']})" if p.get("feedback") else ""))
        if choice["decision"] == "denied":
            break
    keep(sid, activeTurnId=None, pending=None)
    out({"method": "approval/resolved", "params": {"approvalId": approval, "sessionId": sid}})
    return decided


def turn(sid, command_id, text):
    turn_id = command_id
    # t2 lines 45-47.
    out({"method": "session/statusChanged", "params": {"sessionId": sid, "status": "running"}})
    out({"method": "turn/started", "params": {"commandId": command_id, "sessionId": sid, "turnId": turn_id}})
    item("item/completed", sid, turn_id, {"commandId": command_id, "itemId": str(uuid.uuid4()), "kind": "userMessage",
                                           "revision": 1, "status": "completed", "text": text})
    # t2 line 48: Muse's own reminder child, nothing of weft's.
    item("item/started", sid, turn_id, {"itemId": str(uuid.uuid4()), "kind": "reminderChild", "fallbackText": "Reminder child session",
                                         "reminderAgentId": "skill-reminder", "revision": 1, "status": "inProgress"})
    reply = "echo: " + text
    if "fail" in text:
        # t1 lines 16-17.
        out({"method": "session/statusChanged", "params": {"sessionId": sid, "status": "idle"}})
        out({"method": "turn/completed", "params": {"durationMs": 88, "error": {"kind": "authRequired", "message": "not logged in: run /login to add an API key",
                                                                                 "retryable": False},
                                                    "reason": "not logged in: run /login to add an API key", "sessionId": sid,
                                                    "terminal": "failed", "turnId": turn_id}})
        return
    if "run" in text:
        stages = ["ls", "wc -l"] if "pipe" in text else ["date"]
        tool_item = str(uuid.uuid4())
        args = json.dumps({"command": " | ".join(stages)})
        item("item/started", sid, turn_id, {"itemId": tool_item, "kind": "toolCall", "tool": "shell", "args": args,
                                             "callId": "call_1", "revision": 1, "status": "inProgress"})
        decided = approve(sid, turn_id, tool_item, stages)
        ran = all(d.startswith("approved") for d in decided)
        item("item/completed", sid, turn_id, {"itemId": tool_item, "kind": "toolCall", "tool": "shell", "args": args, "revision": 2,
                                               "status": "completed" if ran else "rejected", "visibleOutput": "Tue" if ran else "",
                                               "failureReason": None if ran else "denied by the user"})
        reply = ",".join(decided)
    if "ask" in text:
        user_input = str(uuid.uuid4())
        receipt = ask("userInput/request", {
            "userInputId": user_input, "sessionId": sid, "turnId": turn_id, "itemId": "q-item", "toolCallId": "call_2",
            "toolName": "request_user_input", "autoResolutionMs": 60000, "viewCursor": "v4:x:2",
            "questions": [{"id": "colour", "header": "Colour", "question": "Red or blue?", "selection": {"mode": "single"},
                           "options": [{"label": "Red", "description": "warm"}, {"label": "Blue"}]},
                          {"id": "sizes", "header": "Sizes", "question": "Which sizes?",
                           "selection": {"mode": "multiple", "minSelections": 1, "maxSelections": 2},
                           "options": [{"label": "S"}, {"label": "M"}, {"label": "L"}]}]})
        assert receipt["result"] == {}, receipt
        answer = command("userInput/answer")
        out({"id": answer["id"], "result": {"commandId": answer["params"]["commandId"], "status": "accepted", "userInputId": user_input}})
        out({"method": "userInput/settled", "params": {"userInputId": user_input, "sessionId": sid, "outcome": "answered",
                                                        "answers": answer["params"]["answers"]}})
        reply = json.dumps(answer["params"]["answers"])
    if "tool" in text:
        tool_item = str(uuid.uuid4())
        item("item/started", sid, turn_id, {"itemId": tool_item, "kind": "toolCall", "tool": "mcp__weft__lookup", "args": "{\"key\": \"x\"}",
                                             "revision": 1, "status": "inProgress"})
        result = mcp("tools/call", {"name": "lookup", "arguments": {"key": "x"}}, 3)["result"]
        output = result["content"][0]["text"]
        item("item/completed", sid, turn_id, {"itemId": tool_item, "kind": "toolCall", "tool": "mcp__weft__lookup", "args": "{\"key\": \"x\"}",
                                               "revision": 2, "status": "failed" if result.get("isError") else "completed",
                                               "visibleOutput": output})
        reply = json.dumps(result)
    message = str(uuid.uuid4())
    # t2 lines 49-60.
    item("item/started", sid, turn_id, {"itemId": message, "kind": "agentMessage", "revision": 1, "status": "inProgress", "text": ""})
    out({"method": "item/delta", "params": {"delta": reply[:5], "field": "text", "itemId": message, "sessionId": sid}})
    out({"method": "item/delta", "params": {"delta": reply[5:], "field": "text", "itemId": message, "sessionId": sid}})
    item("item/completed", sid, turn_id, {"itemId": message, "kind": "agentMessage", "messagePhase": "finalAnswer", "revision": 2,
                                           "status": "completed", "text": reply})
    out({"method": "turn/foregroundCompleted", "params": {"blockingAgents": ["goal-reminder", "verify-reminder"], "sessionId": sid, "turnId": turn_id}})
    out({"method": "session/statusChanged", "params": {"sessionId": sid, "status": "idle"}})
    out({"method": "turn/completed", "params": {"durationMs": 215, "sessionId": sid, "terminal": "completed", "turnId": turn_id}})


def main():
    if os.environ.get("META_API_KEY") == "rejected":
        sys.stderr.write("tbh serve: startup model-catalog fetch rejected the credential (failed to fetch model catalog: "
                         "authentication failed: your API key from META_API_KEY was rejected \u2014 update or unset it) "
                         "\u2014 composing the logged-out fallback\n")
        sys.stderr.flush()
    while True:
        message = read()
        method, rid, params = message.get("method"), message.get("id"), message.get("params") or {}
        if method == "initialize":
            # t2 line 33.
            out({"id": rid, "result": {"experimentalApi": False, "grantedCapabilities": ["sessionMcp"], "museHome": "/home/u/.local/share/muse",
                                       "platformFamily": "unix", "platformOs": "linux", "sandboxAvailability": "available",
                                       "schema": {"fingerprint": "sha256:c94c3d1f2f51e6424074b9ec43eac3eedcf532d3dcd77351ec1383a87d3049c6", "version": 1},
                                       "serverInfo": {"name": "muse", "version": "1.4.14"}, "sessionDurability": "durable",
                                       "userAgent": "muse-build/1.4.14 (serve; linux-x86_64; build dec7d7497fecdad135ec5835a8161dbc92e55b58)"}})
        elif method == "session/start":
            sid = str(uuid.uuid4())
            SESSION["id"], SESSION["approval"] = sid, params.get("approvalMode") or "onRequest"
            start_mcp(params.get("config"))
            keep(sid, workspace=params.get("workspaceRoot"), activeTurnId=None, pending=None)
            # t2 lines 38-39.
            out({"method": "session/started", "params": {"session": session(sid, params.get("workspaceRoot"))}})
            out({"id": rid, "result": {"session": session(sid, params.get("workspaceRoot")), "viewCursor": f"v4:{sid}:2"}})
        elif method == "session/resume":
            sid = params["sessionId"]
            kept = sessions().get(sid)
            if kept is None:
                # t3 line 92.
                out({"error": {"code": -32020, "data": {"kind": "sessionNotFound", "retryable": False, "sessionId": sid},
                               "message": f"session {sid} not found"}, "id": rid})
                continue
            SESSION["id"] = sid
            start_mcp(params.get("config"))
            out({"id": rid, "result": {"session": session(sid, kept["workspace"], kept.get("activeTurnId")), "pendingRequests": [],
                                       "history": {"mode": "none", "items": []}, "viewCursor": f"v4:{sid}:15"}})
            if kept.get("pending"):
                # Re-issued on the resume, with `judgeEscalated` false.
                NEXT[0] += 1
                out({"id": NEXT[0], "method": "approval/request", "params": dict(kept["pending"], judgeEscalated=False)})
        elif method == "session/setApprovalMode":
            SESSION["approval"] = params["mode"]
            out({"id": rid, "result": {"commandId": params["commandId"], "outcome": "completed", "status": "accepted"}})
        elif method == "turn/cancel":
            keep(params["sessionId"], activeTurnId=None, pending=None)
            out({"id": rid, "result": {"commandId": params["commandId"], "status": "accepted", "turnId": params["turnId"]}})
            out({"method": "turn/completed", "params": {"sessionId": params["sessionId"], "terminal": "cancelled", "turnId": params["turnId"]}})
        elif method == "turn/start":
            # t2 line 44.
            out({"id": rid, "result": {"commandId": params["commandId"], "disposition": "started", "startedNewTurn": True,
                                       "status": "accepted", "turnId": params["commandId"]}})
            turn(params["sessionId"], params["commandId"], params["input"][0]["text"])
        elif rid is not None and method is not None:
            out({"error": {"code": -32601, "data": {"kind": "methodNotFound", "retryable": False}, "message": f"no method {method}"}, "id": rid})


if __name__ == "__main__":
    main()
