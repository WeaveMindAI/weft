"""A stand-in for `copilot --headless --stdio`: the Copilot SDK's protocol,
JSON-RPC framed by `Content-Length` headers, replayed from what GitHub
Copilot CLI 1.0.105 really wrote (recorded against a local stand-in model
provider, no account): the same requests, the same events in the same
order, the same shapes. It holds none of Copilot's logic beyond that.

Each turn's message says what its model does, by keyword, in this order:

  "ask"          asks with its `ask_user` tool (`userInput.request`,
                 between `user_input.requested` and `user_input.completed`)
  "run"          runs `touch made.txt`, asking first (`permission.requested`,
                 a shell command) unless the permission mode is `allow-all`
  "call:<name>"  calls the tool <name>: one of the session's `tools`
                 (`external_tool.requested`, answered with
                 `session.tools.handlePendingToolCall`), else a built-in it
                 runs itself
  "sub:<name>"   a sub-agent (`task`) calls the tool <name>, its events
                 carrying an `agentId`, then says "sub done"
  "think"        reasons first (`assistant.reasoning`)
  "abandon"      calls the session's first tool and gives up on it at once
                 (`external_tool.completed` with no answer), then goes on
  "fail"         ends the turn with a `session.error`
  "crash"        exits in the middle of the turn

and replies with what it was answered, one line each, streamed in two
`assistant.message_delta` pieces when the session streams.

`session.tools.initializeAndValidate` refuses a tool named like a built-in
without `overridesBuiltInTool`, with Copilot's own error. A session lives
in `$COPILOT_HOME/session-state/<id>.json` (`$FAKE_LOG`'s folder without a
home), so a stand-in started again resumes it, and an unknown id is
refused as Copilot refuses it. A connection's token (`--auth-token-env`)
that is "bad" fails `session.create` with GitHub's 401.

A plan's models (`$FAKE_COPILOT_PLAN`, comma-separated, a Free plan's
own when set): a session whose model is not one of them has each turn
routed by Copilot's auto mode (`session.auto_mode_resolved`, as 1.0.105
wrote it on a Free plan, before the message), and goes on with the plan's
first model. It reads nothing while a turn runs, so a `session.abort`
reaches it once the turn is over, and is answered as Copilot 1.0.105
answers one with nothing running: success, and no event.

`$FAKE_LOG` gets the start (`{"start": {args, env}}`) and every message it
was sent, one JSON line each.
"""

import json
import os
import sys
import uuid

# What `session.tools.getBuiltinDescriptors` listed on 1.0.105.
BUILT_IN = ["bash", "read_bash", "stop_bash", "list_bash", "fetch_copilot_cli_documentation", "ask_user",
            "dynamic_workflows_manage", "context_board", "exit_plan_mode", "read_inbox", "run_dynamic_workflow", "list_agents",
            "manage_schedule", "read_agent", "apply_patch", "create", "edit", "str_replace_editor", "send_inbox", "sql",
            "skill", "task_complete", "generic_tool_search", "tool_search_tool", "update_todo", "view", "web_fetch",
            "write_agent", "semantic_code_search", "grep", "glob", "task"]

STDIN = sys.stdin.buffer
STDOUT = sys.stdout.buffer
NEXT = [0]
SESSION = {}


def logged(entry):
    with open(os.environ["FAKE_LOG"], "a") as f:
        f.write(json.dumps(entry) + "\n")


def out(message):
    body = json.dumps(dict(message, jsonrpc="2.0")).encode()
    STDOUT.write(b"Content-Length: %d\r\n\r\n" % len(body) + body)
    STDOUT.flush()


def read():
    length = None
    while True:
        line = STDIN.readline()
        if not line:
            sys.exit(0)
        line = line.strip()
        if not line and length is not None:
            break
        if line.lower().startswith(b"content-length:"):
            length = int(line.split(b":")[1])
    message = json.loads(STDIN.read(length))
    logged(message)
    return message


def event(kind, data, agent=None, ephemeral=False):
    sent = {"type": kind, "data": data, "id": str(uuid.uuid4()), "timestamp": "2026-11-05T11:14:46.577Z", "parentId": None}
    if ephemeral:
        sent["ephemeral"] = True
    if agent:
        sent["agentId"] = agent
    out({"method": "session.event", "params": {"sessionId": SESSION["id"], "event": sent}})


def answer_of(rid):
    """Read until the answer to our request `rid`; whatever else comes first
    (weft's answers to the session's pending work) is handled on the way."""
    while True:
        message = read()
        if message.get("id") == rid and "method" not in message:
            return message
        handle(message)


def ask(method, params):
    NEXT[0] += 1
    out({"id": NEXT[0], "method": method, "params": params})
    return answer_of(NEXT[0])


# The session's pending work weft answers by RPC, by request id:
# its answer, once it came.
PENDING = {}


def wait_pending(request_id):
    while request_id not in PENDING or PENDING[request_id] is None:
        handle(read())
    return PENDING.pop(request_id)


def path():
    home = os.environ.get("COPILOT_HOME") or os.path.join(os.path.dirname(os.environ["FAKE_LOG"]), "copilot-home")
    return os.path.join(home, "session-state", f"{SESSION['id']}.json")


def save():
    os.makedirs(os.path.dirname(path()), exist_ok=True)
    with open(path(), "w") as f:
        json.dump({k: v for k, v in SESSION.items()}, f)


def tool_call(name, arguments, call_id, agent=None, parent=None):
    """One tool call of the model's (or a sub-agent's): what it was answered."""
    extra = {"parentToolCallId": parent} if parent else {}
    event("tool.execution_start", {"toolCallId": call_id, "toolName": name, "arguments": arguments, "turnId": "0", **extra}, agent)
    if name in [t["name"] for t in SESSION["tools"]]:
        request_id = str(uuid.uuid4())
        PENDING[request_id] = None
        event("external_tool.requested", {"requestId": request_id, "sessionId": SESSION["id"], "toolCallId": call_id,
                                          "toolName": name, "arguments": arguments}, agent, ephemeral=True)
        given = wait_pending(request_id)
        event("external_tool.completed", {"requestId": request_id}, ephemeral=True)
        out({"id": given["id"], "result": {"success": True}})
        params = given["params"]
        if "error" in params:
            text, success = params["error"], False
        else:
            result = params["result"]
            text = result if isinstance(result, str) else result.get("textResultForLlm", "")
            success = not isinstance(result, dict) or result.get("resultType", "success") == "success"
    else:
        text, success = f"built-in {name} ran", True
    done = {"toolCallId": call_id, "success": success, "turnId": "0", **extra}
    done.update({"result": {"content": text, "detailedContent": text}} if success else {"error": {"message": text, "code": "failure"}})
    event("tool.execution_complete", done, agent)
    return f"{name}: {text}" if success else f"{name} failed: {text}"


def routed():
    """Copilot's auto mode picking the turn's model, when the plan does not offer the session's."""
    plan = [m for m in os.environ.get("FAKE_COPILOT_PLAN", "").split(",") if m]
    if not plan or SESSION.get("model") in plan:
        return
    event("session.auto_mode_resolved", {
        "chosenModel": plan[0], "selectionReason": f"Auto selected {plan[0]} because it was the best choice out of the models "
                                                   "available in your plan.",
        "categoryScores": {"tool_use": 0.0034, "debugging": 0.0023, "code_gen": 0.0047, "reasoning": 0.0208},
        "candidateModels": plan, "routingMethod": "auto_v2", "fallback": False, "availableModels": plan,
        "endToEndLatencyMs": 183.3, "hasImage": False})


def turn(prompt, message_id):
    routed()
    event("user.message", {"content": prompt, "messageId": message_id, "turnId": "0"})
    event("assistant.turn_start", {"turnId": "0"})
    lines = []
    words = prompt.split()
    if "think" in words:
        event("assistant.reasoning", {"reasoningId": "r1", "content": "thinking it over"})
    if "fail" in words:
        event("session.error", {"errorType": "query", "message": "Execution failed: the model refused"})
        event("session.idle", {"mode": SESSION["mode"]}, ephemeral=True)
        return
    if "ask" in words:
        event("tool.execution_start", {"toolCallId": "call_ask", "toolName": "ask_user",
                                       "arguments": {"question": "Red or blue?", "choices": ["Red", "Blue"]}})
        request_id = str(uuid.uuid4())
        event("user_input.requested", {"requestId": request_id, "question": "Red or blue?", "choices": ["Red", "Blue"],
                                       "allowFreeform": True, "toolCallId": "call_ask"}, ephemeral=True)
        given = ask("userInput.request", {"sessionId": SESSION["id"], "question": "Red or blue?", "choices": ["Red", "Blue"],
                                          "allowFreeform": True})
        got = given.get("result") or {}
        event("user_input.completed", {"requestId": request_id, "answer": got.get("answer"), "wasFreeform": got.get("wasFreeform")},
              ephemeral=True)
        text = f"User selected: {got.get('answer')}" if "result" in given else f"refused: {given['error']['message']}"
        event("tool.execution_complete", {"toolCallId": "call_ask", "success": True, "result": {"content": text, "detailedContent": text}})
        lines.append(f"ask: {json.dumps(got)}")
    if "run" in words:
        event("tool.execution_start", {"toolCallId": "call_run", "toolName": "bash",
                                       "arguments": {"command": "touch made.txt", "description": "make a file"}})
        decision = {"kind": "approved"}
        if SESSION["permissionMode"] != "allow-all":
            request_id = str(uuid.uuid4())
            PENDING[request_id] = None
            event("permission.requested", {
                "requestId": request_id,
                "permissionRequest": {"kind": "shell", "toolCallId": "call_run", "fullCommandText": "touch made.txt",
                                      "intention": "make a file", "commands": [{"identifier": "touch made.txt", "readOnly": False}],
                                      "canOfferSessionApproval": False},
                "promptRequest": {"kind": "commands", "fullCommandText": "touch made.txt", "intention": "make a file",
                                  "commandIdentifiers": ["touch made.txt"], "canOfferSessionApproval": False, "toolCallId": "call_run"},
                "agentMode": SESSION["mode"], "permissionMode": SESSION["permissionMode"]}, ephemeral=True)
            given = wait_pending(request_id)
            decision = given["params"]["result"]
            event("permission.completed", {"requestId": request_id, "toolCallId": "call_run", "result": decision}, ephemeral=True)
            out({"id": given["id"], "result": {"success": True}})
        if decision["kind"] in ("reject", "user-not-available"):
            event("tool.execution_complete", {"toolCallId": "call_run", "success": False,
                                              "error": {"message": f"The user rejected this tool call. User feedback: {decision.get('feedback')}",
                                                        "code": "denied"}})
        else:
            event("tool.execution_complete", {"toolCallId": "call_run", "success": True,
                                              "result": {"content": "<exited with exit code 0>"},
                                              "fileEdits": [{"path": "made.txt", "kind": "create"}]})
        lines.append(f"run: {json.dumps(decision)}")
    for index, word in enumerate(words):
        if word.startswith("call:"):
            lines.append(tool_call(word[5:], {"q": "x"}, f"call_{index}"))
        if word.startswith("sub:"):
            agent = "agent-" + uuid.uuid4().hex[:8]
            event("tool.execution_start", {"toolCallId": f"call_{index}", "toolName": "task", "arguments": {"prompt": "go"}})
            event("subagent.started", {"toolCallId": f"call_{index}", "agentName": "general-purpose"}, agent)
            tool_call(word[4:], {"q": "from sub"}, f"call_{index}_sub", agent, parent=f"call_{index}")
            event("assistant.message", {"messageId": str(uuid.uuid4()), "content": "sub done", "toolRequests": [],
                                        "parentToolCallId": f"call_{index}"}, agent)
            event("tool.execution_complete", {"toolCallId": f"call_{index}", "success": True, "result": {"content": "sub done"}})
            lines.append("sub: sub done")
    if "abandon" in words:
        call_id, request_id = "call_abandoned", str(uuid.uuid4())
        name = SESSION["tools"][0]["name"]
        event("tool.execution_start", {"toolCallId": call_id, "toolName": name, "arguments": {}})
        event("external_tool.requested", {"requestId": request_id, "sessionId": SESSION["id"], "toolCallId": call_id,
                                          "toolName": name, "arguments": {}}, ephemeral=True)
        event("external_tool.completed", {"requestId": request_id}, ephemeral=True)
        event("tool.execution_complete", {"toolCallId": call_id, "success": False, "error": {"message": "abandoned", "code": "failure"}})
        lines.append("abandoned")
    if "crash" in words:
        sys.exit(3)
    reply = "\n".join(lines) or "done"
    message = str(uuid.uuid4())
    if SESSION.get("streaming"):
        half = len(reply) // 2
        event("assistant.message_start", {"messageId": message}, ephemeral=True)
        event("assistant.message_delta", {"messageId": message, "deltaContent": reply[:half]}, ephemeral=True)
        event("assistant.message_delta", {"messageId": message, "deltaContent": reply[half:]}, ephemeral=True)
    event("assistant.message", {"messageId": message, "content": reply, "toolRequests": []})
    event("assistant.turn_end", {"turnId": "0"})
    event("assistant.idle", {}, ephemeral=True)
    event("session.idle", {"mode": SESSION["mode"]}, ephemeral=True)
    SESSION["turns"] = SESSION.get("turns", 0) + 1
    save()


def open_session(rid, params, resume):
    if resume:
        SESSION["id"] = params["sessionId"]
        if not os.path.exists(path()):
            out({"id": rid, "error": {"code": -32603, "message": "Request session.resume failed with message: Failed to load "
                                                                 f"session events: Session not found: {params['sessionId']}"}})
            return
        with open(path()) as f:
            SESSION.update(json.load(f))
    else:
        token_env = flag("--auth-token-env")
        if token_env and os.environ.get(token_env) == "bad":
            out({"id": rid, "error": {"code": -32603, "message": "Request session.create failed with message: Authentication "
                                                                 "failed: Failed to validate SDK token (401): GitHub returned: Bad credentials"}})
            return
        SESSION.update(id=str(uuid.uuid4()), mode="interactive", permissionMode="manual")
    SESSION.update(tools=params.get("tools") or [], streaming=bool(params.get("streaming")), model=params.get("model"))
    save()
    out({"id": rid, "result": {"sessionId": SESSION["id"], "nativeSessionId": SESSION["id"], "isRemote": False,
                               "mode": "interactive", "capabilities": {"ui": {"elicitation": False}}, "modelState": {}}})
    event("session.resume" if resume else "session.start", {"sessionId": SESSION["id"], "copilotVersion": "1.0.105"})


def flag(name):
    args = sys.argv[1:]
    return args[args.index(name) + 1] if name in args else None


def handle(message):
    method, rid, params = message.get("method"), message.get("id"), message.get("params") or {}
    if method == "connect":
        out({"id": rid, "result": {"ok": True, "protocolVersion": int(os.environ.get("FAKE_PROTOCOL", "3")), "version": "1.0.105"}})
    elif method in ("session.create", "session.resume"):
        open_session(rid, params, method == "session.resume")
    elif method == "session.mode.set":
        previous, SESSION["mode"] = SESSION["mode"], params["mode"]
        event("session.mode_changed", {"previousMode": previous, "newMode": params["mode"]})
        out({"id": rid, "result": {"status": "unchanged", "modelChanged": False, "modeApplied": True}})
        save()
    elif method == "session.permissions.setMode":
        SESSION["permissionMode"] = params["mode"]
        out({"id": rid, "result": {"success": True, "mode": params["mode"]}})
        save()
    elif method == "session.tools.initializeAndValidate":
        for tool in SESSION["tools"]:
            if tool["name"] in BUILT_IN and not tool.get("overridesBuiltInTool"):
                out({"id": rid, "error": {"code": -32603, "message": "Request session.tools.initializeAndValidate failed with "
                                                                     f"message: GenericFailure, External tool \"{tool['name']}\" "
                                                                     "conflicts with a built-in tool of the same name. Set "
                                                                     "overridesBuiltInTool: true to explicitly override it."}})
                return
        out({"id": rid, "result": {}})
    elif method == "session.send":
        message_id = str(uuid.uuid4())
        event("pending_messages.modified", {}, ephemeral=True)
        out({"id": rid, "result": {"messageId": message_id}})
        turn(params["prompt"], message_id)
    elif method == "session.abort":
        out({"id": rid, "result": {"success": True}})
    elif method in ("session.tools.handlePendingToolCall", "session.permissions.handlePendingPermissionRequest"):
        if params.get("requestId") in PENDING:
            # Its answer goes out once the pending work has moved on.
            PENDING[params["requestId"]] = message
        else:
            out({"id": rid, "result": {"success": False}})
    elif rid is not None and method is not None:
        out({"id": rid, "error": {"code": -32601, "message": f"Unhandled method {method}"}})


def main():
    logged({"start": {"args": sys.argv[1:], "env": {k: v for k, v in os.environ.items() if k.startswith(("COPILOT", "GH_", "GITHUB"))}}})
    while True:
        handle(read())


if __name__ == "__main__":
    main()
