"""A stand-in for `codex app-server`: JSON-RPC over stdio, one object per line.

It answers the requests the adapter makes with the shapes `codex app-server
generate-json-schema` describes (and real runs showed), and does what each
turn's message asks, by keyword:

  "run"      asks to approve a command (`item/commandExecution/requestApproval`)
  "ask"      asks a question (`item/tool/requestUserInput`)
  "tool"     calls the program's `lookup` tool (`item/tool/call`), and ends
             its `dynamicToolCall` item `failed` when the answer is
             `success: false`, as Codex does
  "abandon"  calls it and gives up on it at once, as Codex does after about
             twenty minutes (a failed `dynamicToolCall` item, with the id of
             the call's `callId`), then ends the turn
  "strays"   calls it and gives up on a call of another id
  "nocallid" calls it with no `callId`
  "expired"  its ChatGPT token was refused: asks this side for a fresh one
             (`account/chatgptAuthTokens/refresh`) and replies with the token
             it got, or fails the turn with the refusal
  "whoami"   replies with the ChatGPT token it holds
  "fail"     ends the turn failed
  "crash"    exits in the middle of the turn
  "search"   calls the program's `web_search` (`item/tool/call`)

Tool names, as codex-cli 0.166.0 has them: `thread/start` refuses a dynamic
tool named `mcp` or starting `mcp__`; a dynamic tool named like a built-in
it still has (`BUILT_INS`, less those the thread's `config` turns off) fails
the turn "duplicate tool" under
`features.tool_registry.error_on_tool_collisions`, and is dropped without it.

`$FAKE_LOG` gets every request it was sent, one JSON line each.
"""

import json
import os
import sys
import uuid

NEXT = [1000]
# The ChatGPT token it was logged in with (`chatgptAuthTokens`), as Codex
# keeps it: in memory, with no `auth.json` written.
TOKEN = [None]

# A few of Codex's built-ins, each with the setting that turns it off (None: none does).
BUILT_INS = {"exec_command": None, "apply_patch": None, "view_image": ("features.view_image", False),
             "web_search": ("web_search", "disabled")}
COLLISIONS = "features.tool_registry.error_on_tool_collisions"


def out(message):
    sys.stdout.write(json.dumps(message) + "\n")
    sys.stdout.flush()


def logged(message):
    with open(os.environ["FAKE_LOG"], "a") as f:
        f.write(json.dumps(message) + "\n")


def read():
    line = sys.stdin.readline()
    if not line:
        sys.exit(0)
    message = json.loads(line)
    logged(message)
    return message


def ask(method, params):
    """A request of ours; the answer, skipping anything else on the way."""
    NEXT[0] += 1
    rid = NEXT[0]
    out({"id": rid, "method": method, "params": params})
    while True:
        message = read()
        if message.get("id") == rid and "method" not in message:
            return message


def dynamic_call(thread, turn_id, call_id, tool, arguments):
    """A call of the program's tool as codex-cli 0.166.0 makes it in a real
    turn: its `dynamicToolCall` item started, `item/tool/call`, the item
    completed (`failed` when the answer is `success: false`). The answer."""
    item = {"type": "dynamicToolCall", "id": call_id, "namespace": None, "tool": tool, "arguments": arguments}
    out({"method": "item/started", "params": {"item": dict(item, status="inProgress", contentItems=None, success=None),
                                              "threadId": thread, "turnId": turn_id}})
    answer = ask("item/tool/call", {"threadId": thread, "turnId": turn_id, "callId": call_id, "namespace": None, "tool": tool,
                                    "arguments": arguments})
    result = answer["result"]
    out({"method": "item/completed", "params": {"item": dict(item, status="completed" if result["success"] else "failed",
                                                             contentItems=result["contentItems"], success=result["success"]),
                                                "threadId": thread, "turnId": turn_id}})
    return answer


def turn(thread, turn_id, text, params):
    out({"method": "turn/started", "params": {"threadId": thread, "turn": {"id": turn_id}}})
    config = params.get("config") or {}
    for tool in params.get("dynamicTools") or []:
        off = BUILT_INS.get(tool["name"], ())
        if tool["name"] in BUILT_INS and (off is None or config.get(off[0]) != off[1]) and config.get(COLLISIONS):
            error = {"message": f"duplicate tool: functions.{tool['name']}"}
            out({"method": "turn/completed", "params": {"threadId": thread, "turn": {"id": turn_id, "status": "failed", "error": error}}})
            return
    reply = "done"
    if "run" in text:
        item = "exec-1"
        out({"method": "item/started", "params": {"item": {"type": "commandExecution", "id": item, "command": "date"}}})
        answer = ask("item/commandExecution/requestApproval", {"threadId": thread, "turnId": turn_id, "itemId": item, "command": "date", "cwd": "/w"})
        decision = answer["result"]["decision"]
        out({"method": "item/completed", "params": {"item": {"type": "commandExecution", "id": item, "command": "date",
                                                             "aggregatedOutput": "Tue" if decision == "accept" else "", "exitCode": 0}}})
        reply = decision
    if "ask" in text:
        answer = ask("item/tool/requestUserInput", {"threadId": thread, "turnId": turn_id, "itemId": "q", "isBlocking": True,
                                                    "questions": [{"id": "colour", "header": "Colour", "question": "Red or blue?", "isOther": True,
                                                                   "isSecret": False, "options": [{"label": "Red", "description": ""}, {"label": "Blue", "description": ""}]}]})
        reply = json.dumps(answer["result"])
    if "tool" in text:
        answer = dynamic_call(thread, turn_id, "exec-tool", "lookup", {"key": "x"})
        reply = json.dumps(answer["result"])
    if "abandon" in text or "strays" in text:
        NEXT[0] += 1
        out({"id": NEXT[0], "method": "item/tool/call",
             "params": {"threadId": thread, "turnId": turn_id, "callId": "exec-tool", "tool": "lookup", "arguments": {}}})
        given_up = "exec-tool" if "abandon" in text else "another-call"
        out({"method": "item/completed", "params": {"item": {"type": "dynamicToolCall", "id": given_up, "tool": "lookup",
                                                             "arguments": {}, "status": "failed", "contentItems": None}}})
    if "search" in text:
        answer = dynamic_call(thread, turn_id, "exec-search", "web_search", {"q": "sky"})
        reply = answer["result"]["contentItems"][0]["text"]
    if "nocallid" in text:
        answer = ask("item/tool/call", {"threadId": thread, "turnId": turn_id, "tool": "lookup", "arguments": {}})
        reply = json.dumps(answer.get("error"))
    if "expired" in text:
        answer = ask("account/chatgptAuthTokens/refresh", {"reason": "unauthorized", "previousAccountId": "acct-1"})
        if "error" in answer:
            error = {"message": f"unexpected status 401 Unauthorized: {answer['error']['message']}"}
            out({"method": "turn/completed", "params": {"threadId": thread, "turn": {"id": turn_id, "status": "failed", "error": error}}})
            return
        TOKEN[0] = answer["result"]["accessToken"]
        reply = TOKEN[0]
    if "whoami" in text:
        reply = TOKEN[0] or "not logged in"
    if "crash" in text:
        sys.exit(3)
    out({"method": "item/agentMessage/delta", "params": {"itemId": "m1", "delta": reply[:3]}})
    out({"method": "item/completed", "params": {"item": {"type": "agentMessage", "id": "m1", "text": reply}}})
    status = "failed" if "fail" in text else "completed"
    error = {"message": "the model refused"} if status == "failed" else None
    out({"method": "turn/completed", "params": {"threadId": thread, "turn": {"id": turn_id, "status": status, "error": error}}})


def main():
    thread, thread_params = None, {}
    while True:
        message = read()
        method, rid, params = message.get("method"), message.get("id"), message.get("params") or {}
        if method == "initialize":
            out({"id": rid, "result": {"userAgent": "fake", "codexHome": os.environ.get("CODEX_HOME", "~/.codex")}})
        elif method == "account/read":
            out({"id": rid, "result": {"account": {"type": "chatgpt"}, "requiresOpenaiAuth": True}})
        elif method == "account/login/start":
            if params.get("type") == "chatgptAuthTokens":
                TOKEN[0] = params["accessToken"]
            out({"id": rid, "result": {"type": params.get("type")}})
        elif method == "model/list":
            out({"id": rid, "result": {"data": [{"model": "small"}, {"model": "big", "isDefault": True}]}})
        elif method in ("thread/start", "thread/resume"):
            reserved = [t["name"] for t in params.get("dynamicTools") or [] if t["name"] == "mcp" or t["name"].startswith("mcp__")]
            if reserved:
                out({"id": rid, "error": {"code": -32600, "message": f"dynamic tool name is reserved: {reserved[0]}"}})
                continue
            thread_params = params
            thread = params["threadId"] if method == "thread/resume" else str(uuid.uuid4())
            out({"id": rid, "result": {"thread": {"id": thread}}})
        elif method == "turn/start":
            turn_id = str(uuid.uuid4())
            out({"id": rid, "result": {"turn": {"id": turn_id}}})
            turn(thread, turn_id, params["input"][0]["text"], thread_params)


if __name__ == "__main__":
    main()
