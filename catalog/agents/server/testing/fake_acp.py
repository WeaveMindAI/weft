"""A stand-in for an agent speaking the Agent Client Protocol over stdio.

The shapes are ACP's (protocol version 1): `initialize`, `session/new`,
`session/load` (which replays the conversation as updates first),
`session/set_mode`, `session/prompt` answered with a `stopReason`, and
`session/update` notifications on the way. By keyword in the prompt:

  "edit"   asks `session/request_permission` for an edit, then reports a diff
  "tool"   calls the program's `lookup` tool over the MCP server it was handed

`$FAKE_LOG` gets every message it was sent, one JSON line each.
"""

import json
import os
import sys
import urllib.request
import uuid

NEXT = [500]
SERVERS = {}


def out(message):
    sys.stdout.write(json.dumps({"jsonrpc": "2.0", **message}) + "\n")
    sys.stdout.flush()


def read():
    line = sys.stdin.readline()
    if not line:
        sys.exit(0)
    message = json.loads(line)
    with open(os.environ["FAKE_LOG"], "a") as f:
        f.write(json.dumps(message) + "\n")
    return message


def update(session, body):
    out({"method": "session/update", "params": {"sessionId": session, "update": body}})


def prompt(rid, session, text):
    update(session, {"sessionUpdate": "agent_message_chunk", "content": {"type": "text", "text": "on "}})
    update(session, {"sessionUpdate": "agent_message_chunk", "content": {"type": "text", "text": "it"}})
    if "edit" in text:
        call = {"toolCallId": "call-1", "title": "Edit notes.md", "kind": "edit", "status": "pending",
                "locations": [{"path": "/w/notes.md"}], "rawInput": {"path": "/w/notes.md"}}
        update(session, {"sessionUpdate": "tool_call", **call})
        NEXT[0] += 1
        out({"id": NEXT[0], "method": "session/request_permission", "params": {"sessionId": session, "toolCall": call, "options": [
            {"optionId": "allow-once", "name": "Allow", "kind": "allow_once"},
            {"optionId": "allow-always", "name": "Always allow", "kind": "allow_always"},
            {"optionId": "reject-once", "name": "Reject", "kind": "reject_once"}]}})
        while True:
            answer = read()
            if answer.get("id") == NEXT[0] and "method" not in answer:
                break
        picked = answer["result"]["outcome"]["optionId"]
        content = [{"type": "diff", "path": "/w/notes.md", "oldText": "a", "newText": "b"}] if picked.startswith("allow") else []
        update(session, {"sessionUpdate": "tool_call_update", "toolCallId": "call-1",
                         "status": "completed" if picked.startswith("allow") else "failed", "content": content})
        update(session, {"sessionUpdate": "agent_message_chunk", "content": {"type": "text", "text": f"picked {picked}"}})
    if "tool" in text:
        url = SERVERS[session]
        body = json.dumps({"jsonrpc": "2.0", "id": 1, "method": "tools/call", "params": {"name": "lookup", "arguments": {}}}).encode()
        request = urllib.request.Request(url, data=body, method="POST", headers={"Content-Type": "application/json"})
        with urllib.request.urlopen(request, timeout=None) as answer:
            result = json.loads(answer.read())["result"]
        update(session, {"sessionUpdate": "agent_message_chunk", "content": {"type": "text", "text": result["content"][0]["text"]}})
    out({"id": rid, "result": {"stopReason": "end_turn"}})


def main():
    while True:
        message = read()
        method, rid, params = message.get("method"), message.get("id"), message.get("params") or {}
        if method == "initialize":
            out({"id": rid, "result": {"protocolVersion": 1, "agentCapabilities": {"loadSession": True, "mcpCapabilities": {"http": True}},
                                       "authMethods": []}})
        elif method in ("session/new", "session/load"):
            session = params.get("sessionId") or str(uuid.uuid4())
            for server in params.get("mcpServers") or []:
                SERVERS[session] = server.get("url")
            if method == "session/load":
                update(session, {"sessionUpdate": "user_message_chunk", "content": {"type": "text", "text": "an earlier message"}})
                out({"id": rid, "result": {}})
            else:
                out({"id": rid, "result": {"sessionId": session, "modes": {"currentModeId": "default",
                                                                          "availableModes": [{"id": "default"}, {"id": "yolo"}]}}})
        elif method == "session/set_mode":
            out({"id": rid, "result": {}})
        elif method == "session/prompt":
            prompt(rid, params["sessionId"], params["prompt"][0]["text"])
        elif rid is not None and method:
            out({"id": rid, "error": {"code": -32601, "message": f"no {method}"}})


if __name__ == "__main__":
    main()
