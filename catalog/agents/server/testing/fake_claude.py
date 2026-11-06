"""A stand-in for `claude -p --input-format stream-json --output-format stream-json`.

It speaks the lines Claude Code writes, recorded from real runs (2.1.287),
and does what the message asks, by keyword, so a test can drive every path
without an account:

  "bash"     asks permission to run Bash (`can_use_tool`), then runs it or not
  "ask"      asks with AskUserQuestion, and replies with the answers it got
  "tool"     calls the program's `lookup` tool through the sdk MCP server it
             was given (a `control_request` of subtype `mcp_message`)
  "twice"    calls it twice side by side, as Claude Code runs parallel tool calls
  "search"   calls `WebSearch`: the program's tool of that name when the
             program has one and the built-in is disallowed, else the built-in
             (which wins over a program's tool of its name, as in Claude Code)
  "crash"    exits in the middle of the turn
  "helper"   first starts a long-sleeping helper that holds its stdout and
             stderr (as an MCP server or a tool's shell does), left running
  "escapes"  the same helper, in a session of its own (a daemon), its pid
             written to `helper.pid` in the folder it runs in
  "slow"     waits for a line on stdin before it ends the turn (an interrupt)

The sdk MCP server, as Claude Code drives it: it waits for the `initialize`
control request, and for each server it declares (`sdkMcpServers`) sends
`initialize`, `notifications/initialized` and `tools/list` as `mcp_message`
control requests before its init line. A tool's name is plain with
`CLAUDE_AGENT_SDK_MCP_NO_PREFIX`, `mcp__<server>__<tool>` without it.
`--disallowedTools` removes a built-in of each name it lists; `ToolSearch`
of the program never reaches the model, as in Claude Code.

Everything it was started with goes to `$FAKE_LOG` as one JSON line (when it
is set), so a test can read the command line and the environment it got.
"""

import json
import os
import subprocess
import sys
import uuid

# The built-ins this stand-in has (a few of Claude Code's).
BUILT_INS = ["Task", "AskUserQuestion", "Bash", "Read", "Edit", "Write", "WebSearch", "ToolSearch"]
# The one a program's tool cannot take the place of.
NEVER_REPLACED = {"ToolSearch"}

# Lines read while waiting for another (`answer_to`), handed out next.
WAITING_LINES = []


def out(message):
    sys.stdout.write(json.dumps(message) + "\n")
    sys.stdout.flush()


def read():
    if WAITING_LINES:
        return WAITING_LINES.pop(0)
    line = sys.stdin.readline()
    if not line:
        sys.exit(0)
    return json.loads(line)


def answer_to(request_ids):
    """The control responses to `request_ids`, by id, whatever order they come in."""
    wanted, got, other = set(request_ids), {}, []
    while wanted - set(got):
        line = sys.stdin.readline()
        if not line:
            sys.exit(0)
        message = json.loads(line)
        rid = (message.get("response") or {}).get("request_id") if message.get("type") == "control_response" else None
        if rid in wanted:
            got[rid] = message["response"]
        else:
            other.append(message)
    WAITING_LINES.extend(other)
    return got


def control(tool, tool_input, suggestions=None, server=None):
    request_id = str(uuid.uuid4())
    request = {"subtype": "can_use_tool", "tool_name": tool, "input": tool_input}
    if suggestions:
        request["permission_suggestions"] = suggestions
    if server:
        request["mcp_server"] = {"name": server, "source": "sdk"}
    out({"type": "control_request", "request_id": request_id, "request": request})
    return answer_to([request_id])[request_id]["response"]


def mcp_send(server, message):
    request_id = str(uuid.uuid4())
    out({"type": "control_request", "request_id": request_id,
         "request": {"subtype": "mcp_message", "server_name": server, "message": message}})
    return request_id


def mcp(server, method, params, rid):
    request_id = mcp_send(server, {"jsonrpc": "2.0", "id": rid, "method": method, "params": params})
    return answer_to([request_id])[request_id]["response"]["mcp_response"]


def main():
    args = sys.argv[1:]
    # The unit tests read what it was started with; a run through weft
    # (the e2e) has no log to write to.
    if os.environ.get("FAKE_LOG"):
        with open(os.environ["FAKE_LOG"], "a") as f:
            f.write(json.dumps({"args": args, "env": {k: v for k, v in os.environ.items() if k in (
                "CLAUDE_CODE_OAUTH_TOKEN", "ANTHROPIC_API_KEY", "PATH", "HOME", "CLAUDE_AGENT_SDK_MCP_NO_PREFIX",
                "MCP_TOOL_TIMEOUT")}}) + "\n")
    session = args[args.index("--resume") + 1] if "--resume" in args else str(uuid.uuid4())
    disallowed = set(args[args.index("--disallowedTools") + 1].split(",")) if "--disallowedTools" in args else set()
    allowed = set(args[args.index("--allowedTools") + 1].split(",")) if "--allowedTools" in args else set()
    servers = json.loads(args[args.index("--mcp-config") + 1])["mcpServers"] if "--mcp-config" in args else {}
    plain = os.environ.get("CLAUDE_AGENT_SDK_MCP_NO_PREFIX") == "1"
    # The initialize control request comes first; it declares the sdk servers.
    first = read()
    if first.get("type") != "control_request" or first["request"].get("subtype") != "initialize":
        sys.stderr.write(f"fake claude: the first line was not the initialize control request: {first}\n")
        sys.exit(2)
    declared = first["request"].get("sdkMcpServers") or []
    for name, server in servers.items():
        if server.get("type") == "sdk" and name not in declared:
            sys.stderr.write(f"fake claude: --mcp-config names sdk server {name}, which initialize never declared\n")
            sys.exit(2)
    out({"type": "control_response", "response": {"subtype": "success", "request_id": first["request_id"], "response": {"commands": []}}})
    program = {}
    for name in declared:
        mcp(name, "initialize", {"protocolVersion": "2025-11-25", "capabilities": {}, "clientInfo": {"name": "claude-code"}}, 0)
        mcp(name, "notifications/initialized", {}, None)
        for tool in mcp(name, "tools/list", {}, 1)["result"]["tools"]:
            program[tool["name"] if plain else f"mcp__{name}__{tool['name']}"] = (name, tool["name"])
    tools = [t for t in BUILT_INS if t not in disallowed]
    tools += [shown for shown, (_, raw) in program.items() if shown not in tools and raw not in NEVER_REPLACED]
    out({"type": "system", "subtype": "init", "session_id": session, "tools": tools,
         "mcp_servers": [{"name": name, "status": "connected", "source": "sdk"} for name in declared]})

    def call_program(shown, arguments, rid):
        """Call the program's tool `shown` as the model would: the tool_use,
        the permission check unless a rule allows the server's tools (on
        2.1.287 `--allowedTools mcp__<server>` sent no `can_use_tool` for
        them), the MCP call."""
        server, raw = program[shown]
        use = f"toolu_{rid}"
        out({"type": "assistant", "session_id": session, "message": {"content": [
            {"type": "tool_use", "id": use, "name": shown, "input": arguments, "caller": {"type": "direct"}}]}})
        if f"mcp__{server}" not in allowed:
            control(shown, arguments, server=server)
        result = mcp(server, "tools/call", {"name": raw, "arguments": arguments, "_meta": {"claudecode/toolUseId": use}}, rid)["result"]
        out({"type": "user", "session_id": session, "message": {"content": [{"type": "tool_result", "tool_use_id": use, "content": result["content"]}]}})
        return result

    while True:
        message = read()
        if message.get("type") != "user":
            continue
        text = message["message"]["content"]
        # Lines no reader of weft's acts on, as 2.1.287 wrote them on a subscription login (trimmed).
        out({"type": "rate_limit_event", "rate_limit_info": {"status": "allowed", "rateLimitType": "seven_day", "isUsingOverage": False},
             "session_id": session})
        out({"type": "assistant", "session_id": session, "message": {"content": [{"type": "text", "text": f"on it: {text}"}]}})
        reply = "done"
        if "helper" in text:
            subprocess.Popen([sys.executable, "-c", "import time; time.sleep(600)"])
        if "escapes" in text:
            daemon = subprocess.Popen([sys.executable, "-c", "import time; time.sleep(600)"], start_new_session=True)
            with open("helper.pid", "w") as pid:
                pid.write(str(daemon.pid))
        if "crash" in text:
            sys.stderr.write("fake claude: crashing as asked\n")
            sys.stderr.flush()
            sys.exit(3)
        if "bash" in text:
            command = {"command": "date > d.txt", "description": "Write the date"}
            out({"type": "assistant", "session_id": session, "message": {"content": [{"type": "tool_use", "id": "toolu_bash", "name": "Bash", "input": command}]}})
            decision = control("Bash", command, suggestions=[{"type": "addRules", "rules": [{"toolName": "Bash"}]}])
            result = "ran" if decision["behavior"] == "allow" else f"refused: {decision.get('message')}"
            out({"type": "user", "session_id": session, "message": {"content": [{"type": "tool_result", "tool_use_id": "toolu_bash", "content": result}]}})
            reply = json.dumps(decision)
        if "ask" in text:
            asked = {"questions": [{"question": "Red or blue?", "header": "Colour", "multiSelect": False,
                                    "options": [{"label": "Red", "description": "warm"}, {"label": "Blue", "description": "cold"}]}]}
            decision = control("AskUserQuestion", asked)
            reply = json.dumps(decision["updatedInput"].get("answers"))
        if "tool" in text:
            shown = "lookup" if plain else "mcp__weft__lookup"
            result = call_program(shown, {"key": "x"}, 2)
            reply = json.dumps({"tools": tools, "result": result})
        if "twice" in text:
            server, raw = program["lookup" if plain else "mcp__weft__lookup"]
            ids = [mcp_send(server, {"jsonrpc": "2.0", "id": 10 + index, "method": "tools/call",
                                     "params": {"name": raw, "arguments": {"key": str(index)}}}) for index in (0, 1)]
            got = answer_to(ids)
            reply = json.dumps([got[i]["response"]["mcp_response"]["result"]["content"][0]["text"] for i in ids])
        if "search" in text:
            if "WebSearch" in tools and "WebSearch" in BUILT_INS and "WebSearch" not in disallowed:
                # The built-in, which wins over a program's tool of its name.
                out({"type": "assistant", "session_id": session, "message": {"content": [
                    {"type": "tool_use", "id": "toolu_search", "name": "WebSearch", "input": {"query": "sky"}}]}})
                out({"type": "user", "session_id": session, "message": {"content": [
                    {"type": "tool_result", "tool_use_id": "toolu_search", "content": "the built-in searched"}]}})
                reply = "the built-in searched"
            else:
                reply = call_program("WebSearch", {"query": "sky"}, 3)["content"][0]["text"]
        if "slow" in text:
            read()
        out({"type": "system", "subtype": "commands_changed", "commands": [], "session_id": session})
        out({"type": "result", "subtype": "success", "is_error": False, "result": reply, "session_id": session,
             "stop_reason": "end_turn", "terminal_reason": "completed", "permission_denials": [], "num_turns": 1})


if __name__ == "__main__":
    main()
