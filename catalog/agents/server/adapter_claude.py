"""Claude Code, driven through `claude -p` with stream-json in and out.

One process holds the conversation: each user message written on its stdin
is a turn, and the `result` line ends it. `--permission-prompt-tool stdio`
sends every permission request, and every AskUserQuestion, to this server as
a `control_request` (`can_use_tool`), which waits for our `control_response`
for as long as weft takes (a held request ten minutes long was answered and
the turn went on).

The program's own tools are an "sdk" MCP server whose messages travel on
that same channel: `--mcp-config` names it, the `initialize` control request
declares it (`sdkMcpServers`), and Claude Code hands each MCP message over as
a `control_request` of subtype `mcp_message`, answered with a
`control_response` carrying `mcp_response`, which this side gets from weft's
MCP server (`Conversation.mcp`, the program's route). With
`CLAUDE_AGENT_SDK_MCP_NO_PREFIX` the model sees each tool by its plain name
(`lookup`, never `mcp__weft__lookup`), and the init line lists it so; a call
held open 80 seconds on 2.1.287 was answered and the turn went on, where
its HTTP MCP client gave up after a minute.

A program's tool named like a built-in replaces it: each program tool's
name goes into `--disallowedTools`, which removes the built-in of that name
from the model's context and leaves the program's tool, whose rules are
matched as `mcp__weft__<tool>` (with `Read`, `Agent`, `WebSearch` and every
other built-in removed this way, 2.1.287 called the program's). Without it
the built-in wins and the program's tool is dropped. `ToolSearch` is the one
it cannot give up (a program tool of that name never reaches the model), so
weft refuses a program tool of that name when the program is built and
when AskAgent fires (the node's `reservedToolNames`).

`--bare` is never used: it skips the OAuth token, the subscription login.
With both an API key and an OAuth token in the environment the key wins, so
exactly one of them is set.
"""

import json
import threading

import adapters
import events as ev
from events import Refused

PROGRAM = "claude"

# SYNC: Claude Code's permission settings <-> catalog/agents/claude_code/metadata.json (permission), catalog/agents/server/agents.json
PERMISSION_MODES = {
    # Every request a person would be asked comes to weft as a question.
    "ask": "manual",
    "accept_edits": "acceptEdits",
    "auto": "auto",
    "plan": "plan",
    "dont_ask": "dontAsk",
    "bypass": "bypassPermissions",
}

# The tool AskUserQuestion is: Claude's own way of asking with options.
ASK_TOOL = "AskUserQuestion"

# The program's tools as an "sdk" MCP server, whose messages ride the
# stream-json control channel. `alwaysLoad`: never deferred behind tool
# search, so the model has them in front of it (and a program that replaces
# every built-in still leaves a tool the API takes: with every tool deferred,
# 2.1.287 refused the request, "At least one tool must have
# defer_loading=false").
MCP_CONFIG = {"mcpServers": {adapters.MCP_SERVER: {"type": "sdk", "name": adapters.MCP_SERVER, "alwaysLoad": True}}}
# Plain names for the tools of an sdk MCP server.
NO_PREFIX_ENV = "CLAUDE_AGENT_SDK_MCP_NO_PREFIX"


def command(config: dict, conversation, tools: list) -> list:
    """The `claude` command line for `config`, resuming `conversation` when there is one."""
    options = config.get("options") or {}
    cmd = [adapters.program(config, PROGRAM), "-p", "--input-format", "stream-json", "--output-format", "stream-json", "--verbose",
           "--permission-prompt-tool", "stdio"]
    permission = config.get("permission") or "ask"
    if permission not in PERMISSION_MODES:
        raise Refused(f"Claude Code has no permission setting {permission!r}; it takes {', '.join(PERMISSION_MODES)}", 400)
    cmd += ["--permission-mode", PERMISSION_MODES[permission]]
    if permission == "bypass":
        # The mode alone is refused unless bypassing is allowed for the session.
        cmd.append("--allow-dangerously-skip-permissions")
    if config.get("streamBy") == "token":
        cmd.append("--include-partial-messages")
    if config.get("model"):
        cmd += ["--model", config["model"]]
    if options.get("effort"):
        cmd += ["--effort", options["effort"]]
    if options.get("fallbackModel"):
        cmd += ["--fallback-model", options["fallbackModel"]]
    if options.get("appendSystemPrompt"):
        cmd += ["--append-system-prompt", options["appendSystemPrompt"]]
    for directory in options.get("addDirs") or []:
        cmd += ["--add-dir", directory]
    allowed = list(options.get("allowedTools") or [])
    disallowed = list(options.get("disallowedTools") or [])
    if tools:
        # The program's own tools need no permission: wiring them is the
        # permission. Their rules are matched under the server's name even
        # with plain names (a rule naming `lookup` still asked; one naming
        # the server never does).
        allowed.append(f"mcp__{adapters.MCP_SERVER}")
        # Every name, built-in or not, rather than a list of built-ins that
        # goes stale with each release: a bare name removes only a built-in
        # (the program's tool of that name is ruled as `mcp__weft__<name>`).
        disallowed += [t["name"] for t in tools if t["name"] not in disallowed]
        cmd += ["--mcp-config", json.dumps(MCP_CONFIG)]
    if allowed:
        cmd += ["--allowedTools", ",".join(allowed)]
    if disallowed:
        cmd += ["--disallowedTools", ",".join(disallowed)]
    if options.get("agents"):
        cmd += ["--agents", json.dumps(options["agents"]) if not isinstance(options["agents"], str) else options["agents"]]
    if options.get("chrome"):
        cmd.append("--chrome")
    if options.get("remoteControl"):
        cmd += ["--remote-control", options["remoteControl"]]
    if conversation:
        cmd += ["--resume", conversation]
    return cmd


# How long Claude Code waits on one MCP tool call, in milliseconds. Over
# HTTP its default gave up after a minute ("The operation timed out"); over
# the SDK channel a call held 80 seconds went through without it, and no
# longer hold was tried, so it stays set: a call of the program's tools
# waits for whatever the program does. The largest delay a JavaScript timer
# takes, about 24 days: above it the timer fires at once.
MCP_TOOL_TIMEOUT_ENV = "MCP_TOOL_TIMEOUT"
MCP_TOOL_TIMEOUT_MS = "2147483647"

# Claude Code's native binary updates itself into the person's
# `~/.local/share/claude`: a copy weft installed in its own folder would
# write outside it, and a conversation would change version under it. Weft
# installs the version it runs; the person's own Claude Code still updates
# when they run it themselves.
NO_AUTOUPDATE_ENV = "DISABLE_AUTOUPDATER"


def credential_env(env: dict, credentials) -> None:
    """The login, as agents.json says: an OAuth token or an API key. With
    both in the environment the key wins, so exactly one is ever set."""
    env[MCP_TOOL_TIMEOUT_ENV] = MCP_TOOL_TIMEOUT_MS
    env[NO_PREFIX_ENV] = "1"
    env[NO_AUTOUPDATE_ENV] = "1"
    adapters.login_env(ClaudeCode.kind, env, credentials, None)


def question_of(request_id: str, request: dict) -> dict:
    """A `can_use_tool` request as weft's question."""
    tool, tool_input = request.get("tool_name", ""), request.get("input") or {}
    if tool == ASK_TOOL:
        asked = []
        for index, q in enumerate(tool_input.get("questions") or []):
            options = [{"label": o.get("label", ""), "description": o.get("description", "")} for o in q.get("options") or []]
            # AskUserQuestion always lets the person write their own answer.
            asked.append(ev.asked(str(index), q.get("header", ""), q.get("question", ""), options,
                                  q.get("multiSelect", False), True))
        return ev.questions(request_id, asked)
    choices = [ev.choice("allow", "Allow", ev.ALLOW_ONCE)]
    if request.get("permission_suggestions"):
        choices.append(ev.choice("always", "Always allow", ev.ALLOW_ALWAYS))
    choices.append(ev.choice("deny", "Deny", ev.REJECT))
    title = request.get("title") or f"{tool} wants to run"
    detail = describe(tool, tool_input)
    if request.get("decision_reason"):
        detail += f"\n({request['decision_reason']})"
    return ev.permission(request_id, tool, title, detail, tool_input, choices)


def describe(tool: str, tool_input: dict) -> str:
    for key in ("command", "file_path", "path", "url", "pattern", "plan"):
        if isinstance(tool_input.get(key), str):
            return f"{tool}: {tool_input[key]}"
    return f"{tool}: {json.dumps(tool_input)[:500]}"


def control_answer(request_id: str, request: dict, question: dict, answer: dict) -> dict:
    """weft's answer as the `control_response` Claude Code waits for."""
    tool_input = request.get("input") or {}
    if question["kind"] == ev.KIND_QUESTION:
        answers = ev.answers_for(answer, question)
        by_text = {}
        for q, asked in zip(tool_input.get("questions") or [], question["questions"]):
            by_text[q.get("question", "")] = ", ".join(answers[asked["id"]])
        response = {"behavior": "allow", "updatedInput": dict(tool_input, answers=by_text)}
    else:
        picked = ev.chosen(answer, question)
        if picked["kind"] == ev.REJECT:
            response = {"behavior": "deny", "message": answer.get("note") or "The person refused this."}
        else:
            response = {"behavior": "allow", "updatedInput": tool_input}
            if picked["kind"] == ev.ALLOW_ALWAYS:
                response["updatedPermissions"] = request.get("permission_suggestions") or []
    return {"type": "control_response", "response": {"subtype": "success", "request_id": request_id, "response": response}}


class ClaudeCode(adapters.Adapter):
    kind = "claude"
    program_name = PROGRAM
    permissions = PERMISSION_MODES
    option_names = ("effort", "fallbackModel", "appendSystemPrompt", "addDirs", "allowedTools", "disallowedTools", "agents",
                    "chrome", "remoteControl")

    def __init__(self, conversation, config, tools):
        super().__init__(conversation, config, tools)
        self.text = []
        # The ids of the program's own tool calls: their call and result go
        # out from the program's route, so the stream shows each once.
        self.ours = set()

    def command(self) -> list:
        return command(self.config, self.conv.conversation_id(), self.tools)

    def credential_env(self, env: dict) -> None:
        credential_env(env, self.credentials)

    def handshake(self) -> None:
        # Declares the sdk MCP server `--mcp-config` names; Claude Code asks
        # its MCP questions only once this is answered.
        rid = f"weft-{self.next_request_id()}"
        servers = [adapters.MCP_SERVER] if self.tools else []
        self.call(rid, {"type": "control_request", "request_id": rid,
                        "request": {"subtype": "initialize", "hooks": None, "sdkMcpServers": servers}}, "initialize")

    def begin(self, turn) -> None:
        self.text = []
        self.send({"type": "user", "message": {"role": "user", "content": turn.message}})

    def on_message(self, message: dict) -> None:
        kind = message.get("type")
        turn = self.turn
        if message.get("session_id"):
            self.conv.set_conversation(message["session_id"])
        if kind == "control_response":
            # The answer to one of our control requests (`handshake`).
            response = message.get("response") or {}
            if response.get("subtype") == "success":
                self.answered({"id": response.get("request_id"), "result": response.get("response") or {}})
            else:
                self.answered({"id": response.get("request_id"), "error": {"message": response.get("error") or json.dumps(response)}})
            return
        if kind == "control_request":
            # Its MCP messages come before any turn (the tool list, at start).
            self._control(turn, message)
            return
        if turn is None:
            return
        if kind == "assistant":
            for block in (message.get("message") or {}).get("content") or []:
                self._assistant_block(turn, block)
        elif kind == "user":
            content = (message.get("message") or {}).get("content")
            for block in content if isinstance(content, list) else []:
                if block.get("type") == "tool_result" and block.get("tool_use_id") not in self.ours:
                    output = adapters.text_of(block.get("content"))
                    self.conv.emit(turn, ev.TOOL_RESULT, id=block.get("tool_use_id"), output=output, isError=bool(block.get("is_error")))
                    self.remember_tool_result(block.get("tool_use_id"), output)
        elif kind == "stream_event":
            delta = (message.get("event") or {}).get("delta") or {}
            if delta.get("type") == "text_delta":
                self.conv.emit(turn, ev.TEXT_DELTA, text=delta.get("text", ""))
        elif kind == "result":
            if message.get("subtype") == "success" and not message.get("is_error"):
                self.conv.finish(turn, message.get("result") or "\n\n".join(self.text))
            else:
                errors = message.get("errors") or []
                why = "; ".join(str(e) for e in errors) or message.get("result") or message.get("subtype")
                self.conv.fail(turn, f"Claude Code ended the turn with an error: {why}")

    def _assistant_block(self, turn, block: dict) -> None:
        kind = block.get("type")
        if kind == "text" and block.get("text"):
            self.text.append(block["text"])
            self.conv.emit(turn, ev.TEXT, text=block["text"])
            self.remember_text(block["text"])
        elif kind == "thinking" and block.get("thinking"):
            self.conv.emit(turn, ev.THINKING, text=block["thinking"])
        elif kind == "tool_use":
            name = block.get("name", "")
            if self.program_tool(name) is not None:
                # The program's tool, by its plain name (its built-in of that
                # name, if any, is removed): the call goes out from the route.
                self.ours.add(block.get("id"))
                return
            self.conv.emit(turn, ev.TOOL_USE, id=block.get("id"), name=name, input=block.get("input") or {})
            self.remember_tool_use(block.get("id"), name, block.get("input") or {})

    def _control(self, turn, message: dict) -> None:
        request = message.get("request") or {}
        request_id = message.get("request_id")
        subtype = request.get("subtype")
        if subtype == "mcp_message" and request.get("server_name") == adapters.MCP_SERVER:
            # A tool call waits on weft for as long as weft takes: never on
            # the reader, which must keep reading the stream meanwhile.
            threading.Thread(target=self._mcp, args=(request_id, request.get("message") or {}),
                             name="claude-mcp", daemon=True).start()
            return
        if subtype != "can_use_tool":
            # Nothing else is ever asked of this side; say so rather than
            # leave the agent waiting.
            self._control_error(request_id, f"weft does not answer {subtype!r}")
            return
        if turn is None or turn.status in ev.ENDED:
            self._control_error(request_id, "no turn is running on the weft side")
            return
        if (request.get("mcp_server") or {}).get("name") == adapters.MCP_SERVER:
            # The program's tool, by the server Claude Code says it is on.
            self.send({"type": "control_response", "response": {"subtype": "success", "request_id": request_id,
                                                                 "response": {"behavior": "allow", "updatedInput": request.get("input") or {}}}})
            return
        question = question_of(request_id, request)

        def answer(given: dict) -> None:
            self.send(control_answer(request_id, request, question, given))

        self.block(turn, ev.QUESTION, request_id, {"question": question}, answer,
                   lambda why: self._control_error(request_id, why))

    def _mcp(self, request_id: str, mcp_message: dict) -> None:
        """One MCP message of the sdk server, answered by weft's MCP server."""
        answer = self.conv.mcp.handle(mcp_message)
        if answer is None:
            # A notification: the control channel still answers it.
            answer = {"jsonrpc": "2.0", "result": {}}
        try:
            self.send({"type": "control_response", "response": {"subtype": "success", "request_id": request_id,
                                                                 "response": {"mcp_response": answer}}})
        except Refused as gone:
            # The agent stopped while the call waited: nobody is left to tell.
            adapters.log(f"the answer to an MCP message of {self.label} found it gone: {gone}")

    def _control_error(self, request_id: str, why: str) -> None:
        self.send({"type": "control_response", "response": {"subtype": "error", "request_id": request_id, "error": why}})


adapters.register(ClaudeCode)
