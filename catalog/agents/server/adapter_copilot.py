"""GitHub Copilot CLI, driven through `copilot --headless --stdio`: the
protocol of the Copilot SDK, JSON-RPC with each message framed by a
`Content-Length` header. It is spoken here straight from its schema
(`schemas/api.schema.json` and `schemas/session-events.schema.json` in
Copilot's own package) rather than through the Python SDK, which needs
pydantic, httpx and Python 3.11, where this server runs on the standard
library alone. Copilot's ACP mode (`--acp`) does less: it prefixes the
program's tools and cannot replace a built-in.

`connect`, then `session.create` (or `session.resume`, which carries the
conversation on with its history), then the session's mode and permission
mode, then `session.tools.initializeAndValidate`, which builds the tool list
the model will see and fails then, never in the first turn, on a tool it
refuses. One `session.send` per turn; the turn ends at `session.idle`, and
everything in between arrives as `session.event` notifications.

The program's tools are the session's own `tools`, under their own names:
the model sees `search`, never a prefix. Every one carries
`overridesBuiltInTool`, so one named like a built-in replaces it (without
it the clash fails the turn) and any other is a new tool: a built-in a newer
Copilot adds is replaced the same way, with no list to keep. One name is
dropped without a word, `tool_search_tool`, which the node refuses (its
`lists.reservedToolNames`) when the program is built and when AskAgent
fires, so it never reaches this adapter.
A call of one arrives as an `external_tool.requested` event, answered with
`session.tools.handlePendingToolCall`: that event is the program's route,
and the only thing that makes a call the program's (the route writes it in
the history, `Conversation.block`). It carries the `toolCallId` the model's
call had (`tool.execution_start`, which comes first): a call of a program
tool's name is held until the route says it is the program's, and shown as
the agent's own only if its result comes without it.

Its permission requests are `permission.requested` events, answered with
`session.permissions.handlePendingPermissionRequest`; a refusal goes back to
the model with the person's note, and the turn goes on. Its questions are
its `ask_user` tool, asked as a `userInput.request` request. `plan` runs it
in its plan mode, where a change outside its session folder is blocked,
without its `exit_plan_mode` tool: leaving the plan is the person's, by
another permission setting. A sub-agent's events carry an `agentId`: its
questions, permissions and calls of the program's tools are answered like
the main agent's, and only the main agent's messages and its own tools
make the reply, the stream and the history (a call of the program's tools
is the route's, whoever made it).

A model the node names that the person's plan does not offer is not
refused by `session.create`: Copilot routes the turn to a model of its own
choice (`session.auto_mode_resolved`, before it calls the model), which
this adapter stops (`session.abort`), failing the turn with the models the
plan offers (1.0.105, on a Free plan, which offers its auto mode alone).
The turn fails once Copilot has both taken the stop (its answer) and ended
its turn (`session.idle`), in whichever order they come: an abort that
lands while the turn runs is answered, then the turn goes idle as aborted;
one that lands after the turn went idle is answered alone, with no event.
Nothing Copilot says in between is the turn's.

Its login: with no flag it tries `COPILOT_GITHUB_TOKEN`, `GH_TOKEN`,
`GITHUB_TOKEN`, its stored login, then `gh auth token`. `--no-auto-login`
turns all of that off, and `--auth-token-env` then names the one variable it
reads, so a connection's token is the only login it has, and a bad one
fails `session.create` (GitHub's 401) instead of falling through to the
person's `gh` account. A model provider of one's own is handed over in
`session.create` (`provider`): its `COPILOT_PROVIDER_*` variables are not
read by a headless session.
"""

import json
import urllib.parse

import adapters
import events as ev
from adapter_acp import Lever, Setup, deep_merge
from events import Refused

# The SDK protocol this adapter speaks (`connect` answers the server's).
PROTOCOL_VERSION = 3

# SYNC: Copilot's login ways <-> catalog/agents/server/agents.json (copilot.logins)
WAY_TOKEN = "githubToken"
WAY_PROVIDER = "ownProvider"
# The variable a GitHub token login hands its token in (agents.json's own).
TOKEN_ENV = adapters.AGENTS["copilot"]["logins"][WAY_TOKEN]["env"]["token"]
LOGIN_ARGS = {WAY_TOKEN: ["--no-auto-login", "--auth-token-env", TOKEN_ENV], WAY_PROVIDER: ["--no-auto-login"]}

# Read at its start: it approves every tool, whatever the settings say.
UNSET_ENV = ("COPILOT_ALLOW_ALL",)
# The person's own model and provider variables, taken out under a
# connection's login, which is the only one it may have.
CONNECTION_UNSET_ENV = ("COPILOT_MODEL",)
CONNECTION_UNSET_PREFIX = "COPILOT_PROVIDER_"

# weft's permission setting -> (the session's mode, its permission mode).
# Both are kept with the session and come back on a resume, so every
# setting sets both, `ask` included.
# SYNC: Copilot's permission settings <-> catalog/agents/copilot_cli/metadata.json (permission), catalog/agents/server/agents.json
PERMISSIONS = {"ask": ("interactive", "manual"), "plan": ("plan", "manual"), "bypass": ("interactive", "allow-all")}

# The model id that lets Copilot pick the model each turn.
# SYNC: <-> catalog/agents/copilot_cli/metadata.json (model)
AUTO_MODEL = "auto"

OPTIONS = {
    "effort": Lever(Lever.TEXT, lambda v: Setup(session={"reasoningEffort": v}),
                    ["none", "minimal", "low", "medium", "high", "xhigh", "max"], "How hard the model thinks"),
    "addDirs": Lever(Lever.LIST, lambda vs: Setup(session={"additionalDirectories": vs}), None,
                     "More folders it may read and change"),
    "maxAiCredits": Lever(Lever.WHOLE, lambda v: Setup(session={"sessionLimits": {"maxAiCredits": int(v)}}), None,
                          "The most AI credits this conversation may spend"),
    "availableTools": Lever(Lever.LIST, lambda vs: Setup(session={"availableTools": vs}), None,
                            "The only built-in tools it may use"),
    "excludedTools": Lever(Lever.LIST, lambda vs: Setup(session={"excludedTools": vs}), None,
                           "Built-in tools it may not use"),
}

# A tool's file changes (`fileEdits`), as weft's `file_change` names them.
FILE_CHANGES = {"create": "add", "edit": "update", "delete": "delete"}


class FramedOut:
    """Copilot's standard output read as one JSON message per item, for the
    base adapter's reader, which reads one per line: each message is a
    `Content-Length` header, a blank line, and that many bytes of JSON."""

    def __init__(self, stream):
        self.stream = stream
        self.raw = stream.buffer

    def __iter__(self):
        while True:
            length = None
            while True:
                line = self.raw.readline()
                if not line:
                    return
                line = line.strip()
                if not line and length is not None:
                    break
                name, colon, value = line.partition(b":")
                if colon and name.strip().lower() == b"content-length":
                    length = int(value)
                elif line:
                    # Not the protocol (a stray print): the reader keeps it
                    # with what the agent printed, as a line it cannot read.
                    yield line.decode("utf-8", "replace")
            body = self.raw.read(length)
            if len(body) < length:
                return
            yield body.decode("utf-8")

    def close(self) -> None:
        self.stream.close()


class FramedIn:
    """Copilot's standard input written one framed message per line the base
    adapter writes (`_write`, one JSON object and a newline)."""

    def __init__(self, stream):
        self.stream = stream
        self.raw = stream.buffer

    def write(self, text: str) -> None:
        body = text.rstrip("\n").encode("utf-8")
        self.raw.write(b"Content-Length: %d\r\n\r\n" % len(body) + body)

    def flush(self) -> None:
        self.raw.flush()

    def close(self) -> None:
        self.stream.close()


def tool_definition(tool: dict) -> dict:
    """A program tool (`LlmTool`) as the session's `tools` takes it."""
    # `skipPermission`: wiring a tool to the agent is the permission to call it.
    return {"name": tool["name"], "description": tool.get("description") or "",
            "parameters": tool.get("parameters") or {"type": "object", "properties": {}},
            "overridesBuiltInTool": True, "skipPermission": True}


def tool_answer(result: dict) -> dict:
    """weft's result for a program tool, as `handlePendingToolCall`'s `result`."""
    output = str(result.get("output", ""))
    if result.get("isError"):
        return {"textResultForLlm": output, "resultType": "failure", "error": output}
    return {"textResultForLlm": output, "resultType": "success"}


def session_approval(prompt: dict):
    """What `approve-for-session` carries for a permission prompt, or None when
    the prompt is of a kind no session approval covers."""
    kind = prompt.get("kind")
    if kind == "commands" and prompt.get("commandIdentifiers"):
        return {"approval": {"kind": "commands", "commandIdentifiers": prompt["commandIdentifiers"]}}
    if kind in ("read", "write", "memory"):
        return {"approval": {"kind": kind}}
    if kind == "mcp" and prompt.get("serverName"):
        return {"approval": {"kind": "mcp", "serverName": prompt["serverName"], "toolName": prompt.get("toolName")}}
    if kind == "custom-tool" and prompt.get("toolName"):
        return {"approval": {"kind": "custom-tool", "toolName": prompt["toolName"]}}
    if kind == "url" and urllib.parse.urlparse(prompt.get("url") or "").hostname:
        return {"domain": urllib.parse.urlparse(prompt["url"]).hostname}
    return None


def describe(request: dict) -> tuple:
    """A permission request's title and detail."""
    kind = request.get("kind")
    if kind == "shell":
        return "GitHub Copilot CLI wants to run a command", request.get("fullCommandText") or ""
    if kind == "write":
        return "GitHub Copilot CLI wants to change a file", request.get("fileName") or ""
    if kind == "read":
        return "GitHub Copilot CLI wants to read outside its folder", request.get("path") or ""
    if kind == "url":
        return "GitHub Copilot CLI wants to fetch a URL", request.get("url") or ""
    if kind == "mcp":
        return "GitHub Copilot CLI wants to use an MCP tool", f"{request.get('serverName')}: {request.get('toolName')}"
    if kind == "custom-tool":
        return "GitHub Copilot CLI wants to use a tool", request.get("toolName") or ""
    if kind == "memory":
        return "GitHub Copilot CLI wants to remember something", request.get("fact") or request.get("subject") or ""
    return f"GitHub Copilot CLI asks for permission ({kind})", request.get("intention") or json.dumps(request)[:500]


def permission_question(question_id: str, data: dict) -> tuple:
    """A `permission.requested` event as weft's permission question, and the
    decision an answer to it becomes."""
    request = data.get("permissionRequest") or {}
    approval = session_approval(data.get("promptRequest") or {})
    choices = [ev.choice("approve-once", "Allow", ev.ALLOW_ONCE)]
    if approval is not None:
        choices.append(ev.choice("approve-for-session", "Allow for this conversation", ev.ALLOW_ALWAYS))
    choices.append(ev.choice("reject", "Deny", ev.REJECT))
    title, detail = describe(request)
    if request.get("intention") and request.get("intention") not in detail:
        detail = f"{detail}\n({request['intention']})" if detail else request["intention"]
    question = ev.permission(question_id, request.get("kind") or "tool", title, detail, request, choices)

    def decision_of(answer: dict) -> dict:
        picked = ev.chosen(answer, question)
        if picked["kind"] == ev.REJECT:
            return {"kind": "reject", "feedback": answer["note"]} if answer.get("note") else {"kind": "reject"}
        if picked["kind"] == ev.ALLOW_ALWAYS:
            return {"kind": "approve-for-session", **approval}
        return {"kind": "approve-once", "approvedInteractively": True}

    return question, decision_of


def input_question(question_id: str, params: dict) -> dict:
    """A `userInput.request` (its `ask_user` tool) as weft's question."""
    choices = [c for c in params.get("choices") or [] if isinstance(c, str)]
    options = [{"label": c, "description": ""} for c in choices]
    allow_other = params.get("allowFreeform", True) or not options
    return ev.questions(question_id, [ev.asked("answer", "", params.get("question") or "", options, False, allow_other)])


def input_answer(question: dict, answer: dict) -> dict:
    given = ev.answers_for(answer, question)["answer"][0]
    return {"answer": given, "wasFreeform": given not in {o["label"] for o in question["questions"][0]["options"]}}


class Copilot(adapters.Adapter):
    kind = "copilot"
    program_name = "copilot"
    permissions = PERMISSIONS
    option_names = tuple(OPTIONS)

    def __init__(self, conversation, config, tools):
        super().__init__(conversation, config, tools)
        self.setup = Setup()
        for name, value in self.options.items():
            if value is not None:
                self.setup = self.setup.add(OPTIONS[name].setup(self.label, name, value))
        excluded = sorted(self.program_tools.names & set(self.setup.session.get("excludedTools") or []))
        if excluded:
            raise Refused(f"{self.label}'s `excludedTools` names the program's tool {', '.join(excluded)}, which would "
                          "hide it from the model; take it out of `excludedTools`", 400)
        self.session = None
        self.begin_turn()

    def begin_turn(self) -> None:
        """What one turn gathers, from nothing."""
        self.reply = []
        # The last error the main agent said in this turn (`session.error`).
        self.error = None
        # Why this adapter stopped the turn itself (`_routed`), the id of its
        # `session.abort` until Copilot answers it, and whether Copilot's
        # turn is over: the turn fails once it has both.
        self.stopped = None
        self.aborting = None
        self.idle = False
        # Whether the turn's message reached the agent, or failed before it
        # did: only then is a `session.idle` its end.
        self.started = False
        # The main agent's calls of a tool named like one of the program's,
        # by call id, until the program's route says whose they are:
        # [name, arguments].
        self.held = {}
        # The call ids the program's route saw, whose results stay out of
        # the stream (the program had the call as a `tool_call`).
        self.routed = set()
        # Copilot's request id of each call and permission this turn waits
        # on weft for (each ended on its side by a `.completed` event) ->
        # the id weft waits on it by.
        self.waits = {}

    def command(self) -> list:
        way = self.credentials.get("way") if self.credentials is not None else None
        return [self.program(), "--headless", "--stdio", "--no-auto-update", *LOGIN_ARGS.get(way, [])]

    def credential_env(self, env: dict) -> None:
        for name in UNSET_ENV:
            env.pop(name, None)
        if self.credentials is not None:
            for name in [n for n in env if n in CONNECTION_UNSET_ENV or n.startswith(CONNECTION_UNSET_PREFIX)]:
                env.pop(name)
        super().credential_env(env)

    def session_params(self) -> dict:
        """What `session.create` (and `session.resume`) is sent."""
        params = {"workingDirectory": self.config["folder"], "tools": [tool_definition(t) for t in self.tools],
                  "requestPermission": True, "requestUserInput": True, "requestExitPlanMode": False,
                  "requestElicitation": False, "streaming": self.config.get("streamBy") == "token",
                  # Its sub-agents' messages are nothing of the reply.
                  "includeSubAgentStreamingEvents": False,
                  # The person's `excludedTools` wins over a tool their `availableTools` names.
                  "toolFilterPrecedence": "excluded"}
        params = deep_merge(params, self.setup.session)
        if "availableTools" in params:
            # The allowlist hides every tool it does not name, the
            # program's too: the program's are always available.
            params["availableTools"] = params["availableTools"] + sorted(self.program_tools.names - set(params["availableTools"]))
        model = self.config.get("model")
        if self.credentials is not None and self.credentials.get("way") == WAY_PROVIDER:
            params["provider"] = {"type": self.login_value("type"), "baseUrl": self.login_value("baseUrl"),
                                  "apiKey": self.login_value("key"), "wireApi": self.login_value("wireApi")}
            model = model or self.login_value("model")
        if model:
            params["model"] = model
        return params

    # ----- JSON-RPC ----------------------------------------------------------

    def _read_out(self, proc, waiting: dict) -> None:
        proc.stdout = FramedOut(proc.stdout)
        super()._read_out(proc, waiting)

    def request(self, method: str, params: dict) -> dict:
        rid = self.next_request_id()
        return self.call(rid, {"jsonrpc": "2.0", "id": rid, "method": method, "params": params}, method)

    def post(self, method: str, params: dict) -> int:
        """Send a request whose answer nobody waits for: from the reader's own
        thread, which reads that answer, so it cannot wait for it. Its id."""
        rid = self.next_request_id()
        self.send({"jsonrpc": "2.0", "id": rid, "method": method, "params": params})
        return rid

    def handshake(self) -> None:
        with self.write_lock:
            self.proc.stdin = FramedIn(self.proc.stdin)
        version = self.request("connect", {}).get("protocolVersion")
        if version != PROTOCOL_VERSION:
            raise Refused(f"this {self.label} speaks version {version} of the Copilot SDK's protocol, and weft speaks "
                          f"version {PROTOCOL_VERSION}; install the version weft names (`copilot --version`), or update weft")
        params = self.session_params()
        conversation = self.conv.conversation_id()
        if conversation:
            try:
                answer = self.request("session.resume", dict(params, sessionId=conversation))
            except Refused as refused:
                raise Refused(f"{refused}; the conversation cannot be picked up again, so start it over from the node's card")
        else:
            answer = self.request("session.create", params)
        self.session = answer["sessionId"]
        self.conv.set_conversation(self.session)
        mode, approvals = PERMISSIONS[self.permission]
        moved = self.request("session.mode.set", {"sessionId": self.session, "mode": mode})
        if not moved.get("modeApplied"):
            raise Refused(f"{self.label} did not take the `{mode}` mode the `{self.permission}` permission setting runs it in: "
                          f"{json.dumps(moved)}")
        set_to = self.request("session.permissions.setMode", {"sessionId": self.session, "mode": approvals})
        if set_to.get("mode") != approvals:
            raise Refused(f"{self.label} did not take the `{approvals}` permission mode the `{self.permission}` permission "
                          f"setting runs it in: {json.dumps(set_to)}")
        self.request("session.tools.initializeAndValidate", {"sessionId": self.session})

    def begin(self, turn) -> None:
        self.begin_turn()
        self.request("session.send", {"sessionId": self.session, "prompt": turn.message})

    # ----- what Copilot writes -----------------------------------------------

    def on_message(self, message: dict) -> None:
        method = message.get("method")
        turn = self.turn
        if method is None:
            if self.aborting is not None and message.get("id") == self.aborting:
                self._abort_answered(turn, message)
            else:
                self.answered(message)
            return
        if "id" in message:
            self._server_request(turn, message["id"], method, message.get("params") or {})
            return
        # `session.lifecycle` and `host.event` say nothing of a turn.
        if method != "session.event" or turn is None or turn.status in ev.ENDED:
            return
        event = (message.get("params") or {}).get("event") or {}
        kind, data = event.get("type"), event.get("data") or {}
        main = not event.get("agentId")
        if self.stopped is not None:
            # Stopped by this adapter: only the end of Copilot's turn counts.
            if kind == "session.idle" and main:
                self.idle = True
                self._end_stopped(turn)
            return
        if kind == "external_tool.requested":
            self._program_call(turn, data)
        elif kind == "permission.requested" and not data.get("resolvedByHook"):
            self._permission(turn, data)
        elif kind in ("external_tool.completed", "permission.completed") and data.get("requestId") in self.waits:
            # Weft's answer reached it, or it went on without one.
            try:
                self.conv.stop_waiting(turn, self.waits.pop(data["requestId"]))
            except Refused as refused:
                self.conv.fail(turn, str(refused))
        elif main:
            self._main_event(turn, kind, data)

    def _main_event(self, turn, kind: str, data: dict) -> None:
        if kind == "user.message":
            self.started = True
        elif kind == "assistant.message_delta" and self.config.get("streamBy") == "token":
            self.conv.emit(turn, ev.TEXT_DELTA, text=data.get("deltaContent", ""))
        elif kind == "assistant.reasoning" and (data.get("content") or "").strip():
            self.conv.emit(turn, ev.THINKING, text=data["content"])
        elif kind == "assistant.message" and (data.get("content") or "").strip():
            self.reply.append(data["content"])
            self.conv.emit(turn, ev.TEXT, text=data["content"])
            self.remember_text(data["content"])
        elif kind == "session.auto_mode_resolved":
            self._routed(turn, data)
        elif kind == "tool.execution_start":
            call_id, name, arguments = data.get("toolCallId"), data.get("toolName") or "tool", data.get("arguments") or {}
            if self.program_tool(name) is not None:
                self.held[call_id] = [name, arguments]
                return
            self.conv.emit(turn, ev.TOOL_USE, id=call_id, name=name, input=arguments)
            self.remember_tool_use(call_id, name, arguments)
        elif kind == "tool.execution_complete":
            self._tool_done(turn, data)
        elif kind == "session.error":
            # The message it failed on: a turn that never reached the model
            # (no login, a tool it refused) ends here too.
            self.started = True
            self.error = data.get("message") or json.dumps(data)
        elif kind == "session.idle" and self.started:
            if self.error is not None:
                self.conv.fail(turn, f"{self.label} could not finish the turn: {self.error}")
            elif data.get("aborted"):
                self.conv.fail(turn, f"{self.label} stopped the turn before it ended")
            else:
                self.conv.finish(turn, "\n\n".join(self.reply))

    def _routed(self, turn, data: dict) -> None:
        """Copilot picked the turn's model itself, before calling it. With the
        node's own model, that is a model the person's plan does not offer
        (a Free plan offers only what Copilot's auto mode picks): the turn
        is stopped before the model runs, and fails saying so."""
        wanted, chosen = self.config.get("model"), data.get("chosenModel")
        if not wanted or wanted == AUTO_MODEL or chosen == wanted:
            return
        offered = ", ".join(data.get("availableModels") or []) or "none"
        self.stopped = (f"{self.label} would run {chosen} instead of the node's model {wanted}, which this Copilot plan "
                        f"does not offer (it offers {offered}); set `model` to one of those or to `{AUTO_MODEL}`, or "
                        "leave it empty")
        self.aborting = self.post("session.abort", {"sessionId": self.session})

    def _abort_answered(self, turn, message: dict) -> None:
        """Copilot's answer to the turn's `session.abort`."""
        self.aborting = None
        if turn is None or turn.status in ev.ENDED:
            return
        if (message.get("result") or {}).get("success") is not True:
            self.conv.fail(turn, f"{self.stopped}; and {self.label} did not take the stop (session.abort): {json.dumps(message)}")
            return
        self._end_stopped(turn)

    def _end_stopped(self, turn) -> None:
        """Fail a turn this adapter stopped, once Copilot took the stop and
        its turn is over."""
        if self.idle and self.aborting is None:
            self.conv.fail(turn, self.stopped)

    def _tool_done(self, turn, data: dict) -> None:
        call_id = data.get("toolCallId")
        result, error = data.get("result") or {}, data.get("error") or {}
        output = result.get("content") if data.get("success") else error.get("message") or json.dumps(error)
        output = adapters.text_of(output)
        if call_id in self.routed:
            # Its result is the route's (`Conversation.deliver`).
            return
        held = self.held.pop(call_id, None)
        if held is not None:
            # Named like one of the program's tools, and it never came
            # through the program's route: one of its own.
            self.conv.emit(turn, ev.TOOL_USE, id=call_id, name=held[0], input=held[1])
            self.remember_tool_use(call_id, *held)
        self.conv.emit(turn, ev.TOOL_RESULT, id=call_id, output=output, isError=not data.get("success"))
        self.remember_tool_result(call_id, output)
        for edit in data.get("fileEdits") or []:
            self.conv.emit(turn, ev.FILE_CHANGE, path=edit.get("path"), change=FILE_CHANGES.get(edit.get("kind"), edit.get("kind")))

    def _program_call(self, turn, data: dict) -> None:
        request_id, name, call_id = data.get("requestId"), data.get("toolName"), data.get("toolCallId")
        arguments = data.get("arguments") or {}
        if self.program_tool(name) is None:
            why = f"{self.label} called {name!r} as one of the program's tools, and the program has no tool of that name"
            self.post("session.tools.handlePendingToolCall", {"sessionId": self.session, "requestId": request_id, "error": why})
            self.conv.fail(turn, why)
            return
        # Weft waits on it by the model's id of the call, the one the
        # history keeps it under.
        self.held.pop(call_id, None)
        self.routed.add(call_id)
        self.waits[request_id] = call_id
        self.block(turn, ev.TOOL_CALL, call_id, {"callId": call_id, "name": name, "arguments": json.dumps(arguments)},
                   lambda result: self._settle("session.tools.handlePendingToolCall",
                                               {"sessionId": self.session, "requestId": request_id, "result": tool_answer(result)}),
                   lambda why: self.post("session.tools.handlePendingToolCall",
                                         {"sessionId": self.session, "requestId": request_id, "error": why}))

    def _permission(self, turn, data: dict) -> None:
        request_id = data.get("requestId")
        question, decision_of = permission_question(request_id, data)
        self.waits[request_id] = request_id
        self.block(turn, ev.QUESTION, request_id, {"question": question},
                   lambda answer: self._settle("session.permissions.handlePendingPermissionRequest",
                                               {"sessionId": self.session, "requestId": request_id, "result": decision_of(answer)}),
                   lambda why: self.post("session.permissions.handlePendingPermissionRequest",
                                         {"sessionId": self.session, "requestId": request_id,
                                          "result": {"kind": "reject", "feedback": why}}))

    def _settle(self, method: str, params: dict) -> None:
        """Hand weft's answer over, refused when Copilot did not take it (the
        call was already settled on its side)."""
        answer = self.request(method, params)
        if answer.get("success") is not True:
            raise Refused(f"{self.label} did not take the answer ({method}): {json.dumps(answer)}")

    def _server_request(self, turn, rid, method: str, params: dict) -> None:
        def refuse(why: str) -> None:
            self.send({"jsonrpc": "2.0", "id": rid, "error": {"code": -32000, "message": why}})

        if method != "userInput.request":
            self.send({"jsonrpc": "2.0", "id": rid, "error": {"code": -32601, "message": f"weft does not answer {method}"}})
            return
        if turn is None or turn.status in ev.ENDED:
            refuse("no turn is running on the weft side")
            return
        question = input_question(f"{self.kind}-{rid}", params)
        self.block(turn, ev.QUESTION, question["id"], {"question": question},
                   lambda answer: self.send({"jsonrpc": "2.0", "id": rid, "result": input_answer(question, answer)}), refuse)


adapters.register(Copilot)
