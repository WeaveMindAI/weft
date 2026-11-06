"""Mistral Vibe, driven through its own app server: `vibe-app-server
--agent-socket PATH`.

Vibe's app server speaks JSON-RPC, one compact JSON object per line each
way, over its standard input and output: `initialize` and the
`initialized` notification, then `session/start` (or `session/resume`,
which carries the conversation on with its history), then one
`turn/start` per turn. Everything a turn does arrives as notifications of
the session: `history/entryAdded` and `history/entryUpdated` (its
messages, its reasoning, its tool calls, each patched as it goes, a
message's text appended piece by piece), and `turn/completed`. Its
questions and permission requests are `callback/call` requests, taken at
once (`{callbackId, accepted}`) and answered later with `callback/result`.

Its app server, not its ACP mode (`vibe-acp`), because of the program's
tools. Over ACP Vibe hands a session's MCP servers to its model only
behind its search tool, as functions it finds with `search_tool_functions`
and calls from TypeScript (`run_typescript`): never a tool of their own,
never a plain name. The app server takes tools of its client's own
through the agent socket: a Unix socket this adapter serves, which Vibe
connects to at its start (`agent/initialize`, whose answer declares the
tools) and once per call (`tools/call`). Declared with `modelAccess`
`direct`, each one is a tool of the model's own under its plain name
(3.0.4, against a scripted model: it was offered `lookup` beside `bash`,
and called it as `lookup`). Vibe names it `weft.lookup` everywhere else
(the socket's call, the session's history), the namespace being weft's
own: Vibe refuses a namespace named like one of its own groups or an MCP
server of the session, so that spelling is the program's tool alone. A
call is the program's because it came in on the socket, the program's
route (the route writes it in the history, `Conversation.block`); the
history entry of the same call (`effect-<id>` for the socket's
`client_tool:<id>`) is held until the route says whose it is, and shown
as the agent's own only if it ends without the route having seen it.

A program tool named like one of Vibe's tools: four of them can be turned
off for the session (`disabledTools`), which frees the name, and the
program's takes their place (`ask_user_question`, `todo`, `cron`,
`unified_harness_scratchpad`). The others Vibe refuses for any client
tool, turned off or not ("direct tool name ... conflicts with a built-in
tool"), and the node refuses those (its `lists.reservedToolNames`) when the
program is built and when AskAgent fires, so they never reach this
adapter. Both seen on 3.0.4.

A call of the program's tools waits on the socket for as long as weft
takes to answer it (a person may take days): the socket's wait has no
"none", so it is set past any real wait (`TOOL_WAIT_SECS`).

The permission settings are Vibe's own agents (`ask`, `accept-edits`,
`auto-approve`). A resumed session comes back on the agent it was left on,
whatever `session/resume` says, so the agent is switched after every
start (`session/agent/update`). The model and the effort are set for the
session alone (`config/model/write` with `sessionOnly`), never written into
the person's own config.

With a connection, Vibe's home (`VIBE_HOME`) is a private one, holding no
config of its own: Vibe's defaults, Mistral's API with the key from
`MISTRAL_API_KEY`, and none of the person's own `VIBE_*` settings. The
folder it works in is trusted there, so its `AGENTS.md` and `.vibe/` are
read as they are in the person's own home once they trusted it there.
Self-updates are off: the install the server runs must stay the same
while a conversation runs.
"""

import json
import os
import shutil
import socketserver
import tempfile
import threading

import adapters
import events as ev
from adapter_acp import Lever, Setup, deep_merge
from events import Refused

# The agent socket's protocol, as `agent/initialize` names it.
SOCKET_PROTOCOL = 2
# The namespace the program's tools are declared in: the model sees each
# under its plain name, Vibe's history and the socket as `weft.<name>`.
NAMESPACE = adapters.MCP_SERVER
# How long a call of the program's tools may wait on the socket. Vibe takes
# a finite number only, and weft sets no deadline on a call (a person may
# answer days later): this is past any wait that really happens.
TOOL_WAIT_SECS = 1e9

# Its tools a program tool takes the place of, by turning them off.
REPLACEABLE_TOOL_NAMES = ("ask_user_question", "cron", "todo", "unified_harness_scratchpad")

# weft's permission setting -> Vibe's agent.
# SYNC: Vibe's permission settings <-> catalog/agents/server/agents.json (vibe.permissions), catalog/agents/mistral_vibe/metadata.json (permission)
PERMISSIONS = {"ask": "ask", "accept_edits": "accept-edits", "bypass": "auto-approve"}

# The choices of a permission request weft offers, by Vibe's decision.
# `approve_permanently` is left out: it writes the grant into the person's
# own config for good, beyond this conversation. So is `cancel_turn`: weft
# stops a turn its own way.
CHOICES = {"approve": ("Allow", ev.ALLOW_ONCE),
           "approve_for_session": ("Allow for this conversation", ev.ALLOW_ALWAYS),
           "deny": ("Deny", ev.REJECT)}

# Its settings, read before its config.toml and never written back, that it
# keeps off: the install must stay as it is while a conversation runs.
ENV = {"VIBE_ENABLE_UPDATE_CHECKS": "false", "VIBE_ENABLE_AUTO_UPDATE": "false"}
# The prefix of every setting Vibe reads from the environment: under a
# connection, none of the person's own is read.
ENV_PREFIX = "VIBE_"

# `Setup.session` merges into the session's `agentConfig`, `Setup.config`
# into the session's `config/model/write`.
OPTIONS = {
    "effort": Lever(Lever.TEXT, lambda v: Setup(config={"reasoningEffort": v}), ["off", "low", "medium", "high", "max"],
                    "How much the model thinks before it answers"),
    "addDirs": Lever(Lever.LIST, lambda dirs: Setup(session={"workspaceRoots": dirs}), None,
                     "More folders it may read and change"),
    "maxTurns": Lever(Lever.WHOLE, lambda v: Setup(session={"maxTurns": int(v)}), None,
                      "The most model calls one turn may make"),
}

# A file tool's history entry (its `detail.kind`), as weft's `file_change`.
FILE_KINDS = ("file_write", "file_edit")


def tool_declaration(tool: dict) -> dict:
    """A program tool (`LlmTool`) as `agent/initialize` declares it."""
    return {"namespace": NAMESPACE, "name": tool["name"], "description": tool.get("description") or "",
            "inputSchema": tool.get("parameters") or {"type": "object", "properties": {}}, "modelAccess": "direct"}


def tool_reply(result: dict) -> dict:
    """weft's result for a program tool, as the socket's `tools/call` answer."""
    output = str(result.get("output", ""))
    if result.get("isError"):
        return {"output": None, "error": {"message": output, "code": "tool_failed"}}
    return {"output": output}


def bare(tool_name: str) -> str:
    """The name the model calls a tool by: Vibe's name without its group
    (`file_system.bash` is `bash`)."""
    return tool_name.rpartition(".")[2]


def apply_patch(doc: dict, patch: list) -> None:
    """A history entry patch (`history/entryUpdated`): JSON Patch's `add`,
    `replace` and `remove`, and Vibe's own `append`, which adds to the text
    already at the path."""
    for op in patch:
        keys = [k.replace("~1", "/").replace("~0", "~") for k in op["path"].split("/")[1:]]
        parent = doc
        for key in keys[:-1]:
            parent = parent[int(key)] if isinstance(parent, list) else parent.setdefault(key, {})
        last = keys[-1]
        kind = op["op"]
        if isinstance(parent, list):
            index = len(parent) if last == "-" else int(last)
            if kind == "remove":
                parent.pop(index)
            elif kind == "append":
                parent[index] = (parent[index] or "") + op["value"]
            elif kind == "add":
                parent.insert(index, op["value"])
            else:
                parent[index] = op["value"]
        elif kind == "remove":
            parent.pop(last, None)
        elif kind == "append":
            parent[last] = (parent.get(last) or "") + op["value"]
        else:
            parent[last] = op["value"]


def text_of(entry: dict) -> str:
    """A message entry's text, every text part joined."""
    return "".join(part.get("text", "") for part in entry.get("content") or [] if part.get("type") == "text")


def effect_output(state: dict) -> str:
    """What a tool gave back, from its history entry's state."""
    if state.get("outputText"):
        return state["outputText"]
    if state.get("error"):
        return state["error"].get("message") or json.dumps(state["error"])
    output = state.get("output")
    if not isinstance(output, dict):
        return adapters.text_of(output)
    if isinstance(output.get("content"), list):
        # An MCP tool's result: its content parts.
        return adapters.text_of(output["content"])
    if isinstance(output.get("output"), str):
        # A command's: what it printed.
        return output["output"]
    # Any other tool's own result (a file written: its path, whether it was there).
    return json.dumps(output)


def permission_question(callback: dict) -> tuple:
    """An approval callback as weft's permission question, and the output an answer to it becomes."""
    detail = callback.get("detail") or {}
    effect = detail.get("effect") or {}
    display = effect.get("display") or {}
    name = bare(effect.get("toolName") or "tool")
    choices = [ev.choice(c, *CHOICES[c]) for c in detail.get("choices") or [] if c in CHOICES]
    if not any(c["kind"] == ev.REJECT for c in choices):
        # Every request can be turned down, the one way out a person always has.
        choices.append(ev.choice("deny", *CHOICES["deny"]))
    detail_text = display.get("summary") or json.dumps(effect.get("input"))[:500]
    if detail.get("reason"):
        detail_text = f"{detail_text}\n({detail['reason']})"
    question = ev.permission(callback["callbackId"], name, f"Mistral Vibe wants to use {name}", detail_text,
                             effect.get("input"), choices)

    def output_of(answer: dict) -> dict:
        picked = ev.chosen(answer, question)
        return {"type": "approval", "decision": {"type": picked["id"]}, "feedback": answer.get("note") or None}

    return question, output_of


def input_question(callback: dict) -> dict:
    """A user input callback (its `ask_user_question` tool) as weft's question."""
    asked = (callback.get("detail") or {}).get("request") or {}
    return ev.questions(callback["callbackId"], [
        ev.asked(f"q{i}", q.get("header") or "", q.get("question") or "",
                 [{"label": o.get("label", ""), "description": o.get("description") or ""} for o in q.get("options") or []],
                 q.get("multiSelect"), not q.get("hideOther"))
        for i, q in enumerate(asked.get("questions") or [])])


def input_answer(question: dict, answer: dict) -> dict:
    """weft's answer to a question as the user input callback's output: one
    answer per question, a multi-select's joined as Vibe's own clients join it."""
    given = ev.answers_for(answer, question)
    answers = []
    for q in question["questions"]:
        labels = {o["label"] for o in q["options"]}
        values = given[q["id"]]
        answers.append({"question": q["question"], "answer": ", ".join(values), "isOther": any(v not in labels for v in values)})
    return {"type": "user_input", "result": {"answers": answers, "cancelled": False}}


class Socket(socketserver.ThreadingMixIn, socketserver.UnixStreamServer):
    """The agent socket: one connection per request of Vibe's, one line each way."""

    daemon_threads = True

    def __init__(self, path: str, adapter):
        self.adapter = adapter
        super().__init__(path, SocketRequest)


class SocketRequest(socketserver.StreamRequestHandler):
    def handle(self) -> None:
        line = self.rfile.readline()
        if not line:
            return
        message = json.loads(line)
        self.server.adapter.on_socket(message, lambda reply: self._reply(message.get("id"), reply))

    def _reply(self, rid, reply: dict) -> None:
        self.wfile.write((json.dumps({"jsonrpc": "2.0", "id": rid, **reply}) + "\n").encode("utf-8"))
        self.wfile.flush()


class Vibe(adapters.Adapter):
    kind = "vibe"
    program_name = "vibe-app-server"
    permissions = PERMISSIONS
    option_names = tuple(OPTIONS)

    def __init__(self, conversation, config, tools):
        super().__init__(conversation, config, tools)
        self.setup = Setup()
        for name, value in self.options.items():
            if value is not None:
                self.setup = self.setup.add(OPTIONS[name].setup(self.label, name, value))
        self.session = None
        self.socket = None
        self.socket_dir = None
        # Wakes a call of the program's tools once the reader has seen its
        # history entry (`_program_call`).
        self.seen = threading.Condition(self.life)
        self.begin_turn()

    def begin_turn(self) -> None:
        """What one turn gathers, from nothing."""
        self.reply = []
        # The turn's history entries, by (session, entry id), as Vibe patched them so far.
        self.entries = {}
        # The program's calls the route saw, by the action id the socket's
        # `client_tool:<id>` and the history's `effect-<id>` share -> weft's call id.
        self.routed = {}
        # The history entries named like one of the program's tools the
        # reader has seen, by action id.
        self.named = set()
        # Each call, permission and question this turn waits on weft for, by
        # the history entry that says when Vibe is done with it -> weft's id.
        self.waits = {}

    @staticmethod
    def shown_name(plain: str) -> str:
        """The program's tool `plain` as Vibe's history and its socket name it."""
        return f"{NAMESPACE}.{plain}"

    def command(self) -> list:
        return [self.program(), "--agent-socket", os.path.join(self.socket_dir, "agent.sock")]

    def credential_env(self, env: dict) -> None:
        if self.credentials is not None:
            for name in [n for n in env if n.upper().startswith(ENV_PREFIX)]:
                env.pop(name)
        super().credential_env(env)
        env.update(ENV)

    def prepare(self) -> None:
        if self.home is not None:
            self.write_home_file("trusted_folders.toml", f"trusted = [{json.dumps(self.config['folder'])}]\nuntrusted = []\n")
        if self.socket is None:
            # Short and private: a socket's path has a length limit, and Vibe
            # asks for a socket of mode 0600 in a folder of mode 0700.
            self.socket_dir = tempfile.mkdtemp(prefix="weft-vibe-")
            path = os.path.join(self.socket_dir, "agent.sock")
            self.socket = Socket(path, self)
            os.chmod(path, 0o600)
            threading.Thread(target=self.socket.serve_forever, kwargs={"poll_interval": 0.1}, name="vibe-socket", daemon=True).start()

    def stop_rest(self) -> None:
        if self.socket is not None:
            self.socket.shutdown()
            self.socket.server_close()
            self.socket = None
        if self.socket_dir is not None:
            shutil.rmtree(self.socket_dir, ignore_errors=True)
            self.socket_dir = None

    def agent_config(self) -> dict:
        """The session's `agentConfig`, on a start and a resume alike."""
        disabled = sorted(self.program_tools.names & set(REPLACEABLE_TOOL_NAMES))
        return deep_merge({"cwd": self.config["folder"], "agent": PERMISSIONS[self.permission], "disabledTools": disabled},
                          self.setup.session)

    def model_settings(self):
        """The session's `config/model/write`, or None when the node leaves both to Vibe."""
        settings = dict(self.setup.config)
        if self.config.get("model"):
            settings["modelAlias"] = self.config["model"]
        return dict(settings, sessionId=self.session, sessionOnly=True) if settings else None

    # ----- JSON-RPC ----------------------------------------------------------

    def request(self, method: str, params: dict) -> dict:
        rid = self.next_request_id()
        return self.call(rid, {"jsonrpc": "2.0", "id": rid, "method": method, "params": params}, method)

    def post(self, method: str, params: dict) -> None:
        """Send a request whose answer nobody waits for: from the reader's own
        thread, which reads that answer, so it cannot wait for it."""
        rid = self.next_request_id()
        self.send({"jsonrpc": "2.0", "id": rid, "method": method, "params": params})

    def handshake(self) -> None:
        self.request("initialize", {"clientInfo": {"name": "weft", "version": "1"},
                                    "capabilities": {"callbackKinds": ["approval", "user_input"]}})
        self.send({"jsonrpc": "2.0", "method": "initialized", "params": {}})
        conversation = self.conv.conversation_id()
        if conversation:
            try:
                answer = self.request("session/resume", {"sessionId": conversation, "agentConfig": self.agent_config()})
            except Refused as refused:
                raise Refused(f"{refused}; the conversation cannot be picked up again, so start it over from the node's card")
        else:
            answer = self.request("session/start", {"agentConfig": self.agent_config()})
        self.session = answer["state"]["session"]["id"]
        agent = PERMISSIONS[self.permission]
        switched = self.request("session/agent/update", {"sessionId": self.session, "agentName": agent})
        active = ((switched.get("runtime") or {}).get("activeAgent") or {}).get("name")
        if active != agent:
            raise Refused(f"{self.label} did not take the `{agent}` agent the `{self.permission}` permission setting runs it "
                          f"as (its agent is {active!r})")
        settings = self.model_settings()
        if settings is not None:
            self.request("config/model/write", settings)

    def begin(self, turn) -> None:
        self.begin_turn()
        self.request("turn/start", {"sessionId": self.session, "message": [{"type": "text", "text": turn.message}]})
        if not self.conv.conversation_id():
            # Kept once a turn runs on it: Vibe saves a session at its first
            # turn, and cannot resume one that never had any.
            self.conv.set_conversation(self.session)

    # ----- the agent socket: the program's route -----------------------------

    def on_socket(self, message: dict, reply) -> None:
        """One request of Vibe's on the agent socket, answered by `reply`."""
        method, params = message.get("method"), message.get("params") or {}
        if method == "agent/initialize":
            if params.get("protocolVersion") != SOCKET_PROTOCOL:
                reply({"error": {"code": -32600, "message": f"weft speaks version {SOCKET_PROTOCOL} of the agent socket, and "
                                                            f"this Vibe version {params.get('protocolVersion')}"}})
                return
            reply({"result": {"protocolVersion": SOCKET_PROTOCOL, "tools": [tool_declaration(t) for t in self.tools],
                              "toolTimeoutSeconds": TOOL_WAIT_SECS}})
        elif method == "tools/call":
            self._program_call(params, reply)
        else:
            # The sandbox half (`sandbox/*`): weft serves none, so Vibe runs
            # its own tools where it runs.
            reply({"error": {"code": -32601, "message": f"weft does not answer {method}"}})

    def _program_call(self, params: dict, reply) -> None:
        name = params.get("name")
        plain = self.program_tool(name)
        action = (params.get("callbackId") or "").removeprefix("client_tool:")
        call_id = params.get("toolCallId") or action
        arguments = params.get("input") or {}
        settled = threading.Event()

        def answer(result: dict) -> None:
            try:
                reply({"result": tool_reply(result)})
            except OSError as e:
                raise Refused(f"{self.label} no longer waits for this call ({e})") from e
            finally:
                settled.set()

        def refuse(why: str) -> None:
            try:
                reply({"result": {"output": None, "error": {"message": why, "code": "tool_failed"}}})
            except OSError:
                pass
            settled.set()

        with self.life:
            turn = self.turn
            # Vibe writes a call's history entry before it makes the call,
            # and on another channel: waiting here for the reader to have
            # seen it puts the call after everything Vibe wrote before it
            # (in the stream and the history), and makes it the route's
            # before its entry could be shown as the agent's own. Only the
            # main session's: a sub-agent's entries are its own session's.
            main = (params.get("caller") or {}).get("sessionId") == self.session
            while main and action not in self.named and turn is not None and turn.status not in ev.ENDED and self.proc is not None \
                    and self.proc.poll() is None:
                self.seen.wait(1)
            if turn is None or turn.status in ev.ENDED:
                refuse("no turn is running on the weft side")
                return
            if plain is None:
                why = f"{self.label} called {name!r} as one of the program's tools, and the program has no tool of that name"
                refuse(why)
                self.conv.fail(turn, why)
                return
            self.routed[action] = call_id
            self.waits[("effect", f"effect-{action}")] = call_id
            self.block(turn, ev.TOOL_CALL, call_id, {"callId": call_id, "name": plain, "arguments": json.dumps(arguments)},
                       answer, refuse)
        # No deadline: weft answers when the program (or a person) does. The
        # connection stays open until then, which is what keeps Vibe waiting.
        while not settled.wait(60):
            if turn.status in ev.ENDED:
                refuse("the turn ended before this tool was answered")

    # ----- what Vibe writes --------------------------------------------------

    def on_message(self, message: dict) -> None:
        method = message.get("method")
        if method is None:
            self.answered(message)
            return
        turn = self.turn
        params = message.get("params") or {}
        if "id" in message:
            self._server_request(turn, message["id"], method, params)
            return
        if turn is None or turn.status in ev.ENDED:
            return
        if method == "history/entryAdded":
            entry = params.get("entry") or {}
            self.entries[(params.get("sessionId"), entry.get("id"))] = entry
            self._entry(turn, params.get("sessionId"), entry, None)
        elif method == "history/entryUpdated":
            entry = self.entries.get((params.get("sessionId"), params.get("entryId")))
            if entry is not None:
                before = json.loads(json.dumps(entry))
                apply_patch(entry, params.get("patch") or [])
                self._entry(turn, params.get("sessionId"), entry, before)
        elif method == "turn/completed" and params.get("sessionId") == self.session:
            self._turn_completed(turn, params.get("turn") or {})
        elif method == "turn/retrying" and params.get("sessionId") == self.session:
            # Vibe retries a refused request for minutes before it gives up
            # (about 280 s against a rate limit): the card says so meanwhile.
            why = " ".join(str(params[k]) for k in ("category", "detail") if params.get(k))
            self.conv.notice(turn, f"Mistral Vibe is retrying a refused request: {why}" if why else "Mistral Vibe is retrying a refused request")
        # Everything else (`session/updated`, `session/statsUpdated`,
        # `runtime/updated`, `turn/started`, `warning`) says nothing weft
        # shows.

    def _entry(self, turn, session: str, entry: dict, before) -> None:
        """A history entry added (`before` None) or patched (`before`, as it was)."""
        kind = entry.get("type")
        done = entry.get("generationStatus") == "completed" and (before is None or before.get("generationStatus") != "completed")
        if kind == "callback":
            waited = self.waits.get(("callback", entry.get("callbackId")))
            if waited is not None and (entry.get("state") or {}).get("status") != "open":
                # Answered by weft, or Vibe went on without the answer.
                self._stop_waiting(turn, ("callback", entry.get("callbackId")))
            return
        if kind == "effect":
            self._effect(turn, session, entry, before)
            return
        if session != self.session:
            # A sub-agent's messages are nothing of the reply.
            return
        if kind == "message" and entry.get("role") == "assistant":
            if self.config.get("streamBy") == "token":
                piece = text_of(entry)[len(text_of(before)) if before is not None else 0:]
                if piece:
                    self.conv.emit(turn, ev.TEXT_DELTA, text=piece)
            if done and text_of(entry).strip():
                self.reply.append(text_of(entry))
                self.conv.emit(turn, ev.TEXT, text=text_of(entry))
                self.remember_text(text_of(entry))
        elif kind == "reasoning" and done and (entry.get("text") or "").strip():
            self.conv.emit(turn, ev.THINKING, text=entry["text"])

    def _effect(self, turn, session: str, entry: dict, before) -> None:
        """A tool call's history entry: the agent's own, unless the program's route saw it."""
        detail, state = entry.get("detail") or {}, entry.get("state") or {}
        entry_id = entry.get("id") or ""
        action = entry_id.removeprefix("effect-")
        ended = state.get("status") in ("completed", "failed") and (before is None or (before.get("state") or {}).get("status")
                                                                   not in ("completed", "failed"))
        if before is None and self.program_tool(detail.get("toolName")) is not None:
            self.named.add(action)
            self.seen.notify_all()
        if action in self.routed:
            # The program's call: its call and result are the route's
            # (`Conversation.block`, `deliver`). Its end says Vibe is done
            # with it, answered or not.
            if ended:
                self._stop_waiting(turn, ("effect", entry_id))
            return
        if session != self.session:
            # A sub-agent's own tools are nothing of the stream.
            return
        name = detail.get("toolName") or "tool"
        held = self.program_tool(name) is not None
        if before is None and not held:
            self.conv.emit(turn, ev.TOOL_USE, id=entry_id, name=bare(name), input=detail.get("input"))
            self.remember_tool_use(entry_id, bare(name), detail.get("input") or {})
        if not ended:
            return
        if held:
            # Named like one of the program's tools, and it never came
            # through the program's route: one of its own.
            self.conv.emit(turn, ev.TOOL_USE, id=entry_id, name=bare(name), input=detail.get("input"))
            self.remember_tool_use(entry_id, bare(name), detail.get("input") or {})
        output = effect_output(state)
        self.conv.emit(turn, ev.TOOL_RESULT, id=entry_id, output=output, isError=state.get("status") == "failed")
        self.remember_tool_result(entry_id, output)
        written = state.get("output") if isinstance(state.get("output"), dict) else {}
        if detail.get("kind") in FILE_KINDS and state.get("status") == "completed":
            path = written.get("filePath") or (detail.get("input") or {}).get("filePath")
            change = "add" if detail.get("kind") == "file_write" and not written.get("fileExisted") else "update"
            self.conv.emit(turn, ev.FILE_CHANGE, path=path, change=change)

    def _stop_waiting(self, turn, key) -> None:
        call_id = self.waits.pop(key, None)
        if call_id is None:
            return
        try:
            self.conv.stop_waiting(turn, call_id)
        except Refused as refused:
            self.conv.fail(turn, str(refused))

    def _turn_completed(self, turn, ended: dict) -> None:
        status, error = ended.get("status"), ended.get("error") or {}
        if status == "completed" and ended.get("stopReason") == "limit":
            self.conv.fail(turn, f"{self.label} stopped the turn at its limit (the node's `maxTurns`, or a token or price "
                                 "limit of its config) before it ended")
        elif status == "completed":
            self.conv.finish(turn, "\n\n".join(self.reply))
        elif status == "failed":
            self.conv.fail(turn, f"{self.label} could not finish the turn: {error.get('message') or json.dumps(error)}")
        else:
            self.conv.fail(turn, f"{self.label} stopped the turn before it ended ({status})")

    def _server_request(self, turn, rid, method: str, params: dict) -> None:
        if method != "callback/call":
            self.send({"jsonrpc": "2.0", "id": rid, "error": {"code": "not_found", "message": f"weft does not answer {method}"}})
            return
        callback = params.get("callback") or {}
        callback_id, session = callback.get("callbackId"), callback.get("sessionId")
        kind = (callback.get("detail") or {}).get("kind")
        # Taken at once; the answer is a `callback/result` of its own.
        self.send({"jsonrpc": "2.0", "id": rid, "result": {"callbackId": callback_id, "accepted": True}})

        def result(output: dict) -> dict:
            return {"sessionId": session, "result": {"callbackId": callback_id, "output": output}}

        refused = {"type": "approval", "decision": {"type": "deny"}} if kind == "approval" else \
            {"type": "user_input", "result": {"answers": [], "cancelled": True}}

        def refuse(why: str) -> None:
            self.post("callback/result", result(dict(refused, feedback=why) if kind == "approval" else refused))

        if turn is None or turn.status in ev.ENDED:
            refuse("no turn is running on the weft side")
            return
        if kind == "approval":
            question, output_of = permission_question(callback)
            settle = lambda answer: self._settle(result(output_of(answer)))  # noqa: E731
        elif kind == "user_input":
            question = input_question(callback)
            settle = lambda answer: self._settle(result(input_answer(question, answer)))  # noqa: E731
        else:
            refuse(f"weft does not answer a {kind} callback")
            return
        self.waits[("callback", callback_id)] = callback_id
        self.block(turn, ev.QUESTION, callback_id, {"question": question}, settle, refuse)

    def _settle(self, params: dict) -> None:
        """Hand weft's answer over, refused when Vibe did not take it (the
        callback was already settled on its side)."""
        answer = self.request("callback/result", params)
        if answer.get("accepted") is not True:
            raise Refused(f"{self.label} did not take the answer: {json.dumps(answer)}")


adapters.register(Vibe)
