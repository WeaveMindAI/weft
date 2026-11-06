"""OpenClaw, driven through its own Gateway protocol: `openclaw gateway run`,
and a WebSocket to it.

OpenClaw's agent is its Gateway: one long-running process that holds the
model calls, the tools and the sessions. Its ACP mode (`openclaw acp`) is
a second process relaying to that Gateway, and it relays less than the
Gateway says: it passes on shell approvals, and drops the questions of
OpenClaw's own `ask_user` tool, which then end unanswered while the model
carries on. Spoken to directly, the Gateway hands over both (2026.11.1):
an `exec.approval.requested` event answered with `exec.approval.resolve`,
and a `question.requested` event answered with `question.resolve`.

The Gateway is this conversation's alone: started in the conversation's
private home (`OPENCLAW_HOME`), under `tether.py` (it reads nothing on its
standard input, and would outlive a server that died), and stopped with
the adapter. Its config, `openclaw.json`, is written fresh at every start
with a port and a token of its own, on loopback. This side connects as the
Gateway's own kind of local client (`gateway-client` in `backend` mode),
which a loopback connection with the Gateway's token may be without the
signed device identity every other client needs, and asks for every
operator scope: an approval reaches only a client that may see it, and
for a request made by the agent's own tool that is an admin.

One conversation is one Gateway session of the one agent the config
declares, `weft`, under a key this side mints (`agent:weft:weft-<id>`),
created with `sessions.create` and looked up again with
`sessions.describe` after a restart: the Gateway keeps every session's
history in its own database in the private home, and a message to a key
it does not know would start an empty one. One `chat.send` per turn, its
`idempotencyKey` the run's id; everything after comes as events of that
run: `agent` events (its text, its thinking and its tools as they
happen) and the `chat` event that ends it (`final`, `error`, `aborted`).
Every approval and question on this Gateway is this conversation's,
whichever run asked (a sub-agent's included).

What the config turns off, so an idle Gateway calls no model and starts
nothing on its own:
  - the heartbeat (`heartbeat.every: 0m`, a model call every 30 minutes by
    default) and the whole scheduler (`cron.enabled: false`), which runs
    the heartbeat and every job the Gateway or its model would schedule;
  - memory dreaming, which creates a job of its own at the first start;
  - the model call a background command's exit would wake (`notifyOnExit`);
  - the tools that would wake it later (scheduled jobs, standing intents)
    or change its settings;
  - the update check, and session pruning, which would drop a conversation
    idle for 30 days.

The agent's workspace (memory, its own files) is in the private home and
its working directory is the node's `folder`, with the bootstrap files
(AGENTS.md, SOUL.md, ...) never written. Its tools are the `coding`
profile's, each a tool of its own (its tool search off), the program's
included.

Only shell commands ask (`tools.exec.mode`): `allow-once`, `allow-always`
(the same command runs without asking from then on) and `deny`, and a
command nobody answers is denied after 30 minutes, a wait OpenClaw fixes in
its code. File writes and edits never ask, and stay in the folder in every
setting but `bypass`; `read_only` takes them and the shell away. In `ask`,
a command made of several (`a; b`) is refused without asking.

Its questions are its `ask_user` tool: one to three questions, each with
its options and a free answer. The model says how long one waits; weft's
plugin raises every one to the hour OpenClaw allows at most, and a question
still unanswered then ends with no answer and the model carries on
(`question.resolved`, the wait given up). Its credential prompt (`secrets`)
and the tools that wait on the Control UI stay off.
SYNC: ASK_TOOL, ASK_MAX_SECS <-> catalog/agents/server/openclaw_weft.mjs

The program's tools are a plugin of weft's own in the private home
(`openclaw_weft.mjs`, listed in `plugins.load.paths`), each tool under its
plain name, where an MCP server's would be `weft__<tool>`; the plugin is
the program's route. OpenClaw skips a plugin tool named like one of its
core tools, so those names are refused when the program is built and when
AskAgent fires (the node's `reservedToolNames`), and a call under one of the program's names
is the program's.

The model is the provider's own model id; the login way names the provider,
and the model reaches OpenClaw as `<provider>/<model>`. It never runs on the
machine's own OpenClaw: that one's Gateway holds its own logins, channels and
jobs, shared with everything else there.
"""

import json
import os
import secrets
import socket
import subprocess
import sys
import threading
import uuid

import adapters
import events as ev
import js_plugin
from adapter_acp import Lever, Setup
from events import Refused
from websocket_client import PONG, TEXT, FrameReader, HandshakeFailed, ws_frame, ws_handshake

PROGRAM = "openclaw"
# The one OpenClaw agent of the conversation's Gateway.
AGENT = "weft"
# The Gateway protocol this adapter speaks (`connect` refuses another).
PROTOCOL_VERSION = 4
# What this side is to the Gateway: its own local client, which a loopback
# connection with the shared token may be without a device identity.
CLIENT = {"id": "gateway-client", "displayName": "weft", "version": "1", "platform": sys.platform, "mode": "backend"}
# Every operator scope: an approval the agent's own tool asks for is shown
# only to an admin, and `sessions.create` is an admin's.
SCOPES = ["operator.admin", "operator.read", "operator.write", "operator.approvals", "operator.questions"]
# The tool events and the shell approvals, which the Gateway sends only to
# a client that says it takes them.
CAPS = ["tool-events", "exec-approvals"]

# The provider each login way's key is for (OpenClaw's provider ids).
# SYNC: OpenClaw's login ways <-> catalog/agents/server/agents.json (openclaw.logins)
PROVIDERS = {"anthropicKey": "anthropic", "openaiKey": "openai", "geminiKey": "google", "openRouter": "openrouter",
             "openaiCompatible": "custom"}
# The folder of weft's plugin, in the private home.
PLUGIN = "weft-plugin"
# Tools weft takes away in every setting: the ones that wait on a person
# through a surface weft has not (a masked secret prompt, the Control UI's
# terminals, portals and dashboard), and the ones that wake the Gateway
# later or change it (scheduled jobs, standing intents, the Gateway's own
# settings).
DENIED = ["secrets", "terminal", "portal", "dashboard", "intent", "group:automation"]
# What `read_only` takes away: the shell (which `deny` refuses anyway) and
# the file tools that change a file, so the model is offered none of them.
READ_ONLY_DENIED = ["exec", "process", "write", "edit", "apply_patch"]

# weft's permission setting -> OpenClaw's `tools.exec.mode`, and the tools it denies.
# SYNC: OpenClaw's permission settings <-> catalog/agents/server/agents.json (openclaw.permissions), catalog/agents/open_claw/metadata.json (permission)
PERMISSIONS = {"ask": ("ask", []), "auto": ("auto", []), "read_only": ("deny", READ_ONLY_DENIED), "bypass": ("full", [])}

# The session's thinking level (`sessions.patch` `thinkingLevel`), set at every
# start: left out, none, which is the Gateway's own default (`adaptive`).
OPTIONS = {
    "effort": Lever(Lever.TEXT, lambda v: Setup(session={"thinkingLevel": v}),
                    ["off", "minimal", "low", "medium", "high", "xhigh", "max", "ultra", "adaptive"],
                    "How much the model reasons, among the levels the model offers"),
}

# Set on the Gateway's environment: no banner or notes in its output, no
# update of its own.
ENV = {"OPENCLAW_HIDE_BANNER": "1", "OPENCLAW_SUPPRESS_NOTES": "1", "OPENCLAW_NO_AUTO_UPDATE": "1"}
# The person's own OpenClaw settings, which would point the Gateway at
# their state, their config or another Gateway, and Gemini's second key
# variable, which wins over the one a Gemini key login sets.
UNSET_ENV = ("OPENCLAW_STATE_DIR", "OPENCLAW_CONFIG_PATH", "OPENCLAW_PROFILE", "OPENCLAW_WORKSPACE_DIR",
             "OPENCLAW_GATEWAY_TOKEN", "OPENCLAW_GATEWAY_PASSWORD", "OPENCLAW_GATEWAY_URL", "OPENCLAW_GATEWAY_PORT",
             "OPENCLAW_INCLUDE_ROOTS", "OPENCLAW_LOAD_SHELL_ENV", "GOOGLE_API_KEY")

# How long a starting Gateway may take to take this side's connection, and
# to answer a request of ours, before the turn fails: waits on the Gateway
# itself, never on a person.
STARTUP_SECS = 120
ANSWER_SECS = 60
# How often a starting Gateway's port is looked at: a refused connect
# costs nothing.
PORT_LOOK_SECS = 0.1

# The decisions a shell approval offers, as weft's choices.
DECISIONS = {"allow-once": ("Allow once", ev.ALLOW_ONCE),
             "allow-always": ("Allow this command from now on", ev.ALLOW_ALWAYS),
             "deny": ("Deny", ev.REJECT)}


def reserved_port() -> int:
    """A loopback port for the Gateway that nothing else on the machine is
    handed while it starts. The Gateway takes its port only from its config
    (it refuses 0) and binds it seconds later, so a port merely found free
    and let go is free for anyone meanwhile: the next process asking the
    system for any port is often given that same one (the system's range of
    such ports can be a few thousand), and this side then connects to
    whoever listens there instead of its own Gateway. So the port is left
    in TIME_WAIT: one connection to it, closed from its listening side
    first. For TIME_WAIT's minute, a bind to any port and a connect's own
    port both pass over it, and a bind to exactly it with SO_REUSEADDR
    takes it, which is how Node (libuv) binds every TCP port."""
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as listener:
        listener.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        listener.bind(("127.0.0.1", 0))
        listener.listen(1)
        port = listener.getsockname()[1]
        with socket.create_connection(("127.0.0.1", port)) as client:
            accepted, _ = listener.accept()
            # The side that closes first keeps the port in TIME_WAIT.
            accepted.close()
            client.recv(1)
    return port


def gateway_config(adapter, port: int, token: str) -> dict:
    """The files written into the private home at every start: the
    Gateway's config, and weft's plugin."""
    model = adapter.config.get("model")
    if not model:
        raise Refused(f"{adapter.label} needs `model`: the provider's own model id, such as claude-sonnet-4-5 with an Anthropic key", 400)
    way = adapter.credentials["way"]
    provider = PROVIDERS[way]
    mode, denied = PERMISSIONS[adapter.permission]
    plugin = os.path.join(adapter.home, PLUGIN)
    config = {
        "gateway": {"mode": "local", "port": port, "bind": "loopback", "auth": {"mode": "token", "token": token}},
        "agents": {
            "defaults": {"heartbeat": {"every": "0m"}, "skipBootstrap": True},
            "entries": {AGENT: {"workspace": os.path.join(adapter.home, "workspace"), "cwd": adapter.config["folder"],
                                "model": f"{provider}/{model}"}},
        },
        "cron": {"enabled": False},
        # weft's plugin loads at every start, with the program's tools or
        # none: it also holds the wait of every question (the module's doc).
        "plugins": {"load": {"paths": [plugin]},
                    "entries": {"weft": {"enabled": True}, "memory-core": {"config": {"dreaming": {"enabled": False}}}}},
        "update": {"checkOnStart": False},
        "session": {"maintenance": {"mode": "warn"}},
        "logging": {"file": os.path.join(adapter.home, "openclaw.log")},
        # The coding tools (files, the shell, the web, sessions, memory, its
        # question tool) and the program's, each a tool of its own rather
        # than behind its tool search; none of the browser, device, voice or
        # channel tools.
        "tools": {"profile": "coding", "exec": {"mode": mode, "notifyOnExit": False}, "deny": DENIED + denied,
                  "toolSearch": False, "fs": {"workspaceOnly": adapter.permission != "bypass"}},
    }
    if way == "openaiCompatible":
        # The key is read from the environment, so it is never written down.
        config["models"] = {"providers": {provider: {
            "baseUrl": adapter.login_value("baseUrl"), "apiKey": "${OPENAI_API_KEY}", "api": "openai-completions",
            "models": [{"id": model, "name": model}]}}}
    return {".openclaw/openclaw.json": json.dumps(config, indent=2), **plugin_files(adapter.tools)}


def plugin_files(tools: list) -> dict:
    """weft's plugin folder: its manifest declares every tool (OpenClaw finds
    a plugin's tools there without loading it) and puts each in the `coding`
    profile, the tools the agent runs with."""
    names = [t["name"] for t in tools]
    manifest = {"id": "weft", "name": "weft", "description": "The tools of the weft program this agent works for.",
                "contracts": {"tools": names}, "toolMetadata": {name: {"profiles": ["coding"]} for name in names},
                "activation": {"onStartup": True}, "configSchema": {"type": "object", "additionalProperties": False}}
    package = {"name": "weft-openclaw-plugin", "version": "1.0.0", "type": "module",
               "openclaw": {"extensions": ["./index.js"]}}
    return {f"{PLUGIN}/openclaw.plugin.json": json.dumps(manifest, indent=2),
            f"{PLUGIN}/package.json": json.dumps(package, indent=2),
            f"{PLUGIN}/index.js": js_plugin.loader(js_plugin.plugin_file("openclaw_weft.mjs"), tools)}


def text_of_message(message) -> str:
    """The text of a `chat` event's message (its `content` parts)."""
    if not isinstance(message, dict):
        return ""
    return "".join(part.get("text", "") for part in message.get("content") or []
                   if isinstance(part, dict) and part.get("type") == "text")


def file_changes(details) -> list:
    """The files a tool changed, from the unified diff its result carries
    (`details.patch`, as `write` and `edit` give it): (path, change)."""
    patch = details.get("patch") if isinstance(details, dict) else None
    if not isinstance(patch, str):
        return []
    changes, old = [], None
    for line in patch.splitlines():
        if line.startswith("--- "):
            old = line[4:].strip()
        elif line.startswith("+++ "):
            new = line[4:].strip()
            if new == "/dev/null":
                changes.append((old, "delete"))
            else:
                changes.append((new, "add" if old == "/dev/null" or details.get("created") else "update"))
    return changes


def permission_question(approval: dict) -> dict:
    """An `exec.approval.requested` event as weft's permission question."""
    request = approval.get("request") or {}
    offered = request.get("allowedDecisions") or list(DECISIONS)
    unknown = [d for d in offered if d not in DECISIONS]
    if unknown:
        raise Refused(f"OpenClaw offers the decision {', '.join(unknown)} on a shell command, which weft does not know", 500)
    choices = [ev.choice(d, DECISIONS[d][0], DECISIONS[d][1]) for d in offered]
    detail = request.get("command") or ""
    if request.get("cwd"):
        detail = f"{detail}\n(in {request['cwd']})"
    return ev.permission(approval["id"], "exec", "OpenClaw wants to run a command", detail, request, choices)


def input_question(asked: dict) -> dict:
    """A `question.requested` event (its `ask_user` tool) as weft's question:
    each of its questions under its own id, which the answer keeps."""
    questions = []
    for q in asked.get("questions") or []:
        options = [{"label": o.get("label", ""), "description": o.get("description") or ""} for o in q.get("options") or []]
        questions.append(ev.asked(q["questionId"], q.get("header") or "", q.get("question") or "", options,
                                  q.get("multiSelect"), q.get("isOther") is True or not options, q.get("isSecret") is True))
    return ev.questions(asked["id"], questions)


class GatewayRefused(Refused):
    """The Gateway answered a request of ours with an error."""

    def __init__(self, method: str, error: dict):
        super().__init__(f"OpenClaw's Gateway refused {method}: {adapters.error_text(error)}")
        self.error = error


class OpenClaw(adapters.Adapter):
    kind = "openclaw"
    program_name = PROGRAM
    permissions = PERMISSIONS
    option_names = tuple(OPTIONS)

    def __init__(self, conversation, config, tools):
        super().__init__(conversation, config, tools)
        if self.credentials is None:
            raise Refused(f"{self.label} runs only on a connection's login: your machine's own OpenClaw Gateway holds its "
                          "own logins, channels and scheduled jobs, shared with everything else that runs OpenClaw there; "
                          "wire an access node to `access`", 400)
        self.setup = Setup()
        for name, value in self.options.items():
            if value is not None:
                self.setup = self.setup.add(OPTIONS[name].setup(self.label, name, value))
        self.port = None
        self.token = None
        self.sock = None
        # Set by the socket's first frame (`_open`).
        self.challenge = None
        self.key = None
        self.begin_turn()

    def begin_turn(self) -> None:
        """What one turn gathers, from nothing."""
        # The run this turn is (`chat.send`'s `idempotencyKey`).
        self.run = None
        # Its text and thinking so far, by item, in the order they came:
        # each a whole message once the next thing happens.
        self.writing = {}
        self.thinking = {}
        self.texts = []
        # Each approval and question this turn waits on weft for, by the
        # Gateway's id (which is the id weft waits on it by).
        self.waits = set()

    # ----- the process -------------------------------------------------------

    def command(self) -> list:
        return [sys.executable, adapters.TETHER, "--", self.program(), "gateway", "run"]

    def credential_env(self, env: dict) -> None:
        for name in UNSET_ENV:
            env.pop(name, None)
        env.update(ENV)
        super().credential_env(env)

    def prepare(self) -> None:
        self.port = reserved_port()
        self.token = secrets.token_urlsafe(32)
        # Private: the config carries the token minted for this start.
        for path, content in gateway_config(self, self.port, self.token).items():
            self.write_home_file(path, content, private=True)

    def on_message(self, message: dict) -> None:
        # The Gateway's standard output is its log; the protocol is the socket.
        self.stderr.append(json.dumps(message))

    # ----- the connection ----------------------------------------------------

    def handshake(self) -> None:
        hello = self._connect()
        if hello.get("protocol") != PROTOCOL_VERSION:
            raise Refused(f"this {self.label} speaks version {hello.get('protocol')} of its Gateway protocol, and weft speaks "
                          f"version {PROTOCOL_VERSION}; install the version weft names, or update weft")
        key = self.conv.conversation_id()
        if key:
            found = self.request("sessions.describe", {"key": key}).get("session")
            if not found:
                raise Refused(f"{self.label} no longer has this conversation (session {key}, kept under {self.home}); "
                              "start the conversation over from the node's card")
        else:
            key = f"agent:{AGENT}:weft-{uuid.uuid4()}"
            self.request("sessions.create", {"key": key, "cwd": self.config["folder"]})
            self.conv.set_conversation(key)
        self.key = key
        # Every start, so a level taken off since goes back to the Gateway's own.
        self.request("sessions.patch", {"key": key, "thinkingLevel": self.setup.session.get("thinkingLevel")})

    def _connect(self) -> dict:
        """A WebSocket to the Gateway and its `connect`: its `hello-ok`. A
        Gateway still starting refuses `connect` with a wait to retry after,
        and then answers nothing more on that socket: each try is a socket
        of its own."""
        params = {"minProtocol": PROTOCOL_VERSION, "maxProtocol": PROTOCOL_VERSION, "client": CLIENT, "role": "operator",
                  "scopes": SCOPES, "caps": CAPS, "auth": {"token": self.token}}
        waited = 0.0
        while True:
            self._open()
            try:
                return self.request("connect", params)
            except GatewayRefused as refused:
                # `{"code": "UNAVAILABLE", "retryable": true, "retryAfterMs": 500,
                # "details": {"reason": "startup-sidecars"}}` while it starts.
                after = refused.error.get("retryAfterMs")
                if refused.error.get("retryable") is not True or not after or waited >= STARTUP_SECS:
                    raise
            self.stop_rest()
            self._wait_on_gateway(after / 1000)
            waited += after / 1000

    def _open(self) -> None:
        """A WebSocket to the Gateway once it listens, read from then on, and
        its first frame: the Gateway speaks first, with a `connect.challenge`."""
        waited = 0.0
        while True:
            try:
                sock = socket.create_connection(("127.0.0.1", self.port), timeout=ANSWER_SECS)
                break
            except ConnectionRefusedError:
                pass
            if waited >= STARTUP_SECS:
                raise Refused(f"{self.label}'s Gateway did not take a connection within {STARTUP_SECS}s: {self.stderr_tail()}")
            self._wait_on_gateway(PORT_LOOK_SECS)
            waited += PORT_LOOK_SECS
        try:
            rest = ws_handshake(sock, f"127.0.0.1:{self.port}", "/")
        except HandshakeFailed as failed:
            raise Refused(f"{self.label}'s Gateway {failed}: {self.stderr_tail()}") from None
        self.challenge = threading.Event()
        with self.write_lock:
            self.sock = sock
        threading.Thread(target=self._read_socket, args=(sock, FrameReader(sock.recv, rest)), name="openclaw-ws", daemon=True).start()
        if not self.challenge.wait(ANSWER_SECS):
            raise Refused(f"{self.label}'s Gateway sent no `connect.challenge` within {ANSWER_SECS}s: {self.stderr_tail()}")

    def _wait_on_gateway(self, secs: float) -> None:
        """Wait `secs` for a starting Gateway, refused at once if it ends meanwhile."""
        try:
            code = self.proc.wait(secs)
        except subprocess.TimeoutExpired:
            return
        raise Refused(f"{self.label}'s Gateway ended as it started (exit {code}): {self.stderr_tail()}")

    def request(self, method: str, params: dict, timeout=ANSWER_SECS) -> dict:
        rid = f"weft-{self.next_request_id()}"
        try:
            return self.call(rid, {"type": "req", "id": rid, "method": method, "params": params}, method, timeout)
        except adapters.RpcRefused as refused:
            raise GatewayRefused(method, refused.error) from None

    def post(self, method: str, params: dict) -> None:
        """Send a request whose answer nobody waits for: from the socket's own
        reader, which reads that answer, so it cannot wait for it."""
        rid = f"weft-{self.next_request_id()}"
        self.send({"type": "req", "id": rid, "method": method, "params": params})

    def _write(self, message: dict, expect) -> dict:
        # The base's one way out, onto the socket instead of a pipe.
        payload = json.dumps(message).encode("utf-8")
        with self.write_lock:
            if self.sock is None or self.proc is None or self.proc.poll() is not None:
                raise Refused(f"{self.label} is no longer running, so it cannot be answered; send the message again")
            waiting = self.waiting
            if expect is not None:
                waiting[expect[0]] = expect[1]
            try:
                self.sock.sendall(ws_frame(TEXT, payload))
            except BaseException:
                if expect is not None:
                    waiting.pop(expect[0], None)
                raise
        return waiting

    def _pong(self, payload: bytes) -> None:
        with self.write_lock:
            if self.sock is not None:
                self.sock.sendall(ws_frame(PONG, payload))

    def _read_socket(self, sock, reader: FrameReader) -> None:
        try:
            for payload in reader.texts(self._pong):
                self.on_frame(json.loads(payload))
        except (EOFError, OSError):
            pass
        except Exception as e:  # noqa: BLE001 (a frame the adapter cannot read fails the turn, loudly)
            import traceback  # noqa: PLC0415
            adapters.log(traceback.format_exc())
            if self.turn is not None:
                self.conv.fail(self.turn, f"the agent server could not read a frame {self.label}'s Gateway sent: "
                                          f"{type(e).__name__}: {e}")
        with self.life:
            if sock is not self.sock or self.stopping:
                return
            if self.turn is not None and self.turn.status not in ev.ENDED:
                self.conv.fail(self.turn, f"{self.label}'s Gateway closed the WebSocket in the middle of the turn: "
                                          f"{self.stderr_tail()}", status=ev.LOST)

    def stop_rest(self) -> None:
        with self.write_lock:
            sock, self.sock = self.sock, None
        if sock is not None:
            try:
                sock.shutdown(socket.SHUT_RDWR)
            except OSError:
                pass
            sock.close()

    # ----- what the Gateway sends ----------------------------------------------

    def on_frame(self, frame: dict) -> None:
        kind = frame.get("type")
        if kind == "res":
            if frame.get("ok"):
                self.answered({"id": frame.get("id"), "result": frame.get("payload") or {}})
            else:
                self.answered({"id": frame.get("id"), "error": frame.get("error") or {"message": json.dumps(frame)}})
            return
        if kind != "event":
            return
        event, payload = frame.get("event"), frame.get("payload") or {}
        if event == "connect.challenge":
            self.challenge.set()
            return
        turn = self.turn
        if event == "exec.approval.requested":
            self._approval(turn, payload)
        elif event == "question.requested":
            self._question(turn, payload)
        elif event in ("exec.approval.resolved", "question.resolved"):
            self._resolved(turn, payload.get("id"))
        elif turn is None or turn.status in ev.ENDED or payload.get("runId") != self.run:
            # Between turns, or another run's (a sub-agent's): nothing of this turn.
            return
        elif event == "agent":
            self._agent(turn, payload.get("stream"), payload.get("data") or {})
        elif event == "chat":
            self._chat(turn, payload)

    def begin(self, turn) -> None:
        self.begin_turn()
        self.run = str(uuid.uuid4())
        # The message is the model's, whatever it starts with: a `/new` or a
        # `/config` in it is never one of the Gateway's own commands.
        ack = self.request("chat.send", {"sessionKey": self.key, "message": turn.message, "idempotencyKey": self.run,
                                         "suppressCommandInterpretation": True})
        if ack.get("status") != "started":
            raise Refused(f"{self.label} did not start the turn: {json.dumps(ack)}")

    def _agent(self, turn, stream: str, data: dict) -> None:
        if stream == "assistant":
            item = data.get("itemId")
            if data.get("replace"):
                self.writing[item] = data.get("text") or ""
            else:
                self.writing[item] = self.writing.get(item, "") + (data.get("delta") or "")
            if self.config.get("streamBy") == "token" and data.get("delta"):
                self.conv.emit(turn, ev.TEXT_DELTA, text=data["delta"])
        elif stream == "thinking":
            item = data.get("itemId")
            if data.get("phase") == "persisted":
                text = self.thinking.pop(item, "")
                if text.strip():
                    self.conv.emit(turn, ev.THINKING, text=text)
            elif "delta" in data:
                self.thinking[item] = self.thinking.get(item, "") + (data.get("delta") or "")
        elif stream == "tool":
            self._tool(turn, data)

    def _flush(self, turn) -> None:
        """The text written so far, each item a whole message."""
        for text in self.writing.values():
            if text.strip():
                self.texts.append(text)
                self.conv.emit(turn, ev.TEXT, text=text)
                self.remember_text(text)
        self.writing = {}

    def _tool(self, turn, data: dict) -> None:
        call_id, name, phase = data.get("toolCallId"), data.get("name") or "tool", data.get("phase")
        ours = self.program_tool(name) is not None
        if phase == "start":
            self._flush(turn)
            if not ours:
                self.conv.emit(turn, ev.TOOL_USE, id=call_id, name=name, input=data.get("args") or {})
            # The program's is the route's to write (`remember_tool_use`).
            self.remember_tool_use(call_id, name, data.get("args") or {})
        elif phase == "result":
            result = data.get("result") or {}
            output = adapters.text_of(result.get("content")) or str(data.get("error") or "")
            if not ours:
                self.conv.emit(turn, ev.TOOL_RESULT, id=call_id, output=output, isError=data.get("isError") is True)
                for path, change in file_changes(result.get("details")):
                    self.conv.emit(turn, ev.FILE_CHANGE, path=path, change=change)
            self.remember_tool_result(call_id, output)

    def _chat(self, turn, payload: dict) -> None:
        state = payload.get("state")
        if state == "final":
            self._flush(turn)
            self.conv.finish(turn, text_of_message(payload.get("message")) or "\n\n".join(self.texts))
        elif state == "error":
            self.conv.fail(turn, f"{self.label} could not finish the turn: {payload.get('errorMessage') or json.dumps(payload)}")
        elif state == "aborted":
            self.conv.fail(turn, f"{self.label} stopped the turn before it ended")

    def _approval(self, turn, approval: dict) -> None:
        approval_id = approval.get("id")

        def refuse(why: str) -> None:
            self.post("exec.approval.resolve", {"id": approval_id, "decision": "deny"})

        if turn is None or turn.status in ev.ENDED:
            # Nobody to ask: the command is denied at once rather than in 30 minutes.
            refuse("no turn is running on the weft side")
            return
        question = permission_question(approval)
        self.waits.add(approval_id)
        self.block(turn, ev.QUESTION, approval_id, {"question": question},
                   lambda answer: self.request("exec.approval.resolve",
                                               {"id": approval_id, "decision": ev.chosen(answer, question)["id"]}),
                   refuse)

    def _question(self, turn, asked: dict) -> None:
        question_id = asked.get("id")

        def refuse(why: str) -> None:
            self.post("question.resolve", {"id": question_id, "cancel": True})

        if turn is None or turn.status in ev.ENDED:
            refuse("no turn is running on the weft side")
            return
        question = input_question(asked)

        def answer(given: dict) -> None:
            answered = self.request("question.resolve", {"id": question_id, "answers": {"answers": ev.answers_for(given, question)}})
            if answered.get("status") != "answered":
                raise Refused(f"{self.label} did not take the answer (the question was {answered.get('status')})")

        self.waits.add(question_id)
        self.block(turn, ev.QUESTION, question_id, {"question": question}, answer, refuse)

    def _resolved(self, turn, request_id) -> None:
        """An approval or a question ended on the Gateway's side: weft's answer
        reached it, or it went on without one (its wait ran out)."""
        if request_id not in self.waits or turn is None:
            return
        self.waits.discard(request_id)
        try:
            self.conv.stop_waiting(turn, request_id)
        except Refused as refused:
            self.conv.fail(turn, str(refused))


adapters.register(OpenClaw)
