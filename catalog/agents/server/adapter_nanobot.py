"""nanobot (HKUDS), driven through its gateway's WebSocket channel.

nanobot has no stdio protocol: `nanobot gateway` runs the agent and serves
its WebUI's WebSocket, and that is what this adapter speaks, over a unix
socket only this server knows, with a token minted for each start. One
chat is the conversation: `new_chat` (bound to the folder) the first time,
`attach` after a restart, then one `message` per turn. Every message is
sent with `webui: true`, which makes nanobot stamp the turn's id on every
frame it sends back: a frame of any other turn (a job the model scheduled
with its `cron` tool, a subagent's late report) is not weft's, and is left
alone. The turn ends at `turn_end`.

nanobot never asks: no permission request, no ask tool. What it may do is
fixed before the turn, so weft's permission setting is a policy (the
chat's access mode, the shell's sandbox), and the agent carries on when a
tool is refused.

Each conversation is a nanobot instance of its own (`--home`, the config
written fresh at every start), in a home of its own: nanobot's OpenAI Codex
login copies `~/.codex/auth.json` when it has none, so the person's real
home is never its home. Its background model calls (the heartbeat, Dream's
memory consolidation, compaction of an idle chat) are switched off: the
conversation spends nothing between turns. The key reaches nanobot as the
provider's variable, named in the config as `${VAR}` and resolved in
memory, so it is never written to disk.

The program's tools are a tool plugin of nanobot's own (`plugin_nanobot.py`,
found through a `nanobot.tools` entry point in a folder put on the
gateway's `PYTHONPATH`), so the model sees each under its own name, never
nanobot's `mcp_weft_<tool>` for an MCP server's. A plugin tool named like
a built-in is skipped while the built-in is registered, and nanobot turns
its built-ins off only by group: a program tool with a built-in's name
turns that built-in off where its group holds nothing else (`SWITCHES`),
and every other built-in keeps its name (the node's `reservedToolNames`).

The gateway reads nothing on its standard input, so it runs under
`tether.py`, which ends it when the agent server goes. The one model
call nanobot makes on its own is the chat's title, once, after its first
turn (what `webui: true` costs).
"""

import base64
import json
import os
import re
import shutil
import socket
import subprocess
import sys
import tempfile
import threading

import adapters
import events as ev
import program_tools
from events import Refused
from websocket_client import PONG, TEXT, FrameReader, HandshakeFailed, ws_frame, ws_handshake

# weft's permission setting -> the chat's access mode, and what the config
# changes for it. nanobot's shell deny list (`rm -r`, `mkfs`, `dd if=`,
# `shutdown`, ...) holds in every one of them.
PROGRAM = "nanobot"

# SYNC: nanobot's permission settings <-> catalog/agents/server/agents.json (nanobot.permissions)
PERMISSIONS = {
    # File tools stay in the folder, and a shell command may not leave it.
    "restricted": "restricted",
    # The same, and every shell command runs in the OS sandbox (bubblewrap
    # on Linux, seatbelt on macOS), which keeps its files to the folder.
    "sandboxed": "restricted",
    # The folder's files only, and no shell at all.
    "no_shell": "restricted",
    # Anywhere on the machine.
    "full": "full",
}

# The OS sandbox of `sandboxed`, by platform: nanobot's name, and the program it runs.
SANDBOXES = {"linux": ("bwrap", "bwrap"), "darwin": ("seatbelt", "sandbox-exec")}

# Each login way: the provider of nanobot's config it is, and the fields of
# that provider read from the way's variables.
# SYNC: nanobot's login ways <-> catalog/agents/server/agents.json (nanobot.logins)
PROVIDERS = {
    "anthropicKey": ("anthropic", {"apiKey": "ANTHROPIC_API_KEY"}),
    "openaiKey": ("openai", {"apiKey": "OPENAI_API_KEY"}),
    "openrouterKey": ("openrouter", {"apiKey": "OPENROUTER_API_KEY"}),
    "googleKey": ("gemini", {"apiKey": "GEMINI_API_KEY"}),
    "deepseekKey": ("deepseek", {"apiKey": "DEEPSEEK_API_KEY"}),
    "mistralKey": ("mistral", {"apiKey": "MISTRAL_API_KEY"}),
    "openaiCompatible": ("custom", {"apiKey": "OPENAI_COMPATIBLE_API_KEY", "apiBase": "OPENAI_COMPATIBLE_BASE_URL"}),
}
assert set(PROVIDERS) == set(adapters.AGENTS["nanobot"]["logins"]), "agents.json lists nanobot's login ways"

# `agents.defaults.reasoningEffort`; which of them a model takes is the provider's.
EFFORTS = ("none", "low", "medium", "high", "xhigh", "max", "adaptive")

# Variables that would win over the config written here (nanobot reads
# `NANOBOT_<SECTION>__<KEY>` over every config key), or point its token
# files and data somewhere other than its own home.
FOREIGN_ENV_PREFIXES = ("NANOBOT_", "XDG_")
FOREIGN_ENV = ("OAUTH_CLI_KIT_TOKEN_PATH",)

# Each built-in a setting turns off by itself, and that setting's `tools`
# key: nanobot 0.4.0 (`nanobot/agent/tools/`) has one switch per group
# (`<key>.enable`, `imageGeneration.enabled`), and no list or filter of single
# tools, so only a group of one tool is a switch for that tool. The tools of
# the other groups (`file`: `read_file`, `write_file`, ...; `exec`: `exec`,
# `exec_session`, ...; `web`: `web_search`, `web_fetch`) and the built-ins no
# setting turns off (`cron`, `message`, `subagent`, ...) are the node's
# `reservedToolNames`, which the build and AskAgent refuse a program tool of.
SWITCHES = {"run_cli_app": "cliApps", "my": "my", "generate_image": "imageGeneration"}
# The folder of the instance the plugin lives in, on the gateway's `PYTHONPATH`.
PLUGIN_DIR = "weft-tools"
PLUGIN_FILE = os.path.join(os.path.dirname(os.path.abspath(__file__)), "plugin_nanobot.py")

# How long the gateway may take to open its socket, and to answer a
# request of ours, before the turn fails.
STARTUP_SECS = 120
ANSWER_SECS = 60
# How often a starting gateway's socket is looked for: it says nothing when
# it listens, and a connect costs nothing.
SOCKET_LOOK_SECS = 0.02
# The key a frame answering our request carries, by event: nanobot also
# tells every subscriber of the chat `session_updated` without the scope.
ANSWER_CARRIES = {"session_updated": "workspace_scope"}


# ----- the configuration ------------------------------------------------------

def policy(permission: str) -> tuple:
    """The chat's access mode for `permission`, and the `tools` keys it sets."""
    if permission not in PERMISSIONS:
        raise Refused(f"nanobot has no permission setting {permission!r}; it takes {', '.join(PERMISSIONS)}", 400)
    tools = {"restrictToWorkspace": PERMISSIONS[permission] == "restricted", "exec": {}}
    if permission == "sandboxed":
        if sys.platform not in SANDBOXES:
            raise Refused(f"nanobot's `sandboxed` runs commands in bubblewrap (Linux) or seatbelt (macOS), and this machine is {sys.platform}", 400)
        name, program = SANDBOXES[sys.platform]
        if shutil.which(program) is None:
            raise Refused(f"nanobot's `sandboxed` runs every command in {program}, which is not installed here"
                          + ("; install it (`sudo apt-get install -y bubblewrap`)" if program == "bwrap" else ""), 400)
        tools["exec"]["sandbox"] = name
    if permission == "no_shell":
        tools["exec"]["enable"] = False
        tools["cliApps"] = {"enable": False}
    return PERMISSIONS[permission], tools


def whole_number(options: dict, name: str):
    value = options.get(name)
    if value is None:
        return None
    if isinstance(value, bool) or not isinstance(value, int) or value < 1:
        raise Refused(f"nanobot's `{name}` is a whole number above 0, and got {value!r}", 400)
    return value


def strings(options: dict, name: str) -> list:
    value = options.get(name) or []
    if not isinstance(value, list) or not all(isinstance(v, str) and v.strip() for v in value):
        raise Refused(f"nanobot's `{name}` is a list of texts, and got {value!r}", 400)
    return value


def patterns(options: dict, name: str) -> list:
    value = strings(options, name)
    for pattern in value:
        try:
            re.compile(pattern)
        except re.error as e:
            raise Refused(f"nanobot's `{name}` holds {pattern!r}, which is not a regular expression: {e}", 400) from None
    return value


def instance_config(config: dict, permission: str, way: str, workspace: str, socket_path: str, token: str, tools: list) -> dict:
    """The `config.json` of the conversation's nanobot instance."""
    options = config.get("options") or {}
    if not config.get("model"):
        raise Refused(f"nanobot needs a model for the connection's {PROVIDERS[way][0]} key: set `model` to one of its model ids", 400)
    provider, fields = PROVIDERS[way]
    _, tool_keys = policy(permission)
    defaults = {
        "workspace": workspace,
        "provider": provider,
        "model": config["model"],
        # Every model call outside a turn, off: compaction of an idle chat
        # (0 turns it off) and Dream's memory consolidation.
        "idleCompactAfterMinutes": 0,
        "dream": {"enabled": False},
    }
    effort = options.get("effort")
    if effort is not None:
        if effort not in EFFORTS:
            raise Refused(f"nanobot's `effort` takes {', '.join(EFFORTS)}, and got {effort!r}", 400)
        defaults["reasoningEffort"] = effort
    for option, key in (("maxTokens", "maxTokens"), ("contextWindowTokens", "contextWindowTokens"),
                        ("maxToolIterations", "maxToolIterations")):
        value = whole_number(options, option)
        if value is not None:
            defaults[key] = value
    fallbacks = strings(options, "fallbackModels")
    if fallbacks:
        defaults["fallbackModels"] = [{"model": model, "provider": provider} for model in fallbacks]
    tools_config = dict(tool_keys, exec=dict(tool_keys["exec"]))
    runtime = whole_number(options, "commandMaxSeconds")
    if runtime is not None:
        tools_config["exec"]["maxRuntime"] = runtime
    for option in ("denyPatterns", "allowPatterns"):
        value = patterns(options, option)
        if value:
            tools_config["exec"][option] = value
    web = options.get("webTools", True)
    if not isinstance(web, bool):
        raise Refused(f"nanobot's `webTools` is on or off (true or false), and got {web!r}", 400)
    tools_config["web"] = {"enable": web}
    # Its view of its own runtime: nothing a weft conversation asks of it.
    tools_config["my"] = {"enable": False}
    for tool in tools:
        group = SWITCHES.get(tool["name"])
        if group is not None:
            key = "enabled" if group == "imageGeneration" else "enable"
            tools_config[group] = {**tools_config.get(group, {}), key: False}
    return {
        "agents": {"defaults": defaults},
        "providers": {provider: {key: "${" + variable + "}" for key, variable in fields.items()}},
        # Port 0: a health port of its own for every gateway on the machine.
        "gateway": {"port": 0, "heartbeat": {"enabled": False}},
        "channels": {"websocket": {"enabled": True, "unixSocketPath": socket_path, "token": token,
                                   "websocketRequiresToken": True, "streaming": True}},
        "tools": tools_config,
    }


def refusal(frame: dict) -> str:
    """What nanobot's `error` frame says: its `detail`, and its `reason` when
    it gives one; the whole frame when it says neither."""
    if not frame.get("detail"):
        return json.dumps(frame)
    return frame["detail"] + (f" ({frame['reason']})" if frame.get("reason") else "")


def file_change(edit: dict) -> str:
    return {"create": "add", "delete": "delete"}.get(edit.get("operation"), "update")


# ----- the WebSocket ----------------------------------------------------------

def ws_connect(path: str, token: str) -> tuple:
    """A WebSocket on the unix socket `path`: the socket, and what came after the handshake."""
    sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
    sock.settimeout(ANSWER_SECS)
    try:
        sock.connect(path)
    except OSError:
        sock.close()
        raise
    try:
        rest = ws_handshake(sock, "localhost", f"/?client_id=weft&token={token}")
    except HandshakeFailed as failed:
        raise Refused(f"nanobot's gateway {failed}") from None
    return sock, rest


# ----- the adapter ------------------------------------------------------------

class Nanobot(adapters.Adapter):
    kind = "nanobot"
    program_name = PROGRAM
    permissions = PERMISSIONS
    option_names = ("effort", "maxTokens", "contextWindowTokens", "maxToolIterations", "fallbackModels", "commandMaxSeconds",
                    "denyPatterns", "allowPatterns", "webTools", "appendSystemPrompt")

    def __init__(self, conversation, config, tools):
        super().__init__(conversation, config, tools)
        if self.credentials is None:
            raise Refused("nanobot has no login of its own to fall back on: it runs on a model provider's key. "
                          "Wire a NanobotAccess into `access` with the key of the provider to use", 400)
        self.way = self.credentials.get("way")
        self.access, _ = policy(self.permission)
        self.instance = os.path.join(self.home, "instance")
        self.workspace = os.path.join(self.instance, "workspace")
        self.socket_dir = None
        self.token = None
        self.sock = None
        # Requests of the handshake wait in the base's `waiting` (the dict of
        # the gateway's process, so the base fails every one still waiting
        # when it exits), by the event of the frame that answers them:
        # nanobot's frames carry no request id.
        self.chat = None
        # The turn's text so far, by stream, and its reasoning.
        self.streams = {}
        self.thinking = {}
        self.texts = []
        # The ids of the program's own tool calls in this turn.
        self.ours = set()
        # Checked before anything starts.
        instance_config(config, self.permission, self.way, self.workspace, "", "", tools)
        self.instructions = self.options.get("appendSystemPrompt") or ""
        if not isinstance(self.instructions, str):
            raise Refused(f"nanobot's `appendSystemPrompt` is a text, and got {self.instructions!r}", 400)
        self.instructions = self.instructions.strip()

    def command(self) -> list:
        # The gateway runs under the tether (see the module's doc).
        return [sys.executable, adapters.TETHER, self.socket_dir, "--",
                self.program(), "--home", self.instance, "gateway", "--foreground"]

    def credential_env(self, env: dict) -> None:
        for name in [n for n in env if n.startswith(FOREIGN_ENV_PREFIXES) or n in FOREIGN_ENV]:
            env.pop(name)
        super().credential_env(env)
        if self.tools:
            # Where nanobot finds the plugin (see the module's doc).
            env["PYTHONPATH"] = os.pathsep.join(p for p in (self.plugin_dir(), env.get("PYTHONPATH")) if p)

    def plugin_dir(self) -> str:
        return os.path.join(self.instance, PLUGIN_DIR)

    def write_plugin(self) -> None:
        """The program's tools as nanobot's plugin (`plugin_nanobot.py`), or
        no plugin when the program wires none."""
        folder = self.plugin_dir()
        shutil.rmtree(folder, ignore_errors=True)
        if not self.tools:
            return
        info = os.path.join(folder, "weft_nanobot_tools-1.dist-info")
        os.makedirs(info)
        with open(PLUGIN_FILE, encoding="utf-8") as f:
            plugin = f.read()
        files = {
            os.path.join(folder, "weft_nanobot.py"): plugin,
            os.path.join(folder, "server.path"): os.path.dirname(PLUGIN_FILE),
            os.path.join(folder, "tools.json"): program_tools.listing(adapters.mcp_url(), self.tools),
            os.path.join(info, "METADATA"): "Metadata-Version: 2.1\nName: weft-nanobot-tools\nVersion: 1\n",
            os.path.join(info, "entry_points.txt"): "[nanobot.tools]\n" + "".join(
                f"weft_{index} = weft_nanobot:tool_{index}\n" for index in range(len(self.tools))),
        }
        for path, content in files.items():
            with open(path, "w", encoding="utf-8") as f:
                f.write(content)

    def prepare(self) -> None:
        # A unix socket path has a short limit (108 bytes on Linux), which a
        # conversation's home can pass: the socket lives in a folder of its own.
        self.socket_dir = tempfile.mkdtemp(prefix="weft-nanobot-", dir="/tmp")
        self.token = base64.urlsafe_b64encode(os.urandom(24)).decode().rstrip("=")
        os.makedirs(self.workspace, exist_ok=True)
        # Private: it holds the gateway's token.
        config = instance_config(self.config, self.permission, self.way, self.workspace, self.socket_path(), self.token, self.tools)
        adapters.write_whole(os.path.join(self.instance, "config.json"), json.dumps(config, indent=1), 0o600)
        self.write_plugin()
        # nanobot reads USER.md of its workspace into the system prompt as it
        # is, and writes its own blank template there when there is none.
        user = os.path.join(self.workspace, "USER.md")
        if self.instructions:
            with open(user, "w", encoding="utf-8") as f:
                f.write(self.instructions + "\n")
        elif os.path.exists(user):
            os.remove(user)

    def socket_path(self) -> str:
        return os.path.join(self.socket_dir, "ws.sock")

    # ----- the connection ----------------------------------------------------

    def handshake(self) -> None:
        waited = 0.0
        while True:
            try:
                self.sock, rest = ws_connect(self.socket_path(), self.token)
                break
            except (FileNotFoundError, ConnectionRefusedError):
                # Not listening yet.
                pass
            try:
                code = self.proc.wait(SOCKET_LOOK_SECS)
            except subprocess.TimeoutExpired:
                waited += SOCKET_LOOK_SECS
                if waited >= STARTUP_SECS:
                    raise Refused(f"nanobot's gateway did not open its WebSocket within {STARTUP_SECS}s: {self.stderr_tail()}") from None
                continue
            raise Refused(f"nanobot's gateway ended as it started (exit {code}): {self.stderr_tail()}")
        box = self.expecting("ready")
        threading.Thread(target=self._read_socket, args=(self.sock, FrameReader(self.sock.recv, rest)),
                         name="nanobot-ws", daemon=True).start()
        ready = self.answer("ready", box)
        if ready.get("event") != "ready":
            raise Refused(f"nanobot's gateway opened the WebSocket with {ready}")
        chat = self.conv.conversation_id()
        if chat:
            if not self._has_sessions():
                raise Refused(f"nanobot no longer has this conversation (chat {chat}, kept under {self.instance}/sessions); "
                              "start the conversation over from the node's card")
            self.request({"type": "attach", "chat_id": chat}, "attached")
            self.chat = chat
            # The folder and the access mode are the chat's own: given again,
            # for a configuration that changed since.
            [updated] = self.request({"type": "set_workspace_scope", "chat_id": chat, "workspace_scope": self.scope()},
                                     "session_updated")
            self._check_scope(updated)
        else:
            attached, updated = self.request({"type": "new_chat", "workspace_scope": self.scope()}, "attached", "session_updated")
            self.chat = attached["chat_id"]
            self._check_scope(updated)
            self.conv.set_conversation(self.chat)

    def scope(self) -> dict:
        return {"project_path": self.config["folder"], "access_mode": self.access}

    def _check_scope(self, updated: dict) -> None:
        scope = updated.get("workspace_scope") or {}
        if scope.get("access_mode") != self.access or os.path.realpath(scope.get("project_path") or "") != os.path.realpath(self.config["folder"]):
            raise Refused(f"nanobot did not take the folder and access mode weft set ({self.scope()}); it holds {scope}")

    def _has_sessions(self) -> bool:
        sessions = os.path.join(self.instance, "sessions")
        return any(name.endswith(".sqlite3") for _, _, names in os.walk(sessions) for name in names)

    def expecting(self, event: str) -> dict:
        """Wait for a frame `event`: the box it lands in, held by the caller
        from here on. Never looked up again by its event: a gateway that
        exits takes every box out of `waiting` as it fails it."""
        box = {"done": threading.Event()}
        self.waiting[event] = box
        return box

    def answer(self, event: str, box: dict) -> dict:
        came = box["done"].wait(ANSWER_SECS)
        if self.waiting.get(event) is box:
            del self.waiting[event]
        if not came:
            raise Refused(f"nanobot did not send `{event}` within {ANSWER_SECS}s: {self.stderr_tail()}")
        if "error" in box:
            raise Refused(box["error"]["message"])
        return box["frame"]

    def request(self, message: dict, *events: str) -> list:
        """Send `message` and wait for the frames `events`, which answer it
        (`new_chat` is answered twice: `attached`, then `session_updated`)."""
        boxes = [self.expecting(event) for event in events]
        self.send(message)
        return [self.answer(event, box) for event, box in zip(events, boxes)]

    def send(self, message: dict) -> None:
        self._write(TEXT, json.dumps(message).encode("utf-8"))

    def _write(self, opcode: int, payload: bytes) -> None:
        with self.write_lock:
            if self.sock is None or self.proc is None or self.proc.poll() is not None:
                raise Refused(f"{self.label} is no longer running, so it cannot be answered; send the message again")
            self.sock.sendall(ws_frame(opcode, payload))

    def _read_socket(self, sock, reader: FrameReader) -> None:
        try:
            for payload in reader.texts(lambda payload: self._write(PONG, payload)):
                self.on_frame(json.loads(payload))
        except (EOFError, OSError, Refused):
            pass
        except Exception as e:  # noqa: BLE001 (a frame the adapter cannot read fails the turn, loudly)
            import traceback  # noqa: PLC0415
            adapters.log(traceback.format_exc())
            if self.turn is not None:
                self.conv.fail(self.turn, f"the agent server could not read a frame nanobot sent: {type(e).__name__}: {e}")
        if not self.stopping and self.turn is not None and self.turn.status not in ev.ENDED:
            self.conv.fail(self.turn, f"nanobot's gateway closed the WebSocket in the middle of the turn: {self.stderr_tail()}", status=ev.LOST)

    def stop_rest(self) -> None:
        if self.sock is not None:
            try:
                self.sock.shutdown(socket.SHUT_RDWR)
            except OSError:
                pass
            self.sock.close()
        if self.socket_dir is not None:
            shutil.rmtree(self.socket_dir, ignore_errors=True)

    # ----- what nanobot writes -----------------------------------------------

    def on_message(self, message: dict) -> None:
        # The gateway's standard output is its log; the protocol is the socket.
        self.stderr.append(json.dumps(message))

    def begin(self, turn) -> None:
        self.streams, self.thinking, self.texts, self.ours = {}, {}, [], set()
        self.send({"type": "message", "chat_id": self.chat, "content": turn.message, "turn_id": turn.id, "webui": True})

    def on_frame(self, frame: dict) -> None:
        event = frame.get("event")
        pending = [box for name, box in list(self.waiting.items()) if not box["done"].is_set()
                   and (event == "error" or name == event and ANSWER_CARRIES.get(event, "event") in frame)]
        if pending:
            # An answer to a request of the handshake; an error answers all of them.
            for box in pending:
                if event == "error":
                    # The base's shape for a box's error, `{"message": <the whole
                    # sentence>}`, which is what it fails every box still waiting
                    # with when the gateway exits: `answer` reads that one shape.
                    box["error"] = {"message": f"nanobot refused the request: {refusal(frame)}"}
                else:
                    box["frame"] = frame
                box["done"].set()
            return
        turn = self.turn
        if turn is None or turn.status in ev.ENDED:
            return
        if event == "error" and frame.get("chat_id") in (None, self.chat) and frame.get("turn_id") in (None, turn.id):
            # In a turn, the one envelope of ours an error can answer is its message.
            self.conv.fail(turn, f"nanobot refused the message: {refusal(frame)}")
            return
        if frame.get("chat_id") != self.chat or frame.get("turn_id") != turn.id:
            # Another turn's: a scheduled job, a subagent's report.
            return
        if event == "delta":
            stream = frame.get("stream_id", "")
            self.streams.setdefault(stream, []).append(frame.get("text", ""))
            if self.config.get("streamBy") == "token":
                self.conv.emit(turn, ev.TEXT_DELTA, text=frame.get("text", ""))
        elif event == "stream_end":
            self._text(turn, "".join(self.streams.pop(frame.get("stream_id", ""), [])))
        elif event == "reasoning_delta":
            self.thinking.setdefault(frame.get("stream_id", ""), []).append(frame.get("text", ""))
        elif event == "reasoning_end":
            text = "".join(self.thinking.pop(frame.get("stream_id", ""), []))
            if text.strip():
                self.conv.emit(turn, ev.THINKING, text=text)
        elif event == "message":
            if frame.get("tool_events"):
                for tool in frame["tool_events"]:
                    self._tool(turn, tool)
            elif not frame.get("kind"):
                # A whole answer that came unstreamed (a command's, `/new`'s).
                self._text(turn, frame.get("text") or "")
        elif event == "file_edit":
            for edit in frame.get("edits") or []:
                if edit.get("phase") == "end" and edit.get("status") == "done":
                    self.conv.emit(turn, ev.FILE_CHANGE, path=edit.get("absolute_path") or edit.get("path"), change=file_change(edit))
        elif event == "turn_end":
            self._turn_end(turn, frame)

    def _text(self, turn, text: str) -> None:
        if text.strip():
            self.texts.append(text)
            self.conv.emit(turn, ev.TEXT, text=text)
            self.remember_text(text)

    def _tool(self, turn, tool: dict) -> None:
        call_id, name, phase = tool.get("call_id"), tool.get("name") or "", tool.get("phase")
        # Plain names, and the built-in of a program tool's name is turned
        # off (`SWITCHES`): the name is the program's.
        ours = self.program_tool(name) is not None
        if phase == "start":
            if ours:
                self.ours.add(call_id)
            else:
                self.conv.emit(turn, ev.TOOL_USE, id=call_id, name=name, input=tool.get("arguments") or {})
            self.remember_tool_use(call_id, name, tool.get("arguments") or {})
        elif phase in ("end", "error"):
            failed = phase == "error"
            output = str(tool.get("error") if failed else tool.get("result") or "")
            if call_id not in self.ours:
                self.conv.emit(turn, ev.TOOL_RESULT, id=call_id, output=output, isError=failed)
            self.remember_tool_result(call_id, output)

    def _turn_end(self, turn, frame: dict) -> None:
        outcome = frame.get("outcome") or "completed"
        if outcome == "completed":
            self.conv.finish(turn, "\n\n".join(self.texts))
            return
        kind = frame.get("failure_error_kind")
        message = frame.get("failure_message") or frame.get("failure_kind") or "no reason given"
        if kind == "authentication":
            self.conv.fail(turn, f"the model provider refused the connection's key ({message}); "
                                 "reconnect `access` with a key that works")
            return
        self.conv.fail(turn, f"nanobot ended the turn {outcome}: {message}" + (f" ({kind})" if kind else ""))


adapters.register(Nanobot)

