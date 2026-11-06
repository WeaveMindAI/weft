"""Kilo Code CLI, driven through its own server: `kilo serve`, HTTP and an
event stream (server-sent events), the API Kilo's own TUI and editor
extensions use. Its ACP mode (`kilo acp`) does less: Kilo's question tool
is offered to the model there, but the question never reaches the client,
so the turn hangs on it. Over its own server the question is a
`question.asked` event, answered with `POST /question/<id>/reply`.

At its start Kilo prints the address it listens on; the adapter reads it,
subscribes to `GET /event` (every event of the folder's instance, the
sub-agents' too) and waits for its `server.connected` before anything
else, so no event of a turn is missed. Then `POST /session` makes the
conversation (or `GET /session/<id>` finds it again in a new process: Kilo
keeps it in its own database). One `POST /session/<id>/prompt_async` per
turn; the turn ends at `session.turn.close`, whose `reason` says how.
The server answers only with a password made for this start
(`KILO_SERVER_PASSWORD`, in the agent's environment alone; Kilo takes it
out of the environment of every command it runs).

`KILO_CLIENT` is `weft`: Kilo turns on a set of tools only for its own
terminal and editor clients (scheduled wake-ups, cron jobs, background
processes, goals, a plan's follow-up question), which would start turns
nobody sent or ask what weft never asked for. Its question tool is among
them, and `KILO_ENABLE_QUESTION_TOOL` turns that one back on.

Its quirks, and what the adapter does about each:

  - Its permission settings are OpenCode 1's `permission` map, handed in
    `KILO_PERMISSION`, which Kilo merges last, after the folder's own
    `kilo.json`: a project that allows `bash` still asks.
  - A permission refused with a note goes back to the model with the note,
    and the turn goes on; refused without one, it fails that tool and ends
    the turn, which then fails saying so.
  - A turn names its agent (Kilo's "mode") and its model, or Kilo takes the
    conversation's own or its config's. No setting but `plan` names an
    agent, since a person's Kilo config may turn Kilo's own `code` off and
    make an agent of their own the default: the agent and the model Kilo
    picked for the conversation's first turn that named neither are kept
    with it, and named on every turn after, so a conversation once in
    `plan`, or once on the node's model, goes back to how it began. An
    agent the config no longer offers (`plan`, or the one it began in)
    fails the turn naming the ones it does offer.
  - Its `plan` agent offers the model no tool it does not know, the
    program's included, and no setting lifts that: `plan` with the
    program's tools is refused.
  - With no model, it picks a model of Kilo's own gateway whatever the
    login is, so a provider's key needs the node's `model`.
  - An effort level is a "variant" of the model, and Kilo ignores one the
    model does not have: the turn fails on it instead, once Kilo says which
    model it runs.
  - It reads Claude Code's folder (`~/.claude`: instructions, skills) as
    config of its own; `KILO_DISABLE_CLAUDE_CODE` turns that off.
  - The program's tools are a plugin of weft's own (`kilo_weft.mjs`),
    named in `KILO_CONFIG_CONTENT`: each keeps its plain name (an MCP
    server's would be `weft_<tool>`), and Kilo puts its plugins' tools after
    its own built-ins in the session's tool map, so a program tool named
    like a built-in (`read`, `bash`, `question`, ...) is the one the model
    calls. The plugin forwards each call to weft's MCP route, the program's
    route, which writes it in the history; Kilo's own stream shows it as a
    tool part under its plain name, which, since the program's tool always
    wins its name, is how the adapter tells it apart. A plugin named in the
    config content, unlike one in a config folder, needs no package
    install. What is reserved (`reservedToolNames`) are the tools Kilo adds
    after its plugins' when an MCP server of the person's has resources.

A sub-agent (Kilo's `task` tool) is a session of its own, made with the
conversation's as its parent: its permissions and questions are answered
like the main agent's, and only the main session's messages and its own
tools make the reply, the stream and the history.
"""

import base64
import json
import os
import re
import secrets
import signal
import sys
import threading
import urllib.error
import urllib.request

import adapters
import events as ev
import js_plugin
from adapter_acp import Lever, Setup
from events import Refused

# What `ask` asks about; reading, searching, folders outside `folder` (which
# Kilo asks about already) and the program's tools keep Kilo's own rules.
ASKED = ("bash", "edit", "webfetch", "websearch")
# Kilo's question tool, which weft passes on to the person: an agent of the
# person's own config denies it unless told otherwise (Kilo's own agents
# allow it), and its own config can still turn it off for that agent.
QUESTION = {"question": "allow"}
ASK = {**{action: "ask" for action in ASKED}, **QUESTION}
# SYNC: Kilo's permission settings <-> catalog/agents/server/agents.json (kilo.permissions), catalog/agents/kilo_code/metadata.json (permission)
PERMISSIONS = {
    "ask": ASK,
    "accept_edits": {**ASK, "edit": "allow"},
    # Its plan agent reads, runs read-only commands and writes only its plan file.
    "plan": QUESTION,
    "bypass": {"*": "allow"},
}
PLAN_AGENT = "plan"

# The login ways that are a model provider's key, and so need a model.
PROVIDER_KEYS = ("anthropicKey", "openaiKey", "openrouterKey", "googleKey")

ENV = {"KILO_DISABLE_CLAUDE_CODE": "1", "KILO_DISABLE_AUTOUPDATE": "1", "KILO_DISABLE_SHARE": "1",
       "KILO_DISABLE_SESSION_INGEST": "1", "KILO_CLIENT": "weft", "KILO_ENABLE_QUESTION_TOOL": "1"}
# The person's own, which would change what weft sets. `KILO_PURE` would
# leave out every plugin, weft's too.
UNSET_ENV = ("KILO_PERMISSION", "KILO_CONFIG_CONTENT", "KILO_PURE", "KILO_SERVER_USERNAME", "KILO_SERVER_PASSWORD")
# Kilo finds its config, login and sessions through these, under `HOME`: in
# a private home they follow it, on the machine's login they stay.
HOME_UNSET = ("XDG_CONFIG_HOME", "XDG_DATA_HOME", "XDG_STATE_HOME", "XDG_CACHE_HOME")
USERNAME = "kilo"

# The loader of weft's plugin, in weft's own folder for the agent.
PLUGIN = js_plugin.plugin_file("kilo_weft.mjs")
LOADER = "weft-kilo-plugin.mjs"

# What Kilo prints once it listens.
LISTENING = re.compile(r"listening on (http://\S+)")

OPTIONS = {
    "effort": Lever(Lever.TEXT, lambda v: Setup(session={"variant": v}),
                    ["none", "minimal", "low", "medium", "high", "xhigh", "max"],
                    "How hard the model thinks, among the levels the model offers"),
}

# Each turn's end (`session.turn.close`'s reason) other than `completed`.
STOPPED = {"interrupted": "stopped the turn before it ended", "superseded": "dropped the turn for another message",
           "error": "could not finish the turn"}

# Kilo's word that one of its requests was settled, by weft's answer or
# without it.
ANSWERED = ("permission.replied", "question.replied", "question.rejected")


def model_of(text: str) -> dict:
    """The node's `model` (`<provider>/<model>`) as a prompt names it."""
    provider, slash, model = text.partition("/")
    if not slash or not provider or not model:
        raise Refused(f"Kilo Code's `model` is `<provider>/<model>` (`anthropic/claude-sonnet-4-6`, ...), and got {text!r}", 400)
    return {"providerID": provider, "modelID": model}


def error_text(error: dict) -> str:
    """A `session.error`'s error, as a person reads it."""
    data = error.get("data") or {}
    message = data.get("message") or json.dumps(data)[:500]
    return f"{message} ({error['name']})" if error.get("name") else message


def describe(request: dict) -> tuple:
    """A permission request's title and detail."""
    permission, meta = request.get("permission") or "tool", request.get("metadata") or {}
    patterns = ", ".join(request.get("patterns") or [])
    if permission == "bash":
        return "Kilo Code wants to run a command", meta.get("command") or patterns
    if permission == "edit":
        return "Kilo Code wants to change a file", meta.get("filepath") or patterns
    if permission == "webfetch":
        return "Kilo Code wants to fetch a URL", meta.get("url") or patterns
    if permission == "websearch":
        return "Kilo Code wants to search the web", meta.get("query") or patterns
    if permission == "external_directory":
        return "Kilo Code wants to work outside its folder", patterns
    if permission == "doom_loop":
        return "Kilo Code keeps calling the same tool the same way", patterns
    return f"Kilo Code asks for permission ({permission})", patterns or json.dumps(meta)[:500]


def permission_question(question_id: str, request: dict) -> dict:
    title, detail = describe(request)
    choices = [ev.choice("once", "Allow", ev.ALLOW_ONCE)]
    if request.get("always"):
        choices.append(ev.choice("always", f"Allow {', '.join(request['always'])} from now on", ev.ALLOW_ALWAYS))
    choices.append(ev.choice("reject", "Deny", ev.REJECT))
    return ev.permission(question_id, request.get("permission") or "tool", title, detail, request.get("metadata") or {}, choices)


def permission_reply(question: dict, answer: dict) -> dict:
    """What `POST /permission/<id>/reply` is sent for weft's answer: a note
    goes to the model with a refusal."""
    picked = ev.chosen(answer, question)
    if picked["kind"] == ev.REJECT and answer.get("note"):
        return {"reply": "reject", "message": answer["note"]}
    return {"reply": picked["id"]}


def asked_questions(question_id: str, request: dict) -> dict:
    """A `question.asked` (its question tool: several questions at once) as weft's question."""
    asked = []
    for index, q in enumerate(request.get("questions") or []):
        options = [{"label": o.get("label") or "", "description": o.get("description") or ""} for o in q.get("options") or []]
        # An answer of the person's own is taken unless the question says not.
        asked.append(ev.asked(f"q{index}", q.get("header") or "", q.get("question") or "", options, q.get("multiple"),
                              q.get("custom") is not False or not options))
    return ev.questions(question_id, asked)


def question_reply(question: dict, answer: dict) -> dict:
    """What `POST /question/<id>/reply` is sent: each question's answers, in order."""
    given = ev.answers_for(answer, question)
    return {"answers": [given[q["id"]] for q in question["questions"]]}


def file_changes(tool: str, state: dict) -> list:
    """The files a finished tool changed, as (path, change)."""
    meta = state.get("metadata") or {}
    if tool == "write" and meta.get("filepath"):
        return [(meta["filepath"], "update" if meta.get("exists") else "add")]
    if tool == "edit" and (meta.get("filediff") or {}).get("file"):
        return [(meta["filediff"]["file"], "update")]
    if tool == "apply_patch":
        changed = []
        for f in meta.get("files") or []:
            # Its kinds are weft's (`add`, `update`, `delete`), and `move`.
            if f.get("type") == "move":
                changed += [(f.get("filePath"), "delete"), (f.get("movePath"), "add")]
            else:
                changed.append((f.get("filePath"), f.get("type")))
        return changed
    return []


class Events:
    """Kilo's standard output until it says where it listens, then its event
    stream, one event's JSON per item: what the base adapter's reader reads
    as the agent's lines. The rest of its standard output goes with what it
    printed. The stream ends when Kilo does; a stream that ends while Kilo
    still runs stops Kilo, so the turn fails saying so."""

    def __init__(self, adapter, proc):
        self.adapter = adapter
        self.proc = proc
        self.stdout = proc.stdout
        self.stream = None

    def __iter__(self):
        for line in self.stdout:
            found = LISTENING.search(line)
            if found:
                self.adapter.listening(self.proc, found.group(1))
                break
            if line.strip():
                self.adapter.stderr.append(line.rstrip())
        else:
            return
        threading.Thread(target=self._drain, name="kilo-stdout", daemon=True).start()
        try:
            request = urllib.request.Request(found.group(1) + "/event", headers={**self.adapter.auth(), "Accept": "text/event-stream"})
            # No timeout: a turn may wait on a person for hours, with only
            # Kilo's heartbeat on the stream meanwhile.
            self.stream = urllib.request.urlopen(request, timeout=None)  # noqa: S310 (Kilo's own local address)
            data = []
            for raw in self.stream:
                line = raw.decode("utf-8", "replace").rstrip("\r\n")
                if line.startswith("data:"):
                    data.append(line[5:].lstrip())
                elif not line and data:
                    yield "\n".join(data)
                    data = []
        except (OSError, ValueError) as e:
            if not self.adapter.stopping:
                self.adapter.stderr.append(f"weft lost Kilo Code's event stream: {e}")
        if self.proc.poll() is None and not self.adapter.stopping:
            self.adapter.stderr.append("Kilo Code's event stream ended while it was still running, so weft stopped it")
            adapters.signal_group(self.proc, signal.SIGTERM)

    def _drain(self) -> None:
        for line in self.stdout:
            if line.strip():
                self.adapter.stderr.append(line.rstrip())

    def close(self) -> None:
        if self.stream is not None:
            self.stream.close()
        self.stdout.close()


class Kilo(adapters.Adapter):
    kind = "kilo"
    program_name = "kilo"
    permissions = PERMISSIONS
    option_names = tuple(OPTIONS)

    def __init__(self, conversation, config, tools):
        super().__init__(conversation, config, tools)
        self.variant = None
        for name, value in self.options.items():
            if value is not None:
                self.variant = OPTIONS[name].setup(self.label, name, value).session.get("variant") or self.variant
        self.model = model_of(config["model"]) if config.get("model") else None
        if self.credentials is not None and self.credentials["way"] in PROVIDER_KEYS and self.model is None:
            raise Refused(f"{self.label} needs a model for a connection's provider key: set `model` to `<provider>/<model>` of "
                          "that provider (`anthropic/claude-sonnet-4-6`, ...), or it would pick a model of Kilo's own gateway", 400)
        if self.permission == "plan" and tools:
            raise Refused(f"{self.label}'s plan mode offers the model no tool it does not know, the program's included, and no "
                          "setting lifts that; pick another permission setting, or hand this agent no tools", 400)
        # Per start: where Kilo listens, and the password it answers to.
        self.url = None
        self.password = None
        self.config_content = None
        self.connected = False
        self.session = None
        # The conversation's sessions: its own, and its sub-agents'.
        self.family = set()
        # What the conversation began with (`started`): its agent and model.
        self.started = {}
        # The models' effort levels, by `<provider>/<model>`, read when the
        # node sets one.
        self.variants = None
        self.begin_turn()

    def begin_turn(self) -> None:
        """What one turn gathers, from nothing."""
        self.reply = []
        self.error = None
        # Why the turn fails when Kilo ends it as if it went well: a refusal
        # without a note in the main session, or the person stopping it
        # while the model could not be reached.
        self.ended_by = None
        # Whether Kilo took this turn's message (its user message came).
        self.taken = False
        # Each part's type, by id, for its deltas; the parts already shown.
        self.part_types = {}
        self.shown = set()
        # The messages of the person's side (this turn's, and any Kilo adds
        # itself), whose parts are nothing of the reply.
        self.user_messages = set()
        # The tool calls already shown, by call id.
        self.calls = set()
        # Kilo's id of each permission and question this turn waits on weft
        # for -> the id weft waits on it by.
        self.waits = {}

    # ----- the process -------------------------------------------------------

    def command(self) -> list:
        # Under the tether: Kilo's server reads nothing on its standard input,
        # so it would outlive an agent server that dies, held up for good.
        return [sys.executable, adapters.TETHER, "--", self.program(), "serve", "--port", "0", "--hostname", "127.0.0.1"]

    def prepare(self) -> None:
        self.url, self.connected = None, False
        self.password = secrets.token_urlsafe(32)
        self.config_content = None
        if self.tools:
            # In a folder of weft's own: the private home, or the
            # conversation's folder on the machine's own login.
            folder = self.home if self.home is not None else self.conv.home
            loader = js_plugin.write_loader(os.path.join(folder, LOADER), PLUGIN, self.tools)
            self.config_content = json.dumps({"plugin": [js_plugin.uri(loader)]})

    def credential_env(self, env: dict) -> None:
        for name in UNSET_ENV:
            env.pop(name, None)
        if self.home is not None:
            for name in HOME_UNSET:
                env.pop(name, None)
        env.update(ENV)
        env["KILO_PERMISSION"] = json.dumps(PERMISSIONS[self.permission])
        if self.config_content is not None:
            env["KILO_CONFIG_CONTENT"] = self.config_content
        env["KILO_SERVER_USERNAME"] = USERNAME
        env["KILO_SERVER_PASSWORD"] = self.password
        super().credential_env(env)

    def _read_out(self, proc, waiting: dict) -> None:
        proc.stdout = Events(self, proc)
        super()._read_out(proc, waiting)

    def listening(self, proc, url: str) -> None:
        with self.life:
            if proc is self.proc:
                self.url = url

    def auth(self) -> dict:
        token = base64.b64encode(f"{USERNAME}:{self.password}".encode()).decode()
        return {"Authorization": f"Basic {token}"}

    def api(self, method: str, path: str, body=None):
        """One request to Kilo's server; its answer's JSON. Refused with what
        Kilo said when it refuses."""
        if self.url is None:
            raise Refused(f"{self.label} is not listening yet, so it cannot be asked {method} {path}")
        data = json.dumps(body).encode("utf-8") if body is not None else None
        request = urllib.request.Request(self.url + path, data=data, method=method,
                                         headers={**self.auth(), "Content-Type": "application/json"})
        try:
            with urllib.request.urlopen(request, timeout=adapters.RPC_TIMEOUT_SECS) as answer:  # noqa: S310
                text = answer.read().decode("utf-8")
        except urllib.error.HTTPError as e:
            with e:
                said = e.read().decode("utf-8", "replace")
            try:
                said = ((json.loads(said).get("data") or {}).get("message")) or said
            except (ValueError, AttributeError):
                pass
            raise Refused(f"{self.label} refused {method} {path}: {said[:500]} (HTTP {e.code})") from None
        except OSError as e:
            raise Refused(f"{self.label} did not answer {method} {path}: {e}: {self.stderr_tail()}") from None
        return json.loads(text) if text.strip() else None

    def handshake(self) -> None:
        # Nothing is asked before the event stream is open: an event Kilo
        # sends before it would be lost.
        box = {"done": threading.Event()}
        with self.life:
            if self.connected:
                box["done"].set()
            else:
                # Failed by the reader with what Kilo printed, if it exits first.
                self.waiting["connected"] = box
        if not box["done"].is_set() and self.proc.poll() is not None:
            raise Refused(f"{self.label} exited before it was listening: {self.stderr_tail()}")
        if not box["done"].wait(adapters.RPC_TIMEOUT_SECS):
            self.waiting.pop("connected", None)
            raise Refused(f"{self.label} did not start listening within {adapters.RPC_TIMEOUT_SECS}s: {self.stderr_tail()}")
        if "error" in box:
            raise Refused(box["error"]["message"])
        offered = [a["name"] for a in self.api("GET", "/agent") or [] if a.get("mode") != "subagent" and not a.get("hidden")]
        conversation = self.conv.conversation_id()
        if conversation:
            try:
                self.api("GET", f"/session/{conversation}")
            except Refused as refused:
                raise Refused(f"{refused}; the conversation cannot be picked up again, so start it over from the node's card")
            self.session = conversation
            self.started = dict(self.conv.conversation_kept().get("started") or {})
        else:
            self.session = self.api("POST", "/session", {})["id"]
            self.started = {}
            self.conv.set_conversation(self.session, {"started": self.started})
        self.family = {self.session}
        names = ", ".join(f"`{a}`" for a in offered) or "none"
        if self.permission == "plan" and PLAN_AGENT not in offered:
            raise Refused(f"{self.label} has no `{PLAN_AGENT}` mode here, which the `plan` permission setting runs it in: "
                          f"{self.label}'s own config turns it off (the modes it offers: {names}); turn it back on there, or "
                          "pick another permission setting", 400)
        if self.permission != "plan" and self.started.get("agent") and self.started["agent"] not in offered:
            raise Refused(f"{self.label} has no `{self.started['agent']}` mode any more, the mode this conversation started "
                          f"in: {self.label}'s own config turns it off now (the modes it offers: {names}); turn it back on "
                          "there, or start the conversation over from the node's card", 400)
        if self.variant is not None:
            listed = self.api("GET", "/config/providers") or {}
            self.variants = {f"{p['id']}/{model_id}": sorted((model.get("variants") or {}).keys())
                             for p in listed.get("providers") or [] for model_id, model in (p.get("models") or {}).items()}

    def begin(self, turn) -> None:
        self.begin_turn()
        body = {"parts": [{"type": "text", "text": turn.message}]}
        agent = PLAN_AGENT if self.permission == "plan" else self.started.get("agent")
        if agent:
            body["agent"] = agent
        model = self.model or self.started.get("model")
        if model:
            body["model"] = model
        if self.variant is not None:
            body["variant"] = self.variant
        self.api("POST", f"/session/{self.session}/prompt_async", body)

    # ----- what Kilo writes ----------------------------------------------------

    def on_message(self, message: dict) -> None:
        kind, props = message.get("type"), message.get("properties") or {}
        if kind == "server.connected":
            self.connected = True
            box = self.waiting.pop("connected", None)
            if box is not None:
                box["done"].set()
            return
        if kind == "session.created":
            info = props.get("info") or {}
            if info.get("parentID") in self.family:
                self.family.add(info.get("id"))
            return
        turn = self.turn
        if turn is None or turn.status in ev.ENDED or props.get("sessionID") not in self.family:
            return
        main = props.get("sessionID") == self.session
        if kind == "permission.asked":
            self._permission(turn, props, main)
        elif kind == "question.asked":
            self._question(turn, props)
        elif kind == "session.status":
            self._status(turn, props.get("status") or {})
        elif kind in ANSWERED and props.get("requestID") in self.waits:
            # Weft's answer reached it, or it went on without one.
            try:
                self.conv.stop_waiting(turn, self.waits.pop(props["requestID"]))
            except Refused as refused:
                self.conv.fail(turn, str(refused))
        elif not main:
            return
        elif kind == "message.updated":
            self._message(turn, props.get("info") or {})
        elif kind == "message.part.delta":
            if (props.get("field") == "text" and self.part_types.get(props.get("partID")) == "text"
                    and self.config.get("streamBy") == "token" and props.get("delta")):
                self.conv.emit(turn, ev.TEXT_DELTA, text=props["delta"])
        elif kind == "message.part.updated":
            self._part(turn, props.get("part") or {})
        elif kind == "session.error":
            self.error = error_text(props.get("error") or {})
        elif kind == "session.turn.close" and self.taken:
            self._closed(turn, props.get("reason"))

    def _message(self, turn, info: dict) -> None:
        if info.get("role") != "user":
            return
        self.user_messages.add(info.get("id"))
        if self.taken:
            return
        # The message Kilo took for this turn, with the agent and the model
        # it runs it on.
        self.taken = True
        model = info.get("model") or {}
        picked = {"providerID": model.get("providerID"), "modelID": model.get("modelID")}
        kept = dict(self.started)
        if self.permission != "plan" and "agent" not in kept and info.get("agent"):
            kept["agent"] = info["agent"]
        if self.model is None and "model" not in kept and picked["modelID"]:
            kept["model"] = picked
        if kept != self.started:
            self.started = kept
            self.conv.set_conversation(self.session, {"started": kept})
        if self.variant is not None:
            name = f"{picked['providerID']}/{picked['modelID']}"
            offered = self.variants.get(name, [])
            if self.variant not in offered:
                why = (f"{self.label}'s model {name} has no `{self.variant}` effort level (it offers: "
                       f"{', '.join(offered) or 'none'}); pick one of those, or leave `effort` empty")
                # Stopped before the model is asked anything.
                try:
                    self.api("POST", f"/session/{self.session}/abort")
                except Refused as refused:
                    why += f" ({refused})"
                self.conv.fail(turn, why)

    def _part(self, turn, part: dict) -> None:
        part_id, kind = part.get("id"), part.get("type")
        if part.get("messageID") in self.user_messages:
            return
        self.part_types[part_id] = kind
        ended = (part.get("time") or {}).get("end") is not None
        if kind in ("text", "reasoning") and ended and part_id not in self.shown:
            self.shown.add(part_id)
            text = part.get("text") or ""
            if not text.strip() or part.get("synthetic"):
                return
            if kind == "reasoning":
                self.conv.emit(turn, ev.THINKING, text=text)
                return
            self.reply.append(text)
            self.conv.emit(turn, ev.TEXT, text=text)
            self.remember_text(text)
        elif kind == "tool":
            self._tool(turn, part)

    def _tool(self, turn, part: dict) -> None:
        call_id, name, state = part.get("callID"), part.get("tool") or "tool", part.get("state") or {}
        status = state.get("status")
        if status == "pending":
            # Its arguments are still being written.
            return
        if self.program_tool(name) is not None:
            # The program's tool always wins its name (see the module's
            # docstring): the call went through the program's route, which
            # wrote it in the history and streams it as a `tool_call`.
            return
        if call_id not in self.calls:
            self.calls.add(call_id)
            self.conv.emit(turn, ev.TOOL_USE, id=call_id, name=name, input=state.get("input") or {})
            self.remember_tool_use(call_id, name, state.get("input") or {})
        if status in ("completed", "error"):
            output = state.get("output") if status == "completed" else state.get("error")
            output = adapters.text_of(output)
            self.conv.emit(turn, ev.TOOL_RESULT, id=call_id, output=output, isError=status == "error")
            self.remember_tool_result(call_id, output)
            if status == "completed":
                for path, change in file_changes(name, state):
                    self.conv.emit(turn, ev.FILE_CHANGE, path=path, change=change)

    def _closed(self, turn, reason: str) -> None:
        if reason == "completed" and self.error is None:
            if self.ended_by is not None:
                self.conv.fail(turn, self.ended_by)
            else:
                self.conv.finish(turn, "\n\n".join(self.reply))
            return
        what = STOPPED.get(reason, f"ended the turn ({reason})")
        self.conv.fail(turn, f"{self.label} {what}: {self.error}" if self.error else f"{self.label} {what}")

    # ----- what Kilo asks --------------------------------------------------------

    def _permission(self, turn, request: dict, main: bool) -> None:
        request_id = request.get("id")
        question = permission_question(f"{self.kind}-{request_id}", request)
        self.waits[request_id] = question["id"]

        def answer(given: dict) -> None:
            reply = permission_reply(question, given)
            if reply["reply"] == "reject" and "message" not in reply and main:
                self.ended_by = (f"{self.label} ended the turn when `{question['permission']['title']}` was refused: refused "
                                 "without a note, it stops its whole turn rather than going on without it. Send a message "
                                 "saying what to do instead, or refuse with a note next time.")
            self.api("POST", f"/permission/{request_id}/reply", reply)

        self.block(turn, ev.QUESTION, question["id"], {"question": question}, answer,
                   lambda why: self.api("POST", f"/permission/{request_id}/reply", {"reply": "reject", "message": why}))

    def _question(self, turn, request: dict) -> None:
        request_id = request.get("id")
        question = asked_questions(f"{self.kind}-{request_id}", request)
        self.waits[request_id] = question["id"]
        self.block(turn, ev.QUESTION, question["id"], {"question": question},
                   lambda given: self.api("POST", f"/question/{request_id}/reply", question_reply(question, given)),
                   lambda why: self.api("POST", f"/question/{request_id}/reject"))


    def _status(self, turn, status: dict) -> None:
        """Kilo retrying its model, said on the card (until the next event):
        a request refused (`retry`, with its attempt), or the network lost
        (`offline`). Offline, Kilo waits on its own,
        probes the network every few seconds and goes on by itself once it is
        back (`SessionNetwork.ask`, `watch`, `restore`), so weft answers
        nothing and only shows it."""
        message = status.get("message") or "no reason given"
        if status.get("type") == "retry":
            self.conv.notice(turn, f"{self.label} is retrying its model (attempt {status.get('attempt')}): {message}")
        elif status.get("type") == "offline":
            self.conv.notice(turn, f"{self.label} cannot reach its model ({message}); it goes on by itself once the "
                                   "network is back")


adapters.register(Kilo)
