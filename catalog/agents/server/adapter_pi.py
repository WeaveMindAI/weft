"""Pi (the pi coding agent), driven through `pi --mode rpc`: Pi's own
protocol, one JSON object per line on stdin and stdout, records tagged by
`type` (not JSON-RPC). A command carries a string `id` and is answered by a
`response` with that id; the turn's events carry none.

There is no handshake. One `prompt` per turn, answered at once with its
`disposition` (`started` when a run begins); the turn ends at
`agent_settled`, never at `agent_end`, since retries, compaction and
queued follow-ups can run after `agent_end`. A failed model call is the last
assistant message with `stopReason: "error"`.

Pi asks nothing before a tool and has no tool for asking a person. Both come
from weft's own extension, `pi_weft.ts`, loaded with `-e` through a loader
written into Pi's home at every start (`js_plugin.py`): under `ask` and
`accept_edits` it asks before each tool call that needs it, and it adds
`ask_user`. Each goes out as one of Pi's RPC dialogs, an
`extension_ui_request` (`select` for a permission, `input` for a question)
whose title is a JSON object naming what is asked; the answer is an
`extension_ui_response` with the same id. Pi runs several tool calls of one
message at once, so two questions can be open together: they reach weft one
at a time, in the order Pi asked them.

The conversation is a Pi session under an id this side mints
(`--session-id`, which opens the session or creates it). Pi keeps one per
folder: in another folder the same id is a new, empty session, which a
resume refuses rather than carrying on with no history.

Everything Pi keeps (its `models.json`, its sessions) lives in a home of its
own, `PI_CODING_AGENT_DIR`, never the person's `~/.pi/agent`, where a Claude
Pro/Max sign-in may wait: the conversation always runs on the connection's
API key. The program's tools are the extension's too: it registers each
under its plain name (an MCP server's would be `mcp__weft__<tool>`), in the
place of Pi's built-in of that name, and forwards its calls to the agent
server's MCP route. So a tool call whose name is one of the program's is
the program's, and the program's `tool_call` event comes from that route.
The folder's own `.pi/` settings, extensions and MCP servers are never
loaded (`--no-approve`), so nothing in it can change what Pi may do.
"""

import json
import os
import threading
import uuid

import adapters
import events as ev
import js_plugin
from events import Refused

PROGRAM = "pi"

# SYNC: these names <-> catalog/agents/server/pi_weft.ts (ASK_TOOL, PERMISSION_FLAG, CHOICES, PERMISSIONS)
EXTENSION = js_plugin.plugin_file("pi_weft.ts")
# The extension's loader, in Pi's home.
LOADER = "weft-extension.ts"
ASK_TOOL = "ask_user"
PERMISSION_FLAG = "--weft-permission"

# weft's permission setting -> the tools Pi gets (`--tools`, the program's
# added), or None for its default four (read, bash, edit, write) and every
# extension tool. What asks is the extension's.
# SYNC: Pi's permission settings <-> catalog/agents/server/agents.json (pi.permissions), pi_weft.ts (PERMISSIONS)
PERMISSIONS = {
    # Reading inside the folder goes ahead; every other tool call asks.
    "ask": None,
    # Reading and editing inside the folder go ahead; commands and anything outside it ask.
    "accept_edits": None,
    # Pi's tools that only read, the ask tool and the program's tools: nothing asks.
    "read_only": ["read", "grep", "find", "ls", ASK_TOOL],
    "bypass": None,
}

# A permission's choices, as the extension takes them.
CHOICES = (("allow_once", "Allow once", ev.ALLOW_ONCE),
           ("allow_always", "Allow {tool} from now on", ev.ALLOW_ALWAYS),
           ("reject", "Refuse", ev.REJECT))

# Each login way: the Pi provider its key is for. `openaiCompatible` is a
# provider of weft's own, declared in the home's `models.json`.
# SYNC: Pi's login ways <-> catalog/agents/server/agents.json (pi.logins)
COMPATIBLE = "compatible"
PROVIDERS = {
    "anthropicKey": "anthropic",
    "openaiKey": "openai",
    "googleKey": "google",
    "openrouterKey": "openrouter",
    "deepseekKey": "deepseek",
    "mistralKey": "mistral",
    "xaiKey": "xai",
    "groqKey": "groq",
    "openaiCompatible": COMPATIBLE,
}
assert set(PROVIDERS) == set(adapters.AGENTS["pi"]["logins"]), "agents.json lists Pi's login ways"
# The connection value of `openaiCompatible` written into `models.json`; its
# key stays in the environment, which `models.json` names as `$VARIABLE`.
HOST_VALUE = "host"

THINKING = ("off", "minimal", "low", "medium", "high", "xhigh", "max")

# The home's file the appended system prompt is written to: Pi reads a
# value that names an existing file as that file, so text always goes
# through one.
APPENDED_PROMPT = "weft-system-prompt.md"

# How long Pi may take to answer a command of ours before the turn fails.
ANSWER_SECS = 60


def command(config: dict, permission: str, provider: str, session: str, home: str, tools: list) -> list:
    """The `pi --mode rpc` command line for `config`, with the program's `tools`."""
    options = config.get("options") or {}
    cmd = [adapters.program(config, PROGRAM), "--mode", "rpc", "--provider", provider, "--model", config["model"],
           "--session-id", session, "-e", os.path.join(home, LOADER), PERMISSION_FLAG, permission, "--no-approve"]
    allowed = PERMISSIONS[permission]
    if allowed is not None:
        cmd += ["--tools", ",".join(dict.fromkeys([*allowed, *(t["name"] for t in tools)]))]
    effort = options.get("effort")
    if effort is not None:
        if effort not in THINKING:
            raise Refused(f"Pi's `effort` takes {', '.join(THINKING)}, and got {effort!r}", 400)
        cmd += ["--thinking", effort]
    if appended_prompt(options):
        cmd += ["--append-system-prompt", os.path.join(home, APPENDED_PROMPT)]
    no_context = options.get("noContextFiles", False)
    if not isinstance(no_context, bool):
        raise Refused(f"Pi's `noContextFiles` is on or off (true or false), and got {no_context!r}", 400)
    if no_context:
        cmd.append("--no-context-files")
    return cmd


def appended_prompt(options: dict) -> str:
    text = options.get("appendSystemPrompt") or ""
    if not isinstance(text, str):
        raise Refused(f"Pi's `appendSystemPrompt` is a text, and got {text!r}", 400)
    return text.strip()


def models_config(host: str, model: str) -> dict:
    """`models.json` declaring the OpenAI-compatible server as a provider, its key read from the environment."""
    key = adapters.AGENTS["pi"]["logins"]["openaiCompatible"]["env"]["key"]
    return {"providers": {COMPATIBLE: {"baseUrl": host, "api": "openai-completions", "apiKey": f"${key}",
                                       "models": [{"id": model}]}}}


def asked(request: dict):
    """What an `extension_ui_request` of weft's extension asks (its JSON title), or None for anything else."""
    try:
        what = json.loads(request.get("title") or "")
    except json.JSONDecodeError:
        return None
    if not isinstance(what, dict) or what.get("weft") not in ("permission", "question"):
        return None
    return what


def permission_question(question_id: str, what: dict) -> dict:
    tool, tool_input = what.get("tool") or "", what.get("input")
    if isinstance(tool_input, dict) and isinstance(tool_input.get("command"), str):
        detail = tool_input["command"]
    elif what.get("path"):
        detail = what["path"]
    else:
        detail = json.dumps(tool_input)
    choices = [ev.choice(choice_id, label.format(tool=tool), kind) for choice_id, label, kind in CHOICES]
    return ev.permission(question_id, tool, f"Pi wants to use {tool}", detail, tool_input, choices)


def ask_question(question_id: str, what: dict) -> dict:
    options = [{"label": o, "description": ""} for o in what.get("options") or [] if isinstance(o, str)]
    return ev.questions(question_id, [ev.asked("answer", "", what.get("question") or "", options, what.get("multiSelect"), True)])


def text_parts(message: dict, kind: str) -> str:
    field = "text" if kind == "text" else "thinking"
    return "\n".join(part.get(field) or "" for part in message.get("content") or []
                     if isinstance(part, dict) and part.get("type") == kind).strip()


class Pi(adapters.Adapter):
    kind = "pi"
    program_name = PROGRAM
    permissions = PERMISSIONS
    option_names = ("effort", "appendSystemPrompt", "noContextFiles")

    def __init__(self, conversation, config, tools):
        super().__init__(conversation, config, tools)
        if self.credentials is None:
            raise Refused("Pi runs only on a model provider's API key here, never on a login of your machine's own Pi: "
                          "wire a PiAccess into `access` with the key of the provider to use", 400)
        if not config.get("model"):
            raise Refused("Pi needs a model for the connection's key: set `model` to one of that provider's model ids", 400)
        adapters.login_way(self.kind, self.credentials)
        self.provider = PROVIDERS[self.credentials["way"]]
        # weft's id for the Pi session: kept once a run has started in it.
        self.session = self.conv.conversation_id() or str(uuid.uuid4())
        self.resuming = self.conv.conversation_id() is not None
        # Checked before anything starts.
        self.command_line = command(config, self.permission, self.provider, self.session, self.home, tools)
        self.appended = appended_prompt(self.options)
        # The turn's last answer, and what it failed with.
        self.reply = ""
        self.error = None
        # The program's own tool calls and the ask tool's, which the stream
        # shows as a `tool_call` or a `question` instead.
        self.quiet = set()
        # Each running tool call's arguments: its end names only the call.
        self.args = {}
        # Pi's open dialogs, weft's question first: (request, what it asks).
        self.dialogs = []
        self.dialog_lock = threading.Lock()
        # Whether a run of Pi's is going: from its `agent_start` to its
        # `agent_settled`, both read off the one thread that reads Pi.
        self.running = False

    def command(self) -> list:
        return self.command_line

    def credential_env(self, env: dict) -> None:
        # The person's own Pi settings (`PI_CODING_AGENT_DIR`, its session
        # folder, ...) are never this conversation's.
        for name in [n for n in env if n.startswith("PI_")]:
            env.pop(name)
        super().credential_env(env)
        # No call to pi.dev for a newer version, no install telemetry.
        env["PI_SKIP_VERSION_CHECK"] = "1"
        env["PI_TELEMETRY"] = "0"

    def prepare(self) -> None:
        # A new process has no run and no dialog yet: the last one's went with it.
        with self.dialog_lock:
            self.running = False
            self.dialogs = []
        self.write_home_file(LOADER, js_plugin.loader(EXTENSION, self.tools))
        if self.provider == COMPATIBLE:
            self.write_home_file("models.json", json.dumps(models_config(self.login_value(HOST_VALUE), self.config["model"]), indent=1))
        elif os.path.exists(os.path.join(self.home, "models.json")):
            os.remove(os.path.join(self.home, "models.json"))
        if self.appended:
            self.write_home_file(APPENDED_PROMPT, self.appended + "\n")

    # ----- Pi's commands ---------------------------------------------------------

    def request(self, kind: str, **fields) -> dict:
        rid = f"weft-{self.next_request_id()}"
        return self.call(rid, {"id": rid, "type": kind, **fields}, kind, timeout=ANSWER_SECS)

    def answered(self, message: dict) -> None:
        box = self.waiting.pop(message.get("id"), None)
        if box is None:
            return
        if message.get("success"):
            box["result"] = message.get("data") or {}
        else:
            box["error"] = {"message": message.get("error") or "no reason given"}
        box["done"].set()

    def handshake(self) -> None:
        state = self.request("get_state")
        if self.resuming and not state.get("messageCount"):
            raise Refused(f"Pi no longer has this conversation (session {self.session}): it keeps one per folder, so the "
                          f"folder changed since, or the session under {self.home}/sessions was deleted; start the "
                          "conversation over from the node's card, or set `folder` back")

    def begin(self, turn) -> None:
        self.reply = ""
        self.error = None
        with self.dialog_lock:
            if self.running or self.dialogs:
                # A run and its dialogs end at its `agent_settled`, or with the
                # process, and a turn begins only once the one before it
                # ended: either still there is a run of the last turn that
                # outlived it, and answering one of its dialogs now would hand
                # this turn's answer to that run.
                raise RuntimeError(f"Pi's run of the last turn is still going as this one begins ({len(self.dialogs)} "
                                   f"dialog(s) of it open: {', '.join(request['id'] for request, _ in self.dialogs) or 'none'}); "
                                   "the agent server let that turn end without ending its run, which is a bug")
        threading.Thread(target=self._prompt, args=(turn,), daemon=True).start()

    def _prompt(self, turn) -> None:
        try:
            accepted = self.request("prompt", message=turn.message)
        except Refused as refused:
            self.conv.fail(turn, str(refused))
            return
        disposition = accepted.get("disposition")
        if disposition == "started":
            # The session exists from its first run on: a restart resumes it.
            self.conv.set_conversation(self.session)
        elif disposition == "handled":
            # One of Pi's own commands (`/mcp`, ...) took the message, and no run starts.
            self.conv.finish(turn, "")
        else:
            self.conv.fail(turn, f"Pi did not start a run for the message (disposition {disposition!r})")

    # ----- what Pi writes ---------------------------------------------------------

    def on_message(self, message: dict) -> None:
        kind = message.get("type")
        if kind == "response":
            self.answered(message)
            return
        if kind == "extension_ui_request":
            self._dialog(message)
            return
        if kind in ("agent_start", "agent_settled"):
            # Settled, Pi is idle and none of its dialogs is open any more: an
            # aborted run's dialogs Pi resolved itself, as unanswered, without
            # a word of it.
            with self.dialog_lock:
                self.running = kind == "agent_start"
                if not self.running:
                    self.dialogs = []
        turn = self.turn
        if turn is None or turn.status in ev.ENDED:
            return
        if kind == "message_update":
            delta = message.get("assistantMessageEvent") or {}
            if delta.get("type") == "text_delta" and self.config.get("streamBy") == "token":
                self.conv.emit(turn, ev.TEXT_DELTA, text=delta.get("delta", ""))
        elif kind == "message_end":
            self._message_end(turn, message.get("message") or {})
        elif kind == "tool_execution_start":
            self._tool_start(turn, message)
        elif kind == "tool_execution_end":
            self._tool_end(turn, message)
        elif kind == "agent_settled":
            self._settled(turn, message)

    def _message_end(self, turn, message: dict) -> None:
        if message.get("role") != "assistant":
            return
        thinking = text_parts(message, "thinking")
        if thinking:
            self.conv.emit(turn, ev.THINKING, text=thinking)
        text = text_parts(message, "text")
        if text:
            self.reply = text
            self.conv.emit(turn, ev.TEXT, text=text)
            self.remember_text(text)
        if message.get("stopReason") == "error":
            self.error = message.get("errorMessage") or "no reason given"
        elif message.get("stopReason") != "aborted":
            self.error = None

    def _tool_start(self, turn, message: dict) -> None:
        call_id, name, args = message.get("toolCallId"), message.get("toolName") or "", message.get("args") or {}
        self.args[call_id] = args
        # A name of the program's is the program's tool: the extension's tool
        # of that name took the built-in's place, and the route wrote it
        # into the history (`remember_tool_use` leaves it there).
        if self.program_tool(name) is not None or name == ASK_TOOL:
            self.quiet.add(call_id)
        else:
            self.conv.emit(turn, ev.TOOL_USE, id=call_id, name=name, input=args)
        self.remember_tool_use(call_id, name, args)

    def _tool_end(self, turn, message: dict) -> None:
        call_id, name = message.get("toolCallId"), message.get("toolName") or ""
        output = adapters.text_of((message.get("result") or {}).get("content"))
        failed = bool(message.get("isError"))
        args = self.args.pop(call_id, {})
        if call_id not in self.quiet:
            if name in ("edit", "write") and not failed:
                path = args.get("path") or ""
                # As Pi resolves it: `@` dropped, `~` the home, relative to the folder.
                path = os.path.normpath(os.path.join(self.config["folder"], os.path.expanduser(path[1:] if path.startswith("@") else path)))
                self.conv.emit(turn, ev.FILE_CHANGE, path=path, change="update" if name == "edit" else "write")
            self.conv.emit(turn, ev.TOOL_RESULT, id=call_id, output=output, isError=failed)
        self.remember_tool_result(call_id, output)

    def _settled(self, turn, message: dict) -> None:
        if message.get("aborted"):
            self.conv.fail(turn, "Pi aborted the turn")
        elif self.error is not None:
            self.conv.fail(turn, f"the model call failed: {self.error}")
        else:
            self.conv.finish(turn, self.reply)

    # ----- the dialogs: permissions and questions -------------------------------

    def _dialog(self, request: dict) -> None:
        if request.get("method") not in ("select", "input", "confirm", "editor"):
            # `notify`, `setStatus`, ...: nothing waits on them.
            return
        what = asked(request)
        if what is None:
            # Only weft's extension asks here; whatever else asks gets no answer it could act on.
            self._respond(request, cancelled=True)
            return
        with self.dialog_lock:
            self.dialogs.append((request, what))
            first = len(self.dialogs) == 1
        if first:
            self._ask(request, what)

    def _ask(self, request: dict, what: dict) -> None:
        turn = self.turn
        if turn is None or turn.status in ev.ENDED:
            self._answered(request, cancelled=True)
            return
        question_id = f"pi-{request['id']}"
        if what["weft"] == "permission":
            question = permission_question(question_id, what)

            def answer(given: dict) -> None:
                picked = ev.chosen(given, question)
                self._answered(request, value=json.dumps({"choice": picked["id"], "note": given.get("note") or ""}))
        else:
            question = ask_question(question_id, what)

            def answer(given: dict) -> None:
                self._answered(request, value="\n".join(ev.answers_for(given, question)["answer"]))
        # Refused (the turn ended meanwhile): the dialog is cancelled, as
        # for a turn found ended above.
        self.block(turn, ev.QUESTION, question_id, {"question": question}, answer,
                   lambda why: self._answered(request, cancelled=True))

    def _answered(self, request: dict, **answer) -> None:
        """Answer Pi's open dialog `request`, then put the next one waiting to weft."""
        # Off the list first: a Pi that is gone by now (`send` refuses) took
        # the dialog with it.
        with self.dialog_lock:
            self.dialogs = [d for d in self.dialogs if d[0]["id"] != request["id"]]
            following = self.dialogs[0] if self.dialogs else None
        self._respond(request, **answer)
        if following is not None:
            self._ask(*following)

    def _respond(self, request: dict, **answer) -> None:
        self.send({"type": "extension_ui_response", "id": request["id"], **answer})


adapters.register(Pi)
