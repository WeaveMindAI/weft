"""Cline CLI (Cline Bot Inc.), driven through its SDK: `node cline_bridge.mjs <the SDK>`.

The `cline` npm package carries Cline's SDK (`@cline/sdk`, its engine in
JavaScript) beside the program. weft runs the SDK under Node with its
bridge, `cline_bridge.mjs`, which drives one session and hands everything
to this adapter. The protocol is one JSON object per line each way:
SYNC: the protocol <-> catalog/agents/server/cline_bridge.mjs (the header)

  this side -> bridge
    {"id", "type": "start", "cwd", "session", "resume", "model", "mode",
     "autoApprove", "effort", "tools"}
                                  answered {"session", "provider", "model"}
    {"id", "type": "prompt", "text"}
                                  answered {"finishReason", "text"} once the
                                  turn is over
    {"type": "answer", "ask", "response"}
  bridge -> this side
    {"id", "result"} / {"id", "error": {"message"}}
    {"type": "event", "event"}    an AgentEvent of the lead agent, as is
    {"type": "ask", "ask", "kind": "approval" | "question" | "tool", "request"}

Cline's SDK, not `cline --acp`, because it does more. Over ACP the
program's tools were a plugin of weft's, whose tools lose to a built-in of
the same name, and Cline had no question tool. Through the SDK each program
tool is one of the session's extra tools: it keeps its plain name, and one
named like a built-in (`read_files`, `run_commands`, `spawn_agent`, ...)
takes its place, since the engine keeps one tool per name and sets the
extra tools last (3.0.74: the model was offered the program's `read_files`
alone). A Cline plugin's tool of that name loses to it too (plugin tools are
set first); a call of the name that runs anywhere but the program's route
fails the turn, saying what to rename. So no name is refused when the program is built (the node's
`reservedToolNames` is empty). The lead agent has only the program's tool
of such a name, so a call of a program tool's name in its events is the
program's; the call itself reaches this side through the bridge's `tool`
ask, the program's route, under the call id the events carry, and the
history gets it from that route (`Conversation.block`). Its `ask_question`
reaches weft as a question.

Every other tool asks first (`approval`), as Cline's ACP mode did; the
program's tools and `ask_question` never do (the bridge's tool policies).
`allow_always` approves that tool for the rest of this agent process. A
refusal fails that one tool and the turn goes on. `plan` runs Cline in its
plan mode (it reads and plans, and its command guard refuses edits);
`bypass` approves every call.

The conversation is a Cline session under an id this side mints, kept in
Cline's data folder; a new process reads its messages back and starts the
session again with them, under the same id.

With a connection, Cline's config folder (`CLINE_DIR`) is a private home,
and the key is `CLINE_API_KEY` whichever provider `CLINE_PROVIDER` names
(Cline's own when unset). A provider that runs on another vendor's login on
this machine (Claude Code, the Codex CLI, OpenCode) or a sign-in (a ChatGPT
subscription) is refused for a key login, so no connection adopts the
person's other logins. On the machine's own login everything is the
person's own Cline: its folders, its login (`cline auth`), the provider it
last used, its plugins, rules and hooks.
"""

import json
import os
import pathlib
import re
import shutil
import threading
import uuid

import adapters
import events as ev
from events import Refused

PROGRAM = "cline"
BRIDGE = os.path.join(os.path.dirname(os.path.abspath(__file__)), "cline_bridge.mjs")
# The SDK's entry, from the `cline` package's folder (the npm wrapper `cline`
# is that folder's `bin/cline`).
SDK = os.path.join("node_modules", "@cline", "sdk", "dist", "index.js")

# weft's permission setting -> Cline's mode, and whether every call is approved.
# SYNC: Cline's permission settings <-> catalog/agents/server/agents.json (cline.permissions), catalog/agents/cline_cli/metadata.json (permission)
PERMISSIONS = {
    "ask": ("act", False),
    "plan": ("plan", False),
    "bypass": ("act", True),
}

# SYNC: the efforts <-> catalog/agents/cline_cli/metadata.json (effort)
EFFORTS = ("none", "low", "medium", "high", "xhigh")

# Providers a key cannot log in to: Cline's own (the `clineKey` login), and
# those that run on a login of this machine (another vendor's CLI) or a
# browser sign-in.
NOT_A_KEY = ("cline", "cline-pass", "openai-codex", "openai-codex-cli", "claude-code", "opencode")

# What moves Cline's folders out of its config folder: taken out in a
# private home, so the connection's data never lands in the person's.
MOVERS = ("CLINE_DATA_DIR", "CLINE_GLOBAL_SETTINGS_PATH", "CLINE_HOOKS_LOG_PATH")

# A permission's choices.
CHOICES = (("allow_once", "Allow once", ev.ALLOW_ONCE),
           ("allow_always", "Allow for the rest of this session", ev.ALLOW_ALWAYS),
           ("reject", "Reject", ev.REJECT))
# The reason Cline's own clients give when the person refuses a call
# (`USER_REJECTED_TOOL_REASON`); the engine adds its own advice to it.
REJECTED = "This tool call was rejected by the user and not executed."

# What a patch's header says it does to a file, as a `file_change`.
PATCH_FILE = re.compile(r"^\*\*\* (Add|Update|Delete) File: (.+)$", re.MULTILINE)


def sdk_entry(program: str) -> str:
    """The SDK beside the `cline` program `program` (a name on PATH, or the
    node's own path): the npm wrapper sits in its package's `bin`."""
    found = shutil.which(program)
    package = os.path.dirname(os.path.dirname(os.path.realpath(found)))
    entry = os.path.join(package, SDK)
    if not os.path.exists(entry):
        raise Refused(f"{found} is not Cline's npm package (no {SDK} beside it, in {package}): weft runs Cline through the SDK "
                      "that package carries. weft installs that package itself only when no `cline` is on PATH, so take this one off PATH "
                      "and start the node's infra again, or point the node's `program` at a `cline` of that package")
    return entry


def describe(tool_input) -> str:
    """What a call does, as the detail of its permission question."""
    if isinstance(tool_input, dict):
        if isinstance(tool_input.get("commands"), list):
            return "\n".join(c if isinstance(c, str) else json.dumps(c) for c in tool_input["commands"])
        if isinstance(tool_input.get("path"), str):
            return tool_input["path"]
    return json.dumps(tool_input)


def file_changes(name: str, tool_input) -> list:
    """The files a call of Cline's editing tools changed: (path, change)."""
    if not isinstance(tool_input, dict):
        return []
    if name == "editor" and isinstance(tool_input.get("path"), str):
        # No text to replace and no line to insert at: it writes the file whole.
        new = tool_input.get("old_text") is None and tool_input.get("insert_line") is None
        return [(tool_input["path"], "add" if new else "update")]
    if name == "apply_patch" and isinstance(tool_input.get("input"), str):
        return [(path.strip(), verb.lower()) for verb, path in PATCH_FILE.findall(tool_input["input"])]
    return []


def failed_text(event: dict) -> str:
    """Why a call failed: its output is `{"error": why}`, and its `error` that
    same object as JSON."""
    output = event.get("output")
    return output["error"] if isinstance(output, dict) and isinstance(output.get("error"), str) else event["error"]


def output_text(output) -> str:
    """A tool's result as text: Cline gives some as lists of records."""
    if output is None or isinstance(output, str):
        return output or ""
    return json.dumps(output)


class Cline(adapters.Adapter):
    kind = "cline"
    program_name = PROGRAM
    permissions = PERMISSIONS
    option_names = ("effort",)

    def __init__(self, conversation, config, tools):
        super().__init__(conversation, config, tools)
        self.effort = self.options.get("effort") or ""
        if self.effort and self.effort not in EFFORTS:
            raise Refused(f"{self.label}'s `effort` takes {', '.join(EFFORTS)}, and got {self.effort!r}", 400)
        if self.credentials is not None and self.credentials.get("way") == "providerKey":
            provider = self.login_value("provider")
            if provider in NOT_A_KEY:
                raise Refused(f"Cline's provider {provider!r} takes no API key: use the `clineKey` login for Cline's own "
                              "provider, and this machine's own login for a sign-in", 400)
        # The tools the person allowed for the rest of this process.
        self.always = set()
        self.reset_turn()

    def reset_turn(self) -> None:
        # The message and the reasoning being written, piece by piece.
        self.writing, self.thinking_now = [], []
        self.reply = []
        # Each shown call's (name, input), by Cline's call id.
        self.calls = {}
        # The calls whose ask came: the program's, by call id.
        self.routed = set()
        # The error Cline ended the turn with, from its `error` event.
        self.fatal = None

    def command(self) -> list:
        node = shutil.which("node")
        if node is None:
            raise Refused(f"`node` is not installed where the agent server runs (looked on PATH: {os.environ.get('PATH', '')}): "
                          f"weft runs {self.label} through its SDK, on Node 22 or newer")
        return [node, BRIDGE, pathlib.Path(sdk_entry(self.program())).as_uri()]

    def credential_env(self, env: dict) -> None:
        super().credential_env(env)
        if self.home is not None:
            for name in MOVERS:
                env.pop(name, None)

    def handshake(self) -> None:
        conversation = self.conv.conversation_id()
        mode, approve_all = PERMISSIONS[self.permission]
        rid = self.next_request_id()
        try:
            started = self.call(rid, {"id": rid, "type": "start", "cwd": self.config["folder"],
                                      "session": conversation or str(uuid.uuid4()), "resume": conversation is not None,
                                      "model": self.config.get("model") or "", "mode": mode, "autoApprove": approve_all,
                                      "effort": self.effort, "tools": self.tools}, "start")
        except adapters.RpcRefused as refused:
            if conversation is None:
                raise
            raise Refused(f"{refused}; the conversation cannot be picked up again, so start it over from the node's card") from None
        if conversation is None:
            self.conv.set_conversation(started["session"])

    def begin(self, turn) -> None:
        self.reset_turn()
        threading.Thread(target=self._prompt, args=(turn,), daemon=True).start()

    def _prompt(self, turn) -> None:
        rid = self.next_request_id()
        try:
            # No deadline: the turn takes as long as it takes.
            result = self.call(rid, {"id": rid, "type": "prompt", "text": turn.message}, "prompt", timeout=None)
        except Refused as refused:
            self._flush(turn)
            self.conv.fail(turn, str(refused))
            return
        self._flush(turn)
        reason = result.get("finishReason")
        said = "\n\n".join(self.reply).strip()
        if self.fatal is None and reason == "completed":
            self.conv.finish(turn, "\n\n".join(self.reply))
            return
        why = self.fatal or (result.get("text") if reason == "error" else None) or f"{self.label} stopped the turn: {reason}"
        self.conv.fail(turn, f"{said} ({why})" if said else why)

    # ----- what the bridge writes -------------------------------------------

    def on_message(self, message: dict) -> None:
        kind = message.get("type")
        if kind is None:
            self.answered(message)
            return
        turn = self.turn
        if kind == "ask":
            self._ask(turn, message)
        elif kind == "event":
            if turn is not None and turn.status not in ev.ENDED:
                self._event(turn, message["event"])
        else:
            raise ValueError(f"the bridge wrote a line of type {kind!r}, which this adapter does not know")

    def _event(self, turn, event: dict) -> None:
        kind, content = event.get("type"), event.get("contentType")
        if kind == "error":
            if not event.get("recoverable"):
                self.fatal = (event.get("error") or {}).get("message") or f"{self.label} failed the turn"
            return
        if kind == "content_start" and content == "text":
            self._flush_thinking(turn)
            self.writing.append(event.get("text") or "")
            if self.config.get("streamBy") == "token" and event.get("text"):
                self.conv.emit(turn, ev.TEXT_DELTA, text=event["text"])
        elif kind == "content_start" and content == "reasoning":
            self._flush_text(turn)
            self.thinking_now.append(event.get("reasoning") or "")
        elif kind == "content_end" and content == "text":
            self._flush_text(turn)
        elif kind == "content_end" and content == "reasoning":
            self._flush_thinking(turn)
        elif kind == "content_start" and content == "tool":
            self._flush(turn)
            self._started(turn, event)
        elif kind == "content_end" and content == "tool":
            self._flush(turn)
            self._ended(turn, event)

    def _started(self, turn, event: dict) -> None:
        call_id, name, tool_input = event.get("toolCallId"), event.get("toolName") or "", event.get("input") or {}
        self.calls[call_id] = (name, tool_input)
        if call_id in self.routed or self.program_tool(name) is not None:
            # A call of the program's tool (the lead agent holds one tool per
            # name, the program's): its ask shows it, as weft's `tool_call`.
            return
        self.conv.emit(turn, ev.TOOL_USE, id=call_id, name=name, input=tool_input)
        self.remember_tool_use(call_id, name, tool_input)

    def _ended(self, turn, event: dict) -> None:
        call_id = event.get("toolCallId")
        failed = bool(event.get("error"))
        output = failed_text(event) if failed else output_text(event.get("output"))
        name, tool_input = self.calls.pop(call_id, (event.get("toolName") or "", {}))
        if call_id in self.routed:
            # The route wrote its result.
            self.routed.discard(call_id)
            return
        if self.program_tool(name) is not None and not failed:
            # A call of the program's tool's name that ran, and never through
            # the program's route: something else of Cline's holds the name.
            # Cline 3.0.74 sets the program's tools over its built-ins and
            # over a plugin's tool of the same name (checked against its
            # engine), so this is a Cline that changed that: the turn fails
            # rather than let the program's tool be silently replaced.
            self.conv.fail(turn, f"{self.label} ran a tool of its own named `{name}` instead of the program's `{name}`: something "
                                 "of Cline's (a Cline plugin of yours, or a tool of a newer Cline) holds that name over the program's. "
                                 f"Rename the program's tool, or remove or rename the Cline plugin that defines `{name}`")
            return
        if self.program_tool(name) is not None:
            # Ended before it reached the program (input Cline refused):
            # shown as the call it was, which never ran.
            self.conv.emit(turn, ev.TOOL_USE, id=call_id, name=name, input=tool_input)
            self.remember_tool_use(call_id, name, tool_input)
        self.conv.emit(turn, ev.TOOL_RESULT, id=call_id, output=output, isError=failed)
        self.remember_tool_result(call_id, output)
        if not failed:
            for path, change in file_changes(name, tool_input):
                # Cline takes a path relative to the folder it works in.
                self.conv.emit(turn, ev.FILE_CHANGE, path=os.path.join(self.config["folder"], path), change=change)

    def _flush_text(self, turn) -> None:
        if self.writing:
            text = "".join(self.writing)
            self.writing = []
            if text.strip():
                self.reply.append(text)
                self.conv.emit(turn, ev.TEXT, text=text)
                self.remember_text(text)

    def _flush_thinking(self, turn) -> None:
        if self.thinking_now:
            text = "".join(self.thinking_now)
            self.thinking_now = []
            if text.strip():
                self.conv.emit(turn, ev.THINKING, text=text)

    def _flush(self, turn) -> None:
        self._flush_thinking(turn)
        self._flush_text(turn)

    # ----- what the bridge asks ---------------------------------------------------

    def _ask(self, turn, message: dict) -> None:
        ask, kind, request = message["ask"], message.get("kind"), message.get("request") or {}
        if kind == "approval" and request.get("toolName") in self.always:
            self._answer(ask, {"approved": True})
            return
        if turn is None or turn.status in ev.ENDED:
            # Nobody is here to answer: Cline hears so at once.
            gone = "the turn ended first, so weft will never answer this"
            self._answer(ask, {"approval": {"approved": False, "reason": gone}, "question": {"refused": gone},
                               "tool": {"output": gone, "isError": True}}[kind])
            return
        self._flush(turn)
        question_id = f"cline-{ask}"
        if kind == "tool":
            self._tool(turn, ask, request)
        elif kind == "approval":
            name = request.get("toolName") or "tool"
            question = ev.permission(question_id, name, f"{self.label} wants to use {name}", describe(request.get("input")),
                                     request.get("input"), [ev.choice(*c) for c in CHOICES])
            self.block(turn, ev.QUESTION, question_id, {"question": question},
                       lambda answer: self._answer(ask, self._approval(question, answer, name)),
                       lambda why: self._answer(ask, {"approved": False, "reason": why}))
        elif kind == "question":
            options = [{"label": str(o), "description": ""} for o in request.get("options") or []]
            # Cline's own clients take an answer of the person's own besides the options.
            question = ev.questions(question_id, [ev.asked("q0", "", request.get("question") or "", options, False, True)])
            self.block(turn, ev.QUESTION, question_id, {"question": question},
                       lambda answer: self._answer(ask, {"answer": ", ".join(ev.answers_for(answer, question)["q0"])}),
                       lambda why: self._answer(ask, {"refused": why}))
        else:
            raise ValueError(f"the bridge asked {kind!r}, which this adapter does not know")

    def _approval(self, question: dict, answer: dict, name: str) -> dict:
        picked = ev.chosen(answer, question)
        if picked["kind"] == ev.REJECT:
            note = answer.get("note") or ""
            return {"approved": False, "reason": f"{REJECTED} {note}".strip()}
        if picked["kind"] == ev.ALLOW_ALWAYS:
            self.always.add(name)
        return {"approved": True}

    def _tool(self, turn, ask: str, request: dict) -> None:
        """A call of the program's tool, through its route: weft's `tool_call`,
        which the history gets under its plain name (`Conversation.block`)."""
        call_id, name, tool_input = request.get("toolCallId"), request.get("name") or "", request.get("input") or {}
        if name not in self.program_tools:
            raise ValueError(f"Cline called the program's tool {name!r}, which the program does not have")
        self.routed.add(call_id)
        self.block(turn, ev.TOOL_CALL, call_id, {"callId": call_id, "name": name, "arguments": json.dumps(tool_input)},
                   lambda result: self._answer(ask, {"output": str(result.get("output", "")), "isError": bool(result.get("isError"))}),
                   lambda why: self._answer(ask, {"output": why, "isError": True}))

    def _answer(self, ask: str, response) -> None:
        self.send({"type": "answer", "ask": ask, "response": response})


adapters.register(Cline)
