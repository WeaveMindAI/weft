"""Kimi Code, driven through its SDK: `kimi-code-sdk kimi_bridge.mjs`.

Kimi Code's SDK is JavaScript and runs Kimi's whole engine in one process.
`kimi-code-sdk` is a build of it (`agents.json`, `kimi.install`) that loads
weft's bridge, `kimi_bridge.mjs`; the bridge drives one session and hands
everything to this adapter, which decides what each thing means. The
protocol is one JSON object per line each way:
SYNC: the protocol <-> catalog/agents/server/kimi_bridge.mjs (the header)

  this side -> bridge
    {"id", "type": "start", "workDir", "session", "resume", "model",
     "thinking", "permission", "plan", "additionalDirs", "tools"}
    {"id", "type": "tools"}       the main agent's tools, as its registry has them
    {"id", "type": "prompt", "text"}
                                  answered once Kimi took the message; the
                                  turn ends with the `turn.ended` event
    {"type": "answer", "ask", "response"}
  bridge -> this side
    {"id", "result"} / {"id", "error": {"message"}}
    {"type": "event", "event"}    an SDK event of the main agent, as is
    {"type": "ask", "ask", "kind": "approval" | "question" | "tool", "request"}

Kimi's SDK, not its ACP mode, because of the program's tools: ACP hands them
to Kimi as an MCP server, whose tools the model sees as
`mcp__weft__<tool>`. Through the SDK each one is a Kimi "user tool", which
keeps its plain name, and one named like a built-in (`Read`, `Agent`,
`Grep`, ...) takes the built-in's place: Kimi's registry holds one tool per
name, the last one registered (2.5.1, checked against its engine: the model
was offered the program's `Read` alone). A call of one reaches this side
through the bridge's `tool` ask, the program's route, carrying the call id
Kimi's own `tool.call.started` gave it: that pairs the two exactly, and a
call is the program's when its ask came, and the history gets it from that
route (`Conversation.block`). One name cannot be the program's:
`ReadMediaFile`, which Kimi registers again whenever the session's
environment changes, so weft refuses a program tool of that name when the
program is built and when AskAgent fires (the node's `reservedToolNames`).

Kimi asks for approval before most tools under `ask`, the program's too
(`Approve lookup`): the program handed those over, so this side approves
them without asking anyone. The approval comes before the call's
`tool.call.started`.

The conversation is a Kimi session under an id this side mints, created
with it and resumed by it in a new process. Kimi keeps the program's tools
with the session, and the bridge sets them to the program's list again on
every start (a built-in one of them had replaced comes back once the
program drops it). A resumed session comes back in the mode it was left
in, so every setting is set again.

With a connection, Kimi's home (`KIMI_CODE_HOME`) is a private one, whose
`config.toml` declares one Moonshot provider reading the key from
`KIMI_API_KEY` and the node's model as its only alias. On the machine's own
login it is the person's own `~/.kimi-code` (their `kimi login`, their
`config.toml`).
"""

import json
import os
import threading
import uuid

import adapters
import events as ev
from events import Refused

PROGRAM = "kimi-code-sdk"
BRIDGE = os.path.join(os.path.dirname(os.path.abspath(__file__)), "kimi_bridge.mjs")

# weft's permission setting -> Kimi's permission mode and plan mode, as Kimi's
# own ACP modes map to them (`default`, `plan`, `yolo`, `auto`): `yolo` still
# asks before a dangerous command or a sensitive file, and `auto` never asks
# and turns its question tool off.
# SYNC: Kimi's permission settings <-> catalog/agents/server/agents.json (kimi.permissions), catalog/agents/kimi_code/metadata.json (permission)
PERMISSIONS = {
    "ask": ("manual", False),
    "plan": ("manual", True),
    "auto": ("yolo", False),
    "bypass": ("auto", False),
}

THINKING = ("on", "off")

# The context window Kimi gives a model defined by its variables alone.
CONTEXT_SIZE = 262144

# A permission's choices, as Kimi's own ACP server offers them (the answer
# each one makes is `approval_answer`'s).
CHOICES = (("approve_once", "Approve once", ev.ALLOW_ONCE),
           ("approve_always", "Approve for this session", ev.ALLOW_ALWAYS),
           ("reject", "Reject", ev.REJECT))
# A plan to review: its own options to approve with, else one approval; then
# the two ways to turn it down, by the labels Kimi reads.
PLAN_APPROVE = ("plan_approve", "Approve", ev.ALLOW_ONCE)
PLAN_REVISE = ("plan_revise", "Revise", ev.REJECT)
PLAN_REJECT = ("plan_reject_and_exit", "Reject and Exit", ev.REJECT)
PLAN_OPTION = "plan_opt_"

# What a file tool's display says it did, as a `file_change`.
FILE_CHANGES = {"write": "write", "edit": "update"}


def kimi_config(model: str) -> str:
    """A Moonshot key's `config.toml`: one provider reading the key from
    `KIMI_API_KEY`, and `model` as the default alias."""
    alias = json.dumps(model)
    key = adapters.AGENTS["kimi"]["logins"]["apiKey"]["env"]["key"]
    return "\n".join([
        f"default_model = {alias}",
        "",
        "[providers.moonshot]",
        'type = "kimi"',
        f"api_key_env = {json.dumps(key)}",
        "",
        f"[models.{alias}]",
        'provider = "moonshot"',
        f"model = {alias}",
        f"max_context_size = {CONTEXT_SIZE}",
        "",
    ])


def describe(display: dict) -> str:
    """What a tool's display shows, as the detail of its permission question."""
    kind = display.get("kind")
    if kind == "command":
        return display.get("command") or ""
    if kind in ("file_io", "diff"):
        return display.get("path") or ""
    if kind == "url_fetch":
        return display.get("url") or ""
    if kind == "search":
        return display.get("query") or ""
    if kind == "plan_review":
        return display.get("plan") or ""
    if kind == "agent_call":
        return display.get("prompt") or ""
    if kind == "generic":
        detail = display.get("detail")
        return display.get("summary", "") + ("" if detail is None else "\n" + json.dumps(detail))
    return json.dumps(display)


def approval_question(question_id: str, request: dict) -> dict:
    display = request.get("display") or {}
    if display.get("kind") == "plan_review":
        offered = display.get("options") or []
        approvals = ([(f"{PLAN_OPTION}{i}", o.get("label") or f"Option {i + 1}", ev.ALLOW_ONCE) for i, o in enumerate(offered)]
                     if len(offered) >= 2 else [PLAN_APPROVE])
        choices = [*approvals, PLAN_REVISE, PLAN_REJECT]
    else:
        choices = CHOICES
    return ev.permission(question_id, request.get("toolName") or "tool", request.get("action") or "Kimi Code wants to use a tool",
                         describe(display), display, [ev.choice(*c) for c in choices])


def approval_answer(question: dict, answer: dict, display: dict) -> dict:
    """weft's answer to an approval, as Kimi's ApprovalResponse."""
    picked = ev.chosen(answer, question)
    note = answer.get("note") or ""
    response = {}
    if picked["id"] == "approve_always":
        response = {"decision": "approved", "scope": "session"}
    elif picked["id"].startswith(PLAN_OPTION):
        index = int(picked["id"][len(PLAN_OPTION):])
        response = {"decision": "approved", "selectedLabel": display["options"][index]["label"]}
    elif picked["kind"] == ev.REJECT:
        response = {"decision": "rejected"}
        if picked["id"] != "reject":
            response["selectedLabel"] = picked["label"]
    else:
        response = {"decision": "approved"}
    if note:
        response["feedback"] = note
    return response


def ask_question(question_id: str, request: dict) -> dict:
    """Kimi's AskUserQuestion as weft's question: one per item, each taking
    an answer of the person's own besides its options, as Kimi's own
    question dialog does."""
    asked = []
    for index, item in enumerate(request.get("questions") or []):
        text = item.get("question") or ""
        if item.get("body"):
            text += "\n\n" + item["body"]
        options = [{"label": o.get("label", ""), "description": o.get("description") or ""} for o in item.get("options") or []]
        asked.append(ev.asked(f"q{index}", item.get("header") or "", text, options, item.get("multiSelect"), True))
    return ev.questions(question_id, asked)


def question_answer(question: dict, answer: dict, request: dict) -> dict:
    """weft's answers as Kimi's: each keyed by its question's text, several
    picks joined by a comma, as Kimi's own dialog writes them."""
    given = ev.answers_for(answer, question)
    return {"answers": {item["question"]: ", ".join(given[f"q{index}"])
                        for index, item in enumerate(request.get("questions") or [])}}


class Kimi(adapters.Adapter):
    kind = "kimi"
    program_name = PROGRAM
    permissions = PERMISSIONS
    option_names = ("thinking", "addDirs")

    def __init__(self, conversation, config, tools):
        super().__init__(conversation, config, tools)
        self.model = config.get("model") or ""
        if self.credentials is not None and not self.model:
            raise Refused(f"{self.label} with a Moonshot API key needs `model`: the Moonshot model id to run, "
                          "such as kimi-k2-turbo-preview (https://platform.moonshot.ai/docs)", 400)
        self.thinking = self.options.get("thinking") or ""
        if self.thinking and self.thinking not in THINKING:
            raise Refused(f"{self.label}'s `thinking` takes {', '.join(THINKING)}, and got {self.thinking!r}", 400)
        dirs = self.options.get("addDirs") or []
        if isinstance(dirs, str):
            dirs = [d.strip() for d in dirs.split(",") if d.strip()]
        if not isinstance(dirs, list) or not all(isinstance(d, str) for d in dirs):
            raise Refused(f"{self.label}'s `addDirs` is a list of folders, and got {dirs!r}", 400)
        self.dirs = dirs
        self.reset_turn()

    def reset_turn(self) -> None:
        # The message and the reasoning being written, piece by piece.
        self.writing, self.thinking_now = [], []
        self.reply = []
        # Kimi's turn id of the turn running now, from its `turn.started`.
        self.kimi_turn = None
        # Each shown call's (name, arguments, display), by Kimi's call id.
        self.calls = {}
        # The calls whose ask came: the program's, by call id.
        self.routed = set()

    def command(self) -> list:
        return [self.program(), BRIDGE]

    def prepare(self) -> None:
        if self.credentials is not None:
            self.write_home_file("config.toml", kimi_config(self.model), private=True)

    def handshake(self) -> None:
        conversation = self.conv.conversation_id()
        mode, plan = PERMISSIONS[self.permission]
        rid = self.next_request_id()
        try:
            started = self.call(rid, {"id": rid, "type": "start", "workDir": self.config["folder"],
                                      "session": conversation or str(uuid.uuid4()), "resume": conversation is not None,
                                      "model": self.model, "thinking": self.thinking, "permission": mode, "plan": plan,
                                      "additionalDirs": self.dirs, "tools": self.tools}, "start")
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
            # No deadline: Kimi answers once it took the message.
            self.call(rid, {"id": rid, "type": "prompt", "text": turn.message}, "prompt", timeout=None)
        except Refused as refused:
            self.conv.fail(turn, str(refused))

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
        kind = event.get("type")
        if kind == "turn.started":
            if (event.get("origin") or {}).get("kind") == "user" and self.kimi_turn is None:
                self.kimi_turn = event.get("turnId")
            return
        if event.get("turnId") is not None and event.get("turnId") != self.kimi_turn:
            # A turn Kimi started on its own (a cron, a goal): nobody's here.
            return
        if kind == "assistant.delta":
            self._flush_thinking(turn)
            self.writing.append(event.get("delta") or "")
            if self.config.get("streamBy") == "token":
                self.conv.emit(turn, ev.TEXT_DELTA, text=event.get("delta") or "")
        elif kind == "thinking.delta":
            self._flush_text(turn)
            self.thinking_now.append(event.get("delta") or "")
        elif kind == "tool.call.started":
            self._flush(turn)
            self._started(turn, event)
        elif kind == "tool.result":
            self._flush(turn)
            self._result(turn, event)
        elif kind == "turn.ended":
            self._flush(turn)
            self._ended(turn, event)

    def _started(self, turn, event: dict) -> None:
        call_id, name, args = event.get("toolCallId"), event.get("name") or "", event.get("args") or {}
        self.calls[call_id] = (name, args, event.get("display") or {})
        if call_id in self.routed or self.program_tool(name) is not None:
            # A call of the program's tool (Kimi's registry holds one tool
            # per name): its ask shows it, as weft's `tool_call`.
            return
        self.conv.emit(turn, ev.TOOL_USE, id=call_id, name=name, input=args)
        self.remember_tool_use(call_id, name, args)

    def _result(self, turn, event: dict) -> None:
        call_id = event.get("toolCallId")
        output = adapters.text_of(event.get("output"))
        failed = bool(event.get("isError"))
        name, args, display = self.calls.pop(call_id, ("", {}, {}))
        if call_id in self.routed:
            # The route wrote its result.
            self.routed.discard(call_id)
            return
        if self.program_tool(name) is not None:
            # Ended before it reached the program (refused by Kimi itself):
            # shown as the call it was, which never ran.
            self.conv.emit(turn, ev.TOOL_USE, id=call_id, name=name, input=args)
            self.remember_tool_use(call_id, name, args)
        self.conv.emit(turn, ev.TOOL_RESULT, id=call_id, output=output, isError=failed)
        self.remember_tool_result(call_id, output)
        change = FILE_CHANGES.get(display.get("operation")) if display.get("kind") == "file_io" else "update" if display.get("kind") == "diff" else None
        if change is not None and not failed:
            self.conv.emit(turn, ev.FILE_CHANGE, path=display.get("path"), change=change)

    def _ended(self, turn, event: dict) -> None:
        reason = event.get("reason")
        if reason == "completed":
            self.conv.finish(turn, "\n\n".join(self.reply))
            return
        error = (event.get("error") or {}).get("message")
        said = "\n\n".join(self.reply).strip()
        why = error or f"{self.label} ended the turn {reason}" + (f" ({event['interruptReason']})" if event.get("interruptReason") else "")
        self.conv.fail(turn, f"{said} ({why})" if said else why)

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
        if kind == "approval" and self.program_tool(request.get("toolName")) is not None:
            # The program handed its tools over: calling one is never a question.
            self._answer(ask, {"decision": "approved"})
            return
        if turn is None or turn.status in ev.ENDED:
            # Nobody is here to answer: Kimi hears so at once.
            gone = "the turn ended first, so weft will never answer this"
            self._answer(ask, {"approval": {"decision": "cancelled", "feedback": gone}, "question": None,
                               "tool": {"output": gone, "isError": True}}[kind])
            return
        self._flush(turn)
        question_id = f"kimi-{ask}"
        if kind == "tool":
            self._tool(turn, ask, request)
        elif kind == "approval":
            question = approval_question(question_id, request)
            self.block(turn, ev.QUESTION, question_id, {"question": question},
                       lambda answer: self._answer(ask, approval_answer(question, answer, request.get("display") or {})),
                       lambda why: self._answer(ask, {"decision": "cancelled", "feedback": why}))
        elif kind == "question":
            question = ask_question(question_id, request)
            self.block(turn, ev.QUESTION, question_id, {"question": question},
                       lambda answer: self._answer(ask, question_answer(question, answer, request)),
                       lambda why: self._answer(ask, None))
        else:
            raise ValueError(f"the bridge asked {kind!r}, which this adapter does not know")

    def _tool(self, turn, ask: str, request: dict) -> None:
        """A call of the program's tool, through its route: weft's `tool_call`,
        which the history gets under its plain name (`Conversation.block`),
        a sub-agent's call too (its events are never shown)."""
        call_id, name, args = request.get("toolCallId"), request.get("name") or "", request.get("args") or {}
        if name not in self.program_tools:
            raise ValueError(f"Kimi called the program's tool {name!r}, which the program does not have")
        self.routed.add(call_id)
        self.block(turn, ev.TOOL_CALL, call_id, {"callId": call_id, "name": name, "arguments": json.dumps(args)},
                   lambda result: self._answer(ask, {"output": str(result.get("output", "")), "isError": bool(result.get("isError"))}),
                   lambda why: self._answer(ask, {"output": why, "isError": True}))

    def _answer(self, ask: str, response) -> None:
        self.send({"type": "answer", "ask": ask, "response": response})


adapters.register(Kimi)
