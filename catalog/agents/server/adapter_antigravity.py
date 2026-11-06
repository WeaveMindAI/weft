"""Google Antigravity, driven through its SDK: `antigravity-sdk antigravity_bridge.py`.

Antigravity's SDK (`google-antigravity`, Python, with pydantic and protobuf)
runs Antigravity's own harness (`localharness`, in the SDK's package) and
speaks to it. It lives in an environment of its own that the install makes
(`agents.json`, `antigravity.install`), where `antigravity-sdk` runs weft's
bridge, `antigravity_bridge.py`, on that environment's Python: the bridge
drives one conversation and hands everything to this adapter, which decides
what each thing means. The protocol is one JSON object per line each way:
SYNC: the protocol <-> catalog/agents/server/antigravity_bridge.py (the header)

  this side -> bridge
    {"id", "type": "start", "workDir", "dirs", "conversation", "resume",
     "saveDir", "model", "effort", "systemPrompt", "key", "tools", "enabled"}
    {"id", "type": "prompt", "text"}   answered once the message went; the
                                       turn ends with the `ended` line
    {"type": "answer", "ask", "response"}
  bridge -> this side
    {"id", "result"} / {"id", "error": {"message"}}
    {"type": "step", "step"}           a step of the turn, as the SDK gives it
    {"type": "tool_done", "result"}    a tool's result ({id, name, result, error})
    {"type": "ask", "ask", "kind": "pre_tool" | "question" | "tool", "request"}
    {"type": "ended", "error"}

The SDK, not Antigravity's ACP server (which is built on it), because of the
program's tools: over ACP they are an MCP server's, which the model reaches
behind one generic `call_mcp_tool`, or under a prefix. Through the SDK each
one is a tool of the session under its own name, and one named like a
built-in takes the built-in's place (checked against the harness of
0.1.23: the model is offered the program's `view_file` alone). Each
built-in has two names: the one the model sees (`write_to_file`) and the
SDK's (`create_file`), which is also the name its calls carry through the
SDK's hooks. A program tool named either way turns that built-in off
(`enabled`), so a name in the program's list is never also a built-in's,
wherever it is read. The skill tools cannot be turned off, and the harness
refuses the whole turn when a program tool takes one of their names: the
node refuses those names (its `lists.reservedToolNames`) when the program
is built and when AskAgent fires, so they never reach this adapter.

A call of the program's tools reaches this side as the bridge's `tool` ask,
the program's route, carrying the id the model gave the call: a call is the
program's when that ask came, and the history gets it from that route
(`Conversation.block`). Every tool call is first put to this side as a
`pre_tool` ask (the SDK's pre-tool hook, the same id), which this adapter
answers with weft's permission setting: the program's tools go ahead
without a question (the program handed them over), and the agent's own
ones go ahead, or come out as a permission question, by setting. The SDK's
post-tool hook (`tool_done`, the same id) says what a tool gave back.

Its questions are its `ask_question` tool, asked through the SDK's
interaction hook: several questions at once, each with its options, and an
answer of the person's own taken besides them. Sub-agents run inside the
harness: their permissions and questions are answered like the main
agent's, and only the main agent's messages and tools make the reply, the
stream and the history (a step, or a call's step id, of the conversation's
own trajectory is the main agent's).

The conversation is one under an id this side mints, created with it and
resumed by it in a new process from the folder the harness keeps it in
(`saveDir`, in the agent server's own folder, so it outlives the process).

Its login: a connection's key (a Gemini API key, or a Vertex AI key in
Vertex AI's express mode) goes to the bridge in the environment, and the
bridge hands it to the SDK by the variable's name; the harness then runs
with `HOME` a private home (agents.json's `home`), so nothing of the
person's own Antigravity data is read. On the machine's own login the SDK
finds the key the way it always does, in the person's own environment
(`GEMINI_API_KEY`, or Vertex AI's variables with Google Cloud's application
default login), which is why nothing is taken out of it then.
"""

import json
import os
import uuid

import adapters
import events as ev
from adapter_acp import Lever, Setup
from events import Refused

PROGRAM = "antigravity-sdk"
BRIDGE = os.path.join(os.path.dirname(os.path.abspath(__file__)), "antigravity_bridge.py")

# Every built-in tool the SDK can turn off: its SDK name (the name its
# hooks carry) -> the name the model sees. On by default: the SDK's own
# default set, and `ask_question`, which weft's questions need.
# (`list_directory`, `search_directory` and `find_file` are off unless asked
# for, and stay off.)
BUILT_IN = {
    "view_file": "view_file", "create_file": "write_to_file", "edit_file": "replace_file_content",
    "run_command": "run_command", "ask_question": "ask_question", "invoke_subagent": "invoke_subagent",
    "run_workflow": "run_workflow", "generate_image": "generate_image", "search_web": "search_web",
    "read_url_content": "read_url_content", "schedule": "schedule", "finish": "finish",
}

# What each permission setting lets go ahead without asking, by the SDK's
# names: its read-only tools (the SDK's own `BuiltinTools.read_only`, and
# the deprecated finders) and the question tool, which is itself a question;
# then file edits too; then everything.
# SYNC: Antigravity's permission settings <-> catalog/agents/server/agents.json (antigravity.permissions), catalog/agents/google_antigravity/metadata.json (permission)
READ_ONLY = frozenset({"view_file", "read_url_content", "schedule", "finish", "list_directory", "search_directory", "find_file",
                       "skill_lookup", "ask_question"})
EDITS = frozenset({"create_file", "edit_file"})
PERMISSIONS = {"ask": READ_ONLY, "accept_edits": READ_ONLY | EDITS, "bypass": None}

# How the bridge finds a connection's key: the variable it is in, and
# whether it is a Vertex AI key.
# SYNC: Antigravity's login ways <-> catalog/agents/server/agents.json (antigravity.logins)
VERTEX = {"apiKey": False, "vertexKey": True}
# The person's own Vertex AI settings, taken out under a connection's login,
# which is the only one it may have.
CONNECTION_UNSET_ENV = ("GOOGLE_GENAI_USE_VERTEXAI", "GOOGLE_GENAI_USE_ENTERPRISE", "GOOGLE_CLOUD_PROJECT",
                        "GOOGLE_CLOUD_LOCATION", "GOOGLE_APPLICATION_CREDENTIALS")

EFFORTS = ("minimal", "low", "medium", "high", "extra_high")
OPTIONS = {
    "effort": Lever(Lever.TEXT, lambda v: Setup(session={"effort": v}), EFFORTS, "How hard the model thinks"),
    "appendSystemPrompt": Lever(Lever.TEXT, lambda v: Setup(session={"systemPrompt": v}), None,
                                "Instructions added to its system prompt"),
    "addDirs": Lever(Lever.LIST, lambda vs: Setup(session={"dirs": vs}), None, "More folders it may read and change"),
}

# A tool's file changes, as weft's `file_change` names them.
FILE_CHANGES = {"create_file": "add", "edit_file": "update"}

# Where the harness keeps the conversations, in the agent server's folder.
SAVE_DIR = "antigravity-conversations"


def describe(name: str, args: dict) -> tuple:
    """A permission request's title and detail."""
    if name == "run_command":
        return "Google Antigravity wants to run a command", args.get("CommandLine") or ""
    if name in EDITS:
        return "Google Antigravity wants to change a file", args.get("TargetFile") or ""
    if name == "search_web":
        return "Google Antigravity wants to search the web", args.get("query") or json.dumps(args)
    return f"Google Antigravity wants to use {name}", json.dumps(args)


def ask_questions(question_id: str, request: dict) -> dict:
    """Its `ask_question` as weft's question: one per item, each taking an
    answer of the person's own besides its options (the harness carries a
    free answer beside the picks)."""
    return ev.questions(question_id, [
        ev.asked(f"q{index}", "", item.get("question") or "", [{"label": o["text"], "description": ""} for o in item.get("options") or []],
                 item.get("isMultiSelect"), True)
        for index, item in enumerate(request.get("questions") or [])])


def question_answer(question: dict, answer: dict, request: dict) -> dict:
    """weft's answers as the SDK's: each pick by its option's id, anything
    else as the free answer."""
    given = ev.answers_for(answer, question)
    responses = []
    for index, item in enumerate(request.get("questions") or []):
        by_text = {o["text"]: o["id"] for o in item.get("options") or []}
        values = given[f"q{index}"]
        responses.append({"selectedOptionIds": [by_text[v] for v in values if v in by_text],
                          "freeformResponse": ", ".join(v for v in values if v not in by_text), "skipped": False})
    return {"responses": responses}


class Antigravity(adapters.Adapter):
    kind = "antigravity"
    program_name = PROGRAM
    permissions = PERMISSIONS
    option_names = tuple(OPTIONS)

    def __init__(self, conversation, config, tools):
        super().__init__(conversation, config, tools)
        self.setup = Setup()
        for name, value in self.options.items():
            if value is not None:
                self.setup = self.setup.add(OPTIONS[name].setup(self.label, name, value))
        self.reset_turn()

    def reset_turn(self) -> None:
        self.reply = []
        # The main agent's steps already said, by step id: their thinking
        # and their message go out once, when the step is done.
        self.said = set()
        # The main agent's calls of its own tools that went ahead (by call
        # id): [name, arguments]. And the calls named like one of the
        # program's tools, until the route says they are the program's.
        self.own = {}
        self.held = {}
        # The call ids the program's route saw.
        self.routed = set()

    def command(self) -> list:
        return [self.program(), BRIDGE]

    def credential_env(self, env: dict) -> None:
        if self.credentials is None:
            # The machine's own login is the key in the person's own
            # environment: nothing of it is taken out.
            return
        for name in CONNECTION_UNSET_ENV:
            env.pop(name, None)
        super().credential_env(env)

    def enabled(self) -> list:
        """The built-ins left on: every one but those a program tool is named
        like, by either of its names."""
        names = self.program_tools.names
        return [sdk for sdk, shown in BUILT_IN.items() if sdk not in names and shown not in names]

    def start_message(self, conversation: str, resume: bool) -> dict:
        """What `start` carries, but its id."""
        session = self.setup.session
        key = None
        if self.credentials is not None:
            way = adapters.login_way(self.kind, self.credentials)
            key = {"env": way["env"]["key"], "vertex": VERTEX[self.credentials["way"]]}
        return {"type": "start", "workDir": self.config["folder"], "dirs": session.get("dirs") or [],
                "conversation": conversation, "resume": resume, "saveDir": os.path.join(self.conv.home, SAVE_DIR),
                "model": self.config.get("model") or "", "effort": session.get("effort") or "",
                "systemPrompt": session.get("systemPrompt") or "", "key": key, "tools": self.tools, "enabled": self.enabled()}

    def handshake(self) -> None:
        conversation = self.conv.conversation_id()
        # A new conversation's id is this side's: the harness takes any of
        # 32 or more letters, digits and dashes.
        minted = conversation or uuid.uuid4().hex
        rid = self.next_request_id()
        try:
            self.call(rid, {"id": rid, **self.start_message(minted, conversation is not None)}, "start")
        except adapters.RpcRefused as refused:
            if conversation is None:
                raise
            raise Refused(f"{refused}; the conversation cannot be picked up again, so start it over from the node's card") from None
        if conversation is None:
            self.conv.set_conversation(minted)

    def begin(self, turn) -> None:
        self.reset_turn()
        rid = self.next_request_id()
        self.call(rid, {"id": rid, "type": "prompt", "text": turn.message}, "prompt")

    def main(self, trajectory: str) -> bool:
        """Whether a step of `trajectory` is the main agent's."""
        return trajectory == self.conv.conversation_id()

    # ----- what the bridge writes -------------------------------------------

    def on_message(self, message: dict) -> None:
        kind = message.get("type")
        if kind is None:
            self.answered(message)
            return
        turn = self.turn
        if kind == "ask":
            self._ask(turn, message)
            return
        if turn is None or turn.status in ev.ENDED:
            return
        if kind == "step":
            self._step(turn, message["step"])
        elif kind == "tool_done":
            self._tool_done(turn, message["result"])
        elif kind == "ended":
            if message.get("error"):
                said = "\n\n".join(self.reply).strip()
                why = f"{self.label} could not run the turn: {message['error']}"
                self.conv.fail(turn, f"{said} ({why})" if said else why)
            else:
                self.conv.finish(turn, "\n\n".join(self.reply))
        else:
            raise ValueError(f"the bridge wrote a line of type {kind!r}, which this adapter does not know")

    def _step(self, turn, step: dict) -> None:
        """A message of the main agent's, to the person. Its tool calls'
        steps are left out: the hooks say each call, by its own id."""
        if not self.main(step.get("trajectory_id")) or step.get("source") != "MODEL" or step.get("target") != "TARGET_USER":
            return
        if self.config.get("streamBy") == "token" and step.get("type") == "TEXT_RESPONSE" and step.get("content_delta"):
            self.conv.emit(turn, ev.TEXT_DELTA, text=step["content_delta"])
        if step.get("status") != "DONE" or step["id"] in self.said:
            return
        self.said.add(step["id"])
        if (step.get("thinking") or "").strip():
            self.conv.emit(turn, ev.THINKING, text=step["thinking"])
        text = step.get("content") or ""
        if step.get("type") == "TEXT_RESPONSE" and text.strip():
            self.reply.append(text)
            self.conv.emit(turn, ev.TEXT, text=text)
            self.remember_text(text)

    def _tool_done(self, turn, result: dict) -> None:
        call_id = result.get("id")
        if call_id in self.routed:
            # Its result is the route's (`Conversation.deliver`).
            return
        held = self.held.pop(call_id, None)
        if held is not None:
            # Named like one of the program's tools, and it never came
            # through the program's route (refused before it ran).
            self.own[call_id] = held
            self.conv.emit(turn, ev.TOOL_USE, id=call_id, name=held[0], input=held[1])
            self.remember_tool_use(call_id, *held)
        own = self.own.pop(call_id, None)
        if own is None:
            # A sub-agent's, or one already said (the SDK may say a result twice).
            return
        failed = result.get("error") is not None
        output = result["error"] if failed else result.get("result") or ""
        self.conv.emit(turn, ev.TOOL_RESULT, id=call_id, output=output, isError=failed)
        self.remember_tool_result(call_id, output)
        change = FILE_CHANGES.get(own[0])
        if change is not None and not failed:
            self.conv.emit(turn, ev.FILE_CHANGE, path=own[1].get("TargetFile"), change=change)

    # ----- what the bridge asks ---------------------------------------------------

    def _ask(self, turn, message: dict) -> None:
        ask, kind, request = message["ask"], message.get("kind"), message.get("request") or {}
        if kind == "pre_tool":
            self._pre_tool(turn, ask, request)
            return
        if turn is None or turn.status in ev.ENDED:
            # Nobody is here to answer: the harness hears so at once.
            gone = "the turn ended first, so weft will never answer this"
            self._answer(ask, {"question": {"cancelled": True, "responses": []},
                               "tool": {"output": gone, "isError": True}}[kind])
            return
        if kind == "tool":
            self._program_call(turn, ask, request)
        elif kind == "question":
            question_id = f"antigravity-{ask}"
            question = ask_questions(question_id, request)
            self.block(turn, ev.QUESTION, question_id, {"question": question},
                       lambda answer: self._answer(ask, question_answer(question, answer, request)),
                       lambda why: self._answer(ask, {"cancelled": True, "responses": []}))
        else:
            raise ValueError(f"the bridge asked {kind!r}, which this adapter does not know")

    def _pre_tool(self, turn, ask: str, request: dict) -> None:
        """A tool is about to run: the main agent's call of a program tool's
        name goes ahead (the built-in of that name is turned off, so the
        call is the program's), anything else by the permission setting. A
        sub-agent's call is never taken for the program's by its name: its
        own built-ins are not the main agent's, and none is turned off."""
        call_id, name, args = request.get("id"), request.get("name") or "", request.get("args") or {}
        main = self.main((request.get("stepId") or "").split(":")[0])
        if main and name in self.program_tools:
            self.held[call_id] = [name, args]
            self._answer(ask, {"allow": True})
            return
        if turn is None or turn.status in ev.ENDED:
            self._answer(ask, {"allow": False, "message": "the turn ended first, so weft will never answer this"})
            return
        free = PERMISSIONS[self.permission]
        if free is None or name in free:
            self._went_ahead(turn, main, call_id, name, args)
            self._answer(ask, {"allow": True})
            return
        question_id = f"antigravity-{ask}"
        title, detail = describe(name, args)
        question = ev.permission(question_id, name, title, detail, args,
                                 [ev.choice("allow", "Allow", ev.ALLOW_ONCE), ev.choice("reject", "Deny", ev.REJECT)])

        def answered(answer: dict) -> None:
            picked = ev.chosen(answer, question)
            if picked["kind"] == ev.REJECT:
                self._answer(ask, {"allow": False, "message": answer.get("note") or "The person did not allow this."})
                return
            self._went_ahead(turn, main, call_id, name, args)
            self._answer(ask, {"allow": True})

        self.block(turn, ev.QUESTION, question_id, {"question": question}, answered,
                   lambda why: self._answer(ask, {"allow": False, "message": why}))

    def _went_ahead(self, turn, main: bool, call_id: str, name: str, args: dict) -> None:
        if not main:
            return
        self.own[call_id] = [name, args]
        self.conv.emit(turn, ev.TOOL_USE, id=call_id, name=name, input=args)
        self.remember_tool_use(call_id, name, args)

    def _program_call(self, turn, ask: str, request: dict) -> None:
        """A call of the program's tool, through its route: weft's `tool_call`,
        which the history gets under its plain name (`Conversation.block`)."""
        call_id, name, args = request.get("id"), request.get("name") or "", request.get("args") or {}
        if name not in self.program_tools:
            raise ValueError(f"{self.label} called the program's tool {name!r}, which the program does not have")
        self.held.pop(call_id, None)
        self.routed.add(call_id)
        self.block(turn, ev.TOOL_CALL, call_id, {"callId": call_id, "name": name, "arguments": json.dumps(args)},
                   lambda result: self._answer(ask, {"output": str(result.get("output", "")), "isError": bool(result.get("isError"))}),
                   lambda why: self._answer(ask, {"output": why, "isError": True}))

    def _answer(self, ask: str, response: dict) -> None:
        self.send({"type": "answer", "ask": ask, "response": response})


adapters.register(Antigravity)
