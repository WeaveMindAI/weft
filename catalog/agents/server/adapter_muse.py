"""Muse Code, driven through `muse serve`: the Muse Session Protocol (MSP),
JSON-RPC, one JSON object per line.

`initialize` (asking for `sessionMcp`, without which a session takes no MCP
server), the `initialized` notification (without it every later request is
refused `notInitialized`), then `session/start` or `session/resume`, then one
`turn/start` per turn, ended by `turn/completed` (`turn/foregroundCompleted`
comes first while Muse's own reminder checks still hold the turn open).
Every command carries a UUIDv7 `commandId` this side mints.

Muse asks this side with two requests, `approval/request` and
`userInput/request`. Each is answered at once with an empty receipt; the
decision travels later as its own command (`approval/decide`,
`userInput/answer`), sent when weft answers. A question has a countdown
after which Muse settles it by itself, and `userInput/engaged` (a
notification) stops it: it is sent the moment the question arrives, since a
person may answer days later. A shell pipeline is approved stage by stage:
each stage is its own `approval/request`, so its own weft question.

`muse acp` speaks ACP too, but only behind `MUSE_EXPERIMENTAL_ACP=1` with no
compatibility promise, and it is a translation over MSP; MSP is Muse's own
published, versioned protocol.

The program's tools are a per-session MCP server over HTTP (`config.mcpServers`
of `session/start`, given again on every `session/resume`, which does not
restart a server the start named; real `muse serve` called this server's
`initialize` and `tools/list` at both). MSP has no tools of the client's own
and no way to show an MCP tool by its plain name, so the model sees each as
`mcp__weft__<tool>`; its only switches over built-ins turn whole groups off
(`--disable-shell`, `--disable-write`, `--disable-request-user-input`), so a
program's tool never replaces one, and weft refuses, when the program is
built and when AskAgent fires, a program tool named like any of them (the node's
`reservedToolNames`, read from the toolset a 1.4.14 session recorded). Muse can take other tools' personal
rules and skills (it reads `CLAUDE_CONFIG_DIR` and `CODEX_HOME`), and
`context.foreign_personal_rules` / `_skills` turn that off, always.
"""

import json
import os
import threading
import time
import uuid

import adapters
import events as ev
from events import Refused

PROGRAM = "muse"

# weft's permission setting -> (Muse's permission profile, its approval mode).
# The profiles are what `permissionProfile/list` serves; the modes are MSP's
# closed `ApprovalMode`.
# SYNC: Muse Code's permission settings <-> catalog/agents/server/agents.json (muse.permissions)
PERMISSIONS = {
    # Every action no rule covers comes to weft, and no AI reviewer decides first.
    "ask": (":ask-me", "promptUnmatched"),
    # Muse's own posture: it asks only for what it deems needs it.
    "on_request": (":ask-me", "onRequest"),
    # Muse's default: an AI reviewer decides eligible requests, the rest come to weft.
    "auto_review": (":auto-review", "onRequest"),
    "read_only": (":read-only", "onRequest"),
    # Nothing is asked: what no rule covers is refused.
    "deny_unmatched": (":ask-me", "denyUnmatched"),
    "bypass": (":unrestricted", "allowAll"),
}

# Reasoning effort, per turn (`turn/start.reasoningEffort`); Meta's models refuse `none`.
EFFORTS = ("minimal", "low", "medium", "high", "xhigh", "max", "ultra")
# `--sandbox-network`, as weft spells its values.
NETWORKS = {"restricted": "restricted", "proxy_only": "proxy-only", "enabled": "enabled"}

# What a choice's `decision` is to weft. The set is open; a decision not
# here fails the turn rather than being shown as something it is not.
DECISIONS = {
    "approved": ev.ALLOW_ONCE,
    "approvedForSession": ev.ALLOW_ALWAYS,
    "approvedPolicyAmendment": ev.ALLOW_ALWAYS,
    "denied": ev.REJECT,
    "deniedPolicyAmendment": ev.REJECT,
    "abort": ev.REJECT,
}

# Variables the binary reads for providers no `--provider` of this build
# selects: another vendor's key is never Muse's to see.
OTHER_VENDORS = ("ANTHROPIC_API_KEY", "ANTHROPIC_BASE_URL", "OPENAI_API_KEY", "OPENAI_BASE_URL",
                 "OPENROUTER_API_KEY", "OPENROUTER_BASE_URL")


def uuid7() -> str:
    """A UUIDv7: what every MSP command's `commandId` must be."""
    millis = int(time.time() * 1000)
    rand = int.from_bytes(os.urandom(10), "big")
    value = (millis << 80) | (0x7 << 76) | (((rand >> 62) & 0xFFF) << 64) | (0b10 << 62) | (rand & ((1 << 62) - 1))
    return str(uuid.UUID(int=value))


def command(config: dict) -> list:
    """The `muse serve` command line for `config`."""
    options = config.get("options") or {}
    cmd = [adapters.program(config, PROGRAM), "serve", "--provider", "meta",
           "-c", "context.foreign_personal_rules=false", "-c", "context.foreign_personal_skills=false"]
    if switch(options, "trustWorkspace"):
        cmd.append("--trust-workspace")
    if switch(options, "contributorMode"):
        cmd += ["--contributor-mode", "on"]
    if switch(options, "disableQuestions"):
        cmd.append("--disable-request-user-input")
    if switch(options, "disableSandbox"):
        cmd.append("--disable-sandbox")
    network = options.get("network")
    if network:
        if network not in NETWORKS:
            raise Refused(f"Muse Code's `network` takes {', '.join(NETWORKS)}, and got {network!r}", 400)
        cmd += ["--sandbox-network", NETWORKS[network]]
    return cmd


def switch(options: dict, name: str) -> bool:
    value = options.get(name, False)
    if not isinstance(value, bool):
        raise Refused(f"Muse Code's `{name}` is on or off (true or false), and got {value!r}", 400)
    return value


def workspace_roots(config: dict) -> list:
    """The folder and the extra ones (`addDirs`), or nothing when there are none."""
    extra = (config.get("options") or {}).get("addDirs") or []
    if isinstance(extra, str):
        extra = [d.strip() for d in extra.split(",") if d.strip()]
    if not isinstance(extra, list) or not all(isinstance(d, str) for d in extra):
        raise Refused(f"Muse Code's `addDirs` is a list of folders, and got {extra!r}", 400)
    if not extra:
        return []
    roots = [config["folder"]]
    for directory in extra:
        directory = os.path.abspath(os.path.expanduser(directory))
        if not os.path.isdir(directory):
            raise Refused(f"Muse Code's `addDirs` names {directory}, which is not a folder; create it, or take it out", 400)
        roots.append(directory)
    return roots


def effort(config: dict):
    value = (config.get("options") or {}).get("effort")
    if value and value not in EFFORTS:
        raise Refused(f"Muse Code's `effort` takes {', '.join(EFFORTS)}, and got {value!r}", 400)
    return value or None


def mcp_servers() -> dict:
    return {adapters.MCP_SERVER: {"transport": "streamableHttp", "url": adapters.mcp_url(), "mode": "required"}}


def approval_question(question_id: str, params: dict) -> dict:
    """An `approval/request` as weft's permission question."""
    subject = params.get("subject") or {}
    kind = subject.get("kind") or "tool"
    if kind == "shell":
        title, detail = "Muse Code wants to run a command", subject.get("command") or ""
        if subject.get("workspaceRoot"):
            detail += f"\n(in {subject['workspaceRoot']})"
    elif kind == "fileAccess":
        title, detail = f"Muse Code wants to {subject.get('access') or 'access'} a file", subject.get("path") or ""
    elif kind == "network":
        title = "Muse Code wants to reach the network"
        detail = subject.get("host") or ""
        if subject.get("port"):
            detail += f":{subject['port']}"
        if subject.get("protocol"):
            detail = f"{subject['protocol']}://{detail}"
    else:
        title = f"Muse Code wants to use {subject.get('toolName') or params.get('toolName') or kind}"
        detail = subject.get("path") or subject.get("target") or params.get("rawArgs") or ""
    choices = []
    for offered in params.get("availableChoices") or []:
        decision = offered.get("decision")
        if decision not in DECISIONS:
            raise Refused(f"Muse Code offers a choice weft does not know ({decision!r}, labelled {offered.get('label')!r}); "
                          "update the agent server")
        label = offered.get("label") or decision
        if offered.get("rulePreview"):
            label += f" ({offered['rulePreview']})"
        choices.append(ev.choice(offered["choiceId"], label, DECISIONS[decision]))
    try:
        tool_input = json.loads(params.get("rawArgs") or "null")
    except json.JSONDecodeError:
        tool_input = params.get("rawArgs")
    return ev.permission(question_id, kind, title, detail, tool_input, choices)


def decision(session: str, params: dict, question: dict, answer: dict) -> dict:
    """What `approval/decide` is sent for weft's answer."""
    picked = ev.chosen(answer, question)
    decide = {"commandId": uuid7(), "sessionId": session, "approvalId": params["approvalId"],
              "choiceId": picked["id"], "requirementId": params["currentRequirementId"]}
    offered = next(c for c in params["availableChoices"] if c["choiceId"] == picked["id"])
    if answer.get("note") and offered.get("acceptsFeedback"):
        decide["feedback"] = answer["note"]
    return decide


def input_question(question_id: str, params: dict) -> dict:
    asked = []
    for q in params.get("questions") or []:
        options = [{"label": o.get("label", ""), "description": o.get("description", "")} for o in q.get("options") or []]
        multi = (q.get("selection") or {}).get("mode") == "multiple"
        # Every question takes free text besides its options.
        asked.append(ev.asked(q.get("id", ""), q.get("header", ""), q.get("question", ""), options, multi, True))
    return ev.questions(question_id, asked)


def input_answer(session: str, params: dict, question: dict, answer: dict) -> dict:
    """What `userInput/answer` is sent for weft's answer: an option by its label, else free text."""
    given = ev.answers_for(answer, question)
    answers = []
    for q in question["questions"]:
        values = given[q["id"]]
        labels = {o["label"] for o in q["options"]}
        entry = {"questionId": q["id"]}
        if all(v in labels for v in values):
            if q["multiSelect"]:
                entry["selectedLabels"] = values
            else:
                entry["selectedLabel"] = values[0]
        else:
            entry["freeText"] = "\n".join(values)
        answers.append(entry)
    return {"commandId": uuid7(), "sessionId": session, "userInputId": params["userInputId"], "answers": answers}


class Muse(adapters.Adapter):
    kind = "muse"
    program_name = PROGRAM
    permissions = PERMISSIONS
    option_names = ("trustWorkspace", "contributorMode", "disableQuestions", "disableSandbox", "network", "addDirs", "effort")

    def __init__(self, conversation, config, tools):
        super().__init__(conversation, config, tools)
        self.session = None
        # The Muse turn weft's running turn is, and what it said last.
        self.muse_turn = None
        self.text = ""
        # Item id -> kind, for the deltas (which name only the item).
        self.items = {}
        # The item ids of the program's own tool calls (`_item_started`).
        self.ours = set()
        # Checked before anything starts.
        self.command_line = command(config)
        self.roots = workspace_roots(config)
        self.effort = effort(config)

    @staticmethod
    def shown_name(plain: str) -> str:
        # How Muse names a tool of a session MCP server.
        return f"mcp__{adapters.MCP_SERVER}__{plain}"

    def command(self) -> list:
        return self.command_line

    def credential_env(self, env: dict) -> None:
        for name in OTHER_VENDORS:
            env.pop(name, None)
        super().credential_env(env)

    # ----- JSON-RPC ----------------------------------------------------------

    def request(self, method: str, params: dict) -> dict:
        rid = self.next_request_id()
        return self.call(rid, {"jsonrpc": "2.0", "id": rid, "method": method, "params": params}, method)

    def handshake(self) -> None:
        self.request("initialize", {"clientInfo": {"name": "weft", "version": "1"},
                                    "capabilities": {"requestedCapabilities": ["sessionMcp"], "userInputDialogs": True}})
        self.send({"jsonrpc": "2.0", "method": "initialized"})
        profile, mode = PERMISSIONS[self.permission]
        session = self.conv.conversation_id()
        if session:
            params = {"commandId": uuid7(), "sessionId": session, "permissionProfile": profile, "excludeItems": True,
                      "config": {"mcpServers": mcp_servers()}}
            if self.config.get("model"):
                params["modelId"] = self.config["model"]
            try:
                resumed = self.request("session/resume", params)
            except adapters.RpcRefused as refused:
                if (refused.error.get("data") or {}).get("kind") == "sessionNotFound":
                    raise Refused(f"Muse Code no longer has this conversation (session {session}, kept under "
                                  "~/.local/share/muse/sessions); start the conversation over from the node's card") from None
                raise
            self.session = resumed["session"]["sessionId"]
            # The approval mode is the session's own; resuming does not take one.
            self.request("session/setApprovalMode", {"commandId": uuid7(), "sessionId": self.session, "mode": mode})
            active = resumed["session"].get("activeTurnId")
            if active:
                # A turn of a server that is gone: whatever it waits on, nobody
                # on the weft side will answer it.
                self.request("turn/cancel", {"commandId": uuid7(), "sessionId": self.session, "turnId": active})
        else:
            params = {"commandId": uuid7(), "workspaceRoot": self.config["folder"], "approvalMode": mode,
                      "permissionProfile": profile, "config": {"mcpServers": mcp_servers()}}
            if self.config.get("model"):
                params["modelId"] = self.config["model"]
            started = self.request("session/start", params)
            self.session = started["session"]["sessionId"]
            self.conv.set_conversation(self.session)

    def begin(self, turn) -> None:
        self.text = ""
        command_id = uuid7()
        # A fresh turn's id is its command's.
        self.muse_turn = command_id
        params = {"commandId": command_id, "sessionId": self.session, "input": [{"type": "text", "text": turn.message}]}
        if self.effort:
            params["reasoningEffort"] = self.effort
        if self.roots:
            params["workspaceRoots"] = self.roots
        threading.Thread(target=self._start, args=(turn, params), daemon=True).start()

    def _start(self, turn, params: dict) -> None:
        try:
            ack = self.request("turn/start", params)
            self.muse_turn = ack.get("turnId") or self.muse_turn
        except Refused as refused:
            self.conv.fail(turn, str(refused))

    # ----- what Muse writes --------------------------------------------------

    def on_message(self, message: dict) -> None:
        if "method" not in message:
            self.answered(message)
            return
        method, params = message["method"], message.get("params") or {}
        if "id" in message:
            self._server_request(message["id"], method, params)
            return
        turn = self.turn
        if turn is None or turn.status in ev.ENDED or params.get("sessionId") != self.session:
            return
        if method in ("item/started", "item/completed"):
            item = params.get("item") or {}
            if item.get("turnId") not in (None, self.muse_turn):
                return
            self.items[item.get("itemId")] = item.get("kind")
            if method == "item/started":
                self._item_started(turn, item)
            else:
                self._item_completed(turn, item)
        elif method == "item/delta":
            if (self.items.get(params.get("itemId")) == "agentMessage" and params.get("field") == "text"
                    and self.config.get("streamBy") == "token"):
                self.conv.emit(turn, ev.TEXT_DELTA, text=params.get("delta", ""))
        elif method == "turn/completed" and params.get("turnId") == self.muse_turn:
            self._turn_completed(turn, params)

    def _item_started(self, turn, item: dict) -> None:
        if item.get("kind") == "toolCall":
            name = item.get("tool") or ""
            if self.program_tool(name) is not None:
                # The program's own tool: its call and result go out from the
                # program's route (weft's MCP server), which also writes them
                # into the history under the plain name, so the stream shows
                # each once.
                self.ours.add(item.get("itemId"))
                return
            self.conv.emit(turn, ev.TOOL_USE, id=item.get("itemId"), name=name, input=parsed(item.get("args")))
            self.remember_tool_use(item.get("itemId"), name, item.get("args") or "{}")

    def _item_completed(self, turn, item: dict) -> None:
        kind = item.get("kind")
        if kind == "agentMessage" and item.get("text"):
            self.text = item["text"]
            self.conv.emit(turn, ev.TEXT, text=item["text"])
            self.remember_text(item["text"])
        elif kind == "reasoning":
            text = "\n".join(item.get("summary") or []) or item.get("text") or ""
            if text.strip():
                self.conv.emit(turn, ev.THINKING, text=text)
        elif kind == "toolCall" and item.get("itemId") not in self.ours:
            output = item.get("visibleOutput") or item.get("failureReason") or ""
            self.conv.emit(turn, ev.TOOL_RESULT, id=item.get("itemId"), output=output, isError=item.get("status") != "completed")
            self.remember_tool_result(item.get("itemId"), output)

    def _turn_completed(self, turn, params: dict) -> None:
        terminal = params.get("terminal")
        if terminal == "completed":
            self.conv.finish(turn, self.text)
            return
        error = params.get("error") or {}
        if error.get("kind") == "authRequired":
            # A key Meta refused is said only on stderr, at startup; the
            # turn then says no more than "not logged in".
            refused = [line for line in self.stderr if "META_API_KEY was rejected" in line]
            if refused:
                why = "Meta rejected the connection's API key"
            elif self.credentials is not None:
                why = f"the connection's API key did not log it in ({error.get('message')})"
            else:
                why = f"this machine has no Muse Code login ({error.get('message')})"
            self.conv.fail(turn, f"Muse Code is not logged in: {why}; give the node a Muse Code connection with a "
                                 "working API key from dev.meta.ai, or run `muse login` on this machine")
            return
        reason = error.get("message") or params.get("reason") or "no reason given"
        self.conv.fail(turn, f"Muse Code ended the turn {terminal}: {reason}" + (f" ({error['kind']})" if error.get("kind") else ""))

    def _server_request(self, rid, method: str, params: dict) -> None:
        if method not in ("approval/request", "userInput/request"):
            self.send({"jsonrpc": "2.0", "id": rid, "error": {"code": -32601, "message": f"weft does not answer {method}"}})
            return
        # The receipt: the decision travels later, as its own command.
        self.send({"jsonrpc": "2.0", "id": rid, "result": {}})
        turn = self.turn
        if turn is None or turn.status in ev.ENDED or params.get("turnId") != self.muse_turn:
            # A request of a turn weft no longer runs (re-sent on a resume):
            # that turn is cancelled, which settles it.
            return
        if method == "approval/request":
            question_id = f"muse-approval-{params['approvalId']}-{params['currentRequirementId'].get('sourceIndex', 0)}"
            try:
                question = approval_question(question_id, params)
            except Refused as refused:
                self.conv.fail(turn, str(refused))
                return
            self.block(turn, ev.QUESTION, question_id, {"question": question},
                       lambda answer: self._command("approval/decide", decision(self.session, params, question, answer)),
                       lambda why: self._cancel(params["turnId"]))
        else:
            question_id = f"muse-input-{params['userInputId']}"
            # A person may answer days later: stop the countdown that would settle it alone.
            self.send({"jsonrpc": "2.0", "method": "userInput/engaged",
                       "params": {"sessionId": self.session, "userInputId": params["userInputId"]}})
            question = input_question(question_id, params)
            self.block(turn, ev.QUESTION, question_id, {"question": question},
                       lambda answer: self._command("userInput/answer", input_answer(self.session, params, question, answer)),
                       lambda why: self._cancel(params["turnId"]))

    def _cancel(self, muse_turn: str) -> None:
        """Cancel the Muse turn `muse_turn`, which settles what it waits on:
        a request weft refused (its turn ended meanwhile) has had its receipt
        and has no error answer of its own. Not waited on: this runs on the
        reader, which is what would read the answer."""
        rid = self.next_request_id()
        self.send({"jsonrpc": "2.0", "id": rid, "method": "turn/cancel",
                   "params": {"commandId": uuid7(), "sessionId": self.session, "turnId": muse_turn}})

    def _command(self, method: str, params: dict) -> None:
        answer = self.request(method, params)
        if answer.get("status") not in (None, "accepted"):
            raise Refused(f"Muse Code did not take the answer ({method}: {answer.get('status')})")


def parsed(arguments):
    """A tool call's arguments: the model's JSON, verbatim, read when it is JSON."""
    try:
        return json.loads(arguments) if arguments else {}
    except json.JSONDecodeError:
        return arguments


adapters.register(Muse)
