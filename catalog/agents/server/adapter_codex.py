"""Codex, driven through `codex app-server`: JSON-RPC, one JSON object per line.

`initialize` (with the experimental API, which the program's tools need:
`dynamicTools`), then `thread/start` or `thread/resume`, then one
`turn/start` per turn, ended by `turn/completed`. Codex asks this side as
JSON-RPC requests, each answered for as long as weft takes: approvals
(`item/commandExecution/requestApproval`, `item/fileChange/requestApproval`,
`item/permissions/requestApproval`), questions (`item/tool/requestUserInput`,
which in the default mode needs the `default_mode_request_user_input`
feature) and the program's own tools (`item/tool/call`).

While one of the program's tools is unanswered, Codex asks its model again
every half minute or so (a reasoning item each time), so a tool a person
answers days later costs model requests for as long as it waits; and it does
not wait for ever: a tool held open on codex-cli 0.166.0 came back failed
after 21 minutes (a dynamic tool's `item/completed` with status `failed`,
which is also how a call weft answered with an error ends).

The program's tools are `dynamicTools` of the thread, under their plain
names, and Codex calls them back with `item/tool/call` (the program's route).
A program's tool named like a built-in replaces it where Codex has a switch
that turns that built-in alone off (`REPLACEABLE`: on codex-cli 0.166.0, with
each switch the name no longer collided, and the model called the program's
tool of every one of those names in a real turn); the names it cannot give
up alone are refused when the program is built and when AskAgent fires
(the node's `reservedToolNames`). With
`features.tool_registry.error_on_tool_collisions` a name still taken fails
the turn ("duplicate tool: functions.<name>") where Codex would otherwise
drop the program's tool and go on. Codex refuses a dynamic tool named `mcp`,
or starting `mcp__`, or with any character outside `[a-zA-Z0-9_-]`, at
`thread/start` ("dynamic tool name is reserved"); the node refuses those
names first (its `reservedToolNames` and `reservedToolNamePatterns`), when
the program is built and when AskAgent fires, so Codex never sees one.

The model is always passed: a person's own `config.toml` may name one their
plan refuses. Left unset, the one Codex lists as its default for the login.
Inside a computer there is no bubblewrap, so Codex's own sandbox cannot
write: the computer is the wall, and the sandbox there is full access.

A ChatGPT login reaches Codex as its token and account alone
(`account/login/start` with `chatgptAuthTokens`), never with the refresh
token: Codex keeps that login in memory, writes no `auth.json`, and never
renews it on its own (its refresh token rotates, so two copies renewing it
would log each other out). weft renews it, once for every node holding the
connection. Codex asks this side for a fresh one when its service refuses
the token (`account/chatgptAuthTokens/refresh`, reason `unauthorized`), and
on codex-cli 0.166.0 waits about ten seconds for the answer before it gives
up on the request; so this side also asks weft for one well before the
token dies (`LOGIN_LEAD_SECS`), and hands every login weft gives it to Codex
at once, answering the refresh Codex waits on or logging it in again.
"""

import base64
import json
import threading
import time

import adapters
import events as ev
from events import Refused

# SYNC: Codex's permission settings <-> catalog/agents/codex/metadata.json (permission, sandbox), catalog/agents/server/agents.json
APPROVALS = {"ask": "untrusted", "on_request": "on-request", "never": "never", "bypass": "never"}
SANDBOXES = {"read_only": "read-only", "workspace_write": "workspace-write", "full_access": "danger-full-access"}

# The login ways Codex takes through `account/login/start`: an API key, and
# a ChatGPT login's token and account.
# SYNC: Codex's login ways <-> catalog/agents/server/agents.json (codex.logins),
#       catalog/agents/codex_access/metadata.json (the ways' ids)
WAY_API_KEY = "apiKey"
WAY_CHATGPT = "subscription"

# How long before its token dies Codex asks weft for a fresh one. Longer
# than the longest Codex works with nobody reading its turn (one of the
# program's tools held open, which codex-cli 0.166.0 gives up on after about
# twenty minutes): an ask raised then reaches the node once the tool's result
# carries the turn on, still before the token dies. A ChatGPT token lasts ten
# days, so this renews it about once in that time.
LOGIN_LEAD_SECS = 3600


def token_expiry(token: str):
    """When a JWT says it stops working (its `exp`, epoch seconds), read
    unverified; None for a token that is no JWT, or says no expiry."""
    parts = token.split(".")
    if len(parts) != 3:
        return None
    try:
        claims = json.loads(base64.urlsafe_b64decode(parts[1] + "=" * (-len(parts[1]) % 4)))
    except (ValueError, TypeError):
        return None
    exp = claims.get("exp") if isinstance(claims, dict) else None
    return exp if isinstance(exp, (int, float)) else None


def chatgpt_login(tokens: tuple) -> dict:
    """What `account/login/start` is sent for a ChatGPT login, and what a
    refresh Codex asked for is answered with (`chatgptAuthTokens`); the plan
    left out, for Codex to read off the token."""
    return {"accessToken": tokens[0], "chatgptAccountId": tokens[1], "chatgptPlanType": None}


# A built-in each program tool of its name replaces, and the setting that
# turns that built-in, and nothing else, off (a `config` override of the
# thread). A built-in whose only switch takes others with it is reserved
# instead (the node's `reservedToolNames`), read off codex-rs 0.166.0's
# `core/src/tools/spec_plan.rs`: `write_stdin` (its only switch is the whole
# shell, `features.shell_tool`; `features.unified_exec`, which would drop it
# alone, is forced back on unless a managed `requirements.toml` pins it), the
# goal tools (`features.goals` registers all three together) and
# `request_plugin_install` (`features.tool_suggest` also drops
# `list_available_plugins_to_install`). `exec_command` stays reserved too:
# with the shell off the model answered "unavailable in this session" for
# the program's tool of that name; so do `shell_command` (never collides,
# never reaches the model) and `clock__curr_time` (Codex's own answered the
# call), which are dropped without the collision error, so only weft's
# refusal at build time catches them.
REPLACEABLE = {
    "view_image": {"features.view_image": False},
    "web_search": {"web_search": "disabled"},
    "update_plan": {"tools.update_plan.enabled": False},
    "image_generation": {"features.image_generation": False},
}
# A clash left fails the turn, loudly.
COLLISIONS_FAIL = {"features.tool_registry.error_on_tool_collisions": True}


def thread_config(tools: list) -> dict:
    """The `config` overrides of the thread for the program's `tools`."""
    config = dict(COLLISIONS_FAIL)
    for tool in tools:
        config.update(REPLACEABLE.get(tool["name"], {}))
    return config


def thread_params(config: dict, tools: list, model: str) -> dict:
    """What `thread/start` (and `thread/resume`) is sent for `config`."""
    options = config.get("options") or {}
    permission = config.get("permission") or "ask"
    if permission not in APPROVALS:
        raise Refused(f"Codex has no permission setting {permission!r}; it takes {', '.join(APPROVALS)}", 400)
    if config.get("inComputer") or permission == "bypass":
        sandbox = "danger-full-access"
    else:
        sandbox = SANDBOXES.get(options.get("sandbox") or "workspace_write")
        if sandbox is None:
            raise Refused(f"Codex has no sandbox setting {options.get('sandbox')!r}; it takes {', '.join(SANDBOXES)}", 400)
    params = {"cwd": config["folder"], "model": model, "approvalPolicy": APPROVALS[permission], "sandbox": sandbox,
              "config": thread_config(tools)}
    if options.get("appendSystemPrompt"):
        params["developerInstructions"] = options["appendSystemPrompt"]
    if tools:
        params["dynamicTools"] = [{"type": "function", "name": t["name"], "description": t.get("description") or "",
                                   "inputSchema": t.get("parameters") or {"type": "object", "properties": {}}}
                                  for t in tools]
    return params


def approval_question(request_id: str, method: str, params: dict) -> dict:
    """An approval request as weft's permission question."""
    if method == "item/commandExecution/requestApproval":
        tool, title = "shell", "Codex wants to run a command"
        detail = params.get("command") or ""
        if params.get("cwd"):
            detail += f"\n(in {params['cwd']})"
    elif method == "item/fileChange/requestApproval":
        tool, title = "edit", "Codex wants to change files"
        detail = params.get("reason") or (f"write under {params['grantRoot']}" if params.get("grantRoot") else "apply its changes")
    else:
        tool, title = "permissions", "Codex wants more permissions"
        detail = params.get("reason") or json.dumps(params.get("permissions"))
    choices = [ev.choice("accept", "Allow", ev.ALLOW_ONCE), ev.choice("acceptForSession", "Allow for this conversation", ev.ALLOW_ALWAYS),
               ev.choice("decline", "Deny", ev.REJECT)]
    return ev.permission(request_id, tool, title, detail, params, choices)


def approval_answer(method: str, params: dict, question: dict, answer: dict) -> dict:
    picked = ev.chosen(answer, question)
    if method == "item/permissions/requestApproval":
        if picked["kind"] == ev.REJECT:
            return {"permissions": {}}
        return {"permissions": params.get("permissions") or {}, "scope": "session" if picked["kind"] == ev.ALLOW_ALWAYS else "turn"}
    return {"decision": picked["id"]}


def input_question(request_id: str, params: dict) -> dict:
    asked = []
    for q in params.get("questions") or []:
        options = [{"label": o.get("label", ""), "description": o.get("description", "")} for o in q.get("options") or []]
        asked.append(ev.asked(q.get("id", ""), q.get("header", ""), q.get("question", ""), options, False,
                              q.get("isOther", False) or not options, q.get("isSecret", False)))
    return ev.questions(request_id, asked)


def input_answer(question: dict, answer: dict) -> dict:
    answers = ev.answers_for(answer, question)
    return {"answers": {qid: {"answers": values} for qid, values in answers.items()}}


def tool_answer(result: dict) -> dict:
    return {"success": not result.get("isError"), "contentItems": [{"type": "inputText", "text": str(result.get("output", ""))}]}


class Codex(adapters.Adapter):
    kind = "codex"
    program_name = "codex"
    permissions = APPROVALS
    option_names = ("sandbox", "appendSystemPrompt", "effort")

    def __init__(self, conversation, config, tools):
        super().__init__(conversation, config, tools)
        self.thread = None
        self.text = ""
        # A ChatGPT login, behind `login_lock`: the token and account Codex
        # holds (`held`, None until the handshake logged it in), the
        # refresh requests Codex waits on, whether weft was asked for a
        # login not handed yet (`wanted`), and the token an ask before its
        # expiry was made for (one ask per token).
        self.login_lock = threading.Lock()
        self.held = None
        self.refreshes = []
        self.wanted = False
        self.asked_for = None
        # The one timer asking for a fresh login before the held token dies
        # (`_arm_expiry`): a newer login replaces it.
        self.expiry_timer = None

    def command(self) -> list:
        return [self.program(), "app-server", "--enable", "default_mode_request_user_input"]

    def credential_env(self, env: dict) -> None:
        # Keys Codex would read on its own, over the login it was handed.
        for name in ("OPENAI_API_KEY", "CODEX_API_KEY"):
            env.pop(name, None)
        super().credential_env(env)

    # ----- JSON-RPC ----------------------------------------------------------

    def request(self, method: str, params: dict) -> dict:
        rid = self.next_request_id()
        return self.call(rid, {"id": rid, "method": method, "params": params}, method)

    def handshake(self) -> None:
        self.request("initialize", {"clientInfo": {"name": "weft", "version": "1"}, "capabilities": {"experimentalApi": True}})
        self.send({"method": "initialized"})
        credentials = self.credentials
        if credentials is not None and credentials.get("way") == WAY_API_KEY:
            self.request("account/login/start", {"type": "apiKey", "apiKey": self.login_value("key")})
        if credentials is not None and credentials.get("way") == WAY_CHATGPT:
            self._log_in()
        account = self.request("account/read", {})
        if account.get("requiresOpenaiAuth") and not account.get("account"):
            where = "the connection's login" if credentials is not None else "this machine's own login (`codex login`)"
            # Without a login Codex retries every call five times and then
            # fails; say it before the turn rather than after.
            raise Refused(f"Codex is not logged in with {where}; log in again, or give the node a Codex connection")
        model = self.config.get("model") or self.default_model()
        params = thread_params(self.config, self.tools, model)
        conversation = self.conv.conversation_id()
        if conversation:
            answer = self.request("thread/resume", dict(params, threadId=conversation))
        else:
            answer = self.request("thread/start", params)
        self.thread = answer["thread"]["id"]
        self.conv.set_conversation(self.thread)

    def default_model(self) -> str:
        models = self.request("model/list", {}).get("data") or []
        for model in models:
            if model.get("isDefault"):
                return model["model"]
        if models:
            return models[0]["model"]
        raise Refused("Codex lists no model for this login; pick one in the node's `model`")

    def begin(self, turn) -> None:
        self.text = ""
        # An ask made while no turn ran had nothing to carry it.
        self._check_expiry()
        params = {"threadId": self.thread, "input": [{"type": "text", "text": turn.message}]}
        if self.options.get("effort"):
            params["effort"] = self.options["effort"]
        threading.Thread(target=self._start, args=(turn, params), daemon=True).start()

    def _start(self, turn, params: dict) -> None:
        try:
            self.request("turn/start", params)
        except Refused as refused:
            self.conv.fail(turn, str(refused))

    # ----- what Codex writes -------------------------------------------------

    def on_message(self, message: dict) -> None:
        if "method" not in message:
            self.answered(message)
            return
        method, params = message["method"], message.get("params") or {}
        turn = self.turn
        if "id" in message:
            self._server_request(turn, message["id"], method, params)
            return
        if turn is None:
            return
        if method == "item/started":
            self._item_started(turn, params.get("item") or {})
        elif method == "item/completed":
            self._item_completed(turn, params.get("item") or {})
        elif method == "item/agentMessage/delta" and self.config.get("streamBy") == "token":
            self.conv.emit(turn, ev.TEXT_DELTA, text=params.get("delta", ""))
        elif method == "turn/completed":
            self._turn_completed(turn, params.get("turn") or {})

    def _item_started(self, turn, item: dict) -> None:
        kind = item.get("type")
        if kind == "commandExecution":
            self.conv.emit(turn, ev.TOOL_USE, id=item.get("id"), name="shell", input={"command": item.get("command")})
            self.remember_tool_use(item.get("id"), "shell", {"command": item.get("command")})
        elif kind in ("mcpToolCall", "webSearch"):
            name = item.get("tool") or kind
            self.conv.emit(turn, ev.TOOL_USE, id=item.get("id"), name=name, input=item.get("arguments") or {"query": item.get("query")})
            self.remember_tool_use(item.get("id"), name, item.get("arguments") or {})

    def _item_completed(self, turn, item: dict) -> None:
        kind = item.get("type")
        if kind == "agentMessage" and item.get("text"):
            self.text = item["text"]
            self.conv.emit(turn, ev.TEXT, text=item["text"])
            self.remember_text(item["text"])
        elif kind == "reasoning":
            text = "\n".join(adapters.text_of(part) if not isinstance(part, str) else part
                             for part in (item.get("summary") or []) + (item.get("content") or []))
            if text.strip():
                self.conv.emit(turn, ev.THINKING, text=text)
        elif kind == "commandExecution":
            output = item.get("aggregatedOutput") or ""
            self.conv.emit(turn, ev.TOOL_RESULT, id=item.get("id"), output=output, isError=item.get("exitCode") not in (0, None))
            self.remember_tool_result(item.get("id"), output)
        elif kind == "fileChange":
            for change in item.get("changes") or []:
                self.conv.emit(turn, ev.FILE_CHANGE, path=change.get("path"), change=change.get("kind"))
        elif kind in ("mcpToolCall", "webSearch"):
            output = adapters.text_of((item.get("result") or {}).get("content")) if isinstance(item.get("result"), dict) else str(item.get("result") or "")
            self.conv.emit(turn, ev.TOOL_RESULT, id=item.get("id"), output=output, isError=item.get("status") == "failed")
            self.remember_tool_result(item.get("id"), output)
        elif kind == "dynamicToolCall" and item.get("status") == "failed":
            # Either the normal end of a call weft answered with an error
            # (`success: false`), or Codex giving up on one weft has not
            # answered yet (it waits about twenty minutes): the item's id
            # is the call's `callId`, and `stop_waiting` tells the two
            # apart; an id that is neither fails the turn, naming both. The
            # call's history is its route's (`Conversation.block`).
            self.conv.stop_waiting(turn, item.get("id"))

    def _turn_completed(self, turn, completed: dict) -> None:
        if completed.get("status") == "completed":
            self.conv.finish(turn, self.text)
        else:
            error = completed.get("error") or {}
            self.conv.fail(turn, f"Codex ended the turn {completed.get('status')}: {error.get('message') or error or 'no reason given'}")

    def _server_request(self, turn, rid, method: str, params: dict) -> None:
        def reply(result: dict) -> None:
            self.send({"id": rid, "result": result})

        if method == "account/chatgptAuthTokens/refresh":
            # Whatever runs: Codex asks during its handshake too.
            self._refresh_asked(rid)
            return

        def refuse(why: str) -> None:
            self.send({"id": rid, "error": {"code": -32000, "message": why}})

        if turn is None or turn.status in ev.ENDED:
            refuse("no turn is running on the weft side")
            return
        question_id = f"codex-{rid}"
        if method in ("item/commandExecution/requestApproval", "item/fileChange/requestApproval", "item/permissions/requestApproval"):
            question = approval_question(question_id, method, params)
            self.block(turn, ev.QUESTION, question_id, {"question": question},
                       lambda answer: reply(approval_answer(method, params, question, answer)), refuse)
        elif method == "item/tool/requestUserInput":
            question = input_question(question_id, params)
            self.block(turn, ev.QUESTION, question_id, {"question": question},
                       lambda answer: reply(input_answer(question, answer)), refuse)
        elif method == "item/tool/call":
            call_id = params.get("callId")
            if not isinstance(call_id, str) or not call_id:
                # Its `callId` is what Codex names the call by when it gives
                # up on it; a call without one could never be matched.
                why = f"Codex called the program's tool {params.get('tool')!r} with no callId; update Codex, or the agent server"
                self.send({"id": rid, "error": {"code": -32602, "message": why}})
                self.conv.fail(turn, why)
                return
            self.block(turn, ev.TOOL_CALL, call_id,
                       {"callId": call_id, "name": params.get("tool"), "arguments": json.dumps(params.get("arguments") or {})},
                       lambda result: reply(tool_answer(result)), refuse)
        else:
            self.send({"id": rid, "error": {"code": -32601, "message": f"weft does not answer {method}"}})


    # ----- a ChatGPT login weft renews ----------------------------------------

    def _log_in(self) -> None:
        """Log Codex in with the configured ChatGPT login, again if a newer
        one was handed while the first went over."""
        while True:
            with self.login_lock:
                tokens = self._tokens()
            self.request("account/login/start", dict(chatgpt_login(tokens), type="chatgptAuthTokens"))
            with self.login_lock:
                if self._tokens() == tokens:
                    self.held = tokens
                    break
        self._arm_expiry()

    def take_login(self, credentials: dict):
        """A login weft handed while Codex runs (`Adapter.take_login`): a
        ChatGPT login goes to Codex at once, answering the refreshes it
        waits on and logging it in again. Not one it holds already, which
        is taken only while Codex asks for nothing. An API key, or a change
        of way, replaces the process instead."""
        if credentials is None or credentials.get("way") != WAY_CHATGPT or (self.credentials or {}).get("way") != WAY_CHATGPT:
            return None
        with self.login_lock:
            self.credentials = credentials
            tokens = self._tokens()
            if self.held is None:
                # Not logged in yet: the handshake logs it in on this one.
                return True
            if tokens == self.held:
                return not self.wanted
            waiting, self.refreshes = self.refreshes, []
            self.wanted = False
            self.held = tokens
            expiry = token_expiry(tokens[0])
            if expiry is not None and expiry - time.time() <= LOGIN_LEAD_SECS:
                # Handed as weft's freshest already: asking again before it
                # dies would only bring this one back, and ask again. The
                # first login is asked about (the connection may hold an old
                # one weft renews on that ask); Codex asks for this one once
                # its service refuses it.
                self.asked_for = tokens[0]
        for rid in waiting:
            self.send({"id": rid, "result": chatgpt_login(tokens)})
        # Logged in again whether or not a refresh waited: Codex gives up on
        # one after about ten seconds, and an answer to a refresh it gave up
        # on never reaches its login.
        self.request("account/login/start", dict(chatgpt_login(tokens), type="chatgptAuthTokens"))
        self._arm_expiry()
        return True

    def refuse_login(self, why: str) -> None:
        with self.login_lock:
            waiting, self.refreshes = self.refreshes, []
            self.wanted = False
        for rid in waiting:
            self.send({"id": rid, "error": {"code": -32000, "message": why}})

    def _refresh_asked(self, rid) -> None:
        """Codex's service refused its token: a newer one weft already
        handed answers at once; else the refresh waits for the one the
        agent node brings (`take_login`), asked for once."""
        with self.login_lock:
            configured = self._tokens() if (self.credentials or {}).get("way") == WAY_CHATGPT else None
            if configured is None:
                answer, ask = None, False
            elif configured != self.held:
                self.held = answer = configured
                ask = False
            else:
                answer = None
                self.refreshes.append(rid)
                ask, self.wanted = not self.wanted, True
        if configured is None:
            self.send({"id": rid, "error": {"code": -32000, "message": "weft handed Codex no ChatGPT login to refresh; give the node a Codex connection"}})
        elif answer is not None:
            self.send({"id": rid, "result": chatgpt_login(answer)})
        elif ask and not self.want_login("unauthorized"):
            self.refuse_login(f"{self.label}'s ChatGPT login was refused, and no turn is running to ask weft for a fresh one; "
                              "the next turn starts on the login the connection holds then")

    def _tokens(self) -> tuple:
        """The configured ChatGPT login's token and account."""
        return self.login_value("token"), self.login_value("accountId")

    def _arm_expiry(self) -> None:
        """Ask for a fresh login `LOGIN_LEAD_SECS` before the held token dies,
        on one timer: the one for the token held now replaces any other."""
        with self.login_lock:
            expiry = token_expiry(self.held[0]) if self.held else None
            timer = None
            if expiry is not None:
                timer = threading.Timer(max(0.0, expiry - LOGIN_LEAD_SECS - time.time()), self._check_expiry)
                timer.daemon = True
            before, self.expiry_timer = self.expiry_timer, timer
            if before is not None:
                before.cancel()
            if timer is not None:
                timer.start()

    def stop_rest(self) -> None:
        """The expiry timer goes with the process it watched the login of."""
        with self.login_lock:
            timer, self.expiry_timer = self.expiry_timer, None
        if timer is not None:
            timer.cancel()

    def _check_expiry(self) -> None:
        """The held token dies within `LOGIN_LEAD_SECS`: ask weft for a fresh
        one, once per token, while a turn runs to carry the ask (else the
        next turn's start asks)."""
        with self.login_lock:
            token = self.held[0] if self.held else None
            expiry = token_expiry(token) if token else None
            if (self.retired is not None or expiry is None or expiry - time.time() > LOGIN_LEAD_SECS
                    or self.wanted or self.asked_for == token):
                return
            self.wanted, self.asked_for = True, token
        if not self.want_login("expiring"):
            with self.login_lock:
                self.wanted, self.asked_for = False, None


adapters.register(Codex)
