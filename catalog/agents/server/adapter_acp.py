"""Agents that speak the Agent Client Protocol (ACP): JSON-RPC over stdio.

`initialize`, then `session/new` (or `session/load` / `session/resume` to
pick a conversation up again), then one `session/prompt` per turn, whose
answer (its `stopReason`) ends the turn. What the agent does arrives as
`session/update` notifications. What it asks this side arrives as requests,
each answered for as long as weft takes: `session/request_permission`,
`elicitation/create` (a form: the agent's own ask tool), and the extension
requests some agents ask with instead.

The protocol is shared; what differs between two ACP agents is an `AcpSpec`
(`acp_<kind>.py`, one per agent, written from that agent's documentation,
its CLI and recorded lines). A spec is data plus a few hooks, and nothing in
this file knows one agent from another:

  - the command line, the environment, a config file written in the
    agent's private home;
  - what each weft permission setting, the model and each option does to
    the agent (`Setup`: flags, variables, a session mode, ACP config
    options, params of `session/new`);
  - what each login way adds (flags, the `authenticate` method);
  - how it picks a conversation up again;
  - how it takes the program's MCP server;
  - how its forms read, the requests it asks with besides the two above,
    and a permission request that is really a question.

A loaded conversation is replayed as `session/update` notifications, and
some agents send part of the replay after the load's answer: until the
agent has been quiet for `REPLAY_QUIET_SECS`, nothing it sends is a turn's.
"""

import json
import os
import re
import signal
import sys
import threading
import time

import adapters
import events as ev
from events import Refused

PROTOCOL_VERSION = 1
# How long a loaded conversation's replay must have been silent before the
# first turn starts: the replay of a long conversation comes in a burst,
# and its tail can follow the load's own answer.
REPLAY_QUIET_SECS = 1.0

# The stop reasons of a turn that ended as it should.
FINISHED = ("end_turn", "max_tokens", "max_turn_requests")


class Setup:
    """What one setting does to the agent. Every field adds to the others."""

    def __init__(self, args=(), env=None, mode=None, config=None, session=None):
        # Launch flags.
        self.args = list(args)
        # Environment variables.
        self.env = dict(env or {})
        # The session mode (`session/set_mode`).
        self.mode = mode
        # ACP config options (`session/set_config_option`), by config id.
        self.config = dict(config or {})
        # Params merged into `session/new` and the load (`_meta`, ...).
        self.session = dict(session or {})

    def add(self, other: "Setup") -> "Setup":
        merged = Setup(self.args + other.args, {**self.env, **other.env}, other.mode or self.mode,
                       {**self.config, **other.config}, deep_merge(self.session, other.session))
        return merged


class Lever:
    """One setting an agent takes (the model, or an option under
    `config.options`), and the `Setup` a value of it gives."""

    # The value's type, as the node's metadata declares it.
    TEXT, SWITCH, LIST, WHOLE = "text", "switch", "list", "whole"

    def __init__(self, kind: str, to_setup, values=None, description: str = ""):
        self.kind = kind
        self.to_setup = to_setup
        # The values it takes, when it takes only some.
        self.values = list(values) if values else None
        self.description = description

    def setup(self, label: str, name: str, value) -> Setup:
        if self.kind == Lever.SWITCH:
            if not isinstance(value, bool):
                raise Refused(f"{label}'s `{name}` is on or off (true or false), and got {value!r}", 400)
            return self.to_setup(value) if value else Setup()
        if self.kind == Lever.WHOLE:
            # A Number input may arrive as 50.0.
            if isinstance(value, bool) or not isinstance(value, (int, float)) or value != int(value) or value < 1:
                raise Refused(f"{label}'s `{name}` is a whole number, 1 or more, and got {value!r}", 400)
            return self.to_setup(str(int(value)))
        if self.kind == Lever.LIST:
            if isinstance(value, str):
                value = [v.strip() for v in value.split(",") if v.strip()]
            if not isinstance(value, list) or not all(isinstance(v, str) for v in value):
                raise Refused(f"{label}'s `{name}` is a list of text, and got {value!r}", 400)
            return self.to_setup(value) if value else Setup()
        if not isinstance(value, str):
            raise Refused(f"{label}'s `{name}` is text, and got {value!r}", 400)
        if self.values is not None and value not in self.values:
            raise Refused(f"{label}'s `{name}` takes {', '.join(self.values)}, and got {value!r}", 400)
        return self.to_setup(value) if value else Setup()


def flag(name: str, values=None, description: str = "", kind: str = Lever.TEXT) -> Lever:
    """`name value` on the command line (a whole number written out, for `kind` WHOLE)."""
    return Lever(kind, lambda v: Setup(args=[name, v]), values, description)


def switch(*args: str, description: str = "") -> Lever:
    """These flags when on."""
    return Lever(Lever.SWITCH, lambda v: Setup(args=list(args)), None, description)


def flags(name: str, description: str = "") -> Lever:
    """`name value` once per value."""
    return Lever(Lever.LIST, lambda vs: Setup(args=[a for v in vs for a in (name, v)]), None, description)


def joined(name: str, separator: str = ",", description: str = "") -> Lever:
    """`name a,b` once."""
    return Lever(Lever.LIST, lambda vs: Setup(args=[name, separator.join(vs)]), None, description)


def variable(name: str, values=None, description: str = "", kind: str = Lever.TEXT) -> Lever:
    """An environment variable."""
    return Lever(kind, lambda v: Setup(env={name: v}), values, description)


def config_option(config_id: str, values=None, description: str = "", kind: str = Lever.TEXT) -> Lever:
    """An ACP config option, set on the session (`session/set_config_option`,
    whose value is always text: a whole number written out)."""
    return Lever(kind, lambda v: Setup(config={config_id: v}), values, description)


def in_files(kind: str = Lever.TEXT, values=None, description: str = "") -> Lever:
    """A setting the spec's `files` hook writes into the agent's config file."""
    return Lever(kind, lambda v: Setup(), values, description)


class Script(str):
    """A file of `AcpSpec.files` written runnable (a command the agent runs)."""


def deep_merge(base: dict, more: dict) -> dict:
    out = dict(base)
    for key, value in more.items():
        out[key] = deep_merge(out[key], value) if isinstance(value, dict) and isinstance(out.get(key), dict) else value
    return out


# The machine's own login, as a key of `AcpSpec.auth_methods` beside the
# login ways of agents.json.
MACHINE = "machine"


class Sibling:
    """A form field that carries the answer the person types to the question
    of another field, named after it (`<question><suffix>`): marked by
    `marker` in its `_meta` when the agent marks it, else by its name alone."""

    def __init__(self, suffix: str, marker: str = None):
        self.suffix = suffix
        self.marker = marker

    def owner(self, key: str, field: dict, fields: dict):
        """The question `key` carries the typed answer of, or None when it is a question itself."""
        named = key[:-len(self.suffix)] if key.endswith(self.suffix) else None
        if self.marker is None:
            return named if named in fields else None
        if not (field.get("_meta") or {}).get(self.marker):
            return None
        if named not in fields:
            raise Refused(f"the agent's form has a field of the person's own answer, {key!r}, for no question it asks", 500)
        return named


class Form:
    """How an agent's `elicitation/create` forms read: one reader for every
    agent, these two facts being what differs between them.

    `message` is what the form's message is: "question" (the question of a
    form that asks one, else nothing of its own), "header" (the header of
    every question, each field's title its question), or "lines" (one line
    per question, each field's title its header).

    `own_answer` is where the person's typed answer to a question with
    options goes: None (nowhere: only a question with no options takes
    text), ANY_TEXT (its own field takes any text), or a `Sibling`."""

    def __init__(self, message: str = "question", own_answer=None):
        if message not in ("question", "header", "lines"):
            raise SystemExit(f"adapter_acp: a form's message is a question, a header or lines, and got {message!r}")
        self.message = message
        self.own_answer = own_answer


# `Form.own_answer` for an agent that takes any text as an answer to every question.
ANY_TEXT = "any_text"


class ProgramTool:
    """Where an agent's `tool_call` names the tool it calls: the text at `at`
    (keys into the update) matches `pattern`, `{server}` standing for the MCP
    server's name, and its group `name` is the tool's bare name
    (`mcp__weft__lookup`, `weft: lookup`, `lookup (weft MCP Server)`, or the
    bare `lookup`). Where the agent's update says the route a call took (an
    extension or server field), `at` points there. A call whose name read
    here is exactly one of the program's is the agent's own echo of a call
    that went through the program's route, which writes it into the history
    (`Conversation.block`); a name the pattern does not read, or one the
    program has no tool of, is one of the agent's own tools."""

    def __init__(self, pattern: str, at=("title",)):
        self.pattern = re.compile(pattern.format(server=re.escape(adapters.MCP_SERVER)), re.DOTALL)
        self.at = tuple(at)

    def read(self, update: dict, names) -> str:
        """The bare name of the program's tool this call is, or None."""
        found = re.fullmatch(self.pattern, str(dig(update, self.at) or ""))
        if found is None or found["name"] not in names:
            return None
        return found["name"]


def dig(value, keys: tuple):
    for key in keys:
        value = value.get(key) if isinstance(value, dict) else None
    return value


class AcpSpec:
    """What differs between two ACP agents. Its facts (label, logins,
    permission settings, home) are its entry in agents.json."""

    def __init__(self, kind: str, command: list, *, permissions: dict, model: Lever = None, options: dict = None,
                 env: dict = None, unset_env=(), home_unset=(), initialize: dict = None, logins: dict = None,
                 auth_methods: dict = None, settings_refusal=None, agent_refusal=None, resume: str = "session/load",
                 fixed_permission: bool = False, mcp="session", mcp_refusal: str = None,
                 requests: dict = None, permission_hook=None, form: Form = None, program_tool: ProgramTool = None,
                 files=None, reject_ends_turn: bool = False, tools_setup=None, session_check=None,
                 stop=None, turn_failure=None, machine_login: str = None, own_home=None, subcommand=(), home_env=(),
                 call_check=None):
        self.kind = kind
        # The command line, before any setting's flags: the agent's program
        # (on PATH, or the person's own build in its place), then its words.
        self.command = list(command)
        # The words after every setting's flags: the subcommand of a CLI
        # that reads its flags only before it (`agent --force acp`).
        self.subcommand = list(subcommand)
        # weft's permission setting -> its Setup.
        self.permissions = permissions
        # How the node's `model` reaches the agent, when it takes one.
        self.model = model
        # `config.options` name (camelCase) -> its Lever.
        self.options = options or {}
        # Variables always set (telemetry off, ...), and always taken out.
        self.env = env or {}
        self.unset_env = list(unset_env)
        # Variables taken out only in a private home, so the folders they
        # name follow it (`XDG_*` under a home that is `HOME`); on the
        # machine's own login they stay the person's own.
        self.home_unset = list(home_unset)
        # Merged into `initialize`'s params: the client capabilities (and
        # `_meta`) an agent needs to hear before it asks with a form or an
        # extension request.
        self.initialize = initialize or {}
        # Login way -> the Setup a connection's login of that way adds
        # (`--no-auto-login`, ...), beside the variables agents.json names.
        self.logins = logins or {}
        # Login way (or MACHINE) -> the `authenticate` method it sends after
        # `initialize`; a login not named sends none.
        self.auth_methods = auth_methods or {}
        # adapter -> why its settings cannot run (str), or None: checked with
        # the other refusals of `settings`, before anything starts.
        self.settings_refusal = settings_refusal
        # adapter -> why this agent cannot be driven (str), or None: checked
        # on every start once `initialize` has said what it is, before
        # `authenticate`.
        self.agent_refusal = agent_refusal
        # How a conversation is picked up again: "session/load" (replays
        # it), "session/resume" (does not), or None when it cannot be.
        self.resume = resume
        # Whether a conversation keeps the permission setting it started
        # with whatever the picked-up agent is told (its pick-up ignores
        # it): another one is then refused rather than run under the old.
        self.fixed_permission = fixed_permission
        # How the program's MCP server reaches the agent: "session" (in
        # `session/new`, over HTTP when the agent says it takes it, else as
        # a command), "stdio" (in `session/new`, always as a command), or
        # adapter -> Setup (a flag, a variable) for an agent that ignores
        # the servers `session/new` hands it.
        self.mcp = mcp
        # Why the agent cannot take the program's tools at all, when it cannot.
        self.mcp_refusal = mcp_refusal
        # Agent -> client request method -> handler(adapter, turn, rid, params).
        self.requests = requests or {}
        # (adapter, rid, params) -> (question, result_of) for a permission
        # request that is really a question, else None.
        self.permission_hook = permission_hook
        # How its `elicitation/create` forms read.
        self.form = form or Form()
        # Where its `tool_call` names the tool it calls, so its echo of a call
        # of the program's tool is left to the program's route, which keeps
        # it in the history under the tool's bare name; None for an agent
        # that never shows one as a call of its own (its code calls the
        # program's tools).
        self.program_tool = program_tool
        # adapter -> the Setup the program's tools add, whichever way the MCP
        # server reaches the agent (`mcp`): a session `_meta` that makes the
        # server's tools tools of the model's own, and the like.
        self.tools_setup = tools_setup
        # adapter -> {path: content}: the files written before every start,
        # in its private home, or with none (the machine's own login) in a
        # folder of weft's own (`Acp.files_dir`), each private to this user;
        # a content that is a `Script` is written runnable. A hook that
        # writes only for a connection's login returns {} without one.
        self.files = files
        # adapter -> why the agent cannot go on with this session (str), or
        # None: asked once the session is made or picked up and set, before
        # the first prompt (a plugin it was to load and did not, a model it
        # put another in place of; `Acp.opened` is what the session opened with).
        self.session_check = session_check
        # Whether a refused permission ends the agent's whole turn.
        self.reject_ends_turn = reject_ends_turn
        # adapter -> None, after the agent's process is stopped: what it
        # started that outlives it (a daemon).
        self.stop = stop
        # (adapter, reply) -> why the turn failed, or None: for an agent that
        # says a failure (no login, no connection) as its reply and ends the
        # turn as if it went well.
        self.turn_failure = turn_failure
        # Why the agent cannot run on the machine's own login (no connection
        # on `access`), when it cannot: what it would borrow or share there.
        self.machine_login = machine_login
        # adapter -> Setup, for an agent whose settings, sessions and login
        # live in a folder its command line names: that folder is a home of
        # weft's own on every login, the machine's own one too (`files` are
        # written there), and this points the agent at it, and on the
        # machine's own login at that login where it is.
        self.own_home = own_home
        # Variables pointed at the private home besides its agents.json
        # `home`, for an agent that finds its login, its settings and its
        # sessions through more than one (each taken out on every login).
        self.home_env = list(home_env)
        # (adapter, turn, update) -> why the turn cannot go on (str), or None:
        # asked of every `tool_call` and every call's end before it is shown,
        # for an agent whose way of naming the program's tools rests on
        # something it does not promise to keep. A reason fails the turn and
        # stops the agent (`Acp.abandon`).
        self.call_check = call_check


def started_with(answer: dict, permission: str) -> dict:
    """What a new session begins with: the permission setting it was made
    under and, from `session/new`'s answer, its mode and the current value
    of each config option it reports."""
    answer = answer if isinstance(answer, dict) else {}
    return {"permission": permission, "mode": reported_mode(answer),
            "options": {o.get("id"): o.get("currentValue") for o in answer.get("configOptions") or [] if o.get("id")}}


def reported_mode(answer: dict):
    """The session's mode as a `session/new` or load answer reports it: how
    it is set (`config` and its config id, or `mode` for `session/set_mode`),
    its current value, and the values it takes; None when it reports none."""
    answer = answer if isinstance(answer, dict) else {}
    for option in answer.get("configOptions") or []:
        if option.get("category") == "mode":
            return {"by": "config", "id": option.get("id"), "value": option.get("currentValue"),
                    "offered": [one.get("value") for one in option.get("options") or []]}
    modes = answer.get("modes")
    if modes:
        return {"by": "mode", "id": None, "value": modes.get("currentModeId"),
                "offered": [one.get("id") for one in modes.get("availableModes") or []]}
    return None


def mcp_server(capabilities: dict, stdio: bool = False) -> dict:
    """The program's tools as `session/new` hands an MCP server over."""
    if not stdio and (capabilities.get("mcpCapabilities") or {}).get("http"):
        return {"type": "http", "name": adapters.MCP_SERVER, "url": adapters.mcp_url(), "headers": []}
    return mcp_command()


def mcp_command() -> dict:
    """The program's tools as a command the agent starts: this server's `mcp-bridge`."""
    return {"name": adapters.MCP_SERVER, "command": sys.executable, "args": [adapters.SERVER_SCRIPT, "mcp-bridge", adapters.mcp_url()],
            "env": []}


def choice_kind(kind: str) -> str:
    if kind == "allow_always":
        return ev.ALLOW_ALWAYS
    if kind == "allow_once":
        return ev.ALLOW_ONCE
    return ev.REJECT


def permission_question(request_id: str, params: dict) -> dict:
    call = params.get("toolCall") or {}
    choices = [ev.choice(o.get("optionId", ""), o.get("name") or o.get("optionId", ""), choice_kind(o.get("kind", "")))
               for o in params.get("options") or []]
    title = call.get("title") or "The agent wants to use a tool"
    return ev.permission(request_id, call.get("kind") or "tool", title, describe(call), call.get("rawInput"), choices)


def describe(call: dict) -> str:
    parts = []
    for location in call.get("locations") or []:
        if location.get("path"):
            parts.append(location["path"])
    raw = call.get("rawInput")
    if isinstance(raw, dict):
        for key in ("command", "cmd", "file_path", "path", "url"):
            if isinstance(raw.get(key), str):
                parts.append(raw[key])
                break
    if parts:
        return "\n".join(parts)
    if raw is not None:
        return json.dumps(raw)[:500]
    return call.get("title", "")


def permission_answer(question: dict, answer: dict) -> dict:
    picked = ev.chosen(answer, question)
    return {"outcome": {"outcome": "selected", "optionId": picked["id"]}}


def form_question(question_id: str, params: dict, form: Form):
    """An `elicitation/create` form as weft's question, and the result an
    answer to it becomes: one question per field of the form, a field that
    carries another's typed answer (`Form.own_answer`) folded into it."""
    schema = params.get("requestedSchema") or {}
    fields = schema.get("properties") or {}
    message = params.get("message") or ""
    siblings = {}
    if isinstance(form.own_answer, Sibling):
        for key, field in fields.items():
            owner = form.own_answer.owner(key, field, fields)
            if owner is not None:
                siblings[owner] = key
    questions = {key: field for key, field in fields.items() if key not in siblings.values()}
    lines = message.split("\n") if form.message == "lines" and len(questions) > 1 else None
    if lines is not None and len(lines) != len(questions):
        # A question text with a line break in it, so which line is whose
        # cannot be told: the whole message leads, each field keeps its title.
        lines = [message] + [""] * (len(questions) - 1)
    asked, readers = [], {}
    for index, (key, field) in enumerate(questions.items()):
        multi = field.get("type") == "array"
        source = (field.get("items") or {}) if multi else field
        options, values = form_choices(source)
        header = field.get("title") or ""
        if form.message == "header":
            text, header = field.get("description") or header or key, message
        elif lines is not None:
            text = lines[index] or header
        else:
            text = field.get("description") or (message if len(questions) == 1 else header or key)
        takes_text = not options or form.own_answer == ANY_TEXT or key in siblings
        asked.append(ev.asked(key, "" if header == text else header, text, options, multi, takes_text))
        readers[key] = (multi, values, source.get("type"), siblings.get(key))
    if not asked:
        # A form with nothing to fill in: the agent asks to go on.
        asked.append(ev.asked("", "", message, [{"label": "Continue", "description": ""}], False, False))
    question = ev.questions(question_id, asked)

    def result_of(answer: dict) -> dict:
        given = ev.answers_for(answer, question)
        content = {}
        for key, (multi, values, kind, sibling) in readers.items():
            if sibling is None:
                read = [typed(values.get(v, v), kind, key) for v in given[key]]
                content[key] = read if multi else read[0]
                continue
            picked = [values[v] for v in given[key] if v in values]
            own = [v for v in given[key] if v not in values]
            if len(own) > 1:
                raise Refused(f"question {key!r} takes one answer of your own besides its choices, and got {len(own)}", 400)
            # A field left empty is left out: the agent's form requires none of them.
            if picked:
                content[key] = picked if multi else picked[0]
            if own:
                content[sibling] = typed(own[0], fields[sibling].get("type"), sibling)
        return {"action": "accept", "content": content}

    return question, result_of


def form_choices(source: dict) -> tuple:
    """A form field's options (`oneOf`/`anyOf`, `enum`, or a boolean's yes
    and no), and the value each option's label stands for."""
    options, values = [], {}
    for one in source.get("oneOf") or source.get("anyOf") or []:
        text = one.get("title") or str(one.get("const"))
        options.append({"label": text, "description": one.get("description", "")})
        values[text] = one.get("const")
    for one in source.get("enum") or []:
        options.append({"label": str(one), "description": ""})
        values[str(one)] = one
    if source.get("type") == "boolean" and not options:
        options = [{"label": "Yes", "description": ""}, {"label": "No", "description": ""}]
        values = {"Yes": True, "No": False}
    return options, values


def typed(value, kind: str, key: str):
    """A text answer as the type the form's field declares."""
    if not isinstance(value, str):
        return value
    try:
        if kind == "integer":
            return int(value)
        if kind == "number":
            return float(value)
    except ValueError:
        raise Refused(f"the form's {key!r} takes a number, and got {value!r}", 400)
    return value


def tool_output(update: dict) -> str:
    parts = []
    for item in update.get("content") or []:
        if item.get("type") == "content":
            parts.append(adapters.text_of([item.get("content") or {}]))
        elif item.get("type") == "diff":
            parts.append(f"changed {item.get('path')}")
        elif item.get("type") == "terminal":
            parts.append(f"[terminal {item.get('terminalId')}]")
    if not parts and update.get("rawOutput") is not None:
        raw = update["rawOutput"]
        parts.append(raw if isinstance(raw, str) else json.dumps(raw))
    return "\n".join(parts)


class Acp(adapters.Adapter):
    spec = None

    def __init__(self, conversation, config, tools):
        super().__init__(conversation, config, tools)
        self.session = None
        self.capabilities = {}
        self.auth_methods = []
        # What the agent answered the session's `session/new` or pick-up
        # with (its modes, its config options and their current values).
        self.opened = {}
        # While a loaded conversation is replayed: when the last of it came.
        self.loading = False
        self.replayed_at = 0.0
        # The message being written, and the thought being thought, chunk by
        # chunk, until something else happens or the turn ends.
        self.writing = []
        self.thinking = []
        self.reply = []
        # What the person refused in this turn, for an agent that ends its
        # turn at a refusal.
        self.refused = None
        # The tool calls the turn has shown, by id: what the agent's
        # `tool_call` said of each (a permission request may name the call
        # by its id alone).
        self.shown = {}
        # The message the turn goes on with once the agent's prompt returns
        # (`follow_up`), when a spec queued one.
        self.follow_up_message = None
        if self.spec.own_home is not None and self.home is None:
            self.home = os.path.join(self.conv.home, f"{self.kind}-home")
            os.makedirs(self.home, exist_ok=True)
        self.setup = self.settings()

    def settings(self) -> Setup:
        """The Setup of the permission setting, the model and every option."""
        setup = self.spec.permissions[self.permission]
        if self.config.get("model"):
            if self.spec.model is None:
                raise Refused(f"{self.label} takes no model setting; leave `model` empty", 400)
            setup = setup.add(self.spec.model.setup(self.label, "model", self.config["model"]))
        for name, value in self.options.items():
            if value is not None:
                setup = setup.add(self.spec.options[name].setup(self.label, name, value))
        if self.tools and self.spec.mcp_refusal:
            raise Refused(f"{self.label} cannot run the program's tools: {self.spec.mcp_refusal}; "
                          "wire no tools to this agent", 400)
        if self.tools and callable(self.spec.mcp):
            setup = setup.add(self.spec.mcp(self))
        if self.tools and self.spec.tools_setup is not None:
            setup = setup.add(self.spec.tools_setup(self))
        if self.spec.own_home is not None:
            setup = setup.add(self.spec.own_home(self))
        if self.credentials is None and self.spec.machine_login:
            raise Refused(f"{self.label} runs only on a connection's login: {self.spec.machine_login}; "
                          "wire an access node to `access`", 400)
        if self.credentials is not None and self.credentials.get("way") in self.spec.logins:
            setup = setup.add(self.spec.logins[self.credentials["way"]])
        if self.spec.settings_refusal is not None:
            refusal = self.spec.settings_refusal(self)
            if refusal is not None:
                raise Refused(refusal, 400)
        return setup

    def prepare(self) -> None:
        if self.spec.files is not None:
            for path, content in self.spec.files(self).items():
                # Private: a config file may carry a token minted for the start.
                adapters.write_whole(os.path.join(self.files_dir(), path), str(content), 0o700 if isinstance(content, Script) else 0o600)

    def files_dir(self) -> str:
        """Where `AcpSpec.files` are written: the private home, else a folder
        of weft's own in this conversation (the machine's own login has no
        private home, and its files are never written into the person's)."""
        return self.home if self.home is not None else os.path.join(self.conv.home, f"{self.kind}-files")

    def abandon(self, why: str, status: str = None) -> None:
        """Fail the turn under way with `why` and stop the agent, which would
        go on with it: the next turn starts it again and picks the
        conversation up."""
        turn = self.turn
        if turn is not None and turn.status not in ev.ENDED:
            self.conv.fail(turn, why, status=status)
        proc = self.proc
        if proc is not None and proc.poll() is None:
            adapters.signal_group(proc, signal.SIGTERM)

    def command(self) -> list:
        return [self.program(), *self.spec.command[1:], *self.setup.args, *self.spec.subcommand]

    def credential_env(self, env: dict) -> None:
        for name in self.spec.unset_env + self.spec.home_env:
            env.pop(name, None)
        if self.home is not None:
            for name in self.spec.home_unset:
                env.pop(name, None)
            env.update({name: self.home for name in self.spec.home_env})
        env.update(self.spec.env)
        env.update(self.setup.env)
        # An agent in a home of weft's own is pointed at it by its command
        # line (`own_home`), never by a variable.
        adapters.login_env(self.kind, env, self.credentials, None if self.spec.own_home is not None else self.home)

    def stop_rest(self) -> None:
        if self.spec.stop is not None:
            self.spec.stop(self)

    # ----- JSON-RPC ----------------------------------------------------------

    def request(self, method: str, params: dict, timeout=adapters.RPC_TIMEOUT_SECS) -> dict:
        rid = self.next_request_id()
        return self.call(rid, {"jsonrpc": "2.0", "id": rid, "method": method, "params": params}, method, timeout)

    def handshake(self) -> None:
        # No `fs` and no `terminal`: an agent told this side has them hands
        # it its file writes and its commands, which weft does not run.
        params = deep_merge({"protocolVersion": PROTOCOL_VERSION,
                             "clientCapabilities": {"fs": {"readTextFile": False, "writeTextFile": False}, "terminal": False},
                             "clientInfo": {"name": "weft", "version": "1"}}, self.spec.initialize)
        answer = self.request("initialize", params)
        self.capabilities = answer.get("agentCapabilities") or {}
        self.auth_methods = answer.get("authMethods") or []
        if self.spec.agent_refusal is not None:
            refusal = self.spec.agent_refusal(self)
            if refusal is not None:
                raise Refused(refusal, 400)
        method = self.spec.auth_methods.get(MACHINE if self.credentials is None else self.credentials["way"])
        if method is not None:
            self.request("authenticate", {"methodId": method})
        servers = [mcp_server(self.capabilities, self.spec.mcp == "stdio")] if self.tools and isinstance(self.spec.mcp, str) else []
        conversation = self.conv.conversation_id()
        started = None
        if conversation and self.spec.resume is not None:
            started = self.started()
            answer = self.pick_up(conversation, servers)
        else:
            params = deep_merge({"cwd": self.config["folder"], "mcpServers": servers}, self.setup.session)
            answer = self.request("session/new", params)
            self.session = answer["sessionId"]
            # What it starts with, before any setting changes it.
            self.conv.set_conversation(self.session, {"started": started_with(answer, self.permission)})
        self.opened = answer if isinstance(answer, dict) else {}
        config, mode = self.modes(answer, started)
        # Set again after a load: not every agent keeps them with the conversation.
        for config_id, value in config.items():
            self.request("session/set_config_option", {"sessionId": self.session, "configId": config_id, "value": value})
        if mode is not None:
            self.request("session/set_mode", {"sessionId": self.session, "modeId": mode})
        if self.spec.session_check is not None:
            refusal = self.spec.session_check(self)
            if refusal is not None:
                raise Refused(refusal)

    def started(self) -> dict:
        """What the conversation about to be picked up began with (kept by
        `session/new`), refused when these settings cannot carry it on."""
        kept = self.conv.conversation_kept()
        # A conversation kept before weft kept what it began with, or before
        # it kept the permission setting an agent that fixes it began under.
        if "started" not in kept or (self.spec.fixed_permission and "permission" not in kept["started"]):
            raise Refused(f"this conversation began before weft kept the settings {self.label} started it with, so they "
                          "cannot be set back; start the conversation over from the node's card")
        started = kept["started"]
        if self.spec.fixed_permission and started["permission"] != self.permission:
            raise Refused(f"{self.label} keeps the permission setting a conversation started with ({started['permission']}), and "
                          f"this one asks for {self.permission}; set it back, or start the conversation over from the node's card", 400)
        return started

    def pick_up(self, conversation: str, servers: list) -> dict:
        """The conversation again, after a restart: loaded (and its replay
        drained), or resumed."""
        offered = (self.capabilities.get("loadSession") if self.spec.resume == "session/load"
                   else "resume" in (self.capabilities.get("sessionCapabilities") or {}))
        if not offered:
            raise Refused(f"this {self.label} does not offer {self.spec.resume}, so it cannot pick the conversation up again; "
                          "update it, or start the conversation over from the node's card")
        params = deep_merge({"sessionId": conversation, "cwd": self.config["folder"], "mcpServers": servers}, self.setup.session)
        self.loading = True
        self.replayed_at = time.monotonic()
        try:
            answer = self.request(self.spec.resume, params)
            quiet = threading.Event()
            while True:
                left = REPLAY_QUIET_SECS - (time.monotonic() - self.replayed_at)
                if left <= 0:
                    break
                quiet.wait(left)
        except Refused as refused:
            raise Refused(f"{refused}; the conversation cannot be picked up again, so start it over from the node's card")
        finally:
            self.loading = False
        self.session = conversation
        return answer

    def modes(self, answer: dict, started) -> tuple:
        """The config options and the mode to set: the settings', and after a
        load (`started` is what the conversation began with) every option and
        the mode no setting names, set back to how it began wherever the
        loaded conversation has it otherwise (a setting cleared since, a mode
        a setting once picked); the mode refused when the agent does not
        offer it."""
        reported = reported_mode(answer)
        config, mode = dict(self.setup.config), self.setup.mode
        start_mode = started["mode"] if started is not None else None
        named = mode is not None or any(m is not None and m["id"] in config for m in (reported, start_mode))
        if started is not None:
            now = {o.get("id"): o.get("currentValue") for o in (answer or {}).get("configOptions") or []}
            back = {key: value for key, value in started["options"].items()
                    if key not in config and key in now and now[key] != value
                    and not (reported is not None and key == reported["id"])}
            config = {**back, **config}
            # Only where the loaded conversation reports another mode: an
            # agent that reports none after a load may offer no way to set it.
            moved = reported is not None and start_mode is not None and reported["value"] != start_mode["value"]
            if moved and not named:
                if start_mode["by"] == "config":
                    config = {start_mode["id"]: start_mode["value"], **config}
                else:
                    mode = start_mode["value"]
        if reported is not None:
            wanted = mode if reported["by"] == "mode" else config.get(reported["id"])
            if wanted is not None and wanted not in reported["offered"]:
                offered = ", ".join(f"`{m}`" for m in reported["offered"]) or "none"
                if named:
                    raise Refused(f"{self.label} has no `{wanted}` mode here, which the `{self.permission}` permission setting "
                                  f"runs it in: {self.label}'s own config turns it off (the modes it offers: {offered}); "
                                  "turn it back on there, or pick another permission setting", 400)
                raise Refused(f"{self.label} has no `{wanted}` mode any more, the mode this conversation started in: "
                              f"{self.label}'s own config turns it off now (the modes it offers: {offered}); turn it back on "
                              "there, or start the conversation over from the node's card", 400)
        return config, mode

    def begin(self, turn) -> None:
        self.writing, self.thinking, self.reply, self.refused, self.follow_up_message = [], [], [], None, None
        self.shown = {}
        threading.Thread(target=self._prompt, args=(turn,), daemon=True).start()

    def follow_up(self, message: str) -> None:
        """Have the turn go on with `message` once the agent's prompt returns:
        sent as a further `session/prompt` in the same weft turn, for an
        agent that ends its prompt to wait for what a person answered."""
        if self.follow_up_message is not None:
            raise Refused(f"{self.label} asked twice in one prompt for a message to go on with", 500)
        self.follow_up_message = message

    def _prompt(self, turn) -> None:
        message = turn.message
        while True:
            try:
                answer = self.request("session/prompt", {"sessionId": self.session, "prompt": [{"type": "text", "text": message}]},
                                      timeout=None)
            except Refused as refused:
                self._flush(turn)
                # An agent that says why in its reply before it fails the prompt
                # (a refused key) has the reason there, and only a code in the error.
                said = "\n\n".join(self.reply).strip()
                self.conv.fail(turn, f"{said} ({refused})" if said else str(refused))
                return
            self._flush(turn)
            message, self.follow_up_message = self.follow_up_message, None
            # A prompt that did not end as it should ends the turn, and what
            # it queued with it.
            if message is None or answer.get("stopReason") not in FINISHED:
                break
        reason = answer.get("stopReason")
        reply = "\n\n".join(self.reply)
        failure = self.spec.turn_failure(self, reply) if self.spec.turn_failure is not None else None
        if self.refused is not None and self.spec.reject_ends_turn:
            self.conv.fail(turn, f"{self.label} ended the turn when {self.refused} was refused: it stops its whole turn "
                                 "at a refusal rather than going on without it. Send a message saying what to do instead.")
        elif failure is not None:
            self.conv.fail(turn, failure)
        elif reason in FINISHED:
            self.conv.finish(turn, reply)
        else:
            self.conv.fail(turn, f"{self.label} stopped the turn: {reason}")

    # ----- what the agent writes -------------------------------------------

    def on_message(self, message: dict) -> None:
        if "method" not in message:
            self.answered(message)
            return
        method, params, turn = message["method"], message.get("params") or {}, self.turn
        if "id" in message:
            self._agent_request(turn, message["id"], method, params)
        elif method == "session/update":
            if self.loading:
                self.replayed_at = time.monotonic()
            elif turn is not None:
                self._update(turn, params.get("update") or {})

    def _update(self, turn, update: dict) -> None:
        kind = update.get("sessionUpdate")
        if kind == "agent_message_chunk":
            text = adapters.text_of([update.get("content") or {}])
            if not text:
                # Some agents carry usage or lifecycle marks on an empty chunk.
                return
            self._flush_thinking(turn)
            self.writing.append(text)
            if self.config.get("streamBy") == "token":
                self.conv.emit(turn, ev.TEXT_DELTA, text=text)
            return
        if kind == "agent_thought_chunk":
            # One thinking event per thought, however finely the agent
            # streams it (goose and CodeBuddy send a chunk per token).
            text = adapters.text_of([update.get("content") or {}])
            if text:
                self._flush_text(turn)
                self.thinking.append(text)
            return
        ended = kind == "tool_call_update" and update.get("status") in ("completed", "failed")
        if kind != "tool_call" and not ended:
            return
        self._flush(turn)
        call_id = update.get("toolCallId")
        if ended and call_id not in self.shown and not (update.get("title") or update.get("kind")):
            # The end of something never shown as a call, naming nothing
            # (Hermes ends its permission check so): no call of the agent's.
            return
        if self.spec.call_check is not None:
            why = self.spec.call_check(self, turn, update)
            if why is not None:
                self.abandon(why)
                return
        if call_id in self.shown:
            if kind == "tool_call":
                # The same call again (CodeBuddy sends a second `tool_call`
                # once its arguments are complete): an update of the one
                # already shown, never a call of its own.
                self.shown[call_id] = {**self.shown[call_id], **update}
                return
        else:
            # A call shown first by its end (Kiro's `tool_load`) is shown then.
            self._show_call(turn, update)
        if ended:
            output = tool_output(update)
            self.conv.emit(turn, ev.TOOL_RESULT, id=update.get("toolCallId"), output=output, isError=update.get("status") == "failed")
            self.remember_tool_result(update.get("toolCallId"), output)
            for item in update.get("content") or []:
                if item.get("type") == "diff":
                    self.conv.emit(turn, ev.FILE_CHANGE, path=item.get("path"), change="update" if item.get("oldText") else "add")

    def _show_call(self, turn, update: dict) -> None:
        """A call the agent shows. A call of one of the program's tools shows
        here too, as the agent's tool use as well as the program's
        `tool_call`: the route wrote it into the history, so only the
        agent's own tools are written here (its result is skipped with it)."""
        call_id = update.get("toolCallId")
        self.shown[call_id] = update
        name = update.get("title") or update.get("kind") or "tool"
        self.conv.emit(turn, ev.TOOL_USE, id=call_id, name=name, input=update.get("rawInput") or {})
        if self.program_tool_of(update) is not None:
            self.program_calls.add(call_id)
        else:
            self.remember_tool_use(call_id, name, update.get("rawInput") or {})

    def program_tool_of(self, update: dict):
        """The program's tool a `tool_call` the agent showed is a call of (its
        bare name), or None for one of the agent's own."""
        if self.spec.program_tool is None:
            return None
        return self.spec.program_tool.read(update, self.program_tools.names)

    def program_tool(self, shown_name):
        """Never by a title alone: an ACP agent names a call in a field of
        its own (`AcpSpec.program_tool`), and `_update` reads that one. A
        title that happens to be a program tool's name (a built-in the
        agent titles by its bare name) is still the agent's own."""
        return None

    def _flush(self, turn) -> None:
        self._flush_thinking(turn)
        self._flush_text(turn)

    def _flush_thinking(self, turn) -> None:
        if self.thinking:
            text = "".join(self.thinking)
            self.thinking = []
            if text.strip():
                self.conv.emit(turn, ev.THINKING, text=text)

    def _flush_text(self, turn) -> None:
        if self.writing:
            text = "".join(self.writing)
            self.writing = []
            if text.strip():
                self.reply.append(text)
                self.conv.emit(turn, ev.TEXT, text=text)
                self.remember_text(text)

    # ----- what the agent asks -------------------------------------------------

    def _agent_request(self, turn, rid, method: str, params: dict) -> None:
        handler = self.spec.requests.get(method) or BUILT_IN.get(method)
        if handler is None or turn is None or turn.status in ev.ENDED:
            self.send({"jsonrpc": "2.0", "id": rid, "error": {"code": -32601, "message": f"weft does not answer {method}"}})
            return
        self._flush(turn)
        handler(self, turn, rid, params)

    def ask(self, turn, rid, question: dict, result_of) -> None:
        """Ask weft `question`; its answer goes back to the agent as `result_of(answer)`."""
        self.block(turn, ev.QUESTION, question["id"], {"question": question},
                   lambda answer: self.send({"jsonrpc": "2.0", "id": rid, "result": result_of(answer)}),
                   lambda why: self.refuse(rid, why))

    def refuse(self, rid, why: str) -> None:
        """Answer the agent's request `rid` with an error saying `why`."""
        self.send({"jsonrpc": "2.0", "id": rid, "error": {"code": -32000, "message": why}})

    def question_id(self, rid) -> str:
        return f"{self.kind}-{rid}"


def answer_permission(adapter: Acp, turn, rid, params: dict) -> None:
    params = with_shown_call(adapter, params)
    if adapter.spec.permission_hook is not None:
        asked = adapter.spec.permission_hook(adapter, rid, params)
        if asked is not None:
            adapter.ask(turn, rid, *asked)
            return
    question = permission_question(adapter.question_id(rid), params)

    def result_of(answer: dict) -> dict:
        result = permission_answer(question, answer)
        if ev.chosen(answer, question)["kind"] == ev.REJECT:
            adapter.refused = f"`{question['permission']['title']}`"
        return result

    adapter.ask(turn, rid, question, result_of)


def with_shown_call(adapter: Acp, params: dict) -> dict:
    """A permission request whose tool call is filled in from the `tool_call`
    the agent showed under its id (its title, kind, input): Devin, DeepSeek
    Harness and CodeBuddy name the call there by its id alone. What the
    request itself says wins."""
    call = params.get("toolCall") or {}
    shown = {k: v for k, v in (adapter.shown.get(call.get("toolCallId")) or {}).items() if k != "sessionUpdate"}
    return {**params, "toolCall": {**shown, **call}}


def answer_form(adapter: Acp, turn, rid, params: dict) -> None:
    adapter.ask(turn, rid, *form_question(adapter.question_id(rid), params, adapter.spec.form))


BUILT_IN = {"session/request_permission": answer_permission, "elicitation/create": answer_form}


def register(spec: AcpSpec) -> None:
    """Make `spec` an agent kind, checked against its agents.json entry."""
    facts = adapters.AGENTS.get(spec.kind) or {}
    ways = set(facts.get("logins") or {})
    for field, named in (("logins", set(spec.logins)), ("auth_methods", set(spec.auth_methods) - {MACHINE})):
        if not named <= ways:
            raise SystemExit(f"adapter_acp: {spec.kind}'s {field} names {sorted(named - ways)}, which are no login ways of its "
                             f"agents.json entry ({sorted(ways)})")
    adapters.register(type(f"Acp_{spec.kind}", (Acp,), {"spec": spec, "kind": spec.kind, "program_name": spec.command[0],
                                                         "permissions": spec.permissions, "option_names": tuple(spec.options)}))
