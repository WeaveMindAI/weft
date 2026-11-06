"""What every adapter shares: the agent's process, and the registry of kinds.

An adapter owns one agent process for the conversation. It turns a weft
turn into the agent's own protocol and every line the agent writes back into
the events of `events.py`. The process starts on the first turn and stays up
between turns; it is replaced between turns when the configuration or the
program's tools change, or after a turn that did not end done, and the new
one resumes the same conversation.

An adapter never stops itself: it goes through `Conversation.put_down`,
which says a stop that fails.

An agent never renews a login weft handed it: a login whose renewal rotates
(each renewal retires the one before) is renewed by weft alone, once for
every node holding it. An adapter whose agent takes its login over its
protocol takes a new one while it runs (`take_login`, from `/login`, or a
configure that changes only the login), and asks weft for one when the
agent needs it (`want_login`: a `login` event the agent node hears on its
`stream`).
"""

import collections
import json
import os
import shutil
import signal
import subprocess
import sys
import threading
import time

import events as ev
from events import Refused


class Retired(Refused):
    """`ensure_started` met an adapter stopped for good. `on_its_own`: its
    agent ended on its own and its reader put it down, which is no failure
    of the turn (the runner starts it on a new adapter); otherwise a reset
    or a configure retired it, and the turn fails with it."""

    def __init__(self, message: str, on_its_own: bool):
        super().__init__(message)
        self.on_its_own = on_its_own

# Where the program's own tools are served, as `adapters.SERVER_URL + MCP_PATH`.
# Set by the server once it listens.
SERVER_URL = None
# SYNC: MCP_PATH <-> catalog/agents/server/agent_server.py (MCP_PATH)
MCP_PATH = "/mcp"
# The name the program's tools are served under: an agent that cannot show
# them by their plain names shows them as tools of this server
# (`mcp__weft__<tool>` in Muse Code).
MCP_SERVER = "weft"

# Where an agent's `installHint` (agents.json) starts saying how to install
# it by hand: what a turn whose program is missing hands on, after saying
# weft installs it when its infra starts.
BY_HAND = "To install it"
# The program that runs this server, for an agent that starts MCP servers
# as commands (`mcp-bridge`).
SERVER_SCRIPT = os.path.join(os.path.dirname(os.path.abspath(__file__)), "agent_server.py")
# What runs a process for as long as the agent and this server, for an
# agent process that reads nothing on its standard input and so would
# outlive a server that died (`tether.py`'s doc).
TETHER = os.path.join(os.path.dirname(os.path.abspath(__file__)), "tether.py")

# Why an adapter was retired (`Adapter.retired`).
RETIRED_ON_ITS_OWN = "on_its_own"
RETIRED_BY_WEFT = "by_weft"

# How long a stopping agent gets to end on its own before it is killed.
STOP_GRACE_SECS = 5
# How long a request of ours to the agent may take before the turn fails.
RPC_TIMEOUT_SECS = 120


def log(message: str) -> None:
    print(f"agent-server: {message}", file=sys.stderr, flush=True)


def mcp_url() -> str:
    """weft's MCP server: the program's route for every agent without a
    tool callback of its own (Codex's `item/tool/call` is one), whatever
    carries the call there (the agent's MCP client, a plugin inside the
    agent, an SDK channel). Its contract is at the top of `agent_server.Mcp`."""
    return SERVER_URL + MCP_PATH


def mcp_tool(tool: dict) -> dict:
    """A program tool (`LlmTool`: name, description, parameters) as MCP lists it."""
    return {"name": tool["name"], "description": tool.get("description") or "",
            "inputSchema": tool.get("parameters") or {"type": "object", "properties": {}}}


def mcp_result(result: dict) -> dict:
    """What weft answered for a tool call, as MCP's `tools/call` result:
    always the text, and, when the text is JSON, that JSON as
    `structuredContent` too, which MCP allows only as an object: any other
    JSON value (a bare `42`) goes in as `{"result": 42}`. Never left out
    for one: Gemini CLI parses a text answer with no `structuredContent`
    into that field itself, and then fails the call on a value that is no
    object."""
    text = str(result.get("output", ""))
    answer = {"content": [{"type": "text", "text": text}], "isError": bool(result.get("isError"))}
    try:
        parsed = json.loads(text)
    except ValueError:
        return answer
    answer["structuredContent"] = parsed if isinstance(parsed, dict) else {"result": parsed}
    return answer


def signal_group(proc, sig) -> bool:
    """Send `sig` to the agent's process group: the agent and what it
    started. Started in a session of its own (`start_new_session`), the
    group is the agent's alone, never weft's. False when nothing is left in
    it."""
    try:
        os.killpg(proc.pid, sig)
    except ProcessLookupError:
        return False
    return True


def tools_signature(tools: list) -> str:
    return json.dumps(tools, sort_keys=True)


class ProgramTools:
    """The program's tools of one adapter, and the one test every adapter
    uses to tell a call of one of them from a call of the agent's own tools.

    A program's tool may share its name with one of the agent's built-in
    tools (the program's replaces it, where the agent can give it up), so no
    prefix proves anything: a call is the program's when it came in on the
    program's route (weft's MCP server, an agent's SDK callback, a plugin
    forwarding to that server), and, where the agent shows weft only the
    name, when that name is exactly one of the names the agent gives the
    program's tools. `shown(plain)` is that name for the plain name `plain`
    (the plain name itself for an agent that shows plain names, the agent's
    own spelling, `mcp__weft__lookup`, for one that cannot): the test is a
    look-up in that exact set, never a match on a part of the name."""

    def __init__(self, tools: list, shown=None):
        shown = shown or (lambda plain: plain)
        self.names = frozenset(t["name"] for t in tools)
        self.by_shown = {shown(name): name for name in self.names}

    def plain(self, shown_name):
        """The plain name of the program's tool the agent calls `shown_name`,
        or None when it is none of the program's tools."""
        return self.by_shown.get(shown_name)

    def __contains__(self, plain_name) -> bool:
        return plain_name in self.names


def label(kind: str) -> str:
    return AGENTS[kind]["label"] if kind in AGENTS else kind


# Every agent's facts, the one place they live: read here and by the weft
# nodes (`catalog/agents/agent.rs`, `include_str!`).
# SYNC: the shape of an entry <-> catalog/agents/agent.rs (AgentFacts, LoginFacts)
AGENTS_FILE = os.path.join(os.path.dirname(os.path.abspath(__file__)), "agents.json")
with open(AGENTS_FILE, encoding="utf-8") as _file:
    AGENTS = json.load(_file)

# Every agent this server drives, by the kind `/configure` names: each
# adapter module adds its own when imported (`register`).
KINDS = {}


def register(adapter: type) -> None:
    """Make `adapter` the one of its kind, checked against its agents.json entry."""
    facts = AGENTS.get(adapter.kind)
    if facts is None:
        raise SystemExit(f"adapters: agents.json has no entry for {adapter.kind!r}")
    if list(adapter.permissions) != facts["permissions"]:
        raise SystemExit(f"adapters: {adapter.kind}'s permission settings are {list(adapter.permissions)} in its adapter and "
                         f"{facts['permissions']} in agents.json")
    if adapter.kind in KINDS:
        raise SystemExit(f"adapters: two adapters register {adapter.kind!r}")
    KINDS[adapter.kind] = adapter


def program(config: dict, name: str) -> str:
    """The agent's program: `name` on PATH, or the person's own build of it
    in its place (the node's `program`, a path on their machine)."""
    own = config.get("program")
    return os.path.expanduser(own) if own else name


def write_whole(target: str, content: str, mode: int) -> None:
    """Write `target` whole, with `mode`: a temporary file of the same mode
    renamed over it, so no reader ever sees it half written."""
    os.makedirs(os.path.dirname(target) or ".", exist_ok=True)
    temp = target + ".weft-tmp"
    fd = os.open(temp, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, mode)
    # A temporary file left by an older write keeps its own mode.
    os.fchmod(fd, mode)
    with open(fd, "w", encoding="utf-8") as f:
        f.write(content)
    os.replace(temp, target)


def login_env(kind: str, env: dict, credentials, home) -> None:
    """The login `credentials` carries, into `env`: every variable any login
    of this agent sets is taken out first, so exactly the chosen one is
    there, and with a connection the agent's home variable points at `home`."""
    facts = AGENTS[kind]
    for way in facts["logins"].values():
        for variable in way["env"].values():
            env.pop(variable, None)
    if credentials is None:
        # The machine's own login, read from where the person keeps it: the
        # agent's home variable stays as it is (for some agents it is HOME).
        return
    way = login_way(kind, credentials)
    values = credentials.get("values") or {}
    for value, variable in way["env"].items():
        if not values.get(value):
            raise Refused(f"the connection's {value!r} is empty, and {facts['label']} needs it as {variable}; reconnect it", 400)
        env[variable] = values[value]
    if home is not None:
        env[facts["home"]] = home


def login_way(kind: str, credentials: dict) -> dict:
    facts = AGENTS[kind]
    way = facts["logins"].get(credentials.get("way"))
    if way is None:
        raise Refused(f"{facts['label']} has no login way {credentials.get('way')!r}; it takes {', '.join(facts['logins'])}", 400)
    return way


class Adapter:
    """One agent process, driven line by line over its stdin and stdout."""

    kind = ""
    # The agent's program, as it is called on PATH: what the person's own
    # build (the node's `program`) takes the place of.
    program_name = ""
    # weft's permission settings this adapter carries out, in agents.json's
    # order (a dict of what each one does to the agent, or a list).
    permissions = ()
    # The `config.options` it takes (camelCase, as the node names them):
    # any other is refused before anything starts.
    option_names = ()

    def __init__(self, conversation, config: dict, tools: list):
        self.conv = conversation
        self.config = config
        self.options = config.get("options") or {}
        self.tools = tools
        self.program_tools = ProgramTools(tools, self.shown_name)
        # The agent's ids of the program's tool calls its own stream showed
        # (`remember_tool_use`): their result is the route's to write too.
        self.program_calls = set()
        self.proc = None
        self.turn = None
        self.stderr = collections.deque(maxlen=40)
        self.write_lock = threading.Lock()
        # Who the agent's process is: held while one is swapped in
        # (`ensure_started`, never while it waits on the handshake), while a
        # stop marks it stopping and signals it (`stop`, never while it
        # waits for it to exit), and by the reader for each line it hands on
        # and at its process's end, while it checks that its process is
        # still the current one, not stopping, and acts on it. Without it, a
        # line or the end of a process just replaced or stopped could be
        # handled against the next turn, or the ended one. Nothing that
        # holds it waits on the reader, which takes it for every line and so
        # keeps draining a stopping agent's output. Reentrant: a start that
        # fails stops what it started, and a line may end in a stop.
        self.life = threading.RLock()
        self.stopping = False
        # Stopped for good by the conversation (`retire`): never started
        # again. None, else why: RETIRED_ON_ITS_OWN or RETIRED_BY_WEFT.
        self.retired = None
        # Our requests waiting on the agent's answer, by id (`call`). One
        # dict per process, swapped in with it: a request belongs to the
        # process it was sent to, and fails when that one exits, never when
        # another does.
        self.next_id = 0
        self.waiting = {}
        self.facts = AGENTS[self.kind]
        self.permission = config.get("permission") or "ask"
        if self.permission not in self.facts["permissions"]:
            raise Refused(f"{self.label} has no permission setting {self.permission!r}; it takes "
                          f"{', '.join(self.facts['permissions'])}", 400)
        unknown = sorted(set(self.options) - set(self.option_names))
        if unknown:
            raise Refused(f"{self.label} has no option {', '.join(unknown)}; it takes {', '.join(self.option_names) or 'none'}", 400)
        self.credentials = config.get("credentials")
        self.home = None
        if self.credentials is not None and self.facts.get("home"):
            # A connection's login lives in a home of its own, never in the
            # person's own one.
            self.home = os.path.join(self.conv.home, f"{self.kind}-home")
            os.makedirs(self.home, exist_ok=True)

    @property
    def label(self) -> str:
        return self.facts["label"]

    def program(self) -> str:
        """The agent's program this adapter starts: its own name on PATH, or
        the person's own build in its place."""
        return program(self.config, self.program_name)

    def check_program(self) -> None:
        """Refuse the turn, saying what to do, when the agent's program is not there to start."""
        name = self.program()
        if shutil.which(name) is not None:
            return
        if self.config.get("program"):
            raise Refused(f"the node's `program` is {name}, and nothing that runs is there; point it at your own build "
                          f"of {self.label}, or leave it empty to run the one on PATH, or the one weft installs")
        hint = self.facts["installHint"]
        raise Refused(f"`{name}` is not installed where the agent server runs (looked on PATH: "
                      f"{os.environ.get('PATH', '')}); start the node's infra again and weft installs {self.label} "
                      f"before the server starts, then run the turn again. {hint[hint.index(BY_HAND):]}")

    # ----- what subclasses write -------------------------------------------

    @staticmethod
    def shown_name(plain: str) -> str:
        """The name this agent gives the program's tool `plain`, as its own
        stream names the call (`ProgramTools`): the plain name, unless the
        agent cannot show one."""
        return plain

    def program_tool(self, shown_name):
        """The plain name of the program's tool a call the agent's stream
        names `shown_name` is, or None for one of the agent's own tools."""
        return self.program_tools.plain(shown_name)

    def command(self) -> list:
        raise NotImplementedError

    def credential_env(self, env: dict) -> None:
        """Put the configured login into `env`, and take out what would win over it."""
        login_env(self.kind, env, self.credentials, self.home)

    def on_message(self, message: dict) -> None:
        raise NotImplementedError

    def prepare(self) -> None:
        """Anything written before the agent starts (its config files)."""

    def handshake(self) -> None:
        """Anything the protocol needs before the first turn."""

    def begin(self, turn) -> None:
        raise NotImplementedError

    # ----- the process -------------------------------------------------------

    def serves(self, tools: list) -> bool:
        return tools_signature(tools) == tools_signature(self.tools)

    def env(self) -> dict:
        env = dict(os.environ)
        # The server's own variables are nothing the agent should read.
        for name in [n for n in env if n.startswith("WEFT_")]:
            env.pop(name)
        self.credential_env(env)
        return env

    def ensure_started(self) -> None:
        # A process that ended and whose end is not told yet (its watch still
        # waits for its output, held by a helper outside its group) is told
        # now, putting this adapter down: the turn starts on a new one, never
        # beside what is left of the old process.
        ended = self.proc
        if ended is not None and ended.poll() is not None:
            self._ended(ended, self.waiting)
        try:
            with self.life:
                # Before the look at the process: a retired one may still be
                # running out its stop's grace.
                if self.retired == RETIRED_ON_ITS_OWN:
                    raise Retired(f"{self.label} ended on its own as this turn started it", True)
                if self.retired is not None:
                    raise Retired(f"{self.label} was stopped for good while this turn started it (the conversation started "
                                  "over, or its configuration changed); send the message again", False)
                if self.proc is not None and self.proc.poll() is None:
                    return
                self.check_program()
                # Starting again after a stop: what runs from here on is wanted.
                self.stopping = False
                self.prepare()
                proc = subprocess.Popen(
                    self.command(), cwd=self.config["folder"], env=self.env(), stdin=subprocess.PIPE, stdout=subprocess.PIPE,
                    stderr=subprocess.PIPE, text=True, errors="replace", bufsize=1, start_new_session=True,
                )
                # Set by each reader once it has read its pipe to the end
                # (`stderr_tail` and `_watch` wait on them).
                proc.stdout_read, proc.stderr_read = threading.Event(), threading.Event()
                # Whether its end was told (`_ended`), which happens once.
                proc.ended_told = False
                with self.write_lock:
                    self.proc, self.waiting = proc, {}
                threading.Thread(target=self._read_out, args=(proc, self.waiting), name=f"{self.kind}-out", daemon=True).start()
                threading.Thread(target=self._read_err, args=(proc,), name=f"{self.kind}-err", daemon=True).start()
                threading.Thread(target=self._watch, args=(proc, self.waiting), name=f"{self.kind}-exit", daemon=True).start()
            # Outside `life`: the handshake waits on the reader, which takes
            # it if the agent exits meanwhile.
            self.handshake()
        except Retired:
            # Already stopped: nothing to put down.
            raise
        except BaseException as e:
            # An agent that never finished starting is stopped for good,
            # whatever it had done: left up, the next turn would find it
            # running and prompt it without the settings the handshake
            # applies. Through the conversation's one guarded stop, so the
            # turn fails with this error, and a stop that fails beside it is
            # said there too and recorded.
            self.conv.put_down(self, during=str(e) or type(e).__name__)
            raise

    def start_turn(self, turn) -> None:
        self.turn = turn
        self.begin(turn)

    def next_request_id(self) -> int:
        with self.write_lock:
            self.next_id += 1
            return self.next_id

    def call(self, rid, message: dict, method: str, timeout=RPC_TIMEOUT_SECS) -> dict:
        """Send our request `message` (id `rid`, asking `method`) and wait for
        the agent's answer to it, which `answered` hands over."""
        box = {"done": threading.Event()}
        waiting = self._write(message, (rid, box))
        if not box["done"].wait(timeout):
            waiting.pop(rid, None)
            raise Refused(f"{self.label} did not answer {method} within {timeout}s: {self.stderr_tail()}")
        if "error" in box:
            raise RpcRefused(self.label, method, box["error"])
        return box["result"]

    def answered(self, message: dict) -> None:
        """The agent's answer to one of our requests (`call`), by its `id`."""
        box = self.waiting.pop(message.get("id"), None)
        if box is None:
            return
        if "error" in message:
            box["error"] = message["error"]
        else:
            box["result"] = message.get("result") or {}
        box["done"].set()

    def send(self, message: dict) -> None:
        self._write(message, None)

    def _write(self, message: dict, expect) -> dict:
        """Write `message` to the agent; `expect` (an id and its box), when
        given, is registered as waiting in the same breath, with the process
        the message goes to, whose waiting requests are given back."""
        line = json.dumps(message)
        with self.write_lock:
            if self.proc is None or self.proc.poll() is not None:
                raise Refused(f"{self.label} is no longer running, so it cannot be answered; send the message again")
            waiting = self.waiting
            if expect is not None:
                waiting[expect[0]] = expect[1]
            try:
                self.proc.stdin.write(line + "\n")
                self.proc.stdin.flush()
            except BaseException:
                if expect is not None:
                    waiting.pop(expect[0], None)
                raise
        return waiting

    def stop(self) -> None:
        """Stop the agent and what the adapter runs with it (`stop_rest`)
        (idempotent)."""
        with self.life:
            # From here the reader drops every line of this process.
            self.stopping = True
            proc = self.proc
            running = proc is not None and proc.poll() is None and signal_group(proc, signal.SIGTERM)
        # The grace is waited out of `life`, so the reader keeps draining
        # (and dropping) what the agent writes as it shuts down: a full pipe
        # would block it, and it would be killed before it ends.
        if running:
            try:
                proc.wait(STOP_GRACE_SECS)
            except subprocess.TimeoutExpired:
                signal_group(proc, signal.SIGKILL)
                proc.wait()
        with self.life:
            self.stop_rest()

    def _watch(self, proc, waiting: dict) -> None:
        """Once the agent has exited (waited on the process itself, never on
        its pipes), stop what it left running (a helper it started: an MCP
        server, a tool's shell) as `stop` stops the agent, SIGTERM then
        SIGKILL once the grace is out: a helper inherits the agent's output,
        and while one holds it the reader never reaches its end, which is
        where the agent's end is told (`_ended`). A helper that left the
        agent's group (a daemon of its own) and still holds the output is out
        of weft's reach: the end is told from here after the grace, saying
        so, and the readers wait on until that helper lets go."""
        # `wait`, not `os.waitid`, which macOS's Python has only from 3.13.
        # Once reaped, the agent's id stays its group's while anything of
        # the group is left (a process id is never handed out again while a
        # group of that id exists), and with nothing left the signal finds
        # no group.
        proc.wait()
        for sig in (signal.SIGTERM, signal.SIGKILL):
            # Nothing of its group is left to signal, so none other is (its
            # id may already be another process's): what still holds the
            # output is outside the group, and gets its grace once.
            if not signal_group(proc, sig):
                if self._read_to_the_end(proc):
                    return
                break
            if self._read_to_the_end(proc):
                return
        self._ended(proc, waiting)

    @staticmethod
    def _read_to_the_end(proc) -> bool:
        """Whether both readers of `proc` reach the end of its output within
        the grace."""
        deadline = time.monotonic() + STOP_GRACE_SECS
        return all(read.wait(max(0.0, deadline - time.monotonic())) for read in (proc.stdout_read, proc.stderr_read))

    def retire(self, on_its_own: bool = False) -> None:
        """Stop the adapter for good: the conversation is done with it, and
        a start already under way (`ensure_started`) is refused or stopped.
        `on_its_own`: because its agent ended on its own. The first reason
        given is the one kept."""
        with self.life:
            if self.retired is None:
                self.retired = RETIRED_ON_ITS_OWN if on_its_own else RETIRED_BY_WEFT
        self.stop()

    def stop_rest(self) -> None:
        """Whatever else the adapter runs (a daemon the agent relays to, a
        socket), stopped after the agent."""

    def stderr_tail(self, proc=None) -> str:
        """The last lines the agent printed. Of a process (`proc`, else the
        current one) that has exited, once its stderr reader has reached the
        end of the pipe (what it printed as it died can still be in the pipe
        after its exit, unread), or after the grace when a process it
        started outside its group still holds the pipe, saying so."""
        proc = proc or self.proc
        held = proc is not None and proc.poll() is not None and not proc.stderr_read.wait(STOP_GRACE_SECS)
        printed = "\n".join(self.stderr) or "(it printed nothing)"
        if held:
            printed += (f" (and a process {self.label} started outside its own process group still holds its error output, "
                        "so anything it printed after this is not shown)")
        return printed

    def _read_err(self, proc) -> None:
        try:
            for line in proc.stderr:
                line = line.rstrip()
                if line:
                    self.stderr.append(line)
            proc.stderr.close()
        finally:
            proc.stderr_read.set()

    def _read_out(self, proc, waiting: dict) -> None:
        try:
            self._read_lines(proc)
            proc.stdout.close()
        finally:
            proc.stdout_read.set()
        self._ended(proc, waiting)

    def _ended(self, proc, waiting: dict) -> None:
        """The agent process `proc` ended: told once, by its reader at the end
        of its output, or by its watch when a helper outside its group keeps
        that end from coming (`_watch`)."""
        with self.life:
            if proc.ended_told:
                return
            proc.ended_told = True
        code = proc.wait()
        # Out of `life`: the wait for the rest of its stderr holds nothing.
        printed = self.stderr_tail(proc)
        with self.write_lock:
            try:
                proc.stdin.close()
            except BrokenPipeError:
                pass
        # A request of ours this process still owed (an agent that stopped
        # at startup on a bad flag or setting, before any turn) fails now,
        # with what it printed, instead of when its wait runs out. Only this
        # process's: a process started in its place has requests of its own.
        for rid in list(waiting):
            box = waiting.pop(rid, None)
            if box is not None:
                box["error"] = {"message": f"{self.label} exited (exit {code}) before answering: {printed}"}
                box["done"].set()
        with self.life:
            if proc is not self.proc or self.stopping:
                # Stopped by weft, or already replaced: the rest is the new one's.
                return
            if self.turn is not None and self.turn.status not in ev.ENDED:
                self.conv.fail(self.turn, f"{self.label} ended in the middle of the turn (exit {code}): {printed}",
                               status=ev.LOST)
            # The agent ended on its own: the adapter is put down, whatever
            # else it runs (a daemon the agent relays to) stopped with it,
            # through the conversation's one guarded stop; the next turn
            # builds a new one. Under `life`, so no start
            # swaps a process in meanwhile.
            self.conv.put_down(self, on_its_own=True)

    def _read_lines(self, proc) -> None:
        for line in proc.stdout:
            line = line.strip()
            if not line:
                continue
            try:
                message = json.loads(line)
            except json.JSONDecodeError:
                self.stderr.append(line)
                continue
            with self.life:
                if proc is not self.proc or self.stopping:
                    # A process being stopped, or stopped and replaced: what
                    # it still writes is nobody's turn (the one it ran ended).
                    continue
                try:
                    self.on_message(message)
                except Exception as e:  # noqa: BLE001 (a line the adapter cannot read fails the turn, loudly)
                    import traceback  # noqa: PLC0415
                    log(traceback.format_exc())
                    if self.turn is not None:
                        self.conv.fail(self.turn, f"the agent server could not read a line {self.label} wrote: "
                                                  f"{type(e).__name__}: {e}")

    def block(self, turn, kind: str, call_id: str, event_fields: dict, answer, refuse) -> None:
        """The turn waits on weft for the agent's call (`Conversation.block`).
        When weft refuses it (its turn ended meanwhile, or the call is asked
        twice), `refuse(why)` answers the agent's request with that error,
        so it never waits on an answer nobody will send, and a turn still
        running fails with it."""
        try:
            self.conv.block(turn, kind, call_id, event_fields, answer)
        except Refused as refused:
            refuse(str(refused))
            self.conv.fail(turn, str(refused))

    # ----- the login -----------------------------------------------------------

    def login_value(self, name: str) -> str:
        """The value `name` of the connection's login, for an adapter that
        hands it to the agent itself (over its protocol, in a file it
        writes): one its way in agents.json names, and never empty."""
        way = login_way(self.kind, self.credentials)
        if name not in [*way["env"], *way.get("protocol", [])]:
            raise Refused(f"{self.label}'s adapter reads the login value {name!r}, which agents.json does not name for its "
                          f"way {self.credentials.get('way')!r}", 500)
        value = (self.credentials.get("values") or {}).get(name)
        if not value:
            raise Refused(f"the connection's {name!r} is empty, and {self.label} needs it; reconnect it", 400)
        return value

    def take_login(self, credentials: dict):
        """Take `credentials`, the connection's login as weft holds it now,
        while the agent runs: True once the agent holds it (or held it
        already and asks for nothing), False when it holds exactly that and
        asks for another, None for an agent that takes no login while it
        runs (a new login then replaces its process between turns, as any
        change of its configuration does)."""
        return None

    def refuse_login(self, why: str) -> None:
        """weft could not renew the login the agent asked for: whatever of
        the agent waits on one is answered with `why`."""

    def want_login(self, reason: str) -> bool:
        """Ask weft for a fresh login: a `login` event on the running turn,
        which AskAgent carries onto `stream`, where the agent node hears it
        and answers over `/login` (`take_login`). False when no turn runs
        to carry it."""
        return self.conv.want_login(reason)

    def write_home_file(self, path: str, content: str, private: bool = False) -> None:
        """Write a file of the home whole: a temporary file renamed over it,
        so no reader ever sees it truncated or half written. `private`:
        readable by this user alone, the temporary file too (it holds a
        secret)."""
        write_whole(os.path.join(self.home, path), content, 0o600 if private else 0o644)

    # ----- the history ---------------------------------------------------------

    # Each goes into the history as a message of the turn running now. A
    # call of the program's tools is written by its route, as it reaches
    # weft and as weft answers it (`Conversation.block`, `deliver`), under
    # its plain name: the adapter writes only the agent's own tools, so a
    # call its stream shows under one of the names it gives the program's
    # tools (`program_tool`) is left to the route, its result with it.

    def remember_text(self, text: str) -> None:
        if text:
            self.conv.remember(self.turn, {"role": "assistant", "content": text})

    def remember_tool_use(self, call_id: str, name: str, arguments) -> None:
        if self.program_tool(name) is not None:
            self.program_calls.add(call_id)
            return
        arguments = arguments if isinstance(arguments, str) else json.dumps(arguments)
        self.conv.remember(self.turn, {"role": "assistant", "content": "",
                                       "tool_calls": [{"id": call_id, "name": name, "arguments": arguments}]})

    def remember_tool_result(self, call_id: str, output: str) -> None:
        if call_id in self.program_calls:
            return
        self.conv.remember(self.turn, {"role": "tool", "tool_call_id": call_id, "content": output})


class RpcRefused(Refused):
    """The agent answered our request with an error: `error` is that error, as it sent it."""

    def __init__(self, label: str, method: str, error):
        super().__init__(f"{label} refused {method}: {error_text(error)}")
        self.error = error if isinstance(error, dict) else {"message": str(error)}


def error_text(error) -> str:
    text = error.get("message", json.dumps(error)) if isinstance(error, dict) else str(error)
    data = error.get("data") if isinstance(error, dict) else None
    if data:
        text += f" ({json.dumps(data)[:500]})"
    return text


def text_of(content) -> str:
    """Plain text out of a tool's result, whatever shape the agent gave it."""
    if content is None:
        return ""
    if isinstance(content, str):
        return content
    if isinstance(content, list):
        parts = []
        for part in content:
            if isinstance(part, dict) and part.get("type") == "text":
                parts.append(part.get("text", ""))
            elif isinstance(part, dict):
                parts.append(f"[{part.get('type', 'content')}]")
            else:
                parts.append(str(part))
        return "\n".join(parts)
    return json.dumps(content)


# Every adapter, each of which registers its kind when imported: one module
# per agent, `adapter_<kind>.py` for an agent with a protocol of its own and
# `acp_<kind>.py` for an ACP agent (an `AcpSpec` the generic adapter runs).
import adapter_claude  # noqa: E402,F401 (imported once the base class exists)
import adapter_codex  # noqa: E402,F401
import adapter_muse  # noqa: E402,F401
import adapter_nanobot  # noqa: E402,F401
import adapter_pi  # noqa: E402,F401
import adapter_antigravity  # noqa: E402,F401
import adapter_cline  # noqa: E402,F401
import acp_codebuddy  # noqa: E402,F401
import adapter_copilot  # noqa: E402,F401
import acp_cursor  # noqa: E402,F401
import acp_devin  # noqa: E402,F401
import acp_dsh  # noqa: E402,F401
import acp_gemini  # noqa: E402,F401
import acp_goose  # noqa: E402,F401
import acp_grok  # noqa: E402,F401
import acp_hermes  # noqa: E402,F401
import acp_junie  # noqa: E402,F401
import adapter_kilo  # noqa: E402,F401
import adapter_kimi  # noqa: E402,F401
import acp_kiro  # noqa: E402,F401
import acp_minimax  # noqa: E402,F401
import adapter_openclaw  # noqa: E402,F401
import acp_opencode  # noqa: E402,F401
import acp_qwen  # noqa: E402,F401
import adapter_vibe  # noqa: E402,F401
