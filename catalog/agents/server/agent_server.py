"""The agent server: one coding agent's conversation, kept alive for weft.

A weft worker lives for one firing and then goes; a coding agent's turn can
last hours, and a person may take days to answer one of its questions. So
each agent node starts this small HTTP service beside the agent (as a process
on the person's machine, or inside the Computer the node is wired to), and it
owns everything that has to outlive a worker:

  - the agent's own process (`claude`, `codex app-server`, an ACP agent),
    started on the first turn and kept between turns;
  - the conversation's id, so a restarted agent picks the conversation up
    where it was (kept in `state.json`, beside the history);
  - one queue of turns: one turn runs at a time, a second message waits;
  - every event of every turn, numbered, so a reader that paused reads on
    from where it stopped and never sees an event twice;
  - the calls the agent is blocked on (one of the program's own tools, a
    question, or a permission request), in the order it made them, until
    weft answers each;
  - the credentials it was handed, in memory alone. An agent never renews
    a login weft handed it: weft renews it, once for every node holding the
    connection, and an agent that takes its login over its protocol takes
    the renewed one while it runs (`/login`), asking for it when it needs
    one (a `login` event).

Routes (JSON in, JSON out; a POST whose body is not `application/json` is
refused, so a web page cannot start a turn with a form or a plain-text post):

  GET  /health                 ready once listening; says which agent and
                               whether it was configured.
  POST /configure              the agent's settings and credentials.
  POST /turns                  {message, tools?, startOver?} queues a turn;
                               {toolResult: {callId, output, isError?}} or
                               {answer: {questionId, ...}} answers the call
                               the running turn is blocked on. Each answers
                               {turn, after}. With `key` (a string), a post
                               repeated with a key already seen starts
                               nothing and answers what the first answered:
                               a node that crashed after posting posts
                               again with the same key.
  GET  /events?turn=&after=&wait=
                               the turn's events after number `after`, its
                               status, and the call it waits on first
                               (while calls wait, the events stop at that
                               call's). `wait` holds the request until
                               something new happens or that many seconds
                               pass. A turn that ended, whose every event
                               was read (its history read, or the reader
                               asking from its last event), has its events
                               dropped once a later turn is posted:
                               reading it again is a 410 saying so.
  GET  /history?turn=          the conversation as a ChatHistory, as it
                               stood when that turn ended (a turn of the
                               conversation before the last start over too).
  POST /login                  {credentials}: the connection's login as weft
                               holds it now, which the agent takes while it
                               runs; answers {taken}, false when the agent
                               holds exactly that and asks for another (the
                               node then renews the connection and posts
                               again). {error}: weft could not renew it, and
                               the running turn fails with it.
  POST /reset                  start over: a new conversation.
  GET  /live, POST /action     the node's card: its status and a Start over
                               button.
  POST /mcp                    the program's own tools, as an MCP server the
                               agent calls (streamable HTTP, JSON answers;
                               the contract plugins rely on is at `Mcp`).

Run as `python3 agent_server.py --port N --home DIR`, or with `mcp-bridge
URL` to stand in as a stdio MCP server forwarding to `/mcp` (for an agent
that only starts MCP servers as commands).
"""

import argparse
import collections
import http.server
import json
import os
import signal
import sys
import threading
import time
import traceback
import urllib.error
import urllib.parse
import urllib.request
import uuid

import adapters
import events as ev
from events import Refused

# SYNC: the agent server's routes <-> catalog/agents/agent.rs (HEALTH_PATH, CONFIGURE_PATH, TURNS_PATH, EVENTS_PATH
#       (written there without its leading slash, as a URL segment), HISTORY_PATH, LOGIN_PATH),
#       catalog/agents/server/adapters.py (MCP_PATH), catalog/agents/ask_agent/mod.rs (QUESTION_PATH).
#       RESET_PATH, LIVE_PATH, ACTION_PATH and MCP_PATH have no Rust peer.
HEALTH_PATH = "/health"
CONFIGURE_PATH = "/configure"
TURNS_PATH = "/turns"
EVENTS_PATH = "/events"
HISTORY_PATH = "/history"
LOGIN_PATH = "/login"
RESET_PATH = "/reset"
LIVE_PATH = "/live"
QUESTION_PATH = "/question"
ACTION_PATH = "/action"
MCP_PATH = "/mcp"

# The key on a history entry naming the turn it belongs to (`/history?turn=`).
HISTORY_TURN = "turn"

# The longest a reader may hold `/events` open. Long enough that a live
# reader makes few requests, short enough that no proxy on the way cuts it.
MAX_WAIT_SECS = 25

# The kinds of error the server can hit outside a turn (`Conversation.trouble`).
TROUBLE_STOP = "stop"
TROUBLE_ERROR = "error"
# The server is stopping for good (`Conversation.close`): never cleared.
TROUBLE_CLOSING = "closing"

# The card's one button.
# Handled by this server's own /action alone: no Rust peer.
START_OVER = "start_over"


def log(message: str) -> None:
    print(f"agent-server: {message}", file=sys.stderr, flush=True)


class Turn:
    """One turn: a message, everything the agent did with it, and how it ended."""

    def __init__(self, turn_id: str, message: str, tools: list, start_over: bool = False):
        self.id = turn_id
        self.message = message
        self.tools = tools
        # The conversation starts over with this turn, when its time comes:
        # a turn queued before it, another run's, finishes first.
        self.start_over = start_over
        self.status = ev.QUEUED
        # The calls the agent is blocked on, oldest first. An agent can make
        # several at once (Claude Code calls two of the program's tools side
        # by side, Codex opens a second request), and weft answers them one
        # at a time: `/events` shows the first, and answering it shows the
        # next.
        self.waiting = []
        # The calls weft's answer reached the agent for: a call the agent
        # then ends failed was answered with an error, never given up on
        # (`Conversation.stop_waiting`).
        self.answered = set()
        self.error = None
        self.reply = None
        # What the agent says it is doing that no event carries (Vibe
        # retrying a refused request for minutes): the card shows it beside
        # the status, so a long silence is legible. Cleared by the turn's
        # next event.
        self.notice = None
        # Its events, oldest first, and the number of the last one its reader
        # is known to hold: once it has ended and that is its last, it has
        # been read whole (`Conversation._trim`). A reader holds what it
        # asks to read after (`/events?after=`, the poll that wakes a paused
        # node included: both ask from what the node saved), and the whole
        # turn once it reads its history (which a node does after it saved
        # the end). The end of a turn that failed is held once it is handed
        # out: the node stops there, and one that lost it would only fail
        # with another message.
        self.events = []
        self.read_upto = 0

    @property
    def pending(self):
        """The call weft answers next, or None."""
        return self.waiting[0] if self.waiting else None


class Pending:
    """A call the agent is blocked on until weft answers it.

    `answer` is the adapter's: it takes what weft sent and hands it to the
    agent in the agent's own protocol. `event` is what went out about it.
    `answering` is set while an answer is on its way to the agent, and
    `gave_up` when the agent ended the call meanwhile (`stop_waiting`).
    """

    def __init__(self, kind: str, call_id: str, event: dict, answer):
        self.kind = kind
        self.id = call_id
        self.event = event
        self.answer = answer
        self.answering = False
        self.gave_up = False


class Conversation:
    """Everything the server keeps, behind one lock."""

    def __init__(self, home: str):
        self.home = home
        os.makedirs(home, exist_ok=True)
        self.lock = threading.Condition()
        # A new number every time the server starts: a turn id from an
        # earlier life of the server is one this life cannot know.
        self.epoch = uuid.uuid4().hex[:8]
        self.counter = 0
        # The number the last event got: events are numbered across every
        # turn, and each turn keeps its own (`Turn.events`).
        self.numbered = 0
        self.turns = {}
        self.queue = collections.deque()
        self.current = None
        self.config = None
        self.adapter = None
        # Adapters taken out and still stopping (`_take_adapter`): a new one
        # is built only once none is left, since each lives in the same
        # `<kind>-home`.
        self.retiring = set()
        # The errors the server hit outside a turn, by kind, each said to
        # every turn sent until the recovery that retries that very thing
        # works (`_recover`): a stop that failed (TROUBLE_STOP: the adapter
        # is kept in `unstopped`, since its agent may still run and another
        # started beside it would double it), or anything else
        # (TROUBLE_ERROR, cleared by a configure or a reset: the person
        # saying it is fixed).
        self.unstopped = None
        self.trouble = {}
        # A configuration changed while a turn ran: the agent's process is
        # replaced once that turn ends.
        self.restart_after_turn = False
        # What survives a restart of the server: the agent's conversation
        # id and what the adapter keeps with it, the conversation as a
        # ChatHistory.
        self.state = self._load_state()
        # Why each call the agent stopped waiting on went unanswered, so an
        # answer that comes after says what happened to it.
        self.gone = {}
        # The calls the agent gave up on and went on without, each with its
        # turn and the number of the event that made it: the answer that
        # comes late reads the rest of that turn instead.
        self.given_up = {}
        waited = self.state.pop("waitingOn", [])
        for kind, call_id in waited:
            self.gone[(kind, call_id)] = ("the agent server restarted while the agent waited on it, so its turn is lost; "
                                          "send the message again (the conversation itself is kept)")
        if waited:
            self.save_state()
        for kind, call_id in self.state.pop("gaveUp", []):
            self.gone[(kind, call_id)] = ("the agent stopped waiting for it, and the agent server restarted since, so the rest "
                                          "of its turn is lost; send the message again (the conversation itself is kept)")
        # The program's own tools, as an MCP server: the route every call of
        # them without a callback of its agent's own reaches (`Mcp`).
        self.mcp = Mcp(self)
        threading.Thread(target=self._run_turns, name="turns", daemon=True).start()

    # ----- the state kept on disk --------------------------------------

    def _state_path(self) -> str:
        return os.path.join(self.home, "state.json")

    def _load_state(self) -> dict:
        try:
            with open(self._state_path(), encoding="utf-8") as f:
                state = json.load(f)
        except FileNotFoundError:
            state = {"conversation": None, "kept": {}, "history": [], "previousHistory": []}
        except (OSError, json.JSONDecodeError) as e:
            # Never a silent fresh start over a conversation it had: say so
            # and refuse to run until a person starts over.
            raise SystemExit(f"cannot read {self._state_path()}: {e}; delete it to start the conversation over")
        # Every `/turns` key seen, with what its post answered (`_keyed`): on
        # disk, so a post repeated after a restart of the server still
        # starts nothing. A key goes with its turn's events (`_trim`), and
        # the keys of an earlier life of the server (whose turns this one
        # never had) at the next start over (`_forget_conversation`).
        state.setdefault("turnKeys", {})
        return state

    def save_state(self) -> None:
        # Readable by this user alone: the state holds the conversation.
        adapters.write_whole(self._state_path(), json.dumps(self.state), 0o600)

    # ----- what adapters call -------------------------------------------

    def emit(self, turn: Turn, kind: str, **fields):
        """Number an event of `turn` and wake every reader. An event of a
        turn already ended is dropped, and logged: its readers have read its
        end, and what its agent writes after it is nobody's (the reader
        drops a stopped agent's lines; this catches any other way)."""
        with self.lock:
            if turn.status in ev.ENDED:
                log(f"dropped a {kind} event of turn {turn.id}, which already ended {turn.status}")
                return None
            self.numbered += 1
            event = {"n": self.numbered, "turn": turn.id, "type": kind, **fields}
            turn.events.append(event)
            turn.notice = None
            self.lock.notify_all()
            return event

    def notice(self, turn: Turn, text: str) -> None:
        """Say on the card what `turn`'s agent is doing that no event carries
        (until the turn's next event); nothing for a turn already ended."""
        with self.lock:
            if turn.status not in ev.ENDED:
                turn.notice = text

    @property
    def events(self) -> list:
        """Every event still kept, oldest first."""
        with self.lock:
            return sorted((e for turn in self.turns.values() for e in turn.events), key=lambda e: e["n"])

    def remember(self, turn: Turn, message: dict) -> None:
        """Add one message of `turn` to the conversation's history and keep
        it. A message of a turn already ended is dropped, and logged: the
        history read for it is the one it ended with."""
        with self.lock:
            if turn.status in ev.ENDED:
                log(f"dropped a history message of turn {turn.id}, which already ended {turn.status}")
                return
            # The turn goes on the entry, so `/history?turn=` can say what
            # the history was when that turn ended, whatever came after.
            self.state["history"].append({**message, HISTORY_TURN: turn.id})
            self.save_state()

    def _unremember(self, message) -> None:
        """Take `message`, which `remember` added, back out of the history
        (an answer that never reached the agent). The lock is held."""
        history = self.state["history"]
        for index in range(len(history) - 1, -1, -1):
            entry = history[index]
            if message is not None and {k: v for k, v in entry.items() if k != HISTORY_TURN} == message:
                del history[index]
                self.save_state()
                return

    def question(self, question_id: str) -> dict:
        """Whether the agent still waits for weft's answer to its question
        `question_id` (`/question`): what AnswerAgent's form waits on beside
        the person, so a question nobody can answer any more stops being
        asked. `gone` once nothing would take the answer, with why."""
        call = (ev.QUESTION, question_id)
        with self.lock:
            if any(p.kind == ev.QUESTION and p.id == question_id for turn in self.turns.values() for p in turn.waiting):
                return {"questionId": question_id, "gone": False}
            if call in self.given_up:
                why = "the agent stopped waiting for the answer and went on with its turn without it"
            elif call in self.gone:
                why = self.gone[call]
            elif any(question_id in turn.answered for turn in self.turns.values()):
                why = "it was answered already"
            else:
                why = f"the agent server knows no question '{question_id}'"
            return {"questionId": question_id, "gone": True, "why": why}

    def history(self, turn_id: str) -> list:
        """The conversation as it stood when the turn `turn_id` ended: in
        this conversation, or in the one before the last start over (a turn
        that ended just before a queued start over is read after it)."""
        with self.lock:
            turn = self.turns.get(turn_id)
            if turn is not None and turn.status not in ev.ENDED:
                raise Refused(f"turn {turn_id} has not ended yet; read its history once it has", 409)
            if turn is not None and turn.events:
                # Its node read the end before it asked for this.
                turn.read_upto = turn.events[-1]["n"]
            for entries in (self.state.get("history", []), self.state.get("previousHistory", [])):
                last = next((i for i in range(len(entries) - 1, -1, -1) if entries[i].get(HISTORY_TURN) == turn_id), None)
                if last is not None:
                    return [{k: v for k, v in entry.items() if k != HISTORY_TURN} for entry in entries[:last + 1]]
            raise Refused(f"the conversation holds no message of turn {turn_id}: it was started over twice since (the "
                          "conversation before the last start over is kept, no older one), or that turn never ran", 404)

    def set_conversation(self, conversation_id: str, kept: dict = None) -> None:
        """The agent's id of the conversation, and what the adapter keeps
        with it from its start (`kept`: the mode it started in, ...)."""
        with self.lock:
            kept = kept or {}
            if self.state.get("conversation") != conversation_id or self.state.get("kept", {}) != kept:
                self.state["conversation"] = conversation_id
                self.state["kept"] = kept
                self.save_state()

    def conversation_id(self):
        with self.lock:
            return self.state.get("conversation")

    def conversation_kept(self) -> dict:
        """What the adapter kept with the conversation when it began."""
        with self.lock:
            return dict(self.state.get("kept") or {})

    def _forget_conversation(self) -> None:
        """Start the conversation over: nothing of the old one carries, but
        its history is kept until the next start over, for a turn of it whose
        node reads it after this (`history`). The lock is held."""
        if self.state.get("history"):
            # Only a conversation that has something: two start overs in a
            # row (a configuration of another agent, then a queued start
            # over) keep the one before them.
            self.state["previousHistory"] = self.state["history"]
        self.state.update(conversation=None, kept={}, history=[])
        # A post repeated with the key of an earlier life's turn belongs to a
        # conversation that is gone now; this life's turns keep theirs.
        self.state["turnKeys"] = {key: answer for key, answer in self.state["turnKeys"].items() if answer["turn"] in self.turns}

    def block(self, turn: Turn, kind: str, call_id: str, event_fields: dict, answer) -> None:
        """The agent waits on weft: a tool call or a question, behind any it
        already waits on. A `tool_call` is a call of the program's tools
        that came in on the program's route (weft's MCP server, or the
        agent's own tool callback): this is the one place the history gets
        it, under the program's name for it (its result: `deliver`,
        `_give_up`), whatever the agent's own stream calls it."""
        with self.lock:
            if turn.status in ev.ENDED:
                # Refused, so the caller answers the agent's request with an
                # error: an agent left waiting would wait for ever, on an
                # adapter the next turn may reuse.
                raise Refused(f"{kind} '{call_id}' came after its turn ended ({turn.status}), so weft will never answer it")
            if any(p.kind == kind and p.id == call_id for p in turn.waiting):
                raise Refused(f"{kind} '{call_id}' is asked twice in one turn", 500)
            event = self.emit(turn, kind, **event_fields)
            if kind == ev.TOOL_CALL:
                self.remember(turn, {"role": "assistant", "content": "", "tool_calls": [
                    {"id": call_id, "name": event_fields["name"], "arguments": event_fields["arguments"]}]})
            turn.waiting.append(Pending(kind, call_id, event, answer))
            self._waiting_changed(turn)

    def _waiting_changed(self, turn: Turn) -> None:
        """The turn's status follows the call it waits on first, and every
        call it waits on goes on disk, so a restarted server can say what
        became of each. The lock is held."""
        if turn.status not in ev.ENDED:
            first = turn.pending
            turn.status = ev.WORKING if first is None else ev.WAITING_TOOL if first.kind == ev.TOOL_CALL else ev.WAITING_QUESTION
        if turn.waiting:
            self.state["waitingOn"] = [[p.kind, p.id] for p in turn.waiting]
        else:
            self.state.pop("waitingOn", None)
        self.save_state()
        self.lock.notify_all()

    def _end_waiting(self, turn: Turn, why: str) -> None:
        """The turn ended: every call it still waits on is gone, for the
        reason `why` an answer arriving later gets. The lock is held."""
        for pending in turn.waiting:
            # One being answered reached the agent, or fails loudly itself.
            if not pending.answering:
                self.gone[(pending.kind, pending.id)] = why
        turn.waiting = []
        self._waiting_changed(turn)

    def stop_waiting(self, turn: Turn, call_id: str) -> None:
        """The agent ended the call `call_id` without a result from weft.
        One weft answered (or is answering) ended that way because its
        answer was an error, which is nothing to act on. One still waiting
        was given up on, and the agent carries on with its turn: an answer
        sent after this finds nothing waiting. A call that is neither fails
        the turn, naming both."""
        with self.lock:
            if turn.status in ev.ENDED:
                return
            pending = next((p for p in turn.waiting if p.id == call_id), None)
            if pending is not None and pending.answering:
                # Settled once the answer has gone or not (`deliver`): the
                # normal end of an errored call if it went, a give-up if not.
                pending.gave_up = True
                return
            if call_id in turn.answered:
                return
            if pending is None:
                waits = ", ".join(f"'{p.id}'" for p in turn.waiting) or "nothing"
                raise Refused(f"the agent gave up on call '{call_id}', and the turn waits on {waits}", 500)
            self._give_up(turn, pending)

    def _give_up(self, turn: Turn, pending: Pending) -> None:
        """The agent went on without `pending`, which weft never answered.
        The lock is held."""
        if turn.status in ev.ENDED or pending not in turn.waiting:
            return
        turn.waiting.remove(pending)
        self.given_up[(pending.kind, pending.id)] = (turn, pending.event["n"])
        # On disk too, so a restarted server can say what became of it.
        self.state.setdefault("gaveUp", []).append([pending.kind, pending.id])
        output = "the agent stopped waiting for this call and went on with its turn without it"
        self.emit(turn, ev.TOOL_RESULT, id=pending.id, isError=True, output=output)
        if pending.kind == ev.TOOL_CALL:
            self.remember(turn, {"role": "tool", "tool_call_id": pending.id, "content": output})
        self._waiting_changed(turn)

    def finish(self, turn: Turn, reply: str) -> None:
        with self.lock:
            if turn.status in ev.ENDED:
                return
            turn.reply = reply
            self.emit(turn, ev.DONE, reply=reply)
            turn.status = ev.DONE
            self._ended(turn, "the agent ended its turn without it")

    def fail(self, turn: Turn, message: str, status: str = None) -> None:
        with self.lock:
            if turn.status in ev.ENDED:
                return
            turn.error = message
            self.emit(turn, ev.ERROR, message=message)
            turn.status = status or ev.FAILED
            self._ended(turn, f"its turn ended first: {message}")

    def _ended(self, turn: Turn, why: str) -> None:
        """`turn` just ended: the queue goes on, then what it waited on is
        settled (`_end_waiting`). The lock is held. Whoever ended it (an
        adapter's own thread, often) never sees a failure to write that on
        disk: the turn has ended whatever happens to it, so it is said to
        the next turn sent instead."""
        if self.current is turn:
            self.current = None
        self.lock.notify_all()
        try:
            self._end_waiting(turn, why)
        except Exception as e:  # noqa: BLE001 (said to the next turn sent, never lost)
            self._troubled(TROUBLE_ERROR, f"the agent server could not write down how turn {turn.id} ended: "
                                          f"{type(e).__name__}: {e}; fix what it names, then run the node again")

    def running(self):
        with self.lock:
            return self.current

    def want_login(self, reason: str) -> bool:
        """The agent asks for a fresh login (`Adapter.want_login`): a
        `login` event on the running turn, which AskAgent carries onto
        `stream`, where the agent node hears it and answers over `/login`.
        The turn goes on meanwhile. False when no turn runs: nothing would
        carry the ask, and the next turn's configure hands the login the
        connection holds then."""
        with self.lock:
            turn = self.current
            if turn is None or turn.status in ev.ENDED:
                return False
            self.emit(turn, ev.LOGIN, reason=reason)
            return True

    def take_login(self, body: dict) -> dict:
        """The answer to `/login`. `{credentials}`: the connection's login
        as weft holds it now, for the agent to take while it runs
        (`Adapter.take_login`), kept as the configuration's from here on, so
        an agent started again starts on it. `{taken: false}` when the agent
        holds exactly that and asks for another. `{error}`: weft could not
        renew it, and the agent's ask and the running turn fail with it."""
        if "error" in body:
            why = body["error"]
            if not isinstance(why, str) or not why:
                raise Refused("a login error is {error: <why weft could not renew the login>}", 400)
            with self.lock:
                adapter, turn = self.adapter, self.current
            if adapter is not None:
                adapter.refuse_login(why)
            if turn is not None:
                self.fail(turn, why)
            return {"taken": False}
        credentials = body.get("credentials")
        if not isinstance(credentials, dict) or not isinstance(credentials.get("values"), dict):
            raise Refused("a login is {credentials: {way, values}} or {error}", 400)
        with self.lock:
            if self.config is None:
                raise Refused("the agent server was handed a login before its configuration; configure it first", 409)
            configured = self.config.get("credentials")
            if configured is None or configured.get("way") != credentials.get("way"):
                raise Refused(f"the agent server was configured with the login way {(configured or {}).get('way')!r}, and "
                              f"was handed one of {credentials.get('way')!r}; configure it again", 409)
            self.config = dict(self.config, credentials=credentials)
            adapter = self.adapter
        if adapter is None:
            # No agent runs: the next one starts on it.
            return {"taken": True}
        taken = adapter.take_login(credentials)
        if taken is None:
            raise Refused(f"{adapter.label} takes no login while it runs; its next configure hands it the new one", 409)
        return {"taken": taken}

    # ----- what the routes call -----------------------------------------

    def configure(self, config: dict) -> dict:
        kind = config.get("agent")
        if kind not in adapters.KINDS:
            raise Refused(f"no agent {kind!r}; this server drives {', '.join(sorted(adapters.KINDS))}", 400)
        folder = config.get("folder")
        if not isinstance(folder, str) or not folder:
            raise Refused("the configuration names no folder to work in", 400)
        folder = os.path.expanduser(folder)
        if not os.path.isdir(folder):
            raise Refused(f"the folder {folder} does not exist; create it, or pick one that does", 400)
        config = dict(config, folder=folder)
        owner = config.get("owner")
        with self.lock:
            if self.state.get("owner") not in (None, owner):
                # Two agent nodes (or two instances' copies of one) landed
                # on one server: neither may drive the other's conversation.
                raise Refused(f"this agent server holds the conversation of {self.state['owner']}, and {owner} configured it; "
                              "give each agent node a computer of its own, or a node name of its own", 409)
            self.state["owner"] = owner
            if self.state.get("agent") not in (None, kind):
                # The conversation is another agent's: nothing of it carries.
                self._forget_conversation()
            self.state["agent"] = kind
            self.save_state()
            changed = self.config != config
            # A change of the login alone reaches an agent that takes one
            # while it runs as it is (a login weft renewed), with no
            # restart; any other change takes effect from the next turn: the
            # agent's process is replaced between turns, never in the middle
            # of one.
            login_only = (changed and self.config is not None and self.adapter is not None
                          and dict(self.config, credentials=None) == dict(config, credentials=None))
            self.config = config
            live = self.adapter if login_only else None
            if live is None:
                adapter = self._take_adapter() if changed and self.current is None else None
                if changed and self.current is not None:
                    self.restart_after_turn = True
        if live is not None and live.take_login(config.get("credentials")) is None:
            # It takes no login while it runs: replaced, as any other change.
            with self.lock:
                adapter = self._take_adapter() if self.current is None and self.adapter is live else None
                if self.current is not None:
                    self.restart_after_turn = True
        elif live is not None:
            adapter = None
        self._retire(adapter)
        self._recover()
        self._refuse_if_troubled()
        return {"ok": True, "changed": changed}

    def submit(self, message: str, tools: list, start_over: bool, key: str = None) -> dict:
        """Queue a turn; what `/turns` answers (`{turn, after}`). A `key`
        already seen answers what its first post did, and queues nothing."""
        with self.lock:
            if key is not None and key in self.state["turnKeys"]:
                return dict(self.state["turnKeys"][key])
        if not isinstance(message, str) or not message.strip():
            raise Refused("a turn needs a message, and this one is empty", 400)
        self._refuse_if_troubled()
        with self.lock:
            if key is not None and key in self.state["turnKeys"]:
                # The same post, made twice at once.
                return dict(self.state["turnKeys"][key])
            if self.config is None:
                raise Refused("the agent was never configured: run its node again", 409)
            self._trim()
            self.counter += 1
            turn = Turn(f"{self.epoch}-{self.counter}", message, tools or [], start_over)
            self.turns[turn.id] = turn
            self.queue.append(turn)
            answer = {"turn": turn.id, "after": 0}
            if key is not None:
                self._keyed(key, answer)
            self.lock.notify_all()
            return answer

    def _keyed(self, key: str, answer: dict) -> None:
        """Remember what the post of `key` answered. The lock is held."""
        self.state["turnKeys"][key] = answer
        self.save_state()

    def _trim(self) -> None:
        """Drop the events of every turn that has ended and whose events
        were all read (`Turn.read_upto`), with the keys that answered it.
        The history is kept apart from the events (`remember`), so nothing
        of the conversation goes with them. The lock is held."""
        read = [turn.id for turn in self.turns.values()
                if turn.status in ev.ENDED and turn.events and turn.read_upto >= turn.events[-1]["n"]]
        if not read:
            return
        for turn_id in read:
            del self.turns[turn_id]
        keys = self.state["turnKeys"]
        dropped = [key for key, answer in keys.items() if answer["turn"] in read]
        for key in dropped:
            del keys[key]
        # A late answer to a call its agent gave up on reads the rest of that turn.
        for call, (turn, _) in list(self.given_up.items()):
            if turn.id in read:
                del self.given_up[call]
        if dropped:
            self.save_state()

    def _trimmed(self, turn_id: str) -> bool:
        """Whether `turn_id` is a turn of this life of the server whose
        events `_trim` dropped. The lock is held."""
        epoch, _, number = turn_id.rpartition("-")
        return epoch == self.epoch and number.isdigit() and 0 < int(number) <= self.counter and turn_id not in self.turns

    def deliver(self, kind: str, call_id: str, payload: dict, key: str = None):
        """Hand weft's answer to the call the agent waits on first. Gives
        back the turn id it carries on and the number of the event that
        made the call, which the firing that hands it over has already sent
        on: what `/turns` answers (`{turn, after}`). A `key` already seen
        answers what its first post did, and hands nothing over."""
        with self.lock:
            if key is not None and key in self.state["turnKeys"]:
                return dict(self.state["turnKeys"][key])
            if (kind, call_id) in self.given_up:
                # Too late for the agent: the firing reads the rest of the turn.
                gave_up = self.state.get("gaveUp", [])
                if [kind, call_id] in gave_up:
                    gave_up.remove([kind, call_id])
                    self.save_state()
                turn, after = self.given_up[(kind, call_id)]
                answer = {"turn": turn.id, "after": after}
                if key is not None:
                    self._keyed(key, answer)
                return answer
            turn = self.current
            pending = turn.pending if turn else None
            if pending is None or pending.id != call_id or pending.kind != kind:
                why = self.gone.get((kind, call_id))
                if why is None:
                    # Weft is shown one call at a time, the first, so only
                    # the first is answered: a reader that has read past the
                    # first would otherwise never be shown it.
                    why = f"it waits on {pending.kind} '{pending.id}' first" if pending else "it waits on nothing"
                raise Refused(f"nothing waits for {kind} '{call_id}': {why}")
            if payload.get("dropped"):
                # AnswerAgent sends this only once `/question` said the agent
                # no longer waits: seen still waiting, the two disagree.
                raise Refused(f"the answer to {kind} '{call_id}' says the agent dropped it, yet the agent still waits on it", 500)
            if pending.answering:
                raise Refused(f"{kind} '{call_id}' is being answered already")
            # It stays first until the agent has it, so a refused answer
            # (a choice that was not offered) leaves it waiting as it was.
            pending.answering = True
            # Into the history before the agent has it, so it comes before
            # whatever the agent writes next; taken out again if it never
            # reaches the agent.
            result = None
            if kind == ev.TOOL_CALL:
                result = {"role": "tool", "tool_call_id": call_id, "content": str(payload.get("output", ""))}
                self.remember(turn, result)
        # Outside the lock: the adapter writes to the agent, which may
        # answer on another thread at once.
        try:
            pending.answer(payload)
        except Refused:
            with self.lock:
                self._unremember(result)
                pending.answering = False
                if pending.gave_up:
                    # The agent ended the call while this answer, which never
                    # reached it, was on its way: it gave up on it.
                    self._give_up(turn, pending)
            raise
        except Exception as e:  # noqa: BLE001 (any other failure is the adapter's or the pipe's: the turn fails, loudly)
            log(traceback.format_exc())
            message = f"the agent server could not hand the answer to {kind} '{call_id}' to the agent: {type(e).__name__}: {e}"
            with self.lock:
                self._unremember(result)
            self.fail(turn, f"{message}; send the message again")
            raise Refused(message, 500) from e
        with self.lock:
            turn.answered.add(call_id)
            if pending in turn.waiting:
                turn.waiting.remove(pending)
                self._waiting_changed(turn)
            answer = {"turn": turn.id, "after": pending.event["n"]}
            if key is not None:
                self._keyed(key, answer)
        return answer

    def _take_adapter(self):
        """Take the adapter out, to be stopped by `_retire`; it counts as
        retiring until it has. The lock is held."""
        adapter, self.adapter = self.adapter, None
        if adapter is not None:
            self.retiring.add(adapter)
        return adapter

    def put_down(self, adapter, during: str = None, on_its_own: bool = False):
        """Stop `adapter` for good, taking it out first when it is this
        conversation's: what an agent that ended on its own (`on_its_own`),
        or never finished starting, comes to (`Adapter._read_out`,
        `Adapter.ensure_started`). `during`, as for `_retire`. Whether it
        failed to stop, as `_retire` says it."""
        with self.lock:
            if self.adapter is adapter:
                self.adapter = None
            if adapter in self.retiring or adapter is self.unstopped:
                # Being stopped already, or its stop failed and is recorded.
                return None if adapter in self.retiring else self.trouble.get(TROUBLE_STOP)
            self.retiring.add(adapter)
        return self._retire(adapter, during, on_its_own)

    def _retire(self, adapter, during: str = None, on_its_own: bool = False):
        """Stop `adapter` (taken out, and in `retiring`) for good, then let
        a new one be built. The one place an adapter is stopped: a stop that
        fails (a signal refused) never reaches the caller, which may be the turn runner or the
        agent's own reader. It is said: the running turn and every queued
        one fail with it (the running one with `during` first, the error it
        was already failing with, when there is one), turns are refused
        until a configure or a reset stops the adapter (`_recover`), and
        what it says is given back; None when it stopped. The lock is not
        held: stopping waits on the agent's process. `on_its_own`: its agent
        ended on its own, the one retirement a turn that meets it at its
        start goes on from, on a new adapter (`_started`)."""
        if adapter is None:
            return None
        try:
            adapter.retire(on_its_own)
        except Exception as e:  # noqa: BLE001 (any failure to stop is said where the node reads it)
            log(traceback.format_exc())
            why = (f"the agent server could not stop {adapter.label}: {type(e).__name__}: {e}. It may still be running, so "
                   "no turn starts until it is stopped: stop that agent's process yourself if it runs, then run the node "
                   "again (or start the conversation over from its card), and the agent server tries again")
            with self.lock:
                self.retiring.discard(adapter)
                self.unstopped = adapter
                self.trouble[TROUBLE_STOP] = why
                current, queued = self.current, list(self.queue)
                self.queue.clear()
                self.lock.notify_all()
            if current is not None:
                self.fail(current, f"{during}. Stopping {adapter.label} then failed too: {why}" if during else why)
            for turn in queued:
                self.fail(turn, why)
            return why
        # Stopped: the adapter whose stop failed, if this was it, is too.
        self._clear_trouble(TROUBLE_STOP)
        with self.lock:
            self.retiring.discard(adapter)
            self.lock.notify_all()
        return None

    def _troubled(self, kind: str, why: str) -> None:
        """An error outside any turn: logged, and said to every turn sent
        until the recovery of its kind works, beside any other kind."""
        log(traceback.format_exc())
        with self.lock:
            self.trouble[kind] = why
            self.lock.notify_all()

    def _clear_trouble(self, kind: str) -> None:
        """The recovery of `kind` worked: its trouble ends (a failed stop
        only once no adapter is left unstopped)."""
        with self.lock:
            left = {TROUBLE_STOP: self.unstopped}.get(kind)
            if left is None:
                self.trouble.pop(kind, None)

    def _recover(self) -> None:
        """Retry everything that troubled the server, on a configure or a
        reset: the stop that failed; any other error is the person saying
        it is fixed."""
        with self.lock:
            adapter, self.unstopped = self.unstopped, None
            if adapter is not None:
                self.retiring.add(adapter)
        if adapter is not None:
            self._retire(adapter)
        self._clear_trouble(TROUBLE_ERROR)

    def _refuse_if_troubled(self) -> None:
        with self.lock:
            if self.trouble:
                raise Refused(self.troubles(), 503)

    def troubles(self) -> str:
        """Every error the server is troubled by, as one message (the server
        stopping first, then a failed stop: it is the one to act on). The
        lock is held."""
        kinds = (TROUBLE_CLOSING, TROUBLE_STOP, TROUBLE_ERROR)
        return "; and ".join(self.trouble[kind] for kind in kinds if kind in self.trouble)

    def close(self) -> None:
        """Stop for good, before the server exits on a SIGTERM (a stop of
        its process unit, or the computer removing its app): no turn starts
        from here on, and the agent is stopped the one way an adapter is
        (`_retire`), which also stops what it started outside this
        process's group (a daemon) and removes its sockets. The
        conversation stays on disk as it is, for the
        next start; the turn that was running is lost, as after any
        restart of the server."""
        with self.lock:
            self.trouble[TROUBLE_CLOSING] = "the agent server is stopping; run the node again once it is back"
            adapter = self._take_adapter()
            self.lock.notify_all()
        self._retire(adapter)

    def reset(self, why: str) -> None:
        with self.lock:
            adapter = self._take_adapter()
            # The adapter is gone with the conversation: nothing is left to replace.
            self.restart_after_turn = False
            current = self.current
            self._forget_conversation()
            self.save_state()
        self._retire(adapter)
        if current is not None:
            self.fail(current, f"{why} while this turn ran; send the message again")
        self._recover()
        self._refuse_if_troubled()

    def read(self, turn_id: str, after: int, wait: float) -> dict:
        """The answer to `/events`: built whole under the lock, so the status,
        the call waited on and the reply agree with the events it carries."""
        deadline = time.monotonic() + wait
        with self.lock:
            while True:
                turn = self.turns.get(turn_id)
                if turn is None and self._trimmed(turn_id):
                    raise Refused(f"the events of turn {turn_id} are gone: that turn ended, its node read every event of "
                                  "it, and they were dropped when a later turn was posted. Whoever reads them again (from "
                                  f"event {after}) lost track of what it read; the conversation is still there "
                                  f"(/history?turn={turn_id})", 410)
                if turn is None:
                    return lost(turn_id)
                # While calls wait, the events stop at the first one's: the
                # firing that answers it reads on from just after it (the
                # `after` `/turns` answers), so anything later (the next
                # call's event among them) is shown once, after that answer.
                upto = turn.pending.event["n"] if turn.pending is not None else None
                turn.read_upto = max(turn.read_upto, after)
                news = [e for e in turn.events if e["n"] > after and (upto is None or e["n"] <= upto)]
                answer = turn_answer(turn, news, after)
                left = deadline - time.monotonic()
                if answer["news"] or left <= 0:
                    if news and turn.status in (ev.FAILED, ev.LOST) and news[-1] is turn.events[-1]:
                        turn.read_upto = news[-1]["n"]
                    return answer
                self.lock.wait(left)

    # ----- the turn runner ------------------------------------------------

    def _run_turns(self) -> None:
        while True:
            try:
                self._run_turn()
            except Exception as e:  # noqa: BLE001 (the runner outlives anything one turn hits)
                with self.lock:
                    turn = self.current
                why = f"the agent server failed running a turn: {type(e).__name__}: {e}"
                if turn is not None:
                    log(traceback.format_exc())
                    self.fail(turn, f"{why}; send the message again")
                else:
                    # After its turn ended: nobody reads that turn any more.
                    self._troubled(TROUBLE_ERROR, f"the agent server hit an error between two turns: {type(e).__name__}: {e}; "
                                                  "fix what it names, then run the node again (or start the conversation over "
                                                  "from its card)")

    def _started(self, turn: Turn, config: dict):
        """The adapter `turn` runs on, started. One its own reader put down
        meanwhile (its agent ended on its own between two turns) is no
        failure of the turn: it starts on a new one, once. One retired by
        a reset or a configure fails it: the turn belongs to what was
        stopped, and no new agent is prompted for it."""
        for attempt in range(2):
            with self.lock:
                # No deadline: a stop is bounded by the agent's grace.
                while self.retiring:
                    self.lock.wait()
                if self.trouble:
                    raise Refused(self.troubles(), 503)
                if self.adapter is None:
                    self.adapter = adapters.KINDS[config["agent"]](self, config, turn.tools)
                adapter = self.adapter
            try:
                adapter.ensure_started()
                return adapter
            except adapters.Retired as retired:
                if attempt or not retired.on_its_own:
                    raise
                with self.lock:
                    if self.adapter is adapter:
                        self.adapter = None

    def _run_turn(self) -> None:
        """Take the next turn, run it, and wait for it to end."""
        with self.lock:
            while not self.queue:
                self.lock.wait()
            turn = self.queue.popleft()
            self.current = turn
            turn.status = ev.WORKING
            config = self.config
            if turn.start_over:
                self._forget_conversation()
                self.save_state()
            stale = self.adapter is not None and (turn.start_over or not self.adapter.serves(turn.tools))
            adapter = self._take_adapter() if stale else None
            self.lock.notify_all()
        if self._retire(adapter) is not None:
            # It failed the turn, and every one queued behind it; the stop
            # that failed is this turn's step after it.
            return
        try:
            adapter = self._started(turn, config)
            self.remember(turn, {"role": "user", "content": turn.message})
            adapter.start_turn(turn)
        except Refused as refused:
            self.fail(turn, str(refused))
        except Exception as e:  # noqa: BLE001 (any failure is the turn's, said where the node reads it)
            log(traceback.format_exc())
            self.fail(turn, f"the agent could not start this turn: {type(e).__name__}: {e}")
        with self.lock:
            while turn.status not in ev.ENDED:
                self.lock.wait()
            # A turn that did not end done may have left the agent in the
            # middle of it (weft failed it on its own side: a line it
            # could not read, a request that took too long, an answer
            # the agent refused), and whatever that agent writes next
            # would be credited to the next turn. So its agent is
            # stopped, and the next turn starts it again on the kept
            # conversation, as after a restart. A configuration changed
            # during the turn takes effect the same way.
            retire = turn.status != ev.DONE or self.restart_after_turn
            self.restart_after_turn = False
            ran = self._take_adapter() if retire else self.adapter
        if retire:
            self._retire(ran)

    # ----- the card -------------------------------------------------------

    def live_items(self) -> list:
        with self.lock:
            turn = self.current
            kind = (self.config or {}).get("agent", "agent")
            items = [{"type": "text", "label": "Agent", "data": adapters.label(kind)}]
            if turn is None:
                status = "idle" if self.config else "not configured yet"
            elif turn.pending is not None:
                status = "waiting on " + ("a tool call" if turn.pending.kind == ev.TOOL_CALL else "an answer")
            else:
                status = "working"
            if turn is not None and turn.notice:
                status += f" ({turn.notice})"
            if self.queue:
                status += f", {len(self.queue)} message{'s' if len(self.queue) > 1 else ''} waiting their turn"
            items.append({"type": "text", "label": "Status", "data": status})
            messages = len(self.state.get("history", []))
            items.append({
                "type": "text",
                "label": "Conversation",
                "data": f"{messages} message{'s' if messages != 1 else ''}" if self.state.get("conversation") else "not started",
                "action": {
                    "label": "Start over",
                    "actionKind": START_OVER,
                    "confirm": "Start a new conversation? The agent forgets this one, and a turn it is working on stops.",
                },
            })
            return items


def turn_answer(turn: Turn, news: list, after: int) -> dict:
    """What `/events` says of `turn` to a reader that read up to `after`.
    The lock is held."""
    pending = turn.pending.event if turn.pending is not None else None
    # Whether a reader that read up to `after` has anything to act on: what
    # a parked reader's poll waits for.
    fresh = bool(news) or turn.status in ev.ENDED or (pending is not None and pending["n"] > after)
    return {"turn": turn.id, "status": turn.status, "events": news, "pending": pending,
            "error": turn.error, "reply": turn.reply, "news": fresh}


def lost(turn_id: str) -> dict:
    # An answer the waiting node can act on, never a 404 its checks would
    # retry for ever: the turns live in this process, so a restarted server
    # has none of the old ones.
    return {"turn": turn_id, "status": ev.LOST, "events": [], "pending": None,
            "error": "the agent server restarted in the middle of this turn, so the turn is lost; "
                     "send the message again (the conversation itself is kept)",
            "reply": None, "news": True}


class Server(http.server.ThreadingHTTPServer):
    def handle_error(self, request, client_address) -> None:
        # A client that hangs up mid-request (an agent's MCP client resets
        # its connections) is no error of ours; anything else is logged.
        if isinstance(sys.exc_info()[1], (ConnectionResetError, BrokenPipeError)):
            return
        super().handle_error(request, client_address)


class Handler(http.server.BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"
    conversation = None

    def _send(self, status: int, body) -> None:
        payload = json.dumps(body).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(payload)))
        self.end_headers()
        self.wfile.write(payload)

    def _body(self) -> dict:
        length = max(0, int(self.headers.get("Content-Length", "0")))
        raw = self.rfile.read(length) if length else b""
        try:
            body = json.loads(raw or b"{}")
        except json.JSONDecodeError as e:
            raise Refused(f"the body is not JSON: {e}", 400)
        if not isinstance(body, dict):
            raise Refused("the body is not a JSON object", 400)
        return body

    def do_GET(self) -> None:  # noqa: N802 (the stdlib's spelling)
        url = urllib.parse.urlsplit(self.path)
        query = urllib.parse.parse_qs(url.query)
        conv = self.conversation
        try:
            if url.path == HEALTH_PATH:
                # SYNC: the /health answer's shape (`ok` true, a string `epoch`, a boolean `configured`)
                #       <-> catalog/agents/agent.rs (HEALTH_CHECK_PY, is_agent_server_health)
                # Built under the lock, written after: a client that stops
                # reading must never hold the conversation.
                with conv.lock:
                    health = {"ok": True, "epoch": conv.epoch, "configured": conv.config is not None,
                              "agent": (conv.config or {}).get("agent")}
                self._send(200, health)
            elif url.path == EVENTS_PATH:
                turn_id = (query.get("turn") or [""])[0]
                after = int((query.get("after") or ["0"])[0])
                wait = min(float((query.get("wait") or ["0"])[0]), MAX_WAIT_SECS)
                self._send(200, conv.read(turn_id, after, wait))
            elif url.path == HISTORY_PATH:
                # Always one turn's: the next queued turn may have begun by
                # the time a reader asks, and added its message.
                turn_id = (query.get("turn") or [""])[0]
                if not turn_id:
                    raise Refused("a history read names its turn: /history?turn=<the turn id /turns answered>", 400)
                self._send(200, {"messages": conv.history(turn_id)})
            elif url.path == QUESTION_PATH:
                question_id = (query.get("id") or [""])[0]
                if not question_id:
                    raise Refused("a question read names its question: /question?id=<the question's id>", 400)
                self._send(200, conv.question(question_id))
            elif url.path == LIVE_PATH:
                self._send(200, {"items": conv.live_items()})
            else:
                self._send(404, {"error": f"no route {url.path}"})
        except Refused as refused:
            self._send(refused.status, {"error": str(refused)})
        except ValueError as e:
            self._send(400, {"error": f"a query parameter is not a number: {e}"})

    def do_DELETE(self) -> None:  # noqa: N802
        # A streamable-HTTP MCP client ends its session with a DELETE.
        self._send(200, {})

    def do_POST(self) -> None:  # noqa: N802
        conv = self.conversation
        path = urllib.parse.urlsplit(self.path).path
        try:
            # A web page can post a form or plain text anywhere without
            # asking first, and JSON only after asking (a CORS preflight,
            # which this server never answers): refusing anything but JSON
            # keeps a page the person visits from starting a turn.
            if self.headers.get("Content-Type") is None or self.headers.get_content_type() != "application/json":
                raise Refused("this server takes a JSON body (Content-Type: application/json), and got "
                              f"{self.headers.get('Content-Type') or 'no Content-Type'}", 415)
            body = self._body()
            if path == CONFIGURE_PATH:
                self._send(200, conv.configure(body))
            elif path == TURNS_PATH:
                self._send(200, self._turns(body))
            elif path == LOGIN_PATH:
                self._send(200, conv.take_login(body))
            elif path == RESET_PATH:
                conv.reset("the conversation was started over")
                self._send(200, {"ok": True})
            elif path == ACTION_PATH:
                # SYNC: the /action envelope <-> crates/weft-core/src/infra/action.rs
                if body.get("action") != START_OVER:
                    raise Refused(f"no action {body.get('action')!r}; the card has one: {START_OVER}", 400)
                conv.reset("the conversation was started over from the agent's card")
                self._send(200, {"result": {"startedOver": True}})
            elif path == MCP_PATH:
                answer = conv.mcp.handle(body)
                if answer is None:
                    self.send_response(202)
                    self.send_header("Content-Length", "0")
                    self.end_headers()
                else:
                    self._send(200, answer)
            else:
                self._send(404, {"error": f"no route {path}"})
        except Refused as refused:
            self._send(refused.status, {"error": str(refused)})
        except Exception as e:  # noqa: BLE001 (any failure is the caller's answer and a log line)
            log(traceback.format_exc())
            self._send(500, {"error": f"{type(e).__name__}: {e}"})

    def _turns(self, body: dict) -> dict:
        conv = self.conversation
        given = [key for key in ("message", "toolResult", "answer") if key in body]
        if len(given) != 1:
            raise Refused("a turn request carries exactly one of message, toolResult and answer", 400)
        # SYNC: the /turns `key` <-> catalog/agents/ask_agent/mod.rs (the key AskAgent posts with every turn request)
        key = body.get("key")
        if key is not None and (not isinstance(key, str) or not key):
            raise Refused("a turn request's key is a non-empty string: the same for a post repeated after a crash", 400)
        if "message" in body:
            tools = body.get("tools") or []
            if not isinstance(tools, list):
                raise Refused("tools is a list of {name, description, parameters}", 400)
            return conv.submit(body["message"], tools, bool(body.get("startOver")), key)
        if "toolResult" in body:
            result = body["toolResult"]
            if not isinstance(result, dict) or not isinstance(result.get("callId"), str):
                raise Refused("toolResult is {callId, output, isError?}", 400)
            return conv.deliver(ev.TOOL_CALL, result["callId"], result, key)
        answer = body["answer"]
        if not isinstance(answer, dict) or not isinstance(answer.get("questionId"), str):
            raise Refused("answer is {questionId, answers?, choice?, note?, dropped?}", 400)
        return conv.deliver(ev.QUESTION, answer["questionId"], answer, key)

    def log_message(self, fmt: str, *args) -> None:
        # A live reader asks every few seconds; failures are logged where
        # they happen.
        pass


class Mcp:
    """The program's own tools, as an MCP server: the program's route.

    Every call of the program's tools that reaches weft without a tool
    callback of its agent's own comes through here, and coming through here
    is what makes a call the program's (`Conversation.block` writes it into
    the history under its plain name). The agent's MCP client posts to it
    over HTTP; Claude Code's SDK channel hands it each message
    (`adapter_claude`); a plugin inside an agent forwards its calls to it.

    The contract a plugin relies on, as served:

      URL      `adapters.mcp_url()`: http://127.0.0.1:<port>/mcp, handed to
               the plugin by the adapter that writes it.
      Request  one JSON-RPC 2.0 message per POST, Content-Type
               `application/json` (anything else is refused, 415). No
               `initialize` or session is needed first, and nothing is kept
               between posts: a plain call is
               {"jsonrpc": "2.0", "id": <any>, "method": "tools/call",
                "params": {"name": "<the tool's plain name>", "arguments": {...}}}
      Answer   {"jsonrpc": "2.0", "id": <the same>,
                "result": {"content": [{"type": "text", "text": "<output>"}],
                           "isError": <bool>}}
               plus "structuredContent" when the output is JSON: an object
               as it is, any other value as {"result": <it>}
               (`adapters.mcp_result`).
               A name that is not one of the running turn's tools, or a call
               with no turn running, is answered the same way with
               `isError` true and why in the text.
      Holding  the answer comes once weft answers the call, which may be
               hours later (a person answering): the request is held open
               for as long as that takes, and the client must not time it
               out. If the turn ends first, the answer says so, `isError`.
      Others   `initialize`, `ping` and `tools/list` are answered; a
               notification (no `id`) gets an empty 202; every other method
               is the JSON-RPC error -32601 "method not found", never an
               empty result (Goose sends `server/discover` first, and drops
               a server that answers it with a result).
    """

    def __init__(self, conversation: Conversation):
        self.conv = conversation

    def handle(self, body: dict):
        """The answer to one JSON-RPC message, or None for a notification."""
        method, rid = body.get("method"), body.get("id")
        if rid is None:
            return None
        if method == "initialize":
            version = (body.get("params") or {}).get("protocolVersion") or "2025-06-18"
            return self._result(rid, {"protocolVersion": version, "capabilities": {"tools": {}},
                                      "serverInfo": {"name": adapters.MCP_SERVER, "version": "1"}})
        if method == "ping":
            return self._result(rid, {})
        if method == "tools/list":
            return self._result(rid, {"tools": [adapters.mcp_tool(t) for t in self._tools()]})
        if method == "tools/call":
            params = body.get("params") or {}
            return self._result(rid, self._call(params.get("name"), params.get("arguments") or {}))
        return {"jsonrpc": "2.0", "id": rid, "error": {"code": -32601, "message": f"method not found: {method}"}}

    def _tools(self) -> list:
        turn = self.conv.running()
        if turn is not None:
            return turn.tools
        adapter = self.conv.adapter
        return adapter.tools if adapter is not None else []

    def _call(self, name: str, arguments: dict) -> dict:
        turn = self.conv.running()
        if turn is None:
            return {"content": [{"type": "text", "text": "no turn is running, so nothing can answer this tool"}], "isError": True}
        # The program's tools are its list of names, nothing else.
        if name not in adapters.ProgramTools(turn.tools):
            return {"content": [{"type": "text", "text": f"no tool {name!r} in this turn"}], "isError": True}
        answered = threading.Event()
        box = {}

        def answer(result: dict) -> None:
            box["result"] = result
            answered.set()

        call_id = "call-" + uuid.uuid4().hex[:12]
        try:
            self.conv.block(turn, ev.TOOL_CALL, call_id,
                            {"callId": call_id, "name": name, "arguments": json.dumps(arguments)}, answer)
        except Refused as refused:
            # Its turn ended meanwhile: the agent gets the error as the call's result.
            return {"content": [{"type": "text", "text": str(refused)}], "isError": True}
        # No deadline: weft answers when the program (or a person) does.
        while not answered.wait(60):
            if turn.status in ev.ENDED:
                return {"content": [{"type": "text", "text": "the turn ended before this tool was answered"}], "isError": True}
        return adapters.mcp_result(box["result"])

    @staticmethod
    def _result(rid, result) -> dict:
        return {"jsonrpc": "2.0", "id": rid, "result": result}


def mcp_bridge(url: str) -> None:
    """Stand in as a stdio MCP server, forwarding every message to `url`."""
    for line in sys.stdin:
        line = line.strip()
        if not line:
            continue
        request = urllib.request.Request(url, data=line.encode("utf-8"), method="POST",
                                         headers={"Content-Type": "application/json", "Accept": "application/json"})
        try:
            with urllib.request.urlopen(request, timeout=None) as answer:
                body = answer.read()
        except urllib.error.URLError as e:
            message = json.loads(line)
            if message.get("id") is None:
                continue
            body = json.dumps({"jsonrpc": "2.0", "id": message["id"],
                               "error": {"code": -32000, "message": f"the agent server did not answer: {e}"}}).encode()
        if body:
            sys.stdout.write(body.decode("utf-8") + "\n")
            sys.stdout.flush()


def main() -> None:
    parser = argparse.ArgumentParser()
    sub = parser.add_subparsers(dest="command")
    bridge = sub.add_parser("mcp-bridge")
    bridge.add_argument("url")
    parser.add_argument("--port", type=int)
    parser.add_argument("--home")
    parser.add_argument("--app-file", help="write the port listened on here once listening")
    args = parser.parse_args()
    if args.command == "mcp-bridge":
        mcp_bridge(args.url)
        return
    # SYNC: the agent server's environment <-> catalog/agents/agent.rs (PORT_ENV). WEFT_AGENT_HOME has no Rust peer.
    port = args.port if args.port is not None else int(os.environ.get("WEFT_AGENT_PORT", "0"))
    home = args.home or os.environ.get("WEFT_AGENT_HOME") or os.path.join(os.getcwd(), "state")
    conversation = Conversation(home)
    Handler.conversation = conversation
    server = Server(("127.0.0.1", port), Handler)
    server.daemon_threads = True
    adapters.SERVER_URL = f"http://127.0.0.1:{server.server_address[1]}"
    if args.app_file:
        adapters.write_whole(args.app_file, str(server.server_address[1]), 0o644)
    log(f"listening on {server.server_address[1]}, keeping the conversation in {home}")
    # A SIGTERM (what stops the server, locally and in a computer) ends
    # the loop below rather than the process on the spot, so the agent is
    # stopped first: what it started outside this process's group would
    # outlive a kill of the group, and its sockets would stay behind.
    signal.signal(signal.SIGTERM, _stopping)
    try:
        server.serve_forever()
    except Stopping:
        # A second SIGTERM never cuts the stop short: whoever sent it
        # kills what is left once its grace runs out.
        signal.signal(signal.SIGTERM, signal.SIG_IGN)
        log("stopping: the agent first")
        conversation.close()
        server.server_close()
        log("stopped")


class Stopping(BaseException):
    """The SIGTERM, raised out of the serve loop on the main thread. Not an
    `Exception`: nothing on the way may take it for a failure to recover from."""


def _stopping(signum, frame) -> None:
    raise Stopping()


if __name__ == "__main__":
    main()
