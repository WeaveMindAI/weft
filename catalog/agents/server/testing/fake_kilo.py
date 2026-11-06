"""A stand-in for `kilo serve`: Kilo Code's own server, HTTP and an event
stream, replayed from what Kilo Code CLI 7.8.18 really sent (three turns
on the person's own login with `openrouter/z-ai/glm-5.3-flash`, the rest
against a scripted stand-in model): the same requests, the same events in
the same order, the same shapes, trimmed to the fields anybody reads. It
holds none of Kilo's logic beyond that.

It prints `kilo server listening on http://127.0.0.1:<port>` and answers
only `$KILO_SERVER_USERNAME:$KILO_SERVER_PASSWORD` (HTTP basic). Each
turn's message says what its model does, by keyword, in this order:

  "think"        reasons first (a `reasoning` part)
  "call:<name>"  calls the tool <name>: a tool of weft's plugin (the loader
                 named in `KILO_CONFIG_CONTENT`), whose call it forwards to
                 the route as the plugin does, else a built-in it runs itself
  "ask"          asks with its question tool (`question.asked`), and waits
  "run"          runs `echo hi` with `bash`, asking first
                 (`permission.asked`) unless `KILO_PERMISSION` allows it;
                 refused without a note it ends the turn there
  "write"        writes `made.txt` with `write`
  "sub:<name>"   a sub-agent (`task`, a session of its own) calls <name>
                 and says "sub done"
  "offline"      loses the model's provider (`session.network.asked`, a
                 `session.status` of `offline`), then goes on by itself
                 once it is back, as Kilo does (`session.network.restored`,
                 then `session.network.replied` from its own resume)
  "retry"        retries a refused request (a `session.status` of `retry`)
  "fail"         ends the turn with a `session.error`
  "crash"        exits in the middle of the turn

and replies with what it was answered, one line each, streamed in two
`message.part.delta` pieces. The user message names the agent and the
model the turn runs on: the prompt's, else the session's own, else the
first agent of `/agent` and `fake/fake-model`. `/agent` lists
`$FAKE_KILO_AGENTS` (comma-separated primary agents, Kilo's own when
unset). `/config/providers` lists `fake/fake-model` (effort levels `high`,
`low`) and `fake/other-model` (none).

A session lives in `$FAKE_LOG`'s folder (`kilo-sessions.json`), so a
stand-in started again finds it, and an unknown id is refused as Kilo
refuses it. `$FAKE_LOG` gets the start (`{"start": {args, env}}`) and every
request it was sent (`{"method", "path", "body"}`), one JSON line each.
"""

import base64
import http.server
import json
import os
import queue
import re
import sys
import threading
import urllib.request
import uuid

LOG = os.environ["FAKE_LOG"]
SESSIONS = os.path.join(os.path.dirname(LOG), "kilo-sessions.json")
KILOS_OWN = "code,ask,debug,orchestrator,plan"
LOCK = threading.Lock()
SUBSCRIBERS = []
# Kilo's id of each request it waits on the client for -> its answer box.
WAITING = {}


def logged(entry):
    with LOCK, open(LOG, "a") as f:
        f.write(json.dumps(entry) + "\n")


def ident(prefix):
    return f"{prefix}_{uuid.uuid4().hex[:24]}"


def emit(kind, properties):
    event = {"id": ident("evt"), "type": kind, "properties": properties}
    for subscriber in list(SUBSCRIBERS):
        subscriber.put(event)


def sessions():
    try:
        with open(SESSIONS) as f:
            return json.load(f)
    except FileNotFoundError:
        return {}


def save(all_sessions):
    with open(SESSIONS, "w") as f:
        json.dump(all_sessions, f)


def agents():
    names = (os.environ.get("FAKE_KILO_AGENTS") or KILOS_OWN).split(",")
    listed = [{"name": n, "description": f"the {n} agent", "mode": "primary", "native": n in KILOS_OWN.split(","),
               "permission": [], "options": {}} for n in names]
    return listed + [{"name": "general", "description": "General-purpose agent", "mode": "subagent", "native": True,
                      "permission": [], "options": {}},
                     {"name": "title", "mode": "primary", "native": True, "hidden": True, "permission": [], "options": {}}]


def plugin_tools():
    """The tools of weft's plugin, and its route: what its loader hands `make`."""
    content = json.loads(os.environ.get("KILO_CONFIG_CONTENT") or "{}")
    for uri in content.get("plugin") or []:
        with open(uri.removeprefix("file://")) as f:
            text = f.read()
        made = json.loads(text[text.index("make(") + len("make("):text.rindex(");")])
        return made["url"], {t["name"] for t in made["tools"]}
    return None, set()


def allowed(permission):
    rules = json.loads(os.environ.get("KILO_PERMISSION") or "{}")
    return rules.get(permission, rules.get("*")) == "allow"


class Turn:
    def __init__(self, session, body, parent=None):
        self.session = session
        self.body = body
        self.parent = parent
        self.message = ident("msg")
        self.lines = []

    def part(self, part):
        emit("message.part.updated", {"sessionID": self.session, "part": {"sessionID": self.session, "messageID": self.message, **part},
                                      "time": 0})

    def tool(self, name, call_id, state):
        self.part({"id": f"prt_{call_id}", "type": "tool", "callID": call_id, "tool": name, "state": state})

    def wait(self, request_id):
        box = WAITING[request_id] = queue.Queue()
        return box.get()

    def text(self, text, streamed=True):
        part_id = ident("prt")
        self.part({"id": part_id, "type": "text", "text": "", "time": {"start": 1}})
        if streamed:
            half = len(text) // 2
            for piece in (text[:half], text[half:]):
                emit("message.part.delta", {"sessionID": self.session, "messageID": self.message, "partID": part_id,
                                            "field": "text", "delta": piece})
        self.part({"id": part_id, "type": "text", "text": text, "time": {"start": 1, "end": 2}})

    def close(self, reason):
        emit("session.status", {"sessionID": self.session, "status": {"type": "idle"}})
        emit("session.idle", {"sessionID": self.session})
        emit("session.turn.close", {"sessionID": self.session, **({"parentID": self.parent} if self.parent else {}), "reason": reason})

    def run(self):
        all_sessions = sessions()
        kept = all_sessions[self.session]
        agent = self.body.get("agent") or kept.get("agent") or agents()[0]["name"]
        model = self.body.get("model") or kept.get("model") or {"providerID": "fake", "modelID": "fake-model"}
        kept.update(agent=agent, model=model)
        save(all_sessions)
        user = ident("msg")
        emit("message.updated", {"sessionID": self.session, "info": {
            "id": user, "sessionID": self.session, "role": "user", "time": {"created": 1}, "agent": agent,
            "model": {**model, **({"variant": self.body["variant"]} if self.body.get("variant") else {})}}})
        text = "".join(p.get("text", "") for p in self.body.get("parts") or [])
        emit("message.part.updated", {"sessionID": self.session, "part": {
            "id": ident("prt"), "sessionID": self.session, "messageID": user, "type": "text", "text": text}, "time": 1})
        emit("session.turn.open", {"sessionID": self.session})
        emit("session.status", {"sessionID": self.session, "status": {"type": "busy"}})
        emit("message.updated", {"sessionID": self.session, "info": {
            "id": self.message, "sessionID": self.session, "role": "assistant", "time": {"created": 1}, "parentID": user,
            "modelID": model["modelID"], "providerID": model["providerID"], "mode": agent, "agent": agent}})
        self.part({"id": ident("prt"), "type": "step-start"})
        for word in text.split():
            if word == "crash":
                os._exit(1)
            ended = self.act(word)
            if ended:
                self.close(ended)
                return
        self.text("\n".join(self.lines) or "done")
        self.part({"id": ident("prt"), "type": "step-finish", "reason": "stop"})
        self.close("completed")

    def act(self, word):
        """One keyword; how the turn ends, when it ends there."""
        if word == "think":
            part_id = ident("prt")
            self.part({"id": part_id, "type": "reasoning", "text": "", "time": {"start": 1}})
            # Its reasoning streams as `text` deltas too, as a text part's does.
            emit("message.part.delta", {"sessionID": self.session, "messageID": self.message, "partID": part_id,
                                        "field": "text", "delta": "Thinking it over."})
            self.part({"id": part_id, "type": "reasoning", "text": "Thinking it over.", "time": {"start": 1, "end": 2}})
        elif word.startswith("call:"):
            self.call(word[len("call:"):])
        elif word == "ask":
            call_id = ident("call")
            asked = ident("que")
            questions = [{"question": "Red or blue?", "header": "Color",
                          "options": [{"label": "Red", "description": "warm"}, {"label": "Blue", "description": "cold"}]}]
            emit("question.asked", {"id": asked, "sessionID": self.session, "questions": questions,
                                    "tool": {"messageID": self.message, "callID": call_id}})
            self.tool("question", call_id, {"status": "running", "input": {"questions": questions}, "time": {"start": 1}})
            answer = self.wait(asked)
            if answer is None:
                emit("question.rejected", {"sessionID": self.session, "requestID": asked})
                self.tool("question", call_id, {"status": "error", "input": {"questions": questions},
                                                "error": "The user dismissed this question", "time": {"start": 1, "end": 2}})
                self.lines.append("ask: dismissed")
            else:
                emit("question.replied", {"sessionID": self.session, "requestID": asked, "answers": answer["answers"]})
                self.tool("question", call_id, {"status": "completed", "input": {"questions": questions},
                                                "output": f"User has answered your questions: {json.dumps(answer['answers'])}",
                                                "title": "Asked 1 question", "metadata": {"answers": answer["answers"]},
                                                "time": {"start": 1, "end": 2}})
                self.lines.append(f"ask: {json.dumps(answer['answers'])}")
        elif word == "run":
            return self.run_command()
        elif word == "write":
            call_id = ident("call")
            self.tool("write", call_id, {"status": "running", "input": {"filePath": "made.txt", "content": "x"}, "time": {"start": 1}})
            self.tool("write", call_id, {"status": "completed", "input": {"filePath": "made.txt", "content": "x"},
                                         "output": "Wrote file successfully.", "title": "made.txt",
                                         "metadata": {"filepath": "made.txt", "exists": False, "filediff": {"file": "made.txt"}},
                                         "time": {"start": 1, "end": 2}})
            emit("file.edited", {"file": "made.txt"})
        elif word.startswith("sub:"):
            self.sub(word[len("sub:"):])
        elif word == "offline":
            asked = ident("que")
            emit("session.network.asked", {"id": asked, "sessionID": self.session, "message": "Connection refused",
                                           "restored": False, "time": {"created": 1}})
            emit("session.status", {"sessionID": self.session, "status": {"type": "offline", "requestID": asked,
                                                                          "message": "Connection refused"}})
            emit("session.network.restored", {"sessionID": self.session, "requestID": asked, "time": 2})
            emit("session.network.replied", {"sessionID": self.session, "requestID": asked})
            emit("session.status", {"sessionID": self.session, "status": {"type": "busy"}})
            self.lines.append("offline: back")
        elif word == "retry":
            emit("session.status", {"sessionID": self.session, "status": {
                "type": "retry", "attempt": 2, "message": "Provider is overloaded", "next": 3}})
            self.lines.append("retry: done")
        elif word == "fail":
            emit("session.error", {"sessionID": self.session, "error": {"name": "APIError", "data": {
                "message": "Incorrect API key provided", "statusCode": 401, "isRetryable": False}}})
            return "error"
        return None

    def call(self, name):
        call_id = ident("call")
        arguments = {"q": "x"}
        self.tool(name, call_id, {"status": "pending", "input": {}, "raw": ""})
        self.tool(name, call_id, {"status": "running", "input": arguments, "time": {"start": 1}})
        url, ours = plugin_tools()
        if name in ours:
            # What weft's plugin does with a call of one of its tools.
            body = json.dumps({"jsonrpc": "2.0", "id": "weft-1", "method": "tools/call",
                               "params": {"name": name, "arguments": arguments}}).encode()
            request = urllib.request.Request(url, data=body, method="POST", headers={"Content-Type": "application/json"})
            with urllib.request.urlopen(request, timeout=None) as answer:
                result = json.loads(answer.read())["result"]
            output = "\n".join(c.get("text", "") for c in result["content"])
            failed = result.get("isError")
        else:
            output, failed = f"built-in {name} ran", False
        state = {"status": "error", "error": output} if failed else {"status": "completed", "output": output, "title": "",
                                                                      "metadata": {"truncated": False}}
        self.tool(name, call_id, {**state, "input": arguments, "time": {"start": 1, "end": 2}})
        self.lines.append(f"{name}: {output}" if not failed else f"{name} failed: {output}")

    def run_command(self):
        call_id = ident("call")
        arguments = {"command": "echo hi", "description": "say hi"}
        self.tool("bash", call_id, {"status": "running", "input": arguments, "time": {"start": 1}})
        if not allowed("bash"):
            asked = ident("per")
            emit("permission.asked", {"id": asked, "sessionID": self.session, "permission": "bash", "patterns": ["echo hi"],
                                      "metadata": arguments, "always": ["echo *"],
                                      "tool": {"messageID": self.message, "callID": call_id}})
            reply = self.wait(asked)
            emit("permission.replied", {"sessionID": self.session, "requestID": asked, "reply": reply["reply"]})
            if reply["reply"] == "reject":
                note = reply.get("message")
                error = ("The user rejected permission to use this specific tool call"
                         + (f" with the following feedback: {note}" if note else "."))
                self.tool("bash", call_id, {"status": "error", "input": arguments, "error": error, "time": {"start": 1, "end": 2}})
                if not note:
                    self.part({"id": ident("prt"), "type": "step-finish", "reason": "tool-calls"})
                    return "completed"
                self.lines.append(f"run: refused, {note}")
                return None
        self.tool("bash", call_id, {"status": "completed", "input": arguments, "output": "hi\n", "title": "say hi",
                                    "metadata": {"output": "hi\n", "exit": 0}, "time": {"start": 1, "end": 2}})
        self.lines.append("run: hi")
        return None

    def sub(self, name):
        call_id = ident("call")
        child = ident("ses")
        all_sessions = sessions()
        all_sessions[child] = {"parentID": self.session}
        save(all_sessions)
        arguments = {"description": "look", "prompt": f"call {name}", "subagent_type": "general"}
        emit("session.created", {"sessionID": child, "info": {"id": child, "parentID": self.session, "agent": "general",
                                                              "title": "look (@general subagent)"}})
        self.tool("task", call_id, {"status": "running", "input": arguments, "time": {"start": 1}})
        inner = Turn(child, {"agent": "general"}, parent=self.session)
        inner.call(name)
        inner.text("sub done")
        inner.close("completed")
        self.tool("task", call_id, {"status": "completed", "input": arguments, "time": {"start": 1, "end": 2},
                                    "output": f'<task id="{child}" state="completed">\n<task_result>\nsub done\n</task_result>\n</task>'})
        self.lines.append("sub: sub done")


class Handler(http.server.BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"

    def log_message(self, *args):
        pass

    def answer(self, status, body=None):
        data = b"" if body is None else json.dumps(body).encode()
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(data)))
        self.end_headers()
        self.wfile.write(data)

    def authorized(self):
        wanted = f"{os.environ['KILO_SERVER_USERNAME']}:{os.environ['KILO_SERVER_PASSWORD']}"
        if self.headers.get("Authorization") == "Basic " + base64.b64encode(wanted.encode()).decode():
            return True
        self.answer(401, {"name": "Unauthorized"})
        return False

    def do_GET(self):  # noqa: N802
        logged({"method": "GET", "path": self.path})
        if not self.authorized():
            return
        if self.path == "/event":
            self.events()
        elif self.path == "/agent":
            self.answer(200, agents())
        elif self.path == "/config/providers":
            self.answer(200, {"providers": [{"id": "fake", "name": "fake", "models": {
                "fake-model": {"id": "fake-model", "providerID": "fake", "variants": {"high": {}, "low": {}}},
                "other-model": {"id": "other-model", "providerID": "fake"}}}], "default": {"fake": "fake-model"}})
        elif (found := re.fullmatch(r"/session/([^/]+)", self.path)):
            if found.group(1) in sessions():
                self.answer(200, {"id": found.group(1), "title": "New session"})
            else:
                self.answer(404, {"name": "NotFoundError", "data": {"message": f"Session not found: {found.group(1)}"}})
        else:
            self.answer(404, {"name": "NotFoundError", "data": {"message": f"no route {self.path}"}})

    def do_POST(self):  # noqa: N802
        length = int(self.headers.get("Content-Length") or 0)
        body = json.loads(self.rfile.read(length) or b"null") if length else None
        logged({"method": "POST", "path": self.path, "body": body})
        if not self.authorized():
            return
        if self.path == "/session":
            session = ident("ses")
            all_sessions = sessions()
            all_sessions[session] = {}
            save(all_sessions)
            emit("session.created", {"sessionID": session, "info": {"id": session, "title": "New session"}})
            self.answer(200, {"id": session, "title": "New session"})
        elif (found := re.fullmatch(r"/session/([^/]+)/prompt_async", self.path)):
            self.answer(204)
            threading.Thread(target=Turn(found.group(1), body).run, daemon=True).start()
        elif (found := re.fullmatch(r"/session/([^/]+)/abort", self.path)):
            emit("session.error", {"sessionID": found.group(1), "error": {"name": "MessageAbortedError", "data": {"message": "Aborted"}}})
            emit("session.turn.close", {"sessionID": found.group(1), "reason": "interrupted"})
            self.answer(200, True)
        elif (found := re.fullmatch(r"/(permission|question)/([^/]+)/(reply|reject)", self.path)):
            box = WAITING.pop(found.group(2), None)
            if box is None:
                self.answer(404, {"name": "NotFoundError", "data": {"message": f"Request not found: {found.group(2)}"}})
                return
            kind, verb = found.group(1), found.group(3)
            box.put(body if verb == "reply" else None if kind == "question" else verb)
            self.answer(200, True)
        else:
            self.answer(404, {"name": "NotFoundError", "data": {"message": f"no route {self.path}"}})

    def events(self):
        self.send_response(200)
        self.send_header("Content-Type", "text/event-stream")
        self.send_header("Transfer-Encoding", "chunked")
        self.end_headers()
        mine = queue.Queue()
        mine.put({"id": ident("evt"), "type": "server.connected", "properties": {}})
        SUBSCRIBERS.append(mine)
        try:
            while True:
                data = f"data: {json.dumps(mine.get())}\n\n".encode()
                self.wfile.write(b"%x\r\n%s\r\n" % (len(data), data))
                self.wfile.flush()
        except OSError:
            SUBSCRIBERS.remove(mine)


def main():
    logged({"start": {"args": sys.argv[1:], "env": {k: v for k, v in os.environ.items()
                                                    if k.startswith(("KILO_", "XDG_")) or k in ("HOME",) or k.endswith("_API_KEY")}}})
    server = http.server.ThreadingHTTPServer(("127.0.0.1", 0), Handler)
    server.daemon_threads = True
    print(f"kilo server listening on http://127.0.0.1:{server.server_address[1]}", flush=True)
    server.serve_forever()


if __name__ == "__main__":
    main()
