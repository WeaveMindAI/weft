"""A stand-in for one ACP agent, replaying the lines that agent really wrote.

Each agent's lines are a recording, `testing/recorded/<kind>.json`, cut
from the research transcripts (`source` says from where). This program
plays it: it answers each request of ours with the agent's recorded answer,
and a `session/prompt` whose text is the name of one of the recording's
turns plays that turn's steps, in order:

  {"update": {...}}        a `session/update` with this update
  {"send": {...}}          any other message the agent wrote
  {"ask": {"method", "params"}}
                           a request of the agent's, waiting for our answer
  {"tool": {"name", "arguments"}}
                           a call of the program's tool, over the MCP server
                           it was handed (HTTP, a command, or the place
                           `mcpFrom` says weft wrote it for a plugin)
  {"end": {...}}           the prompt's answer (its `stopReason`)
  {"fail": {...}}          the prompt's error
  {"exit": N}              the agent dies

`$SESSION` in a recorded line is the session's id, `$TOOL` the text the
program's tool answered, `$ANSWER` the JSON of the last answer we gave the
agent. A `session/load` or `session/resume` plays the recording's `replay`:
`before` its answer, then `after` it (as agents really do). A plugin of the
agent's that says it loaded (`pluginLoaded`) does so as the session is made.

`$FAKE_LOG` gets one JSON line for the start (`args`, `env`, `cwd`), then
every message it was sent, so a test reads exactly what reached the agent.
"""

import json
import os
import subprocess
import sys
import urllib.parse
import urllib.request
import uuid

with open(os.environ["FAKE_RECORDING"], encoding="utf-8") as _file:
    RECORDING = json.load(_file)
NEXT = [9000]
STATE = {"servers": {}, "tool": "", "answer": ""}


def log(entry):
    with open(os.environ["FAKE_LOG"], "a", encoding="utf-8") as f:
        f.write(json.dumps(entry) + "\n")


def fill(value, session):
    """A recorded line with its placeholders filled in."""
    if isinstance(value, str):
        return (value.replace("$SESSION", session or "").replace("$TOOL", STATE["tool"]).replace("$ANSWER", STATE["answer"]))
    if isinstance(value, list):
        return [fill(v, session) for v in value]
    if isinstance(value, dict):
        return {k: fill(v, session) for k, v in value.items()}
    return value


def out(message):
    sys.stdout.write(json.dumps({"jsonrpc": "2.0", **message}) + "\n")
    sys.stdout.flush()


def read():
    line = sys.stdin.readline()
    if not line:
        sys.exit(0)
    message = json.loads(line)
    log(message)
    return message


def update(session, body):
    out({"method": "session/update", "params": {"sessionId": session, "update": fill(body, session)}})


def answer(rid, method, params):
    """Our answer to a request: the recorded one, or method-not-found."""
    errors = RECORDING.get("errors") or {}
    if method in errors:
        out({"id": rid, "error": errors[method]})
        return None
    answers = RECORDING.get("answers") or {}
    if method not in answers:
        out({"id": rid, "error": {"code": -32601, "message": f"Method not found: {method}"}})
        return None
    session = params.get("sessionId") or (str(uuid.uuid4()) if method == "session/new" else None)
    if method in ("session/new", "session/load", "session/resume"):
        STATE["servers"][session] = params.get("mcpServers") or []
        plugin_loaded(session)
        replay = RECORDING.get("replay") or {}
        if method != "session/new":
            for body in replay.get("before") or []:
                update(session, body)
    out({"id": rid, "result": fill(answers[method], session)})
    if method in ("session/load", "session/resume"):
        for body in (RECORDING.get("replay") or {}).get("after") or []:
            update(session, body)
    return session


def call_tool(session, name, arguments):
    """The program's tool, over the MCP server this agent was handed."""
    server = mcp_server(session)
    call = {"jsonrpc": "2.0", "id": 2, "method": "tools/call", "params": {"name": name, "arguments": arguments}}
    if server.get("url"):
        request = urllib.request.Request(server["url"], data=json.dumps(call).encode(), method="POST",
                                         headers={"Content-Type": "application/json"})
        with urllib.request.urlopen(request, timeout=None) as reply:
            result = json.loads(reply.read())["result"]
    else:
        env = dict(os.environ, **({e["name"]: e["value"] for e in server.get("env") or []} if isinstance(server.get("env"), list)
                                  else server.get("env") or {}))
        bridge = subprocess.Popen([server["command"], *server.get("args", [])], stdin=subprocess.PIPE, stdout=subprocess.PIPE, text=True, env=env)
        init = {"jsonrpc": "2.0", "id": 1, "method": "initialize", "params": {"protocolVersion": "2025-06-18", "capabilities": {},
                                                                              "clientInfo": {"name": "fake", "version": "1"}}}
        bridge.stdin.write(json.dumps(init) + "\n" + json.dumps(call) + "\n")
        bridge.stdin.flush()
        bridge.stdout.readline()
        result = json.loads(bridge.stdout.readline())["result"]
        bridge.stdin.close()
        bridge.stdout.close()
        bridge.wait()
    STATE["tool"] = "".join(part.get("text", "") for part in result.get("content") or [])
    log({"toolResult": result})


def mcp_server(session) -> dict:
    """Where the program's route is (`mcpFrom`): an MCP server handed in the
    session (the default), or what weft wrote for a plugin of the agent's
    own, found at one place (`place`): the tool list (`list`, the JSON
    `program_tools.listing` writes) or a loader that hands it to a plugin
    (`loader`, `export default make({...});`). Either holds the route's `url`."""
    source = RECORDING.get("mcpFrom") or "session"
    if source == "session":
        servers = [s for s in STATE["servers"].get(session) or [] if s.get("name") == "weft"]
        return servers[0]
    (kind, where), = source.items()
    found = place(where)
    if kind == "list":
        return found if isinstance(found, dict) else json.loads(found)
    if kind == "loader":
        return json.loads(found[found.index("make(") + len("make("):found.rindex(");")])
    raise SystemExit(f"fake_acp_replay: mcpFrom takes list or loader, and got {kind!r}")


def plugin_loaded(session) -> None:
    """A plugin of the agent's own that loads as the session is made and
    says so (`pluginLoaded`: the place it writes the names of the tools it
    registered), when weft gave it a tool list (`mcpFrom`)."""
    loaded = RECORDING.get("pluginLoaded")
    if loaded is None:
        return
    try:
        listed = mcp_server(session)
    except FileNotFoundError:
        # No tools, so weft wrote no list and the plugin is not there.
        return
    path = os.path.join(os.environ[loaded["home"]], loaded["file"])
    with open(path, "w", encoding="utf-8") as f:
        json.dump([tool["name"] for tool in listed["tools"]], f)


def place(where: dict):
    """What weft wrote where `where` says: a file at `file` in the folder the
    variable `home` names, or beside the file the variable `besideEnv`
    names; the file the flag `flag` names; or the JSON in the variable
    `env`. With `key`, the JSON there, followed down `key`: a value that is
    text is a path (or a `file://` URL; a folder's file is its
    `index.mjs`) read in turn, any other is what was written."""
    if "env" in where:
        text = os.environ[where["env"]]
    else:
        if "home" in where:
            path = os.path.join(os.environ[where["home"]], where["file"])
        elif "besideEnv" in where:
            path = os.path.join(os.path.dirname(os.environ[where["besideEnv"]]), where["file"])
        elif "flag" in where:
            args = sys.argv[1:]
            path = args[args.index(where["flag"]) + 1]
        else:
            raise SystemExit(f"fake_acp_replay: an mcpFrom place is home, besideEnv, flag or env, and got {where!r}")
        text = read_file(path)
    if "key" not in where:
        return text
    value = json.loads(text)
    for key in where["key"]:
        value = value[key]
    if not isinstance(value, str):
        return value
    path = urllib.parse.urlsplit(value).path if value.startswith("file:") else value
    return read_file(os.path.join(path, "index.mjs") if os.path.isdir(path) else path)


def read_file(path: str) -> str:
    with open(path, encoding="utf-8") as f:
        return f.read()


def play(rid, session, name):
    turn = (RECORDING.get("turns") or {}).get(name)
    if turn is None:
        out({"id": rid, "error": {"code": -32602, "message": f"the recording has no turn {name!r}"}})
        return
    for step in turn["steps"]:
        if "update" in step:
            update(session, step["update"])
        elif "send" in step:
            out(fill(step["send"], session))
        elif "ask" in step:
            NEXT[0] += 1
            out({"id": NEXT[0], **fill(step["ask"], session)})
            while True:
                reply = read()
                if reply.get("id") == NEXT[0] and "method" not in reply:
                    break
            STATE["answer"] = json.dumps(reply.get("result", reply.get("error")))
        elif "tool" in step:
            call_tool(session, step["tool"]["name"], step["tool"].get("arguments") or {})
        elif "end" in step:
            out({"id": rid, "result": fill(step["end"], session)})
            return
        elif "fail" in step:
            out({"id": rid, "error": fill(step["fail"], session)})
            return
        elif "exit" in step:
            sys.stderr.write(step.get("stderr", "") + "\n")
            sys.stderr.flush()
            sys.exit(step["exit"])


def main():
    log({"start": {"args": sys.argv[1:], "env": dict(os.environ), "cwd": os.getcwd()}})
    while True:
        message = read()
        method, rid, params = message.get("method"), message.get("id"), message.get("params") or {}
        if rid is None or method is None:
            continue
        if method == "session/prompt":
            play(rid, params["sessionId"], "".join(p.get("text", "") for p in params.get("prompt") or []))
        else:
            answer(rid, method, params)


if __name__ == "__main__":
    main()
