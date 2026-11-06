"""A stand-in for `openclaw gateway run`, replaying the frames OpenClaw's
Gateway really sent over its WebSocket (`fake_openclaw.json`, whose
`source` says where each part came from).

It reads the config weft wrote (`$OPENCLAW_HOME/.openclaw/openclaw.json`),
listens on the port it names, and on each WebSocket sends the recorded
`connect.challenge` first, as the Gateway does. It answers `connect` (with
the config's token) and each request of ours with the Gateway's recorded
answer, and plays the recorded turn whose name is a `chat.send`'s message,
in order:

  {"send": {...}}       a frame the Gateway sent
  {"tool": {"name", "arguments"}}
                        a call of the program's tool through weft's plugin
                        (the route its loader, `weft-plugin/index.js`, names)
  {"await": <method>}   the Gateway waiting for that request of ours (an
                        approval's or a question's answer), answered with
                        its recorded answer

Placeholders in a recorded frame: `$RUN` the run (the `chat.send`'s
`idempotencyKey`), `$KEY` the session key, `$FOLDER` the agent's folder,
`$HOME` the private home, `$TOOL` the text the program's tool answered.
The sessions it made are kept in `$OPENCLAW_HOME/fake-sessions.json`, so a
Gateway started again knows them, as the real one's database does.

The config's model picks a failure: `starts-slowly` refuses the first
`connect` as a Gateway still starting does (`connect.starting`) and then
answers nothing more on that socket; `exits-at-start` exits (exit 3) before
it listens.

`$FAKE_LOG` gets one JSON line for the start (`args`, `env`, `cwd`,
`config`), then every frame it was sent.
"""

import json
import os
import re
import socket
import sys
import urllib.request

import websocket_server

with open(os.path.join(os.path.dirname(os.path.abspath(__file__)), "fake_openclaw.json"), encoding="utf-8") as _file:
    RECORDING = json.load(_file)
STARTS_SLOWLY = "custom/starts-slowly"
EXITS_AT_START = "custom/exits-at-start"
HOME = os.environ["OPENCLAW_HOME"]
SESSIONS = os.path.join(HOME, "fake-sessions.json")
STATE = {"run": "", "key": "", "folder": "", "tool": "", "refused_once": False}


def log(entry):
    with open(os.environ["FAKE_LOG"], "a", encoding="utf-8") as f:
        f.write(json.dumps(entry) + "\n")


def fill(value):
    if isinstance(value, str):
        return (value.replace("$RUN", STATE["run"]).replace("$KEY", STATE["key"]).replace("$FOLDER", STATE["folder"])
                .replace("$HOME", HOME).replace("$TOOL", STATE["tool"]))
    if isinstance(value, list):
        return [fill(v) for v in value]
    if isinstance(value, dict):
        return {k: fill(v) for k, v in value.items()}
    return value


def sessions() -> list:
    try:
        with open(SESSIONS, encoding="utf-8") as f:
            return json.load(f)
    except FileNotFoundError:
        return []


class Gateway(websocket_server.Connection):
    def request(self):
        """The next request of ours, logged."""
        while True:
            opcode, payload = self.read()
            if opcode == websocket_server.CLOSE:
                raise EOFError
            if opcode == websocket_server.TEXT:
                frame = json.loads(payload)
                log(frame)
                return frame

    def answer(self, request, recorded):
        self.send(dict(fill(recorded), id=request["id"]))


def handshake(conn):
    _, headers, rest = websocket_server.read_upgrade(conn)
    websocket_server.accept(conn, headers)
    return Gateway(conn, rest)


def call_tool(name, arguments):
    """The program's tool, as OpenClaw calls it: through weft's plugin, whose
    loader names the route."""
    with open(os.path.join(HOME, "weft-plugin", "index.js"), encoding="utf-8") as f:
        made = json.loads(re.search(r"make\((.*)\);\s*$", f.read(), re.DOTALL).group(1))
    if name not in [t["name"] for t in made["tools"]]:
        raise SystemExit(f"fake openclaw: weft's plugin has no tool {name}")
    call = {"jsonrpc": "2.0", "id": 1, "method": "tools/call", "params": {"name": name, "arguments": arguments}}
    request = urllib.request.Request(made["url"], data=json.dumps(call).encode(), method="POST",
                                     headers={"Content-Type": "application/json"})
    with urllib.request.urlopen(request, timeout=None) as reply:
        STATE["tool"] = json.loads(reply.read())["result"]["content"][0]["text"]


def play(ws, name):
    for step in RECORDING["turns"][name]:
        if "send" in step:
            ws.send(fill(step["send"]))
        elif "tool" in step:
            call_tool(step["tool"]["name"], step["tool"]["arguments"])
        else:
            while True:
                request = ws.request()
                if request.get("method") == step["await"]:
                    ws.answer(request, RECORDING["answers"][step["await"]])
                    break
                answer_request(ws, request, None)


def answer_request(ws, request, config):
    method, params = request.get("method"), request.get("params") or {}
    answers = RECORDING["answers"]
    if method == "connect":
        if params.get("auth", {}).get("token") != config["gateway"]["auth"]["token"]:
            raise SystemExit("fake openclaw: `connect` with a token that is not the config's")
        if config["agents"]["entries"]["weft"]["model"] == STARTS_SLOWLY and not STATE["refused_once"]:
            STATE["refused_once"] = True
            ws.answer(request, answers["connect.starting"])
            # As the real one: nothing more is answered on this socket.
            while True:
                ws.request()
        ws.answer(request, answers["connect"])
    elif method == "sessions.create":
        STATE["key"] = params["key"]
        known = sessions() + [params["key"]]
        with open(SESSIONS, "w", encoding="utf-8") as f:
            json.dump(known, f)
        ws.answer(request, answers["sessions.create"])
    elif method == "sessions.describe":
        STATE["key"] = params["key"]
        ws.answer(request, answers["sessions.describe" if params["key"] in sessions() else "sessions.describe.unknown"])
    elif method == "sessions.patch":
        ws.answer(request, answers["sessions.patch"])
    elif method == "chat.send":
        STATE["run"] = params["idempotencyKey"]
        if params["message"] not in RECORDING["turns"]:
            raise SystemExit(f"fake openclaw: no recorded turn {params['message']!r}")
        ws.answer(request, answers["chat.send"])
        play(ws, params["message"])
    elif method in ("exec.approval.resolve", "question.resolve"):
        ws.answer(request, answers[method])
    else:
        raise SystemExit(f"fake openclaw: no recorded answer to {method}")


def main():
    with open(os.path.join(HOME, ".openclaw", "openclaw.json"), encoding="utf-8") as f:
        config = json.load(f)
    log({"start": {"args": sys.argv[1:], "env": dict(os.environ), "cwd": os.getcwd(), "config": config}})
    if config["agents"]["entries"]["weft"]["model"] == EXITS_AT_START:
        print("[gateway] failed to start: the config is not valid", file=sys.stderr, flush=True)
        sys.exit(3)
    STATE["folder"] = config["agents"]["entries"]["weft"]["cwd"]
    listener = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    listener.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    listener.bind(("127.0.0.1", config["gateway"]["port"]))
    listener.listen(4)
    print("[gateway] http server listening", flush=True)
    while True:
        conn, _ = listener.accept()
        ws = handshake(conn)
        ws.send(RECORDING["challenge"])
        try:
            while True:
                answer_request(ws, ws.request(), config)
        except EOFError:
            pass
        conn.close()


if __name__ == "__main__":
    main()
