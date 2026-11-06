"""A stand-in for `nanobot --home <instance> gateway --foreground`, replaying
the frames nanobot's gateway really sent over its WebSocket channel.

The frames are `fake_nanobot.json` (its `source` says where each part came
from). This program reads the instance's `config.json`, listens on the
unix socket it names, takes a WebSocket that brings the config's token,
sends `ready`, answers each request of ours with the gateway's recorded
answer, and plays the recorded turn whose message is the text of a
`message`, in order:

  {"send": {...}}          a frame the gateway sent
  {"tool": {"name", "arguments"}}
                           a call of the program's tool, through weft's tool
                           plugin (its entry points on `PYTHONPATH`)

Placeholders in a recorded frame: `$CHAT` the chat's id, `$READY` the
connection's own, `$UPLOAD` its upload token, `$TURN` the turn's id,
`$FOLDER` the chat's folder, `$TOOL` the text the program's tool answered.
A chat it makes gets a `sessions/<id>/sessions.sqlite3` in the instance, as
nanobot's does. With the model `exits-before-ready` it takes the WebSocket,
then exits (exit 3) before sending `ready`, as a gateway that dies on its
config does; with `exits-after-attached` it answers a `new_chat` with its
first frame (`attached`) alone, then exits (exit 3).

`$FAKE_LOG` gets one JSON line for the start (`args`, `env`, `cwd`,
`config`), then every frame it was sent.
"""

import importlib.metadata
import json
import os
import socket
import sys
import urllib.parse
import urllib.request
import uuid

import websocket_server

with open(os.path.join(os.path.dirname(os.path.abspath(__file__)), "fake_nanobot.json"), encoding="utf-8") as _file:
    RECORDING = json.load(_file)
EXITS_BEFORE_READY = "exits-before-ready"
EXITS_AFTER_ATTACHED = "exits-after-attached"
STATE = {"chat": None, "folder": "", "turn": "", "tool": ""}


def log(entry):
    with open(os.environ["FAKE_LOG"], "a", encoding="utf-8") as f:
        f.write(json.dumps(entry) + "\n")


def fill(value):
    if isinstance(value, str):
        return (value.replace("$CHAT", STATE["chat"] or "").replace("$TURN", STATE["turn"]).replace("$FOLDER", STATE["folder"])
                .replace("$TOOL", STATE["tool"]).replace("$READY", str(uuid.uuid4())).replace("$UPLOAD", "upload-token"))
    if isinstance(value, list):
        return [fill(v) for v in value]
    if isinstance(value, dict):
        return {k: fill(v) for k, v in value.items()}
    return value


def handshake(conn, token):
    """The WebSocket, or None (after a 401) when it does not bring `token`."""
    target, headers, rest = websocket_server.read_upgrade(conn)
    if urllib.parse.parse_qs(urllib.parse.urlsplit(target).query).get("token") != [token]:
        conn.sendall(b"HTTP/1.1 401 Unauthorized\r\nContent-Length: 0\r\n\r\n")
        return None
    websocket_server.accept(conn, headers)
    return websocket_server.Connection(conn, rest)


def call_tool(name, arguments):
    """The program's tool, as nanobot calls it: through the tool plugin its
    `nanobot.tools` entry points name, found on `PYTHONPATH` (the plugin's
    `tools.json` beside its module holds the route)."""
    eps = [ep for ep in importlib.metadata.entry_points(group="nanobot.tools") if ep.value.startswith("weft_nanobot:")]
    if not eps:
        raise SystemExit("fake nanobot: no weft tool plugin among the nanobot.tools entry points")
    folder = next(p for p in os.environ.get("PYTHONPATH", "").split(os.pathsep) if os.path.exists(os.path.join(p, "weft_nanobot.py")))
    with open(os.path.join(folder, "tools.json"), encoding="utf-8") as f:
        url = json.load(f)["url"]
    call = {"jsonrpc": "2.0", "id": 1, "method": "tools/call", "params": {"name": name, "arguments": arguments}}
    request = urllib.request.Request(url, data=json.dumps(call).encode(), method="POST", headers={"Content-Type": "application/json"})
    with urllib.request.urlopen(request, timeout=None) as reply:
        STATE["tool"] = json.loads(reply.read())["result"]["content"][0]["text"]


def scoped(frames, scope):
    """The recorded answers, with the scope this request asked for."""
    out = []
    for frame in fill(frames):
        if "workspace_scope" in frame:
            frame["workspace_scope"].update(scope)
        out.append(frame)
    return out


def made_chat(instance, chat):
    folder = os.path.join(instance, "sessions", chat[:8])
    os.makedirs(folder, exist_ok=True)
    open(os.path.join(folder, "sessions.sqlite3"), "a").close()


def serve(ws, config, instance):
    ws.send(fill(RECORDING["ready"]))
    while True:
        opcode, payload = ws.read()
        if opcode == websocket_server.CLOSE:
            return
        if opcode != websocket_server.TEXT:
            continue
        frame = json.loads(payload)
        log(frame)
        kind = frame.get("type")
        if kind == "new_chat":
            STATE["chat"] = str(uuid.uuid4())
            STATE["folder"] = frame["workspace_scope"]["project_path"]
            made_chat(instance, STATE["chat"])
            answers = scoped(RECORDING["answers"]["new_chat"], frame["workspace_scope"])
            if config["agents"]["defaults"]["model"] == EXITS_AFTER_ATTACHED:
                ws.send(answers[0])
                print("Error: the gateway died after attaching", file=sys.stderr, flush=True)
                sys.exit(3)
            for answer in answers:
                ws.send(answer)
        elif kind == "attach":
            STATE["chat"] = frame["chat_id"]
            for answer in fill(RECORDING["answers"]["attach"]):
                ws.send(answer)
        elif kind == "set_workspace_scope":
            STATE["folder"] = frame["workspace_scope"]["project_path"]
            for answer in scoped(RECORDING["answers"]["set_workspace_scope"], frame["workspace_scope"]):
                ws.send(answer)
        elif kind == "message" and frame.get("content") in RECORDING["turns"]:
            STATE["turn"] = frame.get("turn_id") or ""
            for step in RECORDING["turns"][frame["content"]]:
                if "send" in step:
                    ws.send(fill(step["send"]))
                else:
                    call_tool(step["tool"]["name"], step["tool"]["arguments"])
        else:
            for answer in fill(RECORDING["answers"]["bogus"]):
                ws.send(answer)


def main():
    instance = sys.argv[sys.argv.index("--home") + 1]
    with open(os.path.join(instance, "config.json"), encoding="utf-8") as f:
        config = json.load(f)
    log({"start": {"args": sys.argv[1:], "env": dict(os.environ), "cwd": os.getcwd(), "config": config}})
    channel = config["channels"]["websocket"]
    listener = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
    listener.bind(channel["unixSocketPath"])
    listener.listen(1)
    print("✓ Channels enabled: websocket", flush=True)
    while True:
        conn, _ = listener.accept()
        ws = handshake(conn, channel["token"])
        if ws is None:
            conn.close()
            continue
        if config["agents"]["defaults"]["model"] == EXITS_BEFORE_READY:
            print("Error: the gateway could not start its agent", file=sys.stderr, flush=True)
            sys.exit(3)
        try:
            serve(ws, config, instance)
        except EOFError:
            pass
        conn.close()


if __name__ == "__main__":
    main()
