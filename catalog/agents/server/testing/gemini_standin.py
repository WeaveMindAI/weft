"""A stand-in of Gemini's API (`streamGenerateContent`), for driving Google
Antigravity's real SDK without an account: `gemini_standin.py <port> <log>`.

Every request goes into `<log>` (one JSON line: its path and body, never a
header). What the model does is picked by keyword of the turn's message,
and the step of the turn by what the request ends on (the message, or the
result of the call before):

  "lookup"   calls the program's `lookup`, then says what it gave
  "readit"   calls `view_file`, which the program's tool of that name replaced
  "runit"    calls the built-in `run_command` (`echo hi`)
  "writeit"  calls the built-in `write_to_file`
  "askit"    calls `ask_question`: one question, two options
  "think"    thinks, then answers
  "fail"     answers 400, as Gemini does to a key it refuses
  anything else: "hello from antigravity", in three pieces
"""

import http.server
import json
import os
import sys

KEY_REFUSED = {"error": {"code": 400, "message": "API key not valid. Please pass a valid API key.", "status": "INVALID_ARGUMENT"}}


def request_text(contents: list) -> str:
    """The turn's message: the last user message's request."""
    for content in reversed(contents):
        for part in content.get("parts") or []:
            text = part.get("text") or ""
            start = text.find("<USER_REQUEST>\n")
            end = text.find("\n</USER_REQUEST>", start)
            if start != -1 and end != -1:
                return text[start + len("<USER_REQUEST>\n"):end]
    return ""


def last_result(contents: list):
    """The result the request ends on, when it ends on one."""
    parts = (contents[-1].get("parts") or []) if contents else []
    for part in parts:
        if "functionResponse" in part:
            return part["functionResponse"]
    return None


def call(name: str, args: dict) -> list:
    return [{"functionCall": {"name": name, "args": {**args, "toolAction": "Working", "toolSummary": "a step"}}}]


def answer(body: dict, folder: str) -> list:
    """The model's response to `body`: a list of chunks, each a list of parts."""
    contents = body.get("contents") or []
    message, result = request_text(contents), last_result(contents)
    if result is not None:
        return [[{"text": f"tool said: {json.dumps(result.get('response'))}"}]]
    if "lookup" in message:
        return [call("lookup", {"word": "x"})]
    if "readit" in message:
        return [call("view_file", {"AbsolutePath": os.path.join(folder, "a.txt")})]
    if "runit" in message:
        return [call("run_command", {"CommandLine": "echo hi", "Cwd": folder, "WaitMsBeforeAsync": 5000})]
    if "writeit" in message:
        return [call("write_to_file", {"TargetFile": os.path.join(folder, "new.txt"), "CodeContent": "new\n", "Overwrite": False,
                                       "Description": "a new file"})]
    if "askit" in message:
        return [call("ask_question", {"questions": [{"question": "Which color?", "options": ["red", "blue"], "is_multi_select": False}]})]
    if "think" in message:
        return [[{"text": "let me think", "thought": True}], [{"text": "thought and answered"}]]
    return [[{"text": "hello"}], [{"text": " from"}], [{"text": " antigravity"}]]


class Handler(http.server.BaseHTTPRequestHandler):
    def log_message(self, *args):
        pass

    def do_POST(self):  # noqa: N802 (the stdlib's spelling)
        body = json.loads(self.rfile.read(int(self.headers.get("Content-Length") or 0)) or b"{}")
        with open(self.server.log, "a") as f:
            f.write(json.dumps({"path": self.path, "body": body}) + "\n")
        message = request_text(body.get("contents") or [])
        if "fail" in message:
            data = json.dumps(KEY_REFUSED).encode()
            self.send_response(400)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(data)))
            self.end_headers()
            self.wfile.write(data)
            return
        self.send_response(200)
        self.send_header("Content-Type", "text/event-stream")
        self.end_headers()
        chunks = answer(body, self.server.folder)
        for index, parts in enumerate(chunks):
            chunk = {"candidates": [{"content": {"role": "model", "parts": parts}, "index": 0}],
                     "modelVersion": "gemini-3.8-flash"}
            if index == len(chunks) - 1:
                chunk["candidates"][0]["finishReason"] = "STOP"
                chunk["usageMetadata"] = {"promptTokenCount": 10, "candidatesTokenCount": 5, "totalTokenCount": 15}
            self.wfile.write(b"data: " + json.dumps(chunk).encode() + b"\r\n\r\n")
            self.wfile.flush()


def serve(port: int, log: str, folder: str) -> http.server.ThreadingHTTPServer:
    server = http.server.ThreadingHTTPServer(("127.0.0.1", port), Handler)
    server.log, server.folder = log, folder
    return server


if __name__ == "__main__":
    serve(int(sys.argv[1]), sys.argv[2], sys.argv[3]).serve_forever()
