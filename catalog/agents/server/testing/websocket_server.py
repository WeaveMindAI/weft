"""The server's end of a WebSocket, for the stand-ins of the agents that
speak one (`fake_nanobot.py`, `fake_openclaw.py`).

Written apart from the adapters' own `websocket_client.py` on purpose: a
stand-in that framed with the client's code would agree with any mistake
in it, and the tests would pass on a client no real server understands.
"""

import base64
import hashlib
import json
import struct

GUID = "258EAFA5-E914-47DA-95CA-C5AB0DC85B11"
TEXT, CLOSE = 0x1, 0x8


def read_upgrade(conn) -> tuple:
    """The client's upgrade request: its target (path and query), its
    headers (names lowercase), and what came after it."""
    request = b""
    while b"\r\n\r\n" not in request:
        chunk = conn.recv(4096)
        if not chunk:
            raise EOFError
        request += chunk
    head, rest = request.split(b"\r\n\r\n", 1)
    lines = head.decode().split("\r\n")
    headers = {k.strip().lower(): v.strip() for k, _, v in (line.partition(":") for line in lines[1:])}
    return lines[0].split(" ")[1], headers, rest


def accept(conn, headers: dict) -> None:
    """Take the WebSocket the upgrade request `headers` asked for: from here
    on, `conn` is a `Connection` (built on what came after the request)."""
    key = base64.b64encode(hashlib.sha1((headers["sec-websocket-key"] + GUID).encode()).digest())
    conn.sendall(b"HTTP/1.1 101 Switching Protocols\r\nUpgrade: websocket\r\nConnection: Upgrade\r\nSec-WebSocket-Accept: "
                 + key + b"\r\n\r\n")


class Connection:
    def __init__(self, conn, buffered):
        self.conn, self.buffer = conn, buffered

    def take(self, n):
        while len(self.buffer) < n:
            chunk = self.conn.recv(65536)
            if not chunk:
                raise EOFError
            self.buffer += chunk
        taken, self.buffer = self.buffer[:n], self.buffer[n:]
        return taken

    def read(self):
        """One client frame (masked, final): its opcode and payload."""
        first, second = self.take(2)
        n = second & 0x7F
        if n == 126:
            n = struct.unpack(">H", self.take(2))[0]
        elif n == 127:
            n = struct.unpack(">Q", self.take(8))[0]
        mask = self.take(4)
        return first & 0x0F, bytes(b ^ mask[i % 4] for i, b in enumerate(self.take(n)))

    def send(self, frame):
        """`frame` as JSON, in one unmasked text frame."""
        payload = json.dumps(frame).encode()
        n = len(payload)
        head = bytes([0x80 | TEXT]) + (bytes([n]) if n < 126 else bytes([126]) + struct.pack(">H", n) if n < 65536
                                       else bytes([127]) + struct.pack(">Q", n))
        self.conn.sendall(head + payload)
