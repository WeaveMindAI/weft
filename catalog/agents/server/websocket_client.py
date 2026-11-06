"""A WebSocket client on the standard library, for the agents whose protocol
rides one (nanobot's gateway, OpenClaw's Gateway): the handshake on a socket
already connected, frames out, whole messages in.

Named `websocket_client`, not `websocket`: the `websocket-client` package
installs a module `websocket`, and whichever came first on the path would
win the import.
"""

import base64
import hashlib
import os
import struct

# RFC 6455's fixed suffix of the key the server's accept is the hash of.
GUID = "258EAFA5-E914-47DA-95CA-C5AB0DC85B11"
CONTINUATION, TEXT, CLOSE, PING, PONG = 0x0, 0x1, 0x8, 0x9, 0xA


class HandshakeFailed(Exception):
    """The server did not take the WebSocket: what it did, said of it, for
    the caller to put after the server's name ("closed the connection
    during the WebSocket handshake")."""


def ws_handshake(sock, host: str, target: str) -> bytes:
    """Upgrade `sock` (connected, with the timeout its answer gets) on
    `target` (the path and query): what the server sent after its answer,
    the start of its first frames. `sock` is closed when the server does
    not upgrade. From here on it blocks without a timeout: its reader
    waits for as long as the agent takes to speak."""
    key = base64.b64encode(os.urandom(16)).decode()
    sock.sendall((f"GET {target} HTTP/1.1\r\nHost: {host}\r\nUpgrade: websocket\r\nConnection: Upgrade\r\n"
                  f"Sec-WebSocket-Key: {key}\r\nSec-WebSocket-Version: 13\r\n\r\n").encode())
    answer = b""
    while b"\r\n\r\n" not in answer:
        chunk = sock.recv(4096)
        if not chunk:
            sock.close()
            raise HandshakeFailed("closed the connection during the WebSocket handshake")
        answer += chunk
    head, rest = answer.split(b"\r\n\r\n", 1)
    lines = head.decode("latin-1").split("\r\n")
    headers = {k.strip().lower(): v.strip() for k, _, v in (line.partition(":") for line in lines[1:])}
    if " 101 " not in lines[0] + " " or headers.get("sec-websocket-accept") != ws_accept(key):
        sock.close()
        raise HandshakeFailed(f"refused the WebSocket: {lines[0]}")
    sock.settimeout(None)
    return rest


def ws_accept(key: str) -> str:
    """The `Sec-WebSocket-Accept` a server answers the key `key` with."""
    return base64.b64encode(hashlib.sha1((key + GUID).encode()).digest()).decode()


def ws_frame(opcode: int, payload: bytes) -> bytes:
    """One final frame, as a client sends it: masked, with a mask of its own
    (RFC 6455 wants each one fresh and unpredictable)."""
    mask = os.urandom(4)
    n = len(payload)
    if n < 126:
        head = bytes([0x80 | opcode, 0x80 | n])
    elif n < 1 << 16:
        head = bytes([0x80 | opcode, 0x80 | 126]) + struct.pack(">H", n)
    else:
        head = bytes([0x80 | opcode, 0x80 | 127]) + struct.pack(">Q", n)
    return head + mask + bytes(b ^ mask[i % 4] for i, b in enumerate(payload))


class FrameReader:
    """Whole messages out of a server's frames: `read()` gives (opcode, payload)."""

    def __init__(self, recv, buffered: bytes = b""):
        self.recv = recv
        self.buffer = buffered
        # The message whose pieces are coming, [opcode, payload so far], or None.
        self.partial = None

    def _take(self, n: int) -> bytes:
        while len(self.buffer) < n:
            chunk = self.recv(65536)
            if not chunk:
                raise EOFError("the connection closed")
            self.buffer += chunk
        taken, self.buffer = self.buffer[:n], self.buffer[n:]
        return taken

    def _frame(self) -> tuple:
        first, second = self._take(2)
        n = second & 0x7F
        if n == 126:
            n = struct.unpack(">H", self._take(2))[0]
        elif n == 127:
            n = struct.unpack(">Q", self._take(8))[0]
        mask = self._take(4) if second & 0x80 else None
        payload = self._take(n)
        if mask:
            payload = bytes(b ^ mask[i % 4] for i, b in enumerate(payload))
        return bool(first & 0x80), first & 0x0F, payload

    def read(self) -> tuple:
        """The next control frame, or the next whole message."""
        while True:
            final, opcode, payload = self._frame()
            # A control frame is never in pieces, and may come between the
            # pieces of a message: it is given at once, the message kept.
            if opcode & 0x8:
                return opcode, payload
            if opcode == CONTINUATION:
                if self.partial is None:
                    raise ValueError("a continuation frame came with no message before it")
                self.partial[1] += payload
            elif self.partial is not None:
                raise ValueError(f"a frame of kind {opcode} began a message before the last one ended")
            else:
                self.partial = [opcode, payload]
            if final:
                (opcode, payload), self.partial = self.partial, None
                return opcode, payload

    def texts(self, pong):
        """The text messages, until the server closes: each ping is answered
        with `pong(payload)` as it comes (RFC 6455 wants every one
        answered). `EOFError` when the connection ends without a close."""
        while True:
            opcode, payload = self.read()
            if opcode == PING:
                pong(payload)
            elif opcode == CLOSE:
                return
            elif opcode == TEXT:
                yield payload
