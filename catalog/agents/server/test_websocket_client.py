"""The WebSocket client the agents that speak one share (`websocket_client.py`).

Run with the others: `python3 -m unittest test_websocket_client`.
"""

import os
import unittest

import websocket_client as ws


class FramesTest(unittest.TestCase):
    def frames(self, data: bytes) -> list:
        chunks = [data[i:i + 3] for i in range(0, len(data), 3)]
        reader = ws.FrameReader(lambda _: chunks.pop(0) if chunks else b"")
        out = []
        while True:
            try:
                out.append(reader.read())
            except EOFError:
                return out

    def test_a_frame_reads_back_whatever_its_length(self):
        for size in (5, 300, 70000):
            payload = os.urandom(size)
            self.assertEqual(self.frames(ws.ws_frame(ws.TEXT, payload)), [(ws.TEXT, payload)])

    def test_each_frame_has_a_mask_of_its_own(self):
        self.assertNotEqual(ws.ws_frame(ws.TEXT, b"same")[2:6], ws.ws_frame(ws.TEXT, b"same")[2:6])

    def test_a_message_in_pieces_reads_as_one(self):
        # A server's frames: unmasked, the first not final.
        data = bytes([0x01, 3]) + b"abc" + bytes([0x80, 2]) + b"de" + bytes([0x89, 0])
        self.assertEqual(self.frames(data), [(ws.TEXT, b"abcde"), (ws.PING, b"")])

    def test_a_ping_between_the_pieces_of_a_message_comes_first_and_keeps_the_message(self):
        data = bytes([0x01, 3]) + b"abc" + bytes([0x89, 1]) + b"p" + bytes([0x80, 2]) + b"de"
        self.assertEqual(self.frames(data), [(ws.PING, b"p"), (ws.TEXT, b"abcde")])

    def test_a_message_begun_before_the_last_one_ended_is_refused(self):
        with self.assertRaises(ValueError):
            self.frames(bytes([0x01, 1]) + b"a" + bytes([0x81, 1]) + b"b")

    def test_texts_answers_each_ping_and_ends_at_the_close(self):
        data = bytes([0x81, 2]) + b"hi" + bytes([0x89, 1]) + b"p" + bytes([0x82, 1]) + b"b" + bytes([0x88, 0]) + bytes([0x81, 1]) + b"x"
        chunks = [data]
        pongs = []
        reader = ws.FrameReader(lambda _: chunks.pop(0) if chunks else b"")
        self.assertEqual(list(reader.texts(pongs.append)), [b"hi"])
        self.assertEqual(pongs, [b"p"])

    def test_the_accept_key_is_the_rfc_one(self):
        self.assertEqual(ws.ws_accept("dGhlIHNhbXBsZSBub25jZQ=="), "s3pPLMBiTxaQ9kYGzzhZRbK+xOo=")


if __name__ == "__main__":
    unittest.main()
