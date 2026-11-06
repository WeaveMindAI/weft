"""Run an agent's daemon for as long as the agent server that started it:
`python3 tether.py [folder] -- <command...>`.

A daemon that reads nothing on its standard input (nanobot's gateway) would
outlive an agent server that dies (killed, crashed), holding what it holds:
the next one would refuse to start ("Gateway is already running"). The
tether stands between them: the end of its standard input (the agent server
gone) or a SIGTERM stops the daemon, and `folder` (its socket's), when
there is one, goes with it. Its exit is the daemon's.
"""

import shutil
import signal
import subprocess
import sys
import threading

# How long the daemon gets to end on its own before it is killed.
# SYNC: STOP_GRACE_SECS <-> catalog/agents/server/adapters.py (STOP_GRACE_SECS)
STOP_GRACE_SECS = 5


def tether(folder, command: list) -> int:
    child = subprocess.Popen(command, stdin=subprocess.DEVNULL)

    def server_gone() -> None:
        sys.stdin.read()
        child.terminate()
        try:
            child.wait(STOP_GRACE_SECS)
        except subprocess.TimeoutExpired:
            child.kill()

    # Handed on, never waited for here: the handler runs on the thread that
    # waits for the daemon below. Whoever sent it kills what outlives it.
    signal.signal(signal.SIGTERM, lambda *_: child.terminate())
    threading.Thread(target=server_gone, name="tether", daemon=True).start()
    code = child.wait()
    if folder is not None:
        shutil.rmtree(folder, ignore_errors=True)
    # A daemon a signal ended exits as a shell says it: 128 plus the signal.
    return code if code >= 0 else 128 - code


if __name__ == "__main__":
    words = sys.argv[1:]
    at = words.index("--") if "--" in words else -1
    if at not in (0, 1) or not words[at + 1:]:
        raise SystemExit("usage: tether.py [folder] -- <command...>")
    sys.exit(tether(words[0] if at == 1 else None, words[at + 1:]))
