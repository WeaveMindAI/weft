"""A computer a program can work on: a screen, a mouse, a keyboard, a shell.

One HTTP service on one port, beside a virtual screen (Xvfb) with a window
manager and a terminal open on it:

  POST /action   every action the computer nodes take, in weft's action
                 envelope `{"action": <name>, "payload": {...}}`, answered
                 `{"result": {...}}`, or `{"result": {"error": "..."}}` when
                 the action did not fully happen. The buttons on the node's
                 card post here too.
  GET /jobs/<id> where a long shell command stands, for the node that waits
                 on it without holding a worker. The command's full output is
                 read afterwards in pieces, through the `output` action.
  GET /live      the node's card: the screen, the shell, the buttons.
  GET /health    ready once the screen answers.

Coordinates are pixels of the screen, which is the size of every
screenshot, so a coordinate read off a screenshot is where the click lands.

The shell has named sessions (the empty name is the default one). A
session keeps its folder and its exported variables from one command to the
next, the way one terminal would, but each command is a bash process of its
own, in its own process group: a timeout or an end stops exactly what that
command started, and nothing a command types can leave the session waiting
for more input. When a command ends, it hands back the folder it ended in
and its exported variables, and the session's next command starts from
them. A session runs one command at a time; different sessions run side by
side. Nothing is open between commands, so an idle session costs nothing.
Each command's full output is spooled to a file and read by the node in
pieces once the command ends.

Where to listen and the screen size come from the node that declares this
container, so the two sides cannot drift.
"""

import base64
import codecs
import http.server
import json
import math
import os
import signal
import subprocess
import sys
import threading
import time
import uuid

# SYNC: the computer's environment <-> catalog/computer/computer/mod.rs (WEFT_COMPUTER_PORT, WEFT_COMPUTER_SCREEN)
PORT = os.environ.get("WEFT_COMPUTER_PORT")
SCREEN = os.environ.get("WEFT_COMPUTER_SCREEN")
# SYNC: the home folder <-> catalog/computer/computer/mod.rs (HOME), images/computer/Dockerfile (ENV HOME)
HOME = os.environ.get("HOME")
if not PORT or not SCREEN or not HOME:
    sys.exit("WEFT_COMPUTER_PORT and WEFT_COMPUTER_SCREEN must be set by the node, and HOME by the image")
PORT = int(PORT)
try:
    WIDTH, HEIGHT = (int(side) for side in SCREEN.lower().split("x"))
except ValueError:
    sys.exit(f"WEFT_COMPUTER_SCREEN is {SCREEN!r}; it must be WIDTHxHEIGHT, like 1280x800")

DISPLAY = ":1"

# SYNC: computer routes <-> catalog/computer/computer/mod.rs (HEALTH_PATH), catalog/computer/actions.rs (JOBS_PATH)
HEALTH_PATH = "/health"
LIVE_PATH = "/live"
ACTION_PATH = "/action"
JOBS_PREFIX = "/jobs/"

# How long `exec` holds its answer for a command to finish, so a quick
# command answers in the same call; a longer one answers with its id and
# the node waits on `/jobs/<id>` instead.
EXEC_ANSWER_WITHIN_SECS = 10

# How long a finished command's answer and output are kept after anybody
# last asked for them: far longer than the five seconds between a waiting
# node's looks. They are let go of when the shell is next asked anything.
KEEP_FINISHED_SECS = 600

# Once a command's process has ended, how long its output and its state
# are given to finish arriving down their pipes. What it wrote before it
# ended is already in the pipe; this only waits for it to be read. A
# program it left running in the background may hold the pipes open for
# good, so the wait has to end on its own.
DRAIN_SECS = 1.0

# Where each command's full output is spooled until the node has read it:
# outside `/tmp`, so a command clearing that folder does not take it away.
# SYNC: OUTPUT_DIR <-> catalog/computer/computer/images/computer/start.sh
OUTPUT_DIR = "/var/tmp/weft-computer"

# How much of an output one read hands over: a few calls for a big
# output, one for the usual one.
OUTPUT_PIECE_BYTES = 4 * 1024 * 1024

# The most idle sessions the card lists; a computer many runs used keeps
# one per run that never ended its session.
CARD_IDLE_SESSIONS = 20

# How long a stopped command is given to end on SIGTERM before SIGKILL.
STOP_GRACE_SECS = 2.0

# xdotool's numbers for the mouse buttons and the wheel.
BUTTONS = {"left": "1", "middle": "2", "right": "3", "back": "8", "forward": "9"}
WHEEL = {"up": "4", "down": "5", "left": "6", "right": "7"}

# The most repeats of a key, seconds a key is held, and clicks of one scroll.
# SYNC: bounds <-> catalog/computer/styles.rs (MOST_REPEATS, MOST_SECONDS, MOST_WHEEL_CLICKS), the `maximum`s in the action nodes' `tools` metadata ports (catalog/computer/*/metadata.json) and perform.weft's `wait`
MOST_REPEATS = 100
MOST_SECONDS = 300
MOST_WHEEL_CLICKS = 100

# The keys an action may hold down around itself (a ctrl+click, a
# shift+drag). Only these: a modifier is put straight into an xdotool
# command chain, where any other word could be read as a command.
MODIFIERS = {"shift", "ctrl", "control", "alt", "super", "meta", "hyper"}

# The answer to an `exec` and to `/jobs/<id>`.
# SYNC: job answer <-> catalog/computer/run_command/mod.rs (finished: the statuses and fields)
RUNNING = "running"
DONE = "done"
LOST = "lost"


def log(message: str) -> None:
    sys.stderr.write(f"computer: {message}\n")
    sys.stderr.flush()


class Refused(Exception):
    """An action that did not fully happen; its text is what the caller reads."""


def run_x(args: list[str], what: str) -> str:
    """Run an X tool against the screen, refusing with its own words when it fails.

    A complaint on stderr is a failure even with exit code 0: xdotool
    answers 0 to a key name it does not know, after skipping that key."""
    env = dict(os.environ, DISPLAY=DISPLAY)
    done = subprocess.run(args, env=env, capture_output=True)
    said = done.stderr.decode("utf-8", "replace").strip()
    if done.returncode != 0 or said:
        raise Refused(f"{what} failed: {said or f'exit code {done.returncode}'}")
    return done.stdout.decode("utf-8", "replace")


def capture(args: list[str], what: str) -> bytes:
    """An image off the screen, as the bytes ImageMagick wrote."""
    env = dict(os.environ, DISPLAY=DISPLAY)
    done = subprocess.run(args, env=env, capture_output=True)
    if done.returncode != 0 or not done.stdout:
        said = done.stderr.decode("utf-8", "replace").strip()
        raise Refused(f"{what} failed: {said or f'exit code {done.returncode}'}")
    return done.stdout


def number(payload: dict, name: str) -> float:
    """A finite number from the payload (a JSON `true` is not one)."""
    value = payload.get(name)
    if not isinstance(value, (int, float)) or isinstance(value, bool) or not math.isfinite(value):
        raise Refused(f"{name} is a number; got {value!r}")
    return value


def whole(payload: dict, name: str, default: int | None, low: int, high: int | None) -> int:
    """A whole number from the payload (JSON may spell 3 as 3.0), within bounds."""
    value = payload.get(name, default)
    if isinstance(value, float) and value.is_integer():
        value = int(value)
    if not isinstance(value, int) or isinstance(value, bool) or value < low or (high is not None and value > high):
        bounds = f"from {low} to {high}" if high is not None else f"of {low} or more"
        raise Refused(f"{name} is a whole number {bounds}; got {payload.get(name)!r}")
    return value


# ---------------------------------------------------------------- the screen


class Desktop:
    """The virtual screen, its window manager and the terminal on it."""

    def __init__(self) -> None:
        self.processes: list[subprocess.Popen] = []
        self.lock = threading.Lock()

    def start(self) -> None:
        with self.lock:
            self._start()

    def restart(self) -> None:
        with self.lock:
            self._stop()
            self._start()

    def _start(self) -> None:
        # What a screen that died without cleaning up leaves behind (the
        # usual reason to restart it). A new Xvfb refuses to start beside a
        # lock, and a stale socket would look like a screen that is up.
        number_ = DISPLAY[1:]
        for stale in (f"/tmp/.X{number_}-lock", f"/tmp/.X11-unix/X{number_}"):
            if os.path.exists(stale):
                os.remove(stale)
        xvfb = subprocess.Popen(
            ["Xvfb", DISPLAY, "-screen", "0", f"{WIDTH}x{HEIGHT}x24", "-nolisten", "tcp"],
            stdout=sys.stderr, stderr=sys.stderr,
        )
        self.processes = [xvfb]
        env = dict(os.environ, DISPLAY=DISPLAY)
        deadline = time.monotonic() + 30

        def wait_for(check: list[str], what: str) -> None:
            while subprocess.run(check, env=env, capture_output=True).returncode != 0:
                if xvfb.poll() is not None:
                    raise RuntimeError(f"Xvfb exited with code {xvfb.returncode} while waiting for {what}")
                if time.monotonic() > deadline:
                    raise RuntimeError(f"{what} did not come up within 30 seconds")
                time.sleep(0.1)

        # Up once it answers, never on the socket's mere existence.
        wait_for(["xdotool", "getdisplaygeometry"], "the screen")
        for args in (["openbox"], ["xterm", "-fa", "Monospace", "-fs", "11", "-geometry", "100x30+40+40"]):
            self.processes.append(
                subprocess.Popen(args, env=env, cwd=HOME, stdout=sys.stderr, stderr=sys.stderr)
            )
        # Ready once the terminal is on the screen, so the first action
        # never lands on a screen still drawing itself.
        wait_for(["xdotool", "search", "--class", "xterm"], "the terminal")
        # After the window manager, which paints the background its own way.
        run_x(["xsetroot", "-solid", "#3a4a5a"], "setting the background")
        log(f"screen {WIDTH}x{HEIGHT} up on {DISPLAY}")

    def _stop(self) -> None:
        for process in reversed(self.processes):
            if process.poll() is None:
                process.terminate()
        for process in self.processes:
            try:
                process.wait(timeout=5)
            except subprocess.TimeoutExpired:
                process.kill()
                process.wait()
        # Windows opened from the shell (a browser) lose their screen with
        # Xvfb and close themselves.
        self.processes = []

    def healthy(self) -> bool:
        return bool(self.processes) and self.processes[0].poll() is None


DESKTOP = Desktop()


# ---------------------------------------------------------------- SSH

# SYNC: SSH_PORT <-> catalog/computer/computer/mod.rs (SSH_PORT)
SSH_PORT = 2222
# Prepared by start.sh, as root, when the node was given keys.
# SYNC: SSH_HOST_KEY <-> catalog/computer/computer/images/computer/start.sh (host_keys)
SSH_HOST_KEY = os.path.join(HOME, ".weft", "ssh", "ssh_host_ed25519_key")


class Ssh:
    """The SSH server, when the node was given keys: watched, so the card says when it is
    down and can start it again. It runs as root through sudo, the one way this user
    starts a program that may listen for any user."""

    def __init__(self) -> None:
        # SYNC: WEFT_COMPUTER_SSH_KEYS <-> catalog/computer/computer/mod.rs, images/computer/start.sh
        self.wanted = bool(os.environ.get("WEFT_COMPUTER_SSH_KEYS", "").strip())
        self.process: subprocess.Popen | None = None
        self.lock = threading.Lock()

    def start(self) -> None:
        with self.lock:
            self._start()

    def restart(self) -> None:
        with self.lock:
            if not self.wanted:
                raise Refused("this computer has no SSH: its node was given no `sshKeys`")
            if self.process is not None and self.process.poll() is None:
                # sudo hands the signal on to the server. A kill it cannot
                # hand on, so a server deaf to the signal is killed through
                # sudo, as root, and sudo then ends with it.
                self.process.terminate()
                try:
                    self.process.wait(timeout=5)
                except subprocess.TimeoutExpired:
                    subprocess.run(["sudo", "-n", "pkill", "-KILL", "-P", str(self.process.pid)], check=False)
                    self.process.wait()
            self._start()

    def _start(self) -> None:
        if not self.wanted:
            return
        self.process = subprocess.Popen(
            ["sudo", "-n", "/usr/sbin/sshd", "-D", "-e", "-p", str(SSH_PORT), "-h", SSH_HOST_KEY,
             "-o", "PasswordAuthentication=no", "-o", "KbdInteractiveAuthentication=no",
             "-o", "PermitRootLogin=no", "-o", "AllowUsers=user", "-o", f"SetEnv=DISPLAY={DISPLAY}"],
            stdin=subprocess.DEVNULL, stdout=sys.stderr, stderr=sys.stderr,
        )
        log(f"SSH open on port {SSH_PORT} for the user 'user', by key only")

    def state(self) -> str:
        with self.lock:
            code = self.process.poll() if self.process is not None else None
        if code is None:
            return f"open on port {SSH_PORT} for the user 'user', by key only"
        return f"down: the SSH server exited with code {code} (its lines are in `weft infra logs`)"


SSH = Ssh()


def screenshot() -> dict:
    png = capture(["import", "-window", "root", "-depth", "8", "png:-"], "taking the screenshot")
    return {"png": base64.b64encode(png).decode("ascii"), "width": WIDTH, "height": HEIGHT}


def zoom(payload: dict) -> dict:
    region = payload.get("region")
    if not (isinstance(region, list) and len(region) == 4):
        raise Refused(f"zoom takes a region [x0, y0, x1, y1]; it got {region!r}")
    x0, y0, x1, y1 = (int(number({"region": v}, "region")) for v in region)
    if not (0 <= x0 < x1 <= WIDTH and 0 <= y0 < y1 <= HEIGHT):
        raise Refused(
            f"the region [{x0}, {y0}, {x1}, {y1}] is not a box inside the {WIDTH}x{HEIGHT} screen "
            "(x0 < x1 and y0 < y1, all within the screen)"
        )
    # Cut out, then blown up to fill the screen's size with its proportions
    # kept, so small text comes back large enough to read.
    png = capture(
        ["import", "-window", "root", "-depth", "8", "-crop", f"{x1 - x0}x{y1 - y0}+{x0}+{y0}", "+repage",
         "-resize", f"{WIDTH}x{HEIGHT}", "png:-"],
        "zooming",
    )
    return {"png": base64.b64encode(png).decode("ascii")}


def point(payload: dict, required: bool) -> list[str]:
    """`mousemove x y` for a payload's x and y, or nothing when both are left out."""
    if payload.get("x") is None and payload.get("y") is None and not required:
        return []
    x, y = number(payload, "x"), number(payload, "y")
    if not (0 <= x < WIDTH and 0 <= y < HEIGHT):
        raise Refused(f"({x:g}, {y:g}) is off the {WIDTH}x{HEIGHT} screen")
    return ["mousemove", str(int(x)), str(int(y))]


def modifiers(payload: dict) -> list[str]:
    """The keys to hold down around an action: `ctrl+shift` is ctrl and shift."""
    held = payload.get("modifiers")
    if held is None or held == "":
        return []
    if not isinstance(held, str):
        raise Refused(f"modifiers are keys joined by +, like ctrl+shift; got {held!r}")
    keys = [key.lower() for key in held.split("+") if key]
    unknown = [key for key in keys if key not in MODIFIERS]
    if unknown:
        raise Refused(f"no modifier key {', '.join(unknown)}; the modifiers are {', '.join(sorted(MODIFIERS))}")
    return keys


def button(payload: dict) -> str:
    name = payload.get("button") or "left"
    if name not in BUTTONS:
        raise Refused(f"no mouse button {name!r}; the buttons are {', '.join(BUTTONS)}")
    return BUTTONS[name]


def holding(keys: list[str], chain: list[str]) -> list[str]:
    down = [arg for key in keys for arg in ("keydown", key)]
    up = [arg for key in reversed(keys) for arg in ("keyup", key)]
    return ["xdotool", *down, *chain, *up]


def click(payload: dict) -> dict:
    count = whole(payload, "count", 1, 1, 3)
    chain = point(payload, required=False) + ["click", "--repeat", str(count), "--delay", "80", button(payload)]
    run_x(holding(modifiers(payload), chain), "clicking")
    return {}


def drag(payload: dict) -> dict:
    path = payload.get("path")
    if not isinstance(path, list) or len(path) < 2:
        raise Refused(f"a drag takes a path of at least two points; got {path!r}")
    steps = [point(step if isinstance(step, dict) else {}, required=True) for step in path]
    chain = steps[0] + ["mousedown", button(payload)]
    for step in steps[1:]:
        chain += ["sleep", "0.05"] + step
    chain += ["mouseup", button(payload)]
    run_x(holding(modifiers(payload), chain), "dragging")
    return {}


def move(payload: dict) -> dict:
    run_x(holding(modifiers(payload), point(payload, required=True)), "moving the mouse")
    return {}


def mouse(payload: dict) -> dict:
    action = payload.get("action")
    if action not in ("down", "up"):
        raise Refused(f"the mouse action is down or up; got {action!r}")
    run_x(["xdotool", f"mouse{action}", button(payload)], f"pressing the mouse {action}")
    return {}


def cursor() -> dict:
    fields = dict(
        line.split("=", 1) for line in run_x(["xdotool", "getmouselocation", "--shell"], "reading the cursor").split()
    )
    return {"x": int(fields["X"]), "y": int(fields["Y"])}


def scroll(payload: dict) -> dict:
    direction = payload.get("direction")
    if direction not in WHEEL:
        raise Refused(f"scroll goes {', '.join(WHEEL)}; got {direction!r}")
    amount = whole(payload, "amount", 1, 1, MOST_WHEEL_CLICKS)
    chain = point(payload, required=False) + ["click", "--repeat", str(amount), "--delay", "40", WHEEL[direction]]
    run_x(holding(modifiers(payload), chain), "scrolling")
    return {}


def type_text(payload: dict) -> dict:
    text = payload.get("text")
    if not isinstance(text, str) or text == "":
        raise Refused(f"type takes some text; got {text!r}")
    run_x(["xdotool", "type", "--delay", "12", "--", text], "typing")
    return {}


def key(payload: dict) -> dict:
    keys = payload.get("keys")
    if not isinstance(keys, str) or not keys.split():
        raise Refused(f"key takes the keys to press, like Return or ctrl+s; got {keys!r}")
    combos = keys.split()
    repeat = whole(payload, "repeat", 1, 1, MOST_REPEATS)
    if payload.get("holdSecs") is None:
        run_x(["xdotool", "key", "--repeat", str(repeat), "--delay", "50", "--", *combos], "pressing keys")
        return {}
    hold = number(payload, "holdSecs")
    if not 0 < hold <= MOST_SECONDS:
        raise Refused(f"holdSecs is a number of seconds above 0 and at most {MOST_SECONDS}; got {hold!r}")
    # Let go in every case, even when pressing failed half way (xdotool
    # presses the names it knows before it complains about the others):
    # a key left down would ride along with every later action.
    try:
        run_x(["xdotool", "keydown", "--", *combos], "holding keys down")
        time.sleep(hold)
    finally:
        let_go = subprocess.run(
            ["xdotool", "keyup", "--", *reversed(combos)], env=dict(os.environ, DISPLAY=DISPLAY), capture_output=True,
        )
        if let_go.returncode != 0:
            log(f"letting {keys!r} go failed: {let_go.stderr.decode('utf-8', 'replace').strip()}")
    return {}


# ----------------------------------------------------------------- the shell


# The bash every command runs in. The command and the state it starts
# from arrive on an input pipe (no size limit, where an environment
# variable stops at 128 KiB), NUL-separated, and the pipe is closed
# before the command runs; the folder and the state pipe's descriptor
# arrive as environment variables, unset before it runs. The shell's own
# names are `__weft_` ones: the descriptor is readonly, so a command
# cannot send the state elsewhere, and every one of them loses its export
# before the state is written, so none can ride into the next command
# (`set -a` exports every assignment). Once the command ends, the folder
# and the exported variables go back down the state pipe, ended by a
# marker so the reader never waits for the pipe to close (a program left
# running in the background may hold it open). An `exit` in the command
# saves them from the exit trap instead, unless the command set an exit
# trap of its own; then they are not kept, and the answer says so.
#
# The save runs in the command's own shell, so it is built to survive what
# a command ordinarily does to that shell (options, `IFS`, `set -a`, traps,
# functions named like the builtins it uses). A command that takes the
# shell apart (`enable -n` on a builtin, a readonly function over one, an
# extdebug trap that skips commands) breaks the save as it would break any
# terminal: the answer then says nothing was kept, or the command does not
# end until its timeoutSecs or a restart, which starts the shell over.
# That boundary is deliberate: capturing the state from outside the shell
# is not possible (a process's exported variables live only inside it).
STATE_END = b"\0weft-state-end\0"
SCRIPT = """
{ IFS= read -r -d '' __weft_command; IFS= read -r -d '' __weft_state; } <&"$WEFT_IN_FD"
exec {WEFT_IN_FD}<&-
readonly __weft_fd=$WEFT_STATE_FD
__weft_dir=$WEFT_DIR __weft_saved=
unset WEFT_IN_FD WEFT_STATE_FD WEFT_DIR
eval "$__weft_state"
unset __weft_state
cd -- "$__weft_dir"
__weft_save() {
  # POSIX mode first, by assignment (a command cannot shadow one): there a
  # special builtin (`unset`, `export`, `return`, `exit`) wins over a
  # function of its name, and `unset -f` clears any function the command
  # defined over the other builtins this uses.
  POSIXLY_CORRECT=1
  unset -f read printf declare compgen
  [[ -n $__weft_saved ]] && return
  __weft_saved=1
  # A readonly variable (bash's own `SHELLOPTS`, or one a command made)
  # cannot be declared again by the next command, so it does not carry.
  # A name per line, never split by the command's own `IFS`.
  while IFS= read -r __weft_name; do
    [[ $(declare -p "$__weft_name") =~ ^declare\\ -[a-zA-Z]*r ]] && export -n "$__weft_name"
  done < <(compgen -A export)
  export -n POSIXLY_CORRECT "${!__weft_@}"
  { printf "%s\\0" "$PWD"; export -p; printf "\\0weft-state-end\\0"; } >&"$__weft_fd" 2>/dev/null
}
trap '__weft_code=$?; __weft_save; exit $__weft_code' EXIT
eval "$__weft_command"
__weft_code=$?
__weft_save
exit $__weft_code
"""


class Job:
    """One command: its full output (spooled to a file the node reads in
    pieces), and how it ended."""

    def __init__(self, session: str, command: str, timeout_secs: float | None) -> None:
        self.id = uuid.uuid4().hex
        self.session = session
        self.command = command
        self.timeout_secs = timeout_secs
        self.started = time.monotonic()
        self.lock = threading.Lock()
        self.output_path = os.path.join(OUTPUT_DIR, f"{self.id}.out")
        self.output = open(self.output_path, "wb")
        self.size = 0
        # Set once the command's process is gone, and when the answer is
        # final (its output and state read): what arrives after is not part
        # of it.
        self.exited = threading.Event()
        self.ended = threading.Event()
        self.exit_code: int | None = None
        # Why the command did not end by itself, and anything else its
        # answer has to say, as lines added after its output.
        self.stopped: str | None = None
        self.note: str | None = None
        # When anybody last needed it ended: its end, then every time the
        # shell is asked for it (a look at its answer, a read of its output).
        self.needed: float | None = None

    def take(self, chunk: bytes) -> None:
        """Add what the command wrote to its output."""
        with self.lock:
            if self.ended.is_set():
                return
            self.output.write(chunk)
            self.size += len(chunk)

    def stop(self, why: str) -> bool:
        """Mark the command as stopped for `why`, unless it already ended by
        itself; whether it was still running, so the caller stops it."""
        with self.lock:
            if self.exited.is_set():
                return False
            self.stopped = why
            return True

    def end(self, exit_code: int, note: str | None = None) -> None:
        with self.lock:
            if self.ended.is_set():
                return
            self.exit_code = exit_code
            self.note = self.note or note
            # The lines the command's answer adds, written into the output
            # itself, so whoever reads it reads them too.
            said = [line for line in (self.note, self.stopped) if line]
            if said:
                tail = "".join(f"\n[{line}]" for line in said).encode("utf-8")
                tail = tail if self.size else tail.lstrip(b"\n")
                self.output.write(tail)
                self.size += len(tail)
            self.output.close()
            self.needed = time.monotonic()
            self.ended.set()

    def answer(self) -> dict:
        with self.lock:
            if not self.ended.is_set():
                return {"status": RUNNING, "id": self.id}
            return {"status": DONE, "id": self.id, "exitCode": self.exit_code, "size": self.size}

    def read(self, offset: int) -> dict:
        """The piece of the finished output from `offset`, base64, with where the next one
        starts; `end` once it reaches the end."""
        with self.lock:
            if not self.ended.is_set():
                raise Refused(f"command {self.id} is still running; its output is read once it ends")
        if not 0 <= offset <= self.size:
            raise Refused(f"offset {offset} is outside the output of command {self.id} ({self.size} bytes)")
        try:
            with open(self.output_path, "rb") as f:
                f.seek(offset)
                piece = f.read(OUTPUT_PIECE_BYTES)
        except OSError as e:
            raise Refused(f"the output of command {self.id} is gone from the computer: {e}") from e
        upto = offset + len(piece)
        if len(piece) < min(OUTPUT_PIECE_BYTES, self.size - offset):
            raise Refused(f"the output of command {self.id} is shorter on the computer than it was ({upto} of {self.size} bytes)")
        return {"data": base64.b64encode(piece).decode("ascii"), "next": upto, "end": upto >= self.size}

    def forget(self) -> None:
        if os.path.exists(self.output_path):
            os.remove(self.output_path)


def base_env() -> dict[str, str]:
    """What every command's environment starts from, before the state is laid over it."""
    return {
        "HOME": HOME,
        "USER": os.environ.get("USER", "user"),
        "PATH": "/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin",
        "LANG": "C.UTF-8",
        "DISPLAY": DISPLAY,
        "TERM": "dumb",
        "PAGER": "cat",
        "GIT_PAGER": "cat",
    }


def read_output(fd: int, job: Job) -> None:
    """Read a command's output pipe until it closes, into the command's output."""
    try:
        while chunk := os.read(fd, 65536):
            job.take(chunk)
    finally:
        os.close(fd)


def read_state(fd: int, sink: list[bytes]) -> None:
    """Read the state pipe until the end marker arrives (or the pipe closes)."""
    seen = b""
    try:
        while chunk := os.read(fd, 65536):
            sink.append(chunk)
            seen = (seen + chunk)[-len(STATE_END):]
            if seen == STATE_END:
                break
    finally:
        os.close(fd)


class Session:
    """One named terminal: the folder and exported variables its commands carry from one
    to the next, the command it runs now, and the process groups its commands started
    (what a background program they left still runs in). Nothing is open between
    commands: a session is this record and nothing else."""

    def __init__(self, name: str) -> None:
        self.name = name
        self.folder = HOME
        self.exports = ""
        self.running: Job | None = None
        self.groups: set[int] = set()
        self.used = time.monotonic()

    def busy(self) -> bool:
        return self.running is not None and not self.running.ended.is_set()


class Shell:
    """The computer's shell: named sessions, each running one command at a time (different
    sessions run side by side), each command its own bash."""

    def __init__(self) -> None:
        self.lock = threading.Lock()
        self.sessions: dict[str, Session] = {}
        self.jobs: dict[str, Job] = {}
        os.makedirs(OUTPUT_DIR, exist_ok=True)

    def start(self, name: str, command: str, timeout_secs: float | None) -> Job:
        with self.lock:
            session = self.sessions.setdefault(name, Session(name))
            if session.busy():
                busy = session.running
                assert busy is not None
                raise Refused(
                    f"a command is still running in {describe(name)} "
                    f"({int(time.monotonic() - busy.started)} seconds so far): {busy.command!r}. "
                    "Wait for it to finish, or end the session (EndSession, `restart` on RunCommand, or "
                    "Restart the shell on the Computer node), which stops it."
                )
            self._sweep()
            # A group whose every process ended holds nothing for an end to
            # stop, and its id may go to another process later.
            session.groups = {group for group in session.groups if group_alive(group)}
            job = Job(name, command, timeout_secs)
            folder = session.folder
            if not (os.path.isdir(folder) and os.access(folder, os.X_OK)):
                job.note = f"the folder {folder} no longer exists or can no longer be entered; the command ran in {HOME}"
                folder = HOME
            state_read, state_write = os.pipe()
            input_read, input_write = os.pipe()
            env = dict(base_env(), WEFT_DIR=folder, WEFT_STATE_FD=str(state_write), WEFT_IN_FD=str(input_read))
            try:
                process = subprocess.Popen(
                    ["bash", "--noprofile", "--norc", "-c", SCRIPT],
                    stdin=subprocess.DEVNULL, stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
                    cwd=folder, env=env, start_new_session=True, pass_fds=(state_write, input_read),
                )
            except BaseException:
                os.close(state_read)
                os.close(input_write)
                job.output.close()
                job.forget()
                raise
            finally:
                os.close(state_write)
                os.close(input_read)
            session.groups.add(process.pid)
            session.running = job
            session.used = time.monotonic()
            self.jobs[job.id] = job
            given = command.encode("utf-8") + b"\0" + session.exports.encode("utf-8") + b"\0"
        output_fd = os.dup(process.stdout.fileno())
        process.stdout.close()
        threading.Thread(target=write_all, args=(input_write, given), daemon=True).start()
        state: list[bytes] = []
        readers = [
            threading.Thread(target=read_output, args=(output_fd, job), daemon=True),
            threading.Thread(target=read_state, args=(state_read, state), daemon=True),
        ]
        for reader in readers:
            reader.start()
        threading.Thread(target=self._finish, args=(session, job, process, readers, state), daemon=True).start()
        if timeout_secs is not None:
            threading.Thread(target=self._stop_after, args=(job, process.pid), daemon=True).start()
        return job

    def _finish(
        self, session: Session, job: Job, process: subprocess.Popen, readers: list[threading.Thread], state: list[bytes]
    ) -> None:
        code = process.wait()
        job.exited.set()
        for reader in readers:
            reader.join(timeout=DRAIN_SECS)
        # A signal ends a process with a negative code here; a shell reports it as 128 + the signal.
        exit_code = code if code >= 0 else 128 - code
        raw = b"".join(state)
        note = None
        with self.lock:
            if session.running is job and self.sessions.get(session.name) is session:
                if raw.endswith(STATE_END):
                    folder, _, exports = raw[: -len(STATE_END)].partition(b"\0")
                    session.folder = folder.decode("utf-8", "replace")
                    session.exports = exports.decode("utf-8", "replace")
                elif job.stopped is None:
                    note = (
                        "the folder and exported variables this command left were not kept: the command did "
                        "not let the shell hand them back (it ended through `exec` or a signal, set an exit "
                        "trap of its own, or closed the shell's descriptor)"
                    )
            job.end(exit_code, note)

    def _stop_after(self, job: Job, group: int) -> None:
        assert job.timeout_secs is not None
        if job.ended.wait(timeout=job.timeout_secs):
            return
        if not job.stop(f"stopped after {job.timeout_secs:g} seconds, its timeoutSecs"):
            return
        log(f"command {job.id} stopped after {job.timeout_secs:g} seconds")
        end_group(group)

    def end(self, name: str, why: str) -> bool:
        """End the session `name`: stop what its commands still run and forget its folder and
        variables, so the name starts over in the home folder. Whether there was one."""
        with self.lock:
            session = self.sessions.pop(name, None)
        if session is None:
            return False
        if session.running is not None:
            session.running.stop(why)
        for group in session.groups:
            end_group(group)
        log(f"{describe(name)} ended: {why}")
        return True

    def end_all(self, why: str) -> None:
        with self.lock:
            names = list(self.sessions)
        for name in names:
            self.end(name, why)

    def _sweep(self) -> None:
        """Forget finished commands nobody has needed for KEEP_FINISHED_SECS, output and all.
        Called, under the lock, whenever the shell is asked to start a command or for one."""
        now = time.monotonic()
        for job in list(self.jobs.values()):
            with job.lock:
                stale = job.ended.is_set() and job.needed is not None and now - job.needed > KEEP_FINISHED_SECS
            if stale:
                job.forget()
                del self.jobs[job.id]

    def job(self, job_id: str) -> Job | None:
        """The command `job_id`, counted as needed now: under the lock, so a sweep can never
        forget it between this lookup and the caller's use of it."""
        with self.lock:
            self._sweep()
            job = self.jobs.get(job_id)
            if job is not None:
                with job.lock:
                    if job.ended.is_set():
                        job.needed = time.monotonic()
            return job

    def states(self) -> str:
        """The card's view of the sessions: every one that runs a command, then the idle ones
        most recently used first, up to CARD_IDLE_SESSIONS. A run stopped before it ended
        its session leaves it here until Restart the shell."""
        with self.lock:
            if not self.sessions:
                return "no session yet"
            now = time.monotonic()
            busy = sorted((s for s in self.sessions.values() if s.busy()), key=lambda s: s.name)
            idle = sorted((s for s in self.sessions.values() if not s.busy()), key=lambda s: -s.used)
            lines = [
                f"{describe(s.name)}: running for {int(now - s.running.started)} seconds: {s.running.command}"
                for s in busy
                if s.running is not None
            ]
            lines += [f"{describe(s.name)}: idle in {s.folder}" for s in idle[:CARD_IDLE_SESSIONS]]
            if len(idle) > CARD_IDLE_SESSIONS:
                lines.append(f"and {len(idle) - CARD_IDLE_SESSIONS} more idle sessions")
            return "\n".join(lines)


def describe(name: str) -> str:
    return f"session {name!r}" if name else "the default session"


def write_all(fd: int, data: bytes) -> None:
    """Write `data` to a pipe and close it; a reader that went away first is fine."""
    try:
        with os.fdopen(fd, "wb") as pipe:
            pipe.write(data)
    except BrokenPipeError:
        # The command's bash ended before reading what it was given (it
        # was stopped): nothing is left to hand it to.
        pass


def group_alive(group: int) -> bool:
    try:
        os.killpg(group, 0)
    except ProcessLookupError:
        return False
    return True


def end_group(group: int) -> None:
    """End a process group: asked first, then killed. A group already gone is fine."""
    try:
        os.killpg(group, signal.SIGTERM)
    except ProcessLookupError:
        return
    deadline = time.monotonic() + STOP_GRACE_SECS
    while time.monotonic() < deadline:
        try:
            os.killpg(group, 0)
        except ProcessLookupError:
            return
        time.sleep(0.1)
    try:
        os.killpg(group, signal.SIGKILL)
    except ProcessLookupError:
        pass


SHELL = Shell()


def session_name(payload: dict) -> str:
    """The session a payload names; the empty name is the computer's default session."""
    name = payload.get("session", "")
    if not isinstance(name, str):
        raise Refused(f"the session is a name; got {name!r}")
    return name


def exec_command(payload: dict) -> dict:
    command = payload.get("command")
    restart = payload.get("restart", False)
    session = session_name(payload)
    if not isinstance(restart, bool):
        raise Refused(f"restart is true or false; got {restart!r}")
    if command is not None and not isinstance(command, str):
        raise Refused(f"the command is text; got {command!r}")
    if not command and not restart:
        raise Refused("exec takes a command, or restart: true")
    timeout_secs = None
    if command:
        if "\0" in command:
            raise Refused("the command holds a NUL character, which no shell command can")
        if payload.get("timeoutSecs") is not None:
            timeout_secs = number(payload, "timeoutSecs")
            # The longest a wait can be told to last: anything longer is no
            # timeout at all, so leave it out.
            if not 0 < timeout_secs <= threading.TIMEOUT_MAX:
                raise Refused(
                    f"timeoutSecs is a number of seconds above 0 and at most {threading.TIMEOUT_MAX:g}; got {timeout_secs!r}"
                )
    if restart:
        SHELL.end(session, "restarted on request")
    if not command:
        return {"status": DONE, "restarted": True}
    job = SHELL.start(session, command, timeout_secs)
    job.ended.wait(timeout=EXEC_ANSWER_WITHIN_SECS)
    return job.answer()


def no_such_job(job_id: object) -> str:
    """Why a command a node asks about is not on the computer."""
    return (
        f"no command {job_id!r} on this computer: it restarted since the command began, or nobody "
        f"asked for the finished command for over {KEEP_FINISHED_SECS // 60} minutes and it was forgotten"
    )


def read_output_piece(payload: dict) -> dict:
    job_id = payload.get("id")
    job = SHELL.job(job_id) if isinstance(job_id, str) else None
    if job is None:
        raise Refused(no_such_job(job_id))
    return job.read(whole(payload, "from", None, 0, None))


def end_session(payload: dict) -> dict:
    name = session_name(payload)
    return {"ended": SHELL.end(name, "ended by EndSession")}


ACTIONS = {
    "screenshot": lambda payload: screenshot(),
    "zoom": zoom,
    "click": click,
    "drag": drag,
    "move": move,
    "mouse": mouse,
    "cursor": lambda payload: cursor(),
    "scroll": scroll,
    "type": type_text,
    "key": key,
    "exec": exec_command,
    "output": read_output_piece,
    "end_session": end_session,
}

# The card's buttons, one way out of each state the computer can sit in.
RESTART_SHELL = "restart_shell"
RESTART_DESKTOP = "restart_desktop"
RESTART_SSH = "restart_ssh"


def press(action: str) -> dict:
    if action == RESTART_SHELL:
        SHELL.end_all("restarted from the Computer node's card")
        return {"restarted": "shell"}
    if action == RESTART_SSH:
        SSH.restart()
        return {"restarted": "ssh"}
    DESKTOP.restart()
    return {"restarted": "desktop"}


def live_items() -> list:
    items = []
    try:
        jpeg = capture(["import", "-window", "root", "-quality", "70", "jpg:-"], "taking the screenshot")
        items.append({"type": "image", "label": "Screen", "data": "data:image/jpeg;base64," + base64.b64encode(jpeg).decode("ascii")})
    except Refused as refused:
        log(str(refused))
        items.append({"type": "text", "label": "Screen", "data": f"cannot be read: {refused}"})
    items.append({
        "type": "text",
        "label": "Shell",
        "data": SHELL.states(),
        "action": {
            "label": "Restart the shell",
            "actionKind": RESTART_SHELL,
            "confirm": "End every session: stop whatever runs, and whatever their commands left running, and start over in the home folder?",
        },
    })
    items.append({
        "type": "text",
        "label": "Screen size",
        "data": f"{WIDTH}x{HEIGHT}",
        "action": {
            "label": "Restart the desktop",
            "actionKind": RESTART_DESKTOP,
            "confirm": "Close every window and start the desktop again? Files are kept.",
        },
    })
    if SSH.wanted:
        items.append({
            "type": "text",
            "label": "SSH",
            "data": SSH.state(),
            "action": {
                "label": "Restart SSH",
                "actionKind": RESTART_SSH,
                "confirm": "Start the SSH server again? Logins already open stay open.",
            },
        })
    return items


class Handler(http.server.BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.0"

    def _send(self, status: int, body: dict) -> None:
        payload = json.dumps(body).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(payload)))
        self.end_headers()
        self.wfile.write(payload)

    def do_GET(self) -> None:  # noqa: N802 (the stdlib's spelling)
        if self.path == HEALTH_PATH:
            if DESKTOP.healthy():
                self._send(200, {"ok": True})
            else:
                self._send(503, {"error": "the screen is not up"})
            return
        if self.path == LIVE_PATH:
            self._send(200, {"items": live_items()})
            return
        if self.path.startswith(JOBS_PREFIX):
            job_id = self.path[len(JOBS_PREFIX):]
            job = SHELL.job(job_id)
            if job is None:
                # An answer the waiting node can act on, never a 404 its
                # checks would retry for ever: the jobs live in this
                # process, so a restarted computer has none of the old ones.
                self._send(200, {"status": LOST, "error": no_such_job(job_id)})
                return
            self._send(200, job.answer())
            return
        self._send(404, {"error": "no such path"})

    def do_POST(self) -> None:  # noqa: N802
        if self.path != ACTION_PATH:
            self._send(404, {"error": "no such path"})
            return
        try:
            length = max(0, int(self.headers.get("Content-Length", "0")))
            body = json.loads(self.rfile.read(length) or b"{}")
        except (ValueError, json.JSONDecodeError) as e:
            self._send(400, {"error": f"the body is not JSON: {e}"})
            return
        # SYNC: the /action envelope <-> crates/weft-core/src/infra/action.rs, catalog/postgres/database/images/credential/bootstrap.py,
        #       catalog/bailey/bridge/images/bridge/src/actions.js
        action = body.get("action") if isinstance(body, dict) else None
        payload = body.get("payload") if isinstance(body, dict) else None
        payload = payload if isinstance(payload, dict) else {}
        try:
            if action in (RESTART_SHELL, RESTART_DESKTOP, RESTART_SSH):
                result = press(action)
            elif action in ACTIONS:
                result = ACTIONS[action](payload)
            else:
                raise Refused(f"no action {action!r}; the actions are {', '.join(ACTIONS)}")
        except Refused as refused:
            log(f"{action}: {refused}")
            self._send(200, {"result": {"error": str(refused)}})
            return
        except Exception as e:  # noqa: BLE001 (any failure is the caller's answer and a log line)
            log(f"{action}: {type(e).__name__}: {e}")
            self._send(200, {"result": {"error": f"{type(e).__name__}: {e}"}})
            return
        self._send(200, {"result": result})

    def log_message(self, fmt: str, *args: object) -> None:
        # Every request would be a line every three seconds while somebody
        # watches the card; the failures are logged where they happen.
        pass


def serve() -> None:
    DESKTOP.start()
    SSH.start()
    server = http.server.ThreadingHTTPServer(("0.0.0.0", PORT), Handler)
    server.daemon_threads = True
    log(f"listening on {PORT}")
    server.serve_forever()


if __name__ == "__main__":
    serve()
