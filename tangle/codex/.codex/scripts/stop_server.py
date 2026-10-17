#!/usr/bin/env python3
"""Stop a server a specialist started, and only that.

    python3 .codex/scripts/stop_server.py 4242
    python3 .codex/scripts/stop_server.py --port 5174

A dev server started with `pnpm run dev` runs outside Codex's sandbox (the
project's command rules let `pnpm run` out, since it needs the network). A
`kill` typed in the sandbox cannot reach it: on Linux the sandbox runs in a
process namespace of its own, so `kill 4242` answers "No such process".
And a rule allowing `kill` itself would let `kill -9 -1` out too.

So this script is what `.codex/rules/tangle.rules` allows instead. It takes
one process id, or `--port <n>` for the process listening on that port (the
sandbox cannot see that process to find its id), and sends SIGTERM to it and
every process under it. It refuses, naming why:
  - a process whose working folder is not `front/` or `design-lab/` of this
    project, or a folder below them (the project root itself is refused: Codex,
    a terminal or an editor helper started there runs in it too);
  - a process the script itself runs under (its parent shell, Codex);
  - an id that is no process it can read.
Run it from the project root, which is how it finds the project.
"""

import os
import re
import shutil
import signal
import subprocess
import sys
from pathlib import Path


def process_folder(pid: int) -> Path | None:
    """The folder the process runs in, or None when it cannot be read."""
    proc = Path(f"/proc/{pid}/cwd")
    if proc.exists():
        try:
            return proc.resolve(strict=True)
        except OSError:
            return None
    lsof = subprocess.run(
        ["lsof", "-a", "-p", str(pid), "-d", "cwd", "-Fn"], capture_output=True, text=True
    )
    for line in lsof.stdout.splitlines():
        if line.startswith("n"):
            return Path(line[1:]).resolve()
    return None


def listener(port: int) -> int | None:
    """The process listening on a TCP port, or None."""
    lsof = shutil.which("lsof")
    if lsof:
        found = subprocess.run([lsof, "-t", f"-iTCP:{port}", "-sTCP:LISTEN"], capture_output=True, text=True)
        pids = [int(p) for p in found.stdout.split() if p.isdigit()]
        if pids:
            return pids[0]
    ss = shutil.which("ss")
    if ss:
        found = subprocess.run([ss, "-ltnpH", f"sport = :{port}"], capture_output=True, text=True)
        match = re.search(r"pid=(\d+)", found.stdout)
        if match:
            return int(match.group(1))
    return None


def ancestors() -> set[int]:
    """Every process this script runs under, up to the first one."""
    table = subprocess.run(["ps", "-axo", "pid=,ppid="], capture_output=True, text=True, check=True)
    parent = {}
    for line in table.stdout.splitlines():
        parts = line.split()
        if len(parts) == 2:
            parent[int(parts[0])] = int(parts[1])
    found, pid = set(), os.getpid()
    while pid in parent and pid not in found:
        found.add(pid)
        pid = parent[pid]
    found.add(pid)
    return found


def descendants(pid: int) -> list[int]:
    """Every process under `pid`, children before their own children."""
    table = subprocess.run(["ps", "-axo", "pid=,ppid="], capture_output=True, text=True, check=True)
    children: dict[int, list[int]] = {}
    for line in table.stdout.splitlines():
        parts = line.split()
        if len(parts) == 2:
            children.setdefault(int(parts[1]), []).append(int(parts[0]))
    found, queue = [], [pid]
    while queue:
        for child in children.get(queue.pop(0), []):
            found.append(child)
            queue.append(child)
    return found


def main() -> int:
    args = sys.argv[1:]
    if len(args) == 2 and args[0] == "--port" and args[1].isdigit():
        found = listener(int(args[1]))
        if found is None:
            print(f"stop_server: nothing is listening on port {args[1]}", file=sys.stderr)
            return 1
        pid = found
    elif len(args) == 1 and args[0].isdigit():
        pid = int(args[0])
    else:
        print("stop_server: give one process id or --port <n>: python3 .codex/scripts/stop_server.py <pid>", file=sys.stderr)
        return 1
    if pid <= 1:
        print(f"stop_server: {pid} is not a server this project started", file=sys.stderr)
        return 1
    if pid in ancestors():
        print(f"stop_server: process {pid} is one this script runs under, so it is not stopped", file=sys.stderr)
        return 1
    project = Path.cwd().resolve()
    folder = process_folder(pid)
    if folder is None:
        print(f"stop_server: no process {pid} that can be read", file=sys.stderr)
        return 1
    sites = [project / "front", project / "design-lab"]
    if not any(folder == site or site in folder.parents for site in sites):
        print(f"stop_server: process {pid} runs in {folder}, which is not front/ or design-lab/ of {project}, so it is not stopped", file=sys.stderr)
        return 1
    targets = [pid] + descendants(pid)
    for target in targets:
        try:
            os.kill(target, signal.SIGTERM)
        except ProcessLookupError:
            pass
    print(f"stop_server: sent SIGTERM to {', '.join(str(t) for t in targets)}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
