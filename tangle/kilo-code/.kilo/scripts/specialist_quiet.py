#!/usr/bin/env python3
"""Say how long a specialist has been quiet.

Tangle looks once at a specialist that has been out for twenty minutes or
more. This prints how many minutes ago that specialist, or a specialist it
started itself (a frontend-builder's designer), last wrote anything:

    python3 .kilo/scripts/specialist_quiet.py ses_f3fcd67f4ffeiPmzOzWMADfmFm

The argument is the task id the dispatch answered with (`<task id="ses_...">`).
It runs one fixed, read-only query through `kilo db` and nothing else, so it
can be allowed where `kilo db` itself cannot: a `kilo db` rule would let any
SQL through, writes included.

It prints the minutes as a whole number, or `no writes yet` when the session
has none. It runs the `kilo` on PATH, or, when there is none, the newest one
inside the Kilo editor extension. A malformed id, a missing `kilo` or a failed
query ends with a non-zero exit and says why.
"""

import os
import re
import shutil
import subprocess
import sys
from pathlib import Path

TIMEOUT_SECONDS = 30

EXTENSION_GLOBS = (
    ".vscode/extensions/kilocode.kilo-code-*/bin/kilo",
    ".vscode-server/extensions/kilocode.kilo-code-*/bin/kilo",
    ".vscode-insiders/extensions/kilocode.kilo-code-*/bin/kilo",
    ".cursor/extensions/kilocode.kilo-code-*/bin/kilo",
    ".windsurf/extensions/kilocode.kilo-code-*/bin/kilo",
)

# A Kilo session id, as the task tool hands it back: `ses_` then letters and digits.
SESSION = re.compile(r"^ses_[A-Za-z0-9]+$")

QUERY = (
    "SELECT (strftime('%s','now')*1000 - max(p.time_updated))/60000 AS quiet_minutes "
    "FROM part p JOIN session s ON s.id = p.session_id "
    "WHERE s.id = '{id}' OR s.parent_id = '{id}'"
)


def find_kilo() -> str | None:
    """`kilo` on PATH, else the newest one inside an editor extension."""
    on_path = shutil.which("kilo")
    if on_path:
        return on_path
    home = Path.home()
    found = [p for pattern in EXTENSION_GLOBS for p in home.glob(pattern) if os.access(p, os.X_OK)]
    if not found:
        return None
    return str(max(found, key=lambda p: p.stat().st_mtime))


def main() -> int:
    if len(sys.argv) != 2 or not SESSION.match(sys.argv[1]):
        print("usage: specialist_quiet.py ses_<letters and digits> (the task id of one dispatch)", file=sys.stderr)
        return 2
    session = sys.argv[1]

    kilo = find_kilo()
    if kilo is None:
        print("specialist_quiet: found no `kilo` to run, none on PATH and none in an editor extension", file=sys.stderr)
        return 1

    try:
        done = subprocess.run(
            [kilo, "db", QUERY.format(id=session), "--format", "tsv"],
            capture_output=True,
            text=True,
            timeout=TIMEOUT_SECONDS,
        )
    except subprocess.TimeoutExpired:
        print(f"specialist_quiet: kilo db gave no answer in {TIMEOUT_SECONDS} seconds", file=sys.stderr)
        return 1
    if done.returncode != 0:
        print(f"specialist_quiet: kilo db failed with exit {done.returncode}", file=sys.stderr)
        print(done.stderr, file=sys.stderr)
        return done.returncode

    rows = [line.strip() for line in done.stdout.splitlines() if line.strip()]
    if not rows or rows[0] != "quiet_minutes" or len(rows) > 2:
        print(f"specialist_quiet: unexpected answer from kilo db:\n{done.stdout}", file=sys.stderr)
        return 1
    # A session with no parts answers the header and an empty (NULL) row.
    value = rows[1] if len(rows) == 2 else ""
    print("no writes yet" if value in ("", "NULL") else value)
    return 0


if __name__ == "__main__":
    sys.exit(main())
