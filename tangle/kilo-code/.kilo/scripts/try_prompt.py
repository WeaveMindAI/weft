#!/usr/bin/env python3
"""Run one prompt on one input, the way the program's model will see it.

The prompt-engineer tries every prompt it writes on real inputs before it
hands it over, and it does that through Kilo itself rather than through
weft: no run starts, and no provider of the program's is billed.

    python3 .kilo/scripts/try_prompt.py --model kilo/~anthropic/claude-sonnet-latest \
        --prompt assets/prompts/triage.md --input .scratch/prompt-engineer/triage/input-1.txt

    python3 .kilo/scripts/try_prompt.py --models

What it runs is one `kilo run`, with the environment and the one-off agent
built here rather than typed by the caller:

  KILO_CONFIG_CONTENT              defines the agent `trial`, whose prompt is
                                   the prompt file and whose permissions are
                                   {"*": "deny"}, so it has no tools
  KILO_DISABLE_PROJECT_CONFIG=1    this project's agents, plugins and AGENTS.md
                                   stay out
  KILO_DISABLE_CLAUDE_CODE=1       Claude Code instructions on the machine stay out
  --pure                           plugins installed elsewhere stay out
  stdin                            the input file, sent as the one message

The answer is printed as it came back, and nothing else. `--models` prints
`kilo models` instead, for picking a model of the right class when Kilo is
not signed in to its own gateway.

It runs the `kilo` on PATH, or, when there is none, the newest one inside
the Kilo editor extension (`~/.vscode/extensions/kilocode.kilo-code-*/bin/kilo`
and the like). A missing `kilo`, a missing file, a model name that is not one,
or a failed call ends with a non-zero exit and says why.
"""

import argparse
import json
import os
import re
import shutil
import subprocess
import sys
from pathlib import Path

TIMEOUT_SECONDS = 120

# Where the Kilo editor extension keeps its own `kilo`, for a machine where
# that is the only one (VS Code, its remote server, and its forks).
EXTENSION_GLOBS = (
    ".vscode/extensions/kilocode.kilo-code-*/bin/kilo",
    ".vscode-server/extensions/kilocode.kilo-code-*/bin/kilo",
    ".vscode-insiders/extensions/kilocode.kilo-code-*/bin/kilo",
    ".cursor/extensions/kilocode.kilo-code-*/bin/kilo",
    ".windsurf/extensions/kilocode.kilo-code-*/bin/kilo",
)

# `provider/model`, as `kilo models` prints them (`kilo/~anthropic/claude-haiku-latest`).
MODEL = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._~:/-]*$")


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


def run(command: list[str], env: dict[str, str], stdin=None) -> int:
    try:
        done = subprocess.run(
            command, stdin=stdin, capture_output=True, text=True, timeout=TIMEOUT_SECONDS, env=env
        )
    except subprocess.TimeoutExpired:
        print(f"try_prompt: kilo gave no answer in {TIMEOUT_SECONDS} seconds", file=sys.stderr)
        return 1
    if done.returncode != 0:
        print(f"try_prompt: kilo failed with exit {done.returncode}", file=sys.stderr)
        print(done.stderr, file=sys.stderr)
        return done.returncode
    sys.stdout.write(done.stdout)
    return 0


def main() -> int:
    parser = argparse.ArgumentParser(description="Try one prompt on one input through kilo run.")
    parser.add_argument("--models", action="store_true", help="print the models this Kilo can reach, and nothing else")
    parser.add_argument("--model", help="provider/model, for example kilo/~anthropic/claude-sonnet-latest")
    parser.add_argument("--prompt", help="the prompt file, for example assets/prompts/triage.md")
    parser.add_argument("--input", help="the file holding the input exactly as the node sends it")
    args = parser.parse_args()

    kilo = find_kilo()
    if kilo is None:
        print(
            "try_prompt: found no `kilo` to run: none on PATH, and none in an editor "
            f"extension ({', '.join(EXTENSION_GLOBS)} under your home folder), "
            "so the prompt cannot be tried here; report BLOCKED with this command",
            file=sys.stderr,
        )
        return 1

    if args.models:
        if args.model or args.prompt or args.input:
            parser.error("--models takes nothing else")
        return run([kilo, "models"], dict(os.environ))

    if not (args.model and args.prompt and args.input):
        parser.error("--model, --prompt and --input are all needed (or --models alone)")
    if not MODEL.match(args.model):
        parser.error(f"--model {args.model!r} is not a provider/model name")

    prompt = Path(args.prompt).resolve()
    given = Path(args.input).resolve()
    for label, path in (("prompt", prompt), ("input", given)):
        if not path.is_file():
            print(f"try_prompt: the {label} file {path} does not exist", file=sys.stderr)
            return 1

    config = {"agent": {"trial": {"mode": "primary", "prompt": f"{{file:{prompt}}}", "permission": {"*": "deny"}}}}
    env = dict(os.environ)
    env["KILO_CONFIG_CONTENT"] = json.dumps(config)
    env["KILO_DISABLE_PROJECT_CONFIG"] = "1"
    env["KILO_DISABLE_CLAUDE_CODE"] = "1"

    with given.open("rb") as stdin:
        return run([kilo, "run", "--pure", "--agent", "trial", "-m", args.model], env, stdin)


if __name__ == "__main__":
    sys.exit(main())
