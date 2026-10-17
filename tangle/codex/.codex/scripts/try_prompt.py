#!/usr/bin/env python3
"""Run one prompt on one input, the way the program's model will see it.

The prompt-engineer tries every prompt it writes on real inputs before it
hands it over, and it does that through Codex itself rather than through
weft: no run starts, and no provider of the program's is billed.

    python3 .codex/scripts/try_prompt.py --model gpt-6-luna \
        --prompt assets/prompts/triage.md --input .scratch/prompt-engineer/triage/input-1.txt

What it runs is one `codex exec` in a fresh empty folder outside the project,
so no project instructions, hooks or config reach the model:

  --ephemeral                      no session file is written
  --skip-git-repo-check            the empty folder is not a repository
  --ignore-user-config             the user's own config.toml stays out
  --disable hooks, plugins, apps   nothing adds itself to the session
  -s read-only                     nothing can be written
  -c model_instructions_file=...   the prompt replaces Codex's built-in
                                   instructions
  -c project_doc_max_bytes=0       no project AGENTS.md is read
  -c skills.include_instructions=false, include_permissions_instructions=false,
     include_environment_context=false, include_collaboration_mode_instructions=false
                                   no skill list, sandbox notes, folder details
                                   or collaboration mode go in beside the input
  -c agents.enabled=false          no multi-agent tools, and none of the notes
                                   about them that newer models get
  -c features.shell_tool=false     no commands
  -c web_search="disabled"         no web search
  -                                the prompt to answer is read from stdin,
                                   which is the input file

`codex debug prompt-input` with the same settings shows the model receiving
the input as its one message, after the prompt, on gpt-6-luna, gpt-5.6-terra
and gpt-5.6-luna alike. The one thing that still
reaches it is the user's own `~/.codex/AGENTS.md`, when they keep one.

It runs the `codex` on PATH, or, when there is none, the newest one inside
an editor's ChatGPT extension (`~/.vscode-server/extensions/openai.chatgpt-*/
bin/*/codex` and the like). The answer is printed as it came back, and nothing else. A missing `codex`,
a missing file or a failed call ends with a non-zero exit and says why.

`.codex/rules/tangle.rules` allows this exact script outside the sandbox
(the call needs the network), so it runs without an approval prompt.
"""

import argparse
import json
import os
import shutil
import subprocess
import sys
import tempfile
from pathlib import Path

TIMEOUT_SECONDS = 120

# Where the ChatGPT editor extension keeps its own `codex`, for a machine
# where that is the only one (VS Code, its remote server, and its forks).
EXTENSION_GLOBS = (
    ".vscode/extensions/openai.chatgpt-*/bin/*/codex",
    ".vscode-server/extensions/openai.chatgpt-*/bin/*/codex",
    ".vscode-insiders/extensions/openai.chatgpt-*/bin/*/codex",
    ".cursor/extensions/openai.chatgpt-*/bin/*/codex",
    ".windsurf/extensions/openai.chatgpt-*/bin/*/codex",
)


def find_codex() -> str | None:
    """`codex` on PATH, else the newest one inside an editor extension."""
    on_path = shutil.which("codex")
    if on_path:
        return on_path
    home = Path.home()
    found = [p for pattern in EXTENSION_GLOBS for p in home.glob(pattern) if os.access(p, os.X_OK)]
    if not found:
        return None
    return str(max(found, key=lambda p: p.stat().st_mtime))


def main() -> int:
    parser = argparse.ArgumentParser(description="Try one prompt on one input through codex exec.")
    parser.add_argument("--model", required=True, help="the model slug, for example gpt-6-luna")
    parser.add_argument("--prompt", required=True, help="the prompt file, for example assets/prompts/triage.md")
    parser.add_argument("--input", required=True, help="the file holding the input exactly as the node sends it")
    args = parser.parse_args()

    prompt = Path(args.prompt).resolve()
    given = Path(args.input).resolve()
    for label, path in (("prompt", prompt), ("input", given)):
        if not path.is_file():
            print(f"try_prompt: the {label} file {path} does not exist", file=sys.stderr)
            return 1

    codex = find_codex()
    if codex is None:
        print(
            "try_prompt: found no `codex` to run: none on PATH, and none in an editor "
            f"extension ({', '.join(EXTENSION_GLOBS)} under your home folder), "
            "so the prompt cannot be tried here; report BLOCKED with this command",
            file=sys.stderr,
        )
        return 1

    with tempfile.TemporaryDirectory(prefix="try-prompt-") as empty, given.open("rb") as stdin:
        command = [
            codex,
            "exec",
            "--ephemeral",
            "--skip-git-repo-check",
            "--ignore-user-config",
            "--disable",
            "hooks",
            "--disable",
            "plugins",
            "--disable",
            "apps",
            "-C",
            empty,
            "-s",
            "read-only",
            "-m",
            args.model,
            "-c",
            f"model_instructions_file={json.dumps(str(prompt))}",
            "-c",
            "project_doc_max_bytes=0",
            "-c",
            "skills.include_instructions=false",
            "-c",
            "include_permissions_instructions=false",
            "-c",
            "include_environment_context=false",
            "-c",
            "include_collaboration_mode_instructions=false",
            "-c",
            "agents.enabled=false",
            "-c",
            "features.shell_tool=false",
            "-c",
            'web_search="disabled"',
            "-",
        ]
        try:
            done = subprocess.run(
                command, stdin=stdin, capture_output=True, text=True, timeout=TIMEOUT_SECONDS
            )
        except subprocess.TimeoutExpired:
            print(f"try_prompt: codex exec gave no answer in {TIMEOUT_SECONDS} seconds", file=sys.stderr)
            return 1

    if done.returncode != 0:
        print(f"try_prompt: codex exec failed with exit {done.returncode}", file=sys.stderr)
        print(done.stderr, file=sys.stderr)
        return done.returncode
    sys.stdout.write(done.stdout)
    return 0


if __name__ == "__main__":
    sys.exit(main())
