#!/usr/bin/env python3
"""PreToolUse hook on Bash: each specialist runs its own commands and no others.

Codex's command rules (.codex/rules/tangle.rules) hold for every agent alike,
and a custom agent file cannot change them, so a read-only specialist could
otherwise run anything Tangle may run. This hook gives each specialist its
own list instead, kept in specialist_commands.json beside it: a key per agent
name (the `name` in its .codex/agents/ file), and under it the commands that
agent may run, each as the words a command starts with ("weft events" allows
`weft events <run id> --node x`).

Codex puts `agent_id` and `agent_type` (the agent's name) on a hook's input
only inside a subagent. A shell call from a specialist the file names is
answered here, before it runs:
  - every command in the line (split on &&, ||, ;, | and &) starts with one
    of that agent's commands: this hook says nothing, and Codex goes on as
    usual: the command rules decide whether it leaves the sandbox, and an
    approval it would need is refused by refuse_specialist_prompts.py;
  - anything else: refused at once, with a message naming what that agent
    may run, so it does the thing another way or reports BLOCKED.
This hook never allows anything by itself: Codex takes `permissionDecision:
"allow"` from a PreToolUse hook only to rewrite a command, so letting a
command out of the sandbox stays with the rules. The main session's calls
carry no `agent_type` and are left alone, and so is an agent the file does
not name.

The shapes refused for every specialist, whatever its list says:
  - a command built at run time (`$(...)`, `${...}`, backticks) or written
    over several lines;
  - an output redirection (`>`, `>>`, `2>&1`): a command's output is read
    whole, never sent somewhere else;
  - an input redirection from outside the project (`< /etc/x`, `< ../x`);
  - `--on` or `--dispatcher` on a weft command, unless the agent's entry has
    `"names_targets": true` (only the deployer acts on a cloud install);
  - a `cd` out of the project.

An entry may be an object instead of a string:
  {"prefix": "weft test-node", "refuse": [["--tier", "live"], ["--yes"]]}
refuses the command when any of those word sequences appears in it, and
  {"prefix": "kill", "args": "pids"}
allows only plain process ids after the prefix. A prefix whose last word
ends in `@` matches any version (`pnpm dlx sv@` allows
`pnpm dlx sv@0.17.0 create front`).

A leading `timeout <duration>` is stripped first. What Codex hands the hook
is the command text alone: `exec_command`'s own working folder (`workdir`)
does not reach it, so the folder checks read paths from the session's `cwd`.
The project is the nearest folder at or above that `cwd` holding a
`weft.toml`. A malformed hook input is said on stderr with a non-zero exit,
which Codex reports as a hook failure before going on: this hook never
refuses a call it could not read.
"""

import json
import os
import re
import shlex
import sys

LISTS = os.path.join(os.path.dirname(os.path.abspath(__file__)), "specialist_commands.json")
SEPARATORS = {"&&", "||", ";", "|", "&", "|&"}
TARGET_FLAGS = ("--on", "--dispatcher")
TIMEOUT_DURATION = re.compile(r"^\d+(\.\d+)?[smhd]?$")


def main() -> int:
    try:
        request = json.load(sys.stdin)
    except json.JSONDecodeError as error:
        print(f"specialist_commands: the hook input is not JSON: {error}", file=sys.stderr)
        return 1
    if not isinstance(request, dict):
        print("specialist_commands: the hook input is not a JSON object", file=sys.stderr)
        return 1

    agent = request.get("agent_type")
    if not request.get("agent_id") or not agent:
        return 0
    try:
        with open(LISTS, encoding="utf-8") as handle:
            lists = json.load(handle)
    except (OSError, json.JSONDecodeError) as error:
        print(f"specialist_commands: cannot read {LISTS}: {error}", file=sys.stderr)
        return 1
    entry = lists.get(agent)
    if entry is None:
        return 0

    command = (request.get("tool_input") or {}).get("command")
    if not isinstance(command, str):
        print("specialist_commands: the Bash call carries no command", file=sys.stderr)
        return 1

    cwd = request.get("cwd") or os.getcwd()
    project = project_root(cwd)
    reason = refusal(command, entry, cwd, project)
    if reason is not None:
        allowed = ", ".join(prefix_of(item) for item in entry.get("commands", []))
        decide(
            "deny",
            f"{reason}. As {agent} you may run only: {allowed}. "
            "Do it another way, or report BLOCKED with the exact command.",
        )
    return 0


def project_root(cwd: str) -> str:
    """The nearest folder at or above `cwd` holding a `weft.toml`, else `cwd`."""
    folder = os.path.realpath(cwd)
    while True:
        if os.path.isfile(os.path.join(folder, "weft.toml")):
            return folder
        parent = os.path.dirname(folder)
        if parent == folder:
            return os.path.realpath(cwd)
        folder = parent


def decide(decision: str, reason: str) -> None:
    json.dump(
        {
            "hookSpecificOutput": {
                "hookEventName": "PreToolUse",
                "permissionDecision": decision,
                "permissionDecisionReason": reason,
            }
        },
        sys.stdout,
    )


def refusal(command: str, entry: dict, cwd: str, project: str):
    """Why `command` is refused for this entry, or None when it is allowed."""
    if "\n" in command or "\r" in command:
        return "a command on several lines is never run: one command per call"
    if "$(" in command or "${" in command or "`" in command:
        return "a command built at run time ($(...), ${...} or backticks) is never run"
    try:
        lexer = shlex.shlex(command, posix=True, punctuation_chars=True)
        lexer.whitespace_split = True
        tokens = list(lexer)
    except ValueError as error:
        return f"the command could not be read ({error})"
    parts, current = [], []
    for token in tokens:
        if token in SEPARATORS:
            parts.append(current)
            current = []
        else:
            current.append(token)
    parts.append(current)

    for part in parts:
        if not part:
            return "the command has an empty part"
        reason = part_refusal(part, entry, cwd, project)
        if reason is not None:
            return reason
    return None


def part_refusal(words: list, entry: dict, cwd: str, project: str):
    """Why one simple command is refused, or None when it is allowed."""
    if words[0] == "timeout" and len(words) > 2 and TIMEOUT_DURATION.match(words[1]):
        words = words[2:]
    cleaned = []
    i = 0
    while i < len(words):
        word = words[i]
        if word.startswith(">") or word in (">&", "&>") or (word.isdigit() and i + 1 < len(words) and words[i + 1].startswith(">")):
            return "an output redirection is never run: read the output whole"
        if word == "<":
            if i + 1 >= len(words) or not inside(words[i + 1], cwd, project):
                return "an input redirection must read a file inside the project"
            i += 2
            continue
        cleaned.append(word)
        i += 1
    words = cleaned
    if not words:
        return "the command is empty"

    if words[0] == "weft" and not entry.get("names_targets"):
        for word in words:
            if word in TARGET_FLAGS or any(word.startswith(flag + "=") for flag in TARGET_FLAGS):
                return f"`{word}` acts on another install, which only the deployer does"

    for item in entry.get("commands", []):
        prefix = shlex.split(prefix_of(item))
        if not starts_with(words, prefix):
            continue
        rest = words[len(prefix):]
        if prefix == ["cd"]:
            if len(rest) == 1 and inside(rest[0], cwd, project):
                return None
            return "`cd` goes only to a folder inside the project"
        if isinstance(item, dict):
            if item.get("args") == "pids" and not (rest and all(word.isdigit() and int(word) > 1 for word in rest)):
                return f"`{prefix_of(item)}` takes only process ids"
            for banned in item.get("refuse", []):
                if contains(rest, banned):
                    return f"`{' '.join(banned)}` is refused for `{prefix_of(item)}`"
        return None
    return f"`{' '.join(words[:3])}` is not on your list"


def prefix_of(item) -> str:
    return item["prefix"] if isinstance(item, dict) else item


def starts_with(words: list, prefix: list) -> bool:
    if len(words) < len(prefix):
        return False
    for i, want in enumerate(prefix):
        last = i == len(prefix) - 1
        if last and want.endswith("@"):
            if not words[i].startswith(want):
                return False
        elif words[i] != want:
            return False
    return True


def contains(words: list, sequence: list) -> bool:
    n = len(sequence)
    return any(words[i:i + n] == sequence for i in range(len(words) - n + 1))


def inside(path: str, cwd: str, project: str) -> bool:
    """Whether `path`, read from `cwd`, lands inside the project folder."""
    if path.startswith("~"):
        return False
    root = os.path.realpath(project)
    target = os.path.realpath(os.path.join(cwd, path))
    return target == root or target.startswith(root + os.sep)


if __name__ == "__main__":
    sys.exit(main())
