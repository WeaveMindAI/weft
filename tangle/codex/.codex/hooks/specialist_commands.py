#!/usr/bin/env python3
"""PreToolUse hook on Bash: each specialist runs its own commands and no others.

Codex's command rules (.codex/rules/tangle.rules) hold for every agent alike,
and a custom agent file cannot change them, so a read-only specialist could
otherwise run anything Tangle may run. This hook gives each specialist its
own list instead, kept in specialist_commands.json beside it: a key per agent
name (the `name` in its .codex/agents/ file), and under it the commands that
agent may run, each as the words a command starts with ("weft events" allows
`weft events <run-id> --node x`).

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
  - a command written over several lines, and any shell syntax the line is
    not read for (`shell_syntax_refusal` below): outside quotes, a `$` in any
    use (`$NAME`, `$(...)`, `${...}`, `$'...'`, `$1`), a backtick, `<(...)`
    and `>(...)`, an input redirection other than a plain `<`, a `{`, `}`,
    `(`, `)`, `[` or `#`, and a word starting with `=` (zsh reads those too);
    inside double quotes, a `$` or a backtick;
  - an output redirection (`>`, `>>`, `2>&1`): a command's output is read
    whole, never sent somewhere else;
  - an input redirection from outside the project (`< /etc/x`, `< ../x`);
  - `--on` or `--dispatcher` on a weft command, unless the agent's entry has
    `"names_targets": true` (only the deployer acts on a cloud install);
  - a `cd` out of the project, or one not written as `cd ./<folder> && ...`
    (the `./` makes the shell skip CDPATH; the parts after it are checked
    from that folder), or one whose folder holds a `..` or goes through a
    link.

An entry may be an object instead of a string:
  {"prefix": "weft test-node", "refuse": [["--tier", "live"], ["--yes"]]}
refuses the command when any of those word sequences appears in it, and
  {"prefix": "kill", "args": "pids"}
allows only plain process ids after the prefix. A prefix whose last word ends
in `@` matches any version (`pnpm dlx sv@` allows `pnpm dlx sv@0.17.0 create
front`). An entry with `"check"` also runs that tool's own check on the rest
of the line (`CHECKS` below), each knowing the options it allows and refusing
every other: `grep` and `rg` search inside the project and never follow a link
out of it (`rg` never with an option that runs another program, like `--pre`);
`ffprobe` reads one file inside the project and `ffmpeg` reads files inside
it, each as `-f <format> -i <file>` so neither guesses a file's format;
`ffmpeg` writes only under the entry's `"writes_under"` folder. There is no
`psql` check: a specialist's command runs in the sandbox, which has no
network, so `psql` could reach no database.

A leading `timeout <duration>` is stripped first. What Codex hands the hook
is the command text alone: `exec_command`'s own working folder (`workdir`)
does not reach it, so the folder checks read paths from the session's `cwd`.
The project is the nearest folder at or above that `cwd` holding a
`weft.toml`. A malformed hook input is said on stderr with a non-zero exit,
which Codex reports as a hook failure before going on: this hook never
refuses a call it could not read.
"""

import glob
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
    reason = shell_syntax_refusal(command)
    if reason is not None:
        return reason
    try:
        lexer = shlex.shlex(command, posix=True, punctuation_chars=True)
        lexer.whitespace_split = True
        tokens = list(lexer)
    except ValueError as error:
        return f"the command could not be read ({error})"
    # Each part with the separator that follows it (None for the last).
    parts, current = [], []
    for token in tokens:
        if token in SEPARATORS:
            parts.append((current, token))
            current = []
        else:
            current.append(token)
    parts.append((current, None))

    for part, separator in parts:
        if not part:
            return "the command has an empty part"
        reason = part_refusal(part, entry, cwd, project)
        if reason is not None:
            return reason
        # An accepted `cd` moves where the parts after it run, so they are
        # read from there; only `&&` makes sure they run after it, in that folder.
        if part[0] == "cd":
            if separator not in ("&&", None):
                return "a `cd` is followed only by `&&`: write `cd ./<folder> && <command>`"
            cwd = os.path.realpath(os.path.join(cwd, part[1]))
    return None


def shell_syntax_refusal(command: str):
    """Why `command` holds shell syntax the tokenizer does not read the way bash does, or None.

    The tokenizer below (`shlex`) knows words, quotes, the separators and the
    redirections, and nothing else, so anything else bash would act on is
    refused here, before a word is read. The walk follows bash's quoting:
    inside '...' everything is literal up to the next `'`; inside "..." a
    backslash escapes only `"` and `\\`, and a `$` or a backtick is refused;
    outside quotes a backslash escapes the next character. A `$` that bash
    leaves as it is (at the end of the line, before a space, or before the
    `"` that closes its quotes, as in "route$") passes. The line may run in
    zsh as well (the user's login shell), so what zsh alone acts on is
    refused too: `(` and `)` outside quotes (a glob qualifier like
    `*(e:...:)` runs code, and `=(...)` runs a command), and a word starting
    with an unquoted `=` (`=ls` becomes the path of `ls`).
    """
    quote = None
    word_start = True
    i = 0
    while i < len(command):
        c = command[i]
        after = command[i + 1] if i + 1 < len(command) else ""
        if quote == "'":
            if c == "'":
                quote = None
        elif quote == '"':
            if c == '"':
                quote = None
            elif c == "\\":
                if after in ("$", "`"):
                    return f"`\\{after}` inside double quotes is read differently by the shell and by this check: put the text in single quotes"
                if after in ('"', "\\"):
                    i += 1
            elif c == "`" or (c == "$" and after not in ('"', " ", "\t")):
                return shell_syntax_message(c, after)
        elif c == "=" and word_start:
            return "a word starting with `=` outside quotes is a path to a program in zsh: put it in quotes"
        elif c == "\\":
            i += 1
        elif c in ("'", '"'):
            quote = c
        elif c == "[":
            return "`[` outside quotes is a pattern each shell reads its own way: put the pattern in quotes, or write the path out"
        elif c in ("`", "{", "}", "#") or (c == "$" and after not in ("", " ", "\t")):
            return shell_syntax_message(c, after)
        elif c in ("<", ">") and after == "(":
            return f"`{c}(...)` runs a command to make a file: write the file's name out"
        elif c == "<" and after in ("<", ">", "&"):
            return f"`<{after}` is never run: the only input redirection here is a plain `< file` inside the project"
        elif c in ("(", ")"):
            return f"`{c}` outside quotes is shell syntax (a subshell, or in zsh a glob qualifier or `=(...)`, which run code): put it in quotes"
        word_start = quote is None and c in (" ", "\t", ";", "|", "&", "<", ">")
        i += 1
    return None


def shell_syntax_message(c: str, after: str) -> str:
    if c == "$":
        return (
            f"`${after}` is expanded by the shell before the command runs (a variable, `$(...)`, `${{...}}`, `$'...'` and the like), "
            "which this check cannot follow: write the value out, or put text meant literally in single quotes"
        )
    if c == "`":
        return "a backtick runs a command inside the line: write its output out instead"
    if c == "#":
        return "`#` outside quotes is a shell comment, which this check cannot follow: put it in quotes"
    return f"`{c}` outside quotes is shell syntax (brace expansion or a group): write each word out, or put it in quotes"


def part_refusal(words: list, entry: dict, cwd: str, project: str):
    """Why one simple command is refused, or None when it is allowed."""
    part = words
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
            folder = rest[0] if len(rest) == 1 else ""
            if (
                len(part) == 2
                and folder.startswith("./")
                and not any(c in GLOB for c in folder)
                and ".." not in folder.split("/")
                and not through_link(folder, cwd)
                and os.path.isdir(os.path.join(cwd, folder))
                and inside(folder, cwd, project)
            ):
                return None
            return "`cd` goes only to a folder inside the project, written out with no `..` and no link on the way: `cd ./<folder>`"
        if isinstance(item, dict):
            if item.get("args") == "pids" and not (rest and all(word.isdigit() and int(word) > 1 for word in rest)):
                return f"`{prefix_of(item)}` takes only process ids"
            for banned in item.get("refuse", []):
                if contains(rest, banned):
                    return f"`{' '.join(banned)}` is refused for `{prefix_of(item)}`"
            return check_refusal(item, rest, cwd, project)
        return None
    return f"`{' '.join(words[:3])}` is not on your list"


def through_link(folder: str, cwd: str) -> bool:
    """Whether any piece of `folder`, walked from `cwd` (or from `/`), is a link.

    The shell keeps the folder a `cd` goes to as typed and this check keeps
    it with its links resolved; with no link and no `..` in the way, the two
    are the same folder.
    """
    path = "/" if folder.startswith("/") else cwd
    for piece in folder.split("/"):
        if piece in ("", "."):
            continue
        path = os.path.join(path, piece)
        if os.path.islink(path):
            return True
    return False


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


# The checks an entry names with `"check"`. Each reads the words after the
# entry's prefix and says why they are refused, or None. A tool whose own
# options can run a program, write a file or reach past the project is
# allowed only in the shapes its check knows; any option the check does not
# know is refused, so a new or rare option never slips through.

GLOB = set("*?[")
RG_RUNS = ("--pre", "--pre-glob", "--search-zip", "--hostname-bin")
RG_SHORT_TAKES_VALUE = set("ABCdEefgjmMrtT")
GREP_SHORT_TAKES_VALUE = set("ABCDdefm")
FFPROBE_FLAGS = {
    "hide_banner", "show_format", "show_streams", "show_frames", "show_packets",
    "show_chapters", "show_programs", "show_error", "show_data", "show_private_data",
    "count_frames", "count_packets", "pretty", "sexagesimal", "unit", "prefix",
    "byte_binary_prefix", "bitexact",
}
FFPROBE_VALUES = {
    "v", "loglevel", "of", "print_format", "output_format", "show_entries",
    "select_streams", "read_intervals",
}
FFMPEG_FLAGS = {"hide_banner", "y", "n", "nostdin", "nostats", "stats", "an", "vn", "sn", "dn", "shortest", "re"}
FFMPEG_VALUES = {
    "v", "loglevel", "ss", "t", "to", "sseof", "r", "framerate", "s", "frames",
    "vframes", "aframes", "c", "codec", "vcodec", "acodec", "b", "q", "qscale",
    "crf", "preset", "tune", "profile", "level", "pix_fmt", "ar", "ac", "map",
    "loop", "update", "movflags", "aspect", "g", "threads", "metadata",
}
# An input's format is always named, so ffmpeg never guesses it from the
# file: a playlist or a concat list it guessed would read the files it names.
MEDIA_INPUT_USAGE = "reads an input only as `-f <format> -i <file>`, the format named (`-f mp4 -i clip.mp4`), so it never guesses a file's format"
FFMPEG_FILTER_OPTIONS = {"vf", "af", "filter"}
# Formats that read and write one plain file (or a numbered run of images):
# `lavfi`, `concat`, `tee`, `segment` and the like reach other files.
FFMPEG_FORMATS = {"image2", "mp4", "mov", "webm", "matroska", "gif", "apng", "mp3", "wav", "ogg", "flac", "null"}
# Filters that only reshape what flows through them; `movie`, `drawtext`,
# `subtitles`, `sendcmd`, `metadata` and the like read or write files.
FFMPEG_FILTERS = {
    "scale", "fps", "crop", "pad", "trim", "atrim", "setpts", "asetpts", "select",
    "aselect", "thumbnail", "tile", "format", "aformat", "hflip", "vflip",
    "transpose", "split", "asplit", "palettegen", "paletteuse", "volume",
    "aresample", "loudnorm", "fade", "afade", "reverse", "areverse", "setsar",
    "setdar", "framestep", "null", "anull", "showwavespic", "showspectrumpic",
}


def check_refusal(item: dict, rest: list, cwd: str, project: str):
    check = item.get("check")
    if check is None:
        return None
    checker = CHECKS.get(check)
    if checker is None:
        return f"the list names a check `{check}` this hook does not know"
    return checker(item, rest, cwd, project)


def grep_refusal(item: dict, rest: list, cwd: str, project: str):
    """grep: no option that follows a link out of the project, and every word that reads as a path inside it."""
    for word in rest:
        name = word[2:].split("=", 1)[0] if word.startswith("--") else None
        # grep takes any unambiguous start of a long option (`--deref`).
        if name and "dereference-recursive".startswith(name):
            return f"`{word}` follows links out of the project: search with `-r`, which does not"
        if "R" in short_letters(word, GREP_SHORT_TAKES_VALUE):
            return f"`{word}` holds `-R`, which follows links out of the project: search with `-r`, which does not"
    return paths_refusal(rest, cwd, project)


def rg_refusal(item: dict, rest: list, cwd: str, project: str):
    """rg: no option that runs another program or follows a link out of the project, and every path inside it."""
    for word in rest:
        if any(word == flag or word.startswith(flag + "=") for flag in RG_RUNS):
            return f"`{word}` runs another program: `rg` never does here"
        if word == "--follow":
            return "`--follow` follows links out of the project: `rg` never does here"
        letters = short_letters(word, RG_SHORT_TAKES_VALUE)
        if "z" in letters:
            return f"`{word}` holds `-z`, which runs a decompressor: `rg` never runs another program here"
        if "L" in letters:
            return f"`{word}` holds `-L`, which follows links out of the project: `rg` never does here"
    return paths_refusal(rest, cwd, project)


def paths_refusal(rest: list, cwd: str, project: str):
    """Why a search's words reach outside the project, or None."""
    for word in rest:
        if reads_outside(word, cwd, project):
            return f"`{word}` reaches outside the project: search inside it"
    return None


def short_letters(word: str, takes_value: set) -> str:
    """The option letters of a cluster like `-rnL`, up to the first one that
    takes a value (whatever follows it is that value). Empty for any other word."""
    if not word.startswith("-") or word.startswith("--"):
        return ""
    letters = ""
    for letter in word[1:]:
        letters += letter
        if letter in takes_value:
            break
    return letters


def ffprobe_refusal(item: dict, rest: list, cwd: str, project: str):
    """ffprobe: known options only, and one input inside the project."""
    inputs = 0
    i = 0
    while i < len(rest):
        word = rest[i]
        name = word[1:] if word.startswith("-") and len(word) > 1 else None
        if name in FFPROBE_FLAGS:
            i += 1
            continue
        if name in FFPROBE_VALUES and i + 1 < len(rest):
            i += 2
            continue
        if name == "f" and i + 1 < len(rest):
            if rest[i + 1] not in FFMPEG_FORMATS:
                return format_refusal(rest[i + 1])
            i += 2
            continue
        if name == "i" and i + 1 < len(rest):
            if not format_named(rest, i):
                return f"`ffprobe` {MEDIA_INPUT_USAGE}"
            word = rest[i + 1]
            i += 1
        elif name is not None:
            return f"`ffprobe {word}` is not an option you may use here"
        else:
            return f"`ffprobe` {MEDIA_INPUT_USAGE}"
        reason = media_input_refusal(word, cwd, project)
        if reason is not None:
            return reason
        inputs += 1
        i += 1
    return None if inputs == 1 else "`ffprobe` reads exactly one file"


def ffmpeg_refusal(item: dict, rest: list, cwd: str, project: str):
    """ffmpeg: known options only, inputs inside the project, every output under the entry's `writes_under`."""
    folder = item.get("writes_under")
    if not isinstance(folder, str) or not folder:
        return "the list gives `ffmpeg` no `writes_under` folder"
    under = os.path.realpath(os.path.join(project, folder))
    inputs = outputs = 0
    pending_format = None
    i = 0
    while i < len(rest):
        word = rest[i]
        if word.startswith("-") and len(word) > 1:
            name = word[1:].split(":", 1)[0]
            if name in FFMPEG_FLAGS:
                i += 1
                continue
            if i + 1 >= len(rest):
                return f"`ffmpeg {word}` needs a value"
            value = rest[i + 1]
            if name == "i":
                if not format_named(rest, i):
                    return f"`ffmpeg` {MEDIA_INPUT_USAGE}"
                reason = media_input_refusal(value, cwd, project)
                if reason is not None:
                    return reason
                inputs += 1
                pending_format = None
            elif name == "f":
                if value not in FFMPEG_FORMATS:
                    return format_refusal(value)
                pending_format = value
            elif name in FFMPEG_FILTER_OPTIONS:
                reason = filtergraph_refusal(value)
                if reason is not None:
                    return reason
            elif name not in FFMPEG_VALUES:
                return f"`ffmpeg {word}` is not an option you may use here"
            i += 2
            continue
        if not (word == "-" and pending_format == "null"):
            target = os.path.realpath(os.path.join(cwd, word))
            if any(c in word for c in ":|") or any(c in GLOB for c in word) or not target.startswith(under + os.sep):
                return f"`ffmpeg` writes only under `{folder}/` in the project, and `{word}` is not there"
        outputs += 1
        pending_format = None
        i += 1
    if not inputs or not outputs:
        return "`ffmpeg` here takes at least one `-i <input>` and one output file"
    return None


def format_named(rest: list, i: int) -> bool:
    """Whether the `-i` at `rest[i]` comes right after `-f <an allowed format>`."""
    return i >= 2 and rest[i - 2] == "-f" and rest[i - 1] in FFMPEG_FORMATS


def format_refusal(value: str) -> str:
    return f"`-f {value}` is not a format you may use here (these are: {', '.join(sorted(FFMPEG_FORMATS))})"


def media_input_refusal(word: str, cwd: str, project: str):
    """Why `word`, as an ffmpeg or ffprobe input, is refused: it must be a plain file inside the project."""
    if any(c in word for c in ":|") or not inside(word, cwd, project):
        return f"`{word}` is not a plain file inside the project, the only input read here"
    return None


def filtergraph_refusal(graph: str):
    """Why a `-vf` / `-af` filtergraph is refused: every filter in it must be one that reads and writes no file.

    ffmpeg reads a backslash differently inside quotes and out, and at each of
    its two levels of escaping, so a filtergraph holding one is refused
    rather than read: quote a value with `'...'` instead.
    """
    if "\\" in graph:
        return "a backslash is never in a filter here: put a value holding `,` or `;` in '...' instead"
    for piece in split_filtergraph(graph):
        name = re.match(r"(?:\[[^\]]*\]\s*)*([A-Za-z0-9_]*)", piece.strip()).group(1)
        if name not in FFMPEG_FILTERS:
            return f"the filter `{name or piece}` is not one you may use here (these are: {', '.join(sorted(FFMPEG_FILTERS))})"
    return None


def split_filtergraph(graph: str) -> list:
    """The filters of a filtergraph (one with no backslash), split on the `,` and `;` that are not quoted."""
    pieces, current, quoted = [], "", False
    for c in graph:
        if c == "'":
            quoted = not quoted
        if c in ",;" and not quoted:
            pieces.append(current)
            current = ""
        else:
            current += c
    pieces.append(current)
    return pieces


def reads_outside(word: str, cwd: str, project: str) -> bool:
    """Whether `word`, read as a path by the tool or the shell, could name something outside the project.

    A search pattern is a word like any other, so only a word that looks
    like a path is held to the project: one starting with `~`; an absolute
    one or one with a `..` step that the shell would expand (`/etc/*`); or
    one that names something on disk outside the project (`/etc/passwd`,
    `../other`, a link out). A pattern such as `/api/v1` names nothing and
    passes. The value of a `--flag=<value>` is read the same way, and so is
    every possible value glued to a cluster of short options: `-rf/etc/x`
    reads as `f/etc/x` and as `/etc/x`, since any letter but the first may
    be the one that takes it.
    """
    if word.startswith("--"):
        _, eq, value = word.partition("=")
        candidates = [value] if eq else []
    elif word.startswith("-"):
        candidates = [word[k:] for k in range(2, len(word))]
    else:
        candidates = [word]
    for candidate in candidates:
        if not candidate:
            continue
        if candidate.startswith("~"):
            return True
        globbed = any(c in GLOB for c in candidate)
        if (globbed or os.path.lexists(os.path.join(cwd, candidate))) and not inside(candidate, cwd, project):
            return True
    return False


CHECKS = {
    "grep": grep_refusal,
    "rg": rg_refusal,
    "ffprobe": ffprobe_refusal,
    "ffmpeg": ffmpeg_refusal,
}


def dot_glob(path: str) -> bool:
    """Whether a piece of `path` starts with `.` and holds a glob character:
    bash 3.2 to 5.1 match `..` with `.?` or `.*`, so such a piece may step out."""
    return any(piece.startswith(".") and any(c in GLOB for c in piece) for piece in path.split("/"))


def inside(path: str, cwd: str, project: str) -> bool:
    """Whether `path`, read from `cwd`, lands inside the project folder.

    A path holding `*`, `?` or `[` is read the way the shell expands it:
    every file it matches, each with its links resolved, must be inside (so
    `lin*/passwd` through a link to /etc is not), and one that matches
    nothing is the text the shell passes on as it is. `**` is never inside:
    zsh follows links through it in a way this check does not mirror.
    """
    if path.startswith("~") or "**" in path or dot_glob(path):
        return False
    root = os.path.realpath(project)
    matches = glob.glob(path, root_dir=cwd) if any(c in GLOB for c in path) else []
    for match in matches or [path]:
        target = os.path.realpath(os.path.join(cwd, match))
        if not (target == root or target.startswith(root + os.sep)):
            return False
    return True


if __name__ == "__main__":
    sys.exit(main())
