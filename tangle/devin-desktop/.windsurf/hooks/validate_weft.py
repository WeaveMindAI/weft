#!/usr/bin/env python3
"""post_write_code hook: the compiler answers every edit, at the right tier.

The compiler has three tiers, and this hook is the first one:

  1. edit tier (this hook): strict parse + structural validation. Fast,
     local, nothing runs. A broken edit is named the moment it lands.
  2. runtime tier: rule-runtime checks (a connection not picked, other
     things only knowable once the program runs). `weft validate` reports
     them; a build deliberately skips them and a run is not gated on
     them, so they surface at execution as a loud node failure. Fixed by
     a click in the editor, never the source, so they never belong in
     the edit loop.
  3. full compilation: `weft build`, structural errors only by design (a
     program still being wired up still builds) plus the cargo and image
     build.

After every write this runs `weft validate` (strict pipeline) and
keeps tier-1 findings: severity "error" minus the rule-runtime slug. Devin's post-hooks cannot block and cannot
speak back into the model's context, so this one leaves its answer on disk
instead: findings go to `.weft/validate-findings.txt`, and the file is
DELETED when the program is clean. The persona tells Tangle to read that
file after a batch of edits, which is what closes the loop by hand.

One warning rides along: `level-too-large` (a level of the graph holding
more than fifteen items). It is not an error, the program still runs, but
the model is the one writing the graph, and this is the compiler's way of
saying the file is becoming a wall. It is printed under its own heading so
the model can tell law-breaking (errors) from unreadability (this) at a
glance.

Silence means clean: exit 0 with no output when there is nothing to say,
when the edit touched nothing weft-related, or when the toolchain needed to
check is missing (a missing `weft` never blocks work).

What it checks:
  - an edit to `src/main.weft`, or to a `.weft` file it includes (directly
    or through other includes): `main.weft`, since an included file alone
    reports false errors about what its includer wires into it
  - an edit to any other `.weft` file: that file, exactly as saved
  - an edit anywhere under `nodes/` (metadata.json, mod.rs, deps.toml,
    package.toml, tests.rs): the project's entry `main.weft`, because a
    catalog change can break the program that uses it. Findings about node
    types no folder under `nodes/` declares yet (`unknown node type`, and
    what follows from it) are left out: they are another node's unwritten
    work, not this edit's error. An edit to a `.weft` file reports them.

The hook only reports: it never changes, reverts or undoes an edited file.
"""

import json
import os
import re
import shutil
import subprocess
import sys

TIMEOUT = 60


def project_root(start: str):
    """Walk up from a path until the directory holding `weft.toml`."""
    cur = os.path.dirname(start) if not os.path.isdir(start) else start
    while True:
        if os.path.isfile(os.path.join(cur, "weft.toml")):
            return cur
        parent = os.path.dirname(cur)
        if parent == cur:
            return None
        cur = parent


INCLUDE = re.compile(r'@include\(\s*"([^"]+)"\s*\)')


def entry_for(root: str, path: str) -> str:
    """The file to validate for an edit to the `.weft` file `path`: the
    project's entry `src/main.weft` when `path` is that file or is pulled
    in by it, directly or through other includes (an included file
    checked alone misses what its includer wires into it, and reports
    false errors such as an unmet required port); `path` itself when
    nothing includes it. An `@include` path is relative to the file that
    writes it."""
    entry = os.path.join(root, "src", "main.weft")
    if not os.path.isfile(entry):
        return path
    want = os.path.realpath(path)
    seen = set()
    stack = [os.path.realpath(entry)]
    while stack:
        cur = stack.pop()
        if cur in seen:
            continue
        seen.add(cur)
        if cur == want:
            return entry
        try:
            with open(cur, "r", encoding="utf-8") as f:
                text = f.read()
        except OSError:
            continue
        for line in text.splitlines():
            if line.lstrip().startswith("#"):
                continue
            for rel in INCLUDE.findall(line):
                stack.append(os.path.realpath(os.path.join(os.path.dirname(cur), rel)))
    return path


def validate(root: str, target: str, node_edit: bool = False):
    """Run the fast validate; return (blocking_text or None). `node_edit`:
    the edit was under `nodes/`, so findings about node types nobody has
    written yet are left out (`_drop_unwritten`)."""
    weft = shutil.which("weft")
    if weft is None:
        return None
    try:
        with open(target, "r", encoding="utf-8") as f:
            source = f.read()
    except OSError:
        return None
    try:
        proc = subprocess.run(
            [weft, "validate", "--file", target],
            input=source,
            capture_output=True,
            text=True,
            cwd=root,
            timeout=TIMEOUT,
        )
    except (subprocess.TimeoutExpired, OSError):
        return None
    if proc.returncode != 0:
        # The validate pipeline itself refused (broken weft.toml, catalog
        # error). That is real feedback, pass it through.
        err = proc.stderr.strip() or proc.stdout.strip()
        if err:
            return "weft validate could not run:\n" + err
        return None
    try:
        out = json.loads(proc.stdout)
    except ValueError:
        return None
    diags = out.get("diagnostics") or []
    # Tier 1 only: `weft validate` runs the strict pipeline in runtime
    # mode, which adds rule-runtime findings (an unpicked connection and
    # cousins). Those are runtime rules: a build deliberately skips them,
    # a run is not gated on them, and they fire at execution as a loud
    # node failure, fixed by a click in the editor rather than a source
    # edit. Blocking the edit loop on them would trap the model on an
    # error it cannot fix in source.
    errors = [
        d for d in diags
        if d.get("severity") == "error" and d.get("code") != "rule-runtime"
    ]
    if node_edit:
        errors = _drop_unwritten(root, target, errors)
    # The one warning worth the model's attention mid-build: a level of
    # the graph past fifteen items. Everything else at severity warning
    # (orphan-outputs, no-required-skip) is normal half-built noise and
    # stays out of the loop.
    level_warnings = [
        d for d in diags
        if d.get("severity") == "warning" and d.get("code") == "level-too-large"
    ]
    if not errors and not level_warnings:
        return None
    lines = []
    if errors:
        lines.append("weft: {} error{} (fix before continuing):".format(
            len(errors), "" if len(errors) == 1 else "s"))
        for d in errors:
            lines.append(_format_finding(d, target))
    if level_warnings:
        lines.append("weft: {} level warning{} (the program runs; it will not read):".format(
            len(level_warnings), "" if len(level_warnings) == 1 else "s"))
        for d in level_warnings:
            lines.append(_format_finding(d, target))
    return "\n".join(lines)


UNWRITTEN = (
    re.compile(r"unknown node type:? '([^']+)'"),
    re.compile(r"node type '([^']+)' is not ready yet"),
    re.compile(r"node '([^']+)' failed to load"),
)
DECLARED = re.compile(r'"type"\s*:\s*"([^"]+)"')
NODE_LINE = re.compile(r"^\s*([A-Za-z_]\w*)\s*=\s*([A-Z]\w*)\b", re.M)


def _types_with_a_folder(root: str):
    """Every node type some `metadata.json` under `nodes/` declares. Read
    with a regex, not a JSON parse, so a half-written file still names its
    type."""
    found = set()
    for dirpath, _dirs, names in os.walk(os.path.join(root, "nodes")):
        if "metadata.json" not in names:
            continue
        try:
            with open(os.path.join(dirpath, "metadata.json"), "r", encoding="utf-8") as f:
                m = DECLARED.search(f.read())
        except OSError:
            continue
        if m:
            found.add(m.group(1))
    return found


def _unwritten_type(d):
    """The node type a finding says the catalog does not serve, or None."""
    for pattern in UNWRITTEN:
        m = pattern.search(d.get("message") or "")
        if m:
            return m.group(1)
    return None


def _drop_unwritten(root: str, target: str, findings):
    """For an edit under `nodes/`: drop the findings about node types no
    folder declares yet. The program uses them, but they are another
    node's unwritten work, not the edit's error. Findings that follow from
    them go too: any finding on the line of such a node, or naming a node
    declared with such a type. Everything else, the edited node's own type
    and package included, is kept."""
    unwritten = {t for t in map(_unwritten_type, findings) if t}
    if not unwritten:
        return findings
    unwritten -= _types_with_a_folder(root)
    if not unwritten:
        return findings
    ids = set()
    for dirpath, _dirs, names in os.walk(os.path.dirname(target)):
        for name in names:
            if not name.endswith(".weft"):
                continue
            try:
                with open(os.path.join(dirpath, name), "r", encoding="utf-8") as f:
                    text = f.read()
            except OSError:
                continue
            ids.update(i for i, t in NODE_LINE.findall(text) if t in unwritten)
    lines = {
        (d.get("file"), d.get("line"))
        for d in findings if _unwritten_type(d) in unwritten
    }
    names = [re.compile(r"['.]" + re.escape(i) + r"'") for i in ids]

    def unwritten_work(d):
        if _unwritten_type(d) in unwritten:
            return True
        if (d.get("file"), d.get("line")) in lines:
            return True
        message = d.get("message") or ""
        return any(n.search(message) for n in names)

    return [d for d in findings if not unwritten_work(d)]


def _format_finding(d, target):
    """One diagnostic as `file:line:col [slug] message`, never a wrong
    file:line pair: a finding may point into a file spliced in by
    @include, and the diagnostic's own `file` key names it (absent =
    the compiled source)."""
    slug = d.get("code")
    tag = "[{}] ".format(slug) if slug else ""
    where = d.get("file") or target
    return "  {}:{}:{} {}{}".format(
        os.path.basename(where), d.get("line"), d.get("column"),
        tag, d.get("message"))


FINDINGS = os.path.join(".weft", "validate-findings.txt")


def _publish(root: str, text) -> None:
    """Write the findings file, or remove it when the program is clean.

    Removal matters as much as writing: a stale file from an edit that has
    since been fixed would send Tangle chasing an error that no longer
    exists."""
    path = os.path.join(root, FINDINGS)
    if text:
        os.makedirs(os.path.dirname(path), exist_ok=True)
        with open(path, "w", encoding="utf-8") as f:
            f.write(text + "\n")
        return
    try:
        os.remove(path)
    except OSError:
        pass


def main() -> int:
    """Devin hands the hook JSON on stdin. Nothing it prints reaches the
    model, so the finding is published to a file the persona reads."""
    try:
        payload = json.load(sys.stdin)
    except Exception:
        return 0
    # Cascade nests the tool's own fields under `tool_info`; the edited
    # path is `tool_info.file_path`, never a top-level key.
    tool_info = payload.get("tool_info")
    if not isinstance(tool_info, dict):
        return 0
    path = tool_info.get("file_path") or tool_info.get("path") or ""
    if not path or not isinstance(path, str):
        return 0
    if not os.path.isabs(path):
        path = os.path.abspath(path)
    root = project_root(path)
    if root is None:
        return 0

    if path.endswith(".weft"):
        target = entry_for(root, path)
    elif ("{}nodes{}".format(os.sep, os.sep)) in path:
        # A catalog change: check the program that consumes it.
        target = os.path.join(root, "src", "main.weft")
        if not os.path.isfile(target):
            return 0
    else:
        return 0

    _publish(root, validate(root, target, node_edit=not path.endswith(".weft")))
    return 0


if __name__ == "__main__":
    sys.exit(main())
