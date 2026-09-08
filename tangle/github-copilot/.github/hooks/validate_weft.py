#!/usr/bin/env python3
"""postToolUse hook: the compiler answers every edit, at the right tier.

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

After every edit this runs `weft validate` (strict pipeline) and
keeps tier-1 findings: severity "error" minus the rule-runtime slug. The finding rides back on
`additionalContext`, which Copilot appends to the tool result the model
sees, on the same turn as the edit.

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
  - an edit to any `.weft` file: that file, exactly as saved
  - an edit anywhere under `nodes/` (metadata.json, mod.rs, deps.toml,
    package.toml, tests.rs): the project's entry `main.weft`, because a
    catalog change can break the program that uses it
"""

import json
import os
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


def validate(root: str, target: str):
    """Run the fast validate; return (blocking_text or None)."""
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


def main() -> int:
    """Copilot hands the hook JSON on stdin and reads JSON back on stdout.

    This is the camelCase event (`postToolUse`), so the tool's own arguments
    arrive under `toolArgs`; the PascalCase spelling would send `tool_input`
    instead, and both are accepted here so the file survives either
    registration. The matcher narrows to the file-writing tools, but a
    payload with no path still leaves quietly."""
    try:
        payload = json.load(sys.stdin)
    except Exception:
        return 0
    args = payload.get("toolArgs")
    if not isinstance(args, dict):
        args = payload.get("tool_input")
    if not isinstance(args, dict):
        return 0
    path = (
        args.get("file_path")
        or args.get("filePath")
        or args.get("path")
        or ""
    )
    if not path or not isinstance(path, str):
        return 0
    if not os.path.isabs(path):
        path = os.path.abspath(path)
    root = project_root(path)
    if root is None:
        return 0

    if path.endswith(".weft"):
        target = path
    elif ("{}nodes{}".format(os.sep, os.sep)) in path:
        # A catalog change: check the program that consumes it.
        target = os.path.join(root, "main.weft")
        if not os.path.isfile(target):
            return 0
    else:
        return 0

    blocking = validate(root, target)
    if blocking:
        print(json.dumps({"additionalContext": blocking}))
    return 0


if __name__ == "__main__":
    sys.exit(main())
