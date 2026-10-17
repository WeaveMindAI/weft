# Tangle for Cursor

The template that installs Tangle on a weft project when the user opens
it in Cursor. Tangle is not a product: it is the persona and the knowledge
these files install on whatever coding assistant loads them. Here that
assistant is Cursor; sibling folders under `tangle/` carry the same Tangle
for other assistants.

## What is different here

The persona is a project rule with `alwaysApply: true`
(`.cursor/rules/tangle.mdc`), so it loads every session. Skills sit in
`.cursor/skills/`, the eight specialists are subagents in `.cursor/agents/`,
and permissions live in `.cursor/cli.json`. The seven commands are skills too:
Cursor has no separate command file type, and any skill is invocable as
`/skill-name`.

Three things this copy uses that most of the others do not.

**A subagent that cannot write.** `run-digger` and `red-teamer` carry
`readonly: true`, so the promise each one makes in its own
prose ("never fixes", "never edits") is enforced by the tool instead of by
good behaviour. The red-teamer in particular is an attacker walking a program
it must never run; here it structurally cannot.

**A specialist that starts its own.** Since Cursor 2.5 a subagent can start
one of its own (one level down, no further). The `frontend-builder` uses that:
the first thing it does is start the `designer`, which builds the site's look
and motion in `design-lab/` while the frontend-builder wires the pages. Every
specialist shares the project's checkout with Tangle (Cursor gives a subagent
a worktree of its own only when asked), so each one is told exactly which
files are its own: one node folder, one included file, `front/` or
`design-lab/`.

**The compiler answers every edit.** A `postToolUse` hook
(`.cursor/hooks/validate_weft.py`) runs the fast validate and returns the
findings as `additional_context`, which Cursor injects into the conversation
right after the edit. It is `postToolUse` rather than the more obvious
`afterFileEdit` on purpose: that event fires on the same edits but accepts no
output fields, so a finding reported there would reach nobody.

One thing to know about approvals. `.cursor/cli.json` is the allow list for
Cursor's command line, and it allows the same `weft` verbs as the Claude Code
template's `settings.json`, no more, in the form Cursor's docs give
(`Shell(weft:run*)`: the command, a colon, then a pattern for the rest), plus
`pnpm`, `kill`, the site's `node --env-file=.env build`, and `date`, `ls`,
`tail` and `find design-lab` for checking on a specialist. The rules whose pattern holds a space
(`Shell(weft:infra start*)`, `Shell(weft:daemon status)`,
`Shell(weft:catalog update)` and the like, every verb with a subcommand) rest on
matching the docs do not describe: if Cursor does not match a space there,
those commands simply ask.

In the editor the run mode decides (Settings > Agents > Approvals &
Execution). A specialist that writes needs its `weft` commands outside the
sandbox, whose network rules block `127.x` addresses by default and so the
daemon. Run Everything does that, and so does putting those commands on the
editor's allowlist, since allowlisted calls run outside the sandbox. In
Auto-review, the default, a command first runs in the sandbox and only a rerun
outside it goes to Cursor's reviewer; `.cursor/permissions.json` steers that
review, in plain words, toward the same list and away from every other `weft`
command, but the reviewer decides, so it is a help rather than a guarantee.
The update replaces the project's `permissions.json`, so approvals you want
to keep go in your own `~/.cursor/permissions.json`, which Cursor merges with
it. Whether a call is put in front of you is your Cursor mode's
decision, never a list here: in an automatic mode Tangle activates, resyncs,
deactivates and starts or stops infra on its own; in a manual one it asks in
prose. The two things it always asks about are a live-tier node test (your
money) and any secret (yours to paste, never its to read). The daemon's
lifecycle is left off every list because it is a reinstall of weft, never Tangle's move.

## What Cursor does not have, and what Tangle does instead

**No hook that answers a permission prompt.** Cursor's hooks can refuse a
shell command, but none answers a prompt, and none says whether a call came
from a subagent. Every specialist runs in the background (`is_background:
true`), where a prompt would wait until somebody looked, so each agent file
tells it never to wait on one: it does the thing another way, or reports
BLOCKED with the exact command for Tangle (or, for the deployer, the user) to
run.

**No timer that wakes Tangle.** A specialist's report arrives when it
finishes, never when it is stuck. Tangle notes the time of each dispatch with
`date`, and at the start of each of its turns gives every specialist out for
twenty minutes one look at what it has written under `~/.cursor/subagents/`
(and, for a frontend-builder waiting on its designer, at `design-lab/`). One
that wrote nothing for ten minutes is reported to the user as stuck.

**No way to run a model on a prompt alone.** The `agent -p` command line
takes no system prompt and cannot turn its tools off, so the
prompt-engineer's trial of a new prompt on two real inputs goes through the
program: Tangle runs the stage on each input and sends back what came out.

**No project memory the update keeps.** `weft tangle update` replaces every
entry it ships under `.cursor/`, the whole `rules/` folder included, so
Tangle writes what a user asks of it into `.cursor/memory.md`, a file the
template does not ship, and the rule tells Tangle to read it at the start of
every session. The project's root `AGENTS.md` is not used for this: the Codex
and OpenCode templates ship one, so installing either beside Cursor would
overwrite it. A
permission the user wants to keep goes in their own
`~/.cursor/cli-config.json`.

## What is here

Tangle is one persona plus its reference skills, eight specialists and seven commands.
The persona holds the program: the graph shape, the typed contracts, the weft
source. The skills are the big knowledge, loaded only when the work calls for
them. The specialists take the heavy scoped jobs. The commands are the loop's
steps, on demand, for a user who wants to drive.

The whole architecture, and why the pipeline verifies itself at both seams,
is written out once in
[`../claude-code/README.md`](../claude-code/README.md). This file covers only
what is different here.

## This copy is yours to tune

The template directories under `tangle/` are independent copies on purpose.
They start identical and are meant to drift: a wording this assistant answers
better to belongs in this folder and nowhere else. For what stays shared and
what may diverge, go and read
[`../README.md`](../README.md).
