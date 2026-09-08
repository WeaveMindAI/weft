# Tangle for Cursor

The template that installs Tangle on a weft project when the user opens
it in Cursor. Tangle is not a product: it is the persona and the knowledge
these files install on whatever coding assistant loads them. Here that
assistant is Cursor; sibling folders under `tangle/` carry the same Tangle
for other assistants.

## What is different here

The persona is a project rule with `alwaysApply: true`
(`.cursor/rules/tangle.mdc`), so it loads every session. Skills sit in
`.cursor/skills/`, the five specialists are subagents in `.cursor/agents/`,
and permissions live in `.cursor/cli.json`. The five commands are skills too:
Cursor has no separate command file type, and any skill is invocable as
`/skill-name`.

Three things this copy uses that the others cannot.

**A subagent that cannot write.** `catalog-scout`, `run-digger` and
`red-teamer` carry `readonly: true`, so the promise each one makes in its own
prose ("never fixes", "never edits") is enforced by the tool instead of by
good behaviour. The red-teamer in particular is an attacker walking a program
it must never run; here it structurally cannot.

**A worktree per specialist.** Cursor gives each dispatched subagent its own
git worktree and branch. The `node-smith` is told to write exactly one folder
and never touch `main.weft`; on Cursor that isolation is real, so a specialist
that misbehaves cannot reach the user's program at all.

**The compiler answers every edit.** A `postToolUse` hook
(`.cursor/hooks/validate_weft.py`) runs the fast validate and returns the
findings as `additional_context`, which Cursor injects into the conversation
right after the edit. It is `postToolUse` rather than the more obvious
`afterFileEdit` on purpose: that event fires on the same edits but accepts no
output fields, so a finding reported there would reach nobody.

One thing to know: Cursor has no "ask" tier for shell commands. What the other
assistants gate behind a prompt is denied here, in `.cursor/cli.json`, so the
user runs those themselves. Deny beats allow, which is what keeps the broad
`weft connect*` allow from also permitting `weft connect --forget`.

## What is here

Tangle is one persona plus eleven skills, five specialists and five commands.
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
