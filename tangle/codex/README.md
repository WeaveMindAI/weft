# Tangle for OpenAI Codex

The template that installs Tangle on a weft project when the user opens
it in OpenAI Codex. Tangle is not a product: it is the persona and the knowledge
these files install on whatever coding assistant loads them. Here that
assistant is OpenAI Codex; sibling folders under `tangle/` carry the same Tangle
for other assistants.

## What is different here

The persona is a plain `AGENTS.md` at the project root, which is Codex's own
native format rather than a compatibility shim. Skills sit in
`.agents/skills/`, and the five specialists are TOML files in
`.codex/agents/`. The five commands are skills as well: Codex deprecated its
custom-prompts directory in favour of skills, which the CLI invokes with `$name`.

Two things this copy uses.

**A sandbox per specialist.** Each agent's TOML carries `sandbox_mode`. The
three research specialists get `read-only`, so `catalog-scout`, `run-digger`
and `red-teamer` cannot write even if they try; the two builders get
`workspace-write`.

**Specialists that run side by side.** `.codex/config.toml` sets
`max_concurrent_threads_per_session = 5`, so when a program is missing three
nodes, Tangle dispatches three `node-smith` threads at once and waits for all
of them before wiring anything.

The compiler answers every edit through a `PostToolUse` hook
(`.codex/hooks/validate_weft.py`) matched on `apply_patch`, `Edit` and
`Write`. It returns findings as `hookSpecificOutput.additionalContext`, which
Codex injects at the next safe point.

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
