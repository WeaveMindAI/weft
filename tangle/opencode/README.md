# Tangle for OpenCode

The template that installs Tangle on a weft project when the user opens
it in OpenCode. Tangle is not a product: it is the persona and the knowledge
these files install on whatever coding assistant loads them. Here that
assistant is OpenCode; sibling folders under `tangle/` carry the same Tangle
for other assistants.

## What is different here

The persona is `AGENTS.md`, OpenCode's own native instructions file, and
`opencode.json` makes `tangle` the default agent so a session starts in it.
Skills sit in `.opencode/skills/`, the specialists in `.opencode/agents/`, and
the five commands in `.opencode/commands/`.

The specialists use OpenCode's per-agent `permission` block, and the commands
use its `$ARGUMENTS` and positional `$1` templating.

The compiler answers every edit through a `tool.execute.after` plugin
(`.opencode/plugins/weft-validation.ts`), which appends the findings to the
tool result the model reads.

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
