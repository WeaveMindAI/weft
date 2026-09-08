# Tangle for GitHub Copilot

The template that installs Tangle on a weft project when the user opens
it in GitHub Copilot. Tangle is not a product: it is the persona and the knowledge
these files install on whatever coding assistant loads them. Here that
assistant is GitHub Copilot; sibling folders under `tangle/` carry the same Tangle
for other assistants.

## What is different here

The persona is `.github/copilot-instructions.md`. Skills sit in
`.github/skills/`, the five specialists are `.agent.md` files in
`.github/agents/`, and the five commands are prompt files in
`.github/prompts/` (invoked as `/weft-check` and so on).

Copilot's custom agents take a `tools` list, so the three research
specialists are given the reading tools and no `editFiles`.

**The compiler answers every edit.** A `postToolUse` hook
(`.github/hooks/weft.json`) runs the fast validate after the file-writing
tools and hands the findings back as `additionalContext`, which Copilot
appends to the tool result the model sees on that same turn.

Two details worth knowing if you edit that file. Copilot's hook config is
`version: 1` with each event mapping to a flat array of entries, not the
nested shape Claude Code uses. And the event name decides the payload: the
camelCase `postToolUse` sends the tool's arguments as `toolArgs`, while the
PascalCase `PostToolUse` sends `tool_input` instead. The script reads either,
so it survives being re-registered under the other spelling.

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
