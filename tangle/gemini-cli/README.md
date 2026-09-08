# Tangle for Gemini CLI

The template that installs Tangle on a weft project when the user opens
it in Gemini CLI. Tangle is not a product: it is the persona and the knowledge
these files install on whatever coding assistant loads them. Here that
assistant is Gemini CLI; sibling folders under `tangle/` carry the same Tangle
for other assistants.

## What is different here

The persona is `GEMINI.md` at the project root. Gemini does **not** read
`AGENTS.md`, so this is the only file that carries it. Skills sit in
`.gemini/skills/`, the specialists in `.gemini/agents/`, and the five commands
are TOML in `.gemini/commands/`.

Two things this copy uses.

**A tools allowlist per specialist.** Gemini's subagent frontmatter takes a
real `tools` list, so `catalog-scout`, `run-digger` and `red-teamer` are given
the reading tools and never `write_file` or `replace`. What their prose
promises, their tool list enforces.

**The best post-edit loop of any assistant here.** Gemini's `AfterTool` hooks
run synchronously and their `additionalContext` is appended to the tool result
the model sees, so the compiler's answer lands in the same turn as the edit,
not the next one. That is registered in `.gemini/settings.json`.

The commands are TOML rather than markdown, and Gemini's templating can run a
shell command inside a prompt with `!{...}` and pull in a file with `@{...}`.

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
