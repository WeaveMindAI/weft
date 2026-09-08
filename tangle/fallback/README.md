# Tangle for any other assistant

The template that installs Tangle on a weft project when the user opens
it in any other assistant. Tangle is not a product: it is the persona and the knowledge
these files install on whatever coding assistant loads them. Here that
assistant is any other assistant; sibling folders under `tangle/` carry the same Tangle
for other assistants.

## What is different here

This is the fallback, for an assistant weft has no template for. It is a plain
`AGENTS.md` at the project root plus the skills beside it in
`.agents/skills/`, the two shapes almost every assistant reads.

**It installs only when nothing else did**, and that restraint is the whole
design. Cursor, Cline and Gemini each read a root `AGENTS.md` on top of their
own persona file rather than as an alternative to it, so shipping both would
put Tangle in the context twice, at double the tokens, with two copies free to
disagree. `weft new --assistant agents` refuses to run alongside a named
assistant for exactly that reason.

Because it cannot know what the host supports, the persona is written to work
either way. It says that if you can dispatch a subagent, the five specialists
are dispatches, and if you cannot, they are five jobs you do yourself, one at
a time, with the scope limits and refusals still binding. The eleven skills
are described as files to open at the moment they are needed, so they work
with or without a skill loader.

There is no hook, so the persona puts the compiler check in Tangle's own
hands after every edit.

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
