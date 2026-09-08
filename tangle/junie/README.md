# Tangle for JetBrains Junie

The template that installs Tangle on a weft project when the user opens
it in JetBrains Junie. Tangle is not a product: it is the persona and the knowledge
these files install on whatever coding assistant loads them. Here that
assistant is JetBrains Junie; sibling folders under `tangle/` carry the same Tangle
for other assistants.

## What is different here

The persona is `.junie/AGENTS.md`. Skills sit in `.junie/skills/`, the five
specialists are subagents in `.junie/agents/`, and the five commands are
`.junie/commands/`.

Two things about this one are genuinely different.

**You cannot summon a specialist by name.** Junie picks which subagent to
delegate to by matching the job against each `description`, so there is no
explicit dispatch. The persona says so, and tells Tangle to state the work
plainly and completely, because that statement is all the specialist will get.

**There is no compiler check at all, and that is a hard limit rather than a
gap we left.** Junie has no per-edit hook, and it deliberately ignores hooks
configured inside a project: a repository could otherwise make it run any
command, so project-local hook config is only honoured when the user passes it
explicitly on the command line. A `Stop` hook would have made a good gate
(Junie can refuse to let a task be called done and hand the diagnostics back),
but nothing the template installs can register one. So this copy ships no hook
and says so in the persona: running `weft validate` after every edit is
Tangle's own discipline here.

The commands use Junie's named template arguments (`$args`), so
`/weft-live-test args="TelegramSendMedia"`. The two commands whose argument is
optional carry no placeholder at all, because Junie will not run a command
until every template argument has a value.

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
