# Tangle for JetBrains Junie

The template that installs Tangle on a weft project when the user opens
it in JetBrains Junie. Tangle is not a product: it is the persona and the knowledge
these files install on whatever coding assistant loads them. Here that
assistant is JetBrains Junie; sibling folders under `tangle/` carry the same Tangle
for other assistants.

## What is different here

The persona is `.junie/AGENTS.md`. Skills sit in `.junie/skills/`, the eight
specialists are subagents in `.junie/agents/`, and the seven commands are
`.junie/commands/`.

These things about this one are different.

**You cannot summon a specialist by name.** Junie picks which subagent to
delegate to by matching the job against each `description`, so there is no
explicit dispatch. The persona says so, and tells Tangle to state the work
plainly and completely, because that statement is all the specialist will get.

**Tangle cannot reach a specialist while it works.** Junie runs a subagent as
a subtask and hands its result back when it finishes, with no documented way
to look inside it, send it anything or stop it; `maxTurns: 60` in each file is
what bounds it. So an amendment (a changed route for the frontend-builder, a
user's reaction for the designer) is a new dispatch naming the change, and
Tangle's timer on each specialist is a clock it checks at the start of its own
turns: for one out twenty minutes, `find <its folder> -mmin -10` says
whether it still writes, and one that wrote nothing in ten minutes is said to
the user, whom Junie told which subagent started. A subagent has no tool group for starting another one, so Tangle
starts the `designer` itself beside the `frontend-builder`, and the designer's
last act is writing `design-lab/HANDOVER.md`, which is how the frontend-builder
hears from it.

**Nothing in the project can approve or refuse a specialist's command.**
Junie never asks before an edit inside the project (only outside it, and for
a build script), but every terminal command asks unless it is on the user's
own allowlist. Junie reads only `~/.junie/allowlist.json`, never one in the
project. The subagent field `permissionMode` (`default`, `acceptEdits`,
`dontAsk`, `bypassPermissions`, `plan`) is in the docs, but in build 3659.7
the CLI parses it and nothing reads it afterwards, so this copy sets none. And
the `PermissionRequest` hook is ignored in a project's own config and cannot
tell a subagent's request from the main session's. So the template ships
`.junie/tangle-allowlist.json`, the rules the specialists need (every `weft`
verb Tangle runs, never `weft daemon start`, `stop` or `restart`, plus `pnpm`, `node --env-file=.env build`,
`kill` and `cd`), and Tangle offers once to add them to the user's allowlist.
Either way, each specialist file says it never waits on a prompt: it does the
thing another way or reports BLOCKED with the command.

**The prompt-engineer cannot try its prompt through Junie.** The command line
runs a whole agent whose `--system-prompt` adds to its guidelines rather than
replacing them, and a subagent's model is fixed in its own file. So the
prompt-engineer hands over the two real inputs with the right answer to each,
and Tangle runs the stage on both.

**Memory is a file.** Junie documents no memory beyond its guidelines, and
`.junie/AGENTS.md` is replaced by `weft tangle update`. A standing request from
the user goes into `.junie/memory.md`, which the template does not ship, so the
update leaves it alone; the persona reads it first thing. An approval the user
wants to keep lives in `~/.junie/allowlist.json`, outside the project.

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
`/weft-live-test args="TelegramSendMedia"`. The three commands whose argument is
optional carry no placeholder at all, because Junie will not run a command
until every template argument has a value.

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
