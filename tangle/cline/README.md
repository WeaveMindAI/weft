# Tangle for Cline

The template that installs Tangle on a weft project when the user opens
it in Cline. Tangle is not a product: it is the persona and the knowledge
these files install on whatever coding assistant loads them. Here that
assistant is Cline; sibling folders under `tangle/` carry the same Tangle
for other assistants.

## What is different here

The persona is `.clinerules/tangle.md`, which Cline merges into every task.
Skills sit in `.cline/skills/`, and the seven commands are skills too. That is
how a `/name` command reaches Cline now: type `/`, pick the skill, and Cline
loads its `SKILL.md`. The `.clinerules/workflows/` directory older guides
mention is gone from Cline's docs, so nothing here ships into it.

**Cline has no way to define a specialist.** Its subagents exist only in the
Cline CLI and Cline Desktop, not in the VS Code or JetBrains extensions, and
the main agent waits for them before it carries on, so the eight specialists
are skills that Tangle loads and becomes, one at a time, in the same
conversation. The `designer` is the first step of the `frontend-builder` job
rather than one Tangle loads on its own. For the `run-digger` and the `red-teamer`,
which only ever read, Tangle is told it can also ask for parallel research in
a sentence where subagents exist. Each specialist opens with a note saying exactly that:
the scope limits and the refusals still bind, because they are what keeps
the job honest when there is no second context to check it.

Two things the Claude Code copy does with hooks and background agents are
done by Tangle itself here. A declined command is never sent again: the
specialist does it another way or reports BLOCKED with the exact command
for the user. And since a specialist job is Tangle in the foreground, the
twenty-minute look at a specialist is Tangle checking, at the start of a
step, what moved in the job since it began. The prompt-engineer's trial runs
through the stage's own model node (`weft run --from ... --feed ... --target`),
because Cline offers no command that calls a model with no tools.

What a job can do without a click is the user's own auto-approve settings
in Cline, which a project cannot set, so the persona says which ones the jobs
need: every write stays inside the project (throwaway files in
`.scratch/<job>/`), so "Edit project files" covers them, and installs such as
`pnpm install` need "Execute all commands" or the user's click.

What a user asks Tangle to do differently goes into `.clinerules/memory.md`,
which the template does not ship, so `weft tangle update` leaves it alone.

The one thing that genuinely cannot survive the move is stated in the skills
rather than hidden: [the review] is Tangle re-verifying a specialist's claims,
and you cannot re-verify your own claims by rereading them. So the
verification has to be the commands. Run `weft test-node` again and read the
real output; a remembered green is not a green.

**There is no post-edit hook either**, so the persona says so plainly and puts
the validate in Tangle's own hands after every edit. This is the one place
where the discipline is entirely the model's.

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
