# Tangle for Cline

The template that installs Tangle on a weft project when the user opens
it in Cline. Tangle is not a product: it is the persona and the knowledge
these files install on whatever coding assistant loads them. Here that
assistant is Cline; sibling folders under `tangle/` carry the same Tangle
for other assistants.

## What is different here

The persona is `.clinerules/tangle.md`, which Cline merges into every task.
Skills sit in `.cline/skills/`, and the five commands are skills too. That is
how a `/name` command reaches Cline now: type `/`, pick the skill, and Cline
loads its `SKILL.md`. The `.clinerules/workflows/` directory older guides
mention is gone from Cline's docs, so nothing here ships into it.

**Cline has no way to define a specialist.** Its subagents are read-only
research helpers it spawns on its own judgement, with no file where you could
give one a prompt of its own, so the five specialists are skills that Tangle
loads and becomes, one at a time, in the same conversation. For the two that
only ever read, Tangle is told it can also ask for parallel research in a
sentence and let Cline spread the search across its own subagents. Each one opens with a note saying exactly that: the scope
limits and the refusals still bind, because they are what keeps the job honest
when there is no second context to check it.

The one thing that genuinely cannot survive the move is stated in the skills
rather than hidden: [the review] is Tangle re-verifying a specialist's claims,
and you cannot re-verify your own claims by rereading them. So the
verification has to be the commands. Run `weft test-node` again and read the
real output; a remembered green is not a green.

**There is no post-edit hook either**, so the persona says so plainly and puts
the validate in Tangle's own hands after every edit. This is the one place
where the discipline is entirely the model's.

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
