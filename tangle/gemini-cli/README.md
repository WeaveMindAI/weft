# Tangle for Gemini CLI

The template that installs Tangle on a weft project when the user opens
it in Gemini CLI. Tangle is not a product: it is the persona and the knowledge
these files install on whatever coding assistant loads them. Here that
assistant is Gemini CLI; sibling folders under `tangle/` carry the same Tangle
for other assistants.

## What is different here

The persona is `GEMINI.md` at the project root. Gemini does **not** read
`AGENTS.md`, so this is the only file that carries it. Skills sit in
`.gemini/skills/`, the specialists in `.gemini/agents/`, and the seven commands
are TOML in `.gemini/commands/`.

Four things are particular to this copy.

**A tools allowlist per specialist.** Gemini's subagent frontmatter takes a
real `tools` list, so `run-digger` and `red-teamer` are given the reading
tools and never `write_file` or `replace`. What their prose
promises, their tool list enforces.

**The best post-edit loop of any assistant here.** Gemini's `AfterTool` hooks
run synchronously and their `additionalContext` is appended to the tool result
the model sees, so the compiler's answer lands in the same turn as the edit,
not the next one. That is registered in `.gemini/settings.json`.

**A policy that answers a specialist's permission prompts.** A Gemini subagent
that needs approval waits for the user, with its `timeout_mins` clock stopped,
and nobody may be looking. Gemini's policy engine can match a rule to one
subagent (`subagent = "<name>"`), so `.gemini/specialist-policy.toml` gives
each specialist a block naming what its job needs: its reads, its own `weft`
verbs (the readers get no deleting or credential ones), the exact `pnpm`,
`node` and `kill <pid>` forms the two that build the site use, and the edits
of the five that write files, in `default` mode as well as `autoEdit`. Every
block refuses the daemon's lifecycle, any `--on` or `--dispatcher` outside the
deployer's block, and any redirection but the validate run's stdin and the
prompt-engineer's trial. It also refuses a flag with a quote or backslash
glued to its name (`--o''n`), the live tier of `weft test-node`, and any edit
under `.gemini/`, so a specialist cannot rewrite this policy or the trial's.
The trial is let through only when its prompt sits under the project's
`assets/prompts/`, its `--policy` is the project's own
`.gemini/trial-no-tools.toml` (the same root, never under `.scratch/` or
`/tmp/`) and its input is under `.scratch/prompt-engineer/`. `kill` takes a
bare process id, and that is as narrow as Gemini's policy gets: it matches the
command's text and cannot tell which process an id belongs to, so a specialist
could stop any process of the user's. Anything else that would have asked is
refused at once, telling it to do the thing another way or report BLOCKED with
the command. Tangle's own calls are untouched. Project-level policy folders
(`.gemini/policies/`) are switched off in Gemini, so the file is loaded
through `policyPaths` in `.gemini/settings.json`, a path relative to the
folder Gemini starts in: start Gemini from the project folder. Setting
`policyPaths` stops Gemini reading `~/.gemini/policies/` by itself, so the
settings list that folder too. One prompt gets through anyway: Gemini asks
before any edit to a build file (`package.json`, a `Dockerfile`, a lockfile)
whatever a policy says.

**Dispatches that block.** A dispatch is one call that returns the
specialist's report. The specialists sent in one turn work side by side and
Tangle's turn waits for all of them; nothing can be sent to one while it
runs, so an amendment is a fresh dispatch carrying the folder as it is. Each
specialist's timer is its `timeout_mins`, after which Gemini gives it a minute
to report what it has. A subagent cannot start another one, so Tangle
dispatches the `designer` itself, in the same turn as the `frontend-builder`,
and a later frontend-builder dispatch moves the design into the real pages.

The commands are TOML rather than markdown, and Gemini's templating can run a
shell command inside a prompt with `!{...}` and pull in a file with `@{...}`.

## What is here

Tangle is one persona plus its reference skills, eight specialists and seven commands.
The persona holds the program: the graph shape, the typed contracts, the weft
source. The skills are the big knowledge, loaded only when the work calls for
them. The specialists take the heavy scoped jobs. The commands are the loop's
steps, on demand, for a user who wants to drive.

The eight specialists are `node-smith`, `group-builder` (the inside of one
included file, proven alone with `weft run --group`), `frontend-builder`,
`designer` (the site's look and motion in `design-lab/`), `prompt-engineer`
(which tries each prompt on two real inputs with a headless `gemini` call,
`--policy .gemini/trial-no-tools.toml` keeping every tool away from the trial
model), `run-digger`, `red-teamer` and `deployer`.

`weft tangle update` replaces every file Tangle ships and leaves what it does
not ship alone. Gemini has no project-local settings file, so a permission the
user wants to keep goes in a policy file of theirs under
`~/.gemini/policies/`, and a way of working they ask for goes in the
project's private memory (`MEMORY.md` under `~/.gemini/tmp/<project>/memory/`),
never in `GEMINI.md`.

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
