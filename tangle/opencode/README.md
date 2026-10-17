# Tangle for OpenCode

The template that installs Tangle on a weft project when the user opens
it in OpenCode. Tangle is not a product: it is the persona and the knowledge
these files install on whatever coding assistant loads them. Here that
assistant is OpenCode; sibling folders under `tangle/` carry the same Tangle
for other assistants.

## What is different here

The persona is the prompt of the `tangle` agent,
`.opencode/agents/tangle.md`, and `opencode.json` makes `tangle` the default
agent so a session starts in it. `AGENTS.md` is a two-sentence stub: OpenCode
loads it into every session, the specialists' included, so it says only that
this is a weft project built with Tangle and that each specialist follows its
own agent file. It still has to be there, because the install ships it, and an
update then overwrites the full copy of the persona an older install put
there.
Skills sit in `.opencode/skills/`, the specialists in `.opencode/agents/`, and
the seven commands in `.opencode/commands/`.

The specialists use OpenCode's per-agent `permission` block, and the commands
use its `$ARGUMENTS` and positional `$1` templating.

Each specialist runs its own shell commands and no others. The lists live
in `.opencode/specialist-commands/specialist_commands.json`, one per
specialist, keyed by its agent name, in the same shape as claude-code's
`specialist_commands.json`: an entry is the words a command starts with, or
an object that refuses some words after them (`weft test-node` without
`--tier live`), takes only process ids (`kill`), or runs a check
(`opencode-trial`, below). `decide.ts` beside it makes the decision, a port
of claude-code's `specialist_commands.py` with the same shell-aware split
(Python's `shlex` read token by token) and the same refusals for every
specialist: a command built at run time or written over several lines, an
output redirection, a `cd` or an input file outside the project, and `--on`
or `--dispatcher` on a weft command for all but the deployer.
`.opencode/plugins/specialist-commands.ts` runs it in OpenCode's
`tool.execute.before` hook, which fires before the `bash` tool and its
permission check; throwing there refuses the call, and the model reads the
message as the tool's error. A call is a specialist's when its session has a
parent and the session's agent is one the file names; the main session and
any other agent are left to OpenCode's permissions. If the file cannot be
read, a specialist's call is refused. `node tests/specialist-commands.test.ts`
runs claude-code's 52 hook cases, adapted to this host, plus OpenCode's own
(the trial, the plugin's session check); `tests/` is not installed into a
project.

That makes the plugin the one place that decides a specialist's commands, so
each specialist's own `permission` block says only `bash: allow`: OpenCode
neither asks nor refuses a shell command for it, and a stricter config of
the user's cannot turn one into a prompt. What stays in those blocks is what
the plugin does not see, because it is not a shell command: the rules that
keep a specialist from ever raising a prompt (below), and `edit: deny` on the
run-digger, the red-teamer and the deployer. `kill` takes only process ids,
which still reach any process the user owns: a list cannot tell the dev
server a specialist started from another one.

The prompt-engineer's trial runs a second OpenCode in `/tmp/weft-trial-<name>`,
on an agent it wrote into that folder's `opencode.json`, outside this
project and this plugin. So its entry carries a check: the command is
exactly `... --agent trial --title trial --dir /tmp/weft-trial-<name> --model
<model>`, and the folder holds only an `opencode.json` with one agent,
`trial`, whose prompt is a `{file:...}` and whose permission is
`{"*": "deny"}`. An agent written there with tools would otherwise run them
with nothing in the way. Run that way, the model sees the prompt, OpenCode's
short environment note, and the input.

`opencode.json` is Tangle's. OpenCode allows every command no rule names
(its defaults start with `"*": "allow"`, and the last rule that matches
wins), so its `allow` entries grant nothing by themselves: they record which
verbs Tangle runs and keep them working for a user whose own config is
stricter. The rules that change something are the `ask` ones: `weft daemon
start`, `stop`, `restart` (an alias of `start`) and `remove`, written after
every allow so they win, since the first three re-run the weft install and
`remove` deletes a named one. Tangle's own block lists the eight specialists
it may dispatch after a `"*": deny`, in that order, because OpenCode applies
the last rule that matches: with the `"*"` written last, every dispatch is
refused and the `task` tool disappears.

A specialist never waits on a permission prompt, the second net under the
plugin. Its prompt would show up in
Tangle's session, in place of the box the user types into, until somebody
answered it. OpenCode 1.19's plugin types
declare a `permission.ask` hook, but nothing calls it, so no plugin can answer
a prompt. Instead each specialist's own `permission` block leaves it nothing to
ask: OpenCode's defaults allow almost everything and ask on three things, and
there each is settled. A path outside the project is `deny` (the specialists' throwaway
files go in `.scratch/<name>/` inside it; only the prompt-engineer may also
write its trial folder, `/tmp/weft-trial-*`), a `.env` read is `deny`, and the
repeat-call check (`doom_loop`) is `allow`, since refusing it would end a
healthy specialist that checks `weft status` three times while it waits. A
refused call comes back at once with "The user has specified a rule which
prevents you from using this specific tool call", and the specialist reports
BLOCKED with the command.

A dispatch holds Tangle's turn until the specialist is done, unless OpenCode
was started with `OPENCODE_EXPERIMENTAL_BACKGROUND_SUBAGENTS=true`, which
gives the `task` tool a `background` flag and delivers each report as a
message of its own. A shell command here always runs in the foreground, so
there is no timer Tangle can start beside a dispatch: it looks at a
background specialist at the start of its own turns once twenty minutes have
passed, with `opencode export <task id>`. A `task` call carrying a task id
continues that specialist's session, which is how Tangle amends a brief.

At the default `subagent_depth` of 1 a specialist cannot start one of its
own, so Tangle starts the `designer` itself, in the same message as the
`frontend-builder`, and hands the designer's report to the frontend-builder
afterwards.

`weft tangle update` replaces `AGENTS.md`, `opencode.json` and every folder
under `.opencode/` that this template ships, and keeps anything else. So
`opencode.json` lists `.opencode/memory.md` in its `instructions`, and that
file, which the template does not ship, is where Tangle writes down what the
user asks of it. A permission the user wants to keep goes in
`.opencode/opencode.json`, which OpenCode reads on top of the project's own.

The compiler answers every edit through a `tool.execute.after` plugin
(`.opencode/plugins/weft-validation.ts`), which appends the findings to the
tool result the model reads.

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
