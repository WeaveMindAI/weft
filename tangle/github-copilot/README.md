# Tangle for GitHub Copilot

The template that installs Tangle on a weft project when the user opens
it in GitHub Copilot. Tangle is not a product: it is the persona and the knowledge
these files install on whatever coding assistant loads them. Here that
assistant is GitHub Copilot; sibling folders under `tangle/` carry the same Tangle
for other assistants.

## What is different here

The persona is `.github/copilot-instructions.md`. Skills sit in
`.github/skills/`, the eight specialists are `.agent.md` files in
`.github/agents/`, and the seven commands are prompt files in
`.github/prompts/` (invoked as `/weft-check` and so on).

A specialist here runs as a subagent, and a few things about subagents in
VS Code shape how Tangle uses them. A subagent cannot start one of its own
unless the user turns on `chat.subagents.allowInvocationsFromSubagents`, so
Tangle starts the `designer` itself, in the same batch as the
`frontend-builder`, and the two meet in `design-lab/`. Tangle gets a
specialist's report when its call returns, waits on every specialist in a
batch before it goes on, and cannot send one anything once it is out. So
the `frontend-builder` goes out in the same batch as the first `node-smith`s,
and every amendment (a changed route, a user's reaction to the design) is a
fresh dispatch carrying that specialist's folder and the change. Since
Tangle cannot look at a running specialist either, the persona has the user
follow it in the Agents window, where an indicator in the parent chat shows
its model, its elapsed time and the tool call it is on, and lists any that
never reported. Last, no hook input says whether a tool call comes from a
subagent, so nothing here can refuse a specialist's approval prompt the way
Claude Code's `refuse_specialist_prompts.py` does: each specialist file
tells it to avoid what needs approval and to report BLOCKED with the exact
command instead.

Nor is there a permission mode per agent: the level (Manual, Assisted, Allow
all) belongs to the chat session. At Manual, only common read-only terminal
commands run without asking, so before the first dispatch the persona tells
the user once what the specialists need: Allow all for the session, or
`chat.tools.terminal.autoApprove` rules covering the same commands Claude
Code's allow list does (`weft`, `cd`, `pnpm`, `node --env-file=.env build`,
`kill`), minus the deleting and credential verbs and the daemon's lifecycle.

The `prompt-engineer` cannot try its prompt on a model itself: neither
VS Code nor the Copilot CLI hands a model a prompt as its whole system
prompt with no tools around it. So its hand-over carries two trials, a
`weft run` command per real input and what must come back, and Tangle
runs them.

A request to work differently in a project goes into the memory tool's
repository memory (`/memories/repo/`), which VS Code stores on the user's
machine for this workspace, among none of the files the update touches, because `weft tangle update` replaces `copilot-instructions.md`
and the `agents/`, `skills/`, `prompts/` and `hooks/` folders whole. Anything
else under `.github/` (`workflows/`, an `instructions/` folder of the user's)
is left alone.

Copilot's custom agents take a `tools` list, written with the tool set
names VS Code's tools reference uses today (`read`, `search`, `web/fetch`,
`execute`, `edit`). The three research specialists get no `edit`: the
`deployer` has `read`, `search` and `execute`, and the `run-digger` and
`red-teamer` have `web/fetch` as well.

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
