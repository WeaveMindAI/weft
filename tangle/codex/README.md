# Tangle for OpenAI Codex

The template that installs Tangle on a weft project when the user opens
it in OpenAI Codex. Tangle is not a product: it is the persona and the knowledge
these files install on whatever coding assistant loads them. Here that
assistant is OpenAI Codex; sibling folders under `tangle/` carry the same Tangle
for other assistants.

## What is different here

The persona is a plain `AGENTS.md` at the project root, which is Codex's own
native format rather than a compatibility shim. Codex reads only the first
32 KiB of it unless told otherwise, and the persona is longer, so
`.codex/config.toml` raises `project_doc_max_bytes`. Skills sit in
`.agents/skills/`, and the eight specialists are TOML files in
`.codex/agents/`. The seven commands are skills as well: Codex deprecated its
custom-prompts directory in favour of skills, which the CLI invokes with `$name`.

What this copy uses, and where Codex differs from the other hosts.

**One sandbox and one set of rules for everybody.** A custom agent file
cannot change the sandbox or the approval policy: every specialist runs under
the ones the session starts with (`workspace-write`, where edits inside the
project never ask and the network is closed), and the command rules cannot be
scoped to one agent either: what gives each specialist its own commands is
`.codex/hooks/specialist_commands.json`, enforced by a `PreToolUse` hook.
So `run-digger` and `red-teamer` cannot write through the sandbox, but their
lists hold only reads, and a deleting verb the rules allow is on no
specialist's list.

**Specialists that run side by side.** `.codex/config.toml` sets
`max_concurrent_threads_per_session = 5`, so when a program is missing three
nodes, Tangle dispatches three `node-smith` threads at once. Which agent tools
a session has depends on the model: Codex gives each model a multi-agent
version (`multi_agent_version` in `~/.codex/models_cache.json`). On a `v2`
model (the current ones) an agent can start its own, so the
`frontend-builder` starts the `designer`, as in the Claude Code copy; on a
`v1` model agents may start agents only as deep as `agents.max_depth`, which
defaults to 1 and leaves a specialist no agent tools, so `.codex/config.toml`
sets it to 2. If the spawn is refused anyway, the frontend-builder reports
that at once and Tangle starts the designer instead. The persona
names the `v2` tools (`spawn_agent`, `followup_task`, `wait_agent`,
`interrupt_agent`, `list_agents`) and says which `v1` ones stand in for them.

**A look at each specialist after twenty minutes.** Codex tells Tangle when a
specialist finishes, never when it is stuck. Tangle waits with `wait_agent`
and a twenty-minute timeout, or checks the clock between its own steps, and
reads the specialist's rollout file under `~/.codex/sessions/` (and
`design-lab/` for a frontend-builder waiting on its designer) to see whether
it is still moving.

**No specialist waits on an approval.** A `PermissionRequest` hook
(`.codex/hooks/refuse_specialist_prompts.py`) refuses any approval asked for
inside a subagent (Codex marks those hook inputs with `agent_id`), with a
message telling it to do the thing another way or report BLOCKED with the
command. The root thread's approvals pass untouched.

The compiler answers every edit through a `PostToolUse` hook
(`.codex/hooks/validate_weft.py`) matched on `apply_patch`, `Edit` and
`Write`. It returns findings as `hookSpecificOutput.additionalContext`, which
Codex injects at the next safe point. Codex runs a project's hooks only once
the user has trusted them in `/hooks`.

## What is here

Tangle is one persona plus its reference skills, eight specialists and seven commands.
The persona holds the program: the graph shape, the typed contracts, the weft
source. The skills are the big knowledge, loaded only when the work calls for
them. The specialists take the heavy scoped jobs. The commands are the loop's
steps, on demand, for a user who wants to drive.

| Path | What it is |
|---|---|
| `AGENTS.md` | the persona |
| `.agents/skills/` | the reference skills and the seven commands |
| `.codex/agents/` | the eight specialists: `group-builder`, `node-smith`, `frontend-builder`, `designer`, `prompt-engineer`, `run-digger`, `deployer`, `red-teamer` |
| `.codex/config.toml` | the approval policy and sandbox, the persona's size limit, and how many specialists run at once |
| `.codex/hooks.json` | registers the three hooks below |
| `.codex/hooks/validate_weft.py` | after every edit: the compiler's structural findings, fed back to the model |
| `.codex/hooks/refuse_specialist_prompts.py` | on every approval prompt: a specialist's is refused at once, the root thread's pass untouched |
| `.codex/hooks/specialist_commands.py` and `specialist_commands.json` | before every shell command a specialist runs (a `PreToolUse` hook on `Bash`, which also covers `exec_command`) | each specialist runs its own commands and no others: the JSON holds one list per specialist (the run-digger's reads, the frontend-builder's `pnpm --dir front` forms, the deployer's `--on` verbs), and the hook refuses at once any command that does not start with one of them, naming the list. It also refuses, for every specialist, a command built at run time, an output redirection, a path out of the project, and `--on` for anyone but the deployer. It only refuses: a command it lets through goes on to the command rules, which decide whether it leaves the sandbox, and an approval it would need is refused by the hook above. Codex marks a hook's input with `agent_id` and `agent_type` only inside a subagent, so the main session is left alone. |
| `.codex/scripts/try_prompt.py` | the prompt-engineer's trial: one `codex exec` with the prompt in place of Codex's own instructions, no tools and no project context, on one real input |
| `.codex/rules/tangle.rules` | the commands that run outside the sandbox with no approval prompt. They need the outside: the sandbox closes the network (even to 127.0.0.1), every `weft` command talks to the daemon, `weft test-node`'s cargo build fetches crates and writes outside the project, and `pnpm` fetches packages. It holds the Claude Code copy's allow list, narrowed where a prefix rule would let anything out: the `weft` verbs of the loop, the deployer's `weft target list`, `weft domain` and `weft frontend ls`, `node --env-file=.env build`, and of `pnpm` only `--dir front` or `--dir design-lab` with `install`, `add`, `run`, `exec playwright` or `exec auth`, plus the two `dlx` scaffold tools at the versions the `weft-frontend` skill pins. `cd` has no rule, unlike the Claude Code copy: with one, `cd /tmp/x && python3 .codex/scripts/stop_server.py` would run somebody else's script outside the sandbox, so the skills and agents name a folder with a flag (`pnpm --dir front ...`, `--cwd front`) instead. One gap no rule can close: Codex's `exec_command` tool takes a working folder of its own (`workdir`), which rules do not see, so the same script path run with a working folder elsewhere still matches. What these rules let out includes code the agent wrote: `pnpm ... run`, `install` and `add` run the scripts of a `package.json` it edits, `exec playwright test` runs its test files, and `node --env-file=.env build` runs the site it built. And `weft test-node` is allowed: only `--tier live` written straight after the verb is forbidden, and `weft test-node <Type> --tier live`, which spends the user's money, is refused by the specialist hook instead: the node-smith's entry refuses `--tier live` and `--tier=live` anywhere on the line. For Tangle itself only its instructions hold that form back. `kill` is replaced by `.codex/scripts/stop_server.py`, which stops one process of this project's (a sandboxed `kill` cannot reach a server started outside the sandbox, and a `kill` rule would let `kill -9 -1` out). The trial script stands in for Claude Code's own trial, and `wait_agent` for its twenty-minute `sleep`. A rule matches the start of a command, so a bare form there (`weft build`) covers its flags here, and a verb covers `--on <target>` only written after it. `weft daemon start`, `stop` and `restart` are forbidden: they re-run the weft install. A command line runs outside the sandbox only when every command in it is allowed. The rules hold for every agent alike and only decide the sandbox: which commands a specialist may run at all is its list in `specialist_commands.json`, so a deleting verb here is reached by Tangle alone |
| `.codex/scripts/stop_server.py` | stops a dev server by its pid or its port, only when it runs in this project's folder, with every process under it |

`weft tangle update` replaces everything listed here. A file Tangle does not
ship at the top of `.codex/` survives it, which is why Tangle keeps what the
user asks of it in `.codex/memory.md` and never edits its own files. A
command the user allows from Codex's approval prompt is written to
`~/.codex/rules/default.rules`, outside the project, which the update never
touches.

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
