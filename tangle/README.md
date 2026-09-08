# Tangle

Tangle is the AI builder that lives inside a weft project. It is not a
product and not a plugin: it is a persona, eleven skills, five specialists
and five commands, written as files that an AI coding assistant loads when
the user opens the project.

One folder here per assistant. `weft new <name> --assistant <one of them>`
symlinks that folder's files into the new project, so a `git pull` of this
checkout refreshes Tangle in every project that asked for it.

| Folder | Assistant | Flag |
|---|---|---|
| `claude-code/` | Claude Code | `cc` |
| `kilo-code/` | Kilo Code | `kc` |
| `cursor/` | Cursor | `cu` |
| `codex/` | OpenAI Codex | `cx` |
| `github-copilot/` | GitHub Copilot | `gh` |
| `gemini-cli/` | Gemini CLI | `gc` |
| `cline/` | Cline | `cl` |
| `opencode/` | OpenCode | `oc` |
| `devin-desktop/` | Devin Desktop (was Windsurf) | `dd` |
| `junie/` | JetBrains Junie | `ju` |
| `fallback/` | anything else | `agents` |

`fallback/` is the odd one. It ships the persona as a plain `AGENTS.md` plus
the skills beside it, and it installs **only** when nothing else did. Cursor,
Cline and Gemini all read a root `AGENTS.md` on top of their own persona file
rather than instead of it, so installing both would load Tangle twice, at
double the tokens, with two copies free to disagree.

## The folders are copies, and they are meant to drift

There is no shared source, no generation step, and nothing checking that the
folders agree. That is deliberate. Each assistant answers to different
wording, and the point of a folder per assistant is that you can tune one
without touching the rest.

So when you improve a prompt, the default is to carry the change to the other
folders, because a fix to how Tangle thinks is a fix everywhere. But a change
that is about one assistant stays in that assistant's folder, and it is
supposed to.

The line: **what Tangle is has to hold in every folder. How it is said does
not.** Tangle everywhere shapes the graph before building, reads the catalog
before writing a node type, keeps a level of the graph under six items, fails
loudly instead of papering over, and reports in plain words. If a change
alters one of those, it belongs in all eleven folders. If it changes phrasing,
ordering, an example, or which of the assistant's own features gets used, it
belongs in one.

## The mechanisms are not the same everywhere

Tangle wants four things from a host: a persona that loads every session,
knowledge that loads on demand, specialists it can hand a scoped job to, and
something that runs the compiler after an edit. No assistant is missing the
first two. Several are missing one of the last two, and the folders answer
that differently rather than pretending.

| | Specialists | Compiler after an edit |
|---|---|---|
| Claude Code, Kilo, Cursor, Codex, Copilot, Gemini, OpenCode | real subagents | yes, straight into the context |
| Junie | real subagents, but it picks which one by description | no, the persona runs it |
| Devin Desktop | skills Tangle loads and becomes | runs, but leaves its answer in a file |
| Cline | skills Tangle loads and becomes | no, the persona runs it |

Where a specialist cannot be a subagent, it is a skill that opens by saying so:
the scope limits and refusals still bind, and the verification that used to be
a second context reading the first has to become running the command again.

The compiler column is the one that varies most, and the reasons are worth
knowing. Junie ignores hooks configured inside a project on purpose, so nothing
a template installs can register one. Devin's post-hooks cannot speak back into
the model's context, so its hook writes findings to a file the persona reads.
Cline's hooks are an SDK facility rather than something a plain project
configures. In all three the persona says plainly that running `weft validate`
is Tangle's own job.

Each folder's own README says what is different about it.

## Contributing

Improving these prompts is one of the more useful things you can do here, and
you do not need to touch Rust to do it. For how we treat changes to them, go
and read the Tangle section of
[`../CONTRIBUTING.md`](../CONTRIBUTING.md#working-on-tangle).
