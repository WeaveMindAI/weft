# Tangle for Claude Code

The template that installs Tangle on a weft project when the user opens it in
Claude Code. Tangle is not a product: it is the persona and the knowledge
these files install on whatever coding assistant loads them. Here that
assistant is Claude Code; sibling folders under `tangle/` carry the same
Tangle for other assistants.

## The architecture

Tangle is an orchestrator. The main conversation holds the program: the graph
shape, the typed contracts, the weft source. Heavy and scoped work is
dispatched to subagents, and the big knowledge sits in skills that load only
when the work calls for them.

The central loop, in one paragraph: the user asks for something (vibe level
is enough). Tangle shapes it as a graph, scouts the project's catalog
(`nodes/` on disk) directly or through `catalog-scout`, and writes the weft
code. A missing capability is a dispatch: Tangle designs the node's typed
contract and sends a `node-smith` specialist (several in parallel when
several nodes are missing), which researches the real API documentation on
the web, writes the node and extensive tests (live-tier tests included),
and proves the local tiers (`weft test-node`, basic and fake: no cluster,
no credentials, no money), iterating until green before reporting with
evidence. The live tier spends real money, so the specialist writes its
tests but never runs them; the user does, with informed consent, through
`/weft-live-test`. Tangle reviews
the report against the contract and either redispatches with the critique or
wires the node into the program. Then compile, run, read the journal, report
in plain words. If the node already exists, the loop short-circuits straight
to writing the weft code.

The pipeline is self-verifying at both seams, because no actor in it is
trusted on its word. The compiler side: a PostToolUse hook re-runs the fast
validate after every Edit or Write to a `.weft` file or anything under
`nodes/`, and feeds the structural errors back to the model automatically
(the same feedback loop a language server gives a human, delivered to the
agent). The specialist side: Tangle re-runs the tests itself, diffs the
delivered metadata against the report, and reads every test asking how it
would fail, with a named catalog of the shapes half-arsed work takes. A
green claim that runs red is redispatched with the dishonesty named.

Connections close the loop for users without VS Code: the editor's Run,
Activate, and Resync buttons gate on the runtime rules before sending (the
bar's banner lists every finding), and the same power exists in the
terminal through `weft connect`, so Tangle can pick a stored connection
itself and hand the user the exact command for entering a new key, with no
secret ever crossing the chat.

The default is full autonomy for a user with zero expertise (they say what
they want and what feels wrong; Tangle translates feelings into wires). The
commands are the expert's hand on the same loop.

## What installs what

| File | Loaded when | Job |
|---|---|---|
| `CLAUDE.md` | every session, automatically | the orchestrator persona: the loop, ground truth discipline, autonomy, hard rules |
| `.claude/skills/weft-language/` | on demand, before writing weft | the language surface: syntax, types, groups, loops, the pulse model, every error slug |
| `.claude/skills/weft-catalog/` | on demand, before picking nodes | reading `metadata.json`, node families, the recurring wiring patterns |
| `.claude/skills/weft-node-authoring/` | on demand, around dispatches | the dispatch protocol and review checklist for Tangle, and the authoring manual the specialist reads |
| `.claude/skills/weft-running/` | on demand, when running or debugging | the CLI map, the daemon, journal inspection, the debugging playbook |
| `.claude/skills/weft-editor/` | on demand, about the VS Code interface | the graph view: toolbar, action bar, palette, gestures, groups and loops, inspector, labels verbatim |
| `.claude/skills/weft-connections/` | on demand, about accounts | the connect flow door by door, permissions, the browser extension, a public URL |
| `.claude/skills/weft-onboarding/` | on demand, when asked to teach | the guided tour: plain-word vocabulary and the itinerary |
| `.claude/skills/weft-updating/` | on demand, when weft itself updates | the git pull plus setup.sh walk, what an update touches and preserves, and fixing a failed one |
| `.claude/commands/` | `/weft-check`, `/weft-run`, `/weft-debug`, `/weft-new-node`, `/weft-live-test` | the expert's hand: the loop's steps on demand, live tests with informed consent |
| `.claude/hooks/validate_weft.py` | after every Edit/Write | the compiler answers every edit: fast validate on the touched source, structural errors fed back to the model automatically |
| `.claude/agents/catalog-scout.md` | when dispatched | research only: sweeps the catalog, reports exact node specs |
| `.claude/agents/prompt-engineer.md` | when dispatched | writes and overhauls the program's LLM prompts, running the WeaveMind prompt-building playbook verbatim as its mind; its brief carries the job, the model, the data, the output shape |
| `.claude/agents/run-digger.md` | when dispatched | post-mortem only: walks journals, logs, source, and stored files, compares good runs against bad ones, reports the finding with quoted evidence; read-only, never fixes |
| `.claude/agents/node-smith.md` | when dispatched | builds and proves exactly one node, unsupervised, web access for service docs, local test tiers green, live tests written but not run |
| `.claude/settings.json` | every session | permissions: the working loop runs unimpeded; only the genuinely destructive asks (live-tier tests, deactivations that wipe, terminate, rm, clean, forget). Also `claudeMdExcludes`: the user's personal `~/.claude/CLAUDE.md` and `~/.claude/rules/` are excluded, so Tangle is the only persona that loads and the project is isolated from the user's other assistant setup. Commands and agents are `weft-`-named or project-scoped, so personal same-named ones are unlikely; agents resolve project-over-user, so Tangle's specialists always win. |

The design rests on two disciplines. Ground truth: the catalog is on disk
in the project (`nodes/base_catalog/`), so the persona's central rule is to
read a node's interface (`weft describe-nodes --node <Type> --compact`,
the `metadata.json` when more than wiring is needed) instead of trusting
memory, ever, and to refresh the stdlib copy with `weft catalog update`
whenever a node misbehaves in a way its metadata should not allow (the
stdlib moves; the project's copy does not follow on its own). Delegation:
the typed contract makes a node a safely delegable task, the specialist
proves its own work before anyone sees it, and the review checks the
boundary rather than the internals.

## Installing into a project

The intended distribution is built into the CLI:

```
weft new <project> --assistant claude-code     # shorthand: --assistant cc
```

That symlinks `CLAUDE.md` and `.claude/` from the local weft checkout's
`tangle/claude-code/` into the project, so a later `git pull` +
`./setup.sh` of the checkout refreshes Tangle in every such project at once,
no per-project copy to drift stale. The flag's value names the assistant
(repeatable for several), and the choice is remembered: later `weft new`
runs install it with no flag, until `--assistant <name>` changes it or
`--assistant none` stops it. The links are absolute and machine-local:
`weft new` gitignores them, because a teammate cloning the project would
only get dangling pointers into a checkout they do not have.

Opening the project in Claude Code then loads `CLAUDE.md` automatically; the
skills, commands, and agents load from `.claude/`. The manual path still
works for a project that already exists:

```
cp -r tangle/claude-code/{CLAUDE.md,.claude} <project>/
```

A copied install does not follow checkout updates: re-copy after a weft
update, or replace the copy with symlinks by hand. `VERSION` marks the
template release; it is meant to track a tag of this repository, and the
installer can exclude this README from the copy.

Prerequisites sit on the machine, not in the template: the `weft` CLI on the
PATH, Docker, and the local daemon from `setup.sh` in the weft checkout (the
`weft-running` skill knows how to check and start all of it). The
specialist's local test tiers build with plain cargo, no docker and no
cluster, so they additionally need a Rust toolchain on the host; without one
the specialist reports that blocker instead of pretending.

## Maintenance

The language reference in the skills must track the compiler. When the
language changes, the source of truth is
`docs/src/language/` (verified current at the time of this template) plus the
compiler and catalog under `crates/` and `catalog/`. Update the skill, bump
`VERSION`, tag the repository.
