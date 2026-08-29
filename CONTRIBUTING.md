# Contributing

Thank you for considering lending a hand!

It is early days here, so things move fast and plenty is still up for grabs. If
you think a something in the codebase is wrong, it might well be, so please come and tell us on
[Discord](https://discord.com/invite/FGwNu6mDkU).

And if you are not sure whether something is wanted, come ask us: 
Discord for a quick question, an issue for something that we should keep track of. 

Everything about how weft *works* is in
[the book](https://weavemindai.github.io/weft/). This file is about
working on the codebase.

## Set up

Go check [Install](https://weavemindai.github.io/weft/start/install.html).

If you want two branches open at once, `scripts/add-worktree.sh <branch>`
branches off the one you are on into `../weft-trees/`, in a folder named after
the branch with any slashes flattened to dashes. It gets its own `target/`, so
the two builds shouldn't fight. Your `.env`, `.env.extension` and
`access-apps.json` are symlinked in, so both trees have the same ones. Run
`./setup.sh` inside it before you use it.

## Tests

```bash
cargo test                        # the workspace
cargo clippy --workspace --all-targets --locked -- -D warnings
pnpm -C packages/weft-graph test  # the graph renderer
pnpm -C extension-vscode test     # the VS Code extension
```

CI runs all four of those, plus the Postgres ones further down. The two pnpm
suites share the VS Code extension's install, so `./setup.sh` (or at least `./setup.sh --vscode`) has to
have run at least once before they work.

If you are not sure which layer your test belongs at, go check
[the testing pyramid](https://weavemindai.github.io/weft/running/architecture.html#testing-in-four-layers).

Three more suites have their own runners:

```bash
scripts/run-db-tests.sh [crate]         # the tests that need a real Postgres
scripts/run-node-tests.sh [package]     # node tests, over everything in catalog/
scripts/run-e2e.sh [name]               # the end-to-end suite
```

The node and end-to-end ones stop at the first failure, the end-to-end one test
by test and the node one package by package. Both take `--from <name>` to pick
up where they stopped. The node one takes `--parallel` when you want every
package at once, or `--parallel N` for batches of N.

The database one has its own section further down.

If you changed something all the nodes depend on, such as a ctx function or
the code generator, run the node test script: it works in a
scratch project built from this checkout. For how to write node tests, go
and read
[Testing a node](https://weavemindai.github.io/weft/nodes/testing.html).

By default `run-node-tests.sh` runs the basic and fake tiers, which need
nothing set up and is free.
If you want to test with real services `--tier live` runs the nodes against 
real accounts and **spends real money**, so it remembers what passed and skips 
a package until you change it (you can force a rerun with `--retest` or by wiping
the cache in `.live-pass-cache` in `target/node-tests`).

The live tier needs a real connection per service. If you signed into one in any editor, 
it is picked up by itself; Otherwise, for pasted keys, the runner reads
`WEFT_NODE_TEST_*` from this repo's root `.env`. For the full list of
variables, and the three ways to hand a test an account, go and read
[Giving the live tier what it needs](https://weavemindai.github.io/weft/nodes/testing.html#giving-the-live-tier-what-it-needs).

When an end-to-end test fails, its project and any pods it made are left in
the cluster on purpose so you can debug what happened.

If you are setting up the end-to-end suite, the only variables you have to set
are for the outside services. They are `WEFT_E2E_*`, and they are listed in
[`crates/weft-e2e/README.md`](crates/weft-e2e/README.md#credentials-and-what-the-runner-provides-for-you).

A test that fails intermittently is a bug, so put anything timing-sensitive
through `stress_test!`
(`crates/weft-core/src/test_support.rs`). You need `futures` as a
dev-dependency in the crate you call it from. For why we treat it that way, and
the fixes that look tempting and make it worse, go check
[Flakes are bugs](https://weavemindai.github.io/weft/running/architecture.html#flakes-are-bugs).

## Writing a node for the standard catalogue

If you want a first contribution, this is a good one. A node is a folder with
two files in it. You can go and read [the guide](https://weavemindai.github.io/weft/nodes/what-a-node-is.html) 
for how to write one. If it talks to a service weft has never logged into before,
it needs an access node beside it holding the login, which is
[Declaring a service](https://weavemindai.github.io/weft/connections/writing-a-service.html).

A few things we look for in review:

- **The node do one thing**: The test is what somebody reading the graph
  should be able to see. A loop, a retry or a multi-step process they would
  want to watch or resume belongs in the graph rather than hidden inside your
  node, and a node that does two different things depending on what got wired
  in is usually two nodes. We do accept bunch of complex use case in Rust: a browser agent node
  managing its own browser session, an infra node setting itself up. If doing
  it in Rust hides nothing and is clearly simpler, nobody is going to fight you
  over it.
- **Do not write plumbing**: If you find yourself handling
  a credential, keeping a subscription alive, or anything that feels tedious,
  that is usually weft missing something. Build it yourself and send a PR, 
  or open an issue asking for it. For where that line falls today, go check
  [the commandments of plumbing](https://weavemindai.github.io/weft/thinking/plumbing.html).
- **Never ask for a secret in config**: anything in config is stored in
  the clear and shows up in the inspector. Secrets come from
  [a connection](https://weavemindai.github.io/weft/connections/using-a-connection.html).
- **Make it user friendly to use** You can go read
  [which widget when](https://weavemindai.github.io/weft/nodes/metadata.html#widget).
- **Decide `isOutputDefault`** rather than letting it default. There is no safe
  answer, and you can go and read   for the fields, and
  [what your node shows in the graph](https://weavemindai.github.io/weft/nodes/showing-things-in-the-graph.html)
  for what it can put on its own body. [how to choose](https://weavemindai.github.io/weft/nodes/metadata.html#isoutputdefault).
- **Ship a `fake` test**, in a `tests.rs` beside your `mod.rs`. And if it talks
  to a provider, a `live` one on the cheapest path that still exercises the real
  thing.

## Adding a provider

A provider is a service whose prices weft knows. For that you need to code a
**meter**: A wrapper around a client call, that works out what a call cost from the bytes that went out
and came back.

If weft cannot price a service yet and you want every project to get it, open a
PR putting its meter in `crates/weft-providers/src/providers/`. For the trait,
and what a meter has to do before we accept it, go and read
[Measuring what a call costs](https://weavemindai.github.io/weft/connections/meters.html).

## Code style

**No legacy, no compatibility shims.** weft is pre-1.0, so for now do not worry
too much about backwards compatibility. Delete dead code.

**Check for an existing concept before adding one.** If two structs have
overlapping fields under different names, they are one concept split in two.

**Fix root causes.** A fix at the source beats a workaround downstream, however
much smaller the workaround looks.

**Same-language duplicates get merged.** The `// SYNC:` marker is only for a
concept a language boundary forces you to write twice.

For why the code is shaped this way, including the no-fallbacks rule, naming by
contract, and the prose header every file opens with, go and read
[Design principles](https://weavemindai.github.io/weft/thinking/design-principles.html).

**No em dashes**, in code and comments as much as in prose. You can go check
[why we hunt em dashes](https://weavemindai.github.io/weft/thinking/em-dashes.html).

## Working on the database 

Change any table, then:

```bash
./setup.sh --migration add_owner
```

The name for the migration file name. Docker has to be running, because the difference with the existing db is worked out in a throwaway Postgres. Then it installs as usual, and applies it. Change the table again and ask again, as often as you like.

What it writes each time is a **draft**: gitignored and yours alone, and your
database runs it like any other migration. When the shape has settled, collapse
the drafts into the one migration that goes in the PR:

```bash
./setup.sh --migration add_owner --release
```

Releasing also needs your cluster up, since it has to reach the database you
have been developing against.

That collapses every draft into one released migration per table group and
tells your database they are already in it, so nothing re-runs. Forget the step
and the `schema_agreement` test fails.

Never edit a released migration by hand unless you really know what you are doing. Ideally ask for another one instead.

### Running the SQL tests

If you changed SQL, run the tests that exercise it against a real Postgres
before opening the PR:

```bash
scripts/run-db-tests.sh                    # every crate that has them
scripts/run-db-tests.sh weft-broker        # just one
```

It starts a throwaway Postgres in Docker, runs the four crates whose tests need
one, and takes the container away afterwards. Point it at a server of your own
by setting `DATABASE_URL`, which is what CI does. Not the one your weft runs
on, though: these tests create and drop databases on whatever server they are
given.

The one whose output to read is `schema_agreement`, in the dispatcher and
broker runs. It builds one database from the `CREATE TABLE`s and another by
replaying the released migrations, then names whatever differs.

These tests are behind the `db-tests` feature, off by default, so a plain
`cargo test` never builds them.

## Working on the cluster

If the cluster looks wrong, the bug is in the code, a manifest, `setup.sh`, or
the test toolkit. So never `kubectl apply/delete/edit/scale`, never `DROP` or
`ALTER` the live database, and never hand-roll a port-forward (again unless you really know what you are doing but know you are risking your local cluster).

Fix the source instead, then check with a plain `./setup.sh`. If that does not
pick your change up, that is a change-detection bug in the script: fix the
script until a fresh run gets there on its own.

As a last resort, you can `./setup.sh --uninstall --purge` and `./setup.sh` to fully reinstall but know that it will wipe your listeners, infra, and running workers. Before you submit a PR you must install the the previous verison of weft that you are mergin into, and make sure that running your ./setup.sh correclty upgrade the version without any issues.

## Documentation

The book is in [`docs/src/`](docs/src/) and builds with mdBook. For the build
command and the house style, go and read [`docs/README.md`](docs/README.md).

## Working with an AI assistant

Most of this repo is built with one. If you do,
everything our own assistant works from is in `.claude/`: the rules in
`.claude/CLAUDE.md`, the project's own facts in `.claude/MEMORY.md`, the code
reviewer we hand review work to in `.claude/agents/`, and the five flow
commands below in `.claude/commands/`.

Open `.claude/CLAUDE.md` and find-and-replace `[FIRST_NAME]` with your name: it
appears 35 times, because the file talks to you by name throughout.

If you use something other than Claude Code, hand your assistant
`.claude/CLAUDE.md` and `.claude/MEMORY.md` as context. A few passages name
Claude Code's own tools (the Edit tool, `AskUserQuestion`), which yours will
not recognise; the rest is plain instruction. The flow commands below are
Claude Code slash commands, so typing them will do nothing, but the files
behind them in `.claude/commands/` are prose you can paste in, minus the odd
reference to a Claude Code agent.

If you are building a feature, this is our usual workflow. We recommend you use this as this is very effective to write production ready code:

1. **`/flow-1-init`** first, the assistant catch up to the current state of the codebase.
2. **Then babble.** Say what you want, and go back and forth with your AI assistant
  until you both agree on a shape, try to go deep in the details.
3. **`/flow-2-plan`** writes that shape into a file under `~/.claude/plans/`.
4. **Read the plan, and ask what it is still unclear about.** Go round again
   until it says what you meant.
5. I usually run a compaction here
6. **`/flow-3-implement`** builds the whole thing in one session (I usually switch to strongest model I can here, e.g. Fable)
7. I usually run a compaction here and stage the changes
8. **`/flow-4-review`, then `/flow-5-review-check`**, as a pair, and repeat
   the pair until flow-4 turns up nothing.
   `/flow-5-review-check` exists to red-team the fixes `/flow-4-review` just
   made, this is where bugs often comes from.

A small feature is usually done after one pair; a big one takes several.

From step six on it mostly runs itself. Step back in when a review hits a fork. Usually you will see them at the end of the review flows, make sure to read the last message before going to the next flow.

## Pull requests

The checklist is in
[the PR template](.github/PULL_REQUEST_TEMPLATE.md). Try to keep on PR to one feature.

## How we disagree

When we disagree here, we don't hold back.

Say what you actually think is going on, even when you suspect you are missing
something. Holding back because you assume somebody already thought of it is
what makes us miss important flaws.

Four rules:

**Attack the idea as hard as you like, never the person.** Ideas, decisions,
code, situations: hit any of them with everything you have. Swear if you want
to, it moves people in a way that swallowing what you think and handing over a
half-baked pleasantry does not. "This is fucking stupid" is fine if you then
say why. "You are fucking stupid" is not, ever.

**Nobody bails out, however long it takes.** An argument ends when each of you
can understand where the other is coming from and what the crux was. You do not have
to agree, but "let us just move on" is not an ending.

**Listen for real.** Both of you ask about the parts you do not understand,
instead of defending your own position while you wait for your turn to talk.

**Heat is for people who know each other.** A blunt argument works between
people who know the other one cares about them. Somebody who turned up last week has none
of that yet, so be much more careful and patient with them.

## Where to talk

- **[Discord](https://discord.com/invite/FGwNu6mDkU)** for questions. It is the
  fastest way to reach us.
- **[GitHub Discussions](https://github.com/WeaveMindAI/weft/discussions)** for
  longer proposals and design arguments.
- **[GitHub Issues](https://github.com/WeaveMindAI/weft/issues)** for bugs and
  concrete requests.
- **contact@weavemind.ai** for anything private, and for
  [security](SECURITY.md), which never goes in a public issue.

There is also a [code of conduct](CODE_OF_CONDUCT.md).

If you build something with weft, come show us in Discord!
