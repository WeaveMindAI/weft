# Contributing

Thank you for considering lending a hand!

It is early days here, so things move fast and plenty is still up for grabs. If
you think something in the codebase is wrong, it might well be, so please come and tell us on
[Discord](https://discord.com/invite/FGwNu6mDkU).

And if you are not sure whether something is wanted, come ask us:
Discord for a quick question, an issue for something that we should keep track of.

Everything about how weft *works* is in
[the book](https://weavemindai.github.io/weft/). This file is about
working on the codebase.

## Set up

Go check [Install](https://weavemindai.github.io/weft/start/install.html).
As a contributor you additionally need [Node 22.12 or
newer](https://nodejs.org/en/download) and [pnpm](https://pnpm.io/installation)
(`npm i -g pnpm`) to build the extensions from source and running the tests.

## Working on several branches at once

If you want two branches open at once,
[`scripts/add-worktree.sh <new-branch>`](scripts/add-worktree.sh) branches off
the one you are on into `../weft-trees/`, in a folder named after the branch
with any slashes flattened to dashes. It gets its own `target/`, so the two
builds shouldn't fight. Run `./setup.sh` inside it before you use it. That
builds the branch and gives it its own install: a separate weft runtime,
database and files under `~/.local/share/weft/installs/`, so nothing you do on
the branch touches your default install.

Which install a command uses depends on where you run it. That goes for weft
itself, `./setup.sh` and the test runners. On `main`, or outside any weft
checkout, it is your default install; on any other branch, it is that branch's
own. If the branch's install is not set up yet, weft tells you to run
`./setup.sh` in that checkout; it never falls back to your default install. If
you want a particular install from anywhere, set `WEFT_INSTALL`
(`WEFT_INSTALL=default` is your default install). On a detached HEAD there is
no branch to pick from, so a command that acts on an install asks you to set it;
one that needs no install, like `weft validate`, just runs. If you are not sure which install
a command will use, `weft daemon which` tells you.

On a branch, `./setup.sh` installs neither the VS Code extension nor the
browser extension, so your editor keeps the one from your default install.
If you want to try the branch's VS Code extension,
[`scripts/dev-editor.sh`](scripts/dev-editor.sh) opens one extra window
running it from source.

When you are done with a branch,
[`scripts/remove-worktree.sh`](scripts/remove-worktree.sh) removes the
worktree and the branch's install. The git branch itself stays.

Your `.env`, `.env.extension` and `access-apps.json` in every worktree are
links to the main checkout's copies, so an edit in one is an edit in all. If a
setting has to be different in one checkout (its own public tunnel, say), put
it in that checkout's `.env.local`, which is never linked anywhere else and
wins over `.env`.

## Tests

```bash
cargo nextest run --workspace                                  # every test but the Postgres ones
cargo test --workspace --doc                                   # the doc tests, which nextest does not run
cargo clippy --workspace --all-targets --locked -- -D warnings
cargo check -p weft-compiler --no-default-features             # the parse-only build
scripts/validate-examples.sh                                   # every project in examples/ still compiles
pnpm -C packages/weft-graph test                               # the graph renderer
pnpm -C packages/weft-connect test                             # connecting an account
pnpm -C packages/weft-syntax test                              # the .weft grammar and highlighter
pnpm -C extension-vscode test                                  # the VS Code extension
pnpm -C extension-vscode run compile                           # its type check, webview included
node --test catalog/bailey/bridge/images/bridge/src/*.test.js  # the WhatsApp bridge
node --test tangle/kilo-code/tests/*.test.ts                   # Kilo Code's Tangle plugin (Node 22.18 or later)
pnpm -C extension-browser exec tsc --noEmit                    # the browser extension's type check
pnpm -C extension-browser check                                # and its Svelte check
```

The tests run under [cargo-nextest](https://nexte.st). If you do not have it
yet, install it once with `cargo install cargo-nextest --locked`.

If you want only the tests your change can break, keep `--workspace` and
filter what runs: `cargo nextest run --workspace -E 'rdeps(weft-compiler)'`
runs the tests of `weft-compiler` and of every crate that depends on it.
Building with `-p weft-compiler` instead turns on different features in the crates
it depends on, so cargo compiles a second copy of them.

Most crates that have integration tests keep them in one test binary,
`tests/integration/main.rs`, with one module per file, and a crate whose tests
need Postgres keeps those in `tests/db/`. If you add an integration test, make it
a module in `tests/integration/` (or `tests/db/` if it needs Postgres) rather
than a new file directly under `tests/`, which would be one more binary
linking every dependency again.

CI runs all of those, plus the Postgres ones further down. If you want to run
the graph, syntax and VS Code suites, run `./setup.sh` (or just
`./setup.sh --vscode`) once on `main`, or `scripts/dev-editor.sh` once on a
branch: those suites use the VS Code extension's installed npm packages. The
browser extension needs its own: `pnpm -C extension-browser install`.

### Which layer a test belongs at

We sort tests into four layers, and a test's place tells you which one it is.

1. **Pure functions.** Values in, values out, no I/O, in a `#[cfg(test)]`
   block next to the function. Most tests live here.
2. **Wire shapes.** Round-trip a type that crosses a process boundary through
   its serialization, next to the type. This catches a renamed field breaking
   the other side.
3. **Contracts with fakes.** One subsystem's real code against in-memory fakes
   of its I/O. The fakes are hand-rolled, named `Fake*`, and sit behind the
   crate's `test-helpers` feature. We do not use mock libraries, because a
   generated mock hides what the test actually checks.
4. **End to end.** Real binaries on a real install with a real Postgres,
   through `scripts/run-e2e.sh` below.

If you are not sure, the lowest layer that can see the bug is the right one.
Orchestration bugs are the ones only layer 3 catches. A node's own tests span
layers: its `basic` tier is layer 1, `fake` is layer 3, and `live` is layer 4
pointed at a real account.

Three more suites have their own runners:

```bash
scripts/run-db-tests.sh [crate]         # the tests that need a real Postgres
scripts/run-node-tests.sh [package]     # node tests, over everything in catalog/
scripts/run-e2e.sh [name]               # the end-to-end suite
```

The node one goes package by package and stops at the first failure. If you
want to pick up where it stopped, `--from <name>` starts the suite at that
package. If you want them all in one go, pass `--parallel`: one build, then
every package side by side, or up to N at once with `--parallel N`. It does
not stop at a failure: every package runs.

If another end-to-end run is already going on this machine, from any
checkout, yours waits for it to finish.

For the end-to-end runner's flags (including `--failed`, which reruns only
what did not pass last time) and how to read a failure, go and read
[the e2e README](crates/weft-e2e/README.md).

The database one has its own section further down.

If you changed something all the nodes depend on, such as a ctx function or
the code generator, run the node test script: it works in a
scratch project built from this checkout. For how to write node tests, go
and read
[Testing a node](https://weavemindai.github.io/weft/nodes/testing.html).

By default `run-node-tests.sh` runs the basic and fake tiers, which need
nothing set up and are free.
If you want to test with real services `--tier live` runs the nodes against
real accounts and **spends real money**, so it remembers what passed and skips
a package until you change it (you can force a rerun with `--retest` or by wiping
the cache in `.live-pass-cache` in `target/node-tests`).

The live tier needs a real connection per service. If you signed into one in any editor,
it is picked up automatically; otherwise, for pasted keys, the runner reads
`WEFT_NODE_TEST_*` from this repo's root `.env`. For how those variables are
named, and the other ways to hand a test an account, go and read
[Live tests](https://weavemindai.github.io/weft/nodes/testing.html#live-tests).

When an end-to-end test fails, its project and any containers it made are
left on your install on purpose so you can debug what happened.

If you are setting up the end-to-end suite, the only variables you have to set
are for the outside services. They are `WEFT_E2E_*`, and they are listed in
[`crates/weft-e2e/README.md`](crates/weft-e2e/README.md#credentials-and-what-the-runner-provides-for-you).

A test that fails intermittently is a bug, so put anything timing-sensitive
through `stress_test!`
(`crates/weft-core/src/test_support.rs`). You need `futures` as a
dev-dependency in the crate you call it from.

If you add a dependency to a crate or change its features, run
`cargo hakari generate && cargo hakari manage-deps` before you open the PR
(install it once with `cargo install cargo-hakari --locked`). It updates
`crates/weft-workspace-hack`, which turns on the same features of each
dependency for every crate, so testing one crate reuses what another already
compiled instead of compiling its own copy. CI fails if it is out of date.

## Writing a node for the standard catalogue

If you want a first contribution, this is a good one. A node is a folder with
two files in it. [The guide](https://weavemindai.github.io/weft/nodes/what-a-node-is.html)
covers how to write one. If it talks to a service weft has never logged into before,
it needs an access node beside it holding the login, which is
[Declaring a service](https://weavemindai.github.io/weft/connections/declaring-a-service.html). You can ask tangle to build one for you, tangle has all the knowledge to build one properly.

A few things we look for in review:

- **The node does one thing**: The test is what somebody reading the graph
  should be able to see. A loop, a retry or a multi-step process they would
  want to watch or resume belongs in the graph rather than hidden inside your
  node, and a node that does two different things depending on what got wired
  in is usually two nodes. We do accept complex use cases in Rust: a browser agent node
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
  [a connection](https://weavemindai.github.io/weft/connections/using-one-in-a-node.html).
- **Make it easy to use.** You can go read
  [which widget when](https://weavemindai.github.io/weft/nodes/metadata.html#widget).
- **Say what it shows.** [What your node shows in the graph](https://weavemindai.github.io/weft/nodes/display.html)
  says what it can put on its own body.
- **Ship a `fake` test**, in a `tests.rs` beside your `mod.rs`. And if it talks
  to a provider, a `live` one on the cheapest path that still exercises the real
  thing.

## Working on Tangle

Tangle is the AI builder that lives inside a weft project: the persona, the
skills, the specialists and the commands that an assistant loads when a user
opens their project. It is all prose, in [`tangle/`](tangle/), one folder per
assistant, and it is open to contribution. If you use weft with an AI and you
have watched it get something wrong, you are the right person to fix the
words that let it.

Three kinds of change are all welcome:

- **Making Tangle better everywhere.** A rule it needs, a trap it keeps
  falling into, a step of the loop that is vague. Carry these to every folder,
  because a fix to how Tangle thinks is a fix for everyone.
- **Adding a skill.** The skills are the knowledge Tangle loads when the work
  calls for it. If something about weft is hard to get right and no skill
  covers it, that is a skill worth writing.
- **Tuning one assistant.** Assistants answer to different wording, and one
  may need a rule stated twice, or earlier, or in different words, to hold to
  it. That change belongs in that assistant's folder and nowhere else.

**The folders are copies on purpose, and they are meant to drift.** There is
no shared source and nothing checking they agree. The line we hold is that
what Tangle *is* has to be true in every folder, while how it is *said* is
free to differ. Tangle everywhere shapes the graph before it builds, reads a
node's real interface instead of writing one from memory, keeps a level of the
graph small enough to read, fails loudly rather than papering over, and
reports in plain words. Change one of those and it goes in all eleven folders.
Change phrasing, ordering, an example, or which of an assistant's own features
gets used, and it stays in one.

If you are adding a whole new assistant, the mechanics are one row in
`ASSISTANTS` in
[`crates/weft-cli/src/commands/new.rs`](crates/weft-cli/src/commands/new.rs)
plus the folder, and a test checks that the row's files actually exist. Read
that assistant's own documentation first: the folders differ because the hosts
do, and the interesting part is finding what it gives you that the others do
not. For how the current ten each solve it, go and read
[`tangle/README.md`](tangle/README.md).

## Adding a provider

A provider is a service whose prices weft knows. For that you need to code a
**meter**: a wrapper around a client call that works out what a call costs from the bytes that went out
and came back.

If weft cannot price a service yet and you want every project to get it, open a
PR putting its meter in `crates/weft-providers/src/providers/`. The trait,
and what a meter has to do before we accept it, is in
[Measuring what a call costs](https://weavemindai.github.io/weft/connections/meters.html).

## Code style

**No legacy, no compatibility shims.** weft is pre-1.0, so for now do not worry
too much about backwards compatibility. Delete dead code.

**Check for an existing concept before adding one.** Before you add a type,
look for one that already answers the same question, and if there is one,
extend it. If you find two structs with overlapping fields under different
names, they are one concept split in two, so try to merge them. Fields that
only happen to match stay apart: a session's `user_id` says who signed in, and
a request scope's `user_id` says whose data that one request may touch.

**Fix root causes.** If a bug shows up downstream of where it starts, fix it
where it starts, even when a workaround further down would be a smaller
change.

**One fact, one place.** If you find the same fact written twice where one
copy could use the other, merge them. When it has to live in two places that
cannot share it (a Rust type and its TypeScript mirror, or two crates that
cannot depend on each other), each side carries a
`// SYNC: <this> <-> <path> <other>` comment naming the other copy.

**No fallbacks.** When something fails, fail loudly with an error the user can
see, or a log that names it. A fallback that quietly returns a second-best value
hides the bug. Make the error say what broke and what the
person can do next.

**Name by contract, not mechanism.** A name says what the caller asks for and
gets back, not how it is computed. `connect_db_and_login_and_hand_connection`
should be called `db_connection`. Read the call site alone:
does the name say what the caller receives?

**No em dashes**, in committed code and comments as much as in prose. Models
reach for them far more than people do, so an em dash reads as a tell that
nobody read the line again.

**One casing per layer.** If you add a name that people type or read outside
the Rust code (a node, a port, a `metadata.json` key, a JSON field, a flag, a
setting), spell it the way this table says:

| Spelling | For |
|---|---|
| PascalCase | Node types, custom types |
| camelCase | Node names in a program, ports, settings, record fields, `metadata.json` keys, the names of the values a connection stores (`imapHost`), and every JSON field weft produces: API answers, live events, CLI `--json`, what a program call answers, the `examples/<name>.json` files `weft run --save` and `weft freeze` write |
| snake_case | The values of a fixed set (statuses, kinds, the `kind` tag of a tagged event, and the event topics and `accessApps` keys a `metadata.json` names), `.weft` files and folders, `weft.toml` keys, SQL, URL query parameters, the language's own words (`@per_instance`, `@require_one_of`, the `_should_flow` gates) |
| kebab-case | CLI commands and flags |
| SCREAMING_SNAKE_CASE | Environment variables |

If a Rust type goes out to a client, give it
`#[serde(rename_all = "camelCase")]`, so `run_id` in Rust is `runId` on the
wire. If it is a tagged enum, the tag values stay snake_case and only the
fields inside each variant are renamed:
`#[serde(tag = "kind", rename_all = "snake_case", rename_all_fields = "camelCase")]`.
If a type never leaves weft (task payloads, run records, the broker's wire), it
keeps snake_case fields. A name a third party defines keeps its spelling (an
OAuth parameter like `client_id`, a provider's field).

If you add a node to the catalog, or add or rename any name in a node's
`metadata.json`, run `cargo nextest run --workspace -E 'package(weft-catalog)'`. If any name is off,
the `casing` test fails and lists every one with its node and file, and with
the spelling to use whenever one can be worked out. If you write a key
weft renamed in its old snake_case spelling (`requires_infra` instead of
`requiresInfra`), the `casing` test does not catch it: the node fails to load
with an error naming the new spelling, and `every_stdlib_node_loads_strict`
fails. In a package root's `metadata.json` the error is reported against the
root's file, and the package's members are left out.

If you want to change what counts as camelCase, snake_case or any other
spelling, edit `weft_core::casing`, which both the catalog's spelling check and
the compiler's `not-camel-case` warning call. If you want to change which names
get checked, edit the check itself: `crates/weft-catalog/src/spelling.rs` for
the names in a `metadata.json`, or `check_name_casing` in
`crates/weft-compiler/src/validate.rs` for the names in a `.weft` program.

For the general principles behind weft's design, read
[Design principles](https://weavemindai.github.io/weft/thinking/design-principles.html).

Before building a feature, go and read
[the three fronts](https://weavemindai.github.io/weft/thinking/how-we-decide.html):
the levers, the defaults, and what weft refuses.

## Working on the database

Change any table, then:

```bash
./setup.sh --migration add_owner
```

The command names the migration file. Docker has to be running, because the diff against your existing database is computed in a throwaway Postgres. Then it installs as usual and applies the draft. Change the table again and ask again, as often as you like.

What it writes each time is a **draft**: gitignored and yours alone, and your
database runs it like any other migration. When the shape has settled, collapse
the drafts into the one migration that goes in the PR:

```bash
./setup.sh --migration add_owner --release
```

Releasing also needs your checkout's install up, since it runs against that
install's database, the one you have been developing against.

That collapses every draft into one released migration per table group. For
each group it then looks at your database: if it ran the drafts, it is told
the release is already in it, so nothing re-runs; if you skipped the draft
pass for that group, the release runs the file on it now. Both can happen in
one command, and it all lands in one transaction, so a refusal leaves your
tree and your database as they were. Forget the step and the
`schema_agreement` test fails.

Each migration comes as two files, which the command splits for you.
`<id>.sql` only adds: a table, a nullable column, an index, a function.
`<id>.contract.sql` takes away and tightens: a dropped column or table, a NOT
NULL, a CHECK or UNIQUE on a table that already has rows. A booting weft runs
every pending `.sql` file, then carries the JSON documents forward (next
section), then runs every pending `.contract.sql` file, all in one boot. If you
add a NOT NULL column with no default to a table that has rows, the first file
adds it nullable and the second makes it NOT NULL, so whatever fills it runs in
between (a data step, below). If you change a function's arguments, the first
file drops the old one and makes the new one in the same transaction, so a call
with the old arguments fails loudly (no such function), and the arguments you
added need no `DEFAULT`.

If the rows already there need something done before the second file runs,
give the group whose tables the data lands in a `DataStep`: its table, what the
second file is about to do (`until`, which names the column or index it waits
on), and an SQL statement that does the work. If you drop a column whose
content has to live on somewhere else, the step copies it there,
`until: Until::Dropped("<column>")` (`UNITS_MOVE` in
`crates/weft-dispatcher/src/infra_node.rs` is one). If you add a NOT NULL
column the rows already there have to be filled in from somewhere, the step
fills it, `until: Until::NotNull("<column>")`. If you add a unique index the
rows already there might break, the step merges them first,
`until: Until::Indexed("<index>")` (`EXCLUSIVE_FOLD` in
`crates/weft-access-store/src/lib.rs` is one). The boot runs it just before
the pending `.contract.sql` files, in the same transaction, with its table
locked, and only while the column is still there or still nullable, or the
index not built yet. Nothing records that it ran, so the statement has to give
the same result run twice
(`ON CONFLICT DO NOTHING`, a `SET` on the rows still `NULL`).

If you renamed a column or a table, say so. The diff alone sees a column going
and another arriving, and would drop the old one with its rows, so the command
stops and lists every such pair until you name each rename, in the order they
happen:

```bash
./setup.sh --migration rename_owner --rename project.owner=owner_id --rename old_table=new_table
```

If the pair it lists really is two unrelated columns, add `--unrelated` instead,
and the old one's rows are thrown away.

Never edit a released migration by hand: the boot checksums every applied one and refuses the change. Ask for a new migration instead.

### Changing a type weft stores as JSON

Some columns hold JSON that outlives an update: a connection's recipe, a run's
record, a program. If you rename, move, add or remove a field of one of those
Rust types, the rows an older weft wrote no longer read as today's type, so
the change needs a step that rewrites those rows into the new shape. You can
find every such column, with its steps, in the `DOCUMENTS` list in
`crates/weft-runtime/src/schema.rs`.

If you change one of those types:

1. Add a step at the end of the type's list of steps, which sits next to the
   type (`ACCESS_SPEC_UPGRADES` beside `AccessSpec`, for example). The list is
   what the type's `STORED` passes to `Kind::new`; if that says `&[]`, the type
   has no list yet: make one next to the type and pass it there. A step is a few
   `weft_core::stored::Op`s on the old JSON: `RenameKey`, `AddKey`, `DropKey`
   and `RenameTag`. Each takes `at`, a dotted path into the JSON where `*`
   means every item of a list or map. If none of them can say the change,
   `Op::Code` takes a function over the JSON.
2. If your type is also stored inside another one, give the outer type a step
   too: a `Within` at the path where yours sits, running your step's ops.
   `PROJECT_DEFINITION_UPGRADES` does this for the `AccessSpec` a program's
   nodes carry.
3. Add `v<N+1>.json` beside `v<N>.json` in
   `crates/weft-runtime/documents/<table>.<column>/`: the same documents, in
   the new shape.

To check it, run `cargo nextest run --workspace -E
'test(every_document_sample_reads_as_todays_type)'`. Today's file has to read
as today's type and write back unchanged, and every older file, carried
through the steps, has to come out identical to today's file. If you change
the type and forget the step, it fails naming the column, as long as the
samples hold the field you changed. If they don't, first add a document that
does to every version's file.

Every row says which version its document was written at, in the column
beside it, `<column>_version`. If you write one of these columns, write that
column too, with the type's `STORED.version()` (`AccessSpec::STORED`, for
example). If you read one, read both and hand them to `STORED.decode`, which
parses a document at today's version and carries an older one through its
steps first. A row a newer weft wrote is refused, naming both versions. The
version column has no default, so a write that forgets it fails instead of
being labelled version 1.

Between the two parts of the migrations, the boot also rewrites the older rows
in place, in batches of 500, so the readers almost never carry one. The version column is how far it got, so an
interrupted boot carries on with exactly the rows still behind.
`weft_document_version` records the version each column was carried to: a
boot that finds them all current reads that and nothing else, and one that
finds a version it does not know refuses to start. If another column holds a
hash of the JSON (`access_grant.events_recipe_hash` holds one of each
connection's events recipes), the hash changes when the JSON does, so the
entry's `rederive` names a function that computes it again; the boot runs it
in the transaction that records the column. A program is the one exception:
its hash is its identity, and other rows name it by that hash
(`run.definition_hash`, an example's `frozenFrom.definitionHash`), so it keeps
the hash it was made under and its entry has no `rederive`.

If your step moves a value out of the JSON into a column or a table of its
own, the old rows still hold it only in the JSON, so the entry needs a `Fill`:
an SQL statement that reads the rows still at the step's version and writes
the value where it lives now. The boot runs it just before the step, and runs
it again if it was interrupted, so it has to give the same result twice
(`project_store::RESYNC_FILLS` is one). The column or table it writes to
arrives in the migration's first file, so it is there.

A fill is for a column that stays. If the whole column goes, take it out of
the `CREATE TABLE` and its entry out of the list, and if what it held lives
on elsewhere, copy it there with a `DataStep` (see
[Working on the database](#working-on-the-database)).

If you add a column whose rows outlive an update, give its type a `STORED`
next to it (`Kind::new` with what a message calls one, and `&[]`), add
`Document::of(&YourType::STORED, ...)` to the list with a `v1.json`, put
`<column>_version INTEGER NOT NULL` beside it in the `CREATE TABLE`, and bind
it in every `INSERT` into the table, even one that leaves the document empty.
The migration adds the column holding 1 on the rows already there, then drops
that default.

A saved example (`examples/<name>.json`) is a `RunSpec`, and takes the same
steps as the `run.spec` column (`RUN_SPEC_UPGRADES` in
`crates/weft-core/src/run_spec.rs`). The file keeps its own version under
`specVersion`, and the CLI writes an old file back in today's shape the first
time it reads one.

### Running the SQL tests

If you changed SQL, run the tests that exercise it against a real Postgres
before opening the PR:

```bash
scripts/run-db-tests.sh                    # every crate that has them
scripts/run-db-tests.sh weft-broker        # just one
```

It starts a throwaway Postgres in Docker, runs every crate whose tests need
one, and takes the container away afterwards. If an earlier run was killed
before it could clean up, the next run removes that run's container too. Point it at a server of your own
by setting `DATABASE_URL`, which is what CI does. Not the one your weft runs
on, though: these tests create and drop databases on whatever server they are
given.

If a migration is wrong, `schema_agreement` in `weft-runtime` is the test that
tells you. It builds one database from every table group's `CREATE TABLE`s
and another by replaying the released migrations, then names whatever
differs.

These tests live in each crate's `db` test binary. A plain `cargo nextest run`
builds them and leaves them out; the script runs them through nextest's `db`
profile (`.config/nextest.toml`).

## The browser extension

Users install it from their browser's store, so nothing here touches them.
For local work on it: `./setup.sh --browser --no-sign` writes an unpacked
build per browser under `extension-browser/build/` (Chrome loads it from
`chrome://extensions` with Developer mode on; Firefox loads the zip
temporarily from `about:debugging`). A signed Firefox build that survives
closing the browser needs `web-ext` on your `PATH` and Mozilla AMO keys in
`.env.extension`; the script stops before building anything if either is
missing and says where to get them.

Releases are version bumps, nothing else: `./setup.sh --browser --bump` (or
`--vscode --bump` for the editor) bumps the version, and CI on the pushed
commit builds, signs and publishes to the stores, recording each store's
version tag. The full store rules are in the releasing skill
(`.claude/skills/releasing/SKILL.md`).

## Working on your install

If your install looks wrong, fix the cause in the code, in `setup.sh` or in the
test toolkit. A `docker rm` or `docker restart` on weft's containers, or a
`DROP` or `ALTER` on the live database, hides the bug and can break your
install, so only reach for one if you accept that risk.

Fix the source instead, then check with a plain `./setup.sh`. If that does not
pick your change up, that is a change-detection bug in the script: fix the
script until a fresh run gets there on its own.

As a last resort, `./setup.sh --uninstall --purge` followed by `./setup.sh`
reinstalls from scratch, but it deletes everything the install held: your
projects, their infrastructure and data, your stored connections, and every
image weft built, so the next install builds everything again.

Before you submit a PR you must install the previous version of weft that you are merging into and make sure that running your `./setup.sh` correctly upgrades the version without any issues.

If you work in a worktree, its install starts empty, so put the old version
in it first. `weft daemon which` in your worktree prints its install's name.
Make a throwaway checkout of the branch you merge into
(`git worktree add --detach ../weft-trees/base main`), run
`WEFT_INSTALL=<that name> ./setup.sh` there, remove it with
`git worktree remove ../weft-trees/base`, then run `./setup.sh` in your
worktree.

## Documentation

### Found something wrong in the docs?

These docs are AI-written. I'm reading through and refining them, but I
might have missed some spots as they are very long. Mistakes will get through. If you
spot one, please [open an issue](https://github.com/WeavemindAI/weft/issues)
with the page and what looks wrong, or come tell us in
[Discord](https://discord.com/invite/FGwNu6mDkU). You don't need to have the
correction worked out before raising it. 

If something is hard to understand, we want to know about that too.

The book is in [`docs/src/`](docs/src/) and builds with mdBook. The build
command and the house style are in [`docs/README.md`](docs/README.md).

## Working with an AI assistant

Most of this repo is built with one. The repo ships a complete assistant
setup for most coding assistants: the `working-partner` persona, the eight
mode skills, the five flow commands and the code reviewer, as one
derivation per harness (`.claude/` for Claude Code, `.kilo/` for Kilo Code,
and more arriving). Each derivation is written for its harness rather than
translated, nothing is always-loaded, and every harness reads the same root
`MEMORY.md`, the project's own facts, so the knowledge stays shared.

In Claude Code, the derivation is `.claude/`: the rules in
`.claude/CLAUDE.md` (which imports `MEMORY.md` at session start), the eight
mode skills in `.claude/skills/`, the code reviewer in `.claude/agents/`
that our review rounds go to, and the five flow commands below in
`.claude/commands/`.

The setup deliberately excludes your personal memory:
`.claude/settings.json` sets `claudeMdExcludes` for `~/.claude/CLAUDE.md`
and `~/.claude/rules/`, so only the project's rules load here. On Windows,
or if your home is elsewhere, add your own pattern to
`.claude/settings.local.json`. Run `/context` in a session to confirm what
actually loaded.

One thing the repo cannot override: same-named slash commands resolve
personal-over-project in Claude Code. If you keep old copies of the flow
commands in `~/.claude/commands/`, yours will win over this repo's. Rename or
remove your personal copies, or launch with `claude --setting-sources project`
in this repo. Agents resolve the other way (project-over-user), so the
project's `code-reviewer` always wins.

In an assistant we have not shipped a derivation for yet, hand it the root
`CLAUDE.md` (which pulls in the root `MEMORY.md`), the mode skills in
`.claude/skills/mode-*/SKILL.md`, and the agent file in `.claude/agents/` as
context. A few passages name Claude Code's own tools (the Edit tool,
`AskUserQuestion`), which yours will not recognise; the rest is plain
instruction. The flow commands below are Claude Code slash commands, so typing
them will do nothing, but the files behind them in `.claude/commands/` are
prose you can paste in, minus the odd reference to a Claude Code agent.

If you are building a feature, this is our usual workflow:

1. **`/flow-1-init`** first, so the assistant catches up to the current state of the codebase.
2. **Then babble.** Say what you want, and go back and forth with your AI assistant
  until you both agree on a shape, try to go deep in the details.
3. **Run `/flow-2-plan`**; it writes that shape into a file under `~/.claude/plans/`.
4. **Read the plan, and ask what it is still unclear about.** Go round again
   until it says what you meant.
5. **Run a compaction.**
6. **Run `/flow-3-implement`**; it builds the whole thing in one session (I usually switch to the strongest model I can here, e.g. Fable)
7. **Run a compaction and stage the changes.**
8. **Run `/flow-4-review`, then `/flow-5-review-check`**, as a pair, and repeat
   the pair until flow-4 turns up nothing.
   `/flow-5-review-check` exists to red-team the fixes `/flow-4-review` just
   made; this is where bugs often come from.

A small feature is usually done after one pair; a big one takes several.

From step 6 on it mostly runs itself. Step back in when a review hits a fork. Forks usually surface at the end of the review flows; read the last message before starting the next flow.

## Pull requests

The checklist is in
[the PR template](.github/PULL_REQUEST_TEMPLATE.md). Try to keep one PR to one feature.

## How we disagree

When we disagree here, we don't hold back.

Say what you actually think is going on, even when you suspect you are missing
something. Holding back because you assume somebody already thought of it is
what makes us miss important flaws.

Four rules:

**Attack the idea as hard as you like, never the person.** Ideas, decisions,
code, situations: hit any of them with everything you have. Swear if you want
to, it moves people in a way that swallowing what you think and handing over a
half-baked pleasantry does not. "This is stupid" is fine if you then
say why. "You are stupid" is not, ever.

**Nobody bails out, however long it takes.** An argument ends when each of you
can understand where the other is coming from and what the crux was. You do not have
to agree, but "let us just move on" is not an ending.

**Listen for real.** Both of you ask about the parts you do not understand,
instead of defending your own position while you wait for your turn to talk.

**Heat is for people who know each other.** A blunt argument works between
people who know the other one cares about them. Somebody who turned up last week has none of that yet, so be much more careful and patient with them.

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

And once you have sent us anything at all, ask us to put you on
[THANKS.md](THANKS.md), or add yourself in the same pull request.
