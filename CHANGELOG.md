# Changelog

What changed between versions of weft that you need to know about when you
update. To update, run `weft maintenance on`, pull and run `./setup.sh` again,
then run `weft maintenance off`.

## Unreleased

**A PostgresDatabase no longer runs out of connections when the program
scales.** Postgres serves a fixed number of connections, while its clients
keep changing: workers scale, a change of worker settings runs old and new
workers side by side, and a frontend connects too. Each worker used to keep up
to 20 connections open whether it used them or not, so once enough workers ran
at once, Postgres answered `too many clients` to them and to everything else
using that database. A PostgresDatabase now runs a pooler in front of Postgres,
in the same unit. The pooler never opens more connections than Postgres takes,
and a client that finds them all busy waits for one, so a run waits instead of
failing. Watching queries (PostgresWatchQuery) get a quarter of the
connections the pooler opens (what is left after 3 kept for the superuser and
2 spare), at least one, so a crowd of them never leaves the other queries
without a connection, or the other way round. If you want to see who holds the
connections, the node's card now shows them: Postgres's own by user and state,
each of the pooler's two shares with its clients served and waiting, and the
clients by address.

Your existing PostgresDatabase nodes get the pooler on their next `weft infra
upgrade`. When the database starts again it gets a new role, `app`, which
becomes the owner of everything the superuser owned in it (except objects an
extension made). After that:

- **Every client signs in as `app`.** The host, port and password stay the
  same. If a frontend or another client of yours signs in as `weft`, switch it
  to `app`: `weft infra env <node> --into .env --set DATABASE_USER=User` writes
  it again. Through the pooler a client reaches this one database only, so a
  client that also used `postgres` or another database made beside it can no
  longer reach those. The superuser `weft` can only sign in from inside the
  unit. If the program needs an extension, trusted by Postgres or not, list it
  in the node's new `extensions` setting (`["pg_trgm", "postgres_fdw"]`): the
  superuser makes each one on every start, and `app` uses it. A name Postgres
  does not have fails the start, and `weft infra status` names it.
- **A database whose name the pooler cannot read as written gets a second
  name for clients.** Your database keeps its name and its data. When the
  name is letters, digits, `_` and `-` only, at most 57 characters, and none
  of `pgbouncer`, `all`, `sameuser`, `samerole`, `samegroup` or
  `replication`, clients use it as before; otherwise they sign in by a name the
  pooler makes from it (`db_my_app_1a2b3c4d`), which the connection carries
  and the card shows under Database. A client of yours outside the program
  takes it from there.
- **The database closes a connection that sat idle for a minute.** That gives
  back a connection held by a worker that lost its CPU between requests, or by
  an old worker a rollout left behind. If a frontend keeps a pool, set its idle
  timeout under a minute, or have it test a connection before using it. If it
  keeps a connection idle on purpose (one that `LISTEN`s), have it lift that
  limit for its own session when it connects, the way PostgresWatchQuery does:
  `SELECT set_config('idle_session_timeout', '0', false) FROM pg_settings
  WHERE name = 'idle_session_timeout'`.
- **The least `memory` is 565Mi** (it was 886Mi). At 1Gi Postgres serves 23
  connections, at 2Gi 59, at 4Gi 131.
- **A new database needs Postgres 14 or later.** One you already have on an
  older Postgres keeps running, but it cannot close idle connections, so its
  card and `weft infra status` warn of it and list the commands that dump it
  and restore it into a new node on 17.

**Every Postgres node closes a pooled connection that sat idle for 5
seconds**, and never hands out one idle for longer, whichever database it talks
to.

**A stopping worker gives back what its runs shared** (`ctx.shared`) as soon
as its last run has ended, instead of when the process goes, so a pool's
connections return to the database while the worker writes its last records.

**On Cloud Run, a change of worker settings retires the old workers.** Once
the new settings serve, weft moves the calls the old ones took to them and
removes that program's revisions under any other settings, so their copies stop
instead of running beside the new ones. Which settings are current is decided
from what the project has stored, so a dispatcher copy that briefly still held
the old settings and brought them back up never wins: that revision takes no
calls and scales to zero. It goes at the next settings change or activation,
unless it is the service's latest, which Cloud Run does not let anyone delete;
then it goes once a later settings change or build replaces it.

**When an infra node's setup fails, you are told why.** `weft infra start`,
`weft activate` and the other commands that wait on a setup used to answer
`infra setup failed` and nothing else; they now give the error the setup run
failed with (a trigger's setup the same), as `weft runs` shows it.

**A unit that knows why it is not ready says so in `weft infra status`.** When
a container's health check answers an error with a reason in its body (JSON
`{"error": "..."}`, or plain text), the status shows that reason after the
status code.

**A node's fake tests hold a published connection to its service's values.**
A value the service does not declare, or a required one missing, fails the
run the way the access store refuses it on an install; before, the fake rig
stored anything and only a real run found out.

## 2026-10-31 (0.2.280)

**A project with nodes of its own compiles from a cold cache once** after this
update: weft no longer builds with `--cfg tokio_unstable`, which changes how
everything compiles. After that first build it builds incrementally again. The
stock worker, which every project that uses only weft's own nodes runs, is
rebuilt once with the new version, as on every update, and those projects
compile nothing. A worker built by the previous weft is refused when it claims
a run or reads a run's record, and the broker's log names `weft resync` for its
project.

**Reading a long run back takes the same memory however long the run is.**
Opening a run in the editor, `weft events`, `weft logs` and following a run read
its record a page at a time, and forget a node once it has finished and a
loop's iteration once nothing in it is still running. The one thing that still
grows is what a loop gathers, which it hands on whole when it ends. A run
started with `--seed` reads the runs it reuses the same way, keeping only the
nodes it takes from each. A worker driving a run forgets the same way, so it
holds as much on a loop's sixty-thousandth iteration as on its first (what the
loop gathers aside). It reads a run's record a page at a time too, and when it
answers a wait it reads only what landed since it last looked.

**A fast run no longer cuts off the people following it.** `weft run`, `weft
wait` and the editor used to stop with "live updates interrupted" or "Lost the
live connection" when a run sent a burst of events. A run's follower now gets
the run's history and then its live events on one stream, each event once; if
it falls behind, weft catches it up from the record, and if the connection
drops, it carries on from the last event it got.

**A value travels once.** The editor's live events and a run's replay used to
carry a copy of every value each time it was shown (a firing's input, the
output it handed out). They now say where the run's record keeps it, and the
editor reads a node's values when you open its inspector or a debug preview
shows them. `weft events`, `weft run` and a program reading a run see the
values as before.

**weft no longer needs `--cfg tokio_unstable`.** If you build weft yourself, a
`RUSTFLAGS` of your own works again, and a node can set `RUSTFLAGS` in its
`deps.toml` `[build.env]` again. On a local install and in `weft test-node`,
every `[build.env]` variable now reaches the compile: a local build used to drop
the ones cargo reads for itself (`RUSTFLAGS`), and `weft test-node` dropped
`[build.env]` entirely. If your custom worker Dockerfile template
(`[build.worker] dockerfile_template`) has the line
`ENV RUSTFLAGS="{{worker_rustflags}}"`, delete it: weft refuses a template that
still has it, and the error names the line. A project whose nodes set `[build.env]` now compiles in a compile cache
kept for those nodes and their values, so building it no longer makes the next
build of every other project start from scratch. On a local install,
`weft clean --images --all` removes those caches.

**A node that does heavy work in one go no longer puts its run at risk**: the
worker renews its lease from a thread of its own. Such work still slows the
worker's other calls and runs, so it belongs in `tokio::task::spawn_blocking`;
for how, go and read "Five things that would have bitten you" in Your first
node.

**Reading what an instance spent no longer reads every run's record.**
`ctx.costs()` and the `InstanceCosts` node read a table with one row per cost,
written together with the run's record, and answer the same records in the
same order. A run's costs are still deleted with the run (its keep time running
out, `weft clean`, `weft rm`), so a total only counts the runs weft still
keeps: a cap over all time needs runs kept `forever`. The update reads every
stored run's record once to fill the table, so it takes longer on an install
with a lot of history.

**A NUL character in a node's data no longer fails a write.** A trigger whose
setup produced a NUL character could never be baked, and an infra spec, its
health rules or a connection's spec holding one failed to save. These are now
kept exactly, NUL included.

**If you read weft's HTTP API yourself:** `GET /runs/{id}/replay` and `GET
/events/run/{id}` now name each value by where the record keeps it (`{"from":
"row", ...}`); add `?with_values=true` to get the values written in, as before.
`POST /runs/{id}/values` reads back the values some events name.
`/events/run/{id}` takes `after=<seq>:<index>:<k>` (the end of an event's
`eventId`) to start past an event you hold, and sends an SSE event named
`caught_up` once the history is sent. `GET /runs/{id}/events` writes `total`
after `events`. Corruption notices (`journal_corruption`) now arrive in order
with the rows, carrying a `journal:` event id, instead of at the end with an id
of their own.

## 2026-10-31 (ae995e3a)

**Decision models, as nodes.** A new `decision` package asks OpenRouter's
decision models a typed question and gets probabilities back, with no reply
to parse: Decide: Yes or No, Decide: Pick One, Decide: Score and Decide: Find
in Image, fed by an OpenRouter Decision Model node wired from the OpenRouter
Access you already have. They are often cheaper than an LLM for routing,
guardrails and checks around LLM or human steps. What the model judges is the
inputs you add to the node (`DecisionYesNo(ticket: String, photo: Image)`):
each is a value made of JSON, a file, or a list of files, and a file inside a
record is refused when you build, so give each file its own input. Calls on
weft's own OpenRouter key are measured like any other.

**If you write nodes:** a `validate` rule can now ask about the inputs a
program added: `custom_inputs_declared` (it added any) and `custom_inputs_fit`
(each one fits one of the types `oneOf` lists), with `{customInputs}` and
`{names}` in the message. And `weft test-node` now refuses to run when a node
of the package it tests failed to load, naming it, instead of reporting the
rest of the package green.

## 2026-10-30 (671e50f4)

**Long runs open quickly, and weft keeps answering while they do.** Opening a
long run in the editor, `weft events`, `weft logs` and `weft node-state` took
longer the longer the run was: a run of 135,000 events took close to a minute,
and the whole of weft stopped answering meanwhile. The same run now opens in
about three seconds, and everything else keeps answering. A long loop no
longer slows down the further it gets either.

**weft gives memory back.** It used to keep the memory of its largest replay
until it was restarted; it now returns it within seconds.

**A durable run carries on when its worker loses weft's broker.** If a worker
cannot write a durable run's record for over a minute, the run now moves to
another worker and carries on from what was recorded, the way it does when a
worker dies, instead of failing. A fast run still fails.

**Every project's worker compiles from scratch once** after this update.

**If you build weft yourself, or customize how workers build:**

- weft now builds with `--cfg tokio_unstable`, which the checkout's
  `.cargo/config.toml` sets, so build from the checkout (`./setup.sh` does). A
  `RUSTFLAGS` of your own replaces that setting: add `--cfg tokio_unstable` to
  it.
- A custom worker Dockerfile template (`[build.worker] dockerfile_template`)
  has to keep the line `ENV RUSTFLAGS="{{worker_rustflags}}"` before its
  `cargo build`.
- A node can no longer set `RUSTFLAGS` in its `deps.toml` `[build.env]`: a
  flag its code needs goes in its build script (`cargo:rustc-cfg=...`).

## 2026-10-30 (0.2.279)

**Run `weft resync` in every project with a trigger on after this update.**
A trigger now reads as behind when anything in front of it changes (a node
whose value feeds one of its inputs or its gate), since that runs at its setup
and a resync is what runs it again. Before, only an edit to what its fires
run did. Every trigger that is on reads as behind once after the update,
because its fingerprint is now worked out over both.

**A new type, `Json`, holds any JSON value**: text, a number, a boolean, null,
or a list or object of those at any depth. Every type made of JSON fits into
it (`List[List[Number]]` does); a file, a stream or a connection never does,
and a `Json` value goes into anything narrower only through a `Cast`. A
`Route`'s `body` and a `Socket`'s `inbound` items can now be declared `Json`
(or `List[...]`) to take a body or a message that is a list or a bare value;
the default stays `JsonDict`. A request with no body (a `GET`, or a fire with
no `body`) now closes `body` on a `text` or `bytes` route too, where it used to
arrive as an empty text or an empty file, and a json body that is `null` is now
a value: it comes out on a `body` declared `Json` (or `... | Null`), and
closes `body` otherwise. A json route now refuses a body that does not fit
with the type's own message (`body does not fit JsonDict: ...`), and a fired
run's string body on a json route is a JSON string, held to the type like any
other. `LlmInferenceJson`'s `response` can be declared any JSON-made type, and
`HttpRequest`'s `body` too: declared `Json`, a JSON list comes out as the
list, and declared a type that takes no text (a record, `List[...]`), the
response is parsed and a wrong shape fails naming the field. `HttpRequest`
now refuses a response carrying a stored-file object.

**A `Cast` into `String` turns a list or an object into its JSON text.** The
build allowed it before, but the run then failed.

**An install remembers its named tunnel.** A `weft daemon start` or `restart`
from a shell without `WEFT_PUBLIC_TUNNEL_TOKEN` and
`WEFT_PUBLIC_TUNNEL_HOSTNAME` used to replace your stable address with a quick
tunnel; it now opens the named tunnel the install last ran. If a start says
the install's record has no hostname (one an older weft wrote), run
`./setup.sh --public-url` once from the checkout whose `.env.local` names the
tunnel. To go back to a quick tunnel, run `./setup.sh --no-public-url`, then
`./setup.sh --public-url`. Each named tunnel's container is made again once,
on the first start after the update.

## 2026-10-30 (7a1dcb43)

**A NUL character in a run no longer breaks search, or the runs beside it.**
Text a node picks up from a scraped page or a PDF can hold the invisible NUL
character, which Postgres refuses. One run carrying it made every later
`weft runs --search` and `--through` fail with `invalid byte sequence for
encoding "UTF8": 0x00`, and if it ended on an error holding one, the other
runs ending in the same batch were stopped with it. Now an error, a reason or
a search entry is written without the character, and a value that has to come
back exactly (a payload, an answer, a run's starting values, a trigger's
state) keeps it. An install that is already stuck searches again after
`./setup.sh`.

**The Postgres nodes read and write pgvector columns.** A `vector` or
`halfvec` column reads back as a list of numbers, and a list of numbers
writes into one. PostgresInsertRow and PostgresUpdateRows check that they can
read every column of the table back before they write, so a column they
cannot read (a `sparsevec`, for example) refuses the write instead of storing
the row and then failing. A `real` column now reads as `0.1` instead of
`0.10000000149011612`.

## 2026-10-29 (0.2.278)

**If a program hands tools to an LLM node, it may not compile after this
update.** The `tools` input of `LlmInference`, `LlmInferenceJson` and
`LlmStream` now takes a `List[LlmTool]`, and the `LlmTool` node hands out
that type. Three shapes need a change:

- A group that takes the tools as `tools: List[JsonDict]` gets a type
  mismatch: declare it `tools: List[LlmTool]`.
- One `LlmTool` wired straight into `tools` now goes through a `List`
  (`tools = List`, `tools.dog = dog.tool`, then `ask.tools = tools.list`).
- `ComputerTools` is gone: each computer action node now describes its own
  tools, and the premade computer agent gathers them itself. Drop the
  `tools = ComputerTools` and `agent.tools = tools.tools` lines; the agent
  offers Anthropic's names, and `calls.tools.openai` in a copy of
  `agent.weft` offers OpenAI's.

Then run `weft resync` in the project.

**A `?` on a group's input no longer compiles.** A group input passes what
reaches it straight to the nodes inside, so it was never optional itself, and
the `?` did nothing. The build now refuses it with
`optional-pass-through-port`: remove the `?` from the group's header (an
included file's too, and a loop's on any port the loop neither iterates nor
carries), and if a node inside should run without the value, put the `?` on
that node's input.

**A group can now describe itself on a metadata port.** Write `*tool:
LlmTool` in a group's or a loop's outputs and its value in the body
(`self.tool = { "name": "search", ... }`), or gather it from other metadata
ports (`self.tools = [...search.tools, fetch.tool]`). A wire from that port is
written into the input it points at before anything runs, so it may point
back up the graph without making a cycle. The new `Dispatch` node sends a
value to the branch that claims it: wire each tool group's `*tool` into a
`List` for the model's `tools` and into a `Dispatch` inside the loop over
the model's calls, and each call reaches the group that performs it. For
the whole picture, go and read [metadata
ports](https://weavemindai.github.io/weft/language/groups.html#metadata-ports).

## 2026-10-28 (341192de)

**The folders weft compiles in stop growing with every update.** Each
program weft compiles for a project now notes which compiled pieces and
source crates it used, and after every build weft keeps in its compile
folders (`~/.local/share/weft/worker-target/` and
`~/.local/share/weft/worker-crates/`) only what a program still on the
machine was built from. Before, every update left a full copy of weft's
libraries behind in each of them. The first project build after you update
clears out what older versions left, which can free tens of gigabytes. If
you build weft from source, `./setup.sh` now also removes from the
checkout's `target/` every compiled piece a newer build of it replaced, and
`WEFT_TARGET_CAP_GB` is gone.

## 2026-10-28 (0.2.277)

**If a project has its own nodes, or its own copy of the standard nodes, it
may not compile after this update.** weft moved to newer versions of the
libraries a node's code can use, and a node that asks for an older one no
longer fits. The project then stays down after `weft maintenance off`,
with the compile error. To bring it back:

- In the project, run `weft catalog update`, which refreshes the copy of
  the standard nodes under `nodes/base_catalog/` (any edit you made there
  is overwritten).
- In each of your own packages, change `reqwest = "0.12"` to `"0.13"`,
  `reqwest-middleware = "0.4"` to `"0.5"` and `minillmlib = "0.6.0"` to
  `"0.6.1"` in its `package.toml`, where it asks for them.
- A node that sends text over a socket writes
  `SocketMessage::text(value)` where it wrote
  `SocketMessage::Text(value)`.
- Then run `weft resync` in the project.

**If your cloud install keeps its database on Cloud SQL, the next run of
the install workflow restarts that database once.** Until now the workflow
set its `max_connections` to 250 whatever the tier. Now it leaves Cloud
SQL's own number for the tier (50 on `db-g1-small`) unless you set the
fork's `WEFT_CLOUD_SQL_MAX_CONNECTIONS`, and every copy of weft's
dispatcher and broker reads that number when it starts and takes an equal
share of it. If you want more copies running at once, there are two new
repository variables, `WEFT_DISPATCHER_MAX_COPIES` (2 when unset) and
`WEFT_BROKER_MAX_COPIES` (4 when unset); for how the share is worked out,
go and read [how many connections the install
takes](https://weavemindai.github.io/weft/running/cloud.html#how-many-connections-the-install-takes).

**If your cloud install runs infra nodes, stop and start each one after
the update** (`weft infra stop --infra <node>`, then `weft infra start
--infra <node>`). Their machines still run the agent of the weft that
started them, and the new weft cannot read its answers: until you do,
that node shows as failing with the same advice, and the rest of the
project carries on.

When weft's own database is busy, weft now waits instead of failing:
requests, runs and resyncs hold back and go on once a connection is free.
`weft status` says so with a `database: busy` line naming what to raise,
and the log says it once a minute. When the database cannot be reached
at all, the log says that rather than that it is busy.

If weft's own database disappears without closing its connections (its
machine stops, or the network to it breaks), weft now notices within about
a minute. Requests and runs waiting on it fail as "the database could not
be reached" and go on once it answers again. Until now a request could
hang for about fifteen minutes, and the connection that hears the
database's changes never noticed at all.

A Postgres infra node now sets Postgres up for its `memory` setting and
keeps the container to it, so a database short of memory has a process
killed instead of taking the machine down. When that happens, `weft
status` and the editor say so and to give it more memory. It now takes at
least 886Mi; a node set lower is refused when it starts. A query script
that should give up rather than wait on a lock can start with `SET
lock_timeout = '5s';`.

If you build weft from source, it now needs Node 22.12 or newer.

## 2026-10-27 (0.2.276)

**If you have nodes of your own, changed one of the standard library's,
build with `--referenced` or run live node tests, install Rust before you run
`./setup.sh`.** On your machine weft now compiles a project's worker, and a
live test's server, with your own Rust, and the install finds it on the
`PATH` you run `./setup.sh` with. Get it from [rustup.rs](https://rustup.rs/).
Those builds compile side by side in up to four compile caches of about 2GB
each under `~/.local/share/weft/worker-target/`, plus one more they all
start from, shared by every install on the machine. If you want more or fewer, set `WEFT_COMPILE_LANES` in the
shell you run `./setup.sh` and `weft` from.

**The install on your machine now asks for a key.** Until now any web page
open in your browser, and anyone else on your machine, could drive your
local install's API without a credential. Now it asks every caller for an
operator key, as a cloud install always has, and a web page can neither send
it one nor read its answers. `./setup.sh` gives the install a key of its
own. The CLI reads that key by itself and the editor extension asks the CLI
for it, so you never log in, and `weft login` refuses an install on this machine. If you set
the editor's `weft.dispatcherUrl` to another install on this machine, the
editor sends that install's key. If a script of yours calls the local API by hand, it now has to send
`Authorization: Bearer <key>`: for where the key lives, go and read [the key
the install's API asks
for](https://weavemindai.github.io/weft/running/architecture.html#the-key-the-installs-api-asks-for).
Routes, fire URLs, the browser extension and your frontends use their own
tokens and are not affected.

**After `weft maintenance off`, run `weft catalog update` and then `weft
resync` in each project's folder.** The `[system.build]` and
`[system.runtime]` tables of a node's `deps.toml` are gone, and a
`deps.toml` that still has one fails the whole build, naming the node type.
Each project's copy of the standard library (`nodes/base_catalog/`) still
has one, in `ExecPython`, so `weft maintenance off` cannot build those
projects and leaves them down. `weft catalog update` replaces the copy with
this weft's (any edit you made in there is lost), and `weft resync` builds
the project and brings back what maintenance took down. Add `--on <target>`
to the resync for a project on a cloud install.

**A node of your own that declares system packages has to move to
`[[needs]]` too.** What used to read

```toml
[system.runtime.apt]
default = ["python3"]
```

now reads

```toml
[[needs]]
name = "python3"
check = ["python3", "--version"]
[needs.install.apt]
default = ["python3"]
[needs.install.brew]
default = ["python"]
```

`check` is a command that exits 0 when the machine has it. If the package
needs something to compile (the old `[system.build]`), add `stage =
"build"`. If you had a `yum` entry, name it `dnf`. For the whole shape, go and read [what a node
needs from the
machine](https://weavemindai.github.io/weft/nodes/packaging.html#what-a-node-needs-from-the-machine).

**Every project's worker is built once more after the update**, because a
worker is now named by its node packages' names and contents rather than by
where they sit. The `weft resync` that brings a project back does that
build. A project with no nodes of its own (built without `--referenced`)
compiles nothing. On your machine it runs the standard worker that
`./setup.sh` downloads with the release. On a branch, or a commit with no
release, `./setup.sh` builds the CLI from source, and the install then
compiles the standard worker once when it starts, with the Rust that build
already needs. On a cloud install all such projects share one image, built
once, unless the project sets `[build.worker]`. That setting
(`base_image`, `dockerfile_template`) now shapes a cloud install's images
only: a build on your machine ignores it.

**Your programs run on your machine, not in a container.** On a local
install a project's worker is now a plain process in the project's folder,
so its nodes can open your files and start the programs you have installed.
They run as you, so a project can read and change whatever you can. A
cloud install keeps running workers as containers. On your machine,
infrastructure nodes, the database and the file store still run in
Docker. The update removes the worker containers and images the old weft
left in Docker, and from then on `./setup.sh` also removes the worker
programs nothing uses any more, an older weft's standard worker among them.
A worker's log (under `workers/logs/` in the install's folder) is deleted
when the worker stops; if one dies while it runs, its last lines go to `weft
daemon logs`.

**New: `weft deps-install`.** When a node your program uses needs a program
your machine does not have, `weft run`, `weft activate` and `weft resync`
now refuse before anything starts and name the command to run, like `weft
deps-install ExecPython`. It shows the install command for your system and
runs it once you say yes. For its flags, go and read [the
CLI](https://weavemindai.github.io/weft/running/cli.html#what-nodes-need-from-your-machine).

**New: `FolderWatch`.** A trigger that starts a run each time a file is
created, changed or removed in a folder on your machine, with the file's
path. It works on a local install only. What happens in the folder while
weft is stopped or in maintenance fires once the trigger is back, and what
happens while you have the trigger down behind a door that refuses never
does. For how it behaves, go and read [watching a
folder](https://weavemindai.github.io/weft/language/triggers-and-routes.html#watching-a-folder).

**A poll trigger brought back from behind a door that refuses, or after its
`--door-for` window ended, now starts from wherever its feed is.** Before,
it delivered everything the feed had queued since its cursor. If weft itself
was stopped or in maintenance, the trigger still delivers what its feed
queued meanwhile once it is back.

**On a cloud install, the operator key the install workflow gave you now
lives in a secret named `<name>-install-key`.** The install workflow renames
it by itself and the key stays the same, so nothing changes for you unless a
script of yours reads the old `<name>-bootstrap-operator-key` secret by name.
The key is the install's own, read from that secret, so `weft token revoke`
cannot retire it. `weft token ls` still lists a key named `bootstrap`: that
is the row an older weft wrote for the same key. Revoke it if you want the
list to hold only keys you minted; the install's key keeps working through
its secret either way.

## 2026-10-26 (caf76429)

**`weft maintenance off` is faster and shows what it is doing.** It used to
bring projects back one after another, printing one line and then nothing
until the end, even when a project's images took many minutes to build. Now
every project comes back at the same time, and so do the infra copies and
triggers of different instances in one project. While it runs you see a line
per project with its step and a clock, plus where each image's build log is.
Each project's result prints as soon as that project is done. On a local install, the build log now
fills while the build runs, so `tail -f` on it works and the spinner shows the
latest line. `weft maintenance on` also shows a spinner while it takes things
down. Nothing to do on your side.

## 2026-10-26 (a8eef1b0)

**`weft target export` checks a large program's connections again.** On a
program with many included files, the check that every connection is picked
on the target failed with `length limit exceeded`, printed as if there were
connections to fix. The check now runs on your machine against what the
install keeps, so no size of program stops it, and a check that cannot run
says so plainly instead of listing gaps.

## 2026-10-25 (c2d4bc11)

**A domain you give a cloud install becomes its address.** Once Google has
issued the domain's certificate, every link the install hands out uses the
domain instead of the `run.app` address: sign-in callbacks, webhook
addresses, file links, `weft target export`. The `run.app` address keeps
answering, so nothing already handed out breaks. If you already have an
install domain, run `weft domain list --on <target>` once after the update:
it checks the certificate, and the domain becomes the address there and
then. Then add `https://<your domain>/access/oauth/callback` to the allowed
redirect URLs of each provider app you sign in through, and run `weft target
export` again for any site that talks to the install. `weft domain list`
shows where each certificate stands and prints those steps.

**New: `weft migrate connections` and `weft migrate files`.** They copy saved
connections and stored files from one install to another (`--to <target>`),
so moving an app from your machine to the cloud no longer means making every
connection again. Name what to copy with `--all`, `--id`, `--since`/`--until`,
`--service`/`--project` or `--prefix`; `--dry-run` shows it first.

**New: a cloud install can have a Postgres of its own on Cloud SQL.** Set
the fork's repository variable `WEFT_DATABASE` to `cloud_sql` and the install
workflow makes one in the install's region and network, for a busy program
that would run past a hosted free plan. It bills every hour it runs. An
install already on another Postgres moves to it with its data: run `weft
maintenance on`, set the variable, run the workflow, then `weft maintenance
off`. Without the variable, nothing changes.

## 2026-10-25 (ccabce57)

**On a cloud install, rebuild your CLI before `weft maintenance off`.** What
`off` answers changed shape, and a CLI from before this release cannot read
it: run `./setup.sh --cli` from the new commit first, then `weft maintenance
off --on <target>`. The cloud upgrade steps now say so in that order. On your
machine, `./setup.sh` already does this.

**Something that cannot come back no longer keeps the install in
maintenance.** `weft maintenance off` now ends the maintenance once it has
tried every project, even when a project's build, infra or triggers fail, and
it exits successfully. Everything else comes back, and `off` names what did
not and why. If the message says the project's copy of the standard nodes is
older than this weft, run `weft catalog update` in its folder (it replaces
`nodes/base_catalog/`, so any edits you made in there are lost); otherwise
fix what it says. Then `weft resync` there brings back what the update took
down and says what it brought back. If you want some of those triggers left
off, run `weft deactivate` on them before the resync. In `weft maintenance
off --json`, the `lifted` field is gone, because a report now always means
the maintenance is over; a project whose `failed` list is not empty still has
down what the maintenance could not bring back in it (everything it took down
there, when the entry is its build). `weft resync --json` says what it brought back in
`broughtBack`.

**Saving a file and running a command at once no longer fails with a 409.**
On a local install the runtime uploads a saved file while the command you run
next uploads the same bytes, and the second one could be refused with
`already exists (completing)`. It now waits for the other upload to land and
carries on. A folder deleted or moved just as a sync ends is also no longer
made again as an empty one.

**Running pytest in a node's folder no longer rebuilds its images.**
`.pytest_cache/` is now left out of versions and of what decides whether an
image builds again.

**A build checks every file it takes from the install's cache.** A copy that
does not match the version is fetched again. Before, a copy damaged on disk
could give the same version a different image, and that build failed.

**Images are named after the version's file fingerprints.** A build from a
version names its worker and infra images from the fingerprints the version
already stores, so one version always gives the same images. Because the
names changed form, every image builds once more after this update, and a
node package's recorded live test passes count as out of date once.

## 2026-10-24 (0.2.274)

**Update the CLI and the install together.** The requests and answers between
them changed shape, so a CLI and an install from either side of this update do
not understand each other. `./setup.sh` updates both on your machine; for a
cloud install, run its install workflow as well. `./setup.sh` migrates the
database: each project's head moves to the copy that programs write into, and
the first command you run in your folder starts it from that head.

**Your folder is now a copy the install keeps in step with its own.** Every
command that builds, and `weft tree`, `checkpoint`, `branch` and `freeze`,
syncs the folder first and says what it moved. On your machine, the runtime
also watches every folder you have run one of those in: a save reaches the
install about a second later, and when a program writes into your project,
the files land in your folder. On a cloud install the sync before a command
is the only one. If a file changed both in your folder and in the install,
your file is kept and the install's is written beside it as
`<stem>.weft-conflict-<8 characters>.<ext>`, which is never compiled and never
in a version. Builds and runs go on with your file. If you want the install's
version, run `weft resolve <file> --keep incoming`; if you want yours, `weft
resolve <file> --keep folder` deletes the conflict copy. On your machine, a
project on a Windows drive under WSL (`/mnt/c/...`) is refused, since WSL tells Linux
nothing about changes there; move it under `~/`. For the whole story, go and
read [your folder is a copy](docs/src/running/versions.md#your-folder-is-a-copy).

**A version is recorded when you do something with the code** (a run, a
build, an activate, a bake, a resync, an infra start or upgrade, a
checkpoint), never on a save, and recording the same files again gives the
version they already are. `weft build` now adds to the version tree too.
`layouts/` is now part of every version, so going back to one puts the
graph's boxes where they were.

**`weft branch` no longer refuses when your folder holds changes no version
records, and `--discard` is gone.** It records what your folder holds as a
version first, moves the folder, writes the files, and prints the
`weft branch <id>` that comes back. In the editor's **Runs** view (by
version), **Load this version** on a version or a run does the same. Each
folder now has its own head, and `weft tree` lists every working copy with
where its head is and the conflict copies it holds. `weft tree --json` no longer
has `head` or `diskVersion`: `activatedVersions` is now at the top level,
`workingCopies` lists every working copy (each with `headVersion`, `headRun`
and `workingVersion`), and `workingCopy` is the id of this folder's. That
copy's `workingVersion` is always set: it equals `headVersion` when nothing
changed since, and otherwise is the id the files would have as a version,
which may not be recorded yet (in that case `diskVersion` used to be `null`).

**A command that builds no longer compiles anything itself.** The install
compiles the version it recorded, so a compile error a build prints comes
from it. `weft run --check` still compiles on your machine. `@asset` is
resolved by the install too: a file the version holds is used as it is, and a
file outside it (an absolute path, `~/...`) is read off your machine by a
local install and refused by a cloud one, which tells you to move it under
`assets/`.

**A program can now do to any project of its owner what you do with the
CLI**: write its files (`ctx.project(id).code()`), checkpoint, switch, build
and run it, turn its triggers on, bake, resync, start, stop and upgrade its
infra, read and stop its runs, and change its instances, tokens, frontends
and workers, with the same refusals the CLI gets. It cannot start a run of its own project, resync
it or remove it, and each refusal says why (a run's refusal comes before anything is
recorded as a version or built). If it copies
files the owner already holds, such as another project's standard library,
`edit().blob(path, hash)` names each by the hash `state()` gave, so the copy
is one write with nothing downloaded. For every call, go and read [the
ctx](docs/src/nodes/ctx.md#your-project-and-the-owners-other-projects).

**If `weft prune` refuses because of a working copy's head**, the refusal now
names both ways out. `weft branch <version> --copy <copy>` moves another
working copy to a version outside that history, and `weft forget-copy <copy>`
records its files as a version and removes it, where `<copy>` is a folder's
absolute path or `program`. This update turns each project's head into a
program copy, so a prune of the history holding that head names `program`,
and your folder too once you have run a command in it, since a folder starts
on the head of the copy that changed last. Running `weft branch <version>` in the folder and `weft
forget-copy program` clears both. A program can do the same with
`ctx.project(project).forget_working_copy(copy)`, `copy` being a working
copy's id. A frozen example's refusal now names `frozenFrom`, the key as the
example file spells it. For the details,
go and read [cleaning up](docs/src/running/versions.md#cleaning-up).

**If a node of yours calls `ctx.node_state(run, node)`**, it is now
`ctx.run_by_id(run).node_state(node)`. The standard library's
`GetNodeState` changed the same way, so if your project keeps its own copy of
the catalog (`nodes/base_catalog`), run `weft catalog update`.

**Run `weft tangle update` in each project**, so Tangle knows how `weft
branch` works now and what a node can do to other projects.

## 2026-10-24 (0.2.273)

**Updating weft now happens in maintenance.** Run `weft maintenance on`, which
takes down every trigger that is on and stops your infra, then update
(`./setup.sh`, or the cloud's install workflow), then `weft maintenance off`,
which builds those projects again on the new weft, starts that infra again on
the new build and then brings back exactly those triggers. While the install
is in maintenance, what arrives for the triggers it took down is refused (an
event is dropped, a live call is answered `503`), and what they held before
is kept and runs after `weft maintenance off`. Add
`--on <target>` for a cloud install. `./setup.sh` and the workflow now refuse
to replace a weft that has something on, naming these three steps.

**This one update is different**, because the weft you update from has no
`weft maintenance` yet. Just update (`./setup.sh`, or the workflow), then in
each project that is switched on, run, adding `--on <target>` for a cloud
install:

1. `weft catalog update`, if the project keeps its own copy of the catalog
   (`nodes/base_catalog`);
2. `weft infra upgrade --all`, if it has infra;
3. `weft resync --all`.

From the next update on, `weft maintenance off` rebuilds the projects and
brings back their infra and triggers by itself.

The catalog copy needs updating because the nodes that check signatures changed
shape, so the old copy of them no longer loads, and `weft validate` tells you
when the copy is stale. Until you resync, the worker the previous weft built
keeps running, and this weft no longer reads some of what it sends. That
worker answers its callers with its own error, `503 the caller could not be
checked; try again in a moment`; the install's log names the project and says
to run `weft resync`, and so does the worker's log. `weft status` shows which
projects are behind.

`weft resync` now checks, before it takes any trigger down, that the new build
can set up and fire every trigger it brings back. If one needs a node that
does not load (an old catalog copy, say), it takes nothing down and prints the
loader's message, which says what to run. If a resync ever stops because its
build cannot run its triggers, it stops at once rather than trying again, and
tells you what works: `weft resync --resume` would reuse the same build, so
fix the source, then run `weft cancel-activate` and `weft activate`.

**`weft status` shows your running infra as `behind the source` after this
update.** weft's own engine runs each infra node's setup, so a new weft can
change what that setup writes, and a copy set up by the previous one is behind
until it is set up again. `weft infra upgrade` does that, taking down the
triggers that depend on the copy while it restarts. Until you run it, the copy
keeps running as it was.

**If a route of yours is gated by `JwtAuth`:** that node is gone. Gate the
route with an access node of your own whose `verify` block is `oidc`;
[Verifying requests](docs/src/connections/verifying-requests.md) shows one. Its
recipe declares a required `audience` field, and its `jwksUrl` must be
`https`.

**If a node of your own checks webhook signatures:** the `verify` block has a
new shape (`signature`, `timestamp`, `secret`), and the old keys
(`signatureHeader`, `timestampHeader`, `prefix`, `packed`) are refused with the
block printed back in the new shape. Connections you already have are carried
over on their own. The new shape can sign any header or body field, decode a
`whsec_` secret, read several signatures, and check SHA-512, so schemes like
Whop's, Svix's and Resend's can be expressed. The full reference is
[Verifying requests](docs/src/connections/verifying-requests.md).

**If you write an access node with OAuth:** a recipe can now rename the app's
id and secret on the wire (`clientParams`), send `extraParams` on every token
call, read its token, expiry, scopes and refusal from wherever the provider
puts them, and run one more call right after the code exchange
(`afterExchange`). A recipe whose token can be renewed but whose answer says
nothing about when it expires is refused at connect, instead of dying silently
an hour later.

**The first start after this update takes a little longer.** weft now keeps a
version on every JSON document it stores, and this start carries every older
row to today's shape, printing its progress. Nothing to do on your side. A
weft older than this one refuses to start on a database this one has
upgraded, and says so.

`weft events --node`, `weft logs`, `weft wake` and `weft freeze --expect` now
read a node's name in the program the run actually ran, so a node you have
moved since is still found by its old name on an old run. `weft events
--iteration` now needs `--node`, and `weft wake --iteration` picks one
iteration when several wait at the same node. `weft runs --through` finds
nodes inside an included file by the name you write (`one.strip`).

A project opened through a symlinked folder (on macOS, anything under
`/var`) gave the nodes of an included file a different name than the build
did, so `weft events --node` found nothing for them. That is fixed.

A connection you do not name is named after its app, or its service, and
`weft connect --list` shows both the account and the name.

Saved examples (`examples/*.json`) now carry a `specVersion`. An older file
still reads as it is, and takes today's shape the next time weft writes it
(`weft run --save`, `weft freeze`).

Tangle now shapes a program you will keep growing (more channels, sources or
customers) around one general shape that each case plugs into, rather than a
copy per case, and tells you where the next case will go. It also knows the
sign-in keys a service you connect through your own node can declare. Run
`weft tangle update` in each project to get it.

## 2026-10-22 (0.2.272)

weft installs with one line: `curl -fsSL https://weft.weavemind.ai/install.sh | bash`
puts it in `~/weft` and runs the installer, and running the line again updates
it. An existing checkout keeps working as before with `./setup.sh`.

`./setup.sh` no longer scrolls Docker and cargo output past you: each long step
shows a spinner with the time so far and the last line it printed, and a step
that fails shows the end of its output and the file holding all of it.
`--verbose` brings the full output back.

`weft new` without `--assistant` now asks which coding assistant you use (the
ones found on your machine first) and offers to remember it, then prints the
command that opens the project. Without a terminal to ask on, it refuses and
names the flag; `--assistant none` is how to make a project with no Tangle.
`--assistant none --remember` now clears your saved default instead of saving
"none", so the next `weft new` asks again.

## 2026-10-22 (32351dec)

Tangle keeps its messages short and plain while it works: a sentence or two
on what it is doing, with no node names, SQL or file paths unless you asked
for them, and its end-of-work report says what the program now does for you
instead of walking through its inside. An existing project gets this with
`weft tangle update`.

## 2026-10-21 (0.2.271)

Every project's worker is rebuilt once, the first time it starts after this
update: a fix changed how a program's code is fingerprinted. That fix is for
an `@include` whose file does not compile. Re-running part of such a program
from frozen values used to fail with `missing implementation
'IncludedGroup'`; it now runs.

If you wrote a node of type `LoopIn`, `LoopOut` or `IncludedGroup` by hand, or
named a node or group `IncludedGroup`, the compiler now refuses it. Those are
the nodes weft makes itself, at a loop's edge and where an include sits, and
the message says what to write instead: a `Loop`, or `name =
@include("file.weft")`.

In the editor, a node you add, paste or duplicate is named after its type with
no number first (`text`, then `text2`), and every name it makes up is
camelCase, the same as the compiler expects.

`weft resync`, `weft deactivate` and `weft rm` follow their task with a new request, so
update the CLI and the runtime together, which `./setup.sh` does.

## 2026-10-21 (5eafc84b)

If you are on a Mac, `weft test-node` works again. Since 2026-09-19 it failed
before any test ran, with `no method named set_tcp_user_timeout` in
`weft-engine`. Nothing to do beyond `./setup.sh`.

Tangle now copies a catalog node into your project's `nodes/` and changes the
copy when the node is close to what you need but misses something (a Google
permission, a parameter, a slightly different behavior), instead of telling
you weft cannot do it. Run `weft tangle update` in each project to get it.

## 2026-10-20 (0.2.270)

**This release wipes your install's database, so connect your services again
afterwards. Your projects then need changes: an execution is now a run, every
name follows one casing rule, and several nodes and CLI flags changed. Work
through the lists below in order.**

When you run `./setup.sh`, it finds that your install's database was built
before this release and asks before wiping it. Your run history, versions,
stored connections, tokens and instance values go, with the install's stored
files and its projects' running infra. Your project folders stay, and `weft run` registers
them again. Other installs on the machine are not touched until you run
`./setup.sh` in their checkout. If there is no terminal to ask on, run
`./setup.sh --yes`. If you answer no, nothing is installed. If you only want the
CLI (`--no-daemon`, or `--cli` without `--daemon`), that is refused on such a
database, since it would put the new CLI in front of the old runtime.

If you run weft on Google Cloud, `./setup.sh` cannot wipe that database, and
the runtime refuses to start on one built before this release. Put the address
of an empty database in the `WEFT_DATABASE_URL` secret (and
`WEFT_DATABASE_LISTEN_URL`, if you use it), then merge upstream into your fork
and run the install workflow again, which also lets the files bucket answer
range requests and page uploads. Then run `./setup.sh` in your weft checkout
on that same commit, so your CLI matches the install. Everything else the
install kept went with the old database: operator keys apart from the first
one, tokens, domains, frontends, and every project's registration. So log in
again with the first operator key (go and read [run the
workflow](docs/src/running/cloud.md#run-the-workflow)), add your domains and
frontends again, run `weft target export <target> --github` in each project,
and run `weft infra start --on <target>` and `weft activate --on <target>` in
each one once.

What to rename in your projects and files (each is refused, mostly with an
error naming the new spelling, until you do):

- In the CLI, `weft executions` is `weft runs`, and every `<execution-id>`
  argument is a run id.
- If a program wires a node's `error` output, rename it `_error`. Every node
  has `_error` now: wired, the node's failure comes out on it and the run goes
  on; unwired, the failure fails the run. For what goes there and what does
  not, go and read [when a node fails](docs/src/language/how-a-program-runs.md#when-a-node-fails).
- `AnthropicProvider`, `OpenAIProvider` and `OpenRouterProvider` are each two
  nodes now: an Access node holding the connection (`openrouter =
  OpenRouterAccess`) and a Model node taking its `access` and a `model`
  (`OpenRouterModel { access: openrouter.access, model: "z-ai/glm-5.3"
  }.provider`). One Access feeds every Model of that service.
- `parseJson` is gone from `LlmInference`, whose `response` is now always
  text. If you want parsed JSON, use `LlmInferenceJson`, and declare the shape
  you expect on it (`LlmInferenceJson -> (response: Verdict)`): an answer that
  does not fit fails the node. Go and read [saying what a broad output
  holds](docs/src/language/types.md#saying-what-a-broad-output-holds).
- `LlmModerate` takes `access` (an OpenAI Access node's) in place of
  `provider`.
- A `Route` gives the request's body whole on one `body` output. If you
  declared body fields as outputs (`Route -> (body: JsonDict, photo: File)`),
  declare them inside `body` (`Route -> (body: { photo: File })`) and read
  `post.body.photo`. Every other output you add must be a capture of the
  path, typed `String`: the `params` output is gone, so for `cards/{id}` you
  declare `-> (id: String)`. A body that does not fit the type you declared,
  or carries a key it does not name, is answered `400` (end the type with `..`
  to take extra keys). Go and read [answering on a
  URL](docs/src/language/triggers-and-routes.md#answering-on-a-url).
- `FetchToStorage`, `KeepFile` and `TextToFile` take `ttlDays` (it was
  `ttl_days`), and a loop's settings are `maxIters` and `trimOnMismatch`.
- `MustOverride` is gone. Where you typed a port with it, write a type
  variable (`T`) and the port takes what is wired to it.
- A `?` on an input the node requires is a compile error, and a node whose
  wired inputs all carry `?` is refused with `run-undecided` until you gate it
  with `_should_flow`, drop one `?`, or add `@require_one_of`.
- A group port that feeds a wired input inside must be connected, even when
  that input has a default (`required-port-unmet`).
- `DeactivateTriggers`, `StopInfra` and `TerminateInfra` lose `triggers` and
  `graceMinutes` and take `door`, `doorForMinutes` and `held` instead, with the
  same meaning as the CLI flags below.
- These nodes now hand back their result in a different form:
  - `S3ListObjects` reads one page (it read every page): wire `lastKey` into
    the next call's `startAfter` while `hasMore` is true.
    `GoogleDriveListFiles` and `GoogleCalendarListEvents` page the same way
    with `nextPageToken` and `pageToken`, 25 by default, and
    `GoogleSheetsRead` and `GoogleSheetsLookup` with `offset`, `maxRows` and
    `nextOffset`.
  - `CrawlSite`'s `pages` is one stored file, a JSON line per page, and
    `MistralParseDocument`'s `markdown` and `pages` are files too.
  - `GmailGet`, `GmailNewEmail` and `FetchPage` cut a long text and say so on
    `bodyTruncated` or `truncated`.
  - `WebSearch`'s `results`, `GoogleCalendarListEvents`' `events`,
    `S3ListObjects`' `objects` and `InstanceCosts`' `records` are typed
    records, so reading a field they do not declare is refused when you build.
- A saved `examples/<name>.json` that uses an old spelling (`frozen_from`,
  `gave_up`, `keep_for`) is refused, and the error names each new spelling.
- In `access-apps.json` (on a cloud install, the `WEFT_ACCESS_APPS` secret),
  `auth_url` is `authUrl`, `token_url` is `tokenUrl`, and under `events`,
  `signing_secret` is `signingSecret`, `public_key` is `publicKey` and
  `push_email` is `pushEmail`; the `_setup_events` note is `_setupEvents`.
  `client_id` and `client_secret` keep OAuth's spelling. Until the file is
  renamed, its shared connections stop working, and the error names each old
  key, where it is, and its new spelling.
- Run `weft tangle update` in each project, so its assistant learns all of
  this.

What to change in the nodes you wrote:

- In a node's `metadata.json`, every key is camelCase: `requires_infra` is
  `requiresInfra`, `free_text` is `freeText`, and so on. For the copy of the
  standard nodes under `nodes/base_catalog`, run `weft catalog update` in the
  project. A node using one of these keys fails to load, and the error says
  what to do:
  - `features.catchErrors`: remove it, since every node has `_error`. A node
    that declares an output called `_error` is refused.
  - `features.optionalCustomInputs` is `features.join`.
  - `requiredWhenWired`: remove it. Any optional input is required once wired
    now (below).
- If your node declared a `journalEphemeral` input so its users choose how its
  bus is recorded, remove it and set `features.journalSettings`, which gives
  the node `journalEphemeral` and `journalWindowMs` (read with
  `ctx.journal_policy()`). Without it, the input is an ordinary one and the bus
  keeps the default. A trigger with `liveConnection` gets both already, and
  declaring them there is refused.
- `MustOverride` is gone here too: type the port with a variable, and if you
  want to limit what it may be, give it a bound under `typeParams`. Go and
  read [typeParams](docs/src/nodes/metadata.md#typeparams).
- In Rust, `ExecutionContext` is `RunContext`, `ctx.execution_id` is
  `ctx.run_id`, and `ctx.tag_execution` is `ctx.tag_run`.
- On a storage handle, `presign`, `public_link`, `caller_link` and
  `external_url` are gone. Use `internal_link` for code inside the install,
  `external_link` for anything outside it (both answer `{ url, public }`), and
  `external_file` to send a file to a service. A file input no longer comes
  with a link already made. Go and read [handing a file
  out](docs/src/nodes/storage.md#handing-a-file-out).
- In an `ExecPython` script, a file's `url` is gone: fetch the bytes from
  `photo['internalUrl']`, and hand another service `photo['externalUrl']`
  when `photo['public']` is true.
- A package's `build.rs` now runs for the package's own library crate. Its
  `cargo:rustc-link-arg` lines no longer reach the worker binary, so name a
  native library with `rustc-link-lib` and `rustc-link-search` instead, and a
  `[build-dependencies]` section in a package with no `build.rs` at its root
  fails the build. Go and read [a build script](docs/src/nodes/packaging.md#a-build-script).
- A bus ends finished only when the node's body returned `Ok`; any other end
  closes it failed, and its readers are told. If your node opens a bus later
  than it starts, use `ctx.pending_bus`.

What to change in your commands and scripts:

- `weft deactivate`, `weft resync` and `weft infra stop`, `terminate` and
  `upgrade` no longer take `--mode`, `--grace`, `--running-policy` or
  `--drain-timeout`. Each answers three questions instead: `--door
  take|hide|refuse` (with `--door-for <minutes>`), `--held keep|drop` and
  `--running wait|cancel`. `--mode park` is `--door take`, `--mode hibernate
  --grace 30` is `--door hide --door-for 30`, and `--mode wipe` is `--door
  refuse --held drop --running cancel`. `--running wait` has no cap any more,
  so `--drain-timeout` has nothing to replace it. Go and read [taking triggers
  down](docs/src/running/lifecycles.md#taking-triggers-down-three-questions).
- `weft activate`, `weft bake` and `weft infra start` no longer take
  `--running-policy` or `--drain-timeout`: drop them.
- They never ask now, and with no flags they lose nothing: what arrives is
  held, what was held is kept, and the command waits for the runs going to
  finish. Before, off a terminal `weft deactivate` and `weft resync` wiped
  everything and cancelled the runs, so if a script relied on that, pass
  `--door refuse --held drop --running cancel`.
- `weft activate --reactivate-choice` is `--held run|drop`:
  `execute_parked_keep_suspended` is `--held run`, the default, which releases
  what the triggers held while they were down, paced at half each trigger's
  limits, and `wipe_all` is `--held drop`, which deletes it and cancels the
  runs paused on it. `keep_suspended_only` has no equivalent.
- A plain `weft resync` now moves only the program's own triggers. If you
  want every instance's too, as it did before, pass `--all`.
- `weft infra upgrade` now replaces only the infra that is running, and
  starts nothing that is stopped. If you want stopped infra up too, run `weft
  infra start` first.
- `weft cancel-running` is gone. A take-down that waits on runs now carries on
  on the install: Ctrl+C stops following it, and `weft stop <run>` ends one run
  now.
- `weft infra node-stop <node>` is `weft infra stop --infra <node>`, and
  `node-terminate` is `weft infra terminate --infra <node>`.
- `weft run` exits `10` when the run it follows fails or is cancelled, and
  `0` only when it completed.
- In `weft run --from node='{...}'`, a port you hand a value now takes that
  value and no wire into it runs. Before, the value was only a backup, and a
  wire's value won.
- If a script reads a route's failure: a caller whose run ends before
  answering now gets `{"error": "Something went wrong", "runId": "..."}`, not
  the reason. Every answer carries the run's id in a `Weft-Run-Id` header, and
  `weft logs <run id>` shows the reason. Go and read [when a run ends before
  it answers](docs/src/language/triggers-and-routes.md#when-a-run-ends-before-it-answers).
- If a script reads weft's JSON (`--json`, the API), the fields are
  camelCase now: `runId`, not `execution_id`. Values from a fixed list stay
  snake_case (`waiting_for_input`).
- If a project deploys with the workflow `weft ci add` wrote, run `weft ci
  add --cloud gcp` in it again and commit the new
  `.github/workflows/deploy.yml`: the old one passes `--mode park` and
  `--reactivate-choice`, so it fails. If you edited the file, weft leaves it
  alone and says so: move your changes out, delete it, run the command, and
  put them back. The new workflow runs `weft infra upgrade --all`, then `weft
  resync --all`, and never turns anything on, so before a project's first
  deploy run `weft infra start --on <target>` and `weft activate --on
  <target>` once by hand. Go and read [deploy the
  frontend](docs/src/running/cloud.md#deploy-the-frontend).

What behaves differently with no error to tell you:

- An optional input that is wired now waits for its value, and the node skips
  if that wire closes. If you want the node to run without it, write `?` on
  it where you use the node (`SlackSendMessage(threadTs?: String)`). Go
  and read [optional and required
  inputs](docs/src/language/syntax.md#optional-and-required-inputs).
- A group, or an included file, opens on the first value that reaches it
  instead of waiting for all its inputs, so a node inside that nothing feeds
  starts at once. With no gate, it skips when every wire into it closes with
  nothing on it. Go and read [when a group
  starts](docs/src/language/groups.md#when-a-group-starts).
- An LLM call sends only the settings you set. Before, an unset temperature
  went out as `0.7`, and an unset reasoning as a request for none. Now the
  provider's own defaults apply, so a model that reasons by default reasons.
  Set `reasoning: false` on `LlmParams` if you want it off.
- When an infra copy turns `flaky`, the health check takes down the triggers
  whose runs reach it, holding what arrives and cancelling the runs that
  reach it, and brings them back once it is healthy. If you want other rules,
  write them on the node's `InfraSpec`. Go and read [when a unit stops being
  ready](docs/src/nodes/infrastructure.md#when-a-unit-stops-being-ready).
- A json `Socket` with no type declared on `inbound` takes only JSON objects.
  Any other message is answered `{"error": "..."}` and the session goes on.
- Every file link is a download now: opened in a tab, it saves the file under
  its name, while an `<img>`, `<video>` or `<audio>` tag still shows it.
- In the editor, the Specs menu, **Run from a spec…**, **Run from here…** and
  **Run this group** are gone. For starting partway in, one group, or a saved
  example, use `weft run --from`, `--group` or the example's name. Go and read
  [run part of the graph](docs/src/build/the-graph.md#run-part-of-the-graph).

What is new:

- One bottom bar in the editor: select boxes and **RUN N TARGETS** runs them,
  greyed out with weft's reason when the run cannot start. Select a trigger or
  an infra node and the bar offers its own buttons. `weft run --check` gives
  the same answer in a terminal.
- `weft wait <run>` waits for a run to end and says how in its exit code.
  `weft node-state <run> <node>` and the `GetNodeState` node read where one
  node of a run stands, and `GetRunId` gives a node its own run's id. `weft instances` lists a project's
  instances. Go and read [waiting on a run](docs/src/running/cli.md#waiting-on-a-run).
- A visitor of your site can upload a file of any size through an upload slot
  (the `UploadSlot` node, `upload_slot` on a storage handle in Rust,
  `uploadFile` in `@weft/connect`).
  Go and read [a file from a page's
  visitor](docs/src/nodes/storage.md#a-file-from-a-pages-visitor).
- A file link stays the same for a while, so a page that asks again keeps its
  video playing, and it answers range requests, so a video seeks. The
  `LinkOptions` node sets how long links to a file live, `DeleteFile` deletes
  one, and `Reply` can answer a file with a redirect (`answerAs: "redirect"`).
- `Route` and `Socket` take `whenCallerIsSlow` and `whenChunkFails`, and
  `journalWindowMs` beside `journalEphemeral`. Go and read [slow
  callers and messages that cannot be
  made](docs/src/language/triggers-and-routes.md#slow-callers-and-messages-that-cannot-be-made).
- A `.weft` file that does not compile, or a node package that does not
  compile, no longer stops everything: a command refuses only when what it
  needs uses the broken part, and names it. The worker is built without a
  package that fails. Go and read [when a package does not
  compile](docs/src/nodes/packaging.md#when-a-package-does-not-compile).
- Editing a file while a command builds no longer fails it.
- Each trigger runs its own cut of the program, and `weft resync` moves only
  the triggers whose cut or code changed.
  `--all` reaches the program's triggers and every instance's at once.
- The `Computer` package gives a program a desktop, a terminal and a browser
  in one container, with a node per action; include its agent loop with
  `@include("__catalog/base_catalog/computer/agent.weft")`. `TextPreview` shows the start and end of a long
  text.
- If you want phones and other devices on your network to open your install
  and its file links, start it with `WEFT_NETWORK_ADDRESS` set to this
  machine's address on that network; go and read [devices on your own
  network](docs/src/build/public-address.md#devices-on-your-own-network).
- `weft infra env --door` writes where an infra node answers into an env
  file.
- ElevenLabs images and videos are priced, and `InstanceCosts` takes a `run`
  and counts the calls it has no price for.
- A new named install takes its ports from 15000 to 32767; one that exists
  keeps its ports.
- The compiler warns `not-camel-case` on a node, group or port name you
  chose that is not camelCase, with the spelling to use. The program still
  runs.

If you work on weft itself:

- Integration tests live in one binary per crate: add a test as a module in
  `tests/integration/`, or in `tests/db/` if it needs Postgres. The
  `db-tests` feature is gone; `scripts/run-db-tests.sh` runs those through
  nextest's `db` profile.
- If you add a dependency or change its features, run `cargo hakari generate
  && cargo hakari manage-deps`. CI fails if `crates/weft-workspace-hack` is out
  of date.
- If you want only the tests your change can break, keep `--workspace` and
  filter: `cargo nextest run --workspace -E 'rdeps(weft-compiler)'`. Building
  with `-p` compiles a second copy of weft's crates. Go and read
  [tests](CONTRIBUTING.md#tests).
- `./setup.sh` never wipes `target/`: past 30 GB it drops incremental
  directories, then executables a newer build replaced. Removing an install
  removes its images.

## 2026-10-13 (0.2.269)

**Each branch of a checkout can now run its own install. If your checkout is
not on `main`, or you keep a public tunnel's token in `.env`, there is
something to do.**

If you run weft from a checkout on a branch other than `main`, it now uses
that branch's own install. weft stops with an error until you run
`./setup.sh` there, and the install it sets up starts empty. Your projects
stay on your default install. If you want to keep working with them, check
out `main` or set `WEFT_INSTALL=default`.

If you keep `WEFT_PUBLIC_TUNNEL_TOKEN` and `WEFT_PUBLIC_TUNNEL_HOSTNAME` in
`.env`, move both lines into a `.env.local` beside it, then run `./setup.sh`
again. Every checkout made with `scripts/add-worktree.sh` links to that same
`.env`, so a branch you run `./setup.sh --public-url` on would try to open the
same tunnel, and weft refuses to start a second install on a token
another install already uses. `.env.local` belongs to that checkout alone, and
what it sets wins over `.env`.

What you will notice:

- A branch's install lives under `~/.local/share/weft/installs/`, with its
  own runtime, database and files. For how weft picks the install, go and read
  [Working on several branches at once](CONTRIBUTING.md#working-on-several-branches-at-once).
- If you want to know which install a command would act on, run
  `weft daemon which`.
- When you are done with a branch, `scripts/remove-worktree.sh` removes its
  worktree and its install.
- On a branch, `./setup.sh` installs neither the VS Code extension nor the
  browser extension. If you want to try a branch's VS Code extension,
  `scripts/dev-editor.sh` opens a VS Code window running it from source. If
  the branch's extension is 0.2.269 or newer, that window uses the CLI of that
  checkout's install.
- If you want a public address for a branch, run `./setup.sh --public-url`
  there: it opens the tunnel that branch's `.env.local` names, or else a
  random address that changes each time the tunnel restarts.
- `scripts/run-e2e.sh` runs 4 tests at once by default (it was 16).
- A second `scripts/run-e2e.sh` on the same machine now waits for the first
  instead of refusing.

## 2026-10-12 (0.2.268)

**On Google Cloud, an install nobody is using now lets its database sleep,
and the install workflow must be run again.**

weft no longer looks at its database on a timer. It reads it when something
happens: a run working, a timer or poll you set coming due, a call arriving, a
person answering, or a failure to deal with. A trigger that holds a connection
open, a run waiting on a person, and a quiet caller all leave the database
asleep. Locally nothing changes for you.

What you have to do on a Google Cloud install: run the install workflow
again. It adds a second Cloud Tasks queue, a Pub/Sub topic for the processes
that hold connections, and the `wake_attempts` setting (how many times a
failing timer, poll or renewal is tried before its trigger is given up, 20 by
default). `./setup.sh` upgrades the database on its own.

What you will notice:

- `weft status` lists what weft keeps failing to do under `waiting on weft:`,
  with what happens next, until it works again: a part of weft it cannot
  reach, a background job, the infra supervisor, queued runs its workers
  cannot take, news it could not send, leftovers of removed projects.
- A trigger whose timer, poll or renewal keeps failing is given up after its
  tries, with the reason on its node. `weft resync`, or reconnecting its
  connection, brings it back.
- A resync now runs on its own. It takes down only the triggers that are on,
  brings back exactly those, and refuses builds and other activations while it
  holds the project. If it stops, it goes on by itself once what it waited on
  is back; otherwise `weft resync --resume` takes it up, and
  `weft cancel-activate` gives it up and says which triggers stay down.
- `weft activate` after a park or hibernation asks what to do with the work
  the triggers kept; a resync, an infra upgrade and the CI workflow keep it
  without asking.
- A live caller whose run ends early gets `500` (the program failed), `504` (a
  time limit) or `503` (weft or a person stopped it); a socket closes with
  `1011` or `1013`.
- A live node test can now wait on a signal (a node that polls a job until it
  is done), up to the platform's limit on one request.
- On Google Cloud, the install emails you when weft's own work stays stuck
  (about $0.35 a month). It goes to the first person holding Owner on the
  project, or to the repository secret `WEFT_ALERT_EMAIL` if you add one; set
  `alerts_enabled = false` in `deploy/terraform/gcp/variables.tf` to turn it
  off, and `alert_after_minutes` (5 by default) sets how long a failure weft
  still retries waits before it emails.
- A `weft rm` that fails partway leaves the project marked as being removed:
  `weft status` and the editor say so, every verb refuses with the same line,
  and running `weft rm` again finishes it.
- A trigger that holds a connection open stops, with the reason on its node,
  when only a person can fix its connection; reconnecting the account brings
  it back. A busy or rate-limited service is still retried.
- A node may now emit on a plain output, wait on a signal, and emit more; an
  output sent before the wait is not sent again when the run resumes.
- The cost of a call whose price arrives later (an ElevenLabs dub read back
  after its run waited) is now recorded instead of being booked as unknown.

**Dub Audio (`ElevenLabsDub`) has moved to ElevenLabs's new dubbing API:
update the programs that use it.**

ElevenLabs has marked the dubbing route the node used as legacy, so the node
now uses the new one, where each dub is a project on the ElevenLabs account.

What you have to change:

- If a program dubs a video and expects a video back, or expects an mp3: the
  dub now always comes back as the audio track alone, in FLAC, because that is
  the only output the new API gives. The old route gave back an MP4 for a
  video and an MP3 for audio, so `mimeType` and `filename` change too.
- Take `numSpeakers` and `dropBackgroundAudio` out of your `.weft` files: the
  new API has neither.
- `dubbingId` is now `projectId`. The node used to leave every dub on the
  ElevenLabs account; now it deletes the project once the dub is stored. If
  the node runs on your own key and you want to keep the project, wire
  `projectId`.
- If a program gives a three-letter language code like `spa`, change it to
  the two-letter one, `es`. The new API takes language tags, which use a
  language's two-letter code when it has one, with a region added if you want
  one (`es-MX`).

New inputs:

- `model`: the default, `dubbing_v1`, costs $0.50 a minute as before and dubs
  into 29 languages. If you want one of 92 languages, or ElevenLabs's newer
  model, which it says preserves voice and emotion, pick `dubbing_v2`, at
  $2.20 a minute.
- `keyterms` takes names the dub should get right in the transcription and
  the translation.
- `cloningStrength` (dubbing_v2 only) sets how closely the dubbed voices copy
  the speakers.
- `sourceUrl` takes a public link instead of a file, and ElevenLabs downloads
  the recording itself.
- `transcript` (ElevenLabs Enterprise) takes your own transcript, with its
  translations if you have them, instead of ElevenLabs's.
- `reference` puts your own label on a project you keep.

**New ElevenLabs nodes make images and videos.**

Five new nodes call ElevenLabs's Image & Video API:

- **Generate Image** (`ElevenLabsGenerateImage`) makes an image from a prompt,
  or edits images you give it.
- **Generate Video** (`ElevenLabsGenerateVideo`) makes a video from a prompt,
  and can open and end on images you choose.
- **Video From References** (`ElevenLabsVideoFromReferences`) makes a video
  that uses images, clips or sounds you give it.
- **Lip-Sync Video** (`ElevenLabsLipSync`) makes the person in a picture speak
  an audio track.
- **Run Template** (`ElevenLabsRunTemplate`) runs a published ElevenLabs Flows
  template, yours or one shared with you. Add one input port for each of the
  template's inputs and one output port for each output you want back, each
  named by its id in the template. If you do not know the ids, declare any one
  output and run: the node checks your ports before anything is paid, and its
  refusal lists the template's inputs and outputs with their types.

To use them, you need your own ElevenLabs account on a Pro plan or above, and
an API key with ElevenLabs's Image & Video permission. Connect that key in
weft as your own ElevenLabs connection, with "Images and video" ticked. To
see what these calls cost, look at your usage in your ElevenLabs account:
ElevenLabs does not report a cost per generation, so weft records none.

**A new Dialogue node, and Speak to File takes every setting of
ElevenLabs's speech route.**

The new **Dialogue** (`ElevenLabsDialogue`) node turns a script of lines, each
with its own voice, into one audio file of them talking together. It uses
`eleven_v4` unless you pick `eleven_v3`, and like Speak to File it runs on the
built-in connection. Speak to File (`ElevenLabsSpeakFile`) now picks its model
from ElevenLabs's own list of models that speak text, so a new model shows up
without a weft update.

Both nodes take:

- a language, whether numbers are spelled out, a seed, and the text just
  before and after this clip, so the delivery flows into the clips around it;
- pronunciation dictionaries;
- stitching: each node gives out a `requestId`. If you want a later clip to
  carry on from this one, wire that id into the later node's
  `previousRequestIds`. If you are redoing an earlier clip that leads into
  this one, wire it into that node's `nextRequestIds`. Both nodes must be of
  the same kind;
- audio effects: a filter, the distance from the microphone, a room's reverb,
  a background noise and a place between left and right;
- speaking a professional voice clone with its instant clone (`usePvcAsIvc`),
  which ElevenLabs says can sound more expressive and answer faster;
- zero retention, on Enterprise accounts;
- every output format ElevenLabs has, wav and alaw included.

Speak to File also takes the style and speaker boost voice settings and
Japanese normalization, and Dialogue takes stability and similarity for the
whole script.

If a node you wrote calls ElevenLabs's text to dialogue route, or the
streamed or timestamped text to speech routes, it now runs on the built-in
connection too, instead of asking for your own key, because weft now prices
those routes.

**The other ElevenLabs nodes take more of ElevenLabs's settings, and give
big results as JSON files.**

Most settings ElevenLabs offers on these nodes' routes are now inputs; the
ones it has deprecated are not. Most combinations ElevenLabs would refuse are
refused before anything is paid for: when the program is built if you wrote
the values, and when the node runs if they are wired.

A result that grows with the length of the recording or the call (a
transcript, word timings, a conversation) now leaves the node as a JSON file,
because a value passed between nodes is capped at 100 KB. Small results (a
language, a duration, an id) stay plain values.

- **Transcribe File** (`ElevenLabsTranscribeFile`) now defaults to `scribe_v2`
  (it was `scribe_v1`) and also offers `scribe_v2_medical`. ElevenLabs has
  deprecated `scribe_v1` and no longer documents `scribe_v1_experimental`, so
  if a program names either, change it to `scribe_v2`. The `text`, `words`,
  `entities`, `editedTranscript` and `channels` outputs are gone: they now come
  out together as one `transcript` JSON file (the edited copy under
  `editedText`), so a program that reads any of them reads the file instead.
  New: telling speakers apart, speaker
  roles, key terms, entity detection and redaction, subtitle and document
  exports (srt and others), an edited copy of the transcript following an
  instruction you write in plain words, separate channels, a public link
  instead of a file, and video files.
- **Transcribe** (`ElevenLabsTranscribe`, live) takes the commit strategy and
  the voice detection settings, extra languages, key terms, entity detection,
  a transcript edit, background filtering and μ-law audio, and sends word
  timings, entities and edits along with each segment. The live bus of
  segments is now called `segments` (it was `transcript`), and at the end the
  whole session's text, entities and edited copy come out as one `transcript`
  JSON file, in place of the `text`, `entities` and `editedText` outputs.
- **Music** (`ElevenLabsMusic`) now defaults to `music_v2_5` (it was
  `music_v1`), which answers a 48 kHz, 192 kbps mp3 where `music_v1` answered
  44.1 kHz at 128 kbps; `music_v2` is offered too. ElevenLabs has deprecated
  `music_v1`, so if a program names it, change it to `music_v2_5`. New: a
  composition plan instead of a prompt, lyrics, a seed, finetunes, the output
  format, storing the song so a later plan can build on it, and C2PA signing.
  A new `songId` output names the song.
- **Sound Effect** (`ElevenLabsSoundEffect`) takes the output format and the
  model.
- **Speak** (`ElevenLabsSpeak`, streaming) takes the voice settings, a
  language, number spelling, a seed, SSML, pronunciation dictionaries, the
  chunk schedule and more output formats, and can send the timing of every
  character it speaks. ElevenLabs has deprecated `eleven_turbo_v2_5`, so the
  list no longer offers it; if a program names it, change it to
  `eleven_flash_v2_5`, the replacement ElevenLabs suggests.
- **Change Voice** (`ElevenLabsChangeVoice`) picks its model from ElevenLabs's
  own list of voice changing models, and takes the voice settings, a seed, raw
  PCM input, zero retention and every output format.
- **Isolate Voice** (`ElevenLabsIsolateVoice`) takes raw PCM input. Its result
  is now typed and named as the mp3 ElevenLabs sends back
  (`isolated_take.mp3`), where it used to keep the input's type and name
  (`isolated_take.wav`).
- **Design Voice** (`ElevenLabsDesignVoice`) takes the output format,
  loudness, a seed, guidance, quality, a reference recording and its strength,
  and gives out the line the previews speak. Its previews now default to
  `mp3_44100_128`, where ElevenLabs's own default, which the node used before,
  is `mp3_44100_192`; set `outputFormat` to keep the old quality.
- **Remix Voice** (`ElevenLabsRemixVoice`) is new: it turns a voice you already
  have into new previews from a description of the changes, and Create Voice
  saves the one you pick, as it does a designed one.
- **Create Voice** (`ElevenLabsCreateVoice`) and **Clone Voice**
  (`ElevenLabsCloneVoice`) take labels; Create Voice also gives out the
  voice's preview link and labels, and Clone Voice whether the voice needs
  verifying.
- **Align** (`ElevenLabsAlign`): the word timings, now with each word's loss,
  and the timing of every character come out together as one `alignment` JSON
  file, in place of the `words` and `characters` outputs.
- **Create Agent** (`ElevenLabsCreateAgent`) takes the language model (picked
  from ElevenLabs's list), tools, a knowledge base, built-in tools, the voice
  model and settings, timeouts, data to collect, evaluation criteria and which
  settings a call may change, plus JSON inputs for every other setting.
  `builtInTools` starts with `end_call`, so an agent created by an unchanged
  program can now hang up on its own; remove `end_call` from it if you do not
  want that.
- **Agent Call** (`ElevenLabsAgentCall`) takes per-call changes (first
  message, prompt, language, voice), a ring timeout, call recording and
  answering-machine detection, now calls from Exotel numbers too, and gives
  out the call id of a SIP call.
- **Get Conversation** (`ElevenLabsGetConversation`) also gives out the
  collected data, the evaluation results, the summary title, the success
  score, the duration, why the call ended, its cost in US dollars, and an
  OpenTelemetry trace of the conversation. Its `transcript` is now a JSON file
  holding the text and the turns, in place of the `text` output and the list
  of turns, and `conversation` is a JSON file, written only when something
  reads it.

**weft records ElevenLabs costs more exactly.**

- v4, v3 and Multilingual v2 speech: $0.08 per 1,000 characters (was $0.10).
- v4 Turbo, Flash and Turbo: $0.04 per 1,000 characters (was $0.05).
- v3 Conversational: $0.04 per 1,000 characters (was $0.10).
- Audio in opus, wav or alaw is recorded at its real length. Before, all three
  were read as if they were 128 kbps mp3, so alaw was recorded at half its
  length, opus at a quarter to one and a half times its length depending on
  its bitrate, and wav at up to six times its length.
- Music with no output format chosen was recorded as a 128 kbps file, so a
  song from `music_v2` or `music_v2_5`, which answer at 192 kbps, was recorded
  at one and a half times its length. It is now recorded at its real length.
- Batch transcription (Transcribe File) now records the key terms, transcript
  edit, entity detection and speaker role add-ons, each channel of a
  multichannel recording, and the length ElevenLabs reports, so a
  transcription from a link is no longer recorded as one minute. Live
  transcription (Transcribe) already recorded key terms and entity detection:
  key terms are now $0.08 an hour (was $0.05), entity detection 30% of the
  base rate (was $0.07 an hour), and a transcript edit adds $0.12 an hour.
- If a node you wrote calls ElevenLabs's legacy dubbing route (`/v1/dubbing`
  without `/project`), it now asks for your own ElevenLabs key: weft no longer
  prices that route.

**A node's picker can show only the items that match a filter.**

If you write a node whose `remote_select` should show only some of the items a
service lists, add `where` to the list source: an item is kept only when the
value at each named field (a dotted path like `limits.tier` reaches inside it)
equals the one you give. Speak to File uses it to list only the ElevenLabs
models that speak text: `"where": { "can_do_text_to_speech": true }`.

**When ElevenLabs refuses a call, the error says why.**

The error now shows ElevenLabs's own message instead of its whole reply. When
a setting does not fit the model, it names the setting the way ElevenLabs
spells it (`body.veo-3.1-generate-001.duration_secs` for the node's
`durationSecs`) and says what is wrong with it.

If a node you wrote checks replies with `json_call`, `get_json`, `post_json`
or `checked_send`, it now reads two more error shapes from any service: it
quotes a `detail` object's `message`, and turns FastAPI's list of field errors
(`{"detail": [{"loc": ..., "msg": ...}]}`) into one line naming each field and
what is wrong with it. `put_response` used to quote the raw reply; it now
reads every error shape those four read.

## 2026-10-11 (0.2.267)

**On Google Cloud, a run waiting on a person lets the database sleep.**

A run that cannot pause (a route still answering its caller, a trigger with
`recorded: false`, a live stream between nodes) keeps its question in its
worker while it waits. Until now that kept the database awake for the whole
wait. Now nothing touches the database until the answer or a cancel arrives:
the answer reaches the worker through a news topic weft creates in your
Google project. Upgrade as usual (merge upstream into your fork and run the
install workflow again): it creates the topic and the rights that go with it.

A worker on Google Cloud can keep one connection to weft for at most an
hour. If every run on it is still waiting at that point, the worker stops:
runs that can pause carry on elsewhere, and a run that cannot pause ends,
saying which limit ended it. To wait longer, make the run one that can pause.

**Five instance nodes lost "Instance" from their names.**

`StartInstanceInfra`, `StopInstanceInfra`, `TerminateInstanceInfra`,
`ActivateInstanceTriggers` and `DeactivateInstanceTriggers` are now
`StartInfra`, `StopInfra`, `TerminateInfra`, `ActivateTriggers` and
`DeactivateTriggers`. Rename them in your `.weft` files. Their `instance`
input is now optional: if you leave it unwired, the node acts on the
program's own copies, the ones every instance shares.

**GmailSend can send from another address.**

The new optional `from` input sends from one of the account's "Send mail as"
addresses set up in Gmail; any other address fails naming the ones that work.
To read that list, the Google connection needs "Read mail" or "Organize mail"
ticked. Replies are unchanged: they need one of those two or "Read mail
headers".

**A program built before this version needs one rebuild.**

How a waiting node receives its answer changed, so a worker built before this
version fails when a waiting node gets its answer. Resync each program once:
`weft resync` if its triggers are on, `weft build` otherwise. Either one
builds the worker again on this version of weft and moves the program to it.

**Skipping a question ends the node that asked it.**

When a person skips a waiting question (Skip in the browser extension, or
`POST /signal/{token}/skip`), the node that asked now ends skipped: its
outputs close and the nodes reading them skip too. It used to answer the
question with null, so if your program reads a null answer as "skipped", it
now sees the node skipped instead. Skipping a trigger is refused with a `409`;
it used to fire the trigger.

**A Google connection can hold Gmail alone.**

The connect check now asks Google who the account is, through two permissions
every connection asks for ("Know who you are" and "See your email address"),
instead of reading Drive. So a connection with only Gmail, or only Calendar,
ticked now passes. If you run your own shared Google app, its `covers` does
not need to list those two.

**Node authors: inputs can be required only when wired.**

`requiredWhenWired: true` on an input means: left unwired, the node runs
without it; wired, a wire that delivers nothing skips the node. It is for an
input whose absence means something, like the instance nodes' `instance`.

**Builds no longer hold one request open.**

`weft build`, `run`, `activate`, `resync` and `infra start` used to keep one
request open to the install for the whole build. The install now answers as
soon as the builds start, and the CLI follows each one to its end. If a cloud
install does not answer, the error names that install instead of telling you
to start the local daemon. Update the VS Code extension too: it needs this
version.

**A route's limits can overshoot briefly after a quiet spell.**

When a worker copy wakes after a quiet spell, its first calls are decided on
what it heard before it went quiet, until weft's broker answers it: under a
second normally, a few seconds when the broker was asleep too. During that
window, copies waking at the same moment can each let in up to the full
`callsAtOnce`. We chose this so the first call after a pause never waits for
the broker to start.

**The editor's activate button names the triggers that are off.**

When some triggers are off while the others run, the second button now says
which ones ("Reactivate a, b" or "Reactivate a, b and 3 more") instead of
"Reactivate the rest".

## 2026-10-09 (extension 0.2.266): weft no longer needs Kubernetes

**Breaking: your run history does not come along.**

Weft used to run on a local Kubernetes cluster. It now runs as one process
next to Docker on your machine, and on Google Cloud as Cloud Run services plus
one small VM. The database behind it starts over with this version, so the old
one cannot be carried forward.

If you installed weft before this version, `./setup.sh` finds the old install
and stops to ask before it goes on:

```
an older weft (the one that ran on a local Kubernetes cluster) is on this machine,
and this version cannot carry it forward. It is going to be wiped: the projects on
your disk are not touched, but their run history, versions and stored connections are.
  Do you want to proceed? [Y/n]
```

If you say yes, it wipes the old install (`scripts/scrub-old-install.sh`) and
carries on with a normal install. If you say no, nothing is touched and nothing
is installed.

What goes:
- The old cluster.
- Every project's run history and versions.
- Your stored connections: you connect your accounts again the first time a
  program asks.
- Every container, volume and image the old weft made, your programs' own
  infrastructure (and its data) included.
- Every named install on the machine (a test cell, say), with its database.
  The question lists the ones it found.

What stays:
- Your project folders. `weft run` in each one registers it again.
- Whether you turned on a public address with `--public-url`.
- The assistant you picked for `weft new`.

If you would rather wipe first by hand, or `./setup.sh` runs without a
terminal to ask on, run `scripts/scrub-old-install.sh` yourself, then
`./setup.sh`. On a machine that never had the old weft, or that was already
wiped, `./setup.sh` is a normal install or update and asks nothing.

**"Member" is now "instance", everywhere.**

What used to be a program's members are now its instances: separate copies of
part of a program, each under its own id. One per person is one way to use
them, one per session another, several per person a third; which person owns
which instances is kept in your program's own database. Every name changed
with it, and the old ones are gone:
- `@per_member` is `@per_instance`, and `@member_filled` is `@instance_filled`.
  Change them in your `.weft` files.
- The `members` catalog package is `instances`, and its nodes follow:
  `CurrentInstance`, `MintInstanceToken`, `WipeInstance`, `ListInstances`,
  `ListInstanceInfra`, `InstanceInfraStatus`, `StartInstanceInfra`,
  `StopInstanceInfra`, `TerminateInstanceInfra`, `ActivateInstanceTriggers`,
  `DeactivateInstanceTriggers`, `GetInstanceValues`, `SetInstanceValues` and
  `InstanceCosts`.
- The headers are `Weft-Instance` and `Weft-Instance-Token`. If your backend or
  frontend sends the old ones, rename them there.
- The `--member` flag is `--instance` on every command, and
  `--all-members` is `--all-instances`.
- A member token is now an instance token: it acts inside one instance of the
  program.

The page is now [programs with instances](docs/src/running/instances.md).

The name "instance" used to mean a named install (a second weft on the same
machine, such as a test cell). That is now called an install: the field in
`~/.local/share/weft/config.json` is `install`.

**weft moves to new ports.**

Weft now answers at `http://127.0.0.1:14111` (it was 9999). Open the dashboard
there, and point anything you configured by hand at it. The VS Code extension
follows on its own unless you set `weft.dispatcherUrl` yourself (if yours
still says the old `http://localhost:9999`, it is ignored and the extension
offers to remove it). The browser
extension remembers the address with each token you saved, so remove those
tokens and add them again.

If you use a Cloudflare tunnel, point it at `127.0.0.1:14112` (it was 9998).

If you want weft on another port, set its variable once when you run
`./setup.sh` (`WEFT_PUBLIC_PORT`, `WEFT_OUTSIDE_PORT`, `WEFT_INTERNAL_PORT`
or `WEFT_POSTGRES_PORT`). Weft saves the ports it runs on in
`~/.local/share/weft/ports.json`, so the port stays moved on later runs and
the CLI and the VS Code extension find it there.
`WEFT_SEAWEED_PORT` moves the object store, and is read on every run.

**A node the worker was running is never run again by itself.**

If a worker went away while a node was running, the next worker used to run
that node again from the top, so an email, a post or a paid call could happen
twice. Now it fails the node instead, saying the worker went away and the node
was not run again. Check what the node did, then re-run from there with
`weft run --seed`, which reuses every node that completed. A node that was
waiting on an answer still carries on when the answer comes, as before.

**Nodes that catch their failures say so with `catchErrors`.**

If you wrote a node with an optional `error` output and `ctx.catch_into_error`,
change it: remove the `error` output from its `metadata.json`, add
`"catchErrors": true` under `features`, and let `run` return its errors as any
node does (`ctx.catch_into_error` is gone). Weft adds the `error` output
itself, and a failure goes there when a program wires it. A node with `catchErrors`
that still declares `error` is refused when it loads. Programs need no change.

**Conversations are kept in a file only.**

The AI nodes (`LlmInference`, `LlmStream`) and `ChatHistoryAppend` no longer
have a `history` input or output. If a program wires `history`, wire
`historyFile` instead: each call adds its turn to that one file and passes the
same file on. If a call gets no file and its `historyFile` output is wired, it
starts a new one. The graph shows each change to the file under "Files
edited" on the node that made it.

**A node changes a stored file with `edit`.**

`ctx.storage(scope).edit(&file, |old| ...)` changes a file in place without
losing another write's change. `replace` still overwrites.
`replace_stream` is gone. Every stored file value now carries a `version`. A
seeded run that would reuse a node whose file was edited since is refused,
naming the node, the port and the file, and offering `--emit` or
`--seed-before`.
