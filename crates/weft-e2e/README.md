# weft-e2e: Layer-4 end-to-end tests

A test is plain Rust that prepares a fixture, drives it through the dispatcher's
public API exactly as the outside world sees it, and asserts. Gated behind the
`e2e` feature (off by default), so `cargo test --workspace` compiles but runs
none of it.

## Run

Always through the runner:

```bash
scripts/run-e2e.sh                          # every test
scripts/run-e2e.sh live_chat plain          # every test in these files
scripts/run-e2e.sh accesses::some_test      # one test
scripts/run-e2e.sh --failed                 # the tests that did not pass last time
scripts/run-e2e.sh -j 8                     # 8 tests at once instead of 16
scripts/run-e2e.sh --keep-going             # keep going after a failure, to see them all
scripts/run-e2e.sh --clean                  # only remove what failed runs kept
```

The runner brings the install to current code once (`setup.sh --cli --daemon`),
builds every test binary once, then runs every TEST on its own, side by side,
longest first (by how long each took last time). Each test's output lands in
`~/.local/share/weft/e2e/run/<file>::<test>.log`, and the summary prints one
line per test with how long it took.

If you want more tests at once, raise `-j`. Everything runs on your machine,
so past a point that only makes each test slower: watch the
per-test times in the summary grow when you raise it. On a 24-core machine the
whole suite takes under three minutes at the default of 16.

If you want to know where a slow test spends its time, read its log: every
`weft` command it ran and every wait it sat through is a line of its own
(`[e2e] 38.3s: weft infra start`, `[e2e] waited 4.0s for: execution ... to
reach a terminal status`).

If you want to know what happens when something fails: the test keeps what it
made (its projects, its cell if it had one), the runner stops starting new
tests and lets the running ones finish, and it writes a post-mortem next to the
log (`<file>::<test>.post-mortem/`: every container of the install, the runtime's
log, and the logs of the containers of every project the test kept). The
install is left as it was, so you can poke at it. What
the failure kept stays until the next run starts: every run begins by removing
what earlier failed runs kept (cells, projects and connections the suite
made, marked as e2e), then runs `weft clean --images --all`. That last step is
machine-wide and reaches past the suite: it removes every worker image no
project on the default install references (yours too, once nothing runs them),
infra images no project references, every builder base and runtime image
but the current one, the worker compile cache this checkout
moved off (every lane, at once), and compile caches of another retired key that
sat unused. `--clean` does only this, without bringing the install to
current code first, so a failure's runtime is still the one that failed.
`--failed` re-runs just the tests that did not pass: the ones that failed, the
ones the stop kept from starting, and, if you pressed Ctrl-C, the ones it cut
short. Ctrl-C stops every running test (and what each one started) before the
runner lets go of the install, so nothing keeps running into the next run.

The runner needs bash 5.1 or newer, and only one run (or `--clean`) at a time
on a machine, from any checkout: a second one is refused while the first is
alive.

Every run ends with the same `weft clean --images --all`, pass or fail. When a
whole run passes, nothing is left behind: every test removed its own projects,
cells and connections. When some failed, what they kept stays (the reclaim
keeps every image a project still points at) and the rest goes.

## Credentials, and what the runner provides for you

Everything a local machine can serve, the runner provisions itself: the store's
Postgres (the install's own, at the address in its `secrets.env`), and an S3
endpoint (the install's object store container, whose bucket stays across runs
because it belongs to the install rather than to the suite). You set
nothing for those.

What is left is the external services, which come from the
environment. The runner reads the repo-root `.env`, which is gitignored, and
anything you already exported wins over what it would provision.

They are named `WEFT_E2E_<SERVICE>_<FIELD>`:

| Service | Variables |
|---|---|
| Slack | `WEFT_E2E_SLACK_BOT_TOKEN`, `_APP_TOKEN`, `_SIGNING_SECRET`, `_CLIENT_ID`, `_CHANNEL_ID` |
| Google | `WEFT_E2E_GOOGLE_CLIENT_ID`, `_CLIENT_SECRET`, `_REFRESH_TOKEN` |
| Telegram | `WEFT_E2E_TELEGRAM_TOKEN`, `_CHAT_ID` |
| Email | `WEFT_E2E_EMAIL_USER`, `_PASSWORD`, `_IMAP_HOST`, `_IMAP_PORT`, `_SMTP_HOST`, `_SMTP_PORT` |
| ElevenLabs | `WEFT_E2E_ELEVENLABS_API_KEY` |
| JWT issuer (Auth0 or any OAuth issuer with a client-credentials grant) | `WEFT_E2E_JWT_ISSUER` (with its trailing slash, as the `iss` claim spells it), `_AUDIENCE`, `_CLIENT_ID`, `_CLIENT_SECRET`; the test mints its own token on every run, so nothing expires |
| A GCP install (`gcp_install`, spends money on your cloud) | `WEFT_E2E_GCP_URL`, `_OPERATOR_KEY`, `_PROJECT`, `_REGION`; and, on this machine, a project whose `weft.toml` has a target at that address, with `weft login <that target>` done, because the CLI the test drives finds its key there |

A test whose variables are absent **skips** rather than fails, through
`env_or_skip` / `env_group_or_skip`, so a partial `.env` still gets you a
useful run. It also means a green suite does not prove the skipped ones
work: check the output for what skipped before you trust it.

This is a different mechanism from the node self-tests, which read
`WEFT_NODE_TEST_*` and can also use a connection you signed into in the editor.
Theirs is documented in [Testing a node](https://weavemindai.github.io/weft/nodes/testing.html#giving-the-live-tier-what-it-needs).

## When the install looks wrong, fix the source

The rule and its whole list of never-dos live in
[CONTRIBUTING](../../CONTRIBUTING.md#working-on-your-install), because it holds
for every part of this repo, not just this suite.

The one thing worth repeating here: time is never a reason to deviate. A
reinstall is slow (image builds, per-fixture worker compiles), and that is
expected. Take the slow reproducible path, so a fresh
machine and CI end up with the same working system you have.

```bash
./setup.sh                       # bring the install to the current code
```

`--uninstall --purge` exists for a genuine clean slate and takes every project,
execution and stored credential on the machine with it. It is not the way to
apply a change.

If a test needs a capability the toolkit lacks, see "Add a test". Never patch
a test to tolerate the wrong state.

## Add a test

Add `tests/<area>.rs` (the runner discovers it; no list to update). Shape:

```rust
#![cfg(feature = "e2e")]
use weft_e2e::{ensure, project::Project, run};

#[tokio::test]
async fn my_scenario() -> anyhow::Result<()> {
    let disp = ensure::up().await?;                                // the default install
    let mut project = Project::prepare("my_fixture", disp).await?; // fixtures/my_fixture -> isolated project
    let settled = run::run_and_settle(&mut project).await?;        // drive it
    settled.completed()?;                                          // assert (folds the replay log)
    settled.assert_input("out", "data", &serde_json::json!("x"))?;
    project.finish().await                                         // removes it (pass only; a fail keeps it)
}
```

A **fixture** is `fixtures/<name>/` with a `weft.toml` + a `src/main.weft`; a
custom node goes under `fixtures/<name>/nodes/<node>/`. If several fixtures
need the same node, commit it once under `fixtures/_nodes/<node>/` and symlink
it into each fixture's `nodes/` (as `test_sse_trigger` is): the catalog and the
rig's copy both follow the link. The fixture's `[package]` name
must start with `e2e_`: that is how `--clean` tells test projects from yours,
and `Project::prepare` refuses a fixture without it. The id does not matter,
the rig gives every copy a fresh one. For a runtime value baked into the graph,
put a `__E2E_TOKEN__` placeholder in `src/main.weft` and call
`project.substitute_in_main(...)` before building.

To write the `.weft` graph itself, see the language reference under
`../../docs/src/language/` (and `../../docs/src/nodes/` for custom nodes). Get the project
right there; a malformed `src/main.weft` fails at build, not as a test assertion.

**If a test needs something the toolkit doesn't have, extend the toolkit
(`src/`), not the test.** Keep test bodies about WHAT they assert; the HOW (HTTP,
SQL, Docker) lives in the toolkit so it stays DRY and reviewable.

### Five minutes, at most

No test may run longer than 5 minutes. The runner stops one that does,
with everything it started, and reports it as `SLOW` rather than `FAIL`,
with a line at the end of its log saying so (`TEST_LIMIT` in
`scripts/run-e2e.sh`). If you are writing a test that needs longer, it is
doing too much: split it. And if one of your waits could hang (an infra
start, a trigger that never fires), keep its own deadline well under the
cap, so it fails naming what it waited on instead of being cut off.

### Your test runs beside the others

Every test runs at the same time as a few others, on the same install, so a
test may only touch and count what it made itself. In practice:

- Assert on your own project, your own run, your own workers. A query over a whole
  table ("how many links exist", "how many workers run") counts your
  neighbours too; scope it to your execution id or your project, the way
  `Platform::public_file_link_count_for` does.
- Never delete anything you did not create, and never clean up at the start of
  a test. A failed test's leftovers are evidence, and the next run removes them.
- End a passing test with `project.finish()` (and `cell.finish()` if it made a
  cell). A failing test skips that on purpose.
- Fakes bind a port the system picks. Never pick a fixed port.
- Make connections through `access::connect_direct` / `connect_paste` (and wrap
  a grant you seed by hand in `access::SeededGrant`): a connection carries no
  name that says it is a test's, so those record it in a ledger, and the next
  run removes what a failed test left in it.

### When your test needs its own install: a cell

A few things belong to the whole install, and a test that changes one of
them needs an install to itself. Today that means restarting the runtime
(`runtime_restart`), or running the runtime's own clock faster (`api_ticket`).

If your test needs that, start a cell: a whole install of its own (its own
runtime process, Postgres and ports) on the same machine. It starts from the
images already built, and everything else in the toolkit follows its
dispatcher there:

```rust
use weft_e2e::{platform::Platform, project::Project, Cell};

let cell = Cell::start(1.0).await?;                     // an install of this test's own
let disp = cell.dispatcher();
let platform = Platform::connect(&disp).await?;          // this cell's Postgres and containers
let mut project = Project::prepare("reach_out_feed", disp.clone()).await?;
// ... drive, assert ...
project.finish().await?;
cell.finish().await                                      // removes the cell (pass only)
```

The number you hand `Cell::start` is how fast the cell's own timers run: `1.0`
is real time, and `0.1` runs them ten times faster. That is
every lease, reaper tick, ticket and silence window the runtime keeps for
itself, all together, so the protocol behaves the same, only sooner: a lease
that runs out after 60 seconds at real time runs out after 6. What a person
configured (a wait's timeout, a trigger's poll interval) and budgets for real
work (a worker's start) keep their real length. If
your test waits on one of the runtime's own timers, `cell.scaled(...)` turns a
real-time duration into the cell's.

Use real time when a faster clock would end the thing you are observing before
you observe it.

Pick something between the two when one of those timers also has to cover real
work, which no clock compresses. A late live caller may have to wait for a
worker to start, and at a tenth of the ticket's two minutes a busy machine
starting that worker used up most of what was left, so `api_ticket` runs at
`0.25`.

A cell costs its own startup, so a test that does not watch anything
install-wide stays on the default install.

## Toolkit

| Module | What it does |
| --- | --- |
| `ensure` | Reach the default install (the runner brought it up); wait for health. |
| `cell::Cell` | A whole install of a test's own, with its own pace; see above. |
| `project::Project` | Fixture -> isolated project: `prepare`, `build`, `activate`, `weft(args)`, `substitute_in_main`, `finish`. |
| `run` | `run_and_settle`, `start`, `wait_for_triggered_execution`, `SettledRun::observe`. |
| `assert` (`SettledRun`) | `completed`, `failed_with`, `assert_input/output/skipped`, `assert_loop_iterations`. |
| `signal` | Discover + fire signals: `SignalScope`, `fire_token`. |
| `live` | Live caller: `open_ws`, `http_post`, `http_request`, `browser_post_json`. |
| `human` | Human-in-the-loop: `wait_for_form_by_node`, `answer_form`. |
| `fakes` | Servers the system dials OUT to: `SseFake`, `PollFake`, `BytesFake`, `HangingBytesFake`, `QueueFake`. |
| `infra` | Infra lifecycle: `start_and_wait_running`, `call_endpoint`, `terminate_and_wait_gone`. |
| `storage` | `list`, `download`, `assert_file_contents`. |
| `platform::Platform` | Reaches BEHIND the API on purpose (below). |

Driving each trigger kind: **plain** `run::run_and_settle`; **HTTP**
`activate` + `live::http_post`; **WebSocket** `activate` + `live::open_ws`;
**human form** `human::wait_for_form_by_node` + `answer_form`; **dial-out
(SSE/poll)** a `fakes::*` + `substitute_in_main` +
`wait_for_triggered_execution`; **infra** `infra::start_and_wait_running` +
`terminate_and_wait_gone`.

## Platform layer

The toolkit above tests the PROGRAM (what a `.weft` does, via the public API).
`platform` tests the SYSTEM underneath (which worker served an execution, crash
recovery) by reaching behind the API the way an operator would: it reads the
install's Postgres, removes worker containers with Docker (`kill_workers`), and
reads the runtime's log (`wait_for_runtime_log`). **None of it ships**: `platform` + its `sqlx` dep compile only
under `e2e`, and this crate is never built into an image. Scope is LOCAL
reliability only (crash and restart on one machine). The API-driving toolkit is
auth-agnostic (via the `AuthProvider` seam), so a harness that needs tokens can
reuse it.

No silent retries (a transient failure is a real bug). Always end a
passing test with `project.finish()`.
