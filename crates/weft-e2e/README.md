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

The runner brings the cluster to current code once (`setup.sh --cli --daemon`),
builds every test binary once, then runs every TEST on its own, side by side,
longest first (by how long each took last time). Each test's output lands in
`~/.local/share/weft/e2e/run/<file>::<test>.log`, and the summary prints one
line per test with how long it took.

How many run at once is `-j`. The whole cluster is one kind node on your
machine, so past a point more at once only makes each test slower: watch the
per-test times in the summary grow when you raise it. On a 24-core machine the
whole suite takes under three minutes at the default of 16.

If you want to know where a slow test spends its time, read its log: every
`weft` command it ran and every wait it sat through is a line of its own
(`[e2e] 38.3s: weft infra start`, `[e2e] waited 4.0s for: execution ... to
reach a terminal status`).

If you want to know what happens when something fails: the test keeps what it
made (its projects, its cell if it had one), the runner stops starting new
tests and lets the running ones finish, and it writes a post-mortem next to the
log (`<file>::<test>.post-mortem/`: every pod, the recent events, the logs of
the dispatcher, the listeners and the infra supervisors, and of the worker pods
of every project the test kept). The cluster is left as it was, so you can poke at it. What
the failure kept stays until the next run starts: every run begins by removing
what earlier failed runs kept (cells, projects and connections the suite
made, marked as e2e), then runs `weft clean --images --all`. That last step is
machine-wide and reaches past the suite: it removes every worker image no
project on the default install references (yours too, once nothing runs them),
infra images no project references, the kind node's cached copies of both,
every builder base but the current one, and worker compile caches of a retired
key that sat unused. `--clean` does only this, without bringing the cluster to
current code first, so a failure's dispatcher is still the one that failed.
`--failed` re-runs just the tests that did not pass: the ones that failed, the
ones the stop kept from starting, and, if you pressed Ctrl-C, the ones it cut
short. Ctrl-C stops every running test (and what each one started) before the
runner lets go of the cluster, so nothing keeps running into the next run.

The runner needs bash 5.1 or newer, and only one run (or `--clean`) at a time
on a machine, from any checkout: a second one is refused while the first is
alive.

When a whole run passes, nothing is left behind: every test removed its own
projects, cells and connections, and the runner runs the same
`weft clean --images --all`, with the same machine-wide reach.

## Credentials, and what the runner provides for you

Everything a local machine can serve, the runner provisions itself: the store's
Postgres (a port-forward of the cluster's own, which it closes when the run
ends), and an S3 endpoint (the daemon's SeaweedFS container, whose bucket stays
across runs because it belongs to the daemon rather than to the suite). You set
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

A test whose variables are absent **skips** rather than fails, through
`env_or_skip` / `env_group_or_skip`, so a partial `.env` still gets you a
useful run. It also means a green suite does not prove the skipped ones
work: check the output for what skipped before you trust it.

This is a different mechanism from the node self-tests, which read
`WEFT_NODE_TEST_*` and can also use a connection you signed into in the editor.
Theirs is documented in [Testing a node](https://weavemindai.github.io/weft/nodes/testing.html#giving-the-live-tier-what-it-needs).

## When the cluster looks wrong, fix the source (HARD REQUIREMENT)

The rule and its whole list of never-dos live in
[CONTRIBUTING](../../CONTRIBUTING.md#working-on-the-cluster), because it holds
for every part of this repo, not just this suite.

The one thing worth repeating here: time is never a reason to deviate. A
reinstall is slow (image builds, per-fixture worker compiles), and that is
expected. Take the slow reproducible path, so a fresh
machine and CI end up with the same working system you have.

```bash
./setup.sh                       # bring the cluster to the current code
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
custom node goes under `fixtures/<name>/nodes/<node>/`. Its `[package]` name
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
SQL, kubectl) lives in the toolkit so it stays DRY and reviewable.

### Your test runs beside the others

Every test runs at the same time as a few others, on the same cluster, so a
test may only touch and count what it made itself. In practice:

- Assert on your own project, your own run, your own pods. A query over a whole
  table ("how many links exist", "how many pods are alive") counts your
  neighbours too; scope it to your color or your project, the way
  `Platform::public_file_link_count_for` does.
- Never delete anything you did not create, and never clean up at the start of
  a test. A failed test's leftovers are evidence, and the next run removes them.
- End a passing test with `project.finish()` (and `cell.finish()` if it made a
  cell). A failing test skips that on purpose.
- Fakes bind a port the system picks, and the platform layer's port-forwards do
  too. Never pick a fixed port.
- Make connections through `access::connect_direct` / `connect_paste` (and wrap
  a grant you seed by hand in `access::SeededGrant`): a connection carries no
  name that says it is a test's, so those record it in a ledger, and the next
  run removes what a failed test left in it.

### When your test needs its own install: a cell

Some things are shared by every project on an install, and a test that watches
them change cannot share them. The listener and supervisor POOLS are the case
today: a scale-down test waits for the whole pool to fold to one pod, and
another test's trigger on the same pool keeps a second pod busy.

If your test watches something install-wide, start a cell: a whole install of
its own (dispatcher, Postgres, broker, pools) in the same cluster, in
namespaces of its own. It starts from the images already built, and everything
else in the toolkit follows its dispatcher there:

```rust
use weft_e2e::{platform::Platform, project::Project, Cell};

let cell = Cell::start(Cell::FAST).await?;              // an install of this test's own
let disp = cell.dispatcher();
let platform = Platform::connect(&disp).await?;          // this cell's Postgres and namespaces
let mut project = Project::prepare("reach_out_feed", disp.clone()).await?;
// ... drive, assert ...
project.finish().await?;
cell.finish().await                                      // removes the cell (pass only)
```

The number you hand `Cell::start` is how fast the cell's own timers run: `1.0`
is real time, and `Cell::FAST` (`0.1`) runs them ten times faster. That is
every heartbeat, lease, reaper tick and silence window the runtime keeps for
itself, all together, so the protocol behaves the same, only sooner: the
scale-down sweep that comes round every 60 seconds at real time comes round
every 6. What a person configured (a wait's timeout, a trigger's poll interval)
and budgets for real work (a new pod's spawn grace) keep their real length. If
your test waits on one of the runtime's own timers, `cell.scaled(...)` turns a
real-time duration into the cell's.

Use real time when a faster clock would end the thing you are observing before
you observe it: `listener_move` holds a two-pod overlap open, and a fast
scale-down would fold it early.

Pick something between the two when one of those timers also has to cover real
work, which no clock compresses. A live caller's ticket starts counting before
the worker it names is ready, and at a tenth of its two minutes a busy cluster
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
| `fakes` | Servers the system dials OUT to: `SseFake`, `PollFake`, `SocketFake`, `BytesFake`. |
| `infra` | Infra lifecycle: `start_and_wait_running`, `call_endpoint`, `terminate_and_wait_gone`. |
| `storage` | `list`, `download`, `assert_file_contents`. |
| `platform::Platform` | Reaches BEHIND the API on purpose (below). |

Driving each trigger kind: **plain** `run::run_and_settle`; **HTTP**
`activate` + `live::http_post`; **WebSocket** `activate` + `live::open_ws`;
**human form** `human::wait_for_form_by_node` + `answer_form`; **dial-out
(SSE/poll/socket)** a `fakes::*` + `substitute_in_main` +
`wait_for_triggered_execution`; **infra** `infra::start_and_wait_running` +
`terminate_and_wait_gone`.

## Platform layer

The toolkit above tests the PROGRAM (what a `.weft` does, via the public API).
`platform` tests the SYSTEM underneath (which worker served an execution, crash
recovery, idle-reap) by reaching behind the API the way an operator would: reads
the cluster Postgres, drives pods with `kubectl` (`kill_workers`,
`restart_dispatcher`), and fakes "time passed" by BACKDATING the timestamp the
reaper reads (never a clock hook; helpers import the reaper's own constants so
they can't rot). **None of it ships**: `platform` + its `sqlx` dep compile only
under `e2e`, and this crate is never built into an image. Scope is LOCAL
reliability only (crash/restart/reap on one machine). The API-driving toolkit is
auth-agnostic (via the `AuthProvider` seam), so a harness that needs tokens can
reuse it.

No silent retries (a transient failure is a real bug). Always end a
passing test with `project.finish()`.
