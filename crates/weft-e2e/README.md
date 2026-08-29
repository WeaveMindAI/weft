# weft-e2e: Layer-4 end-to-end tests

A test is plain Rust that prepares a fixture, drives it through the dispatcher's
public API exactly as the outside world sees it, and asserts. The rig brings the
cluster up on current code itself (`ensure::up()` runs `./setup.sh`), so you only
ever touch the two sanctioned commands below. Gated behind the `e2e` feature (off
by default), so `cargo test --workspace` compiles but runs none of it.

## Run

Always via the runner. It runs one test binary at a time and STOPS at the first
failure, leaving the cluster in that state for inspection:

```bash
scripts/run-e2e.sh                # whole suite
scripts/run-e2e.sh live_chat      # one test, by file name
```

Need a subset or a flag? Add it to `run-e2e.sh`. Do NOT hand-write a `cargo test`
invocation. First run is slow (cluster bring-up + per-fixture worker-image
compiles); that is expected, never shortcut it.

## Credentials, and what the runner provides for you

Everything a local machine can serve, the runner provisions itself and tears
down when the suite passes: the store's Postgres (a port-forward of the
cluster's own), and an S3 endpoint (the daemon's SeaweedFS container). You set
nothing for those.

What is left is the genuinely external services, which come from the
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

A test whose variables are absent **skips** rather than fails, through
`env_or_skip` / `env_group_or_skip`, so a partial `.env` still gets you a
useful run. Which also means a green suite does not prove the skipped ones
work: check the output for what skipped before you trust it.

This is a different mechanism from the node self-tests, which read
`WEFT_NODE_TEST_*` and can also use a connection you signed into in the editor.
Theirs is documented in [Testing a node](https://weavemindai.github.io/weft/nodes/testing.html#giving-the-live-tier-what-it-needs).

## When the cluster looks wrong, fix the source (HARD REQUIREMENT)

The rule and its whole list of never-dos live in
[CONTRIBUTING](../../CONTRIBUTING.md#working-on-the-cluster), because it holds
for every part of this repo, not just this suite.

The one thing worth repeating here: time is never a reason to deviate. A
reinstall and a full e2e run is slow (per-fixture worker image compiles, serial
scenarios), and that is expected. Take the slow reproducible path, so a fresh
machine and CI end up with the same working system you have.

```bash
./setup.sh                       # bring the cluster to the current code
```

`--uninstall --purge` exists for a genuine clean slate and takes every project,
execution and stored credential on the machine with it. It is not the way to
apply a change.

If a test needs a capability the toolkit lacks, extend the toolkit (see "Add a
test"). Never patch a test to tolerate the wrong state.

## Add a test

Add `tests/<area>.rs` (the runner discovers it; no list to update). Shape:

```rust
#![cfg(feature = "e2e")]
use weft_e2e::{ensure, project::Project, run};

#[tokio::test]
async fn my_scenario() -> anyhow::Result<()> {
    let disp = ensure::up().await?;                                // system up on current code
    let mut project = Project::prepare("my_fixture", disp).await?; // fixtures/my_fixture -> isolated project
    let settled = run::run_and_settle(&mut project).await?;        // drive it
    settled.completed()?;                                          // assert (folds the replay log)
    settled.assert_input("out", "data", &serde_json::json!("x"))?;
    project.finish().await                                         // teardown (pass only; a fail keeps it for inspection)
}
```

A **fixture** is `fixtures/<name>/` with a `weft.toml` (any id; the rig rewrites
it) + a `main.weft`; a custom node goes under `fixtures/<name>/nodes/<node>/`. For
a runtime value baked into the graph, put a `__E2E_TOKEN__` placeholder in
`main.weft` and call `project.substitute_in_main(...)` before building.

To write the `.weft` graph itself, see the language reference under
`../../docs/src/language/` (and `../../docs/src/nodes/` for custom nodes). Get the project
right there; a malformed `main.weft` fails at build, not as a test assertion.

**If a test needs something the toolkit doesn't have, extend the toolkit
(`src/`), not the test.** Keep test bodies about WHAT they assert; the HOW (HTTP,
SQL, kubectl) lives in the toolkit so it stays DRY and reviewable.

## Toolkit

| Module | What it does |
| --- | --- |
| `ensure` | Bring the system up on current code; wait for health. |
| `project::Project` | Fixture -> isolated project: `prepare`, `build`, `activate`, `weft(args)`, `substitute_in_main`, `finish`. |
| `run` | `run_and_settle`, `start`, `wait_for_triggered_execution`, `SettledRun::observe`. |
| `assert` (`SettledRun`) | `completed`, `failed_with`, `assert_input/output/skipped`, `assert_loop_iterations`. |
| `signal` | Discover + fire signals: `SignalScope`, `fire_webhook`, `fire_token`. |
| `live` | Live caller: `open_ws`, `http_post`. |
| `human` | Human-in-the-loop: `wait_for_form_by_node`, `answer_form`. |
| `fakes` | Servers the system dials OUT to: `SseFake`, `PollFake`, `SocketFake`, `BytesFake`. |
| `infra` | Infra lifecycle: `start_and_wait_running`, `call_endpoint`, `terminate_and_wait_gone`. |
| `storage` | `list`, `download`, `assert_file_contents`. |
| `platform::Platform` | Reaches BEHIND the API on purpose (below). |

Driving each trigger kind: **plain** `run::run_and_settle`; **HTTP**
`activate` + `live::http_post`; **WebSocket** `activate` + `live::open_ws`;
**human form** `human::wait_for_form_by_node` + `answer_form`; **dial-out
(SSE/poll/socket)** a `fakes::*` + `substitute_in_main` + `wait_for_triggered_
execution`; **infra** `infra::start_and_wait_running` + `terminate_and_wait_gone`.

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

No silent retries (a transient failure is a real bug, surfaced). Always end a
passing test with `project.finish()`. A bug found in the system is the rig
working: fix it at the source.
