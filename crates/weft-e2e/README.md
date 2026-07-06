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
crates/weft-e2e/run-e2e.sh                # whole suite
crates/weft-e2e/run-e2e.sh live_chat      # one test, by file name
```

Need a subset or a flag? Add it to `run-e2e.sh`. Do NOT hand-write a `cargo test`
invocation. First run is slow (cluster bring-up + per-fixture worker-image
compiles); that is expected, never shortcut it.

## When the cluster looks wrong, fix the SETUP/SOURCE, not the cluster (HARD REQUIREMENT)

This is not advice, it is the protocol, and it is NOT optional. Time is NOT a
reason to deviate: a clean purge + reinstall + full e2e run is slow (cluster
bring-up, per-fixture worker-image compiles, serial scenarios), and that is
expected and fine. Always take the slow, reproducible path over a fast hand-patch,
so every fix is reproducible from source (a fresh machine or CI gets the same
working system).

The cluster is owned by `setup.sh` + the daemon. Your ONLY cluster operations:

```bash
./setup.sh --uninstall --purge   # full teardown (cluster + Postgres volume)
./setup.sh                       # fresh install / bring to current code (idempotent)
```

- **Never `kubectl apply/delete/edit/scale/rollout`, never `DROP`/`ALTER` the
  live DB, never hand-roll a port-forward.** Those patch the running cluster, not
  the source, so the fix evaporates on the next install and the bug ships.
- **A test failure or wrong cluster state is a bug in the code, a manifest,
  `setup.sh`, or the test toolkit. Fix it THERE**, then validate with purge +
  reinstall + re-run. A blocked pod -> the manifest; a missing column -> the
  `CREATE TABLE` + a purge; a wedged cluster -> a clean reinstall.
- **If a plain `./setup.sh` does NOT pick up your change, that is a `setup.sh`
  change-detection bug** (per-image stamp / migration). Fix the script so a fresh
  run produces the right state on its own, and keep re-running it until it does.
  Never patch the cluster to compensate, and never patch a test to tolerate the
  wrong state, if the test needs a capability the toolkit lacks, extend the
  toolkit (see "Add a test").

**DB schema changes REQUIRE a purge.** The Postgres volume is durable across
`setup.sh` runs and tables use `CREATE TABLE IF NOT EXISTS`, so a column change is
silently skipped without `--purge`. After touching any `CREATE TABLE` / migration:
`./setup.sh --uninstall --purge` then `./setup.sh`.

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

To write the `.weft` graph itself, see the language guide: `../../docs/weft-lang-
guide.md` (and `../../docs/authoring-nodes.md` for custom nodes). Get the project
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
