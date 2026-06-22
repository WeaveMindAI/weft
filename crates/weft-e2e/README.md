# weft-e2e: the Layer-4 end-to-end test rig

This crate drives a REAL running Weft cluster (dispatcher + listener + worker
pods on a local kind cluster) and asserts behavior through the dispatcher's
public API, exactly as a user or the outside world would. It is the Layer-4 tier
of the testing pyramid: real binaries, real network, real backing services. It
exists so we stop testing the system by hand (create a project, click the graph,
fire it, eyeball the result) and instead express each scenario as code that runs
and asserts automatically.

## Running it

Run the suite with the stop-at-first-failure runner (the sanctioned workflow):

```bash
crates/weft-e2e/run-e2e.sh                # whole suite, stop on first failure
crates/weft-e2e/run-e2e.sh listener_move  # just these test binaries
```

It runs the tests one binary at a time and HALTS the instant one fails, leaving
the cluster in exactly the state that test left it so you can investigate. Do
NOT run the whole suite past a failure: the tests share one real cluster, so
continuing piles more state on top of the failure and buries the evidence (and
can cascade). A PASSING test cleans up after itself (its project is removed and
any pooled-pod clone it created is swept on its success path); a FAILING test
deliberately leaves its project + clones behind for inspection, which is why the
runner stops right there.

A single test binary directly (same `--test-threads=1` requirement):

```bash
cargo test -p weft-e2e --features e2e --test live_chat -- --test-threads=1
```

The bare `cargo test -p weft-e2e --features e2e -- --test-threads=1` runs every
binary but does NOT stop at the first failure, so prefer `run-e2e.sh`.

- **The `e2e` feature is OFF by default.** Without it, `cargo test --workspace`
  compiles this crate but runs none of its cluster-touching tests, so the normal
  workspace test run never needs a cluster.
- **`--test-threads=1`** is required: the tests share one cluster and drive
  global state (mount paths, the per-tenant listener). They are written to be
  isolated by project id, but run them serially until parallel isolation is
  proven out.
- **The first run is slow.** The rig calls `./setup.sh` once to bring the cluster
  to current code, and each fixture builds its own worker image on first touch
  (a real cargo-in-Docker compile). Subsequent runs reuse the cached images.

## Rules for driving the cluster (READ THIS)

The cluster is owned by `setup.sh` and the `weft daemon`. Your ONLY sanctioned
cluster operations are:

```bash
./setup.sh --uninstall --purge   # full teardown (cluster + Postgres volume)
./setup.sh                       # fresh install / bring to current code
```

Everything else flows from those two. The e2e rig runs `./setup.sh` for you (via
`ensure::up()`), waits for the dispatcher to be healthy, and reaches it through
the port-forward the daemon owns. So the normal loop is just: (purge if the
schema changed) then `cargo test -p weft-e2e --features e2e -- --test-threads=1`.

**Do NOT hand-patch the cluster to make a test pass.** Specifically, never:

- run `kubectl apply` / `kubectl delete` / `kubectl edit` / `kubectl scale` /
  `kubectl rollout restart` against a live resource (NetworkPolicy, Deployment,
  StatefulSet, pod, anything),
- `DROP`/`ALTER` the live Postgres schema, or otherwise mutate the DB by hand,
- start your own `kubectl port-forward` to the dispatcher (or set
  `WEFT_DISPATCHER_URL` to a hand-rolled forward), or otherwise route around the
  daemon's connection,
- restart `kube-proxy` / the CNI / individual pods to chase a networking symptom.

All of these are throwaway patches: they fix the running cluster, not the source,
so the fix evaporates on the next install and the bug ships. They also destabilize
the daemon (manually rolling the dispatcher kills the port-forward it manages,
which then looks like a flaky/unreachable dispatcher, a self-inflicted failure).

**If a test fails or the cluster misbehaves, the bug is in the code or in
`setup.sh` / a manifest, fix it THERE.** A NetworkPolicy that blocks a pod is
fixed in the manifest that renders it; a missing schema column is fixed in the
`CREATE TABLE` plus a purge; a wedged cluster is fixed by a clean reinstall, not
by poking it. Then validate the real way: `./setup.sh --uninstall --purge`,
`./setup.sh`, re-run. The whole point of the purge+reinstall discipline is that
every fix is reproducible from source, so a fresh machine (or CI) gets the same
working system.

**Time is not a constraint here.** A clean purge + reinstall + full e2e run is
slow (cluster bring-up, per-fixture worker-image compiles, serial scenarios).
That is expected and fine. Never take a manual shortcut to save time; always
prefer the slow, correct, reproducible path over a fast hand-patch.

## Fresh install after a database schema change (REQUIRED)

The cluster's Postgres volume is **durable across `setup.sh` runs** (only
`--purge` wipes it). The schema is created with `CREATE TABLE IF NOT EXISTS`, so
if you changed a table's columns (added/removed a column, changed a type) since
the last install, a plain `setup.sh` will NOT pick up the change: the table
already exists, the `IF NOT EXISTS` skips it, and the new column is silently
absent. The e2e run then fails with a `column "..." does not exist` (or a write
that hangs forever because a gate can never match).

So whenever the DB schema changed, do a clean reinstall before running the e2e:

```bash
./setup.sh --uninstall --purge   # tears down the cluster AND wipes the Postgres volume
./setup.sh                       # fresh install, recreates the full schema
cargo test -p weft-e2e --features e2e -- --test-threads=1
```

If you only changed Rust code (no schema change), a plain `./setup.sh` (which the
rig also runs automatically) is enough; the durable DB is fine. When in doubt
after touching any `CREATE TABLE` / migration code, do the purge: it is the only
way the canonical schema is guaranteed to match (this repo deliberately uses no
`ALTER TABLE` migrations, see the "No migration cruft" rule).

## How it works

A test is plain Rust. It prepares a fixture, drives it, and asserts. The shape:

```rust
#![cfg(feature = "e2e")]
use weft_e2e::{ensure, project::Project, run};

#[tokio::test]
async fn my_scenario() -> anyhow::Result<()> {
    let disp = ensure::up().await?;                       // 1. system up on current code
    let mut project = Project::prepare("my_fixture", disp).await?;  // 2. isolated project
    let settled = run::run_and_settle(&mut project).await?;         // 3. drive it
    settled.completed()?;                                 // 4. assert
    settled.assert_input("out", "data", &serde_json::json!("expected"))?;
    project.finish().await                                // 5. teardown (pass only)
}
```

1. **`ensure::up()`** runs `./setup.sh` once per test process (idempotent: a
   no-op when the cluster is already current) and waits for the dispatcher's
   `/health`. Every test calls it; only the first does the work.
2. **`Project::prepare("name", disp)`** copies `fixtures/name/` to a temp dir,
   rewrites its `weft.toml` id to a fresh UUID (so runs never collide), and runs
   `weft catalog update` to refresh the built-in nodes from THIS worktree's code
   (custom nodes the fixture commits are preserved). This is what makes the rig
   test current node code, not a stale mirror.
3. **Drive it** with the toolkit (below).
4. **Assert** on the settled run (below).
5. **`project.finish().await`** tears down a PASSING test: `weft rm` the project
   and delete the temp copy, all awaited. A test that panics / returns early
   never reaches `finish`, so its project and temp dir are KEPT for inspection
   (the `Drop` impl logs exactly where to look and how to clean up).

## The toolkit (what you call from a test)

| Module | What it does |
| --- | --- |
| `ensure` | Bring the system up on current code; wait for health. |
| `project::Project` | Fixture -> isolated project: `prepare`, `build`, `activate`, `weft(args)`, `substitute_in_main`, `unique_live_path`, `finish`. |
| `run` | `start` a plain run, `run_and_settle`, `wait_for_triggered_execution` (for external-fired runs), `SettledRun::observe`. |
| `SettledRun` (in `assert`) | `completed()`, `failed_with(needle)`, `assert_input(node, port, v)`, `assert_output(node, v)`, `assert_skipped(node)`, `assert_loop_iterations(group, n)`, `completed_outputs()`. |
| `signal` | Discover signals via a project-scoped api token: `SignalScope` (`open`, `signal_for_node`), plus `fire_webhook` / `fire_token`. |
| `live` | Live caller: `open_ws` (WebSocket), `http_post` (HTTP), the handshake. |
| `human` | Human-in-the-loop: `wait_for_form_by_node`, `answer_form`. |
| `fakes` | Throwaway servers the system dials OUT to: `SseFake`, `PollFake`, `SocketFake`, `BytesFake`. Cluster-reachable (bound on the host, advertised at the kind host-gateway IP). |
| `infra` | Infra lifecycle: `start_and_wait_running`, `call_endpoint`, `terminate_and_wait_gone`. |
| `storage` | `list`, `download`, `assert_file_contents` for stored files. |
| `bus` (in `bus`) | `assert_bus_conversation`, `bus_messages`, `bus_closed` over the event log. |
| `platform::Platform` | **Reaches BEHIND the API** (see "Platform layer" below): `worker_pods_for_project`, `spawn_attempts`, `kill_workers`, `restart_dispatcher`, `make_worker_pods_stale`. |

Most of the toolkit reads back through the dispatcher's public API. Assertions
fold the `/executions/{color}/replay` event log; a check is `Result`, so a test
`?`s it and a failure carries the relevant slice of the replay. The one
exception is the platform layer, which deliberately reaches behind the API.

## Platform layer (reaching behind the API, on purpose)

The toolkit above tests the **program** layer: what a `.weft` program does,
asserted through the public API exactly as the outside world sees it. The
`platform` module tests the **platform** layer: what the SYSTEM does underneath
a running program (which worker served an execution; whether a crash was
recovered; whether an idle resource was reaped and woke again).

Those facts are NOT on the public API, by design: exposing "list the workers for
this execution" or "force a reap" would add privileged endpoints to the shipped
system (attack surface) for a need only tests have. So the platform layer reaches
behind the API the way an operator with cluster credentials would:

- **Reads the cluster's Postgres directly** (via a `kubectl port-forward` the
  `Platform` owns and tears down on drop): `worker_pods_for_project`,
  `spawn_attempts`, for OBSERVING worker lifecycle. NOTE: these are observation
  helpers, not resume-gates. A worker pod's name is deterministic from its spawn
  task, so a respawn reuses the SAME name, the dead row is GC'd within seconds,
  and the spawn-retry counter is timing-dependent. So none of them is a stable
  "a resume happened" fingerprint. The `resume` test instead proves resume by
  INFERENCE (kill the only live worker while parked, then assert the execution
  still completed correctly, a dead worker can't finish a job).
- **Drives pods with `kubectl`** from the host. `kill_workers(project)` fakes a
  worker crash; `restart_dispatcher()` fakes a local update (off then on).
- **Fakes "time passed" by DATABASE BACKDATING**, never a clock hook. To make a
  resource look idle without waiting the real interval, a helper shifts the
  timestamp column the reaper reads (e.g. `make_worker_pods_stale` backdates
  `last_heartbeat_unix` past `HEARTBEAT_STALE_SECS`, importing the reaper's OWN
  constant so it can't rot). There is no clock-freeze endpoint anywhere.

**Safety invariant: none of this ships.** The `platform` module and its `sqlx`
dependency compile ONLY under the `e2e` feature, and this whole crate is never
compiled into any image (the Dockerfiles build `--release -p weft-dispatcher`
with no `--features`). A normal `setup.sh` / image build cannot produce a binary
that exposes any of it. The privileged surface exists solely in host-side test
code, gated off the production build.

**Scope is LOCAL reliability only.** This layer covers the crash/restart/reap
situations that happen on one machine: a worker crashes mid-execution and the
job resumes (the `resume` test); a local dispatcher update; idle-reap then
cold-spawn. At-scale behavior (multi-Pod lease takeover, multiple dispatcher
copies, multi-tenant isolation, concurrency-under-contention) is OUT: the
open-source local product is single-everything and does not scale; those belong
to the future closed-source cloud testbench. The read/backdate/kill helpers are
shaped to grow into the local-restart and idle-reap tests (currently only the
worker-crash-resume test is written; dispatcher-restart and idle-reap are
ENABLED by the toolkit but not yet written).

## Adding a fixture + test

1. **Fixture**: create `fixtures/<name>/` with a `weft.toml` (any fixed id, the
   rig rewrites it) and a `main.weft`. Do NOT commit `nodes/base_catalog/`, it is
   regenerated. If the fixture needs a custom node, add it under
   `fixtures/<name>/nodes/<node>/` (`metadata.json` + `mod.rs`); it compiles into
   the worker and survives `catalog update`.
   - Need a runtime value baked into the graph (a fake server URL, a unique mount
     path)? Put a placeholder token like `__E2E_FAKE_URL__` in `main.weft` and
     have the test call `project.substitute_in_main(...)` before building.
2. **Test**: add `tests/<area>.rs` (top line `#![cfg(feature = "e2e")]`), one
   `#[tokio::test]` per scenario, following the shape above.

## Driving each trigger kind

- **Plain run**: `run::run_and_settle(&mut project)`.
- **Web request (live HTTP)**: `project.activate()`, then
  `live::http_post(&disp, path, &body)`.
- **Live chat (WebSocket)**: `project.activate()`, then `live::open_ws`,
  `ws.request_json(...)`, `ws.close()`.
- **Human form**: start the flow (an event / trigger), then
  `human::wait_for_form_by_node` + `human::answer_form`, then observe the run.
- **Feed the system dials out to (SSE/poll/socket)**: stand up a `fakes::SseFake`
  / `PollFake` / `SocketFake`, substitute its URL into the fixture, activate,
  snapshot `run::execution_colors`, push an event, then
  `run::wait_for_triggered_execution`.
- **Infra**: `infra::start_and_wait_running`, run against it, then
  `infra::terminate_and_wait_gone`.

## Notes

- **No retries hide failures.** The rig never wraps an operation in a silent
  retry to paper over a flaky system, a transient failure is a real bug, and the
  rig surfaces it. (The system's OWN documented recovery, e.g. ensuring a storage
  box before a write, is the system's job, not the rig's.)
- **Teardown is explicit.** Always end a passing test with `project.finish()`.
- **Bug found in the system?** That is the rig working. Fix it at the source.
