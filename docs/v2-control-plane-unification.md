# Plan: control-plane unification refactor

A multi-part refactor that fixes several drift issues at once. Each part is a substantial piece of work; together they form one coherent direction.

## Parts

1. **CLI as single client**: extension shells out to CLI; one path for every action-bar verb (run, activate, infra lifecycle, etc.).
2. **Drift-aware action bar**: subgraph hashes drive Upgrade/Resync buttons; dispatcher returns the available_actions list.
3. **Hash-tagged worker + sidecar images**: replaces `:latest`; image cache survives across CLI/extension; sidecar rebuild gated by hash.
4. **Atomic activate + atomic InfraSetup on failure**: fix two existing bugs where partial failure leaks signals or pods.
5. **Architecture 4 signal-processing refactor**: dispatcher becomes pure transport (relay + parking gate + journal); listener becomes the kind-aware processor (form, webhook, timer, sse, socket, future browser session). Merge signal+suspension tables. Webhooks are restructured to flow through dispatcher (not landing directly on listener), so parking and the same routing rules apply uniformly. Listener loses Postgres write authority.
6. **Preservation modes** (WIPE / HIBERNATE / PARK): choose at deactivate/resync/upgrade time. Lossless via parked_payload column. User-explicit at every transition.
7. **State-machine enforcement**: dispatcher refuses bad transitions; CLI/extension prompt explicitly for confirmation when triggers must drop.

## Context

Today the VS Code extension and the CLI both talk to the dispatcher independently, with different build behavior. The extension hashes project inputs and skips rebuild when nothing changed. The CLI rebuilds unconditionally. The extension does NOT build sidecar images on `infra start`; the CLI does. Two paths, two implementations, drifting.

Independently, the dispatcher's signal-processing has accumulated a half-finished refactor: extension-driven HumanQuery completion goes through `/ext/{tk}/tasks/{color}/complete` (direct dispatcher journaling); webhook fires go through the listener's `direct_fire` (listener writes Postgres directly). Two parallel paths writing the same canonical journal events. The listener spawns even when nothing will ever hit its user_url. Suspension and signal tables overlap heavily.

We want **one path** for action-bar verbs (CLI as the client) AND **one path** for signal processing (architecture 4: dispatcher relays, listener processes). The CLI/extension unification is the entry point that surfaces the architectural debt; tackling both at once is the right scope.

We also want a **drift-aware action bar**: the dispatcher's status endpoint returns drift signals computed from subgraph hashes. The action bar lights up "Resync" or "Upgrade" buttons when the running version differs from the source. And we want **preservation modes** (WIPE/HIBERNATE/PARK) so upgrades don't lose in-flight HumanQuery work.

## Decisions locked from conversation

**Two drift signals, two buttons, both can light independently:**
- **Upgrade Infra** lights when the *infra subgraph* hash drifted AND infra is running. Effect: stop infra (deactivates triggers), rebuild + push sidecar images that drifted, start infra fresh with new worker binary. Resolves both drift indicators by side effect.
- **Resync Triggers** lights when the *trigger+fire subgraph* hash drifted. Effect: deactivate (drops registered signals) then reactivate (registers fresh signals against the new worker binary). Independent of infra state. Resolves only the trigger+fire drift.
- When BOTH are drifted, BOTH buttons light. User picks. If user clicks Resync alone and the trigger subgraph closure references infra that needs to come up (e.g. genuinely new infra node not yet provisioned), TriggerSetup phase fails at runtime and the failure is surfaced loudly. User must then click Upgrade. Mixed-version (old infra binary + new fire/trigger binary) is fine when the drift is purely fire-side because infra Pods aren't actively executing the fire/trigger graph after provisioning.

**Atomic activation.** Today, when TriggerSetup fails mid-way, half-registered signals leak into the listener while the project status stays at its previous value. We need to make activation atomic: on TriggerSetup failure, the activate handler runs the same cleanup as `deactivate_project()` (drop entry tokens, unregister signals from listener, delete signal rows for the project). User sees "activate failed: <error>", project state is fully clean, they can edit + retry. Same for the InfraSetup phase actually, if the infra setup fail mid exec, it must also clean up the other stuff that did start correclty.

**Special case: project has infra defined in source but infra is NOT running, AND triggers are currently active** (e.g. user added an infra node to a previously trigger-only activated project). Action bar shows ONLY "Resync" + "Deactivate". All infra controls and Activate are hidden until triggers are deactivated. Both Resync and Deactivate drop the active triggers; after that the bar transitions to its normal "infra-defined-but-not-running" state with Start Infra + (disabled) Activate visible.

**No magic, no implicit state changes. Every action is explicit and confirmed.**

- `infra/start`: refuses with 412 if triggers are active. User must explicitly deactivate or resync first. The dispatcher will not silently drop triggers on the user's behalf.
- `infra/stop`, `infra/terminate`, `infra/upgrade`: if triggers are currently active, the CLI/extension shows a confirmation dialog ("This will deactivate your active triggers. You'll need to reactivate manually after [the relevant action] completes. Continue? Y/n") BEFORE making the dispatcher call. On confirm, the dispatcher request includes an explicit `deactivate_triggers: true` flag in the body. If the request omits the flag and triggers are active, dispatcher returns 412 instructing the caller to either deactivate first or pass the flag explicitly. This makes the trigger drop visible at every layer: user sees the prompt, CLI sees the flag, dispatcher sees the flag.
- After any infra lifecycle completes, triggers stay deactivated. User manually reclicks Activate when ready. No auto-reactivate.

**Sidecar PVC reattach** is the infra node's `execute()` body's responsibility. Dispatcher always re-runs InfraSetup on start/upgrade; nodes detect existing PVC data and reattach vs init fresh.

**Signal-processing architecture refactor (Architecture 4: dispatcher = transport, listener = processor).**

The current code has a vestigial design: the `register_signal` task always spawns the listener and POSTs `/register`, even for HumanQuery resumes that are completed via the extension's direct dispatcher route (`/ext/{tk}/tasks/{color}/complete`). The listener's user_url is minted but never called for those flows. Two parallel paths exist (listener-via-`direct_fire` AND extension-via-`complete_task`), both writing the same canonical `SuspensionResolved` journal event. Half-finished refactor.

**The clean model we commit to (matches Quentin's invariant: dispatcher is a stable monolith, listener grows with new signal kinds):**

```
Stateless signals (one-shot: webhook, form, future single-shot kinds):
  caller → dispatcher /signal/{token} → listener /process → dispatcher → worker
            (park gate)                  (kind-specific)     (journal +
                                                              ensure_worker)

Stateful signals (held connections: timer, sse, socket, future browser session):
  external client ─► listener (held connection, lives in user namespace)
                          │
                          │ when held event fires:
                          ▼
                    dispatcher /signal/internal-resume
                          │
                          ▼
                       worker
```

**Webhook routing change.** Today, webhook URLs minted by the listener look like `https://listener.tenant.example/signal/{token}`. External callers (Slack, Stripe, GitHub, etc.) POST directly to the listener. Under architecture 4, **webhooks flow through the dispatcher first** so the parking gate and the uniform routing apply. The user-facing URL becomes `https://dispatcher.example/signal/{webhook_token}` (or similar; the exact host depends on deployment). The dispatcher receives the POST, looks up the token in the signal table, applies the preservation_mode gate (park if hibernating, else relay), and forwards to the listener's `/process` for kind-specific handling. The listener returns the action; dispatcher journals.

Implication: when `register_signal` task processes a Webhook kind, the user_url it stores in the signal row points at the dispatcher, not the listener. The listener still maintains its registry entry for the kind (so it can process when called), but it no longer hosts the externally-visible URL. External webhook providers re-configured at registration time will hit the new dispatcher URL.

**Properties**:
- **Dispatcher knows nothing about signal kinds.** It's a transport: route by token, gate by preservation_mode, journal results, ensure worker. New kinds land entirely in the listener; dispatcher code unchanged.
- **Listener owns kind-specific logic.** Form validation, webhook payload shaping, timer schedules, future browser-session protocol. All in `crates/weft-listener/src/kinds/`.
- **Extension always talks to dispatcher.** Never directly to listener. (Exception, future: browser-session frame stream uses a per-session signed URL minted by dispatcher; extension hits that directly. Documented in the WeaveMind ROADMAP. Out of scope here.)
- **Stateful kinds opt out of parking.** They declare `parkable: false` in their kind metadata. Upgrade path checks for active stateful holds via listener; refuses upgrade if any are live.

**Listener's `/process` response shape** (`ProcessOutcome`: orthogonal split between processing and routing).

The shape has two fields:
- `value: <json>`: what the listener computed from the payload. Today most kinds echo the raw payload; future kinds may validate against a schema, decorate with metadata, sign, or shape across a multi-step protocol. The dispatcher writes this verbatim into the journal.
- `target: <ProcessTarget>`: where the dispatcher should route this fire. Independent of kind. New routing targets land without touching kind code; new kinds land without touching dispatcher routing.

`ProcessTarget` variants:
- `{ kind: "resume", color: "...", node_id: "..." }` → dispatcher journals SuspensionResolved + ensure_worker, using `outcome.value` as the resume value.
- `{ kind: "entry", node_id: "..." }` → dispatcher enqueues route_entry task with `outcome.value` as the payload.
- `{ kind: "drop", reason: <optional string> }` → dispatcher does nothing. Covers Hold (multi-step protocol still in progress) AND NoOp (duplicate fire, unknown token, stateful kind misused). Optional reason is for ops logging.
- 4xx → dispatcher returns error to caller as-is.

**Why the split**: keeps the listener kind-aware (it computes `value` per kind) and the dispatcher kind-unaware (it acts on `target` regardless of kind). Conflating them (where each variant carries both "what I did" and "what you do next") couples future routing changes to every kind module. The split is the architectural rule "dispatcher is monolithic, listener grows with kinds" expressed in the wire shape.

**Merge `signal` and `suspension` tables.** Today they overlap heavily for resume-kind signals (same token, different metadata fields). We merge into one `signal` table with `metadata JSONB NULL` (only set for resume rows). `list_open_suspensions` becomes `SELECT FROM signal WHERE is_resume=true AND parked_payload IS NULL`.

**Suspension and signal preservation across deactivate/resync/upgrade.**

Today, every deactivate path wipes all signals AND cancels all in-flight executions including suspended ones. This loses HumanQuery forms and in-flight resume work. Bad for upgrade scenarios where the user wants in-flight work to survive.

**The three modes** (user picks at deactivate/resync/upgrade time). All three kill the listener Pod. Difference is how dispatcher handles incoming signals + how it advertises pending tasks.

- **WIPE**: delete all signal rows from DB. Cancel all in-flight, including suspended. Incoming `/signal/{token}` requests return 404 (token gone). `/ext/{tk}/tasks` returns empty list. Reactivate is fully fresh.
- **HIBERNATE**: keep signal rows in DB. Incoming stateless signals get parked (payload written to `parked_payload` column on the signal row, return 200 "queued"). `/ext/{tk}/tasks` returns empty list (filtered by `preservation_mode = 'hibernate'`). User can't see pending forms in the extension. BUT if a user already had a form open from before deactivation and submits anyway, the submission gets parked (NOT lost). Rationale: a HumanQuery form may represent hours of human thought; we cannot lose it.
- **PARK**: keep signal rows. Same parking behavior as HIBERNATE on incoming. Difference: `/ext/{tk}/tasks` continues to show pending forms. Users can open new forms and submit during the inactive window; submissions park.

Implementation point: dispatcher's `/signal/{token}` POST handler treats HIBERNATE and PARK identically (both park, no relay to listener). The pending-tasks query is the single divergence: HIBERNATE filters out, PARK includes. Branch is `if preservation_mode IN ('hibernate', 'park') then park else relay to listener`.

**Reactivate-time prompt** (only shown if there's preserved state):
```
Preserved during inactive window:
  - 3 parked signals  (queued submissions; will execute on reactivate)
  - 18 pending suspensions  (signals registered, no submission yet)

Choose:
  1) Execute parked + keep suspensions
  2) Keep suspensions only, drop parked
  3) Wipe all and start fresh
```

User picks + validates explicitly (no default preselected). If 0 parked AND 0 suspensions: no prompt, reactivate proceeds.

**Resume signal node_id matching**: when a resume fires (immediately on reactivate for parked, or later for still-pending), dispatcher relays the parked payload to the listener's `/process` with the original spec. Listener returns `resolve_suspension` with the color. Dispatcher looks up the color's suspended state, finds the node_id in the journal, attempts to resume. If the new graph doesn't have that node_id, the resume fails, journals `ExecutionFailed`, surfaces as red entry in execution history. Doesn't block other resumes.

**Journal display compatibility**: past journaled events that reference nodes/edges absent from the new graph don't render in the graph view (graph view filters by current node IDs). DB rows stay. Future inspector can surface them with a red badge; out of scope here.

**Schema changes**:
- `project` table: add `preservation_mode TEXT NOT NULL DEFAULT 'none'` (values: `none | hibernate | park`). Set on deactivate/resync/upgrade per user choice. Reset to `none` on reactivate or wipe.
- Merge `signal` and `suspension` tables: keep `signal` table, add `metadata JSONB NULL` column (was the `metadata` field of the suspension row), add `parked_payload JSONB NULL` column. Drop the `suspension` table. Migration moves existing suspension rows into the corresponding signal rows.

**Image registry, naming, hash inputs:**
- Worker image: `weft-worker-<project-id>-<source-hash>` where source-hash includes main.weft + weft.toml + nodes/ + engine fingerprint. (Today's tag is `weft-worker-<id>:latest`: we move to hash-tagged.)
- Sidecar image: `weft-sidecar-<sidecar-name>-<sidecar-hash>` where sidecar-hash includes the sidecar Dockerfile + sidecar source dir contents + catalog version. Catalog sidecars get shared hashes across users naturally; user-defined sidecars get unique hashes.
- For OSS / kind: "registry" is `kind load docker-image`. Same hash naming, different transport.
- For cloud later: per-user-namespace registry pull credentials. Out of scope for this plan but the hash-based naming is forward-compatible.

**Versioning of nodes is deferred.** When a registry with versioned nodes lands, the drift logic will need to split user-source-drift from upstream-node-version-drift. For now, drift treats any change as "new version." TODO comment in the hash function points at this future split.

**No new tests beyond verifying the new code paths work end-to-end.** Quentin tests manually.

## Architecture

### CLI (single source of truth)

Every action-bar verb has a CLI command. Each runs the full sequence: discover, hash, skip-or-build, push, post to dispatcher, optionally follow.

Verbs:
- `weft run`: Fire phase. Build worker image if drifted, push, POST `/projects/{id}/run`, follow.
- `weft activate`: TriggerSetup phase. Build worker image if drifted, push, POST `/projects/{id}/activate`. (Dispatcher refuses if requires_infra nodes lack running infra.)
- `weft deactivate`: POST `/projects/{id}/deactivate`. No build. CLI prompts user to pick `WIPE | HIBERNATE | PARK` if suspensions/signals exist; passes the choice as `preservation_mode: "wipe" | "hibernate" | "park"` in body.
- `weft resync`: POST `/projects/{id}/resync`. New endpoint. Internally: deactivate + (if conditions met) reactivate. Build worker image if drifted before reactivation step. CLI prompts at the deactivate step (mode choice) and again at the reactivate step (option 1/2/3 from the preservation model below) if there's preserved state.
- `weft infra start`: InfraSetup phase. Build worker image if drifted (because subworkflow lives in same binary), build + push every drifted sidecar image, POST `/projects/{id}/infra/start`. Dispatcher refuses with 412 if triggers are currently active. Error message instructs user to run `weft resync` or `weft deactivate` first. Never silently drops triggers.
- `weft infra stop`: POST `/projects/{id}/infra/stop`. No build. If triggers are active: CLI prompts user to pick deactivation mode (WIPE / HIBERNATE / PARK). Body includes `deactivate_triggers: true` and `preservation_mode`.
- `weft infra terminate`: POST `/projects/{id}/infra/terminate`. No build. Same prompt flow as stop. Warning in the prompt: "terminate destroys infra PVCs; PARK/HIBERNATE will preserve signals but resume payloads may fail post-reactivate if they depend on infra-stored data."
- `weft infra upgrade`: POST `/projects/{id}/infra/upgrade`. New endpoint. Build worker + drifted sidecars, push, then dispatcher does atomic stop+swap+start. Same prompt flow as stop. PARK is the natural choice for upgrade (whole point is keeping in-flight work alive).
- `weft status`: JSON. Returns project lifecycle state + drift signals + available actions.

All verbs accept `--json` flag. JSON mode emits structured progress events to stdout (one JSON object per line: `{"phase":"build", "detail":"..."}`, `{"phase":"post", "detail":"..."}`, `{"phase":"follow", "color":"abc", "events":[...]}`). Non-JSON mode keeps current human-readable output.

### Extension (thin shell)

`graphView.ts::callProjectLifecycle` and `callInfra` are deleted. `tryRunOnce`, `ensurePinnedBuild`, `hashProjectInputs`, `weftBinaryFingerprint`, `cacheKey` are deleted. Each action-bar button shells out via a new `runWeftCliJson(args)` helper that spawns `weft <verb> --json`, parses each stdout line as JSON, and posts progress events to the webview.

Action bar polls `weft status --json` on graph open and after every action, renders buttons based on the returned `available_actions` array. No client-side state machine.

### Dispatcher (state machine + drift detection + atomic activate)

New columns on existing tables:
- `infra_pod`: add `running_image_hash TEXT NULL` (sidecar hash actually deployed).
- `project`: add `running_worker_hash TEXT NULL` (worker image hash actually used at activate / infra-start time).

New endpoints:
- `POST /projects/{id}/resync`: body `{ worker_hash }`. Atomic deactivate + reactivate against new binary. Refuses with 412 if project has infra nodes and infra is not running (returns "deactivate succeeded, reactivate blocked: start infra first"). On TriggerSetup failure, same atomic-cleanup as activate.
- `POST /projects/{id}/infra/upgrade`: body `{ worker_hash, sidecar_hashes: {<node_id>: <hash>} }`. Atomic stop + swap sidecar image tags + start (re-runs InfraSetup). Refuses with 412 if any triggers active.
- `GET /projects/{id}/status`: extended response. Returns:
  ```json
  {
    "project": { "status": "registered|active|inactive", "running_worker_hash": "..." },
    "infra": { "nodes": [...], "rollup": "running|stopped|partial|none" },
    "triggers": { "registered_count": N },
    "drift": {
      "infra_subgraph_hash": "<desired>",
      "trigger_fire_subgraph_hash": "<desired>",
      "infra_drift": bool,
      "trigger_fire_drift": bool
    },
    "available_actions": ["resync", "upgrade", "deactivate", ...]
  }
  ```
  The CLI passes computed desired hashes as query params on `/status?desired_infra_hash=X&desired_tf_hash=Y`. Dispatcher echoes drift bits + computed available_actions back.

State-machine enforcement (dispatcher-side):
- `activate`: refuses with 412 if any requires_infra node lacks running infra. (Already exists.) Accepts `reactivate_choice: "execute_parked_keep_suspended" | "keep_suspended_only" | "wipe_all"` in body when there's preserved state from a prior PRESERVE deactivate. Refuses with 412 if preserved state exists and no choice was provided.
- `infra/start`: refuses with 412 if triggers currently active. Body explains: "deactivate or resync first." (NEW.) Rationale: triggers are running on a worker binary that may not even know about the infra nodes about to come up; refusing keeps the user in control.
- `infra/stop`, `infra/terminate`, `infra/upgrade`: if triggers are active, refuse with 412 unless the request body contains `deactivate_triggers: true` AND `preserve: bool`. With the flags, deactivate triggers as a first step (using the chosen WIPE or PRESERVE semantics) then proceed.
- `resync`: deactivate step uses `preserve: bool` from body. Reactivate step: refuses if requires_infra lacks running infra (412, project ends in deactivated state). Otherwise consumes `reactivate_choice` if there's preserved state.
- `deactivate`: accepts `preserve: bool` in body. Refuses with 412 if there are suspensions and the body doesn't include the flag (force user to choose explicitly).

**Atomic activate fix (existing bug, tied to this work):**
In `crates/weft-dispatcher/src/api/project.rs::activate`, today: when `run_trigger_setup` returns `Err` at line 831, the `?` propagates and the handler returns 500. Status is not flipped to Active (good). BUT any `register_signal` tasks that already ran during the failed sub-exec leave rows in the `signal` table and entries in the listener. Fix: wrap the `run_trigger_setup` call so that on Err, before returning, we call the same cleanup logic as `deactivate_project` (cancel in-flight executions, drop entry tokens, unregister signals from listener, delete signal rows). Apply the same fix to the new `resync` endpoint's reactivation step.

**Atomic InfraSetup fix (symmetric):**
Today, when InfraSetup fails mid-execution, some `provision_sidecar` tasks may have already succeeded (sidecar Pods/Services/PVCs created, infra_pod rows inserted with status Running) before another node failed. Those leak. Fix: in `crates/weft-dispatcher/src/api/project.rs::run_infra_setup`, on ExecutionFailed, identify the infra_pod rows that were inserted/updated to Running during this sub-exec and roll them back: scale to zero or delete depending on intent. Cleanest approach: track the `infra_pod` rows touched during the InfraSetup color (via the row's `created_at_unix` matching the sub-exec start, or by storing the originating color on the row), and on failure call the same scale-to-zero (or delete) path as `infra::stop` for just those rows. User sees "infra start failed: <error>", project + infra state are clean, retry possible.

Manifest changes:
- Worker image: `weft-worker-<project_id>-<hash>` instead of `:latest`. The dispatcher receives the hash as part of every spawn request body (run/activate/infra-start/infra-upgrade/resync). Stored on project.running_worker_hash. Used in k8s_worker manifest.
- Sidecar image: receives per-node hash from the CLI in infra/start and infra/upgrade payloads. Stored on infra_pod.running_image_hash. Used in sidecar manifest.

## Hash function (Rust, in CLI)

New module: `crates/weft-cli/src/hash.rs`. Uses SHA-256 (not DefaultHasher). Replaces `weft-cli/src/images.rs::hash_inputs`.

Three exposed functions:
- `compute_worker_hash(project_root: &Path) -> String`: hashes main.weft + weft.toml + every file under nodes/ + weft binary fingerprint. (Matches what the extension does today.)
- `compute_infra_subgraph_hash(project: &ProjectDefinition, project_root: &Path) -> String`: hashes the closure of infra nodes (every node where `requires_infra: true` plus their upstream) + every sidecar source dir for those infra nodes + engine fingerprint.
- `compute_trigger_fire_subgraph_hash(project: &ProjectDefinition, project_root: &Path) -> String`: hashes the closure of trigger nodes + every other reachable node in the fire graph + engine fingerprint.

`compute_sidecar_hash(node_type: &str, sidecar_source_dir: &Path) -> String`: hashes sidecar Dockerfile + sidecar source dir + catalog version. One per drifted sidecar.

TODO comment in the hash module: `// When we add registry-versioned nodes, split user-source-drift from upstream-node-version-drift here.`

## Build path (CLI)

Rewrite `crates/weft-cli/src/commands/ensure.rs` and `commands/build.rs`:

`ensure_built_then_post(verb, ctx) -> Result<HandleAndHashes>`:
1. Discover project.
2. Compute worker hash + (if relevant verb) sidecar hashes.
3. For worker image: check if `weft-worker-<id>-<hash>` exists in docker (or kind). If yes, skip. If no, compile + docker build with hash-tagged name + kind load (or push).
4. For each sidecar image (only on infra-start / infra-upgrade): same check, build, load.
5. Register project with dispatcher (existing flow, idempotent).
6. Return handle + computed hashes (used to put hashes in subsequent POST body).

Each verb command (run, activate, etc.) calls `ensure_built_then_post`, then makes its specific dispatcher call passing the hashes in the request body, then optionally follows.

The `weft build` standalone command becomes essentially `ensure_built_then_post` minus the dispatcher POST. Useful for users who want to pre-warm the cache.

## Files to modify (Rust)

- `crates/weft-cli/src/commands/ensure.rs`: rewrite to do hash-first, skip-if-image-exists, build-if-needed.
- `crates/weft-cli/src/commands/build.rs`: rewrite `ensure_worker_image` to take hash-tagged names and check existence first.
- `crates/weft-cli/src/commands/run.rs`, `activate.rs`, `deactivate.rs`, `infra.rs`: pass hashes to dispatcher in request body.
- `crates/weft-cli/src/commands/resync.rs`: NEW. Mirrors activate path but calls `/resync`.
- `crates/weft-cli/src/commands/status.rs`: extend output to include drift bits + available_actions when `--json`.
- `crates/weft-cli/src/main.rs`: add `resync` subcommand, add `infra upgrade` subcommand, add `--json` flag to all action verbs.
- `crates/weft-cli/src/hash.rs`: NEW. Three subgraph hash functions.
- `crates/weft-cli/src/images.rs`: keep, repurpose (`docker_image_exists`, `kind_image_loaded`, etc).

- `crates/weft-dispatcher/migrations/`: NEW migration adding `infra_pod.running_image_hash`, `project.running_worker_hash`, `project.preservation_mode TEXT NOT NULL DEFAULT 'none'`, and `signal.parked_payload TEXT NULL` columns.
- `crates/weft-dispatcher/src/api/project.rs`: 
  - `register()`, `run()`, `activate()`: accept `worker_hash` in body, store on project.
  - `activate()` atomic-cleanup on TriggerSetup failure (existing bug fix).
  - NEW `resync()` handler with same atomic-cleanup semantics.
- `crates/weft-dispatcher/src/api/infra.rs`:
  - `start()`, `upgrade()`: accept `worker_hash` + `sidecar_hashes` in body, refuse if triggers active.
  - NEW `upgrade()` handler doing atomic stop+swap+start.
  - Confirm `stop()` deactivates triggers (it already does).
- `crates/weft-dispatcher/src/api/mod.rs`: extend `/status` response shape (drift signals, available_actions, parked_count, suspended_count), accept `?desired_infra_hash=...&desired_tf_hash=...` query params. Add new route `POST /signal/{token}` (the dispatcher entry for stateless signals) and `POST /signal/internal-resume` (listener-to-dispatcher callback for stateful signal resumes).
- `crates/weft-dispatcher/src/api/signal.rs` (NEW or extended):
  - `POST /signal/{token}`: read signal row, branch on project's preservation_mode. If hibernate/park: write payload to `parked_payload` column, return 200 "queued." Else: relay to listener's `/process` endpoint, inspect returned action, journal accordingly (resolve_suspension → SuspensionResolved + ensure_worker; fire_entry_trigger → enqueue route_entry; hold → 200; no_op → 200).
  - `POST /signal/internal-resume`: trusted endpoint for listener callbacks (held connections that need to wake an execution). Same action discriminant as `/process` response.
- `crates/weft-dispatcher/src/api/extension.rs`:
  - DELETE `complete_task` direct-journaling path. Replace with: extension's `/ext/{tk}/tasks/{color}/complete` becomes a thin wrapper that forwards to `POST /signal/{token}` (dispatcher's own stateless-signal route). One signal-processing path, one parking gate.
  - `list_tasks`: forward to listener's `/pending` endpoint with tenant scope. Filter out projects where `preservation_mode = 'hibernate'` either at dispatcher level or via a flag passed to listener.
- `crates/weft-dispatcher/src/journal/postgres.rs`:
  - Migration: drop `suspension` table; add `metadata JSONB NULL` and `parked_payload JSONB NULL` columns to `signal` table; move existing suspension rows into corresponding signal rows.
  - `list_open_suspensions` → renamed `list_pending_signals(project_id, include_hibernated: bool)`. Returns rows where `is_resume=true AND parked_payload IS NULL` (and optionally filtered by preservation_mode).
  - New helper `list_parked_signals(project_id) -> Vec<(token, payload, spec)>` for the reactivate drain path.
  - New helper `clear_parked_payload(token)` to mark a parked signal as drained.
- `crates/weft-dispatcher/src/task_kinds/register_signal.rs`: simplify. Always POST `/register` to listener (listener is the kind-aware processor; it needs to know about every signal). User_url returned by listener is stored in signal row; some kinds (Form-resume) don't expose a public URL but the field stays for kinds that do (Webhook, public Form). Drop the path that conditionally skipped listener for resume signals.
- `crates/weft-dispatcher/src/api/project.rs` activate/resync reactivate path: after re-registering signals, drain parked rows for the project (POST each to `/signal/internal-resume` for listener processing). Reset `preservation_mode` to `none` on successful reactivate.
- `crates/weft-dispatcher/src/backend/k8s_worker.rs`: read worker image tag from `running_worker_hash` instead of hardcoded `:latest`.
- Sidecar manifest builder: use per-node `running_image_hash`.
- `crates/weft-dispatcher/src/project_store.rs`: add `running_worker_hash` and `preservation_mode` fields to ProjectRow. Helpers to set/clear preservation_mode atomically with status transitions.
- `crates/weft-dispatcher/src/infra.rs`: add `running_image_hash` field to InfraPodRow.

## Files to modify (Listener, Rust)

- `crates/weft-listener/src/router.rs`: add `POST /process` endpoint. Receives `{ token, payload, project_id, color }`. Looks up signal in registry, dispatches to per-kind handler, returns `{ action, ... }` for dispatcher to journal.
- `crates/weft-listener/src/router.rs`: add `GET /pending?project_id=X&include_hibernated=bool` endpoint. Returns pending-task list (kind-aware: forms, future browser sessions, etc.). Used by dispatcher's `/ext/{tk}/tasks` forward.
- `crates/weft-listener/src/router.rs`: add `GET /active-stateful-count?project_id=X` endpoint. Returns count of held connections (websocket, sse, future browser session). Used by upgrade path to refuse if active stateful sessions exist.
- `crates/weft-listener/src/kinds/`: each kind module exposes `process(payload, spec) -> Action`. Form kind validates against schema, returns resolve_suspension. Webhook returns fire_entry_trigger. Timer is server-side only (cron); doesn't have a process path. SSE/Socket return resolve_suspension on event arrival.
- `crates/weft-listener/src/relay.rs`: simplify. Today the listener's relay calls `direct_fire::fire` which writes to Postgres directly. Under architecture 4, listener doesn't write Postgres; it calls back to dispatcher's `/signal/internal-resume` instead. Remove direct_fire from listener entirely.
- `crates/weft-listener/src/direct_fire.rs`: DELETE. Replaced by listener-to-dispatcher callback.
- `crates/weft-listener/src/router.rs`: DELETE the public `POST /signal/{token}` and `POST /signal/{token}/{*path}` routes (today's webhook entry point). Webhooks now arrive at dispatcher; listener no longer hosts externally-visible URLs. Keep `/register`, `/unregister`, `/health`, and add the new `/process`, `/pending`, `/active-stateful-count` endpoints. The listener's HTTP surface becomes admin-only (called by dispatcher, not by external clients).
- `crates/weft-listener/src/kinds/webhook_form.rs::user_url`: update URL minting. The user_url returned to the dispatcher (which stores it on the signal row) becomes the dispatcher's URL for this token, not the listener's. The exact URL format depends on dispatcher's host configuration; pass dispatcher base URL via listener config.

The listener loses:
- Postgres write authority (no `direct_fire`).
- External-facing HTTP surface (no public `/signal/{token}` endpoint).

The listener becomes purely an admin-controlled, kind-aware processor. Dispatcher is the single externally-visible HTTP entry point for signals.

## Files to modify (TypeScript)

- `extension-vscode/src/extension.ts`:
  - DELETE: `tryRunOnce`, `ensurePinnedBuild`, `hashProjectInputs`, `weftBinaryFingerprint`, `cacheKey`, the buildChain machinery.
  - REPLACE: `runWeftCli` becomes `runWeftCliJson(args, cwd, onEvent)` that parses NDJSON stdout and dispatches events.
  - REWIRE: `runProject` button → `weft run --json`, `activate` → `weft activate --json`, `infra start` → `weft infra start --json`, etc.
- `extension-vscode/src/graphView.ts`:
  - DELETE: `callProjectLifecycle`, `callInfra`.
  - REPLACE: action-bar message handlers shell out via `runWeftCliJson` instead of dispatcher HTTP.
  - REPLACE: status polling fetches `weft status --json` instead of `/projects/{id}/status` directly. Render buttons from `available_actions` array.
- `extension-vscode/src/webview/lib/components/project/ActionBar.svelte` (or wherever the buttons live): render based on `available_actions` from status payload. Add Resync + Upgrade buttons.
- `extension-vscode/src/shared/protocol.ts`: add new message kinds (`resync`, `infraUpgrade`).

## Verification

End-to-end manual checks (no automated test suite for this PR):

1. **Cold build path**: fresh project, `weft run` from CLI. Confirm image gets hash-tagged name. Run `docker images | grep weft-worker`. Run again immediately, confirm "image exists, skipping build" message and no docker build invocation.

2. **Extension run path**: same project, click Run in graph. Confirm CLI shells out (visible in Weft output channel), same hash-tagged image used, same skip on second click.

3. **Drift detection**: edit main.weft, save, re-open graph. Confirm `weft status --json` returns `trigger_fire_drift: true` (if activated) or no drift (if not activated). Action bar shows Resync if active.

4. **Resync happy path**: project with no infra, activated. Edit fire subgraph. Click Resync. Confirm dispatcher deactivate + reactivate happens, signals re-registered against new worker binary, drift cleared on next status poll.

5. **Resync blocked path**: project with infra defined in source but not running, triggers active (artificial setup: activate before adding infra to source, then add infra). Action bar should show only Resync + Deactivate. Clicking Resync should deactivate, then refuse to reactivate with explanatory message. Bar transitions to normal "infra defined but stopped" state.

6. **Resync with mixed drift, fire-only changes**: infra running, triggers active, edit a fire-only node. Both Resync and Upgrade light. Click Resync. Confirm trigger+fire drift clears, infra drift remains lit, infra Pods unchanged (still running old binary). Click Upgrade afterwards: confirm infra fully refreshes.

7. **Resync with mixed drift, new infra node**: infra running, triggers active. Add a new infra node whose output feeds an existing trigger. Both buttons light. Click Resync alone. Confirm it fails loudly (TriggerSetup tries to read /outputs from non-existent sidecar), atomic-cleanup runs (no half-registered signals), project stays at previous status. User clicks Upgrade, that succeeds. Confirm.

8. **Atomic activate on failure**: any project, force a failure in TriggerSetup (e.g. transiently kill the listener mid-activate, or simulate via test hook). Confirm: project stays at previous status, signal table is clean (no leftover rows for this project), listener has no registered signals for this project. User can re-activate cleanly afterwards.

9. **Infra upgrade**: project with running infra. Edit a sidecar source file. Open graph. Confirm Upgrade button lights. Click. Confirm: triggers deactivated, sidecar image rebuilt + tagged with new hash, k8s manifest applies the new tag, sidecar Deployment rolls, InfraSetup phase re-runs, infra returns to Running. Triggers stay deactivated; user must click Activate to bring them back.

10. **Explicit-consent on infra lifecycle**:
    - Project active with running infra. Click Stop Infra in the graph. Confirmation dialog appears: "This will deactivate your active triggers. Continue?" Click Cancel: nothing happens. Click again, click Confirm: triggers deactivate, infra stops.
    - Same flow for Terminate and Upgrade.
    - `curl POST /projects/<id>/infra/stop` (no flag): expect 412 with message "active triggers; pass deactivate_triggers: true to confirm." With `{"deactivate_triggers": true}` body: succeeds.
    - For Start Infra with triggers active: 412 with instructive error message ("deactivate or resync first"), no state change. The graph dialog uses the same wording.
    - `weft infra stop` from CLI with active triggers prompts at the terminal: "This will deactivate your active triggers. Continue? [Y/n]". Pressing y proceeds; n aborts with no state change.

11. **Atomic InfraSetup on failure**: project with two infra nodes, one of which is configured to fail at provision time. Click Start Infra. Confirm: the successful infra node's resources get rolled back (scaled to zero or deleted), no orphaned infra_pod rows for this color, error surfaced to user. Project state is clean.

12. **Build cache survives engine bump**: rebuild engine binary, observe that the worker hash changes (because engine fingerprint is part of it), drift surfaces on next status poll. Honest behavior.

13. **HIBERNATE preservation**: project active with one HumanQuery suspended. Open the form in the extension (don't submit yet). Run `weft deactivate` and pick HIBERNATE at the prompt. Confirm: extension's pending-tasks view no longer shows the form. Now submit the form anyway (the user already had it open). Confirm: dispatcher accepts the submission, parks it (signal row's `parked_payload` populated), responds 200. Reactivate: prompt shows `1 parked, 1 suspended`. Pick option 1. Confirm parked submission flows through, suspended execution resumes.

14. **PARK preservation**: same setup as 13 but pick PARK. Confirm extension still shows the form during the inactive window. Open form afresh, submit. Confirm parked. Reactivate option 1: drains correctly.

15. **WIPE preservation**: same setup but pick WIPE. Confirm extension shows nothing. Form submission attempts return error (signal row gone). Reactivate is fully fresh (no prompt about preserved state).

16. **Reactivate prompt skipped when nothing preserved**: project never had suspensions or parked items. Deactivate, reactivate. No reactivate-time prompt; reactivate proceeds directly.

17. **Resume node_id missing after upgrade**: project with one HumanQuery suspended. Resync with code change that removes the HumanQuery node. Reactivate option 1 (execute parked + keep suspended). The suspended execution's resume fires, dispatcher tries to dispatch to missing node_id, journals ExecutionFailed. Confirm execution history shows red entry. Other resumes (if any) unaffected.

18. **Architecture 4 path verification**: project with HumanQuery active. Form submission via extension. Confirm via logs that the dispatcher relays to listener's `/process`, listener returns `resolve_suspension`, dispatcher journals SuspensionResolved + ensures worker. Worker resumes correctly. Listener's role is end-to-end (no `direct_fire` invocation, no listener Postgres writes for this flow).

19. **No vestigial listener spawn**: project with extension-only HumanQuery (no public URL configured). Run from a fresh state. Confirm listener still spawns (under architecture 4 it is the kind-processor, always needed when signals are registered) but the user_url field on the signal row is properly returned even if no external caller will hit it.

20. **Stateful kind upgrade refusal** (forward-compat verification, mock-only): manually insert a fake "active stateful session" entry in the listener's registry (via test endpoint) for a project. Attempt to upgrade infra. Confirm dispatcher refuses with 412 "active stateful session in progress." Remove the fake entry; upgrade proceeds.

21. **Webhook routing through dispatcher**: project with a Webhook trigger. Activate. Confirm the user_url returned in the activation response points at the dispatcher (not the listener). curl the dispatcher URL with a sample payload. Confirm dispatcher relays to listener, listener returns fire_entry_trigger, dispatcher enqueues route_entry, worker spawns, project executes. Confirm direct curl to the OLD listener URL (if anyone has it cached) returns 404 (listener no longer hosts the route).

## Scope and order of work

Suggested implementation order to minimize broken-half-states during development:

1. **Atomic activate fix** (existing bug, prerequisite for safe Resync). Wrap `run_trigger_setup` in `activate()` with cleanup-on-Err logic. Verify with test 8.

1b. **Atomic InfraSetup fix** (symmetric existing bug, prerequisite for safe Upgrade). Same shape as 1: track infra_pod rows touched during the failing sub-exec, roll them back on failure. Verify with test 11.

2. **Hash module + worker-image hash naming.** Rust hash module. Update CLI to use hash-tagged worker images. Update dispatcher k8s_worker to read tag from project row. Migration adds `running_worker_hash`. CLI passes hash on every spawn-relevant call. Verify run/activate still work end-to-end with hash-tagged images.

3. **Sidecar hash naming.** Same shape for sidecars. Migration adds `running_image_hash`. CLI passes per-node hashes on infra/start. Sidecar manifest reads from infra_pod row. Verify infra/start works.

4. **`/status` endpoint extension + drift signals.** Dispatcher accepts desired hashes as query params, returns drift bits + available_actions. CLI `weft status --json` consumes this.

5. **State-machine enforcement.** Dispatcher refuses infra/start with active triggers. Trigger lifecycle audit (confirm stop deactivates).

6. **Resync endpoint + CLI command.** Dispatcher /resync handler. CLI `weft resync`.

7. **Infra upgrade endpoint + CLI command.** Dispatcher /infra/upgrade handler. CLI `weft infra upgrade`.

8. **Architecture 4 signal-processing refactor.** Migration: drop `suspension` table, merge into `signal`. Dispatcher: new `POST /signal/{token}` route that relays to listener `/process` and journals based on returned action. `complete_task` becomes a thin wrapper around it. `register_signal` task simplified (always uses listener). Listener: new `/process`, `/pending`, `/active-stateful-count` endpoints. Each kind module exposes `process(payload, spec) -> Action`. Listener loses direct Postgres write authority (no more `direct_fire`); calls back to dispatcher's `/signal/internal-resume` for held-connection events. Verify with test 18.

9. **Preservation modes (HIBERNATE / PARK / WIPE).** Migration adds `project.preservation_mode` and `signal.parked_payload`. Dispatcher's `/signal/{token}` parks instead of relaying when in hibernate/park. `list_pending_signals` filters under HIBERNATE, reactivate path drains parked rows. CLI prompts at deactivate/resync/upgrade, prompts at reactivate when there's preserved state. Verify with tests 13–17.

10. **Extension cutover.** Delete TS-side hash + dispatcher direct calls. Rewire to runWeftCliJson. Action bar reads available_actions. Add the new preservation prompts to the graph view (modal dialogs at deactivate/resync/upgrade time).

Each step lands as a working state with the action bar and CLI both functional. No flag-day cutover.
