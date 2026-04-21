# V1 Weft: Architectural Survey (Reference for v2 Rewrite)

Captured during v2 scaffold preparation. Snapshot of v1 as archived in
`crates-v1/`, `dashboard-v1/`, `extension-browser-v1/`, `catalog-v1/`,
`sidecars-v1/`. Use this as the canonical reference for what to port,
reshape, or delete. Do not rely on reading v1 code cold; come here
first.

---

## 1. Rust Crates

### 1.1 weft-core

Types shared by everyone. Source of truth for the data model.

Files:
- `lib.rs`: re-exports.
- `project.rs`: `ProjectDefinition`, `NodeDefinition`, `Edge`,
  `PortDefinition`, `LaneMode` (Single/Expand/Gather), `EdgeIndex`.
- `weft_type.rs`: type system (`WeftPrimitive`, `WeftType` with
  unions, type vars, runtime checks, string serialization).
- `weft_compiler.rs` (~1200 lines): pre-execution compiler.
  Normalizes node types, enriches ports, validates lane compat,
  expands group nodes, builds edge indices, applies config defaults.
- `executor_core.rs` (~1700 lines): pure executor logic. Stateless
  functions on `ProjectDefinition` + `PulseTable`. **No restate
  dependency. v2 can reuse wholesale.**
- `executor.rs`: restate-only auxiliary services (TaskRegistry trait
  and impl).
- `instance_registry.rs`: NodeInstanceRegistry (restate virtual
  object). Also `NodeExecuteRequest/Response`, `NodeCallbackRequest`
  for orchestrator-to-node HTTP.
- `infrastructure.rs`: `InfrastructureManager` (restate) for k8s/VM
  provisioning.
- `k8s_provisioner.rs`: helm/namespace/probe logic.
- `sidecar.rs`: `ActionRequest`/`ActionResponse` for sidecars.
- `media_types.rs`: image/video/audio/document types + MIME map.
- `node.rs`: re-exports `NodeFeatures`, `TriggerCategory`.

Key types:
- `Pulse`: id, color (UUID), lane (Vec<SplitFrame>), status
  (Pending/Absorbed), data (Value), port name, gathered flag. No
  execution metadata.
- `PulseTable`: `BTreeMap<node_id, Vec<Pulse>>`.
- `NodeExecution`: dispatch record. id, status
  (Running/Completed/Failed/WaitingForInput/Skipped), pulseIds,
  callback_id, timestamps, cost, logs, color, lane.
- `NodeExecutionTable`: `BTreeMap<node_id, Vec<NodeExecution>>`.
- `SplitFrame`: `{count, index}`. One level of list expansion.
- `ReadyGroup`: aggregated input ready to dispatch. lane, color,
  input (Value), should_skip, pulse_ids, error.

### 1.2 weft-nodes

The node catalog + trigger/form machinery.

Files:
- `node.rs` (~1100 lines): the `Node` trait. `ExecutionContext`
  with helpers (config_u64, config_str, input_string_list,
  is_cancelled, request_form_input, report_usage_cost,
  tracked_ai_context, store_temp_media, notify_action, infra_client,
  completion_context). Also defines `NodeFeatures`,
  `TriggerCategory`, `TriggerStatus`, `TriggerHandle`, `keep_alive`
  trigger protocol. `register_node!` macro + `NodeEntry` inventory.
- `registry.rs`: `NodeTypeRegistry`
  `HashMap<&'static str, &'static dyn Node>`, built from
  `inventory::iter::<NodeEntry>`.
- `trigger_service.rs`: `TriggerService` lifecycle manager
  (register/unregister/stop/heartbeat).
- `form_input.rs`: `FormInputChannels`
  `Mutex<HashMap<callback_id, oneshot::Sender>>`.
  `request_form_input_impl`: register channel, POST
  WaitingForInput callback to executor, await channel.
- `form_registrar.rs`: routes form submissions for triggers.
- `service.rs`: `NodeService` axum HTTP server. `/health`,
  `/execute`, `/input_response/{callback_id}`, `/complete_trigger`.
- `runner.rs`: `NodeRunner` wraps Node, builds ctx, calls execute,
  posts callback.
- `enrich.rs` (~2000 lines): **post-compilation enrichment.**
  Resolves TypeVar and dynamic ports (Pack/Unpack, hasFormSchema).
  **v2 needs this logic ported carefully.**
- `infra_helpers.rs`, `passthrough.rs`, `constants.rs`.
- `nodes/`: ~100 node impls. Each a unit struct + trait impl +
  `register_node!`.

### 1.3 weft-api

HTTP surface for everything non-execution.

Files:
- `main.rs`: axum on :3000. Initializes AppState, loads triggers
  from postgres, starts trigger event listener, trigger
  heartbeat/recovery, daily usage aggregation cron.
- `routes.rs` (~2000 lines): `/api/v1/triggers`,
  `/api/v1/webhooks/{trigger_id}`, `/api/infra/*`,
  `/api/v1/usage/*`, and more.
- `state.rs`: `AppState` with trigger_service, restate_url,
  executor_url, db_pool, instance_id, internal_api_key,
  http_client, node_registry.
- `extension_api.rs` (~700 lines): `/ext/{token}/*` routes. tasks,
  triggers, actions, health.
- `extension_tokens.rs`: opaque token CRUD.
- `trigger_store.rs` (~600 lines): postgres `triggers` table
  (id, project_id, trigger_node_id, category, config,
  credentials, status, instance_id, last_heartbeat). Claim model
  for HA.
- `usage_store.rs` (~700 lines): `usage_events` +
  `daily_usage` tables, billing aggregation.
- `webhooks.rs` (~400 lines): HMAC-SHA256 signature validation.
- `publish.rs` (~2000 lines): internal API for project publish
  and sync execution invocation.
- `crypto.rs`, `log_utils.rs`.

### 1.4 weft-orchestrator

The in-memory execution engine.

Files:
- `main.rs`: runs restate services (:9080) AND axum executor
  (:9081) in parallel.
- `executor_axum.rs` (~2500 lines): **the execution engine.**
  `ExecutorState` with `DashMap<execution_id, Arc<Execution>>`.
  Routes: `/start`, `/execution_callback`, `/cancel`,
  `/provide_input`, `/get_status`, `/get_node_statuses`,
  `/get_all_outputs`, `/get_node_executions`,
  `/retry_node_dispatch`.

Core data flow:
1. weft-api webhook → orchestrator `/start` → billing gate →
   `weft_compiler::compile` → init pulses → main loop.
2. Main loop: `preprocess_input` (Expand/Gather) →
   `find_ready_nodes` → dispatch via HTTP to node instance.
3. Node callback `/execution_callback` → update NodeExecution,
   generate output pulses, next iteration.
4. `WaitingForInput`: register PendingTask in TaskRegistry,
   node awaits channel in FormInputChannels.

---

## 2. Frontend (browser extension)

**This is NOT the VS Code extension. It is a browser extension
for human-in-the-loop tasks.** Stays in v2 with minor
adjustments (point it at the dispatcher's token routes instead
of dashboard-proxied ones).

Stack: WXT, Svelte 5, Manifest V3.

Entry points:
- `background.ts`: service worker. 30s polling loop via
  `browser.alarms`, calls `/api/ext/{token}/tasks` per token,
  dedup via `seenTaskIds`, updates badge.
- `popup/App.svelte`: popup UI (manage tokens, see tasks).
- `toast.content/Toast.svelte`: content script for in-page
  toasts.

Auth: opaque tokens stored in `browser.storage.local`. Struct
`ExtensionToken { token, name, cloudUrl }`. No OAuth, no user
login. Tokens added manually via popup form.

Endpoints hit (through dashboard proxy today):
- `GET /api/ext/{token}/tasks`
- `POST /api/ext/{token}/tasks/{execId}/complete`
- `POST /api/ext/{token}/triggers/{triggerTaskId}/submit`
- `POST /api/ext/{token}/actions/{actionId}/dismiss`
- `POST /api/ext/{token}/cleanup/all`
- `POST /api/ext/{token}/cleanup/execution/{execId}`
- `GET /api/ext/{token}/health`

v2 change: these move from dashboard proxy to dispatcher direct.
Paths stay compatible or become `/ext/{token}/*` on the
dispatcher's HTTP surface.

PendingTask shape (lib/api.ts:15-25):
```
{ executionId, nodeId, title, description?, createdAt,
  taskType?: 'Task'|'Action'|'Trigger', actionUrl?,
  formSchema?, metadata? }
```

---

## 3. Dashboard (dashboard-v1)

SvelteKit 2.55, Vite, Tailwind, xyflow (Svelte 5), codemirror,
elkjs, echarts.

**v2: most of this is authoring UX and moves to the VS Code
extension. A smaller ops-only dashboard stays (embedded in
dispatcher).**

Auth middleware (`hooks.server.ts:128-361`): JWT for cloud, local
sentinel user for standalone. CORS allows `*` on `/api/ext/*`
(extension has opaque origin).

Major routes:
- `/(app)/dashboard`: project list.
- `/(app)/projects/[id]`: editor canvas (xyflow).
- `/(app)/executions`: execution history.
- `/(app)/tasks/[executionId]`: task runner.
- `/(app)/extension`: extension token management.
- `/(app)/files`: file picker.
- `/p/[username]/[slug]`: public deployment view.
- `/playground`: standalone AI builder.

Graph rendering: xyflow client-side. Nodes parsed from
`weftCode` text via `parseWeft()` in `lib/ai/weft-parser.ts`.
Layout in separate `layoutCode` string, elkjs for auto-layout.

AI builder (Tangle):
- `lib/ai/node-catalog.ts`: builds node catalog for LLM prompts.
- `lib/ai/weft-parser.ts`, `lib/ai/weft-editor.ts`: parse/edit
  Weft DSL text.
- Streaming: `weftStreamStart/Delta/End`, backend streams chunks,
  frontend parses incrementally.

ProjectDefinition (lib/types):
- `id, name, description`
- `weftCode?: string` (source of truth)
- `layoutCode?: string` (positions, separate)
- `loomCode?: string` (setup manifest, experimental)
- `nodes, edges` (derived, ephemeral)

API proxy layer: all `/api/*` routes proxy to backend (weft-api,
restate, executor) via `getBackendUrl()` and related config.

---

## 4. Catalog (catalog-v1)

File-based node catalog. Each node in `category/:prefix/name/`
with `backend.rs` + `frontend.ts` (both optional).

Example: `communication/:discord/send/`:
- `frontend.ts`: exports `NodeTemplate` (type, label, icon,
  color, category, tags, fields, defaultInputs, defaultOutputs,
  features, validate fn).
- `backend.rs`: implements `Node` trait + `register_node!`.

Build step (`catalog-link.sh`):
1. Symlinks `frontend.ts` → `dashboard/src/lib/nodes/{name}.ts`.
2. Symlinks `backend.rs` → `crates/weft-nodes/src/nodes/{snake}/mod.rs`.
3. Generates `nodes/mod.rs` with `pub mod {name};`.
4. Generates `catalog-tree.json` (hierarchy for AI context).

Dashboard discovery: Vite glob `src/lib/nodes/*.ts` at build
time, extracts `NodeTemplate` exports, binds to `ALL_NODES`.

**v2 shift: frontend.ts metadata moves into the node's rust
impl (NodeMetadata) since VS Code extension reads graph state
from the compiler, not static TS files. catalog/ structure may
survive for organization but the frontend.ts layer disappears.**

---

## 5. Sidecars (sidecars-v1)

Long-lived external services exposing a standard HTTP contract.
v2 keeps this pattern as "infra nodes."

Standard endpoints:
- `GET /health`
- `GET /outputs`: current state kv
- `GET /live`: dashboard render data
- `GET /events`: SSE stream with event filter query param
- `POST /action`: generic action dispatch
- `GET /download`: optional, media download

### 5.1 postgres-database

Rust + axum. Durable KV backed by postgres.

Actions: `kv_set`, `kv_get`, `kv_delete`, `kv_list`,
`kv_query` (regex), `kv_delete_pattern`.

Schema: `kv_store (key PK, value JSONB, created_at, updated_at)`.

### 5.2 whatsapp-bridge

Node.js + Express + Baileys (unofficial WhatsApp client).

Endpoints: standard + `/qr` (auth QR image), `/status`
(connection status), `/events` (SSE filterable).

Actions: `send`, `receive`, `create-group`, `group-add`,
`group-kick`, `group-admin`, `react`, `delete-message`,
`history`.

Auth: QR-code flow. Sidecar → client polls `/qr` → user scans
with phone → baileys connects → state broadcast via SSE.

---

## 6. Cross-System Flows

### Human-in-loop form flow (v1, for v2 port)

1. User clicks "Complete Task" in browser extension popup.
2. Extension `POST /api/ext/{token}/tasks/{execId}/complete`.
3. Dashboard proxy route → `POST {backendUrl}/ext/{token}/...`.
4. weft-api validates token, looks up executor URL, POSTs to
   executor's input resolution endpoint.
5. Executor routes to NodeService `/input_response/{callback_id}`.
6. NodeService resolves `FormInputChannels[callback_id]` with
   form value via `oneshot::Sender::send()`.
7. Node's `await` resolves, resumes with form response.

No node process restart; same tokio task blocks and resumes.

**v2 port**: dispatcher owns the entire flow. Extension POSTs to
dispatcher directly. Dispatcher updates journal, wakes worker
(spawns new pod with wake context since worker may have exited
during suspension). Worker replays from journal, resumes at
`await_form`, returns form value.

### Trigger lifecycle (v1, for v2 port)

v1 trigger activation:
1. POST `/api/v1/triggers` with config.
2. weft-api calls orchestrator setup execution.
3. Orchestrator runs setup graph, returns success.
4. weft-api `register_trigger_db` (postgres insert).
5. `TriggerService.register_trigger`: looks up node, calls
   `node.keep_alive(config, ctx)`, stores `TriggerHandle`.
6. Node's keep_alive spawns event loop, emits via
   `ctx.emit(payload)` → `TriggerEvent` → event_tx channel.
7. weft-api main loop reads TriggerEvent from channel, calls
   orchestrator `/start` with payload.

**v2 port**: entry primitives replace `TriggerService`. For
webhooks/cron, the dispatcher handles routing directly (no
long-lived "trigger"). For infra-backed events (Slack, Discord,
WhatsApp), infra nodes publish to the dispatcher's event bus,
dispatcher subscribes based on graph analysis.

---

## 7. What v2 Reuses, Reshapes, Deletes

### Reuse (copy into v2 crates as starting point)

- `executor_core.rs`: pure functions for preprocess, readiness,
  postprocess. Zero restate. **v2 keeps almost verbatim.**
- `weft_type.rs`: WeftType, WeftPrimitive, unions, type vars.
- `project.rs`: ProjectDefinition, NodeDefinition, Edge,
  PortDefinition, LaneMode.
- `weft_compiler.rs`: normalize, enrich ports, validate lane
  compat, expand groups, edge indices.
- `enrich.rs`: TypeVar resolution, dynamic port expansion.
- Many individual node impls (ports to new Node trait but the
  body is the same).
- Browser extension (extension-browser-v1): stays almost as-is,
  points at dispatcher instead of dashboard proxy.
- Sidecar contract (`/health`, `/outputs`, `/live`, `/events`,
  `/action`): stays for infra nodes.

### Reshape (same semantics, new implementation)

- `Pulse` gets color explicitly (was implicit in v1).
- `Node` trait gets entry primitives in metadata, drops
  `isTrigger`/`triggerCategory`/`requiresRunningInstance`.
- `ExecutionContext` gains `await_form`, `await_timer`,
  `await_callback`; loses `request_form_input` as a node-level
  callback mechanism.
- Form/task flow: browser extension talks to dispatcher directly
  instead of through dashboard proxy.
- Node registration: still inventory-based but the registered
  struct declares entry primitives in its metadata.

### Delete (no v2 equivalent)

- `weft-orchestrator` crate entirely.
- `trigger_service.rs`, `TriggerHandle`, `TriggerCategory`,
  `keep_alive` protocol.
- `form_input.rs` channel-based suspension (replaced by journal-
  backed `await_form`).
- `NodeInstanceRegistry` (no per-node-type microservices).
- Orchestrator-to-node HTTP (nodes are linked into user binary).
- Restate's TaskRegistry as a separate service (unified into the
  dispatcher's journal).
- `instance_id` claim model for trigger HA (dispatcher is
  stateless, no claim needed).
- `NodeService` HTTP server (per node-type deployment).
- Dashboard `/api/ext/*` proxy routes (extension hits dispatcher).
- `catalog-link.sh` and the symlinking build step (nodes become
  normal rust modules).

### Not yet decided

- `frontend.ts` per-node metadata files: delete (move into rust
  NodeMetadata) or keep (for the VS Code extension to parse)?
  v2 doc leans "delete, VS Code extension reads rust-side
  metadata via the compiler." Needs a concrete decision when
  VS Code extension work starts.
- Dashboard (SvelteKit): kill entirely, move everything to VS
  Code? Or keep a thin ops-only dashboard embedded in dispatcher
  + VS Code for authoring? v2 doc says the latter.
- `weftCode` / `layoutCode` split: inherited from v1. v2 can
  keep this or simplify. Not scaffold-critical.

---

## 8. Quick File Reference

If you need to go look at v1 code while implementing v2:

- Pulse/lane model: `crates-v1/weft-core/src/project.rs`,
  `crates-v1/weft-core/src/executor_core.rs`.
- The execution loop: `crates-v1/weft-orchestrator/src/executor_axum.rs`.
- Node trait and ctx: `crates-v1/weft-nodes/src/node.rs`.
- Node registry: `crates-v1/weft-nodes/src/registry.rs`.
- Trigger lifecycle: `crates-v1/weft-nodes/src/trigger_service.rs`.
- Form flow: `crates-v1/weft-nodes/src/form_input.rs` +
  `crates-v1/weft-api/src/extension_api.rs`.
- Webhooks: `crates-v1/weft-api/src/webhooks.rs` +
  `crates-v1/weft-api/src/routes.rs`.
- Usage/billing: `crates-v1/weft-api/src/usage_store.rs`.
- Extension API: `crates-v1/weft-api/src/extension_api.rs`.
- Enrich: `crates-v1/weft-nodes/src/enrich.rs`.
- Compiler: `crates-v1/weft-core/src/weft_compiler.rs`.

All paths relative to `/home/quent/projekt/Weavemind/weft/`.
