# V2 Design Decisions (Locked)

Captured during the scaffolding conversation. Decisions committed with
Quentin. Treat as hard constraints. Updates must be explicit decisions,
not drift.

---

## 1. Nodes are consumers of language features

Nodes implement specific use cases by composing language primitives.
They are not themselves language features. If a node needs a dirty
workaround, the language is missing a primitive and must be extended.

- Nodes can be small (Text, ApiPost) or large (Email, HTTP) depending
  on the protocol surface they bridge.
- Size is fine. Dirty workarounds are not.

## 2. Compilation is per-project and lazy

`weft build` walks `main.weft`, finds which nodes the graph actually
uses, follows transitive dependencies (libs shared by nodes), and
compiles exactly those into the binary. Unused stdlib nodes do not
bloat the binary.

## 3. Catalog layout

Stdlib lives at `catalog/` in the weft repo, organized in arbitrarily
nested folders by category. Examples:
- `catalog/communication/:email/send/` (node folder)
- `catalog/communication/:email/lib.rs` (shared lib for email family)
- `catalog/ai/llm/` (node folder)
- `catalog/triggers/:api/post/` (node folder with webhook entry)

Rules:
- Folders prefixed with `:` are "prefix folders" whose name becomes
  part of the node identity. `:email/send/` → node type includes
  `email` in its namespace.
- `lib.rs` at any folder level is a shared library accessible to
  sibling nodes and descendants via `super::<last_prefix>_lib`.
  E.g. `:email/lib.rs` is importable as `email_lib` from
  `:email/send/backend.rs`.
- Each node has its own folder containing `mod.rs` (node
  implementation) and optionally `metadata.json` (machine-readable
  metadata for tooling).
- Sidecars for a node live in a `sidecar/` subfolder of that node.

## 4. Metadata.json is the source of truth for node metadata

A node's `metadata.json` file (co-located with `mod.rs`) is embedded
into the compiled binary at build time via `include_str!`. The node's
rust impl parses this JSON and returns it from the `metadata()` trait
method. One file, two consumers: rust runtime and external tooling
(Tangle, VS Code extension, dashboard).

- `metadata.json` is optional. Small rust-only nodes can hand-write
  `metadata()` inline.
- When both exist (rust code and metadata.json), the compiler verifies
  they match.
- Tangle and the VS Code extension read `metadata.json` directly for
  fast catalog introspection without full compilation.

## 5. User-defined nodes

Users write nodes in rust under `myproject/nodes/`. Same `Node` trait,
same primitives, same build step. No stdlib shadowing: user nodes have
distinct names.

External packages installed via `weft add git.host/user/repo` land under
`myproject/nodes/vendor/`. They are git-backed like Go modules. No
central registry for v2 ship. Registry is a later concern.

## 6. Cost tracking is a language primitive

`ctx.report_cost(CostReport { service, model, amount_usd, metadata })`.
Fire-and-forget, journaled, dispatcher aggregates per execution /
project / user. Replaces v1's node-posts-HTTP-to-weft-api pattern.

## 7. Sidecars are Docker containers with an HTTP contract

Sidecars are long-lived external services provisioned as Docker
containers. Language-agnostic: WhatsApp bridge is Node (Baileys),
Postgres sidecar is Rust. No rust trait constrains them.

Manifest: `sidecar.toml` co-located with the node's `sidecar/` folder.
Declares Dockerfile path, ports, env vars, health check.

HTTP contract (required):
- `GET /health`
- `GET /outputs`
- `GET /live` (dashboard render data)
- `GET /events` (SSE stream, event-type filter query param)
- `POST /action`

Optional extensions (add as needed):
- `GET /download`
- `GET /logs`
- `GET /qr` (auth flows)

The contract is a documented spec, not a rigid interface. We extend
it by adding optional endpoints.

## 8. The dispatcher is the orchestration layer

Dispatcher owns:
- Event routing (webhook URLs, form URLs, cron, infra events)
- Worker spawning (via worker backend)
- Journal (via restate)
- Infra orchestration (via infra backend)
- Ops dashboard
- Cost aggregation (from worker `report_cost` events)

Dispatcher does NOT execute node code. It spawns workers; workers run
the user's compiled binary; nodes are linked into that binary; the
binary talks to the dispatcher only via restate (journal writes) and
HTTP (initial wake context handoff).

## 9. CLI is a client of the dispatcher

`weft run`, `weft stop`, `weft follow`, `weft ps`, etc. all map to
HTTP calls on the dispatcher. The CLI never owns execution lifecycle.
Ctrl-C disconnects the streaming client; the execution continues.

`weft start` / `weft daemon-stop` manage the local dispatcher daemon
lifecycle. In cloud or hosted workspace, the daemon is managed for the
user.

## 10. The language primitive surface (v2 ship target)

Entry primitives (node-metadata-level, not called from execute):
- `Webhook { path, auth }`
- `Cron { schedule }`
- `Event { connection_port, filter }`
- `Manual`

Suspension primitives (called via ctx, journaled):
- `ctx.await_form(schema) -> FormSubmission`
- `ctx.await_timer(duration) -> ()`
- `ctx.await_callback<I, O>(subgraph, input) -> O`
- `ctx.await_first(primitives) -> usize` (architected-for, not ship-day)

Fire-and-forget helpers (called via ctx, journaled):
- `ctx.report_cost(CostReport)`
- `ctx.log(level, message)`
- `ctx.emit(port, value)` (internal to runtime; nodes emit via NodeResult
  return, not this primitive)

Read helpers (ctx-local, no journal):
- `ctx.config.get::<T>(key)`
- `ctx.input.get::<T>(port)`
- `ctx.is_cancelled() -> bool`
- `ctx.execution_id`, `ctx.project_id`, `ctx.color`, `ctx.lane`

Deferred (port as needed when the nodes that use them land):
- `ctx.store_temp_media(...)`
- `ctx.infra_client(node_id)`
- `ctx.tracked_ai_context(...)` (may be subsumed by `report_cost`)

## 11. Crate names

- `weft-core`: types shared by everyone (Pulse, Color, Lane, Node,
  ExecutionContext, primitives).
- `weft-compiler`: parser, enrich, codegen, project loader.
- `weft-dispatcher`: the daemon binary (HTTP, journal, backends,
  dashboard serving).
- `weft-cli`: the `weft` binary.
- `weft-stdlib`: stdlib node crate (modules sourced from `catalog/`).

No `weft-runtime` crate: the runtime code lives inside `weft-core` and
is linked into user binaries via `weft build`-generated glue code.

## 12. Browser extension stays as-is, repointed

The `extension-browser-v1/` (renamed from `extension-v1/`) stays in
the repo and its logic is preserved. The only change is repointing
from dashboard-proxied `/api/ext/*` URLs to direct dispatcher
`/ext/*` URLs. Opaque tokens, 30s polling, `cloudUrl` per-token all
stay.

Later (post-ship) improvements: project-scoped keys, per-execution
scoping, optional user-auth enforcement.

## 13. VS Code extension is new

Separate folder `extension-vscode/`. Scaffolded in phase A1.
Responsibilities (open source, ops-only): Projects tree view,
`.weft` graph view, `.loom` runner view, command palette entries
for run/activate/deactivate. AI authoring is NOT in this extension;
it ships from a separate commercial extension maintained outside
this repository.

## 14. Five starter nodes for the scaffold

1. `ApiPost` (webhook entry, validates entry primitive + dispatcher
   URL routing)
2. `HumanQuery` (form suspension, validates `await_form` + journal
   replay)
3. `Text` (literal, validates basic config+output)
4. `Debug` (output viewer, validates downstream routing)
5. `LLM` (real-world node, validates `report_cost` + external
   service integration)

Scaffold can ship with these and still demonstrate an end-to-end flow.
Additional nodes port during the hours-long solo phase.

## 15. Tangle metadata extraction

Tangle needs fast per-project catalog introspection, including for
partially-written user nodes (where full compilation may fail).

Plan: `weft describe-nodes` outputs JSON describing every node
available in the current project's scope (stdlib + user + vendored).
Reads `metadata.json` directly for speed. Falls back to partial
parse of rust source when `metadata.json` is missing.

The CLI command is an introspection endpoint, not a build step.

## 16. Restate embedded locally, managed in cloud

No sqlite backend. Locally, the dispatcher embeds restate as a
single-node instance. Cloud uses managed restate. License-permitted
(restate BSL carves out workflow platforms that abstract over it).

## 17. Isolation for cloud workers

GKE Agent Sandbox as the worker backend for cloud workers. Alpha
now, purpose-built for our use case. Architecture permits fallback
to E2B if Agent Sandbox isn't ready when phase B starts. Decision
documented in the weavemind cloud doc, not here.

Locally, workers are plain subprocesses (user trusts their own
machine).

## 18. Editor/dashboard split

- VS Code extension (open source) = ops sidebar + file previews
  (graph view for `.weft`, runner view for `.loom`). Talks to the
  local dispatcher.
- Dispatcher's embedded dashboard = ops UX (watch executions,
  manage projects, view trigger URLs, inspect infra). Served by the
  dispatcher on its HTTP port.
- No standalone dashboard SvelteKit app in v2. The old
  `dashboard-v1/` is archived.
- AI authoring ships from a separate commercial extension and is
  not part of this repository.
- Marketing website is separate, minimal, SEO-focused. Not in the
  weft repo.
