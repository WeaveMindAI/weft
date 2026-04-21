# V2 Repository Layout

Snapshot of the weft repo after the phase A1 scaffold.

```
weft/
├── Cargo.toml                      Workspace manifest (v2 crates only).
├── ROADMAP.md
├── README.md, CONTRIBUTING.md, ...  (high-level, unchanged)
│
├── crates/                         V2 rust crates.
│   ├── weft-core/                  Shared types (Pulse, Color, Lane,
│   │                                Node trait, ExecutionContext,
│   │                                entry/suspension primitives).
│   ├── weft-compiler/              Project loader, parser, enrich,
│   │                                validate, codegen, describe.
│   ├── weft-dispatcher/            The daemon binary. HTTP API,
│   │                                journal trait, backend traits,
│   │                                ops dashboard serving.
│   ├── weft-cli/                   The `weft` binary. Thin client
│   │                                of the dispatcher.
│   └── weft-stdlib/                Aggregator that #[path]-links the
│                                    catalog's stdlib nodes.
│
├── catalog/                        Standard library of nodes. Each
│   │                                node folder contains mod.rs and
│   │                                (optionally) metadata.json.
│   │                                `lib.rs` at any folder level is a
│   │                                shared library (e.g.
│   │                                catalog/communication/email/lib.rs
│   │                                becomes `email_lib`).
│   ├── triggers/api/post/
│   ├── human/query/
│   ├── basic/text/, basic/debug/
│   └── ai/llm/
│
├── extension-vscode/               VS Code extension (TypeScript).
│                                    Tangle panel, graph view, runner
│                                    view, Projects tree view.
│
├── extension-browser-v1/           Browser extension (kept as-is,
│                                    repointed to dispatcher in A2).
│                                    Formerly `extension/`.
│
├── docs/
│   ├── v2-design.md                V2 spec (language + runtime).
│   └── v2-refs/                    Reference material:
│       ├── v1-architecture-survey.md
│       ├── design-decisions.md
│       └── repo-layout.md            (this file)
│
├── crates-v1/                      V1 crates (frozen, outside build).
├── dashboard-v1/                   V1 dashboard (archived).
├── catalog-v1/                     V1 catalog with .ts+.rs duality
│                                    (archived, reference for porting).
└── sidecars-v1/                    V1 sidecars (archived; contract
                                     preserved in v2 as sidecar.toml
                                     with /health, /outputs, /live,
                                     /events, /action).
```

## How compilation works in v2

1. `weft build` from within a user project (`myproject/`) reads
   `weft.toml`, parses `main.weft`.
2. Walks the graph, collects every node type referenced.
3. Resolves each type against (stdlib catalog) + (user `nodes/`) +
   (`nodes/vendor/` for git-installed packages).
4. Pulls in any `lib.rs` co-located with a used node (siblings access
   via `super::<prefix>_lib`).
5. Codegens a rust source tree that links exactly those nodes and
   runs the pulse loop.
6. Invokes cargo to produce the final binary.

Unused stdlib nodes do NOT compile into the binary.

## How the dispatcher runs

1. `weft start` (local) or deployment (cloud) boots the dispatcher
   binary.
2. Dispatcher binds the HTTP port (default 9999 local), mounts the
   full API router.
3. Workers are NOT the dispatcher. When a wake needs to happen, the
   dispatcher asks the `WorkerBackend` to spawn one. The worker runs
   the user's compiled binary with wake context.
4. Worker talks to restate for journal writes; dispatcher reads/writes
   the same restate.

## Phase A2 TODOs (already queued, not scaffolded)

- Port executor_core (preprocess/readiness/postprocess logic) from
  v1 into weft-core.
- Port weft_type full system into weft-core.
- Port weft_compiler into weft-compiler.
- Port enrich into weft-compiler.
- Implement codegen for user binaries.
- Implement the SubprocessWorkerBackend and KindInfraBackend.
- Implement the embedded restate integration in journal.rs.
- Implement every CLI command's real behavior.
- Port the remaining ~100 nodes from catalog-v1.
- Wire the VS Code extension's Tangle + graph + runner + SSE pieces.
- Bundle the ops dashboard (svelte or similar) into the dispatcher
  via rust-embed.
