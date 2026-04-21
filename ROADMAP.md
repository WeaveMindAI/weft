# Roadmap

What's coming next. Not prioritized, not promised, just directions we're exploring.

## Language

- **User-defined types (structs)**: `type Lead { name: String, email: String, score: Number }`. Ports reference named types instead of raw Dict.
- **Function calls (callbacks)**: Nodes call sub-graphs mid-execution and get results back. LLM tool_use routes through a visual sub-graph. Code nodes call graph-defined functions. Breaks the DAG executor model: node suspension, re-entry, recursion. Groups become callable with typed signatures.
  - **Loops**: Re-execute a group until a condition port says stop. Could be sugar over callbacks or a simpler separate primitive (re-dispatch without node suspension). Might ship first.
  - Callbacks + loops unlock agent loops, tool-use routing, retry/pagination. Without them, agents are second-class.
- **Multi-file / imports**: `import { MyGroup } from "./shared/group.weft"`. Reusable groups across projects.
- **Error handling**: Try/catch equivalent. Catch node failures and route to error-handling sub-graphs instead of killing the execution.
- **Explicit expand / gather**: The implicit `List[T] → T` expand and `T → List[T]` gather is beloved UX but AI writers (and humans) confuse it. When a downstream port is `T` (especially a TypeVar that absorbs the parent shape), the expected gather silently doesn't happen and produces wrong data. Fix: keep the whole mechanism as-is, but make it opt-in. By default `List[T] → T` is a type error. An explicit `expand T` / `gather List[T]` keyword on the port (or edge) turns on the existing logic and the compiler validates that the depth/breadth matches exactly as today. Same compiler machinery, different default: explicit over implicit.

## Editor & Tooling

- **Centralize parsing and compilation in Rust**: The frontend currently runs its own TypeScript parser. Move all parsing, validation, and compilation to the Rust backend. The frontend calls the backend API and gets back the AST, errors, and compiled output. Rust is fast enough that re-parsing on every edit is viable, and it eliminates the dual-parser synchronization problem.
- **Optimize re-parsing for multi-file projects**: Once multi-file imports land, re-parsing the entire project on every keystroke won't scale. Incremental parsing (only re-parse changed files, cache the rest) or a persistent AST server that applies deltas.

## Execution Model

- **Outputs as endpoints, subgraph execution**: A project is not a single monolithic graph that always runs top to bottom. Instead, `output` nodes (a rename of the current `debug` node) mark end states, the explicit points where the system produces a result. A project can have multiple `output` nodes, each representing a distinct "thing the system can produce". At run time the user (or a deployed page's visitor, or a fired trigger) selects which outputs to produce. The executor extracts the subgraph upstream of the selected outputs and runs only that. Rationale:
  - **Multiple front-end views per project**: one humanizer output, one summarizer output, one translator output in the same file sharing the same `llm_config` node. Each output gets its own CTA in the deployed runner page. Clicking a CTA runs only that subgraph.
  - **Triggers that don't step on each other**: each trigger declares which outputs it produces. Firing a trigger only runs what's needed for those outputs, not the whole project. One project file can hold several independent trigger flows.
  - **Composition**: the mental model becomes "a project is a set of functions sharing a library of nodes", which is how programmers already think about code.
  - **Cost clarity**: each CTA can show its exact per-run cost because we know which nodes are on its subgraph.
  - Builds on the existing subgraph extraction that infra and triggers already use. Extending it to "walk backward from a set of seed output nodes" is the same algorithm with a different seed set.
  - Requires extending the runner DSL (Loom) to scope phases and CTAs to a target output, and extending the ActionBar so the admin can pick which outputs to run. Visitor runs of a published page are always scoped to the CTA's declared target. `ProjectExecutionRequest` gains an `outputNodeIds: string[]` field so the executor knows which endpoints to materialize.

## Compilation & Execution

- **In-process node execution**: Link weft-nodes into the orchestrator. Local nodes (Text, Gate, Template) run in-process, no HTTP round-trip. ~10ms saved per computational node.
- **Compile to standalone binary**: Weft compiler emits Rust code. The binary contains the execution graph and node implementations. `cargo build` a Weft project into a self-contained binary.
- **Per-execution container isolation**: Each execution spawns its own container with the compiled binary. Full filesystem access within execution scope, no external sandbox service needed. Cross-language file sharing (Python writes a file, another node reads it).
- **Binary execution modes**:
  - `./program`, run once: provision infra, execute, tear down, exit.
  - `./program serve`, service mode: provision infra, run trigger setup, stay alive listening. On each trigger event, run the execution sub-graph. Ctrl+C tears down.
  - `./program infra up` / `infra down`, manage infrastructure lifecycle independently.
  - Infrastructure targets: local k8s (kind), remote cluster, or WeaveMind cloud.
- **Distributed compiled subprograms**: Compile different parts of a program independently. Infrastructure subgraph runs on a remote server, execution subgraph runs locally, they discover each other at runtime.

## Stabilization

- **Big refactor and code cleanup pass**: Weft was built fast, solo, over a few months. Several core files are too big (the compiler, the executor, the REST API), the error handling in `weft-api` is inconsistent, Restate is tangled into `weft-core` in ways that block standalone use, and parts of the codebase are under-documented. None of this is urgent, the project works, but it will start to hurt as soon as external contributions pick up. The plan is to dedicate a few uninterrupted weeks to a real stabilization pass (splitting the oversized files, extracting Restate out of `weft-core`, standardizing error handling, tightening the registry lifecycle, adding the comments a contributor coming in cold actually needs) as soon as the project is financially stable enough that I can spend that time on cleanup instead of shipping features. Until then, contributors should expect some rough edges and are welcome to open PRs that chip away at the list.

