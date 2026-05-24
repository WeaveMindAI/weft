# Standard library as cloned user nodes

## Goal

Stop treating the weft repo's `catalog/` as a privileged build-time
source. A project's `nodes/` folder becomes the single source of truth
for every node. Build, run, codegen, parse, validate, and describe all
read ONLY `nodes/`. A user can uninstall weft and the project still
builds. The stdlib is just ordinary nodes the user can read, edit,
fork, or delete.

`weft new` seeds `nodes/` by copying the weft repo's `catalog/` into
it (the one place `stdlib_root()` survives, at scaffold time). Later
this copy gets replaced by a registry fetch; the destination shape
(`nodes/` is self-contained) is already correct, so that swap is
local to `weft new`.

## Current shape (verified against code)

- **Discovery** (`weft-catalog/src/lib.rs`): `visit_dir` recurses
  arbitrarily deep, detects package-root (`package.toml`) vs bare-node
  (`metadata.json`), stops descending at a unit. Deep nesting and both
  forms already work. Two issues: (1) bare-node and package register
  via two separate fns producing the same `Package` struct; (2)
  multi-node packages require an explicit `nodes = [...]` list in
  `package.toml`.
- **The weld**: `codegen.rs` and `worker_image.rs` compute each node's
  `#[path]` as `source_dir.strip_prefix(stdlib_root())` rebased onto
  `/weft/catalog` (`CATALOG_MOUNT`). `stage_build_context`
  (`build.rs:160`) copies `weft_root/catalog` into the build context.
  So nodes can ONLY live under `stdlib_root()`; user nodes in `nodes/`
  are never staged and would fail `strip_prefix`. This is the core
  problem.
- **Three sources** (`build_project_catalog`, `build.rs:244`): stdlib +
  `nodes/vendor/` + `nodes/`, later shadows earlier.
- **`weft new`** creates an empty `nodes/`. No clone.
- **Dispatcher parse authority**: `/parse`, `/validate`,
  `/describe/nodes` build their catalog from `stdlib_catalog()` baked
  into the dispatcher image. The dispatcher pod (k8s) cannot see the
  user's local `nodes/`. The extension hits these on every keystroke.

## Target shape

One root: `project_root/nodes/`. Recursive walk; at each dir, if it's
a package root or bare node it's a unit (register, stop descending),
else recurse. Position and depth irrelevant. No stdlib source, no
vendor source.

### Decisions (resolved from the rules, not asked)

- **Vendor dies.** Redundant once discovery is single-rooted and
  recursive. Delete `vendor_dir`, the vendor `CatalogSource`, all
  vendor mentions.
- **Package member discovery: auto-detect.** A package root
  (`package.toml`) auto-discovers its member nodes (any subdir with a
  `metadata.json`), no hand-maintained `nodes = [...]` list. Fits
  "drop a folder and it works"; `package.toml` becomes "this dir is a
  package root with shared deps + shared `.rs`", not a manifest.
- **Collision = hard error.** Two units declaring the same `node_type`
  under `nodes/` is ambiguous, not a shadow. Fail loud with both paths
  (no-fallback rule).
- **Merge the two register paths** into one: a unit is a package root
  (has shared code/deps, ≥1 member node) or a bare node (one node, no
  shared code); both produce a `Package`.
- **All node-aware work moves local; the dispatcher only ever sees an
  already-compiled `ProjectDefinition`.** Self-containment + "dispatcher
  does pure routing, no node knowledge, no domain compute" demand it.
  Three current violations all resolve the same way:
  - `/parse`, `/validate`, `/describe/nodes` (editor convenience):
    deleted from the dispatcher; the extension drives them through the
    CLI reading `nodes/`.
  - `POST /projects` register (`project.rs:120`) currently re-parses
    AND enriches the source against `stdlib_catalog()`. It cannot
    enrich nodes it can't see. Fix: the CLI sends the already
    compiled+enriched `ProjectDefinition` (it already does this work
    locally during build/ensure), the dispatcher stores it opaque and
    does zero node-aware work. This is the correct shape, not a
    compromise: the dispatcher as a dumb store of a compiled artifact.

## Work (single delivery, ordered for clean implementation)

### A. Catalog: single root, merged forms, auto-detect members

1. `FsCatalog::discover` takes one root (`project_root/nodes/`). Delete
   `CatalogSource` multiplicity and `CatalogOrigin` (no more
   stdlib/vendor/user origin distinction; everything is a user node).
2. Merge `register_single_node_package` / `register_multi_node_package`
   into one `register_unit`. Package root auto-detects members by
   scanning for `metadata.json` subdirs; bare node is the degenerate
   one-member case.
3. Collision detection: inserting a `node_type` that already exists is
   a hard `CatalogError` naming both source dirs.
4. `stdlib_root()` stays in the crate but is used ONLY by `weft new`.
   Delete `stdlib_catalog()` (it discovers from the weft repo; nothing
   should do that at build/parse time anymore). Repoint its callers.

### B. Build weld removal

5. `build_project_catalog(project_root)` discovers the single
   `nodes/` root. No stdlib, no vendor.
6. `codegen.rs` / `worker_image.rs`: strip node `#[path]` against
   `project_root/nodes` (new anchor), rebase onto a new mount
   `/weft/project-nodes` (rename `CATALOG_MOUNT` → `NODES_MOUNT`).
7. `stage_build_context`: copy `project_root/nodes` into the build
   context at the mount path. Stop copying `weft_root/catalog`. (The
   weft `crates/` are still staged: nodes depend on `weft-engine`,
   `weft-core` etc. as path deps. That dependency is the language
   runtime, not the node library, and is correct to keep.)
8. Dockerfile template: update the catalog COPY/WORKDIR to the new
   mount.

### C. `weft new` seeds the stdlib

9. After `Project::init` creates `nodes/`, copy the contents of
   `stdlib_root()` (the weft repo `catalog/`) into `nodes/`. Recursive
   copy, skip symlinks. This is the sole `stdlib_root()` build/scaffold
   consumer remaining.

### D. Local parse path (editor self-containment)

10. CLI grows a parse/describe surface reading `nodes/`: implement the
    `describe-nodes` stub; add a parse/validate path the extension can
    call (CLI subcommand emitting JSON, reusing `parse_only` +
    `build_project_catalog`).
11. Extension: repoint `/parse`, `/validate`, `/describe/nodes` from
    `client.<dispatcher>` to the local CLI.
12. Dispatcher: delete `api/parse.rs`, the `/parse` `/validate`
    `/describe/nodes` routes, and their `stdlib_catalog()` use.

### E. Tests

13. Layer-1: discovery over a temp `nodes/` tree exercising both forms
    at depth 1 and depth N, auto-detected package members, and a
    collision (expect hard error).
14. Update existing `enrich_tests` / `validate_tests` (they call
    `stdlib_catalog()`) to discover from a fixture `nodes/` dir.

## Blast radius (stdlib_root / stdlib_catalog / vendor callers)

- `weft-catalog/src/lib.rs`: discover, stdlib_catalog (delete), tests.
- `weft-compiler`: build.rs (sources), codegen.rs, worker_image.rs,
  describe.rs doc, lib.rs doc, tests.
- `weft-cli`: infra.rs:358 (repoint to project root), new.rs (clone),
  describe_nodes.rs (implement), ensure.rs/status.rs (already local).
- `weft-dispatcher`: api/parse.rs (delete), describe.rs (delete),
  project.rs:120 (this one compiles a registered project; check
  whether it should read uploaded source or also move local).
