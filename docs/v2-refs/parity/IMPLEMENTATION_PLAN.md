# Implementation Plan (capsule memory after spec phase)

**Purpose**: This file is my written-down context for after
Quentin resets my memory. The deep specs in this folder
(parser.md, project-editor.md, etc) have the "what". This file
has the "how + why + current state" so I can pick up where I
left off.

## Current status (post-parity-pass)

Phases W1 / W2 / W3 / W4 / S1 / R1 / D18 all landed. See each
spec's `v2 port status` for component-level detail. Still open:

- Phase R2: `config_spans` per field on NodeDefinition in the
  Rust parser. Blocks Phase S16 granular `updateNodeConfig`.
- Phase S16: `expandNodeToMultiLine` + granular configSpans +
  `tryMaterializeAnon`. Depends on R2.
- form_builder full UI + CodeEditor + BlobField upload (deferred
  for scope reasons; spec calls these out).

Key files to know now:
- `extension-vscode/src/webview/compose/` — all composition
  primitives (group-synthesis, edge-rewrite, visibility,
  exec-overlay, scope-lock, cycle-check, layout).
- `compose/index.ts` exposes `composeGraph` — Graph.svelte uses
  this one call.
- `extension-vscode/src/surgical.ts` — all Stage-1 mutations.
- `extension-vscode/src/webview/components/*.svelte` — ported
  per spec.

## Current state of the tree

- All 16 parity specs in `weft/docs/v2-refs/parity/` are
  complete. Totals ~5000 lines. Each has a "v2 port plan" +
  "divergences" section.
- `GroupDefinition` added to `weft-core::project` and exposed
  via `weft-core::lib` + compiler's `ProjectDefinition`. Mirrored
  in `extension-vscode/src/shared/protocol.ts` as
  `GroupDefinition + ProjectDefinition.groups: GroupDefinition[]`.
  Populated in compiler's `collect_group_definitions` before
  `flatten_group` runs. Fields that ARE populated: id,
  in_ports, out_ports, one_of_required, parent_group_id,
  child_group_ids, node_ids, label (currently just = id).
  Fields still `None` / TODO: `span`, `header_span`,
  `originalName` (label should be LOCAL name for nested, not
  scoped id), `rawLines`.

## What's already ported (but needs audit against specs)

The VS Code extension webview has partial components. These
were written BEFORE the deep-read phase and need auditing
against the parity docs:

- `port-marker.ts` — ported verbatim (port-system.md)
- `port-context-menu.ts` — ported verbatim (port-system.md)
- `field-editor.svelte.ts` — ported verbatim (field-editor.md)
- `status.ts` — ported verbatim (types.md / execution.md)
- `weft-type.ts` + `colors.ts` — ported verbatim (colors.md)
- `ProjectNode.svelte` — partial (see project-node.md "v2 port
  status" for gaps: textareaHeights, debug preview 4-states, live
  data, Setup Guide, nowheel, executions array instead of single
  exec, _raw handle wrapping)
- `GroupNode.svelte` — stub only (collapsed-only rendering, no
  __inner handles, no scope-lock). Needs FRESH port against
  group-node.md.
- `CustomEdge.svelte` — wrong (has label, missing
  EdgeReconnectAnchor). Needs rewrite per edges.md.
- `JsonTree.svelte`, `ExecutionInspector.svelte` — partial. Need
  exec-array + pager + per-column copy per execution-inspector.md
- `CommandPalette.svelte` — stub. Needs full actions + preview
  panel.
- `AnnotationNode.svelte` — NOT ported. Needs markdown renderer.
- `Graph.svelte` — wires everything but missing: group
  synthesis, passthrough hiding, edge rewriting,
  topo-sort, z-index rules, scope-lock drag, viewport
  anchoring, visibility walk.
- `surgical.ts` (extension host) — has addNode/removeNode/
  addEdge/removeEdge/updateConfig/updateLabel/duplicateNode.
  Missing: addGroup, removeGroup, renameGroup, updateGroupPorts,
  moveNodeScope, moveGroupScope, updateNodePorts,
  updateProjectMeta. Plus: per-field configSpans (blocked on
  backend), expandNodeToMultiLine, tryMaterializeAnon.

## The implementation order (committed plan)

### Phase R (runtime parser / compiler gaps)

Before most webview work, we need the Rust compiler to expose
more metadata. These are small additions to
`crates/weft-compiler/src/weft_compiler.rs` and
`crates/weft-core/src/project.rs`:

1. **GroupDefinition.span + header_span + originalName +
   rawLines**: when walking `ParsedGroup` to collect
   `GroupDefinition`, capture the source span (line range of the
   `= Group { ... }` block), header_span (just the declaration
   line), originalName (local identifier, not scoped), and
   rawLines (verbatim text). Needed by:
   - serializer ops (renameGroup, moveGroupScope,
     updateGroupPorts)
   - webview's group label rendering (local name)
2. **NodeDefinition.configSpans per field**: v1's ParsedNode has
   `configSpans: Record<key, ConfigFieldSpan>`. Add to the Rust
   parser — during `parseNodeBlockBody` equivalent, track
   start/end lines per field + origin (inline/connection). This
   unblocks `updateNodeConfig` doing granular splices instead of
   whole-node rewrites.

These are small enough to land together. Neither blocks webview
start; the webview uses coarse updateConfig until they land.

### Phase W1 (webview foundation)

3. **Graph.svelte core rewrite**: group synthesis from
   `project.groups[]`, hide passthroughs, rewrite edges. This is
   the key change that makes groups render as v1 did.
   - Detect all `{groupId}__in` / `{groupId}__out` nodes; mark hidden.
   - For each `GroupDefinition`: synthesize a virtual NodeInstance
     with nodeType `'Group'`, inputs=group.inPorts,
     outputs=group.outPorts, config derived from layout sidecar,
     parentId from group.parentGroupId.
   - Rewrite edges touching passthroughs:
     - `source="gid__in"` → `source="gid"`, append `__inner` to
       sourceHandle (internal-source side of an in-port).
     - `target="gid__out"` → `target="gid"`, append `__inner` to
       targetHandle (internal-target side of an out-port).
     - `target="gid__in"` external → `target="gid"`, handle
       stays bare.
     - `source="gid__out"` external → `source="gid"`, handle
       stays bare.
   - Topo-sort: groups before children, parent-group before
     child-group. xyflow requires this.
   - Visibility walk: any ancestor with `expanded=false` hides
     the node (`style: 'display: none;'`, `parentId: undefined`,
     edges hidden).
   - Z-index: annotations -1, expanded groups -1+nestingDepth,
     collapsed groups + regular nodes 4.
   - Per project-editor.md `buildNodes`.

4. **GroupNode.svelte fresh port** matching group-node.md:
   expanded frame (dashed border, side ports with dual handles,
   NodeResizer, Label editing, add-port UI, context menu) +
   collapsed pill (same shape as ProjectNode collapsed, bare
   external handles only, line-clamp description with "Show
   more/less").

5. **CustomEdge.svelte rewrite** per edges.md: bezier + no
   label + 20px EdgeReconnectAnchor at target end + hide-while-
   reconnecting.

### Phase W2 (interactions)

6. **Scope-lock drag** (scope-lock.md): port the 3-check
   pipeline (checkNodeLeavesGroup, checkNodeCapturedByGroup,
   checkGroupCapturesNodes), preDragPositions map, 3-second
   debounced toast "Cannot change scope". Wire to xyflow's
   onnodedragstart / onnodedragstop / onselectiondragstop. The
   accompanying surgical mutations (`moveNodeScope`,
   `moveGroupScope`) need to exist too — see phase S.

7. **Expand/collapse with viewport anchoring**: matches
   project-editor.md viewport anchoring. Capture top-right
   corner in screen space before toggle, run ELK (phase W3),
   compute post-toggle position, offset viewport by delta. The
   double rAF wait is not cosmetic — xyflow's `measured` field
   isn't settled until after it.

### Phase W3 (layout)

8. **ELK per-scope layout** (layout.md): port `autoOrganize`
   from v1 weft-parser.ts:4714-5442. Bottom-up per-scope with
   SEPARATE_CHILDREN wrapper. Connected-component finder.
   Disconnected-component side-by-side arrangement. Port Y
   positions match CSS constants (NODE_PORT_START_Y=58,
   NODE_PORT_HEIGHT=25, NODE_PORT_GAP=4,
   GROUP_PORT_START_Y=44, GROUP_PORT_HEIGHT=30,
   GROUP_PORT_GAP=6).

9. **measurePortPositions from DOM**: before running ELK,
   measure actual handle Y positions for each node. Fallback to
   CSS constants during streaming (no DOM yet).

### Phase W4 (node components)

10. **ProjectNode audit** per project-node.md:
    - Accept `executions: NodeExecution[]` (not single exec).
    - Live data rendering (text/image/progress).
    - Setup Guide collapsible.
    - Debug preview 4-state placeholder + spin animation.
    - Textarea height persistence via ResizeObserver +
      `config.textareaHeights`.
    - `nowheel` class toggle on code/textarea focus.
    - `_raw` handle wraps the SVG (Handle IS the square).
    - Run Location selector (deferred, playground-only).
    - Infra status badge (deferred, playground-only).
    - Verify status glow class names match v1 exactly.

11. **ExecutionInspector** per execution-inspector.md: accept
    executions array, inline `‹ N/M ›` pager, magnifier button,
    3-column modal (Input/Details/Output), per-column copy,
    full-text copy, footer (status/duration/cost/timestamp),
    proper formatDuration/formatCost (already in status.ts).
    Status-specific Details column text (running / waiting /
    completed / failed / skipped with correct colors + pulse).

12. **CommandPalette** per command-palette.md: full actions
    list (save/run/undo/redo/selectAll/duplicate/delete/fitView/
    autoOrganize — some map to VS Code commands, some to
    webview handlers), preview panel with input/output/tag
    chips, `data-selected` attribute + scroll-into-view,
    category icons, ranked fuzzy search.

13. **AnnotationNode** per annotation-node.md: markdown
    rendering via `marked` (add dep), NodeResizer, double-click
    edit mode, per-element CSS for h1/h2/h3/p/code/pre/ul/ol/
    blockquote/a/placeholder.

14. **Execution overlay in Graph.svelte** per execution.md:
    - Group execution synthesis from `{gid}__in`/`{gid}__out`/
      internal children's exec records.
    - `class: 'node-running' | 'node-completed' | 'node-failed'`
      applied via xyflow wrapper based on latest exec status.
    - Edge active class + animated + stroke-width patch.
    - Protocol needs full `NodeExec` shape (pulseIdsAbsorbed,
      costUsd, logs, color, lane) — extend shared/protocol.ts.

### Phase S (surgical mutations in dispatcher)

Run these in parallel with W1-W4 since the webview needs some
of them:

15. **Stage 1 (simple, whole-region ops)** per serializer.md:
    - `addGroup(code, label, parentGroupId?)`
    - `removeGroup(code, groupLabel)` — with de-indent +
      self-connection filter
    - `renameGroup(code, oldLabel, newLabel)` — regex global
      rename + caveat
    - `updateGroupPorts(code, groupLabel, inputs, outputs)`
    - `moveNodeScope(code, nodeId, targetGroupLabel?)` — with
      connected-node guard
    - `moveGroupScope(code, groupLabel, targetGroupLabel?)`
    - `updateNodePorts(code, nodeId, inputs, outputs)` + 
      `invalidateOrphanedConnections`
    - `updateProjectMeta(code, name?, description?)`

    Each lives in `extension-vscode/src/surgical.ts` initially
    as the TS shim; long-term home is Rust dispatcher with the
    extension just applying WorkspaceEdit ops. For phase A,
    TypeScript is fine — parser still reparses via dispatcher.

16. **Stage 2 (depends on Phase R per-field configSpans)**:
    - `expandNodeToMultiLine(code, node)` (3 cases A/B/C).
    - Granular `updateNodeConfig` via configSpans splice +
      origin-preserving prefix + buildFieldLines.

17. **Stage 3 (deferred until needed)**:
    - `tryMaterializeAnon(lines, anonId)` — inline anon
      materialization on binding-edge removal.

### Phase D (dispatcher SSE enhancements)

18. **activeEdges events**: emit `EdgeActive { color, edgeId }` /
    `EdgeInactive` when pulses enter/leave an edge. Simpler
    approximation for phase A: on `NodeStarted(X)`, mark all
    incoming edges of X active for 200ms; on
    `NodeCompleted(X)`, mark outgoing edges active for 200ms.
    Upgrade to exact tracking later.

### Phase protocol

19. Extend `shared/protocol.ts` per types.md:
    - `FieldDef` full fields (min/max/step/pattern/accept/
      provider/maxLength/minLength/defaultValue).
    - `LiveDataItem`.
    - Full `NodeExec` shape (pulseIdsAbsorbed, costUsd, logs,
      color, lane).
    - `NodeCategory` enum.
    - `NodeExecutionStatus` full set.

## Deferred (not in this implementation pass)

- `@file(./path)` sugar in Rust compiler. Follows a different
  design (simple path resolution; per Quentin's direction).
- File-backed edits via surgical (writes to target file instead
  of .weft when field value is `@file(...)`).
- CodeEditor with CodeMirror (bundle size cost ~500KB).
- BlobField upload (phase B cloud R2 endpoint).
- Runner mode / published projects.
- Undo stack in webview (VS Code's native text undo suffices).

## Divergences (v2 intentionally differs from v1)

- Layout persisted to `.layout.json` sidecar, not `@layout` in
  source.
- No in-webview undo stack (VS Code Ctrl+Z on the text buffer).
- No right sidebar / config panel / history panel (config
  inline in ProjectNode body; history in ExecutionInspector).
- No code panel embedded (VS Code IS the code editor).
- Surgical ops live in dispatcher eventually (TypeScript shim
  in extension host for phase A).
- Streaming code-gen happens in extension host, webview just
  gets updated `project` after each streaming parse.
- BlobField is URL-paste only in phase A.
- ActiveEdge tracking is approximate in phase A.

## How to resume

1. Read `weft/docs/v2-refs/parity/README.md` (index).
2. Read `weft/docs/v2-refs/parity/IMPLEMENTATION_PLAN.md`
   (this file).
3. Start at Phase R unless Quentin says otherwise.
4. Each phase step cross-references a specific parity doc for
   the "what". This plan says the "when/why/current state".

## Things I don't want to forget

- v1's parser keeps groups as `nodeType: 'Group'` NodeInstance
  in the public output. v2's compiler flattens to passthroughs
  by default. The webview un-flattens by reading
  `project.groups`. Same end-rendered state as v1.
- `__inner` suffix on edge handles distinguishes internal vs
  external side of a group boundary port. Do NOT strip it at
  render time — GroupNode renders BOTH bare-named and
  `__inner`-suffixed handles per side-port.
- Cycle detection MUST skip `__inner` edges (they represent
  group boundary pass-through, not real dependencies).
- `isValidConnection` scope check uses `getHandleScope(nodeId,
  handleId)`: if group + handle ends with `__inner` → scope is
  the group itself; else → parentId or '__root__'. Both sides
  of an edge must be in same scope.
- Viewport anchoring on expand/collapse requires tick() +
  requestAnimationFrame × 2 + runAutoOrganize flow. The double
  rAF waits for xyflow's measurements to settle.
- `updateNodeInternals(nodeId)` must be called after port changes
  so xyflow rescans Handle positions.
- When restoring from undo history (if we end up needing it),
  JSON clone destroys function references. Re-attach
  `onUpdate` callbacks per node after restore.
- Group execution synthesis: input = `__in.output`, output =
  `__out.output`. Status aggregates across all related. Cost
  sums across all related. Paired by index `inExecs[i]` with
  `outExecs[i]` because groups can execute multiple times per
  session.
- scope-lock rule: a node with edges to same-scope neighbors
  can't change scope. It's dataflow scoping mirroring lexical
  scoping in programming languages. Snap back + debounced
  toast.
- `computeMinNodeWidth` (ProjectEditorInner.svelte:811-830)
  covers both collapsed regular nodes AND collapsed groups. Port
  verbatim.
- `_raw` output port is synthetic on all non-Group nodes,
  rendered as a 10x10 square in top-right corner. The Handle
  wraps the SVG (Handle IS the square; I got this wrong in my
  partial port).

## Honest audit of where I was sloppy

The earlier partial port (before this spec pass) was
surface-level. Cumulative mistakes:
1. Kept the webview pinned to VS Code theme; v1 uses a fixed
   light palette. v2 should too.
2. Rendered edge labels always; v1 never renders them.
3. Drag position reset on every parse; should diff-update.
4. Treated `exec` as single entry; v1 uses `executions[]` array
   with history navigation.
5. Ported GroupNode as a collapsible pill only; missed the
   entire dual-handle expanded frame.
6. Collapsible used a simple chevron; v1 uses Maximize2/Minimize2
   icons with viewport anchoring.
7. Command palette had a simple list; v1 has preview panel +
   full action set + category icons.
8. `_raw` handle was rendered as sibling of SVG; v1 wraps.
9. CSS debug-placeholder 4 states + spinner animation not
   ported.
10. Live data (text/image/progress) not rendered.
11. Scope-lock drag not implemented.

The parity specs now have each of these gaps explicitly listed
in their "v2 port status" sections.

## Order-of-operations note

Phase R (backend parser additions) doesn't BLOCK any webview
work — the webview can use coarse updateConfig until per-field
configSpans land. But it DOES enable nicer surgical edits once
ready. Recommended sequence:
- Start Phase W1 (Graph rewrite + GroupNode + CustomEdge) in
  parallel with Phase R.
- When Phase R ships configSpans, land Phase S Stage 2
  (granular updateConfig).
- Otherwise straight down W1 → W2 → W3 → W4 then S then D.

## Key file paths to reach quickly

- `weft/docs/v2-refs/parity/*.md` — all specs.
- `weft/crates/weft-core/src/project.rs` — ProjectDefinition, GroupDefinition.
- `weft/crates/weft-compiler/src/weft_compiler.rs` — parser,
  flatten, collect_group_definitions.
- `weft/extension-vscode/src/shared/protocol.ts` — shared types.
- `weft/extension-vscode/src/surgical.ts` — extension host
  surgical edits.
- `weft/extension-vscode/src/webview/Graph.svelte` — main
  orchestrator (needs rewrite).
- `weft/extension-vscode/src/webview/components/*.svelte` —
  node / edge components (most need rewrites).
- `weft/extension-vscode/src/webview/utils/*` — ported utilities.
- `weft/dashboard-v1/src/lib/` — v1 source (read via spec, not
  directly).
