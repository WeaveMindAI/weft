# Graph edit pipeline: optimistic projection with unified rollback

## Context

The Weft VS Code extension's graph editor has a recurring bug. When the user edits the graph quickly (drag, connect, rename, delete, type into a config field), each edit takes about half a second to apply to the underlying `.weft` source. If edits are stacked within that window, they corrupt each other: the last one sometimes silently fails to apply, the visual diverges from the source, and the user has to reload or undo into a clean state.

The half-second is structural. The round-trip is `webview -> postMessage -> host -> parse-server -> CST edit + reparse -> WorkspaceEdit.replace + doc.save() -> applyParseResult -> webview`. The bulk is VS Code's `applyEdit + save()` writing to disk, not our parse-server. We cannot make it instant. We can make it FEEL instant by keeping the visual responsive while the round-trip runs in the background.

The current code tries to do that with optimistic mutations on the live `nodes`/`edges` arrays before sending the EditOp, but there is no rollback when the server rejects. The current code also has a silent bail in `applyExternalSource` (ProjectEditorInner.svelte:1664-1667) that drops a host-authoritative parse if a config-typing debounce is pending and the source differs. The bail was added to protect in-progress typing from being clobbered, but it also drops the host's parsed result for any structural edit the user did mid-typing, leaving the visual unreconciled. On top of that, there are at least 7 different preflight rejection paths (cycle detection, single-driver rule, scope-boundary validation, scope-blocking on drag, scope-blocking on capture, reconnection-failure detection, missing-node-hydration) and 5 different post-flight rejection paths (server-side move-with-cross-boundary-connections, duplicate-ID, wrong container kind, generic edit-server rejection, post-edit parse error), each with its own ad-hoc rollback shape (position-restore-from-map, edge-filter, layout-snapshot, null-return, implicit-skip, toast/alert/console.warn). The "post-edit parse error" path has no rollback at all; the visual just diverges from the source silently.

We are reshaping this into one mechanism. The visible graph becomes a derived projection of `(truth, pendingOps, layoutCode)`. Truth is the most recent parsed source. PendingOps is a FIFO log of optimistic edits not yet confirmed by the server. The user's gesture applies immediately because the projection re-derives synchronously when the op is appended; the EditOp is sent in the background; on confirmation it leaves pendingOps and truth advances; on rejection it leaves pendingOps and the truth is resynced from the server so the visual snaps back to a consistent state. Preflight rules (cycle, single-driver, etc.) still exist, but they now produce the same rejection shape as a server rejection so the rollback path is uniform. A short toast tells the user which action failed and why. There is one path. No silent drops. No phantom edges.

## Shape

### Three inputs, one derived visible state

```
truth: { project: ProjectDefinition; weftCode: string }   // last successful parse
pendingOps: PendingOp[]                                    // optimistic queue
layoutCode: LayoutCode                                     // positions/sizes/collapse

visible = derive(truth, pendingOps, layoutCode, catalog)
nodes   = applyOverlays(visible.nodes, executionState, infraState, ...)
edges   = visible.edges
```

`derive` is pure JS. It takes the parsed project, applies each pending op in order via a per-op-kind visual function, merges with the layout, and returns a projected project. `buildNodes` and `buildEdges` (the existing functions) then convert that to xyflow shape. The total cost matches today's `patchFromProject`, which already runs on every parseResult. We do not introduce incremental projection. One re-derivation per change to any of the three inputs.

`nodes` is `$derived` on top of `visible.nodes` plus all the overlays (execution status, infra status, file contents, bus participation, subgraph highlight, body feed). These are read-only computations today wrongly expressed as effects that mutate `nodes` in place; the reshape moves them into the derivation. xyflow sees one `nodes` array and is happy.

### PendingOp

```ts
type PendingOp = {
  id: string                                    // local UUID for tracking
  op: EditOp                                    // the wire op sent to host
  produces: string[]                            // entity IDs this op creates (node id, group label, etc.)
  consumes: string[]                            // entity IDs this op references (and ports as `${nodeId}.${portName}`)
  rewrites: Array<{from: string; to: string}>   // rename/move id substitutions
  layoutInverse?: LayoutOp[]                    // how to undo the layout side-effect, if any
  state: 'pending' | 'sending' | 'failed'       // 'confirmed' = removed from queue
  origin: 'gesture' | 'preflight-rejected'      // see preflight unification below
}
```

`analyzeOp(op, catalog)` returns `{produces, consumes, rewrites}` for every EditOp variant. Lives in `extension-vscode/src/webview/lib/projection/analyze.ts`. One branch per op kind. Unit tested. Rename and move ops produce `rewrites` so subsequent ops' `consumes` are translated through them at dependency analysis time.

### The gesture-to-confirmation flow

1. User gesture (drag, connect, click, type) calls `recordEdit(ops, layoutMutator?)`.
2. `recordEdit` runs preflight checks for each op. If any check rejects, the op is converted into an immediate failure: it's pushed into pendingOps with `state: 'failed'` and `origin: 'preflight-rejected'`, the rejection toast is shown via the same handler that handles server rejections, and the op is then removed by the same rollback path. This is the "one path" requirement. (For perf: we can short-circuit the actual queue insert for preflight rejections and call the rejection handler directly, AS LONG AS the rejection handler is the same code path either way.)
3. For ops that pass preflight, `recordEdit` appends them to pendingOps with `state: 'pending'`. The projection re-derives synchronously. The user sees the change immediately.
4. The layout mutator (if any) runs immediately and writes to `layoutCode`. The `layoutInverse` is captured onto the op.
5. The op is sent to the host via `applyEdits` RPC over the existing `historyChain` (which serializes the await side so order is preserved).
6. While the op is in flight, `state` is `'sending'`.
7. Host responds `editApplied {ok: true, inverse, newSource, newParse}`. We:
   - Replace `truth = {project: newParse, weftCode: newSource}`.
   - Remove the op from pendingOps.
   - Push an undoStack entry of kind `confirmed` with the inverse TextEdit and the layoutInverse.
   - Re-derive (truth advanced AND pendingOps shrank, both inputs changed in one tick).
8. Host responds `editApplied {ok: false, reason}`. We:
   - Send a `resyncSource` request to the host (new message kind, see Host changes below).
   - Host responds with the current parsed truth (no edit applied).
   - Replace `truth` with the resynced parse.
   - Remove the failed op from pendingOps.
   - Re-validate the rest of pendingOps against the new truth: any whose `consumes` reference entities not in the new truth (or whose `produces` already exist) are also removed, each with its own toast.
   - Re-derive.
   - Run the layoutInverse for the failed op.
   - Show a toast: "Edit failed: <human-readable reason>. Rolled back to last good state."

The resync on rejection is deliberate. We do NOT try to mirror server semantics locally. The server is authority on what its truth is post-rejection. One round-trip per failure is cheap (failures should be rare; one round-trip is what we pay per success).

### Truth replacement from outside the gesture path

When the user types in the `.weft` text tab, the host eventually posts a `parseResult` to the webview. With the reshape:
- Replace `truth` with the new parse.
- Do NOT bail. The current `applyExternalSource` early-return at line 1665-1667 is removed entirely.
- Re-validate pendingOps against the new truth (same logic as on rejection). Drop invalidated ops with a toast each.
- Re-derive.

Pending ops are always re-applied on top of the latest truth. The race the current bail was protecting against (host echo overwriting in-progress typing) is gone because the typing op lives in pendingOps and gets re-applied on top of any incoming truth.

### Config-typing inside the new model

The current code holds typing in `pendingConfigOps: EditOp[]` outside the projection, with a 1000ms debounce timer that flushes the buffer as one `recordEdit` batch. With the reshape, typing becomes a pendingOp of its own from the first keystroke:
- Keypress on a config field finds or creates a pendingOp with op kind `setConfig`, identity `(nodeId, key)`, current value = the typing buffer.
- Each subsequent keystroke replaces the pendingOp's value in place (NOT inserts a new one) so the queue stays small.
- A debounce timer (250ms, shorter than today's 1000ms because the bail is gone and we no longer need the long window for staleness protection) transitions the pendingOp's `state` from `'pending'` to `'sending'` and posts the EditOp.
- On confirmation: truth advances, op leaves the queue.
- On structural edit landing during typing: the structural op appends to pendingOps. Its EditOp is sent immediately (not debounced). Both ops go through `historyChain` in order. Host applies them in order. Confirmations advance truth one at a time. The typing op stays pinned on top of every truth update. The user keeps typing without seeing flicker.

### Preflight check unification

The preflight rules become one shared dispatcher: `runPreflight(op, currentVisible, catalog, lockState) -> { ok: true } | { ok: false, reason: string }`. Each rule is one function with its own check. Rule list (in order; `notLocked` runs first):
- `notLocked`: the logical-graph-edit path is locked. See "Logic lock" below.
- `noCycle`: addEdge that would create a cycle.
- `singleDriverPerInput`: addEdge whose target port already has a driver, replaced by removing the existing edge in the same op batch (this is a rewrite, not a rejection).
- `sameScope`: addEdge whose endpoints are in different scopes.
- `noStaleHydration`: moveNodeScope/moveGroupScope/moveLoopScope on a container whose kind hasn't hydrated yet.
- `noOrphanOnScopeChange`: moveNodeScope where the node has in-scope connections that would dangle (replaces the current `nodeHasConnectionsInScope` + `preDragPositions` mechanism).
- `noOrphanOnCapture`: drag-into-group analog.
- `validReconnection`: reconnect that resolved to an empty drop = removeEdge (this is also a rewrite, not a rejection).

### Logic lock and the code-edit race

Two failure modes to handle:

**A. External code edits in progress.** The user (or AI assistant) is editing the .weft file directly in the text tab. A graph edit landing on top of in-progress code edits would corrupt the code state. The AI case is especially important: an AI streams keystrokes every ~500ms, and a user graph edit could slip between them and break the code mid-write.

**B. Race between an in-flight graph edit and a code-tab keystroke.** Graph edit was sent to the host. Mid-roundtrip, the user types in the text tab. Even if neither side is "locked" the graph edit's `writeTextRaw` would overwrite the just-typed character.

We handle both with two gates feeding one effective state.

**Gate 1: time-based auto-lock.** When `onDidChangeTextDocument` fires on the watched doc (and `writingPaths` says it isn't our own write), the host posts `codeEditTouched` to the webview. The webview sets `codeEditLockUntil = Date.now() + 1000`. Every additional keystroke moves the deadline forward by 1000ms from that moment. While `Date.now() < codeEditLockUntil`, logical graph edits are rejected. After the deadline passes with no more keystrokes, the lock releases automatically; no extra message needed.

**Gate 2: explicit `lockGraphLogic` flag.** A boolean on the webview state. Set by an external caller (the AI assistant in future, or the user via a UI toggle). While `true`, logical graph edits are rejected. The webview shows a banner: "Graph locked while <reason>." `setGraphLogicLock({locked: boolean, reason?: string})` is the message the AI assistant sends; symmetric `unlockGraphLogic` clears it. The banner exposes a "deactivate lock" button for the user case.

**Effective state:**
```ts
isLogicLocked = Date.now() < codeEditLockUntil || lockGraphLogic
```

This feeds the `notLocked` preflight rule. If locked, the op rejects through the same uniform rollback path with reason `"Graph logic locked (Weft code is being edited)"` or `"Graph logic locked (AI is editing)"` or `"Graph logic locked"` (user-set).

**The lock is scoped to one path only: webview gesture -> source-mutating EditOp.** That is the only thing gated. Everything else flows normally:

- Code-tab keystrokes are not gated (the lock exists because the code is being edited; gating the code editor would be nonsensical and impossible from our side anyway).
- The truth advance path triggered by code edits stays open. `onDidChangeTextDocument` -> parse -> `parseResult` -> webview replaces truth -> re-derives the visible graph. This is what lets the user SEE the graph reflecting their typing while the lock is engaged.
- Layout/visual graph edits (drag positions, resize, collapse, expand, pan, zoom) stay open. The user can rearrange the visible representation freely.
- Pending ops re-validation runs normally when truth advances. Pending ops that were OK before the code edit but are now invalidated get dropped via the standard revalidation toast.
- Overlay updates (execution status, infra status) stay open.

Only the path `recordEdit` -> source-mutating op -> host RPC is gated by `isLogicLocked`. The `notLocked` preflight rule is the single check that enforces this.

**Gate 3: doc.version backstop on the host.** Even with both gates passing, a sub-second race remains possible: webview sent a graph edit at T=0, user typed at T=10ms (before the lock had a chance to be posted to the webview), server finished applying the edit at T=200ms. Without a check, `writeTextRaw` would overwrite the user's typing.

`applyEditTransaction` (graphView.ts:1148-1191) captures `doc.version` at the start. After parse-server returns the new source but BEFORE `writeTextRaw`, it checks `doc.version` is unchanged. If changed, the transaction aborts: no write, no parse mirror, reply `editApplied {ok: false, reason: 'code-was-edited'}`. The webview's standard rejection handler runs: resync, drop the op, toast "Graph edit dropped because Weft code was edited during the round-trip."

So three layers cover the case:
1. Preflight `notLocked` rejects most attempts before they round-trip.
2. The 1s auto-lock window covers the gap between "keystroke happened" and "webview knew about it" (the `codeEditTouched` message travels webview-ward).
3. doc.version check is the race-safe backstop on the host side for anything that slips through.

All three failure modes drain into the same rollback path: drop the op from pendingOps, resync truth, run layoutInverse, pop undoStack entry, show toast.

The "rewrite" rules (singleDriverPerInput, validReconnection) transform the op batch BEFORE the rejection check, so they remain preflight optimizations. The pure "reject" rules produce a uniform `{ok: false, reason}` that flows through the same rollback path as a server rejection.

The current ad-hoc visual reverts (preDragPositions, edge-filter, position-restore-from-map, null-from-onBeforeConnect) all DISAPPEAR. The reshape uses one mechanism: if preflight rejects, the gesture's visual effect never lands in pendingOps and the projection never reflects it. The user sees their drag snap back because the projection re-derives WITHOUT their attempted op. One toast. One path.

For preflight gestures that already wrote to `layoutCode` (e.g. mid-drag position updates), the gesture handler must either (a) hold the layout write until after preflight, or (b) capture a layoutInverse for the rejection handler to apply. Cleanest is (a) for drags (xyflow's `onNodeDragStop` is where preflight runs, so layoutCode isn't committed until the drag ends successfully) and (b) for gestures that commit layout earlier.

### Undo and redo

`undoStack` entries become polymorphic:
```ts
type UndoEntry =
  | { kind: 'pending'; opId: string }
  | { kind: 'confirmed'; inverse: TextEdit; layoutInverse?: LayoutOp[] }
```

Append per recordEdit batch (one entry per gesture; multi-op gestures coalesce into one entry).

On op confirmation: swap the entry in undoStack from `pending` to `confirmed` (replace `opId` with the confirmed inverse).

On op rejection: pop the entry from undoStack (the user never saw this op succeed; pretend it didn't happen for undo purposes).

Ctrl-Z:
- If top entry is `pending`: drop the op from pendingOps. The layoutInverse on the op runs. The projection re-derives.
- If top entry is `confirmed`: replay `inverse` as a fresh applyTextEdit RPC. Run layoutInverse locally.

Ctrl-Y mirrors.

This collapses the current `undoStack`/`redoStack` shapes into one polymorphic stack and eliminates the "action skipped from undoStack because source was undefined" silent path at ProjectEditorInner.svelte:1363-1366.

### Host changes

`extension-vscode/src/shared/protocol.ts`: add the following message types.
- Webview to host: `{ kind: 'resyncSource'; requestId: number }`.
- Host to webview: `{ kind: 'sourceResynced'; requestId: number; project: ProjectDefinition; source: string; layoutCode: LayoutCode }`.
- Host to webview: `{ kind: 'codeEditTouched' }` (posted on every external `onDidChangeTextDocument` on the watched doc, after the `writingPaths` check).
- Webview to host: `{ kind: 'setGraphLogicLock'; locked: boolean; reason?: string }` (used by future AI assistant integration; also exposed via a UI toggle).
- Add `reason?: string` to the `editApplied { ok: false }` shape so the host can carry the EditError variant's human-readable translation OR the `'code-was-edited'` sentinel.

`extension-vscode/src/graphView.ts`:
- Handle `resyncSource` by reading the open doc's text, calling parse-server's `kind:'parse'`, and posting `sourceResynced`. Wrap in `serializeOnPath` to ensure the resync sees the post-rejection truth.
- In the existing `onDidChangeTextDocument` handler at lines 254-268, after the `writingPaths` skip check, post `codeEditTouched` to the webview.
- In `applyEditTransaction`: capture `doc.version` at entry. After `parseServer.request({kind:'edit', ...})` returns but before `writeTextRaw`, check `doc.version` is unchanged. If changed, abort: skip `writeTextRaw`, skip `applyParseResult`, reply `editApplied {ok: false, reason: 'code-was-edited'}`. The serializer still returns cleanly (no exception, just an early return).
- Attach `source`, `project`, `layoutCode` to the successful `editApplied` reply so the webview can advance truth in one message.

The existing `parseResult` message stays for text-tab edits (where there is no `editApplied` to piggy-back on).

### Server-side changes

`crates/weft-compiler/src/edit.rs` and `edit/ops.rs`: no behavior change required. The Rust side already returns clear `EditError` variants. We just need to make sure the host extension translates each variant into a human-readable reason for the toast (e.g. `EditError::InvalidArgument(msg)` for move-with-cross-boundary-connections passes `msg` through; `EditError::DuplicateId(id)` becomes "An item named '{id}' already exists in that scope"; etc.).

The current Rust check for `move_scope` that rejects cross-boundary connections (ops.rs:1052-1140) stays. With the unified flow, this becomes a server-side rejection that gets resync'd and toasted like any other. The preflight `noOrphanOnScopeChange` check still runs locally to avoid the round-trip when we can detect it, but the server check is the backstop.

### What goes away (delete list)

- `applyExternalSource`'s silent bail (ProjectEditorInner.svelte:1664-1667).
- `pendingConfigOps` flat buffer (ProjectEditorInner.svelte:2783) and `saveProjectTimer` ad-hoc state. Replaced by typing-as-pendingOp.
- `preDragPositions` map and the position-restore logic in `onNodeDragStop` (ProjectEditorInner.svelte:2463-2504). Replaced by preflight that prevents the layout commit when the move would be rejected.
- The `checkGroupCapturesNodes` ad-hoc reject (lines 2633-2677). Replaced by preflight.
- The `weftMoveScopeAny` hydration-race bail (lines 214-221). Replaced by preflight `noStaleHydration`.
- The cycle-detection inline DFS at the connect site. Becomes preflight `noCycle`.
- The single-driver edge filter at the connect site. Becomes preflight rewrite `singleDriverPerInput`.
- The reconnection-failure flag-state machine (lines 1921-1970). Becomes preflight rewrite `validReconnection`.
- The `commit` function's "source undefined skips undoStack" silent path (ProjectEditorInner.svelte:1363-1366). Replaced by uniform rejection handling.
- The `editInFlight` counter (ProjectEditorInner.svelte:1303, 1311, 1319) and the layout-ownership conditional in `applyExternalSource` (line 1675). Replaced by the projection model: pendingOps inherently captures "we have unconfirmed work."
- `mergeInferredPortMetadata`. Subsumed by the derivation.
- The overlay effects that mutate nodes in place (ProjectEditorInner.svelte:978, 1137, 1173, 1191, 1213-1266). Replaced by the `applyOverlays` derivation.
- The 4 separate `flushAllPendingSaves` call sites in App.svelte (around lines 468, 473, 483, 493, 499, 504, 508, 512, 524). With typing as pendingOp, there is one queue. Before a "run" or "activate" action, the call site asks for `awaitAllConfirmations()` once instead.
- The parse-error-after-edit-no-rollback gap at App.svelte:201-203. Becomes a server-side rejection with resync.

## Steps

This is one delivery. The order below is implementation sequence; everything ships together. No intermediate state has half the old model and half the new.

1. **Create the projection library.** New directory `extension-vscode/src/webview/lib/projection/`:
   - `types.ts`: PendingOp, Truth, UndoEntry, RejectionReason types.
   - `analyze.ts`: `analyzeOp(op, catalog)` returning `{produces, consumes, rewrites}` for every EditOp variant in protocol.ts.
   - `apply.ts`: `applyOpToVisible(visible, op, catalog)` returning the post-op visible state. One branch per op kind. For `setConfig` on a form-builder field, the templating logic just leaves the un-templated ports in place (the eventual truth update will carry the templated ports correctly; we accept the brief stale-port window, same as today's port-inference window).
   - `dependencies.ts`: `transitiveDependents(ops, rejectedId)` returns the set to drop.
   - `validate.ts`: `revalidate(pendingOps, newTruth)` returns `{kept, dropped}` with reasons.
   - `derive.ts`: `derive(truth, pendingOps, layoutCode, catalog)` returns a projected `ProjectDefinition` ready for `buildNodes`/`buildEdges`.
   - Each module is unit tested. No Svelte yet, no host integration yet. Pure functions.

2. **Create the unified preflight module.** New file `extension-vscode/src/webview/lib/projection/preflight.ts`:
   - `runPreflight(ops, currentVisible, catalog, lockState)` returns `{ops: EditOp[], rejected: Array<{op, reason}>}` after applying rewrite rules and reject rules.
   - `lockState: { codeEditLockUntil: number | null; lockGraphLogic: boolean; lockReason?: string }`. Computed as `isLogicLocked = (codeEditLockUntil !== null && Date.now() < codeEditLockUntil) || lockGraphLogic`.
   - Each rule is one function: `notLocked` (runs first), `noCycle`, `singleDriverPerInput`, `sameScope`, `noStaleHydration`, `noOrphanOnScopeChange`, `noOrphanOnCapture`, `validReconnection`.
   - Layout-only ops (drag positions, resize, collapse, expand) bypass `notLocked` because they don't mutate source.
   - Unit tested.

3. **Extend the protocol.** `extension-vscode/src/shared/protocol.ts`:
   - Add `{ kind: 'resyncSource'; requestId: number }` (webview to host).
   - Add `{ kind: 'sourceResynced'; requestId: number; project: ProjectDefinition; source: string; layoutCode: LayoutCode }` (host to webview).
   - Add `{ kind: 'codeEditTouched' }` (host to webview).
   - Add `{ kind: 'setGraphLogicLock'; locked: boolean; reason?: string }` (webview to host, optional persistence; also a self-contained webview-state change for the simple case).
   - Extend `editApplied { ok: true, inverse }` to also carry `{ source, project, layoutCode }`.
   - Extend `editApplied { ok: false }` to carry `{ reason: string }` (a sentinel like `'code-was-edited'` or a user-readable EditError translation).
   - These are additive.

4. **Wire host changes.** `extension-vscode/src/graphView.ts`:
   - Handle `resyncSource` by reading the open doc's text, calling parse-server's `kind:'parse'`, and posting `sourceResynced`. Wrap in `serializeOnPath` to ensure the resync sees the post-rejection truth.
   - In `onDidChangeTextDocument` handler, after the `writingPaths` check, post `codeEditTouched` to the webview.
   - In `applyEditTransaction`: capture `doc.version` at entry; after parse-server returns, if `doc.version` changed, abort the transaction (no write, no parse mirror) and reply `editApplied {ok: false, reason: 'code-was-edited'}`.
   - In `applyEditTransaction`, attach `source`, `project`, `layoutCode` to the successful `editApplied` reply.

5. **The reshape of ProjectEditorInner.svelte.** This is the big disruptive change. Done in one PR; no half-state.
   - Add lock state: `let codeEditLockUntil = $state<number | null>(null)`, `let lockGraphLogic = $state(false)`, `let lockReason = $state<string | undefined>(undefined)`. Compute `isLogicLocked` as a $derived.
   - Handle incoming `codeEditTouched` message: `codeEditLockUntil = Date.now() + 1000`. Use a single `setTimeout` reset pattern so the timer self-fires to clear the lock when no more keystrokes arrive.
   - Handle incoming `setGraphLogicLock` from caller: update `lockGraphLogic` and `lockReason`. Render a banner with reason + "deactivate lock" button (button calls `setGraphLogicLock(false)`).
   - Pass `lockState` into `runPreflight` calls.
   - Replace `let nodes = $state.raw<Node[]>(...)` and `let edges = $state.raw<Edge[]>(...)` with:
     - `let truth = $state<Truth>(...)`
     - `let pendingOps = $state<PendingOp[]>([])`
     - `let visible = $derived(derive(truth, pendingOps, layoutCode, catalog))`
     - `let baseNodes = $derived(buildNodes(visible.project, layoutMap, catalog, ...))`
     - `let baseEdges = $derived(buildEdges(visible.project, ...))`
     - `let nodes = $derived(applyOverlays(baseNodes, executionState, infraNodes, fileContents, infraFeedByNode, signalFeedByNode, showInfraSubgraph, showTriggerSubgraph))`
     - `let edges = $derived(applyEdgeOverlays(baseEdges, ...))`
   - Rewrite `recordEdit` to: run preflight, fast-fail rejected ops through the rejection handler, append surviving ops to pendingOps, run layout mutator, capture layoutInverse onto the op, kick off the send via historyChain.
   - Rewrite the receive handler for `editApplied`:
     - On ok: advance truth from the reply payload, remove op from pendingOps, swap undoStack entry to `confirmed`.
     - On not-ok: send `resyncSource`, on the reply advance truth, remove failed op + invalidated dependents, run their layoutInverses, pop their undoStack entries, show toasts.
   - Rewrite `applyExternalSource` to: advance truth, revalidate pendingOps, re-derive. Delete the bail.
   - Delete the overlay effects (lines 978, 1137, 1173, 1191, 1213-1266). Move their logic into `applyOverlays` and `applyEdgeOverlays`.
   - Delete `preDragPositions`, the `checkNodeLeavesGroup` ad-hoc revert, `checkGroupCapturesNodes`'s implicit skip, the cycle-detection inline DFS, the single-driver edge filter at the connect site, the reconnection-failure flag machinery. Each is replaced by a preflight rule.
   - Rewrite `pendingConfigOps` / `flushPendingConfigOps`: typing becomes a `setConfig` pendingOp identified by `(nodeId, key)`. Debounce 250ms then transition to sending.
   - Rewrite `weftMoveScopeAny`: no more hydration-race bail; preflight `noStaleHydration` rejects upstream.
   - Rewrite undo/redo to use the polymorphic UndoEntry.
   - Delete `mergeInferredPortMetadata`, `editInFlight`, the `flushAllPendingSaves` 4-site dance.

6. **Wire App.svelte changes.** Replace the `pendingEdits` Map with one that handles both confirmed and rejected callbacks. Add `resyncSource` send and `sourceResynced` receive. Delete the parse-error-no-rollback bail (lines 201-203).

7. **Translate EditError variants to user-readable reasons.** `extension-vscode/src/graphView.ts`: map each `EditError` variant to a clear sentence used in the rejection toast. Move-with-cross-boundary-connections gets the existing Rust message verbatim (it's already user-readable). DuplicateId becomes "An item named '{id}' already exists in that scope." etc.

8. **Out of scope but verified untouched:** `streamingEdits.ts` (raw text writes, lives outside the EditOp model). `autoOrganize.ts` (layout-only, no source ops). Action-bar effects (don't touch nodes/edges).

9. **Cleanup pass.** After the reshape compiles, sweep the file for any remaining references to deleted symbols (preDragPositions, pendingConfigOps, saveProjectTimer, editInFlight, mergeInferredPortMetadata, showScopeBlockedToast) and the four old `flushAllPendingSaves` call sites.

## Tests

Layer 1 (pure-function unit tests on the projection library):
- `analyzeOp` for every EditOp variant: correct produces/consumes/rewrites. Renames and moves produce rewrites; addEdge consumes ports as `(nodeId, portName)`; addGroup with parentGroup scopes the produced label correctly.
- `applyOpToVisible` for every EditOp variant: result matches the equivalent server-side mutation on a representative project.
- `transitiveDependents`: a chain `addNode A -> addEdge A.out -> B.in -> renameGroup containing A` drops everything when addNode A is rejected.
- `revalidate`: pendingOp whose consumed entity is missing from new truth -> dropped with reason. PendingOp whose produced ID is already in new truth -> dropped with reason. Rewrite chains compose correctly.
- `derive`: truth with 50 nodes + 5 pendingOps containing a mix of every op kind produces the same `nodes`/`edges` shape as `patchFromProject` would after the host round-tripped each op.

Layer 1 (preflight):
- Each of the 7 rules: positive (passes), negative (rejects with the right reason), edge cases (empty inputs, single op, batch of mixed accept/reject).
- Rewrite rules (`singleDriverPerInput`, `validReconnection`) produce the right transformed batch.

Layer 3 (component-level, exercising ProjectEditorInner with a fake host):
- The three corruption cases from the research phase, as integration tests:
  - **A.** Type field, drag node, type field, connect edge, type field, all within 600ms. Verify all five effects land in the source AND in the visible state. No phantom edges, no lost typing.
  - **B.** AddEdge whose host-side application fails because of a stale ID after a previous op renamed things. Verify the optimistic edge is removed visually, a toast is shown, the rest of pendingOps survives if it doesn't depend on the failed op.
  - **C.** User types into a config field of node N. Mid-typing, another change to the .weft text tab arrives that removes node N. Verify the typing op is dropped with a toast, the visible state reflects the new truth (without N), no error in the console.
- Lock cases:
  - **D (auto-lock).** External code edit arrives. Within 1s, user attempts a logical graph edit (e.g. addEdge). Verify preflight rejects with the auto-lock reason, no source op sent, toast shown. Verify that during the lock window, layout edits (drag positions, collapse/expand) still apply normally.
  - **E (auto-lock release).** No more code edits arrive for 1s after the last one. Verify the next logical graph edit succeeds without a lock rejection.
  - **F (auto-lock reset on burst).** Code edits arrive every 500ms for 10 seconds (simulating AI streaming). Verify the lock stays engaged the entire time. Verify the lock releases 1s after the final keystroke.
  - **G (explicit lock).** Caller sends `setGraphLogicLock {locked: true, reason: 'AI editing'}`. Verify the banner renders with the reason. Verify all logical graph edits reject. Verify layout edits work. Verify the deactivate button sends `setGraphLogicLock {locked: false}` and the lock releases.
  - **H (doc.version race).** User starts a graph edit (sent to host). Before the host's parse-server replies, the user types a character in the text tab. Host detects the version change, replies `ok: false, reason: 'code-was-edited'`. Verify the webview rolls back via the standard rejection path, toast shown.
  - **I (truth-advance during lock).** Lock is engaged (auto or explicit). User types in the code tab. Verify the graph re-renders to reflect the new truth (parse arrives, truth advances, projection re-derives). Verify the lock remains engaged. Verify pending ops that were not yet sent but became invalidated by the new truth get dropped with toasts.
- Drag a node out of a group where it has in-scope connections. Verify preflight rejects, no source op sent, position snaps back, toast shown. Verify NO `preDragPositions` map or other ad-hoc state remains in the code path.
- Drag a node into a group where it would dangle connections. Same: preflight rejects, toast, no snap-back glitch.
- Connect that creates a cycle: preflight rejects, no edge added, toast.
- Connect onto a port that already has a driver: preflight rewrites the batch to include a removeEdge, visible edge is replaced cleanly, one undo step undoes both.
- Reconnect dropped on empty space: preflight rewrites to removeEdge.
- Undo a still-pending op: op is peeled from pendingOps, projection re-derives, layoutInverse runs.
- Undo a confirmed op: inverse TextEdit replays through host round-trip.
- Redo after undo: mirror semantics work.
- Burst of 50 ops where every 10th op is a rejection from the server: verify resyncs land, dependents are dropped, the final state matches the host's source plus the surviving ops.

Layer 4 (end-to-end, manual):
- Smoke test the editor with real Weft projects. Confirm the feel: drags, connects, deletes, typing all feel instant. Rejections show a clear toast and snap back cleanly. No phantom anything.

## Decisions and open questions

**Settled:**

- Truth + pendingOps + layoutCode as three inputs; visible state as a `$derived` projection.
- One full re-derivation per change to any input. No incremental projection.
- Dependency tracking uses `(produces, consumes, rewrites)` per op. Renames and moves use rewrites.
- On server rejection: resync truth from the host (one round-trip), then revalidate pendingOps and re-derive. We do not mirror server semantics locally. Server is authority.
- Preflight checks unified into one dispatcher. Rewrite rules transform the batch before the reject pass. Reject rules produce the same shape as a server rejection so the rollback path is uniform.
- The 7 ad-hoc preflight reverts and the 5 ad-hoc post-flight reverts all DISAPPEAR. Their behavior is preserved by the unified mechanism.
- `applyExternalSource`'s silent bail is removed entirely. Truth replacement always wins; pendingOps re-apply on top.
- Config typing lives in pendingOps from the first keystroke. Debounce becomes 250ms. The `pendingConfigOps` flat buffer is gone.
- Layout-only changes (drag, resize, collapse) commit to `layoutCode` directly and never roll back unless paired with a rejected source op (in which case the op's `layoutInverse` runs).
- Source-coupled layout changes (drag-to-reparent) capture a `layoutInverse` so rejection can undo both the source and the layout.
- Undo entries are polymorphic over `pending` vs `confirmed`. Same Ctrl-Z UX as today.
- The overlay effects on nodes (execution/infra/file/bus/subgraph-highlight/body-feed) get refactored into a `$derived` overlay layer. Action-bar effects (don't touch nodes) stay as effects.
- Form-builder `setConfig` does NOT re-template ports in the projection. The brief stale-port window between optimistic apply and truth landing is acceptable (matches today's port-inference window). No parallel TS templating implementation.
- streamingEdits, autoOrganize: out of scope.
- `editApplied` reply on success carries the new source + project + layoutCode so truth advances in one message.
- `resyncSource` is a new message pair, serialized through `serializeOnPath` on the host.
- EditError variants get a human-readable translation table in graphView.ts for the toast reason.
- Logic lock: two gates feeding one `isLogicLocked` state. Gate 1 is an auto-lock that engages on any external `onDidChangeTextDocument` and releases 1s after the last keystroke (each keystroke resets the timer; AI streaming at 500ms intervals keeps the lock engaged). Gate 2 is an explicit `lockGraphLogic` boolean settable via `setGraphLogicLock` message, used by future AI assistant integration and a manual user toggle. The lock gates ONE path: `recordEdit` -> source-mutating EditOp. It does NOT gate code-tab keystrokes, the truth-advance path from code edits to the graph view, layout/visual edits (drag/resize/collapse/expand/pan/zoom), pending-op revalidation, or overlay updates. The graph still reflects code changes in real time while locked; the user just can't push logical changes back the other way.
- Doc.version backstop on the host: `applyEditTransaction` captures `doc.version` at entry; if it changed by the time parse-server replies, the transaction aborts cleanly with `ok: false, reason: 'code-was-edited'` and the standard rejection path runs in the webview. This covers the sub-second race between "user typed" and "webview received `codeEditTouched`."
- This is one delivery. No phases.

**Genuinely open (implementer's call, not blockers):**

- Whether the `$derived` cost of running `buildNodes` + `applyOverlays` over a 200-node project on every keystroke (config typing) is noticeable. If yes, the typing path can avoid full re-derive by writing the typed value directly into the live derived `data.config` (xyflow node mutation) AND into the pendingOp. The projection runs only when the typing op is appended/replaced, which is once per keystroke anyway, so this is likely a non-issue. Verify in implementation; if slow, add a typing fast path.
- Whether to keep the existing 100ms parse debounce on text-tab edits or shorten it. Independent of this reshape; leave alone unless it becomes a problem.

**Known bug to fix on the way through (nested-container layout):** a `Loop` (or `Group`) nested inside another `Group` renders OUTSIDE its parent's box: the parent group-box is not drawn large enough to contain a child that is itself a container. Repro: a `MyGroup` containing a `MyLoop` (which contains a Python node) draws `MyLoop` ejected below `MyGroup` instead of inside it, even though the `.weft` scope is correct. The source is right; this is purely the TS-owned layout/projection drawing the parent's bounds from its children, and a nested container child is not folded into the parent's extent. A `Loop` is itself a `GroupNode` (`isLoopNodeType`), so "group-in-group" and "loop-in-group" are the same containment case. When the projection re-derives node bounds, make a container's bounding box include nested container children (recursively), so arbitrarily-nested groups/loops stay visually enclosed. Files: `extension-vscode/src/webview/lib/layout.ts`, `GroupNode.svelte`, `loop-layout.ts`.

## Verification

End-to-end smoke after implementation:

1. `pnpm exec tsc --noEmit` and `pnpm exec svelte-check` in `extension-vscode/` pass cleanly. No type errors, no Svelte runes warnings.
2. The Layer 1 unit test suite (projection + preflight) passes.
3. The Layer 3 integration suite (corruption cases A/B/C, preflight rejections, undo/redo, burst-with-rejections) passes.
4. Manual: open a real Weft project, perform the burst-edit pattern that reproduces the corruption today. Verify it now feels smooth and no phantoms appear.
5. Em-dash sweep on the diff: `git diff HEAD | grep -P "\xe2\x80\x94"` must be empty.
6. Grep sweep on the diff for deleted-symbol names: `pendingConfigOps`, `saveProjectTimer`, `preDragPositions`, `editInFlight`, `mergeInferredPortMetadata`, `showScopeBlockedToast`, `checkGroupCapturesNodes`, `checkNodeLeavesGroup`, the `applyExternalSource` bail comment. All must be gone.
7. `./setup.sh --vsix` builds clean and the extension installs.
8. Adversarial agent review batch via `/code-review` confirming: no special rejection paths remain; one queue; one rollback shape; the projection is purely derived; preflight and server-rejection both flow through the same rollback; the toast text is clear.
