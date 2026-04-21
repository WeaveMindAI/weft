# Execution Tracking Parity

**v1 source**: `ProjectEditorInner.svelte:1018-1111`.

## State shape

`executionState` prop on ProjectEditorInner:

```ts
{
  isRunning: boolean;
  activeEdges: Set<string>;
  nodeOutputs: Record<string, unknown>;     // nodeId -> last-seen output, for debugPreview
  nodeStatuses: Record<string, string>;     // (not primarily used for rendering)
  nodeExecutions: NodeExecutionTable;       // nodeId -> NodeExecution[]
}
```

`NodeExecution` (from `$lib/types`):

```ts
{
  id: string;
  nodeId: string;
  status: 'pending' | 'running' | 'waiting_for_input' | 'accumulating' | 'completed' | 'skipped' | 'failed' | 'cancelled';
  pulseIdsAbsorbed: string[];
  pulseId: string;
  error?: string;
  startedAt: number;      // ms epoch
  completedAt?: number;   // ms epoch
  input?: unknown;        // per-node input snapshot
  output?: unknown;       // per-node output snapshot
  costUsd: number;
  logs: LogLine[];
  color: string;          // execution color (session uuid)
  lane: LaneFrame[];
}
```

Every node has a zero-or-more history; the latest determines the
render status.

## Regular node execution wiring (lines 1079, 1082-1098)

```ts
executions = nodeExecutions[n.id] || [];
const latestExec = executions[executions.length - 1];
const execStatus = latestExec?.status;
const nodeClass = execStatus === 'running' || execStatus === 'waiting_for_input' ? 'node-running'
  : execStatus === 'failed' ? 'node-failed'
  : execStatus === 'completed' || execStatus === 'skipped' ? 'node-completed'
  : '';
return {
  ...n,
  data: { ...n.data, debugData, executions, executionCount: executions.length },
  class: nodeClass,
};
```

- `debugData` comes from `nodeOutputs[n.id]` only when
  `features.showDebugPreview` is true on the node type (Debug node).
- `executions` array is passed to `ProjectNode` + `GroupNode` so
  the ExecutionInspector can paginate.
- `nodeClass` is set on the xyflow node wrapper. The :global CSS
  rules in ProjectNode/GroupNode read this class to apply the
  glow:
  - `.node-running` → amber 2px box-shadow ring (+ pulse animation)
  - `.node-completed` → green 2px ring
  - `.node-failed` → red 2px ring

## Group execution wiring (lines 1031-1077)

**Key insight**: in the v2 compiler, a Group's runtime presence
is the two boundary Passthrough nodes (`{id}__in`, `{id}__out`)
plus all internal children. v1's parser does the same expansion
during `expandGroupsForValidation`, producing executions keyed by
`{groupId}__in` and `{groupId}__out`. The dashboard synthesizes
a Group-level NodeExecution by combining all of them:

```ts
if (nodeType === 'Group') {
  const groupId = n.id;

  // Boundary passthrough executions (compiled IDs)
  const inExecs = nodeExecutions[`${groupId}__in`] || [];
  const outExecs = nodeExecutions[`${groupId}__out`] || [];

  // Collect internal node executions via scope field
  const internalExecs = [];
  for (const projNode of project.nodes) {
    if (projNode.scope?.includes(groupId) && nodeExecutions[projNode.id]) {
      internalExecs.push(...nodeExecutions[projNode.id]);
    }
  }

  // Build synthetic execution: one per __in execution
  executions = inExecs.map((inExec, i) => {
    const outExec = outExecs[i];
    const allRelated = [...internalExecs, ...inExecs, ...outExecs];
    const hasRunning = allRelated.some(e => e.status === 'running' || e.status === 'waiting_for_input');
    const hasFailed = allRelated.some(e => e.status === 'failed');
    const allTerminal = allRelated.length > 0 && allRelated.every(e =>
      ['completed', 'skipped', 'failed', 'cancelled'].includes(e.status)
    );
    const status = hasRunning ? 'running'
      : hasFailed ? 'failed'
      : allTerminal ? 'completed'
      : inExec.status;

    return {
      id: `${groupId}-synth-${i}`,
      nodeId: groupId,
      status,
      pulseIdsAbsorbed: inExec.pulseIdsAbsorbed,
      pulseId: inExec.pulseId,
      error: outExec?.error ?? inExec.error,
      startedAt: inExec.startedAt,
      completedAt: outExec?.completedAt ?? inExec.completedAt,
      input: inExec.output,     // __in's output = what flowed into the group
      output: outExec?.output,  // __out's output = what the group produced
      costUsd: allRelated.reduce((sum, e) => sum + (e.costUsd || 0), 0),
      logs: [],
      color: inExec.color,
      lane: inExec.lane,
    };
  });
}
```

### Semantics

- The group's **input snapshot** is `__in.output`. The `__in`
  passthrough passes through what the group received; its output
  is therefore "the values wired into the group's interface
  in-ports".
- The group's **output snapshot** is `__out.output`. Same
  reasoning.
- The group's **status** aggregates: running if any related node
  running; failed if any failed; completed only when all are
  terminal; otherwise mirror `__in`'s status (which for a group
  that hasn't started firing internally would be pending /
  waiting).
- The group's **cost** is the sum over all related executions.
  Internal nodes + boundaries contribute.
- Groups can execute multiple times per session (loops / fanout);
  v1 pairs them by index `inExecs[i]` with `outExecs[i]`.

### What v2 already has

- The compiler flattens exactly this way. `__in` / `__out`
  Passthrough NodeDefinitions with `groupBoundary: {groupId, role}`.
- NodeExecEvents are already keyed by node id (including
  `{groupId}__in`).
- Children carry `scope: [...chain]`.

### What v2 needs to add

The Graph.svelte layer synthesizes the group's NodeExecution
array at render time, same way v1 does. The group's virtual
NodeInstance gets `executions: [synth1, synth2, ...]` into its
`data.exec` slot.

When an exec event arrives for `{groupId}__in`, we re-derive the
synthesized Group execution and push a new `execEvent` to the
virtual Group node's state.

**Suggested implementation in Graph.svelte**:
1. Maintain `execByNode: Map<nodeId, NodeExec[]>`, not just a
   single-status entry.
2. On each `execEvent`, append to the per-node history.
3. Derive virtual Group executions reactively from `execByNode`
   and `project.groups`.
4. Pass `data.executions` into GroupNode / ProjectNode props.

The ExecutionInspector modal already handles `executions: []`, so
once the group virtual exec arrays are populated, the paginated
‹ N/M › browser works for groups out of the box.

## Active edges (lines 1101-1108)

```ts
edges = edges.map(e => ({
  ...e,
  animated: activeEdges.has(e.id),
  style: activeEdges.has(e.id)
    ? e.style?.replace(/stroke-width: \d+px/, 'stroke-width: 3px')
    : e.style?.replace(/stroke-width: \d+px/, 'stroke-width: 2px'),
  class: activeEdges.has(e.id) ? 'edge-active' : '',
}));
```

An edge is "active" when a pulse is currently travelling through
it. The dispatcher reports active edges via a separate SSE event;
the backend tracks which edges are carrying pulses. Visual: edge
animates (dashes flow) + thicker stroke + `edge-active` CSS class.

The `.edge-active .svelte-flow__edge-path` rule sets a stronger
stroke color (defined in ProjectEditorInner's CSS).

## v2 port plan

### Protocol

Dispatcher already emits `NodeStarted / NodeCompleted / NodeFailed
/ NodeSkipped` events. Add:

- **Active edge events**: `EdgeActive { color, edgeId }`,
  `EdgeInactive { color, edgeId }`, pushed when a pulse is in
  transit. Dispatcher needs to track this.

OR a simpler approximation: every `NodeStarted(X)` marks all
incoming edges of X as "active" for a short window (200ms?), then
they clear. v1 has an exact activeEdges Set driven by the
dispatcher; v2 should match long-term but short-term the window
trick is fine if latency is tight.

### Graph.svelte changes

- `execByNode: Record<nodeId, NodeExec[]>` (array, not single).
- Append on each event. Clear on `execReset`.
- Derived `groupExecutions: Record<groupId, NodeExec[]>` using
  the v1 synthesis logic above.
- Virtual Group NodeInstance gets `executions: groupExecutions[id]`.
- Regular nodes get `executions: execByNode[id]`.
- Each node's `class` is set to node-running / node-completed /
  node-failed per latest exec.
- Edges get `class: 'edge-active'` + animated when activeEdges
  reports them.

### ExecutionInspector changes

My current ExecutionInspector takes a single `{status, input, output, error}`
prop set. For parity I need it to take `executions: NodeExec[]`
and render a paginator. v1's modal is already in
`execution-inspector.md` (not written yet; this doc supersedes that).

### ProjectNode / GroupNode changes

- Read `data.executions: NodeExec[]` instead of `data.exec`.
- Display inline `‹ N/M ›` pager in header when length > 1.
- Pass through to `<ExecutionInspector>`.

## Divergences

None intended. v2 should match v1's model exactly. The underlying
data (`nodeExecutions` keyed by id including passthrough ids) is
already present in v2.
