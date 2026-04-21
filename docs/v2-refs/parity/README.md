# V1 → V2 Frontend Parity Spec

**Purpose.** Capture every feature, interaction, state machine, and
visual rule from the v1 dashboard that must be preserved in the v2
VS Code extension webview. Written by reading the v1 source file by
file, not by summarizing via subagents. Source lines are cited with
exact paths and line numbers so the mapping is auditable.

Each sub-doc covers one component or one concern:

- `parser.md` - v1 weft-parser.ts: ParsedGroup / NodeInstance /
  `__inner` handle routing / ELK per-scope / expandGroupsForValidation.
- `project-editor.md` - ProjectEditorInner: orchestrator,
  bidirectional weft ↔ graph sync, viewport, selection, keyboard,
  undo/redo, visibility walk, drag scope-lock.
- `project-node.md` - ProjectNode.svelte: all visual states,
  port marker application, field rendering, expand/collapse,
  label editing, add-port UI, execution inspector integration.
- `group-node.md` - GroupNode.svelte: expanded frame + collapsed
  pill, dual-handle scheme with `__inner`, scope interactions,
  size computation, resize, label sanitization.
- `edges.md` - CustomEdge, edge color from port type, _inner routing.
- `annotation-node.md` - markdown renderer, double-click edit, resize.
- `execution.md` - Execution state model, NodeExecution record,
  running/completed/failed/skipped indicators, live data,
  activeEdges, debug preview, ExecutionInspector modal.
- `field-editor.md` - field-editor.svelte.ts debounce, all
  FieldDefinition kinds (text, textarea, code, select, multiselect,
  checkbox, password, api_key, blob, form_builder), textarea
  resize persistence.
- `port-system.md` - port-marker (state machine + triangle SVGs),
  port-context-menu (right-click), _raw handle semantics, one-of-
  required groups, config-filled ports, canAddInputPorts /
  canAddOutputPorts, add-port UI.
- `command-palette.md` - cmd+P behavior, ranking, preview panel.
- `layout.md` - autoOrganize ELK per-scope layout bottom-up,
  port Y positions matching CSS, disconnected components, layout
  code persistence, viewport anchoring on expand/collapse.
- `scope-lock.md` - drag semantics: checkNodeLeavesGroup,
  checkNodeCapturedByGroup, checkGroupCapturesNodes, revert-on-
  scope-change rule, toast, preDragPositions map.
- `serializer.md` - weft-editor.ts: surgical code edits the editor
  produces when a mutation fires (addNode, addEdge, renameGroup,
  updatePorts, moveNodeScope, etc).
- `types.md` - PortDefinition, NodeInstance, NodeFeatures,
  FormFieldDef, FileRef, LaneMode, WeftType, port type colors.
- `colors.md` - PORT_TYPE_COLORS mapping, FALLBACK, getPortTypeColor
  WeftType resolution.

**Process.** I read v1 in chunks, capturing facts into the relevant
sub-doc. When a doc grows big enough to spec a component, I write
the v2 port in one shot and tick the todo. No delegation. No
summaries-only. Direct quotes with file:line.

## What v1 emits from parser

**v1's parser does NOT flatten groups into passthroughs at the
public API.** It keeps `nodeType: 'Group'` NodeInstances with
`inputs`/`outputs` for the interface ports. See
`weft-parser.ts:4363-4398`. The flat passthrough shape only exists
transiently in `expandGroupsForValidation`
(`weft-parser.ts:3285-3403`) for uniform type-checking and is
thrown away.

**Edge handles use a `__inner` suffix** to distinguish the internal
side of a group boundary port from the external side. See
`weft-parser.ts:4543-4554`.

```
// v1 rule:
// source/target is the group's id in both cases.
// source handle / target handle gets '__inner' appended when it's
// an internal connection (self.x).
sourceHandle = conn.sourceIsSelf ? `${conn.sourcePort}__inner` : conn.sourcePort;
targetHandle = conn.targetIsSelf ? `${conn.targetPort}__inner` : conn.targetPort;
```

**`_raw` is an implicit output port** on all non-Group nodes, added
at runtime by the executor. The parser recognizes it as a valid
source when it sees `node._raw = ...`. See `weft-parser.ts:4502-4503`.

## What v2 compiler emits

Different shape, same math. Our v2 Rust compiler `flatten_group`
(weft-compiler.rs) performs the exact passthrough expansion v1
does in `expandGroupsForValidation`, but ships it as the runtime
model. To render groups as v1 did, the v2 webview needs to either:

(a) read the structured group list (new `project.groups` field,
    added in the last commit) and treat Group as a synthesized
    virtual node with `__inner` handles on boundaries; hide the
    `__in`/`__out` Passthrough nodes from the rendered node array
    and rewrite their edges to the group's id. This is closest to
    v1's model.

(b) reverse the flatten pass entirely in the webview, reconstruct
    ParsedGroup-equivalent structure, ignore `project.groups`. More
    work.

(a) is the right path. The parity spec assumes (a).

## How this doc is maintained

- When I finish reading a v1 component, I commit the corresponding
  spec file.
- When I port a component to v2, I add a "v2 port status" section
  to the spec file summarizing what shipped and any deferred parts
  (with reasons).
- If v2 ends up diverging from v1 on a design call, I note it here
  in a `### Divergences` section with rationale.
