# Annotation Node Parity

**v1 source**: `dashboard-v1/src/lib/components/project/AnnotationNode.svelte`
(286 lines). Text note floating behind the graph. No ports, no
data wiring, just markdown.

## Data prop

```ts
data: {
  label: string | null;
  nodeType: 'Annotation';
  config: { content?: string; width?: number; height?: number };
  onUpdate?: (updates: NodeDataUpdates) => void;
}
```

## States

Two modes:

1. **Display**: renders `marked(content)` as HTML. Empty content →
   placeholder `"Double-click to add notes..."` in italic gray.
2. **Editing**: plain `<textarea>` bound to `editContent`.
   Triggered by double-click (line 43-56). Blur saves.

Classes on outer `<div>`: `annotation-node`, `selected?`, `editing?`.

## Dimensions

- `minWidth={180}`, `minHeight={80}`.
- Width/height persisted in `config.width`, `config.height`.
- NodeResizer shown when selected. Handles: `background-color:
  #94a3b8; width: 8px; height: 8px; border-radius: 2px`.
- `isResizing` flag blocks `saveContent` while the user is
  resize-dragging (line 58-66).

## Markdown styling

Custom renderer makes links open in new tab with `rel="noopener
noreferrer"`. `marked.setOptions({ breaks: true, gfm: true })`.

Per-element CSS (lines 161-265):
- h1: 18px 600 #111827
- h2: 15px 600 #1f2937
- h3: 13px 600 #374151
- p: margin 0 0 6px (last-child 0)
- ul/ol: padding-left 18px, disc/decimal
- code: bg #f1f5f9, pad 1px 4px, font 12px mono
- pre: bg #f1f5f9, pad 8px 10px, font 12px, overflow-x auto
- a: #3b82f6 underline, hover #2563eb
- blockquote: 2px left border #d4d4d8
- .placeholder: #9ca3af italic

## Keyboard handling

- Container `keydown`: Escape exits editing (line 68-73).
- Textarea `keydown`: stopImmediatePropagation + stopPropagation
  so Ctrl+A, Ctrl+Z etc. hit the textarea natively (line 75-85).
  Only Escape exits editing.

## z-index

Set in ProjectEditorInner `buildNodes` line 903:
`isAnnotation ? -1 : ...`. Annotations render behind everything
else by default.

## v2 port plan

### Needed

- Port component with markdown renderer (`marked` lib, ~45KB gz).
- NodeResizer wired.
- Double-click to edit, Escape/blur to save.
- `AnnotationNode` registered in xyflow nodeTypes as
  `type: 'annotation'`.

### Creating annotations

- Add `addAnnotation` command palette entry.
- `addNode('Annotation')` in Graph.svelte.
- Surgical `addNode` needs an Annotation branch that writes
  `annot_N = Annotation { content: "...", width: 250, height: 120 }`.

### Deferred

None. This is small enough to port straight.
