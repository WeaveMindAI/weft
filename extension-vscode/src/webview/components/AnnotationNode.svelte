<script lang="ts">
  // Ported from dashboard-v1/src/lib/components/project/AnnotationNode.svelte.
  // Floating markdown note behind the graph. Two modes:
  //   • display: rendered markdown. Empty → "Double-click to add notes..."
  //   • editing: plain <textarea>. Double-click enters, Escape/blur exits.
  //
  // z-index is -1 (set in build-nodes.ts).

  import { NodeResizer } from '@xyflow/svelte';
  import { marked } from 'marked';
  import { cn } from '../utils/cn';
  import type { NodeViewData } from './node-view-data';

  interface Props {
    data: NodeViewData;
    id: string;
    selected?: boolean;
  }

  let { data, selected }: Props = $props();

  const node = $derived(data.node);
  const config = $derived((node.config ?? {}) as Record<string, unknown>);
  const content = $derived((config.content as string | undefined) ?? '');

  let editing = $state(false);
  let editContent = $state('');
  let isResizing = $state(false);

  // Cached markdown renderer — opens links in new tabs (no-opener).
  marked.setOptions({ breaks: true, gfm: true });
  const rendered = $derived.by(() => {
    const src = (editing ? editContent : content).trim();
    if (!src) return '<span class="placeholder">Double-click to add notes...</span>';
    const html = marked.parse(src, { async: false }) as string;
    return html.replace(/<a\s/g, '<a target="_blank" rel="noopener noreferrer" ');
  });

  function startEdit(e: MouseEvent) {
    e.stopPropagation();
    if (editing) return;
    editContent = content;
    editing = true;
  }

  function saveContent() {
    if (isResizing) return;
    editing = false;
    if (editContent !== content) {
      data.onConfigChange(node.id, 'content', editContent);
    }
  }

  function onContainerKey(e: KeyboardEvent) {
    if (!editing) return;
    if (e.key === 'Escape') {
      editing = false;
      editContent = content;
    }
  }

  function onTextareaKey(e: KeyboardEvent) {
    e.stopImmediatePropagation();
    e.stopPropagation();
    if (e.key === 'Escape') {
      editing = false;
      editContent = content;
    }
  }

  function handleResizeEnd(_event: unknown, params: { width: number; height: number }) {
    isResizing = false;
    data.onConfigChange(node.id, 'width', params.width);
    data.onConfigChange(node.id, 'height', params.height);
  }
  function handleResizeStart() {
    isResizing = true;
  }
</script>

<NodeResizer
  minWidth={180}
  minHeight={80}
  isVisible={selected}
  lineStyle="border-color: #94a3b8; border-width: 1px;"
  handleStyle="background-color: #94a3b8; width: 8px; height: 8px; border-radius: 2px;"
  onResizeStart={handleResizeStart}
  onResizeEnd={handleResizeEnd}
/>

<!-- svelte-ignore a11y_no_static_element_interactions -->
<!-- svelte-ignore a11y_click_events_have_key_events -->
<div
  class={cn('annotation-node', selected && 'selected', editing && 'editing')}
  ondblclick={startEdit}
  onkeydown={onContainerKey}
  role="textbox"
  tabindex={0}
>
  {#if editing}
    <!-- svelte-ignore a11y_autofocus -->
    <textarea
      class="edit-textarea"
      bind:value={editContent}
      onkeydown={onTextareaKey}
      onblur={saveContent}
      autofocus
    ></textarea>
  {:else}
    <div class="rendered">{@html rendered}</div>
  {/if}
</div>

<style>
  .annotation-node {
    width: 100%;
    height: 100%;
    padding: 12px;
    background: rgba(255, 255, 255, 0.8);
    border: 1px solid #e4e4e7;
    border-radius: 8px;
    overflow: auto;
    box-sizing: border-box;
  }
  .annotation-node.selected {
    border-color: #4f46e5;
    box-shadow: 0 0 0 2px rgba(79, 70, 229, 0.15);
  }
  .annotation-node.editing {
    padding: 0;
  }
  .edit-textarea {
    width: 100%;
    height: 100%;
    padding: 12px;
    border: none;
    outline: none;
    resize: none;
    background: white;
    font: 12px ui-sans-serif, system-ui, sans-serif;
    color: #18181b;
  }
  .rendered :global(h1) {
    font-size: 18px;
    font-weight: 600;
    color: #111827;
    margin: 0 0 6px;
  }
  .rendered :global(h2) {
    font-size: 15px;
    font-weight: 600;
    color: #1f2937;
    margin: 0 0 6px;
  }
  .rendered :global(h3) {
    font-size: 13px;
    font-weight: 600;
    color: #374151;
    margin: 0 0 6px;
  }
  .rendered :global(p) {
    margin: 0 0 6px;
  }
  .rendered :global(p:last-child) {
    margin: 0;
  }
  .rendered :global(ul),
  .rendered :global(ol) {
    padding-left: 18px;
  }
  .rendered :global(ul) {
    list-style: disc;
  }
  .rendered :global(ol) {
    list-style: decimal;
  }
  .rendered :global(code) {
    background: #f1f5f9;
    padding: 1px 4px;
    border-radius: 3px;
    font: 12px ui-monospace, 'SF Mono', Monaco, monospace;
  }
  .rendered :global(pre) {
    background: #f1f5f9;
    padding: 8px 10px;
    border-radius: 6px;
    overflow-x: auto;
    font: 12px ui-monospace, 'SF Mono', Monaco, monospace;
  }
  .rendered :global(a) {
    color: #3b82f6;
    text-decoration: underline;
  }
  .rendered :global(a:hover) {
    color: #2563eb;
  }
  .rendered :global(blockquote) {
    border-left: 2px solid #d4d4d8;
    padding-left: 8px;
    margin: 0 0 6px;
    color: #52525b;
  }
  .rendered :global(.placeholder) {
    color: #9ca3af;
    font-style: italic;
  }
</style>
