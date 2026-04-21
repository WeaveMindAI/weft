<script lang="ts">
  import {
    SvelteFlow,
    Background,
    Controls,
    type Node,
    type Edge,
  } from '@xyflow/svelte';
  import '@xyflow/svelte/dist/style.css';
  import { send, onMessage } from './vscode';
  import { autoLayout } from './layout';
  import { onMount } from 'svelte';
  import type { ProjectDefinition } from '../shared/protocol';

  interface Props {
    project: ProjectDefinition;
  }

  let { project }: Props = $props();

  let nodes = $state<Node[]>([]);
  let edges = $state<Edge[]>([]);
  let savedPositions = $state<Record<string, { x: number; y: number }>>({});
  let layoutDebounceMs = $state(400);
  let layoutTimer: ReturnType<typeof setTimeout> | undefined;
  let lastSignature = '';

  onMount(() => {
    const unsub = onMessage((msg) => {
      if (msg.kind === 'layoutHint') {
        savedPositions = msg.positions;
      } else if (msg.kind === 'settings') {
        layoutDebounceMs = msg.layoutDebounceMs;
      }
    });
    return unsub;
  });

  $effect(() => {
    const structuralNodes = project.nodes.filter((n) => n.nodeType !== 'Passthrough');
    const signature = structuralSignature(project);

    const nextNodes: Node[] = structuralNodes.map((n, i) => ({
      id: n.id,
      position: resolvePosition(n.id, i, structuralNodes.length),
      data: {
        label: n.label ?? `${n.id}: ${n.nodeType}`,
        nodeType: n.nodeType,
        inputs: n.inputs,
        outputs: n.outputs,
      },
      type: 'default',
    }));
    const nextEdges: Edge[] = project.edges.map((e) => ({
      id: e.id,
      source: e.source,
      target: e.target,
      sourceHandle: e.sourceHandle ?? undefined,
      targetHandle: e.targetHandle ?? undefined,
      label: `${e.sourceHandle ?? ''} → ${e.targetHandle ?? ''}`,
    }));
    nodes = nextNodes;
    edges = nextEdges;

    if (signature !== lastSignature) {
      lastSignature = signature;
      scheduleLayout();
    }
  });

  function resolvePosition(id: string, i: number, n: number): { x: number; y: number } {
    if (savedPositions[id]) return savedPositions[id];
    return { x: 100, y: 100 + i * 120 };
  }

  function structuralSignature(p: ProjectDefinition): string {
    const ns = p.nodes
      .map((n) => `${n.id}:${n.nodeType}`)
      .sort()
      .join(',');
    const es = p.edges
      .map((e) => `${e.source}.${e.sourceHandle}->${e.target}.${e.targetHandle}`)
      .sort()
      .join(',');
    return `${ns}|${es}`;
  }

  function scheduleLayout() {
    if (layoutTimer) clearTimeout(layoutTimer);
    layoutTimer = setTimeout(runLayout, layoutDebounceMs);
  }

  async function runLayout() {
    try {
      const positions = await autoLayout({ nodes, edges });
      // Respect user-dragged (saved) positions; ELK only fills gaps.
      const merged = { ...positions, ...savedPositions };
      nodes = nodes.map((n) => ({ ...n, position: merged[n.id] ?? n.position }));
    } catch (err) {
      send({ kind: 'log', level: 'warn', message: `elk layout failed: ${String(err)}` });
    }
  }

  function onNodeDragStop() {
    const positions: Record<string, { x: number; y: number }> = {};
    for (const n of nodes) positions[n.id] = { x: n.position.x, y: n.position.y };
    savedPositions = { ...savedPositions, ...positions };
    send({ kind: 'positionsChanged', positions });
  }
</script>

<div class="graph">
  <SvelteFlow
    nodes={nodes as any}
    edges={edges as any}
    fitView
    onnodedragstop={onNodeDragStop}
  >
    <Background />
    <Controls />
  </SvelteFlow>
</div>

<style>
  .graph {
    width: 100%;
    height: 100%;
  }
  :global(.svelte-flow) {
    background: var(--vscode-editor-background);
  }
</style>
