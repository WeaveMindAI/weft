<script lang="ts">
  import {
    SvelteFlow,
    Background,
    Controls,
    type Node,
    type Edge,
  } from '@xyflow/svelte';
  import '@xyflow/svelte/dist/style.css';
  import { send } from './vscode';
  import type { ProjectDefinition } from '../shared/protocol';

  interface Props {
    project: ProjectDefinition;
  }

  let { project }: Props = $props();

  // Node layout: very simple stacked grid until ELK lands. Nodes
  // declared in order; render top-to-bottom, 200px apart. Works for
  // the phase A smoke test; ELK layout is the next task.
  let nodes = $state<Node[]>([]);
  let edges = $state<Edge[]>([]);

  $effect(() => {
    const next_nodes: Node[] = project.nodes
      .filter((n) => n.nodeType !== 'Passthrough')
      .map((n, i) => ({
        id: n.id,
        position: n.position.x !== 0 || n.position.y !== 0
          ? n.position
          : { x: 100, y: 100 + i * 120 },
        data: {
          label: n.label ?? `${n.id}: ${n.nodeType}`,
          nodeType: n.nodeType,
          inputs: n.inputs,
          outputs: n.outputs,
        },
        type: 'default',
      }));
    const next_edges: Edge[] = project.edges.map((e) => ({
      id: e.id,
      source: e.source,
      target: e.target,
      sourceHandle: e.sourceHandle ?? undefined,
      targetHandle: e.targetHandle ?? undefined,
      label: `${e.sourceHandle ?? ''} → ${e.targetHandle ?? ''}`,
    }));
    nodes = next_nodes;
    edges = next_edges;
  });

  function onNodeDragStop() {
    // Collect positions, ship to host so it can persist them to
    // .weft.layout.json. The host side is scaffolded but not yet
    // wired; this is a no-op until the layout-persistence task.
    const positions: Record<string, { x: number; y: number }> = {};
    for (const n of nodes) {
      positions[n.id] = { x: n.position.x, y: n.position.y };
    }
    send({ kind: 'positionsChanged', positions });
  }
</script>

<div class="graph">
  <SvelteFlow
    nodes={nodes as any}
    edges={edges as any}
    fitView
    on:nodedragstop={onNodeDragStop}
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
