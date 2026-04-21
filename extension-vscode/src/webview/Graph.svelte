<script lang="ts">
  import {
    SvelteFlow,
    Background,
    Controls,
    type Node,
    type Edge,
  } from '@xyflow/svelte';
  import '@xyflow/svelte/dist/style.css';
  import { onMount } from 'svelte';
  import { send, onMessage } from './vscode';
  import { autoLayout } from './layout';
  import type {
    CatalogEntry,
    NodeExecEvent,
    ProjectDefinition,
  } from '../shared/protocol';
  import ProjectNode from './components/ProjectNode.svelte';
  import CustomEdge from './components/CustomEdge.svelte';
  import GroupNode from './components/GroupNode.svelte';
  import CommandPalette from './components/CommandPalette.svelte';
  import type { NodeExecStatus } from './components/ExecutionInspector.svelte';

  interface Props {
    project: ProjectDefinition;
    catalog: Record<string, CatalogEntry>;
  }

  let { project, catalog }: Props = $props();

  let nodes = $state<Node[]>([]);
  let edges = $state<Edge[]>([]);
  let savedPositions = $state<Record<string, { x: number; y: number }>>({});
  let layoutDebounceMs = $state(400);
  let layoutTimer: ReturnType<typeof setTimeout> | undefined;
  let lastSignature = '';
  let paletteOpen = $state(false);

  // Per-node execution state. Keyed by node id. Updated by
  // execEvent messages from the host.
  interface NodeExecState {
    status: NodeExecStatus;
    input?: unknown;
    output?: unknown;
    error?: string;
  }
  let execByNode = $state<Record<string, NodeExecState>>({});

  const nodeTypes = { weft: ProjectNode, weftGroup: GroupNode };
  const edgeTypes = { weft: CustomEdge };

  onMount(() => {
    const unsub = onMessage((msg) => {
      if (msg.kind === 'layoutHint') {
        savedPositions = msg.positions;
      } else if (msg.kind === 'settings') {
        layoutDebounceMs = msg.settings?.layoutDebounceMs ?? layoutDebounceMs;
      } else if (msg.kind === 'execEvent') {
        applyExecEvent(msg.event);
      } else if (msg.kind === 'execReset') {
        execByNode = {};
      }
    });
    window.addEventListener('keydown', onHotkey);
    return () => {
      unsub();
      window.removeEventListener('keydown', onHotkey);
    };
  });

  function onHotkey(e: KeyboardEvent) {
    if ((e.metaKey || e.ctrlKey) && e.key === 'k') {
      paletteOpen = true;
      e.preventDefault();
    }
  }

  function applyExecEvent(event: NodeExecEvent) {
    const current = execByNode[event.node_id] ?? { status: 'idle' as NodeExecStatus };
    const next: NodeExecState = { ...current };
    if (event.kind === 'started') {
      next.status = 'started';
      next.input = event.input;
      next.output = undefined;
      next.error = undefined;
    } else if (event.kind === 'completed') {
      next.status = 'completed';
      next.output = event.output;
    } else if (event.kind === 'failed') {
      next.status = 'failed';
      next.error = event.error;
    } else if (event.kind === 'skipped') {
      next.status = 'skipped';
    }
    execByNode = { ...execByNode, [event.node_id]: next };
  }

  /** Per-target wiring: set of targetHandle names for each target node. */
  const wiredByTarget = $derived.by(() => {
    const m: Record<string, Set<string>> = {};
    for (const e of project.edges) {
      if (!e.targetHandle) continue;
      (m[e.target] ??= new Set()).add(e.targetHandle);
    }
    return m;
  });

  $effect(() => {
    const structural = project.nodes.filter((n) => n.nodeType !== 'Passthrough');
    const signature = structuralSignature(project);

    const nextNodes: Node[] = structural.map((n, i) => ({
      id: n.id,
      position: resolvePosition(n.id, i),
      type: 'weft',
      data: {
        node: n,
        catalog: catalog[n.nodeType] ?? null,
        wiredInputs: wiredByTarget[n.id] ?? new Set<string>(),
        exec: execByNode[n.id] ?? { status: 'idle' as NodeExecStatus },
        onConfigChange: (nodeId: string, key: string, value: unknown) =>
          send({
            kind: 'mutation',
            mutation: { kind: 'updateConfig', nodeId, key, value },
          }),
      },
    }));

    const nextEdges: Edge[] = project.edges.map((e) => ({
      id: e.id,
      source: e.source,
      target: e.target,
      sourceHandle: e.sourceHandle ?? undefined,
      targetHandle: e.targetHandle ?? undefined,
      type: 'weft',
    }));

    nodes = nextNodes;
    edges = nextEdges;

    if (signature !== lastSignature) {
      lastSignature = signature;
      scheduleLayout();
    }
  });

  function resolvePosition(id: string, i: number): { x: number; y: number } {
    if (savedPositions[id]) return savedPositions[id];
    return { x: 100 + (i % 4) * 260, y: 100 + Math.floor(i / 4) * 200 };
  }

  function structuralSignature(p: ProjectDefinition): string {
    const ns = p.nodes.map((n) => `${n.id}:${n.nodeType}`).sort().join(',');
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

  function onConnect(e: { source: string; sourceHandle?: string | null; target: string; targetHandle?: string | null }) {
    if (!e.sourceHandle || !e.targetHandle) return;
    send({
      kind: 'mutation',
      mutation: {
        kind: 'addEdge',
        source: e.source,
        sourcePort: e.sourceHandle,
        target: e.target,
        targetPort: e.targetHandle,
      },
    });
  }

  function onPickNode(nodeType: string) {
    paletteOpen = false;
    const id = `n_${Date.now().toString(36)}`;
    send({ kind: 'mutation', mutation: { kind: 'addNode', id, nodeType } });
  }
</script>

<div class="w-full h-full relative">
  <SvelteFlow
    nodes={nodes as any}
    edges={edges as any}
    nodeTypes={nodeTypes as any}
    edgeTypes={edgeTypes as any}
    fitView
    onnodedragstop={onNodeDragStop}
    onconnect={onConnect}
  >
    <Background />
    <Controls />
  </SvelteFlow>

  <CommandPalette
    open={paletteOpen}
    catalog={catalog}
    onPick={onPickNode}
    onClose={() => (paletteOpen = false)}
  />
</div>

<style>
  div {
    width: 100%;
    height: 100%;
  }
</style>
