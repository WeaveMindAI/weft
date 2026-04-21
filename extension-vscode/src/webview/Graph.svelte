<script lang="ts">
  import {
    SvelteFlow,
    Background,
    Controls,
    type Node,
    type Edge as FlowEdge,
  } from '@xyflow/svelte';
  import '@xyflow/svelte/dist/style.css';
  import { onMount } from 'svelte';
  import { send, onMessage } from './vscode';
  import { autoLayout } from './layout';
  import type {
    CatalogEntry,
    NodeExecEvent,
    ProjectDefinition,
    NodeDefinition,
  } from '../shared/protocol';
  import ProjectNode from './components/ProjectNode.svelte';
  import CustomEdge from './components/CustomEdge.svelte';
  import GroupNode from './components/GroupNode.svelte';
  import CommandPalette from './components/CommandPalette.svelte';
  import type { NodeExecStatus } from './components/exec-types';
  import type { NodeViewData } from './components/node-view-data';

  interface Props {
    project: ProjectDefinition;
    catalog: Record<string, CatalogEntry>;
  }

  let { project, catalog }: Props = $props();

  // nodes / edges: the xyflow source-of-truth. Mutated in-place so
  // xyflow doesn't remount a node (which would reset position,
  // focused input, etc).
  let nodes = $state<Node[]>([]);
  let edges = $state<FlowEdge[]>([]);

  // Layout cache. Drives initial position for new nodes and survives
  // across parse rounds. Written to the .layout.json sidecar when the
  // user drags.
  let savedPositions = $state<Record<string, { x: number; y: number }>>({});
  let layoutDebounceMs = $state(400);
  let layoutTimer: ReturnType<typeof setTimeout> | undefined;
  let structuralSignature = '';

  let paletteOpen = $state(false);

  // Per-node execution state. Updated by `execEvent` messages.
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
        savedPositions = { ...savedPositions, ...msg.positions };
        // Apply incoming positions to existing nodes we haven't
        // placed from layout yet.
        for (const n of nodes) {
          const saved = savedPositions[n.id];
          if (saved) n.position = saved;
        }
      } else if (msg.kind === 'settings') {
        layoutDebounceMs = (msg as any).layoutDebounceMs ?? layoutDebounceMs;
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
    const target = e.target as HTMLElement | null;
    const inInput =
      target && (target.tagName === 'INPUT' || target.tagName === 'TEXTAREA' || target.isContentEditable);

    // Ctrl+P always works even inside inputs: it's the primary way
    // to open the palette.
    if ((e.metaKey || e.ctrlKey) && e.key.toLowerCase() === 'p') {
      paletteOpen = !paletteOpen;
      e.preventDefault();
      return;
    }

    if (inInput) return;

    // Ctrl+D: duplicate selected.
    if ((e.metaKey || e.ctrlKey) && e.key.toLowerCase() === 'd') {
      const sel = nodes.find((n) => n.selected);
      if (sel) {
        e.preventDefault();
        send({ kind: 'mutation', mutation: { kind: 'duplicateNode', nodeId: sel.id } });
      }
      return;
    }

    // Delete selected nodes and edges.
    if (e.key === 'Delete' || e.key === 'Backspace') {
      let touched = false;
      for (const n of nodes) {
        if (n.selected) {
          send({ kind: 'mutation', mutation: { kind: 'removeNode', id: n.id } });
          touched = true;
        }
      }
      for (const edge of edges) {
        if (edge.selected) {
          send({ kind: 'mutation', mutation: { kind: 'removeEdge', edgeId: edge.id } });
          touched = true;
        }
      }
      if (touched) e.preventDefault();
      return;
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

  // Wired input set per target node. Recomputed when edges change.
  const wiredByTarget = $derived.by(() => {
    const m: Record<string, Set<string>> = {};
    for (const e of project.edges) {
      if (!e.targetHandle) continue;
      (m[e.target] ??= new Set()).add(e.targetHandle);
    }
    return m;
  });

  function onConfigChange(nodeId: string, key: string, value: unknown) {
    send({ kind: 'mutation', mutation: { kind: 'updateConfig', nodeId, key, value } });
  }
  function onLabelChange(nodeId: string, label: string | null) {
    send({ kind: 'mutation', mutation: { kind: 'updateLabel', nodeId, label } });
  }
  function onPortsChange(
    nodeId: string,
    changes: { inputs?: unknown; outputs?: unknown },
  ) {
    // Ports changes are stored as a _ports config key, which the
    // Weft compiler reads when rendering the node header. Until the
    // compiler round-trips ports in the config sugar, we piggyback
    // on updateConfig so the edit lands in the file.
    const key = changes.inputs ? '_inputs' : '_outputs';
    const value = changes.inputs ?? changes.outputs;
    send({ kind: 'mutation', mutation: { kind: 'updateConfig', nodeId, key, value } });
  }

  function makeNodeData(n: NodeDefinition): NodeViewData {
    return {
      node: n,
      catalog: catalog[n.nodeType] ?? null,
      wiredInputs: wiredByTarget[n.id] ?? new Set<string>(),
      exec: execByNode[n.id] ?? { status: 'idle' as NodeExecStatus },
      onConfigChange,
      onLabelChange,
      onPortsChange,
    };
  }

  function structuralSignatureOf(p: ProjectDefinition): string {
    const ns = p.nodes
      .filter((n) => n.nodeType !== 'Passthrough')
      .map((n) => `${n.id}:${n.nodeType}`)
      .sort()
      .join(',');
    const es = p.edges
      .map((e) => `${e.source}.${e.sourceHandle}->${e.target}.${e.targetHandle}`)
      .sort()
      .join(',');
    return `${ns}|${es}`;
  }

  function fallbackPosition(i: number): { x: number; y: number } {
    return { x: 100 + (i % 4) * 280, y: 100 + Math.floor(i / 4) * 220 };
  }

  // Diff `project` into `nodes`/`edges`. Preserves user-drag
  // positions: only new nodes receive a fresh position.
  $effect(() => {
    const structural = project.nodes.filter((n) => n.nodeType !== 'Passthrough');
    const byId = new Map(nodes.map((n) => [n.id, n]));
    const keep = new Set(structural.map((n) => n.id));

    const next: Node[] = [];
    structural.forEach((n, i) => {
      const existing = byId.get(n.id);
      const data = makeNodeData(n);
      if (existing) {
        // Preserve position. Only replace data.
        next.push({ ...existing, data });
      } else {
        next.push({
          id: n.id,
          position: savedPositions[n.id] ?? fallbackPosition(i),
          type: 'weft',
          data,
        });
      }
    });
    // Drop nodes that no longer exist in `project`.
    nodes = next.filter((n) => keep.has(n.id));

    edges = project.edges.map((e) => ({
      id: e.id,
      source: e.source,
      target: e.target,
      sourceHandle: e.sourceHandle ?? undefined,
      targetHandle: e.targetHandle ?? undefined,
      type: 'weft',
      data: {
        // Only show a label when ports differ.
        showLabel:
          (e.sourceHandle ?? '') !== (e.targetHandle ?? '') &&
          Boolean(e.sourceHandle || e.targetHandle),
        sourcePort: e.sourceHandle ?? '',
        targetPort: e.targetHandle ?? '',
      },
    }));

    // Only re-run ELK when the structural signature actually changed
    // (nodes added/removed/retyped, edges added/removed). Config
    // edits and field changes don't trigger layout.
    const sig = structuralSignatureOf(project);
    if (sig !== structuralSignature) {
      structuralSignature = sig;
      scheduleLayout();
    }
  });

  function scheduleLayout() {
    if (layoutTimer) clearTimeout(layoutTimer);
    layoutTimer = setTimeout(runLayout, layoutDebounceMs);
  }

  async function runLayout() {
    try {
      // Only auto-layout nodes that don't have a saved position yet.
      const unplaced = nodes.filter((n) => !savedPositions[n.id]);
      if (unplaced.length === 0) return;
      const positions = await autoLayout({ nodes: unplaced, edges });
      for (const n of nodes) {
        const p = positions[n.id];
        if (p) n.position = p;
      }
    } catch (err) {
      send({ kind: 'log', level: 'warn', message: `elk layout failed: ${String(err)}` });
    }
  }

  function onNodeDragStop() {
    // Persist every node's current position. Shipping all on every
    // drag stop is fine (<100 nodes per project).
    const positions: Record<string, { x: number; y: number }> = {};
    for (const n of nodes) positions[n.id] = { x: n.position.x, y: n.position.y };
    savedPositions = { ...savedPositions, ...positions };
    send({ kind: 'positionsChanged', positions });
  }

  function onConnect(e: {
    source: string;
    sourceHandle?: string | null;
    target: string;
    targetHandle?: string | null;
  }) {
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

  // Right-click context menu on the canvas or on a node. Floating
  // menu on document.body (same helper as the port context menu).
  let menuCleanup: (() => void) | undefined;

  function closeMenu() {
    menuCleanup?.();
    menuCleanup = undefined;
  }

  function openContextMenu(e: MouseEvent, items: Array<{ label: string; color?: string; onClick: () => void }>) {
    e.preventDefault();
    closeMenu();
    const backdrop = document.createElement('div');
    backdrop.style.cssText = 'position:fixed;inset:0;z-index:9998;';
    backdrop.addEventListener('click', closeMenu);
    backdrop.addEventListener('contextmenu', (ev) => {
      ev.preventDefault();
      closeMenu();
    });
    const menu = document.createElement('div');
    menu.style.cssText = `position:fixed;left:${e.clientX}px;top:${e.clientY}px;z-index:9999;background:white;border:1px solid #e4e4e7;border-radius:8px;box-shadow:0 4px 12px rgba(0,0,0,0.15);padding:4px 0;min-width:180px;`;
    for (const item of items) {
      const btn = document.createElement('button');
      const c = item.color ?? '#18181b';
      btn.style.cssText = `width:100%;display:flex;align-items:center;gap:8px;padding:6px 12px;font-size:12px;text-align:left;border:none;background:none;cursor:pointer;color:${c};`;
      btn.addEventListener('mouseenter', () => (btn.style.background = '#f4f4f5'));
      btn.addEventListener('mouseleave', () => (btn.style.background = 'none'));
      btn.innerHTML = `<span>${item.label}</span>`;
      btn.addEventListener('click', () => {
        item.onClick();
        closeMenu();
      });
      menu.appendChild(btn);
    }
    document.body.appendChild(backdrop);
    document.body.appendChild(menu);
    menuCleanup = () => {
      backdrop.remove();
      menu.remove();
      menuCleanup = undefined;
    };
  }

  function onPaneContextMenu(params: { event: MouseEvent }) {
    openContextMenu(params.event, [
      {
        label: 'Add Node...  (Ctrl+P)',
        onClick: () => {
          paletteOpen = true;
        },
      },
    ]);
  }

  function handleNodeContextMenu(ev: any) {
    if (!ev || !ev.event || !ev.node) return;
    onNodeContextMenu(ev.event as MouseEvent, ev.node);
  }

  function onNodeContextMenu(e: MouseEvent, n: any) {
    openContextMenu(e, [
      {
        label: 'Duplicate  (Ctrl+D)',
        onClick: () => {
          send({ kind: 'mutation', mutation: { kind: 'duplicateNode', nodeId: n.id } });
        },
      },
      {
        label: 'Delete  (Del)',
        color: '#ef4444',
        onClick: () => {
          send({ kind: 'mutation', mutation: { kind: 'removeNode', id: n.id } });
        },
      },
    ]);
  }
</script>

<div class="w-full h-full relative">
  <SvelteFlow
    bind:nodes
    bind:edges
    {nodeTypes}
    {edgeTypes}
    fitView
    fitViewOptions={{ padding: 0.2 }}
    minZoom={0.05}
    maxZoom={2}
    proOptions={{ hideAttribution: true }}
    onnodedragstop={onNodeDragStop}
    onconnect={onConnect}
    onpanecontextmenu={onPaneContextMenu}
    onnodecontextmenu={handleNodeContextMenu}
  >
    <Background />
    <Controls position="bottom-left" showZoom showFitView showLock={false} />
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
