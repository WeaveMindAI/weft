<script lang="ts">
  import {
    SvelteFlow,
    Background,
    Controls,
    useSvelteFlow,
    type Node,
    type Edge as FlowEdge,
  } from '@xyflow/svelte';
  import '@xyflow/svelte/dist/style.css';
  import { onMount, tick } from 'svelte';
  import { send, onMessage } from './vscode';
  import type {
    CatalogEntry,
    Edge as ProtoEdge,
    NodeDefinition,
    NodeExecEvent,
    PortDefinition,
    ProjectDefinition,
  } from '../shared/protocol';
  import ProjectNode from './components/ProjectNode.svelte';
  import CustomEdge from './components/CustomEdge.svelte';
  import GroupNode from './components/GroupNode.svelte';
  import AnnotationNode from './components/AnnotationNode.svelte';
  import CommandPalette from './components/CommandPalette.svelte';
  import { composeGraph } from './compose';
  import { toWeftEdgeRef } from './compose/build-edges';
  import { autoOrganize, buildLayoutInput } from './compose/layout';
  import {
    wouldCreateCycle,
    isValidConnection as scopeValid,
  } from './compose/cycle-check';
  import {
    absolutePosition,
    DebouncedToast,
    deepestGroupContaining,
    descendantIds,
    nodeHasConnectionsInScope,
    nodeRect,
    toScopeEdges,
  } from './compose/scope-lock';
  import { applyExecEvent } from './compose/exec-overlay';
  import type { ExecMap, LayoutMap } from './compose/types';

  interface Props {
    project: ProjectDefinition;
    catalog: Record<string, CatalogEntry>;
  }

  let { project, catalog }: Props = $props();

  const { getViewport, setViewport, updateNodeInternals } = useSvelteFlow();

  // xyflow state.
  let nodes = $state<Node[]>([]);
  let edges = $state<FlowEdge[]>([]);

  // Layout sidecar cache. Writes are batched to the extension host
  // after drag / resize / expand events.
  let layout = $state<LayoutMap>({});
  let execByNode = $state<ExecMap>({});
  let activeEdges = $state<Set<string>>(new Set());

  // Z-index boosts per click (ProjectEditorInner.svelte:2273-2288).
  let nextNodeZ = 6;
  const nodeZBoost: Record<string, number> = {};
  const edgeZBoost: Record<string, number> = {};

  // Drag state for scope-lock.
  const preDragPositions = new Map<string, { x: number; y: number }>();
  const toast = new DebouncedToast((msg) =>
    send({ kind: 'log', level: 'warn', message: msg }),
  );

  let paletteOpen = $state(false);
  let pendingConnection: { sourceNodeId: string; sourceHandle: string | null } | null = null;
  let contextFlowPos: { x: number; y: number } | null = null;

  const nodeTypes = {
    weft: ProjectNode,
    weftGroup: GroupNode,
    weftGroupCollapsed: GroupNode,
    annotation: AnnotationNode,
  };
  const edgeTypes = { weft: CustomEdge };

  // ─── Messages from the host ──────────────────────────────────────

  onMount(() => {
    const unsub = onMessage((msg) => {
      if (msg.kind === 'layoutHint') {
        // Hint from the sidecar: merge but let user drags win.
        const next = { ...layout };
        for (const [id, pos] of Object.entries(msg.positions)) {
          next[id] = { ...(next[id] ?? { x: 0, y: 0 }), x: pos.x, y: pos.y };
        }
        layout = next;
      } else if (msg.kind === 'execEvent') {
        execByNode = applyExecEvent(execByNode, msg.event);
      } else if (msg.kind === 'edgeActive') {
        const set = new Set(activeEdges);
        if (msg.event.active) set.add(msg.event.edgeId);
        else set.delete(msg.event.edgeId);
        activeEdges = set;
      } else if (msg.kind === 'execReset') {
        execByNode = {};
        activeEdges = new Set();
      } else if (msg.kind === 'liveData') {
        // Live data flows in through the exec events' output side in
        // v2, so there's nothing node-specific to cache here yet.
      }
    });
    window.addEventListener('keydown', onHotkey);
    return () => {
      unsub();
      window.removeEventListener('keydown', onHotkey);
    };
  });

  // ─── Recompose on every project / layout / exec change ───────────

  $effect(() => {
    const result = composeGraph({
      project,
      catalog,
      layout,
      exec: execByNode,
      activeEdges,
      previous: nodes,
      nodeZBoost,
      edgeZBoost,
      onConfigChange,
      onLabelChange,
      onPortsChange,
    });
    nodes = result.nodes;
    edges = result.edges;
  });

  // ─── Mutations ──────────────────────────────────────────────────

  function onConfigChange(nodeId: string, key: string, value: unknown) {
    // Layout-only keys don't round-trip through the weft source.
    if (key === 'width' || key === 'height' || key === 'expanded') {
      persistLayoutOverride(nodeId, { [key === 'width' ? 'w' : key === 'height' ? 'h' : 'expanded']: value });
      return;
    }
    send({ kind: 'mutation', mutation: { kind: 'updateConfig', nodeId, key, value } });
  }
  function onLabelChange(nodeId: string, label: string | null) {
    send({ kind: 'mutation', mutation: { kind: 'updateLabel', nodeId, label } });
  }
  function onPortsChange(
    nodeId: string,
    changes: { inputs?: PortDefinition[]; outputs?: PortDefinition[] },
  ) {
    const isGroup = project.groups.some((g) => g.id === nodeId);
    const mutation = isGroup
      ? ({
          kind: 'updateGroupPorts',
          groupLabel: nodeId,
          inputs: changes.inputs ?? portsOf(nodeId, 'in'),
          outputs: changes.outputs ?? portsOf(nodeId, 'out'),
        } as const)
      : ({
          kind: 'updateNodePorts',
          nodeId,
          inputs: changes.inputs ?? portsOf(nodeId, 'in'),
          outputs: changes.outputs ?? portsOf(nodeId, 'out'),
        } as const);
    send({ kind: 'mutation', mutation });
    tick().then(() => updateNodeInternals(nodeId));
  }

  function portsOf(id: string, side: 'in' | 'out'): PortDefinition[] {
    const g = project.groups.find((gg) => gg.id === id);
    if (g) return side === 'in' ? g.inPorts : g.outPorts;
    const n = project.nodes.find((nn) => nn.id === id);
    if (!n) return [];
    return side === 'in' ? n.inputs : n.outputs;
  }

  function persistLayoutOverride(
    nodeId: string,
    patch: Partial<{ x: number; y: number; w: number; h: number; expanded: boolean }>,
  ) {
    const cur = layout[nodeId] ?? { x: 0, y: 0 };
    layout = { ...layout, [nodeId]: { ...cur, ...patch } };
    send({ kind: 'layoutChanged', layout });
  }

  // ─── Keyboard ────────────────────────────────────────────────────

  function onHotkey(e: KeyboardEvent) {
    const target = e.target as HTMLElement | null;
    const inInput =
      target &&
      (target.tagName === 'INPUT' ||
        target.tagName === 'TEXTAREA' ||
        target.isContentEditable ||
        target.closest('[role="dialog"]'));

    if ((e.metaKey || e.ctrlKey) && e.key.toLowerCase() === 'p') {
      paletteOpen = !paletteOpen;
      e.preventDefault();
      return;
    }

    if (inInput) return;

    if ((e.metaKey || e.ctrlKey) && e.key.toLowerCase() === 'd') {
      const sel = nodes.find((n) => n.selected);
      if (sel) {
        e.preventDefault();
        send({ kind: 'mutation', mutation: { kind: 'duplicateNode', nodeId: sel.id } });
      }
      return;
    }

    if (e.key === 'Delete' || e.key === 'Backspace') {
      let touched = false;
      // Delete selected edges FIRST so Delete with both selected
      // doesn't drop their endpoint too.
      for (const edge of edges) {
        if (edge.selected) {
          sendDeleteEdge(edge);
          touched = true;
        }
      }
      for (const n of nodes) {
        if (n.selected) {
          const isGroup = (n.data as { node?: NodeDefinition }).node?.nodeType === 'Group';
          send({
            kind: 'mutation',
            mutation: isGroup
              ? { kind: 'removeGroup', label: n.id }
              : { kind: 'removeNode', id: n.id },
          });
          touched = true;
        }
      }
      if (touched) e.preventDefault();
      return;
    }
  }

  function sendDeleteEdge(edge: FlowEdge) {
    // Look up the RAW project edge by id. This sidesteps __inner /
    // `self` translation — the dispatcher needs the flat passthrough
    // form anyway because that's what it has spans for.
    const raw = project.edges.find((e) => e.id === edge.id);
    if (!raw) return;
    send({
      kind: 'mutation',
      mutation: {
        kind: 'removeEdge',
        source: raw.source,
        sourcePort: raw.sourceHandle ?? '',
        target: raw.target,
        targetPort: raw.targetHandle ?? '',
      },
    });
  }

  // ─── Connection flow ────────────────────────────────────────────

  function onBeforeConnect(c: {
    source: string;
    sourceHandle?: string | null;
    target: string;
    targetHandle?: string | null;
  }): FlowEdge | null {
    if (!c.source || !c.target) return null;
    if (wouldCreateCycle(c.source, c.target, edges)) {
      send({ kind: 'log', level: 'warn', message: 'Would create a cycle' });
      return null;
    }
    // 1-driver-per-input: drop any existing edge matching the same
    // (target, targetHandle). xyflow lets us return the NEW edge
    // (surgical removeEdge will handle the cleanup server-side).
    const existing = edges.find(
      (e) =>
        e.target === c.target && (e.targetHandle ?? null) === (c.targetHandle ?? null),
    );
    if (existing) sendDeleteEdge(existing);
    setTimeout(() => {
      const ref = toWeftEdgeRef(
        c.source,
        c.sourceHandle ?? null,
        c.target,
        c.targetHandle ?? null,
        new Map(
          nodes.map((n) => [
            n.id,
            (n.data as { node?: NodeDefinition }).node ?? (n as unknown as NodeDefinition),
          ]),
        ),
      );
      send({
        kind: 'mutation',
        mutation: {
          kind: 'addEdge',
          source: ref.source,
          sourcePort: ref.sourcePort,
          target: ref.target,
          targetPort: ref.targetPort,
          scopeGroupLabel: ref.scopeGroupLabel,
        },
      });
    }, 0);
    return {
      id: `${c.source}.${c.sourceHandle}->${c.target}.${c.targetHandle}`,
      source: c.source,
      target: c.target,
      sourceHandle: c.sourceHandle ?? undefined,
      targetHandle: c.targetHandle ?? undefined,
      type: 'weft',
      zIndex: 5,
    };
  }

  function onReconnect(oldEdge: FlowEdge, newConn: {
    source: string;
    sourceHandle?: string | null;
    target: string;
    targetHandle?: string | null;
  }) {
    sendDeleteEdge(oldEdge);
    onBeforeConnect(newConn);
    edges = edges.map((e) =>
      e.id === oldEdge.id
        ? {
            ...e,
            source: newConn.source,
            target: newConn.target,
            sourceHandle: newConn.sourceHandle ?? undefined,
            targetHandle: newConn.targetHandle ?? undefined,
          }
        : e,
    );
  }

  function validateConnection(c: {
    source: string;
    sourceHandle: string | null;
    target: string;
    targetHandle: string | null;
  }): boolean {
    return scopeValid(
      { nodeId: c.source, handleId: c.sourceHandle },
      { nodeId: c.target, handleId: c.targetHandle },
      nodes.map((n) => ({ id: n.id, type: n.type, parentId: n.parentId as string | undefined })),
    );
  }

  function onConnectEnd(e: unknown) {
    const ev = e as { connection?: { fromNode?: { id: string }; fromHandle?: { id: string } }; isValid?: boolean; clientX: number; clientY: number };
    if (ev.isValid) return;
    const src = ev.connection?.fromNode?.id;
    const handle = ev.connection?.fromHandle?.id ?? null;
    if (!src) return;
    pendingConnection = { sourceNodeId: src, sourceHandle: handle };
    contextFlowPos = { x: ev.clientX, y: ev.clientY };
    paletteOpen = true;
  }

  // ─── Drag / scope-lock ──────────────────────────────────────────

  function onNodeDragStart(e: unknown) {
    const ev = e as { targetNode?: Node };
    const n = ev?.targetNode;
    if (!n) return;
    preDragPositions.set(n.id, { x: n.position.x, y: n.position.y });
    nodeZBoost[n.id] = nextNodeZ;
    for (const edge of edges) {
      if (edge.source === n.id || edge.target === n.id) {
        edgeZBoost[edge.id] = nextNodeZ + 1;
      }
    }
    nextNodeZ++;
  }

  function onNodeDragStop(e: unknown) {
    const ev = e as { targetNode?: Node };
    const target = ev?.targetNode;
    if (!target) return;
    const nodesById = new Map(nodes.map((n) => [n.id, n]));
    const scopeEdges = toScopeEdges(edges);

    let current = nodesById.get(target.id);
    if (!current) return;

    // 1. checkNodeLeavesGroup
    if (current.parentId) {
      const parent = nodesById.get(current.parentId);
      if (parent) {
        const parentR = nodeRect(parent);
        const stillInside =
          current.position.x >= 0 &&
          current.position.y >= 0 &&
          current.position.x <= parentR.w &&
          current.position.y <= parentR.h;
        if (!stillInside) {
          if (nodeHasConnectionsInScope(current.id, current.parentId, nodes, scopeEdges)) {
            const snap = preDragPositions.get(current.id);
            if (snap) current.position = snap;
            toast.fire(
              'Cannot change scope. Disconnect this node from other nodes in its current scope first.',
            );
          } else {
            const abs = absolutePosition(current, nodesById);
            current.position = abs;
            current.parentId = undefined;
            send({
              kind: 'mutation',
              mutation: { kind: 'moveNodeScope', nodeId: current.id, targetGroupLabel: null },
            });
          }
        }
      }
    }

    // 2. checkNodeCapturedByGroup — only if node isn't itself a group
    const isGroup = current.type === 'weftGroup' || current.type === 'weftGroupCollapsed';
    if (!isGroup) {
      const abs = absolutePosition(current, nodesById);
      const exclude = new Set<string>([current.id]);
      for (const child of descendantIds(current.id, nodes)) exclude.add(child);
      const host = deepestGroupContaining(abs, nodes, exclude);
      if (host && host.id !== current.parentId) {
        if (nodeHasConnectionsInScope(current.id, current.parentId ?? null, nodes, scopeEdges)) {
          const snap = preDragPositions.get(current.id);
          if (snap) current.position = snap;
          toast.fire(
            'Cannot change scope. Disconnect this node from other nodes in its current scope first.',
          );
        } else {
          const hostAbs = absolutePosition(host, nodesById);
          current.position = { x: abs.x - hostAbs.x, y: abs.y - hostAbs.y };
          current.parentId = host.id;
          send({
            kind: 'mutation',
            mutation: { kind: 'moveNodeScope', nodeId: current.id, targetGroupLabel: host.id },
          });
        }
      }
    }

    // 3. checkGroupCapturesNodes — deferred to a follow-up pass;
    //    left as a TODO because it requires replaying all siblings.
    //    Not needed for the common single-node drag.

    persistLayoutOverride(current.id, { x: current.position.x, y: current.position.y });
  }

  // ─── Node selection → z raise ──────────────────────────────────

  function onNodeClick(e: unknown) {
    const ev = e as { node?: Node };
    const n = ev?.node;
    if (!n) return;
    nodeZBoost[n.id] = nextNodeZ;
    for (const edge of edges) {
      if (edge.source === n.id || edge.target === n.id) {
        edgeZBoost[edge.id] = nextNodeZ + 1;
      }
    }
    nextNodeZ++;
  }

  // ─── Context menu + palette ────────────────────────────────────

  let menuCleanup: (() => void) | undefined;
  function closeMenu() {
    menuCleanup?.();
    menuCleanup = undefined;
  }
  function openContextMenu(
    e: MouseEvent,
    items: Array<{ label: string; color?: string; onClick: () => void }>,
  ) {
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
      { label: 'Add Node...  (Ctrl+P)', onClick: () => (paletteOpen = true) },
      {
        label: 'Add Annotation',
        onClick: () =>
          send({
            kind: 'mutation',
            mutation: {
              kind: 'addNode',
              id: `annot_${Date.now().toString(36)}`,
              nodeType: 'Annotation',
            },
          }),
      },
      {
        label: 'Add Group',
        onClick: () =>
          send({
            kind: 'mutation',
            mutation: { kind: 'addGroup', label: generateUniqueGroupLabel('Group') },
          }),
      },
    ]);
  }

  function onNodeContextMenu(ev: { event: MouseEvent; node: Node }) {
    const isGroup = ev.node.type === 'weftGroup' || ev.node.type === 'weftGroupCollapsed';
    const items: Array<{ label: string; color?: string; onClick: () => void }> = [
      {
        label: 'Duplicate  (Ctrl+D)',
        onClick: () =>
          send({
            kind: 'mutation',
            mutation: { kind: 'duplicateNode', nodeId: ev.node.id },
          }),
      },
      {
        label: 'Delete  (Del)',
        color: '#ef4444',
        onClick: () =>
          send({
            kind: 'mutation',
            mutation: isGroup
              ? { kind: 'removeGroup', label: ev.node.id }
              : { kind: 'removeNode', id: ev.node.id },
          }),
      },
    ];
    openContextMenu(ev.event, items);
  }

  function onPickNode(nodeType: string) {
    paletteOpen = false;
    const id = `n_${Date.now().toString(36)}`;
    send({
      kind: 'mutation',
      mutation: { kind: 'addNode', id, nodeType },
    });
    if (pendingConnection) {
      const pc = pendingConnection;
      pendingConnection = null;
      setTimeout(() => {
        send({
          kind: 'mutation',
          mutation: {
            kind: 'addEdge',
            source: pc.sourceNodeId,
            sourcePort: pc.sourceHandle ?? 'value',
            target: id,
            targetPort: 'value',
          },
        });
      }, 50);
    }
  }

  function onPickAction(action: string) {
    paletteOpen = false;
    switch (action) {
      case 'delete': {
        for (const n of nodes.filter((nn) => nn.selected)) {
          const isGroup = n.type === 'weftGroup' || n.type === 'weftGroupCollapsed';
          send({
            kind: 'mutation',
            mutation: isGroup
              ? { kind: 'removeGroup', label: n.id }
              : { kind: 'removeNode', id: n.id },
          });
        }
        break;
      }
      case 'duplicate': {
        const sel = nodes.find((n) => n.selected);
        if (sel) send({ kind: 'mutation', mutation: { kind: 'duplicateNode', nodeId: sel.id } });
        break;
      }
      case 'selectAll': {
        nodes = nodes.map((n) => ({ ...n, selected: true }));
        break;
      }
      case 'fitView':
        // xyflow's fit-view button lives in <Controls>; the action
        // here emits a synthetic click by dispatching a resize event
        // so the SvelteFlow store recomputes its bounds.
        window.dispatchEvent(new Event('resize'));
        break;
      case 'autoOrganize': {
        void runAutoLayout(true);
        break;
      }
    }
  }

  async function runAutoLayout(persist: boolean): Promise<void> {
    const input = buildLayoutInput(nodes, edges);
    try {
      const { positions, groupSizes } = await autoOrganize(input.nodes, input.edges);
      const nextLayout: typeof layout = { ...layout };
      for (const [id, p] of positions) {
        const cur = nextLayout[id] ?? { x: 0, y: 0 };
        nextLayout[id] = { ...cur, x: p.x, y: p.y };
      }
      for (const [id, s] of groupSizes) {
        const cur = nextLayout[id] ?? { x: 0, y: 0 };
        nextLayout[id] = { ...cur, w: s.w, h: s.h };
      }
      layout = nextLayout;
      if (persist) send({ kind: 'layoutChanged', layout });
    } catch (err) {
      send({ kind: 'log', level: 'warn', message: `elk failed: ${String(err)}` });
    }
  }

  function generateUniqueGroupLabel(base: string): string {
    const taken = new Set(project.groups.map((g) => g.label ?? g.id));
    if (!taken.has(base)) return base;
    for (let i = 2; i < 9999; i++) {
      const cand = `${base}_${i}`;
      if (!taken.has(cand)) return cand;
    }
    return base;
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
    onnodedragstart={onNodeDragStart}
    onnodedragstop={onNodeDragStop}
    onnodeclick={onNodeClick}
    onbeforeconnect={onBeforeConnect}
    onreconnect={onReconnect}
    onconnectend={onConnectEnd}
    isValidConnection={validateConnection}
    onpanecontextmenu={onPaneContextMenu}
    onnodecontextmenu={onNodeContextMenu}
  >
    <Background />
    <Controls position="bottom-left" showZoom showFitView showLock={false} />
  </SvelteFlow>

  <CommandPalette
    open={paletteOpen}
    catalog={catalog}
    onPick={onPickNode}
    onAction={onPickAction}
    onClose={() => (paletteOpen = false)}
  />
</div>

<style>
  div {
    width: 100%;
    height: 100%;
  }
</style>
