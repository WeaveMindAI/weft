<script lang="ts">
  import { onMount } from 'svelte';
  import ProjectEditor from './lib/components/project/ProjectEditor.svelte';
  import { send, onMessage } from './vscode';
  import {
    setCachedParseResponse,
    setRemoteParseTrigger,
    type WeftParseError,
  } from './lib/ai/weft-parser';
  import { registerCatalog, type CatalogEntry } from './lib/nodes';
  import { translateProject } from './host-bridge';
  import type { ProjectDefinition as V1Project, LiveDataItem, NodeExecution } from './lib/types';

  let project: V1Project | null = $state(null);
  let error: string | null = $state(null);
  let weftCode = $state('');
  let layoutCode = $state('');
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  let editorRef: any = $state();

  // Live execution state fed by the host's exec follower.
  // activeEdges is a Set of graph edge IDs currently pulsing.
  // nodeStatuses/nodeOutputs snapshot the last-observed values.
  let executionState = $state<{
    isRunning: boolean;
    activeEdges: Set<string>;
    nodeStatuses: Record<string, string>;
    nodeOutputs: Record<string, unknown>;
    nodeExecutions: Record<string, NodeExecution[]>;
  }>({
    isRunning: false,
    activeEdges: new Set(),
    nodeStatuses: {},
    nodeOutputs: {},
    nodeExecutions: {},
  });

  // Per-node live data items (the node body renders these inline when
  // features.showDebugPreview / features.hasLiveData is set).
  let liveDataByNode = $state<Record<string, LiveDataItem[]>>({});

  onMount(() => {
    setRemoteParseTrigger((_source) => {
      // Webview text surgery produced a new source; the host writes
      // it to the document and re-parses. The resulting parseResult
      // message lands through onMessage below.
    });

    const unsub = onMessage((msg) => {
      if (msg.kind === 'catalogAll') {
        registerCatalog(msg.catalog as unknown as Record<string, CatalogEntry>);
        return;
      }
      if (msg.kind === 'parseResult') {
        const errs: WeftParseError[] = msg.response.diagnostics
          .filter((d) => d.severity === 'error')
          .map((d) => ({ line: d.line, message: d.message }));
        registerCatalog(msg.response.catalog as unknown as Record<string, CatalogEntry>);
        const firstMount = project === null;
        setCachedParseResponse(msg.response.project, msg.source, msg.layoutCode, errs);
        weftCode = msg.source;
        layoutCode = msg.layoutCode;
        if (firstMount) {
          project = translateProject(msg.response.project, msg.source, msg.layoutCode);
        } else if (editorRef) {
          editorRef.applyExternalSource?.(msg.source, msg.layoutCode);
        }
        error = null;
        return;
      }
      if (msg.kind === 'parseError') {
        error = msg.error;
        return;
      }
      if (msg.kind === 'execReset') {
        executionState = {
          isRunning: true,
          activeEdges: new Set(),
          nodeStatuses: {},
          nodeOutputs: {},
          nodeExecutions: {},
        };
        liveDataByNode = {};
        return;
      }
      if (msg.kind === 'execEvent') {
        const e = msg.event;
        // Normalize 'started' → 'running' so downstream class
        // checks (which test === 'running') fire correctly.
        const state = e.state === 'started' ? 'running' : e.state;
        executionState.nodeStatuses = {
          ...executionState.nodeStatuses,
          [e.nodeId]: state,
        };
        // Also maintain a NodeExecution row so ProjectEditorInner's
        // per-node class derivation works (it reads the LATEST row's
        // .status). We keep history, matching v1 semantics.
        const now = Date.now();
        const rows = executionState.nodeExecutions[e.nodeId] ?? [];
        const last = rows[rows.length - 1];
        let nextRows: NodeExecution[];
        if (state === 'running') {
          // Open a new execution row.
          nextRows = [
            ...rows,
            {
              id: `${e.nodeId}-${now}`,
              nodeId: e.nodeId,
              status: 'running',
              pulseIdsAbsorbed: [],
              pulseId: `${e.nodeId}-${now}`,
              startedAt: now,
              costUsd: 0,
              logs: [],
              color: '',
              lane: [],
            },
          ];
        } else if (last && last.status === 'running') {
          // Close the open row in place.
          nextRows = rows.map((r) =>
            r.id === last.id
              ? { ...r, status: state as NodeExecution['status'], completedAt: now, error: e.error }
              : r,
          );
        } else {
          // No open row (unexpected ordering): record a terminal
          // row so the node still paints the right color.
          nextRows = [
            ...rows,
            {
              id: `${e.nodeId}-${now}`,
              nodeId: e.nodeId,
              status: state as NodeExecution['status'],
              pulseIdsAbsorbed: [],
              pulseId: `${e.nodeId}-${now}`,
              startedAt: now,
              completedAt: now,
              error: e.error,
              costUsd: 0,
              logs: [],
              color: '',
              lane: [],
            },
          ];
        }
        executionState.nodeExecutions = {
          ...executionState.nodeExecutions,
          [e.nodeId]: nextRows,
        };
        if (state === 'completed' || state === 'failed' || state === 'skipped') {
          const allDone = Object.values(executionState.nodeStatuses).every(
            (s) => s === 'completed' || s === 'failed' || s === 'skipped' || s === 'cancelled',
          );
          if (allDone) executionState.isRunning = false;
        }
        return;
      }
      if (msg.kind === 'edgeActive') {
        const next = new Set(executionState.activeEdges);
        if (msg.event.active) next.add(msg.event.edgeId);
        else next.delete(msg.event.edgeId);
        executionState.activeEdges = next;
        return;
      }
      if (msg.kind === 'liveData') {
        liveDataByNode = {
          ...liveDataByNode,
          [msg.nodeId]: msg.items,
        };
        // Fold outputs into nodeOutputs so the graph's
        // showDebugPreview / inline chip rendering has something
        // canonical to read. We treat any item with label
        // "out.<port>" as an output pulse.
        for (const item of msg.items) {
          if (typeof item.label === 'string' && item.label.startsWith('out.')) {
            executionState.nodeOutputs = {
              ...executionState.nodeOutputs,
              [msg.nodeId]: item.data,
            };
          }
        }
        return;
      }
    });
    send({ kind: 'ready' });
    return unsub;
  });

  function onSave(data: {
    name?: string;
    description?: string;
    weftCode?: string;
    loomCode?: string;
    layoutCode?: string;
  }) {
    if (data.weftCode !== undefined && data.weftCode !== weftCode) {
      weftCode = data.weftCode;
      send({ kind: 'saveWeft', source: data.weftCode });
    }
    if (data.layoutCode !== undefined && data.layoutCode !== layoutCode) {
      layoutCode = data.layoutCode;
      send({ kind: 'saveLayout', layoutCode: data.layoutCode });
    }
  }

  function onRun() {
    executionState.isRunning = true;
    send({ kind: 'runProject' });
  }

  function onStop() {
    send({ kind: 'stopProject' });
  }
</script>

<div class="absolute inset-0">
  {#if error}
    <div class="p-4 text-destructive">parse error: {error}</div>
  {:else if project}
    <ProjectEditor
      bind:this={editorRef}
      {project}
      {onSave}
      {onRun}
      {onStop}
      {executionState}
      playground={true}
    />
  {:else}
    <div class="p-4 text-muted-foreground">loading graph...</div>
  {/if}
</div>
