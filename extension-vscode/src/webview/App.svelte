<script lang="ts">
  import { onMount } from 'svelte';
  import ProjectEditor from './lib/components/project/ProjectEditor.svelte';
  import GraphToolbar from './lib/components/project/GraphToolbar.svelte';
  import { send, onMessage } from './vscode';
  import {
    setCachedParseResponse,
    type WeftParseError,
  } from './lib/ai/weft-parser';
  import { registerCatalog, type CatalogEntry } from './lib/nodes';
  import { translateProject } from './host-bridge';
  import { nodeIsTrigger, nodeRequiresInfra } from './lib/utils/node-roles';
  import type { ProjectDefinition as V1Project, NodeExecution } from './lib/types';
  import type { ActionBarState, ActionAvailability, NodeFeedState } from '../shared/protocol';

  let project: V1Project | null = $state(null);
  let error: string | null = $state(null);
  let weftCode = $state('');
  let layoutCode = $state('');
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  let editorRef: any = $state();

  // Live execution state fed by the host's exec follower.
  // nodeStatuses/nodeOutputs snapshot the last-observed values per
  // node; nodeExecutions tracks the rolling history per node.
  let executionState = $state<{
    isRunning: boolean;
    nodeStatuses: Record<string, string>;
    nodeOutputs: Record<string, unknown>;
    nodeExecutions: Record<string, NodeExecution[]>;
  }>({
    isRunning: false,
    nodeStatuses: {},
    nodeOutputs: {},
    nodeExecutions: {},
  });

  // Per-node body-panel feeds. Each node ID maps to AT MOST ONE
  // feed depending on type:
  //   - infra nodes (requires_infra=true)  → infraFeedByNode
  //   - trigger nodes (features.isTrigger) → signalFeedByNode
  //   - debug nodes  (features.showDebugPreview) → executionState.nodeOutputs
  //   - everything else → no body-panel; modal inspector only.
  // Each feed entry is `NodeFeedState`: ok with items, or error with a
  // message. NEVER a fallback to execution data on the wrong feed.
  let infraFeedByNode = $state<Record<string, NodeFeedState>>({});
  let signalFeedByNode = $state<Record<string, NodeFeedState>>({});

  // Source-derived flags: does the project DECLARE infra / trigger
  // nodes. Driven by parse results, not by backend state. Used to
  // gate visibility of bar sections (don't show the Infra section
  // for a project with no infra nodes in source).
  let hasInfraInGraph = $state(false);
  let hasTriggersInGraph = $state(false);

  // Auto-follow state. The host-side controller owns the actual
  // decisions; we just render the badge and forward clicks.
  let followMode = $state<'latest' | 'pinned'>('latest');
  let followColor = $state<string | undefined>(undefined);
  let followPendingCount = $state(0);
  let sourceOpen = $state(false);

  // Action-bar state: single source of truth for what the bar
  // renders. The host's ActionBarStore pushes every transition;
  // the webview is a pure renderer that reads from this store.
  // `backend` always present, `overlay` carries the user-action
  // layer, `error` sticky banner.
  let actionBarState = $state<ActionBarState>({
    backend: {
      available: [],
      status: 'unknown',
      mode: 'unknown',
      infraRollup: 'none',
      runningCount: 0,
    },
    overlay: { kind: 'idle' },
  });

  // Latest /status snapshot. Drives the action bar's drift
  // indicators (Resync/Upgrade lights) AND the graph's per-node
  // infra badges. Stays current across cli_running so the lights
  // don't blink mid-verb.
  let statusSnapshot = $state<ActionAvailability | undefined>(undefined);

  onMount(() => {
    // Bubble-up listener for per-node action buttons (e.g. the
    // Regenerate-API-key button on a trigger node). ProjectNode
    // dispatches a `weft-signal-action` CustomEvent; we forward
    // to the host which calls /projects/{id}/signals/{node_id}/action.
    const onSignalAction = (e: Event) => {
      const ce = e as CustomEvent<{ nodeId: string; actionKind: string; payload?: unknown; confirm?: string }>;
      const detail = ce.detail;
      if (!detail || typeof detail.nodeId !== 'string' || typeof detail.actionKind !== 'string') return;
      send({
        kind: 'signalAction',
        nodeId: detail.nodeId,
        actionKind: detail.actionKind,
        payload: detail.payload,
        confirm: detail.confirm,
      });
    };
    window.addEventListener('weft-signal-action', onSignalAction as EventListener);
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
        // Recompute "source has infra / triggers" flags on every
        // parse so the ActionBar follows the user's edits. Source-
        // derived (independent of backend state) so a freshly
        // authored project with infra nodes shows the Start button
        // even before anything is provisioned.
        hasInfraInGraph = msg.response.project.nodes.some((n) =>
          nodeRequiresInfra({
            nodeType: n.nodeType,
            requiresInfra: (n as unknown as { requiresInfra?: boolean }).requiresInfra,
          }),
        );
        hasTriggersInGraph = msg.response.project.nodes.some((n) =>
          nodeIsTrigger({ nodeType: n.nodeType, features: n.features }),
        );
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
          nodeStatuses: {},
          nodeOutputs: {},
          nodeExecutions: {},
        };
        return;
      }
      if (msg.kind === 'execTerminal') {
        // The dispatcher reached ExecutionCompleted / ExecutionFailed.
        // Whatever the per-node tally says, the run is over: hide
        // the Stop button. Also close any node row still in
        // 'running' (in case a NodeCompleted slipped through SSE).
        executionState.isRunning = false;
        const now = Date.now();
        const rows = { ...executionState.nodeExecutions };
        for (const [nodeId, history] of Object.entries(rows)) {
          const last = history[history.length - 1];
          if (last && last.status === 'running') {
            rows[nodeId] = history.map((r) =>
              r.id === last.id
                ? { ...r, status: msg.state, completedAt: now }
                : r,
            );
          }
        }
        executionState.nodeExecutions = rows;
        return;
      }
      if (msg.kind === 'execEvent') {
        const e = msg.event;
        // 'started' is a Started event from the wire; normalize
        // for class checks (CSS tests 'running'). Suspended is a
        // first-class state from the dispatcher's lifecycle.
        const state = e.state === 'started' ? 'running' : e.state;
        executionState.nodeStatuses = {
          ...executionState.nodeStatuses,
          [e.nodeId]: state,
        };
        // One execution row per (nodeId, laneKey). The dispatcher
        // produces lifecycle events (started/suspended/resumed/
        // retried/completed/failed/skipped) on the same record;
        // we mutate the existing row, not append. Failure +
        // retry will close the live attempt into prior_attempts
        // and reset the live fields; until that wires up, a
        // fresh dispatch after a terminal row goes into the same
        // row's history (one pulse per (node, lane)).
        const now = Date.now();
        const rows = executionState.nodeExecutions[e.nodeId] ?? [];
        const laneKey = e.lane ?? '';
        const idx = rows.findIndex((r) => r.laneKey === laneKey);
        let nextRows: NodeExecution[];
        if (idx < 0) {
          // First event for this (node, lane). Open the record.
          nextRows = [
            ...rows,
            {
              id: `${e.nodeId}-${laneKey}-${now}`,
              nodeId: e.nodeId,
              status: state as NodeExecution['status'],
              pulseIdsAbsorbed: [],
              pulseId: `${e.nodeId}-${laneKey}-${now}`,
              startedAt: now,
              completedAt:
                state === 'completed' || state === 'failed' || state === 'skipped' || state === 'cancelled'
                  ? now
                  : undefined,
              error: e.error,
              costUsd: 0,
              logs: [],
              color: '',
              lane: [],
              laneKey,
              input: e.input,
              output: e.output,
            },
          ];
        } else {
          // Existing record: mutate in place per state.
          nextRows = rows.map((r, i) => {
            if (i !== idx) return r;
            const updated: NodeExecution = { ...r, status: state as NodeExecution['status'] };
            if (state === 'completed' || state === 'failed' || state === 'skipped' || state === 'cancelled') {
              updated.completedAt = now;
              if (e.output !== undefined) updated.output = e.output;
              if (e.error !== undefined) updated.error = e.error;
            }
            if (state === 'running' && e.input !== undefined && r.input === undefined) {
              updated.input = e.input;
            }
            return updated;
          });
        }
        executionState.nodeExecutions = {
          ...executionState.nodeExecutions,
          [e.nodeId]: nextRows,
        };
        // Debug preview (`features.showDebugPreview`) reads its
        // last output from `executionState.nodeOutputs[id]`. Update
        // it on completion. Earlier this rode the liveData channel;
        // now it taps the exec event directly so the body-panel
        // feeds (infra / signal display) cannot interfere.
        if (state === 'completed' && e.output !== undefined) {
          executionState.nodeOutputs = {
            ...executionState.nodeOutputs,
            [e.nodeId]: e.output,
          };
        }
        return;
      }
      if (msg.kind === 'followStatus') {
        followMode = msg.status.mode;
        followColor = msg.status.color;
        followPendingCount = msg.status.pendingCount;
        return;
      }
      if (msg.kind === 'sourceState') {
        sourceOpen = msg.open;
        return;
      }
      if (msg.kind === 'actionBarState') {
        actionBarState = msg.state;
        return;
      }
      if (msg.kind === 'statusSnapshot') {
        statusSnapshot = msg.snapshot;
        return;
      }
      if (msg.kind === 'infraLive') {
        // Sidecar /live tick for one infra node. Always overwrite
        // the previous tick: pollers are independent, errors are
        // user-visible, no fallback.
        const { nodeId, ...feed } = msg;
        infraFeedByNode = { ...infraFeedByNode, [nodeId]: feed };
        return;
      }
      if (msg.kind === 'signalDisplay') {
        // Listener /display tick for one trigger node. Overwrite
        // semantics same as infraLive.
        const { nodeId, ...feed } = msg;
        signalFeedByNode = { ...signalFeedByNode, [nodeId]: feed };
        return;
      }
    });
    send({ kind: 'ready' });
    return () => {
      unsub();
      window.removeEventListener('weft-signal-action', onSignalAction as EventListener);
    };
  });

  function onSave(data: {
    name?: string;
    description?: string;
    weftCode?: string;
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

  // Verb dispatchers. Each flushes pending edit saves (so the
  // build sees the user's freshest source) when the verb spawns
  // a worker, and forwards the message to the host. The host's
  // CLI runner emits progress events that drive every UI
  // transition; the webview never sets transitional flags itself.
  function onRun() {
    editorRef?.flushAllPendingSaves?.();
    send({ kind: 'runProject' });
  }
  // Stop is generic now: the host inspects the bar's current
  // state and either kills the spawned CLI process group
  // (cli_running) or POSTs /executions/{color}/cancel
  // (execution_running). One verb, state-aware behavior.
  function onStop() { send({ kind: 'stopAction' }); }
  function onDismissError() { send({ kind: 'dismissError' }); }
  function onActivate() {
    editorRef?.flushAllPendingSaves?.();
    send({ kind: 'activateProject' });
  }
  function onDeactivate() { send({ kind: 'deactivateProject' }); }
  function onCancelActivate() { send({ kind: 'cancelActivate' }); }
  function onReactivate() {
    editorRef?.flushAllPendingSaves?.();
    send({ kind: 'reactivateProject' });
  }
  function onCancelRunning() { send({ kind: 'cancelRunning' }); }
  function onResumeActive() {
    editorRef?.flushAllPendingSaves?.();
    send({ kind: 'resumeActive' });
  }
  function onResync() {
    editorRef?.flushAllPendingSaves?.();
    send({ kind: 'resyncProject' });
  }
  function onStartInfra() {
    editorRef?.flushAllPendingSaves?.();
    send({ kind: 'infraStart' });
  }
  function onStopInfra() { send({ kind: 'infraStop' }); }
  function onTerminateInfra() { send({ kind: 'infraTerminate' }); }
  function onUpgradeInfra() {
    editorRef?.flushAllPendingSaves?.();
    send({ kind: 'infraUpgrade' });
  }
</script>

<div class="absolute inset-0">
  {#if error}
    <div class="p-4 text-destructive">parse error: {error}</div>
  {:else if project}
    <GraphToolbar
      mode={followMode}
      color={followColor}
      pendingCount={followPendingCount}
      onTogglePin={() => send({ kind: 'followTogglePin' })}
      onCatchUp={() => send({ kind: 'followCatchUp' })}
      onOpenSource={() => send({ kind: 'openSource' })}
      sourceOpen={sourceOpen}
    />
    <ProjectEditor
      bind:this={editorRef}
      {project}
      {onSave}
      {onRun}
      {onStop}
      {onDismissError}
      {onActivate}
      {onCancelActivate}
      {onDeactivate}
      {onReactivate}
      {onCancelRunning}
      {onResumeActive}
      {onResync}
      {onStartInfra}
      {onStopInfra}
      {onTerminateInfra}
      {onUpgradeInfra}
      {actionBarState}
      drift={statusSnapshot}
      infraNodes={statusSnapshot?.infraNodes}
      {hasInfraInGraph}
      {hasTriggersInGraph}
      {executionState}
      {infraFeedByNode}
      {signalFeedByNode}
    />
  {:else}
    <div class="p-4 text-muted-foreground">loading graph...</div>
  {/if}
</div>
