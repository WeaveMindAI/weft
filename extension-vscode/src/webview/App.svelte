<script lang="ts">
  import { onMount } from 'svelte';
  import ProjectEditor from './lib/components/project/ProjectEditor.svelte';
  import FollowBadge from './lib/components/project/FollowBadge.svelte';
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

  // Cluster-side infra lifecycle state. Combined from:
  //   - the parse result (does the project declare any infra node?
  //     → hasInfraInFrontend), and
  //   - the host's /infra/status poll (what's the cluster saying
  //     now? → hasInfraInBackend + status + nodes).
  // The ActionBar hides entirely unless hasInfrastructure is true,
  // so we need the frontend-derived signal so a freshly-authored
  // project with infra nodes but nothing provisioned yet still
  // shows the Start button.
  let hasInfraInFrontend = $state(false);
  let hasInfraInBackend = $state(false);
  let hasTriggersInFrontend = $state(false);
  let infraBackendStatus = $state<'none' | 'running' | 'stopped' | 'mixed'>('none');
  let infraTransitional = $state<'starting' | 'stopping' | 'terminating' | null>(null);
  let infraBackendNodes = $state<Array<{ nodeId: string; nodeType: string; instanceId: string; status: string }>>([]);
  let infraIsLoading = $state(true);
  let triggerProjectStatus = $state<'registered' | 'active' | 'inactive' | 'unknown'>('unknown');
  let triggerTransitional = $state<'activating' | 'deactivating' | null>(null);
  let triggerFirstResponseReceived = $state(false);

  // Auto-follow state. The host-side controller owns the actual
  // decisions; we just render the badge and forward clicks.
  let followMode = $state<'latest' | 'pinned'>('latest');
  let followColor = $state<string | undefined>(undefined);
  let followPendingCount = $state(0);

  // Per-node gate for the inline live-data strip. Only nodes
  // whose type declares `features.hasLiveData` opt into having
  // in/out pulses rendered under the node body. Without this
  // gate every node that executed would show an input/output
  // strip, which is noise: users should use the modal inspector
  // (click the icon) to see input/output for arbitrary nodes.
  // WhatsAppBridge is the canonical yes (QR code + status);
  // WhatsAppReceive, WhatsAppSend, Debug, etc. are no.
  let hasLiveDataByNode = $state<Record<string, boolean>>({});

  // Effective status the ActionBar reads: transitional wins over
  // the polled backend state so the button shows "Starting..."
  // immediately when the user clicks, not 3 seconds later.
  let infraState = $derived({
    hasInfrastructure: hasInfraInFrontend || hasInfraInBackend,
    hasInfraInFrontend,
    hasInfraInBackend,
    status: (infraTransitional ?? infraBackendStatus) as string,
    nodes: infraBackendNodes,
    isLoading: infraIsLoading,
  });

  let triggerState = $derived({
    hasTriggers: hasTriggersInFrontend,
    isActive: triggerProjectStatus === 'active' && triggerTransitional !== 'deactivating',
    // Only show "Checking..." while a transition the user just
    // asked for is in flight, or on the very first poll before
    // any response lands. The ActionBar greys the Activate button
    // out via its own `infraBlocking` check when infra exists but
    // isn't running, so we don't have to re-implement that here.
    isLoading:
      triggerTransitional !== null || (!triggerFirstResponseReceived && hasTriggersInFrontend),
  });

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
        // Recompute "source has infra / triggers" flags on every
        // parse so the ActionBar follows the user's edits. These
        // drive ActionBar visibility without waiting for the poll.
        const catalog = msg.response.catalog;
        hasInfraInFrontend = msg.response.project.nodes.some((n) => {
          const entry = catalog[n.nodeType] as { requires_infra?: boolean } | undefined;
          return (n as unknown as { requiresInfra?: boolean }).requiresInfra ?? entry?.requires_infra ?? false;
        });
        hasTriggersInFrontend = msg.response.project.nodes.some((n) => {
          const entry = catalog[n.nodeType] as { features?: { isTrigger?: boolean } } | undefined;
          return (n.features?.isTrigger ?? entry?.features?.isTrigger) ?? false;
        });
        // Build the per-node gate so the liveData handler can
        // drop pulses for nodes that didn't opt in.
        hasLiveDataByNode = Object.fromEntries(
          msg.response.project.nodes.map((n) => {
            const entry = catalog[n.nodeType] as { features?: { hasLiveData?: boolean } } | undefined;
            const on = (n.features?.hasLiveData ?? entry?.features?.hasLiveData) ?? false;
            return [n.id, on];
          }),
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
          activeEdges: new Set(),
          nodeStatuses: {},
          nodeOutputs: {},
          nodeExecutions: {},
        };
        liveDataByNode = {};
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
          // Open a new execution row with the input payload so the
          // modal inspector can render it even before the node has
          // completed.
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
              input: e.input,
            },
          ];
        } else if (last && last.status === 'running') {
          // Close the open row in place. Keep whatever input was
          // stashed at start; add output now.
          nextRows = rows.map((r) =>
            r.id === last.id
              ? {
                  ...r,
                  status: state as NodeExecution['status'],
                  completedAt: now,
                  error: e.error,
                  output: e.output ?? r.output,
                }
              : r,
          );
        } else {
          // No open row (unexpected ordering or skipped without a
          // start, which is how the engine reports downstream
          // skips). Record a terminal row carrying whatever the
          // event contained so the modal has something to show.
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
              input: e.input,
              output: e.output,
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
      if (msg.kind === 'infraStatus') {
        const snap = msg.snapshot;
        hasInfraInBackend = snap.nodes.length > 0;
        infraBackendStatus = snap.rollup;
        infraBackendNodes = snap.nodes.map((n) => ({
          nodeId: n.nodeId,
          nodeType: '',
          instanceId: n.nodeId,
          status: n.status,
        }));
        infraIsLoading = false;
        // Clear the optimistic transitional state once the backend
        // poll reports ANY settled rollup. We match by expected
        // end state where possible ('starting' expects 'running',
        // 'stopping' expects 'stopped'/'none', 'terminating'
        // expects 'none'), but we also bail out if the user
        // interacted and the backend rollup is plainly different
        // from what we'd see mid-transition, to avoid getting
        // stuck forever if the transition overshoots or the
        // click-side optimistic state is stale.
        const expected: Record<string, Array<'running' | 'stopped' | 'mixed' | 'none'>> = {
          starting: ['running', 'mixed'],
          stopping: ['stopped', 'none'],
          terminating: ['none'],
        };
        if (infraTransitional && expected[infraTransitional]?.includes(snap.rollup)) {
          infraTransitional = null;
        }
        return;
      }
      if (msg.kind === 'followStatus') {
        followMode = msg.status.mode;
        followColor = msg.status.color;
        followPendingCount = msg.status.pendingCount;
        return;
      }
      if (msg.kind === 'actionFailed') {
        // Something went wrong on the dispatcher side. Drop any
        // optimistic transitional flag so the ActionBar stops
        // showing "Activating..." / "Starting..." forever. The
        // toast has already surfaced the concrete error to the
        // user via the host-side showErrorMessage.
        if (
          msg.action === 'infraStart' ||
          msg.action === 'infraStop' ||
          msg.action === 'infraTerminate'
        ) {
          infraTransitional = null;
        } else {
          triggerTransitional = null;
        }
        return;
      }
      if (msg.kind === 'triggerStatus') {
        const prevStatus = triggerProjectStatus;
        triggerProjectStatus = msg.snapshot.projectStatus;
        triggerFirstResponseReceived = true;
        // Clear the optimistic transitional flag on any settled
        // state that matches the user's intent, or when the
        // status visibly changed (even if not to the exact end
        // state we expected). Last-resort timeout handled by
        // actionFailed when the POST errors out.
        if (triggerTransitional === 'activating') {
          if (triggerProjectStatus === 'active' || prevStatus !== triggerProjectStatus) {
            triggerTransitional = null;
          }
        } else if (triggerTransitional === 'deactivating') {
          if (triggerProjectStatus !== 'active' || prevStatus !== triggerProjectStatus) {
            triggerTransitional = null;
          }
        }
        return;
      }
      if (msg.kind === 'liveData') {
        // Only surface the inline live strip on nodes that opted in
        // via `features.hasLiveData`. Everything else flows through
        // the modal inspector (click the node's icon) and the
        // NodeExecution rows we maintain separately.
        if (hasLiveDataByNode[msg.nodeId]) {
          liveDataByNode = {
            ...liveDataByNode,
            [msg.nodeId]: msg.items,
          };
        }
        // Still fold outputs into nodeOutputs so the Debug node's
        // inline preview (showDebugPreview) has something to read,
        // regardless of hasLiveData. Debug doesn't opt into
        // hasLiveData but DOES read the last output chip.
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

  function onStartInfra() {
    infraTransitional = 'starting';
    send({ kind: 'infraStart' });
  }
  function onStopInfra() {
    infraTransitional = 'stopping';
    send({ kind: 'infraStop' });
  }
  function onTerminateInfra() {
    infraTransitional = 'terminating';
    send({ kind: 'infraTerminate' });
  }

  function onToggleTrigger() {
    if (triggerProjectStatus === 'active') {
      triggerTransitional = 'deactivating';
      send({ kind: 'deactivateProject' });
    } else {
      triggerTransitional = 'activating';
      send({ kind: 'activateProject' });
    }
  }
</script>

<div class="absolute inset-0">
  {#if error}
    <div class="p-4 text-destructive">parse error: {error}</div>
  {:else if project}
    <FollowBadge
      mode={followMode}
      color={followColor}
      pendingCount={followPendingCount}
      onTogglePin={() => send({ kind: 'followTogglePin' })}
      onCatchUp={() => send({ kind: 'followCatchUp' })}
    />
    <ProjectEditor
      bind:this={editorRef}
      {project}
      {onSave}
      {onRun}
      {onStop}
      {executionState}
      {infraState}
      {triggerState}
      {onToggleTrigger}
      {onStartInfra}
      {onStopInfra}
      {onTerminateInfra}
      infraLiveData={liveDataByNode}
      playground={true}
    />
  {:else}
    <div class="p-4 text-muted-foreground">loading graph...</div>
  {/if}
</div>
