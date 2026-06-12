<script lang="ts">
  import { onMount } from 'svelte';
  import { toast } from 'svelte-sonner';
  import ProjectEditor from './lib/components/project/ProjectEditor.svelte';
  import GraphToolbar from './lib/components/project/GraphToolbar.svelte';
  import { send, onMessage } from './vscode';
  import { registerCatalog, setCatalog, type CatalogEntry } from '$lib/nodes';
  import { translateProject } from './host-bridge';
  import { nodeIsTrigger, nodeRequiresInfra } from './lib/utils/node-roles';
  import type { ProjectDefinition as V1Project, NodeExecution, ExecutionState } from './lib/types';
  import type { ActionBarState, ActionAvailability, NodeFeedState, TextEdit, EditOp, FileContent, ProjectDefinition as ProtocolProject } from '../shared/protocol';
  import type { EditRpcResult } from '$lib/projection/types';

  let project: V1Project | null = $state(null);
  let error: string | null = $state(null);
  // Catalog feedback, independent of the parse banner: a full-load
  // failure (catalogError.error) or per-node soft warnings. Lives on
  // its own so a successful parse can't erase a live catalog problem.
  let catalogError: string | null = $state(null);
  let catalogWarnings: string[] = $state([]);
  let layoutCode = $state('');
  // RPC for source edits: applyEdits/applyTextEdit await the host's
  // `editApplied` reply (correlated by id). Success carries the inverse text
  // edit (the editor's undo) PLUS the post-edit truth, translated here at the
  // wire boundary so the editor stays in its own project shape. A refusal
  // REJECTS with the host's reason: resolve-vs-reject is the success/failure
  // channel, so a refused edit always rolls the editor's optimistic op back.
  let nextRequestId = 0;
  const pendingEdits = new Map<number, { resolve: (r: EditRpcResult) => void; reject: (err: Error) => void }>();
  function requestEdit(make: (requestId: number) => void): Promise<EditRpcResult> {
    const requestId = nextRequestId++;
    return new Promise((resolve, reject) => {
      pendingEdits.set(requestId, { resolve, reject });
      make(requestId);
    });
  }
  // RPC for post-rejection resyncs: resolves the host's current truth, or
  // null when the source doesn't parse right now (the editor keeps its
  // previous truth until the parse path delivers a fresh one).
  const pendingResyncs = new Map<number, { resolve: (r: { project: V1Project; weftCode: string } | null) => void }>();
  function requestResync(): Promise<{ project: V1Project; weftCode: string } | null> {
    const requestId = nextRequestId++;
    return new Promise((resolve) => {
      pendingResyncs.set(requestId, { resolve });
      send({ kind: 'resyncSource', requestId });
    });
  }
  // Include-navigation back-stack state, driven by the host's `navState`.
  let navDepth = $state(0);
  let navFileName = $state('');
  let execPrefix = $state('');
  // Resolved state of @file targets, keyed by the marker's relative path:
  // content or a read error. The display value for a file-backed field;
  // config holds only the marker.
  let fileContents = $state<Record<string, FileContent>>({});
  // Set true when the opened file has no saved layout, so the editor runs
  // auto-organize on mount instead of piling nodes at the origin.
  let autoOrganizeOnMount = $state(false);
  // Bumped on every fresh view (first open + each include navigation) so the
  // ProjectEditor is keyed to remount, re-running its mount-time layout
  // (auto-organize when the file has no saved layout).
  let viewGeneration = $state(0);
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  let editorRef: any = $state();

  // Live execution state fed by the host's exec follower.
  // nodeOutputs snapshots the last-observed output per node;
  // nodeExecutions tracks the rolling history per node.
  //
  // busLogByBus is the source of truth for bus conversations: every
  // bus event ever observed (live or replay), keyed by busId. The
  // inspector for a given node N renders one IRC panel per bus in
  // `busParticipantsByBus` whose participant set contains N.
  // `busMetaByBus` carries per-bus header metadata (mode) seeded
  // from the first BusParticipant edge the dispatcher derives from
  // the bus marker JSON.
  let executionState = $state<ExecutionState>({
    isRunning: false,
    nodeOutputs: {},
    nodeExecutions: {},
    busLogByBus: {},
    busMetaByBus: {},
    busParticipantsByBus: {},
    journalCorruptions: [],
    loopEventsByGroup: {},
  });

  // Dedup keys for append-only inspector logs. The execution follower
  // subscribes to the live SSE stream BEFORE replaying the journal
  // snapshot, so an event can arrive via BOTH the replay and the live
  // buffer (the overlap window). Node executions are idempotent (keyed
  // by (nodeId, framesKey), mutated in place), but bus and loop logs
  // are append-only, so without dedup the same chat line / loop
  // iteration would render twice. Bus events carry a per-bus `offset`;
  // loop events are keyed by (groupId, kind, parentFrames, index).
  // Reset on execReset (a fresh follow). Kept OUTSIDE `executionState`
  // ($state) since they're pure bookkeeping, not rendered.
  let seenBusKeys = new Set<string>();
  let seenLoopKeys = new Set<string>();

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
  // nodes. Driven by every truth carrier (parseResult, editApplied,
  // sourceResynced), not by backend state. Used to gate visibility of
  // bar sections (don't show the Infra section for a project with no
  // infra nodes in source). Recomputed on every truth so the bar
  // follows graph edits too, e.g. dropping in the first infra node.
  let hasInfraInGraph = $state(false);
  let hasTriggersInGraph = $state(false);
  function recomputeSourceFlags(project: ProtocolProject): void {
    hasInfraInGraph = project.nodes.some((n) =>
      nodeRequiresInfra({
        nodeType: n.nodeType,
        requiresInfra: (n as unknown as { requiresInfra?: boolean }).requiresInfra,
      }),
    );
    hasTriggersInGraph = project.nodes.some((n) =>
      nodeIsTrigger({ nodeType: n.nodeType, features: n.features }),
    );
  }

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
      if (msg.kind === 'editApplied') {
        // Reply to applyEdits/applyTextEdit. Success resolves with the inverse
        // PLUS the post-edit truth (translated here); a refusal REJECTS with
        // the host's reason so the editor rolls its optimistic op back. A
        // translation failure on the reply (wire-shape drift) is surfaced as a
        // rejection too: the editor rolls back instead of diverging silently.
        const pending = pendingEdits.get(msg.requestId);
        if (!pending) return;
        pendingEdits.delete(msg.requestId);
        if (!msg.ok) {
          pending.reject(new Error(msg.reason));
          return;
        }
        // No truth payload: the host applied the edit but the user switched
        // `.weft` tabs mid-round-trip. Resolve the inverse for undo bookkeeping
        // WITHOUT advancing truth (the new doc's parseResult is its truth).
        if (msg.response === undefined || msg.source === undefined) {
          pending.resolve({ inverse: msg.inverse ?? null, project: null, weftCode: '' });
          return;
        }
        try {
          // Translate FIRST: only advance catalog + bar flags once the new
          // truth actually renders, so a failure can't leave them ahead of the
          // displayed project.
          const translated = translateProject(msg.response.project, msg.source, layoutCode);
          registerCatalog(msg.response.catalog as unknown as Record<string, CatalogEntry>);
          recomputeSourceFlags(msg.response.project);
          error = null;
          pending.resolve({ inverse: msg.inverse ?? null, project: translated, weftCode: msg.source });
        } catch (e) {
          // The edit IS on disk; this is not a refused edit, it's a truth the
          // webview cannot render (wire-shape drift). Do NOT roll back (that
          // would diverge the editor from disk). Surface the inline error
          // banner (same as a parseResult translation failure) and resolve the
          // undo inverse with no truth advance.
          const errMsg = e instanceof Error ? e.message : String(e);
          console.error('translateProject failed on editApplied:', errMsg);
          error = `Project translation failed: ${errMsg}`;
          pending.resolve({ inverse: msg.inverse ?? null, project: null, weftCode: '' });
        }
        return;
      }
      if (msg.kind === 'sourceResynced') {
        const pending = pendingResyncs.get(msg.requestId);
        if (!pending) return;
        pendingResyncs.delete(msg.requestId);
        if (!msg.ok) {
          // The current source doesn't parse (user mid-edit in the text tab).
          // The editor keeps its previous truth; surface why.
          toast.warning('Source has errors', { description: msg.error, duration: 4000 });
          pending.resolve(null);
          return;
        }
        try {
          const translated = translateProject(msg.response.project, msg.source, layoutCode);
          registerCatalog(msg.response.catalog as unknown as Record<string, CatalogEntry>);
          recomputeSourceFlags(msg.response.project);
          pending.resolve({ project: translated, weftCode: msg.source });
        } catch (e) {
          const errMsg = e instanceof Error ? e.message : String(e);
          console.error('translateProject failed on sourceResynced:', errMsg);
          toast.warning('Resync failed', { description: errMsg, duration: 4000 });
          pending.resolve(null);
        }
        return;
      }
      if (msg.kind === 'codeEditTouched') {
        // An external change landed on the watched doc: slide the editor's
        // auto-lock forward (source-mutating graph gestures pause for 1s).
        editorRef?.setCodeEditTouched?.();
        return;
      }
      if (msg.kind === 'setGraphLogicLock') {
        editorRef?.setGraphLogicLock?.(msg.locked, msg.reason);
        return;
      }
      if (msg.kind === 'catalogAll') {
        setCatalog(msg.catalog as unknown as Record<string, CatalogEntry>);
        return;
      }
      if (msg.kind === 'navState') {
        navDepth = msg.depth;
        navFileName = msg.fileName;
        execPrefix = msg.execPrefix;
        return;
      }
      if (msg.kind === 'fileContents') {
        // File-backed field display values. Resent when a backing file
        // changes externally, so file -> graph stays live with no reparse.
        fileContents = msg.contents;
        return;
      }
      if (msg.kind === 'parseResult') {
        registerCatalog(msg.response.catalog as unknown as Record<string, CatalogEntry>);
        // A navigation file-swap (freshMount) rebuilds the view from scratch,
        // same as the very first mount, so it takes the rebuild path (and
        // auto-organizes when the file has no layout) rather than the
        // in-place edit-reconciliation path.
        const freshMount = project === null || msg.freshMount === true;
        layoutCode = msg.layoutCode;
        // host-bridge's `translateProject` throws on an unknown container
        // kind (a wire-shape drift the user wants surfaced, not
        // silently dropped). Catch it and show the inline `error`
        // banner (the same one parse failures use) rather than freezing
        // the view on the previous project. (This is the inline banner
        // only; it does not also push to the action-bar error slot.)
        let translated;
        try {
          translated = translateProject(msg.response.project, msg.source, msg.layoutCode);
        } catch (e) {
          const errMsg = e instanceof Error ? e.message : String(e);
          console.error('translateProject failed:', errMsg);
          error = `Project translation failed: ${errMsg}`;
          return;
        }
        if (freshMount) {
          // No saved layout (fresh file, or layout absent): auto-organize on
          // mount so the graph isn't a pile at the origin.
          autoOrganizeOnMount = msg.layoutCode.trim() === '';
          viewGeneration += 1;
          project = translated;
        } else if (editorRef) {
          editorRef.applyExternalSource?.(translated, msg.source, msg.layoutCode);
        }
        recomputeSourceFlags(msg.response.project);
        error = null;
        return;
      }
      if (msg.kind === 'parseError') {
        error = msg.error;
        return;
      }
      if (msg.kind === 'catalogError') {
        catalogError = msg.error ?? null;
        catalogWarnings = msg.warnings ?? [];
        return;
      }
      if (msg.kind === 'execReset') {
        executionState = {
          isRunning: true,
          nodeOutputs: {},
          nodeExecutions: {},
          busLogByBus: {},
          busMetaByBus: {},
          busParticipantsByBus: {},
          journalCorruptions: [],
          loopEventsByGroup: {},
        };
        seenBusKeys = new Set();
        seenLoopKeys = new Set();
        return;
      }
      if (msg.kind === 'execTerminal') {
        // The dispatcher reached ExecutionCompleted / ExecutionFailed.
        // Whatever the per-node tally says, the run is over: hide
        // the Stop button. Close EVERY non-terminal row (in case a
        // NodeCompleted slipped through SSE, a parallel-loop iteration
        // left several rows open, or a node was still
        // `waiting_for_input` when the run was cancelled): only
        // completed/failed/skipped/cancelled are terminal, so anything
        // else (running, waiting_for_input) must be force-closed to the
        // execution's terminal state, not just the last `running` row.
        executionState.isRunning = false;
        const now = Date.now();
        const isTerminal = (s: NodeExecution['status']) =>
          s === 'completed' || s === 'failed' || s === 'skipped' || s === 'cancelled';
        const rows = { ...executionState.nodeExecutions };
        for (const [nodeId, history] of Object.entries(rows)) {
          if (history.some((r) => !isTerminal(r.status))) {
            rows[nodeId] = history.map((r) =>
              isTerminal(r.status) ? r : { ...r, status: msg.state, completedAt: now },
            );
          }
        }
        executionState.nodeExecutions = rows;
        return;
      }
      if (msg.kind === 'execEvent') {
        const e = msg.event;
        const state = e.state;
        // One execution row per (nodeId, framesKey). The dispatcher
        // produces state events (running/waiting_for_input/completed/
        // failed/skipped/cancelled) on the same record; we mutate
        // the existing row, not append. A fresh dispatch after a
        // terminal row goes into the same row's history (one pulse
        // per (node, frames)).
        const now = Date.now();
        const rows = executionState.nodeExecutions[e.nodeId] ?? [];
        // Key the record by frame stack: each firing
        // at a distinct frame stack gets its own card so parallel fan-outs
        // don't cross-correlate. `lane` is a structured array;
        // serialize for the stable string identity the render layer
        // needs.
        const framesKey = JSON.stringify(e.frames);
        const idx = rows.findIndex((r) => r.framesKey === framesKey);
        let nextRows: NodeExecution[];
        if (idx < 0) {
          // First event for this (node, frames). Open the record.
          nextRows = [
            ...rows,
            {
              id: `${e.nodeId}-${framesKey}-${now}`,
              nodeId: e.nodeId,
              status: state as NodeExecution['status'],
              pulseIdsAbsorbed: [],
              pulseId: `${e.nodeId}-${framesKey}-${now}`,
              startedAt: now,
              completedAt:
                state === 'completed' || state === 'failed' || state === 'skipped' || state === 'cancelled'
                  ? now
                  : undefined,
              error: e.error,
              costUsd: 0,
              logs: [],
              color: '',
              frames: e.frames,
              framesKey,
              input: e.input,
              closedPorts: e.closedPorts,
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
            // closedPorts may arrive on node_started (state=running) or
            // node_skipped (state=skipped); always refresh from the event
            // if present so the inspector shows the per-port (closed)
            // labels for skipped firings as well as running ones.
            if (e.closedPorts !== undefined) {
              updated.closedPorts = e.closedPorts;
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
      if (msg.kind === 'busEvent') {
        // Append the event to the bus's full log. Participation (which
        // node panels render this bus) is fed by a SEPARATE
        // `busParticipant` channel; bus events themselves carry only
        // bus-layer state.
        const busId = msg.event.busId;
        // Dedup on (busId, offset): the replay snapshot and the live
        // stream overlap during a replay follow, so the same offset can
        // arrive twice. Offsets are unique per bus, so this is exact.
        const busKey = `${busId}:${msg.event.offset}`;
        if (seenBusKeys.has(busKey)) return;
        seenBusKeys.add(busKey);
        const log = executionState.busLogByBus[busId] ?? [];
        executionState.busLogByBus = {
          ...executionState.busLogByBus,
          [busId]: [...log, msg.event],
        };
        return;
      }
      if (msg.kind === 'loopEvent') {
        const gid = msg.event.groupId;
        // Dedup on (groupId, kind, parentFrames, index): loop events
        // have no single offset, but this tuple uniquely identifies each
        // one (instantiated/terminated have no index; the JSON of
        // parentFrames disambiguates nested + parallel iterations). Same
        // replay/live overlap as bus events.
        const idx = 'index' in msg.event ? msg.event.index : '';
        const loopKey = `${gid}:${msg.event.kind}:${JSON.stringify(msg.event.parentFrames)}:${idx}`;
        if (seenLoopKeys.has(loopKey)) return;
        seenLoopKeys.add(loopKey);
        const log = executionState.loopEventsByGroup[gid] ?? [];
        executionState.loopEventsByGroup = {
          ...executionState.loopEventsByGroup,
          [gid]: [...log, msg.event],
        };
        return;
      }
      if (msg.kind === 'journalCorruption') {
        // One-shot at replay: append. The inspector aggregates these
        // into a muted "N journal rows corrupted" line per execution.
        // Not alarming; the user only sees it if they look.
        executionState.journalCorruptions = [
          ...executionState.journalCorruptions,
          { site: msg.site, reason: msg.reason },
        ];
        return;
      }
      if (msg.kind === 'busParticipant') {
        // Add the (busId, nodeId) edge if new. Idempotent: PulseEmitted
        // fires once per source/target on a per-pulse basis, so we may
        // see the same edge multiple times when a producer emits
        // several values on the same bus port. The first edge for a
        // bus also seeds `busMetaByBus[busId]` from the dispatcher-
        // derived mode, so the inspector renders the panel header
        // badge from the same source as participation.
        const set = executionState.busParticipantsByBus[msg.busId] ?? new Set<string>();
        if (!set.has(msg.nodeId)) {
          const next = new Set(set);
          next.add(msg.nodeId);
          executionState.busParticipantsByBus = {
            ...executionState.busParticipantsByBus,
            [msg.busId]: next,
          };
        }
        // Bus mode is immutable per bus (the marker carries it from
        // creation). Pin the first-seen meta: divergence is a
        // dispatcher / wire bug, so accepting "latest wins" would
        // silently mutate a known-good state with a known-bad one.
        // Log loud so the bug is visible in the dev tools without
        // corrupting the UI.
        const prevMeta = executionState.busMetaByBus[msg.busId];
        if (prevMeta) {
          if (prevMeta.ephemeral !== msg.meta.ephemeral) {
            console.warn(
              `bus meta diverged for ${msg.busId}: pinned ephemeral=${prevMeta.ephemeral}, incoming ephemeral=${msg.meta.ephemeral} (kept the first-seen value; the bus marker is the authoritative shape)`,
            );
          }
        } else {
          executionState.busMetaByBus = {
            ...executionState.busMetaByBus,
            [msg.busId]: msg.meta,
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
      if (msg.kind === 'followLost') {
        // The live SSE link to this execution ended or broke. Stop
        // presenting the run as live (hide the Stop button) WITHOUT
        // falsely marking nodes completed: the per-node rows keep
        // their last known state, we just aren't following anymore.
        // (execTerminal is the run actually finishing; this is the
        // link dying with the run possibly still in flight.)
        executionState.isRunning = false;
        toast.warning(
          msg.reason === 'error'
            ? 'Lost the live connection to this execution. Re-open it to reconnect.'
            : 'The live execution stream ended. Re-open the execution to reconnect.',
        );
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
    layoutCode?: string;
    fileRef?: { path: string; content: string };
  }) {
    if (data.layoutCode !== undefined && data.layoutCode !== layoutCode) {
      layoutCode = data.layoutCode;
      send({ kind: 'saveLayout', layoutCode: data.layoutCode });
    }
    if (data.fileRef !== undefined) {
      // File-backed config field edit: the content goes to the
      // referenced file, not the `@file(...)` token in the source.
      send({ kind: 'saveFileRef', path: data.fileRef.path, content: data.fileRef.content });
    }
  }

  /// A graph (GUI) edit: send the intents to the host (Rust edit-server
  /// applies + writes the source). Resolves with the inverse text edit (this
  /// action's undo) plus the post-edit truth; rejects with the host's reason.
  function onApplyEdits(ops: EditOp[]): Promise<EditRpcResult> {
    return requestEdit((requestId) => send({ kind: 'applyEdits', ops, requestId }));
  }

  /// Replay a raw source text edit (undo/redo of a source action). Same reply
  /// shape (the inverse undoes THIS replay, so undo<->redo round-trips).
  function onApplyTextEdit(edit: TextEdit): Promise<EditRpcResult> {
    return requestEdit((requestId) => send({ kind: 'applyTextEdit', edit, requestId }));
  }

  /// Fetch the host's current truth after a rejected edit.
  function onResyncSource(): Promise<{ project: V1Project; weftCode: string } | null> {
    return requestResync();
  }

  function onOpenInclude(path: string, alias: string) {
    // Flush pending config-edit saves BEFORE navigating: this queues saveWeft
    // for the current (parent) doc ahead of openInclude, so the parent's edits
    // are persisted while it's still the watched doc. (Flushing on the editor's
    // destroy would fire too late, after the host swapped the watched doc.)
    editorRef?.flushAllPendingSaves?.();
    send({ kind: 'openInclude', path, alias });
  }

  function onNavigateBack() {
    editorRef?.flushAllPendingSaves?.();
    send({ kind: 'navigateBack' });
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
  function onInfraNodeStop(nodeId: string) {
    send({ kind: 'infraNodeStop', nodeId });
  }
  function onInfraNodeTerminate(nodeId: string) {
    send({ kind: 'infraNodeTerminate', nodeId });
  }
  function onUpgradeInfra() {
    editorRef?.flushAllPendingSaves?.();
    send({ kind: 'infraUpgrade' });
  }
</script>

<div class="absolute inset-0">
  <!-- Catalog feedback: a non-blocking banner. When the graph renders,
       it floats over the top edge (z-10) so it doesn't push layout.
       When a parse error has taken over the view, it stacks ABOVE the
       parse-error text (normal flow) so the two error surfaces don't
       overlap. Independent of the parse `error` state either way. -->
  {#if catalogError || catalogWarnings.length > 0}
    <div class="{error ? 'relative' : 'absolute top-0 inset-x-0 z-10'} p-2 text-xs">
      {#if catalogError}
        <div class="text-destructive">node catalog: {catalogError}</div>
      {/if}
      {#each catalogWarnings as w}
        <div class="text-yellow-600">node catalog: {w}</div>
      {/each}
    </div>
  {/if}
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
      {navDepth}
      {navFileName}
      {onNavigateBack}
    />
    {#key viewGeneration}
    <ProjectEditor
      bind:this={editorRef}
      {project}
      {onSave}
      {onApplyEdits}
      {onApplyTextEdit}
      {onResyncSource}
      {onOpenInclude}
      {execPrefix}
      {fileContents}
      {autoOrganizeOnMount}
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
      {onInfraNodeStop}
      {onInfraNodeTerminate}
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
    {/key}
  {:else}
    <div class="p-4 text-muted-foreground">loading graph...</div>
  {/if}
</div>
