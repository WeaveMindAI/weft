<script lang="ts">
  import { onMount } from 'svelte';
  import { LoaderCircle } from '@lucide/svelte';
  import { toast } from 'svelte-sonner';
  import ProjectEditor from './lib/components/project/ProjectEditor.svelte';
  import { MessagesSquare } from '@lucide/svelte';
  import GraphToolbar from './lib/components/project/GraphToolbar.svelte';
  import CallerPanel from './lib/components/project/CallerPanel.svelte';
  import { send, onMessage, teardownTransport, hostRequest, cancelPendingHostRequests, HostRequestCancelled } from './host';
  import { registerCatalog, setCatalog } from './lib/nodes';
  import { translateProject } from './host-bridge';
  import { projectHasInfra, projectHasTriggers } from './lib/utils/node-roles';
  import type { ProjectDefinition as V1Project, NodeFiring, RunState } from './lib/types';
  import { bareRecord } from './lib/types';
  import { runValues } from './lib/utils/run-values.svelte';
  import type { ActionBarState, CatalogWarning, ActionAvailability, LifecycleAction, NodeFeedState, TextEdit, EditOp, FileContent, Diagnostic, FollowMode, ProjectDefinition as ProtocolProject, HostMessage, RunCheck, WebviewMessage } from '../protocol';
  import type { EditRpcResult } from './lib/projection/types';
  import { idleActionBarState } from '../status';
  import { credentialOwnerKind } from '../protocol';
  import type { Snippet } from 'svelte';
  import type { EditorContext } from './editor-context';

  // Panel SLOTS. The editor renders the canvas + toolbar; a consumer (a
  // browser-based host) injects its own side panels into these slots, while
  // another (the VS Code extension) injects none and uses its native chrome
  // instead (a file tab, its own right sidebar). Each slot is handed the FULL `EditorContext` (all
  // live editor state + the `send`/`onMessage` channel to the host), so a panel
  // can do anything a first-class part of the editor could; the editor never
  // curates what a panel may see. Absent snippet = empty region (VS Code).
  let {
    leftPanel,
    rightPanel,
    toolbarLeading,
    toolbarTrailing,
  }: {
    leftPanel?: Snippet<[EditorContext]>;
    rightPanel?: Snippet<[EditorContext]>;
    // Host-injected controls at the START of the editor's floating toolbar
    // (before Live), handed the full EditorContext. A web host can put
    // a "back to projects" button here so it sits inline with the editor's own
    // buttons; VS Code injects nothing. Absent = nothing rendered.
    toolbarLeading?: Snippet<[EditorContext]>;
    // Host-injected controls at the END of the toolbar (after the editor's
    // buttons), handed the full EditorContext. Symmetric with toolbarLeading.
    toolbarTrailing?: Snippet<[EditorContext]>;
  } = $props();

  let project: V1Project | null = $state(null);
  let error: string | null = $state(null);
  // Structured parse/enrich diagnostics (line + column) for the active file,
  // from the latest parseResult. Exposed on the context so a code panel paints
  // inline squiggles. Cleared to [] when a fresh parse has no diagnostics.
  let diagnostics: Diagnostic[] = $state([]);
  // Catalog feedback, independent of the parse banner: a full-load
  // failure (catalogError.error) or per-node soft warnings. Lives on
  // its own so a successful parse can't erase a live catalog problem.
  let catalogError: string | null = $state(null);
  let catalogWarnings: CatalogWarning[] = $state([]);
  // The in-canvas problems pill: parse + catalog problems folded into
  // one surface, each entry a short collapsed label plus the full text
  // for the expanded view.
  const problems = $derived([
    ...(error ? [{ label: 'source error', text: error, severity: 'error' as const }] : []),
    ...(catalogError
      ? [{ label: 'node catalog error', text: catalogError, severity: 'error' as const }]
      : []),
    ...catalogWarnings.map((w) => ({
      label: w.kind === 'spelling' ? 'node spelling warning' : 'node catalog warning',
      text: w.message,
      severity: 'warning' as const,
    })),
  ]);
  let problemsExpanded = $state(false);
  // Most catalog/metadata failures are a stale per-project node copy;
  // the fix is the same every time, so the hint travels with every
  // problem surface.
  const CATALOG_HINT =
    'Stale node copies cause most catalog/metadata errors: run `weft catalog update` in the project, then save again.';
  // The one notice a dropped live link keeps up while the host reconnects.
  const FOLLOW_RECONNECTING_TOAST = 'follow-reconnecting';
  let layoutCode = $state('');
  // RPC for source edits: applyEdits/applyTextEdit await the host's
  // `editApplied` reply through the ONE shared request registry
  // (`hostRequest`, the same correlator the storage/access/asset calls
  // ride). Success carries the inverse text edit (the editor's undo) PLUS
  // the post-edit truth, translated here at the wire boundary so the editor
  // stays in its own project shape. A refusal REJECTS with the host's
  // reason: resolve-vs-reject is the success/failure channel, so a refused
  // edit always rolls the editor's optimistic op back. A CANCELLED request
  // (the view switched away mid-round-trip; `cancelPendingHostRequests`
  // swept it) resolves with the dedicated `cancelled` outcome: the engine
  // stands down quietly, without confirming, persisting, or rolling back
  // (this dying view's layout must never reach the newly watched file).
  async function requestEdit(make: (requestId: number) => WebviewMessage): Promise<EditRpcResult> {
    let reply: Extract<HostMessage, { kind: 'editApplied' }>;
    try {
      reply = await hostRequest('editApplied', make);
    } catch (e) {
      if (e instanceof HostRequestCancelled) return { inverse: null, project: null, weftCode: '', cancelled: true };
      throw e;
    }
    if (!reply.ok) throw new Error(reply.reason);
    // No truth payload: the host applied the edit but the user switched
    // `.weft` tabs mid-round-trip. Resolve the inverse for undo bookkeeping
    // WITHOUT advancing truth (the new doc's parseResult is its truth).
    if (reply.response === undefined || reply.source === undefined) {
      return { inverse: reply.inverse ?? null, project: null, weftCode: '' };
    }
    try {
      // Translate FIRST: only advance catalog + bar flags once the new truth
      // actually renders, so a failure can't leave them ahead of the
      // displayed project.
      const translated = translateProject(reply.response.project, reply.source, layoutCode);
      registerCatalog(reply.response.catalog);
      recomputeSourceFlags(reply.response.project);
      // Advance the active source so a code-view slot bound to
      // `ctx.activeSource` reflects the edit LIVE.
      activeSource = reply.source;
      error = null;
      releaseTranslationLock();
      return { inverse: reply.inverse ?? null, project: translated, weftCode: reply.source };
    } catch (e) {
      // The edit IS on disk; this is not a refused edit, it's a truth the
      // webview cannot render (wire-shape drift). Do NOT roll back (that
      // would diverge the editor from disk). Lock source-mutating gestures
      // (every further edit would be computed against a stale picture and
      // bounce off the host in a rejection loop) until a later parse
      // translates cleanly, and resolve the undo inverse with no truth.
      engageTranslationLock(e);
      return { inverse: reply.inverse ?? null, project: null, weftCode: '' };
    }
  }
  // RPC for post-rejection resyncs: resolves the host's current truth, or
  // null when the source doesn't parse right now (the editor keeps its
  // previous truth until the parse path delivers a fresh one) or the view
  // switched away mid-flight (the request was swept; the dying editor keeps
  // its truth and settles).
  async function requestResync(): Promise<{ project: V1Project; weftCode: string } | null> {
    let reply: Extract<HostMessage, { kind: 'sourceResynced' }>;
    try {
      reply = await hostRequest('sourceResynced', (requestId) => ({ kind: 'resyncSource', requestId }));
    } catch (e) {
      if (e instanceof HostRequestCancelled) return null;
      throw e;
    }
    if (!reply.ok) {
      // The current source doesn't parse (user mid-edit in the text tab).
      // The editor keeps its previous truth; surface why.
      toast.warning('Source has errors', { description: reply.error, duration: 4000 });
      return null;
    }
    try {
      const translated = translateProject(reply.response.project, reply.source, layoutCode);
      registerCatalog(reply.response.catalog);
      recomputeSourceFlags(reply.response.project);
      activeSource = reply.source;
      releaseTranslationLock();
      return { project: translated, weftCode: reply.source };
    } catch (e) {
      const errMsg = e instanceof Error ? e.message : String(e);
      console.error('translateProject failed on sourceResynced:', errMsg);
      toast.warning('Resync failed', { description: errMsg, duration: 4000 });
      engageTranslationLock(e);
      return null;
    }
  }

  // Translation-failure lock: when a reply carries truth the webview cannot
  // render, the graph on screen is known-stale, so source-mutating gestures
  // are locked through the engine's existing graph-logic lock (preflight
  // then refuses them with this reason) instead of letting the user edit on
  // top of a picture the host disagrees with. Released by the next reply or
  // parse that translates cleanly. Tracked locally so releasing never
  // clobbers a lock someone ELSE set (the AI-streaming lock).
  let translationLocked = false;
  function engageTranslationLock(e: unknown): void {
    const errMsg = e instanceof Error ? e.message : String(e);
    console.error('translateProject failed:', errMsg);
    error = `Project translation failed: ${errMsg}`;
    translationLocked = true;
    // No editorRef = no editor exists yet (a first-mount failure): the
    // blocked-first-render state shows instead of an editor, so there
    // is no gesture surface to lock; the flag alone carries the state.
    editorRef?.setGraphLogicLock?.(true, `the last edit could not be rendered (${errMsg}); fix the source in the code tab`);
  }
  function releaseTranslationLock(): void {
    if (!translationLocked) return;
    translationLocked = false;
    editorRef?.setGraphLogicLock?.(false);
  }
  // Include-navigation back-stack state, driven by the host's `navState`.
  let navDepth = $state(0);
  let navFileName = $state('');
  let callPath = $state<string[]>([]);
  // Whether this view is a place in the program (see `navState`). A file
  // opened on its own is drawn and nothing else: no runs, no trigger
  // displays, no action bar, no aiming a run at its nodes. The host that
  // owns include navigation sends it; a host without one has only entry
  // files, which is what the default says.
  let interactive = $state(true);
  /// The installs the project can be shown on (the switch, top right).
  let installView = $state<import('../protocol').InstallView | undefined>(undefined);
  // The raw source text of the CURRENTLY ACTIVE file (the entry file at depth 0,
  // or the included sub-file after navigation). Set from every `parseResult`'s
  // `source`, so it tracks include navigation. Exposed on `EditorContext` for
  // panel slots (e.g. a host's code view) that render the active source.
  let activeSource = $state('');
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

  // Live run state fed by the host's run follower.
  // nodeOutputs snapshots the last-observed output per node;
  // nodeFirings tracks the rolling history per node.
  //
  // busLogByBus is the source of truth for bus conversations: every
  // bus event ever observed (live or replay), keyed by busId. The
  // inspector for a given node N renders one IRC panel per bus in
  // `busParticipantsByBus` whose participant set contains N.
  // `busMetaByBus` carries per-bus header metadata (mode) seeded
  // from the first BusParticipant edge the dispatcher derives from
  // the bus marker JSON.
  /// A canvas with no run painted on it. `isRunning` is true while a
  /// run is about to be replayed onto it (`runReset`), false when
  /// there is simply nothing to show (startup, `runCleared`).
  function emptyRunState(isRunning: boolean): RunState {
    return {
      isRunning,
      scope: undefined,
      seed: undefined,
      version: undefined,
      tags: [],
      nodeOutputs: bareRecord(),
      nodeFirings: bareRecord(),
      busLogByBus: bareRecord(),
      busMetaByBus: bareRecord(),
      busParticipantsByBus: bareRecord(),
      journalCorruptions: [],
      loopEventsByGroup: {},
      scopeDecisions: bareRecord(),
      callerLog: [],
    };
  }
  let runState = $state<RunState>(emptyRunState(false));
  // The run this view paints: the followed one, or nothing when the view
  // is no place (a run's rows belong to a call, and a file opened on its
  // own names none). ONE gate, here, so the editor, the toolbar's follow
  // controls, the caller panel and the slot context all read the same
  // answer instead of each checking for itself.
  const shownRunState = $derived(interactive ? runState : emptyRunState(false));

  // Dedup keys for append-only inspector logs. A host may hand the same
  // event twice (one that reads a run's history beside its live stream,
  // where the two overlap). Node firings are idempotent (keyed
  // by (nodeId, framesKey), mutated in place), but bus and loop logs
  // are append-only, so without dedup the same chat line / loop
  // iteration would render twice. Bus events carry a per-bus `offset`;
  // loop events are keyed by (groupId, kind, parentFrames, index).
  // Reset on runReset (a fresh follow). Kept OUTSIDE `runState`
  // ($state) since they're pure bookkeeping, not rendered.
  let seenBusKeys = new Set<string>();
  let seenLoopKeys = new Set<string>();
  let seenCallerOffsets = new Set<string>();

  // What each node is showing, keyed by node id. An infra node's
  // container and the listener kind holding a signal (a trigger's, or
  // a wait's in the followed run) all serve the same shape, so there is
  // one map for all of them.
  //
  //   - a node with a display (infra serving /live, a trigger, or a
  //     node waiting in the followed run) → displayByNode
  //   - debug nodes (features.showDebugPreview)
  //     → runState.nodeOutputs
  //   - everything else → no body panel; modal inspector only.
  //
  // Each entry is `NodeFeedState`: ok with items, absent, or error
  // with a message. NEVER a fallback to run data.
  let displayByNode = $state<Record<string, NodeFeedState>>(bareRecord());

  // Source-derived flags: does the project DECLARE infra / trigger
  // nodes. Driven by every truth carrier (parseResult, editApplied,
  // sourceResynced), not by backend state. Used to gate visibility of
  // bar sections (don't show the Infra section for a project with no
  // infra nodes in source). Recomputed on every truth so the bar
  // follows graph edits too, e.g. dropping in the first infra node.
  //
  // Counted through `@include`s: this parse is interface mode, so an
  // included file is one opaque node and its body is absent, and a
  // project whose only trigger (or only infra node) lives in one would
  // read as having neither. The include node carries what its file
  // holds, and the project predicates fold that in.
  let hasInfraInGraph = $state(false);
  let hasTriggersInGraph = $state(false);
  function recomputeSourceFlags(project: ProtocolProject): void {
    hasInfraInGraph = projectHasInfra(project.nodes);
    hasTriggersInGraph = projectHasTriggers(project.nodes);
  }

  // Auto-follow state. The host-side controller owns the actual
  // decisions; we just render the badge and forward clicks.
  let followMode = $state<FollowMode>('following');
  let followRunId = $state<string | undefined>(undefined);
  let followPendingCount = $state(0);
  /// Why the followed run cannot be painted whole, when it cannot: the
  /// code it ran is no longer recorded, so the rows are all there and
  /// every value is missing. It goes beside the run pill, because a
  /// canvas with nothing on it and no reason for it reads as a broken
  /// viewer and sends people looking for a bug that is not there.
  const runNotPainted = $derived(
    runState.journalCorruptions.find((c) => c.site === 'MissingProgram')?.reason,
  );
  /// Whether the caller panel is showing. Closed on every load: it is a
  /// thing you go and look at, not a thing that takes up the canvas
  /// until you dismiss it.
  let callerPanelOpen = $state(false);

  // Action-bar state: single source of truth for what the bar
  // renders. The host's ActionBarStore pushes every transition;
  // the webview is a pure renderer that reads from this store.
  // `backend` always present, `overlay` carries the user-action
  // layer, `error` sticky banner.
  let actionBarState = $state<ActionBarState>(idleActionBarState());

  // Latest /status snapshot: every trigger's, infra copy's and
  // instance's state. Drives the node badges, the contextual bar's
  // counts and the bottom bar's in-flight states. Stays current across
  // cli_running so the badges don't blink mid-verb.
  let statusSnapshot = $state<ActionAvailability | undefined>(undefined);

  onMount(() => {
    // Bubble-up listener for the buttons a node's display items
    // carry (an infra container's "disconnect phone", say).
    // ProjectNode dispatches a `weft-display-action` CustomEvent; we
    // forward it to the host, which posts to the container's own
    // door at /projects/{id}/infra/nodes/{place}/action. A
    // trigger's display is read-only and carries no button.
    const onDisplayAction = (e: Event) => {
      const ce = e as CustomEvent<{ nodeId: string; actionKind: string; payload?: unknown; confirm?: string }>;
      const detail = ce.detail;
      if (!detail || typeof detail.nodeId !== 'string' || typeof detail.actionKind !== 'string') return;
      send({
        kind: 'displayAction',
        nodeId: detail.nodeId,
        actionKind: detail.actionKind,
        payload: detail.payload,
        confirm: detail.confirm,
      });
    };
    window.addEventListener('weft-display-action', onDisplayAction as EventListener);
    // The mouse's side buttons. Back (button 3) steps out of the
    // included file this view descended into; forward (button 4) has
    // nothing to redo, so it does nothing. Both are swallowed: left to
    // the browser they navigate the webview's own frame away from the
    // graph.
    const onSideButton = (e: MouseEvent) => {
      if (e.button !== 3 && e.button !== 4) return;
      e.preventDefault();
      e.stopPropagation();
      if (e.type === 'mouseup' && e.button === 3 && navDepth > 0) onNavigateBack();
    };
    window.addEventListener('mousedown', onSideButton, true);
    window.addEventListener('mouseup', onSideButton, true);
    const unsub = onMessage((msg) => {
      // editApplied and sourceResynced replies are consumed by the shared
      // hostRequest correlator (see requestEdit/requestResync above), not
      // handled here.
      if (msg.kind === 'codeEditTouched') {
        // An external change landed on the watched doc: slide the editor's
        // auto-lock forward (source-mutating graph gestures pause for 1s).
        editorRef?.setCodeEditTouched?.();
        return;
      }
      if (msg.kind === 'installView') {
        installView = msg.view;
        return;
      }
      if (msg.kind === 'setGraphLogicLock') {
        editorRef?.setGraphLogicLock?.(msg.locked, msg.reason);
        return;
      }
      if (msg.kind === 'catalogAll') {
        setCatalog(msg.catalog);
        return;
      }
      if (msg.kind === 'navState') {
        navDepth = msg.depth;
        navFileName = msg.fileName;
        callPath = msg.callPath;
        interactive = msg.interactive;
        return;
      }
      if (msg.kind === 'fileContents') {
        // File-backed field display values. MERGED (not replaced) so progressive
        // lazy-load updates work: the host first sends `{loading}` for a `@file`
        // target, then `{content}` once its bytes arrive, touching only that path
        // and leaving the others' states intact. Also resent when a backing file
        // changes externally, so file -> graph stays live with no reparse.
        fileContents = { ...fileContents, ...msg.contents };
        return;
      }
      if (msg.kind === 'parseResult') {
        registerCatalog(msg.response.catalog);
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
        // Track the active file's source for the panel slots (the code view).
        // Updated here so it follows include navigation (each nav sends a fresh
        // parseResult with the included file's source).
        activeSource = msg.source;
        let translated;
        try {
          translated = translateProject(msg.response.project, msg.source, msg.layoutCode);
        } catch (e) {
          engageTranslationLock(e);
          return;
        }
        if (freshMount) {
          // The view is being rebuilt from scratch: any request still in
          // flight belongs to the editor instance about to be destroyed.
          // Sweep them so its engine chain settles (a promise left pending
          // forever wedges it) and no late reply lands in the new view.
          cancelPendingHostRequests('the view changed');
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
        releaseTranslationLock();
        // Surface the structured diagnostics (line + column) so the code panel
        // paints inline squiggles. A clean parse resets them to [].
        diagnostics = msg.response.diagnostics ?? [];
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
      if (msg.kind === 'runReset' || msg.kind === 'runCleared') {
        runState = emptyRunState(msg.kind === 'runReset');
        // The values the canvas shows are read from the run it follows.
        runValues.follow(msg.kind === 'runReset' ? msg.runId : undefined);
        seenBusKeys = new Set();
        seenLoopKeys = new Set();
        seenCallerOffsets = new Set();
        return;
      }
      if (msg.kind === 'runScope') {
        // The run's birth: what it is held to, and what it inherits.
        runState.scope = msg.subgraph;
        runState.seed = msg.seed;
        return;
      }
      if (msg.kind === 'runVersion') {
        if (followRunId && msg.runId !== followRunId) return;
        // Both null means there is no followed run here any more (the
        // graph moved to another project), so the banner goes rather
        // than reading as "this run matches disk".
        runState.version =
          msg.version === null && msg.diskVersion === null
            ? undefined
            : { version: msg.version, diskVersion: msg.diskVersion };
        return;
      }
      if (msg.kind === 'runTerminal') {
        // The dispatcher reached RunCompleted / RunFailed.
        // Whatever the per-node tally says, the run is over: hide
        // the Stop button. Close EVERY non-terminal row (in case a
        // NodeCompleted slipped through SSE, a parallel-loop iteration
        // left several rows open, or a node was still
        // `waiting_for_input` when the run was cancelled): only
        // completed/failed/skipped/cancelled are terminal, so anything
        // else (running, waiting_for_input) must be force-closed to the
        // run's terminal state, not just the last `running` row.
        runState.isRunning = false;
        // Keep how it ended: a cancel's cause is what tells a run a
        // sibling stopped (through `ctx.stop_tagged`) apart from one a
        // person stopped, and the inspector shows it on the run.
        runState.terminal = { state: msg.state, reason: msg.reason, cause: msg.cause };
        // The journal's stamp, so a replayed run closes its rows at the
        // time it really ended, not at the time it was opened. A row
        // journaled after the terminal (a node that started in the same
        // second the cancel landed) closes at its own start, never
        // before it.
        const now = msg.atUnix * 1000;
        const isTerminal = (s: NodeFiring['status']) =>
          s === 'completed' || s === 'failed' || s === 'skipped' || s === 'cancelled';
        const rows = bareRecord(runState.nodeFirings);
        for (const [nodeId, history] of Object.entries(rows)) {
          if (history.some((r) => !isTerminal(r.status))) {
            rows[nodeId] = history.map((r) =>
              isTerminal(r.status)
                ? r
                : { ...r, status: msg.state, completedAt: Math.max(now, r.startedAt) },
            );
          }
        }
        runState.nodeFirings = rows;
        return;
      }
      if (msg.kind === 'runCost') {
        // One metered call's cost, added onto the matching firing's total.
        // Dedup by the record's stable identity: the dispatcher re-streams
        // journal events on every follow/reconnect (replay + live overlap),
        // so the same record can arrive twice. An unknown amount (null)
        // sets the firing's costUnknown flag so it renders as "unknown",
        // never disappearing into a $0-looking total.
        const framesKey = JSON.stringify(msg.frames);
        const rows = runState.nodeFirings[msg.nodeId];
        const idx = rows?.findIndex((r) => r.framesKey === framesKey) ?? -1;
        if (!rows || idx < 0) {
          // The firing's row must already exist (node_started precedes any
          // cost record on both the live and replay paths). A miss means an
          // ordering/keying regression; surface it instead of losing money
          // display silently.
          console.warn('[weft] cost record for an unknown firing', msg);
          return;
        }
        const seen = rows[idx].costIds ?? [];
        if (seen.includes(msg.costId)) {
          return;
        }
        runState.nodeFirings = bareRecord(runState.nodeFirings, {
          [msg.nodeId]: rows.map((r, i) =>
            i === idx
              ? {
                  ...r,
                  costIds: [...seen, msg.costId],
                  // A cost this run INHERITED was paid by the run it was
                  // seeded from: real money, already spent, and adding it
                  // to this run's own total counts it twice. It lands in
                  // the reused column the group rows already use.
                  costUsd: r.costUsd + (msg.inheritedFrom ? 0 : msg.amountUsd ?? 0),
                  costUnknown:
                    (r.costUnknown ?? false) ||
                    (!msg.inheritedFrom && msg.amountUsd === null),
                  inheritedCostUsd:
                    (r.inheritedCostUsd ?? 0) +
                    (msg.inheritedFrom ? msg.amountUsd ?? 0 : 0),
                  inheritedCostUnknown:
                    (r.inheritedCostUnknown ?? false) ||
                    (!!msg.inheritedFrom && msg.amountUsd === null),
                  credentialOwner:
                    r.credentialOwner === undefined || r.credentialOwner === credentialOwnerKind(msg.origin)
                      ? credentialOwnerKind(msg.origin)
                      : 'mixed',
                }
              : r,
          ),
        });
        return;
      }
      if (msg.kind === 'execFileEdit') {
        // One change a firing made to a stored file, appended to that
        // firing's row for the inspector's "Files edited" card. Dedup on
        // the file's key and the version the change produced: the same
        // journal row arrives twice when replay and live overlap.
        const framesKey = JSON.stringify(msg.frames);
        const rows = runState.nodeFirings[msg.nodeId];
        const idx = rows?.findIndex((r) => r.framesKey === framesKey) ?? -1;
        if (!rows || idx < 0) {
          // node_started precedes every edit on both paths: a miss is an
          // ordering or keying regression, surfaced instead of hidden.
          console.warn('[weft] file edit for an unknown firing', msg);
          return;
        }
        const seen = rows[idx].fileEdits ?? [];
        if (seen.some((f) => f.key === msg.edit.key && f.toVersion === msg.edit.toVersion)) {
          return;
        }
        runState.nodeFirings = bareRecord(runState.nodeFirings, {
          [msg.nodeId]: rows.map((r, i) => (i === idx ? { ...r, fileEdits: [...seen, msg.edit] } : r)),
        });
        return;
      }
      if (msg.kind === 'firingEvent') {
        const e = msg.event;
        const state = e.state;
        // One run row per (nodeId, framesKey). The dispatcher
        // produces state events (running/waiting_for_input/completed/
        // failed/skipped/cancelled) on the same record; we mutate
        // the existing row, not append. A fresh dispatch after a
        // terminal row goes into the same row's history (one pulse
        // per (node, frames)). Rows are stamped with the journal's own
        // time, so a replay reads as when it happened.
        const now = e.atUnix * 1000;
        const rows = runState.nodeFirings[e.nodeId] ?? [];
        // Key the record by frame stack: each firing
        // at a distinct frame stack gets its own card so parallel fan-outs
        // don't cross-correlate. `lane` is a structured array;
        // serialize for the stable string identity the render layer
        // needs.
        const framesKey = JSON.stringify(e.frames);
        const idx = rows.findIndex((r) => r.framesKey === framesKey);
        let nextRows: NodeFiring[];
        if (idx < 0) {
          // First event for this (node, frames). Open the record.
          nextRows = [
            ...rows,
            {
              id: `${e.nodeId}-${framesKey}-${now}`,
              nodeId: e.nodeId,
              status: state as NodeFiring['status'],
              pulseId: `${e.nodeId}-${framesKey}-${now}`,
              startedAt: now,
              completedAt:
                state === 'completed' || state === 'failed' || state === 'skipped' || state === 'cancelled'
                  ? now
                  : undefined,
              error: e.error,
              costUsd: 0,
              logs: [],
              runId: '',
              frames: e.frames,
              framesKey,
              input: e.input,
              closedPorts: e.closedPorts,
              skipReason: e.skipReason,
              output: e.output,
              inheritedFrom: e.inheritedFrom,
              providedPorts: e.providedPorts,
              inheritedPorts: e.inheritedPorts,
            },
          ];
        } else {
          // Existing record: mutate in place per state.
          nextRows = rows.map((r, i) => {
            if (i !== idx) return r;
            const updated: NodeFiring = { ...r, status: state as NodeFiring['status'] };
            if (state === 'completed' || state === 'failed' || state === 'skipped' || state === 'cancelled') {
              updated.completedAt = now;
              if (e.output !== undefined) updated.output = e.output;
              if (e.error !== undefined) updated.error = e.error;
            }
            if (state === 'running' && e.input !== undefined && r.input === undefined) {
              updated.input = e.input;
            }
            if (e.inheritedFrom !== undefined) updated.inheritedFrom = e.inheritedFrom;
            if (e.providedPorts !== undefined) updated.providedPorts = e.providedPorts;
            if (e.inheritedPorts !== undefined) updated.inheritedPorts = e.inheritedPorts;
            // closedPorts may arrive on node_started (state=running) or
            // node_skipped (state=skipped); always refresh from the event
            // if present so the inspector shows the per-port (closed)
            // labels for skipped firings as well as running ones.
            if (e.closedPorts !== undefined) {
              updated.closedPorts = e.closedPorts;
            }
            // Why the firing did not run, on a skip. The inspector says
            // it in words: a decision reads differently from an input
            // that never arrived.
            if (e.skipReason !== undefined) {
              updated.skipReason = e.skipReason;
            }
            return updated;
          });
        }
        runState.nodeFirings = bareRecord(runState.nodeFirings, {
          [e.nodeId]: nextRows,
        });
        // Debug preview (`features.showDebugPreview`) reads its
        // last output from `runState.nodeOutputs[id]`. Update
        // it on completion. Earlier this rode the liveData channel;
        // now it taps the firing event directly so a node's display
        // cannot interfere.
        if (state === 'completed' && e.output !== undefined) {
          runState.nodeOutputs = bareRecord(runState.nodeOutputs, {
            [e.nodeId]: e.output,
          });
        }
        return;
      }
      if (msg.kind === 'scopeDecided') {
        // A copy is decided once; the replay and the live stream overlap,
        // so the same decision can arrive twice and the first one stands.
        const e = msg.event;
        const framesKey = JSON.stringify(e.frames);
        const decided = runState.scopeDecisions[e.scope] ?? [];
        if (decided.some((d) => JSON.stringify(d.frames) === framesKey)) return;
        runState.scopeDecisions = bareRecord(runState.scopeDecisions, {
          [e.scope]: [...decided, e],
        });
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
        const log = runState.busLogByBus[busId] ?? [];
        runState.busLogByBus = bareRecord(runState.busLogByBus, {
          [busId]: [...log, msg.event],
        });
        return;
      }
      if (msg.kind === 'callerEvent') {
        // One caller per run: a flat ordered log. Dedup on the
        // row's own position (an offset for a lifecycle row, the span
        // for a window of messages) because the replay snapshot and the
        // live stream overlap during a replay follow.
        const callerKey =
          msg.event.kind === 'window'
            ? `caller:${msg.event.firstOffset}-${msg.event.lastOffset}`
            : `caller:${msg.event.offset}`;
        if (seenCallerOffsets.has(callerKey)) return;
        seenCallerOffsets.add(callerKey);
        runState.callerLog = [...runState.callerLog, msg.event];
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
        const log = runState.loopEventsByGroup[gid] ?? [];
        runState.loopEventsByGroup = {
          ...runState.loopEventsByGroup,
          [gid]: [...log, msg.event],
        };
        return;
      }
      if (msg.kind === 'runTags') {
        // The run tagged itself. Accumulate the set in claim order; the
        // same tag can arrive twice (replay then live, or a body re-run
        // re-tagging), so it is deduplicated here.
        const seen = new Set(runState.tags);
        const fresh = msg.tags.filter((t) => !seen.has(t));
        if (fresh.length > 0) runState.tags = [...runState.tags, ...fresh];
        return;
      }
      if (msg.kind === 'journalCorruption') {
        // One-shot at replay: append. The inspector aggregates these
        // into a muted "N journal rows corrupted" line per run.
        // Not alarming; the user only sees it if they look.
        runState.journalCorruptions = [
          ...runState.journalCorruptions,
          { site: msg.site, reason: msg.reason },
        ];
        return;
      }
      if (msg.kind === 'busParticipant') {
        // Add the (busId, nodeId) edge if new. Idempotent: emitted pulses
        // fires once per source/target on a per-pulse basis, so we may
        // see the same edge multiple times when a producer emits
        // several values on the same bus port. The first edge for a
        // bus also seeds `busMetaByBus[busId]` from the dispatcher-
        // derived mode, so the inspector renders the panel header
        // badge from the same source as participation.
        const set = runState.busParticipantsByBus[msg.busId] ?? new Set<string>();
        if (!set.has(msg.nodeId)) {
          const next = new Set(set);
          next.add(msg.nodeId);
          runState.busParticipantsByBus = bareRecord(runState.busParticipantsByBus, {
            [msg.busId]: next,
          });
        }
        // Bus mode is immutable per bus (the marker carries it from
        // creation). Pin the first-seen meta: divergence is a
        // dispatcher / wire bug, so accepting "latest wins" would
        // silently mutate a known-good state with a known-bad one.
        // Log loud so the bug is visible in the dev tools without
        // corrupting the UI.
        const prevMeta = runState.busMetaByBus[msg.busId];
        if (prevMeta) {
          if (prevMeta.ephemeral !== msg.meta.ephemeral) {
            console.warn(
              `bus meta diverged for ${msg.busId}: pinned ephemeral=${prevMeta.ephemeral}, incoming ephemeral=${msg.meta.ephemeral} (kept the first-seen value; the bus marker is the authoritative shape)`,
            );
          }
        } else {
          runState.busMetaByBus = bareRecord(runState.busMetaByBus, {
            [msg.busId]: msg.meta,
          });
        }
        return;
      }
      if (msg.kind === 'followStatus') {
        followMode = msg.status.mode;
        followRunId = msg.status.runId;
        followPendingCount = msg.status.pendingCount;
        return;
      }
      if (msg.kind === 'followLost') {
        // The live SSE link to this run ended or broke. Stop
        // presenting the run as live (hide the Stop button) WITHOUT
        // falsely marking nodes completed: the per-node rows keep
        // their last known state, we just aren't following anymore.
        // (runTerminal is the run actually finishing; this is the
        // link dying with the run possibly still in flight.)
        runState.isRunning = false;
        toast.warning(
          msg.reason === 'error'
            ? 'Lost the live connection to this run. Re-open it to reconnect.'
            : 'The live run stream ended. Re-open the run to reconnect.',
        );
        return;
      }
      if (msg.kind === 'followReconnecting') {
        // The live link dropped and the host keeps opening it again: one
        // notice, kept up and updated with each failed attempt's reason,
        // gone once the link is back or the follow stops. The run stays
        // live meanwhile.
        if (msg.why === null) toast.dismiss(FOLLOW_RECONNECTING_TOAST);
        else toast.warning('Reconnecting to the live run...', {
          id: FOLLOW_RECONNECTING_TOAST,
          description: msg.why,
          duration: Number.POSITIVE_INFINITY,
        });
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
      if (msg.kind === 'nodeDisplay') {
        // One node's /live tick. Always overwrite the previous one:
        // pollers are independent, errors are user-visible, no
        // fallback to what the last tick said.
        // `kind` is how the message got here, not part of what the
        // node is showing.
        const { kind: _kind, nodeId, ...feed } = msg;
        displayByNode = bareRecord(displayByNode, { [nodeId]: feed });
        return;
      }
    });
    send({ kind: 'ready' });
    return () => {
      unsub();
      window.removeEventListener('weft-display-action', onDisplayAction as EventListener);
      window.removeEventListener('mousedown', onSideButton, true);
      window.removeEventListener('mouseup', onSideButton, true);
      // The whole editor is going away: settle every in-flight host request
      // so nothing awaits a reply that will never be consumed.
      cancelPendingHostRequests('the editor closed');
      // Last consumer of this editor mount is gone: detach the module-global
      // fan-out from the transport so a host that tears down on "no receivers"
      // (the browser host closes its SSE + pull timer) actually does. The VS
      // Code transport is idempotent under this; a browser host stops leaking
      // the last-opened project's live connection after navigation.
      teardownTransport();
    };
  });

  function onSave(data: { fileRef?: { path: string; content: string } }) {
    if (data.fileRef !== undefined) {
      // File-backed config field edit: the content goes to the
      // referenced file, not the `@file(...)` token in the source.
      send({ kind: 'saveFileRef', path: data.fileRef.path, content: data.fileRef.content });
    }
  }

  /// Persist the layout durably. Resolves once the host ACKED the disk
  /// write: the editor refuses to adopt any layout echoed back by a parse
  /// while a save is un-acked (that echo predates the write and adopting it
  /// would erase the just-saved positions). Cancellation (the view switched
  /// away; the request was swept) resolves too: the dying editor just needs
  /// its awaiter settled.
  async function onPersistLayout(code: string): Promise<void> {
    if (code === layoutCode) return;
    layoutCode = code;
    try {
      await hostRequest('layoutSaved', (requestId) => ({ kind: 'saveLayout', layoutCode: code, requestId }));
    } catch (e) {
      if (e instanceof HostRequestCancelled) return;
      throw e;
    }
  }

  /// A graph (GUI) edit: send the intents to the host (Rust edit-server
  /// applies + writes the source). Resolves with the inverse text edit (this
  /// action's undo) plus the post-edit truth; rejects with the host's reason.
  function onApplyEdits(ops: EditOp[]): Promise<EditRpcResult> {
    return requestEdit((requestId) => ({ kind: 'applyEdits', ops, requestId }));
  }

  /// Replay a raw source text edit (undo/redo of a source action). Same reply
  /// shape (the inverse undoes THIS replay, so undo<->redo round-trips).
  function onApplyTextEdit(edit: TextEdit): Promise<EditRpcResult> {
    return requestEdit((requestId) => ({ kind: 'applyTextEdit', edit, requestId }));
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
  function onRun(targets: [string, ...string[]]) {
    editorRef?.flushAllPendingSaves?.();
    send({ kind: 'runProject', targets });
  }
  /// Whether a run of these targets would start (`weft run --check`):
  /// its refusal, or the infra it reads, which the contextual bar holds
  /// against the infra status to disable Run with a reason.
  async function checkRun(targets: [string, ...string[]]): Promise<RunCheck> {
    editorRef?.flushAllPendingSaves?.();
    const reply = await hostRequest('runChecked', (requestId) => ({ kind: 'checkRun', requestId, targets }));
    return reply.check;
  }

  /// Put the files on disk onto a run's version (the version banner).
  function onBranchTo(reference: string) {
    send({ kind: 'branchTo', reference });
  }
  // Stop is generic now: the host inspects the bar's current
  // state and either kills the spawned CLI process group
  // (cli_running) or POSTs /runs/{runId}/cancel
  // (run_running). One verb, state-aware behavior.
  function onStop() { send({ kind: 'stopAction' }); }
  function onDismissError() { send({ kind: 'dismissError' }); }
  /// A lifecycle verb on the selected triggers or infra. Turning on,
  /// resyncing, starting and upgrading build from the source, so the
  /// pending edits are flushed first.
  function onLifecycle(action: LifecycleAction) {
    editorRef?.flushAllPendingSaves?.();
    send({ kind: 'lifecycle', action });
  }
  function onCancelActivate() { send({ kind: 'cancelActivate' }); }
  function onCancelBuild() { send({ kind: 'cancelBuild' }); }
  function onCancelInfra() { send({ kind: 'infraCancel' }); }
  // A resync that stopped is taken up again with the take-down choice it
  // was started with, and never as a new resync.
  function onResumeResync() { send({ kind: 'resumeResync' }); }

  // The context handed to the panel slots. Getters (not a snapshot) so each read
  // is reactive: a slot consumer that reads `ctx.activeSource` / `ctx.runState`
  // re-renders when those change, while the object identity stays stable. `send`
  // / `onMessage` are the editor's own host channel, exposed verbatim so a panel
  // can drive the system however it wants.
  const editorContext: EditorContext = {
    get project() { return project; },
    get activeSource() { return activeSource; },
    get activeFileName() { return navFileName; },
    get navDepth() { return navDepth; },
    get callPath() { return callPath; },
    get runState() { return shownRunState; },
    get error() { return error; },
    get diagnostics() { return diagnostics; },
    editActiveSource(source: string) { send({ kind: 'editActiveSource', source }); },
    // The proper applyEdits RPC: it registers a pending resolver, so the host's
    // `editApplied` reply carries the post-edit truth back into `project` (the
    // canvas AND any panel reading `ctx.project` refresh). A raw
    // `send({applyEdits})` would apply + autosave on the host but the reply
    // would be dropped (no resolver) and the on-screen graph would stay stale.
    applyEdits(ops: EditOp[]) { return onApplyEdits(ops); },
    send,
    onMessage,
  };
</script>

<div class="absolute inset-0">
  <!-- Problems pill: ONE compact, expandable surface floating over the
       canvas for parse + catalog problems. A failure never takes the
       rendered graph down: the last good graph stays on screen (source
       gestures are locked separately while it is stale), and this pill
       is the in-context signal. The action bar's sticky error chip
       carries the same failure with the full details modal. -->
  {#if project && problems.length > 0}
    <div class="absolute top-2 inset-x-0 z-20 flex justify-center px-4">
      <div class="max-w-[80%] rounded-md border border-destructive/40 bg-background/95 text-xs shadow-md">
        <button
          class="flex w-full items-center gap-2 px-3 py-1.5 text-left"
          onclick={() => (problemsExpanded = !problemsExpanded)}
        >
          <span class="shrink-0 font-medium text-destructive">
            {problems.length === 1 ? problems[0].label : `${problems.length} problems`}
          </span>
          {#if !problemsExpanded && problems.length === 1}
            <span class="truncate text-muted-foreground">{problems[0].text}</span>
          {/if}
          {#if error}
            <span class="shrink-0 text-muted-foreground">· showing the last rendered graph</span>
          {/if}
          <span class="ml-auto shrink-0 text-muted-foreground">{problemsExpanded ? '▴' : '▾'}</span>
        </button>
        {#if problemsExpanded}
          <div class="space-y-2 border-t border-destructive/20 px-3 py-2">
            {#each problems as p}
              <div class="whitespace-pre-wrap break-words {p.severity === 'warning' ? 'text-yellow-600' : 'text-destructive'}">{p.text}</div>
            {/each}
            <div class="text-muted-foreground">{CATALOG_HINT}</div>
          </div>
        {/if}
      </div>
    </div>
  {/if}
  {#if project}
    <!-- Editor frame: toolbar on top, then a row of [left slot | canvas | right
         slot]. A host fills the slots (e.g. a web host injects a code view +
         replay bar); when a snippet is absent (the VS Code extension) its region
         simply doesn't render and the canvas spans the full width. -->
    <div class="flex h-full w-full flex-col">
      <div class="flex min-h-0 flex-1">
        {#if leftPanel}{@render leftPanel(editorContext)}{/if}
        <!-- The canvas. `relative` so ProjectEditor's `absolute inset-0` fills
             this cell; `min-w-0` so a wide panel can't shove it off-screen. The
             editor's own floating toolbar (follow toggle / Source / nav) lives
             INSIDE this cell so it floats over the CANVAS only: when the left
             panel opens it pushes the canvas right, and the toolbar travels with
             it instead of overlapping the panel. -->
        <div class="relative min-w-0 flex-1">
          <GraphToolbar
            mode={followMode}
            runId={followRunId}
            pendingCount={followPendingCount}
            notPainted={runNotPainted}
            onSetMode={(mode) => send({ kind: 'followSetMode', mode })}
            {navDepth}
            {navFileName}
            {interactive}
            {onNavigateBack}
          >
            {#snippet leading()}
              {#if toolbarLeading}{@render toolbarLeading(editorContext)}{/if}
            {/snippet}
            {#snippet trailing()}
              {#if toolbarTrailing}{@render toolbarTrailing(editorContext)}{/if}
            {/snippet}
            {#snippet below()}
              <!-- The run's conversation with its caller. A run-level
                   fact, so it lives beside the run's own controls rather
                   than inside a node's inspector: there is one caller per
                   run and the exchange is the same whichever node you
                   open. Nothing is drawn for a run that had no caller. -->
              {#if shownRunState.callerLog.length > 0}
                <button
                  type="button"
                  onclick={() => (callerPanelOpen = !callerPanelOpen)}
                  class="flex items-center gap-1.5 px-2.5 py-1.5 rounded-md border shadow-sm text-xs font-medium transition
                    {callerPanelOpen
                      ? 'bg-zinc-900 text-white border-zinc-900 hover:bg-zinc-800'
                      : 'bg-white text-zinc-700 border-zinc-200 hover:bg-zinc-50'}"
                  title="Show what this run and its caller said to each other"
                >
                  <MessagesSquare class="w-3 h-3" />
                  Caller
                </button>
              {/if}
            {/snippet}
          </GraphToolbar>
          {#if callerPanelOpen && shownRunState.callerLog.length > 0}
            <div class="absolute top-24 left-3 z-30 pointer-events-auto">
              <CallerPanel
                events={shownRunState.callerLog}
                onClose={() => (callerPanelOpen = false)}
              />
            </div>
          {/if}
    {#key viewGeneration}
    <!-- Failure wall for the editor: an uncaught throw in its render or
         effects would otherwise kill the whole webview SILENTLY, and the
         panel is a singleton, so every file opened afterwards would show
         the same blank canvas until a window reload. Fail loud instead:
         name the error on screen and offer a remount. -->
    <svelte:boundary>
      {#snippet failed(error, reset)}
        <div class="absolute inset-0 flex flex-col items-center justify-center gap-3 p-8 text-center">
          <div class="text-sm font-semibold text-red-600">The graph view crashed</div>
          <div class="max-w-xl text-xs text-muted-foreground break-words">
            {error instanceof Error ? error.message : String(error)}
          </div>
          <!-- Bumping the key remounts the whole {#key} block, boundary
               included, which IS the reset; calling the snippet's reset()
               too would run against a block already being torn down. -->
          <button
            class="px-3 py-1.5 rounded bg-primary text-primary-foreground text-xs"
            onclick={() => { viewGeneration += 1; }}
          >
            Reload the graph
          </button>
        </div>
      {/snippet}
    <ProjectEditor
      bind:this={editorRef}
      {project}
      {onSave}
      {onPersistLayout}
      {onApplyEdits}
      {onApplyTextEdit}
      {onResyncSource}
      {onOpenInclude}
      {callPath}
      {interactive}
      {fileContents}
      {installView}
      onSwitchInstall={(install) => send({ kind: 'switchInstall', install })}
      {autoOrganizeOnMount}
      {onRun}
      {onBranchTo}
      {onStop}
      {onDismissError}
      onOpenLocation={(location) => send({ kind: 'openSource', location })}
      {onCancelActivate}
      {onCancelBuild}
      {onCancelInfra}
      {onResumeResync}
      {onLifecycle}
      {checkRun}
      {actionBarState}
      status={statusSnapshot}
      {hasInfraInGraph}
      {hasTriggersInGraph}
      runState={shownRunState}
      {displayByNode}
    />
    </svelte:boundary>
    {/key}
        </div>
        {#if rightPanel}{@render rightPanel(editorContext)}{/if}
      </div>
    </div>
  {:else if error || catalogError}
    <!-- Nothing has rendered this session AND the first parse (or
         catalog load) failed: there is no last graph to keep showing,
         so explain compactly instead of dumping a wall of red text. -->
    <div class="flex h-full w-full items-center justify-center p-8">
      <div class="max-w-xl space-y-2 text-sm">
        <div class="font-medium text-destructive">The graph can't render yet</div>
        <div class="whitespace-pre-wrap break-words text-muted-foreground">{error ?? catalogError}</div>
        <div class="text-xs text-muted-foreground">{CATALOG_HINT}</div>
      </div>
    </div>
  {:else}
    <!-- Loading: a centered spinner over the canvas frame, NOT bare text (which
         reads as a broken page). The outer `absolute inset-0` already fills the
         graph area, so this just centers within it. -->
    <div class="flex h-full w-full items-center justify-center">
      <LoaderCircle class="h-6 w-6 animate-spin text-muted-foreground/60" />
    </div>
  {/if}
</div>
