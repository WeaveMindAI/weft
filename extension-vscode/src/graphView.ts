// Extension-host side of the graph view. Owns a single WebviewPanel
// that tracks the currently-active .weft document, parses via the
// dispatcher on every text change (debounced), and streams saves
// back into the document / .layout.json sidecar.
//
// The webview does all the text surgery in-process via v1's
// weft-editor.ts. When the user edits something, the webview sends
// the entire new weft source via `saveWeft`; the host applies a
// full-range TextEdit and the resulting onDidChangeTextDocument
// kicks off the next parse.

import * as vscode from 'vscode';
import type { DispatcherClient } from './dispatcher';
import type { HostMessage, LiveDataItem, ProjectDefinition, WebviewMessage } from './shared/protocol';
import { readProjectIdFromToml } from './sidebar/projects';

export interface SelectedNode {
  nodeId: string;
  nodeType: string;
  label?: string;
  config?: Record<string, unknown>;
  inputs?: Array<{ name: string; type: string }>;
  outputs?: Array<{ name: string; type: string }>;
}

export class GraphViewController {
  private panel: vscode.WebviewPanel | undefined;
  private watchedDoc: vscode.TextDocument | undefined;
  private watchedProjectId: string | undefined;
  private parseTimer: NodeJS.Timeout | undefined;
  private disposables: vscode.Disposable[] = [];
  private lastProject: ProjectDefinition | undefined;
  // Host-side callbacks wired by extension.ts.
  private runHandler: (() => void) | undefined;
  private stopHandler: (() => void) | undefined;
  private selectionHandler: ((sel: SelectedNode | undefined) => void) | undefined;
  private followTogglePinHandler: (() => void) | undefined;
  private followCatchUpHandler: (() => void) | undefined;
  /// Hooks fired when the user triggers an action that spawns an
  /// execution whose color we don't yet know (activate / infra
  /// start). Extension.ts uses these to tell AutoFollow "next
  /// ExecutionStarted on this project, jump to it."
  private lifecycleStartHandler: ((verb: 'activate' | 'infraStart') => void) | undefined;
  private openSourceHandler: (() => void) | undefined;
  private cancelBuildHandler: (() => void) | undefined;
  private ensureBuildHandler:
    | ((verb: 'run' | 'activate' | 'infraStart') => Promise<void>)
    | undefined;
  // Set while we're applying our own TextEdit to the document.
  // onDidChangeTextDocument fires during the edit; if we parsed
  // twice (once for the webview save, once for the VS Code change)
  // we'd loop.
  private suppressReparse = false;
  // One entry per (project, infra node) we're polling /live for.
  // Keyed by nodeId. Cleared on parseResult and dispose.
  private liveTimers: Map<string, NodeJS.Timeout> = new Map();
  // Interval between polls for infra /live. 3s matches v1; the
  // sidecar's /live is cheap (returns current state snapshot), so
  // this is fine.
  private readonly liveIntervalMs = 3000;
  // Infra status poller: one timer per open panel, polling the
  // dispatcher's /infra/status endpoint so the ActionBar reflects
  // the actual cluster state (running / stopped / none).
  private infraStatusTimer: NodeJS.Timeout | undefined;
  // Trigger status poller: tells the ActionBar whether the project
  // is currently activated (dispatcher status == Active).
  private triggerStatusTimer: NodeJS.Timeout | undefined;
  // Whether the open project has any infra nodes at all. Cached
  // from the latest parse so we can skip the poll when there's
  // nothing to report.
  private hasInfraNodes = false;
  // Whether the open project has any trigger nodes. Drives
  // visibility of the trigger section of the ActionBar.
  private hasTriggerNodes = false;

  constructor(
    private readonly context: vscode.ExtensionContext,
    private readonly client: DispatcherClient,
  ) {}

  /** Called by extension.ts so sidebar-initiated runs and action-bar
   *  clicks route through the same business logic. */
  setRunHandler(fn: () => void): void { this.runHandler = fn; }
  setStopHandler(fn: () => void): void { this.stopHandler = fn; }
  setNodeSelectionHandler(fn: (sel: SelectedNode | undefined) => void): void { this.selectionHandler = fn; }
  setFollowTogglePinHandler(fn: () => void): void { this.followTogglePinHandler = fn; }
  setFollowCatchUpHandler(fn: () => void): void { this.followCatchUpHandler = fn; }
  setLifecycleStartHandler(fn: (verb: 'activate' | 'infraStart') => void): void {
    this.lifecycleStartHandler = fn;
  }
  setOpenSourceHandler(fn: () => void): void { this.openSourceHandler = fn; }
  /// Hook fired when the user clicks Stop during the Building
  /// phase. Lets extension.ts kill the in-flight `weft build`
  /// child process so the UI returns to its idle state.
  setCancelBuildHandler(fn: () => void): void { this.cancelBuildHandler = fn; }
  /// Hook fired whenever a graph-bar verb that may spawn worker
  /// pods is about to fire (Run, Activate, InfraStart). Lets
  /// extension.ts rebuild the worker image first so the spawned
  /// pod runs the current source instead of the stale image
  /// baked from the previous build. The verb lets the webview
  /// show "Building..." in place of the verb's normal pending
  /// state.
  setEnsureBuildHandler(
    fn: (verb: 'run' | 'activate' | 'infraStart') => Promise<void>,
  ): void {
    this.ensureBuildHandler = fn;
  }

  /** Public so execFollower can push events into the panel. */
  post(msg: HostMessage): void {
    this.panel?.webview.postMessage(msg);
  }

  /** True iff the graph webview panel currently exists. The
   *  cold-open handler in extension.ts uses this to distinguish
   *  "user just opened a .weft and we should swap it to graph"
   *  from "user is refocusing an already-pinned project's text
   *  tab and we should leave it alone". */
  isOpen(): boolean {
    return this.panel !== undefined;
  }

  /** Infer which edges are currently pulsing from the latest node
   *  lifecycle event. Called by ExecutionFollower. */
  approximateActiveEdges(
    nodeId: string,
    kind: 'started' | 'completed' | 'failed' | 'skipped',
  ): void {
    if (!this.lastProject) return;
    const relevant: string[] = [];
    if (kind === 'started') {
      for (const e of this.lastProject.edges) {
        if (e.target === nodeId) relevant.push(e.id);
      }
    } else if (kind === 'completed') {
      for (const e of this.lastProject.edges) {
        if (e.source === nodeId) relevant.push(e.id);
      }
    }
    for (const edgeId of relevant) {
      this.post({ kind: 'edgeActive', event: { edgeId, active: true } });
      setTimeout(() => {
        this.post({ kind: 'edgeActive', event: { edgeId, active: false } });
      }, 200);
    }
  }

  async open(doc: vscode.TextDocument, projectId?: string): Promise<void> {
    // Resolve the project id for this file. Explicit caller arg
    // wins (sidebar pin path); otherwise walk up from the .weft
    // file looking for a `weft.toml` that declares an id. Falling
    // back to undefined means the dispatcher returns nil UUID on
    // /parse, which breaks every /projects/{id}/... endpoint for
    // this panel (live poll, infra status, trigger status).
    const resolved = projectId ?? readProjectIdFromToml(doc.uri.fsPath);
    if (resolved) this.watchedProjectId = resolved;
    // Graph takes ViewColumn.Active so the .weft text doesn't
    // show by default. The "Source" button opens the text in
    // ViewColumn.Beside (column 2). We don't try to swap them
    // because moving a webview between columns destroys the
    // iframe (microsoft/vscode#141001).
    if (this.panel) {
      this.panel.reveal(vscode.ViewColumn.Active);
      this.watchedDoc = doc;
      await this.triggerParse();
      return;
    }

    this.panel = vscode.window.createWebviewPanel(
      'weft.graph',
      `Weft Graph: ${doc.fileName.split(/[\\/]/).pop() ?? ''}`,
      vscode.ViewColumn.Active,
      {
        enableScripts: true,
        retainContextWhenHidden: true,
        localResourceRoots: [
          vscode.Uri.joinPath(this.context.extensionUri, 'media'),
        ],
      },
    );

    this.panel.webview.html = this.renderHtml();
    this.watchedDoc = doc;

    // Initial state (settings, catalog, parse result) is pushed
    // from the 'ready' message handler. The webview emits 'ready'
    // on every iframe boot, including after a column move (which
    // destroys and rebuilds the iframe; see microsoft/vscode
    // #172391 + #106693). Pushing from here would race the
    // webview's onMessage subscription and lose messages.

    this.disposables.push(
      this.panel.webview.onDidReceiveMessage((msg) => this.onMessage(msg)),
      this.panel.onDidDispose(() => this.onDispose()),
      vscode.workspace.onDidChangeTextDocument((e) => {
        if (this.suppressReparse) return;
        if (this.watchedDoc && e.document === this.watchedDoc) {
          this.scheduleParse();
        }
      }),
      vscode.window.onDidChangeActiveTextEditor((ed) => {
        if (ed && ed.document.languageId === 'weft') {
          this.watchedDoc = ed.document;
          // Re-resolve project id for the new file: different
          // .weft files can belong to different projects, and
          // the old watchedProjectId must not leak into the
          // polls we kick off from triggerParse below. If the
          // new file has no weft.toml, clear the id so the
          // dispatcher resolves it on /parse instead of using
          // the stale previous-project id.
          const newId = readProjectIdFromToml(ed.document.uri.fsPath);
          this.watchedProjectId = newId ?? undefined;
          void this.triggerParse();
        }
      }),
      // Push source-open state to the webview so the "Source"
      // button can render as active when the .weft is visible in
      // some tab. Fires on every tab change anywhere; the
      // computeSourceOpen helper short-circuits when the watched
      // doc hasn't moved.
      vscode.window.tabGroups.onDidChangeTabs(() => this.pushSourceState()),
    );
    // Initial state push.
    this.pushSourceState();
  }

  private pushSourceState(): void {
    if (!this.panel || !this.watchedDoc) return;
    const target = this.watchedDoc.uri.fsPath;
    const open = vscode.window.tabGroups.all.some((g) =>
      g.tabs.some(
        (t) =>
          t.input instanceof vscode.TabInputText
          && t.input.uri.fsPath === target,
      ),
    );
    this.post({ kind: 'sourceState', open });
  }

  private scheduleParse(): void {
    const debounce = vscode.workspace
      .getConfiguration('weft.parse')
      .get<number>('debounceMs', 100);
    if (this.parseTimer) clearTimeout(this.parseTimer);
    this.parseTimer = setTimeout(() => void this.triggerParse(), debounce);
  }

  private async triggerParse(): Promise<void> {
    if (!this.panel || !this.watchedDoc) return;
    const source = this.watchedDoc.getText();
    const layoutCode = await this.readLayoutCode(this.watchedDoc);
    try {
      const response = await this.client.parse(source, this.watchedProjectId);
      this.lastProject = response.project;
      // Latch the parsed project id as the authoritative watch id
      // when we don't already have one. Guards against the case
      // where weft.toml lookup failed on open but the dispatcher
      // returns a real uuid (e.g. a project registered by the CLI
      // that knows the id from its own weft.toml).
      const nilUuid = '00000000-0000-0000-0000-000000000000';
      if (
        response.project.id &&
        response.project.id !== nilUuid &&
        !this.watchedProjectId
      ) {
        this.watchedProjectId = response.project.id;
      }
      this.post({ kind: 'parseResult', response, source, layoutCode });
      this.syncInfraLivePollers(response);
    } catch (err) {
      this.post({
        kind: 'parseError',
        error: err instanceof Error ? err.message : String(err),
      });
    }
  }

  /** Compare the latest parse to the set of infra nodes we're
   *  currently polling `/live` for. Start pollers for any newly-
   *  introduced infra nodes, stop those that no longer exist. The
   *  dispatcher answers 404 cleanly when `weft infra up` hasn't run
   *  yet, so starting a poller is harmless either way.
   *
   *  Also drives the ActionBar's infra + trigger status pollers
   *  based on which node families the project contains.
   */
  private syncInfraLivePollers(response: { project: ProjectDefinition; catalog: Record<string, { requires_infra?: boolean; features?: { isTrigger?: boolean } }> }): void {
    const projectId = response.project.id;
    if (!projectId) {
      this.stopAllLivePollers();
      return;
    }
    const infraNodeIds = new Set(
      response.project.nodes
        .filter((n) => {
          const entry = response.catalog[n.nodeType];
          // NodeDefinition serializes `requires_infra` as camelCase
          // `requiresInfra`; the catalog entry uses snake_case (its
          // serde config differs). Check both so either one flips
          // polling on.
          const nodeFlag = (n as unknown as { requiresInfra?: boolean }).requiresInfra;
          return nodeFlag ?? entry?.requires_infra ?? false;
        })
        .map((n) => n.id),
    );

    // Stop pollers for nodes no longer in the project (or no longer
    // requires_infra).
    for (const [id, timer] of this.liveTimers.entries()) {
      if (!infraNodeIds.has(id)) {
        clearInterval(timer);
        this.liveTimers.delete(id);
      }
    }
    // Start pollers for new infra nodes.
    for (const id of infraNodeIds) {
      if (this.liveTimers.has(id)) continue;
      this.liveTimers.set(id, this.startLivePoller(projectId, id));
    }

    // Keep the infra-status poller alive while this project has
    // infra nodes; tear it down otherwise.
    const hadInfra = this.hasInfraNodes;
    this.hasInfraNodes = infraNodeIds.size > 0;
    if (this.hasInfraNodes && !hadInfra) {
      this.startInfraStatusPoller(projectId);
    } else if (!this.hasInfraNodes && hadInfra) {
      this.stopInfraStatusPoller();
    }

    // Trigger-status poller runs as long as the project has any
    // trigger node. `features.isTrigger` is mirrored onto the
    // NodeDefinition during enrich; the protocol uses camelCase.
    const triggerIds = new Set(
      response.project.nodes
        .filter((n) => {
          const entry = response.catalog[n.nodeType];
          return (n.features?.isTrigger ?? entry?.features?.isTrigger) ?? false;
        })
        .map((n) => n.id),
    );
    const hadTriggers = this.hasTriggerNodes;
    this.hasTriggerNodes = triggerIds.size > 0;
    if (this.hasTriggerNodes && !hadTriggers) {
      this.startTriggerStatusPoller(projectId);
    } else if (!this.hasTriggerNodes && hadTriggers) {
      this.stopTriggerStatusPoller();
    }
  }

  private startLivePoller(projectId: string, nodeId: string): NodeJS.Timeout {
    // Fire one poll immediately so the user doesn't wait 3s to see
    // the QR on first activation, then repeat on the interval.
    const tick = async () => {
      try {
        const body = await this.client.get<{ items: unknown[] }>(
          `/projects/${projectId}/infra/nodes/${nodeId}/live`,
        );
        const items = Array.isArray(body.items)
          ? body.items.filter(isLiveDataItem)
          : [];
        this.post({ kind: 'liveData', nodeId, items });
      } catch {
        // Infra not provisioned yet (404) or sidecar down (BAD_GATEWAY).
        // Clear any previous render with an empty items list so the
        // node stops showing stale data.
        this.post({ kind: 'liveData', nodeId, items: [] });
      }
    };
    void tick();
    return setInterval(() => void tick(), this.liveIntervalMs);
  }

  private stopAllLivePollers(): void {
    for (const timer of this.liveTimers.values()) clearInterval(timer);
    this.liveTimers.clear();
  }

  private startInfraStatusPoller(projectId: string): void {
    this.stopInfraStatusPoller();
    const tick = async () => {
      try {
        const body = await this.client.get<{
          nodes: Array<{ node_id: string; status: 'running' | 'stopped'; endpoint_url: string | null }>;
        }>(`/projects/${projectId}/infra/status`);
        const nodes = (body.nodes ?? []).map((n) => ({
          nodeId: n.node_id,
          status: n.status,
          endpointUrl: n.endpoint_url,
        }));
        const rollup: 'running' | 'stopped' | 'mixed' | 'none' =
          nodes.length === 0
            ? 'none'
            : nodes.every((n) => n.status === 'running')
              ? 'running'
              : nodes.every((n) => n.status === 'stopped')
                ? 'stopped'
                : 'mixed';
        this.post({ kind: 'infraStatus', snapshot: { nodes, rollup } });
      } catch {
        this.post({ kind: 'infraStatus', snapshot: { nodes: [], rollup: 'none' } });
      }
    };
    void tick();
    this.infraStatusTimer = setInterval(() => void tick(), this.liveIntervalMs);
  }

  private stopInfraStatusPoller(): void {
    if (this.infraStatusTimer) {
      clearInterval(this.infraStatusTimer);
      this.infraStatusTimer = undefined;
    }
  }

  private startTriggerStatusPoller(projectId: string): void {
    this.stopTriggerStatusPoller();
    const tick = async () => {
      try {
        const body = await this.client.get<{ status: string }>(
          `/projects/${projectId}`,
        );
        const s = body.status as 'registered' | 'active' | 'inactive';
        this.post({
          kind: 'triggerStatus',
          snapshot: {
            projectStatus:
              s === 'active' || s === 'inactive' || s === 'registered' ? s : 'unknown',
          },
        });
      } catch {
        this.post({ kind: 'triggerStatus', snapshot: { projectStatus: 'unknown' } });
      }
    };
    void tick();
    this.triggerStatusTimer = setInterval(() => void tick(), this.liveIntervalMs);
  }

  private stopTriggerStatusPoller(): void {
    if (this.triggerStatusTimer) {
      clearInterval(this.triggerStatusTimer);
      this.triggerStatusTimer = undefined;
    }
  }

  /** Fire `activate` or `deactivate` against the dispatcher and
   *  kick a fresh trigger-status poll so the ActionBar settles
   *  immediately. */
  private async callProjectLifecycle(verb: 'activate' | 'deactivate'): Promise<void> {
    const projectId = this.lastProject?.id ?? this.watchedProjectId;
    if (!projectId) return;
    if (verb === 'activate') {
      // Trigger-setup spawns a worker pod that runs the project's
      // binary; rebuild the image first so the run reflects the
      // user's latest source. Skipped for deactivate (no spawn).
      try {
        await this.ensureBuildHandler?.('activate');
      } catch (err) {
        this.reportActionFailure('activate', err);
        return;
      }
      // Prime auto-follow before firing the request: the trigger
      // setup sub-exec's ExecutionStarted can arrive over SSE
      // before the HTTP response returns.
      this.lifecycleStartHandler?.('activate');
    }
    try {
      await this.client.post<unknown>(`/projects/${projectId}/${verb}`, {});
    } catch (err) {
      this.reportActionFailure(verb, err);
    }
    if (this.hasTriggerNodes) {
      this.startTriggerStatusPoller(projectId);
    }
  }

  /** Fire-and-forget: hit the dispatcher's infra endpoint on the
   *  watched project's behalf, then force a poll so the ActionBar
   *  reflects the new state without waiting for the next tick. */
  private async callInfra(verb: 'start' | 'stop' | 'terminate'): Promise<void> {
    const projectId = this.lastProject?.id ?? this.watchedProjectId;
    if (!projectId) return;
    if (verb === 'start') {
      // Infra start now spawns an InfraSetup-phase worker pod
      // (v2 model). The pod runs the project's binary, so the
      // image needs to reflect the user's latest source. Stop /
      // terminate don't spawn workers.
      try {
        await this.ensureBuildHandler?.('infraStart');
      } catch (err) {
        this.reportActionFailure('infraStart', err);
        return;
      }
      // Prime auto-follow: first-time provision spawns no child
      // exec, but scale_up eventually does if the user then hits
      // activate. We still prime on start so the next exec after
      // readiness auto-follows.
      this.lifecycleStartHandler?.('infraStart');
    }
    try {
      await this.client.post<unknown>(`/projects/${projectId}/infra/${verb}`, {});
    } catch (err) {
      this.reportActionFailure(`infra${verb.charAt(0).toUpperCase()}${verb.slice(1)}` as
        | 'infraStart'
        | 'infraStop'
        | 'infraTerminate', err);
    }
    // Refresh status immediately; the next interval tick will
    // pick up any late k8s transitions (starting → running).
    if (this.hasInfraNodes) {
      this.startInfraStatusPoller(projectId);
    }
  }

  /** Shared failure path for the ActionBar verbs. Posts an
   *  `actionFailed` message so the webview can clear its optimistic
   *  transitional state, and surfaces a toast so the user isn't
   *  left guessing why nothing happened. */
  private reportActionFailure(
    action: 'infraStart' | 'infraStop' | 'infraTerminate' | 'activate' | 'deactivate',
    err: unknown,
  ): void {
    const message = err instanceof Error ? err.message : String(err);
    console.warn(`[weft/action] ${action} failed:`, err);
    this.post({ kind: 'actionFailed', action, message });
    void vscode.window.showErrorMessage(`Weft: ${action} failed — ${message}`);
  }

  private onMessage(msg: WebviewMessage): void {
    switch (msg.kind) {
      case 'ready':
        // Webview just booted (initial open OR iframe rebuild
        // after a column move). Re-send the full initial state:
        // settings, catalog, source parse, source-open flag.
        // Don't assume the webview retained anything; column
        // moves destroy the iframe even with
        // retainContextWhenHidden.
        void this.sendSettings();
        void this.sendGlobalCatalog();
        void this.triggerParse();
        this.pushSourceState();
        break;
      case 'saveWeft':
        void this.saveWeft(msg.source);
        break;
      case 'saveLayout':
        void this.saveLayoutCode(msg.layoutCode);
        break;
      case 'log':
        console[msg.level]('[weft/webview]', msg.message);
        break;
      case 'runProject':
        this.runHandler?.();
        break;
      case 'stopProject':
        this.stopHandler?.();
        break;
      case 'infraStart':
        void this.callInfra('start');
        break;
      case 'infraStop':
        void this.callInfra('stop');
        break;
      case 'infraTerminate':
        void this.callInfra('terminate');
        break;
      case 'activateProject':
        void this.callProjectLifecycle('activate');
        break;
      case 'deactivateProject':
        void this.callProjectLifecycle('deactivate');
        break;
      case 'followTogglePin':
        this.followTogglePinHandler?.();
        break;
      case 'followCatchUp':
        this.followCatchUpHandler?.();
        break;
      case 'openSource':
        this.openSourceHandler?.();
        break;
      case 'cancelBuild':
        this.cancelBuildHandler?.();
        break;
      case 'nodeSelected':
        if (msg.nodeId === null) {
          this.selectionHandler?.(undefined);
        } else {
          const node = this.lastProject?.nodes.find((n) => n.id === msg.nodeId);
          if (node) {
            this.selectionHandler?.({
              nodeId: node.id,
              nodeType: node.nodeType,
              label: node.label ?? undefined,
              config: node.config,
              inputs: node.inputs.map((p) => ({ name: p.name, type: p.portType })),
              outputs: node.outputs.map((p) => ({ name: p.name, type: p.portType })),
            });
          }
        }
        break;
    }
  }

  /// Promise that resolves when the most-recent saveWeft has
  /// finished writing to disk. extension.ts's runPinned awaits
  /// this before reading the .weft so the build always sees the
  /// freshest source, even if the user clicked Run before the
  /// debounced graph-edit save round-trip finished.
  private pendingSaveWeft: Promise<void> | undefined;

  /// Public: extension.ts's runPinned calls this before reading
  /// the .weft from disk. Resolves immediately if no save is in
  /// flight.
  async waitForPendingSave(): Promise<void> {
    if (this.pendingSaveWeft) {
      await this.pendingSaveWeft;
    }
  }

  /** Replace the watched document's text with the webview's copy
   *  AND persist it to disk. The webview already debounces
   *  saveWeft (~1s after the user stops editing), so this is
   *  effectively "auto-save the .weft 1s after every graph edit
   *  pause." Without persisting to disk, the .weft tab stays
   *  dirty and `weft build` reads the stale on-disk version
   *  when the user clicks Run.
   *
   *  Suppress re-entry so we don't reparse on our own edit.
   *  Tracks the in-flight write via `pendingSaveWeft` so
   *  runPinned can await it before reading the file from disk. */
  private async saveWeft(source: string): Promise<void> {
    // Capture watchedDoc at entry. If the user switches the
    // active editor between now and when applyEdit/save runs,
    // we must NOT redirect this write to the new doc — the
    // source we were handed belongs to whatever was watched
    // when the saveWeft message was queued.
    const doc = this.watchedDoc;
    if (!doc) return;
    if (doc.getText() === source) return;
    const edit = new vscode.WorkspaceEdit();
    const last = doc.lineCount - 1;
    const end = doc.lineAt(last).range.end;
    edit.replace(doc.uri, new vscode.Range(0, 0, end.line, end.character), source);
    const previous = this.pendingSaveWeft ?? Promise.resolve();
    const work = (async () => {
      await previous;
      this.suppressReparse = true;
      try {
        await vscode.workspace.applyEdit(edit);
        await doc.save();
      } finally {
        this.suppressReparse = false;
      }
    })();
    this.pendingSaveWeft = work;
    try {
      await work;
      void this.triggerParse();
    } finally {
      // Clear only if no later saveWeft has taken over.
      if (this.pendingSaveWeft === work) {
        this.pendingSaveWeft = undefined;
      }
    }
  }

  private layoutUriFor(doc: vscode.TextDocument): vscode.Uri {
    return vscode.Uri.parse(doc.uri.toString() + '.layout.json');
  }

  private async readLayoutCode(doc: vscode.TextDocument): Promise<string> {
    try {
      const data = await vscode.workspace.fs.readFile(this.layoutUriFor(doc));
      return new TextDecoder().decode(data);
    } catch {
      return '';
    }
  }

  private async saveLayoutCode(layoutCode: string): Promise<void> {
    if (!this.watchedDoc) return;
    const uri = this.layoutUriFor(this.watchedDoc);
    await vscode.workspace.fs.writeFile(uri, new TextEncoder().encode(layoutCode));
  }

  /** Fetch every node type available in the current project scope
   *  (stdlib + project-local `nodes/`) and ship the catalog to the
   *  webview so the command palette can list them all, even types
   *  the current `main.weft` doesn't reference yet. */

  private async sendGlobalCatalog(): Promise<void> {
    if (!this.watchedDoc) return;
    const docPath = this.watchedDoc.uri.fsPath;
    const lastSep = Math.max(docPath.lastIndexOf('/'), docPath.lastIndexOf('\\'));
    const projectRoot = lastSep > 0 ? docPath.slice(0, lastSep) : undefined;
    const qs = projectRoot ? `?project_root=${encodeURIComponent(projectRoot)}` : '';
    try {
      const response = await this.client.get<{
        catalog: Record<string, unknown>;
        warnings?: string[];
      }>(`/describe/nodes${qs}`);
      this.post({
        kind: 'catalogAll',
        catalog: response.catalog as Record<string, import('./shared/protocol').CatalogEntry>,
      });
    } catch (err) {
      console.warn('[weft/graphView] /describe/nodes failed', err);
    }
  }

  private async sendSettings(): Promise<void> {
    const cfg = vscode.workspace.getConfiguration('weft');
    this.post({
      kind: 'settings',
      parseDebounceMs: cfg.get<number>('parse.debounceMs', 100),
      layoutDebounceMs: cfg.get<number>('layout.debounceMs', 400),
    });
  }

  private onDispose(): void {
    if (this.parseTimer) clearTimeout(this.parseTimer);
    this.stopAllLivePollers();
    this.stopInfraStatusPoller();
    this.stopTriggerStatusPoller();
    // Reset the "did the last parse see infra / triggers?" flags
    // so a subsequent reopen triggers the 0→N transition in
    // syncInfraLivePollers and the pollers restart. Without this
    // a close/reopen cycle leaves the new webview with
    // `isLoading: true` forever (no poller = no status message).
    this.hasInfraNodes = false;
    this.hasTriggerNodes = false;
    for (const d of this.disposables) d.dispose();
    this.disposables = [];
    this.panel = undefined;
    this.watchedDoc = undefined;
  }

  private renderHtml(): string {
    const panel = this.panel!;
    const bundleJs = panel.webview.asWebviewUri(
      vscode.Uri.joinPath(this.context.extensionUri, 'media', 'webview', 'bundle.js'),
    );
    const bundleCss = panel.webview.asWebviewUri(
      vscode.Uri.joinPath(this.context.extensionUri, 'media', 'webview', 'bundle.css'),
    );
    const cspSource = panel.webview.cspSource;
    const nonce = randomNonce();
    return `<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta http-equiv="Content-Security-Policy" content="default-src 'none'; style-src ${cspSource} 'unsafe-inline'; script-src 'nonce-${nonce}' ${cspSource}; img-src ${cspSource} data:; font-src ${cspSource}; connect-src ${cspSource};">
<link rel="stylesheet" href="${bundleCss}">
<title>Weft Graph</title>
<style>html,body,#app{margin:0;padding:0;width:100%;height:100%;overflow:hidden}</style>
</head>
<body>
<div id="app"></div>
<script nonce="${nonce}" src="${bundleJs}"></script>
</body>
</html>`;
  }
}

function randomNonce(): string {
  const chars = 'ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789';
  let out = '';
  for (let i = 0; i < 24; i++) out += chars[Math.floor(Math.random() * chars.length)];
  return out;
}

function isLiveDataItem(v: unknown): v is LiveDataItem {
  if (!v || typeof v !== 'object') return false;
  const o = v as Record<string, unknown>;
  if (typeof o.label !== 'string') return false;
  if (typeof o.data !== 'string' && typeof o.data !== 'number') return false;
  return o.type === 'text' || o.type === 'image' || o.type === 'progress';
}
