// Extension-host side of the graph view. Owns a single WebviewPanel that
// tracks the currently-active .weft document, parses/edits it through the
// long-lived `weft parse-server` (debounced on text change), and persists
// node positions to the project's `layouts/` mirror tree.
//
// The webview never touches `.weft` text: GUI edits arrive as structured
// EditOps which this host runs through the parse-server's edit request (Rust
// is the single source of truth for rewriting `.weft`), writing the returned
// source to the document. The raw code-panel path is gone. Parse and
// describe-nodes go through the local CLI / server (not the dispatcher)
// because they need the project's `nodes/` catalog on the user's machine.

import * as vscode from 'vscode';
import type { DispatcherClient } from './dispatcher';
import { HttpError } from './dispatcher';
import { runWeftJson, projectDirOf } from './cli';
import type { ParseServer } from './parseServer';
import { textTabsForPath } from './tabs';
import type { HostMessage, LiveDataItem, ParseResponse, ProjectDefinition, WebviewMessage } from './shared/protocol';
import * as nodePath from 'node:path';
import { readProjectIdFromToml, findProjectRoot } from './sidebar/projects';

export class GraphViewController {
  private panel: vscode.WebviewPanel | undefined;
  private watchedDoc: vscode.TextDocument | undefined;
  private watchedProjectId: string | undefined;
  /// Include-navigation back-stack. Each frame records the doc the user came
  /// from and the include alias they clicked to descend (used to build the
  /// execution-id prefix so sub-graph journal values render). `openInclude`
  /// pushes; Return pops. The bottom is the project's main.weft.
  private navStack: { doc: vscode.TextDocument; alias: string }[] = [];
  /// Set when the next parse is for a freshly-swapped file (navigation),
  /// so the webview treats its parseResult as a fresh mount (rebuild +
  /// auto-organize if no layout) rather than an in-place edit. Consumed once.
  private freshMount = false;
  private parseTimer: NodeJS.Timeout | undefined;
  private catalogRefreshTimer: NodeJS.Timeout | undefined;
  private disposables: vscode.Disposable[] = [];
  /// The `nodes/` watcher for the currently-watched doc's project.
  /// Rebound whenever the panel follows a .weft file in a different
  /// project (its `nodes/` dir moves with it).
  private nodesWatcher: vscode.Disposable | undefined;
  /// Watches the `@file`/`@include` targets the current view references, so
  /// editing a backing file externally re-parses the graph (file -> graph).
  /// Rebuilt after each parse from the response's fileRefs + include paths.
  private refWatcher: vscode.Disposable | undefined;
  private watchedRefPaths = '';
  /// The current view's `@file` resolution base (the watched file's dir) and
  /// the relative paths it references, so saveFileRef and the watcher reship
  /// contents consistently.
  private fileBaseDir = '';
  private fileRelPaths = new Set<string>();
  /// Monotonic stamp per parse; a result is dropped if a newer parse started
  /// while it was in flight (see triggerParse).
  private parseSeq = 0;
  // Host-side callbacks wired by extension.ts.
  private runHandler: (() => void) | undefined;
  private followTogglePinHandler: (() => void) | undefined;
  private followCatchUpHandler: (() => void) | undefined;
  /// Hooks fired when the user triggers an action that spawns an
  /// execution whose color we don't yet know (activate / infra
  /// start). Extension.ts uses these to tell AutoFollow "next
  /// ExecutionStarted on this project, jump to it."
  private lifecycleStartHandler: (() => void) | undefined;
  private openSourceHandler: (() => void) | undefined;
  /// Stop / Cancel button on the action bar. Extension inspects
  /// the current ActionBarState to decide whether to kill the CLI
  /// process or POST /executions/{color}/cancel.
  private stopActionHandler: (() => void) | undefined;
  /// User dismissed the action-bar error banner. Extension.ts
  /// clears the slot's `error` field via `actionBar.clearError`.
  private dismissErrorHandler: (() => void) | undefined;
  /// Architecture-4: every action-bar verb (activate, deactivate,
  /// resync, infra start/stop/terminate/upgrade) shells out to the
  /// CLI. Extension.ts installs this; graphView calls it with the
  /// verb name + arg list.
  private cliVerbHandler:
    | ((verb: string, args: string[]) => Promise<void>)
    | undefined;
  /// Runs `weft status --json` and pushes drift bits + available
  /// actions into the action-bar state machine. Used on graph
  /// open + after every action + on file-change debounce + on
  /// user-clicked Refresh.
  private cliStatusHandler: (() => Promise<void>) | undefined;
  /// Called whenever the webview signals `ready` (initial mount,
  /// or iframe rebuild after a column move). Lets extension.ts
  /// re-push state that's owned outside graphView (action bar
  /// state, status snapshot); without this, those messages can
  /// race the webview's listener registration and get dropped on
  /// VS Code restart with a .weft already open.
  private readyHandler: (() => void) | undefined;
  /// Called every time a parse succeeds. Extension uses this to
  /// schedule a debounced status refetch so live source edits keep
  /// the action bar's drift signals (source / infra) current.
  private parseSuccessHandler: (() => void) | undefined;
  // One entry per (project, infra node) we're polling /live for.
  // Keyed by nodeId. Cleared on parseResult and dispose. Posts
  // `infraLive` messages to the webview.
  private liveTimers: Map<string, NodeJS.Timeout> = new Map();
  // Same shape, for trigger nodes' signal display info (mount URL,
  // freshly-minted api keys, etc). Keyed by nodeId. Polls
  // `/projects/{id}/signals/{node_id}/display` and posts
  // `signalDisplay` messages. A node is either infra OR trigger;
  // never both, so a node never has both timers.
  private signalDisplayTimers: Map<string, NodeJS.Timeout> = new Map();
  // Interval between polls for infra /live. 3s matches v1; the
  // /live is cheap (returns current state snapshot), so
  // this is fine.
  private readonly liveIntervalMs = 3000;
  // Action bar state, drift, and per-node infra status come from
  // the host's `weft status --json` calls (handled by extension.ts'
  // ActionBarStore). graphView used to run its own infra/trigger
  // pollers; those are gone now because the single status endpoint
  // delivers all the data in one shot.

  constructor(
    private readonly context: vscode.ExtensionContext,
    private readonly client: DispatcherClient,
    private readonly parseServer: ParseServer,
  ) {}

  /** Called by extension.ts so sidebar-initiated runs and action-bar
   *  clicks route through the same business logic. */
  setRunHandler(fn: () => void): void { this.runHandler = fn; }
  setFollowTogglePinHandler(fn: () => void): void { this.followTogglePinHandler = fn; }
  setFollowCatchUpHandler(fn: () => void): void { this.followCatchUpHandler = fn; }
  setLifecycleStartHandler(fn: () => void): void {
    this.lifecycleStartHandler = fn;
  }
  setOpenSourceHandler(fn: () => void): void { this.openSourceHandler = fn; }
  /// Stop / Cancel pressed on the action bar. Extension dispatches
  /// based on whether the bar is in cli_running (kill CLI) or
  /// execution_running (POST /cancel) state.
  setStopActionHandler(fn: () => void): void { this.stopActionHandler = fn; }
  /// X-button on the action-bar error banner. Extension clears
  /// the pinned project's slot.error.
  setDismissErrorHandler(fn: () => void): void { this.dismissErrorHandler = fn; }
  /// Architecture-4: graphView delegates every action-bar verb to
  /// extension.ts via this handler, which shells out to the CLI.
  /// The CLI handles build, hash-skip, registry push, dispatcher
  /// call, and any user prompts.
  setCliVerbHandler(
    fn: (verb: string, args: string[]) => Promise<void>,
  ): void {
    this.cliVerbHandler = fn;
  }
  setReadyHandler(fn: () => void): void {
    this.readyHandler = fn;
  }
  setParseSuccessHandler(fn: () => void): void {
    this.parseSuccessHandler = fn;
  }

  setCliStatusHandler(fn: () => Promise<void>): void {
    this.cliStatusHandler = fn;
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

  /// Bring the graph panel to front (e.g. after a `.weft` click stole focus
  /// to a stray text tab we're about to close).
  reveal(): void {
    this.panel?.reveal();
  }

  /// Path of the `.weft` the graph is currently showing. This tracks include
  /// navigation (it's the navigated-into file, not the project entry), so the
  /// Source button opens the file you're actually looking at.
  currentFilePath(): string | undefined {
    return this.watchedDoc?.uri.fsPath;
  }


  async open(doc: vscode.TextDocument, projectId?: string, keepNavStack = false): Promise<void> {
    // A fresh open (sidebar, command) resets include-navigation; only
    // navigateInto/navigateBack preserve the stack.
    if (!keepNavStack && this.navStack.length > 0) {
      this.navStack = [];
      this.sendNavState();
    }
    // Resolve the project id for this file. Explicit caller arg
    // wins (sidebar pin path); otherwise walk up from the .weft
    // file looking for a `weft.toml` that declares an id. Falling
    // back to undefined leaves the panel without a project id, which
    // breaks every /projects/{id}/... dispatcher endpoint for this
    // panel (live poll, infra status, trigger status).
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
      this.panelTitle(doc),
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
        // Skip our OWN write to this path (the shared writer is mid-edit);
        // path-scoped so an unrelated concurrent write can't unsuppress us.
        if (this.writingPaths.has(e.document.uri.fsPath)) return;
        if (this.watchedDoc && e.document === this.watchedDoc) {
          this.scheduleParse();
          return;
        }
        // A referenced @file target edited in its own editor tab (even
        // unsaved): reship contents so the graph field updates live.
        const rel = nodePath.relative(this.fileBaseDir, e.document.uri.fsPath);
        if (this.fileBaseDir && this.fileRelPaths.has(rel)) {
          void this.shipFileContents(this.fileBaseDir, this.fileRelPaths);
        }
      }),
      vscode.window.onDidChangeActiveTextEditor((ed) => {
        if (ed && ed.document.languageId === 'weft') {
          // Focusing a DIFFERENT .weft tab is a fresh context (a whole new
          // graph), not an include navigation (navigateInto sets watchedDoc to
          // its target before this fires, so that case sees no change here).
          const isDifferentDoc = ed.document !== this.watchedDoc;
          // Drop any include back-stack so the Return button / execPrefix don't
          // dangle against an unrelated graph.
          if (isDifferentDoc && this.navStack.length > 0) {
            this.navStack = [];
            this.sendNavState();
          }
          this.watchedDoc = ed.document;
          // Re-resolve project id for the new file: different
          // .weft files can belong to different projects, and
          // the old watchedProjectId must not leak into the
          // polls we kick off from triggerParse below. If the
          // new file has no weft.toml, clear the id rather than
          // carry the stale previous-project id.
          const newId = readProjectIdFromToml(ed.document.uri.fsPath);
          this.watchedProjectId = newId ?? undefined;
          this.watchNodesDir(ed.document);
          // Switching to a different graph is a fresh mount: rebuild from the
          // new project + its saved layout, not the in-place edit-reconcile
          // path (which would diff the new graph against the old one's
          // positions and stack everything vertically). Refocusing the SAME
          // tab is not a fresh mount (no needless rebuild/relayout).
          if (isDifferentDoc) {
            this.freshMount = true;
            // Retitle to the new project's root folder.
            if (this.panel) this.panel.title = this.panelTitle(ed.document);
          }
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
    this.watchNodesDir(doc);
    // Initial state push.
    this.pushSourceState();
  }

  /// Watch the project's `nodes/` directory. Editing a node's
  /// metadata.json (ports, fields) changes the catalog on disk, but
  /// nothing in the text-change path notices. Without this, the open
  /// graph shows the stale catalog until the file is reopened. On any
  /// change under `nodes/`, re-run the full catalog refresh (palette +
  /// parse), debounced so a multi-file save fires once.
  ///
  /// Rebound when the panel follows a .weft in a different project, so
  /// the watcher always points at the watched doc's own `nodes/`.
  private watchNodesDir(doc: vscode.TextDocument): void {
    this.nodesWatcher?.dispose();
    const root = projectDirOf(doc);
    const watcher = vscode.workspace.createFileSystemWatcher(
      new vscode.RelativePattern(root, 'nodes/**'),
    );
    const onChange = () => this.scheduleCatalogRefresh();
    this.nodesWatcher = vscode.Disposable.from(
      watcher,
      watcher.onDidCreate(onChange),
      watcher.onDidChange(onChange),
      watcher.onDidDelete(onChange),
    );
  }

  /// Ship the content of every `@file` target (so the webview can display
  /// file-backed fields, whose config holds only the marker) and watch all
  /// referenced files so external edits stay live. Two kinds of change:
  ///   - a `@file` backing file changed -> reship contents (the graph shape
  ///     is unchanged; only the displayed value updates). file -> graph.
  ///   - an `@include` target changed -> reparse (the included interface or
  ///     the graph structure may have changed).
  /// `@file` paths are kept relative (the marker's path) so the webview can
  /// look them up by the path in its `@file(...)` tag. Rebuilt only when the
  /// referenced set changes, to avoid watcher churn on every parse.
  private watchReferencedFiles(response: ParseResponse): void {
    const doc = this.watchedDoc;
    if (!doc) return;
    const baseDir = nodePath.dirname(doc.uri.fsPath);
    // relative path (marker form) -> absolute, for the two kinds.
    const fileRel = new Set<string>();
    const includeRel = new Set<string>();
    for (const n of response.project.nodes as Array<{ fileRefs?: Record<string, { path: string }>; includePath?: string }>) {
      if (n.fileRefs) for (const ref of Object.values(n.fileRefs)) fileRel.add(ref.path);
      if (n.includePath) includeRel.add(n.includePath);
    }
    // Record the resolution base + @file set so saveFileRef and the watcher
    // reship from the same source of truth.
    this.fileBaseDir = baseDir;
    this.fileRelPaths = fileRel;
    // Ship current @file contents now (every parse), keyed by relative path.
    void this.shipFileContents(baseDir, fileRel);

    // Key on ABSOLUTE resolved paths, not the relative marker strings: two
    // files in different dirs can reference the same relative path, and a
    // relative-only key would skip the rebuild on navigation, leaving the
    // watchers bound to the previous file's dir (silent file -> graph break).
    const absFile = [...fileRel].map((r) => nodePath.resolve(baseDir, r)).sort();
    const absInclude = [...includeRel].map((r) => nodePath.resolve(baseDir, r)).sort();
    const key = JSON.stringify([absFile, absInclude]);
    if (key === this.watchedRefPaths) return; // set unchanged: keep watchers
    this.watchedRefPaths = key;
    this.refWatcher?.dispose();
    this.refWatcher = undefined;
    if (fileRel.size === 0 && includeRel.size === 0) return;
    const disposables: vscode.Disposable[] = [];
    for (const abs of absFile) {
      const w = vscode.workspace.createFileSystemWatcher(abs);
      // Reship from current instance state (set every parse), not captured
      // locals, so a base-dir change is always honored.
      const onChange = () => this.shipFileContents(this.fileBaseDir, this.fileRelPaths);
      disposables.push(w, w.onDidChange(onChange), w.onDidCreate(onChange), w.onDidDelete(onChange));
    }
    for (const rel of includeRel) {
      const w = vscode.workspace.createFileSystemWatcher(nodePath.resolve(baseDir, rel));
      const onChange = () => this.scheduleParse();
      disposables.push(w, w.onDidChange(onChange), w.onDidCreate(onChange), w.onDidDelete(onChange));
    }
    this.refWatcher = vscode.Disposable.from(...disposables);
  }

  /// Read each `@file` target and post the relative-path -> state map. An
  /// unreadable file ships an `{error}` (no silent omit): a file-backed field
  /// fails loudly rather than falling back to showing the marker as a value.
  private async shipFileContents(baseDir: string, relPaths: Set<string>): Promise<void> {
    const contents: Record<string, { content: string } | { error: string }> = {};
    await Promise.all([...relPaths].map(async (rel) => {
      const resolved = nodePath.resolve(baseDir, rel);
      // Prefer an open editor's live text (reflects in-editor typing even
      // while dirty) over disk, so file -> graph stays live and disk/buffer
      // never disagree.
      const openDoc = vscode.workspace.textDocuments.find((d) => d.uri.fsPath === resolved);
      if (openDoc) {
        contents[rel] = { content: openDoc.getText() };
        return;
      }
      try {
        const data = await vscode.workspace.fs.readFile(vscode.Uri.file(resolved));
        contents[rel] = { content: new TextDecoder().decode(data) };
      } catch (e) {
        contents[rel] = { error: e instanceof Error ? e.message : String(e) };
      }
    }));
    this.post({ kind: 'fileContents', contents });
  }

  private scheduleCatalogRefresh(): void {
    const debounce = vscode.workspace
      .getConfiguration('weft.parse')
      .get<number>('debounceMs', 100);
    if (this.catalogRefreshTimer) clearTimeout(this.catalogRefreshTimer);
    this.catalogRefreshTimer = setTimeout(() => {
      void this.sendGlobalCatalog();
      // The `nodes/` catalog changed: have the warm server rebuild it.
      void this.triggerParse(true);
    }, debounce);
  }

  private pushSourceState(): void {
    if (!this.panel || !this.watchedDoc) return;
    const open = textTabsForPath(this.watchedDoc.uri.fsPath).length > 0;
    this.post({ kind: 'sourceState', open });
  }

  private scheduleParse(): void {
    const debounce = vscode.workspace
      .getConfiguration('weft.parse')
      .get<number>('debounceMs', 100);
    if (this.parseTimer) clearTimeout(this.parseTimer);
    this.parseTimer = setTimeout(() => void this.triggerParse(), debounce);
  }

  private async triggerParse(reloadCatalog = false): Promise<void> {
    if (!this.panel || !this.watchedDoc) return;
    const source = this.watchedDoc.getText();
    const layoutCode = await this.readLayoutCode(this.watchedDoc);
    // Parses run concurrently (many triggers); stamp each and drop a result if
    // a newer parse started while this one was in flight, otherwise a slow
    // older parse could land last and render stale graph or consume freshMount
    // (the navigation/switch rebuild) on the wrong response.
    const seq = ++this.parseSeq;
    try {
      const response = await this.parseServer.request<ParseResponse>({
        kind: 'parse',
        source,
        file: this.watchedDoc.uri.fsPath,
        reloadCatalog,
      });
      if (seq !== this.parseSeq) return; // superseded by a newer parse
      this.applyParseResult(response, source, layoutCode);
    } catch (err) {
      this.post({
        kind: 'parseError',
        error: err instanceof Error ? err.message : String(err),
      });
    }
  }

  /** Render a parse into the webview + sync host-side state from it. The single
   *  post-parse path: a `parse` request feeds it (via triggerParse, behind the
   *  seq guard), and an `edit` feeds the parse the edit-server already returned
   *  (so a GUI edit re-renders from that ONE round-trip, no second parse). */
  private applyParseResult(response: ParseResponse, source: string, layoutCode: string): void {
    // Latch the parsed project id as the authoritative watch id when we don't
    // already have one (weft.toml lookup failed on open but the parse returns a
    // real uuid, e.g. a project the CLI knows from its own weft.toml).
    const nilUuid = '00000000-0000-0000-0000-000000000000';
    if (response.project.id && response.project.id !== nilUuid && !this.watchedProjectId) {
      this.watchedProjectId = response.project.id;
    }
    // Update referenced-file state (fileBaseDir/fileRelPaths + watchers) before
    // posting parseResult, so the edit-reship handler reads state consistent
    // with the parse the webview is rendering. The fileContents post is async
    // (file reads) and lands AFTER parseResult; file-backed fields briefly show
    // "loading…" and the late-arrival $effect in the webview reconciles them.
    this.watchReferencedFiles(response);
    this.post({ kind: 'parseResult', response, source, layoutCode, freshMount: this.freshMount });
    this.freshMount = false;
    this.syncInfraLivePollers(response);
    this.syncSignalDisplayPollers(response);
    this.parseSuccessHandler?.();
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
  private syncInfraLivePollers(response: { project: ProjectDefinition; catalog: Record<string, { requires_infra?: boolean; features?: { isTrigger?: boolean; liveEndpoint?: string } }> }): void {
    const projectId = response.project.id;
    if (!projectId) {
      this.stopAllLivePollers();
      return;
    }
    // Only poll /live for infra nodes whose catalog metadata names a
    // `features.liveEndpoint`. TCP-only infra (Postgres, Redis) leaves
    // it unset and would otherwise return 502 on every tick.
    const infraNodeIds = new Set(
      response.project.nodes
        .filter((n) => {
          const entry = response.catalog[n.nodeType];
          const isInfra = n.requiresInfra ?? entry?.requires_infra ?? false;
          if (!isInfra) return false;
          const liveEndpoint = entry?.features?.liveEndpoint
            ?? n.features?.liveEndpoint;
          return liveEndpoint != null;
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
        this.post({ kind: 'infraLive', nodeId, state: 'ok', items });
      } catch (err) {
        // 404 = infra not provisioned. Anything else (BAD_GATEWAY,
        // network) is a real failure; surface the underlying message.
        const error = err instanceof HttpError && err.status === 404
          ? "Infra not running. Start it from the project's action bar."
          : err instanceof Error ? err.message : String(err);
        this.post({ kind: 'infraLive', nodeId, state: 'error', error });
      }
    };
    void tick();
    return setInterval(() => void tick(), this.liveIntervalMs);
  }

  private stopAllLivePollers(): void {
    for (const timer of this.liveTimers.values()) clearInterval(timer);
    this.liveTimers.clear();
    for (const timer of this.signalDisplayTimers.values()) clearInterval(timer);
    this.signalDisplayTimers.clear();
  }

  /** Mirror of `syncInfraLivePollers` for trigger nodes. Starts a
   *  /display poller per trigger node so the inspector shows the
   *  signal's mount URL + minted plaintext key. The dispatcher
   *  returns 404 until activate registers the signal; we render an
   *  empty items list in that case so the inspector clears stale
   *  data instead of showing it forever.
   */
  private syncSignalDisplayPollers(response: { project: ProjectDefinition; catalog: Record<string, { features?: { isTrigger?: boolean } }> }): void {
    const projectId = response.project.id;
    if (!projectId) {
      for (const timer of this.signalDisplayTimers.values()) clearInterval(timer);
      this.signalDisplayTimers.clear();
      return;
    }
    const triggerNodeIds = new Set(
      response.project.nodes
        .filter((n) => {
          const entry = response.catalog[n.nodeType];
          return n.features?.isTrigger ?? entry?.features?.isTrigger ?? false;
        })
        .map((n) => n.id),
    );
    for (const [id, timer] of this.signalDisplayTimers.entries()) {
      if (!triggerNodeIds.has(id)) {
        clearInterval(timer);
        this.signalDisplayTimers.delete(id);
      }
    }
    for (const id of triggerNodeIds) {
      if (this.signalDisplayTimers.has(id)) continue;
      this.signalDisplayTimers.set(id, this.startSignalDisplayPoller(projectId, id));
    }
  }

  private startSignalDisplayPoller(projectId: string, nodeId: string): NodeJS.Timeout {
    const tick = async () => {
      try {
        const body = await this.client.get<Record<string, unknown>>(
          `/projects/${projectId}/signals/${nodeId}/display`,
        );
        const items = signalDisplayToLiveItems(body);
        this.post({ kind: 'signalDisplay', nodeId, state: 'ok', items });
      } catch (err) {
        // 404 = signal not registered (project not activated, or
        // trigger setup failed). Anything else (listener down,
        // BAD_GATEWAY) is a real failure; surface the message.
        const error = err instanceof HttpError && err.status === 404
          ? "Trigger not registered. Activate the project from the action bar."
          : err instanceof Error ? err.message : String(err);
        this.post({ kind: 'signalDisplay', nodeId, state: 'error', error });
      }
    };
    void tick();
    return setInterval(() => void tick(), this.liveIntervalMs);
  }

  /** Architecture-4 / control-plane unification: every action-bar
   *  verb shells out to the CLI via the cliVerbHandler installed by
   *  extension.ts. The CLI owns build, hash-skip, registry push,
   *  dispatcher call, and confirmation prompts. graphView's role is
   *  reduced to button-routing; the host's ActionBarStore owns all
   *  state transitions and surfaces them via actionBarState.
   */
  /// Trigger a kind-specific action on a signal (e.g. regenerate
  /// an api key). Hits the dispatcher's per-project action proxy;
  /// the listener's kind impl owns the action's payload schema.
  /// On success, force an immediate /display poll so the inspector
  /// reflects the updated state without waiting for the next tick.
  ///
  /// When `confirm` is set, asks the user via VS Code's QuickPick
  /// before invoking. Same UX as the deactivate-mode picker so the
  /// experience stays consistent across destructive actions.
  private async runSignalAction(
    nodeId: string,
    actionKind: string,
    payload: unknown,
    confirm: string | undefined,
  ): Promise<void> {
    const projectId = this.watchedProjectId;
    if (!projectId) return;
    if (confirm) {
      const choice = await vscode.window.showQuickPick(
        [
          { label: 'Confirm', detail: confirm, value: true },
          { label: 'Cancel', detail: 'Abort the action.', value: false },
        ],
        { placeHolder: confirm, ignoreFocusOut: true },
      );
      if (!choice || !choice.value) return;
    }
    try {
      await this.client.post(
        `/projects/${projectId}/signals/${nodeId}/action`,
        { kind: actionKind, payload: payload ?? null },
      );
      // Force-refresh the display poller for this node so the new
      // plaintext key (etc) shows up immediately.
      const timer = this.signalDisplayTimers.get(nodeId);
      if (timer) {
        clearInterval(timer);
        this.signalDisplayTimers.set(
          nodeId,
          this.startSignalDisplayPoller(projectId, nodeId),
        );
      }
    } catch (err) {
      // 409 means the signal's queue already has the maximum
      // submission this token accepts (today: resume signals are
      // capped at one pending answer). Show the user a clean
      // "already received" message instead of a generic HTTP error
      // toast.
      if (err instanceof HttpError && err.status === 409) {
        void vscode.window.showInformationMessage(
          `This submission was already received and is being processed.`,
        );
      } else {
        void vscode.window.showErrorMessage(
          `Signal action '${actionKind}' failed: ${err instanceof Error ? err.message : String(err)}`,
        );
      }
    }
  }

  /// Shared trigger-deactivation picker. Used by the standalone
  /// Deactivate button AND by every infra verb that takes triggers
  /// down as a side effect (Stop / Terminate / Upgrade). One UX
  /// surface, one set of choices.
  ///
  /// Returns CLI flags (`--mode <m> --running-policy <p> [--grace <g>]`)
  /// ready to splice into a dispatchVerb call, or `null` when the
  /// user cancelled any step.
  private async promptTriggerDeactivation(
    verbHint: string,
  ): Promise<string[] | null> {
    const modes: Array<{
      label: string;
      detail: string;
      mode: 'wipe' | 'hibernate' | 'park';
    }> = [
      {
        label: 'Wipe',
        detail:
          'Drop every signal + cancel suspended runs. Reactivate is a fresh boot.',
        mode: 'wipe',
      },
      {
        label: 'Hibernate',
        detail:
          'Park submissions for a grace window, then refuse them. Project hidden from consumer enumeration the entire time.',
        mode: 'hibernate',
      },
      {
        label: 'Park',
        detail:
          'Park submissions indefinitely. Project visible to consumers; submissions drained on reactivate.',
        mode: 'park',
      },
    ];
    const modeChoice = await vscode.window.showQuickPick(modes, {
      placeHolder: `${verbHint}: choose trigger-preservation mode`,
      ignoreFocusOut: true,
    });
    if (!modeChoice) return null;

    let runningPolicy: 'wait' | 'cancel';
    if (modeChoice.mode === 'wipe') {
      // Wipe forces cancel because waiting before wiping is
      // contradictory (you've asked to drop everything anyway).
      runningPolicy = 'cancel';
    } else {
      const runningPolicies: Array<{
        label: string;
        detail: string;
        value: 'wait' | 'cancel';
      }> = [
        {
          label: 'Wait for running executions to finish',
          detail:
            'Status flips to "deactivating": new fires already park; in-flight runs drain naturally; project becomes inactive once the last finishes. You can cancel running anytime.',
          value: 'wait',
        },
        {
          label: 'Cancel running executions immediately',
          detail:
            'Kills every running, non-suspended execution right away. Project flips to inactive at once.',
          value: 'cancel',
        },
      ];
      const runningChoice = await vscode.window.showQuickPick(
        runningPolicies,
        {
          placeHolder: `${verbHint}: how should running executions be handled?`,
          ignoreFocusOut: true,
        },
      );
      if (!runningChoice) return null;
      runningPolicy = runningChoice.value;
    }

    const args = ['--mode', modeChoice.mode, '--running-policy', runningPolicy];
    if (modeChoice.mode === 'hibernate') {
      const grace = await vscode.window.showInputBox({
        prompt:
          'Hibernate grace window (minutes). Submissions arriving after this point are refused.',
        value: '15',
        ignoreFocusOut: true,
        validateInput: (v) => {
          const n = Number(v.trim());
          if (!Number.isInteger(n) || n < 0) {
            return 'Enter a non-negative integer (minutes).';
          }
          return null;
        },
      });
      if (grace === undefined) return null;
      args.push('--grace', String(Number(grace.trim())));
    }
    return args;
  }

  /// Read the project's current lifecycle.status. Failed reads
  /// default to "not active" so the worst case is we skip the
  /// deactivation prompt and let the dispatcher 412 if it disagrees.
  private async projectIsActive(): Promise<boolean> {
    const projectId = this.watchedProjectId;
    if (!projectId) return false;
    try {
      const status = await this.client.get<{ status?: string }>(
        `/projects/${projectId}/status`,
      );
      return status.status === 'active';
    } catch {
      return false;
    }
  }

  /// Project-level infra Stop / Terminate. Surfaces the shared
  /// deactivation picker when the project is Active; otherwise just
  /// dispatches the bare verb.
  private async confirmAndDispatchInfraVerb(
    verb: 'stop' | 'terminate',
  ): Promise<void> {
    const active = await this.projectIsActive();
    let deactivationArgs: string[] = [];
    if (active) {
      const picked = await this.promptTriggerDeactivation(`infra ${verb}`);
      if (!picked) return;
      deactivationArgs = picked;
    }
    void this.dispatchVerb('infra', [verb, ...deactivationArgs]);
  }

  /// Project-level infra Upgrade. Same trigger-deactivation flow as
  /// Stop / Terminate when the project is Active. The project is left
  /// deactivated after the upgrade; the user clicks Activate when ready
  /// (no auto-reactivate: the user is here, it's their call).
  private async confirmAndDispatchInfraUpgrade(): Promise<void> {
    const active = await this.projectIsActive();
    if (!active) {
      void this.dispatchVerb('infra', ['upgrade']);
      return;
    }
    const deactivationArgs = await this.promptTriggerDeactivation('infra upgrade');
    if (!deactivationArgs) return;
    void this.dispatchVerb('infra', ['upgrade', ...deactivationArgs]);
  }

  /// Per-node infra verb (stop / terminate) for partial-state
  /// recovery from the graph context menu. Confirms via QuickPick
  /// then dispatches the CLI verb, which gives the action bar the
  /// usual cli_running overlay + spinner.
  private async confirmAndDispatchPerNodeVerb(
    nodeId: string,
    verb: 'stop' | 'terminate',
  ): Promise<void> {
    const confirm = await vscode.window.showQuickPick(
      [
        {
          label: verb === 'stop' ? 'Stop this node' : 'Terminate this node',
          detail:
            verb === 'stop'
              ? 'Scale ALL its units to 0 (PVCs preserved), even units that would normally stay up on stop (NoOp). Reversible via Start.'
              : 'Delete all resources, including PVCs unless preserved by the spec.',
          value: true,
        },
        { label: 'Cancel', detail: 'Abort the action.', value: false },
      ],
      { placeHolder: `Confirm per-node ${verb}`, ignoreFocusOut: true },
    );
    if (!confirm || !confirm.value) return;
    // Per-node stop from the graph forces: the user explicitly picked
    // one node to take down, so NoOp units come down too (otherwise a
    // right-click stop on a NoOp-only node would silently do nothing).
    const args = verb === 'stop' ? ['node-stop', nodeId, '--force'] : ['node-terminate', nodeId];
    void this.dispatchVerb('infra', args);
  }

  private async runDeactivate(): Promise<void> {
    const args = await this.promptTriggerDeactivation('deactivate');
    if (!args) return;
    void this.dispatchVerb('deactivate', args);
  }

  /// Cancel running while in `deactivating`. Shells out to
  /// `weft cancel-running` so the architecture-4 rule "every action
  /// bar verb goes through the CLI" stays uniform. The CLI POSTs
  /// the dispatcher's `/cancel-running` endpoint; the drain watcher
  /// CASes status to `inactive` once the running set empties.
  /// The lifecycle target the original deactivate wrote stays in
  /// place (mode/visibility/deadline are unchanged).
  private async runCancelRunning(): Promise<void> {
    void this.dispatchVerb('cancel-running', []);
  }

  /// Cancel an in-flight activate (status=Activating). Wipes
  /// partial trigger registrations; CASes status Activating →
  /// Inactive.
  private async runCancelActivate(): Promise<void> {
    void this.dispatchVerb('cancel-activate', []);
  }

  /// Resume Active during `deactivating`. Same activate verb as
  /// the normal flow; the dispatcher's activate handler handles
  /// the "rolling back from deactivating" case naturally (it just
  /// flips lifecycle to active and runs the drain pass against
  /// anything that parked during the transient).
  private async runResumeActive(): Promise<void> {
    void this.dispatchVerb('activate', []);
  }

  private async dispatchVerb(verb: string, args: string[]): Promise<void> {
    if (
      verb === 'activate'
      || verb === 'resync'
      || (verb === 'infra' && (args[0] === 'start' || args[0] === 'upgrade'))
    ) {
      this.lifecycleStartHandler?.();
    }
    // Errors flow through the host's CLI runner: the spawned `weft
    // <verb> --json` emits an `error` phase event; the host's
    // ActionBarStore picks it up and renders an error banner.
    // graphView no longer needs its own try/catch -> reportActionFailure
    // shim because the bar reads error state directly from
    // actionBarState.
    await this.cliVerbHandler?.(verb, args);
    void this.refreshActionAvailability();
  }

  /// Run `weft status --json` via the host. Pulls the latest drift
  /// bits + project status + per-node infra status into the
  /// host's ActionBarStore, which broadcasts to the webview.
  async refreshActionAvailability(): Promise<void> {
    if (!this.cliStatusHandler) return;
    try {
      await this.cliStatusHandler();
    } catch (err) {
      console.warn('[weft] refreshActionAvailability failed', err);
    }
  }

  private onMessage(msg: WebviewMessage): void {
    switch (msg.kind) {
      case 'ready':
        // Webview just booted (initial open OR iframe rebuild
        // after a column move). Re-send the full initial state:
        // catalog, source parse, source-open flag. Don't assume the
        // webview retained anything; column moves destroy the iframe
        // even with retainContextWhenHidden.
        void this.sendGlobalCatalog();
        void this.triggerParse();
        this.pushSourceState();
        // External state (action bar, status snapshot) lives in
        // extension.ts. Hand off so it can re-push.
        this.readyHandler?.();
        break;
      case 'applyEdits':
        void this.applyEditTransaction(msg.requestId, { kind: 'edit', ops: msg.ops });
        break;
      case 'applyTextEdit':
        void this.applyEditTransaction(msg.requestId, { kind: 'applyEdit', textEdit: msg.edit });
        break;
      case 'saveLayout':
        void this.saveLayoutCode(msg.layoutCode);
        break;
      case 'saveFileRef':
        void this.saveFileRef(msg.path, msg.content);
        break;
      case 'openInclude':
        void this.navigateInto(msg.path, msg.alias);
        break;
      case 'navigateBack':
        void this.navigateBack();
        break;
      case 'log':
        console[msg.level]('[weft/webview]', msg.message);
        break;
      case 'runProject':
        this.runHandler?.();
        break;
      case 'infraStart':
        void this.dispatchVerb('infra', ['start']);
        break;
      case 'infraRestart':
        void this.dispatchVerb('infra', ['restart']);
        break;
      case 'infraStop':
        void this.confirmAndDispatchInfraVerb('stop');
        break;
      case 'infraTerminate':
        void this.confirmAndDispatchInfraVerb('terminate');
        break;
      case 'infraNodeStop':
        // Route through the CLI like every other verb so the action
        // bar's `cli_running` overlay fires (gives us the spinner +
        // label). The CLI proxies to the dispatcher's per-node
        // endpoint.
        void this.confirmAndDispatchPerNodeVerb(msg.nodeId, 'stop');
        break;
      case 'infraNodeTerminate':
        void this.confirmAndDispatchPerNodeVerb(msg.nodeId, 'terminate');
        break;
      case 'activateProject':
        void this.dispatchVerb('activate', []);
        break;
      case 'deactivateProject':
        void this.runDeactivate();
        break;
      case 'reactivateProject':
        // Same CLI verb as activate; the CLI's own reactivate-
        // choice prompt picks up via stdin (or the host's
        // maybePromptReactivateChoice round-trips it via JSON).
        void this.dispatchVerb('activate', []);
        break;
      case 'cancelRunning':
        void this.runCancelRunning();
        break;
      case 'cancelActivate':
        void this.runCancelActivate();
        break;
      case 'resumeActive':
        void this.runResumeActive();
        break;
      case 'signalAction':
        void this.runSignalAction(msg.nodeId, msg.actionKind, msg.payload, msg.confirm);
        break;
      case 'dismissError':
        this.dismissErrorHandler?.();
        break;
      case 'resyncProject':
        void this.dispatchVerb('resync', []);
        break;
      case 'infraUpgrade':
        void this.confirmAndDispatchInfraUpgrade();
        break;
      case 'refreshStatus':
        void this.refreshActionAvailability();
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
      case 'stopAction':
        this.stopActionHandler?.();
        break;
    }
  }

  /// Public: extension.ts's runPinned calls this before reading the .weft from
  /// disk, so the build always sees the freshest source even if the user
  /// clicked Run before an in-flight edit's write+reparse finished. Awaits the
  /// watched doc's per-path write chain (the one `applyEditTransaction` uses).
  async waitForPendingSave(): Promise<void> {
    const key = this.watchedDoc?.uri.fsPath;
    const pending = key ? this.pendingWrites.get(key) : undefined;
    if (pending) await pending.catch(() => {});
  }

  /// In-flight full-text write per file (keyed by resolved fsPath). Each write
  /// to a path chains behind the previous one to that path and computes its
  /// replace range AFTER the predecessor lands, so a queued write never
  /// applies a stale range. Used by edit transactions, raw layout/file-ref
  /// writes, and waitForPendingSave (run/activate awaits it before reading disk).
  /// Value is purely a sequencing token (its result is never read), so it's
  /// `Promise<unknown>`: a transaction can resolve to any type (a write is
  /// void, but an edit transaction is free to return its own value).
  private pendingWrites = new Map<string, Promise<unknown>>();
  /// Paths whose open document we are actively editing right now. The
  /// onDidChangeTextDocument handler skips the self-reparse for these, scoped
  /// exactly to our own edit (no shared boolean that overlapping writes can
  /// clobber).
  private writingPaths = new Set<string>();

  /// Replace a document's entire text with `text`, persisted to disk,
  /// serialized per path. If the file is open in an editor its document is
  /// edited (so the open buffer stays in sync, no disk-write-behind-editor
  /// conflict); otherwise fs.writeFile. Returns the chained promise.
  /// Run `fn` serialized on the per-path chain: it starts only after the
  /// previous transaction for `key` has settled, and becomes the new tail.
  /// Both raw writes (layout / file-ref) and edit transactions (read source
  /// -> edit-server -> write) go through here, so an edit always sees the
  /// result of the edit before it (no stale-read-then-clobber race).
  private serializeOnPath<T>(key: string, fn: () => Promise<T>): Promise<T> {
    // Chain off the predecessor's SETTLEMENT, not its value: a rejected
    // predecessor must not poison the chain for later transactions.
    const previous = (this.pendingWrites.get(key) ?? Promise.resolve()).catch(() => {});
    const work = (async () => {
      await previous;
      return fn();
    })();
    this.pendingWrites.set(key, work);
    void work.finally(() => {
      if (this.pendingWrites.get(key) === work) this.pendingWrites.delete(key);
    });
    return work;
  }

  /// Write `text` to a document/file. Caller must hold the path chain (call
  /// inside `serializeOnPath`). Recomputes the range against the live doc.
  private async writeTextRaw(uri: vscode.Uri, text: string): Promise<void> {
    const key = uri.fsPath;
    const openDoc = vscode.workspace.textDocuments.find((d) => d.uri.fsPath === key);
    if (openDoc) {
      if (openDoc.getText() === text) return;
      const end = openDoc.lineAt(openDoc.lineCount - 1).range.end;
      const edit = new vscode.WorkspaceEdit();
      edit.replace(uri, new vscode.Range(0, 0, end.line, end.character), text);
      // Suppress the self-reparse for exactly this edit (scoped to the path).
      this.writingPaths.add(key);
      try {
        await vscode.workspace.applyEdit(edit);
        await openDoc.save();
      } finally {
        this.writingPaths.delete(key);
      }
    } else {
      await vscode.workspace.fs.writeFile(uri, new TextEncoder().encode(text));
    }
  }

  /** Full-text overwrite of a file, serialized on its path chain (so it can't
   *  race a concurrent edit transaction to the same file). Used for raw writes
   *  that aren't edit-ops: a file-backed config field's content (saveFileRef).
   *  Graph edits don't come through here; they go through applyEditTransaction,
   *  which writes via writeTextRaw inside its own serialized chain. */
  private writeDocumentText(uri: vscode.Uri, text: string): Promise<void> {
    return this.serializeOnPath(uri.fsPath, () => this.writeTextRaw(uri, text));
  }

  /// Run a source-edit transaction: feed `req` (an `edit` ops batch or an
  /// `applyEdit` text-edit replay) to the edit-server, write the result, render
  /// it, and reply to the webview with the INVERSE text edit (its undo). The
  /// webview owns the undo stack, so it correlates the reply by `requestId`.
  /// The whole transaction (read source -> server -> write) is serialized on
  /// the per-path chain so a rapid second edit can't read pre-first-edit text
  /// and clobber it.
  private async applyEditTransaction(
    requestId: number,
    req: { kind: 'edit'; ops: import('./shared/protocol').EditOp[] } | { kind: 'applyEdit'; textEdit: import('./shared/protocol').TextEdit },
  ): Promise<void> {
    const doc = this.watchedDoc;
    if (!doc) {
      this.post({ kind: 'editApplied', requestId, ok: false });
      return;
    }
    const key = doc.uri.fsPath;
    try {
      const result = await this.serializeOnPath(key, async () => {
        const openDoc = vscode.workspace.textDocuments.find((d) => d.uri.fsPath === key) ?? doc;
        const r = await this.parseServer.request<{ source: string; parse: ParseResponse; inverse: import('./shared/protocol').TextEdit }>({
          ...req,
          source: openDoc.getText(),
          file: key,
        });
        await this.writeTextRaw(doc.uri, r.source);
        // Suppress the RENDER (not the write) if the user switched `.weft` while
        // this was in flight: applyParseResult reads the live watchedDoc and
        // consumes freshMount, so rendering A after the view moved to B would
        // bind A's refs against B and steal B's rebuild. Same discipline as
        // triggerParse's seq guard. The write already landed on the right doc.
        if (this.watchedDoc === doc) {
          this.parseSeq++; // authoritative result; drop a concurrent stale parse
          this.applyParseResult(r.parse, r.source, await this.readLayoutCode(doc));
        }
        return r.inverse;
      });
      this.post({ kind: 'editApplied', requestId, ok: true, inverse: result });
    } catch (err) {
      this.post({ kind: 'editApplied', requestId, ok: false });
      this.post({ kind: 'parseError', error: err instanceof Error ? err.message : String(err) });
    }
  }

  /// Panel title: the project's root folder name (so it tracks the project,
  /// not the file, and stays stable across nested `.weft` files in the same
  /// project). Falls back to the file name when the doc isn't under a project
  /// root (no `weft.toml`).
  private panelTitle(doc: vscode.TextDocument): string {
    const root = findProjectRoot(doc.uri.fsPath);
    const name = root
      ? nodePath.basename(root)
      : (doc.fileName.split(/[\\/]/).pop() ?? '');
    return `Weft Graph: ${name}`;
  }

  /// Layout files live in a `layouts/` tree at the project root, mirroring
  /// the source path: `<root>/components/cleaner.weft` -> `<root>/layouts/
  /// components/cleaner.layout`. Keeps the source tree clean (no companion
  /// files next to each `.weft`). Falls back to next-to-file only when the
  /// doc isn't under a project root.
  private layoutUriFor(doc: vscode.TextDocument): vscode.Uri {
    const fsPath = doc.uri.fsPath;
    const root = findProjectRoot(fsPath);
    if (!root) {
      return vscode.Uri.file(fsPath.replace(/\.weft$/, '') + '.layout');
    }
    const rel = nodePath.relative(root, fsPath).replace(/\.weft$/, '') + '.layout';
    return vscode.Uri.file(nodePath.join(root, 'layouts', rel));
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
    // The layouts/ mirror tree may not exist yet; fs.writeFile won't create
    // parent dirs, so ensure them first.
    const dir = vscode.Uri.file(nodePath.dirname(uri.fsPath));
    await vscode.workspace.fs.createDirectory(dir);
    await vscode.workspace.fs.writeFile(uri, new TextEncoder().encode(layoutCode));
  }

  /// Write-back for a file-backed config field (`@file("path", Type)`). The
  /// path is project-root-relative (the same root the compiler resolves
  /// against). The resolved path must stay inside the project root, mirroring
  /// the compiler's escape guard; an escaping path is dropped, not written.
  /// After writing, re-parse so the graph reflects the new resolved value.
  private async saveFileRef(relPath: string, content: string): Promise<void> {
    const doc = this.watchedDoc;
    if (!doc) return;
    // `@file` paths resolve against the file's own dir (same base the compiler
    // and watcher use), not the project root: a navigated-in component edits
    // its own relative paths. The result must stay inside the project root.
    const baseDir = nodePath.dirname(doc.uri.fsPath);
    const root = findProjectRoot(doc.uri.fsPath) ?? baseDir;
    const resolved = nodePath.resolve(baseDir, relPath);
    const relToRoot = nodePath.relative(root, resolved);
    if (relToRoot.startsWith('..') || nodePath.isAbsolute(relToRoot)) {
      console.error('[weft] refusing saveFileRef: path escapes project root', relPath);
      return;
    }
    // Serialized full-text write (open-doc-aware, recomputes range after any
    // predecessor) via the shared writer. No graph reparse: config is
    // unchanged; reship the resolved content for display once the write lands.
    await this.writeDocumentText(vscode.Uri.file(resolved), content);
    void this.shipFileContents(this.fileBaseDir, this.fileRelPaths);
  }

  /// Navigate into an `@include`d file: open its graph in this panel and
  /// push the current doc onto the back-stack. The path is project-root
  /// relative (the compiler's resolution root); escaping paths are refused.
  /// One view = one file, so this swaps the watched doc rather than inlining.
  private async navigateInto(relPath: string, alias: string): Promise<void> {
    const current = this.watchedDoc;
    if (!current) return;
    const root = findProjectRoot(current.uri.fsPath);
    if (!root) return;
    const resolved = nodePath.resolve(root, relPath);
    const rel = nodePath.relative(root, resolved);
    if (rel.startsWith('..') || nodePath.isAbsolute(rel)) {
      console.error('[weft] refusing openInclude: path escapes project root', relPath);
      return;
    }
    let target: vscode.TextDocument;
    try {
      target = await vscode.workspace.openTextDocument(vscode.Uri.file(resolved));
    } catch (e) {
      void vscode.window.showErrorMessage(`Weft: cannot open included file ${relPath}: ${e}`);
      return;
    }
    this.navStack.push({ doc: current, alias });
    this.freshMount = true;
    // Send navState BEFORE the parse it depends on: open() posts parseResult
    // (freshMount), which remounts the editor and looks up execution values via
    // execPrefix. navState (computed from the now-updated navStack) must arrive
    // first so that lookup uses the correct prefix on the first render.
    this.sendNavState();
    await this.open(target, undefined, true);
  }

  /// Pop the include back-stack (Return button): reopen the previous file.
  private async navigateBack(): Promise<void> {
    const previous = this.navStack.pop();
    if (!previous) return;
    this.freshMount = true;
    // navState (from the popped navStack) before the parse it feeds, same as
    // navigateInto.
    this.sendNavState();
    await this.open(previous.doc, undefined, true);
  }

  /// Push the current navigation depth, file name, and execution-id prefix to
  /// the webview. The prefix is the dotted alias chain descended through
  /// (e.g. `c.` or `c.inner.`), so sub-graph journal values (keyed by the
  /// fully-qualified id) line up with this file's bare node ids.
  private sendNavState(): void {
    const fileName = this.watchedDoc?.uri.fsPath.split(/[\\/]/).pop() ?? '';
    const execPrefix = this.navStack.map((f) => `${f.alias}.`).join('');
    void this.panel?.webview.postMessage({
      kind: 'navState',
      depth: this.navStack.length,
      fileName,
      execPrefix,
    });
  }

  /** Fetch every node type available in the current project scope
   *  (stdlib + project-local `nodes/`) and ship the catalog to the
   *  webview so the command palette can list them all, even types
   *  the current `main.weft` doesn't reference yet. */

  private async sendGlobalCatalog(): Promise<void> {
    if (!this.watchedDoc) return;
    try {
      const response = await runWeftJson<{
        catalog: Record<string, unknown>;
        warnings?: string[];
      }>(['describe-nodes'], projectDirOf(this.watchedDoc));
      this.post({
        kind: 'catalogAll',
        catalog: response.catalog as Record<string, import('./shared/protocol').CatalogEntry>,
      });
      // Catalog loaded. Clear any prior catalog error, and surface
      // per-node soft warnings (a node mid-rename with bad metadata)
      // so they aren't computed-then-dropped. Its own channel, not
      // parseError: a successful parse must not erase it.
      this.post({ kind: 'catalogError', warnings: response.warnings ?? [] });
    } catch (err) {
      // The full node catalog failed to load (weft not on PATH, a
      // project error, bad JSON). Surface it on the catalog channel,
      // independent of the parse banner: the source may parse fine
      // while the catalog is unavailable, and a later successful parse
      // must not silently clear this.
      this.post({
        kind: 'catalogError',
        error: `node catalog unavailable: ${err instanceof Error ? err.message : String(err)}`,
      });
    }
  }

  private onDispose(): void {
    if (this.parseTimer) clearTimeout(this.parseTimer);
    if (this.catalogRefreshTimer) clearTimeout(this.catalogRefreshTimer);
    this.nodesWatcher?.dispose();
    this.nodesWatcher = undefined;
    this.refWatcher?.dispose();
    this.refWatcher = undefined;
    this.stopAllLivePollers();
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

/// Set of allowed live-data kinds. New kinds: add the string here
/// AND a render branch in ProjectNode.svelte. The type guard rejects
/// anything else so a malformed payload never reaches the renderer.
const LIVE_DATA_TYPES = ['text', 'image', 'progress', 'secret'] as const;
type LiveDataType = (typeof LIVE_DATA_TYPES)[number];
function isLiveDataType(v: unknown): v is LiveDataType {
  return typeof v === 'string' && (LIVE_DATA_TYPES as readonly string[]).includes(v);
}

function isLiveDataItem(v: unknown): v is LiveDataItem {
  if (!v || typeof v !== 'object') return false;
  const o = v as Record<string, unknown>;
  if (typeof o.label !== 'string') return false;
  if (typeof o.data !== 'string' && typeof o.data !== 'number') return false;
  if (!isLiveDataType(o.type)) return false;
  if (o.action !== undefined) {
    if (!o.action || typeof o.action !== 'object') return false;
    const a = o.action as Record<string, unknown>;
    if (typeof a.label !== 'string') return false;
    if (typeof a.actionKind !== 'string') return false;
  }
  return true;
}

/** Convert the listener's signal /display JSON into the
 *  `LiveDataItem[]` shape the trigger node body panel renders.
 *
 *  The listener returns a free-form blob; the inspector knows
 *  about a few standard fields:
 *    - surface.kind  → "PublicEntry" / "TaskCallback"
 *    - surface.path  → for PublicEntry, the mount path
 *    - auth.kind     → "None" / "ApiKey"
 *    - auth.header_name → for ApiKey
 *    - secret        → plaintext, only present when listener still
 *                      holds a freshly-minted key
 */
function signalDisplayToLiveItems(body: Record<string, unknown>): LiveDataItem[] {
  const items: LiveDataItem[] = [];
  const surface = body.surface as Record<string, unknown> | undefined;
  if (surface && surface.kind === 'public_entry') {
    const path = typeof surface.path === 'string' ? surface.path : '';
    items.push({
      type: 'text',
      label: 'Path',
      data: path === '' ? '/' : `/${path.replace(/^\//, '')}`,
    });
  }
  const auth = body.auth as Record<string, unknown> | undefined;
  if (auth && auth.kind === 'api_key') {
    const header = typeof auth.header_name === 'string' ? auth.header_name : 'X-Api-Key';
    items.push({ type: 'text', label: 'Auth header', data: header });
  } else if (auth && auth.kind === 'none') {
    items.push({ type: 'text', label: 'Auth', data: 'public (no key)' });
  }
  if (typeof body.secret === 'string' && body.secret.length > 0) {
    items.push({
      type: 'secret',
      label: 'API key',
      data: body.secret,
      action: {
        label: 'Regenerate',
        actionKind: 'regenerate_api_key',
        confirm: 'Regenerate the API key? The current key will stop working.',
      },
    });
  } else if (auth && auth.kind === 'api_key') {
    // Auth is api_key but the listener doesn't hold plaintext (pod
    // restarted, original mint dropped). Show a placeholder text
    // item with the regenerate button so the user can recover.
    items.push({
      type: 'text',
      label: 'API key',
      data: '(hidden by listener restart; click Regenerate to mint a new one)',
      action: {
        label: 'Regenerate',
        actionKind: 'regenerate_api_key',
        confirm: 'Mint a new API key? Replaces any current key.',
      },
    });
  }
  return items;
}
