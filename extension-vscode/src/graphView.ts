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
import { runWeftJson, docDirOf } from './cli';
import type { ParseServer } from './parseServer';
import { afterTabModelSettles, isReviewDoc, textTabsForPath } from './tabs';
import type { ActionErrorDetails, CatalogEntry, DeactivationSpec, EditOp, ErrorVerb, HostMessage, LiveDataItem, ParseResponse, ProjectDefinition, ResolveSpecResponse, RunSpec, SourceLocation, TextEdit, WebviewMessage } from '../../packages/weft-graph/src/protocol';
import { exampleNameProblem, groupOfCallPath, parseRunSpec, parseSuppliedJson, specToRunArgs } from '../../packages/weft-graph/src/run-spec';
import type { BakeSummary } from '../../packages/weft-graph/src/run-spec';
import * as nodeFs from 'node:fs';
import { typeReferencesFile } from '../../packages/weft-graph/src/protocol';
import { isLiveDataItem, signalDisplayToLiveItems } from '../../packages/weft-graph/src/live-data';
import * as nodePath from 'node:path';
import { readProjectIdFromToml, findProjectRoot } from './sidebar/projects';

export class GraphViewController {
  private panel: vscode.WebviewPanel | undefined;
  private watchedDoc: vscode.TextDocument | undefined;
  private watchedProjectId: string | undefined;
  /// The OBJECT STORE's browser-facing origin (scheme://host:port),
  /// fetched once from the dispatcher at panel boot. The webview CSP
  /// allows it in img-src/media-src so an <img>/<video> streams file
  /// bytes directly from the box (range requests, seeking). Empty if
  /// the fetch failed (older dispatcher / offline): previews then
  /// can't load, which surfaces as the node's fallback rather than a
  /// silent break.
  private storageOrigin = '';
  /// Include-navigation back-stack. Each frame records the doc the user came
  /// from and the include alias they clicked to descend (used to build the
  /// execution-id prefix so sub-graph journal values render). `openInclude`
  /// pushes; Return pops. The bottom is the project's `src/main.weft`.
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
  /// Watches the watched `.weft` file ITSELF on disk (see watchSelfFile).
  private selfWatcher: vscode.Disposable | undefined;
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
  private runHandler: ((targets: string[]) => void) | undefined;
  /// Told the group the user stands in (an include's alias chain, or
  /// null at the top level) on every navigation, so the Executions view
  /// marks the runs scoped to it.
  private navHandler: ((focusedGroup: string | null) => void) | undefined;
  private followTogglePinHandler: (() => void) | undefined;
  private followCatchUpHandler: (() => void) | undefined;
  private followClearHandler: (() => void) | undefined;
  private openSourceHandler: ((location?: SourceLocation) => void) | undefined;
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
  /// The version-tree verbs (branch, checkpoint), which print one JSON
  /// line and are NOT action-bar verbs: they have no `ActionVerb` tag,
  /// so routing them through `cliVerbHandler` throws before the CLI is
  /// even spawned and the click does nothing. Extension.ts installs the
  /// same runner its own tree commands use.
  private treeVerbHandler:
    | ((args: string[]) => Promise<unknown>)
    | undefined;
  /// Called whenever the webview signals `ready` (initial mount,
  /// or iframe rebuild after a column move). Lets extension.ts
  /// re-push state that's owned outside graphView (action bar
  /// state, status snapshot); without this, those messages can
  /// race the webview's listener registration and get dropped on
  /// VS Code restart with a .weft already open.
  private readyHandler: (() => void) | undefined;
  // One entry per (project, infra node) we're polling /live for.
  // Keyed by nodeId. Cleared on parseResult and dispose. Posts
  // `infraLive` messages to the webview.
  private liveTimers: Map<string, NodeJS.Timeout> = new Map();
  /// The infra nodes whose container serves `/live` (and `/action`), as
  /// of the last parse: a press on one of their buttons goes to the
  /// container, every other button to the listener holding a signal.
  /// Every infra node of the parsed project. This is what says WHERE a
  /// body-panel button goes (the container behind `/infra`, or the
  /// listener holding a signal), which is a fact of the node.
  private infraNodeIds: Set<string> = new Set();
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
  setRunHandler(fn: (targets: string[]) => void): void { this.runHandler = fn; }
  setNavHandler(fn: (focusedGroup: string | null) => void): void { this.navHandler = fn; }

  /** Tell the graph which version the followed run ran and which one
   *  the files on disk are, so it can say when they differ.
   *
   *  Remembered, and re-sent by the ready handler, because the panel may
   *  not be listening yet: clicking "view in graph" on a run of a project
   *  that is not open creates the panel and posts this immediately after,
   *  which the webview never saw, so the "this run is from older code"
   *  banner never appeared on exactly the path it was written for. */
  setExecVersion(color: string, version: string | null, diskVersion: string | null): void {
    this.execVersionFor = this.watchedProjectId;
    this.lastExecVersion = { kind: 'execVersion', color, version, diskVersion };
    void this.panel?.webview.postMessage(this.lastExecVersion satisfies HostMessage);
  }

  /** Re-send the last version message, for a panel that has just mounted.
   *
   *  Only for the PROJECT it was about. The controller outlives both the
   *  panel and the file it shows, so an unguarded re-send put a banner
   *  naming one project's run over another project's graph, with nothing
   *  to clear it (the webview only drops the version when a new run
   *  starts). The project and not the file, because the banner is about
   *  the RUN: navigating into an include changes the file and follows the
   *  same run, so the banner has to survive that. */
  resendExecVersion(): void {
    if (this.lastExecVersion && this.execVersionFor === this.watchedProjectId) {
      void this.panel?.webview.postMessage(this.lastExecVersion satisfies HostMessage);
    }
  }

  /** Drop the remembered run banner, and take it off a live panel.
   *
   *  Revealing the panel for another project does not tear it down, so
   *  the guard on the re-send is not enough on its own: the webview is
   *  still showing the old project's banner and nothing else would ever
   *  clear it. A version of `null` is how the webview is told there is no
   *  followed run here. */
  forgetExecVersion(): void {
    if (!this.lastExecVersion) return;
    const color = this.lastExecVersion.color;
    this.lastExecVersion = undefined;
    this.execVersionFor = undefined;
    void this.panel?.webview.postMessage({
      kind: 'execVersion',
      color,
      version: null,
      diskVersion: null,
    } satisfies HostMessage);
  }
  private lastExecVersion: { kind: 'execVersion'; color: string; version: string | null; diskVersion: string | null } | undefined;
  /// Which project `lastExecVersion` describes a run of.
  private execVersionFor: string | undefined;
  setFollowTogglePinHandler(fn: () => void): void { this.followTogglePinHandler = fn; }
  setFollowCatchUpHandler(fn: () => void): void { this.followCatchUpHandler = fn; }
  setFollowClearHandler(fn: () => void): void { this.followClearHandler = fn; }
  setOpenSourceHandler(fn: (location?: SourceLocation) => void): void { this.openSourceHandler = fn; }
  /// Stop / Cancel pressed on the action bar. Extension dispatches
  /// based on whether the bar is in cli_running (kill CLI) or
  /// execution_running (POST /cancel) state.
  setStopActionHandler(fn: () => void): void { this.stopActionHandler = fn; }
  /// X-button on the action-bar error banner. Extension clears
  /// the pinned project's slot.error.
  setDismissErrorHandler(fn: () => void): void { this.dismissErrorHandler = fn; }
  /// Surface a non-CLI failure (parse, catalog, edit, ...) on the
  /// action-bar error banner. Extension wires this to
  /// ActionBarStore.setError for the watched project. Optional: when
  /// the graph view has no resolved project id yet (early parse
  /// failure on open), the handler can no-op.
  setReportErrorHandler(
    fn: (verb: ErrorVerb, message: string, details?: ActionErrorDetails) => void,
  ): void {
    this.reportErrorHandler = fn;
  }
  private reportErrorHandler?: (
    verb: ErrorVerb,
    message: string,
    details?: ActionErrorDetails,
  ) => void;

  /// Resolve a previously-reported system-side error. Extension wires
  /// this to ActionBarStore.clearErrorIfVerb so a successful parse /
  /// catalog load clears the sticky banner its own prior failure raised
  /// (the user never dismisses a parse error by hand: every half-typed
  /// keystroke fails, so the source has to clear it on its next success).
  setResolveErrorHandler(
    fn: (verb: ErrorVerb) => void,
  ): void {
    this.resolveErrorHandler = fn;
  }
  private resolveErrorHandler?: (verb: ErrorVerb) => void;
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

  setCliStatusHandler(fn: () => Promise<void>): void {
    this.cliStatusHandler = fn;
  }

  setTreeVerbHandler(fn: (args: string[]) => Promise<unknown>): void {
    this.treeVerbHandler = fn;
  }

  /** Public so execFollower can push events into the panel. */
  post(msg: HostMessage): void {
    if (msg.kind === 'execReset') this.forgetExecVersion();
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
    // A banner about another project's run goes now, before the graph
    // under it changes.
    if (this.execVersionFor !== undefined && this.execVersionFor !== this.watchedProjectId) {
      this.forgetExecVersion();
    }
    // Graph takes ViewColumn.Active so the .weft text doesn't
    // show by default. The "Source" button opens the text in
    // ViewColumn.Beside (column 2). We don't try to swap them
    // because moving a webview between columns destroys the
    // iframe (microsoft/vscode#141001).
    if (this.panel) {
      this.panel.reveal(vscode.ViewColumn.Active);
      this.watchedDoc = doc;
      this.watchNodesDir(doc);
      this.watchSelfFile(doc);
      await this.triggerParse();
      return;
    }

    // Learn the media origin before rendering the CSP, so an
    // <img>/<video> can stream the bytes a minted link points at.
    this.loadStorageOrigin();

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
        if (this.watchedDoc && e.document === this.watchedDoc) {
          // Skip when the doc already matches what we rendered: our own edit write,
          // the save pipeline's trim/newline follow-ups, and an undo back to the
          // rendered text all leave the doc equal to `lastRendered` and need no
          // reparse (the graph isn't stale). A genuine edit differs and reparses.
          if (this.isRenderCurrent()) return;
          // A genuine EXTERNAL change (text-tab typing, AI streaming): tell the
          // webview so it engages its auto-lock on source-mutating graph
          // gestures. Re-posted per keystroke; the lock window slides forward.
          // Skip our OWN edit writes: their change events land here (the doc
          // differs from `lastRendered` until applyParseResult runs), but
          // auto-locking the user right after their own GUI edit is wrong. The
          // selfWriteDepth bracket scopes exactly our applyEdit + save events.
          if (this.selfWriteDepth === 0) this.post({ kind: 'codeEditTouched' });
          this.scheduleParse('watched-text');
          return;
        }
        const key = e.document.uri.fsPath;
        // A referenced @file target edited in its own editor tab (even
        // unsaved): reship contents so the graph field updates live.
        const rel = nodePath.relative(this.fileBaseDir, e.document.uri.fsPath);
        if (this.fileBaseDir && this.fileRelPaths.has(rel)) {
          void this.shipFileContents(this.fileBaseDir, this.fileRelPaths);
        }
      }),
      vscode.window.onDidChangeActiveTextEditor(async (ed) => {
        // Reading a diff is reviewing: the open graph keeps showing what it
        // was showing rather than repointing at a comparison. One tick so
        // the tab model reflects this focus change, then only proceed if
        // the editor is still the active one.
        if (!ed) return;
        await afterTabModelSettles();
        // The panel can tear down during the wait; resuming would
        // register a nodes-dir watcher onDispose already swept.
        if (this.panel === undefined) return;
        if (vscode.window.activeTextEditor !== ed) return;
        if (ed.document.languageId === 'weft' && !isReviewDoc(ed.document)) {
          // Focusing a DIFFERENT .weft tab is a fresh context (a whole new
          // graph), not an include navigation (navigateInto sets watchedDoc to
          // its target before this fires, so that case sees no change here).
          const isDifferentDoc = ed.document !== this.watchedDoc;
          // Drop any include back-stack so the Return button / call path don't
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
          this.watchSelfFile(ed.document);
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
          // Refocusing the SAME tab with a (settled) current render has
          // nothing to parse: skip, same invariant as the debounce
          // ("reparse iff the render is stale"). This is not just waste:
          // the parseResult echo can race an edit still in flight, and its
          // truth then ALREADY contains that edit's effect while the op is
          // still pending, so the refold rolls the op back as a duplicate
          // of its own work ("id already exists in scope" on a gesture
          // that succeeded). triggerParse awaits the write chain before
          // deciding, so the skip judges settled text, not a mid-write
          // snapshot. A genuinely stale render still parses.
          void this.triggerParse(false, !isDifferentDoc);
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
    this.watchSelfFile(doc);
    // Initial state push.
    this.pushSourceState();
  }

  /// Watch the watched `.weft` file on disk. `onDidChangeTextDocument`
  /// only reports edits to a TextDocument VS Code still holds; once the
  /// source tab is closed, VS Code detaches that document at a moment
  /// nothing here controls, and a detached document's text is frozen. An
  /// agent writing `src/main.weft` while only the graph is open then changes
  /// nothing the change event can see, and the graph shows the old program
  /// until the source tab is opened by hand. This watcher is the other ear:
  /// a write on disk re-latches a live document (liveDoc re-opens the file
  /// when the held one is detached) and reparses through the same debounce
  /// and the same "skip if the render is current" gate as a typed edit, so
  /// the extension's own writes, which land on disk too, are not reparsed
  /// twice: their text already matches the render by the time this fires.
  ///
  /// Rebound when the panel follows a .weft in a different project, like
  /// the nodes-dir watcher.
  private watchSelfFile(doc: vscode.TextDocument): void {
    this.selfWatcher?.dispose();
    const watcher = vscode.workspace.createFileSystemWatcher(
      new vscode.RelativePattern(nodePath.dirname(doc.uri.fsPath), nodePath.basename(doc.uri.fsPath)),
    );
    const onDiskChange = () => {
      void (async () => {
        if (!this.watchedDoc || this.watchedDoc.uri.fsPath !== doc.uri.fsPath) return;
        await this.liveDoc(this.watchedDoc);
        if (this.isRenderCurrent()) return;
        // An external process wrote the file: the same auto-lock a text-tab
        // edit engages, and never for our own writes (see selfWriteDepth).
        if (this.selfWriteDepth === 0) this.post({ kind: 'codeEditTouched' });
        this.scheduleParse('watched-text');
      })();
    };
    this.selfWatcher = vscode.Disposable.from(
      watcher,
      watcher.onDidChange(onDiskChange),
      watcher.onDidCreate(onDiskChange),
    );
  }

  /// Watch the project's `nodes/` directory. Editing a node's
  /// metadata.json (ports, fields) changes the catalog on disk, but
  /// nothing in the text-change path notices. Without this, the open
  /// graph shows the stale catalog until the file is reopened. On any
  /// change under `nodes/`, re-run the full catalog refresh (palette +
  /// parse), debounced so a multi-file save fires once.
  ///
  /// Rebound when the panel follows a .weft in a different project.
  private watchNodesDir(doc: vscode.TextDocument): void {
    this.nodesWatcher?.dispose();
    // The PROJECT root, never the document's own directory: `nodes/`
    // sits beside `weft.toml`, and a program lives in `src/`, so
    // watching the doc's folder watched `src/nodes/**`, which does not
    // exist. Nothing ever fired, the warm parse server kept the catalog
    // it built at spawn, and a node package written after the window
    // opened stayed unknown until the window was reloaded.
    const root = findProjectRoot(doc.uri.fsPath);
    if (!root) return;
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
    // Two anchors, the compiler's (`weft_compiler::file_reader`): an
    // `@include` path is relative to the file that writes it, while a
    // `@file` / `@asset` path is relative to the PROJECT ROOT wherever
    // it is written, so a navigated-in included file reads the same
    // asset the program does. Outside any project the doc's own dir
    // stands in for both.
    const baseDir = nodePath.dirname(doc.uri.fsPath);
    const rootDir = findProjectRoot(doc.uri.fsPath) ?? baseDir;
    // relative path (marker form) -> absolute, for the two kinds.
    const fileRel = new Set<string>();
    const includeRel = new Set<string>();
    // Include paths already expressed from the project root, which is how
    // a nested `@include` can be named at all.
    const includeRootRel = new Set<string>();
    for (const n of response.project.nodes as Array<{
      fileRefs?: Record<string, { path: string; type: string }>;
      includePath?: string;
      includeContents?: { files?: string[] };
    }>) {
      // Only TEXT refs: a media ref's bytes are never read as text content
      // (they'd ship an image as garbage, and the editor's content-save path
      // would clobber the media file).
      if (n.fileRefs) {
        for (const ref of Object.values(n.fileRefs)) {
          if (!typeReferencesFile(ref.type)) fileRel.add(ref.path);
        }
      }
      if (n.includePath) includeRel.add(n.includePath);
      // Files reached THROUGH that include, nested ones too, each already
      // relative to the project root (an `@include` path is relative to
      // the file that wrote it, so a nested one is meaningless against
      // this file's directory). Without these, editing a deeply included
      // file left the graph showing the old parse.
      for (const rel of n.includeContents?.files ?? []) includeRootRel.add(rel);
    }
    // Record the resolution base + @file set so saveFileRef and the watcher
    // reship from the same source of truth.
    this.fileBaseDir = rootDir;
    this.fileRelPaths = fileRel;
    // Ship current @file contents now (every parse), keyed by relative path.
    void this.shipFileContents(rootDir, fileRel);

    // Key on ABSOLUTE resolved paths, not the relative marker strings: two
    // files in different dirs can reference the same relative path, and a
    // relative-only key would skip the rebuild on navigation, leaving the
    // watchers bound to the previous file's dir (silent file -> graph break).
    const absFile = [...fileRel].map((r) => nodePath.resolve(rootDir, r)).sort();
    // Two anchors, one set: a top-level `@include` is written relative to
    // this file, while the files reached through it come back relative to
    // the project root. Both watch the same way.
    const absInclude = [
      ...new Set([
        ...[...includeRel].map((r) => nodePath.resolve(baseDir, r)),
        ...[...includeRootRel].map((r) => nodePath.resolve(rootDir, r)),
      ]),
    ].sort();
    const key = JSON.stringify([absFile, absInclude]);
    if (key === this.watchedRefPaths) return; // set unchanged: keep watchers
    this.watchedRefPaths = key;
    this.refWatcher?.dispose();
    this.refWatcher = undefined;
    if (absFile.length === 0 && absInclude.length === 0) return;
    const disposables: vscode.Disposable[] = [];
    for (const abs of absFile) {
      const w = vscode.workspace.createFileSystemWatcher(abs);
      // Reship from current instance state (set every parse), not captured
      // locals, so a base-dir change is always honored.
      const onChange = () => this.shipFileContents(this.fileBaseDir, this.fileRelPaths);
      disposables.push(w, w.onDidChange(onChange), w.onDidCreate(onChange), w.onDidDelete(onChange));
    }
    for (const abs of absInclude) {
      const w = vscode.workspace.createFileSystemWatcher(abs);
      // A dependency change: the watched doc's TEXT is untouched, so the
      // debounce's render-current skip must not apply (the render is stale
      // through the include, not the text).
      const onChange = () => this.scheduleParse('dependency');
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

  /// The strongest reason among the schedules coalesced into the pending
  /// debounce. 'dependency' is sticky: once an @include target changed, the
  /// parse MUST run even if a later watched-text schedule (whose skip check
  /// would pass) resets the timer.
  private pendingParseReason: 'watched-text' | 'dependency' | null = null;

  /// Debounced reparse. `reason` decides whether the parse is skippable at
  /// fire time:
  /// - 'watched-text': the watched doc's text changed. Skipped when the doc
  ///   matches the last render BY THEN: the change event fires
  ///   mid-transaction for our own edit writes (the doc differs from the
  ///   pre-edit render at event time), but by fire time the transaction has
  ///   rendered the post-edit truth. Re-parsing then is a pure echo of what
  ///   the edit reply already delivered, and its parseResult races the
  ///   webview's async layout persist (adopting a pre-persist layout snaps
  ///   just-placed nodes away). "Reparse iff the render is stale", applied
  ///   where the parse actually starts.
  /// - 'dependency': something the parse READS changed (an @include target)
  ///   without touching the watched doc's text, so the render is stale even
  ///   though the text matches it. Never skipped.
  private scheduleParse(reason: 'watched-text' | 'dependency'): void {
    const debounce = vscode.workspace
      .getConfiguration('weft.parse')
      .get<number>('debounceMs', 100);
    this.pendingParseReason = reason === 'dependency' ? 'dependency' : (this.pendingParseReason ?? 'watched-text');
    if (this.parseTimer) clearTimeout(this.parseTimer);
    this.parseTimer = setTimeout(() => {
      this.parseTimer = undefined;
      const pending = this.pendingParseReason ?? 'watched-text';
      this.pendingParseReason = null;
      void this.triggerParse(false, pending === 'watched-text');
    }, debounce);
  }

  /// `skipIfCurrent`: the caller's trigger was "the watched text changed",
  /// so a doc that (once settled) still matches the last render has nothing
  /// to reparse. Callers whose trigger makes the render stale regardless of
  /// text (a dependency changed, a doc switch, a catalog reload) leave it
  /// false.
  private async triggerParse(reloadCatalog = false, skipIfCurrent = false): Promise<void> {
    if (!this.panel || !this.watchedDoc) return;
    // Resolve to a live doc: with the source tab closed, watchedDoc may be
    // a detached instance whose text froze at close time (see liveDoc).
    const doc = await this.liveDoc(this.watchedDoc);
    // Parse only SETTLED text. An edit transaction writes the doc mid-flight;
    // a parse fired from that write's own change event would read the
    // post-write text and race the transaction's render: its parseResult can
    // land in the webview BEFORE the edit reply, adopting truth that already
    // contains the pending op's effect, which rolls the op back with a false
    // "not found" toast on a gesture that succeeded. Await the per-path write
    // chain (loop: a settled entry may not have unregistered yet, and a new
    // write may have chained on), then decide staleness on the settled doc.
    let awaited: Promise<unknown> | undefined;
    for (
      let chain = this.pendingWrites.get(doc.uri.fsPath);
      chain && chain !== awaited;
      chain = this.pendingWrites.get(doc.uri.fsPath)
    ) {
      awaited = chain;
      await chain.catch(() => {});
    }
    if (skipIfCurrent && this.isRenderCurrent()) return;
    const source = doc.getText();
    const layoutCode = await this.readLayoutCode(doc);
    // Parses run concurrently (many triggers); stamp each and drop a result if
    // a newer parse started while this one was in flight, otherwise a slow
    // older parse could land last and render stale graph or consume freshMount
    // (the navigation/switch rebuild) on the wrong response.
    const seq = ++this.parseSeq;
    try {
      const response = await this.parseServer.request<ParseResponse>({
        kind: 'parse',
        source,
        file: doc.uri.fsPath,
        reloadCatalog,
      });
      if (seq !== this.parseSeq) return; // superseded by a newer parse
      this.applyParseResult(response, source, { code: layoutCode });
    } catch (err) {
      // Same seq guard as the success path. A stale parse that errors
      // after a fresher parse already succeeded must not overwrite the
      // fresh state: the user would see "parse error" while the graph
      // shows the correct fresh result.
      if (seq !== this.parseSeq) return;
      const message = err instanceof Error ? err.message : String(err);
      this.post({
        kind: 'parseError',
        error: message,
      });
      // Also surface the parse failure on the action-bar so the user
      // can open the details modal. The inline `parse error: ...`
      // banner inside the graph stays for in-context feedback.
      const file = this.watchedDoc?.uri.fsPath;
      this.reportErrorHandler?.('parse', message, {
        what: file ? `Parsing ${nodePath.basename(file)}` : 'Parsing the project',
        stage: 'parse',
        diagnostics: [{ severity: 'error', message }],
        ...(err instanceof Error && err.stack ? { raw: err.stack } : {}),
      });
    }
  }

  /** Render a parse into the webview + sync host-side state from it. The single
   *  post-parse path: a `parse` request feeds it (via triggerParse, behind the
   *  seq guard), and an `edit` feeds the parse the edit-server already returned
   *  (so a GUI edit re-renders from that ONE round-trip, no second parse).
   *  `layoutCode` is consumed ONLY by the posted `parseResult`, so it exists
   *  only on the posting variant of the call: the edit path passes none (its
   *  truth rides the `editApplied` reply, and reading the layout there would
   *  observe the pre-gesture file, the webview's persist for the very gesture
   *  being applied not having arrived yet). */
  private applyParseResult(response: ParseResponse, source: string, layout?: { code: string }): void {
    // The parse succeeded: clear any sticky parse-error banner a prior
    // (half-typed) keystroke raised. The user fixes their code and the
    // graph renders, so the banner the failure put up must come down on
    // its own (an error raised by a system-side source is cleared by
    // that source's next success, not by a manual dismiss).
    this.resolveErrorHandler?.('parse');
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
    // Record the watched doc's SETTLED text (post-save: trim / final-newline
    // participants have already run by the time we render, both on the edit path
    // (writeTextRaw awaited save before this) and the parse path (source IS
    // doc.getText())). The change handler skips a reparse when the doc still equals
    // this. Using the live doc text rather than the rendered `source` (the parser's
    // re-emit, which is pre-trim on the edit path) is what makes the skip match the
    // doc's post-trim change event, so a GUI edit on a file that trim-on-save
    // mutates doesn't fire one spurious reparse. (The graph is identical for
    // whitespace-only differences, so skipping is correct.)
    this.lastRendered = this.watchedDoc
      ? { path: this.watchedDoc.uri.fsPath, text: this.watchedDoc.getText() }
      : null;
    // An edit-fed render hands the webview its truth inside the `editApplied`
    // reply instead (one message, no double render); only parse-fed renders
    // (which carry the layout) post `parseResult`.
    if (layout) {
      this.post({ kind: 'parseResult', response, source, layoutCode: layout.code, freshMount: this.freshMount });
    }
    this.freshMount = false;
    this.syncInfraLivePollers(response);
    this.syncSignalDisplayPollers(response);
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
  private syncInfraLivePollers(response: Pick<ParseResponse, 'project' | 'catalog'>): void {
    const projectId = response.project.id;
    if (!projectId) {
      this.stopAllLivePollers();
      // Nothing is parsed any more, so the set describes nothing.
      // Leaving it behind would route this project's buttons by the
      // last project's answers.
      this.infraNodeIds = new Set();
      return;
    }
    const isInfraNode = (n: ParseResponse['project']['nodes'][number]): boolean =>
      n.requiresInfra ?? response.catalog[n.nodeType]?.requires_infra ?? false;
    this.infraNodeIds = new Set(response.project.nodes.filter(isInfraNode).map((n) => n.id));
    // Only poll /live for infra nodes whose catalog metadata names a
    // `features.liveEndpoint`. TCP-only infra (Postgres, Redis) leaves
    // it unset and would otherwise return 502 on every tick.
    const infraNodeIds = new Set(
      response.project.nodes
        .filter((n) => {
          if (!isInfraNode(n)) return false;
          const liveEndpoint = response.catalog[n.nodeType]?.features?.liveEndpoint
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
        // 404 = the infra endpoint does not exist for this node yet
        // (not provisioned). A distinct RESTING state, never collapsed
        // into a healthy empty list: the webview renders it as its own
        // affordance, and stale items from a previous run clear.
        // Anything else (BAD_GATEWAY, network) is a real failure;
        // surface the underlying message.
        if (err instanceof HttpError && err.status === 404) {
          this.post({ kind: 'infraLive', nodeId, state: 'absent' });
          return;
        }
        const error = err instanceof Error ? err.message : String(err);
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
  private syncSignalDisplayPollers(response: Pick<ParseResponse, 'project' | 'catalog'>): void {
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
        // 404 = nothing is listening for this trigger: the project is
        // not activated, its trigger setup never registered, or the
        // listener that held it is gone or has forgotten it (the
        // dispatcher folds all of those into one 404). A distinct
        // RESTING state, never collapsed into a healthy empty list;
        // stale items from a previous activation clear. Anything else
        // (the dispatcher unreachable, BAD_GATEWAY) is a real failure;
        // surface it.
        if (err instanceof HttpError && err.status === 404) {
          this.post({ kind: 'signalDisplay', nodeId, state: 'absent' });
          return;
        }
        const error = err instanceof Error ? err.message : String(err);
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
    // A button belongs to an infra node's own container, reached
    // behind `/live`. Triggers used to have one too, for regenerating
    // a key the listener minted; that whole mechanism is gone (a key
    // now lives on a connection), so a signal has no action to press
    // and the dispatcher no longer offers a door for one.
    if (!this.infraNodeIds.has(nodeId)) return;
    try {
      await this.client.post(
        `/projects/${projectId}/infra/nodes/${nodeId}/action`,
        { kind: actionKind, payload: payload ?? null },
      );
      // Force-refresh the node's poller so what the press changed (a
      // fresh QR code, a new address) shows up immediately.
      const timer = this.liveTimers.get(nodeId);
      if (timer) {
        clearInterval(timer);
        this.liveTimers.set(nodeId, this.startLivePoller(projectId, nodeId));
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
          `Action '${actionKind}' failed: ${err instanceof Error ? err.message : String(err)}`,
        );
      }
    }
  }

  /// CLI flags for a trigger-deactivation spec chosen in the SHARED
  /// webview picker (`DeactivationPicker`). The picker owns the UX +
  /// business rules (wipe forces cancel, grace only for hibernate,
  /// drain cap only with wait); this host just translates the spec
  /// into `weft` flags.
  private deactivationFlags(spec: DeactivationSpec): string[] {
    const args = ['--mode', spec.mode, '--running-policy', spec.runningPolicy];
    if (spec.mode === 'hibernate' && spec.graceMinutes !== undefined) {
      args.push('--grace', String(spec.graceMinutes));
    }
    if (spec.drainTimeoutSecs !== undefined) {
      args.push('--drain-timeout', String(spec.drainTimeoutSecs));
    }
    return args;
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
    // Follow-latest for the lifecycle verbs is armed by the host, past
    // every refusal (the in-flight guard, the pre-flight gate): an
    // aborted click must not reset the user's pinned execution view.
    //
    // Errors flow through the host's CLI runner: the spawned `weft
    // <verb> --json` emits an `error` phase event; the host's
    // ActionBarStore picks it up and renders an error banner.
    // graphView no longer needs its own try/catch -> reportActionFailure
    // shim because the bar reads error state directly from
    // actionBarState.
    await this.cliVerbHandler?.(verb, args);
    void this.refreshActionAvailability();
  }

  /// Put the files on disk onto another version. A tree verb, not an
  /// action-bar verb: it runs outside the bar's pump and reports its own
  /// failure, the same way the sidebar's "branch here" does.
  private async branchTo(reference: string): Promise<void> {
    if (!this.treeVerbHandler) return;
    const out = await this.treeVerbHandler(['branch', reference]);
    if (out) {
      void vscode.window.showInformationMessage(
        `Weft: branched to ${reference.slice(0, 8)}; the files on disk are that version now.`,
      );
    }
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
      case 'resyncSource':
        void this.resyncSource(msg.requestId);
        break;
      case 'saveLayout':
        // Ack ONLY on success: an un-acked save tells the editor its copy is
        // ahead of disk, so it keeps refusing stale layout echoes (correct
        // for a failed write too). The failure itself is surfaced here.
        void this.saveLayoutCode(msg.layoutCode)
          .then(() => this.post({ kind: 'layoutSaved', requestId: msg.requestId }))
          .catch((err: unknown) => {
            void vscode.window.showErrorMessage(
              `Weft: saving the layout file failed: ${err instanceof Error ? err.message : String(err)}`,
            );
          });
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
        this.runHandler?.(msg.targets ?? []);
        break;
      case 'resolveSpec':
        void this.resolveSpec(msg.requestId, msg.spec, msg.seeded);
        break;
      case 'listSpecs':
        this.postSpecs();
        break;
      case 'runSpec':
        void this.runSpec(msg.spec, msg.seeded);
        break;
      case 'saveSpec':
        void this.saveSpec(msg.spec).catch((err) => {
          void vscode.window.showErrorMessage(
            `Weft: could not save the example: ${err instanceof Error ? err.message : String(err)}`,
          );
        });
        break;
      case 'runSpecFile':
        void this.dispatchVerb('run', [msg.name]);
        break;
      case 'branchTo':
        void this.branchTo(msg.reference);
        break;
      case 'infraStart':
        void this.dispatchVerb('infra', ['start']);
        break;
      case 'infraStop':
        void this.dispatchVerb('infra', [
          'stop',
          ...(msg.deactivation ? this.deactivationFlags(msg.deactivation) : []),
        ]);
        break;
      case 'infraTerminate':
        void this.dispatchVerb('infra', [
          'terminate',
          ...(msg.deactivation ? this.deactivationFlags(msg.deactivation) : []),
        ]);
        break;
      case 'infraCancel':
        void this.dispatchVerb('infra', ['cancel']);
        break;
      case 'cancelBuild':
        void this.dispatchVerb('cancel-build', []);
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
        void this.dispatchVerb('deactivate', this.deactivationFlags(msg.spec));
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
        // The picker's spec rides along when the project was Active
        // (the CLI passes it as trigger-deactivation flags; the
        // dispatcher 412s without them on an Active project).
        void this.dispatchVerb(
          'resync',
          msg.spec ? this.deactivationFlags(msg.spec) : [],
        );
        break;
      case 'infraUpgrade':
        void this.dispatchVerb('infra', [
          'upgrade',
          ...(msg.deactivation ? this.deactivationFlags(msg.deactivation) : []),
        ]);
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
      case 'followClear':
        this.followClearHandler?.();
        break;
      case 'openSource':
        this.openSourceHandler?.(msg.location);
        break;
      case 'stopAction':
        this.stopActionHandler?.();
        break;
      case 'downloadStoredFile':
        void this.runDownloadStoredFile(msg.key);
        break;
      case 'storageCall':
        void this.runStorageCall(msg.requestId, msg.path, msg.body);
        break;
      case 'accessCall':
        void this.runAccessCall(msg.requestId, msg.method, msg.path, msg.body);
        break;
      case 'openExternalUrl':
        void vscode.env.openExternal(vscode.Uri.parse(msg.url));
        break;
      case 'pickAsset':
        void this.runPickAsset(msg.requestId, msg.accept, msg.multiple ?? false, msg.dropped);
        break;
      case 'listRuntimeFiles':
        void this.runListRuntimeFiles(msg.requestId);
        break;
      case 'editActiveSource':
        void this.adoptActiveSource(msg.source);
        break;
      case 'replayExecution':
        // Intentionally ignored by the VS Code host: replaying a past execution
        // onto the canvas is a web-host affordance (its RightPanel history picker
        // emits this). VS Code surfaces past executions through its own history
        // UI, so it never wires the canvas replay. Listed explicitly (not dropped
        // to default) so the exhaustive `never` check below stays a real guarantee
        // that no NEW kind is silently ignored.
        break;
      default: {
        // Every webview->host message kind must be handled explicitly. A silently
        // dropped message is a contract break that only shows up as "the editor
        // did nothing" with no trace. `never` here makes an unhandled kind a
        // compile error; the runtime log covers a wire desync (webview newer than
        // host) that TS can't catch.
        const unhandled: never = msg;
        console.error('[weft] unhandled webview message kind', (unhandled as { kind?: string }).kind);
      }
    }
  }

  /// The webview's code panel edited the active file's `.weft` source directly.
  /// Adopt `source` as the watched document's new text and re-parse: the parse
  /// path posts a NON-fresh `parseResult`, so the editor adopts it as external
  /// truth (canvas updates, pending ops re-apply) instead of rebuilding. The
  /// mirror of a graph gesture: a graph edit writes the source, this is the
  /// source writing the graph. Serialized on the doc's path so it can't race a
  /// concurrent edit transaction to the same file.
  private async adoptActiveSource(source: string): Promise<void> {
    const doc = this.watchedDoc;
    if (!doc) {
      console.error('[weft] editActiveSource with no watched document');
      return;
    }
    const key = doc.uri.fsPath;
    try {
      await this.serializeOnPath(key, () => this.writeTextRaw(doc.uri, source));
    } catch (err) {
      // The write lost the full-document range (buffer changed mid-write) or the
      // save failed. Surface it and re-parse the live doc so the webview re-syncs
      // to whatever the document actually holds now, rather than silently keeping
      // the source it thinks it wrote.
      console.error('[weft] editActiveSource write failed', err);
    }
    await this.triggerParse();
  }

  /// The origin the webview CSP must allow so inline media can load.
  /// A file link is minted on whichever address the request went out
  /// on, so that address IS the origin to allow, and reading it off
  /// the client we are about to call cannot disagree with where the
  /// link comes back pointing.
  private loadStorageOrigin(): void {
    this.storageOrigin = originOf(this.client.getBaseUrl());
  }

  /// Drive one storage-plane verb for the webview: POST the body to the
  /// dispatcher's `/storage/<path>` route (the client authenticates), reply
  /// with a correlated `storageResult`. The single brokered channel behind
  /// every webview storage call (inline preview's download handshake, the
  /// file-drop upload's begin/parts/part-done/complete/abort). Bytes never
  /// pass here: the calls return presigned bucket URLs the webview uses.
  ///
  /// The host owns the project identity (the editor session's project), so it
  /// stamps `project` into every body: the dispatcher scopes uploads +
  /// downloads to it. A 404 becomes an `error` reply (a swept/expired file, or
  /// a download for a missing key) so the caller can show its own fallback.
  private async runStorageCall(requestId: number, path: string, body: unknown): Promise<void> {
    try {
      const merged = { ...(body as Record<string, unknown>), project: this.watchedProjectId ?? null };
      const result = await this.client.post<unknown>(`/storage/${path}`, merged);
      this.post({ kind: 'storageResult', requestId, result });
    } catch (e) {
      const error =
        e instanceof HttpError && e.status === 404
          ? 'expired or deleted'
          : e instanceof Error
            ? e.message
            : String(e);
      this.post({ kind: 'storageResult', requestId, error });
    }
  }

  /// Drive one access-store verb for the webview: send `method` to the
  /// dispatcher's `/access/<path>` route, reply with a correlated
  /// `accessResult`. The single brokered channel behind the connect
  /// flows, grant summaries, and remote_select lookups. Secrets travel
  /// INTO it (pasted fields going editor -> store) and never back out;
  /// the host stamps the active project into POST bodies so grants
  /// scope to the project being edited.
  private async runAccessCall(
    requestId: number,
    method: 'GET' | 'POST' | 'DELETE',
    path: string,
    body?: unknown,
  ): Promise<void> {
    try {
      let result: unknown;
      switch (method) {
        case 'GET':
          result = await this.client.get<unknown>(`/access/${path}`);
          break;
        case 'POST': {
          const merged = {
            ...(body as Record<string, unknown>),
            project_id: this.watchedProjectId ?? null,
          };
          result = await this.client.post<unknown>(`/access/${path}`, merged);
          break;
        }
        case 'DELETE':
          await this.client.del(`/access/${path}`);
          result = {};
          break;
      }
      this.post({ kind: 'accessResult', requestId, result });
    } catch (e) {
      const error =
        e instanceof HttpError ? `${e.body || e.message}` : e instanceof Error ? e.message : String(e);
      this.post({ kind: 'accessResult', requestId, error });
    }
  }

  /// The file-drop field's asset pick. Locally a PICKED file is referenced IN
  /// PLACE: the native dialog returns the real path, written into the
  /// `@file(...)` ref relative to the PROJECT ROOT when the pick is inside
  /// the project (a marker's path is root-relative wherever it is written,
  /// so the spelling is the same from `src/main.weft` and from any module),
  /// absolute otherwise (out-of-project refs are legal locally; the build's
  /// asset sync reads them from wherever they are). A DRAG-DROPPED file
  /// arrives as bytes (the browser hides its OS path), so it is stored as a
  /// project file under the root `assets/` instead, never overwriting an
  /// existing different file, and referenced the same way.
  private async runPickAsset(
    requestId: number,
    accept: string | undefined,
    multiple: boolean,
    dropped: { name: string; bytesBase64: string }[] | undefined,
  ): Promise<void> {
    try {
      const docPath = this.watchedDoc?.uri.fsPath;
      const root = docPath ? findProjectRoot(docPath) : null;
      if (!root) {
        this.post({ kind: 'assetPicked', requestId, error: 'no project root (save the project first)' });
        return;
      }
      // Inside the project a ref is spelled from the root, so it travels
      // with the project; outside, the absolute path is the only honest
      // spelling: the file is read from where it sits and uploaded at
      // compile, never copied onto the disk a second time.
      const refFor = (absolute: string): string => {
        const fromRoot = nodePath.relative(root, absolute);
        const inProject = !fromRoot.startsWith('..') && !nodePath.isAbsolute(fromRoot);
        return inProject ? fromRoot.split(nodePath.sep).join('/') : absolute;
      };
      if (dropped) {
        const paths: string[] = [];
        for (const file of dropped) {
          paths.push(refFor(await this.storeDroppedFile(root, file)));
        }
        this.post({ kind: 'assetPicked', requestId, paths });
        return;
      }
      const picked = await vscode.window.showOpenDialog({
        canSelectMany: multiple,
        filters: dialogFiltersForAccept(accept),
      });
      const paths = (picked ?? []).map((uri) => refFor(uri.fsPath));
      this.post({ kind: 'assetPicked', requestId, paths });
    } catch (e) {
      this.post({
        kind: 'assetPicked',
        requestId,
        error: e instanceof Error ? e.message : String(e),
      });
    }
  }

  /// Store one dropped file under the project's root `assets/`, and hand
  /// back its absolute path. The browser hides a dropped file's OS path, so
  /// its bytes are what travels; a name collision takes a numeric suffix
  /// rather than clobbering a different file that happens to share a name.
  private async storeDroppedFile(
    root: string,
    file: { name: string; bytesBase64: string },
  ): Promise<string> {
    const assetsDir = nodePath.join(root, 'assets');
    await vscode.workspace.fs.createDirectory(vscode.Uri.file(assetsDir));
    const bytes = Buffer.from(file.bytesBase64, 'base64');
    const leaf = file.name.replace(/[\\/]/g, '_');
    let candidate = leaf;
    for (let n = 1; ; n++) {
      const full = nodePath.join(assetsDir, candidate);
      try {
        await vscode.workspace.fs.stat(vscode.Uri.file(full));
      } catch {
        await vscode.workspace.fs.writeFile(vscode.Uri.file(full), bytes);
        return full;
      }
      const dot = leaf.lastIndexOf('.');
      candidate = dot > 0 ? `${leaf.slice(0, dot)}-${n}${leaf.slice(dot)}` : `${leaf}-${n}`;
    }
  }

  /// The project's STORED runtime files for the picker: the same listing door
  /// `weft files` uses (the dispatcher's tenant file list), filtered to THIS
  /// project's `project/` + `asset/` scopes, keys handed back TENANT-LESS
  /// (the short address a picked ref writes into source).
  private async runListRuntimeFiles(requestId: number): Promise<void> {
    const pid = this.watchedProjectId;
    if (!pid) {
      this.post({
        kind: 'runtimeFiles',
        requestId,
        files: [],
        error: 'no watched project: open a project graph before picking stored files',
      });
      return;
    }
    try {
      const resp = await this.client.get<{
        files: { key: string; filename: string; mimeType: string; sizeBytes: number }[];
      }>('/storage/files');
      const files = (resp.files ?? []).flatMap((f) => {
        const slash = f.key.indexOf('/');
        if (slash < 0) return [];
        const scopeKey = f.key.slice(slash + 1);
        if (!scopeKey.startsWith(`project/${pid}/`) && !scopeKey.startsWith(`asset/${pid}/`)) {
          return [];
        }
        return [{ key: scopeKey, filename: f.filename, mimeType: f.mimeType, sizeBytes: f.sizeBytes }];
      });
      this.post({ kind: 'runtimeFiles', requestId, files });
    } catch (e) {
      // A failed listing must never render as "this project has no stored
      // files": ship the reason so the picker shows the failure.
      this.post({
        kind: 'runtimeFiles',
        requestId,
        files: [],
        error: e instanceof Error ? e.message : String(e),
      });
    }
  }

  /// Stored-file download: handshake with the dispatcher (it
  /// authenticates and asks the tenant's storage box to mint a
  /// short-lived capability), then open the box's public URL in the
  /// browser, which streams the bytes DIRECTLY from the box.
  private async runDownloadStoredFile(key: string): Promise<void> {
    try {
      const resp = await this.client.post<{ url: string }>(
        '/storage/files/download',
        { key, project: this.watchedProjectId ?? null },
      );
      await vscode.env.openExternal(vscode.Uri.parse(resp.url));
    } catch (e) {
      if (e instanceof HttpError && e.status === 404) {
        void vscode.window.showWarningMessage(
          `Stored file is expired or deleted (its metadata stays in the replay): ${key}`,
        );
        return;
      }
      void vscode.window.showErrorMessage(
        `Download failed: ${e instanceof Error ? e.message : String(e)}`,
      );
    }
  }

  /// Public: extension.ts's runPinned calls this before reading the .weft from
  /// disk, so the build always sees the freshest source even if the user
  /// clicked Run before an in-flight edit's write+reparse finished. Awaits the
  /// watched doc's per-path write chain (the one `applyEditTransaction` uses).
  async waitForPendingSave(): Promise<void> {
    if (!this.watchedDoc) return;
    // Live resolve: a detached doc's isDirty is frozen, and save() on it
    // cannot land; a fresh open from disk is never dirty (see liveDoc).
    const doc = await this.liveDoc(this.watchedDoc);
    const pending = this.pendingWrites.get(doc.uri.fsPath);
    // A rejected chain already rolled back and surfaced; what matters below
    // is the doc's ACTUAL state once the chain settles, not the chain's
    // memory of how it settled (a finished failed write deletes its chain
    // entry, so remembering failures here would be a check that only fires
    // in a razor-thin window).
    if (pending) await pending.catch(() => {});
    // The condition the build actually depends on: does DISK hold what the
    // user sees? A dirty buffer (an edit whose save failed, or plain
    // unsaved text-tab typing) means no; save it now, and refuse the verb
    // loudly if the save cannot land (building would silently use the
    // stale on-disk source).
    if (doc.isDirty) {
      // Bracketed as a SELF write: the save pipeline's trim/newline
      // follow-ups fire change events, and unbracketed they would read as
      // the user typing and engage the webview's 1s edit lock right after
      // they clicked Run.
      this.selfWriteDepth++;
      try {
        if (!(await doc.save())) {
          throw new Error(
            `${nodePath.basename(doc.uri.fsPath)} has unsaved changes that could not be saved; ` +
              'running now would build the stale on-disk source. Save the file first.',
          );
        }
      } finally {
        this.selfWriteDepth--;
      }
    }
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
  /// The doc (path + settled text) the host last PARSED and rendered. The
  /// onDidChangeTextDocument handler and the parse debounce skip a reparse
  /// when the watched doc still matches this: the rendered graph is not
  /// stale, so there is nothing to reparse. This is the timing-independent
  /// invariant ("reparse iff the render is stale"), which subsumes every
  /// self-write case without enumerating the save pipeline's intermediate
  /// texts: our own edit write, the save pipeline's trim-trailing-whitespace
  /// / insert-final-newline follow-ups, a format-on-save participant, and an
  /// undo back to the rendered text all leave the text equal to what we
  /// rendered and are correctly skipped; a genuine edit differs and
  /// reparses. Staleness is a PER-DOCUMENT fact, so the path rides along:
  /// switching to a different doc whose text happens to equal the previous
  /// render must still read as stale (nothing was rendered for THAT doc).
  /// (The old time-window flag missed late save-pipeline change events and
  /// flickered.)
  private lastRendered: { path: string; text: string } | null = null;

  /// True when the watched doc's CURRENT text is exactly what was last
  /// rendered for that same doc. The one staleness predicate both skip
  /// sites use. A closed doc is never "current": its getText() is frozen
  /// at close time, so the comparison would answer about stale text.
  private isRenderCurrent(): boolean {
    return this.watchedDoc !== undefined && !this.watchedDoc.isClosed
      && this.lastRendered !== null
      && this.lastRendered.path === this.watchedDoc.uri.fsPath
      && this.lastRendered.text === this.watchedDoc.getText();
  }

  /// Resolve `doc` to a LIVE TextDocument for the same file. Closing the
  /// source tab eventually closes its TextDocument (VS Code detaches it at
  /// a moment nothing here controls); a closed doc's getText()/isDirty/
  /// version are frozen at close time, so reading it computes edits and
  /// parses against stale source while writes keep landing on disk. This
  /// re-opens the file (current disk content, and the SAME instance the
  /// user's editor shows if one is open) and re-latches it as the watched
  /// doc when the closed instance was still the watched one.
  private async liveDoc(doc: vscode.TextDocument): Promise<vscode.TextDocument> {
    if (!doc.isClosed) return doc;
    const fresh = await vscode.workspace.openTextDocument(doc.uri);
    if (this.watchedDoc === doc) this.watchedDoc = fresh;
    return fresh;
  }

  /// >0 while THIS controller is writing the watched doc (a graph edit's
  /// writeTextRaw). The `onDidChangeTextDocument` change events for our own
  /// applyEdit + save (and any save participant) are delivered before those
  /// awaits resolve, so they land inside this bracket. Used to suppress the
  /// `codeEditTouched` auto-lock for our own writes: `lastRendered` still
  /// holds the PRE-edit text at that moment (it updates later in
  /// applyParseResult), so a text comparison can't tell our write apart from
  /// external typing, but the depth gate can.
  private selfWriteDepth = 0;

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
      // Bracket the write so our own change events don't trip the auto-lock.
      this.selfWriteDepth++;
      try {
        // applyEdit returns false (without throwing) when the buffer changed
        // under the computed full-document range, the residual TOCTOU window
        // after the version backstop. Failing loud here routes through the
        // caller's rejection path (rollback + resync) instead of saving + and
        // replying ok:true with truth the doc never received.
        if (!(await vscode.workspace.applyEdit(edit))) {
          throw new Error('document edit failed to apply (buffer changed mid-write)');
        }
        await openDoc.save();
      } finally {
        this.selfWriteDepth--;
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
    req: { kind: 'edit'; ops: EditOp[] } | { kind: 'applyEdit'; textEdit: TextEdit },
  ): Promise<void> {
    const doc = this.watchedDoc;
    if (!doc) {
      this.post({ kind: 'editApplied', requestId, ok: false, reason: 'no document is open' });
      return;
    }
    const key = doc.uri.fsPath;
    try {
      const result = await this.serializeOnPath(key, async () => {
        // Live resolve: with the source tab closed, `doc` may be detached
        // (frozen text/version); a same-path doc the user reopened is a
        // different instance. liveDoc covers both (see its comment).
        const openDoc = await this.liveDoc(doc);
        // Doc-version backstop: if the user (or AI) typed into the doc while
        // the edit-server was computing, writing the result would overwrite
        // that keystroke. Capture the version AFTER the predecessor settled,
        // re-check it before writing, and abort cleanly on a change; the
        // webview's standard rejection path rolls the gesture back. This is
        // the race-safe third layer under the webview's preflight lock and
        // the 1s auto-lock window.
        const versionBefore = openDoc.version;
        const r = await this.parseServer.request<{ source: string; parse: ParseResponse; inverse: TextEdit }>({
          ...req,
          source: openDoc.getText(),
          file: key,
        });
        if (openDoc.version !== versionBefore) {
          return { aborted: true as const };
        }
        await this.writeTextRaw(doc.uri, r.source);
        // Suppress the RENDER (not the write) if the user switched `.weft` while
        // this was in flight: applyParseResult reads the live watchedDoc and
        // consumes freshMount, so rendering A after the view moved to B would
        // bind A's refs against B and steal B's rebuild. Same discipline as
        // triggerParse's seq guard. The write already landed on the right doc.
        const sameDoc = this.watchedDoc === doc;
        if (sameDoc) {
          this.parseSeq++; // authoritative result; drop a concurrent stale parse
          // The webview receives this truth inside the editApplied reply
          // (no layout, no parseResult post): one message advances source +
          // parse + undo, and the layout stays whatever the webview holds.
          this.applyParseResult(r.parse, r.source);
        }
        // `current` carries truth ONLY when this is still the watched doc. On a
        // mid-edit doc switch the render was suppressed above; carrying the old
        // doc's truth in the reply would regress the webview (now showing the
        // new doc) to a graph it isn't displaying.
        return { aborted: false as const, inverse: r.inverse, current: sameDoc ? { parse: r.parse, source: r.source } : null };
      });
      if (result.aborted) {
        this.post({ kind: 'editApplied', requestId, ok: false, reason: 'code-was-edited' });
        return;
      }
      this.post({
        kind: 'editApplied', requestId, ok: true, inverse: result.inverse,
        ...(result.current ? { response: result.current.parse, source: result.current.source } : {}),
      });
    } catch (err) {
      // An edit being REJECTED (e.g. a duplicate id, a cross-scope wire) is not
      // a parse failure: the source on disk is unchanged (the write above never
      // ran). The webview owns its optimistic state, so it owns the rollback
      // (resync + drop the pending op). This is NOT a `parseError` (which
      // would blank a perfectly renderable project).
      const message = err instanceof Error ? err.message : String(err);
      this.post({
        kind: 'editApplied', requestId, ok: false,
        // A ParseServerError's message is the server's own words, fit for
        // the user verbatim (no envelope to strip: the wire carries the
        // message bare). A transport failure (WeftCliError) keeps its
        // framing: naming the parse server IS the explanation there.
        reason: message,
      });
    }
  }

  /// Answer the webview's `resyncSource`: parse the open doc's CURRENT text
  /// and reply with the authoritative truth. Sent by the webview after a
  /// rejected edit so it can snap back to the host's state instead of
  /// mirroring server semantics locally. Serialized on the doc's path chain
  /// so the resync sees the post-rejection (settled) source.
  private async resyncSource(requestId: number): Promise<void> {
    const doc = this.watchedDoc;
    if (!doc) {
      this.post({ kind: 'sourceResynced', requestId, ok: false, error: 'no document is open' });
      return;
    }
    const key = doc.uri.fsPath;
    try {
      const { response, source } = await this.serializeOnPath(key, async () => {
        // Live resolve, same reason as applyEditTransaction (see liveDoc).
        const openDoc = await this.liveDoc(doc);
        const source = openDoc.getText();
        const response = await this.parseServer.request<ParseResponse>({
          kind: 'parse',
          source,
          file: key,
        });
        return { response, source };
      });
      this.post({ kind: 'sourceResynced', requestId, ok: true, response, source });
    } catch (err) {
      // The current source doesn't parse (the user is mid-edit in the text
      // tab). The webview keeps its previous truth; the parse path will
      // deliver a fresh one once the source parses again.
      this.post({
        kind: 'sourceResynced', requestId, ok: false,
        error: err instanceof Error ? err.message : String(err),
      });
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

  /// Read the companion `.layout` file. Serialized on the layout path chain, so a
  /// read started while a `saveLayoutCode` for the same file is still in flight
  /// waits for that write to land instead of reading a stale (pre-save) copy. The
  /// webview is the layout source of truth; the host echoes layout back on parse,
  /// and a read racing an unflushed write would echo old positions and snap a
  /// just-moved node back. Routing both read and write through one chain (the same
  /// discipline as the `.weft` source path) closes that race.
  private async readLayoutCode(doc: vscode.TextDocument): Promise<string> {
    const uri = this.layoutUriFor(doc);
    return this.serializeOnPath(uri.fsPath, async () => {
      try {
        const data = await vscode.workspace.fs.readFile(uri);
        return new TextDecoder().decode(data);
      } catch {
        return '';
      }
    });
  }

  private async saveLayoutCode(layoutCode: string): Promise<void> {
    if (!this.watchedDoc) return;
    const uri = this.layoutUriFor(this.watchedDoc);
    await this.serializeOnPath(uri.fsPath, async () => {
      // The layouts/ mirror tree may not exist yet; fs.writeFile won't create
      // parent dirs, so ensure them first.
      const dir = vscode.Uri.file(nodePath.dirname(uri.fsPath));
      await vscode.workspace.fs.createDirectory(dir);
      await vscode.workspace.fs.writeFile(uri, new TextEncoder().encode(layoutCode));
    });
  }

  /// Write-back for a file-backed config field (`@file("path", Type)`). The
  /// path is project-root-relative (the same root the compiler resolves
  /// against). The resolved path must stay inside the project root, mirroring
  /// the compiler's escape guard; an escaping path is dropped, not written.
  /// After writing, re-parse so the graph reflects the new resolved value.
  private async saveFileRef(relPath: string, content: string): Promise<void> {
    const doc = this.watchedDoc;
    if (!doc) return;
    // `@file` paths resolve against the PROJECT ROOT wherever they are
    // written (the compiler's rule, and the anchor `watchReferencedFiles`
    // reads them from), so an included file deep in `src/` writes the
    // same asset it displays. The result must stay inside the root.
    const baseDir = nodePath.dirname(doc.uri.fsPath);
    const root = findProjectRoot(doc.uri.fsPath) ?? baseDir;
    // ONE derivation of the target, so the value checked is the value
    // written. Built from the document's own URI so it keeps that scheme
    // (a remote SSH session's `vscode-remote`), which `Uri.file` would
    // drop: the joined path is the walk from the doc's dir to the asset.
    const target = vscode.Uri.joinPath(
      doc.uri,
      '..',
      nodePath.relative(baseDir, nodePath.resolve(root, relPath)),
    );
    const relToRoot = nodePath.relative(root, target.fsPath);
    if (relToRoot.startsWith('..') || nodePath.isAbsolute(relToRoot)) {
      console.error('[weft] refusing saveFileRef: path escapes project root', relPath);
      return;
    }
    // Serialized full-text write (open-doc-aware, recomputes range after any
    // predecessor) via the shared writer. No graph reparse: config is
    // unchanged; reship the resolved content for display once the write lands.
    await this.writeDocumentText(target, content);
    void this.shipFileContents(this.fileBaseDir, this.fileRelPaths);
  }

  /// Navigate into an `@include`d file: open its graph in this panel and
  /// push the current doc onto the back-stack. The path is relative to the
  /// INCLUDING file's directory, the same base the compiler resolves it
  /// against (`DiskFileReader::resolve_and_read`); it must stay inside the
  /// project root, as there. One view = one file, so this swaps the watched
  /// doc rather than inlining.
  private async navigateInto(relPath: string, alias: string): Promise<void> {
    const current = this.watchedDoc;
    if (!current) return;
    const root = findProjectRoot(current.uri.fsPath);
    if (!root) return;
    // ONE derivation of the file to open, so the value checked is the
    // value opened. Built from the current document's URI so a remote
    // session keeps its scheme; `Uri.file` would point at the local disk.
    const targetUri = vscode.Uri.joinPath(current.uri, '..', relPath);
    const rel = nodePath.relative(root, targetUri.fsPath);
    if (rel.startsWith('..') || nodePath.isAbsolute(rel)) {
      console.error('[weft] refusing openInclude: path escapes project root', relPath);
      return;
    }
    let target: vscode.TextDocument;
    try {
      target = await vscode.workspace.openTextDocument(targetUri);
    } catch (e) {
      void vscode.window.showErrorMessage(`Weft: cannot open included file ${relPath}: ${e}`);
      return;
    }
    this.navStack.push({ doc: current, alias });
    this.freshMount = true;
    // Send navState BEFORE the parse it depends on: open() posts parseResult
    // (freshMount), which remounts the editor and looks up execution values via
    // the call path. navState (computed from the now-updated navStack) must arrive
    // first so that lookup uses the correct prefix on the first render.
    this.sendNavState();
    await this.open(target, undefined, true);
  }

  /// The root of the project the watched document belongs to.
  private projectRoot(): string | undefined {
    const doc = this.watchedDoc;
    if (!doc) return undefined;
    return findProjectRoot(doc.uri.fsPath) ?? undefined;
  }

  /// Resolve a spec through the parse server (the dispatcher's own
  /// resolver over the buffer as it is), answering the dialog; a server
  /// failure is a refusal that names it, never a silent dialog.
  private async resolveSpec(requestId: number, spec: RunSpec, seeded: boolean): Promise<void> {
    const doc = this.watchedDoc;
    let result: ResolveSpecResponse;
    if (!doc) {
      result = { refusal: { errors: ['no .weft file is open'] } };
    } else {
      try {
        const bakes = spec.fire && this.watchedProjectId
          ? await this.client.get<BakeSummary[]>(`/projects/${this.watchedProjectId}/trigger-bakes`)
          : [];
        result = await this.parseServer.request<ResolveSpecResponse>({
          kind: 'resolveSpec',
          source: doc.getText(),
          file: doc.uri.fsPath,
          spec,
          seeded,
          bakes,
        });
      } catch (err) {
        result = { refusal: { errors: [err instanceof Error ? err.message : String(err)] } };
      }
    }
    void this.panel?.webview.postMessage({ kind: 'specResolved', requestId, result } satisfies HostMessage);
  }

  /// The project's `examples/*.json`, read off the disk (they are checked
  /// in, and the CLI reads the same files).
  private postSpecs(): void {
    const root = this.projectRoot();
    const specs: RunSpec[] = [];
    if (root) {
      const dir = nodePath.join(root, 'examples');
      let names: string[] = [];
      try {
        names = nodeFs.readdirSync(dir).filter((n) => n.endsWith('.json')).sort();
      } catch (err) {
        // An `examples/` that cannot be read is not "no examples". The
        // menu would have said so and the person would have gone looking
        // for a missing file that is sitting right there.
        const code = (err as { code?: string }).code;
        if (code !== 'ENOENT') {
          void vscode.window.showWarningMessage(
            `Weft: could not read examples/ (${err instanceof Error ? err.message : String(err)}), so the Run menu is empty.`,
          );
        }
        names = [];
      }
      const broken: string[] = [];
      for (const file of names) {
        try {
          const spec = parseRunSpec(parseSuppliedJson(nodeFs.readFileSync(nodePath.join(dir, file), 'utf8')));
          // Named by its FILE, always. `weft run <name>` resolves
          // `examples/<name>.json`, so the name written inside the file
          // is not what runs it: a file whose two names differed ran a
          // different example or none, and two files sharing an inner
          // name collided on the menu's key, which takes the graph down
          // with a duplicate-key error.
          specs.push({ ...spec, name: file.replace(/\.json$/, '') });
        } catch (err) {
          broken.push(`${file} (${err instanceof Error ? err.message : String(err)})`);
        }
      }
      if (broken.length > 0) {
        void vscode.window.showWarningMessage(
          `Weft: ${broken.length} file(s) in examples/ do not parse and are not listed: ${broken.join(', ')}`,
        );
      }
    }
    void this.panel?.webview.postMessage({ kind: 'specsListed', specs } satisfies HostMessage);
  }

  /// Run a spec from the dialog as a one-off: the flags it spells, the
  /// seed checkbox included (the dialog resolved it WITH seeding to
  /// decide it was runnable, so dropping the flag refuses an input the
  /// person was just told is covered).
  private async runSpec(spec: RunSpec, seeded?: boolean): Promise<void> {
    await this.dispatchVerb('run', specToRunArgs(spec, seeded === true));
  }

  /// Write a spec to `examples/<name>.json`. It does NOT run: saving used
  /// to run it too, so writing down a spec you were not ready for started
  /// a real execution.
  ///
  /// The name becomes a file name and `weft run <name>` resolves it by
  /// file name, so it is checked here rather than joined blindly: a name
  /// carrying a separator or a `..` wrote outside `examples/`, and one
  /// carrying anything else the CLI does not resolve saved a file the Run
  /// menu could never run.
  private async saveSpec(spec: RunSpec): Promise<void> {
    spec = parseRunSpec(spec);
    // The spec's OWN name. It used to travel twice, as `spec.name` and
    // as a second field copied from it, and the host then wrote the
    // second one INTO the spec, so the message did not say which won.
    const clean = spec.name.trim();
    // The SAME rule the CLI applies, shared as one function, and checked
    // here as well as in the dialog: a spec can arrive from any sender
    // on this channel, and this is where the file name is joined.
    const problem = exampleNameProblem(clean);
    if (problem) {
      void vscode.window.showErrorMessage(`Weft: '${spec.name}' cannot name an example: ${problem}.`);
      return;
    }
    const root = this.projectRoot();
    if (!root) {
      void vscode.window.showErrorMessage('Weft: no project to save the spec into.');
      return;
    }
    const dir = nodePath.join(root, 'examples');
    const path = nodePath.join(dir, `${clean}.json`);
    // Ordinary saved parameters can be replaced after confirmation.
    // Frozen evidence is replaced only by explicitly freezing a new run.
    let existed = false;
    try {
      await vscode.workspace.fs.stat(vscode.Uri.file(path));
      existed = true;
    } catch (err) {
      // Only "it is not there" means it is not there. A permissions or
      // unavailable answer swallowed as "nothing there" skips the
      // question below and destroys whatever is in the file.
      if ((err as vscode.FileSystemError)?.code !== 'FileNotFound') throw err;
    }
    if (existed) {
      const saved = parseRunSpec(parseSuppliedJson(Buffer.from(await vscode.workspace.fs.readFile(vscode.Uri.file(path))).toString('utf8')));
      if (saved.expected || saved.frozen_from) {
        void vscode.window.showErrorMessage(`Weft: examples/${clean}.json is frozen. Save under another name, or use weft freeze after accepting a new run.`);
        return;
      }
      const replace = await vscode.window.showWarningMessage(
        `examples/${clean}.json already exists. Replace its saved run parameters?`,
        { modal: true },
        'Replace',
      );
      if (replace !== 'Replace') return;
    }
    const text = `${JSON.stringify({ ...spec, name: clean }, null, 2)}\n`;
    // The directory after the question, so declining leaves nothing
    // behind that was not there before.
    await vscode.workspace.fs.createDirectory(vscode.Uri.file(dir));
    await vscode.workspace.fs.writeFile(vscode.Uri.file(path), Buffer.from(text, 'utf8'));
    void vscode.window.showInformationMessage(
      existed
        ? `Weft: replaced examples/${clean}.json. The Run menu lists it.`
        : `Weft: saved examples/${clean}.json. The Run menu lists it.`,
    );
    this.postSpecs();
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

  /// Push the current navigation depth, file name, and call path to the
  /// webview. The call path is the chain of call sites descended through
  /// (each the site's own id, `c` then `C.inner`), so the webview shows
  /// the rows of the one call on screen. The executions view is told the
  /// same place as a group address (`c.inner`).
  private sendNavState(): void {
    const fileName = this.watchedDoc?.uri.fsPath.split(/[\\/]/).pop() ?? '';
    const callPath = this.navStack.map((f) => f.alias);
    void this.panel?.webview.postMessage({
      kind: 'navState',
      depth: this.navStack.length,
      fileName,
      callPath,
    });
    this.navHandler?.(groupOfCallPath(callPath));
  }

  /** Fetch every node type available in the current project scope
   *  (stdlib + project-local `nodes/`) and ship the catalog to the
   *  webview so the command palette can list them all, even types
   *  the current `src/main.weft` doesn't reference yet. */

  private async sendGlobalCatalog(): Promise<void> {
    if (!this.watchedDoc) return;
    try {
      const response = await runWeftJson<{
        catalog: Record<string, unknown>;
        warnings?: string[];
      }>(['describe-nodes'], docDirOf(this.watchedDoc));
      this.post({
        kind: 'catalogAll',
        catalog: response.catalog as Record<string, CatalogEntry>,
      });
      // Catalog loaded. Clear any prior catalog error (both the webview
      // channel AND the action-bar banner its failure raised), and
      // surface per-node soft warnings (a node mid-rename with bad
      // metadata) so they aren't computed-then-dropped. Its own channel,
      // not parseError: a successful parse must not erase it.
      this.post({ kind: 'catalogError', warnings: response.warnings ?? [] });
      this.resolveErrorHandler?.('catalog');
    } catch (err) {
      // The full node catalog failed to load (weft not on PATH, a
      // project error, bad JSON). Surface it on the catalog channel,
      // independent of the parse banner: the source may parse fine
      // while the catalog is unavailable, and a later successful parse
      // must not silently clear this.
      const message = err instanceof Error ? err.message : String(err);
      this.post({
        kind: 'catalogError',
        error: `node catalog unavailable: ${message}`,
      });
      this.reportErrorHandler?.('catalog', `node catalog unavailable: ${message}`, {
        what: 'Loading the node catalog',
        stage: 'catalog',
        diagnostics: [{ severity: 'error', message }],
        ...(err instanceof Error && err.stack ? { raw: err.stack } : {}),
      });
    }
  }

  private onDispose(): void {
    if (this.parseTimer) clearTimeout(this.parseTimer);
    // The controller object is reused across panel sessions: a parse owed to
    // the torn-down session must not carry into the next one.
    this.pendingParseReason = null;
    if (this.catalogRefreshTimer) clearTimeout(this.catalogRefreshTimer);
    this.nodesWatcher?.dispose();
    this.nodesWatcher = undefined;
    this.selfWatcher?.dispose();
    this.selfWatcher = undefined;
    this.refWatcher?.dispose();
    this.refWatcher = undefined;
    // And forget WHAT was watched. The key is how `watchReferencedFiles`
    // decides it has nothing to do; leaving the old one behind meant
    // that after a close and reopen it saw the same set, returned early,
    // and never rebuilt the watchers. Editing a backing file or an
    // included program outside the editor then stopped updating the
    // graph, in silence, until the panel was opened on something else.
    this.watchedRefPaths = '';
    this.stopAllLivePollers();
    for (const d of this.disposables) d.dispose();
    this.disposables = [];
    this.panel = undefined;
    this.watchedDoc = undefined;
    this.lastExecVersion = undefined;
    this.execVersionFor = undefined;
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
<meta http-equiv="Content-Security-Policy" content="default-src 'none'; style-src ${cspSource} 'unsafe-inline'; script-src 'nonce-${nonce}' ${cspSource}; img-src ${cspSource} data: https: http: ${this.storageOrigin}; media-src ${cspSource} https: http: ${this.storageOrigin}; font-src ${cspSource}; connect-src ${cspSource} ${this.storageOrigin};">
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

/// The origin of a base URL, or '' when it is empty/unparseable. A
/// missing origin narrows the webview's policy (the surface using it
/// fails visibly at the point of use), never widens it.
function originOf(baseUrl: string): string {
  try {
    return new URL(baseUrl).origin;
  } catch {
    return '';
  }
}

function randomNonce(): string {
  const chars = 'ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789';
  let out = '';
  for (let i = 0; i < 24; i++) out += chars[Math.floor(Math.random() * chars.length)];
  return out;
}

/// VS Code's file dialog filters by EXTENSION, the accept filter by MIME:
/// translate the common `<kind>/*` filters to their extension sets (mirroring
/// the shared `EXT_MIME` guess table). An exact-mime or absent filter shows
/// all files: steering, not blocking, exactly like the picker modal.
function dialogFiltersForAccept(
  accept: string | undefined,
): { [name: string]: string[] } | undefined {
  switch (accept) {
    case 'image/*':
      return { Images: ['png', 'jpg', 'jpeg', 'webp', 'gif', 'svg', 'avif'] };
    case 'audio/*':
      return { Audio: ['mp3', 'wav', 'ogg', 'flac', 'm4a'] };
    case 'video/*':
      return { Video: ['mp4', 'mov', 'webm', 'mkv'] };
    default:
      return undefined;
  }
}

// isLiveDataItem + signalDisplayToLiveItems live in the shared weft-graph
// package (imported above), so both hosts use one copy.
