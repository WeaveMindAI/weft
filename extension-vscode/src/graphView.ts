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
import type { ActionErrorDetails, CatalogEntry, DeactivationSpec, EditOp, ErrorVerb, HostMessage, LiveDataItem, ParseResponse, ProjectDefinition, TextEdit, WebviewMessage } from './shared/protocol';
import { typeReferencesFile } from './shared/protocol';
import { isLiveDataItem, signalDisplayToLiveItems } from '../../packages/weft-graph/src/live-data';
import * as nodePath from 'node:path';
import { readProjectIdFromToml, findProjectRoot } from './sidebar/projects';

export class GraphViewController {
  private panel: vscode.WebviewPanel | undefined;
  private watchedDoc: vscode.TextDocument | undefined;
  private watchedProjectId: string | undefined;
  /// Origin (scheme://host:port) the storage box serves file bytes
  /// from, fetched once from the dispatcher at panel boot. The
  /// webview CSP allows this origin in img-src/media-src so an
  /// <img>/<video> streams directly from the box (range requests,
  /// seeking), the same way any web host would. Empty if the fetch
  /// failed (older dispatcher / offline): previews then can't load,
  /// which surfaces as the node's fallback rather than a silent break.
  private storageOrigin = '';
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

    // Learn the storage origin before rendering the CSP, so an
    // <img>/<video> can stream directly from the box.
    await this.loadStorageOrigin();

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
          // rendered text all leave the text == lastRenderedSource and need no
          // reparse (the graph isn't stale). A genuine edit differs and reparses.
          if (e.document.getText() === this.lastRenderedSource) return;
          // A genuine EXTERNAL change (text-tab typing, AI streaming): tell the
          // webview so it engages its auto-lock on source-mutating graph
          // gestures. Re-posted per keystroke; the lock window slides forward.
          // Skip our OWN edit writes: their change events land here (the doc
          // differs from lastRenderedSource until applyParseResult runs), but
          // auto-locking the user right after their own GUI edit is wrong. The
          // selfWriteDepth bracket scopes exactly our applyEdit + save events.
          if (this.selfWriteDepth === 0) this.post({ kind: 'codeEditTouched' });
          this.scheduleParse();
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
    for (const n of response.project.nodes as Array<{ fileRefs?: Record<string, { path: string; type: string }>; includePath?: string }>) {
      // Only TEXT refs: a media ref's bytes are never read as text content
      // (they'd ship an image as garbage, and the editor's content-save path
      // would clobber the media file).
      if (n.fileRefs) {
        for (const ref of Object.values(n.fileRefs)) {
          if (!typeReferencesFile(ref.type)) fileRel.add(ref.path);
        }
      }
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
   *  (so a GUI edit re-renders from that ONE round-trip, no second parse). */
  private applyParseResult(response: ParseResponse, source: string, layoutCode: string, postToWebview = true): void {
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
    this.lastRenderedSource = this.watchedDoc?.getText() ?? source;
    // An edit-fed render hands the webview its truth inside the `editApplied`
    // reply instead (one message, no double render); only parse-fed renders
    // post `parseResult`.
    if (postToWebview) {
      this.post({ kind: 'parseResult', response, source, layoutCode, freshMount: this.freshMount });
    }
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
      case 'resyncSource':
        void this.resyncSource(msg.requestId);
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
      case 'openSource':
        this.openSourceHandler?.();
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
      case 'pickAsset':
        void this.runPickAsset(msg.requestId, msg.accept, msg.dropped);
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

  /// Fetch the storage origin the webview CSP must allow so media
  /// streams directly from the box. Best-effort: on failure the origin
  /// stays empty and image previews can't load (their node shows the
  /// "unavailable" fallback at the point of use, which is the visible
  /// signal). We do NOT block panel boot or pop a modal, since a
  /// storage-less project never previews media; the failure is logged
  /// so a developer can see the real cause (dispatcher unreachable /
  /// too old) rather than guessing from a blank preview.
  private async loadStorageOrigin(): Promise<void> {
    try {
      const resp = await this.client.get<{ public_base_url: string }>('/storage/public-base');
      this.storageOrigin = new URL(resp.public_base_url).origin;
    } catch (err) {
      this.storageOrigin = '';
      console.warn(
        '[weft] storage origin unavailable; inline media previews will show a fallback ' +
          'until the graph is reopened with the dispatcher reachable',
        err,
      );
    }
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

  /// The file-drop field's asset pick. Locally a PICKED file is referenced IN
  /// PLACE: the native dialog returns the real path, written into the
  /// `@file(...)` ref as project-relative when under the root, absolute
  /// otherwise (out-of-project refs are legal locally; the build's asset sync
  /// reads them from wherever they are). A DRAG-DROPPED file arrives as bytes
  /// (the browser hides its OS path), so it is stored as a project file under
  /// `assets/` instead, never overwriting an existing different file.
  private async runPickAsset(
    requestId: number,
    accept: string | undefined,
    dropped: { name: string; bytesBase64: string } | undefined,
  ): Promise<void> {
    try {
      const docPath = this.watchedDoc?.uri.fsPath;
      const root = docPath ? findProjectRoot(docPath) : null;
      if (!root) {
        this.post({ kind: 'assetPicked', requestId, error: 'no project root (save the project first)' });
        return;
      }
      if (dropped) {
        const assetsDir = nodePath.join(root, 'assets');
        await vscode.workspace.fs.createDirectory(vscode.Uri.file(assetsDir));
        const bytes = Buffer.from(dropped.bytesBase64, 'base64');
        // A clean leaf name; collisions get a numeric suffix rather than
        // silently clobbering a different existing file.
        const leaf = dropped.name.replace(/[\\/]/g, '_');
        let candidate = leaf;
        for (let n = 1; ; n++) {
          const full = nodePath.join(assetsDir, candidate);
          try {
            await vscode.workspace.fs.stat(vscode.Uri.file(full));
          } catch {
            await vscode.workspace.fs.writeFile(vscode.Uri.file(full), bytes);
            break;
          }
          const dot = leaf.lastIndexOf('.');
          candidate = dot > 0 ? `${leaf.slice(0, dot)}-${n}${leaf.slice(dot)}` : `${leaf}-${n}`;
        }
        this.post({ kind: 'assetPicked', requestId, path: `assets/${candidate}` });
        return;
      }
      const picked = await vscode.window.showOpenDialog({
        canSelectMany: false,
        filters: dialogFiltersForAccept(accept),
      });
      const fsPath = picked?.[0]?.fsPath;
      if (!fsPath) {
        this.post({ kind: 'assetPicked', requestId }); // user cancelled
        return;
      }
      const rel = nodePath.relative(root, fsPath);
      const inProject = !rel.startsWith('..') && !nodePath.isAbsolute(rel);
      const path = inProject ? rel.split(nodePath.sep).join('/') : fsPath;
      this.post({ kind: 'assetPicked', requestId, path });
    } catch (e) {
      this.post({
        kind: 'assetPicked',
        requestId,
        error: e instanceof Error ? e.message : String(e),
      });
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
  /// The source text the host last PARSED and rendered for the watched doc. The
  /// onDidChangeTextDocument handler skips the self-reparse when the changed doc's
  /// text already equals this: the rendered graph is not stale, so there is nothing
  /// to reparse. This is the timing-independent invariant ("reparse iff the render
  /// is stale"), which subsumes every self-write case without enumerating the save
  /// pipeline's intermediate texts: our own edit write, the save pipeline's
  /// trim-trailing-whitespace / insert-final-newline follow-ups, a format-on-save
  /// participant, and an undo back to the rendered text all leave the text equal to
  /// what we rendered and are correctly skipped; a genuine edit differs and reparses.
  /// (The old time-window flag missed late save-pipeline change events and flickered.)
  private lastRenderedSource: string | null = null;

  /// >0 while THIS controller is writing the watched doc (a graph edit's
  /// writeTextRaw). The `onDidChangeTextDocument` change events for our own
  /// applyEdit + save (and any save participant) are delivered before those
  /// awaits resolve, so they land inside this bracket. Used to suppress the
  /// `codeEditTouched` auto-lock for our own writes: lastRenderedSource still
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
        const openDoc = vscode.workspace.textDocuments.find((d) => d.uri.fsPath === key) ?? doc;
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
          // (postToWebview=false): one message advances source + parse + undo.
          this.applyParseResult(r.parse, r.source, await this.readLayoutCode(doc), false);
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
      // ran). The reply carries the edit-server's message as the rollback
      // toast's reason (minus the wire's `edit: ` envelope prefix); the webview
      // owns its optimistic state, so it owns the rollback (resync + drop the
      // pending op). This is NOT a `parseError` (which would blank a perfectly
      // renderable project).
      const message = err instanceof Error ? err.message : String(err);
      this.post({
        kind: 'editApplied', requestId, ok: false,
        reason: message.replace(/^edit: /, ''),
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
        const openDoc = vscode.workspace.textDocuments.find((d) => d.uri.fsPath === key) ?? doc;
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
