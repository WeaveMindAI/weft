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
import { HttpError } from './dispatcher';
import type { HostMessage, LiveDataItem, ProjectDefinition, WebviewMessage } from './shared/protocol';
import { readProjectIdFromToml } from './sidebar/projects';

export class GraphViewController {
  private panel: vscode.WebviewPanel | undefined;
  private watchedDoc: vscode.TextDocument | undefined;
  private watchedProjectId: string | undefined;
  private parseTimer: NodeJS.Timeout | undefined;
  private disposables: vscode.Disposable[] = [];
  private lastProject: ProjectDefinition | undefined;
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
  // Set while we're applying our own TextEdit to the document.
  // onDidChangeTextDocument fires during the edit; if we parsed
  // twice (once for the webview save, once for the VS Code change)
  // we'd loop.
  private suppressReparse = false;
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
  // sidecar's /live is cheap (returns current state snapshot), so
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
      this.syncSignalDisplayPollers(response);
      this.parseSuccessHandler?.();
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
          // The catalog entry's NodeMetadata still uses snake_case
          // (`requires_infra`), so we check both. NodeDefinition's
          // own field is `requiresInfra` (camelCase) per its wire
          // schema.
          const entry = response.catalog[n.nodeType];
          return n.requiresInfra ?? entry?.requires_infra ?? false;
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
  /// Show a quick-pick for preservation mode and dispatch
  /// `weft deactivate --mode <choice>`. The CLI's interactive prompt
  /// only fires for human terminals; under `--json` it would silently
  /// pick "wipe", which is dangerous (HumanQuery flows mid-suspension
  /// would die). Surfacing the picker host-side keeps the choice
  /// explicit and matches the terminal-CLI behavior.
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

  private async runDeactivate(): Promise<void> {
    // Step 1: preservation mode (wipe vs hibernate vs park).
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
      placeHolder: 'Choose preservation mode',
      ignoreFocusOut: true,
    });
    if (!modeChoice) return;

    // Step 2: running-execution policy. Skipped for wipe (which
    // forces cancel because waiting before wiping is contradictory).
    // For preservation modes, wait is the safe default (in-flight
    // runs finish naturally; new fires already park). Cancel is
    // the panic button.
    let runningPolicy: 'wait' | 'cancel';
    if (modeChoice.mode === 'wipe') {
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
      const runningChoice = await vscode.window.showQuickPick(runningPolicies, {
        placeHolder: 'How should running executions be handled?',
        ignoreFocusOut: true,
      });
      if (!runningChoice) return;
      runningPolicy = runningChoice.value;
    }

    // Step 3: hibernate-specific grace window.
    const args = [
      '--mode',
      modeChoice.mode,
      '--running-policy',
      runningPolicy,
    ];
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
      if (grace === undefined) return;
      args.push('--grace', String(Number(grace.trim())));
    }
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
      case 'infraStart':
        void this.dispatchVerb('infra', ['start']);
        break;
      case 'infraStop':
        void this.dispatchVerb('infra', ['stop']);
        break;
      case 'infraTerminate':
        void this.dispatchVerb('infra', ['terminate']);
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
        void this.dispatchVerb('infra', ['upgrade']);
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
    // we must NOT redirect this write to the new doc; the
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

  private onDispose(): void {
    if (this.parseTimer) clearTimeout(this.parseTimer);
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
