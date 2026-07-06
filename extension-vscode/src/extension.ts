// Weft VS Code extension entrypoint.
//
// Wires together:
//   - the dispatcher HTTP client (one per extension instance)
//   - the graph webview that opens when a .weft file is viewed
//   - the Weft activity-bar sidebar (Projects, Executions, Inspector)
//   - the execution follower that bridges dispatcher SSE events
//     into graph + inspector updates
//   - the VS Code commands that the sidebar, context menus, and
//     keybindings trigger
//
// One commandCenter owns the "what am I currently looking at" state:
// a pinned project (derived from the active .weft file) and an
// optional selected execution. The sidebar items and graph view
// talk to the center through shared callbacks.

import * as vscode from 'vscode';
import { spawn } from 'node:child_process';

import { DispatcherClient } from './dispatcher';
import { GraphViewController } from './graphView';
import { attachDiagnostics } from './diagnostics';
import { ParseServer } from './parseServer';
import { registerStreamingEditApi } from './streamingEdits';
import { textTabsForPath } from './tabs';
import { ActionBarStore } from './actionBarState';

import { ProjectsProvider, ProjectNode, type WeftProject } from './sidebar/projects';
import { ExecutionsProvider, ExecutionNode, type ExecutionSummary } from './sidebar/executions';
import { ExecutionFollower } from './execFollower';
import { AutoFollowController } from './autoFollow';
import type { ActionVerb, ActionErrorDetails, CliEvent } from './shared/protocol';
import { emptyActionAvailability, parseStatusPayload } from './shared/protocol';

export function activate(context: vscode.ExtensionContext) {
  const dispatcher = new DispatcherClient(getDispatcherUrl());

  // One warm `weft parse-server` for the whole extension. Both the graph
  // view (live graph) and diagnostics (Problems panel) parse/validate
  // through it, so the catalog is loaded once and held in memory instead of
  // re-walked on every edit. Spawned lazily on first request, killed on
  // deactivate. cwd is the first workspace folder (the server resolves each
  // request's project from the request file, so cwd only matters for a
  // detached buffer with no file).
  const parseServerCwd = vscode.workspace.workspaceFolders?.[0]?.uri.fsPath ?? process.cwd();
  const parseServer = new ParseServer(parseServerCwd);
  context.subscriptions.push({ dispose: () => parseServer.dispose() });

  const projectsProvider = new ProjectsProvider();
  const executionsProvider = new ExecutionsProvider(dispatcher);

  // Single source of truth for which project + execution the UI is
  // "looking at". Sidebar and graph view both read/write through
  // this so the three stay in sync.
  let pinnedProject: WeftProject | undefined;
  let pinnedExecution: string | undefined;

  const graphView = new GraphViewController(context, dispatcher, parseServer);

  // Action-bar state machine: combines `weft status --json`
  // snapshots (the backend's view) with CLI NDJSON events (the
  // in-flight verb's view) and the auto-follow controller's
  // followStatus into a single derived state per pinned project.
  // The webview is a pure renderer of whatever the store emits.
  const actionBar = new ActionBarStore();
  /// Last status snapshot pushed to the bar for the pinned
  /// project. Cached here so the webview's `ready` handler can
  /// re-receive it after iframe (re)mount; without the cache the
  /// only path is the next status fetch's roundtrip.
  let lastStatusSnapshot: import('./shared/protocol').ActionAvailability | undefined;
  actionBar.subscribe((state) => {
    graphView.post({ kind: 'actionBarState', state });
  });

  graphView.setReadyHandler(() => {
    // Webview just (re)mounted. Push the bar's current derived
    // state and the latest status snapshot directly. Without
    // this, posts that fired before the webview's message
    // listener was up get silently dropped (VS Code restarts
    // with a .weft already open hit this consistently).
    graphView.post({ kind: 'actionBarState', state: actionBar.current() });
    if (lastStatusSnapshot) {
      graphView.post({ kind: 'statusSnapshot', snapshot: lastStatusSnapshot });
    }
  });

  const follower = new ExecutionFollower(
    dispatcher,
    (msg) => graphView.post(msg),
  );

  const autoFollow = new AutoFollowController(
    dispatcher,
    follower,
    (msg) => {
      // Mirror autoFollow's followStatus into the action-bar
      // store so the reducer can compute the watched-live color.
      // Stop button shows iff the user is actually watching a
      // running execution (pinned color in runningColors, or
      // latest mode + something running).
      if (msg.kind === 'followStatus' && pinnedProject) {
        actionBar.setFollow(pinnedProject.id, msg.status.mode, msg.status.color);
      }
      graphView.post(msg);
    },
    (ev) => {
      if (ev.kind === 'execution_started') {
        actionBar.markExecutionStarted(ev.project_id, ev.color);
      } else if (
        ev.kind === 'execution_completed' ||
        ev.kind === 'execution_failed' ||
        ev.kind === 'execution_cancelled'
      ) {
        actionBar.markExecutionFinished(ev.project_id, ev.color);
      }
      scheduleStatusRefresh('sse');
    },
  );

  /// Debounce window for SSE-triggered status refetches. Bursts of
  /// node-start / node-complete events during a run shouldn't
  /// hammer `weft status --json`; we coalesce into one fetch
  /// 500ms after the last event. CLI completion + user-clicked
  /// Refresh fire immediate fetches via direct calls and bypass
  /// this scheduler.
  let sseRefreshTimer: NodeJS.Timeout | undefined;
  function scheduleStatusRefresh(_reason: string): void {
    if (sseRefreshTimer) clearTimeout(sseRefreshTimer);
    sseRefreshTimer = setTimeout(() => {
      sseRefreshTimer = undefined;
      void refreshActionBarFromStatus();
    }, 500);
  }

  /// Live drift detection: when the user edits the project source,
  /// the worker / infra they activated could now be stale. Refetch
  /// status 30s after the LAST parse so the action bar's source
  /// drift / infra drift bits update without a manual Refresh.
  /// Rapid typing keeps deferring the fetch (timer resets on every
  /// new parse) so we only hit `weft status --json` once the user
  /// actually pauses.
  let parseRefreshTimer: NodeJS.Timeout | undefined;
  function scheduleParseDrivenRefresh(): void {
    if (parseRefreshTimer) clearTimeout(parseRefreshTimer);
    parseRefreshTimer = setTimeout(() => {
      parseRefreshTimer = undefined;
      void refreshActionBarFromStatus();
    }, 30_000);
  }

  graphView.setRunHandler(() => runPinned());
  graphView.setParseSuccessHandler(() => scheduleParseDrivenRefresh());
  graphView.setFollowTogglePinHandler(() => autoFollow.togglePin());
  graphView.setFollowCatchUpHandler(() => autoFollow.catchUpToLatest());
  graphView.setLifecycleStartHandler(() => autoFollow.pinAndFollow(undefined));
  graphView.setCliVerbHandler((verb, args) => runCliVerb(verb, args));
  graphView.setCliStatusHandler(() => refreshActionBarFromStatus());
  graphView.setStopActionHandler(() => stopAction());
  graphView.setDismissErrorHandler(() => {
    if (pinnedProject) actionBar.clearError(pinnedProject.id);
  });
  graphView.setReportErrorHandler((verb, message, details) => {
    if (!pinnedProject) return;
    actionBar.setError(pinnedProject.id, verb, message, details);
  });
  graphView.setResolveErrorHandler((verb) => {
    if (!pinnedProject) return;
    actionBar.clearErrorIfVerb(pinnedProject.id, verb);
  });

  async function pinProject(project: WeftProject): Promise<void> {
    // Drop the cached snapshot from whatever was previously
    // pinned. The fresh status fetch below repopulates it.
    lastStatusSnapshot = undefined;
    pinnedProject = project;
    executionsProvider.setPinnedProject(project);
    autoFollow.setProject(project.id);
    // Tell the action-bar store which slot drives webview
    // emissions now. The store keeps every project's slot alive
    // (so an in-flight verb's events keep accumulating in the
    // background); listeners only see the pinned project's view.
    actionBar.setPinnedProject(project.id);
    const doc = await vscode.workspace.openTextDocument(project.entryPath);
    await graphView.open(doc, project.id);
    void graphView.refreshActionAvailability();
  }

  /// User clicked Run. Run is just a CLI verb like the others now;
  /// runPinned exists separately only so the keybinding (Ctrl+Enter)
  /// has a stable target name.
  async function runPinned(): Promise<void> {
    await runCliVerb('run', []);
  }

  /// Spawn a CLI verb and pump its NDJSON event stream into the
  /// action-bar store. Caller passes the verb name + extra args
  /// (e.g. `runCliVerb('infra', ['start'])`). The verb tag drives
  /// store transitions; the extension never re-derives "what verb
  /// is this" from args.
  ///
  /// The store wraps the CLI lifecycle:
  ///   cliStart(verb)       on spawn
  ///   cliEvent(ev)         on each NDJSON line
  ///   complete event       transitions to idle, status refetched
  ///   error event          transitions to error (sticky)
  ///   crash with no event  transitions to error (best-effort msg)
  ///
  /// On success we fire a status refetch to reconcile the bar with
  /// backend ground truth (run started → execution_running).
  async function runCliVerb(verb: string, args: string[]): Promise<void> {
    if (!pinnedProject) {
      void vscode.window.showInformationMessage('Pin a Weft project first.');
      return;
    }
    await graphView.waitForPendingSave();
    // Reactivate-prompt for activate when project is hibernate/park.
    // The CLI's --json mode skips its own terminal prompt, so the
    // extension is responsible for showing the modal when there's
    // preserved state to choose about.
    if (verb === 'activate') {
      const choice = await maybePromptReactivateChoice(pinnedProject);
      if (choice === undefined) {
        // User cancelled the prompt; abort the activate.
        return;
      }
      if (choice !== null) {
        args = [...args, '--reactivate-choice', choice];
      }
    }
    const verbTag = verbTagFor(verb, args);
    // Bind every store mutation to the project that owns the
    // verb. The store keeps each project's slot independent: if
    // the user switches pins mid-verb, the original project's
    // slot still receives all events and the new project's slot
    // is unaffected.
    const projectId = pinnedProject.id;
    const projectRoot = pinnedProject.rootPath;
    actionBar.cliStart(projectId, verbTag);
    try {
      await runWeftCliJson(projectId, [verb, ...args], projectRoot, (ev) => {
        actionBar.cliEvent(projectId, ev);
      });
    } catch (err) {
      const tracking = cliTracking.get(projectId);
      if (tracking?.userKilled) {
        actionBar.cliKilled(projectId);
      } else {
        const message = err instanceof Error ? err.message : String(err);
        const details = (err as { details?: ActionErrorDetails } | undefined)?.details;
        actionBar.cliCrashed(projectId, verbTag, message, details);
      }
    } finally {
      cliTracking.delete(projectId);
      // Sync the verb's project slot to backend ground truth.
      // Refresh runs against the verb's project even if the user
      // has since switched pins; their slot stays accurate for
      // when they pin back.
      void refreshActionBarFromStatus(projectId, projectRoot);
      if (pinnedProject?.id === projectId) {
        void executionsProvider.refresh();
      }
    }
  }

  /// Map (verb, args) onto the CLI's ActionVerb tag. Compound
  /// commands (`infra start`) get folded into a single tag. Unknown
  /// verbs throw: silently mapping a new verb to `run` would leave
  /// the action bar stuck in cli_running for that verb's session.
  function verbTagFor(verb: string, args: string[]): ActionVerb {
    if (verb === 'infra') {
      const sub = args[0] ?? '';
      switch (sub) {
        case 'start': return 'infra_start';
        case 'stop': return 'infra_stop';
        case 'terminate': return 'infra_terminate';
        case 'upgrade': return 'infra_upgrade';
        case 'cancel': return 'infra_cancel';
        case 'node-stop': return 'infra_node_stop';
        case 'node-terminate': return 'infra_node_terminate';
        default:
          throw new Error(`verbTagFor: unknown infra subverb '${sub}'`);
      }
    }
    switch (verb) {
      case 'run': return 'run';
      case 'activate': return 'activate';
      case 'cancel-activate': return 'cancel_activate';
      case 'cancel-build': return 'cancel_build';
      case 'deactivate': return 'deactivate';
      case 'cancel-running': return 'cancel_running';
      case 'resync': return 'resync';
      default:
        throw new Error(`verbTagFor: unknown CLI verb '${verb}'`);
    }
  }

  /// Returns:
  ///   - `null` when the project is in `none` mode OR has no
  ///     preserved state (no prompt shown, CLI activates with
  ///     default behavior).
  ///   - `string` when the user picked one of the three choices.
  ///   - `undefined` when the user cancelled the modal (caller
  ///     aborts the activate).
  async function maybePromptReactivateChoice(
    project: WeftProject,
  ): Promise<string | null | undefined> {
    let status: StatusResult | undefined;
    try {
      status = await fetchActionAvailability(project.rootPath);
    } catch {
      return null;
    }
    if (!status) return null;
    // Reactivate-choice prompt only fires when the project is in
    // an inactive lifecycle state with preserved state worth
    // discussing. Active projects skip; clean-Inactive (no rows)
    // skip; deactivating skips (we should never be activating
    // mid-deactivate via this path; the UI surfaces "Resume
    // Active" instead).
    const ps = status.snapshot.projectStatus;
    if (ps !== 'inactive') return null;
    const parked = status.snapshot.preservation.parked;
    const suspended = status.snapshot.preservation.suspended;
    if (parked === 0 && suspended === 0) return null;
    const choice = await vscode.window.showQuickPick(
      [
        {
          label: 'Execute parked + keep suspensions',
          description: `${parked} parked, ${suspended} suspended`,
          value: 'execute_parked_keep_suspended',
        },
        {
          label: 'Keep suspensions only',
          description: `drops ${parked} parked, keeps ${suspended} suspended`,
          value: 'keep_suspended_only',
        },
        {
          label: 'Wipe all',
          description: 'drops everything preserved, fresh start',
          value: 'wipe_all',
        },
      ],
      {
        title: `Reactivating ${project.label}: preserved state to handle`,
        placeHolder: 'Pick a reactivate choice',
        ignoreFocusOut: true,
      },
    );
    if (!choice) return undefined;
    return choice.value;
  }

  /// Stop button pressed on the pinned project's action bar.
  /// "Stop" is a single user intent: halt whatever the user
  /// thinks is happening. That can be more than one thing at the
  /// same time:
  ///
  ///   - A `weft <verb>` CLI is in flight (cli_running). Kill the
  ///     spawned CLI process group. cargo / docker / kind
  ///     grandchildren die with it.
  ///   - A live execution exists on the project the user is
  ///     watching (watched-live color). POST cancel for that
  ///     color. The dispatcher tears down the worker, journals
  ///     ExecutionFailed { error: "cancelled" }, broadcasts on SSE.
  ///
  /// Both can be true simultaneously: `weft run` spawns a worker
  /// then keeps streaming logs. Stopping the CLI alone leaves the
  /// worker running. So we always check both, and fire both.
  function stopAction(): void {
    if (!pinnedProject) return;
    const projectId = pinnedProject.id;
    const channel = getWeftOutputChannel();
    let actedOn = false;
    if (cliTracking.has(projectId)) {
      // CLI verb: SIGKILL is synchronous from the user's POV
      // (process group dies, runWeftCliJson rejects, store
      // transitions on cliKilled). No HTTP round trip; no
      // pending state needed.
      killCliFor(projectId);
      actedOn = true;
    }
    const liveColor = actionBar.watchedLiveColor(projectId);
    if (liveColor) {
      // HTTP cancel: lock the bar into "Cancelling..." until SSE
      // confirms. The dispatcher enqueues a cancel_execution task;
      // the worker fires its per-color Notify; the loop driver
      // exits Failed { error: "cancelled" }; the journal bridge
      // publishes ExecutionFailed to SSE; the store's
      // markExecutionFinished clears pendingAction.
      actionBar.setPending(projectId, 'run', 'Cancelling...', liveColor);
      channel.appendLine(`> cancel execution ${liveColor}`);
      dispatcher.post(`/executions/${liveColor}/cancel`, {}).catch((err) => {
        channel.appendLine(`! cancel failed: ${err}`);
        // Network failure: revert the pending state so the user
        // can try again. The bar falls back to whatever it would
        // have shown without the cancel intent.
        actionBar.clearPending(projectId);
      });
      actedOn = true;
    }
    if (!actedOn) {
      channel.appendLine('> stop pressed but nothing to stop');
    }
  }

  /// Per-project CLI tracking. Keyed by project_id so concurrent
  /// verbs on different projects don't race each other's
  /// userKilled flag or process handle.
  const cliTracking = new Map<
    string,
    { child: ReturnType<typeof spawn>; userKilled: boolean }
  >();

  function killCliFor(projectId: string): void {
    const entry = cliTracking.get(projectId);
    if (!entry || entry.child.pid === undefined) return;
    entry.userKilled = true;
    const channel = getWeftOutputChannel();
    channel.appendLine(`> cli cancelled by user (project ${projectId})`);
    try {
      if (process.platform === 'win32') {
        entry.child.kill('SIGKILL');
      } else {
        process.kill(-entry.child.pid, 'SIGKILL');
      }
    } catch (err) {
      console.warn('[weft] killCliFor failed:', err);
      try { entry.child.kill('SIGKILL'); } catch { /* nothing else to try */ }
    }
  }

  interface StatusResult {
    snapshot: import('./shared/protocol').ActionAvailability;
    /// Most-recent execution color from the status fetch.
    color: string | undefined;
    /// Whether that color's worker is currently running. SSE
    /// drives the same transition during a live session; this
    /// covers the bootstrap case (graph open / pin switch / a
    /// missed event during a reload).
    isRunning: boolean;
  }

  /// Run `weft status --json` for a specific project root and
  /// shape the result for the action bar. Returns undefined on any
  /// failure so the bar keeps its last-known state instead of
  /// flickering.
  async function fetchActionAvailability(
    projectRoot: string,
  ): Promise<StatusResult | undefined> {
    let out: string;
    try {
      out = await runWeftCliCapture(['--json', 'status'], projectRoot);
    } catch (err) {
      // Project not registered with the dispatcher yet (typical on first graph open,
      // after a wipe, or after a rename), so `weft status` has nothing to report.
      // This is NOT special-cased into a faked action list anymore: the shared
      // ActionBar makes the STARTER verbs (run / activate / infra_start) clickable
      // from the parsed graph shape regardless of backend state, and each verb's CLI
      // path builds + registers on demand. So we return an honest snapshot with an
      // EMPTY available list: the starter verbs light up from shape, and the
      // state-dependent verbs (deactivate, cancel_*, infra_stop/terminate/upgrade,
      // resync) correctly stay hidden until a real status reports the live lifecycle.
      console.warn('[weft] status fetch failed; project not registered yet', err);
      return {
        snapshot: emptyActionAvailability(),
        color: undefined,
        isRunning: false,
      };
    }
    try {
      // The remap lives in the shared package (`parseStatusPayload`)
      // so every host builds the exact same snapshot.
      const json = JSON.parse(out) as import('./shared/protocol').RawStatusPayload;
      const execs = json?.executions ?? {};
      const lastStatus: string | undefined = execs.last_status;
      const lastColor: string | undefined = execs.last_color;
      const isRunning =
        lastStatus === 'running' || lastStatus === 'started' || lastStatus === 'queued';
      return {
        snapshot: parseStatusPayload(json),
        color: typeof lastColor === 'string' ? lastColor : undefined,
        isRunning,
      };
    } catch (err) {
      console.warn('[weft] fetchActionAvailability failed', err);
      return undefined;
    }
  }

  /// Refresh the action bar's view of backend ground truth for a
  /// specific project. One `weft status --json` call yields both
  /// the snapshot (drift + available_actions + infra rollup) and
  /// the active execution color, so the bar can enter
  /// execution_running on graph open / project pin without waiting
  /// for an SSE event.
  ///
  /// Defaults to the pinned project. Pass an explicit (id, root)
  /// when refreshing a non-pinned project's slot (e.g. after the
  /// CLI verb finished on a project the user has since switched
  /// away from).
  async function refreshActionBarFromStatus(
    projectId?: string,
    projectRoot?: string,
  ): Promise<void> {
    const id = projectId ?? pinnedProject?.id;
    const root = projectRoot ?? pinnedProject?.rootPath;
    if (!id || !root) return;
    const result = await fetchActionAvailability(root);
    if (!result) return;
    // Backend snapshot lands first.
    actionBar.pushStatus(
      id,
      result.snapshot,
      result.isRunning ? result.color : undefined,
    );
    // If status reports the most-recent color is terminal, make
    // sure the slot's runningColors set doesn't keep it (covers
    // missed SSE events during reload).
    if (result.color && !result.isRunning) {
      actionBar.markExecutionFinished(id, result.color);
    }
    // Webview only renders the pinned project's snapshot. Older
    // slots get refreshed silently for when the user pins back.
    if (pinnedProject?.id === id) {
      lastStatusSnapshot = result.snapshot;
      graphView.post({ kind: 'statusSnapshot', snapshot: result.snapshot });
    }
  }

  /// Capture stdout from a one-shot CLI invocation. For commands
  /// that emit a single JSON object (status). NDJSON streams use
  /// runWeftCliJson instead.
  async function runWeftCliCapture(args: string[], cwd: string): Promise<string> {
    return new Promise((resolve, reject) => {
      const child = spawn('weft', args, { cwd, env: process.env });
      let stdout = '';
      const channel = getWeftOutputChannel();
      child.stdout?.on('data', (b: Buffer) => { stdout += b.toString(); });
      child.stderr?.on('data', (b: Buffer) => channel.append(b.toString()));
      child.on('error', reject);
      child.on('close', (code) => {
        if (code === 0) resolve(stdout);
        else reject(new Error(`weft ${args.join(' ')} exited ${code}`));
      });
    });
  }

  // Source-change drift refresh: the parse-success handler above
  // (`graphView.setParseSuccessHandler`) covers every edit that
  // produces a parse, including programmatic saves. A separate
  // file-watcher would double-fire the same refresh.

  /// Shell `weft --json <args>` in `cwd` and parse stdout as NDJSON,
  /// dispatching each event to `onEvent`. Stderr streams to the
  /// Weft output channel (compile / docker progress the user can
  /// follow independently).
  ///
  /// Detached spawn so the child leads its own process group;
  /// killCliFor kills the group so cargo / docker / kind
  /// grandchildren die with the parent instead of leaking.
  ///
  /// Per-project tracking: registers `(child, userKilled)` in
  /// cliTracking[projectId] so the Stop button can find the right
  /// child even if the user has multiple verbs in flight on
  /// different projects. Cleared by runCliVerb's finally block.
  async function runWeftCliJson(
    projectId: string,
    args: string[],
    cwd: string,
    onEvent: (ev: CliEvent) => void,
  ): Promise<void> {
    const channel = getWeftOutputChannel();
    const fullArgs = ['--json', ...args];
    const command = `weft ${fullArgs.join(' ')}`;
    channel.appendLine(`> ${command}  (${cwd})`);
    return new Promise((resolve, reject) => {
      const child = spawn('weft', fullArgs, {
        cwd,
        env: process.env,
        detached: process.platform !== 'win32',
      });
      cliTracking.set(projectId, { child, userKilled: false });

      // Capture stderr so a non-zero exit can include the failure
      // explanation in the rejection, not just "exit code N".
      let stderrBuf = '';
      // Capture stdout text that didn't parse as a JSON CliEvent: still
      // useful for the modal when the CLI fell back to plain text.
      let nonJsonStdout = '';
      let buffer = '';
      // Long-running CLIs (cargo --verbose, docker build with many
      // layers) can spew megabytes of stderr; the only consumer is
      // the modal showing the last few lines on failure. Cap both
      // accumulators at 64 KB and keep the TAIL (where the cause
      // lives), not the head.
      const STREAM_BUF_CAP = 64 * 1024;
      const appendCapped = (buf: string, text: string): string => {
        const combined = buf + text;
        if (combined.length <= STREAM_BUF_CAP) return combined;
        return combined.slice(combined.length - STREAM_BUF_CAP);
      };
      const handleLine = (line: string) => {
        const trimmed = line.trim();
        if (!trimmed) return;
        try {
          const ev = JSON.parse(trimmed) as CliEvent;
          onEvent(ev);
        } catch {
          channel.appendLine(`[non-json stdout] ${trimmed}`);
          nonJsonStdout = appendCapped(nonJsonStdout, trimmed + '\n');
        }
      };
      child.stdout?.on('data', (chunk: Buffer) => {
        buffer += chunk.toString();
        let nl;
        while ((nl = buffer.indexOf('\n')) !== -1) {
          const line = buffer.slice(0, nl);
          buffer = buffer.slice(nl + 1);
          handleLine(line);
        }
      });
      child.stderr?.on('data', (chunk: Buffer) => {
        const text = chunk.toString();
        stderrBuf = appendCapped(stderrBuf, text);
        channel.append(text);
      });
      child.on('error', (err) => reject(err));
      child.on('close', (code, signal) => {
        if (buffer.length > 0) handleLine(buffer);
        if (code === 0) {
          resolve();
          return;
        }
        const raw = [stderrBuf.trim(), nonJsonStdout.trim()]
          .filter(Boolean)
          .join('\n\n');
        const head = extractErrorHeadline(stderrBuf)
          ?? extractErrorHeadline(nonJsonStdout);
        const message = head
          ? truncate(head, 200)
          : (signal
              ? `weft ${args.join(' ')} terminated by ${signal}`
              : `weft ${args.join(' ')} exited ${code}`);
        const details: ActionErrorDetails = {
          what: `Running '${args.join(' ')}'`,
          stage: 'cli',
          diagnostics: head
            ? [{ severity: 'error', message: head }]
            : [],
          ...(raw ? { raw } : {}),
          ...(typeof code === 'number' ? { exitCode: code } : {}),
          command,
        };
        const err = new Error(message) as Error & { details?: ActionErrorDetails };
        err.details = details;
        reject(err);
      });
    });
  }

  /// Pick a one-line headline from a stderr/stdout dump. CLI tools
  /// that drive docker/buildkit/cargo flood stderr with progress
  /// (`#0 building...`, `Compiling foo`, `[2/8] downloading`) and
  /// emit the real failure as a final summary line. Strategy:
  ///   1. Prefer the LAST line matching `error:` / `Error:` /
  ///      `ERROR:` / `Caused by:` patterns. That's almost always the
  ///      root cause in chained anyhow / cargo / docker output.
  ///   2. Otherwise, prefer the LAST non-progress non-empty line.
  ///      A progress line is one starting with `#`, `[`, or any of
  ///      the docker buildkit step markers.
  ///   3. Fall back to the first non-empty line so we always show
  ///      SOMETHING.
  function extractErrorHeadline(s: string): string | undefined {
    if (!s) return undefined;
    // Keep RAW lines (untrimmed) for the anyhow scan below: indentation
    // is what distinguishes a cause's continuation from the end of the
    // chain. The trimmed/filtered view is used by the later passes.
    const rawLines = s.split('\n');
    const lines = rawLines.map(l => l.trim()).filter(l => l.length > 0);
    if (lines.length === 0) return undefined;

    // Pass 1: a numbered anyhow chain. anyhow prints a multi-cause
    // chain as `Caused by:` then INDENTED `    0: ...`, `    1: ...`,
    // ... where the HIGHEST number is the ROOT cause. The scan is GATED
    // to the indented block right after `Caused by:`: bare `N: ...`
    // lines elsewhere (docker `2: Pulling fs layer`, a port
    // `8080: bind failed`, a `Stack backtrace:` block's own numbered
    // frames) are NOT anyhow causes and must not hijack the headline.
    //
    // Continuation handling: a cause whose Display wraps prints its
    // extra lines INDENTED (deeper than the `N:` entries), so within
    // the region an indented non-`N:` line is a continuation (ignore,
    // keep scanning) while a line at COLUMN 0 (not indented) ENDS the
    // region (it's the blank/next section after the chain). This keeps
    // wrapped causes inside the block AND closes it before trailing
    // numbered noise, which a "never reset" scan would wrongly capture.
    let inCausedBy = false;
    let deepestNum = -1;
    let deepestLine: string | undefined;
    for (const raw of rawLines) {
      const line = raw.trim();
      if (/^caused by:/i.test(line)) {
        // A fresh `Caused by:` opens a new chain. Reset the
        // accumulator so that among MULTIPLE anyhow chains in one
        // buffer (a retried op that failed twice) the LAST chain's
        // root wins, not the first. (Edge: if the final error is plain
        // non-anyhow with no `Caused by:`, an earlier chain still wins
        // here; the user sees a real cause either way, just possibly
        // the earlier one.)
        inCausedBy = true;
        deepestNum = -1;
        deepestLine = undefined;
        continue;
      }
      if (!inCausedBy) continue;
      const indented = /^\s/.test(raw);
      const m = /^(\d+):\s*(.+)$/.exec(line);
      if (m && indented) {
        const n = Number(m[1]);
        if (n >= deepestNum) {
          deepestNum = n;
          deepestLine = m[2];
        }
      } else if (line.length > 0 && !indented) {
        // A COLUMN-0 non-empty line ends the indented Caused-by block
        // (the next section: `Stack backtrace:`, more tool output,
        // etc). Close the region so its numbered frames can't leak in.
        // A BLANK line does NOT close: anyhow can emit blank lines
        // inside a multi-paragraph cause Display, and closing on those
        // would cut the chain short and return a shallower cause.
        inCausedBy = false;
      }
      // else: a blank line, or an indented non-`N:` line (continuation
      // of the current cause) inside the chain; ignore and keep
      // scanning within the region.
    }
    if (deepestLine !== undefined) return deepestLine;
    // Single-cause chain (a `Caused by:` followed by a plain, non-
    // numbered line): the root is the line right after `Caused by:`.
    for (let i = lines.length - 1; i >= 0; i--) {
      if (/^caused by:/i.test(lines[i]) && i + 1 < lines.length) {
        return lines[i + 1];
      }
    }
    // Pass 2: last explicit error-prefixed line. Require the colon
    // (cargo emits `error:` and `error[E0277]:`; buildkit's
    // `ERROR: cancelled` shutdown banner also lands here, but it
    // SHOULD: it's a real signal and the user can see the cause
    // underneath in the modal's raw section. The earlier shape
    // also matched bare `ERROR` (no colon), which picked up
    // unrelated lines like `error count: 3` from test runners.
    for (let i = lines.length - 1; i >= 0; i--) {
      if (/^(error(\[[a-z0-9]+\])?|err|fatal):\s/i.test(lines[i])) {
        return lines[i];
      }
    }
    // Pass 3: last non-progress line. Progress lines start with `#`
    // (docker buildkit), `[`, or are pure numeric "step n/m" markers.
    for (let i = lines.length - 1; i >= 0; i--) {
      if (isProgressLine(lines[i])) continue;
      return lines[i];
    }
    // Pass 4: total fallback. Even progress is better than nothing.
    return lines[0];
  }

  function isProgressLine(line: string): boolean {
    if (line.startsWith('#')) return true;        // buildkit step header
    if (line.startsWith('[')) return true;        // cargo / many tools
    if (/^\s*\d+\.\d+s/.test(line)) return true;  // buildkit timing
    return false;
  }

  function truncate(s: string, max: number): string {
    return s.length <= max ? s : s.slice(0, max - 1) + '…';
  }

  let weftOutputChannel: vscode.OutputChannel | undefined;
  function getWeftOutputChannel(): vscode.OutputChannel {
    if (!weftOutputChannel) {
      weftOutputChannel = vscode.window.createOutputChannel('Weft');
      context.subscriptions.push(weftOutputChannel);
    }
    return weftOutputChannel;
  }

  async function viewExecution(summary: ExecutionSummary): Promise<void> {
    // Find (or hint) the project that produced this execution and
    // switch the graph to it, then pin auto-follow on it. The
    // controller handles the replay itself.
    const match = projectsProvider.projects().find((p) => p.id === summary.project_id);
    if (match && pinnedProject?.id !== match.id) {
      await pinProject(match);
    }
    pinnedExecution = summary.color;
    autoFollow.pinToExecution(summary.color);
  }

  async function deleteExecution(summary: ExecutionSummary): Promise<void> {
    if (summary.status.toLowerCase() === 'running') {
      // Cancel first so the runtime stops emitting new events and
      // drops pulses, then delete the journal. Prevents stray events
      // after the row disappears.
      try {
        await dispatcher.post(`/executions/${summary.color}/cancel`, {});
      } catch {
        /* keep going */
      }
    }
    if (pinnedExecution === summary.color) {
      follower.stop();
      pinnedExecution = undefined;
    }
    try {
      await dispatcher.del(`/executions/${summary.color}`);
    } catch (err) {
      void vscode.window.showErrorMessage(`Delete failed: ${err}`);
    }
    await executionsProvider.refresh();
  }

  async function clearAllExecutions(): Promise<void> {
    const confirm = await vscode.window.showWarningMessage(
      'Delete all executions? Running ones will be cancelled first.',
      { modal: true },
      'Delete all',
    );
    if (confirm !== 'Delete all') return;
    const all = executionsProvider.summaries();
    for (const s of all) await deleteExecution(s);
  }

  // Register sidebar views + commands.
  context.subscriptions.push(
    vscode.window.registerTreeDataProvider('weft.projects', projectsProvider),
    vscode.window.registerTreeDataProvider('weft.executions', executionsProvider),
    // Close the executions tree's SSE subscription on extension
    // shutdown so reloading VS Code doesn't pile up stale streams
    // against the dispatcher.
    { dispose: () => executionsProvider.dispose() },
    { dispose: () => autoFollow.dispose() },

    vscode.commands.registerCommand('weft.refreshProjects', () => projectsProvider.refresh()),
    vscode.commands.registerCommand('weft.refreshExecutions', () => executionsProvider.refresh()),
    vscode.commands.registerCommand('weft.openInEditor', (p: ProjectNode | WeftProject) => {
      const project = 'project' in p ? p.project : p;
      return pinProject(project);
    }),
    vscode.commands.registerCommand('weft.runProject', (p?: ProjectNode | WeftProject) => {
      if (p) {
        const project = 'project' in p ? p.project : p;
        return pinProject(project).then(runPinned);
      }
      return runPinned();
    }),
    vscode.commands.registerCommand('weft.stopProject', () => stopAction()),

    vscode.commands.registerCommand('weft.viewExecution', (n: ExecutionNode | ExecutionSummary) =>
      viewExecution('summary' in n ? n.summary : n),
    ),
    vscode.commands.registerCommand('weft.deleteExecution', (n: ExecutionNode | ExecutionSummary) =>
      deleteExecution('summary' in n ? n.summary : n),
    ),
    vscode.commands.registerCommand('weft.clearExecutions', () => clearAllExecutions()),
    vscode.commands.registerCommand('weft.loadMoreExecutions', () =>
      executionsProvider.loadMore(),
    ),

    // Legacy commands kept so keybindings/URIs that reference them
    // still work.
    vscode.commands.registerCommand('weft.openGraphView', async () => {
      const editor = vscode.window.activeTextEditor;
      if (!editor || editor.document.languageId !== 'weft') {
        void vscode.window.showInformationMessage('Open a .weft file first.');
        return;
      }
      await graphView.open(editor.document);
    }),
  );

  // .weft files default to the graph view, not the text editor.
  // When a .weft becomes the active text editor AND the graph
  // panel doesn't exist yet (cold open via Ctrl+P, explorer
  // double-click, restored editor on startup), pin its project,
  // open the graph in the same column, and close the underlying
  // text tab. The user can summon the text via the graph's
  // "Open source" button when they want it.
  // The one `.weft` text view the user opened ON PURPOSE (the Source button).
  // Every other active `.weft` text editor is a click that should drive the
  // graph instead of showing code. Identified by URI so it's robust to which
  // column VS Code happens to place tabs in.
  let sourceViewPath: string | undefined;
  graphView.setOpenSourceHandler(async () => {
    // Open the source of the file the graph is CURRENTLY showing, which tracks
    // include navigation (greeter.weft when navigated in), not the project
    // entry. Falls back to the pinned entry if nothing is shown yet.
    const target = graphView.currentFilePath() ?? pinnedProject?.entryPath;
    if (!target) return;
    sourceViewPath = target;
    // Already open somewhere? Reveal the existing tab instead of
    // creating a new one. Otherwise repeated clicks pile up tabs.
    const existing = textTabsForPath(target)[0];
    if (existing) {
      const column = existing.group.viewColumn;
      const doc = await vscode.workspace.openTextDocument(target);
      await vscode.window.showTextDocument(doc, {
        preview: false,
        viewColumn: column,
        preserveFocus: false,
      });
      return;
    }
    // Source opens in `Beside` (column 2). The graph webview
    // stays in column 1.
    //
    // We tried hard to get source-on-the-LEFT, graph-on-the-
    // right via `panel.reveal(Two)` then `showTextDocument(One)`,
    // but moving a webview between columns destroys its iframe
    // (microsoft/vscode#141001) and the canvas blanks out. There
    // is also no built-in command to swap editor GROUPS
    // (microsoft/vscode#85123, closed as backlog). So we settle
    // for the inverse layout the platform supports: graph on
    // the left, source on the right.
    const doc = await vscode.workspace.openTextDocument(target);
    await vscode.window.showTextDocument(doc, {
      preview: false,
      viewColumn: vscode.ViewColumn.Beside,
    });
  });

  // When the Source-view tab is closed, forget it: clicking that file again
  // should then drive the graph (it's no longer the deliberate code view).
  context.subscriptions.push(
    vscode.window.tabGroups.onDidChangeTabs(() => {
      if (sourceViewPath === undefined) return;
      if (textTabsForPath(sourceViewPath).length === 0) sourceViewPath = undefined;
    }),
  );

  context.subscriptions.push(
    vscode.window.onDidChangeActiveTextEditor(async (ed) => {
      if (!ed || ed.document.languageId !== 'weft') return;

      // Pin the project when this `.weft` is a known project entry (drives the
      // action bar / executions). Nested non-entry `.weft` files still get the
      // graph treatment below; they just don't change the pin.
      const found = projectsProvider
        .projects()
        .find((p) => p.entryPath === ed.document.uri.fsPath);
      if (found) {
        pinnedProject = found;
        executionsProvider.setPinnedProject(found);
        autoFollow.setProject(found.id);
      }

      // A `.weft` should be viewed as a GRAPH, not code: clicking any `.weft`
      // (entry or nested) drives the graph view. The one exception is the
      // deliberate "Source" view (the Source button), tracked by URI. So:
      //   - this text editor IS the Source view -> leave it (intentional code).
      //   - graph open -> the click popped a stray text tab over the graph; the
      //     graph's own handler already switched to this file, so reveal the
      //     graph and close the stray tab.
      //   - graph not open -> cold open: show the graph, close the text tab.
      if (ed.document.uri.fsPath === sourceViewPath) return;

      const docUri = ed.document.uri;
      const closeStrayTextTab = async () => {
        const tabs = textTabsForPath(docUri.fsPath).map((e) => e.tab);
        if (tabs.length > 0) await vscode.window.tabGroups.close(tabs);
      };

      if (graphView.isOpen()) {
        graphView.reveal();
        await closeStrayTextTab();
        return;
      }

      // Cold open: show the graph for this file and close the text tab.
      if (found) await pinProject(found);
      else await graphView.open(ed.document);
      await closeStrayTextTab();
    }),
  );

  attachDiagnostics(context, parseServer);

  context.subscriptions.push(
    registerStreamingEditApi(),
    follower,
    vscode.workspace.onDidChangeConfiguration((e) => {
      if (e.affectsConfiguration('weft.dispatcherUrl')) {
        dispatcher.setBaseUrl(getDispatcherUrl());
      }
    }),
  );

  // Kick off first refreshes in the background.
  void projectsProvider.refresh();
  void executionsProvider.refresh();
}

export function deactivate() {}

function getDispatcherUrl(): string {
  return (
    vscode.workspace.getConfiguration('weft').get<string>('dispatcherUrl') ?? 'http://localhost:9999'
  );
}

