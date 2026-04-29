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
import { createHash } from 'node:crypto';
import { promises as fsp } from 'node:fs';
import * as nodePath from 'node:path';

import { DispatcherClient } from './dispatcher';
import { GraphViewController } from './graphView';
import { attachDiagnostics } from './diagnostics';
import { registerStreamingEditApi } from './streamingEdits';

import { ProjectsProvider, ProjectNode, type WeftProject } from './sidebar/projects';
import { ExecutionsProvider, ExecutionNode, type ExecutionSummary } from './sidebar/executions';
import { InspectorView, type InspectorSelection } from './sidebar/inspector';
import { ExecutionFollower } from './execFollower';
import { AutoFollowController } from './autoFollow';

export function activate(context: vscode.ExtensionContext) {
  const dispatcher = new DispatcherClient(getDispatcherUrl());

  const projectsProvider = new ProjectsProvider();
  const executionsProvider = new ExecutionsProvider(dispatcher);
  const inspector = new InspectorView(context.extensionUri);

  // Single source of truth for which project + execution the UI is
  // "looking at". Sidebar and graph view both read/write through
  // this so the three stay in sync.
  let pinnedProject: WeftProject | undefined;
  let pinnedExecution: string | undefined;

  const graphView = new GraphViewController(context, dispatcher);

  const follower = new ExecutionFollower(
    dispatcher,
    (msg) => graphView.post(msg),
    (nodeId, patch) => inspector.updateLive(nodeId, patch),
  );

  const autoFollow = new AutoFollowController(
    dispatcher,
    follower,
    (msg) => graphView.post(msg),
  );

  graphView.setRunHandler(() => runPinned());
  graphView.setStopHandler(() => stopPinned());
  graphView.setFollowTogglePinHandler(() => autoFollow.togglePin());
  graphView.setFollowCatchUpHandler(() => autoFollow.catchUpToLatest());
  graphView.setLifecycleStartHandler(() => autoFollow.pinAndFollow(undefined));
  graphView.setEnsureBuildHandler((verb) => ensurePinnedBuild(verb));
  graphView.setCancelBuildHandler(() => cancelActiveBuild());
  graphView.setNodeSelectionHandler((sel) => {
    if (!pinnedProject) return;
    inspector.setSelection(
      sel
        ? {
            projectId: pinnedProject.id,
            nodeId: sel.nodeId,
            nodeType: sel.nodeType,
            label: sel.label,
            config: sel.config,
            inputs: sel.inputs,
            outputs: sel.outputs,
          }
        : undefined,
    );
  });

  async function pinProject(project: WeftProject): Promise<void> {
    pinnedProject = project;
    executionsProvider.setPinnedProject(project);
    autoFollow.setProject(project.id);
    // Load the document so graphView has something to watch, but
    // do NOT show the text editor: the graph is the default surface
    // for a .weft. Users open the source via the graph's "Open
    // source" button (lands in ViewColumn.Beside).
    const doc = await vscode.workspace.openTextDocument(project.entryPath);
    await graphView.open(doc, project.id);
  }

  async function runPinned(): Promise<void> {
    if (!pinnedProject) {
      void vscode.window.showInformationMessage('Pin a Weft project first.');
      return;
    }
    try {
      await ensurePinnedBuild('run');
      const source = await readFile(pinnedProject.entryPath);
      await dispatcher.post(`/projects`, {
        id: pinnedProject.id,
        name: pinnedProject.label,
        source,
        root: pinnedProject.rootPath,
      });
      const resp = await dispatcher.post<{ color: string }>(`/projects/${pinnedProject.id}/run`, {});
      pinnedExecution = resp.color;
      autoFollow.pinAndFollow(resp.color);
      await executionsProvider.refresh();
    } catch (err) {
      void vscode.window.showErrorMessage(`Run failed: ${err}`);
    }
  }

  /// Rebuild the pinned project's worker image when the source
  /// has changed since the last build, or when the cached image
  /// is missing from local docker. Runs before any graph-bar
  /// verb that spawns worker pods (Run, Activate, InfraStart).
  ///
  /// Skip semantics: hash main.weft + weft.toml + every file
  /// under nodes/ → compare against last-build hash. If equal
  /// AND the worker image still exists in docker, the build is
  /// a no-op so we save the multi-second docker round-trip.
  /// Either signal mismatched → run `weft build` and refresh
  /// the cache.
  ///
  /// `verb` lets the webview show "Building..." in place of
  /// "Running..." / "Starting..." while the cargo+docker work
  /// is in flight. Skipped builds emit no buildState transition
  /// so the UI never flickers.
  ///
  /// Serialized via `buildChain`: every call appends its work to
  /// a single chain so no two builds ever overlap. The cache
  /// check inside the work IIFE runs AFTER any prior build has
  /// finished, so two callers arriving close together (Run +
  /// Activate clicked back-to-back) see the same eventual cache
  /// hit and the second one skips. Without this, both would
  /// spawn `weft build` in parallel — racing for the same docker
  /// image tag and cargo target/, and `cancelActiveBuild` could
  /// only see (and kill) the second of the two children.
  let buildChain: Promise<void> = Promise.resolve();

  async function ensurePinnedBuild(
    verb: 'run' | 'activate' | 'infraStart',
  ): Promise<void> {
    if (!pinnedProject) return;
    const work = buildChain.catch(() => undefined).then(async () => {
      // Re-snapshot pinnedProject AFTER the chain settles. The
      // user may have switched projects while we were queued;
      // we want the project that's pinned NOW, not the one
      // pinned at call time.
      const project = pinnedProject;
      if (!project) return;
      // The graph editor's debounced saveWeft may still be in
      // flight when the user clicks Run / Activate / InfraStart.
      // Wait for it to land on disk so the hash + readFile see
      // the freshest source. graphView resolves immediately if
      // nothing is pending.
      await graphView.waitForPendingSave();
      const hash = await hashProjectInputs(project.rootPath);
      // v3 cache key: bumped when we added the weft-binary fingerprint
      // to `hashProjectInputs`. Old cached entries didn't include the
      // engine's binary identity so worker images stayed stale across
      // engine upgrades; the bump forces every project to re-check
      // once and rebuild if needed.
      const cacheKey = `weft.lastBuild.v4.${project.id}`;
      const last = context.workspaceState.get<string>(cacheKey);
      const tag = `weft-worker-${project.id}:latest`;
      if (last === hash && (await workerImageInCluster(tag))) {
        // Source + engine binary unchanged + image still loaded in
        // the cluster: nothing to do.
        return;
      }
      graphView.post({ kind: 'buildState', active: true, verb });
      try {
        await runWeftCli(['build'], project.rootPath);
        await context.workspaceState.update(cacheKey, hash);
      } finally {
        graphView.post({ kind: 'buildState', active: false });
      }
    });
    buildChain = work;
    return work;
  }

  async function hashProjectInputs(root: string): Promise<string> {
    const h = createHash('sha256');
    const files: string[] = [];
    const candidates = ['main.weft', 'weft.toml'];
    for (const rel of candidates) {
      const abs = nodePath.join(root, rel);
      try {
        await fsp.access(abs);
        files.push(abs);
      } catch {
        // File missing — skip; the build will fail loudly.
      }
    }
    const nodesDir = nodePath.join(root, 'nodes');
    try {
      await collectFiles(nodesDir, files);
    } catch {
      // No nodes/ dir is fine.
    }
    files.sort();
    for (const abs of files) {
      const buf = await fsp.readFile(abs);
      h.update(nodePath.relative(root, abs));
      h.update('\0');
      h.update(buf);
      h.update('\0');
    }
    // Fold the active `weft` binary's identity into the hash so the
    // cache invalidates whenever the user rebuilds the CLI (which
    // happens any time engine / catalog Rust source changes). The
    // worker image bakes those crates into its build, so a stale
    // engine + cached image drops back-channel updates the new
    // engine relies on (e.g. emitted_pulses for expand-fan-out).
    const weftFingerprint = await weftBinaryFingerprint();
    h.update('weft-binary\0');
    h.update(weftFingerprint);
    return h.digest('hex');
  }

  /// Return a stable identity string for the `weft` binary on PATH:
  /// `<absolute-path>:<size>:<mtimeNs>`. Falls back to the literal
  /// "weft" if resolution fails so we still hash *something*; that
  /// case is rare (would mean weft isn't on PATH) and the build
  /// would fail loudly anyway.
  async function weftBinaryFingerprint(): Promise<string> {
    try {
      const which = await new Promise<string>((resolve, reject) => {
        const child = spawn(process.platform === 'win32' ? 'where' : 'which', ['weft'], {
          stdio: ['ignore', 'pipe', 'ignore'],
        });
        let out = '';
        child.stdout?.on('data', (b: Buffer) => { out += b.toString(); });
        child.on('error', reject);
        child.on('close', (code) => {
          if (code === 0) resolve(out.split(/\r?\n/)[0]?.trim() ?? '');
          else reject(new Error(`which weft exited ${code}`));
        });
      });
      if (!which) return 'weft';
      const stat = await fsp.stat(which);
      // mtime in ms is enough granularity for "did this file change."
      return `${which}:${stat.size}:${stat.mtimeMs}`;
    } catch {
      return 'weft';
    }
  }

  async function collectFiles(dir: string, out: string[]): Promise<void> {
    const entries = await fsp.readdir(dir, { withFileTypes: true });
    for (const e of entries) {
      const abs = nodePath.join(dir, e.name);
      if (e.isDirectory()) {
        await collectFiles(abs, out);
      } else if (e.isFile()) {
        out.push(abs);
      }
    }
  }

  /// Check whether the worker image is loaded into the kind
  /// cluster's containerd. Host docker is irrelevant: the
  /// dispatcher pod pulls from the cluster's runtime, not from
  /// the host. A `setup.sh --purge` deletes the kind cluster but
  /// leaves the host docker image; without this check the cache
  /// would say "image is fine" while the cluster has nothing.
  ///
  /// Implementation note: `crictl images -q <ref>` returns exit 0
  /// and dumps EVERY image ID when the ref doesn't match (it
  /// silently ignores the filter), so we can't trust its exit
  /// code. Instead parse the columnar `crictl images` output and
  /// match the repository + tag pair exactly.
  async function workerImageInCluster(tag: string): Promise<boolean> {
    const cluster = process.env.WEFT_CLUSTER_NAME ?? 'weft-local';
    const node = `${cluster}-control-plane`;
    const colon = tag.lastIndexOf(':');
    const namePart = colon >= 0 ? tag.slice(0, colon) : tag;
    const tagPart = colon >= 0 ? tag.slice(colon + 1) : 'latest';
    // kind load normalizes unprefixed tags to docker.io/library/<name>.
    const expectedRepo = namePart.includes('/')
      ? namePart
      : `docker.io/library/${namePart}`;
    return new Promise((resolve) => {
      const child = spawn('docker', ['exec', node, 'crictl', 'images'], {
        stdio: ['ignore', 'pipe', 'ignore'],
      });
      let stdout = '';
      child.stdout?.on('data', (chunk) => {
        stdout += chunk.toString();
      });
      child.on('error', () => resolve(false));
      child.on('close', () => {
        for (const line of stdout.split('\n')) {
          const cols = line.trim().split(/\s+/);
          if (cols.length < 2) continue;
          if (cols[0] === expectedRepo && cols[1] === tagPart) {
            resolve(true);
            return;
          }
        }
        resolve(false);
      });
    });
  }

  /// Tracks the currently-running `weft build` child so the
  /// webview's "Stop" button can kill it. Only one build runs
  /// at a time (Run/Activate/InfraStart all serialize through
  /// ensurePinnedBuild).
  let activeBuildChild: ReturnType<typeof spawn> | undefined;

  function cancelActiveBuild(): void {
    const child = activeBuildChild;
    if (!child || child.pid === undefined) return;
    const channel = getWeftOutputChannel();
    channel.appendLine('> build cancelled by user');
    // `weft build` shells out to docker / cargo / kind, which all
    // run as grandchildren. SIGTERM to the `weft` parent alone
    // leaves them orphaned (still building in the background).
    // We spawn `weft` detached so it leads its own process group,
    // then SIGKILL the whole group via the negative pid: a clean
    // way to take everything down at once. `process.kill(-pid)`
    // is POSIX-portable; on Windows the spawn options don't
    // create a process group so we fall back to killing the
    // child alone (Windows isn't supported by the dispatcher's
    // kind-based local mode anyway).
    try {
      if (process.platform === 'win32') {
        child.kill('SIGKILL');
      } else {
        process.kill(-child.pid, 'SIGKILL');
      }
    } catch (err) {
      console.warn('[weft] cancelActiveBuild kill failed:', err);
      // Fallback: kill just the parent. Children may linger but
      // the runWeftCli promise still rejects on close, so the
      // UI returns to idle.
      try { child.kill('SIGKILL'); } catch { /* nothing else to try */ }
    }
  }

  /// Shell `weft <args>` in the project root and surface output
  /// through a dedicated VS Code OutputChannel so the user can
  /// see compile / docker progress without leaving the editor.
  /// Detached spawn so the child leads its own process group;
  /// cancelActiveBuild kills the group so cargo / docker / kind
  /// children die with the parent instead of leaking.
  async function runWeftCli(args: string[], cwd: string): Promise<void> {
    const channel = getWeftOutputChannel();
    channel.show(true);
    channel.appendLine(`> weft ${args.join(' ')}  (${cwd})`);
    return new Promise((resolve, reject) => {
      const child = spawn('weft', args, {
        cwd,
        env: process.env,
        detached: process.platform !== 'win32',
      });
      activeBuildChild = child;
      child.stdout?.on('data', (chunk: Buffer) =>
        channel.append(chunk.toString()),
      );
      child.stderr?.on('data', (chunk: Buffer) =>
        channel.append(chunk.toString()),
      );
      child.on('error', (err) => {
        activeBuildChild = undefined;
        reject(err);
      });
      child.on('close', (code, signal) => {
        activeBuildChild = undefined;
        if (code === 0) {
          resolve();
        } else if (signal) {
          reject(new Error(`weft ${args.join(' ')} terminated by ${signal}`));
        } else {
          reject(new Error(`weft ${args.join(' ')} exited ${code}`));
        }
      });
    });
  }

  let weftOutputChannel: vscode.OutputChannel | undefined;
  function getWeftOutputChannel(): vscode.OutputChannel {
    if (!weftOutputChannel) {
      weftOutputChannel = vscode.window.createOutputChannel('Weft');
      context.subscriptions.push(weftOutputChannel);
    }
    return weftOutputChannel;
  }

  async function stopPinned(): Promise<void> {
    if (!pinnedExecution) return;
    try {
      await dispatcher.post(`/executions/${pinnedExecution}/cancel`, {});
    } catch (err) {
      void vscode.window.showErrorMessage(`Stop failed: ${err}`);
    }
    follower.stop();
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
    vscode.window.registerWebviewViewProvider('weft.inspector', inspector),

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
    vscode.commands.registerCommand('weft.stopProject', () => stopPinned()),

    vscode.commands.registerCommand('weft.viewExecution', (n: ExecutionNode | ExecutionSummary) =>
      viewExecution('summary' in n ? n.summary : n),
    ),
    vscode.commands.registerCommand('weft.deleteExecution', (n: ExecutionNode | ExecutionSummary) =>
      deleteExecution('summary' in n ? n.summary : n),
    ),
    vscode.commands.registerCommand('weft.clearExecutions', () => clearAllExecutions()),

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
    vscode.commands.registerCommand('weft.openLoomView', () => {
      void vscode.window.showInformationMessage(
        'Runner view lands after graph view ships. Tracking in the roadmap.',
      );
    }),
    vscode.commands.registerCommand('weft.toggleTestMode', () => {
      void vscode.window.showInformationMessage('Test mode coming soon.');
    }),
  );

  // .weft files default to the graph view, not the text editor.
  // When a .weft becomes the active text editor AND the graph
  // panel doesn't exist yet (cold open via Ctrl+P, explorer
  // double-click, restored editor on startup), pin its project,
  // open the graph in the same column, and close the underlying
  // text tab. The user can summon the text via the graph's
  // "Open source" button when they want it.
  graphView.setOpenSourceHandler(async () => {
    if (!pinnedProject) return;
    const target = pinnedProject.entryPath;
    // Already open somewhere? Reveal the existing tab instead of
    // creating a new one. Otherwise repeated clicks pile up tabs.
    const existing = vscode.window.tabGroups.all
      .flatMap((g) => g.tabs.map((t) => ({ tab: t, group: g })))
      .find(
        (e) =>
          e.tab.input instanceof vscode.TabInputText
          && e.tab.input.uri.fsPath === target,
      );
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

  context.subscriptions.push(
    vscode.window.onDidChangeActiveTextEditor(async (ed) => {
      if (!ed || ed.document.languageId !== 'weft') return;
      const found = projectsProvider
        .projects()
        .find((p) => p.entryPath === ed.document.uri.fsPath);
      if (!found) return;
      pinnedProject = found;
      executionsProvider.setPinnedProject(found);
      autoFollow.setProject(found.id);

      // Discriminator: does the graph panel already exist?
      //
      // - Yes → user is refocusing or opened source via the
      //   "Source" button. Leave the text tab alone.
      // - No → cold open (Ctrl+P, explorer click, restored
      //   editor on startup). Swap the text for the graph in
      //   the same column.
      //
      // Counting tabs doesn't work because VS Code has already
      // created the text tab by the time this event fires, in
      // both cases.
      if (graphView.isOpen()) {
        return;
      }

      const docUri = ed.document.uri;
      await pinProject(found);
      const tabs = vscode.window.tabGroups.all
        .flatMap((g) => g.tabs)
        .filter(
          (t) =>
            t.input instanceof vscode.TabInputText
            && t.input.uri.toString() === docUri.toString(),
        );
      if (tabs.length > 0) {
        await vscode.window.tabGroups.close(tabs);
      }
    }),
  );

  attachDiagnostics(context, dispatcher);

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

async function readFile(path: string): Promise<string> {
  const uri = vscode.Uri.file(path);
  const bytes = await vscode.workspace.fs.readFile(uri);
  return new TextDecoder().decode(bytes);
}

// Suppress unused-symbol warning: InspectorSelection is part of the
// public surface that sidebar/inspector uses internally.
// eslint-disable-next-line @typescript-eslint/no-unused-vars
type _UnusedInspectorSelection = InspectorSelection;
