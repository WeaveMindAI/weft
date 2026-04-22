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

import { DispatcherClient } from './dispatcher';
import { GraphViewController } from './graphView';
import { attachDiagnostics } from './diagnostics';
import { registerStreamingEditApi } from './streamingEdits';

import { ProjectsProvider, ProjectNode, type WeftProject } from './sidebar/projects';
import { ExecutionsProvider, ExecutionNode, type ExecutionSummary } from './sidebar/executions';
import { InspectorView, type InspectorSelection } from './sidebar/inspector';
import { ExecutionFollower } from './execFollower';

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

  graphView.setRunHandler(() => runPinned());
  graphView.setStopHandler(() => stopPinned());
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
    const doc = await vscode.workspace.openTextDocument(project.entryPath);
    await vscode.window.showTextDocument(doc, { preview: false });
    await graphView.open(doc, project.id);
  }

  async function runPinned(): Promise<void> {
    if (!pinnedProject) {
      void vscode.window.showInformationMessage('Pin a Weft project first.');
      return;
    }
    try {
      // Register the project with the dispatcher (idempotent) then
      // trigger a run. The dispatcher mints a fresh execution color
      // and broadcasts a NodeStarted event that our follower picks
      // up and forwards to the webview.
      const source = await readFile(pinnedProject.entryPath);
      await dispatcher.post(`/projects`, {
        id: pinnedProject.id,
        name: pinnedProject.label,
        source,
        root: pinnedProject.rootPath,
      });
      const resp = await dispatcher.post<{ color: string }>(`/projects/${pinnedProject.id}/run`, {});
      pinnedExecution = resp.color;
      follower.follow(resp.color);
      await executionsProvider.refresh();
    } catch (err) {
      void vscode.window.showErrorMessage(`Run failed: ${err}`);
    }
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
    // switch the graph to it, then replay journaled events.
    const match = projectsProvider.projects().find((p) => p.id === summary.project_id);
    if (match && pinnedProject?.id !== match.id) {
      await pinProject(match);
    }
    pinnedExecution = summary.color;
    await follower.replay(summary.color);
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
    vscode.window.registerWebviewViewProvider('weft.inspector', inspector),

    vscode.commands.registerCommand('weft.refreshProjects', () => projectsProvider.refresh()),
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

  // Keep the inspector + executions list fresh when the active
  // editor changes (new .weft file -> new project pin).
  context.subscriptions.push(
    vscode.window.onDidChangeActiveTextEditor(async (ed) => {
      if (!ed || ed.document.languageId !== 'weft') return;
      const found = projectsProvider
        .projects()
        .find((p) => p.entryPath === ed.document.uri.fsPath);
      if (found) {
        pinnedProject = found;
        executionsProvider.setPinnedProject(found);
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
