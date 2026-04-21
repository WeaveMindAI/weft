// Weft VS Code extension entrypoint.
//
// Three surfaces:
// - Tangle panel: chat with the AI builder. Lives in the left activity
//   bar (webview). Phase A2 wires this to Tangle's streaming endpoint.
// - Graph view: when a user opens a .weft file, offer a companion tab
//   rendering the graph. Phase A2 implements the rendering.
// - Runner view: when a user opens a .loom file, offer a companion tab
//   rendering the runner UI preview. Phase A2 implements rendering.
// - Projects tree view: shows projects registered with the dispatcher,
//   their status, and active executions. Driven by the dispatcher's
//   SSE stream. Phase A2 implements the live updates.

import * as vscode from 'vscode';
import { DispatcherClient } from './dispatcher';
import { TangleViewProvider } from './tangle';
import { ProjectsTreeProvider } from './projects';
import { openGraphView, openLoomView } from './views';

export function activate(context: vscode.ExtensionContext) {
  const dispatcher = new DispatcherClient(getDispatcherUrl());

  // Left activity-bar panels
  context.subscriptions.push(
    vscode.window.registerWebviewViewProvider(
      'weft.tangle',
      new TangleViewProvider(context, dispatcher),
    ),
    vscode.window.registerTreeDataProvider(
      'weft.projects',
      new ProjectsTreeProvider(dispatcher),
    ),
  );

  // Commands
  context.subscriptions.push(
    vscode.commands.registerCommand('weft.openGraphView', () => openGraphView(context)),
    vscode.commands.registerCommand('weft.openLoomView', () => openLoomView(context)),
    vscode.commands.registerCommand('weft.runProject', async () => {
      await dispatcher.runCurrentProject();
    }),
  );

  // React to config changes.
  context.subscriptions.push(
    vscode.workspace.onDidChangeConfiguration((e) => {
      if (e.affectsConfiguration('weft.dispatcherUrl')) {
        dispatcher.setBaseUrl(getDispatcherUrl());
      }
    }),
  );
}

export function deactivate() {}

function getDispatcherUrl(): string {
  return vscode.workspace.getConfiguration('weft').get<string>('dispatcherUrl') ?? 'http://localhost:9999';
}
