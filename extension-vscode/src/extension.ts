// Weft VS Code extension entrypoint.
//
// Surfaces:
// - Tangle chat participant: invoked as `@weft` from the native
//   VS Code chat panel. Routes to the dispatcher's AI endpoint
//   (Phase B) with fallback to VS Code's default language model.
// - Projects tree view: left activity-bar sidebar showing projects
//   registered with the dispatcher. Polls every 5s.
// - Graph view: editor tab showing a .weft file's graph preview.
// - Runner view: editor tab showing a .loom file's runner preview.

import * as vscode from 'vscode';
import { DispatcherClient } from './dispatcher';
import { registerTangleParticipant } from './tangle';
import { ProjectsTreeProvider } from './projects';
import { openGraphView, openLoomView } from './views';

export function activate(context: vscode.ExtensionContext) {
  const dispatcher = new DispatcherClient(getDispatcherUrl());

  // Chat participant (native VS Code chat panel).
  registerTangleParticipant(context, dispatcher);

  // Projects sidebar.
  context.subscriptions.push(
    vscode.window.registerTreeDataProvider(
      'weft.projects',
      new ProjectsTreeProvider(dispatcher),
    ),
  );

  // Commands.
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
