// Weft VS Code extension entrypoint.
//
// Scope: ops + graph rendering. This extension:
// - shows projects registered with the local dispatcher
// - lets the user run them
// - renders a live xyflow graph of the active .weft file
// - surfaces dispatcher /validate diagnostics in the Problems panel
// - exposes a streaming-edit primitive for AI extensions to drive
//   SEARCH/REPLACE blocks into the active file as they stream

import * as vscode from 'vscode';
import { DispatcherClient } from './dispatcher';
import { ProjectsTreeProvider } from './projects';
import { GraphViewController } from './graphView';
import { attachDiagnostics } from './diagnostics';
import { registerStreamingEditApi } from './streamingEdits';

export function activate(context: vscode.ExtensionContext) {
  const dispatcher = new DispatcherClient(getDispatcherUrl());

  context.subscriptions.push(
    vscode.window.registerTreeDataProvider(
      'weft.projects',
      new ProjectsTreeProvider(dispatcher),
    ),
  );

  const graphView = new GraphViewController(context, dispatcher);

  context.subscriptions.push(
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
    vscode.commands.registerCommand('weft.runProject', async () => {
      void vscode.window.showInformationMessage(
        'weft.runProject wiring pending; for now, use the CLI: `weft run`.',
      );
    }),
  );

  attachDiagnostics(context, dispatcher);

  context.subscriptions.push(registerStreamingEditApi());

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
