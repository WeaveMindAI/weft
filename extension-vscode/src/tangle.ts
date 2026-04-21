// Tangle webview provider. Phase A1 renders a placeholder; phase A2
// wires the AI chat to our Tangle backend (streaming deltas, context
// built from the dispatcher's catalog describe endpoint for the open
// project).

import * as vscode from 'vscode';
import { DispatcherClient } from './dispatcher';

export class TangleViewProvider implements vscode.WebviewViewProvider {
  constructor(
    private readonly context: vscode.ExtensionContext,
    private readonly dispatcher: DispatcherClient,
  ) {}

  resolveWebviewView(webviewView: vscode.WebviewView) {
    webviewView.webview.options = { enableScripts: true };
    webviewView.webview.html = this.renderInitialHtml();
  }

  private renderInitialHtml(): string {
    return `<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8" />
  <title>Tangle</title>
  <style>
    body { font-family: var(--vscode-font-family); padding: 12px; }
    .placeholder { color: var(--vscode-descriptionForeground); }
  </style>
</head>
<body>
  <p class="placeholder">
    Tangle AI builder UI lives here. Phase A2 wires the streaming chat.
  </p>
</body>
</html>`;
  }
}
