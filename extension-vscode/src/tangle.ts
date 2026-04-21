// Tangle webview provider. Minimal chat UI scaffold. Phase B wires
// real model calls + streaming deltas + context built from the
// project's describe output.

import * as vscode from 'vscode';
import { DispatcherClient } from './dispatcher';

export class TangleViewProvider implements vscode.WebviewViewProvider {
  constructor(
    private readonly context: vscode.ExtensionContext,
    private readonly dispatcher: DispatcherClient,
  ) {}

  resolveWebviewView(webviewView: vscode.WebviewView) {
    webviewView.webview.options = { enableScripts: true };
    webviewView.webview.html = this.renderHtml();

    webviewView.webview.onDidReceiveMessage(async (msg) => {
      if (msg?.kind === 'check') {
        try {
          await this.dispatcher.get<unknown>('/projects');
          webviewView.webview.postMessage({ kind: 'status', ok: true });
        } catch {
          webviewView.webview.postMessage({ kind: 'status', ok: false });
        }
      }
    });
  }

  private renderHtml(): string {
    return `<!DOCTYPE html>
<html>
<head>
  <meta charset="UTF-8" />
  <title>Tangle</title>
  <style>
    body { font-family: var(--vscode-font-family); padding: 12px; display: flex; flex-direction: column; gap: 12px; }
    .status { font-size: 0.85em; color: var(--vscode-descriptionForeground); }
    .notice { padding: 12px; background: var(--vscode-textBlockQuote-background); border-left: 2px solid var(--vscode-textLink-foreground); }
    button { padding: 4px 8px; }
  </style>
</head>
<body>
  <div class="notice">
    Tangle chat wires the weft AI builder. Phase B connects streaming
    model output + per-project catalog context.
  </div>
  <button id="check">Check dispatcher connection</button>
  <div class="status" id="status"></div>

  <script>
    const vscode = acquireVsCodeApi();
    document.getElementById('check').addEventListener('click', () => {
      vscode.postMessage({ kind: 'check' });
    });
    window.addEventListener('message', (ev) => {
      if (ev.data?.kind === 'status') {
        const el = document.getElementById('status');
        el.textContent = ev.data.ok ? 'dispatcher reachable' : 'dispatcher unreachable';
      }
    });
  </script>
</body>
</html>`;
  }
}
