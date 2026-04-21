// Graph view (.weft) and Runner view (.loom). Each opens in its own
// editor tab, reads the currently focused file, renders via a webview.
// Phase A2 ports the actual rendering (xyflow for graph, TBD for
// loom runner preview).

import * as vscode from 'vscode';

export function openGraphView(context: vscode.ExtensionContext) {
  const panel = vscode.window.createWebviewPanel(
    'weft.graph',
    'Weft Graph',
    vscode.ViewColumn.Beside,
    { enableScripts: true },
  );
  panel.webview.html = placeholder('Graph view placeholder. Phase A2 implements xyflow-based rendering.');
}

export function openLoomView(_context: vscode.ExtensionContext) {
  const panel = vscode.window.createWebviewPanel(
    'weft.loom',
    'Weft Runner',
    vscode.ViewColumn.Beside,
    { enableScripts: true },
  );
  panel.webview.html = placeholder('Runner (Loom) view placeholder. Phase A2 implements the runner preview.');
}

function placeholder(message: string): string {
  return `<!DOCTYPE html>
<html><body style="font-family: var(--vscode-font-family); padding: 12px;">
<p style="color: var(--vscode-descriptionForeground);">${message}</p>
</body></html>`;
}
