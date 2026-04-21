// Graph view (.weft) and Runner view (.loom). Each opens a webview
// panel that renders the currently-focused file's AST.
//
// Phase A2: the webview asks the dispatcher for a compiled version
// of the file (via `/describe/project` once project context is
// available) and renders a simple node list. Full xyflow-based
// rendering lands in Phase B.

import * as vscode from 'vscode';

export function openGraphView(_context: vscode.ExtensionContext) {
  const editor = vscode.window.activeTextEditor;
  const source = editor?.document.getText() ?? '';
  const fileName = editor?.document.fileName ?? '(unsaved)';

  const panel = vscode.window.createWebviewPanel(
    'weft.graph',
    'Weft Graph',
    vscode.ViewColumn.Beside,
    { enableScripts: true, retainContextWhenHidden: true },
  );
  panel.webview.html = renderGraphHtml(fileName, source);
}

export function openLoomView(_context: vscode.ExtensionContext) {
  const editor = vscode.window.activeTextEditor;
  const source = editor?.document.getText() ?? '';
  const fileName = editor?.document.fileName ?? '(unsaved)';

  const panel = vscode.window.createWebviewPanel(
    'weft.loom',
    'Weft Runner',
    vscode.ViewColumn.Beside,
    { enableScripts: true, retainContextWhenHidden: true },
  );
  panel.webview.html = renderLoomHtml(fileName, source);
}

function renderGraphHtml(fileName: string, source: string): string {
  return `<!DOCTYPE html>
<html>
<head>
  <meta charset="UTF-8" />
  <title>Weft Graph</title>
  <style>
    body { font-family: var(--vscode-font-family); padding: 12px; }
    .file { color: var(--vscode-descriptionForeground); margin-bottom: 8px; }
    pre { background: var(--vscode-textBlockQuote-background); padding: 12px; overflow: auto; }
    .todo { color: var(--vscode-gitDecoration-modifiedResourceForeground); margin-top: 16px; }
  </style>
</head>
<body>
  <div class="file">${escape(fileName)}</div>
  <h3>Source</h3>
  <pre>${escape(source)}</pre>
  <div class="todo">
    Full xyflow-based graph rendering is a Phase B target. For now
    the preview shows the raw weft source.
  </div>
</body>
</html>`;
}

function renderLoomHtml(fileName: string, source: string): string {
  return `<!DOCTYPE html>
<html>
<head>
  <meta charset="UTF-8" />
  <title>Weft Runner</title>
  <style>
    body { font-family: var(--vscode-font-family); padding: 12px; }
    .file { color: var(--vscode-descriptionForeground); margin-bottom: 8px; }
    pre { background: var(--vscode-textBlockQuote-background); padding: 12px; overflow: auto; }
    .todo { color: var(--vscode-gitDecoration-modifiedResourceForeground); margin-top: 16px; }
  </style>
</head>
<body>
  <div class="file">${escape(fileName)}</div>
  <h3>Runner definition</h3>
  <pre>${escape(source)}</pre>
  <div class="todo">
    Runner UI preview (form-builder + action wiring) lands in Phase
    B. For now the preview shows the raw loom source.
  </div>
</body>
</html>`;
}

function escape(s: string): string {
  return s
    .replace(/&/g, '&amp;')
    .replace(/</g, '&lt;')
    .replace(/>/g, '&gt;')
    .replace(/"/g, '&quot;')
    .replace(/'/g, '&#39;');
}
