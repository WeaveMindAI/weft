// Inspector webview: minimal HTML view that shows the selected
// node's config + last observed input/output. Designed as a
// dumb renderer: the graph view posts updates (node clicked,
// liveData pulse, selection cleared) and we redraw.
//
// Keeping this as a separate thin webview (not the full graph
// bundle) avoids paying the bundle cost twice. If it grows beyond
// JSON-tree rendering we can switch to a Svelte child mount later.

import * as vscode from 'vscode';

export interface InspectorSelection {
  projectId: string;
  nodeId: string;
  nodeType: string;
  label?: string;
  config?: Record<string, unknown>;
  inputs?: Array<{ name: string; type: string }>;
  outputs?: Array<{ name: string; type: string }>;
  // Populated by the most recent node exec event.
  lastInputs?: Record<string, unknown>;
  lastOutputs?: Record<string, unknown>;
  lastStatus?: string;
}

export class InspectorView implements vscode.WebviewViewProvider {
  private view?: vscode.WebviewView;
  private selection?: InspectorSelection;

  constructor(private readonly extensionUri: vscode.Uri) {}

  resolveWebviewView(view: vscode.WebviewView): void {
    this.view = view;
    view.webview.options = {
      enableScripts: true,
      localResourceRoots: [vscode.Uri.joinPath(this.extensionUri, 'media')],
    };
    view.webview.html = this.renderHtml();
    if (this.selection) this.post();
  }

  setSelection(sel: InspectorSelection | undefined): void {
    this.selection = sel;
    this.post();
  }

  updateLive(nodeId: string, patch: Partial<InspectorSelection>): void {
    if (!this.selection || this.selection.nodeId !== nodeId) return;
    this.selection = { ...this.selection, ...patch };
    this.post();
  }

  private post(): void {
    this.view?.webview.postMessage({ kind: 'selection', selection: this.selection });
  }

  private renderHtml(): string {
    const nonce = Math.random().toString(36).slice(2);
    const csp = `default-src 'none'; style-src 'unsafe-inline'; script-src 'nonce-${nonce}';`;
    return /* html */ `<!DOCTYPE html>
<html>
<head>
<meta charset="utf-8" />
<meta http-equiv="Content-Security-Policy" content="${csp}" />
<style>
  html, body { margin: 0; padding: 0; color: var(--vscode-foreground); background: var(--vscode-sideBar-background); font-family: var(--vscode-font-family); font-size: var(--vscode-font-size); }
  .empty { padding: 16px; opacity: 0.6; font-style: italic; }
  h2 { margin: 12px 12px 4px; font-size: 11px; text-transform: uppercase; letter-spacing: 0.05em; opacity: 0.7; font-weight: 600; }
  .header { padding: 12px 12px 8px; border-bottom: 1px solid var(--vscode-panel-border); }
  .header .type { font-size: 11px; opacity: 0.6; text-transform: uppercase; letter-spacing: 0.04em; }
  .header .label { font-size: 14px; font-weight: 600; margin-top: 2px; }
  .kv { padding: 0 12px; }
  .kv .row { display: grid; grid-template-columns: 100px 1fr; gap: 8px; padding: 4px 0; border-bottom: 1px solid var(--vscode-panel-border); }
  .kv .row:last-child { border-bottom: none; }
  .kv .k { opacity: 0.7; font-size: 12px; }
  .kv .v { font-family: var(--vscode-editor-font-family); font-size: 12px; white-space: pre-wrap; word-break: break-word; }
  .status { display: inline-block; padding: 2px 6px; border-radius: 3px; font-size: 10px; text-transform: uppercase; letter-spacing: 0.04em; }
  .status.running { background: var(--vscode-charts-blue); color: white; }
  .status.completed { background: var(--vscode-charts-green); color: white; }
  .status.failed { background: var(--vscode-errorForeground); color: white; }
  pre { background: var(--vscode-textCodeBlock-background); padding: 6px 8px; border-radius: 3px; font-size: 11px; margin: 4px 12px 12px; overflow: auto; }
</style>
</head>
<body>
<div id="root"><div class="empty">Select a node to inspect.</div></div>
<script nonce="${nonce}">
const root = document.getElementById('root');
function render(sel) {
  if (!sel) { root.innerHTML = '<div class="empty">Select a node to inspect.</div>'; return; }
  const esc = (s) => String(s).replace(/[&<>"']/g, c => ({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[c]));
  const statusBadge = sel.lastStatus ? '<span class="status ' + sel.lastStatus.toLowerCase() + '">' + esc(sel.lastStatus) + '</span>' : '';
  const configRows = Object.entries(sel.config ?? {}).map(([k, v]) =>
    '<div class="row"><div class="k">' + esc(k) + '</div><div class="v">' + esc(JSON.stringify(v)) + '</div></div>'
  ).join('');
  const liveSection = (title, items) => {
    if (!items || Object.keys(items).length === 0) return '';
    const rows = Object.entries(items).map(([k, v]) =>
      '<div class="row"><div class="k">' + esc(k) + '</div><div class="v">' + esc(JSON.stringify(v, null, 2)) + '</div></div>'
    ).join('');
    return '<h2>' + title + '</h2><div class="kv">' + rows + '</div>';
  };
  root.innerHTML =
    '<div class="header">' +
      '<div class="type">' + esc(sel.nodeType) + ' ' + statusBadge + '</div>' +
      '<div class="label">' + esc(sel.label || sel.nodeId) + '</div>' +
    '</div>' +
    (configRows ? '<h2>Config</h2><div class="kv">' + configRows + '</div>' : '') +
    liveSection('Last inputs', sel.lastInputs) +
    liveSection('Last outputs', sel.lastOutputs);
}
window.addEventListener('message', (ev) => {
  if (ev.data && ev.data.kind === 'selection') render(ev.data.selection);
});
</script>
</body>
</html>`;
  }
}
