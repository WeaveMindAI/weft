// Extension-host side of the graph view. Owns a single WebviewPanel
// that tracks the currently-active .weft document, parses via the
// dispatcher on every text change (debounced), and streams saves
// back into the document / .layout.json sidecar.
//
// The webview does all the text surgery in-process via v1's
// weft-editor.ts. When the user edits something, the webview sends
// the entire new weft source via `saveWeft`; the host applies a
// full-range TextEdit and the resulting onDidChangeTextDocument
// kicks off the next parse.

import * as vscode from 'vscode';
import type { DispatcherClient } from './dispatcher';
import type { HostMessage, ProjectDefinition, WebviewMessage } from './shared/protocol';

export class GraphViewController {
  private panel: vscode.WebviewPanel | undefined;
  private watchedDoc: vscode.TextDocument | undefined;
  private parseTimer: NodeJS.Timeout | undefined;
  private disposables: vscode.Disposable[] = [];
  private lastProject: ProjectDefinition | undefined;
  private follower: EventSource | undefined;
  // Set while we're applying our own TextEdit to the document.
  // onDidChangeTextDocument fires during the edit; if we parsed
  // twice (once for the webview save, once for the VS Code change)
  // we'd loop.
  private suppressReparse = false;

  constructor(
    private readonly context: vscode.ExtensionContext,
    private readonly client: DispatcherClient,
  ) {}

  /** Subscribe to SSE for a live execution (or project) and forward
   *  NodeStarted/NodeCompleted/NodeFailed/NodeSkipped into the panel
   *  as `execEvent` messages. Called by Weft: Follow Execution. */
  followColor(color: string): void {
    this.stopFollowing();
    this.post({ kind: 'execReset' });
    const es = this.client.subscribe(`/events/execution/${color}`, (ev) => {
      try {
        const payload = JSON.parse(ev.data);
        if (
          payload.kind === 'node_started' ||
          payload.kind === 'node_completed' ||
          payload.kind === 'node_failed' ||
          payload.kind === 'node_skipped'
        ) {
          const k = payload.kind.replace('node_', '') as
            | 'started'
            | 'completed'
            | 'failed'
            | 'skipped';
          this.post({
            kind: 'execEvent',
            event: {
              id: payload.id ?? `${payload.node ?? payload.node_id}-${Date.now()}`,
              color,
              node_id: payload.node ?? payload.node_id,
              lane: payload.lane ?? '',
              kind: k,
              input: payload.input,
              output: payload.output,
              error: payload.error,
              at_unix: Math.floor(Date.now() / 1000),
              completed_at_unix: payload.completed_at_unix,
              cost_usd: payload.cost_usd,
              pulse_id: payload.pulse_id,
              pulse_ids_absorbed: payload.pulse_ids_absorbed,
            },
          });
          this.approximateActiveEdges(payload.node ?? payload.node_id, k);
        }
      } catch {
        // malformed event, ignore
      }
    });
    this.follower = es;
  }

  /** Replay a past execution's node events with small delays so the
   *  user watches the graph animate. */
  async replayColor(color: string): Promise<void> {
    this.stopFollowing();
    this.post({ kind: 'execReset' });
    const events = await this.client
      .get<unknown[]>(`/executions/${color}/replay`)
      .catch(() => []);
    for (const e of events as HostMessage[]) {
      this.post(e);
      await new Promise((r) => setTimeout(r, 120));
    }
  }

  stopFollowing(): void {
    if (this.follower) {
      this.follower.close();
      this.follower = undefined;
    }
  }

  private approximateActiveEdges(
    nodeId: string,
    kind: 'started' | 'completed' | 'failed' | 'skipped',
  ): void {
    if (!this.lastProject) return;
    const relevant: string[] = [];
    if (kind === 'started') {
      for (const e of this.lastProject.edges) {
        if (e.target === nodeId) relevant.push(e.id);
      }
    } else if (kind === 'completed') {
      for (const e of this.lastProject.edges) {
        if (e.source === nodeId) relevant.push(e.id);
      }
    }
    for (const edgeId of relevant) {
      this.post({ kind: 'edgeActive', event: { edgeId, active: true } });
      setTimeout(() => {
        this.post({ kind: 'edgeActive', event: { edgeId, active: false } });
      }, 200);
    }
  }

  async open(doc: vscode.TextDocument): Promise<void> {
    if (this.panel) {
      this.panel.reveal(vscode.ViewColumn.Beside);
      this.watchedDoc = doc;
      await this.triggerParse();
      return;
    }

    this.panel = vscode.window.createWebviewPanel(
      'weft.graph',
      `Weft Graph: ${doc.fileName.split(/[\\/]/).pop() ?? ''}`,
      vscode.ViewColumn.Beside,
      {
        enableScripts: true,
        retainContextWhenHidden: true,
        localResourceRoots: [
          vscode.Uri.joinPath(this.context.extensionUri, 'media'),
        ],
      },
    );

    this.panel.webview.html = this.renderHtml();
    this.watchedDoc = doc;

    void this.sendSettings();
    void this.sendGlobalCatalog();

    this.disposables.push(
      this.panel.webview.onDidReceiveMessage((msg) => this.onMessage(msg)),
      this.panel.onDidDispose(() => this.onDispose()),
      vscode.workspace.onDidChangeTextDocument((e) => {
        if (this.suppressReparse) return;
        if (this.watchedDoc && e.document === this.watchedDoc) {
          this.scheduleParse();
        }
      }),
      vscode.window.onDidChangeActiveTextEditor((ed) => {
        if (ed && ed.document.languageId === 'weft') {
          this.watchedDoc = ed.document;
          void this.triggerParse();
        }
      }),
    );
  }

  private scheduleParse(): void {
    const debounce = vscode.workspace
      .getConfiguration('weft.parse')
      .get<number>('debounceMs', 100);
    if (this.parseTimer) clearTimeout(this.parseTimer);
    this.parseTimer = setTimeout(() => void this.triggerParse(), debounce);
  }

  private async triggerParse(): Promise<void> {
    if (!this.panel || !this.watchedDoc) return;
    const source = this.watchedDoc.getText();
    const layoutCode = await this.readLayoutCode(this.watchedDoc);
    try {
      const response = await this.client.parse(source);
      this.lastProject = response.project;
      this.post({ kind: 'parseResult', response, source, layoutCode });
    } catch (err) {
      this.post({
        kind: 'parseError',
        error: err instanceof Error ? err.message : String(err),
      });
    }
  }

  private post(msg: HostMessage): void {
    this.panel?.webview.postMessage(msg);
  }

  private onMessage(msg: WebviewMessage): void {
    switch (msg.kind) {
      case 'ready':
        void this.triggerParse();
        break;
      case 'saveWeft':
        void this.saveWeft(msg.source);
        break;
      case 'saveLayout':
        void this.saveLayoutCode(msg.layoutCode);
        break;
      case 'log':
        console[msg.level]('[weft/webview]', msg.message);
        break;
    }
  }

  /** Replace the watched document's text with the webview's copy.
   *  Simple full-range replace: VS Code diff-compresses this into a
   *  proper TextEdit, preserves the user's cursor unless it was
   *  inside a changed region. Suppress re-entry so we don't reparse
   *  on our own edit. */
  private async saveWeft(source: string): Promise<void> {
    if (!this.watchedDoc) return;
    if (this.watchedDoc.getText() === source) return;
    const edit = new vscode.WorkspaceEdit();
    const last = this.watchedDoc.lineCount - 1;
    const end = this.watchedDoc.lineAt(last).range.end;
    edit.replace(this.watchedDoc.uri, new vscode.Range(0, 0, end.line, end.character), source);
    this.suppressReparse = true;
    try {
      await vscode.workspace.applyEdit(edit);
      // Parse on OUR schedule after the edit lands.
      void this.triggerParse();
    } finally {
      this.suppressReparse = false;
    }
  }

  private layoutUriFor(doc: vscode.TextDocument): vscode.Uri {
    return vscode.Uri.parse(doc.uri.toString() + '.layout.json');
  }

  private async readLayoutCode(doc: vscode.TextDocument): Promise<string> {
    try {
      const data = await vscode.workspace.fs.readFile(this.layoutUriFor(doc));
      return new TextDecoder().decode(data);
    } catch {
      return '';
    }
  }

  private async saveLayoutCode(layoutCode: string): Promise<void> {
    if (!this.watchedDoc) return;
    const uri = this.layoutUriFor(this.watchedDoc);
    await vscode.workspace.fs.writeFile(uri, new TextEncoder().encode(layoutCode));
  }

  /** Fetch every node type available in the current project scope
   *  (stdlib + project-local `nodes/`) and ship the catalog to the
   *  webview so the command palette can list them all, even types
   *  the current `main.weft` doesn't reference yet. */

  private async sendGlobalCatalog(): Promise<void> {
    if (!this.watchedDoc) return;
    const docPath = this.watchedDoc.uri.fsPath;
    const lastSep = Math.max(docPath.lastIndexOf('/'), docPath.lastIndexOf('\\'));
    const projectRoot = lastSep > 0 ? docPath.slice(0, lastSep) : undefined;
    const qs = projectRoot ? `?project_root=${encodeURIComponent(projectRoot)}` : '';
    try {
      const response = await this.client.get<{
        catalog: Record<string, unknown>;
        warnings?: string[];
      }>(`/describe/nodes${qs}`);
      this.post({
        kind: 'catalogAll',
        catalog: response.catalog as Record<string, import('./shared/protocol').CatalogEntry>,
      });
    } catch (err) {
      console.warn('[weft/graphView] /describe/nodes failed', err);
    }
  }

  private async sendSettings(): Promise<void> {
    const cfg = vscode.workspace.getConfiguration('weft');
    this.post({
      kind: 'settings',
      parseDebounceMs: cfg.get<number>('parse.debounceMs', 100),
      layoutDebounceMs: cfg.get<number>('layout.debounceMs', 400),
    });
  }

  private onDispose(): void {
    if (this.parseTimer) clearTimeout(this.parseTimer);
    for (const d of this.disposables) d.dispose();
    this.disposables = [];
    this.panel = undefined;
    this.watchedDoc = undefined;
  }

  private renderHtml(): string {
    const panel = this.panel!;
    const bundleJs = panel.webview.asWebviewUri(
      vscode.Uri.joinPath(this.context.extensionUri, 'media', 'webview', 'bundle.js'),
    );
    const bundleCss = panel.webview.asWebviewUri(
      vscode.Uri.joinPath(this.context.extensionUri, 'media', 'webview', 'bundle.css'),
    );
    const cspSource = panel.webview.cspSource;
    const nonce = randomNonce();
    return `<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta http-equiv="Content-Security-Policy" content="default-src 'none'; style-src ${cspSource} 'unsafe-inline'; script-src 'nonce-${nonce}' ${cspSource}; img-src ${cspSource} data:; font-src ${cspSource}; connect-src ${cspSource};">
<link rel="stylesheet" href="${bundleCss}">
<title>Weft Graph</title>
<style>html,body,#app{margin:0;padding:0;width:100%;height:100%;overflow:hidden}</style>
</head>
<body>
<div id="app"></div>
<script nonce="${nonce}" src="${bundleJs}"></script>
</body>
</html>`;
  }
}

function randomNonce(): string {
  const chars = 'ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789';
  let out = '';
  for (let i = 0; i < 24; i++) out += chars[Math.floor(Math.random() * chars.length)];
  return out;
}
