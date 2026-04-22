// Extension-host side of the graph view. Owns a single WebviewPanel
// that tracks the currently-active .weft document, parses via the
// dispatcher on every text change (debounced), and relays graph
// mutations back as surgical TextEditor.edit() calls.

import * as vscode from 'vscode';
import type { DispatcherClient } from './dispatcher';
import type { GraphMutation, HostMessage, ProjectDefinition, WebviewMessage } from './shared/protocol';
import { buildEdit } from './surgical';

export class GraphViewController {
  private panel: vscode.WebviewPanel | undefined;
  private watchedDoc: vscode.TextDocument | undefined;
  private parseTimer: NodeJS.Timeout | undefined;
  private disposables: vscode.Disposable[] = [];
  private lastProject: ProjectDefinition | undefined;
  private follower: EventSource | undefined;

  constructor(
    private readonly context: vscode.ExtensionContext,
    private readonly client: DispatcherClient,
  ) {}

  /** Subscribe to SSE for a live execution (or project) and forward
   *  NodeStarted/NodeCompleted/NodeFailed/NodeSkipped into the panel
   *  as `execEvent` messages. Called by the Weft: Follow Execution
   *  command.
   */
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
      .get<any[]>(`/executions/${color}/replay`)
      .catch(() => []);
    for (const e of events) {
      this.post({ kind: 'execEvent', event: e });
      await new Promise((r) => setTimeout(r, 120));
    }
  }

  stopFollowing(): void {
    if (this.follower) {
      this.follower.close();
      this.follower = undefined;
    }
  }

  /** Approximate active-edge SSE: when a node starts we flash all
   *  incoming edges for ACTIVE_EDGE_WINDOW_MS; when it completes we
   *  flash all outgoing edges. Exact pulse tracking is a dispatcher
   *  responsibility for later (parity/execution.md).
   */
  private approximateActiveEdges(nodeId: string, kind: 'started' | 'completed' | 'failed' | 'skipped'): void {
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
    void this.sendLayoutHint();

    this.disposables.push(
      this.panel.webview.onDidReceiveMessage((msg) => this.onMessage(msg)),
      this.panel.onDidDispose(() => this.onDispose()),
      vscode.workspace.onDidChangeTextDocument((e) => {
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
    try {
      const response = await this.client.parse(source);
      this.lastProject = response.project;
      this.post({ kind: 'parseResult', response });
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
      case 'positionsChanged':
        void this.saveLayout(msg.positions);
        break;
      case 'layoutChanged':
        void this.saveFullLayout(msg.layout);
        break;
      case 'mutation':
        void this.applyMutation(msg.mutation);
        break;
      case 'log':
        console[msg.level]('[weft/webview]', msg.message);
        break;
    }
  }

  private async applyMutation(mutation: GraphMutation): Promise<void> {
    if (!this.watchedDoc || !this.lastProject) return;
    // Reparse the current document first so spans are fresh (v1's
    // Option A, per our design discussion). This costs ~1ms on
    // localhost.
    try {
      const fresh = await this.client.parse(this.watchedDoc.getText());
      this.lastProject = fresh.project;
    } catch {
      // Continue with stale project; surgical.ts will fail-safe if
      // spans no longer resolve.
    }
    const edit = buildEdit(mutation, { project: this.lastProject, doc: this.watchedDoc });
    if (!edit) {
      void vscode.window.showWarningMessage(
        `Weft: could not apply ${mutation.kind} mutation (missing span info).`,
      );
      return;
    }
    await vscode.workspace.applyEdit(edit);
  }

  private layoutUriFor(doc: vscode.TextDocument): vscode.Uri {
    return vscode.Uri.parse(doc.uri.toString() + '.layout.json');
  }

  private async sendSettings(): Promise<void> {
    const cfg = vscode.workspace.getConfiguration('weft');
    this.post({
      kind: 'settings',
      parseDebounceMs: cfg.get<number>('parse.debounceMs', 100),
      layoutDebounceMs: cfg.get<number>('layout.debounceMs', 400),
    });
  }

  private async sendLayoutHint(): Promise<void> {
    if (!this.watchedDoc) return;
    try {
      const data = await vscode.workspace.fs.readFile(this.layoutUriFor(this.watchedDoc));
      const text = new TextDecoder().decode(data);
      const positions = JSON.parse(text) as Record<string, { x: number; y: number }>;
      this.post({ kind: 'layoutHint', positions });
    } catch {
      // No layout file yet. Webview falls back to ELK auto-layout.
    }
  }

  private async saveLayout(positions: Record<string, { x: number; y: number }>): Promise<void> {
    if (!this.watchedDoc) return;
    const uri = this.layoutUriFor(this.watchedDoc);
    const body = JSON.stringify(positions, null, 2);
    await vscode.workspace.fs.writeFile(uri, new TextEncoder().encode(body));
  }

  private async saveFullLayout(
    layout: Record<
      string,
      { x: number; y: number; w?: number; h?: number; expanded?: boolean }
    >,
  ): Promise<void> {
    if (!this.watchedDoc) return;
    const uri = this.layoutUriFor(this.watchedDoc);
    const body = JSON.stringify(layout, null, 2);
    await vscode.workspace.fs.writeFile(uri, new TextEncoder().encode(body));
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
