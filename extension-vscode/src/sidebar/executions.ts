// Executions sidebar: flat list of every execution the dispatcher
// knows about, newest first. Each execution exposes its id, status,
// timing, and containing project; the view-in-graph and delete
// actions hang off the individual tree items. Clearing all is a
// tree-level action.
//
// We keep the tree flat (rather than project → execution children)
// because the common case is "I just ran something, show me the
// latest run regardless of which project it was on." A header node
// up top shows which project the current graph is pinned to.

import * as vscode from 'vscode';

import type { DispatcherClient, SseSubscription } from '../dispatcher';
import type { WeftProject } from './projects';

// SYNC: ExecutionSummary <-> crates/weft-dispatcher/src/journal/mod.rs (ExecutionSummary), weavemind/website/src/routes/(app)/executions/+page.ts (Execution)
export interface ExecutionSummary {
  color: string;
  project_id: string;
  entry_node: string;
  status: string;
  started_at: number;
  completed_at?: number | null;
}

export class ExecutionsProvider implements vscode.TreeDataProvider<vscode.TreeItem> {
  private _onDidChange = new vscode.EventEmitter<void>();
  readonly onDidChangeTreeData = this._onDidChange.event;

  static readonly PAGE_SIZE = 50;
  private cache: ExecutionSummary[] = [];
  private total = 0;
  // How many rows the tree currently requests. A "Load more" node bumps this by
  // a page; a refresh keeps the expanded count so live updates do not collapse
  // the list the user grew.
  private loaded = ExecutionsProvider.PAGE_SIZE;
  private pinnedProject: WeftProject | undefined;
  private eventSubscription: SseSubscription | undefined;
  // Tight refresh burst after an event: avoids piling a second
  // refresh when multiple events land within milliseconds of each
  // other (ExecutionStarted + NodeStarted arrive back-to-back).
  private refreshDebounceTimer: NodeJS.Timeout | undefined;

  constructor(private readonly client: DispatcherClient) {}

  setPinnedProject(project: WeftProject | undefined): void {
    if (this.pinnedProject?.id === project?.id) return;
    this.pinnedProject = project;
    this.resubscribe();
    // Always refresh on pin changes so the tree shows the new
    // project's runs immediately instead of waiting for the first
    // event to arrive (which might never if the project is idle).
    void this.refresh();
  }

  /** Drop any prior SSE connection and, if a project is pinned,
   *  open a fresh one against `/events/project/{id}`. Every event
   *  (ExecutionStarted / ExecutionCompleted / NodeStarted / etc.)
   *  triggers a debounced list refresh. */
  private resubscribe(): void {
    this.eventSubscription?.close();
    this.eventSubscription = undefined;
    if (!this.pinnedProject) return;
    const projectId = this.pinnedProject.id;
    this.eventSubscription = this.client.subscribe(
      `/events/project/${projectId}`,
      () => this.scheduleRefresh(),
    );
  }

  private scheduleRefresh(): void {
    if (this.refreshDebounceTimer) clearTimeout(this.refreshDebounceTimer);
    this.refreshDebounceTimer = setTimeout(() => {
      this.refreshDebounceTimer = undefined;
      void this.refresh();
    }, 250);
  }

  /** Called by extension.ts when the panel is disposed so we don't
   *  leak the SSE connection past its lifetime. */
  dispose(): void {
    this.eventSubscription?.close();
    if (this.refreshDebounceTimer) clearTimeout(this.refreshDebounceTimer);
  }

  /** Fetch ONE page (`PAGE_SIZE` rows at `offset`, newest first, filtered to the
   *  pinned project). Kept to PAGE_SIZE so a single request never exceeds the
   *  dispatcher's per-request page cap (which is why growing one window's `limit`
   *  past the cap silently stopped returning more). */
  private async fetchPage(offset: number): Promise<ExecutionPage> {
    const params = new URLSearchParams({
      limit: String(ExecutionsProvider.PAGE_SIZE),
      offset: String(offset),
    });
    if (this.pinnedProject) params.set('project_id', this.pinnedProject.id);
    return this.client.get<ExecutionPage>(`/executions?${params}`);
  }

  /** Re-fetch the whole currently-loaded span, page by page (so no single
   *  request exceeds the server cap), rebuilding the cache newest-first with no
   *  duplicate colors (a live insert can shift the window between pages). */
  async refresh(): Promise<void> {
    try {
      const rebuilt: ExecutionSummary[] = [];
      const seen = new Set<string>();
      let total = 0;
      for (let offset = 0; offset < this.loaded; offset += ExecutionsProvider.PAGE_SIZE) {
        const page = await this.fetchPage(offset);
        total = page.total;
        for (const e of page.executions) {
          if (!seen.has(e.color)) {
            seen.add(e.color);
            rebuilt.push(e);
          }
        }
        // Fewer rows than requested => we reached the end; stop early.
        if (page.executions.length < ExecutionsProvider.PAGE_SIZE) break;
      }
      this.cache = rebuilt;
      this.total = total;
    } catch (err) {
      console.warn('[weft/executions] list failed', err);
      this.cache = [];
      this.total = 0;
    }
    this._onDidChange.fire();
  }

  /** Fetch the NEXT page beyond what is loaded and append it (true offset
   *  pagination, so there is no ceiling). Bound to the "Load more" node's
   *  command. */
  async loadMore(): Promise<void> {
    try {
      const page = await this.fetchPage(this.cache.length);
      const seen = new Set(this.cache.map((e) => e.color));
      for (const e of page.executions) {
        if (!seen.has(e.color)) {
          seen.add(e.color);
          this.cache.push(e);
        }
      }
      this.total = page.total;
      this.loaded = this.cache.length;
    } catch (err) {
      console.warn('[weft/executions] load-more failed', err);
    }
    this._onDidChange.fire();
  }

  summaries(): ExecutionSummary[] {
    return this.cache;
  }

  getTreeItem(n: vscode.TreeItem): vscode.TreeItem {
    return n;
  }

  async getChildren(): Promise<vscode.TreeItem[]> {
    if (this.cache.length === 0) await this.refresh();
    // The server already ordered newest-first; no client sort.
    const nodes: vscode.TreeItem[] = this.cache.map(
      (s) => new ExecutionNode(s, this.pinnedProject?.id === s.project_id),
    );
    if (this.cache.length < this.total) nodes.push(new LoadMoreNode(this.total - this.cache.length));
    return nodes;
  }
}

/** A trailing "Load more" tree item, shown when the server has more executions
 *  than the currently loaded window. Its command grows the window. */
export class LoadMoreNode extends vscode.TreeItem {
  constructor(remaining: number) {
    super(`Load more (${remaining} more)`, vscode.TreeItemCollapsibleState.None);
    this.iconPath = new vscode.ThemeIcon('ellipsis');
    this.contextValue = 'weftExecutionLoadMore';
    this.command = { command: 'weft.loadMoreExecutions', title: 'Load more' };
  }
}

/** The `/executions` response: a page plus the total matching count. */
// SYNC: ExecutionPage <-> crates/weft-dispatcher/src/journal/mod.rs (ExecutionPage), weavemind/website/src/routes/(app)/executions/+page.ts (ExecutionPage)
interface ExecutionPage {
  executions: ExecutionSummary[];
  total: number;
}

export class ExecutionNode extends vscode.TreeItem {
  constructor(public readonly summary: ExecutionSummary, pinned: boolean) {
    const statusIcon = {
      running: '$(sync~spin)',
      completed: '$(check)',
      failed: '$(error)',
      cancelled: '$(circle-slash)',
    }[summary.status.toLowerCase()] ?? '$(circle-outline)';
    const started = new Date(summary.started_at * 1000).toLocaleString();
    super(`${statusIcon} ${summary.entry_node} (${started})`, vscode.TreeItemCollapsibleState.None);
    this.id = summary.color;
    this.description = `${summary.status}${pinned ? '' : '  ·  other project'}`;
    this.tooltip = new vscode.MarkdownString(
      [
        `**exec** ${summary.color}`,
        `**project** ${summary.project_id}`,
        `**entry** ${summary.entry_node}`,
        `**status** ${summary.status}`,
        `**started** ${started}`,
      ].join('\n\n'),
    );
    this.contextValue = `weftExecution-${summary.status.toLowerCase()}`;
    this.iconPath = statusThemeIcon(summary.status);
    this.command = {
      command: 'weft.viewExecution',
      title: 'View',
      arguments: [summary],
    };
  }
}

function statusThemeIcon(status: string): vscode.ThemeIcon {
  switch (status.toLowerCase()) {
    case 'running':
      return new vscode.ThemeIcon('sync~spin', new vscode.ThemeColor('charts.blue'));
    case 'completed':
      return new vscode.ThemeIcon('check', new vscode.ThemeColor('charts.green'));
    case 'failed':
      return new vscode.ThemeIcon('error', new vscode.ThemeColor('errorForeground'));
    case 'cancelled':
      return new vscode.ThemeIcon('circle-slash');
    default:
      return new vscode.ThemeIcon('circle-outline');
  }
}
