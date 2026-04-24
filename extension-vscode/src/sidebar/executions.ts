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

export interface ExecutionSummary {
  color: string;
  project_id: string;
  entry_node: string;
  status: string;
  started_at: number;
  completed_at?: number | null;
}

export class ExecutionsProvider implements vscode.TreeDataProvider<ExecutionNode> {
  private _onDidChange = new vscode.EventEmitter<void>();
  readonly onDidChangeTreeData = this._onDidChange.event;

  private cache: ExecutionSummary[] = [];
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

  async refresh(): Promise<void> {
    try {
      this.cache = await this.client.get<ExecutionSummary[]>('/executions');
    } catch (err) {
      console.warn('[weft/executions] list failed', err);
      this.cache = [];
    }
    this._onDidChange.fire();
  }

  summaries(): ExecutionSummary[] {
    return this.cache;
  }

  getTreeItem(n: ExecutionNode): vscode.TreeItem {
    return n;
  }

  async getChildren(): Promise<ExecutionNode[]> {
    if (this.cache.length === 0) await this.refresh();
    const sorted = [...this.cache].sort((a, b) => b.started_at - a.started_at);
    return sorted.map((s) => new ExecutionNode(s, this.pinnedProject?.id === s.project_id));
  }
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
    super(`${statusIcon} ${summary.entry_node} — ${started}`, vscode.TreeItemCollapsibleState.None);
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
