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

import type { DispatcherClient } from '../dispatcher';
import type { WeftProject } from './projects';

// SYNC: ExecutionSummary <-> crates/weft-dispatcher/src/journal/mod.rs (ExecutionSummary), weavemind/website/src/routes/(app)/executions/+page.ts (Execution)
export interface ExecutionSummary {
  color: string;
  project_id: string;
  entry_node: string;
  status: string;
  /** A trigger fire or manual run (`fire`), or one of the two setup
   *  runs an activate / resync / infra start makes. */
  phase: 'fire' | 'trigger_setup' | 'infra_setup';
  started_at: number;
  completed_at?: number | null;
  /** The tags the run put on itself (`ctx.tag_execution`), in claim
   *  order; the handle a sibling's `ctx.stop_tagged` selects on. */
  tags: string[];
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
  // Tight refresh burst after an event: avoids piling a second
  // refresh when multiple events land within milliseconds of each
  // other (ExecutionStarted + NodeStarted arrive back-to-back).
  private refreshDebounceTimer: NodeJS.Timeout | undefined;
  // Three independent callers can refresh (pin change, debounced
  // event, reconnect resync); concurrent multi-page rebuilds racing
  // to assign `cache` could settle on the older snapshot, so callers
  // arriving mid-refresh share the in-flight one. Keyed by the pin it
  // was started under: a pin CHANGE must start a fresh fetch, not
  // join the old pin's.
  private inFlightRefresh:
    | { projectId: string | undefined; promise: Promise<string | undefined> }
    | undefined;
  // Monotonic rebuild ordering: only a rebuild at least as new as the
  // last committed one may write, so an older same-pin rebuild
  // resolving late can never clobber a newer snapshot.
  private refreshSeq = 0;
  private settledSeq = 0;
  private disposed = false;
  // Last list-fetch failure, shown as a tree row. The cache keeps the
  // last successful list: blanking it would render "no executions"
  // for what is actually "could not reach the dispatcher".
  private lastError: string | undefined;

  constructor(private readonly client: DispatcherClient) {}

  setPinnedProject(project: WeftProject | undefined): void {
    if (this.pinnedProject?.id === project?.id) return;
    this.pinnedProject = project;
    // Always refresh on pin changes so the tree shows the new
    // project's runs immediately instead of waiting for the first
    // event to arrive (which might never if the project is idle).
    void this.refresh();
  }

  /** Fed by the shared project event stream (extension.ts): every
   *  event (ExecutionStarted / ExecutionCompleted / NodeStarted /
   *  etc.) triggers a debounced list refresh. */
  noteEvent(): void {
    this.scheduleRefresh();
  }

  /** The newest execution of `projectId` currently in the RUNNING
   *  state, from the last successfully fetched newest-first list.
   *  What the reconnect resync uses to catch the auto-follow up on a
   *  run that started while the project event stream was down. The
   *  explicit project filter keeps the answer scoped even if the
   *  cached list ever holds other projects' rows. */
  newestRunningFor(projectId: string): string | undefined {
    return this.cache.find((e) => e.status === 'running' && e.project_id === projectId)?.color;
  }

  private scheduleRefresh(): void {
    if (this.refreshDebounceTimer) clearTimeout(this.refreshDebounceTimer);
    this.refreshDebounceTimer = setTimeout(() => {
      this.refreshDebounceTimer = undefined;
      void this.refresh();
    }, 250);
  }

  /** Called by extension.ts when the panel is disposed. In-flight
   *  rebuilds check the flag at their commit point, so nothing writes
   *  or fires after teardown. */
  dispose(): void {
    this.disposed = true;
    if (this.refreshDebounceTimer) clearTimeout(this.refreshDebounceTimer);
    this._onDidChange.dispose();
  }

  /** Fetch ONE page (`PAGE_SIZE` rows at `offset`, newest first, filtered to the
   *  pinned project). Kept to PAGE_SIZE so a single request never exceeds the
   *  dispatcher's per-request page cap (which is why growing one window's `limit`
   *  past the cap silently stopped returning more). */
  private async fetchPage(offset: number, projectId: string | undefined): Promise<ExecutionPage> {
    const params = new URLSearchParams({
      limit: String(ExecutionsProvider.PAGE_SIZE),
      offset: String(offset),
    });
    if (projectId) params.set('project_id', projectId);
    return this.client.get<ExecutionPage>(`/executions?${params}`);
  }

  /** Re-fetch the whole currently-loaded span, page by page (so no single
   *  request exceeds the server cap), rebuilding the cache newest-first with no
   *  duplicate colors (a live insert can shift the window between pages).
   *  Never rejects: a fetch failure keeps the last successful list and
   *  surfaces as an error row in the tree. Concurrent callers share the
   *  in-flight refresh instead of racing their rebuilds. Resolves the
   *  pin the rebuild SETTLED under (undefined when it was discarded
   *  because the pin moved mid-flight), so an awaiting caller can tell
   *  "the cache now reflects this pin" from "my awaited refresh was
   *  obsoleted by a pin change". */
  refresh(): Promise<string | undefined> {
    const target = this.pinnedProject?.id;
    const inFlight = this.inFlightRefresh;
    if (inFlight && inFlight.projectId === target) return inFlight.promise;
    return this.startRefresh(target);
  }

  /** Always start a fresh rebuild, never join (loadMore must see its
   *  just-grown window; an in-flight rebuild may already be past it). */
  private startRefresh(target: string | undefined): Promise<string | undefined> {
    const seq = ++this.refreshSeq;
    const promise = this.doRefresh(target, seq).finally(() => {
      if (this.inFlightRefresh?.promise === promise) this.inFlightRefresh = undefined;
    });
    this.inFlightRefresh = { projectId: target, promise };
    return promise;
  }

  /** Rebuild against the pin SNAPSHOTTED at start: a pin change
   *  mid-rebuild must not stitch two projects' pages into one list,
   *  so every page uses the snapshot, and the commit point discards a
   *  result whose pin moved (the pin change already started its own
   *  refresh) or that an even newer rebuild already superseded. */
  private async doRefresh(
    projectId: string | undefined,
    seq: number,
  ): Promise<string | undefined> {
    let rebuilt: ExecutionSummary[] | undefined;
    let total = 0;
    let error: string | undefined;
    try {
      rebuilt = [];
      const seen = new Set<string>();
      for (let offset = 0; offset < this.loaded; offset += ExecutionsProvider.PAGE_SIZE) {
        const page = await this.fetchPage(offset, projectId);
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
    } catch (err) {
      rebuilt = undefined;
      error = err instanceof Error ? err.message : String(err);
    }
    if (this.disposed || this.pinnedProject?.id !== projectId || seq < this.settledSeq) {
      return undefined;
    }
    this.settledSeq = seq;
    if (rebuilt) {
      this.cache = rebuilt;
      this.total = total;
      this.lastError = undefined;
    } else {
      // Fire only when the error CHANGES: getChildren retries on
      // expand while the list is empty, so a repeat failure firing
      // the tree again would loop fetch -> fail -> fire -> fetch.
      if (this.lastError === error) return projectId;
      this.lastError = error;
    }
    this._onDidChange.fire();
    return projectId;
  }

  /** Grow the window by one page and rebuild through the ONE rebuild
   *  path (true offset pagination, so there is no ceiling). Bound to
   *  the "Load more" node's command. Delegating instead of appending
   *  in place keeps a concurrent event-driven refresh from undoing
   *  the growth (both paths share the same sequence-guarded commit). */
  async loadMore(): Promise<void> {
    this.loaded += ExecutionsProvider.PAGE_SIZE;
    await this.startRefresh(this.pinnedProject?.id);
  }

  summaries(): ExecutionSummary[] {
    return this.cache;
  }

  getTreeItem(n: vscode.TreeItem): vscode.TreeItem {
    return n;
  }

  async getChildren(): Promise<vscode.TreeItem[]> {
    // Re-fetch on expand whenever the list is empty, error row
    // included: expanding IS the natural retry, and the in-flight
    // collapse in refresh() prevents a fetch storm.
    if (this.cache.length === 0) await this.refresh();
    const nodes: vscode.TreeItem[] = [];
    // A failed fetch is on screen, not just in the console: the rows
    // below it are the last successful list, not the current truth.
    if (this.lastError) nodes.push(new ListErrorNode(this.lastError));
    // The server already ordered newest-first; no client sort.
    for (const s of this.cache) {
      nodes.push(new ExecutionNode(s, this.pinnedProject?.id === s.project_id));
    }
    if (this.cache.length < this.total) nodes.push(new LoadMoreNode(this.total - this.cache.length));
    return nodes;
  }
}

/** Shown as the first row when the executions list could not be
 *  fetched. The refresh button is the recovery action. */
class ListErrorNode extends vscode.TreeItem {
  constructor(message: string) {
    super('Executions list unavailable', vscode.TreeItemCollapsibleState.None);
    this.description = 'refresh to retry';
    this.iconPath = new vscode.ThemeIcon('warning', new vscode.ThemeColor('errorForeground'));
    this.tooltip = message;
    this.contextValue = 'weftExecutionListError';
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
      corrupt: '$(warning)',
    }[summary.status.toLowerCase()] ?? '$(circle-outline)';
    const started = new Date(summary.started_at * 1000).toLocaleString();
    // A corrupt row has no entry node (its journal payload no longer
    // decodes); it is listed so the user can see and delete it.
    const name = summary.status === 'corrupt' ? '(corrupt journal)' : summary.entry_node;
    super(`${statusIcon} ${name} (${started})`, vscode.TreeItemCollapsibleState.None);
    this.id = summary.color;
    const tags = summary.tags;
    const tagged = tags.length > 0 ? `  ·  ${tags.join(', ')}` : '';
    this.description = `${summary.status}${tagged}${pinned ? '' : '  ·  other project'}`;
    this.tooltip = new vscode.MarkdownString(
      [
        `**exec** ${summary.color}`,
        `**project** ${summary.project_id}`,
        `**entry** ${summary.entry_node}`,
        `**status** ${summary.status}`,
        ...(tags.length > 0 ? [`**tags** ${tags.join(', ')}`] : []),
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
    case 'corrupt':
      return new vscode.ThemeIcon('warning', new vscode.ThemeColor('charts.orange'));
    default:
      return new vscode.ThemeIcon('circle-outline');
  }
}
