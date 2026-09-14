// Executions sidebar, two modes on one provider. Flat: every execution
// the dispatcher knows about, newest first, whichever project it was
// on ("I just ran something, show me the latest run"). By version: the
// pinned project's version tree (`weft tree --json`), root versions,
// child versions, each expanding to its runs with status, seed, scope
// and example, head and the version on disk marked. The view-in-graph
// and delete actions hang off the individual items; branch, checkpoint,
// prune, diff and freeze hang off the version-mode items.

import * as vscode from 'vscode';

import { runWeftJson } from '../cli';
import type { DispatcherClient } from '../dispatcher';
import type { WeftProject } from './projects';
import {
  buildVersionTree,
  runDescription,
  runScopedTo,
  versionLabel,
  versionMarks,
  type RunSummary,
  type TreeJson,
  type VersionTreeNode,
} from './version-tree';

export type ExecutionsMode = 'flat' | 'byVersion';

// SYNC: ExecutionSummary <-> crates/weft-dispatcher/src/journal/mod.rs (ExecutionSummary), weavemind/website/src/routes/(app)/executions/+page.ts (Execution)
export interface ExecutionSummary {
  color: string;
  project_id: string;
  entry_node: string;
  status: string;
  /** A trigger fire or manual run (`fire`), or one of the two setup
   *  runs an activate / resync / infra start makes. */
  // SYNC: ExecutionSummary.phase <-> crates/weft-core/src/primitive.rs Phase
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
    | { projectId: string | undefined; mode: ExecutionsMode; promise: Promise<string | undefined> }
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
  /// Flat newest-first, or the pinned project's version tree.
  private mode: ExecutionsMode = 'flat';
  /// The last tree fetched for the pinned project (by-version mode).
  private tree: TreeJson | undefined;
  /// Which project `tree` belongs to. A tree is only ever reused for the
  /// project it was read in: reusing it across a pin change drew one
  /// project's version tree under another project's name, and handed that
  /// project's version ids to branch, diff-with-head and the graph
  /// banner.
  private treeProjectId: string | undefined;
  /// The group the editor is focused inside (an include's alias chain),
  /// so the runs scoped to it are marked. Set by the graph view.
  private focusedGroup: string | null = null;

  constructor(private readonly client: DispatcherClient) {}

  currentMode(): ExecutionsMode {
    return this.mode;
  }

  /** Switch modes; the loaded flat page is kept for the way back. */
  toggleMode(): void {
    this.mode = this.mode === 'flat' ? 'byVersion' : 'flat';
    // Switching INTO the version view reads the tree now, whatever the
    // floor says: the person just asked to see it.
    this.treeFetchedAt = 0;
    // Redrawn immediately, before the read. The person pressed a button,
    // so the view has to answer; waiting for the refresh to commit meant
    // that with the dispatcher down (where the refresh changes nothing)
    // the button did nothing at all and said nothing either.
    this._onDidChange.fire();
    void this.refresh();
  }

  setFocusedGroup(group: string | null): void {
    if (this.focusedGroup === group) return;
    this.focusedGroup = group;
    this._onDidChange.fire();
  }

  /** The tree last fetched, for the commands that need head or a
   *  run's version (branch, diff with head, the graph banner). */
  currentTree(): TreeJson | undefined {
    return this.tree;
  }

  setPinnedProject(project: WeftProject | undefined): void {
    if (this.pinnedProject?.id === project?.id) return;
    this.pinnedProject = project;
    this.tree = undefined;
    this.treeProjectId = undefined;
    this.treeFetchedAt = 0;
    this.cache = [];
    this.total = 0;
    this.lastError = undefined;
    this._onDidChange.fire();
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

  /** How often the version tree may be re-read from the CLI. */
  private static readonly TREE_MIN_INTERVAL_MS = 2000;
  private treeFetchedAt = 0;

  private scheduleRefresh(delayMs = 250): void {
    if (this.refreshDebounceTimer) clearTimeout(this.refreshDebounceTimer);
    this.refreshDebounceTimer = setTimeout(() => {
      this.refreshDebounceTimer = undefined;
      void this.refresh();
    }, delayMs);
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
    // The MODE is part of what a rebuild answers, so joining one that
    // started in the other mode is not the same question. Toggling to
    // "by version" used to join a flat rebuild already in flight, which
    // fetched no tree and committed `tree: undefined`, and the view then
    // drew "no versions yet" for a project that has them until some
    // unrelated refresh happened along.
    if (inFlight && inFlight.projectId === target && inFlight.mode === this.mode) {
      return inFlight.promise;
    }
    return this.startRefresh(target);
  }

  /** Always start a fresh rebuild, never join (loadMore must see its
   *  just-grown window; an in-flight rebuild may already be past it). */
  private startRefresh(target: string | undefined): Promise<string | undefined> {
    const seq = ++this.refreshSeq;
    const promise = this.doRefresh(target, seq).finally(() => {
      if (this.inFlightRefresh?.promise === promise) this.inFlightRefresh = undefined;
    });
    this.inFlightRefresh = { projectId: target, mode: this.mode, promise };
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
    let tree: TreeJson | undefined;
    let treeFetchedAt = this.treeFetchedAt;
    try {
      // The version tree is the pinned project's alone (it needs the
      // files on disk to say which version they are), read through the
      // CLI in that folder.
      if (this.mode === 'byVersion' && this.pinnedProject) {
        // The tree comes from a CLI PROCESS, so it gets a floor of its
        // own. A refresh is debounced 250ms and every dispatcher event
        // schedules one, so a single running execution emitting node
        // events was spawning `weft tree --json` about four times a
        // second for as long as it ran. Inside the floor the last answer
        // is reused and one more refresh is booked for when the floor
        // lifts, so the view still ends up current.
        const age = Date.now() - this.treeFetchedAt;
        const reusable = this.tree && this.treeProjectId === projectId;
        if (reusable && age < ExecutionsProvider.TREE_MIN_INTERVAL_MS) {
          tree = this.tree;
          this.scheduleRefresh(ExecutionsProvider.TREE_MIN_INTERVAL_MS - age);
        } else {
          tree = await runWeftJson<TreeJson>(['tree', '--json'], this.pinnedProject.rootPath);
          treeFetchedAt = Date.now();
        }
      }
      rebuilt = [];
      const seen = new Set<string>();
      // The list is the open project's runs and nothing else. With no
      // project open there is nothing to list, so nothing is fetched:
      // an unscoped read returned every project's history, and that is
      // what the view showed until the graph opened and pinned one.
      for (let offset = 0; projectId && offset < this.loaded; offset += ExecutionsProvider.PAGE_SIZE) {
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
    const before = this.drawnState();
    if (rebuilt) {
      this.cache = rebuilt;
      this.total = total;
      this.tree = tree;
      // Set with the tree and only here, where the sequence guard has
      // already agreed this answer is the current one: marking it at
      // fetch time would label a DISCARDED refresh's project onto the
      // tree still in hand, and the next refresh would reuse it.
      this.treeProjectId = tree ? projectId : undefined;
      this.treeFetchedAt = treeFetchedAt;
      this.lastError = undefined;
    } else {
      this.lastError = error;
    }
    // Fire only when what is DRAWN changed.
    //
    // `getChildren` re-reads whenever the list is empty, because
    // expanding is the natural retry, so an answer that changes nothing
    // and still fires puts the view in a loop: fetch, same answer, fire,
    // expand, fetch. That bit both an empty project (a successful answer
    // with no executions, for ever) and a repeated failure.
    if (this.drawnState() === before) return projectId;
    this._onDidChange.fire();
    return projectId;
  }

  /// Everything the view draws, as one string, for deciding whether a
  /// refresh actually changed anything.
  private drawnState(): string {
    const runs = this.tree?.runs.map((r) => `${r.color}:${r.status}:${r.example ?? ''}`).join(',') ?? '';
    const versions = this.tree?.versions.map((v) => `${v.id}:${v.parent_id ?? ''}:${v.label ?? ''}`).join(',') ?? '';
    const head = this.tree ? `${this.tree.head.head_version ?? ''}/${this.tree.head.head_run ?? ''}` : '';
    return [
      this.mode,
      this.pinnedProject?.id ?? '',
      this.focusedGroup ?? '',
      this.total,
      this.loaded,
      this.lastError ?? '',
      this.cache.map((e) => `${e.color}:${e.status}`).join(','),
      versions,
      runs,
      head,
      this.tree?.disk_version ?? '',
      this.tree?.head.activation_version ?? '',
    ].join('|');
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

  async getChildren(element?: vscode.TreeItem): Promise<vscode.TreeItem[]> {
    if (element instanceof VersionNode) {
      const headRun = this.tree?.head.head_run ?? null;
      return [
        ...element.node.runs.map(
          (r) =>
            new RunNode(
              r,
              element.node.version.id,
              this.pinnedProject?.id ?? '',
              headRun,
              runScopedTo(r, this.focusedGroup),
            ),
        ),
        ...element.node.children.map((c) => new VersionNode(c)),
      ];
    }
    if (element) return [];
    // Re-fetch on expand whenever the list is empty, error row
    // included: expanding IS the natural retry, and the in-flight
    // collapse in refresh() prevents a fetch storm.
    if (this.cache.length === 0) await this.refresh();
    if (this.mode === 'byVersion') {
      const nodes: vscode.TreeItem[] = [];
      if (this.lastError) nodes.push(new ListErrorNode(this.lastError));
      if (!this.pinnedProject) {
        nodes.push(new HintNode('Pin a project to see its version tree'));
        return nodes;
      }
      const roots = this.tree ? buildVersionTree(this.tree) : [];
      if (roots.length === 0) nodes.push(new HintNode('No versions yet: `weft run` or `weft checkpoint` records one'));
      for (const root of roots) nodes.push(new VersionNode(root));
      return nodes;
    }
    const nodes: vscode.TreeItem[] = [];
    // A failed fetch is on screen, not just in the console: the rows
    // below it are the last successful list, not the current truth.
    if (this.lastError) nodes.push(new ListErrorNode(this.lastError));
    if (!this.pinnedProject) {
      nodes.push(new HintNode('Open a project to see its runs'));
      return nodes;
    }
    // The server already ordered newest-first; no client sort.
    for (const s of this.cache) nodes.push(new ExecutionNode(s));
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

/** A one-line hint row (no project pinned, no versions yet). */
class HintNode extends vscode.TreeItem {
  constructor(message: string) {
    super(message, vscode.TreeItemCollapsibleState.None);
    this.iconPath = new vscode.ThemeIcon('info');
    this.contextValue = 'weftExecutionHint';
  }
}

/** One version of the pinned project, expanding to its runs and the
 *  versions edited from it. */
export class VersionNode extends vscode.TreeItem {
  constructor(public readonly node: VersionTreeNode) {
    super(versionLabel(node), vscode.TreeItemCollapsibleState.Expanded);
    this.id = `version:${node.version.id}`;
    const marks = versionMarks(node);
    this.description = marks.join('  ·  ');
    this.iconPath = new vscode.ThemeIcon(
      node.isHead ? 'git-commit' : 'circle-outline',
      node.isDisk ? new vscode.ThemeColor('charts.green') : undefined,
    );
    this.tooltip = new vscode.MarkdownString(
      [
        `**version** ${node.version.id}`,
        ...(node.version.label ? [`**label** ${node.version.label}`] : []),
        ...(node.version.parent_id ? [`**parent** ${node.version.parent_id}`] : ['**root**']),
        `**created** ${new Date(node.version.created_at * 1000).toLocaleString()}`,
        ...(marks.length > 0 ? [`**marks** ${marks.join(', ')}`] : []),
      ].join('\n\n'),
    );
    this.contextValue = 'weftVersion';
  }
}

/** One run under a version. Opens in the graph like a flat row; carries
 *  its version so the graph can say when the code on disk differs. */
export class RunNode extends vscode.TreeItem {
  readonly summary: ExecutionSummary;
  constructor(
    public readonly run: RunSummary,
    public readonly versionId: string,
    /// The project whose tree this run hangs in. Carried because
    /// "view in graph" needs a project to switch to, and a run row on
    /// its own does not name one.
    projectId: string,
    headRun: string | null,
    scopedToFocus: boolean,
  ) {
    super(`${scopedToFocus ? '◉ ' : ''}${run.color.slice(0, 8)}`, vscode.TreeItemCollapsibleState.None);
    this.id = `run:${run.color}`;
    this.summary = {
      color: run.color,
      project_id: projectId,
      entry_node: run.spec?.name ?? '',
      status: run.status,
      phase: 'fire',
      started_at: run.started_at,
      completed_at: run.completed_at,
      tags: [],
    };
    this.description = runDescription(run, headRun);
    this.tooltip = new vscode.MarkdownString(
      [
        `**run** ${run.color}`,
        `**version** ${versionId}`,
        `**status** ${run.status}`,
        ...(run.seed_color ? [`**seed** ${run.seed_color} (stale: ${run.stale.join(', ') || 'none'})`] : []),
        ...(run.spec ? [`**spec** ${run.spec.name}`] : []),
        ...(run.example ? [`**example** ${run.example}`] : []),
        `**started** ${new Date(run.started_at * 1000).toLocaleString()}`,
      ].join('\n\n'),
    );
    this.contextValue = `weftRun-${run.status.toLowerCase()}`;
    this.iconPath = statusThemeIcon(run.status);
    this.command = { command: 'weft.viewExecution', title: 'View', arguments: [this] };
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
  constructor(public readonly summary: ExecutionSummary) {
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
    this.description = `${summary.status}${tagged}`;
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
