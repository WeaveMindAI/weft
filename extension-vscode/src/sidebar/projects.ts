// Projects sidebar: discovers `.weft` files in the open workspace
// folders and exposes them as a TreeView. Refresh is cheap (a single
// findFiles glob), so we re-run it whenever the user asks or a
// file-system event suggests new/removed .weft files.

import * as vscode from 'vscode';
import * as path from 'node:path';
import * as crypto from 'node:crypto';

/** Stable per-path project ID. Deterministic so reopening a file
 *  keeps its execution history consistent across sessions. Uses
 *  the UUIDv5 namespace algorithm (sha1-based) under a fixed
 *  namespace so the caller doesn't need the uuid package. */
const NAMESPACE = Buffer.from('6ba7b810-9dad-11d1-80b4-00c04fd430c8', 'hex');
export function deriveProjectId(fsPath: string): string {
  const h = crypto.createHash('sha1').update(NAMESPACE).update(fsPath).digest();
  h[6] = (h[6] & 0x0f) | 0x50;
  h[8] = (h[8] & 0x3f) | 0x80;
  const hex = h.slice(0, 16).toString('hex');
  return `${hex.slice(0, 8)}-${hex.slice(8, 12)}-${hex.slice(12, 16)}-${hex.slice(16, 20)}-${hex.slice(20, 32)}`;
}

export interface WeftProject {
  /** Deterministic UUID keyed by the main.weft absolute path. */
  id: string;
  /** Display name: the containing folder. */
  label: string;
  /** Absolute path to the .weft entry file. */
  entryPath: string;
  /** Absolute path to the folder containing the .weft file. */
  rootPath: string;
}

export class ProjectsProvider implements vscode.TreeDataProvider<ProjectNode> {
  private _onDidChange = new vscode.EventEmitter<void>();
  readonly onDidChangeTreeData = this._onDidChange.event;

  private cache: WeftProject[] = [];

  constructor() {
    // Refresh on workspace folder changes + on any .weft save/create/delete.
    const watcher = vscode.workspace.createFileSystemWatcher('**/*.weft');
    watcher.onDidCreate(() => this.refresh());
    watcher.onDidDelete(() => this.refresh());
    vscode.workspace.onDidChangeWorkspaceFolders(() => this.refresh());
  }

  async refresh(): Promise<void> {
    this.cache = await discoverProjects();
    this._onDidChange.fire();
  }

  projects(): WeftProject[] {
    return this.cache;
  }

  getTreeItem(node: ProjectNode): vscode.TreeItem {
    return node;
  }

  async getChildren(): Promise<ProjectNode[]> {
    if (this.cache.length === 0) {
      // Populate once on first expand.
      this.cache = await discoverProjects();
    }
    return this.cache.map((p) => new ProjectNode(p));
  }
}

export class ProjectNode extends vscode.TreeItem {
  constructor(public readonly project: WeftProject) {
    super(project.label, vscode.TreeItemCollapsibleState.None);
    this.id = project.id;
    this.description = path.relative(project.rootPath, project.entryPath);
    this.resourceUri = vscode.Uri.file(project.entryPath);
    this.tooltip = project.entryPath;
    this.iconPath = new vscode.ThemeIcon('symbol-class');
    this.contextValue = 'weftProject';
    this.command = {
      command: 'weft.openInEditor',
      title: 'Open',
      arguments: [project],
    };
  }
}

async function discoverProjects(): Promise<WeftProject[]> {
  const uris = await vscode.workspace.findFiles('**/*.weft', '**/node_modules/**');
  // Group by parent folder; show one entry per folder (take the
  // lexicographically first .weft file as the entry). Multiple
  // .weft files in one folder = multiple entries for now; revisit
  // if that becomes common.
  const byFolder = new Map<string, string[]>();
  for (const uri of uris) {
    const p = uri.fsPath;
    const dir = path.dirname(p);
    const list = byFolder.get(dir) ?? [];
    list.push(p);
    byFolder.set(dir, list);
  }
  const projects: WeftProject[] = [];
  for (const [dir, files] of byFolder) {
    files.sort();
    for (const f of files) {
      projects.push({
        id: deriveProjectId(f),
        label: path.basename(dir),
        entryPath: f,
        rootPath: dir,
      });
    }
  }
  projects.sort((a, b) => a.label.localeCompare(b.label));
  return projects;
}
