// Projects sidebar: discovers `.weft` files in the open workspace
// folders and exposes them as a TreeView. Refresh is cheap (a single
// findFiles glob), so we re-run it whenever the user asks or a
// file-system event suggests new/removed .weft files.

import * as vscode from 'vscode';
import * as path from 'node:path';
import * as crypto from 'node:crypto';
import * as fs from 'node:fs';

/** Fallback per-path project ID for folders without a weft.toml.
 *  Deterministic UUIDv5-style so reopening a file keeps the same
 *  id if we never write one to disk. Real projects should always
 *  have an id in their weft.toml: the CLI's `weft.toml` is the
 *  canonical source of truth and every `weft run/build` guarantees
 *  it exists. */
const NAMESPACE = Buffer.from('6ba7b810-9dad-11d1-80b4-00c04fd430c8', 'hex');
export function deriveProjectId(fsPath: string): string {
  const h = crypto.createHash('sha1').update(NAMESPACE).update(fsPath).digest();
  h[6] = (h[6] & 0x0f) | 0x50;
  h[8] = (h[8] & 0x3f) | 0x80;
  const hex = h.slice(0, 16).toString('hex');
  return `${hex.slice(0, 8)}-${hex.slice(8, 12)}-${hex.slice(12, 16)}-${hex.slice(16, 20)}-${hex.slice(20, 32)}`;
}

/** Read the project id from a `weft.toml` next to the entry file,
 *  or higher up the tree. Matches what `weft-compiler::project`
 *  does on the CLI side so the extension, CLI, and dispatcher all
 *  agree on one id per project. Returns undefined if no weft.toml
 *  is found or if it has no `[package].id`.
 *
 *  Tiny hand-rolled parser: we only want one field, and pulling in
 *  a TOML dep for this is overkill.
 */
export function readProjectIdFromToml(entryFsPath: string): string | undefined {
  let dir = path.dirname(entryFsPath);
  for (let i = 0; i < 8; i++) {
    const candidate = path.join(dir, 'weft.toml');
    if (fs.existsSync(candidate)) {
      try {
        const text = fs.readFileSync(candidate, 'utf8');
        return extractPackageId(text);
      } catch {
        return undefined;
      }
    }
    const parent = path.dirname(dir);
    if (parent === dir) break;
    dir = parent;
  }
  return undefined;
}

function extractPackageId(toml: string): string | undefined {
  // Match `id = "..."` inside a `[package]` section.
  let inPackage = false;
  for (const rawLine of toml.split('\n')) {
    const line = rawLine.trim();
    if (line.startsWith('[')) {
      inPackage = line === '[package]';
      continue;
    }
    if (!inPackage) continue;
    const m = line.match(/^id\s*=\s*"([^"]+)"/);
    if (m) return m[1];
  }
  return undefined;
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
        // weft.toml is the source of truth when present (matches
        // CLI + dispatcher). Fall back to the path-derived id only
        // for orphan .weft files with no weft.toml anywhere up the
        // tree.
        id: readProjectIdFromToml(f) ?? deriveProjectId(f),
        label: path.basename(dir),
        entryPath: f,
        rootPath: dir,
      });
    }
  }
  projects.sort((a, b) => a.label.localeCompare(b.label));
  return projects;
}
