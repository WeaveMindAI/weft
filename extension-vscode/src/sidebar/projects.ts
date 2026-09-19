// Projects sidebar: discovers weft projects (a folder holding a
// `weft.toml`, its program at `src/main.weft`) in the open workspace
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
  const root = findProjectRoot(entryFsPath);
  if (!root) return undefined;
  try {
    return extractPackageId(fs.readFileSync(path.join(root, 'weft.toml'), 'utf8'));
  } catch {
    return undefined;
  }
}

/** Walk up from a file (max 8 levels) to the directory containing `weft.toml`,
 *  the project root the compiler resolves `@file` and `@asset` paths against.
 *  `@include` is the exception: it resolves against the INCLUDING file's own
 *  directory, so a file deep in `src/` includes its neighbours by name.
 *  The single project-root walk; `readProjectIdFromToml` builds on it. */
export function findProjectRoot(entryFsPath: string): string | undefined {
  let dir = path.dirname(entryFsPath);
  for (let i = 0; i < 8; i++) {
    if (fs.existsSync(path.join(dir, 'weft.toml'))) {
      return dir;
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
  /** The project's id from its `weft.toml`, or a deterministic UUID
   *  keyed by the entry file's absolute path when the manifest has none. */
  id: string;
  /** Display name: the project folder. */
  label: string;
  /** Absolute path to `src/main.weft`, the .weft entry file. */
  entryPath: string;
  /** Absolute path to the project root (the folder holding `weft.toml`). */
  rootPath: string;
}

export class ProjectsProvider implements vscode.TreeDataProvider<ProjectNode> {
  private _onDidChange = new vscode.EventEmitter<void>();
  readonly onDidChangeTreeData = this._onDidChange.event;

  private cache: WeftProject[] = [];
  /// The watcher and its subscriptions, so closing this panel closes
  /// them too. Without it, every pin change left another live watcher
  /// behind, each one refreshing this tree for the rest of the session.
  private readonly disposables: vscode.Disposable[] = [];

  constructor() {
    // Refresh on workspace folder changes and on any .weft file being
    // created, saved or deleted. `onDidChange` is the save: it was
    // missing, so a save that turned a folder into a project (or
    // renamed one) did not show up until something else refreshed.
    const watcher = vscode.workspace.createFileSystemWatcher('**/*.weft');
    this.disposables.push(
      watcher,
      watcher.onDidCreate(() => this.refresh()),
      watcher.onDidChange(() => this.refresh()),
      watcher.onDidDelete(() => this.refresh()),
      vscode.workspace.onDidChangeWorkspaceFolders(() => this.refresh()),
    );
  }

  dispose(): void {
    for (const d of this.disposables) d.dispose();
    this.disposables.length = 0;
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

/// A project is a folder with a `weft.toml`, and its program is
/// `src/main.weft` (the one entry the CLI reads, `Project::main_weft`
/// in weft-compiler). One row per manifest: the modules under `src/`
/// are parts of that project, not projects of their own, which is what
/// listing every `.weft` file used to make them. A manifest whose
/// entry file is missing is skipped rather than shown as a project the
/// graph cannot open.
async function discoverProjects(): Promise<WeftProject[]> {
  const manifests = await vscode.workspace.findFiles('**/weft.toml', '**/node_modules/**');
  const projects: WeftProject[] = [];
  for (const uri of manifests) {
    const dir = path.dirname(uri.fsPath);
    const entry = path.join(dir, 'src', 'main.weft');
    if (!fs.existsSync(entry)) continue;
    projects.push({
      id: readProjectIdFromToml(entry) ?? deriveProjectId(entry),
      label: path.basename(dir),
      entryPath: entry,
      rootPath: dir,
    });
  }
  projects.sort((a, b) => a.label.localeCompare(b.label));
  return projects;
}
