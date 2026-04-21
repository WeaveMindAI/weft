// Projects tree view. Polls the dispatcher every 5 seconds for
// registered project summaries. Phase B: swap polling for SSE.

import * as vscode from 'vscode';
import { DispatcherClient } from './dispatcher';

interface ProjectSummary {
  id: string;
  name: string;
  status: string;
}

export class ProjectsTreeProvider implements vscode.TreeDataProvider<ProjectNode> {
  private _onDidChangeTreeData = new vscode.EventEmitter<ProjectNode | undefined>();
  readonly onDidChangeTreeData = this._onDidChangeTreeData.event;

  private timer?: ReturnType<typeof setInterval>;
  private projects: ProjectSummary[] = [];

  constructor(private dispatcher: DispatcherClient) {
    this.startPolling();
  }

  dispose() {
    if (this.timer) clearInterval(this.timer);
  }

  getTreeItem(element: ProjectNode): vscode.TreeItem {
    return element;
  }

  async getChildren(): Promise<ProjectNode[]> {
    return this.projects.map((p) => new ProjectNode(p.id, p.name, p.status));
  }

  private startPolling() {
    const refresh = async () => {
      try {
        const projects = await this.dispatcher.get<ProjectSummary[]>('/projects');
        this.projects = projects;
        this._onDidChangeTreeData.fire(undefined);
      } catch {
        // Dispatcher unreachable. Clear the tree so the user sees it.
        if (this.projects.length > 0) {
          this.projects = [];
          this._onDidChangeTreeData.fire(undefined);
        }
      }
    };
    void refresh();
    this.timer = setInterval(refresh, 5_000);
  }
}

class ProjectNode extends vscode.TreeItem {
  constructor(id: string, name: string, status: string) {
    super(name, vscode.TreeItemCollapsibleState.None);
    this.description = status;
    this.id = id;
    this.tooltip = `${name} [${status}]\n${id}`;
    this.contextValue = 'weft.project';
    this.iconPath = new vscode.ThemeIcon(
      status === 'active' ? 'play-circle' : status === 'inactive' ? 'stop-circle' : 'circle-outline',
    );
  }
}
