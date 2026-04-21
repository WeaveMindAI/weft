// Projects tree view provider. Renders every project registered with
// the dispatcher, with live status driven by SSE events. Phase A2
// implements the SSE subscription and refresh logic.

import * as vscode from 'vscode';
import { DispatcherClient } from './dispatcher';

export class ProjectsTreeProvider implements vscode.TreeDataProvider<ProjectNode> {
  private _onDidChangeTreeData = new vscode.EventEmitter<ProjectNode | undefined>();
  readonly onDidChangeTreeData = this._onDidChangeTreeData.event;

  constructor(private dispatcher: DispatcherClient) {}

  getTreeItem(element: ProjectNode): vscode.TreeItem {
    return element;
  }

  async getChildren(): Promise<ProjectNode[]> {
    try {
      const projects = await this.dispatcher.get<{ id: string; name: string; status: string }[]>('/projects');
      return projects.map((p) => new ProjectNode(p.id, p.name, p.status));
    } catch {
      return [];
    }
  }
}

class ProjectNode extends vscode.TreeItem {
  constructor(id: string, name: string, status: string) {
    super(name, vscode.TreeItemCollapsibleState.None);
    this.description = status;
    this.id = id;
    this.contextValue = 'weft.project';
  }
}
