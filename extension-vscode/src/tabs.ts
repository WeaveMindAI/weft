import * as vscode from 'vscode';

/// Every open text-editor tab (with its group) whose document is `fsPath`.
/// The one place that scans `tabGroups` for a text tab by path, so callers
/// don't each re-open-code the `TabInputText` + path-compare (which had
/// drifted between `.fsPath` and `.toString()`); they derive what they need:
///   - exists:      `textTabsForPath(p).length > 0`
///   - first match: `textTabsForPath(p)[0]`
///   - close all:   `tabGroups.close(textTabsForPath(p).map(e => e.tab))`
export function textTabsForPath(
  fsPath: string,
): { tab: vscode.Tab; group: vscode.TabGroup }[] {
  return vscode.window.tabGroups.all.flatMap((group) =>
    group.tabs
      .filter((tab) => tab.input instanceof vscode.TabInputText && tab.input.uri.fsPath === fsPath)
      .map((tab) => ({ tab, group })),
  );
}
