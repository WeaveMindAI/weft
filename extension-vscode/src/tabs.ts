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

/// True when this document is open for REVIEW rather than for editing: one
/// side of a diff, or a revision from source control (`git:`, and every
/// other read-only scheme a provider serves).
///
/// A `.weft` file normally drives the graph the moment it is focused, which
/// is right when you are working on a program and wrong when you are reading
/// a diff of one: the graph pops over the comparison you came to read. Both
/// the auto-open and the graph's own follow ask this first.
///
/// Only the ACTIVE tab decides. A diff of this file sitting open in a
/// background column must not veto the plain editor tab the user just
/// clicked, or the graph stops following until that diff is closed.
/// Callers that fire from `onDidChangeActiveTextEditor` defer one tick
/// before asking (the tab model can still hold the previous tab when
/// that event fires).
export function isReviewDoc(
  doc: Pick<vscode.TextDocument, 'uri'>,
  activeTab: vscode.Tab | undefined = vscode.window.tabGroups.activeTabGroup.activeTab,
): boolean {
  // A revision from source control (`git:` and every other provider
  // scheme) is never the working tree, so it can never be the file the
  // graph edits. `untitled:` is the one non-file scheme that IS being
  // written, not read.
  if (doc.uri.scheme !== 'file' && doc.uri.scheme !== 'untitled') return true;
  // The working-tree side of a diff IS a `file:` URI; only the tab
  // knows it is being shown as a comparison.
  const shown = doc.uri.toString();
  const input = activeTab?.input;
  return (
    input instanceof vscode.TabInputTextDiff &&
    (input.original.toString() === shown || input.modified.toString() === shown)
  );
}

/// One macrotask, so the tab model has settled by the time
/// `isReviewDoc` reads the active tab. Callers re-check that the editor
/// they reacted to is still the active one after waiting.
export function afterTabModelSettles(): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, 0));
}
