// Publish `weft validate` results to VS Code's Problems panel.
// Triggered on a longer debounce than parse so the Problems panel
// doesn't flash during every keystroke. Validation runs through the
// local CLI (it needs the project's `nodes/` catalog, which the
// dispatcher pod can't see).

import * as path from 'node:path';
import * as vscode from 'vscode';
import { projectDirOf } from './cli';
import type { ParseServer } from './parseServer';
import type { Diagnostic as WeftDiagnostic, Severity } from './shared/protocol';

export function attachDiagnostics(context: vscode.ExtensionContext, parseServer: ParseServer): void {
  const collection = vscode.languages.createDiagnosticCollection('weft');
  context.subscriptions.push(collection);

  const timers = new Map<string, NodeJS.Timeout>();

  const schedule = (doc: vscode.TextDocument, reloadCatalog = false) => {
    if (doc.languageId !== 'weft') return;
    const debounce = vscode.workspace
      .getConfiguration('weft.validate')
      .get<number>('debounceMs', 500);
    const key = doc.uri.toString();
    const existing = timers.get(key);
    if (existing) clearTimeout(existing);
    timers.set(
      key,
      setTimeout(() => {
        void runValidation(collection, doc, parseServer, reloadCatalog);
      }, debounce),
    );
  };

  // A change under a `nodes/` dir alters the catalog validation runs
  // against, but no text changed, so reschedule the affected .weft
  // docs. Scope to the project that owns the changed node (the docs
  // whose project dir is an ancestor of the change), so a node edit in
  // one project doesn't fan out a `weft validate` spawn for every open
  // doc in every other project. One workspace watcher (the set of open
  // projects shifts at runtime; filtering the fan-out is simpler and
  // leak-free versus rebinding N per-project watchers).
  const revalidateForChange = (changed: vscode.Uri) => {
    const changedPath = changed.fsPath;
    for (const doc of vscode.workspace.textDocuments) {
      if (doc.languageId !== 'weft') continue;
      const projectDir = projectDirOf(doc);
      if (changedPath === projectDir || changedPath.startsWith(projectDir + path.sep)) {
        // A `nodes/` change altered the catalog: tell the warm server to
        // rebuild it before this validation, else it serves a stale catalog.
        schedule(doc, true);
      }
    }
  };
  const nodesWatcher = vscode.workspace.createFileSystemWatcher('**/nodes/**');

  context.subscriptions.push(
    vscode.workspace.onDidChangeTextDocument((e) => schedule(e.document)),
    vscode.workspace.onDidOpenTextDocument(schedule),
    vscode.workspace.onDidCloseTextDocument((doc) => {
      collection.delete(doc.uri);
      const t = timers.get(doc.uri.toString());
      if (t) clearTimeout(t);
      timers.delete(doc.uri.toString());
    }),
    nodesWatcher,
    nodesWatcher.onDidCreate(revalidateForChange),
    nodesWatcher.onDidChange(revalidateForChange),
    nodesWatcher.onDidDelete(revalidateForChange),
  );

  for (const doc of vscode.workspace.textDocuments) schedule(doc);
}

async function runValidation(
  collection: vscode.DiagnosticCollection,
  doc: vscode.TextDocument,
  parseServer: ParseServer,
  reloadCatalog: boolean,
): Promise<void> {
  try {
    const result = await parseServer.request<{ diagnostics: WeftDiagnostic[] }>({
      kind: 'validate',
      source: doc.getText(),
      file: doc.uri.fsPath,
      reloadCatalog,
    });
    collection.set(doc.uri, result.diagnostics.map(toVsCodeDiagnostic));
  } catch (err) {
    // CLI failed (not found, project error)? Surface a single warning
    // so the user isn't confused by silent staleness.
    const msg = err instanceof Error ? err.message : String(err);
    collection.set(doc.uri, [
      new vscode.Diagnostic(
        new vscode.Range(0, 0, 0, 0),
        `weft validate failed: ${msg}`,
        vscode.DiagnosticSeverity.Warning,
      ),
    ]);
  }
}

function toVsCodeDiagnostic(d: WeftDiagnostic): vscode.Diagnostic {
  // Rust lines are 1-based, columns 0-based character offsets; vscode is
  // 0-based both. The diagnostic carries the culprit's full range
  // [line:column, endLine:endColumn); underline exactly that. A degenerate
  // point span (end == start, e.g. a project-level diagnostic with no specific
  // location) falls back to a 1-char caret so it's still visible.
  const startLine = Math.max(0, d.line - 1);
  const startCol = Math.max(0, d.column);
  // Nullish-coalesce (not `||`) for both end bounds: the fields are OPTIONAL,
  // so only an absent (null/undefined) end falls back to the start. `||` would
  // wrongly treat a legitimate `endLine: 0` / `endColumn: 0` as absent.
  const endLine = Math.max(0, (d.endLine ?? d.line) - 1);
  const endCol = Math.max(0, d.endColumn ?? d.column);
  const pointSpan = endLine === startLine && endCol <= startCol;
  const range = pointSpan
    ? new vscode.Range(startLine, startCol, startLine, startCol + 1)
    : new vscode.Range(startLine, startCol, endLine, endCol);
  const diag = new vscode.Diagnostic(range, d.message, toSeverity(d.severity));
  if (d.code) diag.code = d.code;
  diag.source = 'weft';
  return diag;
}

function toSeverity(s: Severity): vscode.DiagnosticSeverity {
  switch (s) {
    case 'error':
      return vscode.DiagnosticSeverity.Error;
    case 'warning':
      return vscode.DiagnosticSeverity.Warning;
    case 'info':
      return vscode.DiagnosticSeverity.Information;
    case 'hint':
      return vscode.DiagnosticSeverity.Hint;
  }
}
