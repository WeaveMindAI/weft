// Publish dispatcher /validate results to VS Code's Problems panel.
// Triggered on a longer debounce than /parse so the Problems panel
// doesn't flash during every keystroke.

import * as vscode from 'vscode';
import type { DispatcherClient } from './dispatcher';
import type { Diagnostic as WeftDiagnostic, Severity } from './shared/protocol';

export function attachDiagnostics(
  context: vscode.ExtensionContext,
  client: DispatcherClient,
): void {
  const collection = vscode.languages.createDiagnosticCollection('weft');
  context.subscriptions.push(collection);

  const timers = new Map<string, NodeJS.Timeout>();

  const schedule = (doc: vscode.TextDocument) => {
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
        void runValidation(client, collection, doc);
      }, debounce),
    );
  };

  context.subscriptions.push(
    vscode.workspace.onDidChangeTextDocument((e) => schedule(e.document)),
    vscode.workspace.onDidOpenTextDocument(schedule),
    vscode.workspace.onDidCloseTextDocument((doc) => {
      collection.delete(doc.uri);
      const t = timers.get(doc.uri.toString());
      if (t) clearTimeout(t);
      timers.delete(doc.uri.toString());
    }),
  );

  for (const doc of vscode.workspace.textDocuments) schedule(doc);
}

async function runValidation(
  client: DispatcherClient,
  collection: vscode.DiagnosticCollection,
  doc: vscode.TextDocument,
): Promise<void> {
  try {
    const result = await client.validate(doc.getText());
    collection.set(doc.uri, result.diagnostics.map(toVsCodeDiagnostic));
  } catch (err) {
    // Dispatcher unreachable? Surface a single warning so the user
    // isn't confused by silent staleness.
    const msg = err instanceof Error ? err.message : String(err);
    collection.set(doc.uri, [
      new vscode.Diagnostic(
        new vscode.Range(0, 0, 0, 0),
        `weft dispatcher unreachable: ${msg}`,
        vscode.DiagnosticSeverity.Warning,
      ),
    ]);
  }
}

function toVsCodeDiagnostic(d: WeftDiagnostic): vscode.Diagnostic {
  const line = Math.max(0, d.line - 1);
  const col = Math.max(0, d.column);
  const range = new vscode.Range(line, col, line, col + 1);
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
