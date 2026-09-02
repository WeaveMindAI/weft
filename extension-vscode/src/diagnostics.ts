// Publish `weft validate` results to VS Code's Problems panel.
// Triggered on a longer debounce than parse so the Problems panel
// doesn't flash during every keystroke. Validation runs through the
// local CLI (it needs the project's `nodes/` catalog, which the
// dispatcher pod can't see).
//
// The owner/file bookkeeping (whose findings land where, and at which
// URI they publish) lives in DiagnosticRouter, pure over strings so
// it is tested with a hand-rolled sink; this module owns only the
// vscode wiring: debounce, document lifecycle, the CLI call, and the
// finding -> vscode.Diagnostic conversion.

import * as path from 'node:path';
import * as vscode from 'vscode';
import { projectDirOf } from './cli';
import { DiagnosticRouter } from './diagnosticRouter';
import { canonicalPath, weftPositionToVsCode } from './locations';
import type { ParseServer } from './parseServer';
import type { Diagnostic as WeftDiagnostic, Severity } from '../../packages/weft-graph/src/protocol';

/// One document's diagnostic bookkeeping: its owner key (the URI
/// string), its canonical file path (resolved ONCE, so validate and
/// close cannot disagree when the filesystem answer changes between
/// them), and a run sequence so a late reply can never clobber a newer
/// run's result.
interface DocDiagState {
  owner: string;
  file: string;
  seq: number;
}

export function attachDiagnostics(context: vscode.ExtensionContext, parseServer: ParseServer): void {
  const collection = vscode.languages.createDiagnosticCollection('weft');
  context.subscriptions.push(collection);

  const timers = new Map<string, NodeJS.Timeout>();
  const docStates = new Map<string, DocDiagState>();
  const router = new DiagnosticRouter<vscode.Diagnostic>(
    {
      set: (uri, diags) => collection.set(vscode.Uri.parse(uri), diags),
      delete: (uri) => collection.delete(vscode.Uri.parse(uri)),
    },
    (file) => vscode.Uri.file(file).toString(),
  );

  const docStateFor = (doc: vscode.TextDocument): DocDiagState => {
    const id = doc.uri.toString();
    let state = docStates.get(id);
    if (!state) {
      state = { owner: id, file: canonicalPath(doc.uri.fsPath), seq: 0 };
      docStates.set(id, state);
      router.register(id, state.file);
    }
    return state;
  };

  const schedule = (doc: vscode.TextDocument, reloadCatalog = false) => {
    if (doc.languageId !== 'weft') return;
    // A revision from source control (the left side of a diff) is not the
    // working tree: validating it squiggles a file nobody can fix, against
    // a catalog that has moved on since.
    if (doc.uri.scheme !== 'file') return;
    const debounce = vscode.workspace
      .getConfiguration('weft.validate')
      .get<number>('debounceMs', 500);
    const key = doc.uri.toString();
    const existing = timers.get(key);
    if (existing) clearTimeout(existing);
    timers.set(
      key,
      setTimeout(() => {
        void runValidation(doc, docStateFor(doc), parseServer, reloadCatalog, router);
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
      if (doc.uri.scheme !== 'file') return;
      // Drop this document's slice and its editor's panel entry; a file
      // another open document still reports on keeps that document's
      // findings, republished wherever they now belong. The keys come
      // from the SAME state the validations used, so the drop always
      // hits what was published.
      const state = docStates.get(doc.uri.toString());
      if (state) {
        router.close(state.owner, state.file);
        docStates.delete(doc.uri.toString());
      }
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
  doc: vscode.TextDocument,
  state: DocDiagState,
  parseServer: ParseServer,
  reloadCatalog: boolean,
  router: DiagnosticRouter<vscode.Diagnostic>,
): Promise<void> {
  const { owner, file } = state;
  const seq = ++state.seq;
  // A reply landing after the document closed, or after a NEWER run
  // started, must not publish: the close already dropped the slice, and
  // replies are not guaranteed to return in request order (a timed-out
  // request's rejection can arrive 30s late).
  const stale = () => state.seq !== seq || doc.isClosed;
  try {
    // Structural only: runtime rules (missing credentials and the like) are
    // "not ready to run", not code errors, and surface in the action bar's
    // pre-flight gate instead of squiggling the source.
    const result = await parseServer.request<{ diagnostics: WeftDiagnostic[] }>({
      kind: 'validate',
      source: doc.getText(),
      file: doc.uri.fsPath,
      reloadCatalog,
      mode: 'structural',
    });
    // A diagnostic carrying a `file` has its coordinates in an
    // @include's file; squiggling THIS buffer at those line numbers
    // would underline unrelated text. Bucket each finding under the
    // file it lives in (file-less = this document's file) and hand the
    // whole slice over; the router publishes the union across owners
    // per file, so an open include and every entry including it all
    // contribute without overwriting each other.
    if (stale()) return;
    const slice = new Map<string, vscode.Diagnostic[]>();
    for (const d of result.diagnostics) {
      const key = d.file ? canonicalPath(d.file) : file;
      const bucket = slice.get(key);
      if (bucket) bucket.push(toVsCodeDiagnostic(d));
      else slice.set(key, [toVsCodeDiagnostic(d)]);
    }
    router.publish(owner, slice);
  } catch (err) {
    if (stale()) return;
    // CLI failed (not found, project error)? Surface a single warning
    // so the user isn't confused by silent staleness. The failed run's
    // whole slice is replaced, so no stale include squiggles outlive it.
    const msg = err instanceof Error ? err.message : String(err);
    const warning = new vscode.Diagnostic(
      new vscode.Range(0, 0, 0, 0),
      `weft validate failed: ${msg}`,
      vscode.DiagnosticSeverity.Warning,
    );
    router.publish(owner, new Map([[file, [warning]]]));
  }
}

function toVsCodeDiagnostic(d: WeftDiagnostic): vscode.Diagnostic {
  // The diagnostic carries the culprit's full range
  // [line:column, endLine:endColumn); underline exactly that. A degenerate
  // point span (end == start, e.g. a project-level diagnostic with no specific
  // location) falls back to a 1-char caret so it's still visible.
  const start = weftPositionToVsCode(d.line, d.column);
  // `endLine: 0` is the wire's "the producer only knew a point" (lines
  // are 1-based, so 0 is never a real end): treat it as the start, not
  // as a line before it (which would build an inverted range).
  const end =
    d.endLine === 0
      ? start
      : weftPositionToVsCode(d.endLine, d.endColumn);
  const pointSpan = end.line === start.line && end.character <= start.character;
  const range = pointSpan
    ? new vscode.Range(start, start.translate(0, 1))
    : new vscode.Range(start, end);
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
