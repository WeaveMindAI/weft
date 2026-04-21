// Surgical text-edit helpers. Each helper takes a TextDocument plus
// a description of the graph mutation and returns a list of
// vscode.TextEdit operations that implement it. The host applies
// them via TextEditor.edit() / WorkspaceEdit.
//
// Live vscode.Range tracking is implicit: the caller captures a
// Range from the last /parse response (converted from Span), and
// VS Code auto-adjusts it across user keystrokes until the mutation
// fires. No version reconciliation needed for the single-user case.
//
// Ported from v1's weft-editor.ts surgical-edit pattern.

import * as vscode from 'vscode';
import type {
  Edge as WeftEdge,
  GraphMutation,
  NodeDefinition,
  ProjectDefinition,
  Span,
} from './shared/protocol';

export function spanToRange(span: Span): vscode.Range {
  // Span uses 1-indexed lines; VS Code wants 0-indexed.
  const startLine = Math.max(0, span.start_line - 1);
  const endLine = Math.max(0, span.end_line - 1);
  return new vscode.Range(
    startLine,
    span.start_col,
    endLine,
    span.end_col,
  );
}

export interface SurgicalContext {
  project: ProjectDefinition;
  doc: vscode.TextDocument;
}

export function buildEdit(
  mutation: GraphMutation,
  ctx: SurgicalContext,
): vscode.WorkspaceEdit | null {
  switch (mutation.kind) {
    case 'addNode':
      return buildAddNode(mutation.id, mutation.nodeType, ctx);
    case 'removeNode':
      return buildRemoveNode(mutation.id, ctx);
    case 'addEdge':
      return buildAddEdge(
        mutation.source,
        mutation.sourcePort,
        mutation.target,
        mutation.targetPort,
        ctx,
      );
    case 'removeEdge':
      return buildRemoveEdge(mutation.edgeId, ctx);
    case 'updateConfig':
      return buildUpdateConfig(
        mutation.nodeId,
        mutation.key,
        mutation.value,
        ctx,
      );
  }
}

// ─── addNode ────────────────────────────────────────────────────────────────

function buildAddNode(
  id: string,
  nodeType: string,
  { doc }: SurgicalContext,
): vscode.WorkspaceEdit {
  // Append to end of file. Single blank line before so the new node
  // doesn't fuse onto the last one.
  const lastLine = doc.lineCount - 1;
  const end = doc.lineAt(lastLine).range.end;
  const edit = new vscode.WorkspaceEdit();
  const needsNewline = doc.lineAt(lastLine).text.length > 0;
  const prefix = needsNewline ? '\n\n' : '\n';
  edit.insert(doc.uri, end, `${prefix}${id} = ${nodeType}\n`);
  return edit;
}

// ─── removeNode ─────────────────────────────────────────────────────────────

function buildRemoveNode(
  id: string,
  { project, doc }: SurgicalContext,
): vscode.WorkspaceEdit | null {
  const node = findNode(project, id);
  if (!node || !node.span) return null;
  const edit = new vscode.WorkspaceEdit();

  // Delete the full node span (declaration + config block).
  const nodeRange = spanToRange(node.span);
  // Extend through the trailing newline so we don't leave a blank
  // line. Pointer must land at start of the next line.
  const trailingEnd = new vscode.Position(Math.min(nodeRange.end.line + 1, doc.lineCount - 1), 0);
  edit.delete(doc.uri, new vscode.Range(nodeRange.start, trailingEnd));

  // Cascade: any edges referencing the removed node are now
  // orphans. Delete their lines too.
  for (const e of project.edges) {
    if ((e.source === id || e.target === id) && e.span) {
      const r = spanToRange(e.span);
      const trailEnd = new vscode.Position(
        Math.min(r.end.line + 1, doc.lineCount - 1),
        0,
      );
      edit.delete(doc.uri, new vscode.Range(r.start, trailEnd));
    }
  }

  return edit;
}

// ─── addEdge ────────────────────────────────────────────────────────────────

function buildAddEdge(
  source: string,
  sourcePort: string,
  target: string,
  targetPort: string,
  { project, doc }: SurgicalContext,
): vscode.WorkspaceEdit {
  const edit = new vscode.WorkspaceEdit();

  // Remove any existing edge that already drives the same target
  // input (the 1:1 rule: one input has one driver). The webview
  // usually sends a removeEdge first, but we enforce here too for
  // AI-driven edits.
  for (const existing of project.edges) {
    if (
      existing.target === target &&
      existing.targetHandle === targetPort &&
      existing.span
    ) {
      const r = spanToRange(existing.span);
      const trailEnd = new vscode.Position(
        Math.min(r.end.line + 1, doc.lineCount - 1),
        0,
      );
      edit.delete(doc.uri, new vscode.Range(r.start, trailEnd));
    }
  }

  // Append the new connection at end of file.
  const lastLine = doc.lineCount - 1;
  const end = doc.lineAt(lastLine).range.end;
  const needsNewline = doc.lineAt(lastLine).text.length > 0;
  const prefix = needsNewline ? '\n' : '';
  edit.insert(doc.uri, end, `${prefix}${target}.${targetPort} = ${source}.${sourcePort}\n`);
  return edit;
}

// ─── removeEdge ─────────────────────────────────────────────────────────────

function buildRemoveEdge(
  edgeId: string,
  { project, doc }: SurgicalContext,
): vscode.WorkspaceEdit | null {
  const edge = project.edges.find((e) => e.id === edgeId);
  if (!edge || !edge.span) return null;
  const edit = new vscode.WorkspaceEdit();
  const r = spanToRange(edge.span);
  const trailEnd = new vscode.Position(
    Math.min(r.end.line + 1, doc.lineCount - 1),
    0,
  );
  edit.delete(doc.uri, new vscode.Range(r.start, trailEnd));
  return edit;
}

// ─── updateConfig ───────────────────────────────────────────────────────────
//
// We don't have fine-grained per-field spans yet (just the node's
// header_span). So updateConfig takes the coarse path: re-render
// the whole node's config block. Fine-grained per-field edits land
// once config_spans is populated by the parser.

function buildUpdateConfig(
  nodeId: string,
  key: string,
  value: unknown,
  { project, doc }: SurgicalContext,
): vscode.WorkspaceEdit | null {
  const node = findNode(project, nodeId);
  if (!node || !node.span) return null;
  const edit = new vscode.WorkspaceEdit();

  const mergedConfig = { ...(node.config ?? {}), [key]: value };
  const rendered = renderNode(node, mergedConfig);

  const nodeRange = spanToRange(node.span);
  const trailingEnd = new vscode.Position(Math.min(nodeRange.end.line + 1, doc.lineCount - 1), 0);
  edit.replace(doc.uri, new vscode.Range(nodeRange.start, trailingEnd), rendered + '\n');
  return edit;
}

function findNode(project: ProjectDefinition, id: string): NodeDefinition | null {
  return project.nodes.find((n) => n.id === id) ?? null;
}

function renderNode(node: NodeDefinition, config: Record<string, unknown>): string {
  const header = `${node.id} = ${node.nodeType}`;
  const entries = Object.entries(config);
  if (entries.length === 0) return header;
  if (entries.length === 1) {
    const [k, v] = entries[0]!;
    return `${header} { ${k}: ${renderValue(v)} }`;
  }
  const body = entries.map(([k, v]) => `  ${k}: ${renderValue(v)}`).join('\n');
  return `${header} {\n${body}\n}`;
}

function renderValue(v: unknown): string {
  if (typeof v === 'string') return JSON.stringify(v);
  if (typeof v === 'number' || typeof v === 'boolean') return String(v);
  if (v === null) return 'null';
  // Objects, arrays: JSON.
  return JSON.stringify(v);
}

// Silence unused-import for WeftEdge in case we add edge-specific
// surgery later; currently it isn't needed but the shared types
// keep the type in scope.
export type { WeftEdge };
