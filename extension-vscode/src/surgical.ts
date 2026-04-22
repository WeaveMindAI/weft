// Surgical text-edit helpers. Each helper takes a TextDocument plus
// a description of the graph mutation and returns a vscode.WorkspaceEdit.
//
// The responsibility boundary mirrors v1's weft-editor.ts: the
// dispatcher will eventually carry these edits, but for phase A the
// extension host runs them locally so the webview can iterate fast.

import * as vscode from 'vscode';
import type {
  GraphMutation,
  NodeDefinition,
  PortDefinition,
  ProjectDefinition,
  Span,
} from './shared/protocol';

export function spanToRange(span: Span): vscode.Range {
  const startLine = Math.max(0, span.start_line - 1);
  const endLine = Math.max(0, span.end_line - 1);
  return new vscode.Range(startLine, span.start_col, endLine, span.end_col);
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
      return buildAddNode(mutation.id, mutation.nodeType, mutation.parentGroupLabel ?? null, ctx);
    case 'removeNode':
      return buildRemoveNode(mutation.id, ctx);
    case 'addEdge':
      return buildAddEdge(
        mutation.source,
        mutation.sourcePort,
        mutation.target,
        mutation.targetPort,
        mutation.scopeGroupLabel ?? null,
        ctx,
      );
    case 'removeEdge':
      return buildRemoveEdge(
        mutation.source,
        mutation.sourcePort,
        mutation.target,
        mutation.targetPort,
        ctx,
      );
    case 'updateConfig':
      return buildUpdateConfig(mutation.nodeId, mutation.key, mutation.value, ctx);
    case 'updateLabel':
      return buildUpdateLabel(mutation.nodeId, mutation.label, ctx);
    case 'duplicateNode':
      return buildDuplicateNode(mutation.nodeId, ctx);
    case 'addGroup':
      return buildAddGroup(mutation.label, mutation.parentGroupLabel ?? null, ctx);
    case 'removeGroup':
      return buildRemoveGroup(mutation.label, ctx);
    case 'renameGroup':
      return buildRenameGroup(mutation.oldLabel, mutation.newLabel, ctx);
    case 'updateGroupPorts':
      return buildUpdateGroupPorts(mutation.groupLabel, mutation.inputs, mutation.outputs, ctx);
    case 'updateNodePorts':
      return buildUpdateNodePorts(mutation.nodeId, mutation.inputs, mutation.outputs, ctx);
    case 'moveNodeScope':
      return buildMoveNodeScope(mutation.nodeId, mutation.targetGroupLabel, ctx);
    case 'moveGroupScope':
      return buildMoveGroupScope(mutation.groupLabel, mutation.targetGroupLabel, ctx);
    case 'updateProjectMeta':
      return buildUpdateProjectMeta(
        mutation.name ?? undefined,
        mutation.description ?? undefined,
        ctx,
      );
  }
}

// ─── addNode ────────────────────────────────────────────────────────────────

function buildAddNode(
  id: string,
  nodeType: string,
  parentGroupLabel: string | null,
  { project, doc }: SurgicalContext,
): vscode.WorkspaceEdit {
  const edit = new vscode.WorkspaceEdit();
  const fresh = `${id} = ${nodeType}`;
  if (!parentGroupLabel) {
    appendAtEof(edit, doc, `${fresh}\n`);
    return edit;
  }
  const group = project.groups.find(
    (g) => g.id === parentGroupLabel || g.label === parentGroupLabel,
  );
  if (!group || !group.span) {
    appendAtEof(edit, doc, `${fresh}\n`);
    return edit;
  }
  insertBeforeClose(edit, doc, group.span, `  ${fresh}\n`);
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
  deleteSpan(edit, doc, node.span);
  for (const e of project.edges) {
    if ((e.source === id || e.target === id) && e.span) {
      deleteSpan(edit, doc, e.span);
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
  scopeGroupLabel: string | null,
  { project, doc }: SurgicalContext,
): vscode.WorkspaceEdit {
  const edit = new vscode.WorkspaceEdit();
  for (const existing of project.edges) {
    if (
      existing.target === target &&
      existing.targetHandle === targetPort &&
      existing.span
    ) {
      deleteSpan(edit, doc, existing.span);
    }
  }
  const line = `${target}.${targetPort} = ${source}.${sourcePort}`;
  if (!scopeGroupLabel) {
    appendAtEof(edit, doc, `${line}\n`);
    return edit;
  }
  const group = project.groups.find(
    (g) => g.id === scopeGroupLabel || g.label === scopeGroupLabel,
  );
  if (!group || !group.span) {
    appendAtEof(edit, doc, `${line}\n`);
    return edit;
  }
  insertBeforeClose(edit, doc, group.span, `  ${line}\n`);
  return edit;
}

// ─── removeEdge ─────────────────────────────────────────────────────────────

function buildRemoveEdge(
  source: string,
  sourcePort: string,
  target: string,
  targetPort: string,
  { project, doc }: SurgicalContext,
): vscode.WorkspaceEdit | null {
  // Webview always sends the raw project-edge form (passthrough ids
  // on group boundaries). Match directly.
  const edge = project.edges.find(
    (e) =>
      e.source === source &&
      (e.sourceHandle ?? '') === sourcePort &&
      e.target === target &&
      (e.targetHandle ?? '') === targetPort,
  );
  if (!edge || !edge.span) return null;
  const edit = new vscode.WorkspaceEdit();
  deleteSpan(edit, doc, edge.span);
  return edit;
}

// ─── updateConfig ───────────────────────────────────────────────────────────

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
  const rendered = renderNode(node, mergedConfig, node.label);
  replaceSpan(edit, doc, node.span, rendered + '\n');
  return edit;
}

function buildUpdateLabel(
  nodeId: string,
  label: string | null,
  { project, doc }: SurgicalContext,
): vscode.WorkspaceEdit | null {
  const node = findNode(project, nodeId);
  if (!node || !node.span) return null;
  const edit = new vscode.WorkspaceEdit();
  const rendered = renderNode(node, node.config ?? {}, label);
  replaceSpan(edit, doc, node.span, rendered + '\n');
  return edit;
}

// ─── duplicate ─────────────────────────────────────────────────────────────

function buildDuplicateNode(
  nodeId: string,
  { project, doc }: SurgicalContext,
): vscode.WorkspaceEdit | null {
  const node = findNode(project, nodeId);
  if (!node) return null;
  const usedIds = new Set(project.nodes.map((n) => n.id));
  let copyId = `${node.id}_copy`;
  let i = 2;
  while (usedIds.has(copyId)) {
    copyId = `${node.id}_copy${i}`;
    i++;
  }
  const dupNode: NodeDefinition = { ...node, id: copyId };
  const rendered = renderNode(dupNode, node.config ?? {}, node.label);
  const edit = new vscode.WorkspaceEdit();
  appendAtEof(edit, doc, `\n${rendered}\n`);
  return edit;
}

// ─── groups ────────────────────────────────────────────────────────────────

function buildAddGroup(
  label: string,
  parentGroupLabel: string | null,
  { project, doc }: SurgicalContext,
): vscode.WorkspaceEdit {
  const edit = new vscode.WorkspaceEdit();
  const block = `${label} = Group {\n}`;
  if (!parentGroupLabel) {
    appendAtEof(edit, doc, `\n${block}\n`);
    return edit;
  }
  const parent = project.groups.find(
    (g) => g.id === parentGroupLabel || g.label === parentGroupLabel,
  );
  if (!parent || !parent.span) {
    appendAtEof(edit, doc, `\n${block}\n`);
    return edit;
  }
  const indented = block.split('\n').map((ln) => '  ' + ln).join('\n');
  insertBeforeClose(edit, doc, parent.span, `${indented}\n`);
  return edit;
}

function buildRemoveGroup(
  label: string,
  { project, doc }: SurgicalContext,
): vscode.WorkspaceEdit | null {
  const group = project.groups.find((g) => g.id === label || g.label === label);
  if (!group || !group.span) return null;
  const edit = new vscode.WorkspaceEdit();
  deleteSpan(edit, doc, group.span);
  for (const e of project.edges) {
    if ((e.source === group.id || e.target === group.id) && e.span) {
      deleteSpan(edit, doc, e.span);
    }
  }
  return edit;
}

function buildRenameGroup(
  oldLabel: string,
  newLabel: string,
  { project, doc }: SurgicalContext,
): vscode.WorkspaceEdit {
  // v1 does a regex-global replace. Without per-field spans we can
  // do the same: walk line-by-line, apply a `\b${old}` replace on
  // every non-comment line, and rewrite the header explicitly.
  const edit = new vscode.WorkspaceEdit();
  const text = doc.getText();
  const lines = text.split('\n');
  const escaped = oldLabel.replace(/[.*+?^${}()|[\]\\]/g, '\\$&');
  const headerRe = new RegExp(`^(\\s*)${escaped}(\\s*=\\s*Group)`);
  const refRe = new RegExp(`\\b${escaped}(\\.|\\b)`, 'g');
  const next = lines.map((ln) => {
    if (headerRe.test(ln)) return ln.replace(headerRe, `$1${newLabel}$2`);
    if (ln.trim().startsWith('#')) return ln;
    return ln.replace(refRe, `${newLabel}$1`);
  });
  const full = new vscode.Range(0, 0, doc.lineCount, 0);
  edit.replace(doc.uri, full, next.join('\n'));
  void project; // referenced only for API symmetry
  return edit;
}

function buildUpdateGroupPorts(
  label: string,
  inputs: PortDefinition[],
  outputs: PortDefinition[],
  { project, doc }: SurgicalContext,
): vscode.WorkspaceEdit | null {
  const group = project.groups.find((g) => g.id === label || g.label === label);
  if (!group || !group.span) return null;
  const edit = new vscode.WorkspaceEdit();
  const range = spanToRange(group.span);
  const existingText = doc.getText(range);
  const rewritten = rewriteGroupHeaderPorts(existingText, label, inputs, outputs);
  edit.replace(doc.uri, range, rewritten);
  return edit;
}

function buildUpdateNodePorts(
  nodeId: string,
  inputs: PortDefinition[],
  outputs: PortDefinition[],
  { project, doc }: SurgicalContext,
): vscode.WorkspaceEdit | null {
  const node = findNode(project, nodeId);
  if (!node || !node.span) return null;
  const edit = new vscode.WorkspaceEdit();
  // Whole-node rewrite: render with full port signature (catalog
  // defaults stripped so only overrides remain).
  const sig = buildSignature(inputs, outputs);
  const header = `${node.id} = ${node.nodeType}${sig}`;
  const body = renderConfigBody(node.config ?? {}, node.label);
  const rendered = body ? `${header} {\n${body}\n}` : header;
  replaceSpan(edit, doc, node.span, rendered + '\n');
  return edit;
}

function buildMoveNodeScope(
  nodeId: string,
  targetGroupLabel: string | null,
  { project, doc }: SurgicalContext,
): vscode.WorkspaceEdit | null {
  const node = findNode(project, nodeId);
  if (!node || !node.span) return null;
  // Conservative guard: any edge touching this node blocks the move
  // (v1 rule).
  const hasEdge = project.edges.some(
    (e) => e.source === node.id || e.target === node.id,
  );
  if (hasEdge) return null;
  const edit = new vscode.WorkspaceEdit();
  const text = doc.getText(spanToRange(node.span));
  deleteSpan(edit, doc, node.span);
  if (targetGroupLabel) {
    const group = project.groups.find(
      (g) => g.id === targetGroupLabel || g.label === targetGroupLabel,
    );
    if (group && group.span) {
      const indented = text
        .split('\n')
        .map((ln) => (ln ? '  ' + ln : ln))
        .join('\n');
      insertBeforeClose(edit, doc, group.span, indented + '\n');
      return edit;
    }
  }
  appendAtEof(edit, doc, text + '\n');
  return edit;
}

function buildMoveGroupScope(
  label: string,
  targetGroupLabel: string | null,
  { project, doc }: SurgicalContext,
): vscode.WorkspaceEdit | null {
  const group = project.groups.find((g) => g.id === label || g.label === label);
  if (!group || !group.span) return null;
  // Guard: any edge crossing the group boundary blocks.
  const hasBoundaryEdge = project.edges.some(
    (e) => e.source === group.id || e.target === group.id,
  );
  if (hasBoundaryEdge) return null;
  const edit = new vscode.WorkspaceEdit();
  const text = doc.getText(spanToRange(group.span));
  deleteSpan(edit, doc, group.span);
  if (targetGroupLabel) {
    const parent = project.groups.find(
      (g) => g.id === targetGroupLabel || g.label === targetGroupLabel,
    );
    if (parent && parent.span) {
      const indented = text
        .split('\n')
        .map((ln) => (ln ? '  ' + ln : ln))
        .join('\n');
      insertBeforeClose(edit, doc, parent.span, indented + '\n');
      return edit;
    }
  }
  appendAtEof(edit, doc, text + '\n');
  return edit;
}

function buildUpdateProjectMeta(
  name: string | undefined,
  description: string | undefined,
  { doc }: SurgicalContext,
): vscode.WorkspaceEdit {
  const edit = new vscode.WorkspaceEdit();
  const text = doc.getText();
  const lines = text.split('\n');
  // Locate the header comment block (consecutive leading `#` lines).
  let headerEnd = 0;
  while (headerEnd < lines.length && lines[headerEnd].startsWith('#')) {
    headerEnd++;
  }
  const headerLines = lines.slice(0, headerEnd);
  const rest = lines.slice(headerEnd);
  const nameIdx = headerLines.findIndex((ln) => /^#\s*Name:/.test(ln));
  const descIdx = headerLines.findIndex((ln) => /^#\s*Description:/.test(ln));

  const next = [...headerLines];
  if (name !== undefined) {
    const nextLine = `# Name: ${name}`;
    if (nameIdx >= 0) next[nameIdx] = nextLine;
    else next.unshift(nextLine);
  }
  if (description !== undefined) {
    const nextLine = `# Description: ${description}`;
    if (descIdx >= 0) next[descIdx] = nextLine;
    else next.push(nextLine);
  }
  const full = new vscode.Range(0, 0, doc.lineCount, 0);
  edit.replace(doc.uri, full, [...next, ...rest].join('\n'));
  return edit;
}

// ─── helpers ───────────────────────────────────────────────────────────────

function findNode(project: ProjectDefinition, id: string): NodeDefinition | null {
  return project.nodes.find((n) => n.id === id) ?? null;
}

function renderNode(
  node: NodeDefinition,
  config: Record<string, unknown>,
  label?: string | null,
): string {
  const header = `${node.id} = ${node.nodeType}`;
  const body = renderConfigBody(config, label ?? node.label ?? null);
  if (!body) return header;
  if (!body.includes('\n')) return `${header} { ${body.trim()} }`;
  return `${header} {\n${body}\n}`;
}

function renderConfigBody(
  config: Record<string, unknown>,
  label: string | null,
): string {
  const entries: [string, unknown][] = [];
  if (label) entries.push(['label', label]);
  for (const [k, v] of Object.entries(config)) {
    if (k === 'label') continue;
    if (k === 'parentId' || k === 'textareaHeights' || k === '_opaqueChildren') continue;
    if (k === 'width' || k === 'height' || k === 'expanded') continue;
    entries.push([k, v]);
  }
  if (entries.length === 0) return '';
  return entries.map(([k, v]) => `  ${k}: ${renderValue(v)}`).join('\n');
}

function renderValue(v: unknown): string {
  if (typeof v === 'string') {
    return v.includes('\n') ? '```\n' + v + '\n```' : JSON.stringify(v);
  }
  if (typeof v === 'number' || typeof v === 'boolean') return String(v);
  if (v === null || v === undefined) return 'null';
  return JSON.stringify(v);
}

function buildSignature(
  inputs: PortDefinition[],
  outputs: PortDefinition[],
): string {
  const fmt = (p: PortDefinition) => {
    const base = p.portType ? `${p.name}: ${p.portType}` : p.name;
    return p.required ? base : `${base}?`;
  };
  const inStr = inputs.length ? `(${inputs.map(fmt).join(', ')})` : '';
  const outStr = outputs.length ? ` -> (${outputs.map(fmt).join(', ')})` : '';
  return inStr + outStr;
}

function rewriteGroupHeaderPorts(
  existingBlock: string,
  label: string,
  inputs: PortDefinition[],
  outputs: PortDefinition[],
): string {
  const lines = existingBlock.split('\n');
  if (!lines.length) return existingBlock;
  const header = lines[0];
  const sig = buildSignature(inputs, outputs);
  // Replace anything between the label and the opening `{` with the
  // fresh signature.
  const m = header.match(/^(\s*)(\S+)\s*=\s*Group[^{]*(\{.*)?$/);
  if (!m) return existingBlock;
  const [, indent, _orig, tail] = m;
  const closing = tail && tail.startsWith('{') ? tail : '{';
  lines[0] = `${indent}${label} = Group${sig} ${closing}`;
  return lines.join('\n');
}

function appendAtEof(
  edit: vscode.WorkspaceEdit,
  doc: vscode.TextDocument,
  text: string,
): void {
  const lastLine = doc.lineCount - 1;
  const end = doc.lineAt(lastLine).range.end;
  const needsNewline = doc.lineAt(lastLine).text.length > 0;
  const prefix = needsNewline ? '\n' : '';
  edit.insert(doc.uri, end, prefix + text);
}

function insertBeforeClose(
  edit: vscode.WorkspaceEdit,
  doc: vscode.TextDocument,
  span: Span,
  text: string,
): void {
  // Group/node spans end AFTER the closing `}`. Walk backwards to
  // find the line containing it.
  const endLine = Math.max(0, span.end_line - 1);
  for (let i = endLine; i >= 0; i--) {
    const raw = doc.lineAt(i).text;
    if (raw.trimEnd().endsWith('}')) {
      const pos = new vscode.Position(i, 0);
      edit.insert(doc.uri, pos, text);
      return;
    }
  }
  // Fallback: insert at span end.
  edit.insert(doc.uri, new vscode.Position(endLine, 0), text);
}

function deleteSpan(
  edit: vscode.WorkspaceEdit,
  doc: vscode.TextDocument,
  span: Span,
): void {
  const r = spanToRange(span);
  const trailEnd = new vscode.Position(
    Math.min(r.end.line + 1, doc.lineCount - 1),
    0,
  );
  edit.delete(doc.uri, new vscode.Range(r.start, trailEnd));
}

function replaceSpan(
  edit: vscode.WorkspaceEdit,
  doc: vscode.TextDocument,
  span: Span,
  text: string,
): void {
  const r = spanToRange(span);
  const trailEnd = new vscode.Position(
    Math.min(r.end.line + 1, doc.lineCount - 1),
    0,
  );
  edit.replace(doc.uri, new vscode.Range(r.start, trailEnd), text);
}
