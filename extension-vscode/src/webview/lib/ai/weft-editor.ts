/**
 * Surgical code editor for Weft DSL.
 * Each function takes weftCode + change description, returns new weftCode.
 * Uses the parser for structural lookups, then performs targeted string surgery.
 */

import { parseRawWeft, type ParsedNode, type ParsedGroup, type ParsedConnection, type ParseResult } from './weft-parser';

// ── Helpers ──────────────────────────────────────────────────────────────────

interface NodeLocation {
	startLine: number; // 0-based index into lines array
	endLine: number;   // inclusive
	isOneLiner: boolean;
	indent: string;    // indentation of the node definition line
}

interface GroupLocation {
	startLine: number;
	endLine: number;     // line with closing }
	contentStart: number; // first line inside the group (after opening {)
	indent: string;
	innerIndent: string;
	isOneLiner: boolean;
}

/** Parse weft code and return the raw parse result for structural lookups. */
function parseForEdit(code: string): { result: ParseResult; nodes: ParsedNode[]; groups: ParsedGroup[]; connections: ParsedConnection[] } {
	const { result } = parseRawWeft(code);
	return {
		result,
		nodes: result.nodes,
		groups: result.groups,
		connections: result.connections,
	};
}

/** Derive a NodeLocation from a ParsedNode and the lines array.
 *  Parser line numbers are 1-based, editor uses 0-based. */
function nodeToLocation(lines: string[], node: ParsedNode): NodeLocation {
	const startLine = node.startLine - 1;
	const endLine = node.endLine - 1;
	const indent = lines[startLine]?.match(/^(\s*)/)?.[1] ?? '';
	const isOneLiner = startLine === endLine;
	return { startLine, endLine, isOneLiner, indent };
}

/** Derive a GroupLocation from a ParsedGroup and the lines array.
 *  Parser line numbers are 1-based, editor uses 0-based. */
function groupToLocation(lines: string[], group: ParsedGroup): GroupLocation {
	const startLine = group.startLine - 1;
	const endLine = group.endLine - 1;
	const indent = lines[startLine]?.match(/^(\s*)/)?.[1] ?? '';
	const innerIndent = indent + '  ';
	const isOneLiner = startLine === endLine;

	// Find contentStart: line after the opening {
	let contentStart = startLine + 1;
	if (!isOneLiner) {
		for (let i = startLine; i <= endLine; i++) {
			if (lines[i].includes('{')) {
				contentStart = i + 1;
				break;
			}
		}
	}

	return { startLine, endLine, contentStart, indent, innerIndent, isOneLiner };
}

/** Find a node by ID (possibly scoped like "grp.worker") using the parser. */
function findNode(lines: string[], nodeId: string, scopeGroupId?: string): NodeLocation | null {
	const code = lines.join('\n');
	const parsed = parseForEdit(code);

	// The parser prefixes nested IDs: "grp.worker" for worker inside grp.
	// The editor may pass either scoped ("grp.worker") or local ("worker") + scopeGroupId.
	let searchId = nodeId;
	if (scopeGroupId && !nodeId.includes('.')) {
		searchId = `${scopeGroupId}.${nodeId}`;
	}

	// Search in nodes
	const found = parsed.nodes.find(n => n.id === searchId);
	if (found) return nodeToLocation(lines, found);

	// Search in group-internal nodes
	for (const group of parsed.groups) {
		const inner = group.nodes.find(n => n.id === searchId || n.id.endsWith('.' + nodeId));
		if (inner) return nodeToLocation(lines, inner);
	}

	// Fallback: search by local ID only
	if (nodeId.includes('.')) {
		const localId = nodeId.split('.').pop()!;
		const fallback = parsed.nodes.find(n => n.id.endsWith('.' + localId) || n.id === localId);
		if (fallback) return nodeToLocation(lines, fallback);
		for (const group of parsed.groups) {
			const inner = group.nodes.find(n => n.id.endsWith('.' + localId) || n.id === localId);
			if (inner) return nodeToLocation(lines, inner);
		}
	}

	return null;
}

/** Find a group by name using the parser. */
function findGroup(lines: string[], groupName: string): GroupLocation | null {
	const code = lines.join('\n');
	const parsed = parseForEdit(code);

	const found = parsed.groups.find(g =>
		g.id === groupName ||
		g.originalName === groupName ||
		g.id.endsWith('.' + groupName),
	);
	if (found) return groupToLocation(lines, found);
	return null;
}

/** Find a connection line using the parser. */
function findConnection(lines: string[], srcId: string, srcPort: string, tgtId: string, tgtPort: string): number | null {
	const code = lines.join('\n');
	const parsed = parseForEdit(code);

	// Check all connections including inside groups
	const allConnections = [
		...parsed.connections,
		...parsed.groups.flatMap(g => g.connections),
	];

	for (const conn of allConnections) {
		// Direct match
		if (conn.sourceId === srcId && conn.sourcePort === srcPort &&
			conn.targetId === tgtId && conn.targetPort === tgtPort) {
			return conn.line - 1; // Parser is 1-based, editor is 0-based
		}
		// Match without scope prefixes (connections use local IDs)
		const connSrcLocal = conn.sourceId.split('.').pop()!;
		const connTgtLocal = conn.targetId.split('.').pop()!;
		if (connSrcLocal === srcId && conn.sourcePort === srcPort &&
			connTgtLocal === tgtId && conn.targetPort === tgtPort) {
			return conn.line - 1;
		}
		// Match self-connections: caller passes "self" but parser stores the group ID
		const srcMatch = srcId === 'self' ? conn.sourceIsSelf && conn.sourcePort === srcPort : false;
		const tgtMatch = tgtId === 'self' ? conn.targetIsSelf && conn.targetPort === tgtPort : false;
		if ((srcMatch || conn.sourceId === srcId || connSrcLocal === srcId) &&
			conn.sourcePort === srcPort &&
			(tgtMatch || conn.targetId === tgtId || connTgtLocal === tgtId) &&
			conn.targetPort === tgtPort &&
			(srcMatch || tgtMatch)) {
			return conn.line - 1;
		}
	}
	return null;
}

/** Find a node by its potentially scoped ID (e.g. Group.Group_2.debug_1). */
function findNodeScoped(lines: string[], scopedId: string): NodeLocation | null {
	return findNode(lines, scopedId);
}

/** Expand a one-liner group to multi-line format. */
function expandOneLinerGroup(lines: string[], group: GroupLocation): void {
	if (!group.isOneLiner) return;
	const line = lines[group.startLine];
	const match = line.match(/^(\s*)(.+?)\s*\{(.*)\}\s*$/);
	if (!match) return;
	const [, indent, header, body] = match;
	const innerIndent = indent + '  ';
	const bodyContent = body.trim();
	const newLines = [`${indent}${header} {`];
	if (bodyContent) {
		newLines.push(`${innerIndent}${bodyContent}`);
	}
	newLines.push(`${indent}}`);
	lines.splice(group.startLine, group.endLine - group.startLine + 1, ...newLines);
}

function escapeRegex(s: string): string {
	return s.replace(/[.*+?^${}()|[\]\\]/g, '\\$&');
}

/** Remove all connection lines that reference a given identifier (as source or target). */
function removeConnectionsReferencing(lines: string[], identifier: string): string[] {
	const escaped = escapeRegex(identifier);
	return lines.filter(line => {
		const trimmed = line.trim();
		if (trimmed.startsWith('#') || trimmed.startsWith('//')) return true;
		// Only match connection lines: dotted.port = dotted.port
		const isConn = /^\w[\w.]*\.\w+\s*=\s*\w[\w.]*\.\w+$/.test(trimmed);
		if (!isConn) return true;
		const refPattern = new RegExp(`(?:^|\\s|=\\s*)${escaped}\\.\\w+|${escaped}\\.\\w+\\s*=`);
		return !refPattern.test(trimmed);
	});
}

/** Locate the inline body `{ ... }` span inside a single line of source.
 *  Handles three line shapes (all are one-liner anons):
 *
 *    normal        : `host = Type { body }`             (isAnon = false)
 *    connection    : `host.data = Type { body }.port`   (isAnon = true)
 *    config-block  : `  data: Type { body }.port`       (isAnon = true)
 *
 *  Returns { openCol, closeCol, body, prefix, suffix, isAnon } where `body` is
 *  the raw content between the braces (no trimming), `prefix` is everything
 *  before `{`, and `suffix` is everything after `}`. `isAnon` is true if the
 *  line is NOT the canonical `id = Type { body }` shape.
 *
 *  Returns null if the line has no recognizable inline body. Brace-counting
 *  respects double-quoted strings so `{ template: "has}" }` works. */
function extractInlineBodySpan(line: string): {
	openCol: number;
	closeCol: number;
	body: string;
	prefix: string;
	suffix: string;
	isAnon: boolean;
} | null {
	// Find the first `{` at depth 0 respecting quotes.
	let inQuote = false;
	let openCol = -1;
	for (let i = 0; i < line.length; i++) {
		const c = line[i];
		if (c === '"' && (i === 0 || line[i - 1] !== '\\')) inQuote = !inQuote;
		if (inQuote) continue;
		if (c === '{') { openCol = i; break; }
	}
	if (openCol < 0) return null;

	// Find the matching `}` at depth 0 from openCol.
	let depth = 1;
	inQuote = false;
	let closeCol = -1;
	for (let i = openCol + 1; i < line.length; i++) {
		const c = line[i];
		if (c === '"' && line[i - 1] !== '\\') inQuote = !inQuote;
		if (inQuote) continue;
		if (c === '{') depth++;
		else if (c === '}') { depth--; if (depth === 0) { closeCol = i; break; } }
	}
	if (closeCol < 0) return null;

	const prefix = line.slice(0, openCol);
	const body = line.slice(openCol + 1, closeCol);
	const suffix = line.slice(closeCol + 1);
	// Canonical declaration prefix looks like:
	//   `id = Type`
	//   `id = Type(...)`
	//   `id = Type(...) -> (...)`
	//   `id = Type -> (...)`
	// Anything else (e.g. `parent.field = Type(...) `, `field: Type(...) `) is
	// an inline anon.
	const canonicalPrefix = /^\s*\w+\s*=\s*\w+(\s*\([^)]*\))?(\s*->\s*\([^)]*\))?\s*$/;
	const isAnon = !canonicalPrefix.test(prefix);
	return { openCol, closeCol, body, prefix, suffix, isAnon };
}

/** Parse a one-liner body's comma-separated `key: value` pairs into an array.
 *  Respects double-quoted strings and nested `{ }` so commas inside them don't
 *  split pairs. Each returned pair is the raw text of the pair (no further
 *  trimming beyond edge whitespace). */
function splitBodyPairs(body: string): string[] {
	const pairs: string[] = [];
	let current = '';
	let inQuote = false;
	let braceDepth = 0;
	let bracketDepth = 0;
	for (let i = 0; i < body.length; i++) {
		const c = body[i];
		if (c === '"' && (i === 0 || body[i - 1] !== '\\')) inQuote = !inQuote;
		if (!inQuote) {
			if (c === '{') braceDepth++;
			else if (c === '}') braceDepth--;
			else if (c === '[') bracketDepth++;
			else if (c === ']') bracketDepth--;
			else if (c === ',' && braceDepth === 0 && bracketDepth === 0) {
				if (current.trim()) pairs.push(current.trim());
				current = '';
				continue;
			}
		}
		current += c;
	}
	if (current.trim()) pairs.push(current.trim());
	return pairs;
}

/** Format a config value for Weft syntax. */
function formatConfigValue(value: unknown): string {
	if (typeof value === 'string') {
		if (value.includes('\n')) {
			// Triple backtick for multiline, escape ``` inside content
			const escaped = value.replace(/```/g, '\\```');
			return `\`\`\`\n${escaped}\n\`\`\``;
		}
		return `"${value.replace(/\\/g, '\\\\').replace(/"/g, '\\"')}"`;
	}
	if (typeof value === 'boolean' || typeof value === 'number') {
		return String(value);
	}
	// Objects and arrays: emit pretty-printed multi-line JSON. The editor's
	// caller is responsible for placing the resulting lines at the correct
	// indent in the surrounding scope. Multi-line form parses everywhere
	// (canonical nodes, multi-line anons, one-liner anons after expansion);
	// compact JSON does not parse correctly inside one-liner anon bodies
	// because the body comma-splitter doesn't respect nested braces.
	return JSON.stringify(value, null, 2);
}

// ── Public API ───────────────────────────────────────────────────────────────

const RESERVED_CONFIG_KEYS = ['parentId', 'textareaHeights', 'width', 'height', 'expanded', 'description'];

/** Find a ParsedNode by id, tolerating both scoped (`grp.host__data`) and
 *  local (`host__data`) forms. Searches top-level nodes first, then inside
 *  each group. */
function findParsedNode(parsed: ParseResult, nodeId: string): ParsedNode | null {
	for (const n of parsed.nodes) {
		if (n.id === nodeId) return n;
		if (!nodeId.includes('.')) {
			const local = n.id.includes('.') ? n.id.split('.').pop() : n.id;
			if (local === nodeId) return n;
		}
	}
	for (const g of parsed.groups) {
		for (const n of g.nodes) {
			if (n.id === nodeId) return n;
			if (!nodeId.includes('.')) {
				const local = n.id.includes('.') ? n.id.split('.').pop() : n.id;
				if (local === nodeId) return n;
			}
		}
	}
	return null;
}

/** Update a config field on a node. Pass value=undefined or null to remove
 *  the field.
 *
 *  Algorithm:
 *   1. Parse the code.
 *   2. Find the target node.
 *   3. If the node is a one-liner (entire declaration on one source line) or
 *      a bare node (no body), expand it to multi-line format first and
 *      reparse. Expansion is a pure source transformation that preserves
 *      semantics but gives every field its own line, so field spans become
 *      cleanly separable from the declaration header.
 *   4. Find the field's span (parser-tracked). Replace it in place, or
 *      remove it, or insert a new field at the body insertion point.
 *
 *  This uniform flow handles every value-shape transition (scalar ↔ heredoc
 *  ↔ JSON ↔ list ↔ dict) without special-casing each pair. */
export function updateNodeConfig(code: string, nodeId: string, key: string, value: unknown): string {
	if (RESERVED_CONFIG_KEYS.includes(key)) return code;

	let parsed = parseForEdit(code);
	let node = findParsedNode(parsed.result, nodeId);
	if (!node) return code;

	// Step 1: if the node is a one-liner or bare (no body), expand it to
	// multi-line form so every field lives on its own line and can be
	// edited without touching the declaration header. The expansion is a
	// pure source rewrite; after it we reparse to pick up fresh spans.
	if (node.startLine === node.endLine) {
		const expanded = expandNodeToMultiLine(code, node);
		if (expanded !== code) {
			code = expanded;
			parsed = parseForEdit(code);
			const refreshed = findParsedNode(parsed.result, nodeId);
			if (!refreshed) return code;
			node = refreshed;
		}
	}

	const removing = value === undefined || value === null;
	const oldSpan = node.configSpans[key];
	const lines = code.split('\n');

	if (oldSpan) {
		const startIdx = oldSpan.startLine - 1;
		const countToRemove = oldSpan.endLine - oldSpan.startLine + 1;
		if (removing) {
			lines.splice(startIdx, countToRemove);
			return cleanBlankLines(lines).join('\n');
		}
		const formattedValue = formatConfigValue(value);
		const oldIndent = lines[startIdx].match(/^(\s*)/)?.[1] ?? '';
		let prefix = `${key}: `;
		if (oldSpan.origin === 'connection') {
			// Preserve the original connection-line prefix (e.g. `host.data = `)
			// instead of constructing from nodeId, which may be scoped
			// (`grp.host`) while the source line uses the local id (`host`).
			const eqMatch = lines[startIdx].match(/^(\s*\S+\s*=\s*)/);
			prefix = eqMatch ? eqMatch[1].trimStart() : `${nodeId}.${key} = `;
		}
		const newFieldLines = buildFieldLines(prefix, formattedValue, oldIndent);
		lines.splice(startIdx, countToRemove, ...newFieldLines);
		return cleanBlankLines(lines).join('\n');
	}

	// No existing span for this key. Removing is a noop. Otherwise insert
	// at the node's body insertion point (just before the closing brace).
	if (removing) return code;

	const formattedValue = formatConfigValue(value);
	const newLines = insertFieldInNode(lines, node, key, formattedValue);
	return cleanBlankLines(newLines).join('\n');
}

/** Expand a one-liner or bare node declaration into multi-line form. This is
 *  a pure source transformation: the parsed semantics are unchanged, but
 *  each config field ends up on its own line where the editor can safely
 *  splice it without touching the declaration header. Returns the new code,
 *  or the original if no expansion is possible/needed. */
function expandNodeToMultiLine(code: string, node: ParsedNode): string {
	const lines = code.split('\n');
	const lineIdx = node.startLine - 1;
	const line = lines[lineIdx];
	const lineIndent = line.match(/^(\s*)/)?.[1] ?? '';
	const bodyIndent = lineIndent + '  ';

	// Case A: one-liner with a brace body — `id = Type { body }` or
	// `host.data = Type { body }.port` or `  data: Type { body }.port`.
	const span = extractInlineBodySpan(line);
	if (span) {
		const pairs = splitBodyPairs(span.body);
		const out: string[] = [`${span.prefix.trimEnd()} {`];
		for (const pair of pairs) {
			out.push(`${bodyIndent}${pair}`);
		}
		out.push(`${lineIndent}}${span.suffix}`);
		lines.splice(lineIdx, 1, ...out);
		return lines.join('\n');
	}

	// Case B: bare inline anon — `host.data = Type.port` or `  data: Type.port`.
	const anonBareMatch = line.match(/^(.*?)(\b\w+)(\.\w+)\s*$/);
	if (anonBareMatch && /[:=]\s*$/.test(anonBareMatch[1])) {
		const [, beforeType, typeName, dotPort] = anonBareMatch;
		const out = [
			`${beforeType}${typeName} {`,
			`${lineIndent}}${dotPort}`,
		];
		lines.splice(lineIdx, 1, ...out);
		return lines.join('\n');
	}

	// Case C: bare canonical node — `id = Type` (optionally with signatures).
	const canonicalBareMatch = line.match(/^(\s*\w+\s*=\s*\w+(?:\s*\([^)]*\))?(?:\s*->\s*\([^)]*\))?)\s*$/);
	if (canonicalBareMatch) {
		const out = [
			`${canonicalBareMatch[1]} {`,
			`${lineIndent}}`,
		];
		lines.splice(lineIdx, 1, ...out);
		return lines.join('\n');
	}

	// Not a recognized shape: leave it alone.
	return code;
}

/** Build the source-line array for `<indent><prefix><formattedValue>`. The
 *  formatted value may be multi-line (heredoc or pretty-printed JSON/list).
 *  Heredoc continuation lines stay at column 0 (the parser's dedent
 *  mechanism strips common whitespace on read). JSON/list continuation
 *  lines are indented to match the surrounding body so the source stays
 *  readable and the closing `}` / `]` doesn't collide with the node's
 *  closing brace visually. */
function buildFieldLines(prefix: string, formattedValue: string, indent: string): string[] {
	const valueLines = formattedValue.split('\n');
	const out = [`${indent}${prefix}${valueLines[0]}`];
	// Heredoc values (``` ... ```) have content that should NOT be indented;
	// the parser dedents on read. Everything else (JSON objects, arrays)
	// should be indented to match the surrounding scope.
	const isHeredoc = valueLines[0].startsWith('```');
	for (let i = 1; i < valueLines.length; i++) {
		out.push(isHeredoc ? valueLines[i] : indent + valueLines[i]);
	}
	return out;
}

/** Insert a `key: formattedValue` field into `node`'s body, just before the
 *  closing `}` (or `}.port` for inline anons). The caller guarantees `node`
 *  is multi-line: `updateNodeConfig` expands one-liner and bare nodes via
 *  `expandNodeToMultiLine` before calling this. */
function insertFieldInNode(
	lines: string[],
	node: ParsedNode,
	key: string,
	formattedValue: string,
): string[] {
	const startIdx = node.startLine - 1;
	const endIdx = node.endLine - 1;
	const lineIndent = lines[startIdx].match(/^(\s*)/)?.[1] ?? '';
	const bodyIndent = lineIndent + '  ';
	const newFieldLines = buildFieldLines(`${key}: `, formattedValue, bodyIndent);
	lines.splice(endIdx, 0, ...newFieldLines);
	return lines;
}

/** Update or add a label on a node. The parser promotes the `label` field
 *  out of `config` and onto `node.label`, so the editor can delegate to the
 *  generic updateNodeConfig path and the round-trip just works. */
export function updateNodeLabel(code: string, nodeId: string, newLabel: string | null): string {
	// Empty string and null both mean "remove the label"; updateNodeConfig
	// interprets null/undefined as removal.
	return updateNodeConfig(code, nodeId, 'label', newLabel === '' ? null : newLabel);
}

/** Add a new node at the end of the relevant scope. */
export function addNode(
	code: string,
	nodeType: string,
	nodeId: string,
	parentGroupId?: string,
): string {
	const lines = code.split('\n');
	const snippet = `${nodeId} = ${nodeType} {}`;

	if (parentGroupId) {
		const group = findGroup(lines, parentGroupId);
		if (group) {
			// Insert before the closing brace of the group, with proper indent
			const indented = group.innerIndent + snippet;
			lines.splice(group.endLine, 0, '', indented);
			return lines.join('\n');
		}
	}

	// Top-level: append at the end
	// Find last non-empty line
	let insertAt = lines.length;
	while (insertAt > 0 && lines[insertAt - 1].trim() === '') insertAt--;
	lines.splice(insertAt, 0, '', snippet);

	return lines.join('\n');
}

/** Add a new group scope block. Groups use `"Label" { }` syntax, not `id = Type { }`. */
export function addGroup(
	code: string,
	label: string,
	parentGroupId?: string,
): string {
	const lines = code.split('\n');
	const snippet = `${label} = Group() -> () {}`;

	if (parentGroupId) {
		const group = findGroup(lines, parentGroupId);
		if (group) {
			if (group.isOneLiner) {
				expandOneLinerGroup(lines, group);
				const expanded = findGroup(lines, parentGroupId);
				if (expanded) {
					lines.splice(expanded.endLine, 0, '', expanded.innerIndent + snippet);
					return lines.join('\n');
				}
			} else {
				lines.splice(group.endLine, 0, '', group.innerIndent + snippet);
			}
			return lines.join('\n');
		}
	}

	let insertAt = lines.length;
	while (insertAt > 0 && lines[insertAt - 1].trim() === '') insertAt--;
	lines.splice(insertAt, 0, '', snippet);

	return lines.join('\n');
}

/** Rename a group: update its identifier and all connection references. */
export function renameGroup(code: string, oldLabel: string, newLabel: string): string {
	if (!newLabel || oldLabel === newLabel) return code;
	const lines = code.split('\n');
	const group = findGroup(lines, oldLabel);
	if (!group) return code;

	// Rename the group header line: name = Group(...)
	const escapedOld = escapeRegex(oldLabel);
	lines[group.startLine] = lines[group.startLine].replace(
		new RegExp(`^(\\s*)${escapedOld}(\\s*=\\s*Group)`),
		`$1${newLabel}$2`,
	);

	// Update all connection references: oldLabel.port -> ... or ... -> oldLabel.port
	const connPattern = new RegExp(`\\b${escapedOld}\\.`, 'g');
	for (let i = 0; i < lines.length; i++) {
		if (i === group.startLine) continue; // skip the header line (already renamed)
		lines[i] = lines[i].replace(connPattern, `${newLabel}.`);
	}

	return lines.join('\n');
}

/** Remove a group scope block by its label. Children are moved out (up one scope level). */
export function removeGroup(code: string, groupLabel: string): string {
	let lines = code.split('\n');
	const group = findGroup(lines, groupLabel);
	if (!group) return code;

	if (group.isOneLiner) {
		lines.splice(group.startLine, 1);
		lines = removeConnectionsReferencing(lines, groupLabel);
		return cleanBlankLines(lines).join('\n');
	}

	// Extract child lines (everything inside the group body), filtering out self-connections
	const childLines: string[] = [];
	for (let i = group.contentStart; i < group.endLine; i++) {
		const trimmed = lines[i].trim();
		// Skip self.* connections (they reference the group's own ports, meaningless outside)
		if (isSelfConnection(trimmed)) continue;
		childLines.push(lines[i]);
	}

	// De-indent children by one level
	const deindented = childLines.map(l => {
		if (l.startsWith(group.innerIndent)) return group.indent + l.slice(group.innerIndent.length);
		return l;
	});

	// Replace the group block with the de-indented children
	lines.splice(group.startLine, group.endLine - group.startLine + 1, ...deindented);

	// Remove external connections referencing the group (e.g. groupLabel.port = ... or ... = groupLabel.port)
	lines = removeConnectionsReferencing(lines, groupLabel);

	return cleanBlankLines(lines).join('\n');
}

/** Check if a line is a connection involving self (self.port = x.port or x.port = self.port). */
function isSelfConnection(trimmed: string): boolean {
	if (!trimmed || trimmed.startsWith('#') || trimmed.startsWith('//')) return false;
	const isConn = /^\w[\w.]*\.\w+\s*=\s*\w[\w.]*\.\w+$/.test(trimmed);
	if (!isConn) return false;
	return /\bself\.\w+/.test(trimmed);
}

/** Remove a node and all connections referencing it. */
export function removeNode(code: string, nodeId: string): string {
	let lines = code.split('\n');

	// Handle scoped IDs (e.g. Group.Group_2.debug_1): find within the correct group scope
	const node = findNodeScoped(lines, nodeId);
	if (!node) return code;

	// Remove the node lines
	lines.splice(node.startLine, node.endLine - node.startLine + 1);

	// Remove connections referencing this node (using local ID since weft connections use local refs)
	const localId = nodeId.includes('.') ? nodeId.split('.').pop()! : nodeId;
	lines = removeConnectionsReferencing(lines, localId);

	// Clean up double blank lines
	return cleanBlankLines(lines).join('\n');
}

/** Add a connection line at the end of the relevant scope.
 *
 *  Input ports have a 1:1 relationship with their driver: a port can only be
 *  filled by one source at a time. Before appending the new edge, we remove
 *  any existing edge whose target is the same `(tgtId, tgtPort)`. The removal
 *  goes through `removeEdge` so that inline-anon binding edges trigger the
 *  materialization path (the anon survives as a standalone node instead of
 *  being silently deleted).
 *
 *  We also skip inserting a duplicate of the exact same edge. */
export function addEdge(
	code: string,
	srcId: string, srcPort: string,
	tgtId: string, tgtPort: string,
	scopeGroupId?: string,
): string {
	// Check if the target port already has a driver. Walk all connections
	// looking for one whose target matches. Self-connections are stored with
	// `targetIsSelf === true` and the targetId set to the enclosing group's
	// id, so we special-case `self` against the flag.
	const parsed = parseForEdit(code);
	const tgtLocal = tgtId.includes('.') ? tgtId.split('.').pop()! : tgtId;
	const allConns = [...parsed.connections, ...parsed.groups.flatMap(g => g.connections)];
	const existing = allConns.find(c => {
		if (c.targetPort !== tgtPort) return false;
		if (tgtId === 'self') return c.targetIsSelf === true;
		if (c.targetId === tgtId) return true;
		const cTgtLocal = c.targetId.includes('.') ? c.targetId.split('.').pop() : c.targetId;
		return cTgtLocal === tgtLocal;
	});

	let working = code;
	if (existing) {
		// Skip the exact-duplicate case.
		const srcLocal = srcId.includes('.') ? srcId.split('.').pop()! : srcId;
		const existingSrcLocal = existing.sourceId.includes('.') ? existing.sourceId.split('.').pop() : existing.sourceId;
		const sameSource =
			(srcId === 'self' && existing.sourceIsSelf === true)
			|| existing.sourceId === srcId
			|| existingSrcLocal === srcLocal;
		if (existing.sourcePort === srcPort && sameSource) {
			return code;
		}
		// Replace: remove the previous edge (materializes anon bindings).
		working = removeEdge(
			code,
			existing.sourceIsSelf ? 'self' : (existingSrcLocal || existing.sourceId),
			existing.sourcePort,
			existing.targetIsSelf ? 'self' : (existing.targetId.includes('.') ? existing.targetId.split('.').pop()! : existing.targetId),
			existing.targetPort,
		);
	}

	const lines = working.split('\n');
	const connLine = `${tgtId}.${tgtPort} = ${srcId}.${srcPort}`;

	if (scopeGroupId) {
		const group = findGroup(lines, scopeGroupId);
		if (group) {
			const indented = group.innerIndent + connLine;
			lines.splice(group.endLine, 0, indented);
			return lines.join('\n');
		}
	}

	// Top-level: find a good insertion point.
	// Try to insert after the last connection line, or after the last node.
	let insertAt = lines.length;
	while (insertAt > 0 && lines[insertAt - 1].trim() === '') insertAt--;
	lines.splice(insertAt, 0, connLine);

	return lines.join('\n');
}

/** Remove a connection line. If the edge is the binding of an inline anon
 *  (i.e. `anonParent__field.out → anonParent.field`), the anon is materialized
 *  into a standalone declaration at the same scope as its former host
 *  instead of being silently dropped. */
export function removeEdge(
	code: string,
	srcId: string, srcPort: string,
	tgtId: string, tgtPort: string,
): string {
	const lines = code.split('\n');

	// Detect a binding edge: the source is an inline anon whose id is
	// `<target local id>__<target port>` (possibly with a scope prefix).
	const srcLocal = srcId.includes('.') ? srcId.split('.').pop()! : srcId;
	const tgtLocal = tgtId.includes('.') ? tgtId.split('.').pop()! : tgtId;
	const expectedAnonLocal = `${tgtLocal}__${tgtPort}`;
	const isBindingEdge = srcLocal === expectedAnonLocal;

	if (isBindingEdge) {
		const materialized = tryMaterializeAnon(lines, srcId);
		if (materialized !== null) return materialized;
	}

	const lineIdx = findConnection(lines, srcId, srcPort, tgtId, tgtPort);
	if (lineIdx === null) return code;

	lines.splice(lineIdx, 1);
	return cleanBlankLines(lines).join('\n');
}

/** Try to materialize an inline anon into a standalone declaration at the
 *  scope of its former host. Returns the new source, or null if the anon
 *  could not be located. */
function tryMaterializeAnon(lines: string[], anonId: string): string | null {
	const parsed = parseForEdit(lines.join('\n'));

	// Find the anon and its enclosing group (if any). Caller may pass either
	// a scoped id (`grp.host__data`) or a local id (`host__data`); match on
	// either form.
	const matchesId = (n: ParsedNode): boolean => {
		if (n.id === anonId) return true;
		if (!anonId.includes('.')) {
			const local = n.id.includes('.') ? n.id.split('.').pop() : n.id;
			return local === anonId;
		}
		return false;
	};
	let anon: ParsedNode | null = null;
	let enclosingGroup: ParsedGroup | null = null;
	for (const n of parsed.nodes) {
		if (matchesId(n)) { anon = n; break; }
	}
	if (!anon) {
		for (const g of parsed.groups) {
			const inner = g.nodes.find(matchesId);
			if (inner) { anon = inner; enclosingGroup = g; break; }
		}
	}
	if (!anon) return null;

	const anonStart = anon.startLine - 1; // 0-based
	const anonEnd = anon.endLine - 1;
	const firstLine = lines[anonStart];
	if (!firstLine) return null;

	const anonLocalId = anonId.includes('.') ? anonId.split('.').pop()! : anonId;
	// Parent's local id is `anonLocalId` split at the last `__`.
	const parentLocalId = anonLocalId.slice(0, anonLocalId.lastIndexOf('__'));
	if (!parentLocalId) return null;

	// Locate the parent host in the same scope as the anon (either a sibling
	// top-level node, or a sibling inside the enclosing group).
	const siblings: ParsedNode[] = enclosingGroup ? enclosingGroup.nodes : parsed.nodes;
	const parent = siblings.find(n => {
		const local = n.id.includes('.') ? n.id.split('.').pop() : n.id;
		return local === parentLocalId;
	});
	if (!parent) return null;
	const parentStart = parent.startLine - 1; // 0-based
	const parentEnd = parent.endLine - 1;

	// Determine the form from the anon's first line.
	const connFormRe = /^(\s*)\w+(?:\.\w+)*\.\w+\s*=\s*(.*)$/;
	const configFormRe = /^(\s*)\w+\s*:\s*(.*)$/;

	const connMatch = firstLine.match(connFormRe);

	if (connMatch && anonStart >= parentEnd + 1) {
		// Connection-line form: `parent.field = Type { ... }.port` — the anon
		// is declared OUTSIDE (after) the parent block. Rewrite this line and
		// the last line of the anon's block.
		const [, indent, afterEq] = connMatch;
		lines[anonStart] = `${indent}${anonLocalId} = ${afterEq}`;
		const stripTrailingPort = (s: string): string => s.replace(/\}\.\w+\s*$/, '}');
		if (anonStart === anonEnd) {
			lines[anonStart] = stripTrailingPort(lines[anonStart]);
		} else {
			lines[anonEnd] = stripTrailingPort(lines[anonEnd]);
		}
		return cleanBlankLines(lines).join('\n');
	}

	const configMatch = firstLine.match(configFormRe);
	if (configMatch && anonStart > parentStart && anonEnd < parentEnd) {
		// Config-block form: the anon lives inside the parent's body as a
		// `field: Type { ... }.port` entry. Remove those lines from the
		// parent's body and insert a standalone declaration right after the
		// parent's closing brace, at the parent's indent level.
		const afterColon = configMatch[2];
		const parentIndent = lines[parentStart].match(/^(\s*)/)?.[1] ?? '';

		const materialized: string[] = [];
		if (anonStart === anonEnd) {
			// One-liner value: `field: Type { ... }.port`
			const stripped = afterColon.replace(/\}\.\w+\s*$/, '}');
			materialized.push(`${parentIndent}${anonLocalId} = ${stripped}`);
		} else {
			// Multi-line value: first line has `field: Type { ... {` and
			// last line has `}.port`. Middle lines are body content.
			materialized.push(`${parentIndent}${anonLocalId} = ${afterColon}`);
			// Middle lines keep their original indent relative to body content.
			for (let i = anonStart + 1; i < anonEnd; i++) {
				materialized.push(lines[i]);
			}
			materialized.push(`${parentIndent}${lines[anonEnd].replace(/^\s*/, '').replace(/\}\.\w+\s*$/, '}')}`);
		}

		// Remove the anon's field lines from the parent's body.
		lines.splice(anonStart, anonEnd - anonStart + 1);

		// Insert after the parent's closing brace. Parent's end line was
		// `parentEnd`, but we may have removed lines inside the parent's body,
		// shifting everything after `anonStart` upward by `removedCount`.
		const removedCount = anonEnd - anonStart + 1;
		const newParentEnd = parentEnd - removedCount;
		const insertAt = newParentEnd + 1;

		lines.splice(insertAt, 0, '', ...materialized);
		return cleanBlankLines(lines).join('\n');
	}

	return null;
}

/** Move a node's definition to a different scope (into a group or out to top level).
 *  Pass targetGroupLabel=undefined to move to top level.
 *
 *  Rejects (returns unchanged `code`) when:
 *  - The target scope is the same as the current scope.
 *  - The node has any edge whose other endpoint would become unreachable
 *    after the move. The "can't move a connected node" rule guarantees the
 *    source stays consistent, so users must disconnect edges first. */
export function moveNodeScope(code: string, nodeId: string, targetGroupLabel: string | undefined): string {
	const lines = code.split('\n');
	const node = findNodeScoped(lines, nodeId);
	if (!node) return code;

	// Determine current scope: walk up the parsed tree for this node.
	const parsed = parseForEdit(code);
	const localId = nodeId.includes('.') ? nodeId.split('.').pop()! : nodeId;
	// Find the scope id (either undefined for root, or the enclosing group id).
	let currentScopeId: string | undefined = undefined;
	for (const g of parsed.groups) {
		if (g.nodes.some(n => n.id === nodeId || n.id.endsWith('.' + localId))) {
			currentScopeId = g.id;
			break;
		}
	}
	// Normalize the target scope. `targetGroupLabel` may be the group's local
	// label ("grp") or scoped id ("outer.grp"); findGroup handles both.
	let targetScopeId: string | undefined = undefined;
	if (targetGroupLabel) {
		const tg = parsed.groups.find(g =>
			g.id === targetGroupLabel
			|| g.originalName === targetGroupLabel
			|| g.id.endsWith('.' + targetGroupLabel),
		);
		targetScopeId = tg?.id;
		if (!targetScopeId) return code;
	}
	// Same-scope move: noop.
	if (currentScopeId === targetScopeId) return code;

	// Connected-node guard: refuse the move if the node has any edge whatsoever.
	// (A looser check would allow moves when every edge would still be legal at
	// the destination, but that requires scope-reachability analysis. For now,
	// we use the stricter rule: disconnect first, then move.)
	const hasAnyEdge = [
		...parsed.connections,
		...parsed.groups.flatMap(g => g.connections),
	].some(c => {
		const srcLocal = c.sourceId.includes('.') ? c.sourceId.split('.').pop() : c.sourceId;
		const tgtLocal = c.targetId.includes('.') ? c.targetId.split('.').pop() : c.targetId;
		return srcLocal === localId || tgtLocal === localId;
	});
	if (hasAnyEdge) return code;

	// Extract the node block
	const nodeLines = lines.splice(node.startLine, node.endLine - node.startLine + 1);

	// Strip existing indentation and re-indent for target scope
	const oldIndent = node.indent;
	const stripped = nodeLines.map(l => l.startsWith(oldIndent) ? l.slice(oldIndent.length) : l);

	if (targetGroupLabel) {
		const group = findGroup(lines, targetGroupLabel);
		if (!group) return code;
		// Expand one-liner group to multi-line before inserting
		if (group.isOneLiner) {
			expandOneLinerGroup(lines, group);
			// Re-find after expansion
			const expanded = findGroup(lines, targetGroupLabel);
			if (!expanded) return code;
			const newIndent = expanded.innerIndent;
			const reindented = stripped.map(l => l.trim() === '' ? l : newIndent + l);
			lines.splice(expanded.endLine, 0, '', ...reindented);
		} else {
			const newIndent = group.innerIndent;
			const reindented = stripped.map(l => l.trim() === '' ? l : newIndent + l);
			lines.splice(group.endLine, 0, '', ...reindented);
		}
	} else {
		let insertAt = lines.length;
		while (insertAt > 0 && lines[insertAt - 1].trim() === '') insertAt--;
		lines.splice(insertAt, 0, '', ...stripped);
	}

	return cleanBlankLines(lines).join('\n');
}

/** Move a group scope block to a different scope. Rejects if the group has
 *  any edge crossing its boundary (i.e. any external connection referencing
 *  one of the group's ports). */
export function moveGroupScope(code: string, groupLabel: string, targetGroupLabel: string | undefined): string {
	const lines = code.split('\n');
	const group = findGroup(lines, groupLabel);
	if (!group) return code;

	// Same-scope check.
	const parsed = parseForEdit(code);
	const groupParsed = parsed.groups.find(g =>
		g.id === groupLabel
		|| g.originalName === groupLabel
		|| g.id.endsWith('.' + groupLabel),
	);
	if (!groupParsed) return code;
	let currentScopeId: string | undefined = undefined;
	const dotIdx = groupParsed.id.lastIndexOf('.');
	if (dotIdx >= 0) currentScopeId = groupParsed.id.slice(0, dotIdx);
	let targetScopeId: string | undefined = undefined;
	if (targetGroupLabel) {
		const tg = parsed.groups.find(g =>
			g.id === targetGroupLabel
			|| g.originalName === targetGroupLabel
			|| g.id.endsWith('.' + targetGroupLabel),
		);
		if (!tg) return code;
		targetScopeId = tg.id;
	}
	if (currentScopeId === targetScopeId) return code;

	// Connected-group guard: only edges that cross the group boundary count
	// as "external". Self-connections inside the group body (wiring group
	// inputs to children or children to group outputs) never block a move.
	// We look only at edges outside the group and at connections in sibling
	// groups that reference this group's ports.
	const groupFullId = groupParsed.id;
	const escapedFull = escapeRegex(groupFullId);
	const escapedLocal = escapeRegex(groupParsed.originalName || groupLabel);
	const referencesThisGroup = (id: string): boolean =>
		id === groupFullId
		|| id === groupParsed.originalName
		|| new RegExp(`^${escapedFull}\\.`).test(id)
		|| new RegExp(`^${escapedLocal}\\.`).test(id);

	const externalEdges = [
		...parsed.connections,
		// Include connections in OTHER groups' bodies (they may reference ours).
		...parsed.groups
			.filter(g => g.id !== groupFullId)
			.flatMap(g => g.connections),
	];
	const hasExternalEdge = externalEdges.some(c =>
		referencesThisGroup(c.sourceId) || referencesThisGroup(c.targetId),
	);
	if (hasExternalEdge) return code;

	// Extract the entire group block
	const groupLines = lines.splice(group.startLine, group.endLine - group.startLine + 1);

	// Strip existing indentation and re-indent
	const oldIndent = group.indent;
	const stripped = groupLines.map(l => l.startsWith(oldIndent) ? l.slice(oldIndent.length) : l);

	if (targetGroupLabel) {
		const target = findGroup(lines, targetGroupLabel);
		if (!target) return code;
		if (target.isOneLiner) {
			expandOneLinerGroup(lines, target);
			const expanded = findGroup(lines, targetGroupLabel);
			if (!expanded) return code;
			const reindented = stripped.map(l => l.trim() === '' ? l : expanded.innerIndent + l);
			lines.splice(expanded.endLine, 0, '', ...reindented);
		} else {
			const reindented = stripped.map(l => l.trim() === '' ? l : target.innerIndent + l);
			lines.splice(target.endLine, 0, '', ...reindented);
		}
	} else {
		let insertAt = lines.length;
		while (insertAt > 0 && lines[insertAt - 1].trim() === '') insertAt--;
		lines.splice(insertAt, 0, '', ...stripped);
	}

	return cleanBlankLines(lines).join('\n');
}

/** Format a port for signature: `name: Type` or `name: Type?` for optional. */
function formatPort(p: { name: string; required?: boolean; portType?: string }): string {
	const typeStr = p.portType || 'MustOverride';
	const optional = p.required === false ? '?' : '';
	return `${p.name}: ${typeStr}${optional}`;
}

/** Build the port signature string: `(port1: T, port2: T?) -> (out1: T)` */
function buildSignature(
	inputs: Array<{ name: string; required?: boolean; portType?: string }>,
	outputs: Array<{ name: string; required?: boolean; portType?: string }>,
): string {
	const inParts = inputs.map(formatPort).join(', ');
	const outParts = outputs.map(formatPort).join(', ');
	if (inputs.length === 0 && outputs.length === 0) return '';
	if (outputs.length === 0) return `(${inParts})`;
	return `(${inParts}) -> (${outParts})`;
}

/** Extract old port names from the declaration line(s) signature.
 *  Parses `(name1: T, name2: T?) -> (out1: T)` from the header. */
function extractSignaturePortNames(lines: string[], startLine: number): { inputs: string[]; outputs: string[]; sigEndLine: number } {
	const inputs: string[] = [];
	const outputs: string[] = [];

	// Collect the full header text from startLine until we've balanced all parens
	let headerText = '';
	let parenDepth = 0;
	let foundParen = false;
	let sigEndLine = startLine;

	for (let i = startLine; i < lines.length; i++) {
		headerText += (i > startLine ? '\n' : '') + lines[i];
		for (const c of lines[i]) {
			if (c === '(') { parenDepth++; foundParen = true; }
			if (c === ')') parenDepth--;
		}
		sigEndLine = i;
		// If we found at least one paren and are back to 0, check if there's a -> next
		if (foundParen && parenDepth === 0) {
			// Peek at next line for ->
			if (i + 1 < lines.length && lines[i + 1].trim().startsWith('->')) {
				continue; // keep going
			}
			// Check if current line has -> after the closing )
			const afterLastParen = headerText.slice(headerText.lastIndexOf(')') + 1).trim();
			if (afterLastParen.startsWith('->')) {
				// Check if output parens are still open
				let outDepth = 0;
				for (const c of afterLastParen) {
					if (c === '(') outDepth++;
					if (c === ')') outDepth--;
				}
				if (outDepth > 0) continue; // output parens not closed yet
			}
			break;
		}
		if (lines[i].includes('{')) break; // hit the body
	}

	// Parse port names from the collected header
	// Find the = sign and the type name, then extract (inputs) -> (outputs)
	const eqPos = headerText.indexOf('=');
	if (eqPos < 0) return { inputs, outputs, sigEndLine };

	const afterEq = headerText.slice(eqPos + 1).trim();
	// Skip the type name
	const typeEnd = afterEq.search(/[(\s{]/);
	if (typeEnd < 0) return { inputs, outputs, sigEndLine };

	const sigPart = afterEq.slice(typeEnd).trim();
	if (!sigPart.startsWith('(')) return { inputs, outputs, sigEndLine };

	// Simple extraction: find balanced (...)
	function extractPorts(text: string): string[] {
		const names: string[] = [];
		// Remove the outer parens
		const inner = text.slice(1, text.lastIndexOf(')'));
		for (const part of inner.split(/[,\n]/)) {
			const trimmed = part.trim();
			if (!trimmed || trimmed.startsWith('#') || trimmed.startsWith('@')) continue;
			const nameMatch = trimmed.match(/^([a-zA-Z_]\w*)/);
			if (nameMatch) names.push(nameMatch[1]);
		}
		return names;
	}

	// Find matching ) for inputs
	let depth = 0;
	let inputEnd = -1;
	for (let i = 0; i < sigPart.length; i++) {
		if (sigPart[i] === '(') depth++;
		if (sigPart[i] === ')') { depth--; if (depth === 0) { inputEnd = i; break; } }
	}
	if (inputEnd >= 0) {
		inputs.push(...extractPorts(sigPart.slice(0, inputEnd + 1)));
		const afterInputs = sigPart.slice(inputEnd + 1).trim();
		if (afterInputs.startsWith('->')) {
			const outPart = afterInputs.slice(2).trim();
			if (outPart.startsWith('(')) {
				outputs.push(...extractPorts(outPart));
			}
		}
	}

	return { inputs, outputs, sigEndLine };
}

/** Extract port names from a post-config -> (...) block, if present. */
function extractPostConfigPortNames(lines: string[], node: NodeLocation): string[] {
	if (node.isOneLiner) return [];
	const names: string[] = [];

	// Find the closing } of the config block
	for (let i = node.endLine; i > node.startLine; i--) {
		const trimmed = lines[i].trim();
		if (trimmed.startsWith('}') || trimmed.endsWith('}')) {
			// Check same line for -> after }
			const afterBrace = lines[i].substring(lines[i].indexOf('}') + 1).trim();
			if (afterBrace.startsWith('->')) {
				names.push(...extractPortNamesFromArrow(afterBrace, lines, i));
				return names;
			}
			// Check lines after } for ->
			let peek = i + 1;
			while (peek <= node.endLine && lines[peek].trim() === '') peek++;
			if (peek <= node.endLine && lines[peek].trim().startsWith('->')) {
				let arrowText = '';
				for (let j = peek; j <= node.endLine; j++) {
					arrowText += lines[j] + '\n';
				}
				names.push(...extractPortNamesFromText(arrowText));
			}
			break;
		}
	}
	return names;
}

/** Extract port names from a -> (...) text fragment. */
function extractPortNamesFromArrow(afterBrace: string, lines: string[], startLine: number): string[] {
	let text = afterBrace;
	// If parens aren't balanced, collect continuation lines
	let pd = 0;
	for (const c of text) { if (c === '(') pd++; if (c === ')') pd--; }
	if (pd > 0) {
		for (let i = startLine + 1; i < lines.length; i++) {
			text += '\n' + lines[i];
			for (const c of lines[i]) { if (c === '(') pd++; if (c === ')') pd--; }
			if (pd === 0) break;
		}
	}
	return extractPortNamesFromText(text);
}

/** Extract port names from text containing -> (name: Type, ...). */
function extractPortNamesFromText(text: string): string[] {
	const names: string[] = [];
	const arrowIdx = text.indexOf('->');
	if (arrowIdx < 0) return names;
	const afterArrow = text.slice(arrowIdx + 2).trim();
	if (!afterArrow.startsWith('(')) return names;
	const inner = afterArrow.slice(1, afterArrow.lastIndexOf(')'));
	for (const part of inner.split(/[,\n]/)) {
		const trimmed = part.trim();
		if (!trimmed || trimmed.startsWith('#') || trimmed.startsWith('@')) continue;
		const nameMatch = trimmed.match(/^([a-zA-Z_]\w*)/);
		if (nameMatch) names.push(nameMatch[1]);
	}
	return names;
}

/** Rebuild the declaration line(s) with new port signature.
 *  Preserves the node id, type, and any trailing { or body. */
function rebuildDeclarationWithPorts(
	lines: string[],
	startLine: number,
	sigEndLine: number,
	inputs: Array<{ name: string; required?: boolean; portType?: string }>,
	outputs: Array<{ name: string; required?: boolean; portType?: string }>,
): void {
	// Extract the parts we need to preserve: indent, id = Type, and trailing { or nothing
	const firstLine = lines[startLine];
	const indent = firstLine.match(/^(\s*)/)?.[1] ?? '';
	const eqMatch = firstLine.match(/^(\s*)([a-zA-Z_]\w*)\s*=\s*([A-Z]\w*)/);
	if (!eqMatch) return;

	const id = eqMatch[2];
	const type = eqMatch[3];

	// Find if there's a { on the last signature line
	const anyBrace = lines.slice(startLine, sigEndLine + 1).some(l => l.includes('{'));

	// For one-liners like `id = Type { config }`, preserve the config body
	let configBody = '';
	if (startLine === sigEndLine && anyBrace) {
		const braceStart = firstLine.indexOf('{');
		if (braceStart >= 0) {
			configBody = ' ' + firstLine.substring(braceStart).trim();
		}
	} else if (anyBrace) {
		// Multi-line header ending with {: just add the {
		configBody = ' {';
	}

	const sig = buildSignature(inputs, outputs);
	const newLine = `${indent}${id} = ${type}${sig}${configBody || ''}`;

	// Replace the header lines with the single new line
	lines.splice(startLine, sigEndLine - startLine + 1, newLine);
}

/** Rewrite the type-signature fragment of an inline anon's first line.
 *  Given a prefix like `  data: Template(old: String) ` or
 *  `host.data = Template `, return the prefix with a fresh `(new...)` signature
 *  appended after the type name. `outputs` are ignored because inline
 *  expressions cannot declare post-config outputs. */
function rewriteInlinePrefix(
	prefix: string,
	inputs: Array<{ name: string; required?: boolean; portType?: string }>,
	outputs: Array<{ name: string; required?: boolean; portType?: string }>,
): string {
	// Find the type name: the last word-run before the `{` (which is implicit
	// at the end of prefix) or before an existing `(`.
	// Strip trailing whitespace so we can append cleanly.
	let head = prefix.replace(/\s+$/, '');
	// If a `(...)` signature already exists, remove it. We look for the last
	// `(` balanced to its matching `)` at the end of `head`.
	if (head.endsWith(')')) {
		let depth = 0;
		let openIdx = -1;
		for (let i = head.length - 1; i >= 0; i--) {
			const c = head[i];
			if (c === ')') depth++;
			else if (c === '(') { depth--; if (depth === 0) { openIdx = i; break; } }
		}
		if (openIdx >= 0) head = head.slice(0, openIdx).replace(/\s+$/, '');
	}
	const sig = buildSignature(inputs, outputs);
	return `${head}${sig} `;
}

/** Update ports on a one-liner inline anon. */
function updateAnonPorts(
	code: string,
	lines: string[],
	lineIdx: number,
	span: ReturnType<typeof extractInlineBodySpan> & object,
	inputs: Array<{ name: string; required?: boolean; portType?: string }>,
	outputs: Array<{ name: string; required?: boolean; portType?: string }>,
): string {
	// Outputs are not expressible on inline anons: the line structure ties the
	// output to the `.port` selector at the end. If the caller is trying to add
	// new outputs, refuse.
	if (outputs.length > 0) return code;

	const newPrefix = rewriteInlinePrefix(span.prefix, inputs, outputs);
	lines[lineIdx] = `${newPrefix}{${span.body}}${span.suffix}`;
	return lines.join('\n');
}

/** Update ports on a multi-line inline anon. The first line has the
 *  `prefix Type [sig] {` shape; only that line needs to be rewritten because
 *  the signature lives entirely before the opening `{`. */
function updateAnonPortsMultiLine(
	code: string,
	lines: string[],
	startLine: number,
	inputs: Array<{ name: string; required?: boolean; portType?: string }>,
	outputs: Array<{ name: string; required?: boolean; portType?: string }>,
): string {
	if (outputs.length > 0) return code;
	const first = lines[startLine];
	// Split at the trailing ` {`.
	const braceIdx = first.lastIndexOf('{');
	if (braceIdx < 0) return code;
	const prefix = first.slice(0, braceIdx);
	const suffix = first.slice(braceIdx); // starts with `{`
	const newPrefix = rewriteInlinePrefix(prefix, inputs, outputs);
	lines[startLine] = `${newPrefix}${suffix}`;
	return lines.join('\n');
}

/** Update port declarations on a node by rewriting its signature. */
export function updateNodePorts(
	code: string,
	nodeId: string,
	inputs: Array<{ name: string; required?: boolean; laneMode?: string; portType?: string }>,
	outputs: Array<{ name: string; laneMode?: string; portType?: string }>,
): string {
	const lines = code.split('\n');
	const node = findNodeScoped(lines, nodeId);
	if (!node) return code;

	// Inline anon case: the node's rawLine is not a canonical `id = Type {...}`
	// declaration but an inline expression inside a connection line or a
	// parent's config block. We rewrite the `Type[(sig)]` fragment that sits
	// just before the inline body `{`.
	if (node.isOneLiner) {
		const lineStr = lines[node.startLine];
		const span = extractInlineBodySpan(lineStr);
		if (span && span.isAnon) {
			return updateAnonPorts(code, lines, node.startLine, span, inputs, outputs);
		}
	} else {
		// Multi-line anon: the first line ends with ` {` but doesn't start with
		// `id = Type`. Detect by checking the first line's shape.
		const firstLine = lines[node.startLine];
		const canonical = /^(\s*)\w+\s*=\s*\w+/.test(firstLine);
		if (!canonical) {
			return updateAnonPortsMultiLine(code, lines, node.startLine, inputs, outputs);
		}
	}

	// Extract old port names from the pre-config signature
	const { inputs: oldInPorts, outputs: oldOutPorts, sigEndLine } = extractSignaturePortNames(lines, node.startLine);

	// Also extract post-config output port names (they won't be in the pre-config signature)
	const postConfigOutPorts = extractPostConfigPortNames(lines, node);
	const allOldOutPorts = [...oldOutPorts, ...postConfigOutPorts];

	// Remove any post-config -> (...) block (ports will be in the pre-config signature).
	// The parser's endLine now includes post-config outputs, so we search within the node range.
	// Find the closing } of the config block, then check if -> follows.
	if (!node.isOneLiner) {
		// Find the config block's closing }
		let closingBrace = -1;
		for (let i = node.endLine; i > node.startLine; i--) {
			if (lines[i].trim().startsWith('}') || lines[i].trimEnd().endsWith('}')) {
				// Check if this line also has -> after } (same-line post-config)
				const afterBrace = lines[i].substring(lines[i].indexOf('}') + 1).trim();
				if (afterBrace.startsWith('->')) {
					// Same-line: strip the -> (...) part from this line
					lines[i] = lines[i].substring(0, lines[i].indexOf('}') + 1);
					// Remove any continuation lines of the post-config block
					let removeStart = i + 1;
					let removeEnd = i;
					let pd = 0;
					for (const c of afterBrace) { if (c === '(') pd++; if (c === ')') pd--; }
					if (pd > 0) {
						for (let ri = i + 1; ri <= node.endLine; ri++) {
							for (const c of lines[ri]) { if (c === '(') pd++; if (c === ')') pd--; }
							removeEnd = ri;
							if (pd === 0) break;
						}
						lines.splice(removeStart, removeEnd - removeStart + 1);
					}
					break;
				}
				closingBrace = i;
				break;
			}
		}
		// Check lines after the closing } for a post-config -> block
		if (closingBrace >= 0 && closingBrace < node.endLine) {
			let peekAfter = closingBrace + 1;
			while (peekAfter <= node.endLine && lines[peekAfter].trim() === '') peekAfter++;
			if (peekAfter <= node.endLine && lines[peekAfter].trim().startsWith('->')) {
				let removeEnd = peekAfter;
				let pd = 0;
				for (let ri = peekAfter; ri <= node.endLine; ri++) {
					for (const c of lines[ri]) { if (c === '(') pd++; if (c === ')') pd--; }
					removeEnd = ri;
					if (pd === 0 && lines[ri].includes(')')) break;
				}
				lines.splice(peekAfter, removeEnd - peekAfter + 1);
			}
		}
	}

	// Rebuild the declaration with new ports
	rebuildDeclarationWithPorts(lines, node.startLine, sigEndLine, inputs, outputs);

	// Invalidate orphaned connections
	invalidateOrphanedConnections(lines, { kind: 'node', nodeId }, inputs, outputs, oldInPorts, allOldOutPorts);

	return cleanBlankLines(lines).join('\n');
}

/** Update port declarations on a group by rewriting its signature. */
export function updateGroupPorts(
	code: string,
	groupLabel: string,
	inputs: Array<{ name: string; required?: boolean; laneMode?: string; portType?: string }>,
	outputs: Array<{ name: string; laneMode?: string; portType?: string }>,
): string {
	const lines = code.split('\n');
	const group = findGroup(lines, groupLabel);
	if (!group) return code;

	const { inputs: oldInPorts, outputs: oldOutPorts, sigEndLine } = extractSignaturePortNames(lines, group.startLine);

	rebuildDeclarationWithPorts(lines, group.startLine, sigEndLine, inputs, outputs);

	invalidateOrphanedConnections(lines, { kind: 'group', groupLabel }, inputs, outputs, oldInPorts, oldOutPorts);

	return cleanBlankLines(lines).join('\n');
}

/** Remove connections referencing ports that no longer exist. */
function invalidateOrphanedConnections(
	lines: string[],
	target: { kind: 'node' | 'group'; nodeId?: string; groupLabel?: string },
	inputs: Array<{ name: string }>,
	outputs: Array<{ name: string }>,
	oldInPorts: string[],
	oldOutPorts: string[],
): void {
	const newInNames = new Set(inputs.map(p => p.name));
	const newOutNames = new Set(outputs.map(p => p.name));
	const removedInPorts = oldInPorts.filter(n => !newInNames.has(n));
	const removedOutPorts = oldOutPorts.filter(n => !newOutNames.has(n));

	if (removedInPorts.length === 0 && removedOutPorts.length === 0) return;

	const patterns: RegExp[] = [];

	if (target.kind === 'group' && target.groupLabel) {
		const escaped = escapeRegex(target.groupLabel);
		for (const port of removedInPorts) {
			const ep = escapeRegex(port);
			patterns.push(new RegExp(`=\\s*self\\.${ep}\\s*$`));
			patterns.push(new RegExp(`^\\s*${escaped}\\.${ep}\\s*=`));
		}
		for (const port of removedOutPorts) {
			const ep = escapeRegex(port);
			patterns.push(new RegExp(`^\\s*self\\.${ep}\\s*=`));
			patterns.push(new RegExp(`=\\s*${escaped}\\.${ep}\\s*$`));
		}
	} else if (target.nodeId) {
		const localId = target.nodeId.includes('.') ? target.nodeId.split('.').pop()! : target.nodeId;
		const escaped = escapeRegex(localId);
		for (const port of removedInPorts) {
			patterns.push(new RegExp(`^\\s*${escaped}\\.${escapeRegex(port)}\\s*=`));
		}
		for (const port of removedOutPorts) {
			patterns.push(new RegExp(`=\\s*${escaped}\\.${escapeRegex(port)}\\s*$`));
		}
	}

	for (let i = lines.length - 1; i >= 0; i--) {
		if (patterns.some(p => p.test(lines[i]))) {
			lines.splice(i, 1);
		}
	}
}

/** Update the # Project: and # Description: header comments. */
export function updateProjectMeta(code: string, name?: string, description?: string): string {
	const lines = code.split('\n');

	for (let i = 0; i < Math.min(lines.length, 10); i++) {
		if (name !== undefined && lines[i].trim().startsWith('# Project:')) {
			lines[i] = `# Project: ${name}`;
		}
		if (description !== undefined && lines[i].trim().startsWith('# Description:')) {
			lines[i] = `# Description: ${description}`;
		}
	}

	// If description was provided but no existing line found, add it after the project name line
	if (description !== undefined) {
		const hasDesc = lines.slice(0, 10).some(l => l.trim().startsWith('# Description:'));
		if (!hasDesc && description) {
			const nameIdx = lines.findIndex(l => l.trim().startsWith('# Project:'));
			if (nameIdx >= 0) {
				lines.splice(nameIdx + 1, 0, `# Description: ${description}`);
			}
		}
	}

	return lines.join('\n');
}



// ── Layout Code Management ───────────────────────────────────────────────────

/** Parse layoutCode string into a map of scoped ID → layout entry */
export function parseLayoutCode(layoutCode: string): Record<string, { x: number; y: number; w?: number; h?: number; expanded?: boolean }> {
	const map: Record<string, { x: number; y: number; w?: number; h?: number; expanded?: boolean }> = {};
	if (!layoutCode) return map;
	for (const line of layoutCode.split('\n')) {
		const trimmed = line.trim();
		if (!trimmed) continue;
		// Format: scopedId @layout x y [WxH] [expanded|collapsed]
		const match = trimmed.match(/^(.+?)\s+@layout\s+(-?\d+(?:\.\d+)?)\s+(-?\d+(?:\.\d+)?)(?:\s+(\d+(?:\.\d+)?)x(\d+(?:\.\d+)?))?(?:\s+(collapsed|expanded))?\s*$/);
		if (!match) continue;
		const [, scopedId, xStr, yStr, wStr, hStr, state] = match;
		const entry: { x: number; y: number; w?: number; h?: number; expanded?: boolean } = {
			x: parseFloat(xStr),
			y: parseFloat(yStr),
		};
		if (wStr && hStr) {
			entry.w = parseFloat(wStr);
			entry.h = parseFloat(hStr);
		}
		if (state === 'expanded') entry.expanded = true;
		if (state === 'collapsed') entry.expanded = false;
		map[scopedId] = entry;
	}
	return map;
}

/** Update or insert a layout entry in layoutCode. Returns the new layoutCode string. */
export function updateLayoutEntry(
	layoutCode: string,
	scopedId: string,
	x: number, y: number,
	w?: number, h?: number,
	expanded?: boolean | null,
): string {
	const layoutStr = formatLayoutStr(x, y, w, h, expanded);
	const newLine = `${scopedId} ${layoutStr}`;
	const lines = (layoutCode || '').split('\n');
	const idx = lines.findIndex(l => {
		const t = l.trim();
		return t.startsWith(scopedId + ' @layout') || t.startsWith(scopedId + '\t@layout');
	});
	if (idx >= 0) {
		lines[idx] = newLine;
	} else {
		lines.push(newLine);
	}
	return lines.filter(l => l.trim() !== '').join('\n');
}

/** Remove a layout entry from layoutCode. Returns the new layoutCode string. */
export function removeLayoutEntry(layoutCode: string, scopedId: string): string {
	if (!layoutCode) return '';
	return layoutCode.split('\n')
		.filter(l => {
			const t = l.trim();
			return !(t.startsWith(scopedId + ' @layout') || t.startsWith(scopedId + '\t@layout'));
		})
		.join('\n');
}

function formatLayoutStr(x: number, y: number, w?: number, h?: number, expanded?: boolean | null): string {
	let s = `@layout ${Math.round(x)} ${Math.round(y)}`;
	if (w !== undefined && h !== undefined) {
		s += ` ${Math.round(w)}x${Math.round(h)}`;
	}
	if (expanded === true) s += ' expanded';
	if (expanded === false) s += ' collapsed';
	return s;
}

/**
 * Rename a scoped ID prefix in layoutCode.
 * When a group is renamed (e.g., "Outer" → "Processing"), all layout entries
 * with the old scoped ID or starting with "oldPrefix." are updated.
 * E.g., "Outer @layout ..." → "Processing @layout ..."
 *       "Outer.child @layout ..." → "Processing.child @layout ..."
 */
export function renameLayoutPrefix(layoutCode: string, oldScopedId: string, newScopedId: string): string {
	if (!layoutCode || oldScopedId === newScopedId) return layoutCode;
	return layoutCode.split('\n').map(line => {
		const t = line.trim();
		// Exact match: "oldScopedId @layout ..."
		if (t.startsWith(oldScopedId + ' @layout') || t.startsWith(oldScopedId + '\t@layout')) {
			return newScopedId + t.slice(oldScopedId.length);
		}
		// Prefix match for children: "oldScopedId.child @layout ..."
		if (t.startsWith(oldScopedId + '.')) {
			return newScopedId + t.slice(oldScopedId.length);
		}
		return line;
	}).join('\n');
}

/** Collapse runs of 3+ blank lines into 2. */
function cleanBlankLines(lines: string[]): string[] {
	const result: string[] = [];
	let blankCount = 0;
	for (const line of lines) {
		if (line.trim() === '') {
			blankCount++;
			if (blankCount <= 2) result.push(line);
		} else {
			blankCount = 0;
			result.push(line);
		}
	}
	return result;
}
