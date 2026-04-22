
import type { ProjectDefinition, NodeInstance, Edge, PortDefinition, NodeFeatures, LaneMode, PortType } from '$lib/types';
import { parseWeftType, weftTypeToString, isWeftTypeCompatible, inferTypeFromValue, isCompatible, isPortConfigurable } from '$lib/types';
import { NODE_TYPE_CONFIG } from '$lib/nodes';
import { validateNode } from '$lib/validation';
import { extractInfraSubgraph } from '$lib/utils/infra-subgraph';
import { buildSpecMap, deriveInputsFromFields, deriveOutputsFromFields, type FormFieldDef } from '$lib/utils/form-field-specs';
import ELK from 'elkjs/lib/elk.bundled.js';

/** Suggest the closest match from a list of candidate names. Returns a
 *  "did you mean" phrase or empty string if no close match exists. */
function suggestPortName(unknown: string, candidates: string[]): string {
	if (!candidates.length) return '';

	function distance(a: string, b: string): number {
		const m = a.length, n = b.length;
		if (m === 0) return n;
		if (n === 0) return m;
		const dp: number[] = new Array(n + 1);
		for (let j = 0; j <= n; j++) dp[j] = j;
		for (let i = 1; i <= m; i++) {
			let prev = i - 1;
			dp[0] = i;
			for (let j = 1; j <= n; j++) {
				const temp = dp[j];
				dp[j] = a[i - 1] === b[j - 1] ? prev : 1 + Math.min(prev, dp[j - 1], dp[j]);
				prev = temp;
			}
		}
		return dp[n];
	}

	/** Score a candidate, lower is better.
	 *  Prefers candidates that share a common prefix with the unknown name. */
	function score(u: string, c: string): number {
		const lu = u.toLowerCase();
		const lc = c.toLowerCase();
		let prefixLen = 0;
		while (prefixLen < lu.length && prefixLen < lc.length && lu[prefixLen] === lc[prefixLen]) {
			prefixLen++;
		}
		const dist = distance(lu, lc);
		// Strong prefix bonus: a shared prefix of 3+ chars counts as ~2 distance units each.
		const prefixBonus = prefixLen >= 3 ? prefixLen * 2 : 0;
		// Substring bonus: one name contains the other.
		const substringBonus = (lc.includes(lu) || lu.includes(lc)) ? Math.min(lu.length, lc.length) * 2 : 0;
		return dist - prefixBonus - substringBonus;
	}

	let best: { name: string; score: number; dist: number } | null = null;
	for (const c of candidates) {
		const s = score(unknown, c);
		const d = distance(unknown.toLowerCase(), c.toLowerCase());
		if (best === null || s < best.score) best = { name: c, score: s, dist: d };
	}
	if (!best) return '';

	// Accept suggestion if either:
	//   - composite score is negative (strong prefix/substring match), OR
	//   - raw Levenshtein distance is small relative to name length.
	const lengthThreshold = Math.max(3, Math.floor(Math.max(unknown.length, best.name.length) / 2));
	if (best.score < 0 || best.dist <= lengthThreshold) {
		return ` (did you mean '${best.name}'?)`;
	}
	return '';
}


/** Find matching ) for a ( at startIdx. Returns index of ), or -1. */
function findMatchingParenHelper(text: string, startIdx: number): number {
	let depth = 0;
	for (let ci = startIdx; ci < text.length; ci++) {
		if (text[ci] === '(') depth++;
		if (text[ci] === ')') { depth--; if (depth === 0) return ci; }
	}
	return -1;
}

/** Check if a line looks like it could be JSON content (not Weft syntax). */
function looksLikeJson(line: string): boolean {
	if (!line) return true; // blank lines are fine inside JSON
	// JSON content: strings, numbers, bools, null, brackets, braces, colons, commas
	// NOT JSON: connections (x.y = z.w), declarations (id = Type), bare identifiers without quotes
	if (/^[a-zA-Z_]\w*\.\w+\s*=/.test(line)) return false; // connection
	if (/^[a-zA-Z_]\w*\s*=\s*[A-Z]/.test(line)) return false; // declaration
	if (/^[a-zA-Z_]\w*\s*=\s*Group/.test(line)) return false; // group
	if (line.startsWith('#')) return false; // comment
	if (line.startsWith('@')) return false; // directive
	// If it's only JSON-valid characters (after trimming), it's JSON
	// JSON lines typically contain: " { } [ ], : digits, true, false, null, whitespace
	return true;
}

/** Check if a JSON-like string has balanced brackets/braces. */
function isJsonBalanced(s: string): boolean {
	let depth = 0;
	for (const c of s) {
		if (c === '[' || c === '{') depth++;
		if (c === ']' || c === '}') depth--;
	}
	return depth === 0;
}

/** Dedent: strip common leading whitespace from all non-empty lines. */
function dedent(s: string): string {
	const lines = s.trimEnd().split('\n');
	const nonEmpty = lines.filter(l => l.trim().length > 0);
	if (nonEmpty.length === 0) return s;
	const minIndent = Math.min(...nonEmpty.map(l => l.length - l.trimStart().length));
	if (minIndent === 0) return s.trimEnd();
	return lines.map(l => l.length >= minIndent ? l.slice(minIndent) : l).join('\n');
}

/** Split on commas at depth 0 only (not inside []) */
/** Split a one-liner config body on commas, respecting quotes and all
 *  bracket depths `()` `[]` `{}`. Used by the one-liner node parser so
 *  that inline-expression values (with nested `{}`) and JSON values
 *  (with `[` `]`) stay in the same pair. Mirrors the backend's
 *  split_respecting_quotes which is brace-aware. */
function splitBraceAwareComma(s: string): string[] {
	const parts: string[] = [];
	let current = '';
	let inQuote = false;
	let depth = 0;
	for (let i = 0; i < s.length; i++) {
		const c = s[i];
		if (c === '"' && (i === 0 || s[i - 1] !== '\\')) {
			inQuote = !inQuote;
		}
		if (!inQuote) {
			if (c === '(' || c === '[' || c === '{') depth++;
			else if (c === ')' || c === ']' || c === '}') depth--;
		}
		if (c === ',' && !inQuote && depth === 0) {
			parts.push(current);
			current = '';
		} else {
			current += c;
		}
	}
	if (current) parts.push(current);
	return parts;
}

/** Split a port-list-ish string on top-level separators. Both commas and
 *  newlines act as separators at bracket depth 0, matching the backend's
 *  split_port_items which accepts comma OR newline. This lets users write
 *  multi-line port lists without trailing commas:
 *      (
 *        name: String
 *        age: Number
 *      )
 */
function splitTopLevelComma(s: string): string[] {
	const parts: string[] = [];
	let depth = 0;
	let start = 0;
	for (let i = 0; i < s.length; i++) {
		if (s[i] === '[' || s[i] === '(') depth++;
		else if (s[i] === ']' || s[i] === ')') depth--;
		else if ((s[i] === ',' || s[i] === '\n') && depth === 0) {
			parts.push(s.slice(start, i));
			start = i + 1;
		}
	}
	parts.push(s.slice(start));
	return parts;
}

/**
 * Parse a comma-separated inline port list: `port1: String, port2: Number?`.
 * Each entry is `name: Type` (required) or `name: Type?` (optional).
 * Returns one entry per item. Entries that fail to parse are returned as
 * `{ error: string }` so the caller can attach a line number and emit a
 * structured parse error (instead of silently dropping them).
 */
type PortParseResult = { port: ParsedInterfacePort } | { error: string };

function parseInlinePortList(s: string): PortParseResult[] {
	const normalized = s.trim();
	return splitTopLevelComma(normalized)
		.map(p => p.trim())
		.filter(p => p.length > 0)
		.map<PortParseResult | null>(part => {
			// Skip @require_one_of directives (handled separately by the caller).
			if (part.startsWith('@require_one_of(')) return null;
			// Split `name: Type` or bare `name`.
			const colonIdx = part.indexOf(':');
			const rawName = colonIdx >= 0 ? part.slice(0, colonIdx).trim() : part.trim();
			let typeStr = colonIdx >= 0 ? part.slice(colonIdx + 1).trim() : 'MustOverride';
			let optional = false;
			if (typeStr.endsWith('?')) {
				optional = true;
				typeStr = typeStr.slice(0, -1).trim();
			} else if (colonIdx < 0 && rawName.endsWith('?')) {
				optional = true;
			}
			const name = rawName.replace(/\?$/, '');
			if (!name || !/^[a-zA-Z_][a-zA-Z0-9_]*$/.test(name)) {
				return { error: `Invalid port declaration: '${part}'` };
			}
			return { port: { name, portType: typeStr, required: !optional, laneMode: null } as ParsedInterfacePort };
		})
		.filter((p): p is PortParseResult => p !== null);
}

/**
 * Try to parse a line as part of an in()/out() port declaration.
 *
 * Syntax:
 * - Single-line: `in(*port1, *port2: String)` or `out(result: Dict)`
 * - Multi-line: `in(\n  *port1\n  *port2\n)`
 * - Multiple in()/out() blocks allowed anywhere in node/group body
 *
 * Returns { consumed, section } -- consumed=true means the line was handled.
 */
/** Strip inline comments: everything after # at bracket depth 0. */
function stripInlineComment(line: string): string {
	let depth = 0;
	for (let i = 0; i < line.length; i++) {
		if (line[i] === '[') depth++;
		else if (line[i] === ']') depth--;
		else if (line[i] === '#' && depth === 0) return line.slice(0, i).trimEnd();
	}
	return line;
}

function tryParsePortLine(
	trimmedRaw: string,
	section: 'in' | 'out' | null,
	inPorts: ParsedInterfacePort[],
	outPorts: ParsedInterfacePort[],
	errors: WeftParseError[],
	lineNum: number,
): { consumed: boolean; section: 'in' | 'out' | null } {
	const trimmed = stripInlineComment(trimmedRaw);

	// Single-line or multi-line opener: in(...) or out(...)
	for (const [prefix, direction] of [['in(', 'in'], ['out(', 'out']] as const) {
		if (trimmed.startsWith(prefix)) {
			const rest = trimmed.slice(prefix.length);
			const target = direction === 'in' ? inPorts : outPorts;
			if (rest.endsWith(')')) {
				// Single-line: in(*port1, *port2: String)
				const inner = rest.slice(0, -1).trim();
				if (inner) {
					const parsed = parseInlinePortList(inner);
					pushPortsDeduped(parsed, direction, target, errors, lineNum);
				}
				return { consumed: true, section: null };
			} else {
				// Multi-line opener: in(
				const restTrimmed = rest.trim();
				if (restTrimmed) {
					const parsed = parseInlinePortList(restTrimmed);
					pushPortsDeduped(parsed, direction, target, errors, lineNum);
				}
				return { consumed: true, section: direction };
			}
		}
	}

	// Inside a multi-line in() or out() block
	if (section) {
		// Closing paren
		if (trimmed === ')') return { consumed: true, section: null };

		// Closing paren with ports: `*port1, *port2)`
		if (trimmed.endsWith(')')) {
			const inner = trimmed.slice(0, -1).trim();
			if (inner) {
				const target = section === 'in' ? inPorts : outPorts;
				const parsed = parseInlinePortList(inner);
				pushPortsDeduped(parsed, section, target, errors, lineNum);
			}
			return { consumed: true, section: null };
		}

		// Skip blank lines and comments inside port blocks
		if (!trimmed || trimmed.startsWith('#')) return { consumed: true, section };

		// @require_one_of directive inside port block
		if (trimmed.startsWith('@require_one_of(') && trimmed.endsWith(')')) {
			// handled at the scope level, just consume
			return { consumed: true, section };
		}
		// Port declaration line
		// no prefix characters. Port name must start with letter/underscore.
		const portMatch = trimmed.replace(/,\s*$/, '').match(/^([a-zA-Z_][a-zA-Z0-9_]*)(?:\s*:\s*(.+))?$/);
		if (portMatch) {
			const portName = portMatch[1];
			let typeStr = portMatch[2]?.trim() || 'MustOverride';
			// check for ? suffix (optional)
			let optional = false;
			if (typeStr.endsWith('?')) {
				optional = true;
				typeStr = typeStr.slice(0, -1).trim();
			}
			const port: ParsedInterfacePort = { name: portName, portType: typeStr, required: !optional, laneMode: null };
			const target = section === 'in' ? inPorts : outPorts;
			if (target.some(p => p.name === portName)) {
				errors.push({ line: lineNum, message: `Duplicate ${section} port "${portName}"` });
			} else {
				target.push(port);
			}
			return { consumed: true, section };
		}

		// Comma-separated ports on one line
		if (trimmed.includes(',')) {
			const parsed = parseInlinePortList(trimmed);
			if (parsed.length > 0) {
				const target = section === 'in' ? inPorts : outPorts;
				pushPortsDeduped(parsed, section, target, errors, lineNum);
				return { consumed: true, section };
			}
		}

		// Unknown line inside port block
		errors.push({ line: lineNum, message: `Unexpected line inside ${section}() block: ${trimmed}` });
		return { consumed: true, section };
	}

	return { consumed: false, section };
}

/** Push ports into target array, skipping duplicates, emitting errors for
 *  malformed entries returned from parseInlinePortList. */
function pushPortsDeduped(
	results: PortParseResult[],
	direction: 'in' | 'out',
	target: ParsedInterfacePort[],
	errors: WeftParseError[],
	lineNum: number,
): void {
	const seen = new Set(target.map(p => p.name));
	for (const r of results) {
		if ('error' in r) {
			errors.push({ line: lineNum, message: r.error });
			continue;
		}
		const port = r.port;
		if (seen.has(port.name)) {
			errors.push({ line: lineNum, message: `Duplicate ${direction} port "${port.name}"` });
		} else {
			target.push(port);
			seen.add(port.name);
		}
	}
}

function buildScopeChain(parentId: string | undefined): string[] {
	if (!parentId) return [];
	const parts = parentId.split('.');
	return parts.map((_, i) => parts.slice(0, i + 1).join('.'));
}

const REJECTED_CONFIG_KEYS = new Set(['mock', 'mocked']);

function isRejectedConfigKey(key: string, errors: WeftParseError[], lineNum: number): boolean {
	if (REJECTED_CONFIG_KEYS.has(key)) {
		errors.push({ line: lineNum, message: `'${key}' is not a valid config key. Use test configs for mocking.` });
		return true;
	}
	return false;
}

/** Set a config field and record its source span in one step. Used at every
 *  `config[key] = ...` call site in the parser so the editor has a single
 *  source of truth for "where does this field's value live in the source".
 *  `startLine` and `endLine` are both 1-based and inclusive. */
function setConfigField(
	config: Record<string, unknown>,
	spans: Record<string, ConfigFieldSpan>,
	errors: WeftParseError[],
	key: string,
	value: unknown,
	startLine: number,
	endLine: number,
	origin: 'inline' | 'connection' = 'inline',
): void {
	if (isRejectedConfigKey(key, errors, startLine)) return;
	config[key] = value;
	spans[key] = { startLine, endLine, origin };
}

/** Unescape backslash sequences inside a quoted string, mirroring the
 *  backend's weft_compiler::unescape helper: \n \t \r \" \\ are decoded,
 *  unknown escapes pass through verbatim (backslash kept). */
function unescapeWeftString(s: string): string {
	let out = '';
	let i = 0;
	while (i < s.length) {
		const c = s[i];
		if (c === '\\' && i + 1 < s.length) {
			const next = s[i + 1];
			if (next === 'n') { out += '\n'; i += 2; continue; }
			if (next === 't') { out += '\t'; i += 2; continue; }
			if (next === 'r') { out += '\r'; i += 2; continue; }
			if (next === '"') { out += '"'; i += 2; continue; }
			if (next === '\\') { out += '\\'; i += 2; continue; }
			out += c; out += next; i += 2; continue;
		}
		out += c; i++;
	}
	return out;
}

/** Parse the body of a quoted-label expression. Strips surrounding quotes
 *  if present, then unescapes the interior. Mirrors backend try_extract_label. */
function parseLabelValue(raw: string): string {
	const trimmed = raw.trim();
	if (trimmed.startsWith('"') && trimmed.endsWith('"') && trimmed.length >= 2) {
		return unescapeWeftString(trimmed.slice(1, -1));
	}
	return trimmed;
}

/** Parse a literal RHS value (quoted string, number, bool, JSON array/object)
 *  into a JavaScript value. Returns undefined if the input doesn't match any
 *  literal form. Mirrors backend try_parse_literal. */
function tryParseLiteral(raw: string): unknown | undefined {
	const s = raw.trim();
	if (!s) return undefined;
	if (s === 'true') return true;
	if (s === 'false') return false;
	if (s === 'null') return null;
	if (/^-?\d+(\.\d+)?$/.test(s)) return Number(s);
	if (s.startsWith('"') && s.endsWith('"') && s.length >= 2) {
		return unescapeWeftString(s.slice(1, -1));
	}
	if (s.startsWith('[') || s.startsWith('{')) {
		try { return JSON.parse(s); } catch { return undefined; }
	}
	return undefined;
}

/** Apply a connection-line literal to a target node's config. If the target
 *  exists, the value is inserted (overwriting any prior value for the same
 *  key) and its source span is recorded with `origin: 'connection'`. If the
 *  target doesn't exist, the fill is silently dropped; enrichment catches the
 *  bad target via edge-port validation elsewhere. Mirrors the backend
 *  apply_config_fill helper. */
function applyConfigFill(
	nodes: ParsedNode[],
	targetId: string,
	targetPort: string,
	value: unknown,
	startLine: number,
	endLine: number,
): void {
	const node = nodes.find(n => n.id === targetId);
	if (node) {
		node.config[targetPort] = value;
		node.configSpans[targetPort] = { startLine, endLine, origin: 'connection' };
	}
}

/** Collect all top-level child ids declared in a scope (group body or
 *  root). Starts at `start` (0-based line index) and walks until the
 *  matching closing `}` at depth 0. Returns the set of bare identifiers
 *  that appear as the left side of `id = Type...` declarations. Used for
 *  scope-correct rewriting of port-wiring connections emitted by inline
 *  bodies: only ids that are local should be prefixed with `{scopeId}.`;
 *  external refs (root scope, parent group) must stay unprefixed.
 *  Mirrors backend collect_local_child_ids. */
function collectLocalChildIds(lines: string[], start: number): Set<string> {
	const ids = new Set<string>();
	let depth = 0;
	let i = start + 1;
	while (i < lines.length) {
		const trimmed = lines[i].trim();

		// Closing `}` at depth 0 ends the scope.
		if (depth === 0 && (trimmed === '}' || trimmed.startsWith('} '))) break;

		// Top-level declaration at depth 0: `id = Type...`
		if (depth === 0) {
			const eqPos = trimmed.indexOf('=');
			if (eqPos > 0) {
				const left = trimmed.slice(0, eqPos).trim();
				const right = trimmed.slice(eqPos + 1).trim();
				if (
					left.length > 0
					&& !left.includes('.')
					&& /^[a-zA-Z_][a-zA-Z0-9_]*$/.test(left)
					&& right.length > 0
					&& /^[A-Z]/.test(right)
				) {
					ids.add(left);
				}
			}
		}

		// Track brace depth, respecting string literals. Triple-backtick
		// state is not tracked because we only care about top-level
		// declarations and backtick content cannot contain them.
		let inString: string | null = null;
		let escape = false;
		for (const c of trimmed) {
			if (escape) { escape = false; continue; }
			if (inString) {
				if (c === '\\') { escape = true; }
				else if (c === inString) { inString = null; }
				continue;
			}
			if (c === '"' || c === '\'') { inString = c; continue; }
			if (c === '{') depth++;
			if (c === '}') depth--;
		}

		i++;
	}
	return ids;
}

/** True if a source/target id refers to something local to the current
 *  scope. Local means: it IS a declared child, OR it's an anon inline id
 *  `{localId}__{field}` where localId is declared, OR it starts with
 *  `self__` (anon generated from `self.field = Type{}.port`).
 *  Everything else is treated as an external reference. Mirrors backend
 *  is_local_ref. */
function isLocalRef(id: string, localChildren: Set<string>): boolean {
	if (localChildren.has(id)) return true;
	const idx = id.indexOf('__');
	if (idx > 0) {
		const head = id.slice(0, idx);
		if (head === 'self') return true;
		if (localChildren.has(head)) return true;
	}
	return false;
}

/** True if the first `{` in `s` is followed by real config content on the
 *  same line (not just whitespace or a trailing `#` comment). Mirrors the
 *  backend first_brace_has_content_after. */
function firstBraceHasContentAfter(s: string): boolean {
	const idx = s.indexOf('{');
	if (idx < 0) return false;
	for (let i = idx + 1; i < s.length; i++) {
		const c = s[i];
		if (c === '\n') return false;
		if (c === '#') return false;
		if (c !== ' ' && c !== '\t' && c !== '\r') return true;
	}
	return false;
}

/** True if `s` has a matched `{` / `}` pair at depth 0, respecting quoted
 *  strings and triple-backtick code blocks. Mirrors the backend
 *  is_brace_balanced_respecting_quotes_and_backticks. Returns true as
 *  soon as the first opening `{` is closed. */
function isBraceBalancedRespectingQuotesAndBackticks(s: string): boolean {
	let depth = 0;
	let inString = false;
	let inBacktick = false;
	let i = 0;
	while (i < s.length) {
		if (i + 2 < s.length && s[i] === '`' && s[i + 1] === '`' && s[i + 2] === '`') {
			inBacktick = !inBacktick;
			i += 3;
			continue;
		}
		if (inBacktick) {
			i++;
			continue;
		}
		const c = s[i];
		if (c === '\\' && i + 1 < s.length) {
			i += 2;
			continue;
		}
		if (c === '"') {
			inString = !inString;
			i++;
			continue;
		}
		if (!inString) {
			if (c === '{') depth++;
			if (c === '}') {
				depth--;
				if (depth === 0) return true;
			}
		}
		i++;
	}
	return depth === 0;
}

/** Parse a one-liner-style body that spans multiple lines because a value
 *  (typically a triple-backtick block or multi-line JSON) extends across
 *  lines. The body text has its outer braces already stripped. Each
 *  `key: value` pair is parsed into the host's config, anon children
 *  (inline expressions) are pushed to `nodes`, and port wirings are
 *  pushed to `connections`. Mirrors the one-liner pair processing in
 *  the top-level declaration parser, with triple-backtick and JSON
 *  recognition for values that span newlines.
 */
function parseOneLinerMultilineBody(
	body: string,
	hostNodeId: string,
	config: Record<string, unknown>,
	configSpans: Record<string, ConfigFieldSpan>,
	nodes: ParsedNode[],
	connections: ParsedConnection[],
	errors: WeftParseError[],
	lineNum: number,
	rawLine: string,
): void {
	// Split by top-level commas OR newlines, respecting quotes and bracket
	// depth so that triple-backtick and JSON values stay in one pair.
	const pairs: string[] = [];
	let current = '';
	let inQuote = false;
	let inBacktick = false;
	let depth = 0;
	let j = 0;
	while (j < body.length) {
		// Triple-backtick toggle.
		if (j + 2 < body.length && body[j] === '`' && body[j + 1] === '`' && body[j + 2] === '`') {
			current += '```';
			inBacktick = !inBacktick;
			j += 3;
			continue;
		}
		const c = body[j];
		if (inBacktick) {
			current += c;
			j++;
			continue;
		}
		if (c === '"' && (j === 0 || body[j - 1] !== '\\')) inQuote = !inQuote;
		if (!inQuote) {
			if (c === '[' || c === '{' || c === '(') depth++;
			else if (c === ']' || c === '}' || c === ')') depth--;
		}
		if ((c === ',' || c === '\n') && !inQuote && depth === 0) {
			if (current.trim()) pairs.push(current.trim());
			current = '';
			j++;
			continue;
		}
		current += c;
		j++;
	}
	if (current.trim()) pairs.push(current.trim());

	for (const pair of pairs) {
		const colonIdx = pair.indexOf(':');
		if (colonIdx < 0) continue;
		const key = pair.slice(0, colonIdx).trim();
		const rawValue = pair.slice(colonIdx + 1).trim();
		if (!/^[a-zA-Z_][a-zA-Z0-9_]*$/.test(key)) continue;

		// Triple-backtick multi-line value: `code: ``` ... ``` `
		if (rawValue.startsWith('```') && rawValue.endsWith('```') && rawValue.length >= 6) {
			let inner = rawValue.slice(3, -3);
			// Strip a single leading/trailing newline.
			if (inner.startsWith('\n')) inner = inner.slice(1);
			if (inner.endsWith('\n')) inner = inner.slice(0, -1);
			const dedented = dedent(inner);
			const unescaped = dedented.replace(/\\```/g, '```').replace(/\\`/g, '`');
			setConfigField(config, configSpans, errors, key, unescaped, lineNum, lineNum);
			continue;
		}

		// Inline expression first (covers bare `Type.port`).
		if (looksLikeInlineStart(rawValue)) {
			const synth = [rawValue];
			const rootInlineScope: InlineScope = { nodes: [], connections: [] };
			tryParseInlineExpression(synth, 0, 0, hostNodeId, key, rootInlineScope, errors);
			for (const child of rootInlineScope.nodes) nodes.push(child);
			for (const conn of rootInlineScope.connections) {
				connections.push({ ...conn, line: lineNum, rawText: rawLine, scopeId: '__root__' });
			}
			continue;
		}

		// Port wiring: unquoted dotted ref.
		if (looksLikeDottedRef(rawValue)) {
			const dotIdx = rawValue.indexOf('.');
			connections.push({
				sourceId: rawValue.slice(0, dotIdx),
				sourcePort: rawValue.slice(dotIdx + 1),
				targetId: hostNodeId,
				targetPort: key,
				line: lineNum,
				rawText: rawLine,
				scopeId: '__root__',
			});
			continue;
		}

		if (key === 'label') {
			// Label values shouldn't occur here normally, skip.
			continue;
		}

		// Plain config value (possibly multi-line JSON).
		setConfigField(config, configSpans, errors, key, parseConfigValue(rawValue, errors, lineNum, key), lineNum, lineNum);
	}
}

/** Collect a multi-line literal RHS for a connection line. Handles two
 *  forms:
 *    - triple-backtick: `target.port = ```\n...\n``` ` (multi-line string).
 *      Value is dedented and unescaped (`\``` → `` ``` ``, `\`` → `` ` ``).
 *    - multi-line JSON: `target.port = { ... }` or `= [ ... ]` with braces
 *      spread across lines. Depth-counted; stops at matching close.
 *
 *  Returns the parsed value and the next line index (1-past the literal),
 *  or null if `rhs` is neither form. Mirrors the logic in parse_config_block's
 *  multi-line handling.
 */
function tryCollectMultilineLiteralRhs(
	lines: string[],
	startLineIdx: number,
	rhs: string,
): { value: unknown; nextLineIdx: number } | null {
	// Triple-backtick form.
	if (rhs.startsWith('```')) {
		const afterBt = rhs.slice(3);
		// Inline `target.port = ```content``` ` on a single line.
		if (afterBt.endsWith('```') && afterBt.length > 3) {
			return { value: afterBt.slice(0, -3), nextLineIdx: startLineIdx + 1 };
		}
		let value = afterBt;
		let i = startLineIdx + 1;
		while (i < lines.length) {
			const mlTrimmed = lines[i].trim();
			// A line closes the heredoc only if the trailing ``` is NOT
			// preceded by `\`; `\```` is an escaped literal.
			const closesBare = mlTrimmed === '```';
			const closesSuffix = !closesBare
				&& mlTrimmed.endsWith('```')
				&& mlTrimmed.slice(0, -3).slice(-1) !== '\\';
			if (closesBare || closesSuffix) {
				const beforeClose = closesBare ? '' : mlTrimmed.slice(0, -3);
				if (beforeClose) {
					if (value) value += '\n';
					value += beforeClose;
				}
				i++;
				break;
			}
			if (value) value += '\n';
			value += lines[i];
			i++;
		}
		const dedented = dedent(value);
		const unescaped = dedented.replace(/\\```/g, '```').replace(/\\`/g, '`');
		return { value: unescaped, nextLineIdx: i };
	}

	// Multi-line JSON form.
	if ((rhs.startsWith('[') || rhs.startsWith('{')) && !isJsonBalanced(rhs)) {
		let collected = rhs;
		let depth = 0;
		for (const c of rhs) {
			if (c === '[' || c === '{') depth++;
			if (c === ']' || c === '}') depth--;
		}
		let i = startLineIdx + 1;
		let hitBoundary = false;
		const startI = startLineIdx;
		while (i < lines.length && depth > 0) {
			const ml = lines[i].trim();
			if (i - startI > 500) { hitBoundary = true; break; }
			if (!looksLikeJson(ml)) { hitBoundary = true; break; }
			collected += '\n' + ml;
			for (const c of ml) {
				if (c === '[' || c === '{') depth++;
				if (c === ']' || c === '}') depth--;
			}
			if (depth <= 0) { i++; break; }
			i++;
		}
		if (depth > 0 || hitBoundary) {
			return { value: collected, nextLineIdx: i };
		}
		try {
			return { value: JSON.parse(collected), nextLineIdx: i };
		} catch {
			return { value: collected, nextLineIdx: i };
		}
	}

	return null;
}

function parseConfigValue(rawValue: string, errors: WeftParseError[], lineNum: number, key: string): unknown {
	if (rawValue === 'true') return true;
	if (rawValue === 'false') return false;
	if (rawValue === 'null') return null;
	if (/^-?\d+(\.\d+)?$/.test(rawValue)) return Number(rawValue);
	if (rawValue.startsWith('"') && rawValue.endsWith('"')) {
		return unescapeWeftString(rawValue.slice(1, -1));
	}
	if (rawValue.startsWith('[') || rawValue.startsWith('{')) {
		try { return JSON.parse(rawValue); }
		catch (e) {
			const preview = rawValue.length > 80 ? rawValue.slice(0, 80) + '...' : rawValue;
			const jsonError = e instanceof SyntaxError ? e.message : 'invalid JSON';
			errors.push({ line: lineNum, message: `Invalid JSON for "${key}": ${jsonError}. Starts with: ${preview.replace(/\n/g, ' ')}` });
			return rawValue;
		}
	}
	return rawValue;
}

// ─── Inline Expressions ─────────────────────────────────────────────────────
//
// Mirrors the backend inline parser in weft_compiler.rs. Inline expressions
// let the user declare a short-lived child node directly in the position
// where its output would otherwise be wired:
//
//     target.port = Template { template: "hi" }.text
//
//     my_llm = LlmInference {
//       systemPrompt: Template { template: "{{x}}" x: other.value }.text
//     }
//
// Accumulator for inline children emitted during parse. The caller (scope
// parser) merges these into the current scope's nodes + connections.
interface InlineScope {
	nodes: ParsedNode[];
	connections: ParsedConnection[];
}

/** Check if a string value looks like the start of an inline node expression.
 *  Accepted forms (after stripping leading whitespace):
 *    Type ( ... ) -> ( ... ) { ... }.port   // full form
 *    Type { ... }.port                      // config-only form
 *    Type ( ... ) -> ( ... ).port           // ports-only form
 *    Type.port                              // bare form: default config
 */
function looksLikeInlineStart(s: string): boolean {
	const t = s.replace(/^\s+/, '');
	if (!t) return false;
	const first = t[0];
	if (first < 'A' || first > 'Z') return false;
	let identLen = 0;
	for (const c of t) {
		if (/[A-Za-z0-9_]/.test(c)) identLen++;
		else break;
	}
	const rest = t.slice(identLen).replace(/^\s+/, '');
	if (rest.startsWith('(') || rest.startsWith('{') || rest.startsWith('->')) {
		return true;
	}
	// Bare form `Type.port`: the dot must be followed by an identifier
	// character so we don't catch `Type.` or a trailing dot.
	if (rest.startsWith('.')) {
		const afterDot = rest.slice(1);
		return /^[A-Za-z_]/.test(afterDot);
	}
	return false;
}

/** A dotted ref is 2+ identifier segments joined by `.`, no quotes, no
 *  whitespace, no digits-only segments (so `3.14` is NOT a dotted ref).
 *  Used inside inline bodies to distinguish port wirings from literal values. */
function looksLikeDottedRef(s: string): boolean {
	const t = s.trim();
	if (!t) return false;
	if (t.includes('"') || t.includes("'")) return false;
	// Exactly one dot: `node.port`. Multi-dot refs like `a.b.c` are not
	// valid port references.
	const dotCount = (t.match(/\./g) || []).length;
	if (dotCount !== 1) return false;
	return t.split('.').every(seg =>
		seg.length > 0 && /^[A-Za-z_][A-Za-z0-9_]*$/.test(seg)
	);
}

/** Find the position of the `}` that matches the opening `{` at the start
 *  of `s`. Respects string literals so `{`/`}` inside quotes don't break
 *  matching. Returns -1 if there's no matching close on this line. */
function findMatchingBraceOnLine(s: string): number {
	if (!s.startsWith('{')) return -1;
	let depth = 0;
	let inString: string | null = null;
	let escape = false;
	for (let i = 0; i < s.length; i++) {
		const c = s[i];
		if (escape) { escape = false; continue; }
		if (inString) {
			if (c === '\\') { escape = true; continue; }
			if (c === inString) inString = null;
			continue;
		}
		if (c === '"' || c === "'") { inString = c; continue; }
		if (c === '{') depth++;
		else if (c === '}') {
			depth--;
			if (depth === 0) return i;
		}
	}
	return -1;
}

/** Parse a `.portName` suffix. Returns the port name on success. */
function parseInlineDotPort(s: string): string | null {
	const t = s.trim();
	if (!t.startsWith('.')) return null;
	const rest = t.slice(1);
	const m = rest.match(/^([A-Za-z0-9_]+)\s*$/);
	if (!m) return null;
	return m[1];
}

/** Port signature parser for an inline expression's header.
 *  Given `after_type` (text after the node type, e.g. `(x: String) -> (y: String) {`),
 *  returns the parsed in/out ports and the leftover string starting at `{`. */
function parseInlinePortSignature(afterType: string, lineNum: number, errors: WeftParseError[]): {
	inPorts: ParsedInterfacePort[];
	outPorts: ParsedInterfacePort[];
	afterPorts: string;
} {
	const inPorts: ParsedInterfacePort[] = [];
	const outPorts: ParsedInterfacePort[] = [];
	let afterPorts = afterType;
	// Keep newlines in the text: splitTopLevelComma handles both commas and
	// newlines as separators, matching backend split_port_items.
	const text = afterType;
	if (text.startsWith('(')) {
		const closeIdx = findMatchingParenHelper(text, 0);
		if (closeIdx < 0) {
			errors.push({ line: lineNum, message: 'Unclosed input port list in inline expression' });
			return { inPorts, outPorts, afterPorts: '' };
		}
		const content = text.slice(1, closeIdx);
		for (const item of splitTopLevelComma(content).map(s => s.trim()).filter(s => s && !s.startsWith('#'))) {
			if (item.startsWith('@require_one_of(')) continue;
			pushPortsDeduped(parseInlinePortList(item), 'in', inPorts, errors, lineNum);
		}
		const rest = text.slice(closeIdx + 1).trim();
		if (rest.startsWith('->')) {
			const arrowRest = rest.slice(2).trim();
			if (arrowRest.startsWith('(')) {
				const outClose = findMatchingParenHelper(arrowRest, 0);
				if (outClose < 0) {
					errors.push({ line: lineNum, message: 'Unclosed output port list in inline expression' });
					return { inPorts, outPorts, afterPorts: '' };
				}
				const outContent = arrowRest.slice(1, outClose);
				for (const item of splitTopLevelComma(outContent).map(s => s.trim()).filter(s => s && !s.startsWith('#'))) {
					pushPortsDeduped(parseInlinePortList(item), 'out', outPorts, errors, lineNum);
				}
				afterPorts = arrowRest.slice(outClose + 1).trim();
			} else {
				afterPorts = rest;
			}
		} else {
			afterPorts = rest;
		}
	} else if (text.startsWith('->')) {
		const arrowRest = text.slice(2).trim();
		if (arrowRest.startsWith('(')) {
			const outClose = findMatchingParenHelper(arrowRest, 0);
			if (outClose < 0) {
				errors.push({ line: lineNum, message: 'Unclosed output port list in inline expression' });
				return { inPorts, outPorts, afterPorts: '' };
			}
			const outContent = arrowRest.slice(1, outClose);
			for (const item of splitTopLevelComma(outContent).map(s => s.trim()).filter(s => s && !s.startsWith('#'))) {
				pushPortsDeduped(parseInlinePortList(item), 'out', outPorts, errors, lineNum);
			}
			afterPorts = arrowRest.slice(outClose + 1).trim();
		} else {
			afterPorts = text;
		}
	}
	return { inPorts, outPorts, afterPorts };
}

/** Parse an inline expression from `lines[startLine]` starting at `startCol`.
 *  Appends the child node + connection to `inlineScope` and returns the index
 *  of the first line AFTER the inline expression (past `.portName`). Returns
 *  null on parse failure (errors are pushed). */
function tryParseInlineExpression(
	lines: string[],
	startLine: number,
	startCol: number,
	parentId: string,
	fieldKey: string,
	inlineScope: InlineScope,
	errors: WeftParseError[],
): number | null {
	const firstLine = lines[startLine];
	const lineNum = startLine + 1;
	const afterStart = firstLine.slice(startCol).replace(/^\s+/, '');

	// Extract the type name.
	const typeMatch = afterStart.match(/^([A-Z][A-Za-z0-9_]*)/);
	if (!typeMatch) return null;
	const nodeType = typeMatch[1];
	if (nodeType === 'Group') {
		errors.push({ line: lineNum, message: 'Groups cannot be inlined' });
		return null;
	}
	let afterType = afterStart.slice(nodeType.length).replace(/^\s+/, '');

	// Collect header across multiple lines if port signature spans them.
	let headerEndLine = startLine;
	if (afterType.startsWith('(') || afterType.startsWith('->')) {
		let parenDepth = 0;
		for (const c of afterType) { if (c === '(') parenDepth++; if (c === ')') parenDepth--; }
		let collected = afterType;
		while (parenDepth > 0 && headerEndLine + 1 < lines.length) {
			headerEndLine++;
			const nextLine = lines[headerEndLine].trim();
			collected += '\n' + nextLine;
			for (const c of nextLine) { if (c === '(') parenDepth++; if (c === ')') parenDepth--; }
		}
		// Check for -> on the next line after balanced input parens.
		if (parenDepth === 0 && !collected.includes('->') && headerEndLine + 1 < lines.length) {
			const peek = lines[headerEndLine + 1]?.trim() || '';
			if (peek.startsWith('->')) {
				headerEndLine++;
				collected += '\n' + peek;
				for (const c of peek) { if (c === '(') parenDepth++; if (c === ')') parenDepth--; }
				while (parenDepth > 0 && headerEndLine + 1 < lines.length) {
					headerEndLine++;
					const nextLine = lines[headerEndLine].trim();
					collected += '\n' + nextLine;
					for (const c of nextLine) { if (c === '(') parenDepth++; if (c === ')') parenDepth--; }
				}
			}
		}
		afterType = collected;
		// Also grab `{` from next line if needed.
		if (!afterType.includes('{') && headerEndLine + 1 < lines.length) {
			const peekBrace = lines[headerEndLine + 1]?.trim() || '';
			if (peekBrace.startsWith('{')) {
				headerEndLine++;
				afterType += '\n' + peekBrace;
			}
		}
	}

	// Parse the port signature.
	const { inPorts, outPorts, afterPorts } = parseInlinePortSignature(afterType, lineNum, errors);
	const afterPortsTrimmed = afterPorts.trim();

	// Anon id for this inline expression.
	const anonId = `${parentId}__${fieldKey}`;

	// Bare form `Type.port`: no body, no config, default construction.
	// `afterPortsTrimmed` starts with `.portName` directly.
	if (afterPortsTrimmed.startsWith('.')) {
		const port = parseInlineDotPort(afterPortsTrimmed);
		if (!port) {
			errors.push({ line: lineNum, message: `Expected '.portName' in bare inline expression, got: '${afterPortsTrimmed}'` });
			return null;
		}
		inlineScope.nodes.push({
			id: anonId,
			nodeType,
			label: null,
			config: {},
			configSpans: {},
			parentId: undefined,
			inPorts,
			outPorts,
			oneOfRequired: [],
			startLine: startLine + 1,
			endLine: startLine + 1,
			rawLines: [lines[startLine]],
		});
		inlineScope.connections.push({
			sourceId: anonId,
			sourcePort: port,
			targetId: parentId,
			targetPort: fieldKey,
			line: lineNum,
			rawText: lines[startLine],
		});
		return startLine + 1;
	}

	// Body: either one-liner `{ ... }` or multi-line `{` + body + `}`.
	let config: Record<string, unknown> = {};
	let configSpans: Record<string, ConfigFieldSpan> = {};
	let label: string | null = null;
	let bodyEndLine = headerEndLine;

	if (!afterPortsTrimmed.startsWith('{')) {
		errors.push({ line: lineNum, message: `Expected '{' in inline expression, got: ${afterPortsTrimmed}` });
		return null;
	}

	if (afterPortsTrimmed === '{') {
		// Multi-line body. Delegate to a recursive call of parseNodeBlockBody
		// so nested inlines inside this body are handled naturally.
		// Body starts at headerEndLine + 1 (parseNodeBlockBody takes the line
		// AFTER the opening `{`).
		const bodyResult = parseInlineBodyBlock(lines, headerEndLine + 1, anonId, inlineScope, errors);
		config = bodyResult.config;
		configSpans = bodyResult.configSpans;
		label = bodyResult.label;
		bodyEndLine = bodyResult.closeLine;
	} else {
		// One-liner: find matching `}` on the same "line" (may actually span
		// across lines if headerEndLine advanced for multi-line port sig).
		// Since afterPortsTrimmed is derived from afterType which may include
		// joined lines, we search within it.
		const closePos = findMatchingBraceOnLine(afterPortsTrimmed);
		if (closePos < 0) {
			errors.push({ line: lineNum, message: 'Unclosed inline expression body' });
			return null;
		}
		const bodyContent = afterPortsTrimmed.slice(1, closePos).trim();
		if (bodyContent) {
			const pairs: string[] = [];
			let current = '';
			let inQuote = false;
			for (let ci = 0; ci < bodyContent.length; ci++) {
				const ch = bodyContent[ci];
				if (ch === '"' && (ci === 0 || bodyContent[ci - 1] !== '\\')) inQuote = !inQuote;
				if (ch === ',' && !inQuote) { pairs.push(current.trim()); current = ''; }
				else { current += ch; }
			}
			if (current.trim()) pairs.push(current.trim());
			for (const pair of pairs) {
				const m = pair.match(/^([a-zA-Z_][a-zA-Z0-9_]*)\s*:\s*(.+)$/);
				if (!m) continue;
				const k = m[1], v = m[2].trim();
				if (k === 'label') {
					label = parseLabelValue(v);
					configSpans.label = { startLine: lineNum, endLine: lineNum, origin: 'inline' };
				}
				else if (looksLikeDottedRef(v)) {
					const dotIdx = v.indexOf('.');
					inlineScope.connections.push({
						sourceId: v.slice(0, dotIdx),
						sourcePort: v.slice(dotIdx + 1),
						targetId: anonId,
						targetPort: k,
						line: lineNum,
						rawText: lines[startLine],
					});
				}
				else {
					// One-liner body pair: the whole anon is on `lineNum`, so
					// the field's span is that single line.
					setConfigField(config, configSpans, errors, k, parseConfigValue(v, errors, lineNum, k), lineNum, lineNum);
				}
			}
		}
		bodyEndLine = headerEndLine;
	}

	// After the closing `}` we require `.portName`. Parse from whatever's
	// left on bodyEndLine or the next line.
	const closeLineText = lines[bodyEndLine];
	const closeBracePos = closeLineText.lastIndexOf('}');
	const afterBrace = closeBracePos >= 0 ? closeLineText.slice(closeBracePos + 1).trim() : '';

	// Forbid post-config outputs.
	if (afterBrace.startsWith('->')) {
		errors.push({ line: bodyEndLine + 1, message: 'Inline expressions cannot declare post-config outputs; declare the node with a name instead' });
		return null;
	}

	let outputPort: string | null = null;
	let nextLine = bodyEndLine + 1;
	const dp = parseInlineDotPort(afterBrace);
	if (dp) {
		outputPort = dp;
	} else if (afterBrace === '') {
		// Look on the next line for `.portName`.
		if (bodyEndLine + 1 < lines.length) {
			const next = lines[bodyEndLine + 1].trim();
			if (next.startsWith('->')) {
				errors.push({ line: bodyEndLine + 2, message: 'Inline expressions cannot declare post-config outputs' });
				return null;
			}
			const dp2 = parseInlineDotPort(next);
			if (dp2) {
				outputPort = dp2;
				nextLine = bodyEndLine + 2;
			}
		}
	}
	if (!outputPort) {
		errors.push({ line: bodyEndLine + 1, message: "Inline expression missing required '.portName' after closing '}'" });
		return null;
	}

	// Emit the anon node. Ports come from the explicit signature (the
	// `Type(x: String) { ... }` form) and from catalog defaults at
	// enrichment. Port-wiring assignments inside the body (`x: src.value`)
	// do NOT synthesize ports here: the rule is "edges require a
	// pre-existing, pre-typed port". Literal assignments (`x: "hi"`) may
	// synthesize a port at enrichment via inferTypeFromValue, gated on
	// the catalog type's canAddInputPorts feature.
	inlineScope.nodes.push({
		id: anonId,
		nodeType,
		label,
		config,
		configSpans,
		parentId: undefined,
		startLine: startLine + 1,
		endLine: bodyEndLine + 1,
		rawLines: lines.slice(startLine, bodyEndLine + 1),
		inPorts,
		outPorts,
		oneOfRequired: [],
	});
	// Emit the connection from anon.outputPort → parent.fieldKey.
	inlineScope.connections.push({
		sourceId: anonId,
		sourcePort: outputPort,
		targetId: parentId,
		targetPort: fieldKey,
		line: startLine + 1,
		rawText: lines[startLine],
	});

	return nextLine;
}

/** Parse the body of an inline expression: a `{` block starting at `startIdx`.
 *  Mirrors parseNodeBlockBody but:
 *   - When a `key: value` line has an unquoted dotted-ref value, it's treated
 *     as a port wiring (emits an edge into inlineScope instead of storing).
 *   - When a `key: Type ...` line starts an inline, it recurses via
 *     tryParseInlineExpression.
 *   - The closing `}` may be followed by `.portName` (the caller consumes it).
 *  Returns { config, label, closeLine }. closeLine is the index of the line
 *  containing the matching `}`. */
function parseInlineBodyBlock(
	lines: string[],
	startIdx: number,
	parentAnonId: string,
	inlineScope: InlineScope,
	errors: WeftParseError[],
): { config: Record<string, unknown>; configSpans: Record<string, ConfigFieldSpan>; label: string | null; closeLine: number } {
	const config: Record<string, unknown> = {};
	const configSpans: Record<string, ConfigFieldSpan> = {};
	let label: string | null = null;
	let inMultiLine = false;
	let mlKey = '';
	let mlValue = '';
	let mlDelimiter = '';
	let mlStartLine = 0; // 1-based line where the multi-line value began
	let i = startIdx;

	while (i < lines.length) {
		const nLine = lines[i];
		const nTrimmed = nLine.trim();
		const nLineNum = i + 1;

		if (inMultiLine) {
			// A line closes the heredoc if its trimmed form is exactly
			// `mlDelimiter` (bare close) or ends with `mlDelimiter` preceded
			// by something that isn't `\`. `\```` is an escaped literal and
			// is appended as content, not treated as a terminator. The
			// terminator decoder further down (`replace(/\\```/g, '```')`)
			// strips the backslash in the final value.
			const closesBare = nTrimmed === mlDelimiter;
			const closesSuffix = !closesBare
				&& nTrimmed.endsWith(mlDelimiter)
				&& nTrimmed.slice(0, -mlDelimiter.length).slice(-1) !== '\\';
			if (closesBare) {
				let finalVal = mlValue.trimEnd();
				if (mlDelimiter === '```') { finalVal = dedent(finalVal); finalVal = finalVal.replace(/\\```/g, '```').replace(/\\`/g, '`'); }
				setConfigField(config, configSpans, errors, mlKey, finalVal, mlStartLine, nLineNum);
				inMultiLine = false; mlKey = ''; mlValue = ''; mlDelimiter = ''; mlStartLine = 0;
			} else if (closesSuffix) {
				const lastContent = nLine.slice(0, nLine.lastIndexOf(mlDelimiter));
				if (lastContent.trim()) mlValue += (mlValue ? '\n' : '') + lastContent;
				let finalVal = mlValue.trimEnd();
				if (mlDelimiter === '```') { finalVal = dedent(finalVal); finalVal = finalVal.replace(/\\```/g, '```').replace(/\\`/g, '`'); }
				setConfigField(config, configSpans, errors, mlKey, finalVal, mlStartLine, nLineNum);
				inMultiLine = false; mlKey = ''; mlValue = ''; mlDelimiter = ''; mlStartLine = 0;
			} else {
				mlValue += (mlValue ? '\n' : '') + nLine;
			}
			i++; continue;
		}

		// Closing `}` or `}.port` ends the inline body.
		if (nTrimmed === '}' || nTrimmed.startsWith('}.') || nTrimmed.startsWith('} ')) {
			return { config, configSpans, label, closeLine: i };
		}
		if (nTrimmed === '' || nTrimmed.startsWith('#')) { i++; continue; }

		// Port wiring: `key: source.port` with unquoted dotted ref.
		const kvMatch = nTrimmed.match(/^([a-zA-Z_][a-zA-Z0-9_]*)\s*:\s*(.+)$/);
		if (kvMatch) {
			const key = kvMatch[1];
			const rawValue = kvMatch[2].trim();

			// Inline expression inside this body: key: Type { ... }.port
			if (looksLikeInlineStart(rawValue)) {
				const rawColon = nLine.indexOf(':');
				const next = tryParseInlineExpression(lines, i, rawColon + 1, parentAnonId, key, inlineScope, errors);
				if (next !== null) { i = next; continue; }
				// Parse error already pushed: skip this line to avoid cascade.
				i++; continue;
			}

			// Port wiring via unquoted dotted ref.
			if (looksLikeDottedRef(rawValue)) {
				const dotIdx = rawValue.indexOf('.');
				inlineScope.connections.push({
					sourceId: rawValue.slice(0, dotIdx),
					sourcePort: rawValue.slice(dotIdx + 1),
					targetId: parentAnonId,
					targetPort: key,
					line: nLineNum,
					rawText: nLine,
				});
				i++; continue;
			}

			// Label shorthand.
			if (key === 'label') {
				label = parseLabelValue(rawValue);
				configSpans.label = { startLine: nLineNum, endLine: nLineNum, origin: 'inline' };
				i++; continue;
			}

			// Triple backtick start.
			if (rawValue.startsWith('```')) {
				const afterBt = rawValue.slice(3);
				if (afterBt.endsWith('```') && afterBt.length > 3) {
					const inlineVal = afterBt.slice(0, -3);
					setConfigField(config, configSpans, errors, key, inlineVal, nLineNum, nLineNum);
					i++; continue;
				}
				inMultiLine = true;
				mlKey = key;
				mlValue = afterBt;
				mlDelimiter = '```';
				mlStartLine = nLineNum;
				i++; continue;
			}

			// Multi-line JSON.
			if ((rawValue.startsWith('[') || rawValue.startsWith('{')) && !isJsonBalanced(rawValue)) {
				const startLineNum = nLineNum;
				let collected = rawValue;
				let depth = 0;
				for (const c of rawValue) { if (c === '[' || c === '{') depth++; if (c === ']' || c === '}') depth--; }
				i++;
				let endLineNum = startLineNum;
				while (i < lines.length && depth > 0) {
					const ml = lines[i].trim();
					collected += '\n' + ml;
					for (const c of ml) { if (c === '[' || c === '{') depth++; if (c === ']' || c === '}') depth--; }
					endLineNum = i + 1;
					i++;
					if (depth === 0) break;
				}
				setConfigField(config, configSpans, errors, key, parseConfigValue(collected, errors, startLineNum, key), startLineNum, endLineNum);
				continue;
			}

			// Literal config value (single line).
			setConfigField(config, configSpans, errors, key, parseConfigValue(rawValue, errors, nLineNum, key), nLineNum, nLineNum);
			i++; continue;
		}

		// Unknown line, skip.
		i++;
	}

	// Reached end of file without closing brace.
	errors.push({ line: startIdx + 1, message: 'Unclosed inline expression body' });
	return { config, configSpans, label, closeLine: i - 1 };
}

/**
 * Parse a multi-line node config block starting AFTER the opening `{`.
 * Handles ```...``` multi-line values, label, JSON, string escapes.
 * Returns the index of the closing `}` line (caller should advance past it).
 */
function parseNodeBlockBody(
	lines: string[],
	startIdx: number,
	errors: WeftParseError[],
	hostNodeId: string,
	inlineScope: InlineScope,
): { config: Record<string, unknown>; configSpans: Record<string, ConfigFieldSpan>; label: string | null; inPorts: ParsedInterfacePort[]; outPorts: ParsedInterfacePort[]; oneOfRequired: string[][]; endIdx: number } {
	const config: Record<string, unknown> = {};
	const configSpans: Record<string, ConfigFieldSpan> = {};
	let label: string | null = null;
	const inPorts: ParsedInterfacePort[] = [];
	const outPorts: ParsedInterfacePort[] = [];
	const oneOfRequired: string[][] = [];
	let section: 'in' | 'out' | null = null;
	let inMultiLine = false;
	let mlKey = '';
	let mlValue = '';
	let mlDelimiter = '';
	let mlStartLine = 0;
	let i = startIdx;

	while (i < lines.length) {
		const nLine = lines[i];
		const nTrimmed = nLine.trim();
		const nLineNum = i + 1;

		// Handle multi-line values (``` ... ``` blocks)
		if (inMultiLine) {
			// A line closes the heredoc if its trimmed form is exactly
			// `mlDelimiter` (bare close) or ends with `mlDelimiter` preceded
			// by something that isn't `\`. `\```` is an escaped literal and
			// is appended as content, not treated as a terminator. The
			// terminator decoder further down (`replace(/\\```/g, '```')`)
			// strips the backslash in the final value.
			const closesBare = nTrimmed === mlDelimiter;
			const closesSuffix = !closesBare
				&& nTrimmed.endsWith(mlDelimiter)
				&& nTrimmed.slice(0, -mlDelimiter.length).slice(-1) !== '\\';
			if (closesBare) {
				let finalVal = mlValue.trimEnd();
				if (mlDelimiter === '```') { finalVal = dedent(finalVal); finalVal = finalVal.replace(/\\```/g, '```').replace(/\\`/g, '`'); }
				setConfigField(config, configSpans, errors, mlKey, finalVal, mlStartLine, nLineNum);
				inMultiLine = false; mlKey = ''; mlValue = ''; mlDelimiter = ''; mlStartLine = 0;
			} else if (closesSuffix) {
				const lastContent = nLine.slice(0, nLine.lastIndexOf(mlDelimiter));
				if (lastContent.trim()) mlValue += (mlValue ? '\n' : '') + lastContent;
				let finalVal = mlValue.trimEnd();
				if (mlDelimiter === '```') { finalVal = dedent(finalVal); finalVal = finalVal.replace(/\\```/g, '```').replace(/\\`/g, '`'); }
				setConfigField(config, configSpans, errors, mlKey, finalVal, mlStartLine, nLineNum);
				inMultiLine = false; mlKey = ''; mlValue = ''; mlDelimiter = ''; mlStartLine = 0;
			} else {
				mlValue += (mlValue ? '\n' : '') + nLine;
			}
			i++; continue;
		}

		if (nTrimmed === '}') return { config, configSpans, label, inPorts, outPorts, oneOfRequired, endIdx: i };
		// Post-config output ports: } -> (outputs) or } -> (\n outputs \n)
		if (nTrimmed.startsWith('}') && (nTrimmed.startsWith('} ->') || nTrimmed === '}')) {
			// Already handled bare '}' above; this catches '} -> ...'
			if (nTrimmed.startsWith('} ->')) {
				const arrowRest = nTrimmed.slice(4).trim();
				// Parse output ports from the remainder
				let outSig = arrowRest;
				let outEnd = i;
				// Collect multi-line if parens not balanced
				let parenDepth = 0;
				for (const c of outSig) { if (c === '(') parenDepth++; if (c === ')') parenDepth--; }
				while (parenDepth > 0 && outEnd + 1 < lines.length) {
					outEnd++;
					outSig += ' ' + lines[outEnd].trim();
					for (const c of lines[outEnd].trim()) { if (c === '(') parenDepth++; if (c === ')') parenDepth--; }
				}
				if (outSig.startsWith('(')) {
					const closeIdx = findMatchingParenHelper(outSig, 0);
					if (closeIdx > 0) {
						const content = outSig.slice(1, closeIdx);
						for (const item of splitTopLevelComma(content).map(s => s.trim()).filter(s => s && !s.startsWith('#'))) {
							if (item.startsWith('@require_one_of(')) {
								errors.push({ line: nLineNum, message: '@require_one_of is only valid in input port lists, not outputs' });
								continue;
							}
							pushPortsDeduped(parseInlinePortList(item), 'out', outPorts, errors, nLineNum);
						}
					}
				}
				return { config, configSpans, label, inPorts, outPorts, oneOfRequired, endIdx: outEnd };
			}
		}
		if (nTrimmed === '' || nTrimmed.startsWith('#')) { i++; continue; }

		// in:/out: port sections (all three forms)
		const portResult = tryParsePortLine(nTrimmed, section, inPorts, outPorts, errors, nLineNum);
		section = portResult.section;
		if (portResult.consumed) { i++; continue; }

		const labelMatch = nTrimmed.match(/^label\s*:\s*(.+)$/);
		if (labelMatch) {
			label = parseLabelValue(labelMatch[1]);
			configSpans.label = { startLine: nLineNum, endLine: nLineNum, origin: 'inline' };
			i++; continue;
		}

		// Inline expression: `key: Type { ... }.port` or bare `Type.port`.
		// Must be checked BEFORE port wiring because `Type.port` also looks
		// like a dotted ref. Delegates to tryParseInlineExpression which
		// handles all inline forms + nested inlines recursively.
		const inlineCandidate = nTrimmed.match(/^([a-zA-Z_][a-zA-Z0-9_]*)\s*:\s*(.+)$/);
		if (inlineCandidate && looksLikeInlineStart(inlineCandidate[2])) {
			const key = inlineCandidate[1];
			const rawColon = nLine.indexOf(':');
			const next = tryParseInlineExpression(lines, i, rawColon + 1, hostNodeId, key, inlineScope, errors);
			if (next !== null) { i = next; continue; }
			i++; continue;
		}

		// Port wiring: `key: source.port` where source.port is an unquoted
		// dotted ref. Emits an edge from source.port to hostNode.key.
		// Enrichment validates the target is a real input port.
		if (inlineCandidate && looksLikeDottedRef(inlineCandidate[2])) {
			const key = inlineCandidate[1];
			const ref = inlineCandidate[2].trim();
			const dotIdx = ref.indexOf('.');
			inlineScope.connections.push({
				sourceId: ref.slice(0, dotIdx),
				sourcePort: ref.slice(dotIdx + 1),
				targetId: hostNodeId,
				targetPort: key,
				line: nLineNum,
				rawText: nLine,
			});
			i++; continue;
		}

		// Triple backtick multiline: key: ``` ... ```
		const tripleBacktickMatch = nTrimmed.match(/^([a-zA-Z_][a-zA-Z0-9_]*)\s*:\s*```(.*)$/);
		if (tripleBacktickMatch) {
			const btRemainder = tripleBacktickMatch[2];
			if (btRemainder.endsWith('```') && btRemainder.length > 3) {
				const inlineVal = btRemainder.slice(0, -3);
				setConfigField(config, configSpans, errors, tripleBacktickMatch[1], inlineVal, nLineNum, nLineNum);
				i++; continue;
			}
			inMultiLine = true;
			mlKey = tripleBacktickMatch[1];
			mlValue = btRemainder;
			mlDelimiter = '```';
			mlStartLine = nLineNum;
			i++; continue;
		}

		const kvMatch = nTrimmed.match(/^([a-zA-Z_][a-zA-Z0-9_]*)\s*:\s*(.+)$/);
		if (kvMatch) {
			const key = kvMatch[1];
			const rawValue = kvMatch[2].trim();
			if (key === 'label') {
				label = parseLabelValue(rawValue);
				configSpans.label = { startLine: nLineNum, endLine: nLineNum, origin: 'inline' };
			}
			else if ((rawValue.startsWith('[') || rawValue.startsWith('{')) && !isJsonBalanced(rawValue)) {
				// Multi-line JSON array/object
				const jsonStartLine = nLineNum;
				let collected = rawValue;
				let depth = 0;
				for (const c of rawValue) { if (c === '[' || c === '{') depth++; if (c === ']' || c === '}') depth--; }
				const startI = i;
				i++;
				let hitBoundary = false;
				let jsonEndLine = jsonStartLine;
				const collectedLines: string[] = [];
				while (i < lines.length && depth > 0) {
					const ml = lines[i].trim();
					if (i - startI > 500) { hitBoundary = true; break; }
					if (!looksLikeJson(ml)) { hitBoundary = true; break; }
					collectedLines.push(ml);
					collected += '\n' + ml;
					for (const c of ml) { if (c === '[' || c === '{') depth++; if (c === ']' || c === '}') depth--; }
					if (depth <= 0) {
						jsonEndLine = i + 1;
						i++; // advance past the line that closed brackets
						try {
							JSON.parse(collected);
							break;
						} catch {
							while (collectedLines.length > 0) {
								const lastLine = collectedLines[collectedLines.length - 1];
								if (lastLine === '}' || lastLine === ']' || lastLine === '},' || lastLine === '],') {
									collectedLines.pop();
									collected = rawValue + (collectedLines.length > 0 ? '\n' + collectedLines.join('\n') : '');
									i--;
									depth = 0;
									for (const c of collected) { if (c === '[' || c === '{') depth++; if (c === ']' || c === '}') depth--; }
								} else { break; }
							}
							hitBoundary = true; break;
						}
					}
					jsonEndLine = i + 1;
					i++;
				}
				if (depth > 0 || hitBoundary) {
					errors.push({ line: nLineNum, message: `Broken JSON for "${key}": brackets not balanced` });
				}
				setConfigField(config, configSpans, errors, key, parseConfigValue(collected, errors, nLineNum, key), jsonStartLine, jsonEndLine);
				continue;
			}
			else {
				setConfigField(config, configSpans, errors, key, parseConfigValue(rawValue, errors, nLineNum, key), nLineNum, nLineNum);
			}
			i++; continue;
		}

		// Unknown line inside node block, skip it
		i++;
	}

	// Reached end of file without closing brace
	return { config, configSpans, label, inPorts, outPorts, oneOfRequired, endIdx: i };
}

/** Source span of a single config field: the inclusive 1-based line range
 *  covering the full `key: value` source, including all body lines for
 *  multi-line values (heredoc, JSON, list). For single-line values,
 *  `startLine === endLine`. `origin` tells the editor whether the value was
 *  written as an inline field inside the node body or as a separate
 *  connection-line literal `n.key = ...`. */
export interface ConfigFieldSpan {
	startLine: number;
	endLine: number;
	origin: 'inline' | 'connection';
}

export interface ParsedNode {
	id: string;
	nodeType: string;
	label: string | null;
	config: Record<string, unknown>;
	parentId?: string;
	startLine: number;
	endLine: number;
	rawLines: string[];
	inPorts: ParsedInterfacePort[];
	outPorts: ParsedInterfacePort[];
	oneOfRequired: string[][];
	/** Source spans of each field in `config`, keyed by field name. Populated
	 *  for both canonical node bodies and inline anon bodies. For fields set
	 *  via a connection-line literal (`n.x = "hi"`), `origin === 'connection'`
	 *  and the span covers that external line. Missing keys mean the parser
	 *  couldn't attribute the value to a specific span (fallback / legacy).
	 *  The `label` field is included here with its own span so the editor can
	 *  locate it for removal / replacement, even though `node.label` is
	 *  promoted to a top-level property after parsing. */
	configSpans: Record<string, ConfigFieldSpan>;
}

export interface ParsedConnection {
	sourceId: string;
	sourcePort: string;
	targetId: string;
	targetPort: string;
	line: number;
	rawText: string;
	sourceIsSelf?: boolean;
	targetIsSelf?: boolean;
	scopeId?: string;
}

export interface ParsedInterfacePort {
	name: string;
	portType: string;
	required: boolean;
	laneMode: LaneMode | null;
}

export interface ParsedGroup {
	id: string;
	originalName?: string;
	description?: string;
	inPorts: ParsedInterfacePort[];
	outPorts: ParsedInterfacePort[];
	/** @require_one_of groups declared on the group's input port signature.
	 *  Each inner array is a set of input port names where at least one must
	 *  have a non-null value at runtime, otherwise the whole group body is
	 *  skipped and all group outputs emit null downstream. */
	oneOfRequired: string[][];
	nodes: ParsedNode[];
	connections: ParsedConnection[];
	startLine: number;
	endLine: number;
	parentGroupId?: string;
	rawLines: string[];
}

/**
 * Unified scope parser. Handles both root scope (top-level weft) and group scopes.
 * The only differences controlled by `isRoot`:
 *   - Root: no `}` terminator, no `in:`/`out:` ports, has metadata headers
 *   - Group: terminated by `}`, has `in:`/`out:` ports
 * Everything else (nodes, groups, connections, opaque handling) uses the same code path.
 */
function parseScope(
	lines: string[],
	startIdx: number,
	scopeId: string,
	parentGroupId: string | undefined,
	scopeStartLine: number,
	errors: WeftParseError[],
	allGroups: ParsedGroup[],
	ancestorIds: Set<string>,
	isRoot: boolean,
): {
	nodes: ParsedNode[];
	connections: ParsedConnection[];
	groups: ParsedGroup[];
	opaqueBlocks: OpaqueBlock[];
	itemOrder: string[];
	itemGaps: number[];
	inPorts: ParsedInterfacePort[];
	outPorts: ParsedInterfacePort[];
	endIdx: number;
	closed: boolean;
	name: string;
	description: string;
	rawLines: string[];
} {
	const nodes: ParsedNode[] = [];
	const connections: ParsedConnection[] = [];
	const localGroups: ParsedGroup[] = [];
	const opaqueBlocks: OpaqueBlock[] = [];
	// Pre-scan local child identifiers for scope-correct connection rewriting.
	// Inline bodies can reference outer-scope nodes; without this we'd blindly
	// prefix those refs with `{scopeId}.` and break external wiring.
	const localChildIds = collectLocalChildIds(lines, startIdx - 1);
	const itemOrder: string[] = [];
	const itemGaps: number[] = [];
	const inPorts: ParsedInterfacePort[] = [];
	const outPorts: ParsedInterfacePort[] = [];
	let closed = false;
	let endLine = scopeStartLine;

	// Metadata (root only)
	let name = 'Untitled Project';
	let description = '';
	let descriptionCaptured = isRoot; // root uses # Description: header; groups use first # comment block

	// Port section state (group only)
	let section: 'in' | 'out' | null = null;

	// Scoped ID rewrites for children whose names conflicted with ancestors
	const scopedIdMap = new Map<string, string>();

	// Node block parsing state
	let currentNodeId: string | null = null;
	let currentNodeType: string | null = null;
	let currentLabel: string | null = null;
	let currentConfig: Record<string, unknown> = {};
	let currentConfigSpans: Record<string, ConfigFieldSpan> = {};
	let currentInPorts: ParsedInterfacePort[] = [];
	let currentOutPorts: ParsedInterfacePort[] = [];
	let currentOneOfRequired: string[][] = [];
	let currentNodeSection: 'in' | 'out' | null = null;
	let currentNodeStartLine = 0;
	let currentNodeEndLine = 0;
	let insideNodeBlock = false;
	let inMultiLine = false;
	let multiLineKey = '';
	let multiLineValue = '';
	let multiLineDelimiter = '';
	let multiLineStartLine = 0;

	// Opaque block accumulation
	let lastAnchor: string | null = null;
	let pendingOpaqueLines: string[] = [];
	let pendingOpaqueStart = -1;
	let pendingOpaqueError = '';
	let pendingOpaqueGap = 0;
	let insideOpaqueBlock = false;
	let blankLineCount = 0;

	function flushOpaque() {
		if (pendingOpaqueLines.length > 0) {
			let trailingBlanks = 0;
			while (pendingOpaqueLines.length > 0 && pendingOpaqueLines[pendingOpaqueLines.length - 1].trim() === '') {
				pendingOpaqueLines.pop();
				trailingBlanks++;
			}
			if (pendingOpaqueLines.length > 0) {
				const idx = opaqueBlocks.length;
				opaqueBlocks.push({
					startLine: pendingOpaqueStart,
					endLine: pendingOpaqueStart + pendingOpaqueLines.length - 1,
					text: pendingOpaqueLines.join('\n'),
					error: pendingOpaqueError,
					anchorAfter: lastAnchor,
				});
				itemOrder.push(`opaque:${idx}`);
				itemGaps.push(pendingOpaqueGap);
			}
			blankLineCount = trailingBlanks;
			pendingOpaqueLines = [];
			pendingOpaqueStart = -1;
			pendingOpaqueError = '';
			pendingOpaqueGap = 0;
		}
		insideOpaqueBlock = false;
	}

	function addOpaqueLine(lineNum: number, line: string, error: string) {
		if (pendingOpaqueLines.length === 0) {
			pendingOpaqueStart = lineNum;
			pendingOpaqueError = error;
			pendingOpaqueGap = blankLineCount;
			blankLineCount = 0;
		}
		pendingOpaqueLines.push(line);
		if (error) errors.push({ line: lineNum, message: error });
	}

	function flushNode() {
		if (currentNodeId && currentNodeType) {
			const nodeEndLine = currentNodeEndLine || currentNodeStartLine;
			const rawLines = lines.slice(currentNodeStartLine - 1, nodeEndLine);
			// Scope node IDs inside groups: prefix with scopeId to avoid collisions
			// when two groups contain nodes with the same local name.
			const scopedNodeId = isRoot ? currentNodeId : `${scopeId}.${currentNodeId}`;
			const parentId = isRoot ? undefined : scopeId;
			nodes.push({ id: scopedNodeId, nodeType: currentNodeType, label: currentLabel, parentId, config: { ...currentConfig }, configSpans: { ...currentConfigSpans }, startLine: currentNodeStartLine, endLine: nodeEndLine, rawLines, inPorts: [...currentInPorts], outPorts: [...currentOutPorts], oneOfRequired: [...currentOneOfRequired] });
			itemOrder.push(`node:${scopedNodeId}`);
			itemGaps.push(blankLineCount);
			blankLineCount = 0;
			lastAnchor = `node:${scopedNodeId}`;
			flushOpaque();
		}
		currentNodeId = null;
		currentNodeType = null;
		currentLabel = null;
		currentConfig = {};
		currentConfigSpans = {};
		currentInPorts = [];
		currentOutPorts = [];
		currentOneOfRequired = [];
		currentNodeSection = null;
		currentNodeStartLine = 0;
		currentNodeEndLine = 0;
		insideNodeBlock = false;
	}

	let i = startIdx;
	while (i < lines.length) {
		const line = lines[i];
		const trimmed = line.trim();
		const lineNum = i + 1;

		// Handle multi-line values (``` ... ``` blocks). A line closes the
		// heredoc only if its trimmed form is exactly the delimiter, or ends
		// with the delimiter NOT preceded by `\`. An escaped `\```` inside
		// the value is appended as content; the decoder below strips the
		// backslash.
		if (inMultiLine) {
			if (currentNodeId) currentNodeEndLine = lineNum;
			const closesBare = trimmed === multiLineDelimiter;
			const closesSuffix = !closesBare
				&& trimmed.endsWith(multiLineDelimiter)
				&& trimmed.slice(0, -multiLineDelimiter.length).slice(-1) !== '\\';
			if (closesBare) {
				let finalVal = multiLineValue.trimEnd();
				if (multiLineDelimiter === '```') {
					finalVal = dedent(finalVal);
					finalVal = finalVal.replace(/\\```/g, '```').replace(/\\`/g, '`');
				}
				setConfigField(currentConfig, currentConfigSpans, errors, multiLineKey, finalVal, multiLineStartLine, lineNum);
				inMultiLine = false; multiLineKey = ''; multiLineValue = ''; multiLineDelimiter = ''; multiLineStartLine = 0;
			} else if (closesSuffix) {
				const lastContent = line.slice(0, line.lastIndexOf(multiLineDelimiter));
				if (lastContent.trim()) multiLineValue += (multiLineValue ? '\n' : '') + lastContent;
				let finalVal = multiLineValue.trimEnd();
				if (multiLineDelimiter === '```') {
					finalVal = dedent(finalVal);
					finalVal = finalVal.replace(/\\```/g, '```').replace(/\\`/g, '`');
				}
				setConfigField(currentConfig, currentConfigSpans, errors, multiLineKey, finalVal, multiLineStartLine, lineNum);
				inMultiLine = false; multiLineKey = ''; multiLineValue = ''; multiLineDelimiter = ''; multiLineStartLine = 0;
			} else {
				multiLineValue += (multiLineValue ? '\n' : '') + line;
			}
			i++; continue;
		}

		// Empty lines
		if (trimmed === '') {
			// A blank line after we've started capturing description ends the capture
			if (!descriptionCaptured && description) {
				descriptionCaptured = true;
			}
			if (insideOpaqueBlock || pendingOpaqueLines.length > 0) {
				addOpaqueLine(lineNum, line, '');
			} else if (!insideNodeBlock) {
				blankLineCount++;
			}
			i++; continue;
		}

		// Closing brace, end of this scope (group) or end of node block (root)
		// Also handle `} -> (outputs)` on the same line
		if (trimmed === '}' || trimmed.startsWith('} ->')) {
			if (!isRoot) {
				// End of group scope
				flushNode();
				flushOpaque();
				endLine = lineNum;
				// Check for post-config output ports: } -> (outputs)
				if (trimmed.startsWith('} ->')) {
					let arrowRest = trimmed.slice(4).trim();
					let outEnd = i;
					let parenDepth = 0;
					for (const c of arrowRest) { if (c === '(') parenDepth++; if (c === ')') parenDepth--; }
					while (parenDepth > 0 && outEnd + 1 < lines.length) {
						outEnd++;
						arrowRest += ' ' + lines[outEnd].trim();
						for (const c of lines[outEnd].trim()) { if (c === '(') parenDepth++; if (c === ')') parenDepth--; }
					}
					if (arrowRest.startsWith('(')) {
						const closeIdx = findMatchingParenHelper(arrowRest, 0);
						if (closeIdx >= 0) {
							const content = arrowRest.slice(1, closeIdx);
							for (const item of splitTopLevelComma(content).map(s => s.trim()).filter(s => s && !s.startsWith('#'))) {
								pushPortsDeduped(parseInlinePortList(item), 'out', outPorts, errors, lineNum);
							}
						}
					}
					endLine = outEnd + 1;
				}
				closed = true;
				break;
			}
			// Root scope: closing brace ends a node block or opaque block
			if (insideOpaqueBlock) {
				addOpaqueLine(lineNum, line, '');
				insideOpaqueBlock = false;
				flushOpaque();
			} else {
				if (currentNodeId) currentNodeEndLine = lineNum;
				// Check for post-config output ports: } -> (outputs)
				// Either on the same line as } or on the next non-blank line
				let arrowOnBraceLine = false;
				let peekIdx = i + 1;
				if (trimmed.startsWith('} ->')) {
					arrowOnBraceLine = true;
					peekIdx = i; // current line has the arrow
				} else {
					while (peekIdx < lines.length && lines[peekIdx].trim() === '') peekIdx++;
				}
				if (arrowOnBraceLine || (peekIdx < lines.length && lines[peekIdx].trim().startsWith('->'))) {
					// Collect the output port signature
					let outSig = '';
					let outEndIdx = peekIdx;
					let parenDepth = 0;
					for (let oi = peekIdx; oi < lines.length; oi++) {
						let ol = lines[oi].trim();
						// Strip leading `} ` from the first line if arrow is on same line as brace
						if (oi === peekIdx && arrowOnBraceLine && ol.startsWith('}')) {
							ol = ol.slice(1).trim();
						}
						outSig += (outSig ? ' ' : '') + ol;
						for (const c of ol) { if (c === '(') parenDepth++; if (c === ')') parenDepth--; }
						outEndIdx = oi;
						if (parenDepth === 0 && outSig.includes(')')) break;
					}
					// Parse output ports from -> (...) and merge, checking for duplicates
					const arrowRest = outSig.replace(/^->\s*/, '').trim();
					if (arrowRest.startsWith('(')) {
						const closeIdx = findMatchingParenHelper(arrowRest, 0);
						if (closeIdx >= 0) {
							const existingNames = new Set(currentOutPorts.map(p => p.name));
							const outputContent = arrowRest.slice(1, closeIdx);
							for (const item of splitTopLevelComma(outputContent).map(s => s.trim()).filter(s => s && !s.startsWith('#'))) {
								for (const r of parseInlinePortList(item)) {
									if ('error' in r) {
										errors.push({ line: outEndIdx + 1, message: r.error });
										continue;
									}
									const p = r.port;
									if (existingNames.has(p.name)) {
										errors.push({ line: outEndIdx + 1, message: `Duplicate output port "${p.name}", already declared in the signature before the config block` });
									} else {
										currentOutPorts.push(p);
										existingNames.add(p.name);
									}
								}
							}
						}
					}
					currentNodeEndLine = outEndIdx + 1;
					i = outEndIdx;
				}
				flushNode();
				flushOpaque();
			}
			i++; continue;
		}

		// Root-only: metadata headers
		if (isRoot) {
			if (trimmed.startsWith('# Project:')) {
				flushOpaque(); name = trimmed.substring('# Project:'.length).trim(); i++; continue;
			}
			if (trimmed.startsWith('# Description:')) {
				flushOpaque(); description = trimmed.substring('# Description:'.length).trim(); i++; continue;
			}
		}

		// Comments
		if (trimmed.startsWith('#')) {
			if (insideNodeBlock && currentNodeId) { i++; continue; }
			// For groups (non-root): capture the first comment block as the description
			if (!descriptionCaptured) {
				const commentText = trimmed.substring(1).trim();
				if (commentText) {
					description = description ? description + '\n' + commentText : commentText;
				}
				i++; continue;
			}
			if (isRoot) {
				flushOpaque(); flushNode();
				itemOrder.push(`comment:${line}`);
				itemGaps.push(blankLineCount);
				blankLineCount = 0;
			}
			i++; continue;
		}
		// First non-comment, non-blank line ends the description capture
		if (!descriptionCaptured && trimmed) {
			descriptionCaptured = true;
		}

		// Group-only: in:/out: port sections
		if (!isRoot) {
			const portResult = tryParsePortLine(trimmed, section, inPorts, outPorts, errors, lineNum);
			section = portResult.section;
			if (portResult.consumed) { i++; continue; }
		}

		// One-liner node: `id = Type { key: val, key: val }`. Body can
		// contain inline expressions with nested braces, so we match
		// the prefix with a regex and find the matching `}` manually.
		const oneLinerHead = trimmed.match(/^([a-zA-Z_][a-zA-Z0-9_]*)\s*=\s*([A-Z][a-zA-Z0-9]*)\s*\{/);
		if (oneLinerHead && trimmed.endsWith('}')) {
			const headMatch = oneLinerHead[0];
			const body = trimmed.slice(headMatch.length, -1).trim();
			flushOpaque(); flushNode();
			if (section) section = null;
			currentNodeId = oneLinerHead[1];
			currentNodeType = oneLinerHead[2];
			currentLabel = null;
			currentConfig = {};
			currentConfigSpans = {};
			currentNodeStartLine = lineNum;
			currentNodeEndLine = lineNum;
			if (body) {
				// Brace-aware comma split: respect quotes and all bracket
				// depths so that inline-expression values (with nested `{}`)
				// or JSON values (with `[` `]`) stay in the same pair.
				const pairs = splitBraceAwareComma(body);
				for (const rawPair of pairs) {
					const pair = rawPair.trim();
					if (!pair) continue;
					const colonIdx = pair.indexOf(':');
					if (colonIdx < 0) {
						errors.push({ line: lineNum, message: `Invalid config pair: '${pair}'` });
						continue;
					}
					const key = pair.slice(0, colonIdx).trim();
					const rawValue = pair.slice(colonIdx + 1).trim();
					// Inline expression FIRST (covers bare `Type.port` which
					// would otherwise look like a dotted ref). Enrichment
					// is responsible for rejecting inlines on non-port keys.
					if (/^[a-zA-Z_][a-zA-Z0-9_]*$/.test(key) && looksLikeInlineStart(rawValue)) {
						const synth = [rawValue];
						const rootInlineScope: InlineScope = { nodes: [], connections: [] };
						tryParseInlineExpression(synth, 0, 0, currentNodeId!, key, rootInlineScope, errors);
						for (const child of rootInlineScope.nodes) nodes.push(child);
						for (const conn of rootInlineScope.connections) {
							connections.push({
								...conn,
								line: lineNum,
								rawText: line,
								scopeId: '__root__',
							});
						}
						continue;
					}
					// Port wiring: unquoted dotted ref emits an edge.
					if (/^[a-zA-Z_][a-zA-Z0-9_]*$/.test(key) && looksLikeDottedRef(rawValue)) {
						const dotIdx = rawValue.indexOf('.');
						connections.push({
							sourceId: rawValue.slice(0, dotIdx),
							sourcePort: rawValue.slice(dotIdx + 1),
							targetId: currentNodeId!,
							targetPort: key,
							line: lineNum,
							rawText: line,
							scopeId: '__root__',
						});
						continue;
					}
					if (key === 'label') {
						currentLabel = parseLabelValue(rawValue);
						currentConfigSpans.label = { startLine: lineNum, endLine: lineNum, origin: 'inline' };
						continue;
					}
					setConfigField(currentConfig, currentConfigSpans, errors, key, parseConfigValue(rawValue, errors, lineNum, key), lineNum, lineNum);
				}
			}
			flushNode();
			i++; continue;
		}

		// Declaration: id = Type or id = Type(ports) -> (ports) { ... } or id = Type { ... }
		// Also matches id = Group(ports) -> (ports) { ... } for groups
		const declMatch = trimmed.match(/^([a-zA-Z_][a-zA-Z0-9_]*)\s*=\s*([A-Z][a-zA-Z0-9]*)(.*)$/);
		if (declMatch && declMatch[1] !== 'self') {
			const declId = declMatch[1];
			const declType = declMatch[2];
			let afterType = (declMatch[3] || '').trim();
			let declLabel: string | null = null;

			// Collect multi-line port signature if afterType starts with (
			// Collect ALL text from afterType through subsequent lines until all parens
			// are balanced and we've seen either { or end of declaration.
			let portSignature = '';
			let headerEndLine = i;
			if (afterType.startsWith('(')) {
				let parenDepth = 0;
				let lineIdx = i;

				// Count all parens in afterType
				for (const c of afterType) {
					if (c === '(') parenDepth++;
					if (c === ')') parenDepth--;
				}

				let collected = afterType;

				// If parens aren't balanced, keep collecting lines
				while (parenDepth > 0 && lineIdx + 1 < lines.length) {
					lineIdx++;
					const nextLine = lines[lineIdx].trim();
					collected += '\n' + nextLine;
					for (const c of nextLine) {
						if (c === '(') parenDepth++;
						if (c === ')') parenDepth--;
					}
				}

				// Parens are balanced. Check if there's a -> on the next line.
				// The collected text might end with `) -> (...)` all balanced,
				// or it might end with `)` and `->` is on the next line.
				if (parenDepth === 0 && !collected.includes('->') && lineIdx + 1 < lines.length) {
					const peekLine = lines[lineIdx + 1]?.trim() || '';
					if (peekLine.startsWith('->')) {
						lineIdx++;
						collected += '\n' + peekLine;
						// Count parens in the peeked line
						for (const c of peekLine) {
							if (c === '(') parenDepth++;
							if (c === ')') parenDepth--;
						}
						// Keep collecting output parens
						while (parenDepth > 0 && lineIdx + 1 < lines.length) {
							lineIdx++;
							const nextLine = lines[lineIdx].trim();
							collected += '\n' + nextLine;
							for (const c of nextLine) {
								if (c === '(') parenDepth++;
								if (c === ')') parenDepth--;
							}
						}
					}
				}

				portSignature = collected;
				headerEndLine = lineIdx;

				// Check if { is on the last line or needs to peek at next line
				if (!collected.includes('{') && headerEndLine + 1 < lines.length) {
					const peekBrace = lines[headerEndLine + 1]?.trim() || '';
					if (peekBrace === '{' || peekBrace === '{}') {
						headerEndLine++;
						portSignature += '\n' + peekBrace;
					}
				}
			} else if (afterType.startsWith('->') || afterType.trim() === '->' ) {
				// No input ports, just -> (outputs)
				// e.g. List -> (value: List[String]) {
				let collected = afterType;
				let lineIdx = i;
				let parenDepth = 0;
				for (const c of afterType) {
					if (c === '(') parenDepth++;
					if (c === ')') parenDepth--;
				}
				while (parenDepth > 0 && lineIdx + 1 < lines.length) {
					lineIdx++;
					const nextLine = lines[lineIdx].trim();
					collected += '\n' + nextLine;
					for (const c of nextLine) {
						if (c === '(') parenDepth++;
						if (c === ')') parenDepth--;
					}
				}
				portSignature = collected;
				headerEndLine = lineIdx;
				if (!collected.includes('{') && headerEndLine + 1 < lines.length) {
					const peekBrace = lines[headerEndLine + 1]?.trim() || '';
					if (peekBrace === '{' || peekBrace === '{}') {
						headerEndLine++;
						portSignature += '\n' + peekBrace;
					}
				}
			} else {
				portSignature = afterType;
				headerEndLine = i;
			}

			// Parse port signature to extract ports
			let parsedInPorts: ParsedInterfacePort[] = [];
			let parsedOutPorts: ParsedInterfacePort[] = [];
			let parsedOneOfRequired: string[][] = [];
			let bodyStart = portSignature;

			// Extract (inputs) -> (outputs) from the signature. Keep newlines
			// intact: splitTopLevelComma splits on both commas and newlines
			// (mirrors backend split_port_items), so multi-line port lists
			// without trailing commas work identically on both sides.
			const sigText = portSignature;

			// Find the matching ) for the opening ( using depth counting
			function findMatchingParen(text: string, startIdx: number): number {
				let depth = 0;
				for (let ci = startIdx; ci < text.length; ci++) {
					if (text[ci] === '(') depth++;
					if (text[ci] === ')') { depth--; if (depth === 0) return ci; }
				}
				return -1;
			}

			// Parse output ports from -> (...) section
			function parseOutputSection(arrowText: string) {
				const arrowRest = arrowText.slice(2).trim();
				if (arrowRest.startsWith('(')) {
					const outputClose = findMatchingParen(arrowRest, 0);
					if (outputClose >= 0) {
						const outputContent = arrowRest.slice(1, outputClose);
						bodyStart = arrowRest.slice(outputClose + 1).trim();
						for (const item of splitTopLevelComma(outputContent).map(s => s.trim()).filter(s => s && !s.startsWith('#'))) {
							if (item.startsWith('@require_one_of(')) {
								errors.push({ line: lineNum, message: '@require_one_of is only valid in input port lists, not outputs' });
								continue;
							}
							pushPortsDeduped(parseInlinePortList(item), 'out', parsedOutPorts, errors, lineNum);
						}
					} else {
						bodyStart = arrowRest;
					}
				} else {
					bodyStart = arrowRest;
				}
			}

			if (sigText.startsWith('(')) {
				const inputClose = findMatchingParen(sigText, 0);
				if (inputClose >= 0) {
					const inputContent = sigText.slice(1, inputClose);
					for (const item of splitTopLevelComma(inputContent).map(s => s.trim()).filter(s => s && !s.startsWith('#'))) {
						if (item.startsWith('@require_one_of(')) {
							const body = item.slice(item.indexOf('(') + 1, -1);
							const group = body.split(',').map(s => s.trim()).filter(s => s.length > 0);
							if (group.length > 0) parsedOneOfRequired.push(group);
							continue;
						}
						pushPortsDeduped(parseInlinePortList(item), 'in', parsedInPorts, errors, lineNum);
					}
					const afterInputs = sigText.slice(inputClose + 1).trim();
					if (afterInputs.startsWith('->')) {
						parseOutputSection(afterInputs);
					} else {
						bodyStart = afterInputs;
					}
				}
			} else if (sigText.trim().startsWith('->')) {
				// No input ports, just output: Type -> (outputs)
				parseOutputSection(sigText.trim());
			}

			// Handle Group type as a group declaration
			if (declType === 'Group') {
				flushOpaque(); flushNode();
				if (section) section = null;
				const groupName = declId;
				let groupId = isRoot ? groupName : `${scopeId}.${groupName}`;
				let originalName: string | undefined = isRoot ? undefined : groupName;
				if (allGroups.some(g => g.id === groupId)) {
					let suffix = 2;
					let candidate = `${groupId}__${suffix}`;
					while (allGroups.some(g => g.id === candidate)) { candidate = `${groupId}__${suffix++}`; }
					groupId = candidate;
				}
				const childAncestors = new Set(ancestorIds);
				childAncestors.add(scopeId);
				// Strip inline comments from bodyStart
				let bodyTrimmed = bodyStart.trim();
				if (bodyTrimmed.includes('#')) {
					// Only strip comment if it's after { (not inside a string)
					const hashIdx = bodyTrimmed.indexOf('#');
					if (hashIdx > 0) bodyTrimmed = bodyTrimmed.slice(0, hashIdx).trim();
				}
				let result;
				if (bodyTrimmed === '{}' || bodyTrimmed === '') {
					result = {
						nodes: [] as ParsedNode[],
						connections: [] as ParsedConnection[],
						groups: [] as ParsedGroup[],
						opaqueBlocks: [] as OpaqueBlock[],
						inPorts: parsedInPorts,
						outPorts: parsedOutPorts,
						description: undefined as string | undefined,
						closed: true,
						endIdx: headerEndLine,
					};
				} else if (bodyTrimmed === '{') {
					result = parseScope(lines, headerEndLine + 1, groupId, isRoot ? undefined : scopeId, lineNum, errors, allGroups, childAncestors, false);
					// Pass the ports from the signature, not from the body
					result.inPorts = parsedInPorts.length > 0 ? parsedInPorts : result.inPorts;
					result.outPorts = parsedOutPorts.length > 0 ? parsedOutPorts : result.outPorts;
				} else {
					// Unexpected body content
					errors.push({ line: lineNum, message: `Unexpected after group declaration: ${bodyTrimmed}` });
					result = {
						nodes: [] as ParsedNode[],
						connections: [] as ParsedConnection[],
						groups: [] as ParsedGroup[],
						opaqueBlocks: [] as OpaqueBlock[],
						inPorts: parsedInPorts,
						outPorts: parsedOutPorts,
						description: undefined as string | undefined,
						closed: true,
						endIdx: headerEndLine,
					};
				}
				const group: ParsedGroup = {
					id: groupId,
					originalName: originalName || groupName,
					description: result.description || undefined,
					inPorts: result.inPorts,
					outPorts: result.outPorts,
					oneOfRequired: parsedOneOfRequired,
					nodes: result.nodes,
					connections: result.connections,
					startLine: lineNum,
					endLine: result.closed ? result.endIdx + 1 : lineNum,
					parentGroupId: isRoot ? undefined : scopeId,
					rawLines: [],
				};
				group.rawLines = lines.slice(group.startLine - 1, group.endLine);
				for (const ob of result.opaqueBlocks) opaqueBlocks.push(ob);
				const preScopeId = isRoot ? groupName : `${scopeId}.${groupName}`;
				if (preScopeId !== groupId) scopedIdMap.set(preScopeId, groupId);
				if (!result.closed) {
					errors.push({ line: lineNum, message: `Unclosed group '${groupName}'` });
				}
				localGroups.push(group);
				allGroups.push(group);
				itemOrder.push(`group:${groupId}`);
				itemGaps.push(blankLineCount);
				blankLineCount = 0;
				lastAnchor = `group:${groupId}`;
				i = result.endIdx + 1;
				continue;
			}

			// Regular node declaration
			flushOpaque(); flushNode();
			if (section) section = null;
			currentNodeId = declId;
			currentNodeType = declType;
			currentLabel = declLabel;
			currentConfig = {};
			currentConfigSpans = {};
			currentInPorts = [...parsedInPorts];
			currentOutPorts = [...parsedOutPorts];
			currentOneOfRequired = [...parsedOneOfRequired];
			currentNodeStartLine = lineNum;
			currentNodeEndLine = headerEndLine + 1;
			let nodeBodyTrimmed = bodyStart.trim();
			// Strip inline comments
			if (nodeBodyTrimmed.includes('#')) {
				const hashIdx = nodeBodyTrimmed.indexOf('#');
				if (hashIdx > 0) nodeBodyTrimmed = nodeBodyTrimmed.slice(0, hashIdx).trim();
			}
			// Detect wrong-order post-config outputs: `-> (pre) -> (post) { config }`.
			// The correct order is `-> (pre) { config } -> (post)`.
			if (nodeBodyTrimmed.startsWith('->')) {
				errors.push({
					line: lineNum,
					message: 'Two arrow clauses before the config block. You wrote: Type -> (out: T) -> (extra: T2) { config }. Fix: merge both port lists into one: Type -> (out: T, extra: T2) { config }. Just add the extra ports to the first arrow clause. Other errors below are likely caused by this, ignore them until this is fixed.',
				});
			}
			if (nodeBodyTrimmed === '{') {
				if (!isRoot) {
					// Inline children emitted by parseNodeBlockBody will target
					// this host node. We use the LOCAL declId (not the scoped
					// id) so that anon ids are constructed as
					// `{declId}__{field}`; merge below prefixes them with the
					// group scope to match how regular child nodes are scoped.
					const nodeInlineScope: InlineScope = { nodes: [], connections: [] };
					const block = parseNodeBlockBody(lines, headerEndLine + 1, errors, declId, nodeInlineScope);
					currentConfig = block.config;
					currentConfigSpans = block.configSpans;
					if (block.label) currentLabel = block.label;
					if (block.inPorts.length > 0) currentInPorts.push(...block.inPorts);
					if (block.outPorts.length > 0) currentOutPorts.push(...block.outPorts);
					if (block.oneOfRequired.length > 0) currentOneOfRequired.push(...block.oneOfRequired);
					currentNodeEndLine = block.endIdx + 1;
					i = block.endIdx + 1;
					// Merge inline children: prefix anon ids with scope and
					// push them alongside regular child nodes of this group.
					for (const child of nodeInlineScope.nodes) {
						const scopedChildId = `${scopeId}.${child.id}`;
						nodes.push({
							...child,
							id: scopedChildId,
							parentId: scopeId,
						});
						itemOrder.push(`node:${scopedChildId}`);
						itemGaps.push(0);
					}
					// Rescope inline connections into the group scope. `self.x`
					// becomes { sourceId: scopeId, sourceIsSelf: true }; local
					// ids get the `${scopeId}.` prefix.
					for (const conn of nodeInlineScope.connections) {
						let srcId = conn.sourceId;
						let srcIsSelf = false;
						if (srcId === 'self') {
							srcId = scopeId;
							srcIsSelf = true;
						} else if (isLocalRef(srcId, localChildIds)) {
							srcId = `${scopeId}.${srcId}`;
						}
						let tgtId = conn.targetId;
						let tgtIsSelf = false;
						if (tgtId === 'self') {
							tgtId = scopeId;
							tgtIsSelf = true;
						} else if (isLocalRef(tgtId, localChildIds)) {
							tgtId = `${scopeId}.${tgtId}`;
						}
						connections.push({
							...conn,
							sourceId: srcId,
							sourceIsSelf: srcIsSelf,
							targetId: tgtId,
							targetIsSelf: tgtIsSelf,
							scopeId,
						});
					}
					// Check for post-config output ports: } -> (outputs)
					let peekIdx = i;
					while (peekIdx < lines.length && lines[peekIdx].trim() === '') peekIdx++;
					if (peekIdx < lines.length && lines[peekIdx].trim().startsWith('->')) {
						let outSig = '';
						let outEndIdx = peekIdx;
						let parenDepth = 0;
						for (let oi = peekIdx; oi < lines.length; oi++) {
							const ol = lines[oi].trim();
							outSig += (outSig ? ' ' : '') + ol;
							for (const c of ol) { if (c === '(') parenDepth++; if (c === ')') parenDepth--; }
							outEndIdx = oi;
							if (parenDepth === 0 && outSig.includes(')')) break;
						}
						const arrowRest = outSig.replace(/^->\s*/, '').trim();
						if (arrowRest.startsWith('(')) {
							const closeIdx = findMatchingParenHelper(arrowRest, 0);
							if (closeIdx >= 0) {
								const existingNames = new Set(currentOutPorts.map(p => p.name));
								const outputContent = arrowRest.slice(1, closeIdx);
								for (const item of splitTopLevelComma(outputContent).map(s => s.trim()).filter(s => s && !s.startsWith('#'))) {
									for (const r of parseInlinePortList(item)) {
										if ('error' in r) {
											errors.push({ line: outEndIdx + 1, message: r.error });
											continue;
										}
										const p = r.port;
										if (existingNames.has(p.name)) {
											errors.push({ line: outEndIdx + 1, message: `Duplicate output port "${p.name}", already declared in the signature before the config block` });
										} else {
											currentOutPorts.push(p);
											existingNames.add(p.name);
										}
									}
								}
							}
						}
						currentNodeEndLine = outEndIdx + 1;
						i = outEndIdx + 1;
					}
					flushNode();
					continue;
				} else {
					insideNodeBlock = true;
					i = headerEndLine;
				}
			} else if (nodeBodyTrimmed.startsWith('{') && (nodeBodyTrimmed.endsWith('}') || nodeBodyTrimmed.includes('} ->'))) {
				// One-liner config: { key: val, key: val }
				// Possibly with post-config outputs: { key: val } -> (out: Type)
				let configPart = nodeBodyTrimmed;
				let postConfigArrow: string | null = null;

				// Split off post-config outputs if present: { ... } -> ( ... )
				if (!nodeBodyTrimmed.endsWith('}') && nodeBodyTrimmed.includes('} ->')) {
					let depth = 0;
					let inQ = false;
					let splitPos = -1;
					for (let ci = 0; ci < nodeBodyTrimmed.length; ci++) {
						const ch = nodeBodyTrimmed[ci];
						if (ch === '"') inQ = !inQ;
						if (inQ) continue;
						if (ch === '{') depth++;
						if (ch === '}') { depth--; if (depth === 0) { splitPos = ci; break; } }
					}
					if (splitPos >= 0) {
						const rest = nodeBodyTrimmed.slice(splitPos + 1).trim();
						if (rest.startsWith('->')) {
							configPart = nodeBodyTrimmed.slice(0, splitPos + 1);
							postConfigArrow = rest.slice(2).trim();
						}
					}
				}

				const inlineBody = configPart.slice(1, -1).trim();
				if (inlineBody) {
					const pairs: string[] = [];
					let current = '';
					let inQuote = false;
					for (let ci = 0; ci < inlineBody.length; ci++) {
						const ch = inlineBody[ci];
						if (ch === '"' && (ci === 0 || inlineBody[ci - 1] !== '\\')) inQuote = !inQuote;
						if (ch === ',' && !inQuote) { pairs.push(current.trim()); current = ''; }
						else { current += ch; }
					}
					if (current.trim()) pairs.push(current.trim());
					for (const pair of pairs) {
						const pairMatch = pair.match(/^([a-zA-Z_][a-zA-Z0-9_]*)\s*:\s*(.+)$/);
						if (!pairMatch) continue;
						const key = pairMatch[1];
						const rawValue = pairMatch[2].trim();
						if (key === 'label') {
							currentLabel = parseLabelValue(rawValue);
							currentConfigSpans.label = { startLine: lineNum, endLine: lineNum, origin: 'inline' };
						}
						else setConfigField(currentConfig, currentConfigSpans, errors, key, parseConfigValue(rawValue, errors, lineNum, key), lineNum, lineNum);
					}
				}

				// Parse post-config output ports if present
				if (postConfigArrow && postConfigArrow.startsWith('(')) {
					const closeIdx = findMatchingParenHelper(postConfigArrow, 0);
					if (closeIdx >= 0) {
						const existingNames = new Set(currentOutPorts.map(p => p.name));
						const outputContent = postConfigArrow.slice(1, closeIdx);
						for (const item of splitTopLevelComma(outputContent).map(s => s.trim()).filter(s => s && !s.startsWith('#'))) {
							for (const r of parseInlinePortList(item)) {
								if ('error' in r) {
									errors.push({ line: lineNum, message: r.error });
									continue;
								}
								const p = r.port;
								if (existingNames.has(p.name)) {
									errors.push({ line: lineNum, message: `Duplicate output port "${p.name}", already declared before the config block` });
								} else {
									currentOutPorts.push(p);
									existingNames.add(p.name);
								}
							}
						}
					}
				}

				flushNode();
			} else if (nodeBodyTrimmed.startsWith('{') && firstBraceHasContentAfter(nodeBodyTrimmed)) {
				// One-liner style with multi-line content: `{ code: ``` ...`
				// where the value spans subsequent lines and closes with `}`
				// on a later line. Collect all lines until the matching `}`
				// respecting triple-backtick so `}` inside a code block
				// doesn't close the outer brace.
				let collected = nodeBodyTrimmed;
				let j = headerEndLine + 1;
				while (j < lines.length && !isBraceBalancedRespectingQuotesAndBackticks(collected)) {
					collected += '\n' + lines[j];
					j++;
				}
				// Strip outer `{` `}` and process the inner body as a single
				// multi-line pair list. Each pair value can be a literal,
				// triple-backtick, JSON, or inline expression.
				const openIdx = collected.indexOf('{');
				const closeIdx = collected.lastIndexOf('}');
				if (openIdx >= 0 && closeIdx > openIdx) {
					const bodyText = collected.slice(openIdx + 1, closeIdx);
					// Parse the body as a sequence of `key: value` lines,
					// honoring triple-backtick and multi-line JSON.
					parseOneLinerMultilineBody(bodyText, currentNodeId!, currentConfig, currentConfigSpans, nodes, connections, errors, lineNum, line);
				}
				currentNodeEndLine = j;
				i = j;
				flushNode();
				continue;
			} else if (nodeBodyTrimmed === '' || nodeBodyTrimmed === '{}') {
				flushNode();
			} else {
				// Unexpected, but try to continue
				flushNode();
			}
			i = headerEndLine + 1; continue;
		}

		// Config key-value inside a node block (root uses inline parsing, group uses parseNodeBlockBody above)
		if (insideNodeBlock && currentNodeId) {
			currentNodeEndLine = lineNum;

			// @require_one_of directive
			if (trimmed.startsWith('@require_one_of(') && trimmed.endsWith(')')) {
				const prefixLen = '@require_one_of('.length;
				const body = trimmed.slice(prefixLen, -1);
				const group = body.split(',').map(s => s.trim()).filter(s => s.length > 0);
				if (group.length > 0) currentOneOfRequired.push(group);
				i++; continue;
			}

			// in:/out: port sections (all three forms)
			const portResult = tryParsePortLine(trimmed, currentNodeSection, currentInPorts, currentOutPorts, errors, lineNum);
			currentNodeSection = portResult.section;
			if (portResult.consumed) { i++; continue; }

			const labelMatch = trimmed.match(/^label\s*:\s*(.+)$/);
			if (labelMatch) {
				currentLabel = parseLabelValue(labelMatch[1]);
				currentConfigSpans.label = { startLine: lineNum, endLine: lineNum, origin: 'inline' };
				i++; continue;
			}

			// Inline expression: `key: Type { ... }.port` or bare `Type.port`
			// inside a root-scope node config block. Must run BEFORE port
			// wiring because `Type.port` also looks like a dotted ref.
			const inlineInRootBody = trimmed.match(/^([a-zA-Z_][a-zA-Z0-9_]*)\s*:\s*(.+)$/);
			if (inlineInRootBody && currentNodeId && looksLikeInlineStart(inlineInRootBody[2])) {
				const key = inlineInRootBody[1];
				const rawColon = line.indexOf(':');
				const rootInlineScope: InlineScope = { nodes: [], connections: [] };
				const next = tryParseInlineExpression(lines, i, rawColon + 1, currentNodeId, key, rootInlineScope, errors);
				if (next !== null) {
					for (const child of rootInlineScope.nodes) {
						nodes.push(child);
						itemOrder.push(`node:${child.id}`);
						itemGaps.push(0);
					}
					for (const conn of rootInlineScope.connections) {
						connections.push({ ...conn, scopeId: '__root__' });
					}
					currentNodeEndLine = next;
					i = next;
					continue;
				}
				i++; continue;
			}

			// Port wiring via unquoted dotted ref: `key: source.port` emits
			// an edge from source.port to currentNode.key. Enrichment rejects
			// if `key` is not a real input port on the parent.
			if (inlineInRootBody && currentNodeId && looksLikeDottedRef(inlineInRootBody[2])) {
				const key = inlineInRootBody[1];
				const ref = inlineInRootBody[2].trim();
				const dotIdx = ref.indexOf('.');
				connections.push({
					sourceId: ref.slice(0, dotIdx),
					sourcePort: ref.slice(dotIdx + 1),
					targetId: currentNodeId,
					targetPort: key,
					line: lineNum,
					rawText: line,
					scopeId: '__root__',
				});
				i++; continue;
			}

			// Triple backtick multiline: key: ``` ... ```
			const tripleBacktickMatch = trimmed.match(/^([a-zA-Z_][a-zA-Z0-9_]*)\s*:\s*```(.*)$/);
			if (tripleBacktickMatch) {
				const btRemainder = tripleBacktickMatch[2];
				// Inline form: key: ```content```
				if (btRemainder.endsWith('```') && btRemainder.length > 3) {
					const inlineVal = btRemainder.slice(0, -3);
					setConfigField(currentConfig, currentConfigSpans, errors, tripleBacktickMatch[1], inlineVal, lineNum, lineNum);
					i++; continue;
				}
				inMultiLine = true;
				multiLineKey = tripleBacktickMatch[1];
				multiLineValue = btRemainder;
				multiLineDelimiter = '```';
				multiLineStartLine = lineNum;
				i++; continue;
			}
			const kvMatch = trimmed.match(/^([a-zA-Z_][a-zA-Z0-9_]*)\s*:\s*(.+)$/);
			if (kvMatch) {
				const key = kvMatch[1];
				const rawValue = kvMatch[2].trim();
				// Multi-line JSON: value starts with [ or { but isn't balanced
				if ((rawValue.startsWith('[') || rawValue.startsWith('{')) && !isJsonBalanced(rawValue)) {
					const jsonStartLine = lineNum;
					let collected = rawValue;
					let depth = 0;
					for (const c of rawValue) { if (c === '[' || c === '{') depth++; if (c === ']' || c === '}') depth--; }
					const startI = i;
					i++;
					let hitBoundary = false;
					let jsonEndLine = jsonStartLine;
					const collectedLines: string[] = []; // track lines so we can pop if needed
					while (i < lines.length && depth > 0) {
						const ml = lines[i].trim();
						if (i - startI > 500) { hitBoundary = true; break; }
						if (!looksLikeJson(ml)) { hitBoundary = true; break; }
						collectedLines.push(ml);
						collected += '\n' + ml;
							for (const c of ml) { if (c === '[' || c === '{') depth++; if (c === ']' || c === '}') depth--; }
						if (depth <= 0) {
							jsonEndLine = i + 1;
							i++; // advance past the line that closed the brackets
							try {
								JSON.parse(collected);
								break; // valid JSON, done
							} catch {
								// JSON is broken. Pop standalone } / ] lines from the end
								// (they might be the node's closing brace, not JSON).
								while (collectedLines.length > 0) {
									const lastLine = collectedLines[collectedLines.length - 1];
									if (lastLine === '}' || lastLine === ']' || lastLine === '},' || lastLine === '],') {
										collectedLines.pop();
										collected = rawValue + (collectedLines.length > 0 ? '\n' + collectedLines.join('\n') : '');
										i--; // give this line back
										depth = 0;
										for (const c of collected) { if (c === '[' || c === '{') depth++; if (c === ']' || c === '}') depth--; }
									} else {
										break;
									}
								}
								hitBoundary = true;
								break;
							}
						}
						jsonEndLine = i + 1;
						i++;
					}
					if (depth > 0 || hitBoundary) {
						errors.push({ line: lineNum, message: `Broken JSON for "${key}": brackets not balanced (missing ${depth} closing bracket(s))` });
					}
					setConfigField(currentConfig, currentConfigSpans, errors, key, parseConfigValue(collected, errors, lineNum, key), jsonStartLine, jsonEndLine);
					continue;
				}
				setConfigField(currentConfig, currentConfigSpans, errors, key, parseConfigValue(rawValue, errors, lineNum, key), lineNum, lineNum);
				i++; continue;
			}
			addOpaqueLine(lineNum, line, `Invalid config line: ${trimmed}`);
			i++; continue;
		}

		// Connection with inline RHS: `target.port = Type { ... }.portName`.
		// The RHS spans multiple lines; delegate to tryParseInlineExpression.
		// This must be checked BEFORE the simple connection regex below so
		// that inline expressions are not rejected as unparsable.
		const inlineConnMatch = trimmed.match(/^([a-zA-Z_][a-zA-Z0-9_.]*)\s*=\s*(.+)$/);
		if (inlineConnMatch && inlineConnMatch[1].includes('.') && looksLikeInlineStart(inlineConnMatch[2])) {
			flushOpaque(); flushNode();
			const left = inlineConnMatch[1];
			const eqPos = line.indexOf('=');
			const startCol = eqPos + 1;
			// Parse the LHS into targetId / targetPort (with self handling for groups).
			let parentLocalId: string;
			let fieldKey: string;
			let targetIsSelf = false;
			if (!isRoot && left.startsWith('self.')) {
				parentLocalId = 'self';
				fieldKey = left.slice(5);
				targetIsSelf = true;
			} else {
				const dotPos = left.indexOf('.');
				if (dotPos < 0) {
					errors.push({ line: lineNum, message: `Invalid connection target: '${left}'` });
					i++; continue;
				}
				parentLocalId = left.slice(0, dotPos);
				fieldKey = left.slice(dotPos + 1);
			}
			const rhsInlineScope: InlineScope = { nodes: [], connections: [] };
			const next = tryParseInlineExpression(lines, i, startCol, parentLocalId, fieldKey, rhsInlineScope, errors);
			if (next !== null) {
				// Merge into the current scope.
				if (isRoot) {
					for (const child of rhsInlineScope.nodes) {
						nodes.push(child);
						itemOrder.push(`node:${child.id}`);
						itemGaps.push(0);
					}
					for (const conn of rhsInlineScope.connections) {
						connections.push({ ...conn, scopeId: '__root__' });
					}
				} else {
					// Group scope: prefix anon ids and handle self references.
					for (const child of rhsInlineScope.nodes) {
						const scopedChildId = `${scopeId}.${child.id}`;
						nodes.push({
							...child,
							id: scopedChildId,
							parentId: scopeId,
						});
						itemOrder.push(`node:${scopedChildId}`);
						itemGaps.push(0);
					}
					for (const conn of rhsInlineScope.connections) {
						let srcId = conn.sourceId;
						let srcIsSelf = false;
						if (srcId === 'self') {
							srcId = scopeId;
							srcIsSelf = true;
						} else if (isLocalRef(srcId, localChildIds)) {
							srcId = `${scopeId}.${srcId}`;
						}
						let tgtId = conn.targetId;
						let tgtIsSelf = targetIsSelf && conn.targetId === parentLocalId;
						if (conn.targetId === 'self' || tgtIsSelf) {
							tgtId = scopeId;
							tgtIsSelf = true;
						} else if (isLocalRef(tgtId, localChildIds)) {
							tgtId = `${scopeId}.${tgtId}`;
						}
						connections.push({
							...conn,
							sourceId: srcId,
							sourceIsSelf: srcIsSelf,
							targetId: tgtId,
							targetIsSelf: tgtIsSelf,
							scopeId,
						});
					}
				}
				itemGaps.push(blankLineCount);
				blankLineCount = 0;
				i = next;
				continue;
			}
			// Parse error already pushed; skip.
			i++; continue;
		}

		// Connection parsing: target.port = source.port (edge) OR
		// target.port = "literal" / number / bool / JSON (config fill).
		if (isRoot) {
			const connMatch = trimmed.match(/^([a-zA-Z_][a-zA-Z0-9_]*)\.([a-zA-Z_][a-zA-Z0-9_]*)\s*=\s*([a-zA-Z_][a-zA-Z0-9_]*)\.([a-zA-Z_][a-zA-Z0-9_]*)$/);
			if (connMatch) {
				flushOpaque(); flushNode();
				const targetId = connMatch[1], targetPort = connMatch[2];
				const sourceId = connMatch[3], sourcePort = connMatch[4];
				const connId = `${targetId}.${targetPort}=${sourceId}.${sourcePort}`;
				connections.push({ sourceId, sourcePort, targetId, targetPort, line: lineNum, rawText: line, scopeId: '__root__' });
				itemOrder.push(`conn:${connId}`);
				itemGaps.push(blankLineCount);
				blankLineCount = 0;
				lastAnchor = `conn:${connId}`;
				i++; continue;
			}
			// Literal RHS config fill: target.port = "str" / 42 / true / [...] / {...}
			// or multi-line triple-backtick string / multi-line JSON.
			const literalConnMatch = trimmed.match(/^([a-zA-Z_][a-zA-Z0-9_]*)\.([a-zA-Z_][a-zA-Z0-9_]*)\s*=\s*(.+)$/);
			if (literalConnMatch) {
				const rawValue = literalConnMatch[3].trim();
				const targetId = literalConnMatch[1];
				const targetPort = literalConnMatch[2];
				// Try single-line literal first.
				const literal = tryParseLiteral(rawValue);
				if (literal !== undefined) {
					flushOpaque(); flushNode();
					applyConfigFill(nodes, targetId, targetPort, literal, lineNum, lineNum);
					itemGaps.push(blankLineCount);
					blankLineCount = 0;
					i++; continue;
				}
				// Multi-line triple-backtick or multi-line JSON.
				const multi = tryCollectMultilineLiteralRhs(lines, i, rawValue);
				if (multi) {
					flushOpaque(); flushNode();
					applyConfigFill(nodes, targetId, targetPort, multi.value, lineNum, multi.nextLineIdx);
					itemGaps.push(blankLineCount);
					blankLineCount = 0;
					i = multi.nextLineIdx;
					continue;
				}
			}
		} else {
			// Group: child.port = self.port | self.port = child.port | child.port = child.port
			// Also: child.port = literal for config fills (single-line, triple-backtick
			// multi-line, or multi-line JSON). self.port = literal is not meaningful.
			const literalConnMatch = trimmed.match(/^([a-zA-Z_][a-zA-Z0-9_]*)\.([a-zA-Z_][a-zA-Z0-9_]*)\s*=\s*(.+)$/);
			if (literalConnMatch && !trimmed.startsWith('self.')) {
				const rawValue = literalConnMatch[3].trim();
				const childLocal = literalConnMatch[1];
				const targetPort = literalConnMatch[2];
				const literal = tryParseLiteral(rawValue);
				if (literal !== undefined) {
					applyConfigFill(nodes, `${scopeId}.${childLocal}`, targetPort, literal, lineNum, lineNum);
					i++; continue;
				}
				const multi = tryCollectMultilineLiteralRhs(lines, i, rawValue);
				if (multi) {
					applyConfigFill(nodes, `${scopeId}.${childLocal}`, targetPort, multi.value, lineNum, multi.nextLineIdx);
					i = multi.nextLineIdx;
					continue;
				}
			}
			const connMatch = trimmed.match(/^([a-zA-Z_][a-zA-Z0-9_.]*)\s*=\s*([a-zA-Z_][a-zA-Z0-9_.]*)$/);
			if (connMatch) {
				const left = connMatch[1]; const right = connMatch[2];
				let sourceId: string, sourcePort: string;
				let targetId: string, targetPort: string;
				let sourceIsSelf = false, targetIsSelf = false;

				// left = target (input being set), right = source (output providing value)
				// self.port on left = group output, self.port on right = group input
				if (left.startsWith('self.')) {
					targetId = scopeId; targetPort = left.slice(5); targetIsSelf = true;
				} else if (left.includes('.')) {
					const dotPos = left.indexOf('.');
					targetId = `${scopeId}.${left.slice(0, dotPos)}`; targetPort = left.slice(dotPos + 1);
				} else {
					errors.push({ line: lineNum, message: `Invalid connection target: '${left}'. Use 'self.port' for group output or 'child.port' for a child node` });
					addOpaqueLine(lineNum, line, `Invalid connection target '${left}'`);
					i++; continue;
				}
				if (right.startsWith('self.')) {
					sourceId = scopeId; sourcePort = right.slice(5); sourceIsSelf = true;
				} else if (right.includes('.')) {
					const dotPos = right.indexOf('.');
					sourceId = `${scopeId}.${right.slice(0, dotPos)}`; sourcePort = right.slice(dotPos + 1);
				} else {
					errors.push({ line: lineNum, message: `Invalid connection source: '${right}'. Use 'self.port' for group input or 'child.port' for a child node` });
					addOpaqueLine(lineNum, line, `Invalid connection source '${right}'`);
					i++; continue;
				}
				connections.push({ sourceId, sourcePort, targetId, targetPort, line: lineNum, rawText: line, sourceIsSelf, targetIsSelf, scopeId });
				i++; continue;
			}
		}

		// Unrecognized line → opaque
		if (trimmed.endsWith('{')) {
			insideOpaqueBlock = true;
			addOpaqueLine(lineNum, line, `Unexpected line: ${trimmed}`);
		} else {
			addOpaqueLine(lineNum, line, `Unexpected line: ${trimmed}`);
		}
		i++;
	}

	// Flush remaining state
	flushNode();
	flushOpaque();

	// Rewrite connections that reference scoped child names.
	// Skip rewriting if the ID equals scopeId, that reference means the scope
	// itself (interface port), not the disambiguated child.
	if (scopedIdMap.size > 0) {
		for (const conn of connections) {
			if (!conn.sourceIsSelf) {
				const scopedSrc = scopedIdMap.get(conn.sourceId);
				if (scopedSrc) conn.sourceId = scopedSrc;
			}
			if (!conn.targetIsSelf) {
				const scopedTgt = scopedIdMap.get(conn.targetId);
				if (scopedTgt) conn.targetId = scopedTgt;
			}
		}
	}

	const rawLines = isRoot ? [] : lines.slice(scopeStartLine - 1, endLine);

	return {
		nodes, connections, groups: localGroups, opaqueBlocks, itemOrder, itemGaps,
		inPorts, outPorts, endIdx: i, closed: isRoot ? true : closed,
		name, description, rawLines,
	};
}

export interface OpaqueBlock {
	startLine: number;
	endLine: number;
	text: string;
	error: string;
	anchorAfter: string | null;
}

export interface ParseResult {
	name: string;
	description: string;
	nodes: ParsedNode[];
	connections: ParsedConnection[];
	groups: ParsedGroup[];
	opaqueBlocks: OpaqueBlock[];
	nodeOrder: string[];
	itemOrder: string[];
	itemGaps: number[];
}

export interface WeftParseError {
	line: number;
	message: string;
}

export interface WeftParseMultiOutput {
	projects: { project: ProjectDefinition; errors: WeftParseError[]; warnings: WeftWarning[]; opaqueBlocks: OpaqueBlock[]; nodeOrder: string[]; itemOrder: string[]; itemGaps: number[] }[];
	errors: WeftParseError[];
}

function extractAllWeftBlocks(text: string): string[] {
	const blocks: string[] = [];
	// Command fences are always 4 backticks; 3 backticks are multiline string delimiters
	const pattern = /````weft\s*\n/g;
	let match;
	while ((match = pattern.exec(text)) !== null) {
		const closer = '\n````';
		const contentStart = match.index + match[0].length;
		const rest = text.substring(contentStart);
		const closerIdx = rest.indexOf(closer);
		const weftContent = closerIdx >= 0 ? rest.substring(0, closerIdx) : rest;
		const trimmed = weftContent.trim();
		if (trimmed) blocks.push(trimmed);
		if (closerIdx >= 0) {
			pattern.lastIndex = contentStart + closerIdx + closer.length;
		}
	}
	return blocks;
}

export function parseRawWeft(weft: string): { result: ParseResult; errors: WeftParseError[] } {
	const errors: WeftParseError[] = [];
	const lines = weft.split('\n');
	const allGroups: ParsedGroup[] = [];

	const scope = parseScope(lines, 0, '__root__', undefined, 1, errors, allGroups, new Set(), true);

	// Collect all groups (including nested ones found by parseScope recursively)
	const nodeOrder = scope.nodes.map(n => n.id);
	return {
		result: {
			name: scope.name,
			description: scope.description,
			nodes: scope.nodes,
			connections: scope.connections,
			groups: allGroups,
			opaqueBlocks: scope.opaqueBlocks,
			nodeOrder,
			itemOrder: scope.itemOrder,
			itemGaps: scope.itemGaps,
		},
		errors,
	};
}

/** Coerce config values to match the types expected by the node template.
 *  Uses field definitions (type: 'number', 'textarea', etc.) and output port
 *  types to convert strings to numbers, parse JSON strings into objects/arrays, etc. */
function coerceConfigValues(node: ParsedNode, template: (typeof NODE_TYPE_CONFIG)[string]) {
	// For hasFormSchema nodes, 'fields' must be a parsed array (not a string)
	if (template.features?.hasFormSchema && typeof node.config.fields === 'string') {
		try { node.config.fields = JSON.parse(node.config.fields); } catch { /* keep as-is */ }
	}

	for (const field of template.fields) {
		const val = node.config[field.key];
		if (val === undefined || val === null) continue;

		// Find the output port that matches this field key to determine the expected type
		const outputPort = template.defaultOutputs.find(p => p.name === field.key);
		const portType = outputPort?.portType;

		if (field.type === 'number' || portType === 'Number') {
			if (typeof val === 'string') {
				const n = Number(val);
				if (!isNaN(n)) node.config[field.key] = n;
			}
		} else if (portType?.startsWith('List[') || portType?.startsWith('Dict[')) {
			if (typeof val === 'string') {
				try { node.config[field.key] = JSON.parse(val); } catch { /* keep as string */ }
			}
		}
	}
}

// ── Type resolution and validation pipeline ─────────────────────────────────
// SYNC WARNING: This pipeline mirrors the backend enrich.rs (weft-nodes crate).
// Both must produce identical errors for the same input. When changing logic here,
// update enrich.rs too (and vice versa). The backend is the authoritative check
// at execution time; this frontend copy provides instant editor feedback.
//
// Steps (must match enrich.rs order):
// 1. Call resolveTypes for dynamic nodes (Pack, Unpack)
// 2. Resolve TypeVars per node from connected edges (resolve_and_narrow)
// 2.5. Infer expand/gather lane modes from type mismatches (infer_lane_modes)
// 3. Validate stack depth (validate_stack_depth)
// 4. Validate edge type compatibility (validate_edge_types)
// 5. Check for remaining MustOverride on connected ports (validate_no_unresolved)
// 6. Validate edge ports exist (validate_edge_ports)

/** Try peeling List[] wrappers from source to find expand depth compatible with target. */
function tryExpandDepth(srcType: string, tgtType: string): number {
	let current = srcType;
	let depth = 0;
	while (true) {
		const parsed = parseWeftType(current);
		if (!parsed || parsed.kind !== 'list') return 0;
		depth++;
		const innerStr = weftTypeToString(parsed.inner);
		if (isWeftTypeCompatible(innerStr, tgtType)) return depth;
		current = innerStr;
	}
}

/** Try peeling List[] wrappers from target to find gather depth compatible with source. */
function tryGatherDepth(srcType: string, tgtType: string): number {
	let current = tgtType;
	let depth = 0;
	while (true) {
		const parsed = parseWeftType(current);
		if (!parsed || parsed.kind !== 'list') return 0;
		depth++;
		const innerStr = weftTypeToString(parsed.inner);
		if (isWeftTypeCompatible(srcType, innerStr)) return depth;
		current = innerStr;
	}
}

function isTypeVar(portType: PortType): boolean {
	const parsed = parseWeftType(portType);
	if (!parsed) return false;
	return parsed.kind === 'typevar';
}

function isMustOverride(portType: PortType): boolean {
	return portType === 'MustOverride';
}

function isUnresolved(portType: PortType): boolean {
	return isTypeVar(portType) || isMustOverride(portType);
}

/** Check if a type contains any TypeVar references */
function containsTypeVar(portType: PortType): boolean {
	const parsed = parseWeftType(portType);
	if (!parsed) return false;
	return hasTypeVarInParsed(parsed);
}

function hasTypeVarInParsed(t: import('$lib/types').WeftType): boolean {
	switch (t.kind) {
		case 'typevar': return true;
		case 'must_override': return false;
		case 'primitive': return false;
		case 'json_dict': return false;
		case 'list': return hasTypeVarInParsed(t.inner);
		case 'dict': return hasTypeVarInParsed(t.key) || hasTypeVarInParsed(t.value);
		case 'union': return t.types.some(hasTypeVarInParsed);
	}
}

/** Substitute TypeVars in a type string using bindings map */
function substituteTypeVars(portType: PortType, bindings: Map<string, PortType>): PortType {
	const parsed = parseWeftType(portType);
	if (!parsed) return portType;
	const substituted = substituteInParsed(parsed, bindings);
	return weftTypeToString(substituted);
}

function substituteInParsed(t: import('$lib/types').WeftType, bindings: Map<string, PortType>): import('$lib/types').WeftType {
	switch (t.kind) {
		case 'typevar': {
			const bound = bindings.get(t.name);
			if (bound) {
				const parsed = parseWeftType(bound);
				return parsed ?? t;
			}
			return t;
		}
		case 'list': return { kind: 'list', inner: substituteInParsed(t.inner, bindings) };
		case 'dict': return { kind: 'dict', key: substituteInParsed(t.key, bindings), value: substituteInParsed(t.value, bindings) };
		case 'union': return { kind: 'union', types: t.types.map(u => substituteInParsed(u, bindings)) };
		default: return t;
	}
}

/** Extract TypeVar bindings by structurally matching a pattern against a concrete type.
 *  E.g. pattern List[T] + concrete List[Dict[String, Number]] → {T: "Dict[String, Number]"} */
function extractTypeVarBindings(
	pattern: import('$lib/types').WeftType,
	concrete: import('$lib/types').WeftType,
	bindings: Map<string, string>,
	nodeId: string,
	errors: WeftParseError[],
	line: number = 0,
) {
	if (pattern.kind === 'typevar') {
		const concreteStr = weftTypeToString(concrete);
		if (!isUnresolved(concreteStr)) {
			bindTypeVar(bindings, pattern.name, concreteStr, nodeId, errors, line);
		}
		return;
	}
	if (pattern.kind === 'list' && concrete.kind === 'list') {
		extractTypeVarBindings(pattern.inner, concrete.inner, bindings, nodeId, errors, line);
		return;
	}
	if (pattern.kind === 'dict' && concrete.kind === 'dict') {
		extractTypeVarBindings(pattern.key, concrete.key, bindings, nodeId, errors, line);
		extractTypeVarBindings(pattern.value, concrete.value, bindings, nodeId, errors, line);
		return;
	}
	// Union pattern against any concrete type (union or single)
	if (pattern.kind === 'union') {
		const concreteTypes = concrete.kind === 'union' ? concrete.types : [concrete];
		const concretePatterns = pattern.types.filter(t => t.kind !== 'typevar');
		const typeVars = pattern.types.filter(t => t.kind === 'typevar') as Array<{ kind: 'typevar'; name: string }>;

		if (typeVars.length === 0) {
			if (concrete.kind === 'union' && pattern.types.length === concrete.types.length) {
				for (let i = 0; i < pattern.types.length; i++) {
					extractTypeVarBindings(pattern.types[i], concrete.types[i], bindings, nodeId, errors, line);
				}
			}
		} else {
			// Remove matched concrete types, keep remaining
			const remaining = concreteTypes.filter(c => {
				const cStr = weftTypeToString(c);
				return !concretePatterns.some(p => {
					const pStr = weftTypeToString(p);
					return isWeftTypeCompatible(cStr, pStr) && isWeftTypeCompatible(pStr, cStr);
				});
			});

			// Pad remaining with Empty if more TypeVars than concrete types
			while (remaining.length < typeVars.length) {
				remaining.push({ kind: 'primitive', value: 'Empty' } as any);
			}
			{
				// Each TypeVar consumes one, last one takes all remaining
				const pool = [...remaining];
				for (let i = 0; i < typeVars.length; i++) {
					if (i === typeVars.length - 1) {
						const resolved = pool.length === 1
							? weftTypeToString(pool[0])
							: pool.map(weftTypeToString).join(' | ');
						bindTypeVar(bindings, typeVars[i].name, resolved, nodeId, errors, line);
					} else {
						bindTypeVar(bindings, typeVars[i].name, weftTypeToString(pool.shift()!), nodeId, errors, line);
					}
				}
			}
		}
		return;
	}
}

/** Bind a TypeVar, checking for conflicts */
function bindTypeVar(
	bindings: Map<string, string>,
	varName: string,
	concrete: string,
	nodeId: string,
	errors: WeftParseError[],
	line: number = 0,
) {
	const existing = bindings.get(varName);
	if (existing) {
		if (!isWeftTypeCompatible(concrete, existing) && !isWeftTypeCompatible(existing, concrete)) {
			errors.push({
				line,
				message: `Node ${nodeId}: type variable ${varName} has conflicting bindings: ${existing} vs ${concrete}`,
			});
		}
	} else {
		bindings.set(varName, concrete);
	}
}

/** Expand Group nodes into Passthrough pairs for validation.
 *  Mirrors the backend compiler's flatten logic:
 *  - Group → {id}__in Passthrough + {id}__out Passthrough
 *  - __inner edges rewired to Passthrough ports
 *  - External edges rewired to Passthrough ports
 *  Returns new nodes and edges arrays (originals not mutated). */
function expandGroupsForValidation(
	nodes: NodeInstance[],
	edges: Edge[],
): { nodes: NodeInstance[]; edges: Edge[] } {
	const result: NodeInstance[] = [];
	const newEdges: Edge[] = [];

	for (const node of nodes) {
		if (node.nodeType !== 'Group') {
			result.push(node);
			continue;
		}

		// Create input passthrough: {id}__in
		const inPtId = `${node.id}__in`;
		const inPtInputs: PortDefinition[] = node.inputs.map(p => ({ ...p }));
		const inPtOutputs: PortDefinition[] = node.inputs.map(p => ({
			name: p.name,
			portType: p.portType, // post-transform = declared type
			required: false,
			laneMode: 'Single' as LaneMode,
		}));
		result.push({
			id: inPtId,
			nodeType: 'Passthrough',
			label: `${node.id} (in)`,
			config: {},
			position: { x: 0, y: 0 },
			inputs: inPtInputs,
			outputs: inPtOutputs,
			features: {},
		});

		// Create output passthrough: {id}__out
		const outPtId = `${node.id}__out`;
		const outPtInputs: PortDefinition[] = node.outputs.map(p => {
			// Pre-transform type for the internal input
			let preType = p.portType;
			if (p.laneMode === 'Gather') {
				const parsed = parseWeftType(p.portType);
				if (parsed && parsed.kind === 'list') {
					preType = weftTypeToString(parsed.inner);
				}
			} else if (p.laneMode === 'Expand') {
				preType = `List[${p.portType}]`;
			}
			return {
				name: p.name,
				portType: preType,
				required: false,
				laneMode: 'Single' as LaneMode,
			};
		});
		const outPtOutputs: PortDefinition[] = node.outputs.map(p => ({ ...p }));
		result.push({
			id: outPtId,
			nodeType: 'Passthrough',
			label: `${node.id} (out)`,
			config: {},
			position: { x: 0, y: 0 },
			inputs: outPtInputs,
			outputs: outPtOutputs,
			features: {},
		});
	}

	// Rewrite edges
	for (const edge of edges) {
		const srcHandle = edge.sourceHandle ?? '';
		const tgtHandle = edge.targetHandle ?? '';

		// __inner handles: rewrite to passthrough nodes
		if (srcHandle.endsWith('__inner')) {
			// in.X -> child: source is group input passthrough
			const portName = srcHandle.slice(0, -'__inner'.length);
			newEdges.push({
				...edge,
				source: `${edge.source}__in`,
				sourceHandle: portName,
			});
		} else if (tgtHandle.endsWith('__inner')) {
			// child -> out.X: target is group output passthrough
			const portName = tgtHandle.slice(0, -'__inner'.length);
			newEdges.push({
				...edge,
				target: `${edge.target}__out`,
				targetHandle: portName,
			});
		} else {
			// External edges: check if source/target is a group
			const srcNode = nodes.find(n => n.id === edge.source);
			const tgtNode = nodes.find(n => n.id === edge.target);

			let newSrc = edge.source;
			let newSrcHandle = edge.sourceHandle;
			let newTgt = edge.target;
			let newTgtHandle = edge.targetHandle;

			// Source is a group → route through __out passthrough
			if (srcNode?.nodeType === 'Group') {
				newSrc = `${edge.source}__out`;
			}
			// Target is a group → route through __in passthrough
			if (tgtNode?.nodeType === 'Group') {
				newTgt = `${edge.target}__in`;
			}

			newEdges.push({
				...edge,
				source: newSrc,
				sourceHandle: newSrcHandle,
				target: newTgt,
				targetHandle: newTgtHandle,
			});
		}
	}

	return { nodes: result, edges: newEdges };
}

/** @internal Exported for testing only */
export type WeftWarning = { line: number; message: string };

function validateRequiredPorts(
	nodes: NodeInstance[],
	edges: Edge[],
	errors: WeftParseError[],
): void {
	const edgeTargets = new Set<string>();
	for (const e of edges) {
		const port = e.targetHandle || 'default';
		edgeTargets.add(`${e.target}|${port}`);
	}

	for (const node of nodes) {
		// Passthrough boundaries are validated through the Group that owns
		// them (the Group's interface port metadata is authoritative).
		if (node.nodeType === 'Passthrough') continue;
		const cfg = (node as any).config;
		const inputs = (node as any).inputs as PortDefinition[] | undefined;
		if (!inputs) continue;
		const isGroup = node.nodeType === 'Group';

		for (const port of inputs) {
			if (!port.required) continue;
			if (edgeTargets.has(`${node.id}|${port.name}`)) continue;

			// Groups do not accept config-filled interface ports: their inputs
			// must always be wired with an edge (if it compiles, it runs).
			if (!isGroup && isPortConfigurable(port)) {
				const val = cfg?.[port.name];
				if (val !== undefined && val !== null && val !== '') continue;
			}

			const hint = isGroup
				? ' (wire an edge into this group input)'
				: isPortConfigurable(port)
					? ` (wire an edge or set a '${port.name}' config value)`
					: ' (wire an edge, this port cannot be filled by config)';
			const label = isGroup ? 'Group' : 'Node';
			errors.push({
				line: (node as any).startLine ?? 0,
				message: `${label} ${node.id}: required input port '${port.name}' is not connected${hint}`,
			});
		}

		// oneOfRequired groups: at least one port in each group must be satisfied.
		const oneOfRequired = ((node as any).features?.oneOfRequired) as string[][] | undefined;
		if (oneOfRequired) {
			for (const group of oneOfRequired) {
				if (group.length === 0) continue;
				const anySatisfied = group.some((portName: string) => {
					if (edgeTargets.has(`${node.id}|${portName}`)) return true;
					const port = inputs.find(p => p.name === portName);
					if (!isGroup && port && isPortConfigurable(port)) {
						const val = cfg?.[portName];
						if (val !== undefined && val !== null && val !== '') return true;
					}
					return false;
				});
				if (!anySatisfied) {
					const label = isGroup ? 'Group' : 'Node';
					errors.push({
						line: (node as any).startLine ?? 0,
						message: `${label} ${node.id}: at least one of [${group.join(', ')}] must be connected`,
					});
				}
			}
		}
	}
}

function validateConfigFilledPorts(
	nodes: NodeInstance[],
	edges: Edge[],
	errors: WeftParseError[],
): void {
	const edgeTargets = new Set<string>();
	for (const e of edges) {
		const port = e.targetHandle || 'default';
		edgeTargets.add(`${e.target}|${port}`);
	}

	for (const node of nodes) {
		const cfg = (node as any).config;
		if (!cfg || typeof cfg !== 'object') continue;
		const inputs = (node as any).inputs as PortDefinition[] | undefined;
		if (!inputs) continue;

		for (const port of inputs) {
			const value = cfg[port.name];
			if (value === undefined || value === null || value === '') continue;

			if (edgeTargets.has(`${node.id}|${port.name}`)) continue; // edge wins

			// Port has a config value but is marked wired-only (configurable false).
			// Writing a literal to this port is not allowed.
			if (!isPortConfigurable(port)) {
				errors.push({
					line: (node as any).startLine ?? 0,
					message: `Node ${node.id}: input port '${port.name}' is wired-only and cannot be set from config. Wire an edge instead.`,
				});
				continue;
			}

			const parsedPortType = parseWeftType(port.portType);
			if (!parsedPortType) continue;
			if (parsedPortType.kind === 'typevar' || parsedPortType.kind === 'must_override') continue;

			const inferred = inferTypeFromValue(value);
			if (!isCompatible(inferred, parsedPortType)) {
				errors.push({
					line: (node as any).startLine ?? 0,
					message: `Node ${node.id}: config field '${port.name}' has type ${weftTypeToString(inferred)} but the port expects ${weftTypeToString(parsedPortType)}`,
				});
			}
		}
	}
}

export function resolveAndValidateTypes(
	origNodes: NodeInstance[],
	origEdges: Edge[],
	errors: WeftParseError[],
	warnings?: WeftWarning[],
) {
	// Step 0: Expand Group nodes into Passthrough pairs for uniform validation
	const { nodes, edges } = expandGroupsForValidation(origNodes, origEdges);

	const nodeMap = new Map<string, NodeInstance>();
	for (const n of nodes) nodeMap.set(n.id, n);

	// Step 1: Call resolveTypes for dynamic nodes (Pack, Unpack, etc.)
	for (const node of nodes) {
		const template = NODE_TYPE_CONFIG[node.nodeType];
		if (!template?.resolveTypes) continue;
		const resolved = template.resolveTypes(node.inputs, node.outputs);
		if (resolved.inputs) {
			for (const [name, type] of Object.entries(resolved.inputs)) {
				const port = node.inputs.find(p => p.name === name);
				if (port) port.portType = type;
			}
		}
		if (resolved.outputs) {
			for (const [name, type] of Object.entries(resolved.outputs)) {
				const port = node.outputs.find(p => p.name === name);
				if (port) port.portType = type;
			}
		}
	}

	// Step 2: Resolve TypeVars per node from connected edges
	// Build port type lookup. Groups are already expanded into Passthrough nodes.
	const portTypes = new Map<string, PortType>();
	for (const node of nodes) {
		for (const port of node.inputs) {
			portTypes.set(`${node.id}:in:${port.name}`, port.portType);
		}
		for (const port of node.outputs) {
			portTypes.set(`${node.id}:out:${port.name}`, port.portType);
		}
	}

	for (const node of nodes) {
		const bindings = new Map<string, PortType>();

		for (const edge of edges) {
			if (edge.target === node.id) {
				const targetPort = node.inputs.find(p => p.name === edge.targetHandle);
				const sourceType = portTypes.get(`${edge.source}:out:${edge.sourceHandle}`);
				if (!targetPort || !sourceType || isUnresolved(sourceType)) continue;

				// 1. Extract TypeVar bindings from original type
				if (containsTypeVar(targetPort.portType)) {
					const patternParsed = parseWeftType(targetPort.portType);
					const concreteParsed = parseWeftType(sourceType);
					if (patternParsed && concreteParsed) {
						extractTypeVarBindings(patternParsed, concreteParsed, bindings, node.id, errors, (edge as any)._line ?? 0);
					}
				}

				// 2. Apply TypeVar substitutions immediately
				if (bindings.size > 0 && containsTypeVar(targetPort.portType)) {
					targetPort.portType = substituteTypeVars(targetPort.portType, bindings);
					portTypes.set(`${node.id}:in:${targetPort.name}`, targetPort.portType);
				}

				// No narrowing on input ports.
				// Input types declare what the node accepts, not what it receives.
			}
			// Output side: if this node's output port has a TypeVar, resolve from target's wire type
			if (edge.source === node.id) {
				const sourcePort = node.outputs.find(p => p.name === edge.sourceHandle);
				if (sourcePort && containsTypeVar(sourcePort.portType)) {
					const targetType = portTypes.get(`${edge.target}:in:${edge.targetHandle}`);
					if (targetType && !isUnresolved(targetType)) {
						// Compute wire type based on target port's lane mode
						const targetNode = nodeMap.get(edge.target);
						const targetPort = targetNode?.inputs.find(p => p.name === edge.targetHandle);
						let wireType = targetType;
						if (targetPort?.laneMode === 'Expand') {
							wireType = `List[${targetType}]`;
						} else if (targetPort?.laneMode === 'Gather') {
							const parsedTarget = parseWeftType(targetType);
							if (parsedTarget && parsedTarget.kind === 'list') {
								wireType = weftTypeToString(parsedTarget.inner);
							}
						}
						if (!isUnresolved(wireType)) {
							const patternParsed = parseWeftType(sourcePort.portType);
							const concreteParsed = parseWeftType(wireType);
							if (patternParsed && concreteParsed) {
								extractTypeVarBindings(patternParsed, concreteParsed, bindings, node.id, errors, (edge as any)._line ?? 0);
							}
						}
					}
				}
			}
		}

		// Apply bindings to all ports of this node
		if (bindings.size > 0) {
			for (const port of [...node.inputs, ...node.outputs]) {
				if (containsTypeVar(port.portType)) {
					port.portType = substituteTypeVars(port.portType, bindings);
				}
			}
			// Update the lookup too
			for (const port of node.inputs) portTypes.set(`${node.id}:in:${port.name}`, port.portType);
			for (const port of node.outputs) portTypes.set(`${node.id}:out:${port.name}`, port.portType);
		}
	}

	// Step 2.05: Narrow group-boundary passthrough output ports from their
	// incoming-edge source types. Mirrors narrow_group_passthroughs in enrich.rs.
	// Passthrough inputs keep the declared signature type; outputs reflect the
	// actual wired source so inner/outer consumers see the narrowed type. Run
	// iteratively because narrowing one passthrough can feed another downstream.
	{
		const maxIter = nodes.length + 1;
		for (let iter = 0; iter < maxIter; iter++) {
			// Snapshot output types
			const outTypes = new Map<string, PortType>();
			for (const n of nodes) {
				for (const port of n.outputs) {
					outTypes.set(`${n.id}:${port.name}`, port.portType);
				}
			}
			let changed = false;
			for (const node of nodes) {
				if (node.nodeType !== 'Passthrough') continue;
				for (const outPort of node.outputs) {
					const incomingEdge = edges.find(e =>
						e.target === node.id && (e.targetHandle || 'default') === outPort.name
					);
					if (!incomingEdge) continue;
					const srcPort = incomingEdge.sourceHandle || 'default';
					const srcType = outTypes.get(`${incomingEdge.source}:${srcPort}`);
					if (!srcType || isUnresolved(srcType)) continue;
					if (!isWeftTypeCompatible(srcType, outPort.portType)) continue;
					if (srcType !== outPort.portType) {
						outPort.portType = srcType;
						portTypes.set(`${node.id}:out:${outPort.name}`, srcType);
						changed = true;
					}
				}
			}
			if (!changed) break;
		}
	}

	// Step 2.5: Infer expand/gather lane modes from type mismatches on edges
	{
		const nodeById = new Map<string, NodeInstance>();
		for (const n of nodes) nodeById.set(n.id, n);

		for (const edge of edges) {
			const sourceNode = nodeById.get(edge.source);
			const targetNode = nodeById.get(edge.target);
			if (!sourceNode || !targetNode) continue;

			const sourcePort = sourceNode.outputs.find(p => p.name === edge.sourceHandle);
			const targetPort = targetNode.inputs.find(p => p.name === edge.targetHandle);
			if (!sourcePort || !targetPort) continue;

			// Skip if target already has an explicit lane mode (from catalog)
			if (targetPort.laneMode && targetPort.laneMode !== 'Single') continue;

			const srcType = sourcePort.portType;
			const tgtType = targetPort.portType;

			if (isUnresolved(srcType) || isUnresolved(tgtType)) continue;

			// If types are directly compatible, no inference needed
			if (isWeftTypeCompatible(srcType, tgtType)) continue;

			// Check if the target port is a catalog port (not user-declared)
			const targetTemplate = NODE_TYPE_CONFIG[targetNode.nodeType];
			const isCatalogPort = targetTemplate?.defaultInputs?.some((p: PortDefinition) => p.name === targetPort.name) ?? false;

			// Try expand: peel List[] from source, check compatibility with target
			const expandDepth = tryExpandDepth(srcType, tgtType);
			if (expandDepth > 0) {
				targetPort.laneMode = 'Expand';
				targetPort.laneDepth = expandDepth;
				if (isCatalogPort && warnings) {
					warnings.push({ line: 0, message: `Implicit expand: ${edge.source}.${edge.sourceHandle} (${srcType}) feeds ${edge.target}.${edge.targetHandle} (${tgtType}). This will fan out the list into parallel lanes. If unintended, add a processing node to convert the type before connecting to ${edge.target}.${edge.targetHandle}.` });
				}
				continue;
			}

			// Try gather: peel List[] from target, check source compatibility
			const gatherDepth = tryGatherDepth(srcType, tgtType);
			if (gatherDepth > 0) {
				targetPort.laneMode = 'Gather';
				targetPort.laneDepth = gatherDepth;
				if (isCatalogPort && warnings) {
					warnings.push({ line: 0, message: `Implicit gather: ${edge.source}.${edge.sourceHandle} (${srcType}) feeds ${edge.target}.${edge.targetHandle} (${tgtType}). This will collect all parallel values into a list. If unintended, add a processing node to convert the type before connecting to ${edge.target}.${edge.targetHandle}.` });
				}
				continue;
			}
		}
	}

	// Propagate inferred lane modes back to original nodes
	// (inference ran on expanded copies, need to update originals for rendering)
	for (const node of nodes) {
		// Direct match: non-passthrough nodes
		const orig = origNodes.find(n => n.id === node.id);
		if (orig) {
			for (const port of node.inputs) {
				const origPort = orig.inputs.find(p => p.name === port.name);
				if (origPort && port.laneMode && port.laneMode !== 'Single') {
					origPort.laneMode = port.laneMode;
				}
			}
			continue;
		}
		// Passthrough nodes: map back to the parent Group node
		// group__in passthrough inputs -> Group node inputs
		if (node.id.endsWith('__in')) {
			const groupId = node.id.slice(0, -4); // remove __in
			const groupNode = origNodes.find(n => n.id === groupId);
			if (groupNode) {
				for (const port of node.inputs) {
					const groupPort = groupNode.inputs.find(p => p.name === port.name);
					if (groupPort && port.laneMode && port.laneMode !== 'Single') {
						groupPort.laneMode = port.laneMode;
					}
				}
			}
		}
		// group__out passthrough: gather is inferred on its INPUTS (internal side)
		// Map to the Group node's OUTPUTS (external side) for visual display
		if (node.id.endsWith('__out')) {
			const groupId = node.id.slice(0, -5); // remove __out
			const groupNode = origNodes.find(n => n.id === groupId);
			if (groupNode) {
				for (const port of node.inputs) {
					const groupPort = groupNode.outputs.find(p => p.name === port.name);
					if (groupPort && port.laneMode && port.laneMode !== 'Single') {
						groupPort.laneMode = port.laneMode;
					}
				}
			}
		}
	}

	// Step 3: Validate stack depth (expand/gather balance)
	{
		const nodeById = new Map<string, NodeInstance>();
		for (const n of nodes) nodeById.set(n.id, n);

		// Build incoming edges per node
		const incomingEdges = new Map<string, Edge[]>();
		for (const edge of edges) {
			if (!incomingEdges.has(edge.target)) incomingEdges.set(edge.target, []);
			incomingEdges.get(edge.target)!.push(edge);
		}

		// Compute output depth per node via BFS
		const depthAtOutput = new Map<string, number>();
		const processed = new Set<string>();
		const queue: string[] = [];

		// Start with source nodes (no incoming edges).
		// Account for their output lane modes (e.g. a splitter with Expand output starts at depth 1).
		for (const node of nodes) {
			const hasIncoming = edges.some(e => e.target === node.id);
			if (!hasIncoming) {
				let depth = 0;
				for (const op of node.outputs) {
					if (op.laneMode === 'Expand') depth = 1;
					else if (op.laneMode === 'Gather') depth = -1; // will be caught as error
				}
				depthAtOutput.set(node.id, depth);
				processed.add(node.id);
				queue.push(node.id);
			}
		}

		let iterations = 0;
		const maxIterations = nodes.length * 2 + 1;
		while (queue.length > 0 && iterations < maxIterations) {
			iterations++;
			const sourceId = queue.shift()!;
			const sourceDepth = depthAtOutput.get(sourceId) ?? 0;

			for (const edge of edges) {
				if (edge.source !== sourceId) continue;
				const targetId = edge.target;
				const targetNode = nodeById.get(targetId);
				if (!targetNode) continue;

				const targetPortName = edge.targetHandle ?? 'default';
				const targetPort = targetNode.inputs.find(p => p.name === targetPortName);

				let targetDepth = sourceDepth;

				const portLaneDepth = targetPort?.laneDepth ?? 1;
				if (targetPort?.laneMode === 'Expand') {
					targetDepth += portLaneDepth;
				} else if (targetPort?.laneMode === 'Gather') {
					if (sourceDepth < portLaneDepth) {
						errors.push({
							line: 0,
							message: `Gather error: ${targetId}.${targetPortName} tries to gather ${portLaneDepth} level(s) but current depth is only ${sourceDepth}.`,
						});
					}
					targetDepth -= portLaneDepth;
				}

				// Check output lane modes
				let outputDepth = targetDepth;
				for (const op of targetNode.outputs) {
					if (op.laneMode === 'Expand') {
						const opDepth = op.laneDepth ?? 1;
						outputDepth = targetDepth + opDepth;
					} else if (op.laneMode === 'Gather') {
						const opDepth = op.laneDepth ?? 1;
						if (targetDepth < opDepth) {
							errors.push({
								line: 0,
								message: `Gather error: ${targetId}.${op.name} tries to gather ${opDepth} level(s) but current depth is only ${targetDepth}.`,
							});
						}
						outputDepth = targetDepth - opDepth;
					}
				}

				// Depth merging: lower depths broadcast to higher depths.
				// Take the max, runtime validates shape compatibility.
				const existing = depthAtOutput.get(targetId);
				if (existing !== undefined) {
					if (outputDepth > existing) {
						depthAtOutput.set(targetId, outputDepth);
					}
				} else {
					depthAtOutput.set(targetId, outputDepth);
				}

				if (!processed.has(targetId)) {
					const allSourcesDone = (incomingEdges.get(targetId) ?? [])
						.every(e => processed.has(e.source));
					if (allSourcesDone) {
						processed.add(targetId);
						queue.push(targetId);
					}
				}
			}
		}
	}

	// Step 4: Validate edge type compatibility (with wire type transformation)
	// Port types are post-operation. Wire types are what flows on the edge.
	// Source output wire type: always = declared type (post-operation result flows out)
	// Target input expected wire type:
	//   Single: declared type
	//   Expand: List[declared] (list arrives, expand unwraps to declared element)
	//   Gather: inner(declared) (elements arrive, gather collects to declared List[T])
	for (const edge of edges) {
		const sourceNode = nodeMap.get(edge.source);
		const targetNode = nodeMap.get(edge.target);
		if (!sourceNode || !targetNode) continue;

		const sourcePort = sourceNode.outputs.find(p => p.name === edge.sourceHandle);
		const targetPort = targetNode.inputs.find(p => p.name === edge.targetHandle);
		if (!sourcePort || !targetPort) continue;

		// Compute wire types (same logic as backend)
		const srcWire = sourcePort.portType;
		let tgtWire = targetPort.portType;

		const laneDepth = targetPort.laneDepth ?? 1;
		if (targetPort.laneMode === 'Expand') {
			// Wrap in List[] for each expand level
			for (let d = 0; d < laneDepth; d++) {
				tgtWire = `List[${tgtWire}]`;
			}
		} else if (targetPort.laneMode === 'Gather') {
			// Peel List[] for each gather level
			let current = tgtWire;
			for (let d = 0; d < laneDepth; d++) {
				const parsed = parseWeftType(current);
				if (parsed && parsed.kind === 'list') {
					current = weftTypeToString(parsed.inner);
				} else break;
			}
			tgtWire = current;
		}

		if (isUnresolved(srcWire) || isUnresolved(tgtWire)) continue;

		// Null at the top level of the source type is never a type error.
		// Required ports skip the node on null (null propagation).
		// Optional ports pass null through to the node's code.
		// Either way, the executor handles it. Strip Null before checking
		// compatibility so that e.g. String | Null into String is accepted.
		let effectiveSrc = srcWire;
		if (srcWire.includes('Null')) {
			const parsed = parseWeftType(srcWire);
			if (parsed && parsed.kind === 'union') {
				const filtered = parsed.types.filter(t => !(t.kind === 'primitive' && t.value === 'Null'));
				if (filtered.length > 0) {
					effectiveSrc = filtered.length === 1
						? weftTypeToString(filtered[0])
						: weftTypeToString({ kind: 'union', types: filtered });
				}
			}
		}

		if (!isWeftTypeCompatible(effectiveSrc, tgtWire)) {
			errors.push({
				line: (edge as any)._line ?? 0,
				message: `Type mismatch: ${edge.source}.${edge.sourceHandle} outputs ${srcWire} but ${edge.target}.${edge.targetHandle} expects ${tgtWire}`,
			});
		}
	}

	// Step 4: Check for remaining unresolved TypeVars and MustOverride on connected ports
	const connectedInputs = new Set<string>();
	const connectedOutputs = new Set<string>();
	for (const edge of edges) {
		connectedOutputs.add(`${edge.source}:${edge.sourceHandle}`);
		connectedInputs.add(`${edge.target}:${edge.targetHandle}`);
	}

	for (const node of nodes) {
		const nodeLine = (node as NodeInstance & { sourceLine?: number }).sourceLine ?? 0;
		for (const port of node.inputs) {
			const key = `${node.id}:${port.name}`;
			if (!connectedInputs.has(key)) continue;
			if (isMustOverride(port.portType)) {
				errors.push({
					line: nodeLine,
					message: `Node ${node.id}: input port "${port.name}" requires a type declaration (e.g. ${port.name}: String)`,
				});
			} else if (containsTypeVar(port.portType)) {
				errors.push({
					line: nodeLine,
					message: `Node ${node.id}: input port "${port.name}" has unresolved type variable in '${port.portType}', could not infer type from connections`,
				});
			}
		}
		for (const port of node.outputs) {
			const key = `${node.id}:${port.name}`;
			if (!connectedOutputs.has(key)) continue;
			if (isMustOverride(port.portType)) {
				errors.push({
					line: nodeLine,
					message: `Node ${node.id}: output port "${port.name}" requires a type declaration (e.g. ${port.name}: String)`,
				});
			} else if (containsTypeVar(port.portType)) {
				errors.push({
					line: nodeLine,
					message: `Node ${node.id}: output port "${port.name}" has unresolved type variable in '${port.portType}', could not infer type from connections`,
				});
			}
		}
	}

	// Step 5: Warn when a Gather input port's declared type doesn't include Null.
	// After gather, null lanes from skipped nodes mean the actual type is List[T | Null].
	// If the port declares List[T] without Null, it may fail at runtime.
	if (warnings) {
		for (const node of nodes) {
			for (const port of node.inputs) {
				if (port.laneMode !== 'Gather') continue;
				const parsed = parseWeftType(port.portType);
				if (!parsed || parsed.kind !== 'list') continue;

				const inner = parsed.inner;
				let hasNull = false;
				if (inner.kind === 'union') {
					hasNull = inner.types.some((t) => t.kind === 'primitive' && t.value === 'Null');
				} else if (inner.kind === 'primitive' && inner.value === 'Null') {
					hasNull = true;
				}

				if (!hasNull) {
					const innerStr = weftTypeToString(inner);
					const nLine = (node as NodeInstance & { sourceLine?: number }).sourceLine ?? 0;
					warnings.push({
						line: nLine,
						message: `${node.id}.${port.name} gathers data but its type List[${innerStr}] doesn't handle null lanes. Consider using List[${innerStr} | Null] in case some lanes were skipped.`,
					});
				}
			}
		}
	}

	// Step 5.5: Warn on nodes whose outputs are completely dangling.
	// A node that produces outputs but none of them are connected is usually dead weight:
	// it wastes compute/money (LlmInference, LlmConfig) or signals a bug in the graph.
	// Skip nodes with no outputs at all (pure sinks) and Passthrough boundaries.
	// Note: this is a warning, not an error, because some nodes may be terminal
	// and exposed via the runner's loom `output` declaration (not visible to this parser).
	if (warnings) {
		const origConnectedOutputs = new Set<string>();
		for (const edge of origEdges) {
			origConnectedOutputs.add(`${edge.source}:${edge.sourceHandle}`);
		}
		for (const node of origNodes) {
			if (node.nodeType === 'Passthrough') continue;
			if (node.outputs.length === 0) continue;
			const anyConnected = node.outputs.some(p => origConnectedOutputs.has(`${node.id}:${p.name}`));
			if (!anyConnected) {
				const portNames = node.outputs.map(p => p.name).join(', ');
				const nLine = (node as NodeInstance & { sourceLine?: number }).sourceLine ?? 0;
				warnings.push({
					line: nLine,
					message: `Node ${node.id} (${node.nodeType}): has outputs (${portNames}) but none are connected to other nodes. Consider connecting it or removing it.`,
				});
			}
		}
	}

	// Step 6: Warn about nodes (and groups) where no input will cause a skip.
	// A port causes a skip if it's required AND its type doesn't include Null.
	// Ports that are optional OR have Null in their type are "permissive" (won't skip).
	// Groups use the same rule: if every interface input is permissive and
	// there's no @require_one_of, the group body will run even on all-null
	// input, which is usually unintended.
	if (warnings) {
		for (const node of origNodes) {
			if (node.nodeType === 'Passthrough') continue;
			if (node.inputs.length === 0) continue;
			const wiredInputs = node.inputs.filter(p => connectedInputs.has(`${node.id}:${p.name}`));
			if (wiredInputs.length === 0) continue;
			const typeIncludesNull = (portType: string) => {
				const parsed = parseWeftType(portType);
				if (!parsed) return false;
				if (parsed.kind === 'primitive' && parsed.value === 'Null') return true;
				if (parsed.kind === 'union') return parsed.types.some((t) => t.kind === 'primitive' && t.value === 'Null');
				return false;
			};
			// A port is "permissive" if it won't cause a skip: optional, or accepts Null
			const allPermissive = wiredInputs.every(p => !p.required || typeIncludesNull(p.portType));
			const hasOneOfRequired = (node.features?.oneOfRequired?.length ?? 0) > 0;
			if (allPermissive && !hasOneOfRequired) {
				const portNames = wiredInputs.map(p => p.name).join(', ');
				const label = node.nodeType === 'Group' ? 'Group' : 'Node';
				const nLine = (node as NodeInstance & { sourceLine?: number }).sourceLine ?? 0;
					warnings.push({ line: nLine, message: `${label} ${node.id}: no connected input will cause a skip if null (${portNames}). This ${label.toLowerCase()} will execute even if all inputs are null. Consider making at least one input required (without Null in type) or using @require_one_of().` });
			}
		}
	}
}

function validateAndBuild(parsed: ParseResult): { project: ProjectDefinition; errors: WeftParseError[]; warnings: WeftWarning[]; opaqueBlocks: OpaqueBlock[]; nodeOrder: string[]; itemOrder: string[]; itemGaps: number[] } {
	const errors: WeftParseError[] = [];
	const warnings: WeftWarning[] = [];
	const opaqueBlocks = [...parsed.opaqueBlocks];
	const nodeMap = new Map<string, ParsedNode>();
	const skippedNodeIds = new Set<string>();
	// Metadata keys that are never ports, shared by both literal-driven and
	// edge-driven synthesis passes.
	const RESERVED_CONFIG_KEYS = new Set(['label', 'parentId', 'expanded', 'description', 'textareaHeights', 'width', 'height', 'fields']);

	// Validate node types exist and coerce config values
	for (const node of parsed.nodes) {
		let nodeError: string | null = null;
		if (!NODE_TYPE_CONFIG[node.nodeType]) {
			nodeError = `Unknown node type: ${node.nodeType}`;
		} else if (nodeMap.has(node.id)) {
			nodeError = `Duplicate node ID: ${node.id}`;
		}
		if (nodeError) {
			errors.push({ line: node.startLine, message: nodeError });
			opaqueBlocks.push({
				startLine: node.startLine,
				endLine: node.endLine,
				text: node.rawLines.join('\n'),
				error: nodeError,
				anchorAfter: null,
			});
			skippedNodeIds.add(node.id);
			continue;
		}
		coerceConfigValues(node, NODE_TYPE_CONFIG[node.nodeType]);
		nodeMap.set(node.id, node);
	}

	const validNodes = parsed.nodes.filter(n => !skippedNodeIds.has(n.id));

	// Post-parse: detect same-scope duplicate group names and mark ALL duplicates as errors
	const skippedGroupIds = new Set<string>();
	const nestedOpaqueByParent = new Map<string, string[]>();
	{
		// Group by scope (parentGroupId) → map of originalName → list of groups
		const scopeMap = new Map<string, Map<string, ParsedGroup[]>>();
		for (const group of parsed.groups) {
			const scopeKey = group.parentGroupId || '__top__';
			if (!scopeMap.has(scopeKey)) scopeMap.set(scopeKey, new Map());
			const nameMap = scopeMap.get(scopeKey)!;
			const name = group.originalName || group.id;
			if (!nameMap.has(name)) nameMap.set(name, []);
			nameMap.get(name)!.push(group);
		}
		for (const [scopeKey, nameMap] of scopeMap) {
			for (const [name, groupList] of nameMap) {
				if (groupList.length > 1) {
					const errMsg = `Duplicate group name '${name}' in same scope`;
					for (const g of groupList) {
						errors.push({ line: g.startLine, message: errMsg });
						if (scopeKey === '__top__') {
							// Push as opaque block and rewrite the itemOrder entry
							const opaqueIdx = opaqueBlocks.length;
							opaqueBlocks.push({
								startLine: g.startLine,
								endLine: g.endLine,
								text: g.rawLines.join('\n'),
								error: errMsg,
								anchorAfter: null,
							});
							// Replace group:ID in itemOrder with opaque:N so the serializer emits it
							const ioKey = `group:${g.id}`;
							const ioIdx = parsed.itemOrder.indexOf(ioKey);
							if (ioIdx !== -1) parsed.itemOrder[ioIdx] = `opaque:${opaqueIdx}`;
						} else {
							// Store nested duplicate text to be emitted inside parent group
							if (!nestedOpaqueByParent.has(scopeKey)) nestedOpaqueByParent.set(scopeKey, []);
							nestedOpaqueByParent.get(scopeKey)!.push(g.rawLines.join('\n'));
							// Also push as opaque block for full-block error highlighting in gutter
							opaqueBlocks.push({
								startLine: g.startLine,
								endLine: g.endLine,
								text: g.rawLines.join('\n'),
								error: errMsg,
								anchorAfter: null,
							});
						}
						skippedGroupIds.add(g.id);
					}
				}
			}
		}
	}
	// Filter out duplicate groups and their children
	const validGroups = parsed.groups.filter(g => !skippedGroupIds.has(g.id));

	// Shared: build a NodeInstance from a ParsedNode (ports, in:/out: blocks, formSchema)
	function buildNodeInstance(node: ParsedNode, parentIdOverride?: string): NodeInstance | null {
		const template = NODE_TYPE_CONFIG[node.nodeType];
		if (!template) return null;
		const features: NodeFeatures = template.features ? { ...template.features } : {};
		// Merge weft-declared @require_one_of groups with catalog's
		if (node.oneOfRequired?.length) {
			if (!features.oneOfRequired) features.oneOfRequired = [];
			for (const group of node.oneOfRequired) {
				if (!features.oneOfRequired.some((g: string[]) => JSON.stringify(g) === JSON.stringify(group))) {
					features.oneOfRequired.push(group);
				}
			}
		}

		let inputs: PortDefinition[] = template.defaultInputs.map(p => ({ ...p }));
		let outputs: PortDefinition[] = template.defaultOutputs.map(p => ({ ...p }));

		if (features.hasFormSchema && template.formFieldSpecs) {
			const specMap = buildSpecMap(template.formFieldSpecs);
			const fields = (node.config.fields as FormFieldDef[] | undefined) ?? [];
			for (const f of fields) {
				if (!f.render && specMap[f.fieldType]) {
					f.render = specMap[f.fieldType].render;
				}
			}
			const catalogInputNames = new Set(inputs.map(p => p.name));
			const catalogOutputNames = new Set(outputs.map(p => p.name));
			for (const p of deriveInputsFromFields(fields, specMap)) {
				if (catalogInputNames.has(p.name)) {
					errors.push({ line: node.startLine, message: `Node ${node.id}: form field key "${p.name}" conflicts with an existing input port. Use a different key.` });
				} else {
					inputs.push(p);
				}
			}
			for (const p of deriveOutputsFromFields(fields, specMap)) {
				if (catalogOutputNames.has(p.name)) {
					errors.push({ line: node.startLine, message: `Node ${node.id}: form field key "${p.name}" conflicts with an existing output port. Use a different key.` });
				} else {
					outputs.push(p);
				}
			}
		}

		// Merge weft-declared ports (from in:/out: blocks) with catalog ports
		if (node.inPorts && node.inPorts.length > 0) {
			const canAdd = features.canAddInputPorts ?? false;
			for (const wp of node.inPorts) {
				const existing = inputs.find(p => p.name === wp.name);
				if (existing) {
					// Weft declaration overrides required/optional in either direction
					existing.required = wp.required;
					// Type override: MustOverride = weft provides type.
					// Non-MustOverride = weft can narrow (compatible subset) or re-state.
					// Narrowed type replaces catalog type for downstream validation.
					if (wp.portType && wp.portType !== 'MustOverride') {
						if (existing.portType === 'MustOverride') {
							existing.portType = wp.portType;
						} else if (isWeftTypeCompatible(wp.portType, existing.portType)) {
							existing.portType = wp.portType;
						} else {
							errors.push({ line: node.startLine, message: `Node ${node.id}: input port "${wp.name}" has catalog type ${existing.portType} but Weft declares incompatible type ${wp.portType}` });
						}
					}
					if (wp.laneMode && existing.laneMode && wp.laneMode !== existing.laneMode) {
						errors.push({ line: node.startLine, message: `Node ${node.id}: input port "${wp.name}" declares ${wp.laneMode} but catalog says ${existing.laneMode}. Using catalog.` });
					}
				} else if (canAdd) {
					const portType = wp.portType || 'MustOverride';
					if (portType === 'MustOverride') {
						errors.push({ line: node.startLine, message: `Node ${node.id}: new input port "${wp.name}" requires a type declaration (e.g. ${wp.name}: String)` });
					}
					inputs.push({ name: wp.name, portType, required: wp.required, ...(wp.laneMode ? { laneMode: wp.laneMode } : {}) });
				} else {
					errors.push({ line: node.startLine, message: `Node ${node.id} (${node.nodeType}): cannot add input port "${wp.name}", node does not support custom input ports` });
				}
			}
		}

		if (node.outPorts && node.outPorts.length > 0) {
			const canAdd = features.canAddOutputPorts ?? false;
			for (const wp of node.outPorts) {
				const existing = outputs.find(p => p.name === wp.name);
				if (existing) {
					// Type override: MustOverride = weft provides type.
					// Non-MustOverride = weft can narrow (compatible subset) or re-state.
					if (wp.portType && wp.portType !== 'MustOverride') {
						if (existing.portType === 'MustOverride') {
							existing.portType = wp.portType;
						} else if (isWeftTypeCompatible(wp.portType, existing.portType)) {
							existing.portType = wp.portType;
						} else {
							errors.push({ line: node.startLine, message: `Node ${node.id}: output port "${wp.name}" has catalog type ${existing.portType} but Weft declares incompatible type ${wp.portType}` });
						}
					}
					if (wp.laneMode && existing.laneMode && wp.laneMode !== existing.laneMode) {
						errors.push({ line: node.startLine, message: `Node ${node.id}: output port "${wp.name}" declares ${wp.laneMode} but catalog says ${existing.laneMode}. Using catalog.` });
					}
				} else if (canAdd) {
					const portType = wp.portType || 'MustOverride';
					if (portType === 'MustOverride') {
						errors.push({ line: node.startLine, message: `Node ${node.id}: new output port "${wp.name}" requires a type declaration (e.g. ${wp.name}: String)` });
					}
					outputs.push({ name: wp.name, portType, required: false, ...(wp.laneMode ? { laneMode: wp.laneMode } : {}) });
				} else {
					errors.push({ line: node.startLine, message: `Node ${node.id} (${node.nodeType}): cannot add output port "${wp.name}", node does not support custom output ports` });
				}
			}
		}

		// Literal-driven dynamic input port synthesis. For each config key
		// that isn't already a declared input port, a catalog config field,
		// or a reserved metadata key, infer a port type from the value and
		// synthesize an input port. Gated on canAddInputPorts. Mirrors the
		// backend synthesize_literal_driven_ports pass. Edges to undeclared
		// ports are caught separately by validateEdgePorts.
		const canAddInputs = (features.canAddInputPorts ?? false) || (features.hasFormSchema ?? false);
		const declaredInputNames = new Set(inputs.map(p => p.name));
		const catalogFieldKeys = new Set((template.fields ?? []).map(f => f.key));
		const outputNames = new Set(outputs.map(p => p.name));
		for (const [k, v] of Object.entries(node.config)) {
			if (RESERVED_CONFIG_KEYS.has(k)) continue;
			if (declaredInputNames.has(k)) continue;
			if (catalogFieldKeys.has(k)) continue;
			if (v === null) {
				// Weft accepts bare `null` as a JSON null literal, but null
				// carries no type information for port synthesis and nothing
				// can consume it at runtime. Skip silently-ish: emit a
				// warning so the user sees feedback in the error panel
				// without blocking compilation.
				warnings.push({
					line: node.startLine,
					message: `Node ${node.id} (${node.nodeType}): literal '${k}: null' is ignored. Null carries no value. Use '${k}: String?' in the signature if you want a nullable port.`,
				});
				continue;
			}
			if (v === undefined) continue;

			// Rule #3: literal assignment to an output port is an error.
			if (outputNames.has(k)) {
				errors.push({
					line: node.startLine,
					message: `Node ${node.id} (${node.nodeType}): cannot assign a literal to output port '${k}'. Output ports are produced by the node, not set by the user.`,
				});
				continue;
			}

			// Rule #4: undeclared key.
			if (!canAddInputs) {
				errors.push({
					line: node.startLine,
					message: `Node ${node.id} (${node.nodeType}): cannot add custom input port '${k}' because this node type has a fixed port signature. Remove the assignment or use a node type that supports custom input ports.`,
				});
				continue;
			}

			const inferredType = weftTypeToString(inferTypeFromValue(v));
			inputs.push({
				name: k,
				portType: inferredType,
				required: false,
			});
		}

		const position = { x: 0, y: 0 };

		const nodeConfig = { ...node.config };
		const pid = parentIdOverride ?? node.parentId;
		if (pid) nodeConfig.parentId = pid;

		return {
			id: node.id,
			nodeType: node.nodeType,
			label: node.label,
			config: nodeConfig,
			position,
			parentId: pid,
			inputs,
			outputs,
			features,
			scope: buildScopeChain(pid),
			sourceLine: node.startLine,
			// Non-interface field: carried through for error reporting by
			// validateRequiredPorts / validateConfigFilledPorts so they can
			// point at the node's position in source instead of line 0.
			startLine: node.startLine,
		} as NodeInstance;
	}

	// Build node instances with proper ports
	const nodeInstances: NodeInstance[] = [];
	for (const node of validNodes) {
		const inst = buildNodeInstance(node);
		if (inst) nodeInstances.push(inst);
	}

	// Synthesize Group nodes and their children from parsed groups
	for (const group of validGroups) {
		// Create the Group node with interface ports
		const groupInputs: PortDefinition[] = group.inPorts.map(p => ({
			name: p.name,
			portType: p.portType,
			required: p.required,
			...(p.laneMode ? { laneMode: p.laneMode } : {}),
		}));
		const groupOutputs: PortDefinition[] = group.outPorts.map(p => ({
			name: p.name,
			portType: p.portType,
			required: false,
			...(p.laneMode ? { laneMode: p.laneMode } : {}),
		}));

		const groupPos = { x: 0, y: 0 };
		const groupConfig: Record<string, unknown> = {};
		groupConfig.expanded = true;
		if (group.parentGroupId) groupConfig.parentId = group.parentGroupId;
		if (group.description) groupConfig.description = group.description;
		const opaqueChildren = nestedOpaqueByParent.get(group.id);
		if (opaqueChildren) groupConfig._opaqueChildren = opaqueChildren;

		nodeInstances.push({
			id: group.id,
			nodeType: 'Group',
			label: group.originalName || group.id,
			config: groupConfig,
			position: groupPos,
			parentId: group.parentGroupId,
			inputs: groupInputs,
			outputs: groupOutputs,
			features: { oneOfRequired: group.oneOfRequired },
			scope: buildScopeChain(group.parentGroupId),
			sourceLine: group.startLine,
		});

		// Add nested nodes as children (parentId = group.id)
		for (const nested of group.nodes) {
			if (!NODE_TYPE_CONFIG[nested.nodeType]) {
				errors.push({ line: nested.startLine, message: `Unknown node type in group: ${nested.nodeType}` });
				skippedNodeIds.add(nested.id);
				continue;
			}
			coerceConfigValues(nested, NODE_TYPE_CONFIG[nested.nodeType]);
			const inst = buildNodeInstance(nested, group.id);
			if (inst) nodeInstances.push(inst);
		}

		// Add internal connections from the group
		for (const conn of group.connections) {
			parsed.connections.push(conn);
		}
	}

	// Edge-driven port synthesis. An edge targeting an undeclared port on
	// a canAddInputPorts node synthesizes the port with `required: true`
	// and a fresh TypeVar that unifies with the edge source's type
	// during type resolution. Mirror of the backend
	// synthesize_edge_driven_ports pass. Skipped for reserved metadata
	// keys, catalog fields, catalog outputs, and frozen-port nodes.
	{
		const alreadySynthesized = new Set<string>();
		for (const conn of parsed.connections) {
			if (conn.targetIsSelf) continue;
			const targetPort = conn.targetPort;
			if (RESERVED_CONFIG_KEYS.has(targetPort)) continue;
			const targetNode = nodeInstances.find(n => n.id === conn.targetId);
			if (!targetNode) continue;
			if (targetNode.inputs.some(p => p.name === targetPort)) continue;
			const template = NODE_TYPE_CONFIG[targetNode.nodeType];
			if (!template) continue;
			if ((template.defaultOutputs ?? []).some(p => p.name === targetPort)) continue;
			if ((template.fields ?? []).some(f => f.key === targetPort)) continue;
			const canAdd = (template.features?.canAddInputPorts ?? false) || (template.features?.hasFormSchema ?? false);
			if (!canAdd) continue;
			const dedupKey = `${targetNode.id}.${targetPort}`;
			if (alreadySynthesized.has(dedupKey)) continue;
			alreadySynthesized.add(dedupKey);
			const sanitized = `${targetNode.id}_${targetPort}`.replace(/[^A-Za-z0-9_]/g, '_');
			targetNode.inputs.push({
				name: targetPort,
				portType: `T__${sanitized}`,
				required: true,
			});
		}
	}

	// Build edges and validate connections
	const edges: Edge[] = [];
	const connectedInputs = new Set<string>();
	// Build scope membership: for each scope, which node IDs are direct children
	const scopeMembers = new Map<string, Set<string>>();
	for (const inst of nodeInstances) {
		const scope = inst.parentId || '__root__';
		if (!scopeMembers.has(scope)) scopeMembers.set(scope, new Set());
		scopeMembers.get(scope)!.add(inst.id);
	}
	let lastConnAnchor: string | null = nodeInstances.length > 0 ? `node:${nodeInstances[nodeInstances.length - 1].id}` : null;
	for (const conn of parsed.connections) {
		const sourceNode = nodeInstances.find(n => n.id === conn.sourceId);
		const targetNode = nodeInstances.find(n => n.id === conn.targetId);

		let connError: string | null = null;
		if (!sourceNode) {
			connError = `Connection references unknown source node: ${conn.sourceId}`;
		} else if (!targetNode) {
			connError = `Connection references unknown target node: ${conn.targetId}`;
		} else if (conn.scopeId) {
			// Scope validation: dotted references must be children of this scope.
			// Bare names (sourceIsSelf/targetIsSelf) refer to the scope's own interface, skip check.
			const members = scopeMembers.get(conn.scopeId) ?? new Set();
			// A source or target is acceptable if it's a direct member of this
			// scope, or if it's an ancestor-scope node (root, parent group, etc).
			// Inline expression bodies inside groups can legitimately reference
			// nodes from enclosing scopes via port-wiring.
			const isReachable = (id: string): boolean => {
				if (members.has(id)) return true;
				const n = nodeInstances.find(nn => nn.id === id);
				if (!n) return false;
				const np = n.parentId || '__root__';
				let cur: string | undefined = conn.scopeId;
				while (cur && cur !== '__root__') {
					const parentGroup = nodeInstances.find(nn => nn.id === cur) as NodeInstance | undefined;
					cur = parentGroup?.parentId || '__root__';
					if (np === cur) return true;
				}
				return np === '__root__';
			};
			if (!conn.sourceIsSelf && !isReachable(conn.sourceId)) {
				connError = `Node '${conn.sourceId}' is not in this scope`;
			} else if (!conn.targetIsSelf && !isReachable(conn.targetId)) {
				connError = `Node '${conn.targetId}' is not in this scope`;
			}
		}
		if (!connError && sourceNode && targetNode) {
			// For groups, a bare name (self-reference) on the left side of -> is an input interface port forwarding inward
			// Dotted references from outside can only use output ports
			// _raw is an implicit output port on all non-Group nodes (added at runtime by the executor)
			const sourcePort = sourceNode.outputs.find(p => p.name === conn.sourcePort)
				|| (conn.sourcePort === '_raw' && sourceNode.nodeType !== 'Group' ? { name: '_raw', portType: 'T', required: false } : null)
				|| (conn.sourceIsSelf && sourceNode.nodeType === 'Group' ? sourceNode.inputs.find(p => p.name === conn.sourcePort) : null);
			if (!sourcePort) {
				const candidateOutputs = sourceNode.outputs.map(p => p.name);
					const hint = suggestPortName(conn.sourcePort, candidateOutputs);
					connError = `${sourceNode.nodeType === 'Group' ? 'Group' : 'Node'} ${conn.sourceId} has no output port: ${conn.sourcePort}${hint}`;
			} else {
				// For groups, a bare name (self-reference) on the right side of -> is an output interface port receiving from inside
				// Dotted references from outside can only use input ports
				const targetPort = targetNode.inputs.find(p => p.name === conn.targetPort)
					|| (conn.targetIsSelf && targetNode.nodeType === 'Group' ? targetNode.outputs.find(p => p.name === conn.targetPort) : null);
				if (!targetPort) {
					const candidateInputs = targetNode.inputs.map(p => p.name);
					const hint = suggestPortName(conn.targetPort, candidateInputs);
					const available = candidateInputs.length > 0 ? ` (available: ${candidateInputs.join(', ')})` : '';
					connError = `${targetNode.nodeType === 'Group' ? 'Group' : 'Node'} ${conn.targetId} has no input port: ${conn.targetPort}${hint || available}`;
				} else {
					const inputKey = `${conn.targetId}.${conn.targetPort}${conn.targetIsSelf ? '__inner' : ''}`;
					if (connectedInputs.has(inputKey)) {
						connError = `Input port ${conn.targetId}.${conn.targetPort} already has a connection`;
					} else {
						connectedInputs.add(inputKey);
					}
				}
			}
		}

		const connId = `${conn.sourceId}.${conn.sourcePort}->${conn.targetId}.${conn.targetPort}`;
		if (connError) {
			errors.push({ line: conn.line, message: connError });
			opaqueBlocks.push({
				startLine: conn.line,
				endLine: conn.line,
				text: conn.rawText,
				error: connError,
				anchorAfter: lastConnAnchor,
			});
			continue;
		}

		// For group interface self-references, append __inner so buildEdges connects to the correct handle
		const sourceHandle = conn.sourceIsSelf ? `${conn.sourcePort}__inner` : conn.sourcePort;
		const targetHandle = conn.targetIsSelf ? `${conn.targetPort}__inner` : conn.targetPort;
		edges.push({
			id: `e-${conn.sourceId}-${conn.sourcePort}-${conn.targetId}-${conn.targetPort}`,
			source: conn.sourceId,
			target: conn.targetId,
			sourceHandle,
			targetHandle,
			_line: conn.line, // TODO: use a Map<edgeId, lineNumber> instead of hacking _line onto Edge with `as any`
		} as any);
		lastConnAnchor = `conn:${connId}`;
	}

	// Type validation: resolve TypeVars, check edge compatibility, check MustOverride
	resolveAndValidateTypes(nodeInstances, edges, errors, warnings);

	// Config-fills-port type check: for each configurable input port that has a
	// same-named config value and no incoming edge, infer the value's type and
	// check it against the port type. Catches "user wrote template: 42 but port
	// expects String" at parse time. Mirrors validate_config_filled_ports in
	// weft-nodes/src/enrich.rs.
	validateConfigFilledPorts(nodeInstances, edges, errors);

	// System-wide required-port check: every required input on every non-
	// passthrough/non-group node must be either wired by an edge, or filled
	// from a same-named config value on a configurable port. Mirrors
	// validate_required_ports in weft-nodes/src/enrich.rs.
	validateRequiredPorts(nodeInstances, edges, errors);

	// Validate infrastructure subgraph (non-fatal, just warnings)
	const infraResult = extractInfraSubgraph(nodeInstances, edges);
	for (const err of infraResult.errors) {
		errors.push({ line: 0, message: err });
	}

	const now = new Date().toISOString();
	const project: ProjectDefinition = {
		id: crypto.randomUUID(),
		name: parsed.name,
		description: parsed.description || null,
		nodes: nodeInstances,
		edges,
		createdAt: now,
		updatedAt: now,
	};

	const nodeOrder = parsed.nodeOrder.filter(id => !skippedNodeIds.has(id));
	// Filter itemOrder: remove skipped nodes, keep valid connections and their conn: prefixed IDs
	const validEdgeIds = new Set(edges.map(e => `${e.source}.${e.sourceHandle}->${e.target}.${e.targetHandle}`));
	const itemOrder: string[] = [];
	const itemGaps: number[] = [];
	for (let i = 0; i < parsed.itemOrder.length; i++) {
		const item = parsed.itemOrder[i];
		const gap = parsed.itemGaps[i] ?? 0;
		let keep = true;
		if (item.startsWith('node:')) {
			keep = !skippedNodeIds.has(item.slice(5));
		} else if (item.startsWith('group:')) {
			keep = !skippedGroupIds.has(item.slice(6));
		} else if (item.startsWith('conn:')) {
			keep = validEdgeIds.has(item.slice(5));
		}
		if (keep) {
			itemOrder.push(item);
			itemGaps.push(gap);
		}
	}
	// Run per-node structural validation (required connections, config type checks, etc.)
	// Collect startLine for every parsed node, including nodes nested inside
	// groups. Group-inner nodes in project.nodes use scoped IDs (e.g.
	// "my_group.enrich"), so we build the same scoped IDs here.
	const nodeStartLines = new Map<string, number>();
	for (const node of parsed.nodes) {
		nodeStartLines.set(node.id, node.startLine);
	}
	for (const group of parsed.groups) {
		for (const nested of group.nodes) {
			// project.nodes builds inner node IDs as `${group.id}.${nested.id}`
			// (see buildNodeInstance + the group synthesis loop).
			nodeStartLines.set(`${group.id}.${nested.id}`, nested.startLine);
			// Also register the bare id for any validation path that uses it.
			if (!nodeStartLines.has(nested.id)) {
				nodeStartLines.set(nested.id, nested.startLine);
			}
		}
	}
	for (const node of project.nodes) {
		const nodeErrors = validateNode(node, project.nodes, project.edges);
		const structuralErrors = nodeErrors.filter(e => e.level === 'structural');
		for (const err of structuralErrors) {
			errors.push({
				line: nodeStartLines.get(node.id) ?? 0,
				message: `${node.id}: ${err.message}`,
			});
		}
	}

	return { project, errors, warnings, opaqueBlocks, nodeOrder, itemOrder, itemGaps };
}

function gridLayout(
	nodes: ParsedNode[],
	connections: ParsedConnection[],
): Map<string, { x: number; y: number }> {
	const positions = new Map<string, { x: number; y: number }>();
	if (nodes.length === 0) return positions;

	// Simple topological layering: assign each node to the longest path from a root
	const children = new Map<string, string[]>();
	const parents = new Map<string, string[]>();
	for (const node of nodes) {
		children.set(node.id, []);
		parents.set(node.id, []);
	}
	for (const conn of connections) {
		children.get(conn.sourceId)?.push(conn.targetId);
		parents.get(conn.targetId)?.push(conn.sourceId);
	}

	const layer = new Map<string, number>();
	function assignLayer(id: string, visited: Set<string>): number {
		if (layer.has(id)) return layer.get(id)!;
		if (visited.has(id)) return 0;
		visited.add(id);
		const parentLayers = (parents.get(id) || []).map(p => assignLayer(p, visited));
		const l = parentLayers.length > 0 ? Math.max(...parentLayers) + 1 : 0;
		layer.set(id, l);
		return l;
	}
	const visited = new Set<string>();
	for (const node of nodes) assignLayer(node.id, visited);

	// Group by layer
	const layers = new Map<number, string[]>();
	for (const [id, l] of layer) {
		if (!layers.has(l)) layers.set(l, []);
		layers.get(l)!.push(id);
	}

	// Place in a grid, separating top-level nodes from grouped nodes
	// For grouped nodes, x and y are relative to the parent group
	const groupCounts = new Map<string, number>();
	
	for (const [l, ids] of layers) {
		for (let i = 0; i < ids.length; i++) {
			const id = ids[i];
			const node = nodes.find(n => n.id === id);
			
			if (node?.parentId) {
				// It's a grouped node. Position relative to parent.
				const count = groupCounts.get(node.parentId) || 0;
				groupCounts.set(node.parentId, count + 1);
				
				// Stack vertically inside the group, with some padding
				positions.set(id, { x: 20, y: 40 + count * 150 });
			} else {
				// Top level node
				positions.set(id, { x: 100 + l * 400, y: 100 + i * 250 });
			}
		}
	}

	return positions;
}

export interface AutoOrganizeResult {
	positions: Map<string, { x: number; y: number }>;
	groupSizes: Map<string, { width: number; height: number }>;
}

export async function autoOrganize(
	projectNodes: NodeInstance[],
	projectEdges: Edge[],
	nodeSizes?: Map<string, { width: number; height: number }>,
	portPositions?: Map<string, Map<string, number>>,
): Promise<AutoOrganizeResult> {
	const positions = new Map<string, { x: number; y: number }>();
	const groupSizes = new Map<string, { width: number; height: number }>();
	if (projectNodes.length === 0) return { positions, groupSizes };

	const elk = new ELK();

	const NODE_BASE_HEIGHT = 90;
	const PORT_ROW_HEIGHT = 22;
	const NODE_WIDTH = 280;
	const GROUP_PADDING = 40;

	// Build parent->children map for all nodes (including nested groups)
	const childrenOf = new Map<string, NodeInstance[]>();
	for (const node of projectNodes) {
		if (node.parentId) {
			const arr = childrenOf.get(node.parentId) ?? [];
			arr.push(node);
			childrenOf.set(node.parentId, arr);
		}
	}

	// Only expanded groups are ELK containers; collapsed groups are leaf nodes
	const groupIds = new Set(
		projectNodes
			.filter(n => n.nodeType === 'Group' && (n.config as Record<string, unknown>)?.expanded !== false)
			.map(n => n.id)
	);
	const collapsedGroupIds = new Set(
		projectNodes
			.filter(n => n.nodeType === 'Group' && (n.config as Record<string, unknown>)?.expanded === false)
			.map(n => n.id)
	);

	// Pre-compute annotation sizes from content so ELK reserves space for them
	const ANNOTATION_CHAR_WIDTH = 7.5;
	const ANNOTATION_LINE_HEIGHT = 20;
	const ANNOTATION_PADDING = 24;
	const ANNOTATION_MIN_W = 200;
	const ANNOTATION_MAX_W = 420;
	const ANNOTATION_MIN_H = 80;
	const ANNOTATION_MAX_H = 320;
	const ANNOTATION_TARGET_W = 280;

	const annotationIds = new Set<string>();
	for (const node of projectNodes) {
		if (node.nodeType !== 'Annotation') continue;
		annotationIds.add(node.id);
		const content = (node.config?.content as string) || '';
		const existingW = nodeSizes?.get(node.id)?.width;
		const existingH = nodeSizes?.get(node.id)?.height;
		if (existingW && existingH) {
			// Already measured from DOM, use actual size
			groupSizes.set(node.id, { width: existingW, height: existingH });
		} else if (content) {
			const charsPerLine = Math.floor((ANNOTATION_TARGET_W - ANNOTATION_PADDING * 2) / ANNOTATION_CHAR_WIDTH);
			let totalLines = 0;
			for (const line of content.split('\n')) {
				totalLines += Math.max(1, Math.ceil((line.length || 1) / charsPerLine));
			}
			const w = Math.min(ANNOTATION_MAX_W, Math.max(ANNOTATION_MIN_W, ANNOTATION_TARGET_W));
			const h = Math.min(ANNOTATION_MAX_H, Math.max(ANNOTATION_MIN_H, totalLines * ANNOTATION_LINE_HEIGHT + ANNOTATION_PADDING * 2));
			groupSizes.set(node.id, { width: w, height: h });
		} else {
			groupSizes.set(node.id, { width: ANNOTATION_TARGET_W, height: ANNOTATION_MIN_H });
		}
	}

	const topLevelNodes = projectNodes.filter(n => !n.parentId);

	// Source-code rank of every node, from the `sourceLine` the parser
	// attaches to each NodeInstance. We can't rely on the order of the
	// `projectNodes` array itself because SvelteFlow's buildNodes sorts
	// groups before non-groups (xyflow parent-first requirement), which
	// would make every group look like it was written first in the source.
	const sourceOrder = new Map<string, number>();
	for (const n of projectNodes) {
		const line = (n as NodeInstance & { sourceLine?: number }).sourceLine;
		if (typeof line === 'number') sourceOrder.set(n.id, line);
	}
	// Fallback rank for any node missing sourceLine: use array index so we
	// still get a stable, deterministic order.
	for (let i = 0; i < projectNodes.length; i++) {
		if (!sourceOrder.has(projectNodes[i].id)) {
			sourceOrder.set(projectNodes[i].id, 1_000_000 + i);
		}
	}
	const sourceRank = (id: string) => sourceOrder.get(id) ?? Number.MAX_SAFE_INTEGER;

	// Build set of node IDs that are visible in the ELK tree
	// (not hidden inside collapsed groups)
	const elkVisibleNodeIds = new Set<string>();
	function collectVisible(nodes: NodeInstance[]) {
		for (const n of nodes) {
			elkVisibleNodeIds.add(n.id);
			// Only recurse into expanded groups
			if (groupIds.has(n.id)) {
				const children = childrenOf.get(n.id) ?? [];
				collectVisible(children);
			}
		}
	}
	collectVisible(topLevelNodes);

	// With SEPARATE_CHILDREN, edges must be placed at the correct scope level.
	// An edge between two nodes in the same scope goes on that scope's edge list.
	// An edge crossing a group boundary (using __inner handles) goes on the group's edge list.
	const nodeById = new Map(projectNodes.map(n => [n.id, n]));

	// Determine which scope a node belongs to (its parentId, or 'root' for top-level)
	function getScope(nodeId: string): string {
		const node = nodeById.get(nodeId);
		return node?.parentId || 'root';
	}

	// Build edges grouped by scope
	const edgesByScope = new Map<string, any[]>();
	function addEdgeToScope(scope: string, edge: any) {
		if (!edgesByScope.has(scope)) edgesByScope.set(scope, []);
		edgesByScope.get(scope)!.push(edge);
	}

	let edgeIdx = 0;
	for (const e of projectEdges) {
		if (!elkVisibleNodeIds.has(e.source) || !elkVisibleNodeIds.has(e.target)) continue;
		const rawSrc = e.sourceHandle || 'output';
		const rawTgt = e.targetHandle || 'input';
		const srcIsInner = rawSrc.endsWith('__inner');
		const tgtIsInner = rawTgt.endsWith('__inner');
		const srcHandle = srcIsInner ? rawSrc.slice(0, -7) : rawSrc;
		const tgtHandle = tgtIsInner ? rawTgt.slice(0, -7) : rawTgt;
		const srcDir = srcIsInner ? 'in' : 'out';
		const tgtDir = tgtIsInner ? 'out' : 'in';

		const elkEdge = {
			id: `e${edgeIdx++}`,
			sources: [`${e.source}__${srcDir}__${srcHandle}`],
			targets: [`${e.target}__${tgtDir}__${tgtHandle}`],
		};

		// Determine scope: if both nodes are in the same scope, edge goes there.
		// If one is a group and the handle is __inner, it's an internal edge of that group.
		if (srcIsInner) {
			// Source is a group, edge goes inside that group's scope
			addEdgeToScope(e.source, elkEdge);
		} else if (tgtIsInner) {
			// Target is a group, edge goes inside that group's scope
			addEdgeToScope(e.target, elkEdge);
		} else {
			// Normal edge: goes to the common parent scope
			const srcScope = getScope(e.source);
			const tgtScope = getScope(e.target);
			// If same scope, add there. Otherwise add to root (cross-scope edges).
			addEdgeToScope(srcScope === tgtScope ? srcScope : 'root', elkEdge);
		}
	}

	// For each node, record the lowest port index it connects to on any target,
	// AND the lowest source port index it connects from.
	// Used to pre-sort sibling nodes to reduce crossings.
	const nodeTargetPortOrder = new Map<string, number>();
	const nodeSourcePortOrder = new Map<string, number>();
	for (const edge of projectEdges) {
		const targetNode = projectNodes.find(n => n.id === edge.target);
		if (targetNode) {
			const tgtHandle = (edge.targetHandle || '').replace(/__inner$/, '');
			const portIndex = (targetNode.inputs || []).findIndex(p => p.name === tgtHandle);
			if (portIndex !== -1) {
				const existing = nodeTargetPortOrder.get(edge.source);
				if (existing === undefined || portIndex < existing) {
					nodeTargetPortOrder.set(edge.source, portIndex);
				}
			}
		}
		// Also track which source port feeds into each target node
		const sourceNode = projectNodes.find(n => n.id === edge.source);
		if (sourceNode) {
			const srcHandle = (edge.sourceHandle || '').replace(/__inner$/, '');
			const portIndex = (sourceNode.outputs || []).findIndex(p => p.name === srcHandle);
			if (portIndex !== -1) {
				const existing = nodeSourcePortOrder.get(edge.target);
				if (existing === undefined || portIndex < existing) {
					nodeSourcePortOrder.set(edge.target, portIndex);
				}
			}
		}
	}

	function sortByTargetPortOrder(nodes: NodeInstance[]): NodeInstance[] {
		return [...nodes].sort((a, b) => {
			// Primary: sort by which target port they feed into (lower port index = higher in layout)
			const tgtA = nodeTargetPortOrder.get(a.id) ?? Infinity;
			const tgtB = nodeTargetPortOrder.get(b.id) ?? Infinity;
			if (tgtA !== tgtB) return tgtA - tgtB;
			// Secondary: sort by which source port feeds them (lower port index = higher in layout)
			const srcA = nodeSourcePortOrder.get(a.id) ?? Infinity;
			const srcB = nodeSourcePortOrder.get(b.id) ?? Infinity;
			return srcA - srcB;
		});
	}

	const GROUP_TOP_PADDING = 80;   // header + port labels
	const GROUP_SIDE_PADDING = 60;  // port labels on sides
	const GROUP_BOTTOM_PADDING = 40;
	const COLLAPSED_GROUP_WIDTH = 200;
	const COLLAPSED_GROUP_HEIGHT = 80;

	// Port Y position constants (must match CSS in GroupNode.svelte and ProjectNode.svelte)
	// Expanded group: ports start at top:40px + 4px padding, each ~30px tall with 6px gap
	const GROUP_PORT_START_Y = 44;  // top(40) + padding(4)
	const GROUP_PORT_HEIGHT = 30;   // label row + dots
	const GROUP_PORT_GAP = 6;
	// Regular/collapsed node: header ~50px, then ports ~25px each with ~1px gap
	const NODE_PORT_START_Y = 58;   // accent(2) + header(32) + content-padding(16) + label-area(8)
	const NODE_PORT_HEIGHT = 25;    // PORT_ROW_HEIGHT from ProjectNode.svelte
	const NODE_PORT_GAP = 4;       // space-y-1

	/** Compute port Y position for regular/collapsed nodes */
	function nodePortY(portIndex: number): number {
		return NODE_PORT_START_Y + portIndex * (NODE_PORT_HEIGHT + NODE_PORT_GAP) + NODE_PORT_HEIGHT / 2;
	}

	/** Compute port Y position for expanded groups (side ports) */
	function groupPortY(portIndex: number): number {
		return GROUP_PORT_START_Y + portIndex * (GROUP_PORT_HEIGHT + GROUP_PORT_GAP) + GROUP_PORT_HEIGHT / 2;
	}

	/** Get the actual measured port Y, falling back to computed position if DOM isn't available (e.g. during streaming). */
	function getPortY(nodeId: string, handleId: string, isGroup: boolean, portIndex: number): number {
		const measured = portPositions?.get(nodeId)?.get(handleId);
		if (measured !== undefined) return measured;
		// Fallback: compute from constants (used during streaming when DOM isn't rendered)
		return isGroup ? groupPortY(portIndex) : nodePortY(portIndex);
	}

	// --- Shared ELK layout options ---
	// We rely on model order (the order of children in the input array) to
	// pin siblings left-to-right the way the user wrote them in weft source.
	// `considerModelOrder` + `crossingCounterNodeInfluence > 0` makes ELK treat
	// source order as a strong tiebreaker during crossing minimization, and
	// `nodePromotion.strategy` + tighter spacing keep layers compact.
	const elkLayoutOptions: Record<string, string> = {
		'elk.algorithm': 'layered',
		'elk.direction': 'RIGHT',
		'elk.layered.spacing.nodeNodeBetweenLayers': '50',
		'elk.spacing.nodeNode': '25',
		'elk.layered.spacing.edgeNodeBetweenLayers': '15',
		'elk.layered.nodePlacement.strategy': 'NETWORK_SIMPLEX',
		'elk.layered.crossingMinimization.strategy': 'LAYER_SWEEP',
		'elk.layered.crossingMinimization.greedySwitch.type': 'TWO_SIDED',
		'elk.layered.crossingMinimization.thoroughness': '100',
		'elk.layered.considerModelOrder.strategy': 'NODES_AND_EDGES',
		'elk.layered.considerModelOrder.crossingCounterNodeInfluence': '0.5',
		'elk.layered.considerModelOrder.crossingCounterPortInfluence': '0.5',
		'elk.layered.crossingMinimization.forceNodeModelOrder': 'true',
		'elk.layered.nodePromotion.strategy': 'DUMMYNODE_PERCENTAGE',
		'elk.separateConnectedComponents': 'true',
	};
	const baseOptions = elkLayoutOptions;

	// --- Helper: find connected components among a set of node IDs ---
	function findConnectedComponents(nodeIds: Set<string>, scopeId: string): string[][] {
		const adj = new Map<string, Set<string>>();
		for (const id of nodeIds) adj.set(id, new Set());

		const resolveToScope = (id: string): string | null => {
			if (nodeIds.has(id)) return id;
			let current = id;
			let parent = nodeById.get(current)?.parentId;
			while (parent && !nodeIds.has(current) && nodeById.has(parent)) {
				current = parent;
				parent = nodeById.get(current)?.parentId;
			}
			return nodeIds.has(current) ? current : null;
		};

		const portPeers = new Map<string, Set<string>>();
		for (const e of projectEdges) {
			const src = resolveToScope(e.source);
			const tgt = resolveToScope(e.target);
			if (src && tgt && src !== tgt && nodeIds.has(src) && nodeIds.has(tgt)) {
				adj.get(src)!.add(tgt);
				adj.get(tgt)!.add(src);
			} else if (e.source === scopeId && tgt && nodeIds.has(tgt)) {
				const portKey = e.sourceHandle || 'default';
				if (!portPeers.has(portKey)) portPeers.set(portKey, new Set());
				portPeers.get(portKey)!.add(tgt);
			} else if (e.target === scopeId && src && nodeIds.has(src)) {
				const portKey = e.targetHandle || 'default';
				if (!portPeers.has(portKey)) portPeers.set(portKey, new Set());
				portPeers.get(portKey)!.add(src);
			}
		}
		for (const peers of portPeers.values()) {
			const arr = [...peers];
			for (let i = 0; i < arr.length; i++) {
				for (let j = i + 1; j < arr.length; j++) {
					adj.get(arr[i])!.add(arr[j]);
					adj.get(arr[j])!.add(arr[i]);
				}
			}
		}

		// Walk nodes in weft source order so component discovery is deterministic
		// across runs. Each component inherits the rank of its earliest node, so
		// sorting components by "min rank" below gives left-to-right order that
		// matches the user's source.
		const sorted = [...nodeIds].sort((a, b) => sourceRank(a) - sourceRank(b));
		const visited = new Set<string>();
		const comps: string[][] = [];
		for (const id of sorted) {
			if (visited.has(id)) continue;
			const comp: string[] = [];
			const stack = [id];
			while (stack.length > 0) {
				const cur = stack.pop()!;
				if (visited.has(cur)) continue;
				visited.add(cur);
				comp.push(cur);
				for (const nb of (adj.get(cur) ?? [])) {
					if (!visited.has(nb)) stack.push(nb);
				}
			}
			comps.push(comp);
		}
		return comps;
	}

	// --- Helper: arrange disconnected components side by side ---
	function arrangeDisconnectedComponents(
		comps: string[][],
		padding: { top: number; left: number; bottom: number; right: number },
	): { width: number; height: number } | null {
		if (comps.length <= 1) return null;

		const GAP = 80;
		const compBBoxes: { minX: number; maxX: number; minY: number; maxY: number; ids: string[] }[] = [];
		for (const comp of comps) {
			let minX = Infinity, maxX = -Infinity, minY = Infinity, maxY = -Infinity;
			for (const id of comp) {
				const pos = positions.get(id);
				if (!pos) continue;
				const w = groupSizes.get(id)?.width ?? nodeSizes?.get(id)?.width ?? NODE_WIDTH;
				const h = groupSizes.get(id)?.height ?? nodeSizes?.get(id)?.height ?? NODE_BASE_HEIGHT;
				minX = Math.min(minX, pos.x);
				maxX = Math.max(maxX, pos.x + w);
				minY = Math.min(minY, pos.y);
				maxY = Math.max(maxY, pos.y + h);
			}
			compBBoxes.push({ minX, maxX, minY, maxY, ids: comp });
		}
		// Preserve the caller's ordering (connectivity-based for groups,
		// X-position-based for root level)

		let cursor = padding.left;
		for (const comp of compBBoxes) {
			if (comp.minX === Infinity) continue;
			const shiftX = cursor - comp.minX;
			const shiftY = padding.top - comp.minY;
			for (const id of comp.ids) {
				const pos = positions.get(id);
				if (pos) positions.set(id, { x: pos.x + shiftX, y: pos.y + shiftY });
			}
			cursor += (comp.maxX - comp.minX) + GAP;
		}

		// Compute final bounding box
		let totalMaxX = 0, totalMaxY = 0;
		for (const comp of compBBoxes) {
			for (const id of comp.ids) {
				const pos = positions.get(id);
				if (!pos) continue;
				const w = groupSizes.get(id)?.width ?? nodeSizes?.get(id)?.width ?? NODE_WIDTH;
				const h = groupSizes.get(id)?.height ?? nodeSizes?.get(id)?.height ?? NODE_BASE_HEIGHT;
				totalMaxX = Math.max(totalMaxX, pos.x + w);
				totalMaxY = Math.max(totalMaxY, pos.y + h);
			}
		}
		return {
			width: totalMaxX + padding.right,
			height: totalMaxY + padding.bottom,
		};
	}

	// --- Build ELK node for a single scope (flat, no children for groups) ---
	function buildElkLeafNode(node: NodeInstance): any {
		if (collapsedGroupIds.has(node.id)) {
			const override = nodeSizes?.get(node.id);
			const inputs = (node.inputs || []).map(p => p.name);
			const outputs = (node.outputs || []).map(p => p.name);
			const w = override?.width ?? COLLAPSED_GROUP_WIDTH;
			return ({
				id: node.id,
				width: w,
				height: override?.height ?? COLLAPSED_GROUP_HEIGHT,
				ports: [
					...inputs.map((name, i) => ({
						id: `${node.id}__in__${name}`,
						x: 0,
						y: getPortY(node.id, name, false, i),
						width: 1, height: 1,
						properties: { 'port.side': 'WEST', 'port.index': String(i) },
					})),
					...outputs.map((name, i) => ({
						id: `${node.id}__out__${name}`,
						x: w - 1,
						y: getPortY(node.id, name, false, i),
						width: 1, height: 1,
						properties: { 'port.side': 'EAST', 'port.index': String(i) },
					})),
				],
				layoutOptions: { 'elk.portConstraints': 'FIXED_POS' },
			});
		}

		if (groupIds.has(node.id)) {
			// Groups are leaf nodes here, their children are laid out in a separate pass.
			// Use the resolved size from groupSizes (set by bottom-up layout).
			const inputs = (node.inputs || []).map(p => p.name);
			const outputs = (node.outputs || []).map(p => p.name);
			const size = groupSizes.get(node.id) ?? { width: 400, height: 300 };
			return ({
				id: node.id,
				width: size.width,
				height: size.height,
				ports: [
					...inputs.map((name, i) => ({
						id: `${node.id}__in__${name}`,
						x: 0,
						y: getPortY(node.id, name, true, i),
						width: 1, height: 1,
						properties: { 'port.side': 'WEST', 'port.index': String(i) },
					})),
					...outputs.map((name, i) => ({
						id: `${node.id}__out__${name}`,
						x: size.width - 1,
						y: getPortY(node.id, name, true, i),
						width: 1, height: 1,
						properties: { 'port.side': 'EAST', 'port.index': String(i) },
					})),
				],
				layoutOptions: {
					'elk.portConstraints': 'FIXED_POS',
					'elk.nodeSize.constraints': 'MINIMUM_SIZE',
					'elk.nodeSize.minimum': `(${size.width},${size.height})`,
				},
			});
		}

		if (annotationIds.has(node.id)) {
			const size = groupSizes.get(node.id) ?? { width: ANNOTATION_TARGET_W, height: ANNOTATION_MIN_H };
			return ({
				id: node.id,
				width: size.width,
				height: size.height,
				layoutOptions: { 'elk.portConstraints': 'FREE' },
			});
		}

		const inputs = (node.inputs || []).map(p => p.name);
		const outputs = (node.outputs || []).map(p => p.name);
		const portCount = Math.max(inputs.length, outputs.length, 1);
		const override = nodeSizes?.get(node.id);
		const cfg = node.config as Record<string, unknown>;
		const configW = cfg?.width as number | undefined;
		const configH = cfg?.height as number | undefined;
		const width = override?.width ?? configW ?? NODE_WIDTH;
		const height = override?.height ?? configH ?? (NODE_BASE_HEIGHT + portCount * PORT_ROW_HEIGHT);

		return ({
			id: node.id,
			width,
			height,
			ports: [
				...inputs.map((name, i) => ({
					id: `${node.id}__in__${name}`,
					x: 0,
					y: getPortY(node.id, name, false, i),
					width: 1, height: 1,
					properties: { 'port.side': 'WEST', 'port.index': String(i) },
				})),
				...outputs.map((name, i) => ({
					id: `${node.id}__out__${name}`,
					x: width - 1,
					y: getPortY(node.id, name, false, i),
					width: 1, height: 1,
					properties: { 'port.side': 'EAST', 'port.index': String(i) },
				})),
				{
					id: `${node.id}__out___raw`,
					x: width - 1,
					y: getPortY(node.id, '_raw', false, outputs.length),
					width: 1, height: 1,
					properties: { 'port.side': 'EAST', 'port.index': String(outputs.length) },
				},
			],
			layoutOptions: { 'elk.portConstraints': 'FIXED_POS' },
		});
	}

	// --- Run ELK for a single scope and extract positions ---
	// For group scopes: wrap in a parent graph with SEPARATE_CHILDREN so ELK
	// handles the group's own ports natively. For root scope: run directly.
	async function layoutScope(scopeId: string, children: NodeInstance[], padding: string) {
		// Feed children in weft source order so ELK's model-order machinery can
		// use it as a strong tiebreaker, keeping siblings left-to-right.
		const orderedChildren = [...children].sort((a, b) => sourceRank(a.id) - sourceRank(b.id));
		const elkChildren = orderedChildren.map(c => buildElkLeafNode(c));

		// Collect all valid port IDs from children (and group ports if applicable)
		const validPortIds = new Set<string>();
		for (const child of elkChildren) {
			for (const port of (child.ports || [])) {
				validPortIds.add(port.id);
			}
		}

		// Also include group's own ports (for edges from/to group interface)
		if (groupIds.has(scopeId)) {
			const scopeNode = nodeById.get(scopeId);
			if (scopeNode) {
				for (const p of (scopeNode.inputs || [])) validPortIds.add(`${scopeId}__in__${p.name}`);
				for (const p of (scopeNode.outputs || [])) validPortIds.add(`${scopeId}__out__${p.name}`);
			}
		}

		// Filter edges to only those whose source AND target ports exist in this layout
		const allScopeEdges = edgesByScope.get(scopeId) || [];
		const scopeEdges = allScopeEdges.filter((e: any) => {
			const srcId = e.sources?.[0] as string;
			const tgtId = e.targets?.[0] as string;
			return validPortIds.has(srcId) && validPortIds.has(tgtId);
		});

		if (groupIds.has(scopeId)) {
			const scopeNode = nodeById.get(scopeId)!;
			const inputs = (scopeNode.inputs || []).map(p => p.name);
			const outputs = (scopeNode.outputs || []).map(p => p.name);
			// Use a small default size, ELK will grow the group to fit children.
			// Don't use measured DOM size as minimum, it would prevent ELK from shrinking.
			const minW = 400;
			const minH = 300;
			// Port positions on the east side need a reference width.
			// Use a large value; ELK will place the east ports at the final computed width.
			const portRefW = 400;

			// Wrap the group as a child of a dummy root, using SEPARATE_CHILDREN
			const graph = {
				id: `__wrapper_${scopeId}`,
				layoutOptions: {
					'elk.algorithm': 'layered',
					'elk.hierarchyHandling': 'SEPARATE_CHILDREN',
				},
				children: [{
					id: scopeId,
					width: minW,
					height: minH,
					layoutOptions: {
						...baseOptions,
						'elk.padding': padding,
						'elk.portConstraints': 'FIXED_POS',
						'elk.nodeSize.constraints': 'MINIMUM_SIZE',
						'elk.nodeSize.minimum': `(${minW},${minH})`,
					},
					ports: [
						...inputs.map((name, i) => ({
							id: `${scopeId}__in__${name}`,
							x: 0,
							y: getPortY(scopeId, name, true, i),
							width: 1, height: 1,
							properties: { 'port.side': 'WEST', 'port.index': String(i) },
						})),
						...outputs.map((name, i) => ({
							id: `${scopeId}__out__${name}`,
							x: portRefW - 1,
							y: getPortY(scopeId, name, true, i),
							width: 1, height: 1,
							properties: { 'port.side': 'EAST', 'port.index': String(i) },
						})),
					],
					children: elkChildren,
					edges: scopeEdges,
				}],
				edges: [],
			};

			const result = await elk.layout(graph);
			const groupResult = result.children?.[0];
			if (groupResult) {
				// Store the ELK-computed group size
				if (groupResult.width && groupResult.height) {
					groupSizes.set(scopeId, { width: groupResult.width, height: groupResult.height });
				}
				for (const child of (groupResult.children || [])) {
					positions.set(child.id, { x: child.x ?? 0, y: child.y ?? 0 });
					if (groupIds.has(child.id) && child.width && child.height && !groupSizes.has(child.id)) {
						groupSizes.set(child.id, { width: child.width, height: child.height });
					}
				}
			}
			return result;
		}

		// Root scope, run directly
		const graph = {
			id: scopeId,
			layoutOptions: {
				...baseOptions,
				'elk.padding': padding,
			},
			children: elkChildren,
			edges: scopeEdges,
		};

		const result = await elk.layout(graph);
		for (const child of (result.children || [])) {
			positions.set(child.id, { x: child.x ?? 0, y: child.y ?? 0 });
			if (groupIds.has(child.id) && child.width && child.height && !groupSizes.has(child.id)) {
				groupSizes.set(child.id, { width: child.width, height: child.height });
			}
		}
		return result;
	}

	// --- Bottom-up scope resolution ---
	// 1. Compute depth of each group
	function getGroupDepth(groupId: string): number {
		let depth = 0;
		const children = childrenOf.get(groupId) ?? [];
		for (const child of children) {
			if (groupIds.has(child.id)) {
				depth = Math.max(depth, 1 + getGroupDepth(child.id));
			}
		}
		return depth;
	}

	const groupsByDepth = new Map<number, string[]>();
	let maxDepth = 0;
	for (const groupId of groupIds) {
		if (collapsedGroupIds.has(groupId)) continue;
		const depth = getGroupDepth(groupId);
		maxDepth = Math.max(maxDepth, depth);
		if (!groupsByDepth.has(depth)) groupsByDepth.set(depth, []);
		groupsByDepth.get(depth)!.push(groupId);
	}

	try {
		// 2. Layout from deepest groups up to shallowest
		for (let depth = 0; depth <= maxDepth; depth++) {
			const groups = groupsByDepth.get(depth) ?? [];
			for (const groupId of groups) {
				const children = (childrenOf.get(groupId) ?? []).filter(c => elkVisibleNodeIds.has(c.id));
				if (children.length === 0) continue;

				const padding = `[top=${GROUP_TOP_PADDING},left=${GROUP_SIDE_PADDING},bottom=${GROUP_BOTTOM_PADDING},right=${GROUP_SIDE_PADDING}]`;

				// Find disconnected components first
				const childIds = new Set(children.map(c => c.id));
				const comps = findConnectedComponents(childIds, groupId);

				// Lay out each component independently so ELK can't spread
				// disconnected nodes across connected component's layers.
				for (const comp of comps) {
					const compChildren = children.filter(c => comp.includes(c.id));
					await layoutScope(groupId, compChildren, padding);
				}

				// Sort components for arrangement:
				// - Connected to group input ports → leftmost (score 0)
				// - Connected to both → leftmost (score 0)
				// - Not connected to any group port → middle (score 1)
				// - Connected to group output ports only → rightmost (score 2)
				if (comps.length > 1) {
					const sortedComps = comps.map(comp => {
						const compSet = new Set(comp);
						let connectsToInput = false;
						let connectsToOutput = false;
						for (const e of projectEdges) {
							if (e.source === groupId && compSet.has(e.target)) connectsToInput = true;
							if (e.target === groupId && compSet.has(e.source)) connectsToOutput = true;
						}
						const score = connectsToInput ? 0 : connectsToOutput ? 2 : 1;
						const minRank = Math.min(...comp.map(sourceRank));
						return { comp, score, minRank };
					});
					// Score groups components by port role (input-connected first,
					// output-connected last). Within the same score, weft source
					// order wins so siblings stay left-to-right as the user wrote.
					sortedComps.sort((a, b) => a.score - b.score || a.minRank - b.minRank);

					const newSize = arrangeDisconnectedComponents(
						sortedComps.map(c => c.comp),
						{
							top: GROUP_TOP_PADDING,
							left: GROUP_SIDE_PADDING,
							bottom: GROUP_BOTTOM_PADDING,
							right: GROUP_SIDE_PADDING,
						},
					);
					if (newSize) {
						groupSizes.set(groupId, newSize);
					}
				}
			}
		}

		// 3. Layout root scope (top-level nodes, groups now have final sizes)
		const rootPadding = `[top=${GROUP_PADDING},left=${GROUP_PADDING},bottom=${GROUP_PADDING},right=${GROUP_PADDING}]`;
		await layoutScope('root', topLevelNodes, rootPadding);

		// 4. Arrange disconnected components at root level
		const topIds = new Set(topLevelNodes.map(n => n.id));
		const comps = findConnectedComponents(topIds, 'root');
		arrangeDisconnectedComponents(comps, { top: 0, left: 0, bottom: 0, right: 0 });
	} catch (e) {
		console.warn('[autoOrganize] ELK layout failed:', e);
		for (let i = 0; i < projectNodes.length; i++) {
			positions.set(projectNodes[i].id, { x: 100 + (i % 4) * 350, y: 100 + Math.floor(i / 4) * 250 });
		}
	}

	return { positions, groupSizes };
}

// ─── VS Code webview dispatcher bridge ─────────────────────────────
// In the standalone dashboard, `parseWeft` runs the full parser
// locally. In the VS Code extension we delegate parsing to the
// dispatcher (single source of truth for the Rust compiler).
// `parseWeft` becomes a synchronous read of the latest cached
// dispatcher response, plus a side-effect that triggers a fresh
// parse if the source changed. Callers upstream in ProjectEditor
// already tolerate stale-then-updated flows; Svelte's reactivity
// re-runs their effects when `setCachedParseResponse` updates the
// state the cache is read through.

import type { ProjectDefinition as HostProjectShape } from '../../../shared/protocol';
import { translateProject } from '../../host-bridge';

interface CachedParse {
	projects: WeftParseMultiOutput['projects'];
	errors: WeftParseError[];
	// The raw response we mapped; used to detect staleness.
	source: string;
}

let cached: CachedParse = {
	projects: [],
	errors: [],
	source: '',
};

type RemoteParseTrigger = (source: string) => void;
let onRemoteParse: RemoteParseTrigger | null = null;

/** Install the hook that lets the parser shim request a fresh
 *  parse from the host when the current source drifts from the
 *  last cached one. App.svelte wires this at mount. */
export function setRemoteParseTrigger(trigger: RemoteParseTrigger): void {
	onRemoteParse = trigger;
}

/** Called by App.svelte when a `parseResult` message lands. Updates
 *  the cache the shim reads through. */
export function setCachedParseResponse(
	hostProject: HostProjectShape,
	source: string,
	layoutCode: string,
	errors: WeftParseError[],
): void {
	const translated = translateProject(hostProject, source, layoutCode);
	cached = {
		projects: [
			{
				project: translated as unknown as ProjectDefinition,
				errors,
				warnings: [],
				opaqueBlocks: [],
				nodeOrder: [],
				itemOrder: [],
				itemGaps: [],
			},
		],
		errors: [],
		source,
	};
}

export function parseWeft(rawResponse: string): WeftParseMultiOutput {
	// Strip the leading/trailing ```weft fence v1 wraps around the
	// source before calling parseWeft (see ProjectEditorInner's
	// parseWeftCode helper). The cache stores the inner source.
	const inner = stripWeftFences(rawResponse);

	// If the requested source differs from the cache, ask the host
	// for a fresh parse. The cache updates asynchronously; callers
	// re-run on Svelte reactivity when it lands.
	if (inner !== cached.source && onRemoteParse) {
		onRemoteParse(inner);
	}

	return {
		projects: cached.projects,
		errors: cached.errors,
	};
}

function stripWeftFences(raw: string): string {
	const fenced = raw.match(/^```weft\s*\n([\s\S]*?)\n```\s*$/);
	return fenced ? fenced[1] : raw;
}
