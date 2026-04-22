/**
 * Loom DSL parser.
 *
 * Loom describes the runner/deploy page for a Weft project: which node fields
 * are exposed for configuration, which output ports are displayed as results,
 * which infrastructure nodes stream live data, and the surrounding presentation
 * (hero, text blocks, tabs, theme, etc).
 *
 * Quick reference:
 *
 *   theme {
 *     primary:"#7c3aed" accent:"#ec4899" font:"inter" mode:"light"
 *     radius:"lg" layout:"centered"
 *   }
 *
 *   hero title:"Humanize any text" subtitle:"Sounds like you, not a bot"
 *   text "Paste your robotic draft below."
 *
 *   phase "Configure" "Set up the analysis" {
 *     field llm model label:"Model" description:"Which LLM to use"
 *     field input value as:textarea label:"Input"
 *     field secrets apiKey visibility:admin
 *   }
 *
 *   output result data as:markdown label:"Result"
 *   live bridge label:"Connection Status"
 *
 *   columns cols:2 {
 *     card { hero title:"Fast" subtitle:"..." }
 *     card { hero title:"Private" subtitle:"..." }
 *   }
 *
 *   cta label:"Run now" action:"run"
 *   footer note:"Built by @me"
 *
 * Visibility: `visibility:admin|visitor|both` (default both). Sensitive fields
 * (password, api_key) are re-clamped to admin by the renderer regardless of
 * the DSL's declaration. Infra and trigger controls are admin-only and not
 * configurable from the DSL.
 *
 * Bracket-wrapped attributes are also accepted: [label:"My Label"]
 *
 * Quoted strings support \n for newlines: "line one\nline two"
 *
 * Multi-line values use triple backticks (same as Weft):
 *   field llm systemPrompt description:```
 *   First line
 *   Second line
 *   ```
 *
 * Command fences are always FOUR backticks (````loom). Three backticks are
 * string delimiters, never command fences.
 */

import type {
	SetupManifest,
	SetupPhase,
	SetupItem,
	OutputItem,
	LiveItem,
	Block,
	Brick,
	BrickKind,
	RunnerTheme,
	Visibility,
	ItemVariant,
} from '$lib/types';

export interface LoomParseError {
	line: number;
	message: string;
}

export interface LoomParseResult {
	manifest: SetupManifest;
	errors: LoomParseError[];
}

// ── Tokenizer ────────────────────────────────────────────────────────────────

function parseQuotedString(s: string): string {
	if (s.startsWith('"') && s.endsWith('"')) {
		return s.slice(1, -1).replace(/\\"/g, '"').replace(/\\n/g, '\n').replace(/\\\\/g, '\\');
	}
	return s;
}

function tokenizeLine(line: string): string[] {
	const tokens: string[] = [];
	let i = 0;
	while (i < line.length) {
		if (line[i] === ' ' || line[i] === '\t') { i++; continue; }
		if (line[i] === '[') {
			let j = i + 1;
			while (j < line.length && line[j] !== ']') {
				if (line[j] === '"') {
					j++;
					while (j < line.length && line[j] !== '"') {
						if (line[j] === '\\') j++;
						j++;
					}
				}
				j++;
			}
			if (j < line.length) j++;
			tokens.push(line.slice(i, j));
			i = j;
		} else if (line[i] === '"') {
			let j = i + 1;
			while (j < line.length && line[j] !== '"') {
				if (line[j] === '\\') j++;
				j++;
			}
			tokens.push(line.slice(i, j + 1));
			i = j + 1;
		} else {
			let j = i;
			while (j < line.length && line[j] !== ' ' && line[j] !== '\t') {
				if (line[j] === '"') {
					j++;
					while (j < line.length && line[j] !== '"') {
						if (line[j] === '\\') j++;
						j++;
					}
					if (j < line.length) j++;
					break;
				}
				j++;
			}
			tokens.push(line.slice(i, j));
			i = j;
		}
	}
	return tokens;
}

function parseAttributes(tokens: string[]): Record<string, string> {
	const attrs: Record<string, string> = {};
	const expanded: string[] = [];
	for (const token of tokens) {
		if (token.startsWith('[') && token.endsWith(']')) {
			const inner = token.slice(1, -1);
			expanded.push(...tokenizeLine(inner));
		} else {
			expanded.push(token);
		}
	}
	for (const t of expanded) {
		const colonIdx = t.indexOf(':');
		if (colonIdx === -1) continue;
		const key = t.slice(0, colonIdx);
		const rawVal = t.slice(colonIdx + 1);
		attrs[key] = parseQuotedString(rawVal);
	}
	return attrs;
}

function parseVisibility(raw: string | undefined): Visibility | undefined {
	if (!raw) return undefined;
	if (raw === 'admin' || raw === 'visitor' || raw === 'both') return raw;
	return undefined;
}

function parseItemVariant(raw: string | undefined): ItemVariant | undefined {
	if (!raw) return undefined;
	const allowed: ItemVariant[] = [
		'text','textarea','password','email','url',
		'number','slider',
		'toggle','checkbox',
		'radio','select','cards',
		'multiselect','tags','multicards',
		'date','time','datetime','color','file',
		'markdown','code','json','image','gallery','audio',
		'video','download','progress','chart','log',
	];
	return allowed.includes(raw as ItemVariant) ? (raw as ItemVariant) : undefined;
}

/**
 * Parse an options attribute: comma-separated values, or a JSON array.
 * `parseAttributes` has already unquoted the value, so here we just split.
 */
function parseOptions(raw: string | undefined): string[] | undefined {
	if (!raw) return undefined;
	const s = raw.trim();
	if (!s) return undefined;
	if (s.startsWith('[') && s.endsWith(']')) {
		try {
			const parsed = JSON.parse(s);
			if (Array.isArray(parsed)) return parsed.map(v => String(v));
		} catch {
			// fall through
		}
	}
	return s.split(',').map(x => x.trim()).filter(x => x.length > 0);
}

function parseChrome(raw: string | undefined): 'none' | 'subtle' | 'card' | undefined {
	if (raw === 'none' || raw === 'subtle' || raw === 'card') return raw;
	return undefined;
}

/**
 * Pick the sizing attributes (height, minHeight, maxHeight, width, chrome)
 * out of an attrs map. These apply to fields and outputs.
 */
function extractSizingAttrs(attrs: Record<string, string>): {
	height?: string;
	minHeight?: string;
	maxHeight?: string;
	width?: string;
	chrome?: 'none' | 'subtle' | 'card';
} {
	const out: ReturnType<typeof extractSizingAttrs> = {};
	if (attrs.height) out.height = attrs.height;
	if (attrs.minHeight) out.minHeight = attrs.minHeight;
	if (attrs.maxHeight) out.maxHeight = attrs.maxHeight;
	if (attrs.width) out.width = attrs.width;
	const chrome = parseChrome(attrs.chrome);
	if (chrome) out.chrome = chrome;
	return out;
}

// ── Multi-line + block extraction ────────────────────────────────────────────

/**
 * Extract all loom code blocks from an LLM response. Accepts both 4-backtick
 * (canonical) and 3-backtick (regression-friendly) fences, and tolerates an
 * arbitrary number of trailing backticks on the closer. LLMs often drop a
 * backtick after the first turn because chat UIs render 4-backtick markdown
 * as 3-backtick, and the model copies what it sees.
 */
function extractAllLoomBlocks(text: string): string[] {
	const blocks: string[] = [];
	// Match 3+ backticks followed by "loom" (or "loom-patch" is handled
	// separately by the caller), a newline, then content.
	const pattern = /(`{3,})loom\s*\n/g;
	let match;
	while ((match = pattern.exec(text)) !== null) {
		const openTicks = match[1];
		// Closer: a newline followed by the same number of backticks (or more,
		// but at least as many). Match on line boundary.
		const closerRe = new RegExp(`\\n(\`{${openTicks.length},})`);
		const contentStart = match.index + match[0].length;
		const rest = text.substring(contentStart);
		const closerMatch = closerRe.exec(rest);
		const closerIdx = closerMatch ? closerMatch.index : -1;
		const content = closerIdx >= 0 ? rest.substring(0, closerIdx) : rest;
		const trimmed = content.trim();
		if (trimmed) blocks.push(trimmed);
		if (closerIdx >= 0 && closerMatch) {
			pattern.lastIndex = contentStart + closerIdx + 1 + closerMatch[1].length;
		}
	}
	return blocks;
}

/**
 * Permissive reflow pass. Runs BEFORE multi-line value preprocessing and
 * BEFORE the main parser. It turns LLM-friendly loose syntax into the
 * canonical multi-line form.
 *
 * Rules:
 * 1. A `{` anywhere mid-line gets a newline inserted after it. A `}` mid-line
 *    gets a newline inserted before it.
 * 2. Top-level commas (outside quotes, outside triple-backtick blocks) become
 *    newlines. A trailing comma before `}` is silently dropped.
 * 3. Bare continuation lines (starting with an identifier followed by `:` and
 *    not starting with a known directive keyword) get merged back into the
 *    previous line. This lets the LLM split attrs across newlines without a
 *    proper directive prefix.
 *
 * The pass leaves triple-backtick multi-line string values untouched so the
 * downstream multi-line preprocessor can still handle them.
 */
function permissiveReflow(loom: string): string {
	// Phase 1: scan char by char, tracking quote and triple-backtick state,
	// and emit a transformed version where `{`, `}`, and top-level commas
	// become newlines (or line-flanking newlines for braces).
	let out = '';
	let inQuote = false;
	let inTripleBacktick = false;
	let i = 0;
	while (i < loom.length) {
		const c = loom[i];
		// Triple-backtick toggle (`\`\`\``) only when NOT inside quotes.
		if (!inQuote && c === '`' && loom[i + 1] === '`' && loom[i + 2] === '`') {
			out += '```';
			i += 3;
			inTripleBacktick = !inTripleBacktick;
			continue;
		}
		// Inside a triple-backtick or a quoted string, don't touch anything.
		if (inTripleBacktick) {
			out += c;
			i++;
			continue;
		}
		if (c === '"' && loom[i - 1] !== '\\') {
			inQuote = !inQuote;
			out += c;
			i++;
			continue;
		}
		if (inQuote) {
			out += c;
			i++;
			continue;
		}
		// Outside quotes + outside triple-backtick: apply reflow rules.
		if (c === '{') {
			// Strip preceding inline whitespace, emit ` {`, then newline.
			out = out.replace(/[ \t]+$/, '');
			out += ' {\n';
			i++;
			// Skip any whitespace immediately after `{`.
			while (i < loom.length && (loom[i] === ' ' || loom[i] === '\t')) i++;
			continue;
		}
		if (c === '}') {
			// Make sure `}` is on its own line. Strip preceding inline
			// whitespace and dangling commas, ensure a newline before, then
			// emit `}` and a newline after.
			out = out.replace(/[ \t,]*$/, '');
			if (!out.endsWith('\n')) out += '\n';
			out += '}\n';
			i++;
			continue;
		}
		if (c === ',') {
			// Top-level comma → newline. Collapse trailing whitespace.
			out = out.replace(/[ \t]+$/, '');
			if (!out.endsWith('\n')) out += '\n';
			i++;
			// Skip whitespace that follows the comma.
			while (i < loom.length && (loom[i] === ' ' || loom[i] === '\t')) i++;
			continue;
		}
		out += c;
		i++;
	}

	// Phase 2: line continuation merge. A "bare attribute line" (starts with
	// `identifier:` where identifier isn't a directive or brick kind) gets
	// joined back to the previous non-empty line, as long as the previous
	// line isn't `{` or `}`.
	const DIRECTIVE_WORDS = new Set([
		'phase', 'field', 'output', 'live', 'theme',
	]);
	const isDirectiveOrBrick = (word: string): boolean =>
		DIRECTIVE_WORDS.has(word) || isBrickKind(word);

	const lines = out.split('\n');
	const merged: string[] = [];
	let inTripleBacktickBlock = false;
	for (let j = 0; j < lines.length; j++) {
		const line = lines[j];
		const trimmed = line.trim();
		// Track triple-backtick state so we never touch lines inside a
		// multi-line value. Each ``` on a line toggles the state, so lines
		// with an odd count flip it.
		const wasInBlock = inTripleBacktickBlock;
		const tickCount = (line.match(/```/g) || []).length;
		if (tickCount % 2 === 1) inTripleBacktickBlock = !inTripleBacktickBlock;
		if (wasInBlock || inTripleBacktickBlock) {
			merged.push(line);
			continue;
		}
		if (!trimmed) {
			merged.push(line);
			continue;
		}
		// Does this line start with `identifier:`? Pull the head identifier
		// and check whether the next character is a colon (after optional ws).
		const headMatch = trimmed.match(/^([a-zA-Z][a-zA-Z0-9_-]*)\s*:/);
		const head = headMatch ? headMatch[1] : trimmed.split(/\s+/)[0];
		const isContinuation = headMatch !== null && !isDirectiveOrBrick(head);

		if (isContinuation) {
			// Find the last non-empty merged line. Merge only if the prev
			// line is a directive that can still accept trailing attributes:
			// not `{`, not `}`, not a line that ENDS with `{` (those are
			// block opens that should stay on their own line).
			let mergeIdx = -1;
			for (let k = merged.length - 1; k >= 0; k--) {
				const prev = merged[k].trim();
				if (!prev) continue;
				if (prev === '{' || prev === '}') break;
				if (prev.endsWith('{')) break;
				mergeIdx = k;
				break;
			}
			if (mergeIdx >= 0) {
				merged[mergeIdx] = merged[mergeIdx].trimEnd() + ' ' + trimmed;
				continue;
			}
		}
		merged.push(line);
	}

	// Collapse multiple consecutive blank lines.
	const result: string[] = [];
	for (const line of merged) {
		if (line.trim() === '' && result[result.length - 1]?.trim() === '') continue;
		result.push(line);
	}
	return result.join('\n');
}

// Preprocess multi-line triple-backtick values into single-line quoted values.
function preprocessMultiLineValues(loom: string): string[] {
	const rawLines = loom.split('\n');
	const out: string[] = [];
	let inMultiLine = false;
	let multiLineAccum = '';
	let multiLinePendingLine = '';

	for (let i = 0; i < rawLines.length; i++) {
		const line = rawLines[i];
		const trimmed = line.trim();

		if (inMultiLine) {
			if (trimmed === '```' || trimmed.endsWith('```')) {
				const lastContent = trimmed === '```' ? '' : trimmed.slice(0, trimmed.lastIndexOf('```'));
				const trailing = trimmed === '```' ? '' : trimmed.slice(trimmed.lastIndexOf('```') + 3).trim();
				if (lastContent.trim()) {
					multiLineAccum += (multiLineAccum ? '\n' : '') + lastContent;
				}
				const quotedValue = '"' + multiLineAccum.replace(/\\/g, '\\\\').replace(/"/g, '\\"').replace(/\n/g, '\\n') + '"';
				let reconstructed = multiLinePendingLine.replace(/```.*$/, quotedValue);
				if (trailing) reconstructed += ' ' + trailing;
				inMultiLine = false;
				multiLineAccum = '';
				multiLinePendingLine = '';
				out.push(reconstructed);
				continue;
			} else {
				multiLineAccum += (multiLineAccum ? '\n' : '') + line;
				continue;
			}
		}

		if (trimmed.includes('```')) {
			const openIdx = trimmed.indexOf('```');
			const afterOpen = trimmed.slice(openIdx + 3);
			if (afterOpen.includes('```')) {
				const closeIdx = afterOpen.indexOf('```');
				const content = afterOpen.slice(0, closeIdx);
				const quotedValue = '"' + content.replace(/\\/g, '\\\\').replace(/"/g, '\\"') + '"';
				const reconstructed = trimmed.slice(0, openIdx) + quotedValue + afterOpen.slice(closeIdx + 3);
				out.push(reconstructed);
				continue;
			} else {
				inMultiLine = true;
				multiLinePendingLine = trimmed;
				multiLineAccum = afterOpen;
				continue;
			}
		}

		out.push(line);
	}

	if (inMultiLine && multiLinePendingLine) {
		out.push(multiLinePendingLine);
	}

	return out;
}

// ── Brick kinds ──────────────────────────────────────────────────────────────

const BRICK_KINDS: readonly BrickKind[] = [
	'hero','navbar','navlink','logo','banner','text','heading','divider',
	'image','video','embed','quote','stat','stats','feature','feature-grid',
	'faq','qa','testimonial','badge','spacer','section','columns','card',
	'tabs','tab','cta','footer',
] as const;

function isBrickKind(s: string): s is BrickKind {
	return (BRICK_KINDS as readonly string[]).includes(s);
}

// Bricks that can contain nested children.
const NESTABLE_BRICKS: ReadonlySet<BrickKind> = new Set<BrickKind>([
	'columns', 'card', 'tabs', 'tab', 'feature-grid', 'faq', 'stats',
	'navbar', 'section',
]);

// ── Recursive parser ────────────────────────────────────────────────────────

interface ParseState {
	lines: string[];
	pos: number;
	errors: LoomParseError[];
	theme?: RunnerTheme;
}

function makeId(): string {
	return crypto.randomUUID();
}

/**
 * Parse a sequence of blocks until we hit a line that's just `}` (end of our
 * enclosing block) or EOF. Returns the parsed blocks and advances state.pos
 * past the closing brace (if any).
 */
function parseBlockList(state: ParseState, _depth: number): Block[] {
	const blocks: Block[] = [];

	while (state.pos < state.lines.length) {
		const rawLine = state.lines[state.pos];
		const trimmed = rawLine.trim();
		const lineNum = state.pos + 1;

		if (!trimmed || trimmed.startsWith('#')) {
			state.pos++;
			continue;
		}

		if (trimmed === '}') {
			state.pos++;
			return blocks;
		}

		// theme { ... }
		if (trimmed === 'theme {' || trimmed === 'theme{') {
			state.pos++;
			state.theme = parseThemeBody(state);
			continue;
		}

		// phase "Title" "Optional desc" { ... }
		if (trimmed.startsWith('phase ') || trimmed === 'phase') {
			const phase = parsePhase(state);
			if (phase) blocks.push({ kind: 'phase', phase });
			continue;
		}

		// output <nodeId> <portName> ...
		if (trimmed.startsWith('output ')) {
			state.pos++;
			const rest = trimmed.slice('output '.length).trim();
			const tokens = tokenizeLine(rest);
			if (tokens.length < 2) {
				state.errors.push({ line: lineNum, message: `output requires nodeId and portName: ${trimmed}` });
				continue;
			}
			const attrs = parseAttributes(tokens.slice(2));
			const outVariant = parseItemVariant(attrs.as ?? attrs.type ?? attrs.kind);
			const output: OutputItem = {
				id: makeId(),
				nodeId: tokens[0],
				portName: tokens[1],
				...(attrs.label ? { label: attrs.label } : {}),
				...(attrs.description ? { description: attrs.description } : {}),
				...(parseVisibility(attrs.visibility) ? { visibility: parseVisibility(attrs.visibility)! } : {}),
				...(outVariant ? { as: outVariant } : {}),
				...(attrs.placeholder ? { placeholder: attrs.placeholder } : {}),
				...extractSizingAttrs(attrs),
			};
			blocks.push({ kind: 'output', output });
			continue;
		}

		// live <nodeId> ...
		if (trimmed.startsWith('live ')) {
			state.pos++;
			const rest = trimmed.slice('live '.length).trim();
			const tokens = tokenizeLine(rest);
			if (tokens.length < 1) {
				state.errors.push({ line: lineNum, message: `live requires nodeId: ${trimmed}` });
				continue;
			}
			const attrs = parseAttributes(tokens.slice(1));
			const liveVariant = parseItemVariant(attrs.as ?? attrs.type ?? attrs.kind);
			const live: LiveItem = {
				id: makeId(),
				nodeId: tokens[0],
				...(attrs.label ? { label: attrs.label } : {}),
				...(attrs.description ? { description: attrs.description } : {}),
				...(parseVisibility(attrs.visibility) ? { visibility: parseVisibility(attrs.visibility)! } : {}),
				...(liveVariant ? { as: liveVariant } : {}),
				...extractSizingAttrs(attrs),
			};
			blocks.push({ kind: 'live', live });
			continue;
		}

		// field outside any phase: wrap in its own headerless synthetic phase.
		// Each loose field becomes its own block so it can stand alone inside a
		// `columns` container or any other brick. The title is empty to signal
		// to the renderer that no header should be shown.
		if (trimmed.startsWith('field ')) {
			const item = parseFieldLine(state, lineNum);
			if (item) {
				const phase: SetupPhase = { id: makeId(), title: '', items: [item] };
				blocks.push({ kind: 'phase', phase });
			}
			continue;
		}

		// Brick: first token is a brick kind
		const firstSpace = trimmed.indexOf(' ');
		const head = firstSpace === -1 ? trimmed.replace(/\{$/, '').trim() : trimmed.slice(0, firstSpace);
		if (isBrickKind(head)) {
			const brick = parseBrick(state, lineNum);
			if (brick) blocks.push({ kind: 'brick', brick });
			continue;
		}

		state.errors.push({ line: lineNum, message: `Unexpected line: ${trimmed}` });
		state.pos++;
	}

	return blocks;
}

function parseThemeBody(state: ParseState): RunnerTheme {
	const theme: RunnerTheme = {};
	while (state.pos < state.lines.length) {
		const raw = state.lines[state.pos];
		const trimmed = raw.trim();
		state.pos++;
		if (trimmed === '}') break;
		if (!trimmed || trimmed.startsWith('#')) continue;
		const tokens = tokenizeLine(trimmed);
		const attrs = parseAttributes(tokens);
		if (attrs.primary) theme.primary = attrs.primary;
		if (attrs.accent) theme.accent = attrs.accent;
		if (attrs.background) theme.background = attrs.background;
		if (attrs.font) theme.font = attrs.font as RunnerTheme['font'];
		if (attrs.mode === 'light' || attrs.mode === 'dark' || attrs.mode === 'auto') theme.mode = attrs.mode;
		if (attrs.radius) theme.radius = attrs.radius as RunnerTheme['radius'];
		if (
			attrs.layout === 'narrow' || attrs.layout === 'centered' ||
			attrs.layout === 'wide' || attrs.layout === 'ultrawide' ||
			attrs.layout === 'full'
		) {
			theme.layout = attrs.layout;
		}
		if (attrs.skin) theme.skin = attrs.skin;
		if (
			attrs.surface === 'plain' || attrs.surface === 'subtle' ||
			attrs.surface === 'gradient' || attrs.surface === 'glass' ||
			attrs.surface === 'dark' || attrs.surface === 'mesh'
		) {
			theme.surface = attrs.surface;
		}
		if (attrs.density === 'compact' || attrs.density === 'comfortable' || attrs.density === 'spacious') {
			theme.density = attrs.density;
		}
		if (attrs.contentWidth) theme.contentWidth = attrs.contentWidth;
		if (
			attrs.padding === 'sm' || attrs.padding === 'md' || attrs.padding === 'lg' ||
			attrs.padding === 'xl' || attrs.padding === '2xl'
		) {
			theme.padding = attrs.padding;
		}
	}
	return theme;
}

function parsePhase(state: ParseState): SetupPhase | null {
	const rawLine = state.lines[state.pos];
	const trimmed = rawLine.trim();
	state.pos++;
	const rest = trimmed.slice('phase'.length).trim();
	const hasBlock = rest.endsWith('{');
	const cleaned = hasBlock ? rest.slice(0, -1).trim() : rest;
	const tokens = tokenizeLine(cleaned);
	const title = tokens[0] ? parseQuotedString(tokens[0]) : 'Untitled';
	const remaining = tokens.slice(1);
	const positionalDesc = remaining[0] && !remaining[0].includes(':') ? parseQuotedString(remaining[0]) : undefined;
	const attrs = parseAttributes(remaining.filter(t => t.includes(':')));
	const description = positionalDesc ?? attrs.description;

	const phase: SetupPhase = {
		id: makeId(),
		title,
		...(description ? { description } : {}),
		items: [],
		...(parseVisibility(attrs.visibility) ? { visibility: parseVisibility(attrs.visibility)! } : {}),
	};

	if (!hasBlock) return phase;

	// A phase body is a full block list: fields, live items, and arbitrary
	// bricks (columns, card, etc.) can all appear inside. Items and liveItems
	// get extracted into the dedicated slots for legacy rendering paths;
	// everything else ends up in `phase.children` and renders after.
	const body = parseBlockList(state, 1);
	const children: Block[] = [];
	for (const block of body) {
		if (block.kind === 'phase') {
			// Inline a one-field synthetic phase (loose `field` inside the phase)
			// directly into this phase's items rather than nesting phases.
			if (block.phase.title === '' && block.phase.items.length === 1 && (!block.phase.liveItems || block.phase.liveItems.length === 0) && (!block.phase.children || block.phase.children.length === 0)) {
				phase.items.push(...block.phase.items);
			} else {
				// A real nested phase, keep it as a child block.
				children.push(block);
			}
		} else if (block.kind === 'live') {
			phase.liveItems = [...(phase.liveItems ?? []), block.live];
		} else {
			children.push(block);
		}
	}
	if (children.length > 0) phase.children = children;
	return phase;
}

function parseFieldLine(state: ParseState, lineNum: number): SetupItem | null {
	const raw = state.lines[state.pos];
	const trimmed = raw.trim();
	state.pos++;
	const rest = trimmed.slice('field '.length).trim();
	const tokens = tokenizeLine(rest);
	if (tokens.length < 2) {
		state.errors.push({ line: lineNum, message: `field requires nodeId and fieldKey: ${trimmed}` });
		return null;
	}
	const attrs = parseAttributes(tokens.slice(2));
	// Permissive alias lookup: accept common LLM typos for options.
	const opts = parseOptions(attrs.options ?? attrs.option ?? attrs.choices ?? attrs.values);
	const variant = parseItemVariant(attrs.as ?? attrs.type ?? attrs.kind);

	// Reject `as:<unknown>` with a clear message so the AI knows which
	// variant it mistyped. Picker-options validation happens at render time
	// where we can see the catalog-level options, to avoid false positives.
	if (attrs.as && !variant) {
		state.errors.push({
			line: lineNum,
			message: `field ${tokens[0]} ${tokens[1]}: unknown variant as:"${attrs.as}". Valid variants: text, textarea, password, email, url, number, slider, toggle, checkbox, radio, select, cards, multiselect, multicards, tags, date, time, datetime, color, file.`,
		});
	}

	return {
		id: makeId(),
		nodeId: tokens[0],
		fieldKey: tokens[1],
		...(attrs.label ? { label: attrs.label } : {}),
		...(attrs.description ? { description: attrs.description } : {}),
		...(parseVisibility(attrs.visibility) ? { visibility: parseVisibility(attrs.visibility)! } : {}),
		...(variant ? { as: variant } : {}),
		...(opts ? { options: opts } : {}),
		...extractSizingAttrs(attrs),
	};
}

function parseBrick(state: ParseState, lineNum: number): Brick | null {
	const raw = state.lines[state.pos];
	const trimmed = raw.trim();
	state.pos++;

	const hasBlock = trimmed.endsWith('{');
	const cleaned = hasBlock ? trimmed.slice(0, -1).trim() : trimmed;
	const tokens = tokenizeLine(cleaned);
	const head = tokens[0];
	if (!isBrickKind(head)) {
		state.errors.push({ line: lineNum, message: `Unknown brick: ${head}` });
		return null;
	}

	// Positional content: anything before the first `key:` token is positional.
	const rest = tokens.slice(1);
	const positional: string[] = [];
	const attrTokens: string[] = [];
	for (const t of rest) {
		if (t.includes(':') && !t.startsWith('[')) attrTokens.push(t);
		else if (t.startsWith('[') && t.endsWith(']')) attrTokens.push(t);
		else positional.push(parseQuotedString(t));
	}
	const attrs = parseAttributes(attrTokens);

	const props: Record<string, unknown> = { ...attrs };
	if (positional.length > 0) {
		props.content = positional.join(' ');
	}

	const brick: Brick = {
		id: makeId(),
		kind: head,
		props,
		...(parseVisibility(attrs.visibility as string | undefined) ? { visibility: parseVisibility(attrs.visibility as string | undefined)! } : {}),
	};

	if (hasBlock && NESTABLE_BRICKS.has(head)) {
		const children = parseBlockList(state, 1);
		brick.children = children;
	} else if (hasBlock) {
		// Non-nestable with a block: read until matching } but ignore contents
		while (state.pos < state.lines.length && state.lines[state.pos].trim() !== '}') state.pos++;
		if (state.pos < state.lines.length) state.pos++;
		state.errors.push({ line: lineNum, message: `${head} does not accept child blocks` });
	}

	return brick;
}

// ── Top-level parse ─────────────────────────────────────────────────────────

function parseRawLoom(loom: string): LoomParseResult {
	const reflowed = permissiveReflow(loom);
	const lines = preprocessMultiLineValues(reflowed);
	const state: ParseState = { lines, pos: 0, errors: [] };

	// Pre-scan for `theme { ... }` at top level and lift it.
	let theme: RunnerTheme | undefined;
	let scanPos = 0;
	while (scanPos < lines.length) {
		const t = lines[scanPos].trim();
		if (t === '' || t.startsWith('#')) { scanPos++; continue; }
		if (t.startsWith('theme') && t.endsWith('{')) {
			state.pos = scanPos + 1;
			theme = parseThemeBody(state);
			// Remove the theme block from lines in-place to simplify second pass.
			lines.splice(scanPos, state.pos - scanPos);
			state.pos = 0;
			break;
		}
		break;
	}

	const blocks = parseBlockList(state, 0);

	// Derive phases/outputs/liveItems views from the block list.
	const phases: SetupPhase[] = [];
	const outputs: OutputItem[] = [];
	const topLive: LiveItem[] = [];
	for (const b of blocks) {
		if (b.kind === 'phase') phases.push(b.phase);
		else if (b.kind === 'output') outputs.push(b.output);
		else if (b.kind === 'live') topLive.push(b.live);
	}

	// Filter out the theme-sentinel brick (legacy path, belt-and-suspenders).
	const cleanBlocks = blocks.filter(b => !(b.kind === 'brick' && (b.brick.props as { __theme?: unknown }).__theme));

	const manifest: SetupManifest = {
		phases,
		outputs,
		...(topLive.length > 0 ? { liveItems: topLive } : {}),
		...(theme ? { theme } : {}),
		blocks: cleanBlocks,
	};

	return { manifest, errors: state.errors };
}

export function parseLoom(rawResponse: string): { manifest: SetupManifest | null; errors: LoomParseError[] } {
	const blocks = extractAllLoomBlocks(rawResponse);
	if (blocks.length === 0) {
		return { manifest: null, errors: [{ line: 0, message: 'No ````loom block found in response' }] };
	}

	const { manifest, errors } = parseRawLoom(blocks[blocks.length - 1]);

	const isEmpty = manifest.phases.length === 0
		&& manifest.outputs.length === 0
		&& (!manifest.liveItems || manifest.liveItems.length === 0)
		&& (!manifest.blocks || manifest.blocks.length === 0);
	if (isEmpty) {
		return { manifest: null, errors: [...errors, { line: 0, message: 'Loom block is empty' }] };
	}

	return { manifest, errors };
}

export function hasLoomMarker(text: string): boolean {
	return /`{3,}loom\s*\n/.test(text);
}

// ── Patch application ───────────────────────────────────────────────────────
//
// Loom has no serializer anymore. `loomCode` is the single source of
// truth: the parser above produces a one-way `SetupManifest` view
// used by the runner renderer, and every mutation flows through the
// raw text. User edits go through the LoomEditorModal (which saves
// the text verbatim); AI edits go through `applyLoomPatchText`
// (SEARCH/REPLACE on the text). Removing the serializer killed a
// whole class of round-trip data loss (multi-line text blocks,
// comments, attribute ordering).

export { applyLoomPatchText } from './weft-patch';
