<script lang="ts">
	import { onMount, onDestroy } from 'svelte';
	import { EditorView, keymap, lineNumbers, gutter, GutterMarker, Decoration, type DecorationSet } from '@codemirror/view';
	import { EditorState, StateField, StateEffect, RangeSet, Compartment } from '@codemirror/state';
	import { defaultKeymap, history, historyKeymap, indentWithTab } from '@codemirror/commands';
	import { syntaxHighlighting, defaultHighlightStyle, foldService, foldGutter, codeFolding } from '@codemirror/language';
	import { githubLight } from '@uiw/codemirror-theme-github';
	import { weft } from './weft-lang';
	import type { OpaqueBlock, WeftParseError } from '$lib/ai/weft-parser';

	let errorsCopied = $state(false);

	let {
		value = '',
		maximized = false,
		locked = false,
		opaqueBlocks = [],
		parseErrors = [],
		saveStatus = 'idle',
		onchange,
		onSort,
		onToggleMaximize,
		onClose,
	}: {
		value?: string;
		maximized?: boolean;
		locked?: boolean;
		opaqueBlocks?: OpaqueBlock[];
		parseErrors?: WeftParseError[];
		saveStatus?: 'idle' | 'saved';
		onchange?: (value: string) => void;
		onSort?: () => void;
		onToggleMaximize?: () => void;
		onClose?: () => void;
	} = $props();

	let container: HTMLDivElement;
	let view: EditorView | null = null;
	let isExternalUpdate = false;
	const readOnlyCompartment = new Compartment();

	// Error gutter marker with custom tooltip
	const setErrorLines = StateEffect.define<{ lines: Set<number>; errors: Map<number, string> }>();
	let tooltip: HTMLDivElement | null = null;

	function showTooltip(message: string, x: number, y: number) {
		if (!tooltip) {
			tooltip = document.createElement('div');
			tooltip.className = 'cm-error-tooltip';
			document.body.appendChild(tooltip);
		}
		tooltip.textContent = message;
		tooltip.style.left = `${x + 8}px`;
		tooltip.style.top = `${y - 4}px`;
		tooltip.style.display = 'block';
	}

	function hideTooltip() {
		if (tooltip) tooltip.style.display = 'none';
	}

	class ErrorGutterMarker extends GutterMarker {
		message: string;
		constructor(message: string) {
			super();
			this.message = message;
		}
		toDOM() {
			const el = document.createElement('div');
			el.className = 'cm-error-gutter-marker';
			return el;
		}
	}

	const errorLineField = StateField.define<{ lines: Set<number>; errors: Map<number, string> }>({
		create() { return { lines: new Set(), errors: new Map() }; },
		update(val, tr) {
			for (const effect of tr.effects) {
				if (effect.is(setErrorLines)) return effect.value;
			}
			if (tr.docChanged) return { lines: new Set(), errors: new Map() };
			return val;
		},
	});

	const errorLineDecoration = Decoration.line({ class: 'cm-error-line' });

	const errorLineDecorationField = StateField.define<DecorationSet>({
		create() { return Decoration.none; },
		update(_, tr) {
			const state = tr.state.field(errorLineField);
			const decorations: any[] = [];
			for (const lineNum of state.lines) {
				if (lineNum >= 1 && lineNum <= tr.state.doc.lines) {
					const line = tr.state.doc.line(lineNum);
					decorations.push(errorLineDecoration.range(line.from));
				}
			}
			return RangeSet.of(decorations, true);
		},
		provide: f => EditorView.decorations.from(f),
	});

	const errorGutter = gutter({
		class: 'cm-error-gutter',
		markers(view) {
			const state = view.state.field(errorLineField);
			const markers: any[] = [];
			for (const [lineNum, message] of state.errors) {
				if (lineNum >= 1 && lineNum <= view.state.doc.lines) {
					const line = view.state.doc.line(lineNum);
					markers.push(new ErrorGutterMarker(message).range(line.from));
				}
			}
			return RangeSet.of(markers, true);
		},
		domEventHandlers: {
			mouseover(view, line, event) {
				const lineNo = view.state.doc.lineAt(line.from).number;
				const state = view.state.field(errorLineField);
				const message = state.errors.get(lineNo);
				if (message) {
					showTooltip(message, (event as MouseEvent).clientX, (event as MouseEvent).clientY);
				}
				return false;
			},
			mouseout(_view, _line, _event) {
				hideTooltip();
				return false;
			},
		},
	});

	// Fold service: fold Group blocks from end of { line to end of } line.
	// The placeholder widget renders the description comment + ... + } as a block.
	const groupFoldService = foldService.of((state, lineStart, _lineEnd) => {
		const line = state.doc.lineAt(lineStart);
		const text = line.text.trimEnd();
		// Only trigger on lines ending with { that are part of a Group declaration
		if (!text.endsWith('{')) return null;
		// Check this line or preceding lines for Group
		let isGroup = /=\s*Group[\s({]/.test(text);
		if (!isGroup) {
			// Check if this { is the end of a multiline Group signature
			for (let k = line.number - 1; k >= Math.max(1, line.number - 10); k--) {
				const prev = state.doc.line(k).text;
				if (/=\s*Group[\s({]/.test(prev)) { isGroup = true; break; }
				if (prev.trim() && !/^[\s)>\-,(:]/.test(prev.trim()) && !/^\w+\s*:/.test(prev.trim())) break;
			}
		}
		if (!isGroup) return null;

		const foldFrom = line.to;
		let depth = 0;
		for (let i = line.number; i <= state.doc.lines; i++) {
			const l = state.doc.line(i);
			for (const ch of l.text) {
				if (ch === '{') depth++;
				if (ch === '}') depth--;
			}
			if (depth === 0) {
				const foldTo = l.from;
				if (foldTo > foldFrom) return { from: foldFrom, to: foldTo };
				return null;
			}
		}
		return null;
	});

	onMount(() => {
		const extensions = [
			weft(),
			githubLight,
			syntaxHighlighting(defaultHighlightStyle),
			lineNumbers(),
			groupFoldService,
			codeFolding({
				preparePlaceholder(state, range) {
					const foldedText = state.doc.sliceString(range.from, range.to);
					let desc = '';
					for (const l of foldedText.split('\n')) {
						const trimmed = l.trim();
						if (!trimmed) continue;
						if (trimmed.startsWith('#')) { desc = trimmed; break; }
						break;
					}
					return desc;
				},
				placeholderDOM(_view, onclick, prepared) {
					const desc = prepared as string;
					const container = document.createElement('div');
					container.style.cssText = 'cursor:pointer;';
					container.onclick = onclick;
					if (desc) {
						const commentDiv = document.createElement('div');
						commentDiv.textContent = '  ' + desc;
						commentDiv.style.cssText = 'color:#71717a;';
						container.appendChild(commentDiv);
					}
					const dotsDiv = document.createElement('div');
					dotsDiv.textContent = '  ...';
					dotsDiv.style.cssText = 'color:#a1a1aa;font-style:italic;';
					container.appendChild(dotsDiv);
					return container;
				},
			}),
			foldGutter({
				markerDOM(open) {
					const span = document.createElement('span');
					span.textContent = '›';
					span.className = open ? 'cm-fold-open' : 'cm-fold-closed';
					return span;
				},
			}),
			EditorView.lineWrapping,
			errorLineField,
			errorLineDecorationField,
			errorGutter,
			history(),
			keymap.of([indentWithTab, ...defaultKeymap, ...historyKeymap]),
			readOnlyCompartment.of(EditorState.readOnly.of(false)),
			EditorView.updateListener.of((update) => {
				if (update.docChanged && !isExternalUpdate) {
					onchange?.(update.state.doc.toString());
				}
			}),
			EditorView.theme({
				'&': {
					fontSize: '12px',
					height: '100%',
					backgroundColor: '#fafafa !important',
				},
				'.cm-content': {
					fontFamily: 'ui-monospace, SFMono-Regular, "SF Mono", Menlo, Consolas, monospace',
					padding: '12px 8px',
					caretColor: '#18181b',
				},
				'.cm-line': {
					padding: '0 4px',
				},
				'.cm-gutters': {
					backgroundColor: '#f4f4f5 !important',
					borderRight: '1px solid #e4e4e7',
					color: '#a1a1aa',
					fontSize: '11px',
					minWidth: '36px',
				},
				'.cm-foldGutter': {
					width: '14px',
					minWidth: '14px',
				},
				'.cm-foldGutter .cm-gutterElement': {
					position: 'relative',
				},
				'.cm-fold-open, .cm-fold-closed': {
					position: 'absolute',
					top: '0',
					left: '0',
					width: '14px',
					height: '20px',
					display: 'inline-flex',
					alignItems: 'center',
					justifyContent: 'center',
					fontSize: '13px',
					color: '#a1a1aa',
					cursor: 'pointer',
					userSelect: 'none',
					transition: 'transform 0.15s ease',
				},
				'.cm-fold-open': {
					transform: 'rotate(90deg)',
				},
				'.cm-fold-closed': {
					transform: 'rotate(0deg)',
				},
				'.cm-fold-open:hover, .cm-fold-closed:hover': {
					color: '#52525b',
				},
				'.cm-activeLineGutter': {
					backgroundColor: '#e4e4e7 !important',
				},
				'.cm-activeLine': {
					backgroundColor: '#f4f4f515',
				},
				'.cm-scroller': {
					overflow: 'auto',
				},
				'&.cm-focused': {
					outline: 'none',
				},
				'.cm-selectionBackground, ::selection': {
					backgroundColor: '#d4d4d8 !important',
				},
				'.cm-error-gutter': {
					width: '3px',
					minWidth: '3px',
					marginRight: '2px',
				},
				'.cm-error-gutter-marker': {
					width: '3px',
					height: '100%',
					backgroundColor: '#ef4444',
					borderRadius: '1px',
					cursor: 'pointer',
				},
				'.cm-error-line': {
					backgroundColor: '#fef2f2 !important',
				},
			}, { dark: false }),
			EditorView.domEventHandlers({
				auxclick: (event: MouseEvent) => {
					if (event.button === 1) {
						event.preventDefault();
						return true;
					}
					return false;
				},
				paste: (event: ClipboardEvent, v: EditorView) => {
					if (!v.hasFocus) {
						event.preventDefault();
						return true;
					}
					return false;
				},
			}),
		];

		view = new EditorView({
			state: EditorState.create({
				doc: value || '',
				extensions,
			}),
			parent: container,
		});
	});

	onDestroy(() => {
		view?.destroy();
		if (tooltip) { tooltip.remove(); tooltip = null; }
	});

	$effect(() => {
		if (!view) return;
		const newValue = value || '';
		const oldValue = view.state.doc.toString();
		if (newValue === oldValue) return;

		isExternalUpdate = true;
		// Find the minimal changed range to preserve cursor, scroll, selection, and undo
		let prefixLen = 0;
		const minLen = Math.min(oldValue.length, newValue.length);
		while (prefixLen < minLen && oldValue[prefixLen] === newValue[prefixLen]) prefixLen++;
		let oldSuffix = oldValue.length;
		let newSuffix = newValue.length;
		while (oldSuffix > prefixLen && newSuffix > prefixLen && oldValue[oldSuffix - 1] === newValue[newSuffix - 1]) {
			oldSuffix--;
			newSuffix--;
		}
		view.dispatch({
			changes: { from: prefixLen, to: oldSuffix, insert: newValue.slice(prefixLen, newSuffix) },
			scrollIntoView: false,
		});
		isExternalUpdate = false;
	});

	$effect(() => {
		if (!view) return;
		const lines = new Set<number>();
		const errors = new Map<number, string>();
		for (const block of opaqueBlocks) {
			for (let l = block.startLine; l <= block.endLine; l++) {
				lines.add(l);
				errors.set(l, block.error || 'Unparseable code');
			}
		}
		for (const err of parseErrors) {
			lines.add(err.line);
			const existing = errors.get(err.line);
			errors.set(err.line, existing ? `${existing}; ${err.message}` : err.message);
		}
		view.dispatch({ effects: setErrorLines.of({ lines, errors }) });
	});

	$effect(() => {
		if (!view) return;
		view.dispatch({
			effects: readOnlyCompartment.reconfigure(EditorState.readOnly.of(locked)),
		});
	});

</script>

<div class="weft-code-panel">
	<div class="panel-header">
		<span class="panel-title">Weft{#if locked}<span class="streaming-indicator"> · streaming</span>{:else if saveStatus === 'saved'}<span class="save-indicator"> · saved</span>{/if}{#if parseErrors.length > 0}<span class="error-count"> · {parseErrors.length} error{parseErrors.length > 1 ? 's' : ''}</span>{/if}</span>
		<div class="panel-actions">
			{#if parseErrors.length > 0}
				<button class="panel-btn error-btn" onclick={() => {
					const weftLines = value.split('\n');
					// Group consecutive errors with the same message prefix
					const groups: { message: string; startLine: number; endLine: number; lines: string[] }[] = [];
					for (const e of parseErrors) {
						const lineContent = e.line > 0 && e.line <= weftLines.length ? weftLines[e.line - 1]?.trim() : '';
						const prev = groups[groups.length - 1];
						// Group if same error type and consecutive lines
						const msgPrefix = e.message.replace(/:.+$/, '');
						const prevPrefix = prev?.message.replace(/:.+$/, '');
						if (prev && msgPrefix === prevPrefix && e.line === prev.endLine + 1) {
							prev.endLine = e.line;
							if (lineContent) prev.lines.push(lineContent);
						} else {
							groups.push({ message: e.message, startLine: e.line, endLine: e.line, lines: lineContent ? [lineContent] : [] });
						}
					}
					const detailed = groups.map(g => {
						const lineRange = g.startLine === g.endLine ? `Line ${g.startLine}` : `Lines ${g.startLine}-${g.endLine}`;
						const preview = g.lines.length <= 3
							? g.lines.map(l => `  > ${l}`).join('\n')
							: [...g.lines.slice(0, 2).map(l => `  > ${l}`), `  > ... (${g.lines.length - 2} more lines)`, `  > ${g.lines[g.lines.length - 1]}`].join('\n');
						return `${lineRange}: ${g.message}${preview ? '\n' + preview : ''}`;
					}).join('\n\n');
					navigator.clipboard.writeText(detailed);
					errorsCopied = true;
					setTimeout(() => { errorsCopied = false; }, 1500);
				}} title="Copy all errors to clipboard">
					{#if errorsCopied}
						<svg xmlns="http://www.w3.org/2000/svg" width="12" height="12" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><polyline points="20 6 9 17 4 12"></polyline></svg>
					{:else}
						<svg xmlns="http://www.w3.org/2000/svg" width="12" height="12" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><rect x="9" y="9" width="13" height="13" rx="2" ry="2"></rect><path d="M5 15H4a2 2 0 0 1-2-2V4a2 2 0 0 1 2-2h9a2 2 0 0 1 2 2v1"></path></svg>
					{/if}
				</button>
			{/if}
			{#if onSort}
				<button class="panel-btn" onclick={onSort} title="Sort (auto-organize)">
					<svg xmlns="http://www.w3.org/2000/svg" width="12" height="12" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="m3 16 4 4 4-4"/><path d="M7 20V4"/><path d="m21 8-4-4-4 4"/><path d="M17 4v16"/></svg>
				</button>
			{/if}
			{#if onToggleMaximize}
				<button class="panel-btn" onclick={onToggleMaximize} title={maximized ? 'Restore' : 'Maximize'}>
					{#if maximized}
						<svg xmlns="http://www.w3.org/2000/svg" width="12" height="12" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><polyline points="4 14 10 14 10 20"/><polyline points="20 10 14 10 14 4"/><line x1="14" y1="10" x2="21" y2="3"/><line x1="3" y1="21" x2="10" y2="14"/></svg>
					{:else}
						<svg xmlns="http://www.w3.org/2000/svg" width="12" height="12" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><polyline points="15 3 21 3 21 9"/><polyline points="9 21 3 21 3 15"/><line x1="21" y1="3" x2="14" y2="10"/><line x1="3" y1="21" x2="10" y2="14"/></svg>
					{/if}
				</button>
			{/if}
			{#if onClose}
				<button class="panel-btn" onclick={onClose} title="Close">
					<svg xmlns="http://www.w3.org/2000/svg" width="12" height="12" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><line x1="18" y1="6" x2="6" y2="18"/><line x1="6" y1="6" x2="18" y2="18"/></svg>
				</button>
			{/if}
		</div>
	</div>
	<div class="panel-body" bind:this={container}></div>
</div>

<style>
	.weft-code-panel {
		display: flex;
		flex-direction: column;
		height: 100%;
		background: #fafafa;
	}

	.panel-header {
		display: flex;
		align-items: center;
		justify-content: space-between;
		padding: 0 12px;
		height: 40px;
		border-bottom: 1px solid #e4e4e7;
		background: #f4f4f5;
		flex-shrink: 0;
	}

	.panel-title {
		font-size: 11px;
		font-weight: 600;
		color: #52525b;
		text-transform: uppercase;
		letter-spacing: 0.05em;
		font-family: ui-monospace, SFMono-Regular, "SF Mono", Menlo, Consolas, monospace;
	}

	.panel-actions {
		display: flex;
		align-items: center;
		gap: 2px;
	}

	.panel-btn {
		display: flex;
		align-items: center;
		justify-content: center;
		width: 22px;
		height: 22px;
		border-radius: 4px;
		color: #71717a;
		transition: all 0.15s;
	}

	.panel-btn:hover {
		background: #e4e4e7;
		color: #18181b;
	}

	.error-count {
		color: #ef4444;
		font-weight: 500;
	}

	.error-btn {
		color: #ef4444;
	}

	.error-btn:hover {
		background: #fef2f2;
		color: #dc2626;
	}

	.panel-body {
		flex: 1;
		overflow: hidden;
	}

	.panel-body :global(.cm-editor) {
		height: 100%;
	}

	.panel-body :global(.cm-scroller) {
		overflow: auto !important;
	}

	.streaming-indicator {
		font-weight: 400;
		color: #a1a1aa;
		animation: pulse-opacity 1.5s ease-in-out infinite;
	}

	.save-indicator {
		font-weight: 400;
		color: #22c55e;
	}

	@keyframes pulse-opacity {
		0%, 100% { opacity: 1; }
		50% { opacity: 0.4; }
	}

	:global(.cm-error-tooltip) {
		position: fixed;
		z-index: 10000;
		background: #1c1917;
		color: #fef2f2;
		font-size: 11px;
		font-family: ui-monospace, SFMono-Regular, "SF Mono", Menlo, Consolas, monospace;
		padding: 4px 8px;
		border-radius: 4px;
		max-width: 360px;
		pointer-events: none;
		white-space: pre-wrap;
		box-shadow: 0 2px 8px rgba(0,0,0,0.2);
		display: none;
	}
</style>
