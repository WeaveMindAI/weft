<script lang="ts">
	import { onDestroy } from 'svelte';
	import { EditorView, keymap, lineNumbers } from '@codemirror/view';
	// `EditorView.lineWrapping` is a prebuilt extension on the class itself.
	import { EditorState, Compartment } from '@codemirror/state';
	import { defaultKeymap, history, historyKeymap, indentWithTab } from '@codemirror/commands';
	import { syntaxHighlighting, defaultHighlightStyle, HighlightStyle, StreamLanguage } from '@codemirror/language';
	import { tags as t } from '@lezer/highlight';
	import { githubLight } from '@uiw/codemirror-theme-github';
	import { X } from '@lucide/svelte';
	import { parseLoom } from '$lib/ai/loom-parser';
	import type { ProjectDefinition } from '$lib/types';

	let {
		open = $bindable(false),
		project,
		onSave,
	}: {
		open: boolean;
		project: ProjectDefinition;
		// The editor saves raw loom text. The caller writes it to
		// `project.loomCode` and hydrates the manifest as a derived
		// view. No round-trip through a serializer.
		onSave: (loomText: string) => void;
	} = $props();

	let container = $state<HTMLDivElement | null>(null);
	let view: EditorView | null = null;
	let loomText = $state('');
	let parseErrors = $state<Array<{ line: number; message: string }>>([]);
	let dirty = $state(false);
	const readOnlyCompartment = new Compartment();

	// Minimal Loom tokenizer for syntax highlighting.
	// Keywords: phase, output, field, live, theme, columns, card, tabs, tab,
	// hero, banner, text, heading, divider, image, video, embed, quote, stat,
	// stats, feature, feature-grid, faq, qa, testimonial, badge, spacer, cta,
	// footer.
	const LOOM_KEYWORDS = new Set([
		'phase', 'output', 'field', 'live', 'theme',
		'columns', 'card', 'tabs', 'tab',
		'hero', 'banner', 'text', 'heading', 'divider',
		'image', 'video', 'embed', 'quote',
		'stat', 'stats', 'feature', 'feature-grid',
		'faq', 'qa', 'testimonial', 'badge', 'spacer',
		'cta', 'footer',
	]);

	const loomLanguage = StreamLanguage.define({
		name: 'loom',
		startState: () => ({ inString: false }),
		token(stream, state) {
			// String literal
			if (stream.match('"')) {
				while (!stream.eol()) {
					const ch = stream.next();
					if (ch === '\\') { stream.next(); continue; }
					if (ch === '"') return 'string';
				}
				return 'string';
			}
			// Comment
			if (stream.match('#')) { stream.skipToEnd(); return 'comment'; }
			// Braces
			if (stream.match('{') || stream.match('}')) return 'brace';
			// Attribute key:"value" pattern → highlight key as attribute name
			const keyMatch = stream.match(/^[a-z][a-zA-Z0-9_]*:/);
			if (keyMatch) return 'propertyName';
			// Word: check if it's a known keyword
			if (stream.match(/^[a-zA-Z][a-zA-Z0-9_-]*/)) {
				const word = (stream.current() as string);
				if (LOOM_KEYWORDS.has(word)) return 'keyword';
				return 'variableName';
			}
			stream.next();
			return null;
		},
	});

	const loomHighlight = HighlightStyle.define([
		{ tag: t.keyword, color: '#7c3aed', fontWeight: '600' },
		{ tag: t.string, color: '#16a34a' },
		{ tag: t.comment, color: '#9ca3af', fontStyle: 'italic' },
		{ tag: t.propertyName, color: '#0284c7' },
		{ tag: t.variableName, color: '#111827' },
	]);

	function loadCurrentLoom() {
		// Source of truth: the raw `loomCode` on the project. The
		// parsed `setupManifest` is a one-way derived view used only
		// by the runner renderer, never a write target. Reading from
		// it here would re-serialize and lose content (comments,
		// multi-line text blocks, ordering).
		loomText = project.loomCode ?? '';
		parseErrors = [];
		dirty = false;
	}

	function createEditor() {
		if (!container) return;
		const state = EditorState.create({
			doc: loomText,
			extensions: [
				lineNumbers(),
				history(),
				keymap.of([...defaultKeymap, ...historyKeymap, indentWithTab]),
				loomLanguage,
				syntaxHighlighting(loomHighlight),
				syntaxHighlighting(defaultHighlightStyle),
				githubLight,
				EditorView.lineWrapping,
				readOnlyCompartment.of(EditorState.readOnly.of(false)),
				EditorView.updateListener.of((update) => {
					if (update.docChanged) {
						loomText = update.state.doc.toString();
						dirty = true;
						validate();
					}
				}),
				EditorView.theme({
					'&': { fontSize: '13px', height: '100%' },
					'.cm-content': {
						fontFamily: 'ui-monospace, SFMono-Regular, Menlo, Monaco, Consolas, monospace',
						whiteSpace: 'pre-wrap',
						wordBreak: 'break-word',
					},
					'.cm-gutters': { backgroundColor: 'transparent', border: 'none' },
					'.cm-scroller': { overflow: 'auto' },
				}),
			],
		});
		view = new EditorView({ state, parent: container });
	}

	function validate() {
		const wrapped = `\`\`\`\`loom\n${loomText}\n\`\`\`\``;
		const { manifest, errors } = parseLoom(wrapped);
		parseErrors = errors;
		return manifest;
	}

	function handleSave() {
		// Re-parse to block save on syntax errors. The parser still
		// exists as a validator and a rendering view; we just never
		// feed its output back through a serializer.
		const manifest = validate();
		if (!manifest) return;
		onSave(loomText);
		dirty = false;
		open = false;
	}

	function handleClose() {
		if (dirty && !confirm('Discard unsaved changes?')) return;
		open = false;
	}

	$effect(() => {
		if (open) {
			loadCurrentLoom();
			// Wait for the next tick so `container` is bound.
			queueMicrotask(() => {
				if (view) {
					view.destroy();
					view = null;
				}
				createEditor();
			});
		} else if (view) {
			view.destroy();
			view = null;
		}
	});

	onDestroy(() => {
		if (view) view.destroy();
	});
</script>

{#if open}
	<div class="fixed inset-0 bg-black/50 flex items-center justify-center z-50 p-4" role="dialog" aria-modal="true" tabindex="-1">
		<div class="bg-white rounded-xl shadow-xl w-full max-w-4xl h-[80vh] flex flex-col">
			<div class="flex items-center justify-between px-6 py-4 border-b border-border">
				<div>
					<h2 class="text-lg font-semibold">Runner Page (Loom)</h2>
					<p class="text-xs text-muted-foreground mt-0.5">Edit the Loom DSL directly. Changes apply when you click Save.</p>
				</div>
				<button
					type="button"
					class="text-muted-foreground hover:text-foreground p-1 rounded hover:bg-muted transition-colors"
					onclick={handleClose}
					aria-label="Close"
				>
					<X class="w-5 h-5" />
				</button>
			</div>

			<div class="flex-1 overflow-hidden flex flex-col">
				<div bind:this={container} class="flex-1 overflow-auto"></div>
				{#if parseErrors.length > 0}
					<div class="border-t border-destructive/30 bg-destructive/5 px-4 py-2 max-h-32 overflow-auto">
						<div class="text-xs font-semibold text-destructive mb-1">
							{parseErrors.length} error{parseErrors.length === 1 ? '' : 's'}
						</div>
						<ul class="text-xs text-destructive space-y-0.5 font-mono">
							{#each parseErrors as err}
								<li>line {err.line}: {err.message}</li>
							{/each}
						</ul>
					</div>
				{/if}
			</div>

			<div class="flex items-center justify-between gap-2 px-6 py-3 border-t border-border">
				<div class="text-xs text-muted-foreground">
					{#if dirty}Unsaved changes{:else}No changes{/if}
				</div>
				<div class="flex items-center gap-2">
					<button
						type="button"
						class="text-sm px-4 py-2 rounded-md border border-border hover:bg-muted"
						onclick={handleClose}
					>Cancel</button>
					<button
						type="button"
						class="text-sm px-4 py-2 rounded-md bg-violet-600 text-white hover:bg-violet-700 disabled:opacity-50"
						onclick={handleSave}
						disabled={parseErrors.length > 0}
					>Save</button>
				</div>
			</div>
		</div>
	</div>
{/if}
