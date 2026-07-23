<script lang="ts">
	import { onMount, onDestroy } from 'svelte';
	import { EditorView, keymap, lineNumbers, placeholder as placeholderExt } from '@codemirror/view';
	import { EditorState } from '@codemirror/state';
	import type { Extension } from '@codemirror/state';
	import { python } from '@codemirror/lang-python';
	import { javascript } from '@codemirror/lang-javascript';
	import { defaultKeymap, history, historyKeymap } from '@codemirror/commands';
	import { syntaxHighlighting, defaultHighlightStyle } from '@codemirror/language';
	import { githubLight } from '@uiw/codemirror-theme-github';

	let {
		value = '',
		placeholder = '',
		readonly = false,
		minHeight = '80px',
		language,
		onchange,
	}: {
		value?: string;
		placeholder?: string;
		readonly?: boolean;
		minHeight?: string;
		/// The code widget's declared syntax (metadata `language`). The
		/// Rust widget always carries one; absence is a malformed state
		/// and surfaces loudly, never a silent default language.
		language?: string;
		onchange?: (value: string) => void;
	} = $props();

	/// The one language -> CodeMirror-extension table. An unknown or
	/// missing language is a LOUD console error and renders as plain
	/// text; it is never silently highlighted as some other language.
	function languageExtensions(lang: string | undefined): Extension[] {
		if (lang === undefined) {
			console.error('CodeEditor: code widget carries no language; rendering plain text');
			return [];
		}
		switch (lang) {
			case 'python':
				return [python()];
			case 'javascript':
				return [javascript()];
			default:
				console.error(
					`CodeEditor: no syntax support for language '${lang}' (known: python, javascript); rendering plain text`,
				);
				return [];
		}
	}

	let container: HTMLDivElement;
	let view: EditorView | null = null;
	let isExternalUpdate = false;

	onMount(() => {
		const extensions = [
			...languageExtensions(language),
			githubLight,
			lineNumbers(),
			EditorView.lineWrapping,
			// Override theme background with our zinc-100
			EditorView.theme({
				'&': {
					fontSize: '12px',
					backgroundColor: '#f4f4f5 !important',
				},
				'.cm-gutters': {
					backgroundColor: '#f4f4f5 !important',
				},
			}, { dark: false }),
			history(),
			keymap.of([...defaultKeymap, ...historyKeymap]),
			EditorView.updateListener.of((update) => {
				if (update.docChanged && !isExternalUpdate) {
					onchange?.(update.state.doc.toString());
				}
			}),
			EditorView.theme({
				'.cm-content': {
					fontFamily: 'ui-monospace, SFMono-Regular, "SF Mono", Menlo, Consolas, monospace',
					padding: '8px 12px',
					caretColor: '#18181b',
				},
				'.cm-line': {
					padding: '0',
				},
				'.cm-gutters': {
					backgroundColor: '#f4f4f5 !important',
					borderRight: '1px solid #e4e4e7',
					color: '#a1a1aa',
					fontSize: '11px',
					minWidth: '32px',
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
			}),
			EditorState.readOnly.of(readonly),
			// Prevent middle-click paste when editor is not focused
			EditorView.domEventHandlers({
				auxclick: (event: MouseEvent, view: EditorView) => {
					// Middle click (button 1) - prevent paste when not focused
					if (event.button === 1) {
						event.preventDefault();
						return true;
					}
					return false;
				},
				paste: (event: ClipboardEvent, view: EditorView) => {
					// Block paste if editor wasn't focused before
					if (!view.hasFocus) {
						event.preventDefault();
						return true;
					}
					return false;
				},
			}),
		];

		if (placeholder) {
			extensions.push(placeholderExt(placeholder));
		}

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
	});

	// Sync external value changes using minimal diff to preserve cursor/selection/undo
	$effect(() => {
		if (!view) return;
		const newValue = value || '';
		const oldValue = view.state.doc.toString();
		if (newValue === oldValue) return;

		isExternalUpdate = true;
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
</script>

<div class="code-editor-wrapper" style="min-height: {minHeight}; resize: vertical; overflow: auto;">
	<div bind:this={container} class="editor-container"></div>
</div>

<style>
	.code-editor-wrapper {
		border-radius: 6px;
		border: 1px solid hsl(var(--border));
		background: #f4f4f5;
	}
	
	.editor-container {
		width: 100%;
		height: 100%;
	}
	
	.editor-container :global(.cm-editor) {
		height: 100%;
	}
	
	.editor-container :global(.cm-scroller) {
		overflow: auto !important;
	}
</style>
