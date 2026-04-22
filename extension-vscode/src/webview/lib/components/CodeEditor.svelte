<script lang="ts">
	import { onMount, onDestroy } from 'svelte';
	import { EditorView, keymap, lineNumbers, placeholder as placeholderExt } from '@codemirror/view';
	import { EditorState } from '@codemirror/state';
	import { python } from '@codemirror/lang-python';
	import { defaultKeymap, history, historyKeymap } from '@codemirror/commands';
	import { syntaxHighlighting, defaultHighlightStyle } from '@codemirror/language';
	import { githubLight } from '@uiw/codemirror-theme-github';

	let {
		value = '',
		placeholder = '',
		readonly = false,
		minHeight = '80px',
		onchange,
	}: {
		value?: string;
		placeholder?: string;
		readonly?: boolean;
		minHeight?: string;
		onchange?: (value: string) => void;
	} = $props();

	let container: HTMLDivElement;
	let view: EditorView | null = null;
	let isExternalUpdate = false;

	onMount(() => {
		const extensions = [
			python(),
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
