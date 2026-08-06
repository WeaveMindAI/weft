<script lang="ts">
	import { onMount, onDestroy } from 'svelte';
	import { EditorView, keymap, lineNumbers, placeholder as placeholderExt } from '@codemirror/view';
	import { Compartment, EditorState } from '@codemirror/state';
	import type { Extension } from '@codemirror/state';
	import { python } from '@codemirror/lang-python';
	import { javascript } from '@codemirror/lang-javascript';
	import { defaultKeymap, history, historyKeymap } from '@codemirror/commands';
	import { syntaxHighlighting, defaultHighlightStyle } from '@codemirror/language';
	import { githubLight } from '@uiw/codemirror-theme-github';
	import { minimalChange } from '../minimal-change';

	let {
		value = '',
		placeholder = '',
		readonly = false,
		minHeight = '80px',
		language,
		liveValue = false,
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
		/// True when `value` is a LIVE external document (a file-backed
		/// field's file content): its changes apply to the editor even
		/// while focused, so what's on disk is what's on screen. False
		/// (default) when `value` is a stored config value: while the
		/// user is typing, the doc is the draft and value changes (a
		/// cleared field's declared default flowing back in) are
		/// deferred to blur, so the draft is never repainted mid-edit.
		liveValue?: boolean;
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
	// `readonly` and `language` are LIVE props (a file-backed field
	// mounts read-only while its content loads, then unlocks), so both
	// sit in compartments the effects below reconfigure on change; baked
	// into the mount-time extension array they would be frozen forever.
	const readonlyCompartment = new Compartment();
	const languageCompartment = new Compartment();

	onMount(() => {
		const extensions = [
			languageCompartment.of(languageExtensions(language)),
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
			readonlyCompartment.of(EditorState.readOnly.of(readonly)),
			EditorView.domEventHandlers({
				// Editing ended: drain the external sync deferred while the
				// doc was the user's draft (see the sync $effect). A real DOM
				// blur fires on every focus loss, so the drain cannot be
				// missed; teardown clears `pendingSync` BEFORE destroy (whose
				// internal blur would otherwise drain into a half-destroyed
				// view), so this never runs on a dying editor.
				blur: () => {
					if (pendingSync) {
						pendingSync = false;
						applyExternal();
					}
					return false;
				},
				// Prevent middle-click paste when editor is not focused
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
		// No drain at teardown: the doc dies with the component and the
		// next mount recreates from `value`. Cleared BEFORE destroy so
		// destroy's own internal blur cannot drain into a dying view.
		pendingSync = false;
		view?.destroy();
		view = null;
	});

	/// Apply the CURRENT `value` prop to the doc as a minimal splice, so
	/// cursor/selection/undo survive outside the changed span. Only ever
	/// called on a live view (the effect and the blur drain both check).
	function applyExternal() {
		if (view === null) throw new Error('CodeEditor: external sync ran on a destroyed editor');
		const change = minimalChange(view.state.doc.toString(), value || '');
		if (change === null) return;
		isExternalUpdate = true;
		view.dispatch({ changes: change, scrollIntoView: false });
		isExternalUpdate = false;
	}

	// Keep the live props applied (see the compartments above).
	$effect(() => {
		void readonly;
		if (view === null) return;
		view.dispatch({
			effects: readonlyCompartment.reconfigure(EditorState.readOnly.of(readonly)),
		});
	});
	$effect(() => {
		void language;
		if (view === null) return;
		view.dispatch({
			effects: languageCompartment.reconfigure(languageExtensions(language)),
		});
	});

	// Sync external value changes into the doc. A live value (file
	// content) applies even while focused: disk is the truth and the
	// screen follows it. A stored config value defers while the user is
	// typing (the doc is the draft; applying mid-edit would repaint the
	// user's text, e.g. a cleared field's declared default flowing back
	// in) and the DOM blur handler above drains the deferred sync the
	// moment editing ends. A DOM handler, not CodeMirror's focusChanged
	// update flag: that flag rides a 10ms reconciliation timer and can
	// fire on unrelated dispatches or never (teardown mid-focus).
	let pendingSync = false;
	$effect(() => {
		void value;
		if (view === null) return;
		if (!liveValue && view.hasFocus) {
			pendingSync = true;
			return;
		}
		applyExternal();
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
