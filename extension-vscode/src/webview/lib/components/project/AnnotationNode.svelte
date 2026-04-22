<script lang="ts">
	import { NodeResizer, type ResizeParams } from "@xyflow/svelte";
	import type { NodeDataUpdates } from "$lib/types";
	import { marked } from 'marked';

	const renderer = new marked.Renderer();
	renderer.link = ({ href, title, text }) => {
		const titleAttr = title ? ` title="${title}"` : '';
		return `<a href="${href}"${titleAttr} target="_blank" rel="noopener noreferrer">${text}</a>`;
	};
	marked.setOptions({ breaks: true, gfm: true, renderer });

	let { data, selected }: { 
		data: { 
			label: string | null; 
			nodeType: string; 
			config: Record<string, unknown>;
			onUpdate?: (updates: NodeDataUpdates) => void;
		}; 
		selected?: boolean 
	} = $props();
	
	let editing = $state(false);
	let editContent = $state('');
	let textareaRef = $state<HTMLTextAreaElement | null>(null);
	let isResizing = $state(false);

	function handleResizeStart() {
		isResizing = true;
	}

	function handleResizeEnd(_event: unknown, params: ResizeParams) {
		isResizing = false;
		if (data.onUpdate) {
			data.onUpdate({
				config: { ...data.config, width: params.width, height: params.height }
			});
		}
	}

	const content = $derived((data.config?.content as string) || '');

	function startEditing(e: MouseEvent) {
		if (e.detail === 2) {
			e.stopPropagation();
			editContent = content;
			editing = true;
			// Focus textarea and place cursor at end after it renders
			requestAnimationFrame(() => {
				if (textareaRef) {
					textareaRef.focus();
					textareaRef.setSelectionRange(textareaRef.value.length, textareaRef.value.length);
				}
			});
		}
	}

	function saveContent() {
		if (isResizing) return;
		editing = false;
		if (data.onUpdate) {
			data.onUpdate({
				config: { ...data.config, content: editContent }
			});
		}
	}

	function handleKeydown(e: KeyboardEvent) {
		// Only handle Escape at the container level
		if (e.key === 'Escape' && editing) {
			editing = false;
		}
	}
	
	function handleTextareaKeydown(e: KeyboardEvent) {
		// Stop propagation AND prevent default capture for all keyboard events in textarea
		// This allows Ctrl+A, Ctrl+Z, Ctrl+C, Ctrl+V, etc. to work normally in the textarea
		e.stopPropagation();
		e.stopImmediatePropagation();
		
		// Only handle Escape to exit editing
		if (e.key === 'Escape') {
			editing = false;
		}
	}

	function renderMarkdown(text: string): string {
		if (!text) return '<p class="placeholder">Double-click to add notes...</p>';
		return marked.parse(text, { async: false }) as string;
	}
</script>

<NodeResizer 
	minWidth={180} 
	minHeight={80} 
	isVisible={selected}
	lineStyle="border-color: #94a3b8; border-width: 1px;"
	handleStyle="background-color: #94a3b8; width: 8px; height: 8px; border-radius: 2px;"
	onResizeStart={handleResizeStart}
	onResizeEnd={handleResizeEnd}
/>

<div 
	class="annotation-node" 
	class:selected 
	class:editing
	onclick={startEditing}
	onkeydown={handleKeydown}
	role="button"
	tabindex="0"
>
	{#if editing}
		<textarea
			bind:this={textareaRef}
			bind:value={editContent}
			onblur={saveContent}
			onkeydown={handleTextareaKeydown}
			placeholder="Write your notes here... (supports markdown)"
			class="edit-textarea nodrag nowheel nopan"
		></textarea>
	{:else}
		<div class="markdown-content">
			{@html renderMarkdown(content)}
		</div>
	{/if}
</div>

<style>
	.annotation-node {
		position: absolute;
		inset: 0;
		background: white;
		border: 1px solid #e2e8f0;
		border-radius: 6px;
		min-width: 180px;
		min-height: 80px;
		overflow: hidden;
		display: flex;
		flex-direction: column;
	}

	.annotation-node.selected {
		border-color: #94a3b8;
		box-shadow: 0 0 0 1px #94a3b8;
	}

	.annotation-node.editing {
		border-color: #64748b;
	}

	.markdown-content {
		padding: 12px 16px;
		font-size: 13px;
		line-height: 1.5;
		color: #374151;
		overflow: auto;
		flex: 1;
		min-height: 0;
	}

	.markdown-content :global(h1) {
		font-size: 18px;
		font-weight: 600;
		margin: 0 0 8px 0;
		color: #111827;
	}

	.markdown-content :global(h2) {
		font-size: 15px;
		font-weight: 600;
		margin: 0 0 6px 0;
		color: #1f2937;
	}

	.markdown-content :global(h3) {
		font-size: 13px;
		font-weight: 600;
		margin: 0 0 4px 0;
		color: #374151;
	}

	.markdown-content :global(p) {
		margin: 0 0 6px 0;
	}

	.markdown-content :global(p:last-child) {
		margin-bottom: 0;
	}

	.markdown-content :global(li > p) {
		margin: 0;
	}

	.markdown-content :global(strong) {
		font-weight: 600;
	}

	.markdown-content :global(em) {
		font-style: italic;
	}

	.markdown-content :global(code) {
		background: #f1f5f9;
		padding: 1px 4px;
		border-radius: 3px;
		font-family: ui-monospace, monospace;
		font-size: 12px;
	}

	.markdown-content :global(ul) {
		margin: 0 0 6px 0;
		padding-left: 18px;
		list-style-type: disc;
	}

	.markdown-content :global(ol) {
		margin: 0 0 6px 0;
		padding-left: 18px;
		list-style-type: decimal;
	}

	.markdown-content :global(li) {
		margin: 1px 0;
		display: list-item;
	}

	.markdown-content :global(ul:last-child),
	.markdown-content :global(ol:last-child) {
		margin-bottom: 0;
	}

	.markdown-content :global(a) {
		color: #3b82f6;
		text-decoration: underline;
	}

	.markdown-content :global(a:hover) {
		color: #2563eb;
	}

	.markdown-content :global(blockquote) {
		border-left: 2px solid #d4d4d8;
		padding-left: 10px;
		margin: 4px 0;
		color: #6b7280;
	}

	.markdown-content :global(pre) {
		background: #f1f5f9;
		border-radius: 4px;
		padding: 8px 10px;
		overflow-x: auto;
		margin: 4px 0;
		font-size: 12px;
	}

	.markdown-content :global(pre code) {
		background: none;
		padding: 0;
	}

	.markdown-content :global(.placeholder) {
		color: #9ca3af;
		font-style: italic;
	}

	.edit-textarea {
		flex: 1;
		min-height: 0;
		width: 100%;
		border: none;
		outline: none;
		resize: none;
		padding: 12px 16px;
		font-size: 13px;
		line-height: 1.5;
		font-family: ui-monospace, monospace;
		background: #fafafa;
		color: #374151;
	}

	.edit-textarea::placeholder {
		color: #9ca3af;
	}
</style>
