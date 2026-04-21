<script lang="ts">
	import type { OutputItem, ProjectDefinition } from '$lib/types';
	import { NODE_TYPE_CONFIG } from '$lib/nodes';
	import { sizingStyle, parseSize } from './sizing';
	import { Copy, Check } from '@lucide/svelte';

	let {
		output,
		project,
		executionState,
		renderMarkdown,
	}: {
		output: OutputItem;
		project: ProjectDefinition;
		executionState: { isRunning: boolean; nodeOutputs?: Record<string, unknown> };
		renderMarkdown: (s: string) => string;
	} = $props();

	function getValue(): unknown {
		const nodeOut = executionState.nodeOutputs?.[output.nodeId] as Record<string, unknown> | undefined;
		return nodeOut?.[output.portName] ?? null;
	}

	function nodeLabel(): string {
		const node = project.nodes.find(n => n.id === output.nodeId);
		if (!node) return output.nodeId;
		const template = NODE_TYPE_CONFIG[node.nodeType];
		return node.label ?? template?.label ?? node.nodeType;
	}

	function formatValue(val: unknown): string {
		if (val === null || val === undefined) return '';
		if (typeof val === 'string') return val;
		return JSON.stringify(val, null, 2);
	}

	const val = $derived(getValue());
	const hasValue = $derived(val !== null && val !== undefined);
	const label = $derived(output.label ?? output.portName);
	const variant = $derived(output.as);
	const chrome = $derived(output.chrome ?? 'none');

	// Sizing: `height` means the output *content area* (the markdown / media
	// box), not the whole block. Default for markdown/code/json variants is
	// a sensible min-height so the empty state has room.
	const contentHeight = $derived(parseSize(output.height));
	const contentMinHeight = $derived(parseSize(output.minHeight));
	const contentMaxHeight = $derived(parseSize(output.maxHeight));
	const sizeInline = $derived(sizingStyle({ height: output.height, minHeight: output.minHeight, maxHeight: output.maxHeight, width: output.width }));

	const wrapperClass = $derived(chrome === 'card' ? 'rounded-xl p-4 space-y-3 h-full flex flex-col' : chrome === 'subtle' ? 'py-3 space-y-3 border-b' : 'space-y-2 h-full flex flex-col');
	const wrapperStyle = $derived(chrome === 'card' ? 'background: var(--runner-card-bg); border: var(--runner-card-border)' : chrome === 'subtle' ? 'border-color: var(--runner-card-border)' : '');

	const placeholder = $derived(output.placeholder ?? 'Your result will appear here after running.');

	// Copy button state. Text-ish variants (markdown, code, json, default)
	// show a copy button in the top right of the output panel. The button
	// flips to a checkmark for 1.5 seconds after a successful copy.
	const COPYABLE_VARIANTS = new Set(['markdown', 'code', 'json', undefined]);
	const isCopyable = $derived(
		hasValue &&
		COPYABLE_VARIANTS.has(variant as string | undefined) &&
		typeof val === 'string',
	);
	let copied = $state(false);
	async function copyToClipboard() {
		if (typeof val !== 'string') return;
		try {
			await navigator.clipboard.writeText(formatValue(val));
			copied = true;
			setTimeout(() => { copied = false; }, 1500);
		} catch {
			// Clipboard API unavailable (http, sandboxed iframe, etc.).
			// Silently ignore; user can still select text manually.
		}
	}
</script>

<div class={wrapperClass} style={wrapperStyle}>
	<div class="flex items-start justify-between gap-2">
		<div>
			<span class="text-[11px] font-semibold uppercase tracking-wider" style="color: var(--runner-muted)">{label}</span>
			{#if output.description}
				<div class="text-xs mt-0.5 runner-markdown" style="color: var(--runner-muted)">{@html renderMarkdown(output.description)}</div>
			{/if}
		</div>
	</div>

	<div
		class="relative flex-1 flex flex-col overflow-hidden rounded-xl"
		style="background: var(--runner-input-bg); border: 1px solid var(--runner-input-border); {contentHeight ? `height: ${contentHeight};` : ''} {contentMinHeight ? `min-height: ${contentMinHeight};` : (!contentHeight ? 'min-height: 180px;' : '')} {contentMaxHeight ? `max-height: ${contentMaxHeight};` : ''} {sizeInline}"
	>
		{#if isCopyable}
			<button
				type="button"
				class="absolute top-2 right-2 z-10 inline-flex items-center gap-1.5 text-[11px] font-medium px-2 py-1 rounded-md transition-all opacity-60 hover:opacity-100"
				style="background: var(--runner-card-bg); border: 1px solid var(--runner-card-border); color: var(--runner-muted); backdrop-filter: blur(8px)"
				onclick={copyToClipboard}
				title="Copy to clipboard"
			>
				{#if copied}
					<Check class="w-3 h-3" />
					Copied
				{:else}
					<Copy class="w-3 h-3" />
					Copy
				{/if}
			</button>
		{/if}
		{#if hasValue}
			<div class="overflow-auto flex-1 p-4">
				{#if variant === 'image' && typeof val === 'string'}
					<img src={val} alt={label} class="w-full h-full object-contain" />
				{:else if variant === 'audio' && typeof val === 'string'}
					<audio src={val} controls class="w-full"></audio>
				{:else if variant === 'video' && typeof val === 'string'}
					<!-- svelte-ignore a11y_media_has_caption -->
					<video src={val} controls class="w-full h-full object-contain"></video>
				{:else if variant === 'download' && typeof val === 'string'}
					<a href={val} download class="inline-flex items-center gap-2 text-sm font-medium hover:underline" style="color: var(--runner-primary)">
						↓ Download result
					</a>
				{:else if variant === 'code' || variant === 'json'}
					<pre class="text-xs font-mono whitespace-pre-wrap" style="color: var(--runner-fg); overflow-wrap: anywhere; word-break: break-word">{formatValue(val)}</pre>
				{:else if variant === 'progress' && typeof val === 'number'}
					<div class="w-full h-2 rounded-full overflow-hidden" style="background: color-mix(in srgb, var(--runner-fg) 10%, transparent)">
						<div class="h-full transition-all" style="width: {Math.round(val * 100)}%; background: linear-gradient(to right, var(--runner-primary), var(--runner-accent))"></div>
					</div>
				{:else}
					<div
						class="runner-markdown text-sm leading-relaxed"
						style="color: var(--runner-fg)"
					>{@html renderMarkdown(formatValue(val))}</div>
				{/if}
			</div>
		{:else if executionState.isRunning}
			<!-- Loading state: skeleton shimmer + label -->
			<div class="flex-1 flex flex-col gap-3 p-4">
				<div class="h-3 rounded w-3/4 animate-pulse" style="background: color-mix(in srgb, var(--runner-fg) 8%, transparent)"></div>
				<div class="h-3 rounded w-full animate-pulse" style="background: color-mix(in srgb, var(--runner-fg) 8%, transparent)"></div>
				<div class="h-3 rounded w-5/6 animate-pulse" style="background: color-mix(in srgb, var(--runner-fg) 8%, transparent)"></div>
				<div class="h-3 rounded w-2/3 animate-pulse" style="background: color-mix(in srgb, var(--runner-fg) 8%, transparent)"></div>
				<div class="mt-auto flex items-center gap-2 text-xs" style="color: var(--runner-muted)">
					<span class="inline-block w-3 h-3 border-2 rounded-full animate-spin" style="border-color: var(--runner-primary); border-top-color: transparent"></span>
					Running…
				</div>
			</div>
		{:else}
			<div class="flex-1 flex items-center justify-center p-6">
				<p class="text-sm italic text-center" style="color: var(--runner-muted)">{placeholder}</p>
			</div>
		{/if}
	</div>
</div>
