<script lang="ts">
	import { untrack } from 'svelte';
	import JsonTree from './JsonTree.svelte';

	let { data, label, depth = 0, defaultExpanded = false }: {
		data: unknown;
		label?: string;
		depth?: number;
		defaultExpanded?: boolean;
	} = $props();

	let expanded = $state(untrack(() => defaultExpanded || depth < 1));

	const isObject = $derived(data !== null && typeof data === 'object' && !Array.isArray(data));
	const isArray = $derived(Array.isArray(data));
	const isExpandable = $derived(isObject || isArray);
	const entries = $derived(
		isObject ? Object.entries(data as Record<string, unknown>) :
		isArray ? (data as unknown[]).map((v, i) => [String(i), v] as [string, unknown]) :
		[]
	);
	const preview = $derived(
		isArray ? `[${(data as unknown[]).length}]` :
		isObject ? `{${Object.keys(data as Record<string, unknown>).length}}` :
		''
	);

	function formatValue(val: unknown): string {
		if (val === null) return 'null';
		if (typeof val === 'string') return val.length > 120 ? `"${val.slice(0, 120)}..."` : `"${val}"`;
		if (typeof val === 'boolean' || typeof val === 'number') return String(val);
		return String(val);
	}
</script>

{#if isExpandable}
	<!-- svelte-ignore a11y_no_static_element_interactions -->
	<!-- svelte-ignore a11y_click_events_have_key_events -->
	<div class="json-tree" style="padding-left: {depth > 0 ? 12 : 0}px;">
		<div
			class="flex items-center gap-1 cursor-pointer hover:bg-zinc-100 rounded px-1 py-0.5 -mx-1"
			onclick={() => expanded = !expanded}
		>
			<svg class="w-3 h-3 text-zinc-400 shrink-0 transition-transform {expanded ? 'rotate-90' : ''}" viewBox="0 0 16 16" fill="currentColor"><path d="M6.22 4.22a.75.75 0 0 1 1.06 0l3.25 3.25a.75.75 0 0 1 0 1.06l-3.25 3.25a.75.75 0 0 1-1.06-1.06L8.94 8 6.22 5.28a.75.75 0 0 1 0-1.06Z"/></svg>
			{#if label !== undefined}
				<span class="text-[11px] font-medium text-zinc-600">{label}</span>
			{/if}
			{#if !expanded}
				<span class="text-[10px] text-zinc-400 font-mono">{preview}</span>
			{/if}
		</div>
		{#if expanded}
			{#each entries as [key, value]}
				{@const childExpandable = (value !== null && typeof value === 'object')}
				{#if childExpandable}
					<JsonTree data={value} label={key} depth={depth + 1} />
				{:else}
					<div class="flex items-start gap-1 py-0.5" style="padding-left: {(depth + 1) * 12}px;">
						<span class="text-[11px] font-medium text-zinc-500 shrink-0">{key}:</span>
						<span class="text-[11px] font-mono {value === null ? 'text-zinc-400 italic' : typeof value === 'string' ? 'text-green-700' : typeof value === 'number' ? 'text-blue-700' : typeof value === 'boolean' ? 'text-amber-700' : 'text-zinc-700'} break-all">{formatValue(value)}</span>
					</div>
				{/if}
			{/each}
		{/if}
	</div>
{:else}
	<div class="flex items-start gap-1 py-0.5" style="padding-left: {depth * 12}px;">
		{#if label !== undefined}
			<span class="text-[11px] font-medium text-zinc-500 shrink-0">{label}:</span>
		{/if}
		<span class="text-[11px] font-mono {data === null ? 'text-zinc-400 italic' : typeof data === 'string' ? 'text-green-700' : typeof data === 'number' ? 'text-blue-700' : typeof data === 'boolean' ? 'text-amber-700' : 'text-zinc-700'} break-all">{formatValue(data)}</span>
	</div>
{/if}
