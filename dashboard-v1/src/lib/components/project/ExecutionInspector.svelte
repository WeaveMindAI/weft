<script lang="ts">
	import { Search } from '@lucide/svelte';
	import type { NodeExecution } from '$lib/types';
	import { getStatusIcon } from '$lib/utils/status';
	import JsonTree from './JsonTree.svelte';
	import CopyButton from '$lib/components/ui/CopyButton.svelte';
	import * as Dialog from '$lib/components/ui/dialog';

	let {
		executions = [],
		label = 'Node',
	}: {
		executions: NodeExecution[];
		label: string;
	} = $props();

	let selectedIndex = $state(0);
	let open = $state(false);

	const count = $derived(executions.length);
	$effect(() => {
		if (count > 0) selectedIndex = count - 1;
	});
	const selected = $derived(executions[selectedIndex]);

	function formatDuration(startMs: number, endMs?: number): string {
		if (!endMs) return 'running...';
		const ms = endMs - startMs;
		if (ms < 1000) return `${ms}ms`;
		if (ms < 60000) return `${(ms / 1000).toFixed(1)}s`;
		return `${Math.floor(ms / 60000)}m ${Math.round((ms % 60000) / 1000)}s`;
	}

	function formatCost(usd: number): string {
		if (usd === 0) return '$0';
		if (usd < 0.001) return `$${usd.toFixed(6)}`;
		if (usd < 0.01) return `$${usd.toFixed(4)}`;
		return `$${usd.toFixed(2)}`;
	}
</script>

<!-- Inline: navigator + inspect button. Render inside the node header. -->
{#if count > 1}
	{@const selExec = executions[selectedIndex]}
	{@const statusColor = selExec?.status === 'failed' ? 'text-red-500' : selExec?.status === 'completed' ? 'text-green-600' : selExec?.status === 'running' ? 'text-blue-500' : 'text-muted-foreground'}
	<div class="inline-flex items-center gap-0.5 ml-1.5 text-[9px] select-none {statusColor}">
		<button class="px-0.5 hover:text-foreground disabled:opacity-30 transition-colors" disabled={selectedIndex === 0} onclick={(e) => { e.stopPropagation(); selectedIndex = Math.max(0, selectedIndex - 1); }}>‹</button>
		<span class="font-mono tabular-nums">{selectedIndex + 1}/{count}</span>
		<button class="px-0.5 hover:text-foreground disabled:opacity-30 transition-colors" disabled={selectedIndex >= count - 1} onclick={(e) => { e.stopPropagation(); selectedIndex = Math.min(count - 1, selectedIndex + 1); }}>›</button>
	</div>
{/if}
{#if count > 0}
	<button
		class="w-5 h-5 flex items-center justify-center rounded hover:bg-black/5 cursor-pointer transition-colors text-zinc-400 nodrag"
		onclick={(e) => { e.stopPropagation(); open = true; }}
		title="Inspect execution"
	>
		<Search class="w-3 h-3" />
	</button>
{/if}

<!-- Modal -->
{#if selected}
{@const inputJson = selected.input ? JSON.stringify(selected.input, null, 2) : null}
{@const outputJson = selected.output ? JSON.stringify(selected.output, null, 2) : null}
{@const detailsText = selected.error ?? (selected.status === 'completed' ? 'Completed successfully' : selected.status)}
{@const fullCopyText = [
	`--- Input ---`, inputJson ?? '(none)', ``,
	`--- Details ---`, detailsText, ``,
	`--- Output ---`, outputJson ?? '(none)', ``,
	`Status: ${selected.status} | Duration: ${formatDuration(selected.startedAt, selected.completedAt)}${selected.costUsd > 0 ? ` | Cost: ${formatCost(selected.costUsd)}` : ''} | ${new Date(selected.startedAt).toLocaleString()} | ${selected.id}`,
].join('\n')}
<Dialog.Root bind:open>
	<Dialog.Content class="sm:max-w-[92vw] max-h-[85vh] overflow-hidden p-0 gap-0 [&>button:last-child]:hidden nodrag nopan">
		<div class="flex items-center justify-between px-4 py-2.5 border-b border-zinc-200 shrink-0">
			<div class="flex items-center gap-3">
				<span class="{selected.status === 'failed' ? 'text-red-600' : selected.status === 'completed' ? 'text-green-600' : 'text-zinc-500'}">{getStatusIcon(selected.status)}</span>
				<span class="text-sm font-semibold text-zinc-800">{label}</span>
				{#if count > 1}
					<div class="flex items-center gap-1 text-xs text-zinc-500">
						<button class="px-1 hover:text-zinc-800 disabled:opacity-30" disabled={selectedIndex === 0} onclick={() => { selectedIndex = Math.max(0, selectedIndex - 1); }}>‹</button>
						<span class="font-mono tabular-nums">{selectedIndex + 1}/{count}</span>
						<button class="px-1 hover:text-zinc-800 disabled:opacity-30" disabled={selectedIndex >= count - 1} onclick={() => { selectedIndex = Math.min(count - 1, selectedIndex + 1); }}>›</button>
					</div>
				{/if}
			</div>
			<div class="flex items-center gap-2">
				<CopyButton text={fullCopyText} />
				<button
					class="w-6 h-6 flex items-center justify-center rounded hover:bg-zinc-100 text-zinc-400 hover:text-zinc-700 transition-colors"
					onclick={() => open = false}
				>✕</button>
			</div>
		</div>

		<div class="grid grid-cols-3 min-h-0 overflow-hidden" style="height: calc(85vh - 80px);">
			<div class="flex flex-col min-h-0 border-r border-zinc-200">
				<div class="flex items-center justify-between px-3 py-1.5 bg-zinc-50 border-b border-zinc-200 shrink-0">
					<span class="text-[10px] font-medium text-zinc-400 uppercase tracking-wider">Input</span>
					{#if inputJson}
						<CopyButton text={inputJson} />
					{/if}
				</div>
				<div class="overflow-auto flex-1 p-2">
					{#if selected.input && typeof selected.input === 'object' && Object.keys(selected.input as Record<string, unknown>).length > 0}
						{#each Object.entries(selected.input as Record<string, unknown>) as [key, value]}
							<JsonTree data={value} label={key} defaultExpanded={true} />
						{/each}
					{:else}
						<div class="p-1 text-xs text-zinc-400 italic">No input data</div>
					{/if}
				</div>
			</div>

			<div class="flex flex-col min-h-0 border-r border-zinc-200">
				<div class="flex items-center justify-between px-3 py-1.5 bg-zinc-50 border-b border-zinc-200 shrink-0">
					<span class="text-[10px] font-medium text-zinc-400 uppercase tracking-wider">Details</span>
					<CopyButton text={detailsText} />
				</div>
				<div class="overflow-auto flex-1 p-3 space-y-3">
					{#if selected.error}
						<div class="rounded border border-red-200 bg-red-50 p-2.5">
							<div class="text-[10px] font-semibold text-red-700 mb-1">Error</div>
							<pre class="text-[11px] text-red-600 whitespace-pre-wrap break-words font-mono">{selected.error}</pre>
						</div>
					{:else if selected.status === 'completed'}
						<div class="text-[11px] text-green-600">Completed successfully</div>
					{:else if selected.status === 'running' || selected.status === 'waiting_for_input'}
						<div class="text-[11px] text-blue-600 animate-pulse">
							{selected.status === 'waiting_for_input' ? 'Waiting for input...' : 'Running...'}
						</div>
					{:else if selected.status === 'skipped'}
						<div class="text-[11px] text-zinc-500">Skipped (null input on required port)</div>
					{/if}
				</div>
			</div>

			<div class="flex flex-col min-h-0">
				<div class="flex items-center justify-between px-3 py-1.5 bg-zinc-50 border-b border-zinc-200 shrink-0">
					<span class="text-[10px] font-medium text-zinc-400 uppercase tracking-wider">Output</span>
					{#if outputJson}
						<CopyButton text={outputJson} />
					{/if}
				</div>
				<div class="overflow-auto flex-1 p-2">
					{#if selected.output && typeof selected.output === 'object' && Object.keys(selected.output as Record<string, unknown>).length > 0}
						{#each Object.entries(selected.output as Record<string, unknown>) as [key, value]}
							<JsonTree data={value} label={key} defaultExpanded={true} />
						{/each}
					{:else if selected.output !== null && selected.output !== undefined}
						<div class="p-1 text-[11px] font-mono text-zinc-700">{JSON.stringify(selected.output)}</div>
					{:else}
						<div class="p-1 text-xs text-zinc-400 italic">No output</div>
					{/if}
				</div>
			</div>
		</div>

		<div class="flex items-center gap-4 px-4 py-1.5 border-t border-zinc-200 bg-zinc-50 text-[10px] text-zinc-500 shrink-0">
			<span class="font-medium {selected.status === 'failed' ? 'text-red-600' : selected.status === 'completed' ? 'text-green-600' : ''}">{selected.status}</span>
			<span class="font-mono">{formatDuration(selected.startedAt, selected.completedAt)}</span>
			{#if selected.costUsd > 0}
				<span class="font-mono">{formatCost(selected.costUsd)}</span>
			{/if}
			<span>{new Date(selected.startedAt).toLocaleString()}</span>
		</div>
	</Dialog.Content>
</Dialog.Root>
{/if}
