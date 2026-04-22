<script lang="ts">
	import type { LiveItem, ProjectDefinition, LiveDataItem } from '$lib/types';
	import { NODE_TYPE_CONFIG } from '$lib/nodes';

	let {
		live,
		project,
		infraLiveData,
		infraStatus,
		renderMarkdown,
	}: {
		live: LiveItem;
		project: ProjectDefinition;
		infraLiveData?: Record<string, LiveDataItem[]>;
		infraStatus: string;
		renderMarkdown: (s: string) => string;
	} = $props();

	function nodeLabel(): string {
		const node = project.nodes.find(n => n.id === live.nodeId);
		if (!node) return live.nodeId;
		const template = NODE_TYPE_CONFIG[node.nodeType];
		return node.label ?? template?.label ?? node.nodeType;
	}

	const dataItems = $derived(infraLiveData?.[live.nodeId] ?? []);
	const label = $derived(live.label ?? nodeLabel());
</script>

<div class="rounded-lg border border-emerald-200 bg-emerald-50/30 p-4 space-y-2">
	<div>
		<span class="text-sm font-medium">{label}</span>
		{#if live.description}
			<div class="text-xs text-muted-foreground mt-0.5 runner-markdown">{@html renderMarkdown(live.description)}</div>
		{/if}
	</div>
	{#if dataItems.length > 0}
		<div class="space-y-2">
			{#each dataItems as di}
				{#if di.type === 'image' && typeof di.data === 'string'}
					<div>
						<span class="text-xs text-muted-foreground font-medium">{di.label}</span>
						<img src={di.data} alt={di.label} class="w-full rounded border border-zinc-200 mt-1" />
					</div>
				{:else if di.type === 'text'}
					<div class="flex items-center justify-between">
						<span class="text-xs text-muted-foreground font-medium">{di.label}</span>
						<span class="text-xs text-foreground font-mono">{di.data}</span>
					</div>
				{:else if di.type === 'progress' && typeof di.data === 'number'}
					<div>
						<span class="text-xs text-muted-foreground font-medium">{di.label}</span>
						<div class="w-full h-1.5 bg-zinc-200 rounded-full mt-1 overflow-hidden">
							<div class="h-full bg-emerald-500 rounded-full transition-all" style="width: {Math.round(di.data * 100)}%"></div>
						</div>
					</div>
				{/if}
			{/each}
		</div>
	{:else}
		<div class="text-xs text-muted-foreground bg-muted rounded p-3 italic">
			{infraStatus === 'running' ? 'Waiting for data...' : 'Start infrastructure to see live data'}
		</div>
	{/if}
</div>
