<script lang="ts">
	import type { SetupPhase, RunnerMode, ProjectDefinition, LiveDataItem } from '$lib/types';
	import { visibleInMode, effectiveItemVisibility } from './visibility';
	import FieldItem from './FieldItem.svelte';
	import LiveBlock from './LiveBlock.svelte';
	import BlockList from './BlockList.svelte';

	let {
		phase,
		mode,
		project,
		renderMarkdown,
		onUpdateNodeConfig,
		executionState,
		infraLiveData,
		infraStatus,
	}: {
		phase: SetupPhase;
		mode: RunnerMode;
		project: ProjectDefinition;
		renderMarkdown: (s: string) => string;
		onUpdateNodeConfig: (nodeId: string, config: Record<string, unknown>) => void;
		executionState: { isRunning: boolean; nodeOutputs?: Record<string, unknown> };
		infraLiveData?: Record<string, LiveDataItem[]>;
		infraStatus: string;
	} = $props();

	const visibleItems = $derived(phase.items.filter(it => visibleInMode(effectiveItemVisibility(it, project), mode)));
	const visibleLive = $derived((phase.liveItems ?? []).filter(l => visibleInMode(l.visibility, mode)));
	const children = $derived(phase.children ?? []);
	const empty = $derived(visibleItems.length === 0 && visibleLive.length === 0 && children.length === 0);
</script>

{#if !empty}
	<div class="space-y-5">
		{#if phase.title}
			<div>
				<h2 class="text-lg font-semibold tracking-tight" style="color: var(--runner-fg)">{phase.title}</h2>
				{#if phase.description}
					<p class="text-sm mt-1 runner-markdown" style="color: var(--runner-muted)">{@html renderMarkdown(phase.description)}</p>
				{/if}
			</div>
		{/if}
		{#if visibleItems.length > 0 || visibleLive.length > 0}
			<div class="space-y-5">
				{#each visibleItems as item (item.id)}
					<FieldItem {item} {project} {renderMarkdown} {onUpdateNodeConfig} />
				{/each}
				{#each visibleLive as live (live.id)}
					<LiveBlock {live} {project} {infraLiveData} {infraStatus} {renderMarkdown} />
				{/each}
			</div>
		{/if}
		{#if children.length > 0}
			<BlockList
				blocks={children}
				{mode}
				{project}
				{renderMarkdown}
				{onUpdateNodeConfig}
				{executionState}
				{infraLiveData}
				{infraStatus}
			/>
		{/if}
	</div>
{/if}
