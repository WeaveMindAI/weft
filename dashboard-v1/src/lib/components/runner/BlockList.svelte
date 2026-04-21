<script lang="ts">
	import type { Block, RunnerMode, SetupItem, OutputItem, LiveItem, LiveDataItem, ProjectDefinition } from '$lib/types';
	import BrickRenderer from './BrickRenderer.svelte';
	import PhaseBlock from './PhaseBlock.svelte';
	import OutputBlock from './OutputBlock.svelte';
	import LiveBlock from './LiveBlock.svelte';
	import { visibleInMode } from './visibility';

	let {
		blocks,
		mode,
		project,
		renderMarkdown,
		onUpdateNodeConfig,
		executionState,
		infraLiveData,
		infraStatus,
	}: {
		blocks: Block[];
		mode: RunnerMode;
		project: ProjectDefinition;
		renderMarkdown: (s: string) => string;
		onUpdateNodeConfig: (nodeId: string, config: Record<string, unknown>) => void;
		executionState: { isRunning: boolean; nodeOutputs?: Record<string, unknown> };
		infraLiveData?: Record<string, LiveDataItem[]>;
		infraStatus: string;
	} = $props();

	function blockKey(b: Block): string {
		if (b.kind === 'phase') return 'phase:' + b.phase.id;
		if (b.kind === 'output') return 'output:' + b.output.id;
		if (b.kind === 'live') return 'live:' + b.live.id;
		return 'brick:' + b.brick.id;
	}
</script>

{#each blocks as block (blockKey(block))}
	{#if block.kind === 'brick'}
		{#if visibleInMode(block.brick.visibility, mode)}
			<BrickRenderer brick={block.brick} {mode} {project} {renderMarkdown} {onUpdateNodeConfig} {executionState} {infraLiveData} {infraStatus} />
		{/if}
	{:else if block.kind === 'phase'}
		{#if visibleInMode(block.phase.visibility, mode)}
			<PhaseBlock phase={block.phase} {mode} {project} {renderMarkdown} {onUpdateNodeConfig} {executionState} {infraLiveData} {infraStatus} />
		{/if}
	{:else if block.kind === 'output'}
		{#if visibleInMode(block.output.visibility, mode)}
			<OutputBlock output={block.output} {project} {executionState} {renderMarkdown} />
		{/if}
	{:else if block.kind === 'live'}
		{#if visibleInMode(block.live.visibility, mode)}
			<LiveBlock live={block.live} {project} {infraLiveData} {infraStatus} {renderMarkdown} />
		{/if}
	{/if}
{/each}
