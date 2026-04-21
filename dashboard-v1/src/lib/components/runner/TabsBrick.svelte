<script lang="ts">
	import type { Brick, Block, RunnerMode, ProjectDefinition, LiveDataItem } from '$lib/types';
	import BlockList from './BlockList.svelte';

	let {
		brick,
		mode,
		project,
		renderMarkdown,
		onUpdateNodeConfig,
		executionState,
		infraLiveData,
		infraStatus,
	}: {
		brick: Brick;
		mode: RunnerMode;
		project: ProjectDefinition;
		renderMarkdown: (s: string) => string;
		onUpdateNodeConfig: (nodeId: string, config: Record<string, unknown>) => void;
		executionState: { isRunning: boolean; nodeOutputs?: Record<string, unknown> };
		infraLiveData?: Record<string, LiveDataItem[]>;
		infraStatus: string;
	} = $props();

	const tabs = $derived(
		(brick.children ?? []).filter(
			(b): b is Extract<Block, { kind: 'brick' }> => b.kind === 'brick' && b.brick.kind === 'tab',
		),
	);
	const firstLabel = $derived(tabs.length > 0 ? ((tabs[0].brick.props.label as string) ?? 'Tab 1') : 'Tab 1');
	// svelte-ignore state_referenced_locally
	let active = $state(firstLabel);
	$effect(() => { active = firstLabel; });
</script>

{#if tabs.length > 0}
	<div class="space-y-4">
		<div class="flex gap-1 border-b border-border">
			{#each tabs as t (t.brick.id)}
				{@const label = (t.brick.props.label as string) ?? 'Tab'}
				<button
					type="button"
					class="px-4 py-2 text-sm font-medium border-b-2 transition-colors {active === label ? 'border-[color:var(--runner-primary,theme(colors.violet.600))] text-foreground' : 'border-transparent text-muted-foreground hover:text-foreground'}"
					onclick={() => { active = label; }}
				>{label}</button>
			{/each}
		</div>
		{#each tabs as t (t.brick.id)}
			{@const label = (t.brick.props.label as string) ?? 'Tab'}
			{#if label === active && t.brick.children}
				<div class="space-y-4">
					<BlockList blocks={t.brick.children} {mode} {project} {renderMarkdown} {onUpdateNodeConfig} {executionState} {infraLiveData} {infraStatus} />
				</div>
			{/if}
		{/each}
	</div>
{/if}
