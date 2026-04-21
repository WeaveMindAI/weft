<script lang="ts">
	import * as Dialog from "$lib/components/ui/dialog";
	import { Button } from "$lib/components/ui/button";
	import type { ReconciliationPlan } from "$lib/utils/infra-reconciliation";
	import { NODE_TYPE_CONFIG } from "$lib/nodes";

	export interface ReconciliationResult {
		restoreNodeIds: string[];
		removeNodeIds: string[];
		hasRemainingChanges: boolean;
	}

	let { open = $bindable(false), plan, onConfirm, onCancel }: {
		open: boolean;
		plan: ReconciliationPlan | null;
		onConfirm: (result: ReconciliationResult) => void;
		onCancel: () => void;
	} = $props();

	// Track user overrides: nodeId -> 'restore' (for terminate entries) or 'remove' (for provision entries)
	let overrides = $state<Map<string, 'restore' | 'remove'>>(new Map());

	// Reset overrides when plan changes
	$effect(() => {
		if (plan) {
			overrides = new Map();
		}
	});

	function toggleRestore(nodeId: string) {
		const next = new Map(overrides);
		if (next.has(nodeId)) {
			next.delete(nodeId);
		} else {
			next.set(nodeId, 'restore');
		}
		overrides = next;
	}

	function toggleRemove(nodeId: string) {
		const next = new Map(overrides);
		if (next.has(nodeId)) {
			next.delete(nodeId);
		} else {
			next.set(nodeId, 'remove');
		}
		overrides = next;
	}

	function getNodeLabel(nodeType: string): string {
		return NODE_TYPE_CONFIG[nodeType]?.label || nodeType;
	}

	// Effective entries after user overrides
	const effectiveTerminate = $derived(
		(plan?.entries.filter(e => e.action === 'terminate') ?? [])
			.filter(e => !overrides.has(e.nodeId))
	);
	const effectiveRestore = $derived(
		(plan?.entries.filter(e => e.action === 'terminate') ?? [])
			.filter(e => overrides.get(e.nodeId) === 'restore')
	);
	const effectiveProvision = $derived(
		(plan?.entries.filter(e => e.action === 'provision') ?? [])
			.filter(e => !overrides.has(e.nodeId))
	);
	const effectiveRemoved = $derived(
		(plan?.entries.filter(e => e.action === 'provision') ?? [])
			.filter(e => overrides.get(e.nodeId) === 'remove')
	);
	const restartEntries = $derived(plan?.entries.filter(e => e.action === 'restart') ?? []);
	const keepEntries = $derived(plan?.entries.filter(e => e.action === 'keep') ?? []);

	const hasDestructive = $derived(effectiveTerminate.length > 0 || restartEntries.length > 0);
	const hasAnyChanges = $derived(
		effectiveTerminate.length > 0 || effectiveProvision.length > 0 || restartEntries.length > 0
	);
	const hasOverrides = $derived(overrides.size > 0);

	// What the Apply button does depends on the state
	const applyLabel = $derived.by(() => {
		if (hasOverrides) return 'Apply Graph Changes';
		if (hasDestructive) return 'Apply Changes';
		return 'Start Infrastructure';
	});

	function handleConfirm() {
		onConfirm({
			restoreNodeIds: effectiveRestore.map(e => e.nodeId),
			removeNodeIds: effectiveRemoved.map(e => e.nodeId),
			hasRemainingChanges: hasAnyChanges,
		});
	}
</script>

<Dialog.Root bind:open>
	<Dialog.Content class="sm:max-w-md">
		<Dialog.Header>
			<Dialog.Title>Infrastructure Changes</Dialog.Title>
			<Dialog.Description>
				Your project infrastructure has changed. Review the changes before applying.
			</Dialog.Description>
		</Dialog.Header>

		{#if plan}
			<div class="flex flex-col gap-3 py-2 max-h-[300px] overflow-y-auto">
				{#if plan.entries.filter(e => e.action === 'terminate').length > 0}
					<div class="space-y-1.5">
						<p class="text-xs font-semibold text-red-600 uppercase tracking-wide">
							Terminate ({effectiveTerminate.length}{effectiveRestore.length > 0 ? ` / ${effectiveTerminate.length + effectiveRestore.length}` : ''})
						</p>
						{#each plan.entries.filter(e => e.action === 'terminate') as entry}
							{@const isRestored = overrides.get(entry.nodeId) === 'restore'}
							<div class="flex items-center gap-2 px-3 py-2 rounded-md border transition-colors
								{isRestored ? 'bg-green-50 border-green-200' : 'bg-red-50 border-red-100'}
							">
								<span class="font-mono text-sm {isRestored ? 'text-green-500' : 'text-red-500'}">
									{isRestored ? '✓' : '✕'}
								</span>
								<div class="flex-1 min-w-0">
									<p class="text-sm font-medium truncate {isRestored ? 'text-green-800 line-through' : 'text-red-800'}">
										{getNodeLabel(entry.nodeType)}
									</p>
									<p class="text-xs {isRestored ? 'text-green-600' : 'text-red-600'}">
										{isRestored ? 'Will be restored to project' : entry.reason}
									</p>
								</div>
								<button
									class="shrink-0 px-2 py-1 rounded text-xs font-medium transition-colors
										{isRestored ? 'bg-green-200 text-green-800 hover:bg-green-300' : 'bg-red-100 text-red-700 hover:bg-red-200'}
									"
									onclick={() => toggleRestore(entry.nodeId)}
								>
									{isRestored ? 'Undo' : 'Restore'}
								</button>
							</div>
						{/each}
					</div>
				{/if}

				{#if restartEntries.length > 0}
					<div class="space-y-1.5">
						<p class="text-xs font-semibold text-amber-600 uppercase tracking-wide">Restart ({restartEntries.length})</p>
						{#each restartEntries as entry}
							<div class="flex items-center gap-2 px-3 py-2 rounded-md bg-amber-50 border border-amber-100">
								<span class="text-amber-500 font-mono text-sm">↻</span>
								<div class="flex-1 min-w-0">
									<p class="text-sm font-medium text-amber-800 truncate">{getNodeLabel(entry.nodeType)}</p>
									<p class="text-xs text-amber-600">{entry.reason}</p>
								</div>
							</div>
						{/each}
					</div>
				{/if}

				{#if plan.entries.filter(e => e.action === 'provision').length > 0}
					<div class="space-y-1.5">
						<p class="text-xs font-semibold text-blue-600 uppercase tracking-wide">
							New ({effectiveProvision.length}{effectiveRemoved.length > 0 ? ` / ${effectiveProvision.length + effectiveRemoved.length}` : ''})
						</p>
						{#each plan.entries.filter(e => e.action === 'provision') as entry}
							{@const isRemoved = overrides.get(entry.nodeId) === 'remove'}
							<div class="flex items-center gap-2 px-3 py-2 rounded-md border transition-colors
								{isRemoved ? 'bg-zinc-50 border-zinc-200' : 'bg-blue-50 border-blue-100'}
							">
								<span class="font-mono text-sm {isRemoved ? 'text-zinc-400' : 'text-blue-500'}">
									{isRemoved ? '✕' : '+'}
								</span>
								<div class="flex-1 min-w-0">
									<p class="text-sm font-medium truncate {isRemoved ? 'text-zinc-500 line-through' : 'text-blue-800'}">
										{getNodeLabel(entry.nodeType)}
									</p>
									<p class="text-xs {isRemoved ? 'text-zinc-400' : 'text-blue-600'}">
										{isRemoved ? 'Will be removed from project' : entry.reason}
									</p>
								</div>
								<button
									class="shrink-0 px-2 py-1 rounded text-xs font-medium transition-colors
										{isRemoved ? 'bg-zinc-200 text-zinc-700 hover:bg-zinc-300' : 'bg-blue-100 text-blue-700 hover:bg-blue-200'}
									"
									onclick={() => toggleRemove(entry.nodeId)}
								>
									{isRemoved ? 'Undo' : 'Remove'}
								</button>
							</div>
						{/each}
					</div>
				{/if}

				{#if keepEntries.length > 0}
					<div class="space-y-1.5">
						<p class="text-xs font-semibold text-green-600 uppercase tracking-wide">Unchanged ({keepEntries.length})</p>
						{#each keepEntries as entry}
							<div class="flex items-center gap-2 px-3 py-2 rounded-md bg-green-50 border border-green-100">
								<span class="text-green-500 font-mono text-sm">✓</span>
								<div class="flex-1 min-w-0">
									<p class="text-sm font-medium text-green-800 truncate">{getNodeLabel(entry.nodeType)}</p>
								</div>
							</div>
						{/each}
					</div>
				{/if}
			</div>

			{#if hasDestructive}
				<div class="flex items-start gap-2 px-3 py-2 rounded-md bg-red-50 border border-red-200 text-xs text-red-700">
					<svg xmlns="http://www.w3.org/2000/svg" width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" class="shrink-0 mt-0.5"><path d="M10.29 3.86L1.82 18a2 2 0 0 0 1.71 3h16.94a2 2 0 0 0 1.71-3L13.71 3.86a2 2 0 0 0-3.42 0z"/><line x1="12" y1="9" x2="12" y2="13"/><line x1="12" y1="17" x2="12.01" y2="17"/></svg>
					<span>Terminated infrastructure will lose all its data. This cannot be undone.</span>
				</div>
			{/if}
		{/if}

		<Dialog.Footer>
			<Button variant="outline" onclick={onCancel}>Cancel</Button>
			<Button
				variant={hasDestructive && !hasOverrides ? "destructive" : "default"}
				onclick={handleConfirm}
			>
				{applyLabel}
			</Button>
		</Dialog.Footer>
	</Dialog.Content>
</Dialog.Root>
