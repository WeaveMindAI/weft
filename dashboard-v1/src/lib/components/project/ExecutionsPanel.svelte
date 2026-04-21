<script lang="ts">
	import { onMount, onDestroy } from 'svelte';
	import { goto } from '$app/navigation';
	import { api, authFetch } from '$lib/config';
	import { fetchExecutionCost as _fetchExecutionCost, formatCost } from '$lib/utils/cost';
	import { getStatusIcon, displayStatus } from '$lib/utils/status';
	import { NODE_TYPE_CONFIG } from '$lib/nodes';
	import CopyButton from '$lib/components/ui/CopyButton.svelte';
	import { formatTimeAgo, formatDate, getStatusStyle, cleanOutput } from '$lib/utils/status'; 

	let { projectId, projectNodes }: {
		projectId: string;
		projectNodes?: Array<{ id: string; label?: string; nodeType?: string }>;
	} = $props();

	interface Execution {
		id: string;
		projectId: string;
		userId: string;
		triggerId: string | null;
		nodeType: string | null;
		status: 'pending' | 'running' | 'completed' | 'failed' | 'waiting_for_input' | 'paused' | 'cancelled';
		nodeStatuses: Record<string, unknown>;
		nodeOutputs: Record<string, unknown>;
		error: string | null;
		startedAt: string;
		completedAt: string | null;
	}

	let executions = $state<Execution[]>([]);
	let loading = $state(true);
	let error = $state<string | null>(null);
	let selectedExecutionId = $state<string | null>(null);
	let refreshInterval: ReturnType<typeof setInterval> | null = null;
	let cancelling = $state(false);
	let liveStatuses = $state<Record<string, string>>({});
	let liveOutputs = $state<Record<string, unknown>>({});
	let liveOrdering = $state<Record<string, number>>({});
	let loadingLive = $state(false);

	let expandedOutputs = $state<Set<string>>(new Set());
	let executionCosts = $state<Record<string, number>>({});

	async function fetchExecutionCost(executionId: string) {
		const cost = await _fetchExecutionCost(executionId);
		if (cost !== null) {
			executionCosts = { ...executionCosts, [executionId]: cost };
		}
	}

	let selectedExecution = $derived(executions.find(e => e.id === selectedExecutionId) ?? null);
	let effectiveStatuses = $derived(Object.keys(liveStatuses).length > 0 ? liveStatuses : (selectedExecution?.nodeStatuses ?? {}));
	let effectiveOutputs = $derived(Object.keys(liveOutputs).length > 0 ? liveOutputs : (selectedExecution?.nodeOutputs ?? {}));

	function sortByOrdering<T>(entries: [string, T][]): [string, T][] {
		return entries.sort((a, b) => {
			const ta = liveOrdering[a[0]];
			const tb = liveOrdering[b[0]];
			if (ta !== undefined && tb !== undefined) return ta - tb;
			if (ta !== undefined) return -1;
			if (tb !== undefined) return 1;
			return 0;
		});
	}

	function getTriggerLabel(execution: Execution): string {
		if (!execution.nodeType) return 'Manual';
		const nodeConfig = NODE_TYPE_CONFIG[execution.nodeType];
		return nodeConfig?.label || execution.nodeType;
	}

	function getNodeDisplayName(nodeId: string): string {
		if (projectNodes) {
			const node = projectNodes.find(n => n.id === nodeId);
			if (node) {
				if (node.label) return node.label;
				if (node.nodeType) {
					const config = NODE_TYPE_CONFIG[node.nodeType];
					return config?.label || node.nodeType;
				}
			}
		}
		if (nodeId.length > 20) return nodeId.slice(0, 8) + '...';
		return nodeId;
	}

	function previewOutput(output: unknown): string {
		const cleaned = cleanOutput(output);
		const str = JSON.stringify(cleaned, null, 2);
		if (str.length <= 120) return str;
		return str.slice(0, 120) + '\n...';
	}

	function toggleOutput(nodeId: string) {
		const next = new Set(expandedOutputs);
		if (next.has(nodeId)) next.delete(nodeId);
		else next.add(nodeId);
		expandedOutputs = next;
	}

	async function fetchLiveState(executionId: string) {
		loadingLive = true;
		try {
			const [statusRes, outputRes] = await Promise.all([
				authFetch(api.getNodeStatuses(executionId)),
				authFetch(api.getAllOutputs(executionId)),
			]);
			if (statusRes.ok) {
				const data = await statusRes.json();
				liveStatuses = data.statuses || {};
				if (data.ordering) liveOrdering = data.ordering;
			}
			if (outputRes.ok) {
				const data = await outputRes.json();
				liveOutputs = data.outputs || {};
			}
		} catch (e) {
			console.error('Failed to fetch live state:', e);
		} finally {
			loadingLive = false;
		}
	}

	async function loadExecutions() {
		try {
			const response = await authFetch(`/api/executions?projectId=${projectId}&limit=50`);
			if (!response.ok) throw new Error('Failed to load executions');
			executions = await response.json();
		} catch (e) {
			error = e instanceof Error ? e.message : 'Unknown error';
		} finally {
			loading = false;
		}
	}

	async function cancelExecution(executionId: string) {
		cancelling = true;
		try {
			const savedStatuses = { ...liveStatuses };
			const savedOutputs = { ...liveOutputs };

			const res = await authFetch(api.cancelExecution(executionId), { method: 'POST' });
			if (res.ok) {
				await fetchLiveState(executionId);
				if (Object.keys(liveStatuses).length === 0) liveStatuses = savedStatuses;
				if (Object.keys(liveOutputs).length === 0) liveOutputs = savedOutputs;
				await loadExecutions();
			}
		} catch (e) {
			console.error('Error cancelling execution:', e);
		} finally {
			cancelling = false;
		}
	}

	function buildExecutionText(exec: Execution) {
		const useStatuses = Object.keys(liveStatuses).length > 0 ? liveStatuses : exec.nodeStatuses;
		const useOutputs = Object.keys(liveOutputs).length > 0 ? liveOutputs : exec.nodeOutputs;
		const isLive = Object.keys(liveStatuses).length > 0;

		const lines: string[] = [];
		lines.push(`# Execution${isLive ? ' (live state)' : ''}`);
		lines.push(`Status: ${displayStatus(exec.status)}`);
		lines.push(`ID: ${exec.id}`);
		lines.push(`Started: ${formatDate(exec.startedAt)}`);
		if (exec.completedAt) lines.push(`Completed: ${formatDate(exec.completedAt)}`);
		if (exec.error) lines.push(`\nError: ${exec.error}`);

		const statuses = Object.entries(useStatuses);
		if (statuses.length > 0) {
			lines.push('');
			lines.push('## Node Statuses');
			for (const [nodeId, status] of statuses) {
				const baseStatus = String(status).split(' ')[0];
				const nodeOut = useOutputs[nodeId] as Record<string, unknown> | undefined;
				const nodeErr = nodeOut?._error as string | undefined;
				let line = `- ${getNodeDisplayName(nodeId)}: ${String(status)}`;
				if (baseStatus === 'failed' && nodeErr) line += `\n  Error: ${nodeErr}`;
				lines.push(line);
			}
		}

		const outputs = Object.entries(useOutputs);
		if (outputs.length > 0) {
			lines.push('');
			lines.push('## Node Outputs');
			for (const [nodeId, output] of outputs) {
				lines.push(`\n### ${getNodeDisplayName(nodeId)}`);
				lines.push('```json');
				lines.push(JSON.stringify(cleanOutput(output), null, 2));
				lines.push('```');
			}
		}

		return lines.join('\n');
	}

	function selectExecution(exec: Execution) {
		if (selectedExecutionId === exec.id) {
			selectedExecutionId = null;
			liveStatuses = {};
			liveOutputs = {};
			liveOrdering = {};
		} else {
			selectedExecutionId = exec.id;
			liveStatuses = {};
			liveOutputs = {};
			liveOrdering = {};
			expandedOutputs = new Set();
			fetchLiveState(exec.id);
			fetchExecutionCost(exec.id);
		}
	}

	onMount(() => {
		loadExecutions();
		refreshInterval = setInterval(loadExecutions, 10000);
	});

	onDestroy(() => {
		if (refreshInterval) clearInterval(refreshInterval);
	});
</script>

<div class="flex flex-col h-full overflow-hidden">
	<!-- Refresh button bar -->
	<div class="h-8 border-b border-zinc-200 flex items-center justify-end px-2 shrink-0 bg-[#f3f4f6]">
		<button
			class="flex items-center justify-center w-6 h-6 rounded hover:bg-zinc-200 transition-colors text-zinc-400 hover:text-zinc-600"
			onclick={() => loadExecutions()}
			title="Refresh executions"
		>
			<svg xmlns="http://www.w3.org/2000/svg" width="12" height="12" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2.5" stroke-linecap="round" stroke-linejoin="round">
				<polyline points="23 4 23 10 17 10"/><polyline points="1 20 1 14 7 14"/><path d="M3.51 9a9 9 0 0 1 14.85-3.36L23 10M1 14l4.64 4.36A9 9 0 0 0 20.49 15"/>
			</svg>
		</button>
	</div>

	<!-- Execution list -->
	<div class="flex-1 overflow-y-auto">
		{#if loading}
			<div class="flex items-center justify-center h-32">
				<div class="h-5 w-5 border-2 border-zinc-300 border-t-transparent rounded-full animate-spin"></div>
			</div>
		{:else if error}
			<div class="px-3 py-4 text-xs text-red-600">{error}</div>
		{:else if executions.length === 0}
			<div class="flex flex-col items-center justify-center h-full gap-2 px-4 text-center">
				<svg xmlns="http://www.w3.org/2000/svg" width="28" height="28" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.5" class="text-zinc-300">
					<polygon points="5 3 19 12 5 21 5 3"/>
				</svg>
				<p class="text-xs text-zinc-400">No executions yet for this project.</p>
			</div>
		{:else}
			{#each executions as exec (exec.id)}
				{@const style = getStatusStyle(exec.status)}
				{@const isSelected = selectedExecutionId === exec.id}
				<div class="border-b border-zinc-100 last:border-b-0">
					<button
						class="w-full flex items-center gap-2 px-3 py-2 text-left hover:bg-zinc-100 transition-colors group {isSelected ? 'bg-zinc-100' : ''}"
						onclick={() => selectExecution(exec)}
					>
						<div class="text-zinc-400 group-hover:text-zinc-600 transition-transform duration-200 shrink-0" style="transform: {isSelected ? 'rotate(90deg)' : 'rotate(0deg)'}">
							<svg xmlns="http://www.w3.org/2000/svg" width="12" height="12" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2.5" stroke-linecap="round" stroke-linejoin="round"><path d="m9 18 6-6-6-6"/></svg>
						</div>
						<span class="shrink-0 text-[11px] {style.text}">{getStatusIcon(exec.status)}</span>
						<div class="flex-1 min-w-0">
							<div class="flex items-center gap-1.5">
								<span class="text-[11px] font-medium text-zinc-700 truncate">{displayStatus(exec.status)}</span>
								<span class="text-[10px] text-zinc-400">·</span>
								<span class="text-[10px] text-zinc-400 truncate">{getTriggerLabel(exec)}</span>
							</div>
							<div class="text-[10px] text-zinc-400 flex items-center gap-1.5">
								<span>{formatTimeAgo(exec.startedAt)}</span>
								{#if executionCosts[exec.id] != null && executionCosts[exec.id] > 0}
									<span class="text-zinc-300">·</span>
									<span class="text-amber-600 font-medium">{formatCost(executionCosts[exec.id])}</span>
								{/if}
							</div>
						</div>
					</button>

					{#if isSelected && selectedExecution}
						<div class="bg-white border-t border-zinc-100 px-3 py-2.5 space-y-2.5">
							<div class="flex gap-1.5 flex-wrap">
								<button
									class="px-2 py-1 text-[10px] font-medium rounded border border-zinc-200 text-zinc-600 hover:bg-zinc-50 transition-colors"
									onclick={() => goto(`/projects/${selectedExecution?.projectId}?executionId=${selectedExecution?.id}`)}
								>View in Editor</button>
								{#if selectedExecution}
									<CopyButton text={buildExecutionText(selectedExecution)} />
								{/if}
								{#if selectedExecution.status === 'running' || selectedExecution.status === 'waiting_for_input'}
									<button
										class="px-2 py-1 text-[10px] font-medium rounded border border-zinc-200 text-zinc-600 hover:bg-zinc-50 transition-colors"
										disabled={loadingLive}
										onclick={() => selectedExecution && fetchLiveState(selectedExecution.id)}
									>{loadingLive ? '...' : 'Refresh'}</button>
									<button
										class="px-2 py-1 text-[10px] font-medium rounded border border-red-200 text-red-600 hover:bg-red-50 transition-colors"
										disabled={cancelling}
										onclick={() => selectedExecution && cancelExecution(selectedExecution.id)}
									>{cancelling ? '...' : 'Cancel'}</button>
								{/if}
							</div>

							<div class="space-y-1 text-[11px]">
								<div class="flex justify-between">
									<span class="text-zinc-400">ID</span>
									<code class="text-[10px] bg-zinc-100 px-1.5 py-0.5 rounded text-zinc-600">{selectedExecution.id.slice(0, 8)}...</code>
								</div>
								<div class="flex justify-between">
									<span class="text-zinc-400">Started</span>
									<span class="text-zinc-600">{formatDate(selectedExecution.startedAt)}</span>
								</div>
								{#if selectedExecution.completedAt}
									<div class="flex justify-between">
										<span class="text-zinc-400">Completed</span>
										<span class="text-zinc-600">{formatDate(selectedExecution.completedAt)}</span>
									</div>
								{/if}
								{#if executionCosts[selectedExecution.id] != null && executionCosts[selectedExecution.id] > 0}
									<div class="flex justify-between">
										<span class="text-zinc-400">Cost</span>
										<span class="text-amber-600 font-medium">{formatCost(executionCosts[selectedExecution.id])}</span>
									</div>
								{/if}
							</div>

							{#if selectedExecution.error}
								<div class="bg-red-50 border border-red-200 rounded px-2 py-1.5">
									<pre class="text-[10px] text-red-600 whitespace-pre-wrap break-words">{selectedExecution.error}</pre>
								</div>
							{/if}

							{#if Object.keys(effectiveStatuses).length > 0}
								{@const sortedStatuses = sortByOrdering(Object.entries(effectiveStatuses))}
								<div>
									<div class="text-[10px] font-semibold text-zinc-500 uppercase tracking-wider mb-1">
										Node Statuses {Object.keys(liveStatuses).length > 0 ? '(live)' : ''}
									</div>
									<div class="border border-zinc-200 rounded overflow-hidden">
										{#each sortedStatuses as [nodeId, status], i}
											{@const fullStatus = String(status)}
											{@const baseStatus = fullStatus.split(' ')[0]}
											{@const laneDetail = fullStatus.includes('(') ? fullStatus.slice(fullStatus.indexOf('(')) : ''}
											{@const nodeStyle = getStatusStyle(baseStatus)}
											{@const nodeOutput = effectiveOutputs[nodeId] as Record<string, unknown> | undefined}
											{@const nodeError = nodeOutput?._error as string | undefined}
											<div class="px-2 py-1 text-[10px] {i > 0 ? 'border-t border-zinc-100' : ''} {i % 2 === 0 ? 'bg-zinc-50/50' : ''}">
												<div class="flex items-center justify-between gap-1">
													<div class="flex items-center gap-1.5 min-w-0">
														<span class="{nodeStyle.text}">{getStatusIcon(baseStatus)}</span>
														<span class="truncate text-zinc-700">{getNodeDisplayName(nodeId)}</span>
														{#if laneDetail}
															<span class="text-zinc-400 text-[9px]">{laneDetail}</span>
														{/if}
													</div>
													<span class="{nodeStyle.text} font-medium shrink-0">{displayStatus(baseStatus)}</span>
												</div>
												{#if baseStatus === 'failed' && nodeError}
													<div class="mt-0.5 text-[10px] text-red-600 bg-red-50 rounded px-1.5 py-0.5 break-words">{nodeError}</div>
												{/if}
											</div>
										{/each}
									</div>
								</div>
							{/if}

							{#if Object.keys(effectiveOutputs).length > 0}
								<div>
									<div class="text-[10px] font-semibold text-zinc-500 uppercase tracking-wider mb-1">
										Node Outputs {Object.keys(liveOutputs).length > 0 ? '(live)' : ''}
									</div>
									<div class="space-y-1">
										{#each sortByOrdering(Object.entries(effectiveOutputs)) as [nodeId, output]}
											{@const isExpanded = expandedOutputs.has(nodeId)}
											<div class="border border-zinc-200 rounded overflow-hidden">
												<button
													class="w-full flex items-center justify-between px-2 py-1 text-[10px] hover:bg-zinc-50 transition-colors text-left"
													onclick={() => toggleOutput(nodeId)}
												>
													<span class="font-medium truncate text-zinc-700">{getNodeDisplayName(nodeId)}</span>
													<svg
														class="w-2.5 h-2.5 text-zinc-400 shrink-0 ml-1 transition-transform {isExpanded ? 'rotate-180' : ''}"
														fill="none" stroke="currentColor" viewBox="0 0 24 24"
													>
														<path stroke-linecap="round" stroke-linejoin="round" stroke-width="2.5" d="M19 9l-7 7-7-7" />
													</svg>
												</button>
												{#if isExpanded}
													<div class="border-t border-zinc-100 bg-zinc-50 px-2 py-1.5 relative">
														<CopyButton text={JSON.stringify(cleanOutput(output), null, 2)} class="absolute top-1 right-1" />
														<pre class="text-[10px] overflow-x-auto whitespace-pre-wrap break-words text-zinc-600 select-text cursor-text">{JSON.stringify(cleanOutput(output), null, 2)}</pre>
													</div>
												{:else}
													<div class="border-t border-zinc-100 bg-zinc-50 px-2 py-1">
														<pre class="text-[10px] text-zinc-500 truncate">{previewOutput(output)}</pre>
													</div>
												{/if}
											</div>
										{/each}
									</div>
								</div>
							{/if}
						</div>
					{/if}
				</div>
			{/each}
		{/if}
	</div>
</div>
