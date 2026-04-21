<script lang="ts">
	import { onMount } from 'svelte';
	import { browser } from "$app/environment";
	import { NODE_TYPE_CONFIG } from "$lib/nodes";
	import { goto } from '$app/navigation';
	import { api, authFetch } from '$lib/config';
	import { fetchExecutionCost as _fetchExecutionCost, formatCost } from '$lib/utils/cost';
	import { getStatusIcon, displayStatus } from '$lib/utils/status';
	import CopyButton from '$lib/components/ui/CopyButton.svelte';
	import { formatTimeAgo, formatDate, getStatusStyle, cleanOutput } from '$lib/utils/status'; 

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

	interface Project {
		id: string;
		name: string;
		nodes?: Array<{ id: string; label?: string; nodeType?: string }>;
	}

	let executions = $state<Execution[]>([]);
	let projects = $state<Map<string, Project>>(new Map());
	let loading = $state(true);
	let error = $state<string | null>(null);
	let selectedExecutionId = $state<string | null>(null);
	let autoRefresh = $state(true);
	let refreshInterval: ReturnType<typeof setInterval> | null = null;
	let cancelling = $state(false);
	let liveStatuses = $state<Record<string, string>>({});
	let liveOutputs = $state<Record<string, unknown>>({});
	let liveOrdering = $state<Record<string, number>>({});
	let loadingLive = $state(false);

	const PAGE_SIZE = 50;
	let currentPage = $state(0);
	let hasMore = $state(true);

	let selectedExecution = $derived(executions.find(e => e.id === selectedExecutionId) ?? null);
	let effectiveStatuses = $derived(Object.keys(liveStatuses).length > 0 ? liveStatuses : (selectedExecution?.nodeStatuses ?? {}));
	let effectiveOutputs = $derived(Object.keys(liveOutputs).length > 0 ? liveOutputs : (selectedExecution?.nodeOutputs ?? {}));
	let expandedOutputs = $state<Set<string>>(new Set());
	let executionCosts = $state<Record<string, number>>({});

	async function fetchExecutionCost(executionId: string) {
		const cost = await _fetchExecutionCost(executionId);
		if (cost !== null) {
			executionCosts = { ...executionCosts, [executionId]: cost };
		}
	}

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

	function getTriggerLabel(exec: Execution): string {
		if (!exec.nodeType) return 'Manual';
		return NODE_TYPE_CONFIG[exec.nodeType]?.label || exec.nodeType;
	}

	function getProjectName(projectId: string): string {
		return projects.get(projectId)?.name || 'Unknown';
	}

	function getNodeDisplayName(nodeId: string, projectId: string): string {
		const wf = projects.get(projectId);
		if (wf?.nodes) {
			const node = wf.nodes.find(n => n.id === nodeId);
			if (node?.label) return node.label;
			if (node?.nodeType) return NODE_TYPE_CONFIG[node.nodeType]?.label || node.nodeType;
		}
		return nodeId.length > 20 ? nodeId.slice(0, 8) + '...' : nodeId;
	}

	function previewOutput(output: unknown): string {
		const str = JSON.stringify(cleanOutput(output), null, 2);
		return str.length <= 120 ? str : str.slice(0, 120) + '\n...';
	}

	function toggleOutput(nodeId: string) {
		const next = new Set(expandedOutputs);
		if (next.has(nodeId)) next.delete(nodeId); else next.add(nodeId);
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
				liveOutputs = (await outputRes.json()).outputs || {};
			}
		} catch (e) {
			console.error('Failed to fetch live state:', e);
		} finally {
			loadingLive = false;
		}
	}

	async function loadExecutions(page: number = currentPage) {
		try {
			const offset = page * PAGE_SIZE;
			const response = await authFetch(`/api/executions?limit=${PAGE_SIZE}&offset=${offset}`);
			if (!response.ok) throw new Error('Failed to load executions');
			executions = await response.json();
			hasMore = executions.length === PAGE_SIZE;
			currentPage = page;
			await reconcileRunningExecutions();
		} catch (e) {
			error = e instanceof Error ? e.message : 'Unknown error';
		} finally {
			loading = false;
		}
	}

	async function reconcileRunningExecutions() {
		const needsReconcile = executions.filter(e =>
			e.status === 'running'
			|| (e.status !== 'pending' && Object.keys(e.nodeStatuses ?? {}).length === 0)
		);
		if (needsReconcile.length === 0) return;
		const BATCH_SIZE = 5;
		for (let i = 0; i < needsReconcile.length; i += BATCH_SIZE) {
			const batch = needsReconcile.slice(i, i + BATCH_SIZE);
			await Promise.all(batch.map(async (exec) => {
				try {
					const res = await authFetch(api.getStatus(exec.id));
					if (!res.ok) return;
					const status: string = await res.json();
					if (status === 'running' || status === 'waiting_for_input') return;
					const [nsRes, outRes] = await Promise.all([
						authFetch(api.getNodeStatuses(exec.id)),
						authFetch(api.getAllOutputs(exec.id)),
					]);
					const nodeStatuses = nsRes.ok ? ((await nsRes.json()).statuses ?? {}) : {};
					const nodeOutputs = outRes.ok ? ((await outRes.json()).outputs ?? {}) : {};
					if (Object.keys(nodeStatuses).length > 0 || Object.keys(nodeOutputs).length > 0) {
						await authFetch(`/api/executions/${exec.id}`, {
							method: 'PUT',
							headers: { 'Content-Type': 'application/json' },
							body: JSON.stringify({ status, nodeStatuses, nodeOutputs }),
						});
						executions = executions.map(e => e.id === exec.id ? { ...e, status: status as Execution['status'], nodeStatuses, nodeOutputs } : e);
					} else if (exec.status === 'running') {
						await authFetch(`/api/executions/${exec.id}`, {
							method: 'PUT',
							headers: { 'Content-Type': 'application/json' },
							body: JSON.stringify({ status }),
						});
						executions = executions.map(e => e.id === exec.id ? { ...e, status: status as Execution['status'] } : e);
					}
				} catch {}
			}));
		}
	}

	async function loadProjects() {
		try {
			const response = await authFetch('/api/projects');
			if (!response.ok) return;
			const data = await response.json();
			projects = new Map(data.map((w: Project) => [w.id, w]));
		} catch {}
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

	function buildExecutionText(exec: Execution): string {
		const useStatuses = Object.keys(liveStatuses).length > 0 ? liveStatuses : exec.nodeStatuses;
		const useOutputs = Object.keys(liveOutputs).length > 0 ? liveOutputs : exec.nodeOutputs;
		const isLive = Object.keys(liveStatuses).length > 0;
		const lines: string[] = [];
		lines.push(`# ${getProjectName(exec.projectId)}${isLive ? ' (live)' : ''}`);
		lines.push(`Status: ${displayStatus(exec.status)}`);
		lines.push(`ID: ${exec.id}`);
		lines.push(`Started: ${formatDate(exec.startedAt)}`);
		if (exec.completedAt) lines.push(`Completed: ${formatDate(exec.completedAt)}`);
		if (exec.error) lines.push(`\nError: ${exec.error}`);
		for (const [nodeId, status] of Object.entries(useStatuses)) {
			const base = String(status).split(' ')[0];
			const nodeErr = (useOutputs[nodeId] as Record<string, unknown> | undefined)?._error as string | undefined;
			let line = `- ${getNodeDisplayName(nodeId, exec.projectId)}: ${String(status)}`;
			if (base === 'failed' && nodeErr) line += `\n  Error: ${nodeErr}`;
			lines.push(line);
		}
		return lines.join('\n');
	}

	onMount(() => {
		loadProjects();
		loadExecutions();
		if (autoRefresh) refreshInterval = setInterval(loadExecutions, 10000);
		return () => { if (refreshInterval) clearInterval(refreshInterval); };
	});

	$effect(() => {
		if (autoRefresh && !refreshInterval) {
			refreshInterval = setInterval(loadExecutions, 10000);
		} else if (!autoRefresh && refreshInterval) {
			clearInterval(refreshInterval);
			refreshInterval = null;
		}
	});
</script>

<div class="min-h-screen pt-20 px-6 pb-12" style="background: #f8f9fa; background-image: radial-gradient(circle, #d4d4d8 1px, transparent 1px); background-size: 24px 24px;">
	<div class="max-w-4xl mx-auto">

		<!-- Header toolbar -->
		<div class="flex items-center justify-between mb-5">
			<h2 class="text-[15px] font-semibold text-zinc-800">Executions</h2>
			<div class="flex items-center gap-2">
				<button
					onclick={() => autoRefresh = !autoRefresh}
					class="flex items-center gap-1.5 px-2.5 py-1.5 text-[11px] font-medium rounded-lg border transition-colors {autoRefresh ? 'bg-white border-zinc-300 text-zinc-700' : 'bg-zinc-100 border-zinc-200 text-zinc-400'}"
				>
					{#if autoRefresh}
						<span class="flex h-1.5 w-1.5 relative">
							<span class="animate-ping absolute inline-flex h-full w-full rounded-full bg-emerald-400 opacity-75"></span>
							<span class="relative inline-flex rounded-full h-1.5 w-1.5 bg-emerald-500"></span>
						</span>
						Live
					{:else}
						<svg width="10" height="10" viewBox="0 0 24 24" fill="currentColor"><rect x="6" y="4" width="4" height="16"/><rect x="14" y="4" width="4" height="16"/></svg>
						Paused
					{/if}
				</button>
				<button
					onclick={() => loadExecutions()}
					class="flex items-center justify-center w-7 h-7 rounded-lg border border-zinc-200 bg-white hover:bg-zinc-50 transition-colors text-zinc-400 hover:text-zinc-600"
					title="Refresh"
				>
					<svg width="12" height="12" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2.5" stroke-linecap="round" stroke-linejoin="round">
						<polyline points="23 4 23 10 17 10"/><polyline points="1 20 1 14 7 14"/><path d="M3.51 9a9 9 0 0 1 14.85-3.36L23 10M1 14l4.64 4.36A9 9 0 0 0 20.49 15"/>
					</svg>
				</button>
			</div>
		</div>

		{#if loading}
			<div class="flex items-center justify-center py-24">
				<div class="h-5 w-5 border-2 border-zinc-300 border-t-zinc-600 rounded-full animate-spin"></div>
			</div>
		{:else if error}
			<div class="bg-red-50 border border-red-200 rounded-lg px-4 py-3 text-[12px] text-red-600">{error}</div>
		{:else if executions.length === 0}
			<div class="flex flex-col items-center justify-center py-24">
				<div class="w-14 h-14 rounded-full bg-white border border-zinc-200 flex items-center justify-center mb-5" style="box-shadow: 0 2px 8px rgba(0,0,0,0.06);">
					<svg width="22" height="22" viewBox="0 0 24 24" fill="none" stroke="#a1a1aa" stroke-width="1.5"><polygon points="5 3 19 12 5 21 5 3"/></svg>
				</div>
				<p class="text-[15px] font-medium text-zinc-600 mb-1">No executions yet</p>
				<p class="text-[13px] text-zinc-400">Run a project and executions will appear here</p>
			</div>
		{:else}
			<!-- Execution list (same pattern as builder sidebar) -->
			<div class="bg-white rounded-xl border border-zinc-200 overflow-hidden" style="box-shadow: 0 1px 3px rgba(0,0,0,0.06);">
				{#each executions as exec (exec.id)}
					{@const style = getStatusStyle(exec.status)}
					{@const isSelected = selectedExecutionId === exec.id}
					<div class="border-b border-zinc-100 last:border-b-0">
						<!-- Execution row -->
						<button
							class="w-full flex items-center gap-2.5 px-4 py-2.5 text-left hover:bg-zinc-50 transition-colors group {isSelected ? 'bg-zinc-50' : ''}"
							onclick={() => selectExecution(exec)}
						>
							<div class="text-zinc-400 group-hover:text-zinc-600 transition-transform duration-200 shrink-0" style="transform: {isSelected ? 'rotate(90deg)' : 'rotate(0deg)'}">
								<svg width="12" height="12" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2.5" stroke-linecap="round" stroke-linejoin="round"><path d="m9 18 6-6-6-6"/></svg>
							</div>
							<span class="shrink-0 text-[12px] {style.text}">{getStatusIcon(exec.status)}</span>
							<div class="flex-1 min-w-0">
								<div class="flex items-center gap-2">
									<span class="text-[12px] font-medium text-zinc-800 truncate">{getProjectName(exec.projectId)}</span>
									<span class="text-[10px] font-medium px-1.5 py-0.5 rounded {style.bg} {style.text}">{displayStatus(exec.status)}</span>
								</div>
								<div class="flex items-center gap-1.5 mt-0.5">
									<span class="text-[10px] text-zinc-400">{formatTimeAgo(exec.startedAt)}</span>
									<span class="text-[10px] text-zinc-300">·</span>
									<span class="text-[10px] text-zinc-400">{getTriggerLabel(exec)}</span>
									{#if executionCosts[exec.id] != null && executionCosts[exec.id] > 0}
										<span class="text-[10px] text-zinc-300">·</span>
										<span class="text-[10px] text-amber-600 font-medium">{formatCost(executionCosts[exec.id])}</span>
									{/if}
								</div>
							</div>
						</button>

						<!-- Expanded details -->
						{#if isSelected && selectedExecution}
							<div class="bg-[#f8f9fa] border-t border-zinc-100 px-4 py-3 space-y-3">
								<!-- Actions -->
								<div class="flex gap-1.5 flex-wrap">
									<button
										class="px-2.5 py-1 text-[10px] font-medium rounded-md border border-zinc-200 bg-white text-zinc-600 hover:bg-zinc-50 transition-colors"
										onclick={() => goto(`/projects/${selectedExecution?.projectId}?executionId=${selectedExecution?.id}`)}
									>View in Editor</button>
									{#if selectedExecution}
										<CopyButton text={buildExecutionText(selectedExecution)} />
									{/if}
									{#if selectedExecution.status === 'running' || selectedExecution.status === 'waiting_for_input'}
										<button
											class="px-2.5 py-1 text-[10px] font-medium rounded-md border border-zinc-200 bg-white text-zinc-600 hover:bg-zinc-50 transition-colors"
											disabled={loadingLive}
											onclick={() => selectedExecution && fetchLiveState(selectedExecution.id)}
										>{loadingLive ? '...' : 'Refresh State'}</button>
										<button
											class="px-2.5 py-1 text-[10px] font-medium rounded-md border border-red-200 bg-white text-red-600 hover:bg-red-50 transition-colors"
											disabled={cancelling}
											onclick={() => selectedExecution && cancelExecution(selectedExecution.id)}
										>{cancelling ? '...' : 'Cancel'}</button>
									{/if}
								</div>

								<!-- Meta -->
								<div class="space-y-1.5 text-[11px]">
									<div class="flex justify-between">
										<span class="text-zinc-400">ID</span>
										<code class="text-[10px] bg-zinc-100 px-1.5 py-0.5 rounded text-zinc-600">{selectedExecution.id.slice(0, 8)}...</code>
									</div>
									<div class="flex justify-between">
										<span class="text-zinc-400">Project</span>
										<button class="text-zinc-700 hover:text-zinc-900 hover:underline text-[11px]" onclick={() => goto(`/projects/${selectedExecution?.projectId}`)}>
											{getProjectName(selectedExecution.projectId)}
										</button>
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

								<!-- Error -->
								{#if selectedExecution.error}
									<div class="bg-red-50 border border-red-200 rounded-md px-2.5 py-2">
										<pre class="text-[10px] text-red-600 whitespace-pre-wrap break-words">{selectedExecution.error}</pre>
									</div>
								{/if}

								<!-- Node statuses -->
								{#if Object.keys(effectiveStatuses).length > 0}
									{@const sortedStatuses = sortByOrdering(Object.entries(effectiveStatuses))}
									<div>
										<div class="text-[10px] font-semibold text-zinc-500 uppercase tracking-wider mb-1.5">
											Node Statuses {Object.keys(liveStatuses).length > 0 ? '(live)' : ''}
										</div>
										<div class="border border-zinc-200 rounded-md overflow-hidden bg-white">
											{#each sortedStatuses as [nodeId, status], i}
												{@const fullStatus = String(status)}
												{@const baseStatus = fullStatus.split(' ')[0]}
												{@const laneDetail = fullStatus.includes('(') ? fullStatus.slice(fullStatus.indexOf('(')) : ''}
												{@const nodeStyle = getStatusStyle(baseStatus)}
												{@const nodeOutput = effectiveOutputs[nodeId] as Record<string, unknown> | undefined}
												{@const nodeError = nodeOutput?._error as string | undefined}
												<div class="px-2.5 py-1.5 text-[11px] {i > 0 ? 'border-t border-zinc-100' : ''} {i % 2 === 0 ? 'bg-zinc-50/50' : ''}">
													<div class="flex items-center justify-between gap-1.5">
														<div class="flex items-center gap-1.5 min-w-0">
															<span class="{nodeStyle.text}">{getStatusIcon(baseStatus)}</span>
															<span class="truncate text-zinc-700">{getNodeDisplayName(nodeId, selectedExecution.projectId)}</span>
															{#if laneDetail}
																<span class="text-zinc-400 text-[9px]">{laneDetail}</span>
															{/if}
														</div>
														<span class="{nodeStyle.text} font-medium shrink-0">{displayStatus(baseStatus)}</span>
													</div>
													{#if baseStatus === 'failed' && nodeError}
														<div class="mt-1 text-[10px] text-red-600 bg-red-50 rounded px-2 py-1 break-words">{nodeError}</div>
													{/if}
												</div>
											{/each}
										</div>
									</div>
								{/if}

								<!-- Node outputs -->
								{#if Object.keys(effectiveOutputs).length > 0}
									<div>
										<div class="text-[10px] font-semibold text-zinc-500 uppercase tracking-wider mb-1.5">
											Node Outputs {Object.keys(liveOutputs).length > 0 ? '(live)' : ''}
										</div>
										<div class="space-y-1">
											{#each sortByOrdering(Object.entries(effectiveOutputs)) as [nodeId, output]}
												{@const isExpanded = expandedOutputs.has(nodeId)}
												<div class="border border-zinc-200 rounded-md overflow-hidden bg-white">
													<button
														class="w-full flex items-center justify-between px-2.5 py-1.5 text-[11px] hover:bg-zinc-50 transition-colors text-left"
														onclick={() => toggleOutput(nodeId)}
													>
														<span class="font-medium truncate text-zinc-700">{getNodeDisplayName(nodeId, selectedExecution.projectId)}</span>
														<svg
															class="w-3 h-3 text-zinc-400 shrink-0 ml-1.5 transition-transform {isExpanded ? 'rotate-180' : ''}"
															fill="none" stroke="currentColor" viewBox="0 0 24 24"
														><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2.5" d="M19 9l-7 7-7-7" /></svg>
													</button>
													{#if isExpanded}
														<div class="border-t border-zinc-100 bg-zinc-50 px-2.5 py-2 relative">
															<CopyButton text={JSON.stringify(cleanOutput(output), null, 2)} class="absolute top-1.5 right-1.5" />
															<pre class="text-[10px] overflow-x-auto whitespace-pre-wrap break-words text-zinc-600 select-text cursor-text">{JSON.stringify(cleanOutput(output), null, 2)}</pre>
														</div>
													{:else}
														<div class="border-t border-zinc-100 bg-zinc-50 px-2.5 py-1.5">
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
			</div>

			<!-- Pagination -->
			{#if currentPage > 0 || hasMore}
				<div class="flex items-center justify-center gap-3 mt-6">
					<button
						onclick={() => loadExecutions(currentPage - 1)}
						disabled={currentPage === 0}
						class="px-3 py-1.5 text-[12px] font-medium rounded-lg transition-colors {currentPage === 0 ? 'text-zinc-300 cursor-default' : 'text-zinc-500 hover:text-zinc-700 hover:bg-white hover:shadow-sm'}"
					>Previous</button>
					<span class="text-[11px] text-zinc-400 font-mono tabular-nums">Page {currentPage + 1}</span>
					<button
						onclick={() => loadExecutions(currentPage + 1)}
						disabled={!hasMore}
						class="px-3 py-1.5 text-[12px] font-medium rounded-lg transition-colors {!hasMore ? 'text-zinc-300 cursor-default' : 'text-zinc-500 hover:text-zinc-700 hover:bg-white hover:shadow-sm'}"
					>Next</button>
				</div>
			{/if}
		{/if}
	</div>
</div>
