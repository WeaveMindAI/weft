<script lang="ts">
	import { Play, Square, Zap, ZapOff, Database, Loader2 } from '@lucide/svelte';

	let {
		variant = 'floating',
		infraState,
		triggerState,
		executionState,
		onCheckInfraStatus,
		onStartInfra,
		onStopInfra,
		onTerminateInfra,
		onForceRetry,
		onToggleTrigger,
		onResyncTrigger,
		onRun,
		onStop,
		onToggleInfraSubgraph,
		showInfraSubgraph = false,
		onToggleTriggerSubgraph,
		showTriggerSubgraph = false,
		nodeCount = 1,
		disabled = false,
	}: {
		variant?: 'floating' | 'card';
		infraState?: {
			hasInfrastructure: boolean;
			hasInfraInFrontend?: boolean;
			hasInfraInBackend?: boolean;
			infraDiverged?: boolean;
			status: string;
			nodes?: Array<{ nodeId: string; nodeType: string; instanceId: string; status: string; backend?: string }>;
			isLoading: boolean;
		};
		triggerState?: {
			hasTriggers: boolean;
			hasTriggersInFrontend?: boolean;
			hasTriggersInBackend?: boolean;
			isActive: boolean;
			isLoading: boolean;
			hasError?: boolean;
			isStale?: boolean;
		};
		executionState?: { isRunning: boolean; isStarting?: boolean; isStopping?: boolean; activeEdges?: Set<string>; nodeOutputs?: Record<string, unknown>; nodeStatuses?: Record<string, string> };
		onCheckInfraStatus?: () => void;
		onStartInfra?: () => void;
		onStopInfra?: () => void;
		onTerminateInfra?: () => void;
		onForceRetry?: () => void;
		onToggleTrigger?: () => void;
		onResyncTrigger?: () => void;
		onRun?: () => void;
		onStop?: () => void;
		onToggleInfraSubgraph?: () => void;
		showInfraSubgraph?: boolean;
		onToggleTriggerSubgraph?: () => void;
		showTriggerSubgraph?: boolean;
		nodeCount?: number;
		disabled?: boolean;
	} = $props();

	const isCard = $derived(variant === 'card');

	const infraBlocking = $derived(
		infraState?.hasInfrastructure && (infraState.status !== 'running' || infraState.isLoading)
	);

	const infraIsTransitional = $derived(
		infraState?.isLoading || infraState?.status === 'loading' || infraState?.status === 'starting' || infraState?.status === 'stopping' || infraState?.status === 'terminating'
	);

	const showTerminate = $derived(
		onTerminateInfra && infraState && (
			infraState.status === 'running' ||
			infraState.status === 'stopped' ||
			infraState.status === 'failed' ||
			infraState.status === 'starting' ||
			infraState.status === 'error'
		)
	);

	// Variant-aware CSS helpers
	const btn = $derived(isCard
		? 'flex items-center gap-2 text-sm px-3 py-2 rounded-md transition-colors'
		: 'flex items-center gap-2 px-3 py-1.5 rounded-lg transition-colors');
	const btnDisabled = 'disabled:opacity-50 disabled:cursor-not-allowed';
	const label = $derived(isCard ? '' : 'font-medium text-[11px] uppercase tracking-wider');
</script>

<!-- Outer wrapper: floating pill vs card -->
<div class={isCard
	? 'rounded-lg border border-border bg-card p-4 space-y-3'
	: 'absolute bottom-6 left-1/2 -translate-x-1/2 flex items-center gap-1.5 p-1.5 bg-white border border-zinc-200 rounded-xl shadow-xl z-20 backdrop-blur-md'}>

	<!-- ════════ INFRASTRUCTURE SECTION ════════ -->
	{#if infraState?.hasInfrastructure}
		{#if isCard}
			<div class="flex items-center justify-between">
				<div>
					<p class="text-sm font-medium">Infrastructure</p>
					<p class="text-xs text-muted-foreground capitalize">
						{infraState.status === 'loading' ? 'Checking...' : infraState.status}
					</p>
					{#if infraBlocking && !infraState.isLoading && infraState.status !== 'starting' && infraState.status !== 'stopping' && infraState.status !== 'terminating'}
						<p class="text-xs text-amber-600 mt-0.5">Start infrastructure before running</p>
					{/if}
				</div>
				<div class="flex gap-2">
					{@render infraButtons()}
				</div>
			</div>
		{:else}
			<div class="flex items-center">
				{@render infraButtons()}
				{#if onToggleInfraSubgraph}
					<div class="w-px h-5 bg-zinc-200 mx-1.5"></div>
					<button
						class="flex items-center justify-center w-7 h-7 rounded-lg transition-colors {showInfraSubgraph ? 'bg-blue-100 text-blue-600 border border-blue-200' : 'bg-white text-zinc-400 border border-zinc-200 hover:bg-zinc-50'}"
						onclick={onToggleInfraSubgraph}
						title={showInfraSubgraph ? 'Hide infrastructure subgraph' : 'Show infrastructure subgraph'}
					>
						<svg xmlns="http://www.w3.org/2000/svg" width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round">
							{#if showInfraSubgraph}
								<path d="M1 12s4-8 11-8 11 8 11 8-4 8-11 8-11-8-11-8z"/><circle cx="12" cy="12" r="3"/>
							{:else}
								<path d="M17.94 17.94A10.07 10.07 0 0 1 12 20c-7 0-11-8-11-8a18.45 18.45 0 0 1 5.06-5.94M9.9 4.24A9.12 9.12 0 0 1 12 4c7 0 11 8 11 8a18.5 18.5 0 0 1-2.16 3.19m-6.72-1.07a3 3 0 1 1-4.24-4.24"/><line x1="1" y1="1" x2="23" y2="23"/>
							{/if}
						</svg>
					</button>
				{/if}
			</div>
			<div class="w-px h-6 bg-zinc-200 mx-1"></div>
		{/if}
	{/if}

	<!-- ════════ TRIGGER / RUN SECTION ════════ -->
	{#if triggerState?.hasTriggers && onToggleTrigger}
		{#if isCard && infraState?.hasInfrastructure}
			<div class="border-t border-border pt-3"></div>
		{/if}

		{#if executionState?.isRunning && onStop}
			{#if isCard}
				<div class="flex items-center justify-between">
					<div>
						<p class="text-sm font-medium">Execution</p>
						<p class="text-xs text-muted-foreground">Triggered project is running</p>
					</div>
					{@render stopExecutionButton()}
				</div>
			{:else}
				{@render stopExecutionButton()}
			{/if}
		{/if}

		{#if isCard}
			<div class="flex items-center justify-between {executionState?.isRunning ? 'border-t border-border pt-3' : ''}">
				<div>
					<p class="text-sm font-medium">Trigger</p>
					<p class="text-xs text-muted-foreground">
						{triggerState.isActive ? 'Active: listening for events' : 'Inactive'}
					</p>
				</div>
				{@render triggerButton()}
			</div>
		{:else}
			{@render triggerButton()}
			{#if onToggleTriggerSubgraph}
				<button
					class="flex items-center justify-center w-7 h-7 rounded-lg transition-colors {showTriggerSubgraph ? 'bg-emerald-100 text-emerald-600 border border-emerald-200' : 'bg-white text-zinc-400 border border-zinc-200 hover:bg-zinc-50'}"
					onclick={onToggleTriggerSubgraph}
					title={showTriggerSubgraph ? 'Hide trigger subgraph' : 'Show trigger subgraph'}
				>
					<svg xmlns="http://www.w3.org/2000/svg" width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round">
						{#if showTriggerSubgraph}
							<path d="M1 12s4-8 11-8 11 8 11 8-4 8-11 8-11-8-11-8z"/><circle cx="12" cy="12" r="3"/>
						{:else}
							<path d="M17.94 17.94A10.07 10.07 0 0 1 12 20c-7 0-11-8-11-8a18.45 18.45 0 0 1 5.06-5.94M9.9 4.24A9.12 9.12 0 0 1 12 4c7 0 11 8 11 8a18.5 18.5 0 0 1-2.16 3.19m-6.72-1.07a3 3 0 1 1-4.24-4.24"/><line x1="1" y1="1" x2="23" y2="23"/>
						{/if}
					</svg>
				</button>
			{/if}
		{/if}

		{#if triggerState?.isStale && triggerState.isActive && onResyncTrigger}
			{#if isCard}
				<div class="flex items-center justify-between border-t border-amber-200 pt-3 mt-3">
					<div>
						<p class="text-sm font-medium text-amber-700">Out of sync</p>
						<p class="text-xs text-amber-600">Project changed since activation. Resync to apply changes.</p>
					</div>
					{@render resyncButton()}
				</div>
			{:else}
				<div class="w-px h-6 bg-amber-300 mx-1"></div>
				<span class="text-[10px] font-medium text-amber-600 uppercase tracking-wider whitespace-nowrap">Out of sync</span>
				{@render resyncButton()}
			{/if}
		{/if}
	{:else if onRun || onStop}
		{#if isCard && infraState?.hasInfrastructure}
			<div class="border-t border-border pt-3"></div>
		{/if}
		{#if isCard}
			<div class="flex items-center justify-between">
				<div>
					<p class="text-sm font-medium">Run once</p>
					<p class="text-xs text-muted-foreground">Manually trigger a single execution</p>
				</div>
				{@render runStopButton()}
			</div>
		{:else}
			{@render runStopButton()}
		{/if}
	{/if}
</div>

<!-- ════════════════════════════════════════════════════════════
     SHARED SNIPPETS: rendered once, used by both variants
     ════════════════════════════════════════════════════════════ -->

{#snippet infraButtons()}
	{#if infraIsTransitional}
		<button class="{btn} {isCard ? 'bg-muted text-muted-foreground' : 'bg-blue-50 text-blue-600 border border-blue-200 opacity-75 cursor-not-allowed'}" disabled>
			<Loader2 class="w-3.5 h-3.5 animate-spin" />
			<span class={label}>
				{#if infraState?.status === 'starting'}
					{isCard ? 'Starting...' : `Starting Infra (${(infraState?.nodes ?? []).filter(n => n.status === 'running').length}/${(infraState?.nodes ?? []).length})`}
				{:else if infraState?.status === 'stopping'}
					Stopping{isCard ? '...' : ' Infra...'}
				{:else if infraState?.status === 'terminating'}
					Terminating{isCard ? '...' : ' Infra...'}
				{:else}
					{isCard ? 'Checking...' : 'Infrastructure...'}
				{/if}
			</span>
		</button>
	{:else if infraState?.status === 'failed'}
		<button
			class="{btn} {isCard ? 'bg-destructive/10 text-destructive hover:bg-destructive/20' : 'bg-red-50 text-red-600 border border-red-200 hover:bg-red-100'}"
			onclick={onStartInfra}
		>
			<Database class="w-3.5 h-3.5" />
			<span class={label}>{isCard ? 'Retry' : 'Retry Infra'}</span>
		</button>
		{#if onForceRetry}
			<button
				class="{btn} {isCard ? 'bg-amber-100 text-amber-800 hover:bg-amber-200' : 'ml-1 bg-amber-50 text-amber-700 border border-amber-200 hover:bg-amber-100'}"
				onclick={onForceRetry}
				title="Kill stuck operations and retry from scratch"
			>
				<svg xmlns="http://www.w3.org/2000/svg" width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2">
					<path d="M23 4v6h-6"/><path d="M20.49 15a9 9 0 1 1-2.12-9.36L23 10"/>
				</svg>
				<span class={label}>Force Retry</span>
			</button>
		{/if}
	{:else if infraState?.status === 'error'}
		<button
			class="{btn} {isCard ? 'bg-amber-100 text-amber-800 hover:bg-amber-200' : 'bg-amber-50 text-amber-700 border border-amber-200 hover:bg-amber-100'}"
			onclick={onCheckInfraStatus}
		>
			<Loader2 class="w-3.5 h-3.5" />
			<span class={label}>Retry Check</span>
		</button>
	{:else if infraState?.status === 'running'}
		<button
			class="{btn} {isCard ? 'bg-muted text-muted-foreground hover:bg-muted/80' : 'bg-zinc-100 text-zinc-700 border border-zinc-200 hover:bg-zinc-200'}"
			onclick={onStopInfra}
		>
			<Square class="w-3.5 h-3.5" />
			<span class={label}>Stop Infra</span>
			{#if !isCard}
				<span class="flex h-1.5 w-1.5 relative ml-0.5">
					<span class="animate-ping absolute inline-flex h-full w-full rounded-full bg-green-400 opacity-75"></span>
					<span class="relative inline-flex rounded-full h-1.5 w-1.5 bg-green-500"></span>
				</span>
			{/if}
		</button>
	{:else if infraState?.status === 'stopped' || infraState?.status === 'terminated'}
		<button
			class="{btn} {isCard ? 'bg-primary text-primary-foreground hover:bg-primary/90' : 'bg-blue-50 text-blue-600 border border-blue-200 hover:bg-blue-100'}"
			onclick={onStartInfra}
		>
			<Database class="w-3.5 h-3.5" />
			<span class={label}>{infraState?.status === 'stopped' ? 'Restart Infra' : 'Start Infra'}</span>
		</button>
	{:else if infraState?.hasInfraInFrontend}
		<button
			class="{btn} {btnDisabled} {isCard ? 'bg-primary text-primary-foreground hover:bg-primary/90' : 'bg-blue-50 text-blue-600 border border-blue-200 hover:bg-blue-100'}"
			onclick={onStartInfra}
			disabled={nodeCount === 0}
		>
			<Database class="w-3.5 h-3.5" />
			<span class={label}>Start Infra</span>
		</button>
	{/if}
	{#if showTerminate}
		<button
			class="{btn} {isCard ? 'bg-destructive/10 text-destructive hover:bg-destructive/20' : 'ml-1 bg-red-50 text-red-600 border border-red-200 hover:bg-red-100'}"
			onclick={onTerminateInfra}
			title="Terminate Infra"
		>
			<svg xmlns="http://www.w3.org/2000/svg" width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2">
				<path d="M3 6h18"/><path d="M19 6v14c0 1-1 2-2 2H7c-1 0-2-1-2-2V6"/><path d="M8 6V4c0-1 1-2 2-2h4c1 0 2 1 2 2v2"/>
			</svg>
		</button>
	{/if}
{/snippet}

{#snippet stopExecutionButton()}
	<button
		class="{btn} {isCard ? 'bg-orange-100 text-orange-700 hover:bg-orange-200' : 'bg-orange-50 text-orange-600 border border-orange-200 hover:bg-orange-100'}"
		onclick={onStop}
	>
		<Square class="w-3.5 h-3.5" />
		<span class={label}>Stop Execution</span>
	</button>
{/snippet}

{#snippet triggerButton()}
	<button
		class="{btn} {btnDisabled} {triggerState?.isActive
			? (isCard ? 'bg-red-50 text-red-600 border border-red-200 hover:bg-red-100' : 'bg-emerald-600 border-emerald-600 text-white hover:bg-emerald-700')
			: 'bg-zinc-900 text-white hover:bg-zinc-800'} {triggerState?.hasError ? 'bg-amber-50 border-amber-200 text-amber-600 hover:bg-amber-100' : ''}"
		onclick={onToggleTrigger}
		disabled={triggerState?.isLoading || nodeCount === 0 || infraBlocking || disabled}
	>
		{#if triggerState?.isLoading}
			<Loader2 class="w-3.5 h-3.5 animate-spin" />
			<span class={label}>Checking...</span>
		{:else if triggerState?.hasError}
			<svg xmlns="http://www.w3.org/2000/svg" width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round">
				<path d="M23 4v6h-6"/><path d="M20.49 15a9 9 0 1 1-2.12-9.36L23 10"/>
			</svg>
			<span class={label}>Retry</span>
		{:else if triggerState?.isActive}
			{#if isCard}
				<ZapOff class="w-3.5 h-3.5" />
			{:else}
				<svg xmlns="http://www.w3.org/2000/svg" width="14" height="14" viewBox="0 0 24 24" fill="currentColor">
					<rect x="6" y="6" width="12" height="12" rx="1"/>
				</svg>
			{/if}
			<span class={label}>Deactivate</span>
			{#if !isCard}
				<span class="flex h-2 w-2 relative ml-1">
					<span class="animate-ping absolute inline-flex h-full w-full rounded-full bg-green-400 opacity-75"></span>
					<span class="relative inline-flex rounded-full h-2 w-2 bg-green-500"></span>
				</span>
				{#if executionState?.isRunning}
					<span class="flex h-2 w-2 relative ml-1">
						<span class="animate-ping absolute inline-flex h-full w-full rounded-full bg-orange-400 opacity-75"></span>
						<span class="relative inline-flex rounded-full h-2 w-2 bg-orange-500"></span>
					</span>
				{/if}
			{/if}
		{:else}
			{#if isCard}
				<Zap class="w-3.5 h-3.5" />
			{:else}
				<svg xmlns="http://www.w3.org/2000/svg" width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2">
					<path d="M13 2L3 14h9l-1 8 10-12h-9l1-8z"/>
				</svg>
			{/if}
			<span class={label}>Activate</span>
		{/if}
	</button>
{/snippet}

{#snippet resyncButton()}
	<button
		class="{btn} {btnDisabled} bg-amber-50 text-amber-700 border border-amber-300 hover:bg-amber-100"
		onclick={onResyncTrigger}
		disabled={triggerState?.isLoading}
		title="Project changed since activation. Resync to apply changes."
	>
		<svg xmlns="http://www.w3.org/2000/svg" width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round" class="w-3.5 h-3.5">
			<path d="M21 12a9 9 0 0 0-9-9 9.75 9.75 0 0 0-6.74 2.74L3 8"/>
			<path d="M3 3v5h5"/>
			<path d="M3 12a9 9 0 0 0 9 9 9.75 9.75 0 0 0 6.74-2.74L21 16"/>
			<path d="M16 16h5v5"/>
		</svg>
		<span class={label}>Resync</span>
	</button>
{/snippet}

{#snippet runStopButton()}
	{#if executionState?.isStarting}
		<button
			class="{btn} {btnDisabled} {isCard ? 'bg-primary text-primary-foreground' : 'px-6 bg-zinc-900 border-zinc-900 text-white shadow'}"
			disabled
		>
			<Loader2 class="w-3.5 h-3.5 animate-spin" />
			<span class={label}>Starting...</span>
		</button>
	{:else if executionState?.isRunning && onStop}
		{#if executionState?.isStopping}
			<button
				class="{btn} {btnDisabled} {isCard ? 'bg-destructive text-destructive-foreground' : 'bg-red-50 text-red-600 border border-red-200'}"
				disabled
			>
				<Loader2 class="w-3.5 h-3.5 animate-spin" />
				<span class={label}>Stopping...</span>
			</button>
		{:else}
			<button
				class="{btn} {isCard ? 'bg-destructive text-destructive-foreground hover:bg-destructive/90' : 'bg-red-50 text-red-600 border border-red-200 hover:bg-red-100'}"
				onclick={onStop}
			>
				<Square class="w-3.5 h-3.5" />
				<span class={label}>Stop</span>
			</button>
		{/if}
	{:else if onRun}
		<button
			class="{btn} {btnDisabled} {isCard ? 'bg-primary text-primary-foreground hover:bg-primary/90' : 'px-6 bg-zinc-900 border-zinc-900 text-white shadow hover:bg-zinc-800'}"
			onclick={() => onRun()}
			disabled={nodeCount === 0 || executionState?.isRunning || infraBlocking || disabled}
		>
			<Play class="w-3.5 h-3.5" />
			<span class={label}>{isCard ? 'Run' : 'Run Project'}</span>
		</button>
	{/if}
{/snippet}
