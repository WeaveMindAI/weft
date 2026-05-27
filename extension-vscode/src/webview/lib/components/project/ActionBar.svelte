<script lang="ts">
	import { Play, Square, Zap, Database, Loader2 } from '@lucide/svelte';
	import type {
		ActionBarState,
		ActionAvailability,
		ActionVerb,
		BackendSnapshot,
		ActionBarOverlay,
		CliPhase,
	} from '../../../../shared/protocol';

	let {
		state,
		drift,
		onRun,
		onActivate,
		onCancelActivate,
		onDeactivate,
		onReactivate,
		onCancelRunning,
		onResumeActive,
		onResync,
		onStartInfra,
		onStopInfra,
		onTerminateInfra,
		onUpgradeInfra,
		onStop,
		onDismissError,
		onToggleInfraSubgraph,
		onToggleTriggerSubgraph,
		showInfraSubgraph = false,
		showTriggerSubgraph = false,
		nodeCount = 1,
		hasInfra = false,
		hasTriggers = false,
	}: {
		// Single source of truth. The host's ActionBarStore pushes
		// every transition; this component is a pure renderer.
		// `backend` is always present, `overlay` carries the current
		// user-action layer, `error` sits as a sticky banner.
		state: ActionBarState;
		// Drift signals stay live regardless of overlay. Lights the
		// Resync / Upgrade buttons; resolved on the next status fetch.
		drift: ActionAvailability | undefined;
		// Verb-action callbacks. The webview never decides which
		// verb to run from the state; it just calls the right handler
		// when the user clicks. The host decides scope (infra vs
		// trigger) based on graph contents at the time of the click.
		onRun?: () => void;
		onActivate?: () => void;
		// Mid-activate: cancel TriggerSetup, wipe partial signals,
		// flip back to Inactive.
		onCancelActivate?: () => void;
		onDeactivate?: () => void;
		// Inactive-with-preserved-state path. Host opens the
		// 3-option reactivate-choice dialog before posting activate.
		onReactivate?: () => void;
		// Mid-deactivate: cancel any still-running executions to
		// unblock the drain.
		onCancelRunning?: () => void;
		// Mid-deactivate: roll back to active. Anything that parked
		// during the transient drains immediately.
		onResumeActive?: () => void;
		onResync?: () => void;
		onStartInfra?: () => void;
		onStopInfra?: () => void;
		onTerminateInfra?: () => void;
		onUpgradeInfra?: () => void;
		// Generic Stop. The host inspects the bar overlay to decide
		// whether to kill the CLI process group or POST cancel on
		// the active execution.
		onStop?: () => void;
		// X-button on the error banner. Webview posts `dismissError`;
		// the host clears the slot's error so the banner stops
		// rendering until the next failure.
		onDismissError?: () => void;
		onToggleInfraSubgraph?: () => void;
		onToggleTriggerSubgraph?: () => void;
		showInfraSubgraph?: boolean;
		showTriggerSubgraph?: boolean;
		// Used to grey out Run when the graph is empty (no parser
		// success yet); not part of backend state.
		nodeCount?: number;
		// Static graph-derived: does this project DECLARE infra /
		// trigger nodes in source. Comes from the parsed project,
		// independent of backend state. Drives section visibility
		// (don't show Infra section if no infra nodes exist).
		hasInfra?: boolean;
		hasTriggers?: boolean;
	} = $props();

	// ─── Convenience accessors (all backend-side facts) ──────────
	const backend = $derived<BackendSnapshot>(state.backend);
	const overlay = $derived<ActionBarOverlay>(state.overlay);

	// Verbs the dispatcher will currently accept. Stays the same
	// across overlays (cli_running etc): stale-but-known beats
	// blank for disabled-state derivation.
	const isVerbAvailable = (v: ActionVerb): boolean => backend.available.includes(v);

	// Drift bits.
	const sourceDrift = $derived(drift?.sourceDrift ?? false);
	const infraDrift = $derived(drift?.infraDrift ?? false);
	const hasPreservedState = $derived.by((): boolean => {
		const p = drift?.preservation;
		return !!p && (p.parked + p.suspended) > 0;
	});

	// Source-derived visibility flags.
	const infraExists = $derived(hasInfra || backend.infraRollup !== 'none');

	// ─── Overlay accessors ───────────────────────────────────────
	const cliVerb = $derived(overlay.kind === 'cli_running' ? overlay.verb : undefined);
	const cliPhase = $derived(overlay.kind === 'cli_running' ? overlay.phase : undefined);
	const pendingMessage = $derived(overlay.kind === 'pending' ? overlay.message : undefined);
	const isExecRunning = $derived(overlay.kind === 'execution_running');

	/// Label for the CLI-running spinner.
	///
	/// Mirrors the trigger flow: the spinner's intent comes from the
	/// VERB the user clicked (Stop / Terminate / Start / Activate
	/// / Restart), not from the phase. Phases are progress within a
	/// verb, used here only when they carry information that swaps
	/// the wording mid-flight (e.g. "Building..." → "Loading...").
	///
	/// `verb` is always set whenever `phase` is set (both come from
	/// the same `cli_running` overlay).
	function cliPhaseLabel(
		phase: CliPhase | undefined,
		verb: ActionVerb | undefined,
	): string {
		if (phase === undefined) return '';
		// Phase-specific overrides for stages that have their own
		// dedicated user-facing wording.
		switch (phase) {
			case 'build_start': return 'Building...';
			case 'build_skip': return 'Cached, loading...';
			case 'build_done': return 'Loading...';
			case 'image_push_start':
			case 'image_push_done': return 'Loading image...';
			case 'infra_provision_start':
			case 'infra_provision_done': return 'Provisioning infra...';
			case 'trigger_register_start':
			case 'trigger_register_done': return 'Registering triggers...';
		}
		// Default: derive from the verb. Covers the dispatcher-call
		// phases and any future phase that doesn't have its own
		// override above.
		switch (verb) {
			case 'infra_start': return 'Starting infra...';
			case 'infra_restart': return 'Restarting infra...';
			case 'infra_stop': return 'Stopping infra...';
			case 'infra_terminate': return 'Terminating infra...';
			case 'infra_upgrade': return 'Upgrading infra...';
			case 'infra_node_stop': return 'Stopping node...';
			case 'infra_node_terminate': return 'Terminating node...';
			case 'activate':
			case 'reactivate':
			case 'resume_active': return 'Activating triggers...';
			case 'cancel_activate': return 'Cancelling activate...';
			case 'deactivate': return 'Deactivating triggers...';
			case 'cancel_running': return 'Cancelling running...';
			case 'resync': return 'Resyncing...';
			case 'run': return 'Running...';
			case 'build': return 'Building...';
			case 'rm': return 'Removing...';
		}
		// A phase set with no verb (the type allows `verb: undefined` even
		// though in practice a cli_running overlay carries both): fall back to a
		// generic label rather than nothing.
		if (verb === undefined) return 'Working...';
		// `verb` is `never` here once every ActionVerb variant is covered above;
		// adding a new verb to the union forces a compile error here.
		const _exhaustive: never = verb;
		return _exhaustive;
	}

	// ─── Slot-state derivations ──────────────────────────────────
	//
	// Three slots, three exhaustive discriminated unions. Each slot
	// projects (backend, overlay, source-flags) -> render-time state.
	// The match arms in the template enumerate every variant; new
	// states land as new variants and TypeScript yells about missing
	// arms.

	type InfraSlotState =
		| { kind: 'absent' }
		| { kind: 'cli_working'; phase: CliPhase }
		| {
			kind: 'rollup';
			rollup: BackendSnapshot['infraRollup'];
			canStart: boolean;
			canStop: boolean;
			canTerminate: boolean;
			canUpgrade: boolean;
			showDrift: boolean;
		};

	type MiddleSlotState =
		| { kind: 'absent' }
		| { kind: 'cli_working'; phase: CliPhase }
		| { kind: 'pending'; message: string }
		| { kind: 'stop_execution' }
		| { kind: 'run'; enabled: boolean };

	type TriggerSlotState =
		| { kind: 'absent' }
		| { kind: 'cli_working'; phase: CliPhase }
		| { kind: 'active'; canDeactivate: boolean; showDrift: boolean }
		| { kind: 'activating'; canCancel: boolean }
		| { kind: 'deactivating'; runningCount: number; canCancel: boolean; canResume: boolean }
		| { kind: 'reactivate'; mode: string; enabled: boolean }
		| { kind: 'inactive_fresh'; enabled: boolean };

	const infraSlot = $derived.by((): InfraSlotState => {
		if (!infraExists) return { kind: 'absent' };
		// Spinner if a CLI infra verb is in flight.
		if (cliVerb && cliPhase !== undefined && isInfraVerb(cliVerb)) {
			return { kind: 'cli_working', phase: cliPhase };
		}
		const anyOverlayBlocks = overlay.kind === 'cli_running' || overlay.kind === 'pending';
		return {
			kind: 'rollup',
			rollup: backend.infraRollup,
			canStart: !anyOverlayBlocks && nodeCount > 0 && isVerbAvailable('infra_start'),
			canStop: !anyOverlayBlocks && isVerbAvailable('infra_stop'),
			// Trust the dispatcher's available_actions verbatim (it's
			// the state machine). No redundant rollup re-gate: the old
			// `rollup === running|stopped` check wrongly hid Terminate
			// in `partial` (per-unit mixed state).
			canTerminate: !anyOverlayBlocks && isVerbAvailable('infra_terminate'),
			canUpgrade: !anyOverlayBlocks && infraDrift && isVerbAvailable('infra_upgrade'),
			showDrift: infraDrift,
		};
	});

	// Trigger slot is visible whenever EITHER the source declares
	// triggers OR the backend reports a non-trivial trigger lifecycle.
	// That covers the "deleted the trigger from source but backend
	// still has it active" case: the user needs Deactivate (and
	// Resync) to bring backend back in line with source.
	const triggerSlotVisible = $derived(
		hasTriggers
			|| backend.status === 'active'
			|| backend.status === 'activating'
			|| backend.status === 'deactivating'
			|| hasPreservedState
	);

	const middleSlot = $derived.by((): MiddleSlotState => {
		// CLI run/build claims the spinner.
		if (cliVerb && cliPhase !== undefined && isMiddleVerb(cliVerb)) {
			return { kind: 'cli_working', phase: cliPhase };
		}
		// HTTP-driven action awaiting backend confirmation.
		if (pendingMessage !== undefined) {
			return { kind: 'pending', message: pendingMessage };
		}
		// Currently following a live execution: show Stop Execution
		// regardless of project shape (triggered projects still
		// fire executions that the user may want to cancel).
		if (isExecRunning) {
			return { kind: 'stop_execution' };
		}
		// No execution in flight: only show Run when the trigger
		// slot is hidden. Whenever the trigger slot is visible
		// (source has triggers, or backend still has them active /
		// deactivating / preserved), the trigger lifecycle is the
		// right entry point and Run would conflict.
		if (triggerSlotVisible) {
			return { kind: 'absent' };
		}
		const anyOverlayBlocks = overlay.kind === 'cli_running' || overlay.kind === 'pending';
		// Source-derived gate: if the graph has infra nodes, Run is
		// only legal once every infra node is Running. This is
		// defense-in-depth on top of the dispatcher's own
		// `available_actions` check: the dispatcher gate fails when
		// the project is unregistered (status fetch errors out), so
		// we re-derive from the parsed graph + last-known rollup.
		const infraReady = !hasInfra || backend.infraRollup === 'running';
		return {
			kind: 'run',
			enabled:
				!anyOverlayBlocks
				&& nodeCount > 0
				&& infraReady
				&& isVerbAvailable('run'),
		};
	});

	const triggerSlot = $derived.by((): TriggerSlotState => {
		if (!triggerSlotVisible) return { kind: 'absent' };
		// Spinner if a CLI trigger verb is in flight.
		if (cliVerb && cliPhase !== undefined && isTriggerVerb(cliVerb)) {
			return { kind: 'cli_working', phase: cliPhase };
		}
		const anyOverlayBlocks = overlay.kind === 'cli_running' || overlay.kind === 'pending';
		switch (backend.status) {
			case 'active':
				return {
					kind: 'active',
					canDeactivate: !anyOverlayBlocks && nodeCount > 0 && isVerbAvailable('deactivate'),
					showDrift: sourceDrift && isVerbAvailable('resync'),
				};
			case 'activating':
				return {
					kind: 'activating',
					canCancel: !anyOverlayBlocks && isVerbAvailable('cancel_activate'),
				};
			case 'deactivating':
				return {
					kind: 'deactivating',
					runningCount: backend.runningCount,
					canCancel: !anyOverlayBlocks && backend.runningCount > 0
						&& isVerbAvailable('cancel_running'),
					canResume: !anyOverlayBlocks && isVerbAvailable('resume_active'),
				};
			case 'inactive':
			case 'registered':
				if (hasPreservedState && isVerbAvailable('reactivate')) {
					return {
						kind: 'reactivate',
						mode: backend.mode,
						enabled: !anyOverlayBlocks && nodeCount > 0 && isVerbAvailable('reactivate'),
					};
				}
				return {
					kind: 'inactive_fresh',
					enabled: !anyOverlayBlocks && nodeCount > 0
						&& isVerbAvailable('activate'),
				};
			case 'unknown':
			default:
				return {
					kind: 'inactive_fresh',
					enabled: !anyOverlayBlocks && nodeCount > 0
						&& isVerbAvailable('activate'),
				};
		}
	});

	function isInfraVerb(v: ActionVerb): boolean {
		return v === 'infra_start' || v === 'infra_restart' || v === 'infra_stop'
			|| v === 'infra_terminate' || v === 'infra_upgrade'
			|| v === 'infra_node_stop' || v === 'infra_node_terminate';
	}
	function isMiddleVerb(v: ActionVerb): boolean {
		return v === 'run';
	}
	function isTriggerVerb(v: ActionVerb): boolean {
		return v === 'activate' || v === 'cancel_activate'
			|| v === 'deactivate' || v === 'reactivate'
			|| v === 'resume_active' || v === 'cancel_running'
			|| v === 'resync';
	}


	// CSS helpers for the floating action bar.
	const btn = 'flex items-center gap-2 px-3 py-1.5 rounded-lg transition-colors';
	const btnDisabled = 'disabled:opacity-50 disabled:cursor-not-allowed';
	const labelCss = 'font-medium text-[11px] uppercase tracking-wider';

	const errorMessage = $derived(state.error?.message);
	const errorVerb = $derived(state.error?.verb);
</script>

<div class="absolute bottom-6 left-1/2 -translate-x-1/2 flex flex-col items-center gap-1 z-20">

	<!-- Sticky error banner. Lives independently of the slots; cleared
	     by the dismiss X or by a successful idle push. -->
	{#if errorMessage}
		<div class="flex items-center gap-2 px-3 py-1.5 bg-red-50 border border-red-200 rounded-lg text-xs text-red-700 shadow-sm">
			<svg xmlns="http://www.w3.org/2000/svg" width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><circle cx="12" cy="12" r="10"/><line x1="12" y1="8" x2="12" y2="12"/><line x1="12" y1="16" x2="12.01" y2="16"/></svg>
			<span class="font-medium">{errorVerb}:</span>
			<span class="truncate max-w-[400px]" title={errorMessage}>{errorMessage}</span>
			<button
				type="button"
				class="ml-1 p-0.5 rounded hover:bg-red-100 text-red-700"
				title="Dismiss"
				aria-label="Dismiss error"
				onclick={onDismissError}
			>
				<svg xmlns="http://www.w3.org/2000/svg" width="12" height="12" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2.5" stroke-linecap="round"><line x1="18" y1="6" x2="6" y2="18"/><line x1="6" y1="6" x2="18" y2="18"/></svg>
			</button>
		</div>
	{/if}

	<div class="flex items-center gap-1.5 p-1.5 bg-white border border-zinc-200 rounded-xl shadow-xl backdrop-blur-md">

		<!-- ════════ INFRA SLOT (left) ════════ -->
		{#if infraSlot.kind !== 'absent'}
			<div class="flex items-center">
				{@render infraSlotButtons(infraSlot)}
				{#if onToggleInfraSubgraph}
					<div class="w-px h-5 bg-zinc-200 mx-1.5"></div>
					<button
						class="flex items-center justify-center w-7 h-7 rounded-lg transition-colors {showInfraSubgraph ? 'bg-blue-100 text-blue-600 border border-blue-200' : 'bg-white text-zinc-400 border border-zinc-200 hover:bg-zinc-50'}"
						onclick={onToggleInfraSubgraph}
						title={showInfraSubgraph ? 'Hide infrastructure subgraph' : 'Show infrastructure subgraph'}
					>
						{@render eyeIcon(showInfraSubgraph)}
					</button>
				{/if}
			</div>
		{/if}

		<!-- ════════ MIDDLE SLOT (run / stop-exec / pending / cli) ════════ -->
		{#if middleSlot.kind !== 'absent'}
			{#if infraSlot.kind !== 'absent'}
				<div class="w-px h-6 bg-zinc-200 mx-1"></div>
			{/if}
			{@render middleSlotButton(middleSlot)}
		{/if}

		<!-- ════════ TRIGGER SLOT (right) ════════ -->
		{#if triggerSlot.kind !== 'absent'}
			{#if infraSlot.kind !== 'absent' || middleSlot.kind !== 'absent'}
				<div class="w-px h-6 bg-zinc-200 mx-1"></div>
			{/if}
			{@render triggerSlotButton(triggerSlot)}
			{#if onToggleTriggerSubgraph}
				<button
					class="flex items-center justify-center w-7 h-7 rounded-lg transition-colors {showTriggerSubgraph ? 'bg-emerald-100 text-emerald-600 border border-emerald-200' : 'bg-white text-zinc-400 border border-zinc-200 hover:bg-zinc-50'}"
					onclick={onToggleTriggerSubgraph}
					title={showTriggerSubgraph ? 'Hide trigger subgraph' : 'Show trigger subgraph'}
				>
					{@render eyeIcon(showTriggerSubgraph)}
				</button>
			{/if}

			<!-- Trigger drift addendum: only meaningful in `active` state. -->
			{#if triggerSlot.kind === 'active' && triggerSlot.showDrift}
				<div class="w-px h-6 bg-amber-300 mx-1"></div>
				<span class="text-[10px] font-medium text-amber-600 uppercase tracking-wider whitespace-nowrap">Out of sync</span>
				{@render resyncButton()}
			{/if}
		{/if}
	</div>
</div>

<!-- ════════════════════════════════════════════════════════════
     Snippets. Each match arm renders one variant of one slot.
     New variants land here next to a new arm in the slot's
     discriminated union; TypeScript catches missing arms.
     ════════════════════════════════════════════════════════════ -->

{#snippet eyeIcon(open: boolean)}
	<svg xmlns="http://www.w3.org/2000/svg" width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round">
		{#if open}
			<path d="M1 12s4-8 11-8 11 8 11 8-4 8-11 8-11-8-11-8z"/><circle cx="12" cy="12" r="3"/>
		{:else}
			<path d="M17.94 17.94A10.07 10.07 0 0 1 12 20c-7 0-11-8-11-8a18.45 18.45 0 0 1 5.06-5.94M9.9 4.24A9.12 9.12 0 0 1 12 4c7 0 11 8 11 8a18.5 18.5 0 0 1-2.16 3.19m-6.72-1.07a3 3 0 1 1-4.24-4.24"/><line x1="1" y1="1" x2="23" y2="23"/>
		{/if}
	</svg>
{/snippet}

{#snippet workingButton(text: string, stopAffordance: boolean)}
	<button
		class="{btn} bg-red-50 text-red-600 border border-red-200 hover:bg-red-100"
		onclick={stopAffordance ? onStop : undefined}
		disabled={!stopAffordance}
		title={stopAffordance ? 'Stop the in-flight action' : undefined}
	>
		<Loader2 class="w-3.5 h-3.5 animate-spin" />
		<span class={labelCss}>{text}{stopAffordance ? ' (Stop)' : ''}</span>
	</button>
{/snippet}

{#snippet infraSlotButtons(slot: InfraSlotState)}
	{#if slot.kind === 'absent'}
		<!-- never rendered: outer guard skips the section -->
	{:else if slot.kind === 'cli_working'}
		{@render workingButton(cliPhaseLabel(slot.phase, cliVerb), true)}
	{:else}
		{#if slot.rollup === 'stopping' || slot.rollup === 'terminating' || slot.rollup === 'provisioning'}
			<!-- Supervisor-driven transients: the dispatcher emits NO
			     infra verbs for these (it expects the bar to render a
			     spinner from the rollup alone). No user-actionable
			     button; not a CLI verb, so no Stop affordance. -->
			{@render workingButton(
				slot.rollup === 'stopping'
					? 'Stopping infra...'
					: slot.rollup === 'terminating'
						? 'Terminating infra...'
						: 'Provisioning infra...',
				false,
			)}
		{:else if slot.rollup === 'running'}
			<button class="{btn} {btnDisabled} bg-zinc-100 text-zinc-700 border border-zinc-200 hover:bg-zinc-200" onclick={onStopInfra} disabled={!slot.canStop}>
				<Square class="w-3.5 h-3.5" />
				<span class={labelCss}>Stop Infra</span>
				<span class="flex h-1.5 w-1.5 relative ml-0.5">
					<span class="animate-ping absolute inline-flex h-full w-full rounded-full bg-green-400 opacity-75"></span>
					<span class="relative inline-flex rounded-full h-1.5 w-1.5 bg-green-500"></span>
				</span>
			</button>
			{#if slot.canUpgrade && onUpgradeInfra}
				<div class="w-px h-5 bg-amber-300 mx-1"></div>
				<button
					class="{btn} bg-amber-50 text-amber-700 border border-amber-300 hover:bg-amber-100"
					onclick={onUpgradeInfra}
					title="Source changed since infra started; rebuild infra images"
				>
					<svg xmlns="http://www.w3.org/2000/svg" width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><path d="M21 12a9 9 0 0 0-9-9 9.75 9.75 0 0 0-6.74 2.74L3 8"/><path d="M3 3v5h5"/><path d="M3 12a9 9 0 0 0 9 9 9.75 9.75 0 0 0 6.74-2.74L21 16"/><path d="M16 16h5v5"/></svg>
					<span class={labelCss}>Upgrade Infra</span>
				</button>
			{/if}
		{:else}
			<!-- Non-transient, non-running ('none', 'stopped',
			     'partial', 'failed', 'flaky') all present "Start
			     Infra" as the next step. The dispatcher's
			     available_actions decides whether the button is
			     enabled; the rollup decides the rendering siblings
			     (trash button visibility, drift dot). -->
			<button
				class="{btn} {btnDisabled} bg-blue-50 text-blue-600 border border-blue-200 hover:bg-blue-100"
				onclick={onStartInfra}
				disabled={!slot.canStart}
			>
				<Database class="w-3.5 h-3.5" />
				<span class={labelCss}>Start Infra</span>
			</button>
		{/if}
		{#if slot.canTerminate && onTerminateInfra}
			<button
				class="{btn} ml-1 bg-red-50 text-red-600 border border-red-200 hover:bg-red-100"
				onclick={onTerminateInfra}
				title="Terminate Infra"
			>
				<svg xmlns="http://www.w3.org/2000/svg" width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><path d="M3 6h18"/><path d="M19 6v14c0 1-1 2-2 2H7c-1 0-2-1-2-2V6"/><path d="M8 6V4c0-1 1-2 2-2h4c1 0 2 1 2 2v2"/></svg>
			</button>
		{/if}
	{/if}
{/snippet}

{#snippet middleSlotButton(slot: MiddleSlotState)}
	{#if slot.kind === 'absent'}
		<!-- never rendered: outer guard skips the section -->
	{:else if slot.kind === 'cli_working'}
		{@render workingButton(cliPhaseLabel(slot.phase, cliVerb), true)}
	{:else if slot.kind === 'pending'}
		{@render workingButton(slot.message, false)}
	{:else if slot.kind === 'stop_execution'}
		<button
			class="{btn} bg-orange-50 text-orange-600 border border-orange-200 hover:bg-orange-100"
			onclick={onStop}
		>
			<Square class="w-3.5 h-3.5" />
			<span class={labelCss}>Stop Execution</span>
		</button>
	{:else if slot.kind === 'run' && onRun}
		<button
			class="{btn} {btnDisabled} px-6 bg-zinc-900 border-zinc-900 text-white shadow hover:bg-zinc-800"
			onclick={onRun}
			disabled={!slot.enabled}
		>
			<Play class="w-3.5 h-3.5" />
			<span class={labelCss}>Run Project</span>
		</button>
	{/if}
{/snippet}

{#snippet triggerSlotButton(slot: TriggerSlotState)}
	{#if slot.kind === 'absent'}
		<!-- never rendered: outer guard skips the section -->
	{:else if slot.kind === 'cli_working'}
		{@render workingButton(cliPhaseLabel(slot.phase, cliVerb), true)}
	{:else if slot.kind === 'active'}
		<button
			class="{btn} {btnDisabled} bg-emerald-600 border-emerald-600 text-white hover:bg-emerald-700"
			onclick={onDeactivate}
			disabled={!slot.canDeactivate}
		>
			<svg xmlns="http://www.w3.org/2000/svg" width="14" height="14" viewBox="0 0 24 24" fill="currentColor"><rect x="6" y="6" width="12" height="12" rx="1"/></svg>
			<span class={labelCss}>Deactivate</span>
			<span class="flex h-2 w-2 relative ml-1">
				<span class="animate-ping absolute inline-flex h-full w-full rounded-full bg-green-400 opacity-75"></span>
				<span class="relative inline-flex rounded-full h-2 w-2 bg-green-500"></span>
			</span>
		</button>
	{:else if slot.kind === 'activating'}
		<div class="flex items-center gap-1.5">
			<span class="px-2 py-1 rounded-md bg-amber-50 text-amber-700 border border-amber-200 text-[11px] font-medium inline-flex items-center gap-1.5">
				<svg class="animate-spin h-3 w-3 text-amber-700" xmlns="http://www.w3.org/2000/svg" fill="none" viewBox="0 0 24 24">
					<circle class="opacity-25" cx="12" cy="12" r="10" stroke="currentColor" stroke-width="3"/>
					<path class="opacity-75" fill="currentColor" d="M4 12a8 8 0 018-8v3a5 5 0 00-5 5H4z"/>
				</svg>
				Activating
			</span>
			{#if slot.canCancel}
				<button
					class="{btn} {btnDisabled} bg-red-50 text-red-600 border border-red-200 hover:bg-red-100"
					onclick={onCancelActivate}
					title="Cancel the in-flight activate; partial trigger registrations get wiped"
				>
					<Square class="w-3.5 h-3.5" />
					<span class={labelCss}>Cancel</span>
				</button>
			{/if}
		</div>
	{:else if slot.kind === 'deactivating'}
		<div class="flex items-center gap-1.5">
			<span class="px-2 py-1 rounded-md bg-amber-50 text-amber-700 border border-amber-200 text-[11px] font-medium">
				Deactivating ({slot.runningCount} running)
			</span>
			{#if slot.canCancel}
				<button
					class="{btn} {btnDisabled} bg-red-50 text-red-600 border border-red-200 hover:bg-red-100"
					onclick={onCancelRunning}
					title="Kill running executions to finish deactivating now"
				>
					<Square class="w-3.5 h-3.5" />
					<span class={labelCss}>Cancel Running</span>
				</button>
			{/if}
			{#if slot.canResume}
				<button
					class="{btn} {btnDisabled} bg-zinc-900 text-white hover:bg-zinc-800"
					onclick={onResumeActive}
					title="Roll back to active; new fires resume immediately"
				>
					<Zap class="w-3.5 h-3.5" />
					<span class={labelCss}>Resume Active</span>
				</button>
			{/if}
		</div>
	{:else if slot.kind === 'reactivate'}
		<div class="flex items-center gap-1.5">
			<span class="px-2 py-1 rounded-md bg-zinc-100 text-zinc-600 border border-zinc-200 text-[11px] font-medium capitalize">
				{slot.mode}
			</span>
			<button
				class="{btn} {btnDisabled} bg-zinc-900 text-white hover:bg-zinc-800"
				onclick={onReactivate}
				disabled={!slot.enabled}
				title="Reactivate; choose how to handle preserved state"
			>
				<svg xmlns="http://www.w3.org/2000/svg" width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><path d="M13 2L3 14h9l-1 8 10-12h-9l1-8z"/></svg>
				<span class={labelCss}>Reactivate</span>
			</button>
		</div>
	{:else}
		<button
			class="{btn} {btnDisabled} bg-zinc-900 text-white hover:bg-zinc-800"
			onclick={onActivate}
			disabled={!slot.enabled}
		>
			<svg xmlns="http://www.w3.org/2000/svg" width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><path d="M13 2L3 14h9l-1 8 10-12h-9l1-8z"/></svg>
			<span class={labelCss}>Activate</span>
		</button>
	{/if}
{/snippet}

{#snippet resyncButton()}
	<button
		class="{btn} {btnDisabled} bg-amber-50 text-amber-700 border border-amber-300 hover:bg-amber-100"
		onclick={onResync}
		disabled={!isVerbAvailable('resync')}
		title="Project changed since activation. Resync to apply changes."
	>
		<svg xmlns="http://www.w3.org/2000/svg" width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round" class="w-3.5 h-3.5">
			<path d="M21 12a9 9 0 0 0-9-9 9.75 9.75 0 0 0-6.74 2.74L3 8"/>
			<path d="M3 3v5h5"/>
			<path d="M3 12a9 9 0 0 0 9 9 9.75 9.75 0 0 0 6.74-2.74L21 16"/>
			<path d="M16 16h5v5"/>
		</svg>
		<span class={labelCss}>Resync</span>
	</button>
{/snippet}

