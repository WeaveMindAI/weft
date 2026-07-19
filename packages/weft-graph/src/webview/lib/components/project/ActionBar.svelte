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
	import ErrorDetailsModal from './ErrorDetailsModal.svelte';

	let {
		state: barState,
		drift,
		onRun,
		onActivate,
		onCancelActivate,
		onCancelBuild,
		onCancelInfra,
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
		// Mid-build (backend transition=building): cancel the
		// build. Cancel reconciles, never asserts: the
		// displayed state is whatever the backend reports next.
		onCancelBuild?: () => void;
		// Mid-infra-flip (rollup provisioning/stopping/terminating):
		// HALT the in-flight infra work; per-node partial state stays.
		onCancelInfra?: () => void;
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
	const backend = $derived<BackendSnapshot>(barState.backend);
	const overlay = $derived<ActionBarOverlay>(barState.overlay);

	// STARTER verbs: the ones whose action BUILDS + registers on demand (run,
	// activate/reactivate, infra_start). These are ALWAYS clickable when the graph
	// shape permits them (their slot's shape gate below still applies), NEVER greyed
	// on "no build yet": clicking one does whatever it needs (compile, build, load,
	// register) to make itself happen. So they do NOT consult `backend.available`
	// (which is empty on a fresh/unbuilt project); the backend either performs the
	// verb or returns an error the host surfaces as a toast.
	const STARTER_VERBS = new Set<ActionVerb>([
		'run',
		'activate',
		'reactivate',
		'infra_start',
	]);

	// Whether a verb is currently offered. Starter verbs: always (shape-gated only).
	// State-dependent verbs (deactivate, cancel_*, resume_active, infra_stop/
	// terminate/upgrade, resync): gated on the dispatcher's live-lifecycle
	// `backend.available` (e.g. deactivate only when Active, infra_stop only when
	// infra is running), which is genuine current state, not a built-artifact check.
	const isVerbAvailable = (v: ActionVerb): boolean =>
		STARTER_VERBS.has(v) || backend.available.includes(v);

	// Drift bits. The dispatcher reports three independent signals;
	// each is resolved by its own verb.
	//   `binaryDrift`    → `build` (worker image inputs changed)
	//   `definitionDrift` → `resync` (runtime project shape changed)
	//   `infraDrift`     → `infra_start` / upgrade (infra closure changed)
	// Today the action bar still lights a single "Resync" affordance
	// when EITHER binary or definition drifts (the user-facing label
	// covers both, and the verb is `resync`); `sourceDrift` is the
	// derived OR until the action bar gets a dedicated Rebuild slot.
	const binaryDrift = $derived(drift?.binaryDrift ?? false);
	const definitionDrift = $derived(drift?.definitionDrift ?? false);
	const sourceDrift = $derived(binaryDrift || definitionDrift);
	const infraDrift = $derived(drift?.infraDrift ?? false);
	const hasPreservedState = $derived.by((): boolean => {
		const p = drift?.preservation;
		return !!p && (p.parked + p.suspended) > 0;
	});

	// Source-derived visibility flags, OR-ed with the backend's live
	// state. `orphanedInfra` is the never-lose-track guarantee: live
	// infra whose node was deleted from source keeps the controls
	// visible (Model 1: it never gates run, so visibility is the only
	// thing standing between the user and forgotten billed infra).
	const infraExists = $derived(
		hasInfra || backend.infraRollup !== 'none' || backend.orphanedInfra,
	);

	// The build-transition axis. While not 'none', the whole bar is in
	// the unified transitional pattern: the slot that owns the verb
	// shows "Building... (cancel)" and every other verb is gated (the
	// backend rejects them anyway; greying is the courtesy).
	const buildTransition = $derived(backend.transition);

	// ─── Overlay accessors ───────────────────────────────────────
	const cliVerb = $derived(overlay.kind === 'cli_running' ? overlay.verb : undefined);
	const cliPhase = $derived(overlay.kind === 'cli_running' ? overlay.phase : undefined);
	// A `pending` overlay (an HTTP verb in flight) carries its VERB, so it belongs
	// to the SLOT that owns that verb, exactly like `cli_running`. Routing it to
	// the right slot is what keeps the button transforming IN PLACE (a pending
	// activate turns the trigger slot into "Activating...") instead of showing a
	// stray spinner in the middle slot NEXT TO the still-rendered Activate button.
	const pendingVerb = $derived(overlay.kind === 'pending' ? overlay.verb : undefined);
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
			case 'infra_stop': return 'Stopping infra...';
			case 'infra_terminate': return 'Terminating infra...';
			case 'infra_upgrade': return 'Upgrading infra...';
			case 'infra_cancel': return 'Cancelling infra...';
			case 'infra_node_stop': return 'Stopping node...';
			case 'infra_node_terminate': return 'Terminating node...';
			case 'activate':
			case 'reactivate':
			case 'resume_active': return 'Activating triggers...';
			case 'cancel_activate': return 'Cancelling activate...';
			case 'cancel_build': return 'Cancelling build...';
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
		// An HTTP infra verb POST in flight, before /status catches up.
		| { kind: 'pending'; message: string }
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
		| { kind: 'building'; cancelling: boolean }
		| { kind: 'pending'; message: string }
		| { kind: 'stop_execution' }
		| { kind: 'run'; enabled: boolean };

	type TriggerSlotState =
		| { kind: 'absent' }
		| { kind: 'cli_working'; phase: CliPhase }
		| { kind: 'building'; cancelling: boolean }
		| { kind: 'active'; canDeactivate: boolean; showDrift: boolean }
		| { kind: 'activating'; canCancel: boolean }
		| { kind: 'deactivating'; runningCount: number; canCancel: boolean; canResume: boolean }
		| { kind: 'reactivate'; mode: string; enabled: boolean }
		// An HTTP trigger verb POST in flight, before /status catches up: a
		// spinner with the verb's message, IN this slot (so no stray sibling).
		| { kind: 'pending'; message: string }
		| { kind: 'inactive_fresh'; enabled: boolean };

	// One gate for every user-actionable verb: an overlay is in flight
	// (CLI verb / pending HTTP verb) OR the backend reports a build
	// transition. Transitional = the only offered action is its cancel.
	const verbsBlocked = $derived(
		overlay.kind === 'cli_running'
			|| overlay.kind === 'pending'
			|| buildTransition !== 'none',
	);

	const infraSlot = $derived.by((): InfraSlotState => {
		if (!infraExists) return { kind: 'absent' };
		// Spinner if a CLI infra verb is in flight.
		if (cliVerb && cliPhase !== undefined && isInfraVerb(cliVerb)) {
			return { kind: 'cli_working', phase: cliPhase };
		}
		// A pending HTTP infra verb owns this slot: show its spinner in place.
		if (pendingVerb !== undefined && isInfraVerb(pendingVerb) && pendingMessage !== undefined) {
			return { kind: 'pending', message: pendingMessage };
		}
		return {
			kind: 'rollup',
			rollup: backend.infraRollup,
			canStart: !verbsBlocked && nodeCount > 0 && isVerbAvailable('infra_start'),
			canStop: !verbsBlocked && isVerbAvailable('infra_stop'),
			// Trust the dispatcher's available_actions verbatim (it's
			// the state machine). No redundant rollup re-gate: the old
			// `rollup === running|stopped` check wrongly hid Terminate
			// in `partial` (per-unit mixed state).
			canTerminate: !verbsBlocked && isVerbAvailable('infra_terminate'),
			canUpgrade: !verbsBlocked && infraDrift && isVerbAvailable('infra_upgrade'),
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
		// Backend-reported build. Owned by the trigger slot
		// when that slot is visible (an activate launched it there);
		// otherwise the run button owns it.
		if (buildTransition !== 'none' && !triggerSlotVisible) {
			return { kind: 'building', cancelling: buildTransition === 'cancelling_build' };
		}
		// HTTP-driven action awaiting backend confirmation: only when the pending
		// verb is a MIDDLE verb (run / cancel_build). A pending trigger or infra
		// verb is owned by its own slot, so it must NOT also spin here.
		if (pendingVerb !== undefined && isMiddleVerb(pendingVerb) && pendingMessage !== undefined) {
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
				!verbsBlocked
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
		// An HTTP trigger verb (activate / deactivate / ...) is POSTed and awaiting
		// the /status refresh: show the transitional spinner HERE (this slot owns
		// the verb), so the button transforms in place instead of leaving the
		// Activate button up while a spinner appears in the middle slot.
		if (pendingVerb !== undefined && isTriggerVerb(pendingVerb) && pendingMessage !== undefined) {
			return { kind: 'pending', message: pendingMessage };
		}
		// Backend-reported build launched from this slot
		// (an activate on a not-yet-built project).
		if (buildTransition !== 'none') {
			return { kind: 'building', cancelling: buildTransition === 'cancelling_build' };
		}
		switch (backend.status) {
			case 'active':
				return {
					kind: 'active',
					canDeactivate: !verbsBlocked && nodeCount > 0 && isVerbAvailable('deactivate'),
					showDrift: sourceDrift && isVerbAvailable('resync'),
				};
			case 'activating':
				return {
					kind: 'activating',
					canCancel: !verbsBlocked && isVerbAvailable('cancel_activate'),
				};
			case 'deactivating':
				return {
					kind: 'deactivating',
					runningCount: backend.runningCount,
					canCancel: !verbsBlocked && backend.runningCount > 0
						&& isVerbAvailable('cancel_running'),
					canResume: !verbsBlocked && isVerbAvailable('resume_active'),
				};
			case 'inactive':
			case 'registered':
				if (hasPreservedState && isVerbAvailable('reactivate')) {
					return {
						kind: 'reactivate',
						mode: backend.mode,
						enabled: !verbsBlocked && nodeCount > 0 && isVerbAvailable('reactivate'),
					};
				}
				return {
					kind: 'inactive_fresh',
					enabled: !verbsBlocked && nodeCount > 0
						&& isVerbAvailable('activate'),
				};
			case 'unknown':
			default:
				return {
					kind: 'inactive_fresh',
					enabled: !verbsBlocked && nodeCount > 0
						&& isVerbAvailable('activate'),
				};
		}
	});

	function isInfraVerb(v: ActionVerb): boolean {
		return v === 'infra_start' || v === 'infra_stop'
			|| v === 'infra_terminate' || v === 'infra_upgrade'
			|| v === 'infra_cancel'
			|| v === 'infra_node_stop' || v === 'infra_node_terminate';
	}
	function isMiddleVerb(v: ActionVerb): boolean {
		return v === 'run' || v === 'cancel_build';
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

	const errorMessage = $derived(barState.error?.message);
	const errorVerb = $derived(barState.error?.verb);
	const currentError = $derived(barState.error);
	let errorModalOpen: boolean = $state(false);
	$effect(() => {
		if (!currentError) errorModalOpen = false;
	});
</script>

<div class="absolute bottom-6 left-1/2 -translate-x-1/2 flex flex-col items-center gap-1 z-20">

	<!-- Sticky error banner. Whole banner is clickable: opens the
	     details modal. The trailing X dismisses the error entirely
	     (clears the banner). Lives independently of the slots; the
	     banner is also cleared by a successful idle push. -->
	{#if errorMessage}
		<div class="flex items-center gap-1 bg-red-50 border border-red-200 rounded-lg shadow-sm">
			<button
				type="button"
				class="flex items-center gap-2 px-3 py-1.5 text-xs text-red-700 hover:bg-red-100/60 rounded-l-lg"
				title="Click to see details"
				onclick={() => (errorModalOpen = true)}
			>
				<svg xmlns="http://www.w3.org/2000/svg" width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><circle cx="12" cy="12" r="10"/><line x1="12" y1="8" x2="12" y2="12"/><line x1="12" y1="16" x2="12.01" y2="16"/></svg>
				<span class="font-medium">{errorVerb}:</span>
				<span class="truncate max-w-[400px]">{errorMessage}</span>
				<span class="text-[10px] opacity-60 ml-1 underline">details</span>
			</button>
			<button
				type="button"
				class="mr-1 p-0.5 rounded hover:bg-red-100 text-red-700"
				title="Dismiss"
				aria-label="Dismiss error"
				onclick={onDismissError}
			>
				<svg xmlns="http://www.w3.org/2000/svg" width="12" height="12" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2.5" stroke-linecap="round"><line x1="18" y1="6" x2="6" y2="18"/><line x1="6" y1="6" x2="18" y2="18"/></svg>
			</button>
		</div>
		<ErrorDetailsModal
			error={barState.error}
			bind:open={errorModalOpen}
			onDismissError={() => onDismissError?.()}
		/>
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

<!-- THE one transitional pattern: the button that launched the action
     shows its "...-ing" label AND offers cancel, wired to the matching
     cancel verb. `onCancel` absent = the transitional state is not
     cancellable right now (e.g. cancel already requested); the button
     still shows the live state. Cancel reconciles, never asserts: the
     next backend snapshot decides what renders. -->
<!-- Default-param form, NOT `onCancel?`: snippet parameters live in the
     MARKUP, which no script preprocessor touches, so a TS optional marker
     passes verbatim into the compiled JS and breaks the build ("Expected ',',
     got '?'"). A default value types the same and compiles to valid JS. -->
{#snippet workingButton(text: string, onCancel: (() => void) | undefined = undefined)}
	<button
		class="{btn} bg-red-50 text-red-600 border border-red-200 hover:bg-red-100"
		onclick={onCancel}
		disabled={!onCancel}
		title={onCancel ? 'Cancel the in-flight action' : undefined}
	>
		<Loader2 class="w-3.5 h-3.5 animate-spin" />
		<span class={labelCss}>{text}{onCancel ? ' (Cancel)' : ''}</span>
	</button>
{/snippet}

{#snippet infraSlotButtons(slot: InfraSlotState)}
	{#if slot.kind === 'absent'}
		<!-- never rendered: outer guard skips the section -->
	{:else if slot.kind === 'cli_working'}
		{@render workingButton(cliPhaseLabel(slot.phase, cliVerb), onStop)}
	{:else if slot.kind === 'pending'}
		{@render workingButton(slot.message)}
	{:else}
		{#if slot.rollup === 'stopping' || slot.rollup === 'terminating' || slot.rollup === 'provisioning'}
			<!-- Supervisor-driven transients. The dispatcher offers
			     exactly one action here: infra_cancel (HALT the
			     in-flight work; per-node partial state stays visible
			     for per-node terminate/retry). -->
			{@render workingButton(
				slot.rollup === 'stopping'
					? 'Stopping infra...'
					: slot.rollup === 'terminating'
						? 'Terminating infra...'
						: 'Provisioning infra...',
				isVerbAvailable('infra_cancel') ? onCancelInfra : undefined,
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
		{@render workingButton(cliPhaseLabel(slot.phase, cliVerb), onStop)}
	{:else if slot.kind === 'building'}
		{@render workingButton(
			slot.cancelling ? 'Cancelling build...' : 'Building...',
			slot.cancelling ? undefined : onCancelBuild,
		)}
	{:else if slot.kind === 'pending'}
		{@render workingButton(slot.message, undefined)}
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
		{@render workingButton(cliPhaseLabel(slot.phase, cliVerb), onStop)}
	{:else if slot.kind === 'building'}
		{@render workingButton(
			slot.cancelling ? 'Cancelling build...' : 'Building...',
			slot.cancelling ? undefined : onCancelBuild,
		)}
	{:else if slot.kind === 'pending'}
		{@render workingButton(slot.message)}
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
		{@render workingButton(
			'Activating...',
			slot.canCancel ? onCancelActivate : undefined,
		)}
	{:else if slot.kind === 'deactivating'}
		<div class="flex items-center gap-1.5">
			{@render workingButton(
				`Deactivating (${slot.runningCount} running)...`,
				slot.canCancel ? onCancelRunning : undefined,
			)}
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

