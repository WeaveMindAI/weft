<script lang="ts">
	import type { Snippet } from 'svelte';
	import { Square, Loader2, Terminal } from '@lucide/svelte';
	import type {
		ActionBarState,
		ActionAvailability,
		ActionVerb,
		BarPhase,
		SourceLocation,
	} from '../../../../protocol';
	import ActionDetailsModal from './ActionDetailsModal.svelte';
	import { resyncStopped, type VerbInputs } from '../../verb-gates';

	// The bottom bar: what is going on right now (the run you follow with
	// its Stop, a build, a verb working, a setup or an infra change under
	// way with its cancel), and in the same bar what the selected nodes
	// offer (`selection`). Every lifecycle verb lives there, acting on the
	// selected nodes. With nothing going on, nothing selected and no
	// subgraph to show, no bar is drawn.

	let {
		state: barState,
		status,
		runProgress,
		selection,
		onCancelActivate,
		onCancelBuild,
		onCancelInfra,
		onResumeResync,
		onStop,
		onDismissError,
		onOpenLocation,
		onToggleInfraSubgraph,
		onToggleTriggerSubgraph,
		showInfraSubgraph = false,
		showTriggerSubgraph = false,
		hasInfra = false,
		hasTriggers = false,
	}: {
		// Single source of truth. The host's ActionBarStore pushes
		// every transition; this component is a pure renderer.
		state: ActionBarState;
		/// The latest status answer: which triggers are turning on.
		status: ActionAvailability | undefined;
		/// How far the followed run got, when one is followed.
		runProgress?: { done: number; total: number | null };
		/// What the selected nodes offer (the contextual bar), drawn in
		/// this bar after what is going on; absent with nothing selected.
		selection?: Snippet;
		/// A trigger setup under way: stop it, the triggers it has not
		/// turned on yet stay off.
		onCancelActivate?: () => void;
		/// Mid-build: cancel the build. The next status says what happened.
		onCancelBuild?: () => void;
		/// Mid infra change: HALT it; per-node partial state stays.
		onCancelInfra?: () => void;
		/// A resync that stopped: take it up again where it stopped.
		onResumeResync?: () => void;
		/// Generic Stop: the host kills the CLI verb working, or cancels
		/// the followed run.
		onStop?: () => void;
		onDismissError?: () => void;
		/// A diagnostic's file:line:column in the details modal.
		onOpenLocation: (location: SourceLocation) => void;
		onToggleInfraSubgraph?: () => void;
		onToggleTriggerSubgraph?: () => void;
		showInfraSubgraph?: boolean;
		showTriggerSubgraph?: boolean;
		/// Does the SOURCE declare infra / trigger nodes: whether the
		/// subgraph toggles are drawn.
		hasInfra?: boolean;
		hasTriggers?: boolean;
	} = $props();

	const backend = $derived(barState.backend);
	const overlay = $derived(barState.overlay);

	const verbInputs = $derived<VerbInputs>({
		removing: backend.removing,
		available: backend.available,
		hasInfra,
		hasTriggers,
		infraRollup: backend.infraRollup,
		status: backend.status,
		transition: backend.transition,
		infraBusy: backend.infraBusy,
	});
	const offered = (verb: ActionVerb) => backend.available.includes(verb);

	/// Label for a CLI verb working: the verb the person clicked, or the
	/// phase when it says something the verb does not ("Building...").
	function cliPhaseLabel(phase: BarPhase, verb: ActionVerb, detail?: Record<string, unknown>): string {
		switch (phase) {
			case 'preflight': return 'Checking...';
			case 'build_start':
			case 'build_image':
			case 'build_image_done':
			case 'build_wait': return 'Building...';
			case 'build_unreachable': {
				// SYNC: the build_unreachable detail <-> crates/weft-cli/src/progress.rs (Progress::build_unreachable)
				const elapsed = typeof detail?.elapsedSeconds === 'number' ? ` (${detail.elapsedSeconds}s)` : '';
				return `Install not answering, looking again${elapsed}...`;
			}
			case 'build_done': return 'Loading...';
			case 'infra_provision_start':
			case 'infra_provision_done': return 'Provisioning infra...';
			case 'trigger_register_start':
			case 'trigger_register_done': return 'Registering triggers...';
			case 'trigger_register_wait': {
				// SYNC: the trigger_register_wait detail <-> crates/weft-cli/src/progress.rs (Progress::trigger_register_wait)
				const waiting = typeof detail?.waiting === 'string' ? `: ${detail.waiting}` : '...';
				const elapsed = typeof detail?.elapsedSeconds === 'number' ? ` (${detail.elapsedSeconds}s)` : '';
				return `Turning triggers on${elapsed}, waiting on${waiting}`;
			}
			// The call waits for the runs going to end before it lands:
			// without this the bar looks hung for as long as they take.
			case 'drain_wait': return 'Waiting for the runs still going...';
			case 'warning': {
				// Something the person should know that is not a failure;
				// the verb carries on and still ends `complete`.
				const message = typeof detail?.message === 'string' ? detail.message : '';
				return message ? `Careful: ${message}` : 'Careful...';
			}
			case 'infra_wait': {
				// SYNC: the infra_wait detail <-> crates/weft-cli/src/progress.rs (Progress::infra_wait)
				const which = typeof detail?.verb === 'string' ? ` ${detail.verb}` : '';
				const elapsed = typeof detail?.elapsedSeconds === 'number' ? ` (${detail.elapsedSeconds}s)` : '';
				const doing = Array.isArray(detail?.doing)
					? detail.doing.filter((d: unknown): d is string => typeof d === 'string')
					: [];
				return `Waiting for infra${which}${elapsed}${doing.length ? `: ${doing.join('; ')}` : '...'}`;
			}
		}
		switch (verb) {
			case 'infra_start': return 'Starting infra...';
			case 'infra_stop': return 'Stopping infra...';
			case 'infra_terminate': return 'Terminating infra...';
			case 'infra_upgrade': return 'Upgrading infra...';
			case 'infra_cancel': return 'Cancelling infra...';
			case 'activate': return 'Turning triggers on...';
			case 'bake': return 'Preparing trigger settings...';
			case 'cancel_activate': return 'Cancelling the setup...';
			case 'cancel_build': return 'Cancelling build...';
			case 'deactivate': return 'Turning triggers off...';
			case 'resync': return 'Resyncing...';
			case 'run': return 'Starting the run...';
			case 'build': return 'Building...';
			case 'rm': return 'Removing...';
		}
		const _exhaustive: never = verb;
		return _exhaustive;
	}

	/// What the bar shows, the first that holds: a verb this editor runs,
	/// a Stop on its way, a build, a trigger setup, a resync, an infra
	/// change, the run followed. Each in-flight state carries the cancel
	/// the dispatcher offers for it.
	type Slot =
		| { kind: 'idle' }
		| { kind: 'cli'; label: string }
		| { kind: 'pending'; message: string }
		| { kind: 'working'; label: string; onCancel?: () => void }
		| { kind: 'resync_stopped' }
		| { kind: 'run'; runId: string };

	const turningOn = $derived(
		backend.status === 'activating'
			|| (status?.triggers ?? []).some((t) => [t.shared, ...(t.copies ?? [])].some((c) => c?.state === 'turning_on')),
	);
	/// An infra change under way, whoever started it (this editor, a
	/// terminal, an assistant).
	const infraMoving = $derived(
		barState.infraSetup
			|| backend.infraBusy
			|| backend.infraRollup === 'provisioning'
			|| backend.infraRollup === 'stopping'
			|| backend.infraRollup === 'terminating',
	);

	const slot = $derived.by((): Slot => {
		if (overlay.kind === 'cli_running') return { kind: 'cli', label: cliPhaseLabel(overlay.phase, overlay.verb, overlay.detail) };
		if (overlay.kind === 'pending') return { kind: 'pending', message: overlay.message };
		if (backend.transition === 'building' || backend.transition === 'cancelling_build') {
			return backend.transition === 'cancelling_build'
				? { kind: 'working', label: 'Cancelling build...' }
				: { kind: 'working', label: 'Building...', onCancel: offered('cancel_build') ? onCancelBuild : undefined };
		}
		if (resyncStopped(verbInputs)) return { kind: 'resync_stopped' };
		if (backend.transition === 'resyncing') {
			return { kind: 'working', label: 'Resyncing...', onCancel: offered('cancel_activate') ? onCancelActivate : undefined };
		}
		if (turningOn) {
			return { kind: 'working', label: 'Turning triggers on...', onCancel: offered('cancel_activate') ? onCancelActivate : undefined };
		}
		if (infraMoving) {
			const label = backend.infraRollup === 'stopping' ? 'Stopping infra...'
				: backend.infraRollup === 'terminating' ? 'Terminating infra...'
				: 'Starting infra...';
			return { kind: 'working', label, onCancel: offered('infra_cancel') ? onCancelInfra : undefined };
		}
		if (overlay.kind === 'run_running') return { kind: 'run', runId: overlay.runId };
		return { kind: 'idle' };
	});

	const btn = 'flex items-center gap-2 px-3 py-1.5 rounded-lg transition-colors';
	const labelCss = 'font-medium text-[11px] uppercase tracking-wider';

	const showInfraToggle = $derived(hasInfra && !!onToggleInfraSubgraph);
	const showTriggerToggle = $derived(hasTriggers && !!onToggleTriggerSubgraph);
	/// Anything before the selection's part: an eye, or what is going on.
	const anyStatus = $derived(showInfraToggle || showTriggerToggle || slot.kind !== 'idle');

	const errorMessage = $derived(barState.error?.message);
	const errorVerb = $derived(barState.error?.verb);
	const currentError = $derived(barState.error);

	// What the verb on the bar has printed to the terminal. Present while
	// it runs and, when it fails, for as long as the failure is up.
	const activity = $derived(barState.activity);
	/// Is the verb whose output this is still going? After one fails and
	/// another starts, the bar is running something, but not the thing the
	/// captured output came from.
	const activityRunning = $derived(overlay.kind === 'cli_running' && overlay.verb === activity?.verb);
	/// The pill's one line: the last thing the verb printed.
	const activityTail = $derived(activity?.lines[activity.lines.length - 1] ?? '');

	let detailsOpen: boolean = $state(false);
	$effect(() => {
		if (!currentError && !activity) detailsOpen = false;
	});
</script>

<div class="flex flex-col items-center gap-1">

	<!-- Sticky error banner. The whole banner opens the details modal;
	     the trailing X dismisses the error. -->
	{#if errorMessage}
		<div class="flex items-center gap-1 bg-red-50 border border-red-200 rounded-lg shadow-sm">
			<button
				type="button"
				class="flex items-center gap-2 px-3 py-1.5 text-xs text-red-700 hover:bg-red-100/60 rounded-l-lg"
				title="Click to see details"
				onclick={() => (detailsOpen = true)}
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
	{/if}

	<!-- What the verb is printing to the terminal: one line, the newest,
	     and the whole of it in the same modal on click. It sits under a
	     failure's banner: the failure is the headline, this the trace. -->
	{#if activity && activityTail}
		<button
			type="button"
			class="flex items-center gap-2 px-3 py-1 text-[11px] bg-zinc-900 text-zinc-200 border border-zinc-700 rounded-lg shadow-sm hover:bg-zinc-800 max-w-[560px]"
			title="Click to see the full output"
			onclick={() => (detailsOpen = true)}
		>
			{#if activityRunning}
				<Loader2 class="w-3 h-3 shrink-0 animate-spin text-zinc-400" />
			{:else}
				<Terminal class="w-3 h-3 shrink-0 text-zinc-400" />
			{/if}
			<span class="font-medium text-zinc-400">{activity.verb}</span>
			<span class="font-mono truncate">{activityTail}</span>
			<span class="text-[10px] opacity-60 underline shrink-0">details</span>
		</button>
	{/if}

	<ActionDetailsModal
		error={barState.error}
		{activity}
		running={activityRunning}
		bind:open={detailsOpen}
		onDismissError={() => onDismissError?.()}
		{onOpenLocation}
	/>

	{#if anyStatus || selection}
	<div class="flex flex-wrap items-center justify-center gap-1.5 p-1.5 bg-white border border-zinc-200 rounded-xl shadow-xl backdrop-blur-md max-w-[90vw]">
		{#if showInfraToggle}
			<button
				class="flex items-center justify-center w-7 h-7 rounded-lg transition-colors {showInfraSubgraph ? 'bg-blue-100 text-blue-600 border border-blue-200' : 'bg-white text-zinc-400 border border-zinc-200 hover:bg-zinc-50'}"
				onclick={onToggleInfraSubgraph}
				title={showInfraSubgraph ? 'Hide infrastructure subgraph' : 'Show infrastructure subgraph'}
			>
				{@render eyeIcon(showInfraSubgraph)}
			</button>
		{/if}
		{#if showTriggerToggle}
			<button
				class="flex items-center justify-center w-7 h-7 rounded-lg transition-colors {showTriggerSubgraph ? 'bg-emerald-100 text-emerald-600 border border-emerald-200' : 'bg-white text-zinc-400 border border-zinc-200 hover:bg-zinc-50'}"
				onclick={onToggleTriggerSubgraph}
				title={showTriggerSubgraph ? 'Hide trigger subgraph' : 'Show trigger subgraph'}
			>
				{@render eyeIcon(showTriggerSubgraph)}
			</button>
		{/if}

		{#if slot.kind === 'cli'}
			{@render workingButton(slot.label, onStop)}
		{:else if slot.kind === 'pending'}
			{@render workingButton(slot.message)}
		{:else if slot.kind === 'working'}
			{@render workingButton(slot.label, slot.onCancel)}
		{:else if slot.kind === 'resync_stopped'}
			<span class="text-[10px] font-medium text-red-600 uppercase tracking-wider whitespace-nowrap px-1">Resync stopped</span>
			<button
				class="{btn} bg-amber-50 text-amber-700 border border-amber-300 hover:bg-amber-100"
				onclick={onResumeResync}
				title="A resync stopped with the triggers it took down still down. Try it again where it stopped."
			>
				<span class={labelCss}>Resync</span>
			</button>
			<button
				class="{btn} bg-white text-zinc-700 border border-zinc-300 hover:bg-zinc-50"
				onclick={onCancelActivate}
				title="Give the resync up and leave the triggers it took down off."
			>
				<span class={labelCss}>Give up</span>
			</button>
		{:else if slot.kind === 'run'}
			<button class="{btn} bg-orange-50 text-orange-600 border border-orange-200 hover:bg-orange-100" onclick={onStop}>
				<Square class="w-3.5 h-3.5" />
				<span class={labelCss}>Stop</span>
			</button>
			<span class="px-1 text-[11px] text-zinc-600 whitespace-nowrap">
				run <span class="font-mono">{slot.runId.slice(0, 4)}...</span>
				{#if runProgress}
					{runProgress.total !== null ? `${runProgress.done}/${runProgress.total}` : runProgress.done} nodes
				{/if}
			</span>
		{/if}

		{#if selection}
			{#if anyStatus}
				<div class="w-px h-6 bg-zinc-200 mx-1"></div>
			{/if}
			{@render selection()}
		{/if}
	</div>
	{/if}
</div>

{#snippet eyeIcon(open: boolean)}
	<svg xmlns="http://www.w3.org/2000/svg" width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round">
		{#if open}
			<path d="M1 12s4-8 11-8 11 8 11 8-4 8-11 8-11-8-11-8z"/><circle cx="12" cy="12" r="3"/>
		{:else}
			<path d="M17.94 17.94A10.07 10.07 0 0 1 12 20c-7 0-11-8-11-8a18.45 18.45 0 0 1 5.06-5.94M9.9 4.24A9.12 9.12 0 0 1 12 4c7 0 11 8 11 8a18.5 18.5 0 0 1-2.16 3.19m-6.72-1.07a3 3 0 1 1-4.24-4.24"/><line x1="1" y1="1" x2="23" y2="23"/>
		{/if}
	</svg>
{/snippet}

<!-- THE one in-flight pattern: the "...-ing" label AND its cancel, when
     there is one right now. Cancel reconciles, never asserts: the next
     status decides what renders. -->
<!-- Default-param form, NOT `onCancel?`: snippet parameters live in the
     MARKUP, which no script preprocessor touches, so a TS optional marker
     passes verbatim into the compiled JS and breaks the build ("Expected ',',
     got '?'"). A default value types the same and compiles to valid JS. -->
{#snippet workingButton(text: string, onCancel: (() => void) | undefined = undefined)}
	<button
		class="{btn} bg-red-50 text-red-600 border border-red-200 hover:bg-red-100"
		onclick={onCancel}
		disabled={!onCancel}
		title={onCancel ? 'Cancel it' : undefined}
	>
		<Loader2 class="w-3.5 h-3.5 animate-spin" />
		<span class={labelCss}>{text}{onCancel ? ' (Cancel)' : ''}</span>
	</button>
{/snippet}
