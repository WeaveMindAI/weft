<script lang="ts">
	import { page } from '$app/stores';
	import { onMount, onDestroy } from 'svelte';
	import RunnerView from '$lib/components/project/RunnerView.svelte';
	import { hydrateProject } from '$lib/project-hydration';
	import {
		getPublication,
		getPublicationSession,
		runPublication,
		getLatestTriggerRun,
		type PublicSnapshot,
	} from '$lib/publish/client';
	import type { ProjectDefinition } from '$lib/types';

	const username = $derived($page.params.username ?? '');
	const slug = $derived($page.params.slug ?? '');

	let snapshot = $state<PublicSnapshot | null>(null);
	let project = $state<ProjectDefinition | null>(null);
	let loadError = $state('');
	let loading = $state(true);
	let available = $state(false);
	let running = $state(false);
	let nodeOutputs = $state<Record<string, unknown>>({});
	let visitorSession: { lastInputs: Record<string, unknown> | null; lastOutputs: Record<string, unknown> | null } | null = null;

	// Trigger broadcast state: the latest trigger-fired execution ID we've
	// picked up from the server. We poll every few seconds and, if there's
	// a newer run than the one we've shown, replace the current outputs.
	// This makes trigger-fired results a shared broadcast across visitors:
	// everyone on the page sees the same result update at roughly the same
	// time. Today a deployment is either trigger-driven (no manual Run
	// button) or manual-run-only (no trigger), so we don't yet need the
	// "visitor pressed Run, pause broadcast" priority logic.
	let lastSeenTriggerExecutionId = $state<string | null>(null);
	let triggerPollInterval: ReturnType<typeof setInterval> | null = null;
	// Track consecutive poll failures so we can log the first one without
	// spamming the console every 3 seconds on a genuinely broken backend.
	let triggerPollFailureLogged = false;

	// Transient error shown on the run button when a visitor-initiated run
	// fails. Cleared on next successful run.
	let runErrorMessage = $state<string | null>(null);

	async function load() {
		loading = true;
		loadError = '';
		try {
			snapshot = await getPublication(username, slug);
			available = snapshot.available;
			if (!available) {
				loading = false;
				return;
			}
			project = hydrateProject({
				id: snapshot.slug,
				name: snapshot.projectName,
				description: snapshot.description,
				weftCode: snapshot.weftCode,
				loomCode: snapshot.loomCode,
				layoutCode: snapshot.layoutCode,
				createdAt: new Date().toISOString(),
				updatedAt: new Date().toISOString(),
			});

			// Restore prior visitor session. Failure here is non-fatal —
			// the page renders with empty inputs, which is the correct
			// behaviour for OSS local mode (which has no session endpoint).
			// We still log the first failure to the console so a broken
			// cloud endpoint doesn't go completely unnoticed.
			try {
				const s = await getPublicationSession(username, slug);
				visitorSession = s.session;
				if (visitorSession?.lastInputs && project) {
					const inputs = visitorSession.lastInputs as Record<string, Record<string, unknown>>;
					for (const [nodeId, fields] of Object.entries(inputs)) {
						const node = project.nodes.find(n => n.id === nodeId);
						if (node && typeof fields === 'object' && fields !== null) {
							node.config = { ...node.config, ...fields };
						}
					}
				}
				if (visitorSession?.lastOutputs && typeof visitorSession.lastOutputs === 'object') {
					nodeOutputs = visitorSession.lastOutputs as Record<string, unknown>;
				}
			} catch (e) {
				console.warn('[/p] failed to load visitor session (ok in OSS local mode):', e);
			}
		} catch (e) {
			console.error('[/p] failed to load published snapshot:', e);
			loadError = e instanceof Error ? e.message : 'Failed to load';
		} finally {
			loading = false;
		}
	}

	/** Wake-up handler for H9: when the tab becomes visible again, fire
	 *  a one-shot poll immediately so the visitor doesn't stare at stale
	 *  state for up to 3 seconds. Defined as a named handler so we can
	 *  cleanly detach it on destroy. */
	function handleVisibilityChange() {
		if (!document.hidden) pollLatestTriggerRun();
	}

	onMount(async () => {
		await load();
		// Start polling for trigger-fired outputs once the snapshot is ready.
		// 3 second cadence is a reasonable demo compromise: fast enough to
		// feel "live", slow enough not to hammer the backend. Polling pauses
		// when the tab is hidden (see pollLatestTriggerRun) and resumes via
		// the visibilitychange listener below.
		if (available) {
			triggerPollInterval = setInterval(pollLatestTriggerRun, 3000);
			// Kick an immediate poll so a fresh visitor sees the current
			// state without waiting 3 seconds.
			pollLatestTriggerRun();
			if (typeof document !== 'undefined') {
				document.addEventListener('visibilitychange', handleVisibilityChange);
			}
		}
	});

	onDestroy(() => {
		if (triggerPollInterval) clearInterval(triggerPollInterval);
		if (typeof document !== 'undefined') {
			document.removeEventListener('visibilitychange', handleVisibilityChange);
		}
	});

	async function pollLatestTriggerRun() {
		// H9: suspend polling when the tab is not visible. A typical
		// visitor leaves the tab in the background while doing other
		// things, and a 3-second cadence × thousands of hidden tabs
		// hammers cloud-api for no benefit. When the tab comes back
		// into focus the visibilitychange handler fires an immediate
		// poll so they see the fresh state.
		if (typeof document !== 'undefined' && document.hidden) return;
		try {
			const latest = await getLatestTriggerRun(username, slug);
			triggerPollFailureLogged = false;
			if (!latest) return;
			if (latest.executionId === lastSeenTriggerExecutionId) return;
			lastSeenTriggerExecutionId = latest.executionId;
			nodeOutputs = latest.outputs;
		} catch (e) {
			// Log the first failure only — polling runs every 3 seconds
			// and a genuinely broken backend shouldn't drown the console.
			if (!triggerPollFailureLogged) {
				console.warn('[/p] trigger broadcast poll failed:', e);
				triggerPollFailureLogged = true;
			}
		}
	}

	function handleUpdateNodeConfig(nodeId: string, config: Record<string, unknown>) {
		if (!project) return;
		const node = project.nodes.find(n => n.id === nodeId);
		if (node) node.config = config;
	}

	/** Visitor Run handler.
	 *
	 *  RunnerView maintains a transient per-visitor `previewProject`
	 *  clone even in `forcedMode="visitor"`. Every input the visitor
	 *  types flows through `routeConfigUpdate` which mutates ONLY
	 *  the clone, not the parent's `project` prop (that path is
	 *  reserved for admin edits that persist to weft). So when
	 *  RunnerView's Run button fires, it calls `onRun(previewProject)`
	 *  with the clone as the override — and **that clone is the
	 *  authoritative source of visitor inputs**.
	 *
	 *  Earlier this handler discarded the override and read from
	 *  `project.nodes`, which meant the visitor's typed inputs were
	 *  silently lost and the backend received the deployment's
	 *  pristine defaults. Fixed by preferring the override when
	 *  present.
	 */
	async function handleRun(override?: ProjectDefinition) {
		if (running) return;
		const source = override ?? project;
		if (!source) return;
		running = true;
		runErrorMessage = null;
		try {
			// Collect inputs to send. The backend enforces the loom's input
			// allowlist per node, so even if we sent the full config here,
			// only visitor-writable fields would be merged server-side.
			const inputs: Record<string, Record<string, unknown>> = {};
			for (const node of source.nodes) {
				inputs[node.id] = node.config;
			}
			const result = await runPublication(username, slug, inputs);
			if (result?.result && typeof result.result === 'object' && 'outputs' in result.result) {
				nodeOutputs = (result.result as { outputs: Record<string, unknown> }).outputs;
			}
		} catch (e) {
			console.error('[/p] visitor run failed:', e);
			// Surface the error so the visitor knows their click did something.
			runErrorMessage = e instanceof Error ? e.message : 'Run failed. Please try again.';
		} finally {
			running = false;
		}
	}

	// Stubbed admin-only handlers. Visitor mode hides admin controls, so these
	// are never called. We provide no-ops so the RunnerView prop contract is
	// satisfied without crashing.
	const noop = () => {};
</script>

<svelte:head>
	<title>{snapshot?.projectName ?? 'Loading...'}</title>
	{#if snapshot?.description}
		<meta name="description" content={snapshot.description} />
		<meta property="og:description" content={snapshot.description} />
	{/if}
	{#if snapshot?.projectName}
		<meta property="og:title" content={snapshot.projectName} />
	{/if}
</svelte:head>

{#if loading}
	<div class="min-h-screen flex items-center justify-center bg-background">
		<div class="text-sm text-muted-foreground">Loading…</div>
	</div>
{:else if loadError}
	<div class="min-h-screen flex items-center justify-center bg-background p-6">
		<div class="max-w-md text-center space-y-4">
			<h1 class="text-2xl font-semibold">Page not found</h1>
			<p class="text-muted-foreground text-sm">
				This published page does not exist, or the server could not be reached.
			</p>
			<p class="text-xs text-muted-foreground/70 font-mono break-words">{loadError}</p>
		</div>
	</div>
{:else if !available}
	<div class="min-h-screen flex items-center justify-center bg-background p-6">
		<div class="max-w-md text-center space-y-4">
			<h1 class="text-2xl font-semibold">Temporarily unavailable</h1>
			<p class="text-muted-foreground text-sm">
				This tool is currently unavailable. Please check back later.
			</p>
			<a href="https://weavemind.ai" class="text-xs text-muted-foreground hover:underline">
				Built with WeaveMind
			</a>
		</div>
	</div>
{:else if project}
	<div class="min-h-screen flex flex-col relative">
		{#if runErrorMessage}
			<!-- Toast: visitor-initiated run failed. Dismissed by clicking X
			     or automatically replaced by the next successful run. -->
			<div class="fixed top-4 left-1/2 -translate-x-1/2 z-50 max-w-md w-[calc(100%-2rem)] rounded-lg border border-red-200 bg-red-50 shadow-lg px-4 py-3 flex items-start gap-3">
				<div class="flex-1 min-w-0">
					<div class="text-xs font-semibold text-red-900">Run failed</div>
					<div class="text-xs text-red-800 mt-0.5 break-words">{runErrorMessage}</div>
				</div>
				<button
					type="button"
					class="text-red-700 hover:text-red-900 transition-colors flex-shrink-0"
					aria-label="Dismiss"
					onclick={() => (runErrorMessage = null)}
				>
					<svg xmlns="http://www.w3.org/2000/svg" width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2.5" stroke-linecap="round" stroke-linejoin="round"><path d="M18 6 6 18"/><path d="m6 6 12 12"/></svg>
				</button>
			</div>
		{/if}
		<div class="flex-1 min-h-0">
			<RunnerView
				{project}
				onUpdateNodeConfig={handleUpdateNodeConfig}
				triggerState={{ hasTriggers: false, isActive: false, isLoading: false }}
				onToggleTrigger={noop}
				infraState={{ hasInfrastructure: false, status: 'idle', isLoading: false }}
				onCheckInfraStatus={noop}
				onStartInfra={noop}
				onStopInfra={noop}
				onTerminateInfra={noop}
				onRun={handleRun}
				onStop={noop}
				executionState={{ isRunning: running, nodeOutputs }}
				forcedMode="visitor"
			/>
		</div>
		{#if snapshot?.showBuiltWithFooter}
			<!-- Forced branding footer. Rendered server-side gate: the flag
			     is computed from the deployer's subscription tier by
			     cloud-api, so a deployer below the Builder tier can't
			     suppress it client-side by editing the Loom. Builder tier
			     and above see the flag as false and the footer isn't
			     rendered at all. -->
			<footer class="flex-shrink-0 border-t border-zinc-200/60 bg-white/80 backdrop-blur py-3 px-6 flex items-center justify-center">
				<a
					href="https://weavemind.ai"
					target="_blank"
					rel="noopener noreferrer"
					class="text-xs text-zinc-500 hover:text-zinc-900 transition-colors inline-flex items-center gap-1.5"
				>
					Built with
					<span class="font-semibold">WeaveMind</span>
				</a>
			</footer>
		{/if}
	</div>
{/if}
