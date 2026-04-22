<script lang="ts">
	import type { ProjectDefinition, Block, LiveDataItem, RunnerMode, RunnerTheme } from '$lib/types';
	import ActionBar from './ActionBar.svelte';
	import BlockList from '../runner/BlockList.svelte';
	import {
		resolveSkin,
		resolveSurface,
		densityClass,
		layoutMaxWidth,
		layoutContentWidthStyle,
		pagePaddingClass,
	} from '../runner/skins.ts';
	import { marked } from 'marked';

	const renderer = new marked.Renderer();
	renderer.link = ({ href, title, text }) => {
		const titleAttr = title ? ` title="${title}"` : '';
		return `<a href="${href}"${titleAttr} target="_blank" rel="noopener noreferrer">${text}</a>`;
	};
	marked.setOptions({ breaks: true, gfm: true, renderer });

	function renderMarkdown(content: string): string {
		return marked.parse(content, { async: false }) as string;
	}

	let {
		project,
		onUpdateNodeConfig,
		triggerState,
		onToggleTrigger,
		onResyncTrigger,
		infraState,
		onCheckInfraStatus,
		onStartInfra,
		onStopInfra,
		onTerminateInfra,
		onForceRetry,
		onRun,
		onStop,
		executionState,
		infraLiveData,
		// eslint-disable-next-line @typescript-eslint/no-unused-vars
		testMode: _testMode = false,
		onConfigureRunner,
		onPublish,
		hasPublications = false,
		forcedMode,
		isLiveDeployment = false,
		publicUrl,
	}: {
		project: ProjectDefinition;
		onUpdateNodeConfig: (nodeId: string, config: Record<string, unknown>) => void;
		triggerState: { hasTriggers: boolean; isActive: boolean; isLoading: boolean; isStale?: boolean };
		onToggleTrigger: () => void;
		onResyncTrigger?: () => void;
		infraState: { hasInfrastructure: boolean; hasInfraInFrontend?: boolean; hasInfraInBackend?: boolean; status: string; nodes?: Array<{ nodeId: string; nodeType: string; instanceId: string; status: string; backend?: string }>; isLoading: boolean };
		onCheckInfraStatus: () => void;
		onStartInfra: () => void;
		onStopInfra: () => void;
		onTerminateInfra: () => void;
		onForceRetry?: () => void;
		/** Parent's run handler. Accepts an optional override project so the
		 *  runner can execute a preview copy (visitor preview mode) without
		 *  polluting the persisted weft. When omitted, parent runs whatever
		 *  its own `project` prop currently points at. */
		onRun: (overrideProject?: ProjectDefinition) => void;
		onStop: () => void;
		executionState: { isRunning: boolean; nodeOutputs?: Record<string, unknown> };
		infraLiveData?: Record<string, LiveDataItem[]>;
		testMode?: boolean;
		onConfigureRunner?: () => void;
		onPublish?: () => void;
		/** True when the project already has at least one publication, used
		 *  to switch the toolbar button label from "Publish" to "Manage deployments". */
		hasPublications?: boolean;
		/** If set, disables the toggle and locks the runner to this mode.
		 *  Used by the public /p/<slug> route to render visitor-only. */
		forcedMode?: RunnerMode;
		/** True when edits made in `admin` mode on this project propagate to
		 *  a live public deployment (either the project IS the deployment, or
		 *  it has at least one active publication). Controls whether we show
		 *  the "Live deployment — changes affect all visitors" banner. */
		isLiveDeployment?: boolean;
		/** Public URL to show in the live-deployment banner (e.g.
		 *  `/p/alice/weather-bot`). Purely informational, so the deployer
		 *  knows which audience is affected. */
		publicUrl?: string;
	} = $props();

	// Preview mode toggle: 'admin' = full admin view (infra, triggers, secrets
	// visible, edits persist to weft), 'visitor' = what a publicly-deployed page
	// would show. In visitor preview, edits are local-only so the deployer can
	// debug as a fresh visitor without mutating the real deployment. Runs
	// dispatch the preview project via `onRun(previewProject)` so downstream
	// execution sees the preview values. The public `/p/<slug>` route passes
	// `forcedMode` to lock real visitors into visitor mode and removes the toggle.
	let previewMode = $state<RunnerMode>('admin');
	const mode = $derived<RunnerMode>(forcedMode ?? previewMode);

	// Transient per-visitor-preview copy of `project`. Only populated while
	// the deployer is in `visitor` preview mode. When they toggle back to
	// `admin`, we discard it so a subsequent preview session always starts
	// clean. Using a shallow clone of the nodes array is enough because we
	// only mutate node.config — nothing else.
	let previewProject = $state<ProjectDefinition | null>(null);

	$effect(() => {
		if (mode === 'visitor' && previewProject === null) {
			previewProject = clonePreview(project);
		} else if (mode === 'admin') {
			previewProject = null;
		}
	});

	/** Deep-enough clone for a preview session: copies nodes and their
	 *  config maps so local edits don't leak into the parent's project.
	 *  Other fields (edges, setupManifest, etc.) are read-only in the
	 *  runner so a shallow reference is fine. */
	function clonePreview(source: ProjectDefinition): ProjectDefinition {
		return {
			...source,
			nodes: source.nodes.map(n => ({ ...n, config: { ...n.config } })),
		};
	}

	/** Project the runner should render. In admin mode it's always the live
	 *  persisted project; in visitor preview it's the transient clone so
	 *  the deployer can type without mutating the real deployment. */
	const renderProject = $derived<ProjectDefinition>(
		mode === 'visitor' && previewProject !== null ? previewProject : project,
	);

	/** Field-edit interception: admin edits flow to the parent's
	 *  onUpdateNodeConfig (which eventually writes weft); visitor-preview
	 *  edits mutate the transient preview copy only. */
	function routeConfigUpdate(nodeId: string, config: Record<string, unknown>) {
		if (mode === 'visitor') {
			if (!previewProject) previewProject = clonePreview(project);
			const node = previewProject.nodes.find(n => n.id === nodeId);
			if (node) node.config = config;
			return;
		}
		onUpdateNodeConfig(nodeId, config);
	}

	/** Run button: in visitor preview, dispatch with the transient project
	 *  so the executor sees the deployer's fresh-visitor inputs instead of
	 *  the persisted defaults. In admin mode, pass nothing so the parent
	 *  runs its own persisted project. */
	function routeRun() {
		if (mode === 'visitor' && previewProject !== null) {
			onRun(previewProject);
			return;
		}
		onRun();
	}

	const manifest = $derived(renderProject.setupManifest);
	const theme = $derived<RunnerTheme | undefined>(manifest?.theme);

	// Source of truth: the block list if present. Otherwise synthesize one from
	// legacy phases/outputs/liveItems so old loom files still render in order.
	const blocks = $derived<Block[]>((() => {
		if (manifest?.blocks && manifest.blocks.length > 0) return manifest.blocks;
		if (!manifest) return [];
		const out: Block[] = [];
		for (const phase of manifest.phases) out.push({ kind: 'phase', phase });
		for (const output of manifest.outputs) out.push({ kind: 'output', output });
		for (const live of (manifest.liveItems ?? [])) out.push({ kind: 'live', live });
		return out;
	})());

	const hasManifest = $derived(blocks.length > 0);

	const formErrors = $derived<Record<string, string>>({});
	const hasErrors = $derived(Object.keys(formErrors).length > 0);

	// Resolve the active skin and derive every visual style the shell needs.
	// Brick renderers read the resolved CSS vars, so they never need to know
	// which skin is active.
	const skin = $derived(resolveSkin(theme));
	const surface = $derived(resolveSurface(theme, skin));

	const rootStyle = $derived.by(() => {
		const parts: string[] = [
			...Object.entries(skin.vars).map(([k, v]) => `${k}: ${v}`),
			`--runner-font: ${skin.fontFamily}`,
			`--runner-card-bg: ${skin.defaultCard.background}`,
			`--runner-card-border: ${skin.defaultCard.border}`,
			`--runner-card-shadow: ${skin.defaultCard.shadow}`,
			`--runner-card-blur: ${skin.defaultCard.backdropBlur}`,
			`--runner-card-radius: ${theme?.radius ? skin.radius : skin.defaultCard.radius}`,
			`--runner-surface: ${surface}`,
			`background: var(--runner-surface)`,
			`color: var(--runner-fg)`,
			`font-family: var(--runner-font)`,
		];
		return parts.join('; ');
	});

	const containerStyle = $derived(layoutContentWidthStyle(theme));
	const containerClass = $derived(`${layoutMaxWidth(theme)} mx-auto ${pagePaddingClass(theme)} ${densityClass(theme)}`);
	const rootClass = $derived(skin.rootClass);

	// CTA run event (dispatched by <cta action:"run"> bricks). Routed
	// through `routeRun` so visitor-preview Runs use the transient project.
	$effect(() => {
		const handler = () => { if (!hasErrors) routeRun(); };
		window.addEventListener('runner:cta-run', handler);
		return () => window.removeEventListener('runner:cta-run', handler);
	});
</script>

<div class="h-full overflow-y-auto relative {rootClass}" style={rootStyle}>
	<!-- Admin toolbar: mode toggle + configure + publish. Fixed at the top of
	     the runner view so you can preview and manage the deployed page
	     without switching to builder view. Hidden in visitor preview. -->
	{#if mode === 'admin' && !forcedMode}
		<!-- Admin-mode banner: if this project is a live deployment (or has
		     active publications), warn that edits propagate to all public
		     visitors. Builder-only projects without publications don't need
		     the banner. -->
		{#if isLiveDeployment}
			<div class="sticky top-0 z-30 border-b border-amber-300/70 bg-amber-50/95 backdrop-blur px-6 py-2 flex items-center gap-2">
				<svg xmlns="http://www.w3.org/2000/svg" width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round" class="text-amber-700 flex-shrink-0"><path d="m21.73 18-8-14a2 2 0 0 0-3.48 0l-8 14A2 2 0 0 0 4 21h16a2 2 0 0 0 1.73-3Z"/><path d="M12 9v4"/><path d="M12 17h.01"/></svg>
				<div class="text-[11px] text-amber-900 leading-tight flex-1 min-w-0">
					<span class="font-semibold">Live deployment.</span>
					Changes here apply immediately to every visitor{#if publicUrl}{' '}on <span class="font-mono">{publicUrl}</span>{/if}.
					Toggle to <button type="button" class="underline decoration-dotted underline-offset-2 hover:text-amber-950" onclick={() => { previewMode = 'visitor'; }}>Visitor preview</button> to test edits without persisting.
				</div>
			</div>
		{/if}
		<div class="sticky {isLiveDeployment ? 'top-[34px]' : 'top-0'} z-20 flex items-center justify-between gap-3 border-b border-zinc-200/60 bg-white/80 backdrop-blur px-6 py-2">
			<div class="inline-flex rounded-md border border-border overflow-hidden">
				<button
					type="button"
					class="text-xs px-3 py-1 font-medium transition-colors bg-zinc-900 text-white"
					onclick={() => { previewMode = 'admin'; }}
				>Admin view</button>
				<button
					type="button"
					class="text-xs px-3 py-1 font-medium transition-colors border-l border-border bg-background text-muted-foreground hover:text-foreground"
					onclick={() => { previewMode = 'visitor'; }}
				>Visitor preview</button>
			</div>
			<div class="flex items-center gap-2">
				{#if onConfigureRunner}
					<button
						type="button"
						class="text-xs px-3 py-1 rounded-md border border-border text-muted-foreground hover:text-foreground hover:bg-muted transition-colors"
						onclick={onConfigureRunner}
					>Configure Runner</button>
				{/if}
				{#if onPublish}
					<button
						type="button"
						class="text-xs px-3 py-1 rounded-md bg-violet-600 text-white hover:bg-violet-700 transition-colors"
						onclick={onPublish}
						title={hasPublications ? 'Manage your public deployments' : 'Publish this project to a public URL'}
					>{hasPublications ? 'Manage deployments' : 'Publish'}</button>
				{/if}
			</div>
		</div>
	{:else if !forcedMode}
		<!-- Visitor preview banner: reinforces that edits here are local to
		     this preview session and don't persist. The exit toggle restores
		     the admin view and discards any preview edits. -->
		<div class="sticky top-0 z-30 border-b border-zinc-200/70 bg-zinc-900/95 text-zinc-100 backdrop-blur px-6 py-2 flex items-center justify-between gap-3">
			<div class="flex items-center gap-2 min-w-0">
				<svg xmlns="http://www.w3.org/2000/svg" width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round" class="flex-shrink-0"><path d="M1 12s4-8 11-8 11 8 11 8-4 8-11 8-11-8-11-8z"/><circle cx="12" cy="12" r="3"/></svg>
				<div class="text-[11px] leading-tight">
					<span class="font-semibold">Visitor preview.</span>
					Field edits here are a local preview and do not persist.
				</div>
			</div>
			<button
				type="button"
				class="text-[11px] px-2.5 py-1 rounded-md bg-white/10 hover:bg-white/20 transition-colors"
				onclick={() => { previewMode = 'admin'; }}
			>Exit preview</button>
		</div>
	{/if}

	<div class={containerClass} style={containerStyle}>

		<!-- Header is only rendered if no hero brick is present, to avoid
		     duplication when the DSL includes its own hero section. -->
		{#if !blocks.some(b => b.kind === 'brick' && (b.brick.kind === 'hero' || b.brick.kind === 'navbar'))}
			<div>
				<h1 class="text-3xl font-bold tracking-tight" style="color: var(--runner-fg)">{renderProject.name}</h1>
				{#if renderProject.description}
					<div class="text-sm mt-2 runner-markdown" style="color: var(--runner-muted)">{@html renderMarkdown(renderProject.description)}</div>
				{/if}
			</div>
		{/if}

		{#if !hasManifest}
			<div class="rounded-2xl border border-dashed p-10 text-center" style="border-color: var(--runner-card-border); background: var(--runner-card-bg)">
				<p class="text-sm" style="color: var(--runner-fg)">No runner page has been configured yet.</p>
				<p class="text-xs mt-1" style="color: var(--runner-muted)">Use <strong>Configure Runner</strong> above to build one, or ask Tangle to design it for you.</p>
			</div>
		{:else}
			<BlockList
				{blocks}
				{mode}
				project={renderProject}
				{renderMarkdown}
				onUpdateNodeConfig={routeConfigUpdate}
				{executionState}
				{infraLiveData}
				infraStatus={infraState.status}
			/>
		{/if}

		<!-- Project Controls: admin-only, never shown in visitor mode. -->
		{#if mode === 'admin'}
			<div class="space-y-4">
				<h2 class="text-base font-semibold">Project Controls</h2>
				<ActionBar
					variant="card"
					{infraState}
					{triggerState}
					{executionState}
					{onCheckInfraStatus}
					{onStartInfra}
					{onStopInfra}
					{onTerminateInfra}
					{onForceRetry}
					{onToggleTrigger}
					{onResyncTrigger}
					onRun={routeRun}
					{onStop}
					nodeCount={renderProject.nodes.length}
					disabled={hasErrors}
				/>
			</div>
		{/if}

	</div>
</div>

<style>
	:global(.blob-drag-over) {
		outline: 2px solid rgb(96, 165, 250);
		outline-offset: -2px;
		border-radius: 0.5rem;
		background-color: rgba(96, 165, 250, 0.08);
	}
	:global(.runner-markdown) { overflow-wrap: anywhere; word-break: break-word; }
	:global(.runner-markdown p) { margin: 0 0 0.4em 0; }
	:global(.runner-markdown p:last-child) { margin-bottom: 0; }
	:global(.runner-markdown strong) { font-weight: 600; }
	:global(.runner-markdown em) { font-style: italic; }
	:global(.runner-markdown code) {
		font-size: 0.85em;
		background: rgba(0, 0, 0, 0.06);
		padding: 0.1em 0.35em;
		border-radius: 3px;
		font-family: ui-monospace, monospace;
	}
	:global(.runner-markdown pre) {
		background: rgba(0, 0, 0, 0.04);
		border-radius: 6px;
		padding: 0.5em 0.75em;
		overflow-x: auto;
		margin: 0.4em 0;
		font-size: 0.82em;
	}
	:global(.runner-markdown pre code) { background: none; padding: 0; }
	:global(.runner-markdown ul) { margin: 0.3em 0; padding-left: 1.4em; list-style-type: disc; }
	:global(.runner-markdown ol) { margin: 0.3em 0; padding-left: 1.4em; list-style-type: decimal; }
	:global(.runner-markdown li) { margin: 0.15em 0; display: list-item; }
	:global(.runner-markdown a) { color: #3b82f6; text-decoration: underline; }
	:global(.runner-markdown blockquote) {
		border-left: 2px solid #d4d4d8;
		padding-left: 0.75em;
		margin: 0.4em 0;
		color: #71717a;
	}
	:global(.runner-markdown h1, .runner-markdown h2, .runner-markdown h3) {
		font-weight: 600;
		margin: 0.5em 0 0.25em 0;
	}
	:global(.runner-markdown h1) { font-size: 1.1em; }
	:global(.runner-markdown h2) { font-size: 1.05em; }
	:global(.runner-markdown h3) { font-size: 1em; }
</style>
