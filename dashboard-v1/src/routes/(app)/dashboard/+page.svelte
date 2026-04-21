<script lang="ts">
	import { goto } from "$app/navigation";
	import { onMount } from "svelte";
	import { browser } from "$app/environment";
	import { projects } from "$lib/stores/projects";
	import { ALL_NODES } from "$lib/nodes";
	import { api, authFetch } from "$lib/config";
	import type { ProjectDefinition } from "$lib/types";
	import { formatDate } from "$lib/utils/status";
	import * as te from "$lib/telemetry-events";
	import { listPublications } from "$lib/publish/client";
	import { toast } from "svelte-sonner";
	import * as AlertDialog from "$lib/components/ui/alert-dialog";

	// Trigger & infra detection (same as projects page)
	const triggerNodeTypes = new Set(ALL_NODES.filter(n => n.features?.isTrigger).map(n => n.type));
	const infraNodeTypes = new Set(ALL_NODES.filter(n => n.features?.isInfrastructure).map(n => n.type));

	/** Raw set of project ids (builder OR deployment) with a Running
	 *  trigger in the live dispatcher. Keyed by the project_id on the
	 *  triggers table, which is the DEPLOYMENT id for triggers cloned
	 *  into a deployment on publish. */
	let activeTriggerProjectIds = $state<Set<string>>(new Set());

	/** Raw infra status by project id. Populated for builder projects
	 *  and every deployment descendant so we can roll up status on the
	 *  builder card. */
	let infraStatuses = $state<Record<string, string>>({});

	/** Map from builder project id to the list of deployment project
	 *  ids cloned from it. Populated from `listPublications()`, which
	 *  now returns `origin_project_id` from the deployment row via a
	 *  server-side JOIN. A builder with no deployments has no entry.
	 *  Orphan mapping rows (no origin) are skipped. */
	let deploymentsByBuilder = $state<Record<string, string[]>>({});

	/** True when the builder OR any of its deployment descendants has
	 *  a running trigger. This is what lights the purple thunder in
	 *  the project list. The dashboard only renders builder rows, so
	 *  the roll-up has to happen here rather than at query time. */
	function isProjectActive(builderId: string): boolean {
		if (activeTriggerProjectIds.has(builderId)) return true;
		const deployments = deploymentsByBuilder[builderId];
		if (!deployments) return false;
		return deployments.some(id => activeTriggerProjectIds.has(id));
	}

	/** Same roll-up for infra: returns the "most active" status across
	 *  the builder and its deployments. Priority: `running` > anything
	 *  non-none/error > `none`. Used by the infra indicator on the
	 *  project card. */
	function rollupInfraStatus(builderId: string): string {
		const ids = [builderId, ...(deploymentsByBuilder[builderId] ?? [])];
		let best: string = 'none';
		for (const id of ids) {
			const s = infraStatuses[id];
			if (!s || s === 'none' || s === 'error') continue;
			if (s === 'running') return 'running';
			if (best === 'none') best = s;
		}
		return best;
	}

	function hasTriggers(wf: ProjectDefinition): boolean { return wf.nodes.some(n => triggerNodeTypes.has(n.nodeType)); }
	function hasInfraNodes(wf: ProjectDefinition): boolean { return wf.nodes.some(n => infraNodeTypes.has(n.nodeType)); }

	async function fetchActiveTriggers() {
		try {
			const res = await fetch(api.listTriggers(), { credentials: 'include' });
			if (res.ok) {
				const data = await res.json();
				const next = new Set<string>();
				for (const t of (data.triggers ?? []) as Array<{ status: string; projectId: string }>) {
					if (t.status === 'Running') next.add(t.projectId);
				}
				activeTriggerProjectIds = next;
			}
		} catch {}
	}

	/** Populate the builder->deployments mapping from `listPublications`.
	 *  The backend joins `projects.origin_project_id` onto the mapping
	 *  row so each deployment tells us which builder owns it. We also
	 *  fetch infra status for every deployment project id in the same
	 *  pass so the rollup helpers above have a complete view. */
	async function fetchDeploymentsAndInfra() {
		const builderIds = new Set($projects.map(p => p.id));
		const byBuilder: Record<string, string[]> = {};
		try {
			const list = await listPublications();
			for (const p of list) {
				if (!p.project_id || !p.origin_project_id) continue;
				if (!builderIds.has(p.origin_project_id)) continue;
				(byBuilder[p.origin_project_id] ??= []).push(p.project_id);
			}
		} catch (e) {
			console.warn('[Dashboard] Failed to load publications for rollup:', e);
		}
		deploymentsByBuilder = byBuilder;

		// Fetch infra for every project id we'll need (builders + their
		// deployment descendants). Previously this only fetched the
		// builder list, so a running deployment infra was invisible.
		//
		// M1: cap concurrency at INFRA_FETCH_CONCURRENCY to avoid
		// firing N parallel requests when a user has hundreds of
		// deployments. A small worker pool keeps the dashboard
		// responsive on cold mount without pinning the server.
		const allIds = new Set<string>(Array.from(builderIds));
		for (const ids of Object.values(byBuilder)) {
			for (const id of ids) allIds.add(id);
		}
		const s: Record<string, string> = {};
		await runWithConcurrency(
			Array.from(allIds),
			INFRA_FETCH_CONCURRENCY,
			async (id) => {
				try {
					const r = await authFetch(api.getInfraStatus(id));
					s[id] = r.ok ? ((await r.json()).status as string) : 'none';
				} catch (e) {
					console.warn(`[Dashboard] Failed to fetch infra status for ${id}:`, e);
					s[id] = 'error';
				}
			},
		);
		infraStatuses = s;
	}

	/** Max parallel `getInfraStatus` requests. Low enough to stay
	 *  polite to the backend when a user has many deployments, high
	 *  enough that a 10-project dashboard still loads in ~one RTT. */
	const INFRA_FETCH_CONCURRENCY = 8;

	/** Minimal promise-pool: walks `items` with at most `limit`
	 *  concurrent `worker` invocations. Swallows nothing — the
	 *  caller is expected to handle errors inside `worker`. */
	async function runWithConcurrency<T>(
		items: T[],
		limit: number,
		worker: (item: T) => Promise<void>,
	): Promise<void> {
		if (items.length === 0) return;
		const queue = items.slice();
		const runners: Promise<void>[] = [];
		for (let i = 0; i < Math.min(limit, queue.length); i++) {
			runners.push(
				(async () => {
					while (queue.length > 0) {
						const item = queue.shift();
						if (item === undefined) return;
						await worker(item);
					}
				})(),
			);
		}
		await Promise.all(runners);
	}

	// ── Community projects (injected by parent website in cloud mode) ──
	type CommunityProject = {
		id: string;
		projectName: string;
		description: string | null;
		metadata: Record<string, unknown>;
		adminTested: boolean;
		cloneCount: number;
		likeCount: number;
		tags: string[];
		user: { id: string; displayUsername: string; image: string | null };
	};
	let communityProjects = $state<CommunityProject[]>([]);
	let communityHasMore = $state(false);
	let communitySearch = $state('');
	let communitySort = $state<'newest' | 'most_liked' | 'most_cloned'>('newest');
	let communityTestedOnly = $state(false);
	let communitySearchTimeout: ReturnType<typeof setTimeout> | null = null;

	function sendCommunityAction(action: string, projectId?: string) {
		if (browser && window.parent !== window) {
			window.parent.postMessage({ type: 'communityAction', action, projectId }, '*');
		}
	}

	function requestCommunityRefresh() {
		if (browser && window.parent !== window) {
			window.parent.postMessage({
				type: 'requestCommunityProjects',
				search: communitySearch,
				sort: communitySort,
				tested: communityTestedOnly,
			}, '*');
		}
	}

	function onCommunitySearchInput() {
		if (communitySearchTimeout) clearTimeout(communitySearchTimeout);
		communitySearchTimeout = setTimeout(requestCommunityRefresh, 300);
	}

	// ── Prompt bar (injected by parent website in cloud mode) ──
	let promptBarEnabled = $state(false);
	let promptText = $state('');
	let isRecording = $state(false);
	let isStopping = $state(false);
	let partialTranscript = $state('');
	let audioLevels = $state<number[]>(new Array(48).fill(0));
	let recordingSeconds = $state(0);
	let isSubmitting = $state(false);
	let textareaEl: HTMLTextAreaElement | null = $state(null);
	let ghostOverlayEl: HTMLDivElement | null = $state(null);
	let sendAfterRecording = $state(false);
	let textBeforeRecording = '';
	let lastAppendedTranscript = '';  // track what we already appended to avoid re-adding

	let projectPendingDelete = $state<ProjectDefinition | null>(null);
	let deleteInFlight = $state(false);

	function requestDeleteProject(project: ProjectDefinition, event: MouseEvent) {
		event.stopPropagation();
		event.preventDefault();
		projectPendingDelete = project;
	}

	async function confirmDeleteProject() {
		if (!projectPendingDelete || deleteInFlight) return;
		deleteInFlight = true;
		const target = projectPendingDelete;
		const result = await projects.remove(target.id);
		deleteInFlight = false;
		if (result.ok) {
			toast.success(`Deleted "${target.name}"`);
			projectPendingDelete = null;
			return;
		}
		if (result.status === 409) {
			toast.error('Cannot delete project', { description: result.message });
		} else {
			toast.error('Failed to delete project', { description: result.message });
		}
		projectPendingDelete = null;
	}

	async function createBlankProject() {
		const created = await projects.add({ name: 'Untitled project' });
		if (created) {
			te.project.created(created.id, 'blank');
			goto(`/projects/${created.id}`);
		}
	}

	function importProject() {
		const input = document.createElement('input');
		input.type = 'file';
		input.accept = '.json';
		input.onchange = async (e) => {
			const file = (e.target as HTMLInputElement).files?.[0];
			if (!file) return;
			try {
				const text = await file.text();
				const imported = JSON.parse(text);
				if (!imported.weftCode) {
					alert('Unrecognized project format');
					return;
				}
				const created = await projects.add({
					name: imported.name || file.name.replace(/\.json$/, ''),
					description: imported.description,
					weftCode: imported.weftCode,
					loomCode: imported.loomCode,
				});
				if (created) {
					if (Array.isArray(imported.testConfigs)) {
						for (const tc of imported.testConfigs) {
							try {
								await authFetch(`/api/projects/${created.id}/test-configs`, {
									method: 'POST',
									headers: { 'Content-Type': 'application/json' },
									body: JSON.stringify({ name: tc.name, description: tc.description, mocks: tc.mocks }),
								});
							} catch {}
						}
					}
					te.project.created(created.id, 'import');
					goto(`/projects/${created.id}`);
				}
			} catch (err) {
				alert('Failed to import project: ' + (err instanceof Error ? err.message : String(err)));
			}
		};
		input.click();
	}

	function handlePromptSubmit() {
		if (isSubmitting) return;
		if (isRecording) {
			sendAfterRecording = true;
			if (browser && window.parent !== window)
				window.parent.postMessage({ type: 'promptMicStop' }, '*');
			return;
		}
		const text = promptText.trim();
		if (!text) return;
		isSubmitting = true;
		if (browser && window.parent !== window)
			window.parent.postMessage({ type: 'promptSubmit', text }, '*');
	}

	function handlePromptMicToggle() {
		if (!isRecording) {
			textBeforeRecording = promptText;
			lastAppendedTranscript = '';
		}
		if (browser && window.parent !== window)
			window.parent.postMessage({ type: isRecording ? 'promptMicStop' : 'promptMicStart' }, '*');
	}

	function handlePromptKeydown(e: KeyboardEvent) {
		if (e.key === 'Enter' && !e.shiftKey) { e.preventDefault(); handlePromptSubmit(); }
	}

	let measureEl: HTMLDivElement | null = $state(null);

	function autoResize() {
		if (!textareaEl) return;
		// Use a hidden measuring div to calculate height including ghost preview,
		// so we never touch textareaEl.value (which would reset cursor position)
		if (measureEl) {
			const hasPreview = (isRecording || isStopping) && partialTranscript;
			const sep = promptText && !promptText.endsWith(' ') && !promptText.endsWith('\n') ? ' ' : '';
			const fullText = hasPreview ? promptText + sep + partialTranscript : promptText;
			measureEl.textContent = fullText + '\n';  // trailing newline ensures last line is measured
			const fullHeight = measureEl.scrollHeight;
			const measured = Math.min(fullHeight, 400);
			textareaEl.style.height = 'auto';
			textareaEl.style.height = measured + 'px';
			textareaEl.style.overflowY = fullHeight > 400 ? 'auto' : 'hidden';
			// Add bottom padding so textarea scroll range matches the ghost overlay
			if (hasPreview && fullHeight > 400) {
				measureEl.textContent = promptText + '\n';
				const textOnlyHeight = measureEl.scrollHeight;
				const extraHeight = fullHeight - textOnlyHeight;
				textareaEl.style.paddingBottom = extraHeight > 0 ? `${extraHeight}px` : '';
				measureEl.textContent = fullText + '\n';
				textareaEl.scrollTop = textareaEl.scrollHeight;
				if (ghostOverlayEl) ghostOverlayEl.scrollTop = ghostOverlayEl.scrollHeight;
			} else {
				textareaEl.style.paddingBottom = '';
			}
		} else {
			textareaEl.style.height = 'auto';
			const next = Math.min(textareaEl.scrollHeight, 400);
			textareaEl.style.height = next + 'px';
			textareaEl.style.overflowY = textareaEl.scrollHeight > 400 ? 'auto' : 'hidden';
		}
	}

	function fmtRec(secs: number): string {
		return `${Math.floor(secs / 60)}:${String(secs % 60).padStart(2, '0')}`;
	}

	onMount(() => {
		if (!browser) return;
		fetchActiveTriggers();
		fetchDeploymentsAndInfra();

		async function handleMessage(event: MessageEvent) {
			const d = event.data;
			if (!d?.type) return;
			if (d.type === 'injectPromptBar') promptBarEnabled = true;
			if (d.type === 'injectCommunityProjects') {
				communityProjects = d.projects || [];
				communityHasMore = d.hasMore ?? false;
			}
			if (d.type === 'createProjectAndNavigate') {
				const created = await projects.add({ name: 'Untitled project' });
				if (created) goto(`/projects/${created.id}`);
			}
			if (d.type === 'promptRecordingState') {
				const was = isRecording || isStopping;
				isRecording = d.isRecording ?? false;
				isStopping = d.isStopping ?? false;
				audioLevels = d.audioLevels ?? new Array(48).fill(0);
				recordingSeconds = d.recordingSeconds ?? 0;
				const newPartial = d.partialTranscript ?? '';
				if (newPartial !== partialTranscript) {
					partialTranscript = newPartial;
					requestAnimationFrame(autoResize);
				}
				if (was && !isRecording && !isStopping && sendAfterRecording) {
					sendAfterRecording = false;
					setTimeout(() => handlePromptSubmit(), 200);
				}
			}
			if (d.type === 'promptTranscript') {
				const fullTranscript = d.text ?? '';
				// Only append the part we haven't already appended
				let delta = fullTranscript;
				if (lastAppendedTranscript && fullTranscript.startsWith(lastAppendedTranscript)) {
					delta = fullTranscript.slice(lastAppendedTranscript.length).trimStart();
				}
				if (delta) {
					const separator = promptText.length > 0 && !promptText.endsWith(' ') && !promptText.endsWith('\n') ? ' ' : '';
					promptText = promptText + separator + delta;
				}
				lastAppendedTranscript = fullTranscript;
				partialTranscript = '';
				requestAnimationFrame(autoResize);
			}
			if (d.type === 'promptSubmitComplete') { isSubmitting = false; promptText = ''; }
		}
		window.addEventListener('message', handleMessage);
		return () => {
			window.removeEventListener('message', handleMessage);
			if (communitySearchTimeout) clearTimeout(communitySearchTimeout);
		};
	});

	// ── Pagination: fill rows of 3 ──
	const COLS = 3;
	const ROWS = 3;
	const PAGE_SIZE = COLS * ROWS;
	let currentPage = $state(0);
	let totalPages = $derived(Math.max(1, Math.ceil($projects.length / PAGE_SIZE)));
	let paginatedProjects = $derived($projects.slice(currentPage * PAGE_SIZE, (currentPage + 1) * PAGE_SIZE));

	function nodeCount(wf: ProjectDefinition): number {
		return wf.nodes.filter(n => n.nodeType !== 'Group').length;
	}

	const CATEGORY_COLORS: Record<string, string> = {
		AI: '#7c6f9f', Utility: '#6366f1', Flow: '#f59e0b',
		Triggers: '#0ea5e9', Data: '#5a9eb8', Infrastructure: '#10b981', Debug: '#b05574',
	};

	function projectAccentColor(wf: ProjectDefinition): string {
		const n = wf.nodes.find(n => n.nodeType !== 'Group');
		if (!n) return '#d4d4d8';
		const def = ALL_NODES.find(an => an.type === n.nodeType);
		return (def?.category && CATEGORY_COLORS[def.category]) || '#d4d4d8';
	}
</script>

<div class="min-h-screen pt-20 px-6 pb-12" style="background: #f8f9fa; background-image: radial-gradient(circle, #d4d4d8 1px, transparent 1px); background-size: 24px 24px;">
	<div class="max-w-5xl mx-auto">

		<!-- Prompt bar (cloud mode, injected) -->
		{#if promptBarEnabled}
			<div class="mb-8 mt-4">
				<div class="flex flex-col bg-white border border-zinc-300 rounded-[16px] shadow-sm focus-within:shadow-md focus-within:border-zinc-400 transition-shadow">
					<!-- 1. Textarea with inline ghost preview -->
					<div class="relative pl-4 pr-2 pt-3 pb-1" style="min-height: 28px;">
						<!-- Hidden div for measuring height (same box as textarea) -->
						<div
							bind:this={measureEl}
							class="text-[14px] leading-relaxed py-1 whitespace-pre-wrap break-words w-full"
							style="position: absolute; top: 0; left: 0; right: 0; visibility: hidden; pointer-events: none; padding: 0.75rem 0.5rem 0.25rem 1rem; min-height: 28px;"
							aria-hidden="true"
						></div>
						<!-- Ghost overlay: same font/padding, sits behind textarea -->
						{#if (isRecording || isStopping) && partialTranscript}
							<div
								bind:this={ghostOverlayEl}
								class="absolute top-0 left-0 right-0 bottom-0 pl-4 pr-2 pt-3 pb-1 pointer-events-none overflow-y-auto"
								aria-hidden="true"
								style="font-size: 14px; line-height: 1.625; padding-top: calc(0.75rem + 0.25rem); word-wrap: break-word; white-space: pre-wrap; scrollbar-width: none; -ms-overflow-style: none;"
							><span style="visibility: hidden;">{promptText}{promptText && !promptText.endsWith(' ') && !promptText.endsWith('\n') ? ' ' : ''}</span><span class="text-zinc-400 italic">{partialTranscript}</span></div>
						{/if}
						<textarea
							bind:this={textareaEl}
							bind:value={promptText}
							oninput={autoResize}
							onscroll={() => { if (ghostOverlayEl) ghostOverlayEl.scrollTop = textareaEl?.scrollTop ?? 0; }}
							onpaste={() => requestAnimationFrame(autoResize)}
							onkeydown={handlePromptKeydown}
							placeholder={isRecording ? '' : 'Describe what you want to build...'}
							rows={1}
							class="relative w-full text-[14px] bg-transparent text-zinc-900 placeholder:text-zinc-400 resize-none focus:outline-none py-1 leading-relaxed min-h-[28px] max-h-[400px]"
							style="overflow-y: hidden; position: relative; z-index: 1; background: transparent;"
						></textarea>
					</div>

					<!-- 3. Bottom row: wave + time + mic + send -->
					<div class="flex items-center px-3 pb-2.5 pt-1 gap-1.5">
						{#if isRecording}
							<div class="flex items-center justify-center gap-[2px] h-[28px] flex-1 min-w-0">
								{#each audioLevels as level}
									<div
										class="flex-1 min-w-0 max-w-[4px] rounded-full"
										style="height: {Math.max(3, level * 26)}px; background: rgba(239, 68, 68, {0.35 + level * 0.65}); transition: height 60ms ease-out, background 60ms ease-out;"
									></div>
								{/each}
							</div>
							<span class="text-[10px] text-red-400/80 font-medium tabular-nums shrink-0">{fmtRec(recordingSeconds)}</span>
						{:else}
							<div class="flex-1"></div>
						{/if}
						<button onclick={handlePromptMicToggle} class="w-8 h-8 flex items-center justify-center rounded-full transition-colors shrink-0 {isRecording ? 'bg-red-500 text-white animate-pulse' : isStopping ? 'bg-amber-500 text-white' : 'text-zinc-400 hover:text-zinc-600 hover:bg-zinc-100'}" title={isRecording ? 'Stop recording' : 'Start recording'}>
							{#if isStopping}
								<svg class="animate-spin" width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><path d="M21 12a9 9 0 1 1-6.219-8.56"/></svg>
							{:else if isRecording}
								<svg width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><line x1="1" y1="1" x2="23" y2="23"/><path d="M9 9v3a3 3 0 0 0 5.12 2.12M15 9.34V4a3 3 0 0 0-5.94-.6"/><path d="M17 16.95A7 7 0 0 1 5 12v-2m14 0v2c0 .76-.13 1.49-.35 2.17"/><line x1="12" y1="19" x2="12" y2="23"/><line x1="8" y1="23" x2="16" y2="23"/></svg>
							{:else}
								<svg width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="M12 1a3 3 0 0 0-3 3v8a3 3 0 0 0 6 0V4a3 3 0 0 0-3-3z"/><path d="M19 10v2a7 7 0 0 1-14 0v-2"/><line x1="12" y1="19" x2="12" y2="23"/><line x1="8" y1="23" x2="16" y2="23"/></svg>
							{/if}
						</button>
						<button onclick={handlePromptSubmit} disabled={isSubmitting || (!promptText.trim() && !isRecording)} class="w-8 h-8 flex items-center justify-center rounded-[8px] transition-colors shrink-0 {promptText.trim() && !isSubmitting ? 'bg-zinc-900 text-white hover:bg-zinc-800' : 'bg-zinc-100 text-zinc-400'}" title="Send">
							{#if isSubmitting}
								<svg class="animate-spin" width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><path d="M21 12a9 9 0 1 1-6.219-8.56"/></svg>
							{:else}
								<svg width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2.5" stroke-linecap="round" stroke-linejoin="round"><line x1="12" y1="19" x2="12" y2="5"/><polyline points="5 12 12 5 19 12"/></svg>
							{/if}
						</button>
					</div>
				</div>
			</div>
		{/if}

		<!-- New project / Import -->
		<div class="flex gap-3 mb-5">
			<button
				onclick={createBlankProject}
				class="flex-1 flex items-center justify-center gap-2 px-4 py-3.5 bg-white/80 border border-dashed border-zinc-300 rounded-xl text-[13px] font-medium text-zinc-500 hover:text-zinc-800 hover:border-zinc-400 hover:bg-white hover:shadow-sm transition-all"
			>
				<svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><line x1="12" y1="5" x2="12" y2="19"/><line x1="5" y1="12" x2="19" y2="12"/></svg>
				New project
			</button>
			<button
				onclick={importProject}
				class="flex items-center justify-center gap-2 px-4 py-3.5 bg-white/80 border border-dashed border-zinc-300 rounded-xl text-[13px] font-medium text-zinc-500 hover:text-zinc-800 hover:border-zinc-400 hover:bg-white hover:shadow-sm transition-all"
			>
				<svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="M21 15v4a2 2 0 0 1-2 2H5a2 2 0 0 1-2-2v-4"/><polyline points="7 10 12 15 17 10"/><line x1="12" y1="15" x2="12" y2="3"/></svg>
				Import
			</button>
		</div>

		<!-- Projects grid -->
		{#if $projects.length === 0}
			<div class="flex flex-col items-center justify-center py-24">
				<div class="w-14 h-14 rounded-full bg-white border border-zinc-200 flex items-center justify-center mb-5" style="box-shadow: 0 2px 8px rgba(0,0,0,0.06);">
					<svg width="22" height="22" viewBox="0 0 24 24" fill="none" stroke="#a1a1aa" stroke-width="1.5" stroke-linecap="round" stroke-linejoin="round"><path d="M14.5 2H6a2 2 0 0 0-2 2v16a2 2 0 0 0 2 2h12a2 2 0 0 0 2-2V7.5L14.5 2z"/><polyline points="14 2 14 8 20 8"/></svg>
				</div>
				<p class="text-[15px] font-medium text-zinc-600 mb-1">No projects yet</p>
				<p class="text-[13px] text-zinc-400 mb-5">Create one to get started</p>
				<button onclick={createBlankProject} class="px-5 py-2.5 bg-zinc-900 text-white text-[13px] font-medium rounded-lg hover:bg-zinc-800 transition-colors">
					Create your first project
				</button>
			</div>
		{:else}
			<div class="grid grid-cols-1 sm:grid-cols-2 lg:grid-cols-3 gap-4">
				{#each paginatedProjects as project}
					{@const accent = projectAccentColor(project)}
					{@const infraStatus = rollupInfraStatus(project.id)}
					{@const triggerActive = hasTriggers(project) && isProjectActive(project.id)}
					<div class="group relative">
					<button
						onclick={() => goto(`/projects/${project.id}`)}
						class="text-left rounded-xl overflow-hidden transition-all duration-150 cursor-pointer w-full"
						style="background: white; border: 1px solid #e4e4e7; box-shadow: 0 1px 3px rgba(0,0,0,0.06), 0 4px 12px rgba(0,0,0,0.03); min-height: 140px; display: flex; flex-direction: column;"
						onmouseenter={(e) => { const el = e.currentTarget as HTMLElement; el.style.borderColor = accent; el.style.boxShadow = `0 0 0 1px ${accent}40, 0 4px 16px rgba(0,0,0,0.1)`; }}
						onmouseleave={(e) => { const el = e.currentTarget as HTMLElement; el.style.borderColor = '#e4e4e7'; el.style.boxShadow = '0 1px 3px rgba(0,0,0,0.06), 0 4px 12px rgba(0,0,0,0.03)'; }}
					>
						<!-- Accent bar -->
						<div class="h-[3px] shrink-0" style="background: {accent};"></div>

						<div class="px-4 pt-3.5 pb-3.5 flex-1 flex flex-col">
							<!-- Name -->
							<p class="text-[14px] font-semibold text-zinc-800 truncate mb-1">{project.name}</p>

							<!-- Description -->
							{#if project.description}
								<p class="text-[12px] text-zinc-400 line-clamp-2 mb-auto leading-relaxed">{project.description}</p>
							{:else}
								<div class="mb-auto"></div>
							{/if}

							<!-- Bottom: meta + status -->
							<div class="flex items-center gap-2 mt-3 pt-2.5 border-t border-zinc-100">
								<span class="text-[10px] font-mono text-zinc-500 bg-zinc-100 px-1.5 py-0.5 rounded">{nodeCount(project)}</span>

								<!-- Infra status (same as projects page) -->
								{#if hasInfraNodes(project)}
									{@const status = infraStatus}
									<div class="inline-flex items-center gap-1.5 text-[11px] {
										status === 'running' ? 'text-emerald-600' :
										status === 'starting' || status === 'stopping' || status === 'terminating' ? 'text-amber-600' :
										status === 'failed' ? 'text-red-500' :
										'text-zinc-400'
									}" title="Infrastructure: {status}">
										<svg width="12" height="12" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><rect x="2" y="2" width="20" height="8" rx="2" ry="2"/><rect x="2" y="14" width="20" height="8" rx="2" ry="2"/><line x1="6" y1="6" x2="6.01" y2="6"/><line x1="6" y1="18" x2="6.01" y2="18"/></svg>
										{#if status === 'running'}
											<span class="flex h-1.5 w-1.5 relative">
												<span class="animate-ping absolute inline-flex h-full w-full rounded-full bg-emerald-400 opacity-75"></span>
												<span class="relative inline-flex rounded-full h-1.5 w-1.5 bg-emerald-500"></span>
											</span>
										{:else if status === 'starting' || status === 'stopping' || status === 'terminating'}
											<svg class="animate-spin" width="10" height="10" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2.5"><path d="M21 12a9 9 0 1 1-6.219-8.56"/></svg>
										{:else if status === 'failed'}
											<svg width="10" height="10" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2.5"><circle cx="12" cy="12" r="10"/><line x1="15" y1="9" x2="9" y2="15"/><line x1="9" y1="9" x2="15" y2="15"/></svg>
										{:else if status === 'stopped'}
											<svg width="8" height="8" viewBox="0 0 24 24" fill="currentColor" class="text-zinc-400"><rect x="4" y="4" width="16" height="16" rx="2"/></svg>
										{:else if status === 'terminated'}
											<svg width="8" height="8" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="3" class="text-zinc-300"><line x1="18" y1="6" x2="6" y2="18"/><line x1="6" y1="6" x2="18" y2="18"/></svg>
										{/if}
									</div>
								{/if}

								<!-- Trigger status (same as projects page) -->
								{#if hasTriggers(project)}
									<div class="inline-flex items-center gap-1.5 text-[11px] {triggerActive ? 'text-violet-600' : 'text-zinc-400'}" title="Trigger: {triggerActive ? 'active' : 'inactive'}">
										<svg width="12" height="12" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="M13 2L3 14h9l-1 8 10-12h-9l1-8z"/></svg>
										{#if triggerActive}
											<span class="flex h-1.5 w-1.5 relative">
												<span class="animate-ping absolute inline-flex h-full w-full rounded-full bg-violet-400 opacity-75"></span>
												<span class="relative inline-flex rounded-full h-1.5 w-1.5 bg-violet-500"></span>
											</span>
										{:else}
											<span class="h-1.5 w-1.5 rounded-full bg-zinc-300"></span>
										{/if}
									</div>
								{/if}

								<span class="text-[10px] text-zinc-400 ml-auto shrink-0">{formatDate(project.updatedAt)}</span>
							</div>
						</div>
					</button>
					<button
						type="button"
						aria-label="Delete project"
						title="Delete project"
						onclick={(e) => requestDeleteProject(project, e)}
						class="absolute top-2 right-2 p-1.5 rounded-md bg-white/90 backdrop-blur-sm border border-zinc-200 text-zinc-400 opacity-0 group-hover:opacity-100 hover:text-red-600 hover:border-red-200 hover:bg-red-50 transition-all duration-150 shadow-sm"
					>
						<svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><polyline points="3 6 5 6 21 6"/><path d="M19 6l-1 14a2 2 0 0 1-2 2H8a2 2 0 0 1-2-2L5 6"/><path d="M10 11v6"/><path d="M14 11v6"/><path d="M9 6V4a2 2 0 0 1 2-2h2a2 2 0 0 1 2 2v2"/></svg>
					</button>
					</div>
				{/each}
			</div>

			{#if totalPages > 1}
				<div class="flex items-center justify-center gap-3 mt-8">
					<button
						onclick={() => currentPage = Math.max(0, currentPage - 1)}
						disabled={currentPage === 0}
						class="px-3 py-1.5 text-[12px] font-medium rounded-lg transition-colors {currentPage === 0 ? 'text-zinc-300 cursor-default' : 'text-zinc-500 hover:text-zinc-700 hover:bg-white hover:shadow-sm'}"
					>Previous</button>
					<span class="text-[11px] text-zinc-400 font-mono tabular-nums">{currentPage + 1} / {totalPages}</span>
					<button
						onclick={() => currentPage = Math.min(totalPages - 1, currentPage + 1)}
						disabled={currentPage >= totalPages - 1}
						class="px-3 py-1.5 text-[12px] font-medium rounded-lg transition-colors {currentPage >= totalPages - 1 ? 'text-zinc-300 cursor-default' : 'text-zinc-500 hover:text-zinc-700 hover:bg-white hover:shadow-sm'}"
					>Next</button>
				</div>
			{/if}
		{/if}

		<!-- Community projects (injected from website in cloud mode) -->
		{#if communityProjects.length > 0 || communitySearch || communityTestedOnly || communitySort !== 'newest'}
			<div class="mt-10">
				<!-- Header -->
				<div class="flex items-center gap-2 mb-3">
					<svg width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="#a1a1aa" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="M17 21v-2a4 4 0 0 0-4-4H5a4 4 0 0 0-4 4v2"/><circle cx="9" cy="7" r="4"/><path d="M23 21v-2a4 4 0 0 0-3-3.87"/><path d="M16 3.13a4 4 0 0 1 0 7.75"/></svg>
					<h2 class="text-[13px] font-semibold text-zinc-600">Community Projects</h2>
				</div>

				<!-- Search + Sort + Filter -->
				<div class="flex items-center gap-2 mb-4 flex-wrap">
					<input
						type="text"
						bind:value={communitySearch}
						oninput={onCommunitySearchInput}
						placeholder="Search name, description, tags..."
						class="flex-1 min-w-[180px] px-3 py-1.5 text-[12px] bg-white border border-zinc-200 rounded-lg focus:outline-none focus:border-zinc-400 transition-colors"
					/>
					<div class="flex gap-0.5 bg-white border border-zinc-200 rounded-lg p-0.5">
						{#each [['newest', 'New'], ['most_liked', 'Liked'], ['most_cloned', 'Cloned']] as [value, label]}
							<button
								onclick={() => { communitySort = value as any; requestCommunityRefresh(); }}
								class="px-2.5 py-1 text-[10px] font-medium rounded-md transition-colors {communitySort === value ? 'bg-zinc-800 text-white' : 'text-zinc-400 hover:text-zinc-600'}"
							>{label}</button>
						{/each}
					</div>
					<label class="flex items-center gap-1 text-[10px] text-zinc-400 cursor-pointer shrink-0">
						<input
							type="checkbox"
							bind:checked={communityTestedOnly}
							onchange={requestCommunityRefresh}
							class="rounded border-zinc-300 w-3 h-3"
						/>
						Tested
					</label>
				</div>

				{#if communityProjects.length === 0}
					<p class="text-[12px] text-zinc-400 text-center py-8">No community projects match your search.</p>
				{:else}
				<div class="grid grid-cols-1 sm:grid-cols-2 lg:grid-cols-3 gap-3">
					{#each communityProjects as cp}
						<!-- svelte-ignore a11y_no_static_element_interactions -->
					<!-- svelte-ignore a11y_click_events_have_key_events -->
					<div
							onclick={() => sendCommunityAction('viewDetails', cp.id)}
							class="text-left rounded-xl overflow-hidden transition-all duration-150 cursor-pointer"
							style="background: white; border: 1px solid #e4e4e7; box-shadow: 0 1px 3px rgba(0,0,0,0.04);"
							onmouseenter={(e) => { const el = e.currentTarget as HTMLElement; el.style.borderColor = '#a1a1aa'; el.style.boxShadow = '0 2px 8px rgba(0,0,0,0.08)'; }}
							onmouseleave={(e) => { const el = e.currentTarget as HTMLElement; el.style.borderColor = '#e4e4e7'; el.style.boxShadow = '0 1px 3px rgba(0,0,0,0.04)'; }}
						>
							{#if cp.adminTested}
								<div class="h-[3px] shrink-0 bg-emerald-500"></div>
							{/if}
							<div class="px-4 pt-3 pb-3">
								<div class="flex items-center gap-2 mb-1.5">
									<p class="text-[13px] font-semibold text-zinc-700 truncate flex-1">{cp.projectName}</p>
									{#if cp.adminTested}
										<span class="text-[9px] font-medium bg-emerald-100 text-emerald-700 px-1.5 py-0.5 rounded shrink-0">Tested</span>
									{/if}
								</div>
								{#if cp.description}
									<p class="text-[11px] text-zinc-400 line-clamp-2 mb-1.5 leading-relaxed">{cp.description}</p>
								{/if}
								{#if cp.tags && cp.tags.length > 0}
									<div class="flex flex-wrap gap-1 mb-1.5">
										{#each cp.tags.slice(0, 3) as tag}
											<span class="text-[9px] text-zinc-400 bg-zinc-100 px-1.5 py-0.5 rounded">{tag}</span>
										{/each}
										{#if cp.tags.length > 3}
											<span class="text-[9px] text-zinc-300">+{cp.tags.length - 3}</span>
										{/if}
									</div>
								{/if}
								<div class="flex items-center gap-2 pt-2 border-t border-zinc-100">
									{#if cp.user.image}
										<img src={cp.user.image} alt="" class="w-4 h-4 rounded-full" />
									{:else}
										<div class="w-4 h-4 rounded-full bg-zinc-200 flex items-center justify-center text-[8px] font-bold text-zinc-500">
											{cp.user.displayUsername.charAt(0).toUpperCase()}
										</div>
									{/if}
									<span class="text-[10px] text-zinc-400">{cp.user.displayUsername}</span>
									<div class="ml-auto flex items-center gap-2">
										{#if cp.likeCount > 0}
											<span class="text-[10px] text-zinc-400 flex items-center gap-0.5">
												<svg width="10" height="10" viewBox="0 0 24 24" fill="currentColor" stroke="none"><path d="M12 21.35l-1.45-1.32C5.4 15.36 2 12.28 2 8.5 2 5.42 4.42 3 7.5 3c1.74 0 3.41.81 4.5 2.09C13.09 3.81 14.76 3 16.5 3 19.58 3 22 5.42 22 8.5c0 3.78-3.4 6.86-8.55 11.54L12 21.35z"/></svg>
												{cp.likeCount}
											</span>
										{/if}
										{#if cp.cloneCount > 0}
											<span class="text-[10px] text-zinc-400 flex items-center gap-0.5">
												<svg width="10" height="10" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><path d="M21 15v4a2 2 0 0 1-2 2H5a2 2 0 0 1-2-2v-4"/><polyline points="7 10 12 15 17 10"/><line x1="12" y1="15" x2="12" y2="3"/></svg>
												{cp.cloneCount}
											</span>
										{/if}
									</div>
								</div>
							</div>
						</div>
					{/each}
				</div>
				{/if}
			</div>
		{/if}
	</div>
</div>

<AlertDialog.Root
	open={projectPendingDelete !== null}
	onOpenChange={(open) => { if (!open) projectPendingDelete = null; }}
>
	<AlertDialog.Content>
		<AlertDialog.Header>
			<AlertDialog.Title>Delete project</AlertDialog.Title>
			<AlertDialog.Description>
				{#if projectPendingDelete}
					This will permanently delete <span class="font-semibold text-zinc-900">{projectPendingDelete.name}</span>. This action cannot be undone.
				{/if}
			</AlertDialog.Description>
		</AlertDialog.Header>
		<AlertDialog.Footer>
			<AlertDialog.Cancel disabled={deleteInFlight}>Cancel</AlertDialog.Cancel>
			<AlertDialog.Action
				disabled={deleteInFlight}
				onclick={confirmDeleteProject}
				class="bg-red-600 text-white hover:bg-red-700"
			>
				{deleteInFlight ? 'Deleting…' : 'Delete'}
			</AlertDialog.Action>
		</AlertDialog.Footer>
	</AlertDialog.Content>
</AlertDialog.Root>
