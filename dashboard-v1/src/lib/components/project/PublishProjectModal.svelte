<script lang="ts">
	import type { PublishedProject } from '$lib/publish/client';
	import {
		listPublications,
		deletePublication,
		updatePublication,
	} from '$lib/publish/client';
	import { STORAGE_KEYS } from '$lib/utils';
	import { onMount } from 'svelte';
	import { X, ExternalLink, Trash2, Pause, Play, Settings } from '@lucide/svelte';

	/** Per-slug rate limit bounds. Must match cloud-api's
	 *  `PUBLISH_RATE_LIMIT_MAX_PER_MINUTE` and the default. Changing
	 *  these numbers requires bumping the backend constants too. */
	const RATE_LIMIT_MAX = 500;
	const RATE_LIMIT_DEFAULT = 60;

	let {
		projectData,
		sensitiveValueCount = 0,
		onPublishNew,
		onOverwrite,
		onClose,
	}: {
		projectData: { projectId: string; projectName: string; description: string | null };
		/** How many sensitive values the current project holds. Drives the
		 *  strip-sensitive checkbox warning copy. Computed by the parent
		 *  using `countSensitiveValues` from `$lib/ai/sanitize` because
		 *  the modal doesn't have the raw project nodes. */
		sensitiveValueCount?: number;
		/** Publish a brand-new slug. The parent sanitizes weft (when
		 *  `stripSensitive` is true), computes the visitor access allowlist
		 *  from the current loom manifest, and calls `publishProject` with
		 *  the full payload. */
		onPublishNew: (args: {
			slug: string;
			description: string;
			stripSensitive: boolean;
			rateLimitPerMinute: number | null;
		}) => Promise<void>;
		/** Overwrite an existing slug (re-publish in place). Same sanitize
		 *  and allowlist semantics as `onPublishNew`. */
		onOverwrite: (args: {
			row: PublishedProject;
			stripSensitive: boolean;
			rateLimitPerMinute: number | null;
		}) => Promise<void>;
		/**
		 * Close callback. Passes an optional result so the parent can react
		 * to what happened in the modal:
		 *   - `slug`: the slug that was just published (shown as a toast).
		 *   - `deletedCurrentProject: true`: the user unpublished the
		 *     deployment they were currently viewing. Parent should navigate
		 *     away (back to the origin builder, or /dashboard) because the
		 *     current project row no longer exists.
		 */
		onClose: (result?: { slug?: string; deletedCurrentProject?: boolean }) => void;
	} = $props();

	function slugify(raw: string): string {
		return raw
			.trim()
			.toLowerCase()
			.replace(/[^a-z0-9-]+/g, '-')
			.replace(/-+/g, '-')
			.replace(/^-+|-+$/g, '')
			.slice(0, 63);
	}

	let allPublished = $state<PublishedProject[]>([]);
	let loading = $state(true);
	let busySlug = $state<string | null>(null); // tracks which row is mid-action
	let error = $state('');

	/** Navigate to a deployment project's admin page. Each deployment is
	 *  an independent `projects` row with its own id; opening it is just a
	 *  normal project navigation. The page detects `isDeployment` and
	 *  locks the builder toggle. */
	function openDeployment(projectId: string | null) {
		if (!projectId) return;
		onClose();
		// Using a full navigation so the project page re-hydrates cleanly.
		window.location.href = `/projects/${projectId}`;
	}

	// New publication form
	// svelte-ignore state_referenced_locally
	let slug = $state(slugify(projectData.projectName));
	// svelte-ignore state_referenced_locally
	let description = $state(projectData.description ?? '');
	let publishing = $state(false);

	// Strip sensitive fields (passwords, API keys) before publishing.
	// Default ON. An admin whose loom keeps secrets admin-only might want
	// to leave them embedded in the deployment, which is why we expose the
	// opt-out. The warning copy changes based on `sensitiveValueCount` so
	// the deployer sees exactly how many values are at stake.
	let stripSensitive = $state(true);

	// Per-slug rate limit in req/min. Deployer picks a value up to the
	// hard cap; null/empty means "use default" on the backend.
	let rateLimitInput = $state<string>(String(RATE_LIMIT_DEFAULT));
	let rateLimitError = $state<string>('');

	// Current user's unique pseudonym. Used for the publication URL
	// `/p/<username>/<slug>`. Populated on mount from session storage; if
	// the user hasn't set one yet we surface a warning in the form and
	// block publishing until they pick one.
	let currentUsername = $state<string | null>(null);

	// Only publications owned by the current user AND bound to the current
	// project get "Overwrite" semantics. Other projects' publications still
	// appear in the list so the user can see everything they own in one place.
	const forCurrentProject = $derived(allPublished.filter(p => p.project_id === projectData.projectId));
	const forOtherProjects = $derived(allPublished.filter(p => p.project_id !== projectData.projectId));
	const hasAny = $derived(allPublished.length > 0);

	/** Extract a user-facing error message from a thrown value. Keeps all
	 *  publish-action catch blocks DRY — they all want the same thing. */
	function errorMessage(e: unknown, fallback: string): string {
		return e instanceof Error ? e.message : fallback;
	}

	async function refresh() {
		try {
			allPublished = await listPublications();
		} catch (e) {
			console.error('[publish-modal] failed to list publications:', e);
			error = errorMessage(e, 'Failed to load publications');
		}
	}

	onMount(async () => {
		if (typeof sessionStorage !== 'undefined') {
			currentUsername = sessionStorage.getItem(STORAGE_KEYS.username);
		}
		await refresh();
		loading = false;
	});

	/** Parse `rateLimitInput` to a concrete number-or-null. Returns null
	 *  when the user cleared the field (falls back to backend default).
	 *  Sets `rateLimitError` and returns `undefined` when the value is
	 *  invalid so the caller can abort without publishing. */
	function parseRateLimit(): number | null | undefined {
		rateLimitError = '';
		const raw = rateLimitInput.trim();
		if (raw === '') return null;
		const n = Number(raw);
		if (!Number.isInteger(n) || n < 1 || n > RATE_LIMIT_MAX) {
			rateLimitError = `Must be an integer between 1 and ${RATE_LIMIT_MAX}. To pause visitor runs, use the Pause toggle on the deployment.`;
			return undefined;
		}
		return n;
	}

	async function handlePublishNew() {
		if (publishing) return;
		if (slug.length < 3) { error = 'Slug must be at least 3 characters'; return; }
		const rateLimit = parseRateLimit();
		if (rateLimit === undefined) return;
		publishing = true;
		error = '';
		try {
			await onPublishNew({
				slug,
				description,
				stripSensitive,
				rateLimitPerMinute: rateLimit,
			});
			await refresh();
			// Reset form to next sensible default so you can publish again
			// without reopening the modal.
			slug = slugify(projectData.projectName);
			description = projectData.description ?? '';
		} catch (e) {
			console.error('[publish-modal] publishProject failed:', e);
			error = errorMessage(e, 'Publish failed');
		} finally {
			publishing = false;
		}
	}

	async function handleOverwrite(row: PublishedProject) {
		if (busySlug) return;
		if (row.project_id !== projectData.projectId) {
			error = 'Cannot overwrite a publication that belongs to a different project';
			return;
		}
		const rateLimit = parseRateLimit();
		if (rateLimit === undefined) return;
		busySlug = row.slug;
		error = '';
		try {
			await onOverwrite({
				row,
				stripSensitive,
				rateLimitPerMinute: rateLimit,
			});
			await refresh();
		} catch (e) {
			console.error('[publish-modal] overwrite failed:', e);
			error = errorMessage(e, 'Overwrite failed');
		} finally {
			busySlug = null;
		}
	}

	async function handleTogglePause(row: PublishedProject) {
		if (busySlug) return;
		busySlug = row.slug;
		error = '';
		try {
			await updatePublication(row.slug, { is_live: !row.is_live });
			await refresh();
		} catch (e) {
			console.error('[publish-modal] toggle pause failed:', e);
			error = errorMessage(e, 'Update failed');
		} finally {
			busySlug = null;
		}
	}

	async function handleUnpublish(row: PublishedProject) {
		if (busySlug) return;
		if (!confirm(`Unpublish /p/${row.username}/${row.slug}? This will remove the public page.`)) return;
		busySlug = row.slug;
		error = '';
		// Remember if the row being unpublished is the deployment project
		// the parent is currently viewing. If so, we close the modal after
		// the delete with a flag that tells the parent to navigate away
		// the project row is gone, staying on its page would land on
		// "Project not found".
		const deletedCurrentProject = row.project_id === projectData.projectId;
		try {
			await deletePublication(row.slug);
			if (deletedCurrentProject) {
				// Don't bother refreshing the list, we're navigating away.
				onClose({ deletedCurrentProject: true });
				return;
			}
			await refresh();
		} catch (e) {
			console.error('[publish-modal] unpublish failed:', e);
			error = errorMessage(e, 'Unpublish failed');
		} finally {
			busySlug = null;
		}
	}

	function publicUrl(row: { username: string; slug: string }): string {
		return `${location.origin}/p/${encodeURIComponent(row.username)}/${encodeURIComponent(row.slug)}`;
	}
	function shortUrl(row: { username: string; slug: string }): string {
		return `/p/${row.username}/${row.slug}`;
	}
</script>

<div class="fixed inset-0 bg-black/50 flex items-center justify-center z-50 p-4" role="dialog" aria-modal="true" tabindex="-1">
	<div class="bg-white rounded-xl shadow-xl w-full max-w-5xl h-[85vh] flex flex-col">
		<div class="flex items-center justify-between px-6 py-4 border-b border-border">
			<div>
				<h2 class="text-lg font-semibold">
					{hasAny ? 'Manage deployments' : 'Publish as a page'}
				</h2>
				<p class="text-sm text-muted-foreground mt-0.5">
					Share a runnable version of this project at a public URL. Visitors use your credits.
				</p>
			</div>
			<button
				type="button"
				class="text-muted-foreground hover:text-foreground p-1 rounded hover:bg-muted transition-colors"
				onclick={() => onClose()}
				aria-label="Close"
			>
				<X class="w-5 h-5" />
			</button>
		</div>

		<div class="flex-1 overflow-y-auto px-6 py-4 space-y-6">
			{#if loading}
				<p class="text-sm text-muted-foreground">Loading...</p>
			{:else}
				<!-- Existing publications for THIS project -->
				{#if forCurrentProject.length > 0}
					<section class="space-y-2">
						<h3 class="text-xs font-semibold uppercase tracking-wide text-muted-foreground">This project</h3>
						<ul class="space-y-2">
							{#each forCurrentProject as row (row.id)}
								<li class="rounded-lg border border-border p-3 space-y-2">
									<div class="flex items-start justify-between gap-3">
										<div class="min-w-0 flex-1">
											<div class="flex items-center gap-2">
												<a
													href={publicUrl(row)}
													target="_blank"
													rel="noopener noreferrer"
													class="text-sm font-medium text-violet-600 hover:underline inline-flex items-center gap-1"
												>{shortUrl(row)}<ExternalLink class="w-3 h-3" /></a>
												{#if !row.is_live}
													<span class="text-[10px] uppercase tracking-wide font-semibold px-1.5 py-0.5 rounded bg-amber-100 text-amber-900">Paused</span>
												{:else}
													<span class="text-[10px] uppercase tracking-wide font-semibold px-1.5 py-0.5 rounded bg-emerald-100 text-emerald-900">Live</span>
												{/if}
											</div>
											{#if row.description}
												<p class="text-xs text-muted-foreground mt-0.5 truncate">{row.description}</p>
											{/if}
											<p class="text-[10px] text-muted-foreground mt-1 font-mono">
												{row.view_count} views · {row.run_count} runs · updated {new Date(row.updated_at).toLocaleDateString()}
											</p>
										</div>
										<div class="flex items-center gap-1 flex-shrink-0">
											<button
												type="button"
												class="text-xs px-2.5 py-1 rounded border border-border text-muted-foreground hover:bg-muted disabled:opacity-50 inline-flex items-center gap-1"
												onclick={() => openDeployment(row.project_id)}
												disabled={busySlug !== null}
												title="Manage deployment (triggers, infra, admin-only fields)"
											>
												<Settings class="w-3 h-3" />
												Manage
											</button>
											<button
												type="button"
												class="text-xs px-2.5 py-1 rounded border border-border text-muted-foreground hover:bg-muted disabled:opacity-50"
												onclick={() => handleTogglePause(row)}
												disabled={busySlug !== null}
												title={row.is_live ? 'Pause' : 'Resume'}
											>
												{#if row.is_live}
													<Pause class="w-3 h-3 inline" />
												{:else}
													<Play class="w-3 h-3 inline" />
												{/if}
											</button>
											<button
												type="button"
												class="text-xs px-2.5 py-1 rounded border border-border text-muted-foreground hover:bg-muted disabled:opacity-50"
												onclick={() => handleOverwrite(row)}
												disabled={busySlug !== null}
											>{busySlug === row.slug ? 'Working...' : 'Overwrite'}</button>
											<button
												type="button"
												class="text-xs px-2 py-1 rounded border border-destructive/30 text-destructive hover:bg-destructive/10 disabled:opacity-50"
												onclick={() => handleUnpublish(row)}
												disabled={busySlug !== null}
												title="Unpublish"
											>
												<Trash2 class="w-3 h-3" />
											</button>
										</div>
									</div>
								</li>
							{/each}
						</ul>
					</section>
				{/if}

				<!-- Existing publications for OTHER projects (read-only reference) -->
				{#if forOtherProjects.length > 0}
					<section class="space-y-2">
						<h3 class="text-xs font-semibold uppercase tracking-wide text-muted-foreground">Your other deployments</h3>
						<ul class="space-y-2">
							{#each forOtherProjects as row (row.id)}
								<li class="rounded-lg border border-border/50 bg-muted/30 p-3 flex items-start justify-between gap-3">
									<div class="min-w-0 flex-1">
										<a
											href={publicUrl(row)}
											target="_blank"
											rel="noopener noreferrer"
											class="text-sm font-medium text-muted-foreground hover:text-foreground inline-flex items-center gap-1"
										>{shortUrl(row)}<ExternalLink class="w-3 h-3" /></a>
										<p class="text-xs text-muted-foreground mt-0.5 truncate">{row.project_name}</p>
									</div>
									<div class="flex items-center gap-1 flex-shrink-0">
										<button
											type="button"
											class="text-xs px-2.5 py-1 rounded border border-border text-muted-foreground hover:bg-muted disabled:opacity-50 inline-flex items-center gap-1"
											onclick={() => openDeployment(row.project_id)}
											disabled={busySlug !== null}
											title="Manage deployment"
										>
											<Settings class="w-3 h-3" />
											Manage
										</button>
										<button
											type="button"
											class="text-xs px-2.5 py-1 rounded border border-border text-muted-foreground hover:bg-muted disabled:opacity-50"
											onclick={() => handleTogglePause(row)}
											disabled={busySlug !== null}
										>{row.is_live ? 'Pause' : 'Resume'}</button>
										<button
											type="button"
											class="text-xs px-2 py-1 rounded border border-destructive/30 text-destructive hover:bg-destructive/10 disabled:opacity-50"
											onclick={() => handleUnpublish(row)}
											disabled={busySlug !== null}
										>
											<Trash2 class="w-3 h-3" />
										</button>
									</div>
								</li>
							{/each}
						</ul>
					</section>
				{/if}

				<!-- New publication form -->
				<section class="space-y-3 pt-2 {hasAny ? 'border-t border-border' : ''}">
					<h3 class="text-xs font-semibold uppercase tracking-wide text-muted-foreground mt-4">
						{forCurrentProject.length > 0 ? 'Publish to a new URL' : 'Publish this project'}
					</h3>
					{#if !currentUsername}
						<!-- Publications are keyed on the user's username, so we
						     can't let them publish without one. Surface a clear
						     warning with a link to the profile settings. -->
						<div class="rounded-lg border border-amber-200 bg-amber-50 p-3 text-xs text-amber-900">
							<p class="font-semibold">You need to set a username before publishing.</p>
							<p class="mt-1">Your public URL will be <code class="font-mono">/p/&lt;username&gt;/&lt;slug&gt;</code>. Go to your profile settings to pick one.</p>
						</div>
					{/if}
					<div class="space-y-1">
						<label class="text-xs font-medium" for="slug-input">Public URL</label>
						<div class="flex items-center gap-2 flex-wrap">
							<span class="text-xs text-muted-foreground font-mono">{location.origin}/p/{currentUsername ?? '<username>'}/</span>
							<input
								id="slug-input"
								type="text"
								class="flex-1 min-w-0 text-sm bg-muted px-3 py-2 rounded border border-transparent focus:border-primary outline-none"
								value={slug}
								oninput={(e) => { slug = slugify(e.currentTarget.value); }}
							/>
						</div>
					</div>
					<div class="space-y-1">
						<label class="text-xs font-medium" for="desc-input">Description (for SEO)</label>
						<textarea
							id="desc-input"
							class="w-full text-sm bg-muted px-3 py-2 rounded border border-transparent focus:border-primary outline-none resize-y min-h-[60px]"
							bind:value={description}
							placeholder="A short sentence describing what this tool does..."
						></textarea>
					</div>
					<!-- Rate limit: deployer picks a per-slug ceiling that sits
					     on top of cloud-api's fixed per-IP ceiling. The hard
					     cap (RATE_LIMIT_MAX) is visible in the helper text so
					     the deployer knows the upper bound without trial-and-
					     error. Leaving the field blank uses the default. -->
					<div class="space-y-1">
						<label class="text-xs font-medium" for="rate-limit-input">Max runs per minute (per deployment)</label>
						<div class="flex items-center gap-2">
							<input
								id="rate-limit-input"
								type="number"
								min="1"
								max={RATE_LIMIT_MAX}
								class="w-24 text-sm bg-muted px-3 py-2 rounded border border-transparent focus:border-primary outline-none"
								bind:value={rateLimitInput}
							/>
							<span class="text-[11px] text-muted-foreground">
								Min 1, max {RATE_LIMIT_MAX}/min. Leave blank for default ({RATE_LIMIT_DEFAULT}/min). To pause visitor runs, use the Pause toggle on an existing deployment.
							</span>
						</div>
						{#if rateLimitError}
							<p class="text-[11px] text-destructive">{rateLimitError}</p>
						{/if}
					</div>
					<!-- Strip sensitive fields: default ON. When OFF, show a
					     loud warning because the deployer is explicitly
					     shipping secrets to the deployment. This is sometimes
					     legitimate (admin-only fields that visitors never
					     see), but the default should protect careless users. -->
					<div class="rounded-lg border {stripSensitive ? 'border-border bg-muted/40' : 'border-amber-300 bg-amber-50'} p-3">
						<label class="flex items-start gap-2 cursor-pointer">
							<input
								type="checkbox"
								bind:checked={stripSensitive}
								class="mt-0.5 flex-shrink-0"
							/>
							<div class="flex-1 min-w-0 space-y-1">
								<div class="text-xs font-medium {stripSensitive ? 'text-foreground' : 'text-amber-900'}">
									Strip sensitive fields (passwords, API keys)
								</div>
								{#if stripSensitive}
									<div class="text-[11px] text-muted-foreground">
										{#if sensitiveValueCount > 0}
											{sensitiveValueCount} sensitive value{sensitiveValueCount === 1 ? '' : 's'} will be cleared from the deployed copy. Your builder project is untouched.
										{:else}
											No sensitive values detected in this project.
										{/if}
									</div>
								{:else}
									<div class="text-[11px] text-amber-900 leading-snug">
										<strong>Sensitive values will be included in the deployment.</strong>
										Only leave this unchecked if your loom marks those fields admin-only so
										visitors can't read or write them. If in doubt, keep stripping on.
									</div>
								{/if}
							</div>
						</label>
					</div>
					<div class="flex justify-end">
						<button
							type="button"
							class="text-sm px-4 py-2 rounded-md bg-violet-600 text-white hover:bg-violet-700 disabled:opacity-50"
							onclick={handlePublishNew}
							disabled={publishing || slug.length < 3 || !currentUsername}
						>{publishing ? 'Publishing...' : 'Publish'}</button>
					</div>
				</section>
			{/if}

			{#if error}
				<p class="text-xs text-destructive">{error}</p>
			{/if}
		</div>
	</div>
</div>
