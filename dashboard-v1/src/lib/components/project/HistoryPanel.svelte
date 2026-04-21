<script lang="ts">
	import { onMount } from 'svelte';
	import { authFetch } from '$lib/config';

	let { projectId, getCurrentCode, onRestore }: {
		projectId: string;
		getCurrentCode: () => { weftCode: string; loomCode: string | null; layoutCode: string | null };
		onRestore: (weftCode: string, loomCode: string | null, layoutCode: string | null) => void;
	} = $props();

	interface Version {
		id: string;
		projectId: string;
		weftCode: string | null;
		loomCode: string | null;
		layoutCode: string | null;
		label: string | null;
		versionType: 'auto' | 'manual';
		createdAt: string;
	}

	let versions = $state<Version[]>([]);
	let loading = $state(true);
	let saving = $state(false);
	let saveLabel = $state('');
	let showSaveInput = $state(false);

	// Delete confirmation
	let deleteTarget = $state<string | null>(null);
	let skipDeleteConfirm = $state(false);

	// Inline label editing
	let editingId = $state<string | null>(null);
	let editingLabel = $state('');

	let manualVersions = $derived(versions.filter(v => v.versionType === 'manual'));
	let autoVersions = $derived(versions.filter(v => v.versionType === 'auto'));

	function formatDate(dateStr: string): string {
		const d = new Date(dateStr);
		const now = new Date();
		const isToday = d.toDateString() === now.toDateString();
		const yesterday = new Date(now);
		yesterday.setDate(yesterday.getDate() - 1);
		const isYesterday = d.toDateString() === yesterday.toDateString();

		const time = d.toLocaleTimeString([], { hour: '2-digit', minute: '2-digit' });
		if (isToday) return `Today ${time}`;
		if (isYesterday) return `Yesterday ${time}`;
		return d.toLocaleDateString([], { month: 'short', day: 'numeric' }) + ` ${time}`;
	}

	async function loadVersions() {
		try {
			const res = await authFetch(`/api/projects/${projectId}/versions`);
			if (res.ok) versions = await res.json();
		} catch (e) {
			console.error('Failed to load versions:', e);
		} finally {
			loading = false;
		}
	}

	export async function createVersion(
		weftCode: string,
		loomCode: string | null,
		label: string | null,
		versionType: 'auto' | 'manual',
	) {
		try {
			const res = await authFetch(`/api/projects/${projectId}/versions`, {
				method: 'POST',
				headers: { 'Content-Type': 'application/json' },
				body: JSON.stringify({ weftCode, loomCode, label, versionType }),
			});
			if (res.ok) {
				const version = await res.json();
				versions = [version, ...versions];
			}
		} catch (e) {
			console.error('Failed to create version:', e);
		}
	}

	async function saveManualVersion() {
		saving = true;
		try {
			const current = getCurrentCode();
			const res = await authFetch(`/api/projects/${projectId}/versions`, {
				method: 'POST',
				headers: { 'Content-Type': 'application/json' },
				body: JSON.stringify({
					weftCode: current.weftCode,
					loomCode: current.loomCode,
					layoutCode: current.layoutCode,
					label: saveLabel.trim() || null,
					versionType: 'manual',
				}),
			});
			if (res.ok) {
				await loadVersions();
				saveLabel = '';
				showSaveInput = false;
			}
		} catch (e) {
			console.error('Failed to save version:', e);
		} finally {
			saving = false;
		}
	}

	function requestDelete(id: string) {
		if (skipDeleteConfirm) {
			doDelete(id);
		} else {
			deleteTarget = id;
		}
	}

	async function doDelete(id: string) {
		try {
			const res = await authFetch(`/api/projects/${projectId}/versions/${id}`, { method: 'DELETE' });
			if (res.ok) {
				versions = versions.filter(v => v.id !== id);
			}
		} catch (e) {
			console.error('Failed to delete version:', e);
		}
		deleteTarget = null;
	}

	function startEditLabel(version: Version) {
		editingId = version.id;
		editingLabel = version.label ?? '';
	}

	async function commitEditLabel() {
		if (!editingId) return;
		try {
			await authFetch(`/api/projects/${projectId}/versions/${editingId}`, {
				method: 'PATCH',
				headers: { 'Content-Type': 'application/json' },
				body: JSON.stringify({ label: editingLabel.trim() || null }),
			});
			const v = versions.find(v => v.id === editingId);
			if (v) v.label = editingLabel.trim() || null;
			versions = [...versions];
		} catch (e) {
			console.error('Failed to update label:', e);
		}
		editingId = null;
	}

	function restoreVersion(version: Version) {
		if (version.weftCode != null) {
			onRestore(version.weftCode, version.loomCode, version.layoutCode ?? null);
		}
	}

	onMount(loadVersions);
</script>

<div class="flex flex-col h-full overflow-hidden">
	<!-- Save button bar -->
	<div class="border-b border-zinc-200 shrink-0 bg-[#f3f4f6]">
		{#if showSaveInput}
			<div class="flex items-center gap-1.5 px-2 py-1.5">
				<input
					type="text"
					bind:value={saveLabel}
					placeholder="Version name (optional)"
					class="flex-1 px-2 py-1 text-[11px] bg-white border border-zinc-200 rounded text-zinc-800 placeholder-zinc-400 focus:outline-none focus:ring-1 focus:ring-blue-500"
					onkeydown={(e) => { if (e.key === 'Enter') saveManualVersion(); if (e.key === 'Escape') showSaveInput = false; }}
				/>
				<button
					onclick={saveManualVersion}
					disabled={saving}
					class="px-2 py-1 text-[10px] font-medium rounded bg-zinc-800 text-white hover:bg-zinc-700 transition-colors disabled:opacity-50"
				>{saving ? '...' : 'Save'}</button>
				<button
					onclick={() => showSaveInput = false}
					class="px-1.5 py-1 text-[10px] text-zinc-400 hover:text-zinc-600 transition-colors"
				>Cancel</button>
			</div>
		{:else}
			<div class="flex items-center justify-between px-2 py-1.5">
				<button
					onclick={() => showSaveInput = true}
					class="flex items-center gap-1 px-2 py-1 text-[10px] font-medium rounded border border-zinc-200 text-zinc-600 hover:bg-zinc-100 transition-colors"
				>
					<svg xmlns="http://www.w3.org/2000/svg" width="10" height="10" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2.5">
						<path d="M19 21H5a2 2 0 0 1-2-2V5a2 2 0 0 1 2-2h11l5 5v11a2 2 0 0 1-2 2z"/><polyline points="17 21 17 13 7 13 7 21"/><polyline points="7 3 7 8 15 8"/>
					</svg>
					Save version
				</button>
			</div>
		{/if}
	</div>

	<!-- Version list -->
	<div class="flex-1 overflow-y-auto">
		{#if loading}
			<div class="flex items-center justify-center h-32">
				<div class="h-5 w-5 border-2 border-zinc-300 border-t-transparent rounded-full animate-spin"></div>
			</div>
		{:else if versions.length === 0}
			<div class="flex flex-col items-center justify-center h-full gap-2 px-4 text-center">
				<svg xmlns="http://www.w3.org/2000/svg" width="28" height="28" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.5" class="text-zinc-300">
					<circle cx="12" cy="12" r="10"/><polyline points="12 6 12 12 16 14"/>
				</svg>
				<p class="text-xs text-zinc-400">No saved versions yet.</p>
				<p class="text-[10px] text-zinc-400">Versions are auto-saved periodically and when you leave.</p>
			</div>
		{:else}
			<!-- Manual saves -->
			{#if manualVersions.length > 0}
				<div class="px-2 pt-2 pb-1">
					<span class="text-[10px] font-semibold text-zinc-500 uppercase tracking-wider">Saved</span>
				</div>
				{#each manualVersions as version (version.id)}
					<div class="border-b border-zinc-100 last:border-b-0 hover:bg-zinc-50 transition-colors group">
						<div class="px-3 py-2 flex items-start gap-2">
							<svg xmlns="http://www.w3.org/2000/svg" width="12" height="12" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" class="text-blue-500 shrink-0 mt-0.5">
								<path d="M19 21H5a2 2 0 0 1-2-2V5a2 2 0 0 1 2-2h11l5 5v11a2 2 0 0 1-2 2z"/><polyline points="17 21 17 13 7 13 7 21"/><polyline points="7 3 7 8 15 8"/>
							</svg>
							<div class="flex-1 min-w-0">
								{#if editingId === version.id}
									<input
										type="text"
										bind:value={editingLabel}
										class="w-full px-1.5 py-0.5 text-[11px] bg-white border border-blue-300 rounded text-zinc-800 focus:outline-none focus:ring-1 focus:ring-blue-500"
										onblur={commitEditLabel}
										onkeydown={(e) => { if (e.key === 'Enter') commitEditLabel(); if (e.key === 'Escape') editingId = null; }}
									/>
								{:else}
									<button
										class="text-[11px] font-medium text-zinc-700 truncate block text-left w-full hover:text-blue-600"
										onclick={() => startEditLabel(version)}
										title="Click to edit label"
									>{version.label || 'Untitled'}</button>
								{/if}
								<div class="text-[10px] text-zinc-400">{formatDate(version.createdAt)}</div>
							</div>
							<div class="flex items-center gap-0.5 opacity-0 group-hover:opacity-100 transition-opacity shrink-0">
								<button
									onclick={() => restoreVersion(version)}
									class="px-1.5 py-0.5 text-[10px] rounded text-blue-600 hover:bg-blue-50 transition-colors"
									title="Restore this version"
								>Restore</button>
								<button
									onclick={() => requestDelete(version.id)}
									class="w-5 h-5 flex items-center justify-center rounded text-zinc-400 hover:text-red-500 hover:bg-red-50 transition-colors"
									title="Delete"
								>
									<svg xmlns="http://www.w3.org/2000/svg" width="10" height="10" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2.5"><path d="M18 6 6 18M6 6l12 12"/></svg>
								</button>
							</div>
						</div>
					</div>
				{/each}
			{/if}

			<!-- Auto saves -->
			{#if autoVersions.length > 0}
				<div class="px-2 pt-2 pb-1">
					<span class="text-[10px] font-semibold text-zinc-500 uppercase tracking-wider">Auto-saved</span>
				</div>
				{#each autoVersions as version (version.id)}
					<div class="border-b border-zinc-100 last:border-b-0 hover:bg-zinc-50 transition-colors group">
						<div class="px-3 py-2 flex items-start gap-2">
							<svg xmlns="http://www.w3.org/2000/svg" width="12" height="12" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" class="text-zinc-400 shrink-0 mt-0.5">
								<circle cx="12" cy="12" r="10"/><polyline points="12 6 12 12 16 14"/>
							</svg>
							<div class="flex-1 min-w-0">
								{#if editingId === version.id}
									<input
										type="text"
										bind:value={editingLabel}
										class="w-full px-1.5 py-0.5 text-[11px] bg-white border border-blue-300 rounded text-zinc-800 focus:outline-none focus:ring-1 focus:ring-blue-500"
										onblur={commitEditLabel}
										onkeydown={(e) => { if (e.key === 'Enter') commitEditLabel(); if (e.key === 'Escape') editingId = null; }}
									/>
								{:else}
									{#if version.label}
										<button
											class="text-[11px] font-medium text-zinc-600 truncate block text-left w-full hover:text-blue-600"
											onclick={() => startEditLabel(version)}
											title="Click to edit label"
										>{version.label}</button>
									{:else}
										<button
											class="text-[11px] text-zinc-500 truncate block text-left w-full hover:text-blue-600"
											onclick={() => startEditLabel(version)}
											title="Click to add label"
										>Auto-save</button>
									{/if}
								{/if}
								<div class="text-[10px] text-zinc-400">{formatDate(version.createdAt)}</div>
							</div>
							<div class="flex items-center gap-0.5 opacity-0 group-hover:opacity-100 transition-opacity shrink-0">
								<button
									onclick={() => restoreVersion(version)}
									class="px-1.5 py-0.5 text-[10px] rounded text-blue-600 hover:bg-blue-50 transition-colors"
									title="Restore this version"
								>Restore</button>
								<button
									onclick={() => requestDelete(version.id)}
									class="w-5 h-5 flex items-center justify-center rounded text-zinc-400 hover:text-red-500 hover:bg-red-50 transition-colors"
									title="Delete"
								>
									<svg xmlns="http://www.w3.org/2000/svg" width="10" height="10" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2.5"><path d="M18 6 6 18M6 6l12 12"/></svg>
								</button>
							</div>
						</div>
					</div>
				{/each}
			{/if}
		{/if}
	</div>
</div>

<!-- Delete confirmation modal -->
{#if deleteTarget}
	<!-- svelte-ignore a11y_no_static_element_interactions -->
	<div
		class="fixed inset-0 bg-black/30 z-50 flex items-center justify-center"
		onmousedown={(e) => { if (e.target === e.currentTarget) deleteTarget = null; }}
	>
		<div class="bg-white rounded-lg shadow-xl p-4 max-w-xs w-full mx-4">
			<p class="text-sm text-zinc-800 font-medium mb-1">Delete version?</p>
			<p class="text-xs text-zinc-500 mb-4">This action cannot be undone.</p>
			<label class="flex items-center gap-2 mb-4 cursor-pointer">
				<input type="checkbox" bind:checked={skipDeleteConfirm} class="w-3.5 h-3.5 rounded border-zinc-300" />
				<span class="text-[11px] text-zinc-500">Don't ask again this session</span>
			</label>
			<div class="flex justify-end gap-2">
				<button
					onclick={() => deleteTarget = null}
					class="px-3 py-1.5 text-xs rounded border border-zinc-200 text-zinc-600 hover:bg-zinc-50 transition-colors"
				>Cancel</button>
				<button
					onclick={() => deleteTarget && doDelete(deleteTarget)}
					class="px-3 py-1.5 text-xs rounded bg-red-600 text-white hover:bg-red-700 transition-colors"
				>Delete</button>
			</div>
		</div>
	</div>
{/if}
