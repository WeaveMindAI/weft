<script lang="ts">
	import { Search, FileAudio, FileVideo, FileImage, FileText, X } from '@lucide/svelte';
	import { listRuntimeFiles } from '../../../vscode';
	import { matchesAccept } from '../../utils/file-browser';

	// "Pick a stored file": lists the project's RUNTIME files (its stored
	// `project/` + `asset/` planes, through the same door `weft files`
	// uses), with a filename search box. Files matching
	// the field's accept filter sort first, the rest dimmed but still pickable
	// (steered, not blocked). Picking hands back the TENANT-LESS storage key
	// the field writes into its `@asset("<scope-key>", <Type>)` ref.
	let {
		accept,
		onPick,
		onClose,
	}: {
		accept: string | undefined;
		onPick: (key: string) => void;
		onClose: () => void;
	} = $props();

	type Row = { key: string; filename: string; mimeType: string; sizeBytes: number };

	let query = $state('');
	let files = $state<Row[]>([]);
	let loading = $state(true);
	let errorMsg = $state<string | null>(null);

	(async () => {
		try {
			files = await listRuntimeFiles();
		} catch (err) {
			errorMsg = err instanceof Error ? err.message : 'listing failed';
		} finally {
			loading = false;
		}
	})();

	const shown = $derived.by(() => {
		const q = query.trim().toLowerCase();
		const matches = q
			? files.filter((f) => f.filename.toLowerCase().includes(q) || f.key.toLowerCase().includes(q))
			: files;
		// Accept-matching files first (steering), then the rest, both stable.
		return [
			...matches.filter((f) => matchesAccept(f.mimeType, accept)),
			...matches.filter((f) => !matchesAccept(f.mimeType, accept)),
		];
	});

	function iconFor(mime: string) {
		return mime.startsWith('audio/') ? FileAudio
			: mime.startsWith('video/') ? FileVideo
			: mime.startsWith('image/') ? FileImage
			: FileText;
	}

	function fmtSize(bytes: number): string {
		const units = ['B', 'KiB', 'MiB', 'GiB'];
		let v = bytes;
		let u = 0;
		while (v >= 1024 && u < units.length - 1) { v /= 1024; u += 1; }
		return u === 0 ? `${bytes} B` : `${v.toFixed(1)} ${units[u]}`;
	}
</script>

<!-- Backdrop. Click outside the panel closes. -->
<!-- svelte-ignore a11y_click_events_have_key_events -->
<!-- svelte-ignore a11y_no_static_element_interactions -->
<div
	class="nodrag nopan nowheel fixed inset-0 z-50 flex items-center justify-center bg-black/40 p-4"
	onclick={(e) => { e.stopPropagation(); onClose(); }}
>
	<!-- Panel. Stop propagation so a click inside doesn't close. -->
	<!-- svelte-ignore a11y_click_events_have_key_events -->
	<!-- svelte-ignore a11y_no_static_element_interactions -->
	<div
		class="flex max-h-[70vh] w-full max-w-md flex-col overflow-hidden rounded-lg border border-border bg-background shadow-xl"
		onclick={(e) => e.stopPropagation()}
	>
		<div class="flex items-center gap-2 border-b border-border px-3 py-2">
			<Search class="w-4 h-4 text-muted-foreground shrink-0" />
			<input
				type="text"
				class="flex-1 bg-transparent text-xs outline-none placeholder:text-muted-foreground"
				placeholder="Search this project's stored files…"
				bind:value={query}
			/>
			<button class="text-muted-foreground hover:text-foreground shrink-0" title="Close" onclick={onClose}>
				<X class="w-4 h-4" />
			</button>
		</div>

		<div class="min-h-0 flex-1 overflow-y-auto">
			{#if loading}
				<div class="px-3 py-6 text-center text-[11px] text-muted-foreground">Loading…</div>
			{:else if errorMsg}
				<div class="m-2 rounded bg-destructive/10 px-2 py-1 text-[10px] text-destructive">{errorMsg}</div>
			{:else if shown.length === 0}
				<div class="px-3 py-6 text-center text-[11px] text-muted-foreground">
					{query ? 'No stored files match.' : 'This project has no stored files yet.'}
				</div>
			{:else}
				{#each shown as file (file.key)}
					{@const Icon = iconFor(file.mimeType)}
					{@const ok = matchesAccept(file.mimeType, accept)}
					<button
						type="button"
						class="flex w-full items-center gap-2 border-b border-border/50 px-3 py-2 text-left transition-colors hover:bg-muted/60 {ok ? '' : 'opacity-45'}"
						title={ok ? file.key : `${file.filename} — doesn't match ${accept}, but you can still pick it`}
						onclick={() => onPick(file.key)}
					>
						<Icon class="w-4 h-4 text-muted-foreground shrink-0" />
						<div class="min-w-0 flex-1">
							<div class="truncate text-[11px] font-mono text-foreground">{file.filename || file.key}</div>
							<div class="text-[10px] text-muted-foreground">{file.mimeType} · {fmtSize(file.sizeBytes)}</div>
						</div>
					</button>
				{/each}
			{/if}
		</div>
	</div>
</div>
