<script lang="ts">
	import { Upload, FolderOpen, Link, FileAudio, FileVideo, FileImage, FileText, X } from '@lucide/svelte';
	import { pickAsset } from '../../../host';
	import { acceptForFileType, guessMime } from '../../utils/file-browser';
	import { fileRefsOf, type WeftFileRefValue } from '../../value-format';
	import FilePickerModal from './FilePickerModal.svelte';

	// The file-drop config field. Its value is an `@asset("<path-or-url-or-key>",
	// <Type>)` ref (in memory: WeftFileRefValue): the file lives WITH the
	// project (or at a URL, or in the project's stored files), and the
	// pre-build asset sync publishes + resolves it. Three ways to set it:
	//   - PICK/DROP: the host produces the path (a native dialog references
	//     the picked file in place; dropped bytes are stored as
	//     `assets/<name>`)
	//   - BROWSE: reference an existing project file (the picker modal)
	//   - URL: paste an external address (the worker fetches it at run time)
	// `fileType` is the field's declared weft file type: it derives the accept
	// filter (explicit `accept` narrows) and the `, <Type>)` written to source.
	let {
		value,
		accept,
		fileType,
		multiple = false,
		onUpdate,
	}: {
		value: unknown;
		accept: string | undefined;
		fileType: string | undefined;
		/// The port holds SEVERAL files, so the field keeps a list: every
		/// add appends, each row removes itself, and the value written is
		/// a list of refs. A single-file port is the same control with one
		/// row and no list.
		multiple?: boolean;
		// The value to set: one ref, a list of them when the port takes
		// several, or null to clear (routes to removeConfig).
		onUpdate: (ref: WeftFileRefValue | WeftFileRefValue[] | null) => void;
	} = $props();

	const declaredType = $derived(fileType ?? 'File');
	const effectiveAccept = $derived(acceptForFileType(fileType, accept));
	// The files this field currently names, read from whichever shape the
	// value arrives in: the structural ref a file-backed config field
	// carries, or the marker text a port literal holds.
	const refs = $derived(fileRefsOf(value));
	// A single-file port SHOWS its first file even when the source names
	// several (hand-written `[@asset(a), @asset(b)]` on a non-multiple
	// port): hiding a value that exists, then overwriting it on the next
	// pick, would silently lose data. The extras render as a loud row.
	const current = $derived(!multiple && refs.length > 0 ? refs[0].__weftFileRef : null);

	function isUrl(path: string): boolean {
		return path.startsWith('http://') || path.startsWith('https://');
	}
	// A tenant-less storage key (`project/<id>/<file>` etc.): a stored runtime
	// file picked from the project's storage, not a folder path.
	// SYNC: key shape <-> crates/weft-core/src/storage/key.rs is_scope_key
	function isStored(path: string): boolean {
		return /^(exec|project|shared|asset)\/[^/]+\/[^/]+$/.test(path);
	}
	function originOf(path: string): string {
		return isUrl(path) ? 'external URL' : isStored(path) ? 'stored file' : 'project asset';
	}
	function iconFor(path: string) {
		const mime = guessMime(path);
		return mime.startsWith('audio/') ? FileAudio
			: mime.startsWith('video/') ? FileVideo
			: mime.startsWith('image/') ? FileImage
			: FileText;
	}

	let errorMsg = $state<string | null>(null);
	let busy = $state(false);
	let dragging = $state(false);
	let showPicker = $state(false);
	let showUrlInput = $state(false);
	let urlText = $state('');

	function refFor(path: string): WeftFileRefValue {
		return { __weftFileRef: { path, type: declaredType, marker: 'asset' } };
	}

	/// Write the field's files. A single-file port carries the ref itself
	/// (or null when emptied); a multi-file one carries the list, and an
	/// emptied list clears the value the same way.
	function write(next: WeftFileRefValue[]) {
		errorMsg = null;
		if (!multiple) {
			onUpdate(next[0] ?? null);
			return;
		}
		onUpdate(next.length > 0 ? next : null);
	}

	function addPaths(paths: string[]) {
		if (paths.length === 0) return;
		write(multiple ? [...refs, ...paths.map(refFor)] : [refFor(paths[0])]);
	}

	function removeAt(index: number) {
		write(refs.filter((_, at) => at !== index));
	}

	async function pick(dropped?: { name: string; bytesBase64: string }[]) {
		errorMsg = null;
		busy = true;
		try {
			addPaths(await pickAsset(effectiveAccept, { multiple, dropped }));
		} catch (err) {
			errorMsg = err instanceof Error ? err.message : 'pick failed';
		} finally {
			busy = false;
		}
	}

	/// A dropped file's bytes, base64, for the host to store under
	/// `assets/`: the browser hides the file's OS path, so the bytes are
	/// what can travel.
	async function encode(file: File): Promise<{ name: string; bytesBase64: string }> {
		const buf = new Uint8Array(await file.arrayBuffer());
		let bin = '';
		const CHUNK = 0x8000;
		for (let i = 0; i < buf.length; i += CHUNK) {
			bin += String.fromCharCode(...buf.subarray(i, i + CHUNK));
		}
		return { name: file.name, bytesBase64: btoa(bin) };
	}

	async function onDrop(e: DragEvent) {
		e.preventDefault();
		dragging = false;
		if (busy) return;
		const dropped = Array.from(e.dataTransfer?.files ?? []);
		if (dropped.length === 0) return;
		// A single-file field takes the first even when several are dropped.
		const files = multiple ? dropped : dropped.slice(0, 1);
		void pick(await Promise.all(files.map(encode)));
	}

	function submitUrl() {
		const trimmed = urlText.trim();
		let ok = false;
		try {
			const u = new URL(trimmed);
			ok = u.protocol === 'http:' || u.protocol === 'https:';
		} catch {
			ok = false;
		}
		if (!ok) {
			errorMsg = 'Enter a valid http(s) URL.';
			return;
		}
		urlText = '';
		showUrlInput = false;
		addPaths([trimmed]);
	}
</script>

<!-- nodrag/nopan/nowheel keep the graph canvas from stealing pointer + wheel
     events while the user interacts with the field. The wrapper's only handler
     is stopPropagation (canvas isolation, not a real interaction), so the
     static-element a11y rules don't apply.
-->
<!-- svelte-ignore a11y_click_events_have_key_events -->
<!-- svelte-ignore a11y_no_static_element_interactions -->
<div class="nodrag nopan nowheel" onclick={(e) => e.stopPropagation()}>
	{#if current}
		{@const Icon = iconFor(current.path)}
		<div class="flex items-center gap-2 rounded border border-border bg-muted/40 p-2">
			<Icon class="w-4 h-4 text-muted-foreground shrink-0" />
			<div class="min-w-0 flex-1">
				<div class="truncate text-[11px] font-mono text-foreground" title={current.path}>{current.path}</div>
				<div class="text-[10px] text-muted-foreground">
					{originOf(current.path)} · {current.type}
				</div>
			</div>
			<button
				class="text-muted-foreground hover:text-destructive transition-colors shrink-0"
				title="Remove file"
				disabled={busy}
				onclick={() => removeAt(0)}
			>
				<X class="w-4 h-4" />
			</button>
		</div>
		{#if refs.length > 1}
			<div class="mt-1 rounded border border-rose-200 bg-rose-50 px-2 py-1.5 text-[10px] text-rose-600">
				This port takes one file but the source names {refs.length}; remove the extras:
				{#each refs.slice(1) as ref, i}
					<div class="flex items-center gap-1">
						<span class="min-w-0 flex-1 truncate font-mono" title={ref.__weftFileRef.path}>{ref.__weftFileRef.path}</span>
						<button
							type="button"
							class="shrink-0 hover:text-destructive"
							title="Remove {ref.__weftFileRef.path}"
							disabled={busy}
							onclick={() => removeAt(i + 1)}
						>&times;</button>
					</div>
				{/each}
			</div>
		{/if}
	{:else}
		<!-- A port that takes several: the files it holds sit above the
		     zone, one row each, in the order they were added. -->
		{#if multiple && refs.length > 0}
			<div class="mb-1 space-y-1">
				{#each refs as ref, i}
					{@const path = ref.__weftFileRef.path}
					{@const RowIcon = iconFor(path)}
					<div class="group flex items-center gap-2 rounded bg-muted pl-2 pr-1 py-1">
						<RowIcon class="w-3.5 h-3.5 text-muted-foreground shrink-0" />
						<span class="min-w-0 flex-1 truncate text-[11px] font-mono" title={path}>{path}</span>
						<span class="shrink-0 text-[10px] text-muted-foreground">{originOf(path)}</span>
						<button
							type="button"
							class="shrink-0 w-4 h-4 grid place-items-center rounded text-muted-foreground opacity-0 group-hover:opacity-100 hover:text-destructive hover:bg-background transition"
							title="Remove"
							aria-label="Remove {path}"
							disabled={busy}
							onclick={() => removeAt(i)}
						>&times;</button>
					</div>
				{/each}
			</div>
		{/if}
		<button
			type="button"
			class="flex w-full flex-col items-center justify-center gap-2 rounded-lg border-2 border-dashed px-3 py-6 text-center transition-colors
				{dragging ? 'border-primary bg-primary/5' : 'border-border hover:border-muted-foreground/50'}
				{busy ? 'cursor-default opacity-70' : 'cursor-pointer'}"
			ondragover={(e) => { e.preventDefault(); if (!busy) dragging = true; }}
			ondragleave={() => (dragging = false)}
			ondrop={onDrop}
			onclick={() => { if (!busy) void pick(); }}
			disabled={busy}
		>
			<Upload class="w-5 h-5 text-muted-foreground" />
			<div class="text-[11px] text-muted-foreground">
				<span class="font-medium text-foreground">
					{busy
						? 'Picking…'
						: multiple
							? refs.length > 0 ? 'Click to add more files' : 'Click to pick files'
							: 'Click to pick a file'}
				</span>
				{busy ? '' : ' or drag and drop'}
			</div>
			{#if effectiveAccept}
				<div class="text-[10px] text-muted-foreground/70">{effectiveAccept}</div>
			{/if}
		</button>

		{#if !busy}
			<div class="mt-1 flex items-center gap-2">
				<button
					type="button"
					class="flex flex-1 items-center justify-center gap-1 rounded border border-border px-2 py-1 text-[10px] text-muted-foreground transition-colors hover:bg-muted/60 hover:text-foreground"
					onclick={() => (showPicker = true)}
				>
					<FolderOpen class="w-3 h-3" /> Stored files
				</button>
				<button
					type="button"
					class="flex flex-1 items-center justify-center gap-1 rounded border border-border px-2 py-1 text-[10px] text-muted-foreground transition-colors hover:bg-muted/60 hover:text-foreground"
					onclick={() => { showUrlInput = !showUrlInput; errorMsg = null; }}
				>
					<Link class="w-3 h-3" /> Use a URL
				</button>
			</div>

			{#if showUrlInput}
				<div class="mt-1 flex items-center gap-1">
					<input
						type="url"
						class="flex-1 rounded border border-border bg-muted/40 px-2 py-1 text-[11px] outline-none placeholder:text-muted-foreground"
						placeholder="https://example.com/file.png"
						bind:value={urlText}
						onkeydown={(e) => { if (e.key === 'Enter') { e.preventDefault(); submitUrl(); } }}
					/>
					<button
						type="button"
						class="rounded bg-primary px-2 py-1 text-[10px] font-medium text-primary-foreground transition-colors hover:bg-primary/90"
						onclick={submitUrl}
					>Add</button>
				</div>
			{/if}
		{/if}
	{/if}

	{#if errorMsg}
		<div class="mt-1 rounded bg-destructive/10 px-1.5 py-1 text-[10px] text-destructive">{errorMsg}</div>
	{/if}
</div>

{#if showPicker}
	<FilePickerModal
		accept={effectiveAccept}
		onPick={(path) => { showPicker = false; addPaths([path]); }}
		onClose={() => (showPicker = false)}
	/>
{/if}
