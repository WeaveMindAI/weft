<script lang="ts">
	import { Upload, FolderOpen, Link, FileAudio, FileVideo, FileImage, FileText, X } from '@lucide/svelte';
	import { pickAsset } from '../../../vscode';
	import { acceptForFileType, guessMime } from '../../utils/file-browser';
	import { isFileRefValue, type WeftFileRefValue } from '../../value-format';
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
		onUpdate,
	}: {
		value: unknown;
		accept: string | undefined;
		fileType: string | undefined;
		// A WeftFileRefValue to set, or null to clear (routes to removeConfig).
		onUpdate: (ref: WeftFileRefValue | null) => void;
	} = $props();

	const declaredType = $derived(fileType ?? 'File');
	const effectiveAccept = $derived(acceptForFileType(fileType, accept));
	const current = $derived(isFileRefValue(value) ? value.__weftFileRef : null);
	const currentIsUrl = $derived(
		current !== null && (current.path.startsWith('http://') || current.path.startsWith('https://')),
	);
	// A tenant-less storage key (`project/<id>/<file>` etc.): a stored runtime
	// file picked from the project's storage, not a folder path.
	// SYNC: key shape <-> crates/weft-core/src/storage/key.rs is_scope_key
	const currentIsStored = $derived(
		current !== null && /^(exec|project|shared|asset)\/[^/]+\/[^/]+$/.test(current.path),
	);

	let errorMsg = $state<string | null>(null);
	let busy = $state(false);
	let dragging = $state(false);
	let showPicker = $state(false);
	let showUrlInput = $state(false);
	let urlText = $state('');

	const Icon = $derived.by(() => {
		const mime = current ? guessMime(current.path) : '';
		return mime.startsWith('audio/') ? FileAudio
			: mime.startsWith('video/') ? FileVideo
			: mime.startsWith('image/') ? FileImage
			: FileText;
	});

	function setPath(path: string) {
		errorMsg = null;
		onUpdate({ __weftFileRef: { path, type: declaredType, marker: 'asset' } });
	}

	async function pick(dropped?: { name: string; bytesBase64: string }) {
		errorMsg = null;
		busy = true;
		try {
			const path = await pickAsset(effectiveAccept, dropped);
			if (path !== null) setPath(path);
		} catch (err) {
			errorMsg = err instanceof Error ? err.message : 'pick failed';
		} finally {
			busy = false;
		}
	}

	async function onDrop(e: DragEvent) {
		e.preventDefault();
		dragging = false;
		if (busy) return;
		const file = e.dataTransfer?.files?.[0];
		if (!file) return;
		// The browser hides a dropped file's OS path, so the bytes travel to
		// the host, which stores them as a project file under assets/.
		const buf = new Uint8Array(await file.arrayBuffer());
		let bin = '';
		const CHUNK = 0x8000;
		for (let i = 0; i < buf.length; i += CHUNK) {
			bin += String.fromCharCode(...buf.subarray(i, i + CHUNK));
		}
		void pick({ name: file.name, bytesBase64: btoa(bin) });
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
		setPath(trimmed);
	}

	function clear() {
		onUpdate(null);
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
		<div class="flex items-center gap-2 rounded border border-border bg-muted/40 p-2">
			<Icon class="w-4 h-4 text-muted-foreground shrink-0" />
			<div class="min-w-0 flex-1">
				<div class="truncate text-[11px] font-mono text-foreground" title={current.path}>{current.path}</div>
				<div class="text-[10px] text-muted-foreground">
					{currentIsUrl ? 'external URL' : currentIsStored ? 'stored file' : 'project asset'} · {current.type}
				</div>
			</div>
			<button
				class="text-muted-foreground hover:text-destructive transition-colors shrink-0"
				title="Remove file"
				disabled={busy}
				onclick={clear}
			>
				<X class="w-4 h-4" />
			</button>
		</div>
	{:else}
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
				<span class="font-medium text-foreground">{busy ? 'Picking…' : 'Click to pick a file'}</span>
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
		onPick={(path) => { showPicker = false; setPath(path); }}
		onClose={() => (showPicker = false)}
	/>
{/if}
