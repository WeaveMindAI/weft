<script lang="ts">
	import { Download, ExternalLink, FileAudio, FileVideo, FileText, Image as ImageIcon, AlertCircle } from '@lucide/svelte';
	import type { FileValueWire } from '../../../../shared/protocol';
	import { send, resolveStoredFileUrl } from '../../../vscode';

	// `mode='image'` renders the picture inline (ImageDisplay);
	// `mode='link'` renders a metadata + Download button card
	// (DownloadLink). A key-backed file resolves its bytes through the
	// SAME authenticated handshake a user download uses; a url-backed
	// file points at an external resource and is rendered/linked
	// directly (the browser fetches it, nothing goes through storage).
	let { file, mode }: { file: FileValueWire; mode: 'image' | 'link' } = $props();

	function fmtSize(bytes: number): string {
		const units = ['B', 'KiB', 'MiB', 'GiB'];
		let v = bytes;
		let u = 0;
		while (v >= 1024 && u < units.length - 1) {
			v /= 1024;
			u += 1;
		}
		return u === 0 ? `${bytes} B` : `${v.toFixed(1)} ${units[u]}`;
	}

	const Icon = $derived(
		file.mimeType.startsWith('audio/') ? FileAudio
		: file.mimeType.startsWith('video/') ? FileVideo
		: file.mimeType.startsWith('image/') ? ImageIcon
		: FileText,
	);

	function download() {
		if (file.key) send({ kind: 'downloadStoredFile', key: file.key });
	}

	// Image mode. Key-backed: ask the host for the box's public URL (it
	// runs the brokered handshake) and point the <img> straight at it;
	// the bytes stream directly from the box (CSP admits its origin).
	// Re-resolve once on load error (the short-lived capability may
	// have lapsed before the <img> fetched). Url-backed: the URL IS the
	// src, nothing to resolve; an error means the resource itself is
	// unreachable. A second failure shows the metadata fallback.
	let imageUrl = $state<string | null>(null);
	let imageError = $state(false);
	let retried = false;
	// Generation counter guarding the async resolve: when the previewed file
	// changes while a resolve is in flight, the stale resolve must not land
	// its URL under the NEW file's caption. Every effect run mints a new
	// generation; writes from an older one are dropped.
	let resolveGen = 0;

	async function loadImageUrl(gen: number) {
		if (file.key === undefined) {
			if (gen === resolveGen) imageUrl = file.url;
			return;
		}
		try {
			const url = await resolveStoredFileUrl(file.key);
			if (gen !== resolveGen) return;
			imageUrl = url;
			imageError = false;
		} catch {
			if (gen === resolveGen) imageError = true;
		}
	}

	function onImgError() {
		if (!retried && file.key) {
			retried = true;
			void loadImageUrl(resolveGen);
		} else {
			imageError = true;
		}
	}

	$effect(() => {
		if (mode === 'image' && (file.key || file.url)) {
			retried = false;
			imageError = false;
			imageUrl = null;
			void loadImageUrl(++resolveGen);
		}
	});

	const displayName = $derived(file.filename || file.key || file.url || '');
</script>

{#if mode === 'image'}
	<div class="nodrag nopan nowheel my-1">
		{#if imageError}
			<div class="flex items-center gap-1.5 text-[10px] text-zinc-400 px-1 py-2">
				<AlertCircle class="w-3.5 h-3.5" />
				<span>Image expired or unavailable ({displayName})</span>
			</div>
		{:else if imageUrl}
			<!-- src is the box's public URL (key-backed) or the external
			     URL itself; the browser streams the bytes directly. -->
			<img
				src={imageUrl}
				alt={displayName}
				class="max-w-full max-h-64 rounded border border-zinc-200 object-contain"
				onerror={onImgError}
			/>
			<div class="text-[10px] text-zinc-400 mt-0.5">
				{file.filename}{#if file.sizeBytes > 0}&nbsp;· {fmtSize(file.sizeBytes)}{/if}
			</div>
		{:else}
			<div class="text-[10px] text-zinc-400 px-1 py-2">Loading image…</div>
		{/if}
	</div>
{:else}
	<div class="nodrag my-1 rounded border border-zinc-200 bg-zinc-50 p-2">
		<div class="flex items-center gap-2">
			<Icon class="w-4 h-4 text-zinc-500 shrink-0" />
			<div class="min-w-0 flex-1">
				<div class="text-[11px] font-mono text-zinc-700 truncate">
					{displayName}
				</div>
				<div class="text-[10px] text-zinc-400">{file.mimeType}{#if file.sizeBytes > 0}&nbsp;· {fmtSize(file.sizeBytes)}{/if}</div>
			</div>
			{#if file.key}
				<button
					class="flex items-center gap-1 rounded border border-zinc-300 bg-white px-2 py-0.5 text-[10px] text-zinc-600 hover:bg-zinc-100 transition-colors shrink-0"
					title="Download (streams directly from the storage box; shows 'expired' if swept)"
					onclick={download}
				>
					<Download class="w-3 h-3" />
					Download
				</button>
			{:else}
				<a
					href={file.url}
					target="_blank"
					rel="noopener noreferrer"
					class="flex items-center gap-1 rounded border border-zinc-300 bg-white px-2 py-0.5 text-[10px] text-zinc-600 hover:bg-zinc-100 transition-colors shrink-0"
					title="Open the external URL this file value points at"
				>
					<ExternalLink class="w-3 h-3" />
					Open
				</a>
			{/if}
		</div>
	</div>
{/if}
