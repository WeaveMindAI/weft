<script lang="ts">
	import { Download, FileAudio, FileVideo, FileText, Image as ImageIcon, AlertCircle } from '@lucide/svelte';
	import type { StoredFileWire } from '../../../../shared/protocol';
	import { send, resolveStoredFileUrl } from '../../../vscode';

	// `mode='image'` renders the picture inline (ImageDisplay);
	// `mode='link'` renders a metadata + Download button card
	// (DownloadLink). Both resolve bytes through the SAME authenticated
	// handshake a user download uses; nothing is ever a public link.
	let { file, mode }: { file: StoredFileWire; mode: 'image' | 'link' } = $props();

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
		send({ kind: 'downloadStoredFile', key: file.key });
	}

	// Image mode: ask the host for the box's public URL (it runs the
	// brokered handshake) and point the <img> straight at it; the
	// bytes stream directly from the box (CSP admits its origin).
	// Re-resolve once on load error (the short-lived capability may
	// have lapsed before the <img> fetched). A second failure means
	// the file is gone or unreachable; show the metadata fallback.
	let imageUrl = $state<string | null>(null);
	let imageError = $state(false);
	let retried = false;

	async function loadImageUrl() {
		try {
			imageUrl = await resolveStoredFileUrl(file.key);
			imageError = false;
		} catch {
			imageError = true;
		}
	}

	function onImgError() {
		if (!retried) {
			retried = true;
			void loadImageUrl();
		} else {
			imageError = true;
		}
	}

	$effect(() => {
		if (mode === 'image' && file.key) {
			retried = false;
			imageError = false;
			imageUrl = null;
			void loadImageUrl();
		}
	});
</script>

{#if mode === 'image'}
	<div class="nodrag nopan nowheel my-1">
		{#if imageError}
			<div class="flex items-center gap-1.5 text-[10px] text-zinc-400 px-1 py-2">
				<AlertCircle class="w-3.5 h-3.5" />
				<span>Image expired or unavailable ({file.filename || file.key})</span>
			</div>
		{:else if imageUrl}
			<!-- src is the box's public URL; the browser streams the
			     bytes directly from the box (CSP admits its origin). -->
			<img
				src={imageUrl}
				alt={file.filename || file.key}
				class="max-w-full max-h-64 rounded border border-zinc-200 object-contain"
				onerror={onImgError}
			/>
			<div class="text-[10px] text-zinc-400 mt-0.5">
				{file.filename} · {fmtSize(file.sizeBytes)}
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
					{file.filename || file.key}
				</div>
				<div class="text-[10px] text-zinc-400">{file.mimeType} · {fmtSize(file.sizeBytes)}</div>
			</div>
			<button
				class="flex items-center gap-1 rounded border border-zinc-300 bg-white px-2 py-0.5 text-[10px] text-zinc-600 hover:bg-zinc-100 transition-colors shrink-0"
				title="Download (streams directly from the storage box; shows 'expired' if swept)"
				onclick={download}
			>
				<Download class="w-3 h-3" />
				Download
			</button>
		</div>
	</div>
{/if}
