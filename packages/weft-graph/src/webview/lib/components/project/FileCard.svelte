<script lang="ts">
	import { Download, ExternalLink, FileAudio, FileVideo, FileImage, FileText } from '@lucide/svelte';
	import type { FileValueWire } from '../../../../shared/protocol';
	import { send } from '../../../vscode';

	let { label, file }: { label: string; file: FileValueWire } = $props();

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
		: file.mimeType.startsWith('image/') ? FileImage
		: FileText,
	);

	// The journal carries only the self-describing reference, so the
	// metadata here is ALWAYS renderable, even for swept files. A
	// key-backed file downloads at click time: the host runs the
	// dispatcher handshake and surfaces "expired or deleted" on 404. A
	// url-backed file just links to its external URL.
	function download() {
		if (file.key) send({ kind: 'downloadStoredFile', key: file.key });
	}
</script>

<div class="my-1 rounded border border-zinc-200 bg-zinc-50 p-2">
	<div class="flex items-center gap-2">
		<Icon class="w-4 h-4 text-zinc-500 shrink-0" />
		<div class="min-w-0 flex-1">
			<div class="text-[11px] font-mono text-zinc-700 truncate">
				<span class="text-zinc-400">{label}:</span>
				{file.filename || file.key || file.url}
			</div>
			<div class="text-[10px] text-zinc-400">
				{file.mimeType}{#if file.sizeBytes > 0}&nbsp;· {fmtSize(file.sizeBytes)}{/if}
			</div>
		</div>
		{#if file.key}
			<button
				class="flex items-center gap-1 rounded border border-zinc-300 bg-white px-2 py-0.5 text-[10px] text-zinc-600 hover:bg-zinc-100 transition-colors shrink-0"
				title="Download (streams directly from the storage box; shows 'expired' if the file was swept)"
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
