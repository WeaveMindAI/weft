<script lang="ts">
	import { Download, ExternalLink, FileAudio, FileVideo, FileText, Image as ImageIcon, AlertCircle } from '@lucide/svelte';
	import type PlyrType from 'plyr';
	import 'plyr/dist/plyr.css';
	import type { FileValueWire } from '../../../../protocol';
	import { send, resolveStoredFileUrl } from '../../../vscode';

	// The node body's inline file renderer (`features.display`).
	// `mode='media'` renders the file by its OWN mime type: an image
	// inline, audio/video with a real player (Plyr, the same library
	// the website's media views use), anything unplayable as the file
	// card; a save button rides below whatever played. `mode='link'`
	// renders the metadata + Download card only. A key-backed file
	// resolves its bytes through the SAME authenticated handshake a
	// user download uses; a url-backed file points at an external
	// resource and is rendered/linked directly (the browser fetches
	// it, nothing goes through storage).
	let { file, mode }: { file: FileValueWire; mode: 'media' | 'link' } = $props();

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

	const media = $derived<'image' | 'audio' | 'video' | null>(
		file.mimeType.startsWith('image/') ? 'image'
		: file.mimeType.startsWith('audio/') ? 'audio'
		: file.mimeType.startsWith('video/') ? 'video'
		: null,
	);
	const Icon = $derived(
		media === 'audio' ? FileAudio
		: media === 'video' ? FileVideo
		: media === 'image' ? ImageIcon
		: FileText,
	);

	function download() {
		if (file.key)
			send({ kind: 'downloadStoredFile', key: file.key, filename: file.filename });
	}

	// Media mode. Key-backed: ask the host for the box's public URL (it
	// runs the brokered handshake) and point the element straight at
	// it; the bytes stream directly from the box (CSP admits its
	// origin). Re-resolve once on load error (the short-lived
	// capability may have lapsed before the element fetched).
	// Url-backed: the URL IS the src, nothing to resolve; an error
	// means the resource itself is unreachable. A second failure shows
	// the metadata fallback.
	let mediaUrl = $state<string | null>(null);
	let mediaError = $state(false);
	let retried = false;
	// Generation counter guarding the async resolve: when the previewed file
	// changes while a resolve is in flight, the stale resolve must not land
	// its URL under the NEW file's caption. Every effect run mints a new
	// generation; writes from an older one are dropped.
	let resolveGen = 0;

	async function loadMediaUrl(gen: number) {
		if (file.key === undefined) {
			if (gen === resolveGen) mediaUrl = file.url;
			return;
		}
		try {
			const url = await resolveStoredFileUrl(file.key);
			if (gen !== resolveGen) return;
			mediaUrl = url;
			mediaError = false;
		} catch {
			if (gen === resolveGen) mediaError = true;
		}
	}

	function onMediaError() {
		if (!retried && file.key) {
			retried = true;
			void loadMediaUrl(resolveGen);
		} else {
			mediaError = true;
		}
	}

	$effect(() => {
		if (mode === 'media' && media !== null && (file.key || file.url)) {
			retried = false;
			mediaError = false;
			mediaUrl = null;
			void loadMediaUrl(++resolveGen);
		}
	});

	// The Plyr instance over the audio/video element, rebuilt when the
	// element or source changes, torn down first so none ever leaks.
	// Plyr touches `document` at MODULE LOAD, so it is imported
	// dynamically inside the effect (browser-only by construction),
	// with a cancelled flag guarding the async gap. Same pattern as
	// the website's MediaPlayer, so both surfaces share one media
	// idiom.
	let playerEl = $state<HTMLAudioElement | HTMLVideoElement | null>(null);
	$effect(() => {
		void mediaUrl;
		const host = playerEl;
		if (!host) return;
		let cancelled = false;
		let instance: PlyrType | null = null;
		void (async () => {
			const { default: Plyr } = await import('plyr');
			if (cancelled) return;
			instance = new Plyr(host, {
				controls:
					media === 'video'
						? ['play-large', 'play', 'progress', 'current-time', 'mute', 'volume', 'fullscreen']
						: ['play', 'progress', 'current-time', 'mute', 'volume'],
			});
		})();
		return () => {
			cancelled = true;
			instance?.destroy();
		};
	});

	const displayName = $derived(file.filename || file.key || file.url || '');
</script>

{#snippet saveRow()}
	<div class="flex items-center gap-2 mt-0.5">
		<div class="text-[10px] text-zinc-400 min-w-0 truncate">
			{file.filename}{#if file.sizeBytes > 0}&nbsp;· {fmtSize(file.sizeBytes)}{/if}
		</div>
		{#if file.key}
			<button
				class="flex items-center gap-1 rounded border border-zinc-300 bg-white px-1.5 py-0.5 text-[10px] text-zinc-600 hover:bg-zinc-100 transition-colors shrink-0"
				title="Save the file"
				onclick={download}
			>
				<Download class="w-3 h-3" />
				Save
			</button>
		{:else if file.url}
			<a
				href={file.url}
				target="_blank"
				rel="noopener noreferrer"
				class="flex items-center gap-1 rounded border border-zinc-300 bg-white px-1.5 py-0.5 text-[10px] text-zinc-600 hover:bg-zinc-100 transition-colors shrink-0"
				title="Open the external URL this file value points at"
			>
				<ExternalLink class="w-3 h-3" />
				Open
			</a>
		{/if}
	</div>
{/snippet}

{#if mode === 'media' && media !== null}
	<div class="nodrag nopan nowheel my-1">
		{#if mediaError}
			<div class="flex items-center gap-1.5 text-[10px] text-zinc-400 px-1 py-2">
				<AlertCircle class="w-3.5 h-3.5" />
				<span>Media expired or unavailable ({displayName})</span>
			</div>
		{:else if mediaUrl}
			<!-- src is the box's public URL (key-backed) or the external
			     URL itself; the browser streams the bytes directly. -->
			{#if media === 'image'}
				<img
					src={mediaUrl}
					alt={displayName}
					class="max-w-full max-h-64 rounded border border-zinc-200 object-contain"
					onerror={onMediaError}
				/>
			{:else if media === 'audio'}
				<audio bind:this={playerEl} onerror={onMediaError}>
					<source src={mediaUrl} type={file.mimeType} />
				</audio>
			{:else}
				<!-- svelte-ignore a11y_media_has_caption (generated media has no track) -->
				<video bind:this={playerEl} class="max-w-full max-h-64 rounded" playsinline onerror={onMediaError}>
					<source src={mediaUrl} type={file.mimeType} />
				</video>
			{/if}
			{@render saveRow()}
		{:else}
			<div class="text-[10px] text-zinc-400 px-1 py-2">Loading media…</div>
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

<style>
	/* Theme Plyr to the node body's compact, light aesthetic. */
	:global(.plyr) {
		--plyr-color-main: #6366f1;
		--plyr-control-radius: 4px;
		border-radius: 0.25rem;
		border: 1px solid rgb(228 228 231);
		font-size: 11px;
	}
	:global(.plyr--audio .plyr__controls) {
		padding: 4px 6px;
	}
</style>
