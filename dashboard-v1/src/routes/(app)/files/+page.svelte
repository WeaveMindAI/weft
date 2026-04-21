<script lang="ts">
	import { onMount } from "svelte";
	import { goto } from "$app/navigation";
	import { authFetch, api } from "$lib/config";
	import { isCloudMode, uploadBlob, formatBytes } from "$lib/utils/blob-upload";
	import { toast } from "svelte-sonner";
	import type { FileRef } from "$lib/types";

	interface StorageUsage {
		used_bytes: number;
		quota_bytes: number;
	}

	interface FileEntry {
		id: string;
		filename: string;
		mime_type: string;
		size_bytes: number;
		ephemeral: boolean;
		created_at: string;
		expires_at: string | null;
	}

	let files = $state<FileEntry[]>([]);
	let usage = $state<StorageUsage | null>(null);
	let loading = $state(true);
	let deleting = $state<string | null>(null);
	let uploadProgress: string | null = $state(null);

	onMount(() => {
		if (!isCloudMode()) { goto('/dashboard'); return; }
		loadData();
	});

	async function loadData() {
		loading = true;
		try {
			const [filesRes, usageRes] = await Promise.all([
				authFetch(api.listFiles()),
				authFetch(api.getStorageUsage()),
			]);
			if (filesRes.ok) files = await filesRes.json();
			if (usageRes.ok) usage = await usageRes.json();
		} catch (err) {
			console.error('Failed to load files:', err);
			toast.error('Failed to load files');
		}
		loading = false;
	}

	async function deleteFile(fileId: string) {
		deleting = fileId;
		try {
			const res = await authFetch(api.deleteFile(fileId), { method: 'DELETE' });
			if (res.ok) {
				files = files.filter(f => f.id !== fileId);
				toast.success('File deleted');
				const usageRes = await authFetch(api.getStorageUsage());
				if (usageRes.ok) usage = await usageRes.json();
			} else {
				toast.error('Failed to delete file');
			}
		} catch {
			toast.error('Failed to delete file');
		}
		deleting = null;
	}

	async function handleUpload(e: Event) {
		const input = e.currentTarget as HTMLInputElement;
		const file = input.files?.[0];
		if (!file) return;
		try {
			uploadProgress = `Uploading ${file.name}...`;
			await uploadBlob(file, file.name, file.type || 'application/octet-stream', (loaded, total) => {
				uploadProgress = `Uploading ${file.name} (${formatBytes(loaded)} / ${formatBytes(total)})`;
			});
			uploadProgress = null;
			toast.success(`Uploaded ${file.name}`);
			await loadData();
		} catch (err) {
			uploadProgress = null;
			toast.error(`Upload failed: ${err instanceof Error ? err.message : 'Unknown error'}`);
		}
		input.value = '';
	}

	function formatSize(bytes: number): string {
		if (bytes >= 1073741824) return `${(bytes / 1073741824).toFixed(1)} GB`;
		if (bytes >= 1048576) return `${(bytes / 1048576).toFixed(1)} MB`;
		if (bytes >= 1024) return `${(bytes / 1024).toFixed(0)} KB`;
		return `${bytes} B`;
	}

	function formatTimeAgo(iso: string): string {
		const ms = Date.now() - new Date(iso).getTime();
		const min = Math.floor(ms / 60000);
		if (min < 1) return 'just now';
		if (min < 60) return `${min}m ago`;
		const hr = Math.floor(min / 60);
		if (hr < 24) return `${hr}h ago`;
		return `${Math.floor(hr / 24)}d ago`;
	}

	function mimeIcon(mime: string): string {
		if (mime.startsWith('image/')) return '🖼';
		if (mime.startsWith('video/')) return '🎬';
		if (mime.startsWith('audio/')) return '🎵';
		if (mime.includes('pdf')) return '📄';
		if (mime.includes('json') || mime.includes('javascript') || mime.includes('typescript')) return '📋';
		if (mime.includes('zip') || mime.includes('tar') || mime.includes('gzip')) return '📦';
		if (mime.includes('csv') || mime.includes('spreadsheet') || mime.includes('excel')) return '📊';
		return '📎';
	}
</script>

<div class="min-h-screen pt-20 px-6 pb-12" style="background: #f8f9fa; background-image: radial-gradient(circle, #d4d4d8 1px, transparent 1px); background-size: 24px 24px;">
	<div class="max-w-4xl mx-auto">

		<!-- Header -->
		<div class="flex items-center justify-between mb-5">
			<h2 class="text-[15px] font-semibold text-zinc-800">Files</h2>
			<label class="inline-flex items-center gap-1.5 px-3 py-1.5 text-[11px] font-medium rounded-lg bg-zinc-800 text-white hover:bg-zinc-700 transition-colors cursor-pointer">
				<svg width="12" height="12" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2.5" stroke-linecap="round" stroke-linejoin="round"><path d="M21 15v4a2 2 0 0 1-2 2H5a2 2 0 0 1-2-2v-4"/><polyline points="17 8 12 3 7 8"/><line x1="12" y1="3" x2="12" y2="15"/></svg>
				Upload
				<input type="file" class="hidden" onchange={handleUpload} />
			</label>
		</div>

		<!-- Storage usage -->
		{#if usage}
			<div class="bg-white rounded-xl border border-zinc-200 px-4 py-3 mb-4" style="box-shadow: 0 1px 3px rgba(0,0,0,0.06);">
				<div class="flex items-center justify-between text-[10px] text-zinc-400 mb-1.5">
					<span>{formatSize(usage.used_bytes)} used</span>
					<span>{formatSize(usage.quota_bytes)} total</span>
				</div>
				<div class="w-full h-1.5 bg-zinc-100 rounded-full overflow-hidden">
					<div
						class="h-full rounded-full transition-all"
						style="width: {Math.min(100, (usage.used_bytes / usage.quota_bytes) * 100)}%; background: {(usage.used_bytes / usage.quota_bytes) > 0.8 ? '#ef4444' : '#3f3f46'}"
					></div>
				</div>
			</div>
		{/if}

		<!-- Upload progress -->
		{#if uploadProgress}
			<div class="bg-white rounded-xl border border-zinc-200 px-4 py-3 mb-4 animate-pulse" style="box-shadow: 0 1px 3px rgba(0,0,0,0.06);">
				<p class="text-[11px] text-zinc-500">{uploadProgress}</p>
			</div>
		{/if}

		<!-- File list -->
		{#if loading}
			<div class="flex items-center justify-center py-24">
				<div class="h-5 w-5 border-2 border-zinc-300 border-t-zinc-600 rounded-full animate-spin"></div>
			</div>
		{:else if files.length === 0}
			<div class="flex flex-col items-center justify-center py-24">
				<div class="w-14 h-14 rounded-full bg-white border border-zinc-200 flex items-center justify-center mb-5" style="box-shadow: 0 2px 8px rgba(0,0,0,0.06);">
					<svg width="22" height="22" viewBox="0 0 24 24" fill="none" stroke="#a1a1aa" stroke-width="1.5"><path d="M14 2H6a2 2 0 0 0-2 2v16a2 2 0 0 0 2 2h12a2 2 0 0 0 2-2V8Z"/><polyline points="14 2 14 8 20 8"/></svg>
				</div>
				<p class="text-[15px] font-medium text-zinc-600 mb-1">No files yet</p>
				<p class="text-[13px] text-zinc-400">Upload files here or from blob fields in your projects</p>
			</div>
		{:else}
			<div class="bg-white rounded-xl border border-zinc-200 overflow-hidden" style="box-shadow: 0 1px 3px rgba(0,0,0,0.06);">
				{#each files as file, i (file.id)}
					<div class="flex items-center gap-3 px-4 py-2.5 hover:bg-zinc-50 transition-colors {i > 0 ? 'border-t border-zinc-100' : ''}">
						<span class="text-[16px] shrink-0">{mimeIcon(file.mime_type)}</span>
						<div class="flex-1 min-w-0">
							<div class="flex items-center gap-2">
								<span class="text-[12px] font-medium text-zinc-700 truncate">{file.filename}</span>
								{#if file.ephemeral}
									<span class="text-[9px] font-medium px-1.5 py-0.5 rounded bg-amber-50 text-amber-600 border border-amber-200">ephemeral</span>
								{/if}
							</div>
							<div class="flex items-center gap-2 mt-0.5">
								<span class="text-[10px] text-zinc-400">{file.mime_type}</span>
								<span class="text-[10px] text-zinc-300">·</span>
								<span class="text-[10px] text-zinc-400">{formatSize(file.size_bytes)}</span>
								<span class="text-[10px] text-zinc-300">·</span>
								<span class="text-[10px] text-zinc-400">{formatTimeAgo(file.created_at)}</span>
							</div>
						</div>
						<button
							onclick={() => deleteFile(file.id)}
							disabled={deleting === file.id}
							class="px-2 py-1 text-[10px] font-medium rounded-md border border-red-200 text-red-600 hover:bg-red-50 transition-colors disabled:opacity-40 shrink-0"
						>
							{deleting === file.id ? '...' : 'Delete'}
						</button>
					</div>
				{/each}
			</div>
		{/if}
	</div>
</div>
