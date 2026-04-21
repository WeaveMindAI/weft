<script lang="ts">
	import { listCloudFiles, resolveCloudFile, formatBytes, type CloudFile } from '$lib/utils/blob-upload';
	import type { FileRef } from '$lib/types';
	import { toast } from 'svelte-sonner';

	let { open = $bindable(false), accept, onSelect }: {
		open: boolean;
		accept?: string;
		onSelect: (ref: FileRef) => void;
	} = $props();

	let files = $state<CloudFile[]>([]);
	let loading = $state(true);
	let resolving = $state<string | null>(null);
	let search = $state('');

	$effect(() => {
		if (open) {
			loading = true;
			search = '';
			listCloudFiles().then(f => {
				files = f;
				loading = false;
			});
		}
	});

	let filtered = $derived(() => {
		let result = files;
		// Filter by accept mime types if specified
		if (accept) {
			const acceptTypes = accept.split(',').map(t => t.trim().toLowerCase());
			result = result.filter(f => {
				const mime = f.mime_type.toLowerCase();
				return acceptTypes.some(a => {
					if (a === '*/*') return true;
					if (a.endsWith('/*')) return mime.startsWith(a.replace('/*', '/'));
					return mime === a;
				});
			});
		}
		// Filter by search
		if (search.trim()) {
			const q = search.trim().toLowerCase();
			result = result.filter(f => f.filename.toLowerCase().includes(q));
		}
		return result;
	});

	async function selectFile(file: CloudFile) {
		resolving = file.id;
		try {
			const ref = await resolveCloudFile(file);
			onSelect(ref);
			open = false;
		} catch (err) {
			toast.error(`Failed to select file: ${err instanceof Error ? err.message : 'Unknown error'}`);
		}
		resolving = null;
	}

	function formatDate(iso: string): string {
		return new Date(iso).toLocaleDateString(undefined, {
			month: 'short', day: 'numeric', hour: '2-digit', minute: '2-digit',
		});
	}

	function mimeIcon(mime: string): string {
		if (mime.startsWith('video/')) return '🎬';
		if (mime.startsWith('audio/')) return '🎵';
		if (mime.startsWith('image/')) return '🖼';
		if (mime === 'application/pdf') return '📄';
		return '📎';
	}
</script>

{#if open}
	<!-- svelte-ignore a11y_no_noninteractive_element_interactions -->
	<div
		class="fixed inset-0 z-50 flex items-center justify-center bg-black/40 backdrop-blur-sm"
		role="dialog"
		aria-modal="true"
		tabindex="-1"
		onclick={(e) => { if (e.target === e.currentTarget) open = false; }}
		onkeydown={(e) => { if (e.key === 'Escape') open = false; }}
	>
		<div class="bg-popover rounded-2xl shadow-2xl border border-border w-full max-w-md mx-4 overflow-hidden flex flex-col max-h-[70vh]">
			<div class="px-5 pt-5 pb-3 border-b border-border">
				<div class="flex items-center justify-between mb-3">
					<h2 class="text-sm font-semibold">Choose from your files</h2>
					<button
						class="text-muted-foreground hover:text-foreground text-lg leading-none"
						onclick={() => open = false}
					>&times;</button>
				</div>
				<input
					type="text"
					class="w-full text-sm bg-muted px-3 py-2 rounded-lg border-none outline-none placeholder-muted-foreground"
					placeholder="Search files..."
					bind:value={search}
				/>
			</div>

			<div class="overflow-y-auto flex-1 p-3">
				{#if loading}
					<div class="text-center py-8 text-sm text-muted-foreground">Loading...</div>
				{:else if filtered().length === 0}
					<div class="text-center py-8 text-sm text-muted-foreground">
						{search ? 'No files match your search.' : 'No files uploaded yet.'}
					</div>
				{:else}
					<div class="grid gap-2">
						{#each filtered() as file (file.id)}
							<button
								class="w-full text-left px-3 py-2.5 rounded-lg border border-border hover:border-foreground/20 hover:bg-muted/50 transition-colors flex items-center gap-3 group {resolving === file.id ? 'opacity-50 pointer-events-none' : ''}"
								onclick={() => selectFile(file)}
								disabled={resolving !== null}
							>
								<span class="text-lg shrink-0">{mimeIcon(file.mime_type)}</span>
								<div class="flex-1 min-w-0">
									<div class="text-sm font-medium truncate text-foreground">{file.filename}</div>
									<div class="text-[11px] text-muted-foreground flex items-center gap-2">
										<span>{formatBytes(file.size_bytes)}</span>
										<span>{formatDate(file.created_at)}</span>
									</div>
								</div>
								{#if resolving === file.id}
									<span class="text-xs text-muted-foreground animate-pulse">Loading...</span>
								{/if}
							</button>
						{/each}
					</div>
				{/if}
			</div>
		</div>
	</div>
{/if}
