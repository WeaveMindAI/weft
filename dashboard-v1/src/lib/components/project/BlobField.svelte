<script lang="ts">
	import { handleBlobFieldUpload, validateExternalUrl, formatBytes } from '$lib/utils/blob-upload';
	import FilePicker from './FilePicker.svelte';
	import { toast } from 'svelte-sonner';
	import type { FileRef } from '$lib/types';

	let {
		fileRef,
		accept,
		id,
		placeholder,
		onUpdate,
	}: {
		fileRef: FileRef | undefined | null;
		accept?: string;
		id: string;
		placeholder?: string;
		onUpdate: (ref: FileRef | null) => void;
	} = $props();

	let filePickerOpen = $state(false);
</script>

<!-- svelte-ignore a11y_no_static_element_interactions -->
<div
	class="blob-field-container"
	ondragover={(e: DragEvent) => {
		e.preventDefault();
		e.stopPropagation();
		if (e.dataTransfer) e.dataTransfer.dropEffect = 'copy';
		(e.currentTarget as HTMLElement)?.classList.add('blob-drag-over');
	}}
	ondragleave={(e: DragEvent) => {
		e.preventDefault();
		e.stopPropagation();
		(e.currentTarget as HTMLElement)?.classList.remove('blob-drag-over');
	}}
	ondrop={async (e: DragEvent) => {
		e.preventDefault();
		e.stopPropagation();
		(e.currentTarget as HTMLElement)?.classList.remove('blob-drag-over');
		const file = e.dataTransfer?.files?.[0];
		if (!file) return;
		await handleBlobFieldUpload(file, accept, onUpdate, (msg) => toast.error(`Upload failed: ${msg}`));
	}}
>
	{#if fileRef?.filename && !fileRef?.url && !fileRef?.file_id}
		<div class="flex items-center gap-1.5 w-full text-xs bg-muted px-2 py-1.5 rounded animate-pulse">
			<span class="truncate flex-1 text-muted-foreground">{fileRef.filename}</span>
		</div>
	{:else if fileRef?.url || fileRef?.file_id}
		<div class="flex items-center gap-1.5 w-full text-xs bg-muted px-2 py-1.5 rounded">
			<span class="truncate flex-1 text-foreground">{fileRef.filename}</span>
			{#if fileRef.size_bytes > 0}
				<span class="text-muted-foreground text-[10px] shrink-0">
					{fileRef.size_bytes > 1048576
						? `${(fileRef.size_bytes / 1048576).toFixed(1)}MB`
						: `${(fileRef.size_bytes / 1024).toFixed(0)}KB`}
				</span>
			{/if}
			<button
				class="text-muted-foreground hover:text-destructive text-[10px] shrink-0"
				onclick={() => onUpdate(null)}
			>&times;</button>
		</div>
	{:else}
		<div class="flex flex-col gap-1 w-full">
			<input
				type="file"
				accept={accept || "*/*"}
				class="hidden"
				id={`blob-input-${id}`}
				onchange={async (e) => {
					const file = (e.currentTarget as HTMLInputElement).files?.[0];
					if (!file) return;
					await handleBlobFieldUpload(file, accept, onUpdate, (msg) => toast.error(`Upload failed: ${msg}`));
				}}
			/>
			<!-- svelte-ignore a11y_no_noninteractive_element_interactions -->
			<label
				for={`blob-input-${id}`}
				class="flex items-center justify-center w-full text-[10px] py-1.5 border border-dashed border-zinc-600 hover:border-zinc-400 text-zinc-400 hover:text-zinc-300 rounded cursor-pointer transition-colors"
			>Drop file or click to upload</label>
			<button
				class="flex items-center justify-center w-full text-[10px] py-1.5 text-zinc-400 hover:text-zinc-300 transition-colors"
				onclick={(e) => { e.stopPropagation(); filePickerOpen = true; }}
			>Browse uploaded files</button>
			<input
				type="text"
				class="w-full text-xs bg-muted px-2 py-1.5 rounded border-none outline-none"
				placeholder={placeholder || 'Paste URL (https://...)'}
				onblur={(e) => {
					const val = (e.currentTarget as HTMLInputElement).value;
					if (!val) return;
					if (val.startsWith('data:')) {
						toast.error('Data URIs are not supported. Please use an https:// URL or upload a file.');
						(e.currentTarget as HTMLInputElement).value = '';
						return;
					}
					const ref = validateExternalUrl(val);
					if (ref) {
						onUpdate(ref);
					} else {
						toast.error('Invalid URL. Please use an https:// URL.');
					}
				}}
				onclick={(e) => e.stopPropagation()}
			/>
		</div>
	{/if}
</div>

<FilePicker
	bind:open={filePickerOpen}
	{accept}
	onSelect={onUpdate}
/>
