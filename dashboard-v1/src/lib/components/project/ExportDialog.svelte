<script lang="ts">
	import type { ProjectDefinition } from '$lib/types';
	import { countSensitiveValues } from '$lib/ai/sanitize';

	let { open = $bindable(false), project, onExport }: {
		open: boolean;
		project: ProjectDefinition;
		onExport: (stripSensitive: boolean) => void;
	} = $props();

	let stripSensitive = $state(true);
	// Sourced from the shared sanitizer so the count and the actual
	// strip logic can't drift. See $lib/ai/sanitize.
	let sensitiveCount = $derived(countSensitiveValues(project.nodes));

	function handleExport() {
		onExport(stripSensitive);
		open = false;
	}

	function handleKeydown(e: KeyboardEvent) {
		if (e.key === 'Escape') open = false;
	}
</script>

{#if open}
	<div
		class="fixed inset-0 z-50 flex items-center justify-center bg-black/40 backdrop-blur-sm"
		role="dialog"
		aria-modal="true"
		tabindex="-1"
		onclick={(e) => { if (e.target === e.currentTarget) open = false; }}
		onkeydown={handleKeydown}
	>
		<div class="bg-white rounded-2xl shadow-2xl border border-zinc-200 w-full max-w-sm mx-4 overflow-hidden">
			<div class="px-6 py-4 border-b border-zinc-100 flex items-center justify-between">
				<h2 class="text-base font-semibold text-zinc-900">Export Project</h2>
				<button
					class="text-zinc-400 hover:text-zinc-600 transition-colors"
					onclick={() => open = false}
					aria-label="Close"
				>
					<svg xmlns="http://www.w3.org/2000/svg" width="18" height="18" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><line x1="18" y1="6" x2="6" y2="18"/><line x1="6" y1="6" x2="18" y2="18"/></svg>
				</button>
			</div>

			<div class="px-6 py-5 flex flex-col gap-5">
				<!-- Strip sensitive fields -->
				<label class="flex items-start gap-3 cursor-pointer group">
					<div class="relative mt-0.5">
						<input
							type="checkbox"
							class="sr-only"
							bind:checked={stripSensitive}
						/>
						<div class="w-4 h-4 rounded border-2 transition-colors flex items-center justify-center {stripSensitive ? 'bg-zinc-900 border-zinc-900' : 'bg-white border-zinc-300 group-hover:border-zinc-400'}">
							{#if stripSensitive}
								<svg xmlns="http://www.w3.org/2000/svg" width="10" height="10" viewBox="0 0 24 24" fill="none" stroke="white" stroke-width="3"><polyline points="20 6 9 17 4 12"/></svg>
							{/if}
						</div>
					</div>
					<div class="flex flex-col gap-0.5">
						<span class="text-sm font-medium text-zinc-800">Strip sensitive fields</span>
						<span class="text-xs text-zinc-400">
							Remove passwords and API keys before exporting.
							{#if sensitiveCount > 0}
								<span class="text-amber-600 font-medium">{sensitiveCount} sensitive value{sensitiveCount !== 1 ? 's' : ''} found.</span>
							{:else}
								No sensitive values detected.
							{/if}
						</span>
					</div>
				</label>
			</div>

			<div class="px-6 py-4 border-t border-zinc-100 flex justify-end gap-2">
				<button
					class="px-4 py-2 text-sm text-zinc-600 hover:text-zinc-900 transition-colors"
					onclick={() => open = false}
				>
					Cancel
				</button>
				<button
					class="px-5 py-2 bg-zinc-900 text-white text-sm font-medium rounded-lg hover:bg-zinc-700 transition-colors flex items-center gap-2"
					onclick={handleExport}
				>
					<svg xmlns="http://www.w3.org/2000/svg" width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><path d="M21 15v4a2 2 0 0 1-2 2H5a2 2 0 0 1-2-2v-4"/><polyline points="7 10 12 15 17 10"/><line x1="12" y1="15" x2="12" y2="3"/></svg>
					Export
				</button>
			</div>
		</div>
	</div>
{/if}
