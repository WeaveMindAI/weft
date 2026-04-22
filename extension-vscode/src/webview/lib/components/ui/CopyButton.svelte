<script lang="ts">
	let { text, class: className = '' }: { text: string; class?: string } = $props();
	let copied = $state(false);

	async function copy() {
		try {
			await navigator.clipboard.writeText(text);
		} catch {
			// Fallback for Chrome iframe/embed restrictions
			const ta = document.createElement('textarea');
			ta.value = text;
			ta.style.position = 'fixed';
			ta.style.opacity = '0';
			document.body.appendChild(ta);
			ta.select();
			document.execCommand('copy');
			document.body.removeChild(ta);
		}
		copied = true;
		setTimeout(() => copied = false, 1500);
	}
</script>

<button
	class="p-0.5 rounded hover:bg-zinc-200 transition-colors text-muted-foreground hover:text-foreground {className}"
	title={copied ? 'Copied!' : 'Copy'}
	onclick={copy}
>
	{#if copied}
		<svg class="w-3.5 h-3.5 text-emerald-500" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round">
			<polyline points="20 6 9 17 4 12"></polyline>
		</svg>
	{:else}
		<svg class="w-3.5 h-3.5" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round">
			<rect x="9" y="9" width="13" height="13" rx="2" ry="2"></rect>
			<path d="M5 15H4a2 2 0 0 1-2-2V4a2 2 0 0 1 2-2h9a2 2 0 0 1 2 2v1"></path>
		</svg>
	{/if}
</button>
