<script lang="ts">
	// `label` names WHAT is copied (e.g. "Copy run state"): it becomes the
	// resting tooltip and the copied-confirmation. Omitted, the button falls back
	// to the generic "Copy" / "Copied!".
	// `oncopied` hears each copy once the text is on the clipboard, with the
	// exact text that got there (the one read when the press started).
	let {
		text,
		label,
		class: className = '',
		oncopied
	}: { text: string; label?: string; class?: string; oncopied?: (copied: string) => void } = $props();
	// What the last press did, shown in the tooltip for a moment.
	let outcome = $state<'copied' | 'failed' | null>(null);

	const restingTitle = $derived(label ?? 'Copy');
	const copiedTitle = $derived(label ? `${label} (copied!)` : 'Copied!');
	const failedTitle = $derived(label ? `${label} (could not copy)` : 'Could not copy');

	/// Put `value` on the clipboard: whether it got there.
	async function written(value: string): Promise<boolean> {
		try {
			await navigator.clipboard.writeText(value);
			return true;
		} catch {
			// Fallback for Chrome iframe/embed restrictions, which says
			// whether the copy happened by what it returns.
			const ta = document.createElement('textarea');
			ta.value = value;
			ta.style.position = 'fixed';
			ta.style.opacity = '0';
			document.body.appendChild(ta);
			try {
				ta.select();
				return document.execCommand('copy');
			} catch {
				return false;
			} finally {
				document.body.removeChild(ta);
			}
		}
	}

	async function copy() {
		// Read once: the prop may change while the clipboard is written.
		const value = text;
		const done = await written(value);
		outcome = done ? 'copied' : 'failed';
		// Heard only when the text is really on the clipboard, and with
		// what got there: a secret its node hands out once is retired on
		// this, against the value its reader now holds.
		if (done) oncopied?.(value);
		setTimeout(() => outcome = null, 1500);
	}
</script>

<button
	class="p-0.5 rounded hover:bg-zinc-200 transition-colors text-muted-foreground hover:text-foreground {className}"
	title={outcome === 'copied' ? copiedTitle : outcome === 'failed' ? failedTitle : restingTitle}
	onclick={copy}
>
	{#if outcome === 'failed'}
		<svg class="w-3.5 h-3.5 text-rose-500" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round">
			<line x1="18" y1="6" x2="6" y2="18"></line>
			<line x1="6" y1="6" x2="18" y2="18"></line>
		</svg>
	{:else if outcome === 'copied'}
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
