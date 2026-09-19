<script lang="ts">
	/// One port's value in the execution inspector: a card carrying the
	/// port name, what the value is, a copy button, and the value
	/// itself. Same card shape as `FileCard`, so a file port and a data
	/// port read as the same kind of thing.
	///
	/// Long values were the reason this exists. The old rendering cut
	/// every string at 120 characters with no way to see the rest, so a
	/// prompt, an LLM reply or a caption was unreadable in the one place
	/// built for reading it.
	import { ChevronDown, ChevronRight } from '@lucide/svelte';
	import CopyButton from '../ui/CopyButton.svelte';
	import { isPlainText, jsonTokens, valueSummary, valueText } from '../../utils/value-display';
	import type { JsonTokenKind } from '../../utils/value-display';

	/// One colour per JSON kind. Keys read darker than their values so
	/// the shape of an object is legible before any of its content is.
	function tokenClass(kind: JsonTokenKind): string {
		switch (kind) {
			case 'key': return 'text-zinc-900 font-medium';
			case 'string': return 'text-emerald-700';
			case 'number': return 'text-sky-700';
			case 'boolean': return 'text-amber-700';
			case 'null': return 'text-zinc-400 italic';
			case 'punct': return 'text-zinc-400';
		}
	}

	let { label, value = undefined, closed = false, note = undefined }: {
		/// The port name.
		label: string;
		value?: unknown;
		/// A port that was closed without a value: the card is greyed
		/// out and says so, with nothing to expand or copy.
		closed?: boolean;
		/// Where the value came from when it did not travel a wire in
		/// this run (a backup, a supplied output, the seed run). Shown
		/// in the header beside the value's type.
		note?: string;
	} = $props();

	const text = $derived(valueText(value));
	const summary = $derived(valueSummary(value));
	const plain = $derived(isPlainText(value));
	const tokens = $derived(plain ? [] : jsonTokens(value));
	/// Collapsed height, in lines. A value taller than this gets an
	/// expand control; anything shorter is simply shown, with no chrome
	/// asking to be clicked.
	const COLLAPSED_LINES = 14;
	let expanded = $state(false);
	let body = $state<HTMLElement | null>(null);

	/// Whether the value is actually taller than the box, MEASURED
	/// rather than counted from newlines: one 5000-character line has no
	/// newlines at all and still wraps well past the collapsed height,
	/// and counting would have left that value with no way to open it.
	let clipped = $state(false);
	$effect(() => {
		// Re-measure when the value or the collapsed state changes, and
		// whenever the box changes width: the same text wraps to more
		// lines in a narrower modal, so a value that fit can stop
		// fitting without changing at all.
		void text;
		void expanded;
		const el = body;
		if (!el) return;
		const measure = () => {
			clipped = el.scrollHeight > el.clientHeight + 1;
		};
		measure();
		const observer = new ResizeObserver(measure);
		observer.observe(el);
		return () => observer.disconnect();
	});
	/// Once open, the control stays so it can close again.
	const showToggle = $derived(clipped || expanded);

	/// Double-click selects the WHOLE value, the way a cell does in a
	/// database client: with no word boundaries in a blob of JSON, the
	/// browser's default double-click selection is never what someone
	/// reaching for the value wants.
	function selectAll(event: MouseEvent) {
		if (!body) return;
		event.preventDefault();
		const range = document.createRange();
		range.selectNodeContents(body);
		const selection = window.getSelection();
		selection?.removeAllRanges();
		selection?.addRange(range);
	}
</script>

<div
	class="group/value rounded-md border border-zinc-200 bg-white overflow-hidden
	       transition-colors hover:border-zinc-300 {closed ? 'opacity-60' : ''}"
>
	<div class="flex items-center gap-2 px-2.5 py-1.5 bg-zinc-50/80 border-b border-zinc-200">
		<span class="text-[11px] font-medium text-zinc-700 truncate">{label}</span>
		<span class="text-[10px] text-zinc-400 shrink-0">{closed ? 'closed' : summary}</span>
		{#if note}
			<span class="text-[10px] text-sky-700 truncate">· {note}</span>
		{/if}
		<div class="flex-1"></div>
		{#if closed}
			<!-- Nothing to expand or copy. -->
		{:else}
		{#if showToggle}
			<button
				type="button"
				class="flex items-center gap-0.5 rounded px-1 py-0.5 text-[10px] text-zinc-500
				       hover:bg-zinc-200/70 hover:text-zinc-700 transition-colors shrink-0"
				onclick={() => (expanded = !expanded)}
				title={expanded ? 'Collapse this value' : 'Show the whole value'}
			>
				{#if expanded}
					<ChevronDown class="w-3 h-3" />Collapse
				{:else}
					<ChevronRight class="w-3 h-3" />Expand
				{/if}
			</button>
		{/if}
		<div class="shrink-0 opacity-60 group-hover/value:opacity-100 transition-opacity">
			<CopyButton {text} />
		</div>
		{/if}
	</div>
	{#if closed}
		<div class="px-2.5 py-2 text-[11px] italic text-zinc-400">nothing arrived on this port</div>
	{:else}
	<!-- svelte-ignore a11y_no_static_element_interactions -->
	<pre
		bind:this={body}
		ondblclick={selectAll}
		class="m-0 px-2.5 py-2 font-mono text-[11px] leading-relaxed text-zinc-800
		       whitespace-pre-wrap break-words overflow-auto select-text cursor-text"
		style="max-height: {expanded ? '60vh' : `${COLLAPSED_LINES * 1.625}em`};"
		title="Double-click to select the whole value"
	>{#if plain}{text}{:else}{#each tokens as token}<span class={tokenClass(token.kind)}>{token.text}</span>{/each}{/if}</pre>
	{/if}
</div>
