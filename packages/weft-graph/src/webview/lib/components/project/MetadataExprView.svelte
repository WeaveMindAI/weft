<script lang="ts">
	/// A gathered metadata value as its group's body writes it, read-only:
	/// compact weft-like text, in the font of the box it sits in, where each
	/// named port is a link
	/// (`metadataExprSegments`), and each value written out inline is compact
	/// JSON, shortened when long until clicked.
	import type { MetadataExpr } from '../../../../protocol';
	import { SHORT_LITERAL, metadataExprSegments } from '../../utils/metadata-expr';
	import MetadataRefChip from './MetadataRefChip.svelte';

	let { expr, scope, onReveal }: {
		expr: MetadataExpr;
		/// The id of the group whose body writes it: names read relative to it.
		scope: string | undefined;
		onReveal?: (id: string) => void;
	} = $props();

	const segments = $derived(metadataExprSegments(expr, scope));
	/// The inline values opened in full, by segment index.
	let opened = $state<Set<number>>(new Set());
</script>

<!-- Inherits the font of the box it sits in, so it reads like that field.
     The segments sit with no white space of the template's between them,
     so only the expression's own spaces show; it wraps at those, and a
     long name anywhere it has to. -->
<div class="whitespace-normal [overflow-wrap:anywhere]">{#each segments as segment, i}{#if segment.kind === 'text'}<span>{segment.text}</span>{:else if segment.kind === 'ref'}<MetadataRefChip ref={segment.ref} label={segment.label} spread={segment.spread} {onReveal} />{:else if segment.text.length > SHORT_LITERAL && !opened.has(i)}<button
			type="button"
			class="text-zinc-500 hover:text-zinc-800 nodrag nopan"
			title="Show the whole value"
			onclick={(e) => { e.stopPropagation(); opened = new Set([...opened, i]); }}
		>{segment.text.slice(0, SHORT_LITERAL)}...</button>{:else}<span class="text-zinc-500">{segment.text}</span>{/if}{/each}</div>
