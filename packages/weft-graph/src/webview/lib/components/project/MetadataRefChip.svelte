<script lang="ts">
	/// One named metadata port, as a small inline link in the surrounding
	/// text's font: clicking it takes the person to the node or group it
	/// belongs to (`onReveal` with its scoped id). A span rather than a
	/// button, so a long name wraps with the text around it the way a
	/// button, drawn as one block, does not.
	import type { MetadataRef } from '../../types';

	let { ref, label, spread = false, onReveal }: {
		ref: MetadataRef;
		/// How the name reads where it is written (`metadataRefLabel`).
		label: string;
		/// Written `...name` inside a list: its items are put in.
		spread?: boolean;
		onReveal?: (id: string) => void;
	} = $props();

	function reveal(e: Event) {
		e.stopPropagation();
		onReveal?.(ref.source);
	}
</script>

{#if spread}<span>...</span>{/if}{#if onReveal}<span
		role="link"
		tabindex="0"
		class="text-sky-700 hover:underline cursor-pointer nodrag nopan"
		title={`Go to ${ref.source}`}
		onclick={reveal}
		onkeydown={(e) => { if (e.key === 'Enter' || e.key === ' ') { e.preventDefault(); reveal(e); } }}
	>{label}</span>{:else}<span title={ref.source}>{label}</span>{/if}
