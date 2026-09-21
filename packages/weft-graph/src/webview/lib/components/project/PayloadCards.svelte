<script lang="ts">
	/// One execution payload, drawn as one card per port.
	///
	/// The execution inspector and the Debug-style node preview show the
	/// SAME values, so they share this component: a dict of ports becomes
	/// a card per key, a file value becomes a `FileCard`, anything else
	/// becomes a `ValueCard` (plain text for a string, coloured JSON for
	/// a structure). Before this existed the node rendered the whole
	/// payload as one `JSON.stringify` blob, so a string arrived with its
	/// newlines spelled `\n` and every port ran together.
	import { parseFileValue } from '../../../../protocol';
	import ValueCard from './ValueCard.svelte';
	import FileCard from './FileCard.svelte';

	let { payload, closedPorts = [], noteFor = undefined, empty = 'No data' }: {
		/// The payload: a dict of port name to value in the normal case,
		/// a bare value for a node whose payload is not keyed.
		payload: unknown;
		/// Ports that closed without a value. Each gets a greyed card
		/// saying so, after the ports that did carry one.
		closedPorts?: string[];
		/// Where a port's value came from when it did not travel a wire
		/// in this run (a backup, a supplied output, the seed run).
		noteFor?: (port: string) => string | undefined;
		/// What to say when the payload carries nothing at all.
		empty?: string;
	} = $props();

	/// The port entries, or null when the payload is a bare value (a
	/// string, a number) rather than a dict of ports. An empty dict
	/// carries nothing, so it reads as "nothing arrived" rather than as
	/// a card showing `{}`.
	const entries = $derived.by<Array<[string, unknown]> | null>(() => {
		if (typeof payload !== 'object' || payload === null) return null;
		return Object.entries(payload as Record<string, unknown>);
	});
	const bare = $derived(entries === null && payload !== null && payload !== undefined);
	const nothing = $derived(!bare && (entries?.length ?? 0) === 0);
</script>

<div class="space-y-2">
	{#if entries !== null}
		{#each entries as [key, value]}
			{@const file = parseFileValue(value)}
			{@const note = noteFor?.(key)}
			{#if file}
				<FileCard label={key} {file} {note} />
			{:else}
				<ValueCard label={key} {value} {note} />
			{/if}
		{/each}
	{:else if bare}
		<ValueCard label="value" value={payload} />
	{/if}
	{#if nothing && closedPorts.length === 0}
		<div class="px-1 py-2 text-xs text-zinc-400 italic">{empty}</div>
	{/if}
	{#each closedPorts as port}
		<ValueCard label={port} closed />
	{/each}
</div>
