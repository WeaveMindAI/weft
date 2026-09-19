<script lang="ts">
	/// The back-and-forth between a run and the caller that started it.
	///
	/// Why this sits under the toolbar rather than inside a node's
	/// inspector, where the bus log lives: a bus is a per-node fact (two
	/// nodes are on different buses), while a run has exactly ONE caller
	/// and the same exchange whichever node you open. A copy in every
	/// node's modal would say the same thing N times, each time about
	/// something other than the node in front of you.
	import { ArrowDown, ArrowUp, X } from '@lucide/svelte';
	import type { CallerInspectorEvent } from '../../../../protocol';
	import { formatClockTime, formatStreamBody } from '../../utils/stream-log';
	import CopyButton from '../ui/CopyButton.svelte';

	let {
		events,
		onClose,
	}: {
		events: CallerInspectorEvent[];
		onClose: () => void;
	} = $props();

	/// One line per thing that happened, in the order it happened.
	///
	/// A window row is not a thing that happened: it is the journal's
	/// way of writing a second's worth of messages as one row, so it is
	/// unpacked back into the messages it holds. What a reader wants is
	/// the conversation, not the shape it was stored in.
	interface Line {
		atUnix: number;
		/// 'in' the caller spoke, 'out' the program did, 'note' neither
		/// (connected, errored, hung up).
		side: 'in' | 'out' | 'note';
		text: string;
		/// The program's last word on this exchange.
		terminal?: boolean;
	}

	const lines = $derived<Line[]>(
		events.flatMap((event): Line[] => {
			switch (event.kind) {
				case 'connected':
					return [{ atUnix: event.atUnix, side: 'note', text: `the caller connected over ${event.protocol}` }];
				case 'errored':
					return [{ atUnix: event.atUnix, side: 'note', text: `something went wrong: ${event.message}` }];
				case 'disconnected':
					return [{ atUnix: event.atUnix, side: 'note', text: `the caller went away (${event.reason})` }];
				case 'window':
					return event.messages.map((m) => ({
						atUnix: m.atUnix,
						side: m.direction === 'inbound' ? 'in' : 'out',
						text: formatStreamBody(m.payload, m.payloadByteSize, m.trimmed, 'message'),
						terminal: m.terminal,
					}));
			}
		}),
	);

	/// The same conversation as plain text, for pasting into a message
	/// to somebody who cannot open this panel.
	const copyText = $derived(
		lines
			.map((l) => {
				const who = l.side === 'in' ? 'caller' : l.side === 'out' ? 'program' : '*';
				return `${formatClockTime(l.atUnix)}  ${who}: ${l.text}`;
			})
			.join('\n'),
	);
</script>

<div class="w-[32rem] max-w-[90vw] border border-zinc-200 rounded-md bg-white shadow-lg overflow-hidden">
	<div class="flex items-center justify-between px-2 py-1 border-b border-zinc-200 bg-zinc-50">
		<span class="text-[10px] font-medium text-zinc-500 uppercase tracking-wider">The caller</span>
		<div class="flex items-center gap-1">
			<span class="text-[9px] text-zinc-400">
				{lines.length}
				{lines.length === 1 ? 'line' : 'lines'}
			</span>
			<CopyButton text={copyText} label="Copy the exchange" />
			<button
				type="button"
				onclick={onClose}
				class="flex items-center justify-center w-5 h-5 rounded text-zinc-400 hover:text-zinc-700 hover:bg-zinc-100 transition"
				title="Close"
				aria-label="Close the caller panel"
			>
				<X class="w-3 h-3" />
			</button>
		</div>
	</div>
	<div class="overflow-auto font-mono text-[11px] leading-tight px-2 py-1 max-h-64 min-h-16">
		{#if lines.length === 0}
			<div class="text-zinc-400 italic">(nothing said yet)</div>
		{:else}
			{#each lines as line, idx (idx)}
				<div class="flex items-baseline gap-1.5">
					<span class="text-[9px] text-zinc-300 tabular-nums shrink-0">{formatClockTime(line.atUnix)}</span>
					{#if line.side === 'note'}
						<span class="text-zinc-400 italic">* {line.text}</span>
					{:else if line.side === 'in'}
						<ArrowDown class="w-3 h-3 text-blue-600 shrink-0" />
						<span class="text-zinc-800 break-words whitespace-pre-wrap">{line.text}</span>
					{:else}
						<ArrowUp class="w-3 h-3 text-emerald-600 shrink-0" />
						<span class="text-zinc-800 break-words whitespace-pre-wrap">{line.text}</span>
						{#if line.terminal}
							<span class="text-[9px] text-zinc-400 shrink-0">(the answer)</span>
						{/if}
					{/if}
				</div>
			{/each}
		{/if}
	</div>
</div>
