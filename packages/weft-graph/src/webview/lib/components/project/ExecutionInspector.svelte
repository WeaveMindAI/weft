<script lang="ts">
	import { Search } from '@lucide/svelte';
	import type { NodeExecution } from '../../types';
	import type { BusInspectorEvent, BusMeta, CorruptionSite, LoopInspectorEvent, LoopIteration } from '../../../../shared/protocol';
	import { parseFileValue } from '../../../../shared/protocol';
	import { displayStatus, getStatusIcon } from '../../utils/status';
	import JsonTree from './JsonTree.svelte';
	import FileCard from './FileCard.svelte';
	import CopyButton from '../ui/CopyButton.svelte';
	import * as Dialog from '../ui/dialog';

	let {
		executions = [],
		busLogs = [],
		journalCorruptions = [],
		loopEvents = [],
		label = 'Node',
	}: {
		executions: NodeExecution[];
		busLogs?: Array<{ busId: string; events: BusInspectorEvent[]; meta?: BusMeta }>;
		journalCorruptions?: Array<{ site: CorruptionSite; reason: string }>;
		/// Loop lifecycle events for the loop group this inspector
		/// belongs to. Empty for non-loop nodes (ordinary groups and
		/// catalog nodes). The inspector renders a per-instance card
		/// keyed by `(groupId, parentFrames)` so nested loops and
		/// parallel sibling iterations each get their own section.
		loopEvents?: LoopInspectorEvent[];
		label: string;
	} = $props();

	let corruptionsOpen = $state(false);

	/// Pretty-print a payload for the IRC `from: <text>` line. Strings
	/// render verbatim (the demo uses string payloads); other JSON
	/// values render as compact JSON. Wire payloads arrive as parsed
	/// JSON so `JSON.stringify` cannot fail on the data path; a circular
	/// reference would mean the wire was corrupted upstream, and the
	/// user wants to see that loud rather than have it stringified.
	function prettyPayload(value: unknown): string {
		if (typeof value === 'string') return value;
		return JSON.stringify(value);
	}

	function formatBusTime(atUnix: number): string {
		const d = new Date(atUnix * 1000);
		const hh = String(d.getHours()).padStart(2, '0');
		const mm = String(d.getMinutes()).padStart(2, '0');
		const ss = String(d.getSeconds()).padStart(2, '0');
		return `${hh}:${mm}:${ss}`;
	}

	/// Short human label for a bus id ("bus #" + first 4 chars of the
	/// uuid). The full id is just noise in the header; the first chars
	/// are enough to tell two buses apart in the rare case a node has
	/// more than one.
	function shortBusId(busId: string): string {
		return `bus ${busId.slice(0, 4)}`;
	}

	/// Short label that names the bus mode for the panel header. Empty
	/// string for journaled (the default; no badge clutter), `[ephemeral]`
	/// otherwise. Ephemeral panels also render messages with metadata
	/// only (size + 8-byte SHA-256 prefix), so the header tag tells the
	/// user up front why they see hashes instead of payloads.
	function modeBadge(meta: BusMeta | undefined): string {
		if (!meta) return '';
		return meta.ephemeral ? ' [ephemeral]' : '';
	}

	/// Format one message-kind line for either the on-screen IRC panel
	/// or the copy-text. `payload.kind === 'ephemeral'` means the
	/// journal never carried the bytes; render its size and 8-byte
	/// hash prefix so replay is honest about what flew. `payload.kind
	/// === 'journaled'` carries the actual value (which may legitimately
	/// be JSON null on a journaled bus).
	function formatMessageBody(event: BusInspectorEvent & { kind: 'message' }): string {
		if (event.payload.kind === 'ephemeral') {
			return `sent ${event.msgKind} of ${event.payloadByteSize} bytes [hash: ${event.payloadSha256Prefix}]`;
		}
		return prettyPayload(event.payload.value);
	}

	/// Linearize every bus log into copy-friendly text. Output mirrors
	/// what the on-screen IRC panels render: one section per bus, each
	/// section headed by the short bus id (with mode badge), each line
	/// prefixed with the same `HH:MM:SS` stamp the panel shows. Returns
	/// `null` when there are no buses on this firing (so `fullCopyText`
	/// can skip the section header entirely).
	function formatBusLogsForCopy(
		logs: Array<{ busId: string; events: BusInspectorEvent[]; meta?: BusMeta }> | undefined,
	): string | null {
		if (!logs || logs.length === 0) return null;
		const sections = logs.map(({ busId, events, meta }) => {
			const header = `[${shortBusId(busId)}${modeBadge(meta)}]`;
			if (events.length === 0) return `${header}\n(no traffic yet)`;
			const lines = events.map((e) => {
				const ts = formatBusTime(e.atUnix);
				if (e.kind === 'joined') return `${ts}  * ${e.name} joined`;
				if (e.kind === 'left') return `${ts}  * ${e.name} left`;
				if (e.kind === 'closed') return `${ts}  * the bus closed here`;
				return `${ts}  ${e.from}: ${formatMessageBody(e)}`;
			});
			return [header, ...lines].join('\n');
		});
		return sections.join('\n\n');
	}

	/// Group loop events by their parent_frames stack so nested or
	/// parallel-sibling loop instances each render under their own
	/// card. Key is the JSON-stringified parentFrames; the events
	/// within preserve arrival order.
	function groupLoopEventsByInstance(
		events: LoopInspectorEvent[],
	): Array<{ key: string; parentFrames: LoopIteration[]; events: LoopInspectorEvent[] }> {
		const byKey = new Map<string, { parentFrames: LoopIteration[]; events: LoopInspectorEvent[] }>();
		for (const ev of events) {
			const key = JSON.stringify(ev.parentFrames);
			let bucket = byKey.get(key);
			if (!bucket) {
				bucket = { parentFrames: ev.parentFrames, events: [] };
				byKey.set(key, bucket);
			}
			bucket.events.push(ev);
		}
		return Array.from(byKey.entries()).map(([key, v]) => ({ key, ...v }));
	}

	function parentFramesLabel(frames: LoopIteration[]): string {
		if (!frames || frames.length === 0) return 'root';
		return frames.map((f) => `#${f.index}`).join(' / ');
	}

	function loopEventLine(ev: LoopInspectorEvent): string {
		// Exhaustive switch over the closed union: a new Rust-side
		// variant added to LoopInspectorEvent without a matching case
		// here is a TS compile error, not a silent blank line.
		switch (ev.kind) {
			case 'instantiated':
				return `instantiated (${ev.parallel ? 'parallel' : 'sequential'}, ${ev.iterCount} iterations planned)`;
			case 'iteration_launched':
				return `iter ${ev.index} launched`;
			case 'out_fired': {
				const v = ev.doneVote;
				const tail = v === true ? ' (done=true)' : '';
				return `iter ${ev.index} completed${tail}`;
			}
			case 'terminated':
				return `terminated: ${ev.reason}`;
		}
	}

	let selectedIndex = $state(0);
	let open = $state(false);

	// Auto-follow the newest execution ONLY while the user is pinned to the latest.
	// The old effect force-snapped selectedIndex to count-1 on EVERY new execution,
	// which yanked the user back to the newest mid-read whenever a run streamed in.
	// `pinnedToLatest` stays true while they're viewing the last item (so a new one
	// follows) and flips false the moment they navigate to an older one (so their
	// place is held); navigating back to the last re-pins. Same pinned/latest model
	// the GraphToolbar follow control uses.
	let pinnedToLatest = $state(true);
	const count = $derived(executions.length);
	$effect(() => {
		// Clamp UNCONDITIONALLY, not only when pinned: if `executions` shrinks
		// (a reload resetting the list) while the user is viewing an older one
		// unpinned, `selectedIndex` could point past the end and `selected`
		// would go undefined. Snap to the latest when pinned; otherwise just
		// keep the index inside range.
		if (count === 0) return;
		if (pinnedToLatest) selectedIndex = count - 1;
		else if (selectedIndex > count - 1) selectedIndex = count - 1;
	});
	function goToIndex(i: number): void {
		selectedIndex = i;
		pinnedToLatest = i >= count - 1;
	}
	const selected = $derived(executions[selectedIndex]);

	function formatDuration(startMs: number, endMs?: number): string {
		if (!endMs) return 'running...';
		const ms = endMs - startMs;
		if (ms < 1000) return `${ms}ms`;
		if (ms < 60000) return `${(ms / 1000).toFixed(1)}s`;
		return `${Math.floor(ms / 60000)}m ${Math.round((ms % 60000) / 1000)}s`;
	}

	function formatCost(usd: number): string {
		if (usd === 0) return '$0';
		if (usd < 0.001) return `$${usd.toFixed(6)}`;
		if (usd < 0.01) return `$${usd.toFixed(4)}`;
		return `$${usd.toFixed(2)}`;
	}

	/// The firing's cost line, honest about unresolved records: a firing
	/// with only an unknown cost says "unknown" (never looks free), one
	/// mixing resolved and unknown records says both, and the line names
	/// whose key spent. Empty = no cost.
	function costLabel(firing: {
		costUsd: number;
		costUnknown?: boolean;
		costOrigin?: 'user-provided' | 'runtime' | 'mixed';
	}): string {
		let amount: string;
		if (firing.costUnknown) {
			amount = firing.costUsd > 0 ? `${formatCost(firing.costUsd)} + unknown` : 'cost unknown';
		} else if (firing.costUsd > 0) {
			amount = formatCost(firing.costUsd);
		} else {
			return '';
		}
		const origin =
			firing.costOrigin === 'user-provided'
				? ' (own key)'
				: firing.costOrigin === 'runtime'
					? ' (platform key)'
					: firing.costOrigin === 'mixed'
						? ' (mixed keys)'
						: '';
		return amount + origin;
	}

	/// A short label for one firing so the user can correlate it across
	/// nodes when following a single pulse. The record's `framesKey` is
	/// the frame stack serialized as JSON. Renders
	/// e.g. `frames 5·2`; empty when the firing is at the root frame stack.
	function firingLabel(framesKey: string | undefined): string {
		if (!framesKey) return '';
		// framesKey is JSON produced by the engine; a parse failure
		// means the wire shape drifted, which the user wants to see.
		const frames = JSON.parse(framesKey) as LoopIteration[];
		if (frames.length === 0) return '';
		return 'iter ' + frames.map((f) => `${f.index}`).join('/');
	}
</script>

<!-- Inline: navigator + inspect button. Render inside the node header. -->
{#if count > 1}
	{@const selExec = executions[selectedIndex]}
	{@const statusColor = selExec?.status === 'failed' ? 'text-red-500' : selExec?.status === 'completed' ? 'text-green-600' : selExec?.status === 'running' ? 'text-blue-500' : 'text-muted-foreground'}
	{@const firing = firingLabel(selExec?.framesKey)}
	<div class="inline-flex items-center gap-0.5 ml-1.5 text-[9px] select-none {statusColor}" title={firing}>
		<button class="px-0.5 hover:text-foreground disabled:opacity-30 transition-colors" disabled={selectedIndex === 0} onclick={(e) => { e.stopPropagation(); goToIndex(Math.max(0, selectedIndex - 1)); }}>‹</button>
		<span class="font-mono tabular-nums">{selectedIndex + 1}/{count}</span>
		{#if firing}<span class="font-mono opacity-70">{firing}</span>{/if}
		<button class="px-0.5 hover:text-foreground disabled:opacity-30 transition-colors" disabled={selectedIndex >= count - 1} onclick={(e) => { e.stopPropagation(); goToIndex(Math.min(count - 1, selectedIndex + 1)); }}>›</button>
	</div>
{/if}
{#if count > 0}
	<button
		class="w-5 h-5 flex items-center justify-center rounded hover:bg-black/5 cursor-pointer transition-colors text-zinc-400 nodrag"
		onclick={(e) => { e.stopPropagation(); open = true; }}
		title="Inspect execution"
	>
		<Search class="w-3 h-3" />
	</button>
{/if}

<!-- Modal -->
{#if selected}
{@const inputJson = (selected.input !== null && selected.input !== undefined) ? JSON.stringify(selected.input, null, 2) : null}
{@const outputJson = (selected.output !== null && selected.output !== undefined) ? JSON.stringify(selected.output, null, 2) : null}
{@const detailsText = selected.error ?? (selected.status === 'completed' ? 'Completed successfully' : displayStatus(selected.status))}
{@const closedPortsText = (selected.closedPorts && selected.closedPorts.length > 0)
	? selected.closedPorts.map((p) => `${p}: (closed)`).join('\n')
	: null}
{@const inputSection = [inputJson, closedPortsText].filter(Boolean).join('\n') || '(none)'}
{@const busSection = formatBusLogsForCopy(busLogs)}
{@const fullCopyText = [
	`--- Input ---`, inputSection, ``,
	`--- Details ---`, detailsText, ``,
	`--- Output ---`, outputJson ?? '(none)', ``,
	...(busSection ? [`--- Bus Communication ---`, busSection, ``] : []),
	`Status: ${selected.status} | Duration: ${formatDuration(selected.startedAt, selected.completedAt)}${costLabel(selected) ? ` | Cost: ${costLabel(selected)}` : ''} | ${new Date(selected.startedAt).toLocaleString()} | ${selected.id}`,
].join('\n')}
<Dialog.Root bind:open>
	<Dialog.Content class="sm:max-w-[92vw] max-h-[85vh] overflow-hidden p-0 gap-0 [&>button:last-child]:hidden nodrag nopan flex flex-col">
		<div class="flex items-center justify-between px-4 py-2.5 border-b border-zinc-200 shrink-0">
			<div class="flex items-center gap-3">
				<span class="{selected.status === 'failed' ? 'text-red-600' : selected.status === 'completed' ? 'text-green-600' : 'text-zinc-500'}">{getStatusIcon(selected.status)}</span>
				<span class="text-sm font-semibold text-zinc-800">{label}</span>
				{#if count > 1}
					<div class="flex items-center gap-1 text-xs text-zinc-500">
						<button class="px-1 hover:text-zinc-800 disabled:opacity-30" disabled={selectedIndex === 0} onclick={() => { goToIndex(Math.max(0, selectedIndex - 1)); }}>‹</button>
						<span class="font-mono tabular-nums">{selectedIndex + 1}/{count}</span>
						<button class="px-1 hover:text-zinc-800 disabled:opacity-30" disabled={selectedIndex >= count - 1} onclick={() => { goToIndex(Math.min(count - 1, selectedIndex + 1)); }}>›</button>
						{#if firingLabel(selected?.framesKey)}
							<span class="ml-1 font-mono text-zinc-400">{firingLabel(selected?.framesKey)}</span>
						{/if}
					</div>
				{/if}
			</div>
			<div class="flex items-center gap-2">
				<CopyButton text={fullCopyText} />
				<button
					class="w-6 h-6 flex items-center justify-center rounded hover:bg-zinc-100 text-zinc-400 hover:text-zinc-700 transition-colors"
					onclick={() => open = false}
				>✕</button>
			</div>
		</div>

		<div class="grid grid-cols-3 min-h-0 overflow-hidden flex-1">
			<div class="flex flex-col min-h-0 border-r border-zinc-200">
				<div class="flex items-center justify-between px-3 py-1.5 bg-zinc-50 border-b border-zinc-200 shrink-0">
					<span class="text-[10px] font-medium text-zinc-400 uppercase tracking-wider">Input</span>
					{#if inputJson}
						<CopyButton text={inputJson} />
					{/if}
				</div>
				<div class="overflow-auto flex-1 p-2">
					{#if selected.input && typeof selected.input === 'object' && Object.keys(selected.input as Record<string, unknown>).length > 0}
						{#each Object.entries(selected.input as Record<string, unknown>) as [key, value]}
							{@const file = parseFileValue(value)}
							{#if file}
								<FileCard label={key} {file} />
							{:else}
								<JsonTree data={value} label={key} defaultExpanded={true} />
							{/if}
						{/each}
					{:else if selected.input !== null && selected.input !== undefined && typeof selected.input !== 'object'}
						<div class="p-1 text-[11px] font-mono text-zinc-700">{JSON.stringify(selected.input)}</div>
					{:else if !selected.closedPorts || selected.closedPorts.length === 0}
						<div class="p-1 text-xs text-zinc-400 italic">No input data</div>
					{/if}
					{#if selected.closedPorts && selected.closedPorts.length > 0}
						{#each selected.closedPorts as port}
							<div class="p-1 text-[11px] font-mono text-zinc-500 italic">
								<span class="text-zinc-400">{port}:</span> (closed)
							</div>
						{/each}
					{/if}
				</div>
			</div>

			<div class="flex flex-col min-h-0 border-r border-zinc-200">
				<div class="flex items-center justify-between px-3 py-1.5 bg-zinc-50 border-b border-zinc-200 shrink-0">
					<span class="text-[10px] font-medium text-zinc-400 uppercase tracking-wider">Details</span>
					<CopyButton text={detailsText} />
				</div>
				<div class="overflow-auto flex-1 p-3 space-y-3">
					{#if selected.portWarnings && selected.portWarnings.length > 0}
						<div class="rounded border border-amber-200 bg-amber-50 p-2.5">
							<div class="text-[10px] font-semibold text-amber-700 mb-1">Output type mismatch</div>
							{#each selected.portWarnings as w}
								<div class="text-[11px] text-amber-700 font-mono break-words">
									Port <span class="font-semibold">{w.port}</span> expected <span class="font-semibold">{w.expected}</span> but got <span class="font-semibold">{w.actual}</span>; the value was not sent and the port was closed.
								</div>
							{/each}
						</div>
					{/if}
					{#if selected.error}
						<div class="rounded border border-red-200 bg-red-50 p-2.5">
							<div class="text-[10px] font-semibold text-red-700 mb-1">Error</div>
							<pre class="text-[11px] text-red-600 whitespace-pre-wrap break-words font-mono">{selected.error}</pre>
						</div>
					{:else if selected.status === 'completed'}
						<div class="text-[11px] text-green-600">Completed successfully</div>
					{:else if selected.status === 'running' || selected.status === 'waiting_for_input'}
						<div class="text-[11px] text-blue-600 animate-pulse">
							{selected.status === 'waiting_for_input' ? 'Waiting for input...' : 'Running...'}
						</div>
					{:else if selected.status === 'skipped'}
						<div class="text-[11px] text-zinc-500">Skipped</div>
					{:else}
						<div class="text-[11px] text-zinc-500">{displayStatus(selected.status)}</div>
					{/if}
				</div>
			</div>

			<div class="flex flex-col min-h-0">
				<div class="flex items-center justify-between px-3 py-1.5 bg-zinc-50 border-b border-zinc-200 shrink-0">
					<span class="text-[10px] font-medium text-zinc-400 uppercase tracking-wider">Output</span>
					{#if outputJson}
						<CopyButton text={outputJson} />
					{/if}
				</div>
				<div class="overflow-auto flex-1 p-2">
					{#if selected.output && typeof selected.output === 'object' && Object.keys(selected.output as Record<string, unknown>).length > 0}
						{#each Object.entries(selected.output as Record<string, unknown>) as [key, value]}
							{@const file = parseFileValue(value)}
							{#if file}
								<FileCard label={key} {file} />
							{:else}
								<JsonTree data={value} label={key} defaultExpanded={true} />
							{/if}
						{/each}
					{:else if selected.output !== null && selected.output !== undefined}
						<div class="p-1 text-[11px] font-mono text-zinc-700">{JSON.stringify(selected.output)}</div>
					{:else}
						<div class="p-1 text-xs text-zinc-400 italic">No output</div>
					{/if}
				</div>
			</div>
		</div>

		<!-- Bus communication section. One scrollable IRC-style panel
		     per bus this node participated in. Hidden entirely when
		     the node has touched no bus. Works on replay because the
		     events come from the journal-projected `DispatcherEvent`
		     stream the host feeds to the webview either way. -->
		{#if busLogs.length > 0}
			<div class="border-t border-zinc-200 bg-zinc-50/50 shrink-0">
				<div class="px-4 py-1.5 text-[10px] font-medium text-zinc-400 uppercase tracking-wider">
					Bus Communication
				</div>
				<div class="grid gap-2 px-3 pb-3" style="grid-template-columns: repeat({Math.min(busLogs.length, 2)}, minmax(0, 1fr));">
					{#each busLogs as { busId, events, meta } (busId)}
						<div class="flex flex-col border border-zinc-200 rounded bg-white overflow-hidden">
							<div class="flex items-center justify-between px-2 py-1 border-b border-zinc-200 bg-zinc-50">
								<span class="text-[10px] font-mono text-zinc-500">{shortBusId(busId)}{modeBadge(meta)}</span>
								<span class="text-[9px] text-zinc-400">{events.length} {events.length === 1 ? 'line' : 'lines'}</span>
							</div>
							<div class="overflow-auto font-mono text-[11px] leading-tight px-2 py-1 max-h-40 min-h-16">
								{#if events.length === 0}
									<div class="text-zinc-400 italic">(no traffic yet)</div>
								{:else}
									{#each events as e, idx (idx)}
										<div class="flex items-baseline gap-1.5">
											<span class="text-[9px] text-zinc-300 tabular-nums shrink-0">{formatBusTime(e.atUnix)}</span>
											{#if e.kind === 'joined'}
												<span class="text-emerald-600">* {e.name} joined</span>
											{:else if e.kind === 'left'}
												<span class="text-zinc-400">* {e.name} left</span>
											{:else if e.kind === 'closed'}
												<span class="text-zinc-400 italic">* the bus closed here</span>
											{:else}
												<span class="text-blue-700 shrink-0">{e.from}:</span>
												<span class="text-zinc-800 break-words">{formatMessageBody(e)}</span>
											{/if}
										</div>
									{/each}
								{/if}
							</div>
						</div>
					{/each}
				</div>
			</div>
		{/if}

		{#if loopEvents.length > 0}
			{@const loopInstances = groupLoopEventsByInstance(loopEvents)}
			<div class="border-t border-zinc-200 bg-zinc-50/50 shrink-0">
				<div class="px-4 py-1.5 text-[10px] font-medium text-zinc-400 uppercase tracking-wider">
					Loop Activity
				</div>
				<div class="grid gap-2 px-3 pb-3" style="grid-template-columns: repeat({Math.min(loopInstances.length, 2)}, minmax(0, 1fr));">
					{#each loopInstances as inst (inst.key)}
						<div class="flex flex-col border border-zinc-200 rounded bg-white overflow-hidden">
							<div class="flex items-center justify-between px-2 py-1 border-b border-zinc-200 bg-zinc-50">
								<span class="text-[10px] font-mono text-purple-600">{parentFramesLabel(inst.parentFrames)}</span>
								<span class="text-[9px] text-zinc-400">{inst.events.length} {inst.events.length === 1 ? 'event' : 'events'}</span>
							</div>
							<div class="overflow-auto font-mono text-[11px] leading-tight px-2 py-1 max-h-40 min-h-16">
								{#each inst.events as ev, idx (idx)}
									<div class="flex items-baseline gap-1.5">
										<span class="text-zinc-800 break-words">{loopEventLine(ev)}</span>
									</div>
								{/each}
							</div>
						</div>
					{/each}
				</div>
			</div>
		{/if}

		{#if journalCorruptions.length > 0}
			<div class="px-4 py-1.5 border-t border-zinc-200 bg-zinc-50 text-[10px] text-zinc-500 shrink-0">
				<button
					type="button"
					class="flex items-center gap-1.5 text-zinc-500 hover:text-zinc-700 transition-colors"
					onclick={() => (corruptionsOpen = !corruptionsOpen)}
					aria-expanded={corruptionsOpen}
					aria-controls="journal-corruptions-list"
				>
					<span class="text-zinc-400">{corruptionsOpen ? '▾' : '▸'}</span>
					<span>{journalCorruptions.length} journal row{journalCorruptions.length === 1 ? '' : 's'} corrupted</span>
				</button>
				{#if corruptionsOpen}
					<div id="journal-corruptions-list" class="mt-1 pl-4 space-y-0.5 font-mono text-zinc-500">
						{#each journalCorruptions as c}
							<div><span class="text-zinc-400">{c.site}:</span> {c.reason}</div>
						{/each}
					</div>
				{/if}
			</div>
		{/if}

		<div class="flex items-center gap-4 px-4 py-1.5 border-t border-zinc-200 bg-zinc-50 text-[10px] text-zinc-500 shrink-0">
			<span class="font-medium {selected.status === 'failed' ? 'text-red-600' : selected.status === 'completed' ? 'text-green-600' : ''}">{selected.status}</span>
			<span class="font-mono">{formatDuration(selected.startedAt, selected.completedAt)}</span>
			{#if costLabel(selected)}
				<span class="font-mono">{costLabel(selected)}</span>
			{/if}
			<span>{new Date(selected.startedAt).toLocaleString()}</span>
		</div>
	</Dialog.Content>
</Dialog.Root>
{/if}
