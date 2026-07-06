<script lang="ts">
	/// Full-screen modal that renders an ActionBarError in detail. Opened
	/// when the user clicks the small red banner under the action bar.
	/// Dismissing via Escape / click-outside / the X just closes the
	/// modal; the banner stays. The "Dismiss error" footer button is the
	/// only path that clears the banner too (mirrors the X on the banner).

	import { CircleAlert, Copy, Check, X as XIcon } from '@lucide/svelte';
	import * as Dialog from '../ui/dialog';
	import type { ActionBarError } from '../../../../shared/protocol';

	let {
		error,
		open = $bindable(),
		onDismissError,
	}: {
		error: ActionBarError | undefined;
		open: boolean;
		onDismissError: () => void;
	} = $props();

	const details = $derived(error?.details);
	const diagnostics = $derived(details?.diagnostics ?? []);
	const headline = $derived(error?.message ?? '');
	const subtitle = $derived(details?.what ?? '');
	const stage = $derived(details?.stage);
	const command = $derived(details?.command);
	const exitCode = $derived(details?.exitCode);
	const raw = $derived(details?.raw);

	const fullCopyText = $derived.by(() => {
		if (!error) return '';
		const lines: string[] = [];
		lines.push(`[${(error.verb ?? '?').toString().toUpperCase()}] ${headline}`);
		if (subtitle) lines.push(subtitle);
		if (stage) lines.push(`Stage: ${stage}`);
		if (command) lines.push(`Command: ${command}`);
		if (exitCode !== undefined) lines.push(`Exit code: ${exitCode}`);
		if (diagnostics.length > 0) {
			lines.push('');
			lines.push('Diagnostics:');
			for (const d of diagnostics) {
				const loc = d.location ? ` ${d.location.file}:${d.location.line}:${d.location.column}` : '';
				const code = d.code ? ` [${d.code}]` : '';
				lines.push(`  - ${d.severity.toUpperCase()}${code}${loc}: ${d.message}`);
				if (d.hint) lines.push(`      hint: ${d.hint}`);
			}
		}
		if (raw) {
			lines.push('');
			lines.push('Raw output:');
			lines.push(raw);
		}
		return lines.join('\n');
	});

	let copied = $state(false);
	let copyTimer: ReturnType<typeof setTimeout> | null = null;
	function copyAll() {
		if (!fullCopyText) return;
		void navigator.clipboard.writeText(fullCopyText).then(() => {
			copied = true;
			if (copyTimer) clearTimeout(copyTimer);
			copyTimer = setTimeout(() => { copied = false; }, 1500);
		});
	}
	// Clear any pending copy-state timer on destroy so a modal closed
	// within the 1.5s window doesn't fire a setState on a dead
	// reactivity graph.
	$effect(() => () => {
		if (copyTimer) clearTimeout(copyTimer);
	});

	function dismissAndClose() {
		onDismissError();
		open = false;
	}

	function severityColor(sev: 'error' | 'warning' | 'info'): string {
		switch (sev) {
			case 'error': return 'text-red-600';
			case 'warning': return 'text-amber-600';
			case 'info': return 'text-blue-600';
		}
	}

	function severityChip(sev: 'error' | 'warning' | 'info'): string {
		switch (sev) {
			case 'error': return 'bg-red-50 text-red-700 border-red-200';
			case 'warning': return 'bg-amber-50 text-amber-700 border-amber-200';
			case 'info': return 'bg-blue-50 text-blue-700 border-blue-200';
		}
	}
</script>

{#if error}
<Dialog.Root bind:open>
	<Dialog.Content class="sm:max-w-[80vw] max-h-[85vh] overflow-hidden p-0 gap-0 [&>button:last-child]:hidden nodrag nopan flex flex-col">
		<div class="flex items-start justify-between px-5 py-4 border-b border-zinc-200 shrink-0">
			<div class="flex items-start gap-3 min-w-0">
				<CircleAlert class="w-5 h-5 text-red-500 shrink-0 mt-0.5" />
				<div class="min-w-0">
					<div class="text-[10px] font-medium uppercase tracking-wider text-red-600">
						{(error.verb ?? 'error').toString()} failed
					</div>
					<div class="text-sm font-semibold text-zinc-900 truncate">{headline}</div>
					{#if subtitle}
						<div class="text-xs text-zinc-500 mt-0.5 truncate">{subtitle}</div>
					{/if}
				</div>
			</div>
			<div class="flex items-center gap-2 shrink-0 ml-3">
				<button
					type="button"
					class="flex items-center gap-1.5 text-[11px] px-2 py-1 rounded border border-zinc-200 hover:bg-zinc-50 text-zinc-600"
					onclick={copyAll}
					title="Copy full error to clipboard"
				>
					{#if copied}
						<Check class="w-3 h-3 text-emerald-600" />
						Copied
					{:else}
						<Copy class="w-3 h-3" />
						Copy
					{/if}
				</button>
				<button
					type="button"
					class="w-6 h-6 flex items-center justify-center rounded hover:bg-zinc-100 text-zinc-400 hover:text-zinc-700"
					onclick={() => (open = false)}
					title="Close (banner stays)"
				>
					<XIcon class="w-4 h-4" />
				</button>
			</div>
		</div>

		<div class="flex items-center gap-2 px-5 py-2 bg-zinc-50 border-b border-zinc-200 text-[11px] text-zinc-500 shrink-0">
			{#if stage}
				<span class="px-1.5 py-0.5 rounded bg-white border border-zinc-200 font-medium text-zinc-700">{stage}</span>
			{/if}
			{#if exitCode !== undefined}
				<span class="font-mono">exit {exitCode}</span>
			{/if}
			{#if command}
				<span class="font-mono truncate">{command}</span>
			{/if}
		</div>

		<div class="flex-1 overflow-auto">
			{#if diagnostics.length > 0}
				<div class="px-5 py-3 border-b border-zinc-200">
					<div class="text-[10px] font-medium text-zinc-400 uppercase tracking-wider mb-2">
						{diagnostics.length} {diagnostics.length === 1 ? 'diagnostic' : 'diagnostics'}
					</div>
					<div class="space-y-2">
						{#each diagnostics as d}
							<div class="border border-zinc-200 rounded bg-white p-2.5">
								<div class="flex items-center gap-2 flex-wrap">
									<span class="text-[10px] uppercase font-semibold px-1.5 py-0.5 rounded border {severityChip(d.severity)}">
										{d.severity}
									</span>
									{#if d.code}
										<span class="text-[10px] font-mono px-1.5 py-0.5 rounded bg-zinc-100 text-zinc-600">{d.code}</span>
									{/if}
									{#if d.location}
										<span class="text-[10px] font-mono text-zinc-500">
											{d.location.file}:{d.location.line}:{d.location.column}
										</span>
									{/if}
								</div>
								<div class="mt-1.5 text-xs {severityColor(d.severity)} break-words">{d.message}</div>
								{#if d.hint}
									<div class="mt-1 text-[11px] text-zinc-500 italic break-words">{d.hint}</div>
								{/if}
							</div>
						{/each}
					</div>
				</div>
			{:else if !raw}
				<div class="px-5 py-6 text-xs text-zinc-500 italic">
					No further details available for this error.
				</div>
			{/if}

			{#if raw}
				<div class="px-5 py-3">
					<div class="text-[10px] font-medium text-zinc-400 uppercase tracking-wider mb-2">Raw output</div>
					<pre class="text-[11px] font-mono text-zinc-700 bg-zinc-50 border border-zinc-200 rounded p-2.5 whitespace-pre-wrap break-words max-h-[40vh] overflow-auto">{raw}</pre>
				</div>
			{/if}
		</div>

		<div class="flex items-center justify-end gap-2 px-5 py-3 border-t border-zinc-200 bg-zinc-50 shrink-0">
			<button
				type="button"
				class="text-xs px-3 py-1.5 rounded border border-zinc-200 hover:bg-white text-zinc-700"
				onclick={() => (open = false)}
			>
				Close
			</button>
			<button
				type="button"
				class="text-xs px-3 py-1.5 rounded bg-red-600 hover:bg-red-700 text-white"
				onclick={dismissAndClose}
			>
				Dismiss error
			</button>
		</div>
	</Dialog.Content>
</Dialog.Root>
{/if}
