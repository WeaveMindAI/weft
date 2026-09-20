<script lang="ts">
	/// Full-screen modal for what the last action-bar verb did: the
	/// terminal output it printed, and the failure if it had one.
	///
	/// Opened by clicking either pill under the action bar. When a verb
	/// fails, the failure is on top and the terminal output sits under
	/// it, so the reason comes first and the trace is there when the
	/// reason is not enough. Copy takes the failure alone (that is what
	/// people paste into a chat); the output has its own copy button.
	///
	/// Dismissing via Escape / click-outside / the X just closes the
	/// modal; the pills stay. The "Dismiss error" footer button is the
	/// only path that clears them too (mirrors the X on the banner).

	import { CircleAlert, Copy, Check, Loader2, Terminal, X as XIcon } from '@lucide/svelte';
	import * as Dialog from '../ui/dialog';
	import type { ActionBarActivity, ActionBarError, SourceLocation } from '../../../../protocol';

	let {
		error,
		activity,
		running = false,
		open = $bindable(),
		onDismissError,
		onOpenLocation,
	}: {
		error: ActionBarError | undefined;
		/// What the verb printed to the terminal. Absent when it printed
		/// nothing, or when it finished cleanly.
		activity: ActionBarActivity | undefined;
		/// The verb is still going: the header spins and the output is
		/// still growing.
		running?: boolean;
		open: boolean;
		onDismissError: () => void;
		onOpenLocation: (location: SourceLocation) => void;
	} = $props();

	/// A location's `file` is a full path; the row shows just the file
	/// name (the full path rides the hover title and the copy text).
	function basename(p: string): string {
		return p.split('/').pop()?.split('\\').pop() ?? p;
	}

	/// A verb finishing cleanly drops its error and its output in one
	/// update. The dialog closes on that rather than staying open over
	/// an empty box for the frame before the bar's own effect lands.
	const hasSomethingToShow = $derived(!!(error || activity));
	const details = $derived(error?.details);
	const diagnostics = $derived(details?.diagnostics ?? []);
	const headline = $derived(error?.message ?? '');
	const subtitle = $derived(details?.what ?? '');
	const stage = $derived(details?.stage);
	const command = $derived(details?.command);
	const exitCode = $derived(details?.exitCode);
	const raw = $derived(details?.raw);
	const logText = $derived((activity?.lines ?? []).join('\n'));
	/// The verb this modal is about. The error's verb when there is one
	/// (it may be a system-side source like `parse`, which has no
	/// terminal output at all), otherwise the running verb's.
	const verb = $derived(error?.verb ?? activity?.verb);

	/// What the header's Copy button puts on the clipboard: the failure,
	/// and nothing else. A verb that only printed puts its output there
	/// instead, since that is then the whole story.
	const errorCopyText = $derived.by(() => {
		if (!error) return logText;
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

	/// Which copy button last flashed "Copied", and the timer that ends
	/// the flash. Two buttons sit in this modal (the failure at the top,
	/// the output below) and only the pressed one flashes.
	let copied = $state<'head' | 'log' | null>(null);
	let copyTimer: ReturnType<typeof setTimeout> | null = null;
	function copy(which: 'head' | 'log', text: string): void {
		if (!text) return;
		void navigator.clipboard.writeText(text).then(() => {
			copied = which;
			if (copyTimer) clearTimeout(copyTimer);
			copyTimer = setTimeout(() => { copied = null; }, 1500);
		});
	}
	// A modal closed inside the 1.5s window would otherwise set state on
	// a dead reactivity graph.
	$effect(() => () => { if (copyTimer) clearTimeout(copyTimer); });

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

<!-- Always mounted: when a verb finishes cleanly the store drops its
     error and its output in the same update, and a modal that
     disappeared instead of closing would leave the dialog's own undo
     (scroll lock, focus, aria-hidden on the rest of the app) to its
     destroy handler. The content renders nothing when there is nothing
     to show. -->
<Dialog.Root open={open && hasSomethingToShow} onOpenChange={(next) => (open = next)}>
	<Dialog.Content class="sm:max-w-[80vw] max-h-[85vh] overflow-hidden p-0 gap-0 [&>button:last-child]:hidden nodrag nopan flex flex-col">
		<div class="flex items-start justify-between px-5 py-4 border-b border-zinc-200 shrink-0">
			<div class="flex items-start gap-3 min-w-0">
				{#if error}
					<CircleAlert class="w-5 h-5 text-red-500 shrink-0 mt-0.5" />
				{:else if running}
					<Loader2 class="w-5 h-5 text-zinc-400 shrink-0 mt-0.5 animate-spin" />
				{:else}
					<Terminal class="w-5 h-5 text-zinc-400 shrink-0 mt-0.5" />
				{/if}
				<div class="min-w-0">
					<div class="text-[10px] font-medium uppercase tracking-wider {error ? 'text-red-600' : 'text-zinc-500'}">
						{#if error}
							{(error.verb ?? 'error').toString()} failed
						{:else}
							{(verb ?? 'weft').toString()}{running ? ' running' : ''}
						{/if}
					</div>
					<div class="text-sm font-semibold text-zinc-900 truncate">
						{error ? headline : 'Terminal output'}
					</div>
					{#if error && subtitle}
						<div class="text-xs text-zinc-500 mt-0.5 truncate">{subtitle}</div>
					{/if}
				</div>
			</div>
			<div class="flex items-center gap-2 shrink-0 ml-3">
				<button
					type="button"
					class="flex items-center gap-1.5 text-[11px] px-2 py-1 rounded border border-zinc-200 hover:bg-zinc-50 text-zinc-600"
					onclick={() => copy('head', errorCopyText)}
					title={error ? 'Copy the failure to the clipboard' : 'Copy the output to the clipboard'}
				>
					{#if copied === 'head'}
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
					title="Close"
				>
					<XIcon class="w-4 h-4" />
				</button>
			</div>
		</div>

		{#if error && (stage || exitCode !== undefined || command)}
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
		{/if}

		<div class="flex-1 overflow-auto">
			{#if error}
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
											{@const loc = d.location}
											<button
												type="button"
												class="text-[10px] font-mono text-zinc-500 hover:text-zinc-800 hover:underline"
												title="Open {loc.file} in the source editor"
												onclick={() => onOpenLocation(loc)}
											>
												{basename(loc.file)}:{loc.line}:{loc.column}
											</button>
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
				{:else if !raw && !logText}
					<div class="px-5 py-6 text-xs text-zinc-500 italic">
						No further details available for this error.
					</div>
				{/if}

				{#if raw}
					<div class="px-5 py-3 border-b border-zinc-200">
						<div class="text-[10px] font-medium text-zinc-400 uppercase tracking-wider mb-2">Raw output</div>
						<pre class="text-[11px] font-mono text-zinc-700 bg-zinc-50 border border-zinc-200 rounded p-2.5 whitespace-pre-wrap break-words max-h-[40vh] overflow-auto">{raw}</pre>
					</div>
				{/if}
			{/if}

			<!-- The terminal output, under the failure when there is one. -->
			{#if logText}
				<div class="px-5 py-3">
					<div class="flex items-center justify-between mb-2">
						<div class="text-[10px] font-medium text-zinc-400 uppercase tracking-wider">
							Terminal output{running ? ' (running)' : ''}
						</div>
						<!-- With no failure on top, the header's Copy already puts
						     this very text on the clipboard, so one button is enough. -->
						{#if error}
							<button
								type="button"
								class="flex items-center gap-1.5 text-[11px] px-2 py-0.5 rounded border border-zinc-200 hover:bg-zinc-50 text-zinc-600"
								onclick={() => copy('log', logText)}
								title="Copy the terminal output to the clipboard"
							>
								{#if copied === 'log'}
									<Check class="w-3 h-3 text-emerald-600" />
									Copied
								{:else}
									<Copy class="w-3 h-3" />
									Copy
								{/if}
							</button>
						{/if}
					</div>
					<pre class="text-[11px] font-mono text-zinc-200 bg-zinc-950 border border-zinc-800 rounded p-2.5 whitespace-pre-wrap break-words max-h-[50vh] overflow-auto">{logText}</pre>
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
			{#if error}
				<button
					type="button"
					class="text-xs px-3 py-1.5 rounded bg-red-600 hover:bg-red-700 text-white"
					onclick={dismissAndClose}
				>
					Dismiss error
				</button>
			{/if}
		</div>
	</Dialog.Content>
</Dialog.Root>
