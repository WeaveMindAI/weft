<script lang="ts">
	// THE trigger-deactivation picker, shared by every host (all of them
	// render this exact component). Surfaced whenever a verb
	// takes live triggers down: the standalone Deactivate, and the
	// infra verbs (stop / terminate / upgrade) on an Active project.
	// Owns the business rules:
	//   - wipe forces runningPolicy = cancel (waiting before wiping is
	//     contradictory);
	//   - graceMinutes applies to hibernate only.
	// Produces a `DeactivationSpec`; the host just forwards it.
	import type { DeactivationSpec } from '../../../../shared/protocol';
	import { DEFAULT_DRAIN_TIMEOUT_SECS } from '../../../../shared/protocol';

	let {
		open = false,
		title,
		onConfirm,
		onCancel,
	}: {
		open?: boolean;
		/// e.g. "Deactivate" / "Stop infra" / "Upgrade infra": names the
		/// verb that needs the choice so the user knows what confirms.
		title: string;
		onConfirm: (spec: DeactivationSpec) => void;
		onCancel: () => void;
	} = $props();

	let mode = $state<DeactivationSpec['mode']>('park');
	let runningPolicy = $state<DeactivationSpec['runningPolicy']>('wait');
	let graceMinutes = $state(15);
	let drainTimeoutSecs = $state(DEFAULT_DRAIN_TIMEOUT_SECS);

	const MODES: Array<{ value: DeactivationSpec['mode']; label: string; detail: string }> = [
		{
			value: 'park',
			label: 'Park',
			detail:
				'Park submissions indefinitely. Project visible to consumers; submissions drained on reactivate.',
		},
		{
			value: 'hibernate',
			label: 'Hibernate',
			detail:
				'Park submissions for a grace window, then refuse them. Project hidden from consumer enumeration the entire time.',
		},
		{
			value: 'wipe',
			label: 'Wipe',
			detail: 'Drop every signal + cancel suspended runs. Reactivate is a fresh boot.',
		},
	];

	const POLICIES: Array<{
		value: DeactivationSpec['runningPolicy'];
		label: string;
		detail: string;
	}> = [
		{
			value: 'wait',
			label: 'Wait for running executions',
			detail:
				'New fires park immediately; in-flight runs drain naturally. You can cancel running anytime.',
		},
		{
			value: 'cancel',
			label: 'Cancel running executions',
			detail: 'Kills every running, non-suspended execution right away.',
		},
	];

	// Wipe forces cancel; the policy choice disappears.
	const effectivePolicy = $derived<DeactivationSpec['runningPolicy']>(
		mode === 'wipe' ? 'cancel' : runningPolicy,
	);

	function confirm() {
		onConfirm({
			mode,
			runningPolicy: effectivePolicy,
			...(mode === 'hibernate' ? { graceMinutes: Math.max(0, Math.floor(graceMinutes)) } : {}),
			...(effectivePolicy === 'wait'
				? { drainTimeoutSecs: Math.max(0, Math.floor(drainTimeoutSecs)) }
				: {}),
		});
	}

	const radioRow =
		'flex items-start gap-2 p-2 rounded-lg border cursor-pointer transition-colors';
</script>

{#if open}
	<div class="absolute inset-0 z-40 flex items-center justify-center bg-black/30">
		<div class="w-[440px] max-w-[90vw] rounded-xl bg-white border border-zinc-200 shadow-2xl p-4 flex flex-col gap-3">
			<div class="text-sm font-semibold text-zinc-800">{title}: how should triggers come down?</div>

			<div class="flex flex-col gap-1.5">
				<span class="text-[10px] font-medium uppercase tracking-wider text-zinc-500">Preserved state</span>
				{#each MODES as m}
					<label class="{radioRow} {mode === m.value ? 'border-zinc-800 bg-zinc-50' : 'border-zinc-200 hover:bg-zinc-50'}">
						<input type="radio" class="mt-0.5" bind:group={mode} value={m.value} />
						<span class="flex flex-col">
							<span class="text-xs font-medium text-zinc-800">{m.label}</span>
							<span class="text-[11px] text-zinc-500">{m.detail}</span>
						</span>
					</label>
				{/each}
			</div>

			{#if mode === 'hibernate'}
				<label class="flex items-center gap-2 text-xs text-zinc-700">
					Grace window (minutes)
					<input
						type="number"
						min="0"
						step="1"
						class="w-20 rounded-md border border-zinc-300 px-2 py-1 text-xs"
						bind:value={graceMinutes}
					/>
				</label>
			{/if}

			{#if mode === 'wipe'}
				<div class="text-[11px] text-zinc-500">
					Wipe cancels running executions (waiting before wiping is contradictory).
				</div>
			{:else}
				<div class="flex flex-col gap-1.5">
					<span class="text-[10px] font-medium uppercase tracking-wider text-zinc-500">Running executions</span>
					{#each POLICIES as p}
						<label class="{radioRow} {runningPolicy === p.value ? 'border-zinc-800 bg-zinc-50' : 'border-zinc-200 hover:bg-zinc-50'}">
							<input type="radio" class="mt-0.5" bind:group={runningPolicy} value={p.value} />
							<span class="flex flex-col">
								<span class="text-xs font-medium text-zinc-800">{p.label}</span>
								<span class="text-[11px] text-zinc-500">{p.detail}</span>
							</span>
						</label>
					{/each}
					{#if runningPolicy === 'wait'}
						<label class="flex items-center gap-2 text-xs text-zinc-700 pl-1">
							Wait at most
							<input
								type="number"
								min="0"
								step="1"
								class="w-24 rounded-md border border-zinc-300 px-2 py-1 text-xs"
								bind:value={drainTimeoutSecs}
							/>
							seconds, then proceed anyway
						</label>
					{/if}
				</div>
			{/if}

			<div class="flex justify-end gap-2 pt-1">
				<button
					class="px-3 py-1.5 rounded-lg text-xs font-medium text-zinc-600 border border-zinc-200 hover:bg-zinc-50"
					onclick={onCancel}
				>
					Cancel
				</button>
				<button
					class="px-3 py-1.5 rounded-lg text-xs font-medium bg-zinc-900 text-white hover:bg-zinc-800"
					onclick={confirm}
				>
					Confirm
				</button>
			</div>
		</div>
	</div>
{/if}
