<script lang="ts">
	// The run spec dialog: build a spec (`weft_core::run_spec`), resolve it
	// live through the host (the parse server runs the dispatcher's own
	// resolver), attach values to explicit starts, and run it or save
	// it as `examples/<name>.json`. Opened from a node's "Run from here", a
	// group's "Run this group", or the Run menu.
	import { untrack } from 'svelte';
	import * as Dialog from '../ui/dialog';
	import type { CrossingPort, PortValues, ResolveSpecResponse, RunSpec } from '../../../../run-spec';
	import { exampleNameProblem, parseRunSpec, parseSuppliedJson } from '../../../../run-spec';

	let {
		open = $bindable(false),
		initial,
		seeded = false,
		resolveSpec,
		onRun,
		onSave,
	}: {
		open: boolean;
		/// The spec the dialog opens with (a node action filled in).
		initial: RunSpec;
		seeded?: boolean;
		resolveSpec: (spec: RunSpec, seeded: boolean) => Promise<ResolveSpecResponse>;
		onRun: (spec: RunSpec, seeded: boolean) => void;
		onSave: (spec: RunSpec) => void;
	} = $props();

	let name = $state('');
	let from = $state('');
	let target = $state('');
	let before = $state('');
	let feed = $state('');
	let emit = $state('{}');
	let group = $state('');
	let groupPayload = $state('{}');
	let fireNode = $state('');
	let firePayload = $state('{}');
	let seed = $state(false);
	/// One port-value object per explicit start. Values never target an
	/// interior node unless the person also chooses it as a start.
	let provided = $state<Record<string, string>>({});
	let missing = $state<CrossingPort[]>([]);
	/// Each input crossing into the run, under the start it is handed at:
	/// the first start its value would pass (`hand_at`), which for a
	/// group inside another is that group, not the outer door the wire
	/// lands on. One with no start on its way is listed on its own.
	const handedAt = (port: CrossingPort) => port.hand_at ?? { node: port.node, port: port.port };
	const missingAt = (start: string) => missing.filter(port => handedAt(port).node === start);
	const missingElsewhere = $derived(missing.filter(port => {
		const at = handedAt(port).node;
		return !port.supplied && at !== group.trim() && !list(from).includes(at);
	}));
	let errors = $state<string[]>([]);
	let warnings = $state<string[]>([]);
	let runnable = $state(false);
	let resolving = $state(false);

	const list = (s: string) => s.split(',').map((x) => x.trim()).filter(Boolean);

	/// Reset the fields from `initial` every time the dialog opens.
	///
	/// `untrack`, because the body both WRITES these fields and, through
	/// `resolve` -> `currentSpec`, READS them. Tracked, every read became
	/// a dependency of the effect, so typing one character re-ran the
	/// whole reset and put `initial` back: the fields could not be edited
	/// at all. Only `open` and `initial` may wake this.
	$effect(() => {
		if (!open) return;
		const spec = initial;
		const startSeeded = seeded;
		untrack(() => reset(spec, startSeeded));
	});

	function reset(initial: RunSpec, seeded: boolean): void {
		name = initial.name;
		from = Object.keys(initial.from ?? {}).join(', ');
		target = (initial.target ?? []).join(', ');
		before = (initial.before ?? []).join(', ');
		feed = (initial.feed ?? []).join(', ');
		emit = JSON.stringify(initial.emit ?? {});
		group = initial.group?.[0] ?? '';
		groupPayload = JSON.stringify(initial.group?.[1] ?? {});
		fireNode = initial.fire?.[0] ?? '';
		firePayload = initial.fire ? JSON.stringify(initial.fire[1]) : '{}';
		seed = seeded;
		const next: Record<string, string> = {};
		for (const [node, ports] of Object.entries(initial.from ?? {})) {
			next[node] = JSON.stringify(ports);
		}
		provided = next;
		void resolve();
	}

	/// Validate local edits before asking the server to resolve the graph.
	function currentSpec(): { spec: RunSpec; badJson: string[] } {
		const badJson: string[] = [];
		const parse = (text: string, where: string): unknown => {
			try {
				return parseSuppliedJson(text);
			} catch (error) {
				badJson.push(`${where}: ${String(error)}`);
				return text;
			}
		};
		const nodes = list(from);
		if (new Set(nodes).size !== nodes.length) badJson.push('from: each starting node may be listed only once');
		const starts = Object.fromEntries(nodes.map(node => [node, parse(provided[node] ?? '{}', `from ${node}`)])) as PortValues;
		const spec: RunSpec = {
			...initial,
			name: name.trim() || 'one-off',
			from: starts, target: list(target), before: list(before), feed: list(feed),
			group: group.trim() ? [group.trim(), parse(groupPayload, 'group inputs') as Record<string, unknown>] : undefined,
			fire: fireNode.trim() ? [fireNode.trim(), parse(firePayload, `fire ${fireNode}`)] : undefined,
			emit: parse(emit, 'simulated outputs') as PortValues,
		};
		try { parseRunSpec(spec); } catch (error) { badJson.push(String(error)); }
		return { spec, badJson };
	}

	let resolveSeq = 0;
	async function resolve(): Promise<void> {
		const seq = ++resolveSeq;
		const { spec, badJson } = currentSpec();
		if (badJson.length) {
			errors = badJson;
			missing = [];
			warnings = [];
			runnable = false;
			resolving = false;
			return;
		}
		resolving = true;
		try {
			const result = await resolveSpec(spec, seed);
			if (seq !== resolveSeq) return;
			missing = result.resolved?.crossings ?? [];
			errors = result.refusal?.errors ?? [];
			warnings = result.resolved?.warnings ?? [];
			runnable = result.resolved !== undefined && badJson.length === 0;
		} catch (e) {
			if (seq !== resolveSeq) return;
			errors = [String(e)];
			runnable = false;
		} finally {
			if (seq === resolveSeq) resolving = false;
		}
	}

	let debounce: ReturnType<typeof setTimeout> | undefined;
	function changed(): void {
		provided = Object.fromEntries(list(from).map(node => [node, provided[node] ?? '{}']));
		runnable = false;
		resolveSeq++;
		if (debounce) clearTimeout(debounce);
		debounce = setTimeout(() => void resolve(), 250);
	}

	function run(): void {
		const { spec } = currentSpec();
		// `seed` travels with it: the preview above resolved the spec WITH
		// seeding, so running without it refuses an input the dialog just
		// reported as covered.
		onRun(spec, seed);
		open = false;
	}

	/// Why the typed name cannot name an example, or `undefined` when it
	/// can. The same rule the CLI and the host apply, asked HERE too:
	/// the host's refusal arrives after the dialog has closed, and
	/// closing destroys every field, so a bad name used to cost the
	/// person the whole spec they had just filled in.
	const nameProblem = $derived(exampleNameProblem(name.trim()));

	/// Write it to `examples/` and nothing else. Saving used to run it
	/// too, so writing down a spec you were not ready for started a real
	/// execution.
	function save(): void {
		const current = currentSpec();
		if (current.badJson.length) {
			errors = current.badJson;
			return;
		}
		if (nameProblem) return;
		onSave(current.spec);
		open = false;
	}
</script>

<Dialog.Root bind:open>
	<Dialog.Content class="max-w-2xl">
		<Dialog.Header>
			<Dialog.Title>Run a spec</Dialog.Title>
			<Dialog.Description>
				Choose where to start and the values to supply. A frozen example also records accepted outputs for review.
			</Dialog.Description>
		</Dialog.Header>
		<div class="grid grid-cols-2 gap-3 text-xs">
			<label class="flex flex-col gap-1">
				<span class="text-zinc-500">Name</span>
				<input class="border rounded px-2 py-1 font-mono" bind:value={name} oninput={changed} />
			</label>
			<label class="flex items-center gap-2 mt-5">
				<input type="checkbox" bind:checked={seed} onchange={changed} />
				<span>Seed from head's run (inherit what did not change)</span>
			</label>
			<label class="flex flex-col gap-1">
				<span class="text-zinc-500">From (node ids, comma separated)</span>
				<input class="border rounded px-2 py-1 font-mono" bind:value={from} oninput={changed} />
			</label>
			<label class="flex flex-col gap-1">
				<span class="text-zinc-500">Target (node ids)</span>
				<input class="border rounded px-2 py-1 font-mono" bind:value={target} oninput={changed} />
			</label>
			<label class="flex flex-col gap-1">
				<span class="text-zinc-500">Group (a group or an include, by its id)</span>
				<input class="border rounded px-2 py-1 font-mono" bind:value={group} oninput={changed} />
				{#if group.trim()}
					<span class="text-zinc-500">Group input backups (port values as JSON)</span>
					<textarea class="border rounded px-2 py-1 font-mono h-16" bind:value={groupPayload} oninput={changed}></textarea>
					{@render crossings(missingAt(group.trim()))}
				{/if}
			</label>
			<label class="flex flex-col gap-1">
				<span class="text-zinc-500">Before (exclude these endpoints)</span>
				<input class="border rounded px-2 py-1 font-mono" bind:value={before} oninput={changed} />
			</label>
			<label class="flex flex-col gap-1">
				<span class="text-zinc-500">Also run what feeds these starts (from or group ids)</span>
				<input class="border rounded px-2 py-1 font-mono" bind:value={feed} oninput={changed} />
			</label>
			<label class="flex flex-col gap-1 col-span-2">
				<span class="text-zinc-500">Simulated outputs: node to port values (JSON)</span>
				<textarea class="border rounded px-2 py-1 font-mono h-16" bind:value={emit} oninput={changed}></textarea>
			</label>
			<label class="flex flex-col gap-1">
				<span class="text-zinc-500">Fire a trigger (node id)</span>
				<input class="border rounded px-2 py-1 font-mono" bind:value={fireNode} oninput={changed} />
			</label>
			{#if fireNode.trim()}
				<label class="flex flex-col gap-1 col-span-2">
					<span class="text-zinc-500">The event, as the trigger emits it (JSON)</span>
					<textarea class="border rounded px-2 py-1 font-mono h-16" bind:value={firePayload} oninput={changed}></textarea>
				</label>
			{/if}
		</div>
		{#if list(from).length > 0}
			<div class="mt-3 text-xs">
				<div class="text-zinc-500 mb-1">Starting input backups. Real values win. Supply streams as lists of items.</div>
				{#each [...new Set(list(from))] as node (node)}
					<label class="flex flex-col gap-1 mb-2">
						<span class="font-mono">{node}</span>
						<textarea class="border rounded px-2 py-1 font-mono h-16" placeholder={'{"port": "value"}'} bind:value={provided[node]} oninput={changed}></textarea>
						{@render crossings(missingAt(node))}
					</label>
				{/each}
			</div>
		{/if}
		{#if missingElsewhere.length > 0}
			<div class="mt-3 text-xs flex flex-col gap-1">
				<div class="text-zinc-500">Inputs no start passes: start at them to hand a value</div>
				{@render crossings(missingElsewhere, true)}
			</div>
		{/if}
		{#if errors.length > 0}
			<ul class="mt-3 text-xs text-red-700 space-y-1">
				{#each errors as e}<li>{e}</li>{/each}
			</ul>
		{/if}
		{#if warnings.length > 0}
			<ul class="mt-3 text-xs text-amber-700 space-y-1">
				{#each warnings as w}<li>{w}</li>{/each}
			</ul>
		{/if}
		<Dialog.Footer>
			<span class="text-xs text-zinc-400 mr-auto">
				{resolving ? 'resolving…' : runnable ? 'runnable' : 'not runnable yet'}
				{#if nameProblem}
					<span class="text-amber-600">to save: {nameProblem}</span>
				{/if}
			</span>
			<button
				class="text-xs px-3 py-1.5 rounded border disabled:opacity-50"
				onclick={save}
				disabled={nameProblem !== undefined || errors.length > 0}
				title="Write it to examples/ so it shows in the Run menu. Does not run it."
			>Save as example</button>
			<button class="text-xs px-3 py-1.5 rounded bg-zinc-900 text-white disabled:opacity-50" onclick={run} disabled={!runnable}>Run once</button>
		</Dialog.Footer>
	</Dialog.Content>
</Dialog.Root>

{#snippet crossings(ports: CrossingPort[], named = false)}
	{#each ports as port}
		<span class="text-amber-700">{named ? `${port.node}.${port.port}` : handedAt(port).port}: {port.needed_by ? `needed by ${port.needed_by}` : 'nothing in this run needs it'}{port.supplied ? ', supplied here' : ''}, fed upstream by {port.source_node}.{port.source_port}</span>
	{/each}
{/snippet}
