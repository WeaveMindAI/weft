<script lang="ts">
	// A member's settings page: every field the program asks the member to
	// fill (`@member_filled`), grouped by the step it belongs to, each drawn
	// with its own control: a connection picker for a connection, the
	// searchable list and chooser for a `remote_select` field, a plain
	// input for anything else. Everything goes through the member door with
	// the member's token, so the page reaches that member's values and
	// connections and nothing else. A save that changes a value a live
	// trigger of the member reads re-arms that trigger before it answers,
	// and the page says so.
	import type { MemberDoor } from '../core/transport';
	import type { MemberField, Widget } from '../core/wire';
	import ConnectPicker from './ConnectPicker.svelte';
	import ResourceSelect from './ResourceSelect.svelte';

	// `labels` holds the site's own words for what a member reads, keyed by
	// a step's id (its title) and by `step.field` (a field). Anything it does
	// not name keeps the program's own label, which is written for the
	// program's author.
	let { door, labels = {} }: { door: MemberDoor; labels?: Record<string, string> } = $props();

	let fields = $state<MemberField[]>([]);
	let error = $state<string | null>(null);
	let notice = $state<string | null>(null);
	let loading = $state(true);

	/// The fields, one group per step, in the program's order.
	const steps = $derived.by(() => {
		const groups: { step: string; title: string; fields: MemberField[] }[] = [];
		for (const field of fields) {
			let group = groups.find((g) => g.step === field.step);
			if (!group) {
				group = { step: field.step, title: labels[field.step] ?? field.label ?? field.nodeType, fields: [] };
				groups.push(group);
			}
			group.fields.push(field);
		}
		return groups;
	});

	async function load() {
		loading = true;
		error = null;
		try {
			fields = await door.fields();
		} catch (e) {
			error = e instanceof Error ? e.message : String(e);
		} finally {
			loading = false;
		}
	}

	$effect(() => {
		void load();
	});

	/// Store one value (or clear it with `null`), then show what the
	/// program now holds. A refusal (a value the node's rules refuse)
	/// stays on the page, naming what is wrong.
	async function save(field: MemberField, value: unknown) {
		error = null;
		notice = null;
		try {
			const changed =
				value === null || value === undefined || value === ''
					? await door.setValues([], [{ step: field.step, field: field.field }])
					: await door.setValues([{ step: field.step, field: field.field, value }]);
			if (changed.rearmed.length > 0) notice = `Set up again with your new value: ${changed.rearmed.join(', ')}.`;
			fields = await door.fields();
		} catch (e) {
			error = e instanceof Error ? e.message : String(e);
		}
	}

	function widgetOf(field: MemberField): Widget | null {
		return field.input.widget ?? null;
	}

	/// The picked ids of the fields a list depends on, at the same step.
	function parentsOf(field: MemberField, dependsOn: string[] | undefined): Record<string, string> {
		const parents: Record<string, string> = {};
		for (const name of dependsOn ?? []) {
			const parent = fields.find((f) => f.step === field.step && f.field === name);
			if (typeof parent?.value === 'string') parents[name] = parent.value;
		}
		return parents;
	}

	/// Whether the field draws a control of its own with the field's id. A
	/// connection picker and a resource list draw several controls, so their
	/// title is a heading rather than a label.
	function drawsOwnInput(field: MemberField): boolean {
		const kind = widgetOf(field)?.kind;
		return !((kind === 'access' && field.spec) || kind === 'remote_select');
	}

	function labelOf(field: MemberField): string {
		return labels[`${field.step}.${field.field}`] ?? field.input.label ?? field.field;
	}

	function handleOf(value: unknown): { id: string; identity?: string } | undefined {
		if (typeof value !== 'object' || value === null) return undefined;
		const handle = value as { id?: unknown; identity?: unknown };
		return typeof handle.id === 'string'
			? { id: handle.id, identity: typeof handle.identity === 'string' ? handle.identity : undefined }
			: undefined;
	}

	/// The grey hint in an empty field: what the member gets by leaving it
	/// empty (the program's fallback, else the node's default), and only
	/// with neither the node's own example. An example shown where a
	/// fallback applies read as the value a run would use.
	function hintOf(field: MemberField): string {
		if (field.fallback !== undefined) return textOf(field.fallback);
		if (field.input.default !== undefined && field.input.default !== null) return textOf(field.input.default);
		return field.input.placeholder ?? '';
	}

	function textOf(value: unknown): string {
		if (value === undefined || value === null) return '';
		return typeof value === 'string' ? value : JSON.stringify(value);
	}
</script>

{#snippet title(field: MemberField)}
	{labelOf(field)}{#if field.needed && field.value === undefined}<span class="wc-settings-needed"> (needed)</span>{/if}
{/snippet}

<div class="wc-settings">
	{#if loading}
		<div class="wc-settings-muted">Loading...</div>
	{:else if steps.length === 0 && !error}
		<div class="wc-settings-muted">This program asks you for nothing.</div>
	{:else}
		{#if error}
			<div class="wc-settings-error">{error}</div>
		{/if}
		{#if notice}
			<div class="wc-settings-muted">{notice}</div>
		{/if}
		{#each steps as group (group.step)}
			<section class="wc-settings-step">
				<div class="wc-settings-title">{group.title}</div>
				{#each group.fields as field (field.field)}
					{@const widget = widgetOf(field)}
					<div class="wc-settings-field">
						{#if drawsOwnInput(field)}
							<label class="wc-settings-label" for={`wc-${field.step}-${field.field}`}>{@render title(field)}</label>
						{:else}
							<div class="wc-settings-label">{@render title(field)}</div>
						{/if}
						{#if field.input.description}
							<div class="wc-settings-muted">{field.input.description}</div>
						{/if}
						{#if widget?.kind === 'access' && field.spec}
							<ConnectPicker
								transport={door}
								spec={field.spec}
								value={handleOf(field.value)}
								onUpdate={(handle) => save(field, handle ? { id: handle.id } : null)}
							/>
						{:else if widget?.kind === 'remote_select'}
							<ResourceSelect
								sources={widget.sources}
								value={typeof field.value === 'string' ? field.value : undefined}
								connection={field.connection}
								transport={door.resources(field.step, field.field)}
								placeholder={hintOf(field) || null}
								freeText={widget.free_text ?? false}
								parents={parentsOf(field, widget.depends_on)}
								connectFirst="Connect your account for this step first."
								onUpdate={(id) => save(field, id)}
							/>
						{:else if widget?.kind === 'select'}
							<select
								id={`wc-${field.step}-${field.field}`}
								class="wc-settings-input"
								value={textOf(field.value)}
								onchange={(e) => save(field, e.currentTarget.value || null)}
							>
								<option value="">-</option>
								{#each widget.options as option}
									<option value={option}>{option}</option>
								{/each}
							</select>
						{:else if widget?.kind === 'checkbox'}
							<input
								id={`wc-${field.step}-${field.field}`}
								type="checkbox"
								checked={field.value === true}
								onchange={(e) => save(field, e.currentTarget.checked)}
							/>
						{:else if widget?.kind === 'number'}
							<input
								id={`wc-${field.step}-${field.field}`}
								type="number"
								class="wc-settings-input"
								value={textOf(field.value)}
								placeholder={hintOf(field)}
								onchange={(e) => save(field, e.currentTarget.value === '' ? null : Number(e.currentTarget.value))}
							/>
						{:else if widget?.kind === 'textarea'}
							<textarea
								id={`wc-${field.step}-${field.field}`}
								class="wc-settings-input"
								value={textOf(field.value)}
								placeholder={hintOf(field)}
								onchange={(e) => save(field, e.currentTarget.value || null)}
							></textarea>
						{:else}
							<input
								id={`wc-${field.step}-${field.field}`}
								type={widget?.kind === 'password' ? 'password' : 'text'}
								class="wc-settings-input"
								value={textOf(field.value)}
								placeholder={hintOf(field)}
								onchange={(e) => save(field, e.currentTarget.value || null)}
							/>
						{/if}
						{#if field.value === undefined && field.fallback !== undefined}
							<div class="wc-settings-muted">Left empty, this uses {textOf(field.fallback)}.</div>
						{/if}
					</div>
				{/each}
			</section>
		{/each}
	{/if}
</div>

<style>
	.wc-settings > * + *,
	.wc-settings-step > * + * {
		margin-top: 0.75rem;
	}
	.wc-settings-field > * + * {
		margin-top: 0.25rem;
	}
	.wc-settings-title {
		font-weight: 600;
		font-size: calc(var(--wc-font-size, 10px) * 1.3);
	}
	.wc-settings-label {
		display: block;
		font-weight: 500;
		font-size: calc(var(--wc-font-size, 10px) * 1.1);
	}
	.wc-settings-needed {
		color: var(--wc-danger, #ef4444);
		font-weight: 400;
	}
	.wc-settings-input {
		width: 100%;
		font-size: calc(var(--wc-font-size, 10px) * 1.2);
		background: var(--wc-muted-bg, #f4f4f5);
		color: inherit;
		padding: 0.375rem 0.5rem;
		border-radius: var(--wc-radius, 0.25rem);
		border: none;
	}
	.wc-settings-muted {
		font-size: var(--wc-font-size, 10px);
		color: var(--wc-muted-fg, #71717a);
	}
	.wc-settings-error {
		font-size: var(--wc-font-size, 10px);
		color: var(--wc-danger, #ef4444);
		overflow-wrap: anywhere;
	}
</style>
