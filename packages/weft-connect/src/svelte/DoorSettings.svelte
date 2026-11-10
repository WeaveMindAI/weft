<script lang="ts">
	// A settings page for the fields a door fills, grouped by the node they
	// belong to, each drawn with its own control: a connection picker for a
	// connection, the searchable list and chooser for a `remote_select`
	// field, a plain input for anything else. Given the instance door (an
	// instance's token), it is the instance's page: every field the
	// instance fills (`@instance_filled`), and nothing but that instance's
	// values and connections. Given the program door (through the site's
	// server, which holds a connections key), it is the program's admin
	// page: the connections the program picks on the install, each picked
	// by pasting its key (the connect picks it, so the page stores nothing
	// after it). A change to a value a live trigger reads re-arms that
	// trigger before it answers, and the page says so.
	import { fillField, pickConnection, type FieldsDoor } from '../core/transport';
	import type { FilledField, ValuesChanged, Widget } from '../core/wire';
	import ConnectPicker from './ConnectPicker.svelte';
	import ResourceSelect from './ResourceSelect.svelte';

	// `labels` holds the site's own words for what its visitors read, keyed by
	// a node's place (its title) and by `node.field` (a field). Anything it does
	// not name keeps the program's own label, which is written for the
	// program's author.
	let { door, labels = {} }: { door: FieldsDoor; labels?: Record<string, string> } = $props();

	let fields = $state<FilledField[]>([]);
	let error = $state<string | null>(null);
	let notice = $state<string | null>(null);
	let loading = $state(true);

	/// The fields, one group per node, in the program's order.
	const nodes = $derived.by(() => {
		const groups: { node: string; title: string; fields: FilledField[] }[] = [];
		for (const field of fields) {
			let group = groups.find((g) => g.node === field.node);
			if (!group) {
				group = { node: field.node, title: labels[field.node] ?? field.label ?? field.nodeType, fields: [] };
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

	/// Store one value (an empty one clears the field), then show what the
	/// program now holds.
	function save(field: FilledField, value: unknown) {
		return settle(() => fillField(door, field, value === '' ? null : value));
	}

	/// Store a connection field's new pick, unless the connect already
	/// picked it (`picked`, the program door), then show what it changed.
	function pick(field: FilledField, handle: { id: string } | null, picked: ValuesChanged | null) {
		return settle(() => pickConnection(door, field, handle, picked));
	}

	/// Run one change, say what it set up again or forgot, and reload what
	/// the program now holds. A refusal (a value the node's rules refuse)
	/// stays on the page, naming what is wrong.
	async function settle(change: () => Promise<ValuesChanged>) {
		error = null;
		notice = null;
		try {
			const changed = await change();
			const notices = [
				changed.rearmed.length > 0 ? `Set up again with your new value: ${changed.rearmed.join(', ')}.` : null,
				(changed.forgotten ?? []).length > 0 ? 'The key it held is forgotten: nothing else used it.' : null,
			].filter((n) => n !== null);
			if (notices.length > 0) notice = notices.join(' ');
			fields = await door.fields();
		} catch (e) {
			error = e instanceof Error ? e.message : String(e);
		}
	}

	function widgetOf(field: FilledField): Widget | null {
		return field.input.widget ?? null;
	}

	/// The picked ids of the fields a list depends on, at the same node.
	function parentsOf(field: FilledField, dependsOn: string[] | undefined): Record<string, string> {
		const parents: Record<string, string> = {};
		for (const name of dependsOn ?? []) {
			const parent = fields.find((f) => f.node === field.node && f.field === name);
			if (typeof parent?.value === 'string') parents[name] = parent.value;
		}
		return parents;
	}

	/// Whether the field draws a control of its own with the field's id. A
	/// connection picker and a resource list draw several controls, so their
	/// title is a heading rather than a label.
	function drawsOwnInput(field: FilledField): boolean {
		const kind = widgetOf(field)?.kind;
		return !((kind === 'access' && field.spec) || kind === 'remote_select');
	}

	function labelOf(field: FilledField): string {
		return labels[`${field.node}.${field.field}`] ?? field.input.label ?? field.field;
	}

	function handleOf(value: unknown): { id: string; identity?: string } | undefined {
		if (typeof value !== 'object' || value === null) return undefined;
		const handle = value as { id?: unknown; identity?: unknown };
		return typeof handle.id === 'string'
			? { id: handle.id, identity: typeof handle.identity === 'string' ? handle.identity : undefined }
			: undefined;
	}

	/// The grey hint in an empty field: what the instance gets by leaving it
	/// empty (the program's fallback, else the node's default), and only
	/// with neither the node's own example. An example shown where a
	/// fallback applies read as the value a run would use.
	function hintOf(field: FilledField): string {
		if (field.fallback !== undefined) return textOf(field.fallback);
		if (field.input.default !== undefined && field.input.default !== null) return textOf(field.input.default);
		return field.input.placeholder ?? '';
	}

	function textOf(value: unknown): string {
		if (value === undefined || value === null) return '';
		return typeof value === 'string' ? value : JSON.stringify(value);
	}
</script>

{#snippet title(field: FilledField)}
	{labelOf(field)}{#if field.needed && field.value === undefined}<span class="wc-settings-needed"> (needed)</span>{/if}
{/snippet}

<div class="wc-settings">
	{#if loading}
		<div class="wc-settings-muted">Loading...</div>
	{:else if nodes.length === 0 && !error}
		<div class="wc-settings-muted">This program asks you for nothing.</div>
	{:else}
		{#if error}
			<div class="wc-settings-error">{error}</div>
		{/if}
		{#if notice}
			<div class="wc-settings-muted">{notice}</div>
		{/if}
		{#each nodes as group (group.node)}
			<section class="wc-settings-node">
				<div class="wc-settings-title">{group.title}</div>
				{#each group.fields as field (field.field)}
					{@const widget = widgetOf(field)}
					<div class="wc-settings-field">
						{#if drawsOwnInput(field)}
							<label class="wc-settings-label" for={`wc-${field.node}-${field.field}`}>{@render title(field)}</label>
						{:else}
							<div class="wc-settings-label">{@render title(field)}</div>
						{/if}
						{#if field.input.description}
							<div class="wc-settings-muted">{field.input.description}</div>
						{/if}
						{#if widget?.kind === 'access' && field.spec}
							<ConnectPicker
								transport={door.connectTransport(field)}
								spec={field.spec}
								value={handleOf(field.value)}
								onUpdate={(handle, picked) => pick(field, handle, picked)}
							/>
						{:else if widget?.kind === 'remote_select' && door.resources}
							<ResourceSelect
								sources={widget.sources}
								value={typeof field.value === 'string' ? field.value : undefined}
								connection={field.connection}
								transport={door.resources(field.node, field.field)}
								placeholder={hintOf(field) || null}
								freeText={widget.freeText ?? false}
								parents={parentsOf(field, widget.dependsOn)}
								connectFirst="Connect your account for this node first."
								onUpdate={(id) => save(field, id)}
							/>
						{:else if widget?.kind === 'select' && widget.freeText}
							<!-- Suggestions: any typed value, the options offered. -->
							<input
								id={`wc-${field.node}-${field.field}`}
								class="wc-settings-input"
								type="text"
								list={`wc-${field.node}-${field.field}-options`}
								placeholder={hintOf(field)}
								value={textOf(field.value)}
								onchange={(e) => save(field, e.currentTarget.value.trim() || null)}
							/>
							<datalist id={`wc-${field.node}-${field.field}-options`}>
								{#each widget.options as option}
									<option value={option}></option>
								{/each}
							</datalist>
						{:else if widget?.kind === 'select'}
							<select
								id={`wc-${field.node}-${field.field}`}
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
								id={`wc-${field.node}-${field.field}`}
								type="checkbox"
								checked={field.value === true}
								onchange={(e) => save(field, e.currentTarget.checked)}
							/>
						{:else if widget?.kind === 'number'}
							<input
								id={`wc-${field.node}-${field.field}`}
								type="number"
								class="wc-settings-input"
								value={textOf(field.value)}
								placeholder={hintOf(field)}
								onchange={(e) => save(field, e.currentTarget.value === '' ? null : Number(e.currentTarget.value))}
							/>
						{:else if widget?.kind === 'textarea'}
							<textarea
								id={`wc-${field.node}-${field.field}`}
								class="wc-settings-input"
								value={textOf(field.value)}
								placeholder={hintOf(field)}
								onchange={(e) => save(field, e.currentTarget.value || null)}
							></textarea>
						{:else}
							<input
								id={`wc-${field.node}-${field.field}`}
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
	.wc-settings-node > * + * {
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
