<script lang="ts">
	// A `remote_select` field: pick a resource on the connected service
	// (a spreadsheet, a channel, a model) from several declared SOURCES,
	// using the richest one the connection actually supports:
	//
	//   granted   read off the connection row (free, no call)
	//   list      call the service and enumerate (needs its permissions)
	//   picker    the provider's own chooser (needs a connection)
	//   from_url  paste a link; a pattern extracts the id (needs nothing)
	//
	// Each unusable source drops out because its requirement is not met,
	// which is what makes the field work with no per-service branching,
	// and what leaves `from_url` as the one source standing with NO
	// connection at all. The stored value is the bare id (the node reads
	// exactly that); the human label is a display cache here.
	//
	// ONE control for every page that fills such a field: the weft editor
	// plugs in a transport that signs with the author's connection, a
	// an instance's page one that goes through the instance door.
	import type { ResourceTransport } from '../core/transport';
	import type { FieldConnection, LookupItem, ResourceSource } from '../core/wire';
	import { onDestroy } from 'svelte';

	// Labels for ids picked or listed this session: display sugar, the
	// source holds the bare id and a label falls back to it when unknown.
	const labelCache = new Map<string, string>();

	let {
		sources,
		value,
		connection,
		transport,
		onUpdate,
		placeholder = null,
		freeText = false,
		grantedScopes = null,
		parents = {},
		connectFirst = 'Connect an account first.',
	}: {
		sources: ResourceSource[];
		/** The picked id, or unset. */
		value: string | undefined;
		/** Whose connection signs the sources that need one: a `list`
		 *  runs on any, a `picker` or `granted` only on the person's own. */
		connection: FieldConnection;
		transport: ResourceTransport;
		onUpdate: (id: string | null) => void;
		placeholder?: string | null;
		/** The person may type a value the sources never listed. */
		freeText?: boolean;
		/** The connection's granted permissions, when known (for dropping
		 *  `list` sources whose `requires` are not held); null = unknown. */
		grantedScopes?: string[] | null;
		/** Picked parent ids for drill-down (`depends_on`). */
		parents?: Record<string, string>;
		/** What to say when no source is usable yet. */
		connectFirst?: string;
	} = $props();

	const connected = $derived(connection !== 'none');

	/// The usable sources, with their position in the declared list (the
	/// instance door names a source by it).
	const usable = $derived.by(() =>
		sources
			.map((source, index) => ({ source, index }))
			.filter(({ source: s }) => {
				switch (s.kind) {
					case 'granted':
					case 'picker':
						return connection === 'own';
					case 'list':
						// A public endpoint is called bare.
						if (s.public) return true;
						if (!connected) return false;
						if (grantedScopes == null) return true;
						return (s.requires ?? []).every((r) => grantedScopes.includes(r));
					case 'from_url':
						return true;
				}
			}),
	);
	const listSource = $derived(usable.find((u) => u.source.kind === 'list'));
	const grantedSource = $derived(usable.find((u) => u.source.kind === 'granted'));
	const pickerSource = $derived(usable.find((u) => u.source.kind === 'picker'));
	const fromUrlSource = $derived(usable.find((u) => u.source.kind === 'from_url'));

	const display = $derived.by(() => {
		if (!value) return '';
		return labelCache.get(value) ?? value;
	});

	/// What the search box can actually do right now, so the placeholder
	/// never promises a search that cannot happen.
	const searchPlaceholder = $derived.by(() => {
		if (usable.length === 0) return 'Type the id...';
		if (fromUrlSource) return connected ? 'Search, or paste a URL or id...' : 'Paste a link or id...';
		return 'Search, or paste an id...';
	});

	/// Whether the list call filters server-side: a `{query}` in the URL
	/// means each keystroke re-asks the service. Without one the service
	/// answers the same full list every time, so the typed text narrows it
	/// HERE instead (label or id substring).
	const serverFiltered = $derived(listSource?.source.kind === 'list' && listSource.source.get.includes('{query}'));
	const shownItems = $derived.by(() => {
		const q = query.trim().toLowerCase();
		if (serverFiltered || !q) return items;
		return items.filter((i) => i.label.toLowerCase().includes(q) || i.id.toLowerCase().includes(q));
	});

	let open = $state(false);
	let query = $state('');
	let items = $state<LookupItem[]>([]);
	let nextCursor = $state<string | null>(null);
	/// Whether a load has COMPLETED for the current connection state: its
	/// own state, since `items.length` conflates "never tried", "failed"
	/// and "got nothing", which turned keystrokes into a retry loop.
	let loaded = $state(false);
	let busy = $state(false);
	let error = $state<string | null>(null);
	let searchSeq = 0;

	// The loaded list belongs to ONE connection state and one set of
	// parents; a connect, a rewire or a parent pick invalidates it.
	$effect(() => {
		void connection;
		void JSON.stringify(parents);
		items = [];
		nextCursor = null;
		loaded = false;
	});

	/// Fill the option list from the richest enumerating source:
	/// `granted` (free, off the row) first; when it holds nothing (or is
	/// not declared), the `list` call. A default, never `cursor?: string`:
	/// see `no_optional_params.test.ts`.
	async function loadOptions(cursor: string | undefined = undefined) {
		const seq = ++searchSeq;
		busy = true;
		error = null;
		try {
			if (!cursor && grantedSource?.source.kind === 'granted' && !query.trim()) {
				const recorded = await transport.granted(grantedSource.index, grantedSource.source);
				if (seq !== searchSeq) return;
				if (recorded.length > 0) {
					items = recorded;
					nextCursor = null;
					loaded = true;
					return;
				}
				// Recorded nothing: fall through to the next source.
			}
			if (listSource?.source.kind !== 'list') return;
			// Honest wire: the typed text is a server-side filter only when
			// the URL declares `{query}`; otherwise the narrowing is here.
			const page = await transport.list(listSource.index, listSource.source, serverFiltered ? query : '', parents, cursor ?? null);
			if (seq !== searchSeq) return;
			items = cursor ? [...items, ...page.items] : page.items;
			nextCursor = page.next_cursor;
			loaded = true;
		} catch (e) {
			if (seq !== searchSeq) return;
			error = e instanceof Error ? e.message : String(e);
		} finally {
			if (seq === searchSeq) busy = false;
		}
	}

	let debounce: ReturnType<typeof setTimeout> | undefined;
	function onQueryInput(v: string) {
		query = v;
		// A pasted URL resolves locally through the declared extractor:
		// no network, instant pick.
		if (fromUrlSource?.source.kind === 'from_url') {
			try {
				const m = new RegExp(fromUrlSource.source.pattern).exec(v);
				if (m && m[1]) {
					pick({ id: m[1], label: m[1] });
					return;
				}
			} catch {
				// A bad pattern is refused at metadata load; nothing to do.
			}
		}
		clearTimeout(debounce);
		// A locally-narrowed list re-asks only when no load has completed:
		// the service would answer the same full list on every keystroke.
		if (serverFiltered || !loaded) {
			debounce = setTimeout(() => void loadOptions(), 300);
		}
	}

	/// Whether a chooser session is live (its page open in the person's
	/// browser, the poll running).
	let pickerOpen = $state(false);
	/// Generation counter for the chooser poll: a superseded poll's late
	/// completion never resets the state a newer interaction owns, and a
	/// bump stops the poll.
	let pollSeq = 0;

	// Nothing outlives the field: a pending search, its late answer, and a
	// chooser poll all stop when it goes away.
	onDestroy(() => {
		clearTimeout(debounce);
		searchSeq++;
		pollSeq++;
	});

	/// Open the node-declared chooser on a weft-served page in the
	/// person's browser (where their provider sessions live), and poll its
	/// outcome exactly like a sign-in: for as long as the person takes,
	/// until they close it here, the field goes away, or the server refuses
	/// the poll. The chooser runs on THAT page.
	async function openProviderPicker(e: MouseEvent) {
		e.stopPropagation();
		if (pickerSource?.source.kind !== 'picker') return;
		const seq = ++pollSeq;
		busy = true;
		error = null;
		try {
			const started = await transport.beginPicker(pickerSource.index, pickerSource.source);
			transport.openExternal(started.url);
			pickerOpen = true;
			// The chooser page is its own progress surface from here on.
			busy = false;
			for (;;) {
				await new Promise((r) => setTimeout(r, 1000));
				if (seq !== pollSeq) return;
				const outcome = await transport.pickerOutcome(started.state);
				if (seq !== pollSeq) return;
				if (!outcome) continue;
				pickerOpen = false;
				if (outcome.error) error = outcome.error;
				else if (outcome.picked) pick(outcome.picked);
				// A cancel closes quietly: nothing picked, nothing wrong.
				return;
			}
		} catch (e) {
			if (seq !== pollSeq) return;
			pickerOpen = false;
			error = e instanceof Error ? e.message : String(e);
		} finally {
			if (seq === pollSeq) busy = false;
		}
	}

	function closePicker(e: MouseEvent) {
		e.stopPropagation();
		pollSeq++;
		pickerOpen = false;
	}

	function pick(item: LookupItem) {
		open = false;
		query = '';
		items = [];
		loaded = false;
		if (item.label) labelCache.set(item.id, item.label);
		onUpdate(item.id);
	}

	function useRawId() {
		const id = query.trim();
		if (id) pick({ id, label: id });
	}

	function toggle(e: MouseEvent) {
		e.stopPropagation();
		open = !open;
		// `usable` already dropped every source whose requirement is
		// unmet, so any enumerating source left is loadable.
		if (open && (grantedSource || listSource)) void loadOptions();
	}
</script>

<div class="wc-rs" onclick={(e) => e.stopPropagation()} role="none">
	<button type="button" class="wc-rs-value {display ? '' : 'wc-rs-empty'}" onclick={toggle}>
		{display || placeholder || 'Pick...'}
	</button>

	{#if open}
		<div class="wc-rs-panel">
			{#if usable.length === 0 && !freeText}
				<div class="wc-rs-muted">{connectFirst}</div>
			{:else}
				<input
					type="text"
					class="wc-rs-input"
					placeholder={searchPlaceholder}
					value={query}
					oninput={(e) => onQueryInput(e.currentTarget.value)}
					onkeydown={(e) => {
						// Free-text fields take the typed value as-is on Enter:
						// the list is suggestions, not a closed set.
						if (e.key === 'Enter' && freeText) useRawId();
					}}
				/>
				{#if pickerSource && !pickerOpen}
					<button type="button" class="wc-rs-action" disabled={busy} onclick={openProviderPicker}>
						Browse with the provider's picker...
					</button>
				{/if}
				{#if pickerOpen}
					<div class="wc-rs-row">
						<span class="wc-rs-muted">Pick in the browser tab; this updates on its own.</span>
						<button type="button" class="wc-rs-quiet" onclick={closePicker}>Cancel</button>
					</div>
				{/if}
				{#if busy && items.length === 0}
					<div class="wc-rs-muted">Loading...</div>
				{/if}
				<div class="wc-rs-list">
					{#each shownItems as item (item.id)}
						<button type="button" class="wc-rs-item" title={item.id} onclick={() => pick(item)}>{item.label}</button>
					{/each}
				</div>
				{#if nextCursor}
					<!-- Under a local narrow, a fetched page may add nothing
					     VISIBLE; the loaded count shows the fetch worked. -->
					<button type="button" class="wc-rs-link" disabled={busy} onclick={() => void loadOptions(nextCursor ?? undefined)}>
						More... ({items.length} loaded)
					</button>
				{/if}
				{#if query.trim() && !busy}
					<button type="button" class="wc-rs-quiet" onclick={useRawId}>Use "{query.trim()}" as the id</button>
				{/if}
				{#if error}
					<div class="wc-rs-error">{error}</div>
				{/if}
			{/if}
		</div>
	{/if}
</div>

<style>
	.wc-rs {
		font-size: var(--wc-font-size, 10px);
		color: var(--wc-fg, #18181b);
	}
	.wc-rs > * + *,
	.wc-rs-panel > * + * {
		margin-top: 0.25rem;
	}
	.wc-rs-value,
	.wc-rs-input {
		width: 100%;
		text-align: left;
		font-size: calc(var(--wc-font-size, 10px) * 1.2);
		background: var(--wc-muted-bg, #f4f4f5);
		color: inherit;
		padding: 0.375rem 0.5rem;
		border-radius: var(--wc-radius, 0.25rem);
		border: none;
		outline: none;
		overflow: hidden;
		text-overflow: ellipsis;
		white-space: nowrap;
		cursor: pointer;
	}
	.wc-rs-input {
		cursor: text;
	}
	.wc-rs-empty,
	.wc-rs-muted {
		color: var(--wc-muted-fg, #71717a);
	}
	.wc-rs-panel {
		border: 1px solid var(--wc-border, #e4e4e7);
		border-radius: var(--wc-radius, 0.25rem);
		padding: 0.375rem;
		background: var(--wc-bg, #ffffff);
	}
	.wc-rs-action {
		width: 100%;
		text-align: left;
		padding: 0.25rem 0.5rem;
		border: none;
		border-radius: var(--wc-radius, 0.25rem);
		background: var(--wc-muted-bg, #f4f4f5);
		color: inherit;
		cursor: pointer;
	}
	.wc-rs-row {
		display: flex;
		align-items: center;
		justify-content: space-between;
		gap: 0.5rem;
	}
	.wc-rs-list {
		max-height: 10rem;
		overflow-y: auto;
	}
	.wc-rs-item {
		display: block;
		width: 100%;
		text-align: left;
		padding: 0.25rem 0.375rem;
		border: none;
		border-radius: var(--wc-radius, 0.25rem);
		background: none;
		color: inherit;
		font-size: calc(var(--wc-font-size, 10px) * 1.1);
		overflow: hidden;
		text-overflow: ellipsis;
		white-space: nowrap;
		cursor: pointer;
	}
	.wc-rs-item:hover,
	.wc-rs-action:hover {
		background: var(--wc-border, #e4e4e7);
	}
	.wc-rs-link {
		border: none;
		background: none;
		padding: 0 0.25rem;
		color: var(--wc-accent, #3b82f6);
		cursor: pointer;
	}
	.wc-rs-quiet {
		border: none;
		background: none;
		padding: 0 0.25rem;
		color: var(--wc-muted-fg, #71717a);
		cursor: pointer;
	}
	.wc-rs-error {
		color: var(--wc-danger, #ef4444);
		overflow-wrap: anywhere;
	}
</style>
