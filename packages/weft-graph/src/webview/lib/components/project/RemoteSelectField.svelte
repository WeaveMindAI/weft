<script lang="ts">
	// The `remote_select` widget: pick a resource on the connected
	// service on ONE field with several declared SOURCES, using the
	// richest one the chosen connection actually supports:
	//
	//   granted   read off the connection row (free, no call)
	//   list      call the service and enumerate (needs its permissions)
	//   picker    the provider's own chooser (needs a connection)
	//   from_url  paste a link; a pattern extracts the id (needs nothing)
	//
	// Each unusable source drops out silently because its requirement is
	// not met, which is what makes the field work with no per-service
	// branching, and what leaves `from_url` as the one source standing
	// with NO connection at all (the works-without-signing-in path).
	// The stored value is the bare id (the node reads exactly that, and
	// it is what the field's declared String type honestly holds); the
	// human label is a display cache in this component.
	import { accessCall, openExternalUrl } from '../../../vscode';
	import type { FieldDefinition } from '../../types';
	import type { ResourceSource } from '../../../../protocol';

	interface LookupItem {
		id: string;
		label: string;
	}

	// Labels for ids picked or listed this session. The SOURCE holds the
	// bare id (that is what the node reads, and what the field's String
	// type honestly allows); a human label is display sugar, so it lives
	// here and falls back to the id when unknown (after a reload, until
	// the list is opened again).
	const labelCache = new Map<string, string>();

	let {
		field,
		value,
		accessRef,
		grantedScopes = null,
		parents = {},
		onUpdate,
	}: {
		field: FieldDefinition;
		/// The picked id, or unset. Always a plain string: the field's
		/// declared type is String and the node reads exactly this.
		value: string | undefined;
		/// The feeding access node's connection, traced structurally
		/// through the graph's edges by the parent; null = none picked.
		accessRef: { accessId: string; service: string } | null;
		/// The traced connection's granted permission set, when the
		/// parent fetched it (for dropping `list` sources whose
		/// `requires` are not held); null = unknown, sources stay.
		grantedScopes?: string[] | null;
		/// Picked parent ids for drill-down (`dependsOn`).
		parents?: Record<string, string>;
		onUpdate: (id: string | null) => void;
	} = $props();

	const sources = $derived<ResourceSource[]>(field.sources ?? []);

	/// The usable sources, in declared order: each source's requirement
	/// against what is actually connected. Unknown granted scopes keep
	/// a `list` source (the call itself is the check then).
	const usable = $derived.by(() =>
		sources.filter((s) => {
			switch (s.kind) {
				case 'granted':
				case 'picker':
					return accessRef != null;
				case 'list':
					if (!accessRef) return false;
					if (grantedScopes == null) return true;
					return (s.requires ?? []).every((r) => grantedScopes.includes(r));
				case 'from_url':
					return true;
			}
		}),
	);
	const listSource = $derived(usable.find((s) => s.kind === 'list'));
	const grantedSource = $derived(usable.find((s) => s.kind === 'granted'));
	const pickerSource = $derived(usable.find((s) => s.kind === 'picker'));
	const fromUrlSource = $derived(usable.find((s) => s.kind === 'from_url'));

	const display = $derived.by(() => {
		if (!value) return '';
		return labelCache.get(value) ?? value;
	});

	let open = $state(false);
	let query = $state('');
	let items = $state<LookupItem[]>([]);
	let nextCursor = $state<string | null>(null);
	let busy = $state(false);
	let error = $state<string | null>(null);
	let searchSeq = 0;

	/// Fill the option list from the richest enumerating source:
	/// `granted` (free, off the row) first; when it holds nothing (or
	/// is not declared), the `list` call.
	async function loadOptions(cursor?: string) {
		if (!accessRef) return;
		const seq = ++searchSeq;
		busy = true;
		error = null;
		try {
			if (!cursor && grantedSource?.kind === 'granted' && !query.trim()) {
				const recorded = await accessCall<LookupItem[]>('POST', 'granted', {
					access_id: accessRef.accessId,
					service: accessRef.service,
					from: grantedSource.from,
					label: grantedSource.label,
					value: grantedSource.value,
				});
				if (seq !== searchSeq) return;
				if (recorded.length > 0) {
					items = recorded;
					nextCursor = null;
					return;
				}
				// Recorded nothing (a service whose consent never names
				// resources): fall through to the next source.
			}
			if (listSource?.kind !== 'list') return;
			const { kind: _kind, requires: _requires, ...lookup } = listSource;
			const page = await accessCall<{ items: LookupItem[]; next_cursor: string | null }>(
				'POST',
				'lookup',
				{
					access_id: accessRef.accessId,
					service: accessRef.service,
					lookup,
					query,
					parents,
					cursor: cursor ?? null,
				},
			);
			if (seq !== searchSeq) return;
			items = cursor ? [...items, ...page.items] : page.items;
			nextCursor = page.next_cursor;
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
		error = null;
		// A pasted URL resolves locally through the declared extractor:
		// no network, instant pick.
		if (fromUrlSource?.kind === 'from_url') {
			try {
				const m = new RegExp(fromUrlSource.pattern).exec(v);
				if (m && m[1]) {
					pick({ id: m[1], label: m[1] });
					return;
				}
			} catch {
				// A bad pattern is refused at metadata load; reaching here
				// means an un-round-tripped local edit, and the paste
				// simply does nothing until the parse stamps the real one.
			}
		}
		clearTimeout(debounce);
		debounce = setTimeout(() => void loadOptions(), 300);
	}

	/// Whether a picker session is live (its page open in the user's
	/// browser, the poll running); renders the waiting row below.
	let pickerOpen = $state(false);
	let pickerPollStop = $state(false);
	/// Generation counter for the picker poll (same pattern as
	/// searchSeq): bumped on every picker open and cancel, captured at
	/// poll entry, so a superseded poll's late completion never resets
	/// the state a newer interaction owns.
	let pollSeq = 0;

	/// Start a picker session: park the node-declared chooser in the
	/// store, open the weft-served page in the user's browser
	/// (where their provider sessions live), and poll the parked
	/// outcome exactly like a consent. The chooser's script and glue
	/// run on THAT page, never in the editor.
	async function openProviderPicker(e: MouseEvent) {
		e.stopPropagation();
		if (pickerSource?.kind !== 'picker' || !accessRef) return;
		const seq = ++pollSeq;
		busy = true;
		error = null;
		try {
			// SYNC: picker/begin body <-> crates/weft-access-store/src/flows.rs BeginPicker
			const started = await accessCall<{ state: string; url: string }>(
				'POST',
				'picker/begin',
				{
					access_id: accessRef.accessId,
					service: accessRef.service,
					script: pickerSource.script,
					code: pickerSource.code,
					mime_types: pickerSource.mime_types ?? [],
					grants: pickerSource.grants ?? [],
				},
			);
			openExternalUrl(started.url);
			pickerOpen = true;
			// The picker page is its own progress surface from here on;
			// holding `busy` through the poll would show the list's
			// "Loading..." under it the whole time.
			busy = false;
			pickerPollStop = false;
			for (let i = 0; i < 300 && !pickerPollStop; i++) {
				await new Promise((r) => setTimeout(r, 1000));
				if (seq !== pollSeq) return;
				const outcome = await accessCall<{
					picked?: { id: string; label: string };
					cancelled?: boolean;
					error?: string;
				} | null>('GET', `connect/status?state=${encodeURIComponent(started.state)}`);
				if (seq !== pollSeq) return;
				if (!outcome) continue;
				pickerOpen = false;
				if (outcome.error) error = outcome.error;
				else if (outcome.picked) pick(outcome.picked);
				// A cancel closes quietly: nothing picked, nothing wrong.
				return;
			}
			if (!pickerPollStop) {
				pickerOpen = false;
				error = 'the chooser was not finished; open it again to retry';
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
		pickerPollStop = true;
		pickerOpen = false;
	}

	function pick(item: LookupItem) {
		open = false;
		query = '';
		items = [];
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
		if (open && accessRef && (grantedSource || listSource)) void loadOptions();
	}
</script>

<div class="space-y-1 nodrag nopan" onclick={(e) => e.stopPropagation()} role="none">
	<button
		type="button"
		class="w-full text-left text-xs bg-muted px-2 py-1.5 rounded border-none outline-none truncate {display ? '' : 'text-muted-foreground'}"
		onclick={toggle}
	>{display || field.placeholder || 'Pick...'}</button>

	{#if open}
		<div class="border border-border rounded p-1.5 space-y-1 bg-background">
			{#if usable.length === 0}
				<div class="text-[10px] text-muted-foreground">
					Connect an account first (wire this node's `{field.access}` input to a connected access node).
				</div>
			{:else}
				<input
					type="text"
					class="w-full text-xs bg-muted px-2 py-1 rounded border-none outline-none"
					placeholder={fromUrlSource
						? accessRef
							? 'Search, or paste a URL or id...'
							: 'Paste a link or id...'
						: 'Search, or paste an id...'}
					value={query}
					oninput={(e) => onQueryInput(e.currentTarget.value)}
				/>
				{#if pickerSource && !pickerOpen}
					<button
						type="button"
						class="w-full text-[10px] px-2 py-1 rounded bg-muted hover:bg-muted/70 text-left"
						disabled={busy}
						onclick={openProviderPicker}
					>Browse with the provider's picker...</button>
				{/if}
				{#if pickerOpen}
					<!-- The chooser opened in the user's real browser (where
					     their provider sessions live), same pattern as the
					     sign-in consent; this is just the wait. -->
					<div class="flex items-center justify-between">
						<span class="text-[10px] text-muted-foreground">
							Pick in the browser tab; this updates on its own.
						</span>
						<button
							type="button"
							class="text-[10px] text-muted-foreground hover:text-foreground shrink-0"
							onclick={closePicker}
						>Cancel</button>
					</div>
				{/if}
				{#if busy && items.length === 0}
					<div class="text-[10px] text-muted-foreground px-1">Loading...</div>
				{/if}
				<div class="max-h-40 overflow-y-auto space-y-0.5">
					{#each items as item (item.id)}
						<button
							type="button"
							class="w-full text-left text-[11px] px-1.5 py-1 rounded hover:bg-muted truncate"
							title={item.id}
							onclick={() => pick(item)}
						>{item.label}</button>
					{/each}
				</div>
				{#if nextCursor}
					<button type="button" class="text-[10px] text-blue-500 hover:underline px-1" disabled={busy} onclick={() => void loadOptions(nextCursor ?? undefined)}>More...</button>
				{/if}
				{#if query.trim() && !busy}
					<button type="button" class="text-[10px] text-muted-foreground hover:text-foreground px-1" onclick={useRawId}>Use "{query.trim()}" as the id</button>
				{/if}
				{#if error}
					<div class="text-[10px] text-red-500 break-words px-1">{error}</div>
				{/if}
			{/if}
		</div>
	{/if}
</div>
