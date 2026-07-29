<script lang="ts">
	// The `access` widget: the CONNECTION PICKER on an ACCESS NODE.
	// One screen, one mental model for every service: a list of the
	// tenant's connections (identity / app label / what it can do),
	// plus "+ Add a connection" opening only the doors the service
	// actually offers right now (at most two: ours, or your own).
	//
	// The config value this widget owns is only the small
	// `{id, identity}` handle of the picked connection; pasted values
	// (a key, an app secret) go editor -> store directly and are never
	// written into node config. The "Your own" door is ONE page with up
	// to three optional parts (a mint button, a foldable guide unfolded
	// by default, the paste fields); there is deliberately no mode
	// switch and no second tab.
	import { accessCall, openExternalUrl } from '../../../vscode';
	import type { AccessSpecWire, AppRegistration, Door, GrantSummary } from '../../../../protocol';
	import { defaultPermissions, guideSteps, ownFields } from './own-fields';
	import { grantsForService, invalidateGrants } from './grants-cache.svelte';
	import PermissionPicker from './PermissionPicker.svelte';

	let {
		spec,
		projectApp,
		nodeType = null,
		value,
		onUpdate,
	}: {
		spec: AccessSpecWire;
		/// The access node's type, named in the permission picker's
		/// add-your-own hint (a missing permission is added to this
		/// node's metadata).
		nodeType?: string | null;
		/// The project's declared PUBLIC app for this service (from
		/// `accessApps`), used by the own door when the user pastes no
		/// app of their own.
		projectApp: AppRegistration | undefined;
		/// The config handle of the picked connection, if any.
		value: { id: string; identity?: string } | undefined;
		onUpdate: (v: { id: string; identity?: string } | null) => void;
	} = $props();

	const label = $derived(spec.label ?? spec.service);
	const isConsent = $derived(
		spec.acquisition.kind === 'oauth2' &&
			spec.acquisition.grant?.kind === 'authorization_code',
	);
	const hasPermissions = $derived((spec.permissions ?? []).length > 0);
	const declaredDoors = $derived<Door[]>(spec.doors ?? ['own']);

	let open = $state(false);
	let busy = $state(false);
	let error = $state<string | null>(null);
	/// Which surface the panel shows: the list, or one of the doors.
	let surface = $state<'list' | 'shared' | 'own'>('list');
	let connections = $state<GrantSummary[]>([]);
	/// One registered app the shared door offers as its own option:
	/// its label and its FIXED permission set. The user picks an
	/// option; they never tick permissions on the shared door.
	// SYNC: SharedAppChoice <-> crates/weft-broker/src/access_admin.rs SharedAppChoice, crates/weft-dispatcher/src/api/access.rs SharedAppChoice
	type SharedAppChoice = { label: string; covers: string[] };
	/// The shared-door options actually backed right now; a door with
	/// nothing behind it is HIDDEN.
	let sharedApps = $state<SharedAppChoice[]>([]);
	/// Whether a runtime credential backs the shared door of a
	/// non-oauth (key) service.
	let sharedCredential = $state(false);
	/// The shared app the user clicked; the connect names it by label.
	let chosenApp = $state<SharedAppChoice | null>(null);
	let redirectUri = $state('');
	/// Why no browser consent can run right now (the provider only
	/// accepts https callback URLs and this weft has none); paste
	/// connects stay available, every consent button hides.
	let consentBlocked = $state<string | null>(null);
	let ticked = $state<string[]>([]);
	let formValues = $state<Record<string, string>>({});
	/// The paste-a-credential section's own values, separate from the
	/// app fields: the two sections submit independently.
	let pasteValues = $state<Record<string, string>>({});
	/// The name the user gives their own connection / app; always the
	/// first field on the "Your own" page.
	let ownName = $state('');
	let guideOpen = $state(true);
	/// The shared door's one-time displacement warning: shown before
	/// the FIRST connection is created through it (rarely, since
	/// connections are created rarely), acknowledged per attempt.
	let sharedWarningAcked = $state(false);
	let waitingConsent = $state(false);
	let pollStop = $state(false);
	/// Generation counter for the consent poll (same pattern as
	/// RemoteSelectField's searchSeq): bumped on every panel open and
	/// cancel, captured at poll entry, so a superseded poll's late
	/// completion never resets the state a newer interaction owns.
	let pollSeq = 0;

	async function openPanel(e: MouseEvent) {
		e.stopPropagation();
		pollSeq++;
		open = !open;
		error = null;
		surface = 'list';
		if (!open) return;
		ticked = defaultPermissions(spec);
		busy = true;
		try {
			connections = await grantsForService(spec.service);
			const doors = await accessCall<{
				shared_apps: SharedAppChoice[];
				shared_credential: boolean;
				redirect_uri: string | null;
				consent_blocked?: string;
			}>('POST', 'doors', { spec });
			sharedApps = doors.shared_apps;
			sharedCredential = doors.shared_credential;
			redirectUri = doors.redirect_uri ?? '';
			consentBlocked = doors.consent_blocked ?? null;
		} catch (e) {
			error = e instanceof Error ? e.message : String(e);
		} finally {
			busy = false;
		}
	}

	/// Which row is asking "sure?" right now. The confirm is INLINE:
	/// a webview is sandboxed without modals, so `confirm()` is
	/// ignored outright (a silent no-op, the worst possible outcome
	/// for a delete button).
	let forgetPending = $state<string | null>(null);

	/// Delete a connection for good (the store row, not just this
	/// node's reference). Any node still pointing at it fails loudly
	/// at resolution with the reconnect message, which is the honest
	/// outcome; the inline confirm is what makes it a decision.
	async function forget(e: MouseEvent, c: GrantSummary) {
		e.stopPropagation();
		forgetPending = null;
		busy = true;
		error = null;
		try {
			await accessCall<null>('DELETE', `grants/${encodeURIComponent(c.id)}`);
			invalidateGrants(spec.service);
			connections = connections.filter((x) => x.id !== c.id);
			// The node pointed at exactly this row: clear it, so the
			// field never references a connection that is gone.
			if (value?.id === c.id) onUpdate(null);
		} catch (e) {
			error = e instanceof Error ? e.message : String(e);
		} finally {
			busy = false;
		}
	}

	function pick(e: MouseEvent, c: GrantSummary) {
		e.stopPropagation();
		// Retire any in-flight consent poll: the panel is closing on a
		// DIFFERENT choice, and a poll landing later must not overwrite
		// it or paint an error on a closed panel. The retired poll's
		// own guarded finally never runs, so the flags reset here.
		pollSeq++;
		busy = false;
		waitingConsent = false;
		open = false;
		onUpdate({ id: c.id, identity: c.identity ?? undefined });
	}

	function disconnect(e: MouseEvent) {
		e.stopPropagation();
		onUpdate(null);
	}

	/// Permission ids in plain words, from the service's catalogue; an
	/// id the catalogue does not name stays raw rather than vanishing.
	function permissionLabels(ids: string[]): string[] {
		const catalogue = spec.permissions ?? [];
		return ids.map((id) => catalogue.find((p) => p.id === id)?.label ?? id);
	}

	/// A short "these permissions" summary: the first three labels,
	/// an ellipsis past that.
	function permissionSummary(ids: string[]): string {
		const named = permissionLabels(ids);
		return named.slice(0, 3).join(', ') + (named.length > 3 ? ', ...' : '');
	}

	/// The "what it can do" column: the granted permissions' labels in
	/// plain words, the service label for a permissionless credential.
	function canDo(c: GrantSummary): string {
		if (c.owner === 'ours') return 'uses your credits';
		if (c.scopes.length === 0) return 'full access of its credential';
		const text = permissionSummary(c.scopes);
		return c.permissions_verified ? text : `${text} (claimed)`;
	}

	function middleColumn(c: GrantSummary): string {
		return c.label ?? label;
	}

	function finish(grant: GrantSummary) {
		// Retire any other in-flight poll before closing on this grant.
		pollSeq++;
		invalidateGrants(spec.service);
		open = false;
		formValues = {};
		pasteValues = {};
		ownName = '';
		onUpdate({ id: grant.id, identity: grant.identity ?? undefined });
	}

	/// The one-request connects (paste / server-to-server / shared key).
	/// `paste` = the ready-credential section of a consent service (the
	/// server connects on the spec's static paste variant, no app).
	async function connectDirect(e: MouseEvent, door: Door, paste = false) {
		e.stopPropagation();
		busy = true;
		error = null;
		try {
			const done = await accessCall<{ grant: GrantSummary }>('POST', 'connect/direct', {
				spec,
				door,
				paste,
				values: paste ? { ...pasteValues } : door === 'own' ? { ...formValues } : {},
				label: door === 'own' && ownName.trim() ? ownName.trim() : null,
				permissions: ticked,
				shared_app: door === 'shared' ? (chosenApp?.label ?? null) : null,
				registration: door === 'own' && !paste ? ownRegistration() : null,
				project_id: null,
			});
			finish(done.grant);
		} catch (e) {
			error = e instanceof Error ? e.message : String(e);
		} finally {
			busy = false;
		}
	}

	/// The own door's app for a CONSENT service: the pasted fields (id,
	/// secret, extras) with the typed name. A half-filled form (some
	/// fields typed, others blank) throws, naming the first missing
	/// field: it must never silently connect through the project's app
	/// with the typed secret ignored. Only an entirely untouched form
	/// falls through to the project's declared public app. `null` for
	/// services using no app; the server refuses a consent with none.
	function ownRegistration(): AppRegistration | null {
		if (spec.acquisition.kind !== 'oauth2') return null;
		const fields = ownFields(spec);
		const anyFilled = fields.some((f) => formValues[f.name]?.trim());
		if (anyFilled) {
			const missing = fields.find((f) => !formValues[f.name]?.trim());
			if (missing) {
				throw new Error(
					`Fill in "${missing.label ?? missing.name}" to use your own app, or clear the app fields to use the project's app.`,
				);
			}
			const reg: AppRegistration = {
				label: ownName.trim() || label,
				client_id: formValues['client_id'].trim(),
			};
			for (const f of fields) {
				if (f.name === 'client_id') continue;
				reg[f.name] = formValues[f.name].trim();
			}
			return reg;
		}
		return projectApp ?? null;
	}

	/// Browser consent: begin -> open the consent page -> poll the
	/// parked outcome until the callback lands (or the user gives up).
	async function connectConsent(e: MouseEvent, door: Door, upgradeGrantId?: string, sharedApp?: string) {
		e.stopPropagation();
		const seq = ++pollSeq;
		busy = true;
		error = null;
		try {
			const started = await accessCall<{ consent_url: string; state: string }>(
				'POST',
				'connect/begin',
				{
					spec,
					door,
					permissions: ticked,
					shared_app: door === 'shared' ? (sharedApp ?? chosenApp?.label ?? null) : null,
					upgrade_grant_id: upgradeGrantId ?? null,
					registration: door === 'own' ? ownRegistration() : null,
				},
			);
			openExternalUrl(started.consent_url);
			waitingConsent = true;
			pollStop = false;
			for (let i = 0; i < 150 && !pollStop; i++) {
				await new Promise((r) => setTimeout(r, 2000));
				if (seq !== pollSeq) return;
				const outcome = await accessCall<{ grant?: GrantSummary; error?: string } | null>(
					'GET',
					`connect/status?state=${encodeURIComponent(started.state)}`,
				);
				if (seq !== pollSeq) return;
				if (!outcome) continue;
				if (outcome.error) {
					error = outcome.error;
				} else if (outcome.grant) {
					finish(outcome.grant);
				}
				waitingConsent = false;
				busy = false;
				return;
			}
			if (!pollStop) error = 'the sign-in did not complete; retry from here';
		} catch (e) {
			if (seq !== pollSeq) return;
			error = e instanceof Error ? e.message : String(e);
		} finally {
			if (seq === pollSeq) {
				waitingConsent = false;
				busy = false;
			}
		}
	}

	async function mintApp(e: MouseEvent) {
		e.stopPropagation();
		busy = true;
		error = null;
		try {
			const minted = await accessCall<{ values: Record<string, string> }>(
				'POST',
				'mint-app',
				{ spec, permissions: ticked },
			);
			// Prefill the page's fields with the fresh app's credentials;
			// the user still names it and connects.
			formValues = { ...formValues, ...minted.values };
		} catch (e) {
			error = e instanceof Error ? e.message : String(e);
		} finally {
			busy = false;
		}
	}

	function cancelWait(e: MouseEvent) {
		e.stopPropagation();
		pollSeq++;
		pollStop = true;
		waitingConsent = false;
		busy = false;
	}

	/// Which of an existing connection's rows may be UPGRADED in place
	/// (exclusive class only: the provider structurally rotates the one
	/// grant, so every referencing project follows).
	const exclusive = $derived(spec.grants === 'exclusive');
</script>

<div class="space-y-1.5 nodrag nopan" onclick={(e) => e.stopPropagation()} role="none">
	{#if value}
		<div class="flex items-center justify-between gap-2 bg-emerald-50 border border-emerald-200 rounded px-2 py-1.5">
			<span class="text-[10px] text-emerald-700 truncate" title={value.id}>
				Connected{value.identity ? ` as ${value.identity}` : ''}
			</span>
			<span class="flex items-center gap-2 shrink-0">
				<button type="button" class="text-[10px] text-muted-foreground hover:text-foreground" onclick={openPanel}>Change</button>
				<button type="button" class="text-[10px] text-muted-foreground hover:text-red-500" onclick={disconnect}>Disconnect</button>
			</span>
		</div>
	{:else}
		<button
			type="button"
			class="w-full text-[10px] px-3 py-1.5 rounded font-medium bg-blue-500 text-white hover:bg-blue-600 transition-colors"
			onclick={openPanel}
		>{open ? 'Cancel' : `Connect ${label}...`}</button>
	{/if}

	{#if open}
		<div class="border border-border rounded p-2 space-y-1.5 bg-background">
			{#if busy && !waitingConsent}
				<div class="text-[10px] text-muted-foreground">Working...</div>
			{/if}

			{#if waitingConsent}
				<div class="text-[10px] text-muted-foreground">
					Finish the sign-in in your browser; this updates on its own.
				</div>
				<button type="button" class="text-[10px] text-muted-foreground hover:text-foreground" onclick={cancelWait}>Stop waiting</button>
			{:else if surface === 'list'}
				<!-- The connection list: who / through which app / what it
				     can do. Everything renders off the row; no extra call. -->
				{#each connections as c (c.id)}
					<div class="flex items-center gap-1">
						<button
							type="button"
							class="flex-1 min-w-0 grid grid-cols-[1fr_auto_1fr] items-center gap-2 bg-muted rounded px-1.5 py-1 text-left hover:bg-muted/70"
							title={c.id}
							onclick={(e) => pick(e, c)}
						>
							<span class="text-[10px] truncate">{c.identity ?? middleColumn(c)}</span>
							<span class="text-[10px] text-muted-foreground truncate">{middleColumn(c)}</span>
							<span class="text-[10px] text-muted-foreground truncate text-right">{canDo(c)}</span>
						</button>
						<!-- Forgetting a connection is the user's to make: the row
						     is theirs (a shared-door row only records that they
						     picked the one-click option, so it deletes the same
						     way). The confirm is inline (a webview ignores
						     modal dialogs), and other projects may point at
						     this row, so it is worth asking. -->
						{#if forgetPending === c.id}
							<button
								type="button"
								class="text-[10px] text-red-500 hover:underline shrink-0 px-1"
								onclick={(e) => forget(e, c)}
							>Forget</button>
							<button
								type="button"
								class="text-[10px] text-muted-foreground hover:text-foreground shrink-0 px-1"
								onclick={(e) => { e.stopPropagation(); forgetPending = null; }}
							>Keep</button>
						{:else}
							<button
								type="button"
								class="text-[10px] text-muted-foreground hover:text-red-500 shrink-0 px-1"
								title="Forget this connection"
								onclick={(e) => { e.stopPropagation(); forgetPending = c.id; }}
							>x</button>
						{/if}
					</div>
					{#if exclusive && hasPermissions}
						<div class="flex justify-end">
							<button
								type="button"
								class="text-[10px] text-amber-600 hover:underline"
								title="This service has one grant per account; upgrading its permissions applies to every project using it."
								onclick={(e) => connectConsent(e, c.door, c.id, c.door === 'shared' ? (c.label ?? undefined) : undefined)}
							>Upgrade this connection...</button>
						</div>
					{/if}
				{/each}
				{#if connections.length === 0 && !busy}
					<div class="text-[10px] text-muted-foreground">No {label} connections yet.</div>
				{/if}
				<div class="pt-1 border-t border-border space-y-1">
					<div class="text-[10px] text-muted-foreground">+ Add a connection</div>
					{#if declaredDoors.includes('shared') && !(isConsent && consentBlocked)}
						<!-- One option PER registered app, each with its fixed
						     permission set shown up front: the user picks a
						     preset, they never edit a shared app's permissions. -->
						{#each sharedApps as app (app.label)}
							<button
								type="button"
								class="w-full text-left text-[10px] px-2 py-1 rounded bg-muted hover:bg-muted/70"
								onclick={(e) => { e.stopPropagation(); chosenApp = app; surface = 'shared'; sharedWarningAcked = false; }}
							>
								<span class="font-medium">Use {app.label} (one click)</span>
								<span class="block text-muted-foreground truncate">{permissionSummary(app.covers)}</span>
							</button>
						{/each}
						{#if sharedCredential}
							<button
								type="button"
								class="w-full text-left text-[10px] px-2 py-1 rounded bg-muted hover:bg-muted/70"
								onclick={(e) => { e.stopPropagation(); chosenApp = null; surface = 'shared'; sharedWarningAcked = false; }}
							>Use ours (uses your credits)</button>
						{/if}
					{/if}
					{#if declaredDoors.includes('own')}
						<button
							type="button"
							class="w-full text-left text-[10px] px-2 py-1 rounded bg-muted hover:bg-muted/70"
							onclick={(e) => { e.stopPropagation(); surface = 'own'; guideOpen = true; }}
						>Your own...</button>
					{/if}
				</div>
			{:else if surface === 'shared'}
				{#if chosenApp}
					<!-- The chosen app's permissions are FIXED by the
					     operator (its `covers`); shown read-only. Different
					     permissions = a different registered app, or your own. -->
					<div class="text-[10px] text-muted-foreground">
						{chosenApp.label} can:
					</div>
					<ul class="text-[10px] text-muted-foreground space-y-0.5 pl-3 list-disc">
						{#each permissionLabels(chosenApp.covers) as p (p)}
							<li>{p}</li>
						{/each}
					</ul>
				{/if}
				{#if isConsent && !sharedWarningAcked}
					<!-- The displacement warning, once per connection created
					     through the shared door: on some services a second
					     connect on the same app + workspace disconnects the
					     first. Warn, then stop trying to prevent it. -->
					<div class="text-[10px] text-amber-600 bg-amber-50 rounded px-2 py-1.5">
						By using a shared app, if someone else in the same workspace uses it too,
						this connection may be disconnected. To avoid that, set up your own.
					</div>
					<button
						type="button"
						class="w-full text-[10px] px-3 py-1.5 rounded font-medium bg-blue-500 text-white hover:bg-blue-600"
						onclick={(e) => { e.stopPropagation(); sharedWarningAcked = true; }}
					>Continue</button>
				{:else if isConsent}
					<button
						type="button"
						class="w-full text-[10px] px-3 py-1.5 rounded font-medium bg-blue-500 text-white hover:bg-blue-600 disabled:opacity-50"
						disabled={busy}
						onclick={(e) => connectConsent(e, 'shared')}
					>Sign in with {label}</button>
				{:else}
					<div class="text-[10px] text-muted-foreground">
						One click: calls on this connection spend your credits.
					</div>
					<button
						type="button"
						class="w-full text-[10px] px-3 py-1.5 rounded font-medium bg-blue-500 text-white hover:bg-blue-600 disabled:opacity-50"
						disabled={busy}
						onclick={(e) => connectDirect(e, 'shared')}
					>Use ours</button>
				{/if}
				<button type="button" class="text-[10px] text-muted-foreground hover:text-foreground" onclick={(e) => { e.stopPropagation(); surface = 'list'; }}>Back</button>
			{:else if surface === 'own'}
				<!-- ONE page, three optional parts + the fields. No tabs. -->
				{#if hasPermissions}
					<PermissionPicker
						permissions={spec.permissions ?? []}
						allPermissionsUrl={spec.all_permissions_url ?? null}
						{nodeType}
						bind:ticked
					/>
				{/if}
				{#if spec.own_page?.mint}
					<button
						type="button"
						class="w-full text-[10px] px-3 py-1.5 rounded font-medium bg-emerald-600 text-white hover:bg-emerald-700 disabled:opacity-50"
						disabled={busy}
						onclick={mintApp}
					>Create it for me</button>
				{/if}
				{#if spec.own_page?.guide}
					<button
						type="button"
						class="w-full flex items-center gap-1.5 text-[10px] text-blue-500 hover:text-blue-600 font-medium"
						onclick={(e) => { e.stopPropagation(); guideOpen = !guideOpen; }}
					>
						<span class="text-xs">{guideOpen ? '▾' : '▸'}</span>
						<span>How to create one</span>
					</button>
					{#if guideOpen}
						<div class="text-[10px] text-muted-foreground bg-blue-50 rounded px-2 py-1.5 space-y-1">
							{#if spec.own_page.guide.link}
								<button type="button" class="text-blue-500 hover:underline" onclick={(e) => { e.stopPropagation(); openExternalUrl(spec.own_page!.guide!.link!); }}>Open the provider's page</button>
							{/if}
							{#each guideSteps(spec, ticked) as step, i}
								<p>{i + 1}. {step}</p>
							{/each}
							{#if redirectUri && isConsent}
								<p>Callback URL to register:</p>
								<p class="font-mono bg-muted rounded px-1.5 py-1 break-all select-text">{redirectUri}</p>
							{/if}
						</div>
					{/if}
				{/if}
				<!-- The fields. A name field is always prepended, whatever
				     the service declares: it is the connection list's
				     middle column. -->
				<div class="space-y-0.5">
					<label class="text-[10px] text-muted-foreground" for={`acc-${spec.service}-own-name`}>Name</label>
					<input
						id={`acc-${spec.service}-own-name`}
						type="text"
						class="w-full text-xs bg-muted px-2 py-1 rounded border-none outline-none"
						placeholder={`My ${label} app`}
						bind:value={ownName}
					/>
				</div>
				{#each ownFields(spec) as f (f.name)}
					<div class="space-y-0.5">
						<label class="text-[10px] text-muted-foreground" for={`acc-${spec.service}-${f.name}`}>{f.label ?? f.name}</label>
						<input
							id={`acc-${spec.service}-${f.name}`}
							type={(f.secret ?? true) ? 'password' : 'text'}
							class="w-full text-xs bg-muted px-2 py-1 rounded border-none outline-none font-mono"
							placeholder={f.placeholder}
							bind:value={formValues[f.name]}
						/>
					</div>
				{/each}
				{#if isConsent}
					{#if consentBlocked}
						<!-- No consent can run (the provider demands an https
						     callback this weft cannot offer); say why and
						     leave only the paste path below. -->
						<div class="text-[10px] text-amber-600 bg-amber-50 rounded px-2 py-1.5">
							{consentBlocked}
						</div>
					{:else}
						<button
							type="button"
							class="w-full text-[10px] px-3 py-1.5 rounded font-medium bg-blue-500 text-white hover:bg-blue-600 disabled:opacity-50"
							disabled={busy}
							onclick={(e) => connectConsent(e, 'own')}
						>Sign in with {label}</button>
					{/if}
					{#if spec.own_page?.paste}
						<!-- The ready-credential alternative: no app, no
						     consent; one paste, same list row at the end. -->
						<div class="text-[10px] text-muted-foreground pt-1 border-t border-muted">
							Or paste a credential you already made:
						</div>
						{#each spec.own_page.paste.fields as f (f.name)}
							<div class="space-y-0.5">
								<label class="text-[10px] text-muted-foreground" for={`acc-${spec.service}-paste-${f.name}`}>{f.label ?? f.name}</label>
								<input
									id={`acc-${spec.service}-paste-${f.name}`}
									type={(f.secret ?? true) ? 'password' : 'text'}
									class="w-full text-xs bg-muted px-2 py-1 rounded border-none outline-none font-mono"
									placeholder={f.placeholder}
									bind:value={pasteValues[f.name]}
								/>
							</div>
						{/each}
						<button
							type="button"
							class="w-full text-[10px] px-3 py-1.5 rounded font-medium bg-blue-500 text-white hover:bg-blue-600 disabled:opacity-50"
							disabled={busy}
							onclick={(e) => connectDirect(e, 'own', true)}
						>Connect</button>
					{/if}
				{:else}
					<button
						type="button"
						class="w-full text-[10px] px-3 py-1.5 rounded font-medium bg-blue-500 text-white hover:bg-blue-600 disabled:opacity-50"
						disabled={busy}
						onclick={(e) => connectDirect(e, 'own')}
					>Connect</button>
				{/if}
				<button type="button" class="text-[10px] text-muted-foreground hover:text-foreground" onclick={(e) => { e.stopPropagation(); surface = 'list'; }}>Back</button>
			{/if}

			{#if error}
				<div class="text-[10px] text-red-500 break-words">{error}</div>
			{/if}
		</div>
	{/if}
</div>
