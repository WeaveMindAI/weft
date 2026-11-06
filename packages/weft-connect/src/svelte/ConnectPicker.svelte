<script lang="ts">
	// The connection picker for one service. One screen, one mental model
	// for every service: a list of the connections this page may pick
	// from (identity / app label / what it can do), plus "+ Add a
	// connection" opening only the doors the service actually offers
	// right now (at most two: ours, or your own).
	//
	// What it picks is only the small `{id, identity}` handle of a
	// connection; pasted values (a key, an app secret) go straight to the
	// store through the transport and are never kept here. The "Your own"
	// door is ONE page with up to three optional parts (a mint button, a
	// foldable guide unfolded by default, the paste fields); there is
	// deliberately no mode switch and no second tab. A pasted-credential
	// service offering several ways to sign in shows them as a choice at
	// the top of that page; the guide and fields are the chosen way's.
	//
	// The host decides where it talks (`transport`): the weft editor
	// through its host bridge, an instance's page through the instance door.
	// The look is set with CSS custom properties (`--wc-accent`,
	// `--wc-font-size`, ...) on any ancestor.
	import type { AppRegistration, AccessSpecWire, Door, GrantSummary, LoginWayWire, SharedAppChoice } from '../core/wire';
	import type { ConnectTransport } from '../core/transport';
	import {
		canDo,
		connectionWayLabel,
		defaultPermissions,
		displayLabel,
		guideLink,
		guideSteps,
		needsConsent,
		offeredWays,
		ownFields,
		ownGuide,
		ownRegistration,
		permissionLabels,
		permissionSummary,
		tickablePermissions,
		wayId,
		wayLabel,
		ways,
	} from '../core/recipe';
	import { runConsent } from '../core/consent';
	import { onDestroy } from 'svelte';
	import PermissionPicker from './PermissionPicker.svelte';

	let {
		transport,
		spec,
		projectApp = undefined,
		nodeType = null,
		value,
		onUpdate,
	}: {
		transport: ConnectTransport;
		spec: AccessSpecWire;
		/// The project's declared PUBLIC app for this service (from
		/// `accessApps`), used by the own door when the person pastes no
		/// app of their own.
		projectApp?: AppRegistration | undefined;
		/// The access node's type, named in the permission picker's
		/// add-your-own hint (a missing permission is added to this
		/// node's metadata). Absent on an instance's page.
		nodeType?: string | null;
		/// The handle of the picked connection, if any.
		value: { id: string; identity?: string } | undefined;
		/// Called with the new pick, or `null` for none. A pick the host
		/// cannot store throws; the error shows under the picker.
		onUpdate: (v: { id: string; identity?: string } | null) => void | Promise<void>;
	} = $props();

	const label = $derived(displayLabel(spec));
	const isConsent = $derived(needsConsent(spec));
	const hasPermissions = $derived(tickablePermissions(spec).length > 0);
	const declaredDoors = $derived<Door[]>(spec.doors ?? ['own']);
	const isStatic = $derived(spec.acquisition.kind === 'static');

	let open = $state(false);
	let busy = $state(false);
	let error = $state<string | null>(null);
	/// Which surface the panel shows: the list, or one of the doors.
	let surface = $state<'list' | 'shared' | 'own' | 'upgrade'>('list');
	/// The connection the upgrade surface widens, in place.
	let upgrading = $state<GrantSummary | null>(null);
	let connections = $state<GrantSummary[]>([]);
	/// The shared-door options actually backed right now; a door with
	/// nothing behind it is HIDDEN.
	let sharedApps = $state<SharedAppChoice[]>([]);
	/// Whether a runtime credential backs the shared door of a
	/// non-oauth (key) service, and the page may offer it.
	let sharedCredential = $state(false);
	/// The shared app the person clicked; the connect names it by label.
	let chosenApp = $state<SharedAppChoice | null>(null);
	let redirectUri = $state('');
	/// Why no browser consent can run right now (the provider only
	/// accepts https callback URLs and this weft has none); paste
	/// connects stay available, every consent button hides.
	let consentBlocked = $state<string | null>(null);
	let ticked = $state<string[]>([]);
	/// The ways to sign in this install offers a pasted-credential
	/// service (the doors answer), in the spec's order.
	let offered = $state.raw<LoginWayWire[]>([]);
	/// Whether the doors answered this opening, so an empty `offered`
	/// means "none works here" rather than "not asked yet".
	let doorsKnown = $state(false);
	/// The way the "Your own" page fills: the only one when one is
	/// offered, the person's pick when several are, `null` until then
	/// (and always for a service that pastes no credential).
	let chosenWay = $state.raw<LoginWayWire | null>(null);
	/// The own-page guide (the chosen way's for a pasted-credential
	/// service) and its link with the ticked permission ids interpolated.
	const guide = $derived(ownGuide(spec, chosenWay));
	const providerGuideLink = $derived(guideLink(spec, chosenWay, ticked));
	const fields = $derived(ownFields(spec, chosenWay));
	/// The capability groups the shown fields can fill.
	const capabilities = $derived(
		(spec.capabilities ?? []).filter((c) => c.fields.every((n) => fields.some((f) => f.name === n))),
	);
	let formValues = $state<Record<string, string>>({});
	/// The paste-a-credential section's own values, separate from the
	/// app fields: the two sections submit independently.
	let pasteValues = $state<Record<string, string>>({});
	/// The name the person gives their own connection / app; always the
	/// first field on the "Your own" page.
	let ownName = $state('');
	let guideOpen = $state(true);
	/// The own-account-only capabilities with their own tutorial section
	/// on the "Your own" page; folded state per capability.
	const ownOnlyGuides = $derived((spec.permissions ?? []).filter((p) => p.ownOnly && p.guide));
	const ownOnlyLabels = $derived((spec.permissions ?? []).filter((p) => p.ownOnly).map((p) => p.label));
	let capGuideOpen = $state<Record<string, boolean>>({});
	/// The shared door's one-time displacement warning: shown before the
	/// FIRST connection is created through it, acknowledged per attempt.
	let sharedWarningAcked = $state(false);
	let waitingConsent = $state(false);
	/// Generation counter for the consent wait: bumped on every panel open
	/// and cancel, captured at the wait's start, so a superseded wait's
	/// late end never resets the state a newer interaction owns.
	let waitSeq = 0;
	// A wait still running when the component goes away stops with it.
	onDestroy(() => {
		waitSeq++;
	});
	/// Which row is asking "sure?" right now. The confirm is INLINE: a
	/// webview is sandboxed without modals, so `confirm()` is ignored
	/// outright (a silent no-op, the worst outcome for a delete button).
	let forgetPending = $state<string | null>(null);

	function fail(e: unknown) {
		error = e instanceof Error ? e.message : String(e);
	}

	async function openPanel(e: MouseEvent) {
		e.stopPropagation();
		waitSeq++;
		open = !open;
		error = null;
		surface = 'list';
		if (!open) return;
		ticked = defaultPermissions(spec);
		doorsKnown = false;
		busy = true;
		try {
			connections = await transport.connections(spec.service);
			const doors = await transport.doors(spec);
			sharedApps = doors.sharedApps;
			sharedCredential = doors.sharedCredential && transport.allowsSharedKey;
			redirectUri = doors.redirectUri ?? '';
			consentBlocked = doors.consentBlocked ?? null;
			offered = offeredWays(spec, doors.offeredWays);
			// One way offered: it is the way, with nothing to pick.
			chosenWay = offered.length === 1 ? (offered[0] ?? null) : null;
			doorsKnown = true;
		} catch (e) {
			fail(e);
		} finally {
			busy = false;
		}
	}

	/// Delete a connection for good (the store row, not just this pick).
	/// Anything still pointing at it fails loudly at resolution with the
	/// reconnect message, which is the honest outcome; the inline confirm
	/// is what makes it a decision.
	async function forget(e: MouseEvent, c: GrantSummary) {
		e.stopPropagation();
		forgetPending = null;
		busy = true;
		error = null;
		try {
			await transport.forget(c);
			connections = connections.filter((x) => x.id !== c.id);
			// The pick pointed at exactly this row: clear it, so it never
			// references a connection that is gone.
			if (value?.id === c.id) await onUpdate(null);
		} catch (e) {
			fail(e);
		} finally {
			busy = false;
		}
	}

	async function choose(handle: { id: string; identity?: string } | null) {
		try {
			await onUpdate(handle);
		} catch (e) {
			fail(e);
		}
	}

	function pick(e: MouseEvent, c: GrantSummary) {
		e.stopPropagation();
		// Retire any in-flight consent wait: the panel is closing on a
		// DIFFERENT choice, and a wait ending later must not overwrite it.
		waitSeq++;
		busy = false;
		waitingConsent = false;
		open = false;
		void choose({ id: c.id, identity: c.identity ?? undefined });
	}

	/// Pick a way to sign in. What was typed was for the other way's
	/// fields, so it goes.
	function chooseWay(e: MouseEvent, way: LoginWayWire) {
		e.stopPropagation();
		if (chosenWay === way) return;
		chosenWay = way;
		formValues = {};
	}

	function disconnect(e: MouseEvent) {
		e.stopPropagation();
		void choose(null);
	}

	function finish(grant: GrantSummary) {
		waitSeq++;
		open = false;
		formValues = {};
		pasteValues = {};
		ownName = '';
		void choose({ id: grant.id, identity: grant.identity ?? undefined });
	}

	/// The one-request connects (paste / server-to-server / shared key).
	/// `paste` = the ready-credential section of a consent service (the
	/// server connects on the spec's static paste variant, no app).
	async function connectDirect(e: MouseEvent, door: Door, paste = false) {
		e.stopPropagation();
		busy = true;
		error = null;
		try {
			const done = await transport.connectDirect({
				spec,
				door,
				paste,
				values: paste ? { ...pasteValues } : door === 'own' ? { ...formValues } : {},
				way: door === 'own' && !paste && chosenWay ? wayId(chosenWay) : null,
				label: door === 'own' && ownName.trim() ? ownName.trim() : null,
				permissions: ticked,
				sharedApp: door === 'shared' ? (chosenApp?.label ?? null) : null,
				registration: door === 'own' && !paste ? ownRegistration(spec, formValues, ownName, projectApp) : null,
			});
			finish(done.grant);
		} catch (e) {
			fail(e);
		} finally {
			busy = false;
		}
	}

	/// Browser consent: begin, open the consent page, wait for the
	/// outcome until the callback lands (or the person gives up).
	/// Defaults, never `x?: T`: see `no_optional_params.test.ts`.
	async function connectConsent(
		e: MouseEvent,
		door: Door,
		upgradeGrantId: string | undefined = undefined,
		sharedApp: string | undefined = undefined
	) {
		e.stopPropagation();
		const seq = ++waitSeq;
		busy = true;
		error = null;
		try {
			const grant = await runConsent(
				transport,
				{
					spec,
					door,
					permissions: ticked,
					sharedApp: door === 'shared' ? (sharedApp ?? chosenApp?.label ?? null) : null,
					upgradeGrantId: upgradeGrantId ?? null,
					registration: door === 'own' ? ownRegistration(spec, formValues, ownName, projectApp) : null,
				},
				{ stopped: () => seq !== waitSeq, onWaiting: () => (waitingConsent = true) },
			);
			if (grant && seq === waitSeq) finish(grant);
		} catch (e) {
			if (seq === waitSeq) fail(e);
		} finally {
			if (seq === waitSeq) {
				waitingConsent = false;
				busy = false;
			}
		}
	}

	async function mintApp(e: MouseEvent) {
		e.stopPropagation();
		if (!transport.mintApp) return;
		busy = true;
		error = null;
		try {
			const minted = await transport.mintApp(spec, ticked);
			// Prefill the page's fields with the fresh app's credentials;
			// the person still names it and connects.
			formValues = { ...formValues, ...minted.values };
		} catch (e) {
			fail(e);
		} finally {
			busy = false;
		}
	}

	function cancelWait(e: MouseEvent) {
		e.stopPropagation();
		waitSeq++;
		waitingConsent = false;
		busy = false;
	}

	/// Open the upgrade surface for one connection: the permission picker
	/// starts from what it already holds, and the person ticks what they
	/// want it to hold. A shared-door row widens through a shared app, whose
	/// covers are fixed, so it lists those apps instead.
	function openUpgrade(e: MouseEvent, c: GrantSummary) {
		e.stopPropagation();
		upgrading = c;
		ticked = [...c.scopes];
		error = null;
		surface = 'upgrade';
	}

	/// Which of an existing connection's rows may be UPGRADED in place
	/// (exclusive class only: the provider structurally rotates the one
	/// grant, so everything referencing it follows).
	const exclusive = $derived(spec.grants === 'exclusive');
</script>

<div class="wc-root" onclick={(e) => e.stopPropagation()} role="none">
	{#if value}
		<div class="wc-connected">
			<span class="wc-connected-text" title={value.id}>Connected{value.identity ? ` as ${value.identity}` : ''}</span>
			<span class="wc-row-actions">
				<button type="button" class="wc-quiet" onclick={openPanel}>Change</button>
				<button type="button" class="wc-quiet wc-quiet-danger" onclick={disconnect}>Disconnect</button>
			</span>
		</div>
	{:else}
		<button type="button" class="wc-primary" onclick={openPanel}>{open ? 'Cancel' : `Connect ${label}...`}</button>
	{/if}

	{#if open}
		<div class="wc-panel">
			{#if busy && !waitingConsent}
				<div class="wc-muted">Working...</div>
			{/if}

			{#if waitingConsent}
				<div class="wc-muted">Finish the sign-in in your browser; this updates on its own.</div>
				<button type="button" class="wc-quiet" onclick={cancelWait}>Stop waiting</button>
			{:else if surface === 'list'}
				{#each connections as c (c.id)}
					<div class="wc-row">
						<button type="button" class="wc-connection" title={c.id} onclick={(e) => pick(e, c)}>
							<span class="wc-ellipsis">{c.identity ?? c.label ?? label}</span>
							<span class="wc-ellipsis wc-muted">{c.label ?? label}{#if connectionWayLabel(spec, c)}<span class="wc-faint"> ({connectionWayLabel(spec, c)})</span>{/if}</span>
							<span class="wc-ellipsis wc-end {c.hasCredential ? 'wc-muted' : 'wc-danger'}">{canDo(spec, c)}</span>
						</button>
						{#if forgetPending === c.id}
							<button type="button" class="wc-quiet wc-danger" onclick={(e) => forget(e, c)}>Forget</button>
							<button type="button" class="wc-quiet" onclick={(e) => { e.stopPropagation(); forgetPending = null; }}>Keep</button>
						{:else}
							<button type="button" class="wc-quiet wc-quiet-danger" title="Forget this connection" onclick={(e) => { e.stopPropagation(); forgetPending = c.id; }}>x</button>
						{/if}
					</div>
					{#if exclusive && hasPermissions && isConsent && !consentBlocked}
						<div class="wc-end-row">
							<button
								type="button"
								class="wc-link wc-warn"
								title="This service has one grant per account; upgrading its permissions applies to everything using it."
								onclick={(e) => openUpgrade(e, c)}
							>Upgrade this connection...</button>
						</div>
					{/if}
				{/each}
				{#if connections.length === 0 && !busy}
					<div class="wc-muted">No {label} connections yet.</div>
				{/if}
				<div class="wc-add">
					<div class="wc-muted">+ Add a connection</div>
					{#if declaredDoors.includes('shared') && !(isConsent && consentBlocked)}
						{#each sharedApps as app (app.label)}
							<button type="button" class="wc-option" onclick={(e) => { e.stopPropagation(); chosenApp = app; surface = 'shared'; sharedWarningAcked = false; }}>
								<span class="wc-strong">Use {app.label} (one click)</span>
								<span class="wc-block wc-muted wc-ellipsis">{permissionSummary(spec, app.covers)}</span>
							</button>
						{/each}
						{#if sharedCredential}
							<button type="button" class="wc-option" onclick={(e) => { e.stopPropagation(); chosenApp = null; surface = 'shared'; sharedWarningAcked = false; }}>
								<span>Use ours (uses your credits)</span>
								{#if ownOnlyLabels.length > 0}
									<span class="wc-block wc-muted wc-ellipsis">No {ownOnlyLabels.join(' / ')}: own account only</span>
								{/if}
							</button>
						{/if}
					{/if}
					{#if declaredDoors.includes('own')}
						<button type="button" class="wc-option" onclick={(e) => { e.stopPropagation(); surface = 'own'; guideOpen = true; }}>Your own...</button>
					{/if}
				</div>
			{:else if surface === 'upgrade' && upgrading}
				{@const target = upgrading}
				<div class="wc-muted">
					Upgrading {target.identity ?? target.label ?? label}: this applies to everything using this connection.
				</div>
				{#if target.door === 'shared'}
					{#each sharedApps as app (app.label)}
						<button type="button" class="wc-option" disabled={busy} onclick={(e) => connectConsent(e, 'shared', target.id, app.label)}>
							<span class="wc-strong">Sign in through {app.label}</span>
							<span class="wc-block wc-muted wc-ellipsis">{permissionSummary(spec, app.covers)}</span>
						</button>
					{/each}
				{:else}
					<PermissionPicker
						permissions={tickablePermissions(spec)}
						allPermissionsUrl={spec.allPermissionsUrl ?? null}
						{nodeType}
						openExternal={(url) => transport.openExternal(url)}
						bind:ticked
					/>
					<button type="button" class="wc-primary" disabled={busy} onclick={(e) => connectConsent(e, 'own', target.id)}>Sign in with {label} to upgrade</button>
				{/if}
				<!-- The ticks were this connection's; a new one starts from the defaults. -->
				<button type="button" class="wc-quiet" onclick={(e) => { e.stopPropagation(); ticked = defaultPermissions(spec); surface = 'list'; }}>Back</button>
			{:else if surface === 'shared'}
				{#if chosenApp}
					<div class="wc-muted">{chosenApp.label} can:</div>
					<ul class="wc-list wc-muted">
						{#each permissionLabels(spec, chosenApp.covers) as p (p)}
							<li>{p}</li>
						{/each}
					</ul>
				{/if}
				{#if isConsent && !sharedWarningAcked}
					<!-- The displacement warning, once per connection created
					     through the shared door: on some services a second
					     connect on the same app + workspace disconnects the
					     first. Warn, then stop trying to prevent it. -->
					<div class="wc-notice">
						By using a shared app, if someone else in the same workspace uses it too, this connection may be
						disconnected. To avoid that, set up your own.
					</div>
					<button type="button" class="wc-primary" onclick={(e) => { e.stopPropagation(); sharedWarningAcked = true; }}>Continue</button>
				{:else if isConsent}
					<button type="button" class="wc-primary" disabled={busy} onclick={(e) => connectConsent(e, 'shared')}>Sign in with {label}</button>
				{:else}
					<div class="wc-muted">One click: calls on this connection spend your credits.</div>
					<button type="button" class="wc-primary" disabled={busy} onclick={(e) => connectDirect(e, 'shared')}>Use ours</button>
				{/if}
				<button type="button" class="wc-quiet" onclick={(e) => { e.stopPropagation(); surface = 'list'; }}>Back</button>
			{:else if surface === 'own'}
				{#if hasPermissions}
					<PermissionPicker
						permissions={tickablePermissions(spec)}
						allPermissionsUrl={spec.allPermissionsUrl ?? null}
						{nodeType}
						openExternal={(url) => transport.openExternal(url)}
						bind:ticked
					/>
				{/if}
				{#if spec.ownPage?.mint && transport.mintApp}
					<button type="button" class="wc-primary wc-go" disabled={busy} onclick={mintApp}>Create it for me</button>
				{/if}
				{#if isStatic && doorsKnown && offered.length === 0}
					<div class="wc-notice">
						{#if ways(spec).length === 1}
							{label} signs in only on a local install, so this install cannot connect it.
						{:else}
							None of {label}'s ways to sign in works on this install: each works only on a local install.
						{/if}
					</div>
				{:else if offered.length > 1}
					<div class="wc-muted">How do you sign in?</div>
					{#each offered as way (wayId(way))}
						<button type="button" class="wc-option" class:wc-chosen={chosenWay === way} onclick={(e) => chooseWay(e, way)}>
							{wayLabel(way)}
						</button>
					{/each}
				{/if}
				{#if guide}
					<button type="button" class="wc-fold" onclick={(e) => { e.stopPropagation(); guideOpen = !guideOpen; }}>
						<span>{guideOpen ? '▾' : '▸'}</span>
						<span>{isStatic ? 'How to get one' : 'How to create one'}</span>
					</button>
					{#if guideOpen}
						<div class="wc-guide">
							{#if providerGuideLink}
								<button type="button" class="wc-link" onclick={(e) => { e.stopPropagation(); transport.openExternal(providerGuideLink); }}>Open the provider's page</button>
							{/if}
							{#each guideSteps(spec, chosenWay, ticked) as step, i}
								<p>{i + 1}. {step}</p>
							{/each}
							{#if redirectUri && isConsent}
								<p>Callback URL to register:</p>
								<p class="wc-code">{redirectUri}</p>
							{/if}
						</div>
					{/if}
				{/if}
				{#each ownOnlyGuides as cap (cap.id)}
					<button type="button" class="wc-fold" onclick={(e) => { e.stopPropagation(); capGuideOpen[cap.id] = !capGuideOpen[cap.id]; }}>
						<span>{capGuideOpen[cap.id] ? '▾' : '▸'}</span>
						<span>Set up: {cap.label}</span>
					</button>
					{#if capGuideOpen[cap.id]}
						<div class="wc-guide">
							<p>{cap.description}</p>
							{#if cap.guide?.link}
								<button type="button" class="wc-link" onclick={(e) => { e.stopPropagation(); transport.openExternal(cap.guide!.link!); }}>Open the provider's page</button>
							{/if}
							{#each cap.guide?.steps ?? [] as step, i}
								<p>{i + 1}. {step}</p>
							{/each}
						</div>
					{/if}
				{/each}
				<!-- A name field is always first, whatever the service
				     declares: it is the connection list's middle column. Left
				     empty, the store names it after the service, which is
				     what the placeholder shows. -->
				{#if !isStatic || chosenWay}
					<div class="wc-field">
						<label class="wc-muted" for={`wc-${spec.service}-own-name`}>Name</label>
						<input id={`wc-${spec.service}-own-name`} type="text" class="wc-input" placeholder={label} bind:value={ownName} />
					</div>
				{/if}
				{#if capabilities.length > 0}
					<div class="wc-warn">Fill the fields of at least one of: {capabilities.map((c) => c.label).join(', ')}.</div>
				{/if}
				{#each fields as f (f.name)}
					{@const group = capabilities.find((c) => c.fields.includes(f.name))}
					<div class="wc-field">
						<label class="wc-muted" for={`wc-${spec.service}-${f.name}`}>
							{f.label ?? f.name}{#if group}<span class="wc-faint"> ({group.label})</span>{/if}
						</label>
						<input
							id={`wc-${spec.service}-${f.name}`}
							type={(f.secret ?? true) ? 'password' : 'text'}
							class="wc-input wc-mono"
							placeholder={f.placeholder}
							bind:value={formValues[f.name]}
						/>
					</div>
				{/each}
				{#if isConsent}
					{#if consentBlocked}
						<div class="wc-notice">{consentBlocked}</div>
					{:else}
						<button type="button" class="wc-primary" disabled={busy} onclick={(e) => connectConsent(e, 'own')}>Sign in with {label}</button>
					{/if}
				{:else if !isStatic || chosenWay}
					<button type="button" class="wc-primary" disabled={busy} onclick={(e) => connectDirect(e, 'own')}>Connect</button>
				{/if}
				{#if spec.ownPage?.paste}
					<div class="wc-muted wc-divider">Or paste a credential you already made:</div>
					{#each spec.ownPage.paste.fields as f (f.name)}
						<div class="wc-field">
							<label class="wc-muted" for={`wc-${spec.service}-paste-${f.name}`}>{f.label ?? f.name}</label>
							<input
								id={`wc-${spec.service}-paste-${f.name}`}
								type={(f.secret ?? true) ? 'password' : 'text'}
								class="wc-input wc-mono"
								placeholder={f.placeholder}
								bind:value={pasteValues[f.name]}
							/>
						</div>
					{/each}
					<button type="button" class="wc-primary" disabled={busy} onclick={(e) => connectDirect(e, 'own', true)}>Connect</button>
				{/if}
				<button type="button" class="wc-quiet" onclick={(e) => { e.stopPropagation(); surface = 'list'; }}>Back</button>
			{/if}

			{#if error}
				<div class="wc-danger wc-break">{error}</div>
			{/if}
		</div>
	{/if}
</div>

<style>
	.wc-root {
		font-size: var(--wc-font-size, 10px);
		color: var(--wc-fg, #18181b);
	}
	.wc-root > * + *,
	.wc-panel > * + * {
		margin-top: 0.375rem;
	}
	.wc-panel {
		border: 1px solid var(--wc-border, #e4e4e7);
		border-radius: var(--wc-radius, 0.25rem);
		padding: 0.5rem;
		background: var(--wc-bg, #ffffff);
	}
	.wc-muted {
		color: var(--wc-muted-fg, #71717a);
	}
	.wc-faint {
		opacity: 0.6;
	}
	.wc-strong {
		font-weight: 500;
	}
	.wc-block {
		display: block;
	}
	.wc-ellipsis {
		overflow: hidden;
		text-overflow: ellipsis;
		white-space: nowrap;
		min-width: 0;
	}
	.wc-end {
		text-align: right;
	}
	.wc-break {
		overflow-wrap: anywhere;
	}
	.wc-danger {
		color: var(--wc-danger, #ef4444);
	}
	.wc-warn {
		color: var(--wc-warn-fg, #d97706);
	}
	button {
		font: inherit;
		cursor: pointer;
	}
	button:disabled {
		opacity: 0.5;
		cursor: default;
	}
	.wc-primary {
		width: 100%;
		padding: 0.375rem 0.75rem;
		border: none;
		border-radius: var(--wc-radius, 0.25rem);
		font-weight: 500;
		background: var(--wc-accent, #3b82f6);
		color: var(--wc-accent-fg, #ffffff);
	}
	.wc-primary:hover:not(:disabled) {
		background: var(--wc-accent-hover, #2563eb);
	}
	.wc-go {
		background: var(--wc-go, #059669);
	}
	.wc-quiet {
		background: none;
		border: none;
		padding: 0 0.25rem;
		color: var(--wc-muted-fg, #71717a);
	}
	.wc-quiet:hover {
		color: var(--wc-fg, #18181b);
	}
	.wc-quiet-danger:hover {
		color: var(--wc-danger, #ef4444);
	}
	.wc-link {
		background: none;
		border: none;
		padding: 0;
		color: var(--wc-accent, #3b82f6);
	}
	.wc-link:hover {
		text-decoration: underline;
	}
	.wc-connected {
		display: flex;
		align-items: center;
		justify-content: space-between;
		gap: 0.5rem;
		padding: 0.375rem 0.5rem;
		border: 1px solid var(--wc-ok-border, #a7f3d0);
		border-radius: var(--wc-radius, 0.25rem);
		background: var(--wc-ok-bg, #ecfdf5);
	}
	.wc-connected-text {
		color: var(--wc-ok-fg, #047857);
		overflow: hidden;
		text-overflow: ellipsis;
		white-space: nowrap;
	}
	.wc-row-actions {
		display: flex;
		gap: 0.5rem;
		flex-shrink: 0;
	}
	.wc-row {
		display: flex;
		align-items: center;
		gap: 0.25rem;
	}
	.wc-connection {
		flex: 1;
		min-width: 0;
		display: grid;
		grid-template-columns: 1fr auto 1fr;
		align-items: center;
		gap: 0.5rem;
		padding: 0.25rem 0.375rem;
		border: none;
		border-radius: var(--wc-radius, 0.25rem);
		background: var(--wc-muted-bg, #f4f4f5);
		text-align: left;
	}
	.wc-connection:hover,
	.wc-option:hover {
		filter: brightness(0.96);
	}
	.wc-end-row {
		display: flex;
		justify-content: flex-end;
	}
	.wc-add {
		padding-top: 0.25rem;
		border-top: 1px solid var(--wc-border, #e4e4e7);
	}
	.wc-add > * + * {
		margin-top: 0.25rem;
	}
	.wc-option {
		width: 100%;
		text-align: left;
		padding: 0.25rem 0.5rem;
		border: none;
		border-radius: var(--wc-radius, 0.25rem);
		background: var(--wc-muted-bg, #f4f4f5);
	}
	.wc-chosen {
		outline: 1px solid var(--wc-accent, #3b82f6);
	}
	.wc-list {
		margin: 0;
		padding-left: 0.75rem;
		list-style: disc;
	}
	.wc-notice {
		padding: 0.375rem 0.5rem;
		border-radius: var(--wc-radius, 0.25rem);
		background: var(--wc-warn-bg, #fffbeb);
		color: var(--wc-warn-fg, #d97706);
	}
	.wc-fold {
		width: 100%;
		display: flex;
		align-items: center;
		gap: 0.375rem;
		padding: 0;
		border: none;
		background: none;
		font-weight: 500;
		color: var(--wc-accent, #3b82f6);
	}
	.wc-guide {
		padding: 0.375rem 0.5rem;
		border-radius: var(--wc-radius, 0.25rem);
		background: var(--wc-guide-bg, #eff6ff);
		color: var(--wc-muted-fg, #71717a);
	}
	.wc-guide p {
		margin: 0.25rem 0 0;
	}
	.wc-code {
		font-family: ui-monospace, monospace;
		background: var(--wc-muted-bg, #f4f4f5);
		border-radius: var(--wc-radius, 0.25rem);
		padding: 0.25rem 0.375rem;
		overflow-wrap: anywhere;
		user-select: text;
	}
	.wc-field > * + * {
		margin-top: 0.125rem;
	}
	.wc-field label {
		display: block;
	}
	.wc-input {
		width: 100%;
		box-sizing: border-box;
		font-size: calc(var(--wc-font-size, 10px) * 1.2);
		padding: 0.25rem 0.5rem;
		border: none;
		border-radius: var(--wc-radius, 0.25rem);
		background: var(--wc-muted-bg, #f4f4f5);
		outline: none;
	}
	.wc-mono {
		font-family: ui-monospace, monospace;
	}
	.wc-divider {
		padding-top: 0.25rem;
		border-top: 1px solid var(--wc-border, #e4e4e7);
	}
</style>
