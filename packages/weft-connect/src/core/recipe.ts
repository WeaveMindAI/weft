// What a connect page reads off a service's recipe: its label, whether it
// signs in through a browser consent, the "Your own" page's fields and
// guide, the permission catalogue's defaults, and how a connection row
// reads. Pure functions over the recipe, shared by every connect surface.

import { DEFAULT_WAY } from './wire';
import type { AccessSpecWire, AppRegistration, CredentialFieldWire, GrantSummary, GuideWire, LoginWayWire } from './wire';

// The ways to sign in a service offers: a pasted-credential service's,
// none for any other.
// SYNC: ways <-> crates/weft-core/src/access/spec.rs AccessSpec::ways
export function ways(spec: AccessSpecWire): LoginWayWire[] {
	return spec.acquisition.kind === 'static' ? (spec.acquisition.ways ?? []) : [];
}

// What a connection records for a way: its id, `DEFAULT_WAY` when the
// recipe names none.
export function wayId(way: LoginWayWire): string {
	return way.id ?? DEFAULT_WAY;
}

// The way named `id`, when the service offers it.
// SYNC: way <-> crates/weft-core/src/access/spec.rs AccessSpec::way
export function findWay(spec: AccessSpecWire, id: string): LoginWayWire | undefined {
	return ways(spec).find((w) => wayId(w) === id);
}

// The ways this install offers (the doors answer's `offeredWays`), in the
// spec's order. An id the spec does not declare is a server bug and
// throws rather than vanishing.
export function offeredWays(spec: AccessSpecWire, offered: string[]): LoginWayWire[] {
	return offered.map((id) => {
		const way = findWay(spec, id);
		if (!way) throw new Error(`the server offers a way to sign in named '${id}' that the ${displayLabel(spec)} recipe does not declare`);
		return way;
	});
}

// The way's name as the page shows it: its label, else its id.
// SYNC: wayLabel <-> crates/weft-core/src/access/spec.rs LoginWay::display_label
export function wayLabel(way: LoginWayWire): string {
	return way.label ?? wayId(way);
}

// The label of the way a connection was made with, for the connection
// list: only when the service offers a choice (with one way it says
// nothing), and only for a way the spec still declares.
export function connectionWayLabel(spec: AccessSpecWire, c: GrantSummary): string | null {
	if (ways(spec).length < 2 || !c.way) return null;
	const way = findWay(spec, c.way);
	return way ? wayLabel(way) : null;
}

// The "Your own" page's paste fields, DERIVED from the acquisition
// rather than re-declared: a pasted-credential service pastes the fields
// of the way the person chose (`way`; none until one is), a minted JWT's
// service its key fields, a consent service its app's credentials
// (client id, secret, declared extras). A name field for the connection
// list is always prepended by the page itself, whatever this returns.
// SYNC: ownFields <-> crates/weft-core/src/access/spec.rs AccessSpec::own_fields
export function ownFields(spec: AccessSpecWire, way: LoginWayWire | null): CredentialFieldWire[] {
	const acq = spec.acquisition;
	if (acq.kind === 'oauth2') {
		return [
			{ name: 'client_id', label: 'Client ID', secret: false },
			{ name: 'client_secret', label: 'Client secret', secret: true },
			...(acq.registrationFields ?? []),
		];
	}
	// Runtime acquisition spends the runtime's own credential: the user
	// pastes nothing (mirrors Rust's `Runtime => Vec::new()`).
	if (acq.kind === 'runtime') return [];
	if (acq.kind === 'static') return way?.fields ?? [];
	return acq.fields ?? [];
}

// The guide the "Your own" page shows: the chosen way's for a
// pasted-credential service, else the page's own.
// SYNC: ownGuide <-> crates/weft-core/src/access/spec.rs AccessSpec::own_guide
export function ownGuide(spec: AccessSpecWire, way: LoginWayWire | null): GuideWire | undefined {
	return spec.acquisition.kind === 'static' ? way?.guide : spec.ownPage?.guide;
}

// The guide's steps (`ownGuide`) with `{permissions}` replaced by the
// ticked permissions' labels, mirroring the Rust generation.
// SYNC: guideSteps <-> crates/weft-core/src/access/spec.rs AccessSpec::guide_steps
export function guideSteps(spec: AccessSpecWire, way: LoginWayWire | null, ticked: string[]): string[] {
	const steps = ownGuide(spec, way)?.steps ?? [];
	const requested = requestedPermissions(spec, ticked);
	const labels = (spec.permissions ?? [])
		.filter((p) => requested.includes(p.id))
		.map((p) => p.label)
		.join(', ');
	return steps.map((s) => s.replaceAll('{permissions}', labels));
}

// The guide's link (`ownGuide`) with `{permissions}` replaced by the
// ticked permission IDS, urlencoded and comma-joined (a provider console
// wants machine ids where the steps' prose wants labels), mirroring
// the Rust generation.
// SYNC: guideLink <-> crates/weft-core/src/access/spec.rs AccessSpec::guide_link
export function guideLink(spec: AccessSpecWire, way: LoginWayWire | null, ticked: string[]): string | undefined {
	const link = ownGuide(spec, way)?.link;
	if (!link) return undefined;
	// encodeURIComponent leaves !'()* bare; the Rust side encodes
	// everything outside RFC 3986 unreserved. Encode them too so the
	// two produce byte-identical URLs for every id.
	const encoded = encodeURIComponent(requestedPermissions(spec, ticked).join(',')).replace(
		/[!'()*]/g,
		(c) => '%' + c.charCodeAt(0).toString(16).toUpperCase(),
	);
	return link.replaceAll('{permissions}', encoded);
}

// The catalogue entries that start ticked. Own-account-only entries
// are capability declarations, never consent asks, so they are never
// ticked.
// SYNC: defaultPermissions <-> crates/weft-core/src/access/spec.rs AccessSpec::default_permissions
export function defaultPermissions(spec: AccessSpecWire): string[] {
	return (spec.permissions ?? [])
		.filter((p) => p.default && !p.ownOnly)
		.map((p) => p.id);
}

// The catalogue entries the consent/tick surfaces show: everything
// except what is asked for always, and own-account-only capabilities
// (those surface as their own tutorial sections instead).
// SYNC: tickablePermissions <-> crates/weft-core/src/access/spec.rs AccessSpec::tickable_permissions
export function tickablePermissions(spec: AccessSpecWire) {
	return (spec.permissions ?? []).filter((p) => !p.always && !p.ownOnly);
}

// What a consent asks for when `ticked` is ticked: the permissions asked
// for always, then the ticked ones, each once.
// SYNC: requestedPermissions <-> crates/weft-core/src/access/spec.rs AccessSpec::requested_permissions
export function requestedPermissions(spec: AccessSpecWire, ticked: string[]): string[] {
	const out = (spec.permissions ?? []).filter((p) => p.always).map((p) => p.id);
	for (const id of ticked) if (!out.includes(id)) out.push(id);
	return out;
}

// The name a service is shown under.
// SYNC: displayLabel <-> crates/weft-core/src/access/spec.rs AccessSpec::display_label
export function displayLabel(spec: AccessSpecWire): string {
	return spec.label ?? spec.service;
}

// Whether connecting means a browser consent (OAuth2 authorization code).
// SYNC: needsConsent <-> crates/weft-core/src/access/spec.rs AccessSpec::needs_browser_consent
export function needsConsent(spec: AccessSpecWire): boolean {
	return spec.acquisition.kind === 'oauth2' && spec.acquisition.grant?.kind === 'authorization_code';
}

// Permission ids in plain words, from the service's catalogue; an id the
// catalogue does not name stays raw rather than vanishing.
export function permissionLabels(spec: AccessSpecWire, ids: string[]): string[] {
	const catalogue = spec.permissions ?? [];
	return ids.map((id) => catalogue.find((p) => p.id === id)?.label ?? id);
}

// A short "these permissions" summary: the first three labels, an
// ellipsis past that. What every connection of the service is asked for
// (`always`) says nothing about this one, so it is left out.
export function permissionSummary(spec: AccessSpecWire, ids: string[]): string {
	const always = (spec.permissions ?? []).filter((p) => p.always).map((p) => p.id);
	const named = permissionLabels(spec, ids.filter((id) => !always.includes(id)));
	return named.slice(0, 3).join(', ') + (named.length > 3 ? ', ...' : '');
}

// The "what it can do" column of a connection row: the granted
// permissions in plain words, the runtime's key when the row is the
// platform's, the whole credential for a permissionless one.
export function canDo(spec: AccessSpecWire, c: GrantSummary): string {
	if (c.owner === 'platform' && !c.hasCredential) return "NO KEY BEHIND IT (the runtime's own key is not configured)";
	if (c.owner === 'platform') return 'uses your credits';
	if (c.scopes.length === 0) return 'full access of its credential';
	const text = permissionSummary(spec, c.scopes);
	return c.permissionsVerified ? text : `${text} (claimed)`;
}

// The own door's app for a CONSENT service: the pasted fields (id,
// secret, extras) with the typed name. A half-filled form (some fields
// typed, a required one blank) throws, naming the first missing field; a
// blank optional field is left out of the app. A half-filled form must
// never silently connect through the project's app with the typed secret
// ignored. Only an entirely untouched form falls through to the
// project's declared public app. `null` for services using no app; the
// server refuses a consent with none.
export function ownRegistration(
	spec: AccessSpecWire,
	formValues: Record<string, string>,
	ownName: string,
	projectApp: AppRegistration | undefined,
): AppRegistration | null {
	if (spec.acquisition.kind !== 'oauth2') return null;
	const fields = ownFields(spec, null);
	const anyFilled = fields.some((f) => formValues[f.name]?.trim());
	if (anyFilled) {
		const missing = fields.find((f) => !f.optional && !formValues[f.name]?.trim());
		if (missing) {
			throw new Error(
				`Fill in "${missing.label ?? missing.name}" to use your own app, or clear the app fields to use the project's app.`,
			);
		}
		const reg: AppRegistration = {
			label: ownName.trim() || displayLabel(spec),
			client_id: formValues['client_id']!.trim(),
		};
		for (const f of fields) {
			const value = formValues[f.name]?.trim();
			// A blank optional field is left out: the app simply has none.
			if (f.name === 'client_id' || !value) continue;
			reg[f.name] = value;
		}
		return reg;
	}
	return projectApp ?? null;
}
