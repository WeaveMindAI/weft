// What a connect page reads off a service's recipe: its label, whether it
// signs in through a browser consent, the "Your own" page's fields and
// guide, the permission catalogue's defaults, and how a connection row
// reads. Pure functions over the recipe, shared by every connect surface.

import type { AccessSpecWire, AppRegistration, CredentialFieldWire, GrantSummary } from './wire';

// The "Your own" page's paste fields, DERIVED from the acquisition
// rather than re-declared: a pasted-credential service pastes its own
// fields; a consent service pastes its app's credentials (client id,
// secret, declared extras). A name field for the connection list is
// always prepended by the page itself, whatever this returns.
// SYNC: ownFields <-> crates/weft-core/src/access/spec.rs AccessSpec::own_fields

export function ownFields(spec: AccessSpecWire): CredentialFieldWire[] {
	const acq = spec.acquisition;
	if (acq.kind === 'oauth2') {
		return [
			{ name: 'client_id', label: 'Client ID', secret: false },
			{ name: 'client_secret', label: 'Client secret', secret: true },
			...(acq.registration_fields ?? []),
		];
	}
	// Runtime acquisition spends the runtime's own credential: the user
	// pastes nothing (mirrors Rust's `Runtime => Vec::new()`).
	if (acq.kind === 'runtime') return [];
	return acq.fields ?? [];
}

// The guide's steps with `{permissions}` replaced by the ticked
// permissions' labels, mirroring the Rust generation.
// SYNC: guideSteps <-> crates/weft-core/src/access/spec.rs AccessSpec::guide_steps
export function guideSteps(spec: AccessSpecWire, ticked: string[]): string[] {
	const steps = spec.own_page?.guide?.steps ?? [];
	const requested = requestedPermissions(spec, ticked);
	const labels = (spec.permissions ?? [])
		.filter((p) => requested.includes(p.id))
		.map((p) => p.label)
		.join(', ');
	return steps.map((s) => s.replaceAll('{permissions}', labels));
}

// The guide's link with `{permissions}` replaced by the ticked
// permission IDS, urlencoded and comma-joined (a provider console
// wants machine ids where the steps' prose wants labels), mirroring
// the Rust generation.
// SYNC: guideLink <-> crates/weft-core/src/access/spec.rs AccessSpec::guide_link
export function guideLink(spec: AccessSpecWire, ticked: string[]): string | undefined {
	const link = spec.own_page?.guide?.link;
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
		.filter((p) => p.default && !p.own_only)
		.map((p) => p.id);
}

// The catalogue entries the consent/tick surfaces show: everything
// except what is asked for always, and own-account-only capabilities
// (those surface as their own tutorial sections instead).
// SYNC: tickablePermissions <-> crates/weft-core/src/access/spec.rs AccessSpec::tickable_permissions
export function tickablePermissions(spec: AccessSpecWire) {
	return (spec.permissions ?? []).filter((p) => !p.always && !p.own_only);
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
	if (c.owner === 'platform' && !c.has_credential) return "NO KEY BEHIND IT (the runtime's own key is not configured)";
	if (c.owner === 'platform') return 'uses your credits';
	if (c.scopes.length === 0) return 'full access of its credential';
	const text = permissionSummary(spec, c.scopes);
	return c.permissions_verified ? text : `${text} (claimed)`;
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
	const fields = ownFields(spec);
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
