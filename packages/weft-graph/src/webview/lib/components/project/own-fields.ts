// The "Your own" page's paste fields, DERIVED from the acquisition
// rather than re-declared: a pasted-credential service pastes its own
// fields; a consent service pastes its app's credentials (client id,
// secret, declared extras). A name field for the connection list is
// always prepended by the page itself, whatever this returns.
// SYNC: ownFields <-> crates/weft-core/src/access/spec.rs AccessSpec::own_fields
import type { AccessSpecWire, CredentialFieldWire } from '../../../../protocol';

export function ownFields(spec: AccessSpecWire): CredentialFieldWire[] {
	const acq = spec.acquisition;
	if (acq.kind === 'oauth2') {
		return [
			{ name: 'client_id', label: 'Client ID', secret: false },
			{ name: 'client_secret', label: 'Client secret', secret: true },
			...(acq.registration_fields ?? []),
		];
	}
	return acq.fields ?? [];
}

// The guide's steps with `{permissions}` replaced by the ticked
// permissions' labels, mirroring the Rust generation.
// SYNC: guideSteps <-> crates/weft-core/src/access/spec.rs AccessSpec::guide_steps
export function guideSteps(spec: AccessSpecWire, ticked: string[]): string[] {
	const steps = spec.own_page?.guide?.steps ?? [];
	const labels = (spec.permissions ?? [])
		.filter((p) => ticked.includes(p.id))
		.map((p) => p.label)
		.join(', ');
	return steps.map((s) => s.replaceAll('{permissions}', labels));
}

// The catalogue entries that start ticked.
// SYNC: defaultPermissions <-> crates/weft-core/src/access/spec.rs AccessSpec::default_permissions
export function defaultPermissions(spec: AccessSpecWire): string[] {
	return (spec.permissions ?? []).filter((p) => p.default).map((p) => p.id);
}
