import { describe, expect, it } from 'vitest';
import { canDo, displayLabel, guideSteps, needsConsent, ownFields, ownRegistration, permissionSummary, requestedPermissions, tickablePermissions } from './recipe';
import type { AccessSpecWire, GrantSummary } from './wire';

const oauth: AccessSpecWire = {
	service: 'google',
	label: 'Google',
	acquisition: { kind: 'oauth2', grant: { kind: 'authorization_code' } },
	permissions: [
		{ id: 'mail', label: 'Read mail', description: '' },
		{ id: 'cal', label: 'Calendar', description: '' },
		{ id: 'drive', label: 'Drive', description: '' },
		{ id: 'docs', label: 'Docs', description: '' },
	],
};
const key: AccessSpecWire = { service: 'exa', acquisition: { kind: 'static', fields: [{ name: 'api_key' }] } };

function row(over: Partial<GrantSummary>): GrantSummary {
	return { id: 'g', service: 'google', scopes: [], permissions_verified: true, owner: 'author', door: 'own', has_credential: true, ...over };
}

describe('recipe', () => {
	it('names a service by its label, else its id', () => {
		expect(displayLabel(oauth)).toBe('Google');
		expect(displayLabel(key)).toBe('exa');
	});

	it('knows a browser consent from a paste', () => {
		expect(needsConsent(oauth)).toBe(true);
		expect(needsConsent(key)).toBe(false);
		expect(ownFields(key).map((f) => f.name)).toEqual(['api_key']);
		expect(ownFields(oauth).map((f) => f.name)).toEqual(['client_id', 'client_secret']);
	});

	it('asks for what the service always needs on top of what is ticked, and never offers it to tick', () => {
		const whoami: AccessSpecWire = {
			...oauth,
			permissions: [{ id: 'email', label: 'Your address', description: '', always: true }, ...(oauth.permissions ?? [])],
			own_page: { guide: { steps: ['Allow {permissions}.'] } },
		};
		expect(requestedPermissions(whoami, ['mail'])).toEqual(['email', 'mail']);
		expect(requestedPermissions(whoami, ['email', 'mail'])).toEqual(['email', 'mail']);
		expect(tickablePermissions(whoami).map((p) => p.id)).toEqual(['mail', 'cal', 'drive', 'docs']);
		expect(guideSteps(whoami, ['mail'])).toEqual(['Allow Your address, Read mail.']);
		expect(permissionSummary(whoami, ['email', 'mail'])).toBe('Read mail');
	});

	it('reads a connection row the way the list shows it', () => {
		expect(permissionSummary(oauth, ['mail', 'cal', 'drive', 'docs'])).toBe('Read mail, Calendar, Drive, ...');
		expect(canDo(oauth, row({ scopes: ['mail'], permissions_verified: false }))).toBe('Read mail (claimed)');
		expect(canDo(oauth, row({ owner: 'platform', has_credential: false }))).toContain('NO KEY');
		expect(canDo(oauth, row({ owner: { instance: 'ada' } }))).toBe('full access of its credential');
	});

	it('refuses a half-filled own app rather than ignoring the typed secret', () => {
		expect(() => ownRegistration(oauth, { client_id: 'id' }, '', undefined)).toThrow(/Client secret/);
		expect(ownRegistration(oauth, { client_id: 'id', client_secret: 's' }, 'Mine', undefined)).toEqual({
			label: 'Mine',
			client_id: 'id',
			client_secret: 's',
		});
		const project = { label: 'Project app', client_id: 'p' };
		expect(ownRegistration(oauth, {}, '', project)).toBe(project);
		expect(ownRegistration(key, { api_key: 'k' }, '', undefined)).toBeNull();
	});

	it('connects an own app with an optional field left blank', () => {
		const slack: AccessSpecWire = {
			service: 'slack',
			acquisition: {
				kind: 'oauth2',
				grant: { kind: 'authorization_code' },
				registration_fields: [{ name: 'app_token', label: 'App token', optional: true }],
			},
		};
		expect(ownFields(slack).map((f) => f.name)).toContain('app_token');
		expect(ownRegistration(slack, { client_id: 'id', client_secret: 's', app_token: ' ' }, 'Mine', undefined)).toEqual({
			label: 'Mine',
			client_id: 'id',
			client_secret: 's',
		});
		expect(() => ownRegistration(slack, { client_id: 'id', app_token: 'xapp' }, '', undefined)).toThrow(/Client secret/);
	});
});
