import { describe, expect, it } from 'vitest';
import {
	canDo,
	connectionWayLabel,
	displayLabel,
	findWay,
	guideLink,
	guideSteps,
	needsConsent,
	offeredWays,
	ownFields,
	ownGuide,
	ownRegistration,
	permissionSummary,
	requestedPermissions,
	tickablePermissions,
	wayId,
	wayLabel,
	ways,
} from './recipe';
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
const key: AccessSpecWire = { service: 'exa', acquisition: { kind: 'static', ways: [{ fields: [{ name: 'api_key' }] }] } };
const claude: AccessSpecWire = {
	service: 'anthropic',
	label: 'Anthropic',
	permissions: [{ id: 'chat', label: 'Chat', description: '', default: true }],
	acquisition: {
		kind: 'static',
		ways: [
			{
				id: 'subscription',
				label: 'Claude subscription',
				localOnly: true,
				fields: [{ name: 'token', label: 'Token' }],
				guide: { steps: ['Run `claude setup-token`.', 'Paste the token.'] },
			},
			{
				id: 'apiKey',
				label: 'API key',
				fields: [{ name: 'key', label: 'API key' }],
				guide: { link: 'https://console.example/keys?scopes={permissions}', steps: ['Create a key for {permissions}.'] },
			},
		],
	},
};

function row(over: Partial<GrantSummary>): GrantSummary {
	return { id: 'g', service: 'google', scopes: [], permissionsVerified: true, owner: 'author', door: 'own', hasCredential: true, ...over };
}

describe('recipe', () => {
	it('names a service by its label, else its id', () => {
		expect(displayLabel(oauth)).toBe('Google');
		expect(displayLabel(key)).toBe('exa');
	});

	it('knows a browser consent from a paste', () => {
		expect(needsConsent(oauth)).toBe(true);
		expect(needsConsent(key)).toBe(false);
		expect(ownFields(key, ways(key)[0]).map((f) => f.name)).toEqual(['api_key']);
		expect(ownFields(oauth, null).map((f) => f.name)).toEqual(['client_id', 'client_secret']);
	});

	it('asks for what the service always needs on top of what is ticked, and never offers it to tick', () => {
		const whoami: AccessSpecWire = {
			...oauth,
			permissions: [{ id: 'email', label: 'Your address', description: '', always: true }, ...(oauth.permissions ?? [])],
			ownPage: { guide: { steps: ['Allow {permissions}.'] } },
		};
		expect(requestedPermissions(whoami, ['mail'])).toEqual(['email', 'mail']);
		expect(requestedPermissions(whoami, ['email', 'mail'])).toEqual(['email', 'mail']);
		expect(tickablePermissions(whoami).map((p) => p.id)).toEqual(['mail', 'cal', 'drive', 'docs']);
		expect(guideSteps(whoami, null, ['mail'])).toEqual(['Allow Your address, Read mail.']);
		expect(permissionSummary(whoami, ['email', 'mail'])).toBe('Read mail');
	});

	it('reads a connection row the way the list shows it', () => {
		expect(permissionSummary(oauth, ['mail', 'cal', 'drive', 'docs'])).toBe('Read mail, Calendar, Drive, ...');
		expect(canDo(oauth, row({ scopes: ['mail'], permissionsVerified: false }))).toBe('Read mail (claimed)');
		expect(canDo(oauth, row({ owner: 'platform', hasCredential: false }))).toContain('NO KEY');
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
				registrationFields: [{ name: 'appToken', label: 'App token', optional: true }],
			},
		};
		expect(ownFields(slack, null).map((f) => f.name)).toContain('appToken');
		expect(ownRegistration(slack, { client_id: 'id', client_secret: 's', appToken: ' ' }, 'Mine', undefined)).toEqual({
			label: 'Mine',
			client_id: 'id',
			client_secret: 's',
		});
		expect(() => ownRegistration(slack, { client_id: 'id', appToken: 'xapp' }, '', undefined)).toThrow(/Client secret/);
	});

	it("pastes the chosen way's fields, and none before a way is chosen", () => {
		const [subscription, apiKey] = ways(claude);
		expect(ownFields(claude, null)).toEqual([]);
		expect(ownFields(claude, subscription).map((f) => f.name)).toEqual(['token']);
		expect(ownFields(claude, apiKey).map((f) => f.name)).toEqual(['key']);
		expect(ways(oauth)).toEqual([]);
	});

	it("shows the chosen way's guide for a pasted credential, and the page's own for a consent service", () => {
		const apiKey = findWay(claude, 'apiKey')!;
		expect(ownGuide(claude, null)).toBeUndefined();
		expect(guideSteps(claude, null, ['chat'])).toEqual([]);
		expect(guideSteps(claude, apiKey, ['chat'])).toEqual(['Create a key for Chat.']);
		expect(guideLink(claude, apiKey, ['chat'])).toBe('https://console.example/keys?scopes=chat');
		expect(guideLink(claude, findWay(claude, 'subscription')!, ['chat'])).toBeUndefined();
		// A pasted-credential service never reads `ownPage.guide`.
		const withPageGuide = { ...claude, ownPage: { guide: { steps: ['page step'] } } };
		expect(guideSteps(withPageGuide, apiKey, ['chat'])).toEqual(['Create a key for Chat.']);
		const consent = { ...oauth, ownPage: { guide: { link: 'https://console/{permissions}', steps: ['Make an app.'] } } };
		expect(guideSteps(consent, null, ['mail'])).toEqual(['Make an app.']);
		expect(guideLink(consent, null, ['mail', 'cal'])).toBe('https://console/mail%2Ccal');
	});

	it('names a way the recipe leaves unnamed `default`', () => {
		const [only] = ways(key);
		expect(wayId(only)).toBe('default');
		expect(wayLabel(only)).toBe('default');
		expect(findWay(key, 'default')).toBe(only);
		expect(wayLabel(findWay(claude, 'apiKey')!)).toBe('API key');
	});

	it('offers the ways the install offers, in the order it names them, and refuses one the recipe lacks', () => {
		expect(offeredWays(claude, ['apiKey']).map(wayId)).toEqual(['apiKey']);
		expect(offeredWays(claude, ['subscription', 'apiKey']).map(wayId)).toEqual(['subscription', 'apiKey']);
		expect(offeredWays(claude, [])).toEqual([]);
		expect(() => offeredWays(claude, ['password'])).toThrow(/'password'/);
	});

	it("shows a connection's way only when the service offers a choice", () => {
		const pasted = (way: string | null) => row({ service: 'anthropic', way });
		expect(connectionWayLabel(claude, pasted('subscription'))).toBe('Claude subscription');
		expect(connectionWayLabel(claude, pasted(null))).toBeNull();
		expect(connectionWayLabel(claude, pasted('gone'))).toBeNull();
		expect(connectionWayLabel(key, row({ service: 'exa', way: 'default' }))).toBeNull();
	});
});
