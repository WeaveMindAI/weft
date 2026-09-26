import { describe, expect, it } from 'vitest';
import { runConsent } from './consent';
import type { ConnectTransport } from './transport';
import type { ConsentOutcome, GrantSummary } from './wire';

const grant: GrantSummary = { id: 'g', service: 's', scopes: [], permissions_verified: true, owner: 'author', door: 'own', has_credential: true };

function transport(outcomes: ConsentOutcome[]): ConnectTransport & { opened: string[] } {
	const opened: string[] = [];
	return {
		opened,
		allowsSharedKey: false,
		connections: async () => [],
		forget: async () => {},
		doors: async () => ({ shared_apps: [], shared_credential: false, redirect_uri: null }),
		connectDirect: async () => ({ grant }),
		beginConsent: async () => ({ consent_url: 'https://provider/consent', state: 'st' }),
		consentOutcome: async () => outcomes.shift() ?? null,
		openExternal: (url) => opened.push(url),
	};
}

const request = { spec: { service: 's', acquisition: { kind: 'oauth2' as const } }, door: 'own' as const, permissions: [], shared_app: null, upgrade_grant_id: null, registration: null };
const now = { sleep: async () => {} };

describe('a browser consent', () => {
	it('opens the provider and waits for the connection', async () => {
		const t = transport([null, null, { grant }]);
		expect(await runConsent(t, request, { stopped: () => false, ...now })).toEqual(grant);
		expect(t.opened).toEqual(['https://provider/consent']);
	});

	it('says why the provider refused', async () => {
		await expect(runConsent(transport([{ error: 'denied' }]), request, { stopped: () => false, ...now })).rejects.toThrow('denied');
	});

	it('waits as long as the person does, and answers nothing once stopped', async () => {
		expect(await runConsent(transport([]), request, { stopped: () => true, ...now })).toBeNull();
		let polls = 0;
		const late = transport([]);
		late.consentOutcome = async () => (++polls < 500 ? null : { grant });
		expect(await runConsent(late, request, { stopped: () => false, ...now })).toEqual(grant);
	});

	it('ends the wait when the server refuses the poll', async () => {
		const gone = transport([]);
		gone.consentOutcome = async () => {
			throw new Error('unknown state');
		};
		await expect(runConsent(gone, request, { stopped: () => false, ...now })).rejects.toThrow('unknown state');
	});
});
