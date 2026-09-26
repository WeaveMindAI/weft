// A browser consent, start to finish: begin it, open the provider's page,
// then ask for the outcome every couple of seconds until the callback has
// landed, the provider refused, or the person stops waiting (a sign-in is
// the person's own pace, so there is no deadline). A poll the server
// refuses throws, which ends the wait: a sign-in that expired or is
// unknown answers 410, and its message says to start again.

import type { ConnectTransport, ConsentStart } from './transport';
import type { GrantSummary } from './wire';

export interface ConsentWait {
	/** Stop waiting (the person closed the page, or chose something
	 *  else); a stopped wait answers `null`. */
	stopped(): boolean;
	/** Called once the provider's page is open and the wait starts. */
	onWaiting?(): void;
	/** Milliseconds between polls; 2000 by default. */
	intervalMs?: number;
	sleep?(ms: number): Promise<void>;
}

/** The connection a consent ended with, or `null` when the wait was
 *  stopped. A refused consent throws, naming why. */
export async function runConsent(
	transport: ConnectTransport,
	request: ConsentStart,
	wait: ConsentWait,
): Promise<GrantSummary | null> {
	const started = await transport.beginConsent(request);
	transport.openExternal(started.consent_url);
	wait.onWaiting?.();
	const sleep = wait.sleep ?? ((ms: number) => new Promise<void>((r) => setTimeout(r, ms)));
	for (;;) {
		await sleep(wait.intervalMs ?? 2000);
		if (wait.stopped()) return null;
		const outcome = await transport.consentOutcome(started.state);
		if (wait.stopped()) return null;
		if (!outcome) continue;
		if (outcome.error) throw new Error(outcome.error);
		if (outcome.grant) return outcome.grant;
	}
}
