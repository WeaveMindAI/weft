// What the action bar offers. Two cases carry the weight: a project
// whose registration is older than the source in front of the user
// (the dispatcher answers from the definition it last registered, so a
// database added since the last run is invisible to it), and a project
// in the middle of something, where the dispatcher's word is the only
// one that counts.

import { describe, it, expect } from 'vitest';
import { isVerbOffered, sourceInfraReady, transitional, type VerbInputs } from './verb-gates';

function inputs(over: Partial<VerbInputs> = {}): VerbInputs {
	return {
		available: [],
		hasInfra: false,
		hasTriggers: false,
		infraRollup: 'none',
		status: 'registered',
		infraBusy: false,
		...over,
	};
}

describe('the dispatcher table', () => {
	it('offers whatever it lists', () => {
		const i = inputs({ available: ['deactivate', 'infra_stop'] });
		expect(isVerbOffered('deactivate', i)).toBe(true);
		expect(isVerbOffered('infra_stop', i)).toBe(true);
	});

	it('is the only word on a verb that does not register anything', () => {
		// Nothing about the source can conjure a Stop: there has to be
		// something live to stop, and only the dispatcher knows that.
		const i = inputs({ hasInfra: true, hasTriggers: true });
		expect(isVerbOffered('infra_stop', i)).toBe(false);
		expect(isVerbOffered('infra_terminate', i)).toBe(false);
		expect(isVerbOffered('deactivate', i)).toBe(false);
		expect(isVerbOffered('resync', i)).toBe(false);
	});
});

describe('a project in the middle of something', () => {
	// The dispatcher collapses its whole table to one cancel verb here,
	// and the source cannot know it. Another window (or the CLI) started
	// the operation, so there is no local overlay to gate on either.
	it('offers no starter verb while it activates', () => {
		const i = inputs({
			available: ['cancel_activate'],
			status: 'activating',
			hasInfra: true,
			hasTriggers: true,
		});
		expect(transitional(i)).toBe(true);
		expect(isVerbOffered('infra_start', i)).toBe(false);
		expect(isVerbOffered('run', i)).toBe(false);
		expect(isVerbOffered('activate', i)).toBe(false);
		// The cancel the dispatcher DID name is still offered.
		expect(isVerbOffered('cancel_activate', i)).toBe(true);
	});

	it('offers no starter verb while it drains', () => {
		const i = inputs({ available: ['cancel_running'], status: 'deactivating', hasTriggers: true });
		expect(isVerbOffered('run', i)).toBe(false);
		expect(isVerbOffered('activate', i)).toBe(false);
	});

	it('offers no starter verb in the window the rollup cannot see', () => {
		// `infraBusy` exists for exactly this: a claimed stop still
		// draining reads as `stopped`, and Start Infra would only 409.
		const busy = inputs({ hasInfra: true, infraRollup: 'stopped', infraBusy: true });
		expect(transitional(busy)).toBe(true);
		expect(isVerbOffered('infra_start', busy)).toBe(false);
		const rested = inputs({ hasInfra: true, infraRollup: 'stopped' });
		expect(isVerbOffered('infra_start', rested)).toBe(true);
	});

	it('offers no starter verb while the infra itself is moving', () => {
		for (const infraRollup of ['provisioning', 'stopping', 'terminating'] as const) {
			const i = inputs({ hasInfra: true, infraRollup });
			expect(transitional(i)).toBe(true);
			expect(isVerbOffered('infra_start', i)).toBe(false);
		}
	});
});

describe('a project the dispatcher has never registered', () => {
	it('can start the infra its source declares', () => {
		expect(isVerbOffered('infra_start', inputs({ hasInfra: true }))).toBe(true);
	});

	it('can run and activate', () => {
		const i = inputs({ hasTriggers: true });
		expect(isVerbOffered('run', i)).toBe(true);
		expect(isVerbOffered('activate', i)).toBe(true);
	});

	it('still offers Activate with its infra down', () => {
		// Which infra a trigger DEPENDS ON is a walk the dispatcher does
		// and the editor does not, so the bar offering it on the coarse
		// whole-graph answer would grey out an activate the dispatcher
		// accepts. The door refuses with the nodes to start named.
		const down = inputs({ hasInfra: true, hasTriggers: true, infraRollup: 'stopped' });
		expect(isVerbOffered('activate', down)).toBe(true);
		expect(sourceInfraReady(down)).toBe(false);
	});

	it('is offered no trigger verb when the source has no trigger', () => {
		expect(isVerbOffered('activate', inputs())).toBe(false);
		expect(isVerbOffered('reactivate', inputs())).toBe(false);
	});
});

describe('a registration older than the source', () => {
	// The bug this fallback fixes: the user ran the project once, then
	// wrote a database into it. The dispatcher still answers from the
	// definition of that run, so its table names only `run` and there is
	// no way to bring the database up.
	const stale = inputs({ available: ['run'], hasInfra: true, infraRollup: 'none' });

	it('still offers Start Infra, because starting it registers the source', () => {
		expect(isVerbOffered('infra_start', stale)).toBe(true);
	});

	it('reports the run as not ready, because the new infra is down', () => {
		// `run` stays in the table, and it is the bar's own infra gate
		// (applied at the Run slot) that holds it back.
		expect(isVerbOffered('run', stale)).toBe(true);
		expect(sourceInfraReady(stale)).toBe(false);
	});

	it('offers Activate for a trigger the dispatcher has never seen', () => {
		const withTrigger = inputs({ available: ['run'], hasTriggers: true });
		expect(isVerbOffered('activate', withTrigger)).toBe(true);
	});
});

describe('infra the user deleted from source', () => {
	it('is never offered a start, because there is no spec to start from', () => {
		// The live rows keep Stop and Terminate offered (the dispatcher
		// lists them); Start would have nothing to provision.
		const orphan = inputs({
			available: ['infra_stop', 'infra_terminate'],
			hasInfra: false,
			infraRollup: 'running',
		});
		expect(isVerbOffered('infra_start', orphan)).toBe(false);
		expect(isVerbOffered('infra_terminate', orphan)).toBe(true);
	});
});

describe('the infra gate on the source', () => {
	it('is open when the source declares no infra at all', () => {
		expect(sourceInfraReady(inputs({ infraRollup: 'none' }))).toBe(true);
	});

	it('is open only on a full running rollup', () => {
		for (const rollup of ['none', 'stopped', 'partial', 'failed', 'flaky', 'provisioning'] as const) {
			expect(sourceInfraReady(inputs({ hasInfra: true, infraRollup: rollup }))).toBe(false);
		}
		expect(sourceInfraReady(inputs({ hasInfra: true, infraRollup: 'running' }))).toBe(true);
	});
});
