import { describe, expect, it } from 'vitest';

import type { SkipReason } from '../../../protocol';
import { skipReasonText } from './status';

/// Every reason a firing did not run has to read as a sentence a person
/// can act on, and the mapping has to be exhaustive: a kind the webview
/// has never heard of must say so rather than render as blank, because
/// a blank reason in the inspector reads as "no reason recorded" and
/// sends somebody looking for a bug that is not there.
describe('skipReasonText', () => {
	const cases: [SkipReason, string][] = [
		[{ kind: 'did_not_flow' }, 'its `_should_flow` said no'],
		[{ kind: 'flow_closed' }, 'nothing ever answered its `_should_flow`'],
		[{ kind: 'did_flow' }, 'its `_should_not_flow` saw a value'],
		[{ kind: 'required_input_closed', port: 'value' }, "the required input 'value' closed"],
		[{ kind: 'every_input_closed' }, 'every input closed'],
		[
			{ kind: 'one_of_group_closed', ports: ['a', 'b'] },
			'every input of the group (a, b) closed',
		],
		[{ kind: 'scope_skipped', scope: 'triage' }, "the scope 'triage' it lives in did not run"],
	];

	it.each(cases)('renders %j', (reason, expected) => {
		expect(skipReasonText(reason)).toBe(expected);
	});

	/// The two gates read differently on purpose: the inspector has to
	/// say WHICH decision turned the node off, or a graph using both is
	/// unreadable.
	it('tells the two gates apart', () => {
		expect(skipReasonText({ kind: 'did_not_flow' })).not.toBe(
			skipReasonText({ kind: 'did_flow' }),
		);
	});

	it('says so honestly when nothing was recorded', () => {
		expect(skipReasonText(undefined)).toBe('reason not recorded');
	});

	/// A dispatcher newer than this webview can send a kind it has never
	/// heard of. That is not a crash and not a blank.
	it('survives a kind from a newer dispatcher', () => {
		const future = { kind: 'something_new' } as unknown as SkipReason;
		expect(skipReasonText(future)).toBeTruthy();
	});
});
