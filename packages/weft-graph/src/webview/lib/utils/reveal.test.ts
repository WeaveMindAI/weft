import { describe, expect, it } from 'vitest';
import { revealPlan } from './reveal';

/** `catalog/computer/agent.weft` as the editor draws it: the file's own
 *  group, the `turns` loop, the `calls` loop inside it, and `act`, the
 *  `@include` of perform.weft, one box. */
const agent = [
	{ id: '@agent' },
	{ id: '@agent.turns', parentId: '@agent' },
	{ id: '@agent.turns.ask', parentId: '@agent.turns' },
	{ id: '@agent.turns.calls', parentId: '@agent.turns' },
	{ id: '@agent.turns.calls.act', parentId: '@agent.turns.calls' },
];

/** `catalog/computer/perform.weft` opened on its own: `screen` is a group
 *  in it, `shot` a catalog node inside. */
const perform = [
	{ id: '@perform' },
	{ id: '@perform.screen', parentId: '@perform' },
	{ id: '@perform.screen.shot', parentId: '@perform.screen' },
];

describe('revealPlan', () => {
	it('opens every group a loop sits in and selects it (the model\'s tools come from calls.tools)', () => {
		expect(revealPlan(agent, '@agent.turns.calls')).toEqual({ open: ['@agent', '@agent.turns'], select: '@agent.turns.calls' });
	});

	it('selects an include\'s box for the include itself (calls takes its tools from act.tools)', () => {
		expect(revealPlan(agent, '@agent.turns.calls.act')).toEqual({
			open: ['@agent', '@agent.turns', '@agent.turns.calls'],
			select: '@agent.turns.calls.act',
		});
	});

	it('takes a node inside an include to the include\'s box, which is all the editor draws of it', () => {
		expect(revealPlan(agent, '@agent.turns.calls.act.screen.shot')?.select).toBe('@agent.turns.calls.act');
	});

	it('follows a chain down inside a file opened on its own, to a catalog node', () => {
		expect(revealPlan(perform, '@perform.screen')).toEqual({ open: ['@perform'], select: '@perform.screen' });
		expect(revealPlan(perform, '@perform.screen.shot')).toEqual({ open: ['@perform', '@perform.screen'], select: '@perform.screen.shot' });
	});

	it('selects a top-level node with nothing to open', () => {
		expect(revealPlan([{ id: 'dogAgent' }], 'dogAgent')).toEqual({ open: [], select: 'dogAgent' });
	});

	it('has no plan for an id nothing drawn holds', () => {
		expect(revealPlan(agent, 'elsewhere.node')).toBeNull();
	});
});
