import { describe, expect, it } from 'vitest';
import { buildPortMenuItems, type BasePortMenuOptions, type PortMenuItem, type PortMenuSide } from './port-context-menu';
import type { PortDefinition } from '../types';

// Layer-1 coverage of the menu BUILDER alone (the DOM renderer is not
// exercised here): which rows a port gets, and that informational
// rows are notes with no action.

const port: PortDefinition = { name: 'data', portType: 'String', required: false };
const asInput: PortMenuSide = { side: 'input', onSetRequired: () => {} };

function build(over: Partial<BasePortMenuOptions> = {}, side: PortMenuSide = asInput): PortMenuItem[] {
	return buildPortMenuItems({
		port,
		deleteAction: null,
		onSetType: () => {},
		onRemove: () => {},
		...over,
		...side,
	});
}

const labels = (items: PortMenuItem[]) => items.map((i) => i.label);
const notes = (items: PortMenuItem[]) => items.filter((i) => i.note === true);
const actions = (items: PortMenuItem[]) => items.filter((i) => i.note !== true);

describe('buildPortMenuItems', () => {
	it('a plain input offers the required toggle and the type editor, and no delete without an action', () => {
		const items = build();
		expect(labels(items)).toEqual(['☑ Make required', '✎ Type: String']);
		expect(notes(items)).toEqual([]);
		const typeRow = items[1];
		expect(typeRow.note).not.toBe(true);
		if (typeRow.note !== true) expect(typeRow.editable?.value).toBe('String');
	});

	it('the required row writes the value its label promises, never a live toggle', () => {
		// The menu is a frozen snapshot: if a reparse flipped the port
		// under the open menu, a toggle would write the opposite of the
		// row's text. The row asks for exactly the promised state.
		const written: boolean[] = [];
		const items = build({}, { side: 'input', onSetRequired: (r) => written.push(r) });
		const row = items[0];
		if (row.note !== true) row.onClick();
		expect(written).toEqual([true]);
	});

	it('a loop name collision keeps the delete handle beside its reason', () => {
		const items = build({
			deleteAction: 'remove',
			loopRole: { currentRole: 'name_collision', conflictReason: 'an input of the same name conflicts', onToggleRole: () => {} },
		});
		expect(labels(items)).toContain('Remove port');
		expect(labels(items)).toContain('an input of the same name conflicts');
	});

	it('an output has no required toggle', () => {
		expect(labels(build({}, { side: 'output' }))).toEqual(['✎ Type: String']);
	});

	it('labels the delete row by what it will DO', () => {
		expect(labels(build({ deleteAction: 'remove' }))).toContain('Remove port');
		expect(labels(build({ deleteAction: 'revert' }))).toContain('Reset to default');
	});

	it('a config-derived port gets only the explainer, never edits or delete', () => {
		const items = build({ configDerived: true, deleteAction: 'remove' });
		expect(items).toHaveLength(1);
		expect(items[0].note).toBe(true);
		expect(actions(items)).toEqual([]);
	});

	it('a synthesized carry input gets two notes and no action', () => {
		const items = build({
			loopRole: { currentRole: 'synthesized_carry_input', onToggleRole: () => {} },
		});
		expect(items).toHaveLength(2);
		expect(actions(items)).toEqual([]);
	});

	it('every note carries no handler and every action carries one', () => {
		const items = build({
			deleteAction: 'revert',
			loopRole: { currentRole: 'carry', conflictReason: 'a same-named input conflicts', onToggleRole: () => {} },
		});
		for (const item of items) {
			if (item.note === true) {
				expect('onClick' in item).toBe(false);
			} else {
				expect(typeof item.onClick).toBe('function');
			}
		}
		expect(notes(items).length).toBeGreaterThan(0);
	});
});
