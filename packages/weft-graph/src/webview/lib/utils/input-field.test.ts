import { describe, it, expect } from 'vitest';
import { fieldForInput, clampToRange, hasUnpickedAccess, inputRendersField, nextPortLiterals, shouldFlowField } from './input-field';
import { SHOULD_FLOW_PORT } from '../../../protocol';
import { acceptsLiteral, acceptsWire } from '../types';
import type { PortDefinition } from '../types';

/** The regression class this file pins: an input's resolved widget is
 *  the ONLY source of its editor field, and every widget payload member
 *  must survive the flattening (the old field pipeline dropped `language`
 *  entirely and let `default_value` die unread). */
describe('fieldForInput', () => {
	const base = (over: Partial<PortDefinition>): PortDefinition => ({
		name: 'x',
		portType: 'String',
		required: false,
		accepts: ['literal', 'wire'],
		widget: { kind: 'textarea' },
		...over,
	});

	it('maps each widget kind and carries its payload', () => {
		const code = fieldForInput(base({ widget: { kind: 'code', language: 'javascript' } }));
		expect(code.type).toBe('code');
		expect(code.language).toBe('javascript');

		const num = fieldForInput(base({ portType: 'Number', widget: { kind: 'number', min: 0, max: 2, step: 0.1 } }));
		expect(num.type).toBe('number');
		expect([num.min, num.max, num.step]).toEqual([0, 2, 0.1]);

		const sel = fieldForInput(base({ widget: { kind: 'select', options: ['GET', 'POST'] } }));
		expect(sel.type).toBe('select');
		expect(sel.options).toEqual(['GET', 'POST']);

		const drop = fieldForInput(base({ portType: 'Image', widget: { kind: 'file_drop', type: 'Image', accept: 'image/png' } }));
		expect(drop.type).toBe('file_drop');
		expect(drop.fileType).toBe('Image');
		expect(drop.accept).toBe('image/png');
	});

	it('every input field edits the port literal, the one home a port has', () => {
		expect(fieldForInput(base({ accepts: ['literal', 'wire'] })).portDriven).toBe(true);
		expect(fieldForInput(base({ accepts: ['literal'] })).portDriven).toBe(true);
		expect(fieldForInput(base({ accepts: undefined })).portDriven).toBe(true);
	});

	it('carries default, label, and placeholder', () => {
		const f = fieldForInput(base({ default: 'GET', label: 'Method', placeholder: 'pick one' }));
		expect(f.defaultValue).toBe('GET');
		expect(f.label).toBe('Method');
		expect(f.placeholder).toBe('pick one');
	});

	it('falls back to a textarea for a not-yet-round-tripped port', () => {
		const f = fieldForInput(base({ widget: undefined }));
		expect(f.type).toBe('textarea');
	});

	it('flattens the access widget with its compiler-stamped service', () => {
		const f = fieldForInput(
			base({ accepts: ['literal'], widget: { kind: 'access', service: 'slack' } }),
		);
		expect(f.type).toBe('access');
		expect(f.service).toBe('slack');
		expect(f.portDriven).toBe(true);
	});

	it('flattens the remote_select widget with its sources and parents', () => {
		const f = fieldForInput(
			base({
				widget: {
					kind: 'remote_select',
					access: 'account',
					sources: [
						{ kind: 'granted', from: 'repositories' },
						{ kind: 'list', get: 'https://x/list?q={query}', items: 'channels', label: 'name', value: 'id',
							page: { cursor_param: 'cursor', cursor_path: 'meta.next' } },
						{ kind: 'from_url', pattern: 'x\\.com/([^/]+)' },
					],
					depends_on: ['repo'],
				},
			}),
		);
		expect(f.type).toBe('remote_select');
		expect(f.access).toBe('account');
		expect(f.sources?.length).toBe(3);
		expect(f.sources?.[0]).toEqual({ kind: 'granted', from: 'repositories' });
		expect(f.dependsOn).toEqual(['repo']);
	});
});

describe('accepts', () => {
	it('trusts the resolved list and reads an absent one as both', () => {
		const wireOnly = { name: 'a', portType: 'String', required: false, accepts: ['wire'] as const };
		expect(acceptsWire(wireOnly)).toBe(true);
		expect(acceptsLiteral(wireOnly)).toBe(false);
		const literalOnly = { name: 'a', portType: 'String', required: false, accepts: ['literal'] as const };
		expect(acceptsWire(literalOnly)).toBe(false);
		expect(acceptsLiteral(literalOnly)).toBe(true);
		// A locally-added port pre-round-trip carries no list: both.
		const fresh = { name: 'a', portType: 'MustOverride', required: false };
		expect(acceptsWire(fresh)).toBe(true);
		expect(acceptsLiteral(fresh)).toBe(true);
	});

	it('an input that takes no written value renders no field', () => {
		const wireOnly = { name: 'a', portType: 'String', required: false, accepts: ['wire'] as const };
		expect(inputRendersField(wireOnly, { wired: false, hasWrittenValue: false })).toBe(false);
		const both = { name: 'a', portType: 'String', required: false };
		expect(inputRendersField(both, { wired: false, hasWrittenValue: false })).toBe(true);
	});
});

describe('clampToRange', () => {
	it('clamps to the declared bounds and passes in-range values through', () => {
		expect(clampToRange(5, 0, 2)).toBe(2);
		expect(clampToRange(-1, 0, 2)).toBe(0);
		expect(clampToRange(1.5, 0, 2)).toBe(1.5);
		expect(clampToRange(99, undefined, undefined)).toBe(99);
		expect(clampToRange(-5, 0, undefined)).toBe(0);
	});
});

/** `_should_flow` is the language's own port: it is answered by a wire
 *  and docks in the node's corner, so it must not sit in the body among
 *  the node's settings. A value written into the source is the one case
 *  it has to show, because otherwise there is no way to see or undo it. */
describe('inputRendersField', () => {
	const input = (over: Partial<PortDefinition>): PortDefinition => ({
		name: 'x',
		portType: 'String',
		required: false,
		exposure: 'all',
		widget: { kind: 'textarea' },
		...over,
	});
	const flow = input({ name: SHOULD_FLOW_PORT, portType: 'T__should_flow' });

	it('hides _should_flow when nothing wrote a value for it', () => {
		expect(inputRendersField(flow, { wired: false, hasWrittenValue: false })).toBe(false);
	});

	it('shows _should_flow when the source wrote one', () => {
		expect(inputRendersField(flow, { wired: false, hasWrittenValue: true })).toBe(true);
	});

	it('hides _should_flow driven by a wire, written value or not', () => {
		expect(inputRendersField(flow, { wired: true, hasWrittenValue: false })).toBe(false);
		expect(inputRendersField(flow, { wired: true, hasWrittenValue: true })).toBe(false);
	});

	it('keeps the ordinary rules for every other input', () => {
		expect(inputRendersField(input({}), { wired: false, hasWrittenValue: false })).toBe(true);
		expect(inputRendersField(input({}), { wired: true, hasWrittenValue: false })).toBe(false);
		expect(
			inputRendersField(input({ accepts: ['wire'] }), { wired: false, hasWrittenValue: true }),
		).toBe(false);
		expect(
			inputRendersField(input({ synthesizedFromCarry: true }), { wired: false, hasWrittenValue: false }),
		).toBe(false);
	});

	it('draws the same control for a node and a container', () => {
		expect(shouldFlowField('node').type).toBe('checkbox');
		expect(shouldFlowField('node').portDriven).toBe(true);
		expect(shouldFlowField('container').description).toContain('inside it');
	});
});

/** One emptiness rule for the port-literal writer: null, undefined,
 *  '' and [] all DELETE the key (a multiselect deselect-all and a
 *  text_list with its last row removed must clear the literal, not
 *  strand a phantom [] in the source), and any other value lands. */
describe('nextPortLiterals', () => {
	it('deletes the key on every empty shape', () => {
		for (const empty of [null, undefined, '', []]) {
			expect(nextPortLiterals({ tags: ['a'] }, 'tags', empty)).toEqual({});
		}
	});

	it('stores any non-empty value and leaves other keys alone', () => {
		expect(nextPortLiterals({ other: 1 }, 'tags', ['a'])).toEqual({ other: 1, tags: ['a'] });
		expect(nextPortLiterals({}, 'flag', false)).toEqual({ flag: false });
		expect(nextPortLiterals({}, 'n', 0)).toEqual({ n: 0 });
	});
});

/** The pin that keeps an unconnected access node open: read by both the
 *  node renderer (chevron/toggle) and buildNodes' `expanded` overlay, so
 *  what it answers decides the drawn state AND the computed sizing. */
describe('hasUnpickedAccess', () => {
	const none = new Set<string>();
	const access = (over: Partial<PortDefinition> = {}): PortDefinition => ({
		name: 'connection',
		portType: 'Access',
		required: false,
		// The picker is compiler-read: an inline value only, homed in
		// the port literals like every port's constant.
		accepts: ['literal'],
		widget: { kind: 'access', service: 'openrouter' },
		...over,
	});

	it('pins a node whose access field has no value', () => {
		expect(hasUnpickedAccess([access()], {}, none)).toBe(true);
	});

	it('unlocks once a connection handle is stored', () => {
		const literals = { connection: { id: 'g-1', identity: 'Quentin' } };
		expect(hasUnpickedAccess([access()], literals, none)).toBe(false);
	});

	it('never pins an optional connection (the node runs without one)', () => {
		const optional = access({ widget: { kind: 'access', service: 'custom', optional: true } });
		expect(hasUnpickedAccess([optional], {}, none)).toBe(false);
	});

	it('treats an explicit null (a disconnect) as unpicked', () => {
		expect(hasUnpickedAccess([access()], { connection: null }, none)).toBe(true);
	});

	it('ignores a wired access input (the edge is the driver)', () => {
		expect(hasUnpickedAccess([access()], {}, new Set(['connection']))).toBe(false);
	});

	it('ignores nodes with no access field at all', () => {
		const plain: PortDefinition = {
			name: 'prompt', portType: 'String', required: false, accepts: ['literal', 'wire'],
			widget: { kind: 'textarea' },
		};
		expect(hasUnpickedAccess([plain], {}, none)).toBe(false);
	});

	it('the picker field edits the port literal like every input', () => {
		// The invariant the predicate's read leans on: a port's constant
		// has one home, so the handle can only ever live there.
		expect(fieldForInput(access()).portDriven).toBe(true);
	});
});
