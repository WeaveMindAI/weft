import { describe, expect, it } from 'vitest';
import { copiedPortSigs, headerPortSigs, headerWorthy, partitionRemovals, portDeleteAction, removedPortNames } from './header-ports';
import { SHOULD_FLOW_PORT } from '../../../protocol';

// The bug this pins: adding one custom output through the editor once
// wrote the node type's ENTIRE resolved signature into source,
// `_should_flow` and MustOverride placeholders included; and a naive
// rendered-vs-default type compare would freeze inference-resolved
// generics into the header as if the author wrote them.

const defaults = [
	{ name: 'data', required: false, portType: 'T' },
	{ name: 'response', required: false, portType: 'MustOverride' },
];

const none = { sigs: [], reverted: [] };

describe('headerPortSigs', () => {
	it('an untouched catalog port is not written', () => {
		const p = [{ name: 'data', required: false, portType: 'T' }];
		expect(headerPortSigs(p, p, defaults)).toEqual(none);
	});

	it('an inference-resolved generic is NOT mistaken for an override', () => {
		// Catalog says `data: T`; wired to a String it renders as String,
		// with no declaredType because the header never declared it.
		const p = [{ name: 'data', required: false, portType: 'String' }];
		expect(headerPortSigs(p, p, defaults)).toEqual(none);
	});

	it('a declared override round-trips its DECLARED type, not the rendered one', () => {
		const p = [{ name: 'response', required: false, portType: 'MyThing', declaredType: 'JsonDict' }];
		expect(headerPortSigs(p, p, defaults)).toEqual({
			sigs: [{ name: 'response', required: false, portType: 'JsonDict', rendered: 'MyThing' }],
			reverted: [],
		});
	});

	it('a gesture retype is written with the new type, even on a catalog port', () => {
		const prev = [{ name: 'data', required: false, portType: 'T' }];
		const next = [{ name: 'data', required: false, portType: 'Number' }];
		expect(headerPortSigs(prev, next, defaults)).toEqual({
			sigs: [{ name: 'data', required: false, portType: 'Number', rendered: 'Number' }],
			reverted: [],
		});
	});

	it('a required-only toggle keeps the CATALOG type, never the rendered one', () => {
		// The port renders inference-resolved (`String`), but only its
		// requiredness changed: writing the rendered type would freeze
		// the generic into source. The rendered value still rides along
		// so the projection keeps showing it.
		const prev = [{ name: 'data', required: false, portType: 'String' }];
		const next = [{ name: 'data', required: true, portType: 'String' }];
		expect(headerPortSigs(prev, next, defaults)).toEqual({
			sigs: [{ name: 'data', required: true, portType: 'T', rendered: 'String' }],
			reverted: [],
		});
	});

	it('a required-only toggle on a DECLARED port keeps the declared spelling', () => {
		const prev = [{ name: 'x', required: false, portType: 'String', declaredType: 'T' }];
		const next = [{ name: 'x', required: true, portType: 'String', declaredType: 'T' }];
		expect(headerPortSigs(prev, next, defaults)).toEqual({
			sigs: [{ name: 'x', required: true, portType: 'T', rendered: 'String' }],
			reverted: [],
		});
	});

	it('reverting to the catalog default UN-declares the port instead of freezing it', () => {
		// Toggle required on (previous gesture declared it, apply stamped
		// declaredType), then toggle it back off: the port now restates
		// its default exactly, so it leaves the header rather than
		// staying as a frozen `data: T` line. The revert is REPORTED so
		// the optimistic projection lands on the reparse's surface.
		const prev = [{ name: 'data', required: true, portType: 'String', declaredType: 'T' }];
		const next = [{ name: 'data', required: false, portType: 'String', declaredType: 'T' }];
		expect(headerPortSigs(prev, next, defaults)).toEqual({
			sigs: [],
			reverted: [{ name: 'data', required: false, portType: 'String' }],
		});
	});

	it('retyping back to the catalog default un-declares too', () => {
		const prev = [{ name: 'data', required: false, portType: 'Number', declaredType: 'Number' }];
		const next = [{ name: 'data', required: false, portType: 'T', declaredType: 'Number' }];
		expect(headerPortSigs(prev, next, defaults)).toEqual({
			sigs: [],
			reverted: [{ name: 'data', required: false, portType: 'T' }],
		});
	});

	it('a newly added custom port is written', () => {
		const next = [{ name: 'haiku', required: false, portType: 'String' }];
		expect(headerPortSigs([], next, defaults)).toEqual({
			sigs: [{ name: 'haiku', required: false, portType: 'String', rendered: 'String' }],
			reverted: [],
		});
	});

	it('a polluted header (declares its catalog defaults verbatim) heals to empty', () => {
		const p = [
			{ name: 'data', required: false, portType: 'T', declaredType: 'T' },
			{ name: 'response', required: false, portType: 'MustOverride', declaredType: 'MustOverride' },
			{ name: SHOULD_FLOW_PORT, required: false, portType: 'T__should_flow', declaredType: 'T__should_flow' },
		];
		expect(headerPortSigs(p, p, defaults)).toEqual({
			sigs: [],
			reverted: [
				{ name: 'data', required: false, portType: 'T' },
				{ name: 'response', required: false, portType: 'MustOverride' },
			],
		});
	});

	it('a declared custom port whose generic got inference-resolved round-trips the authored generic', () => {
		const p = [{ name: 'x', required: true, portType: 'String', declaredType: 'T' }];
		expect(headerPortSigs(p, p, defaults)).toEqual({
			sigs: [{ name: 'x', required: true, portType: 'T', rendered: 'String' }],
			reverted: [],
		});
	});

	it('with no defaults known, a declared port still round-trips and an undeclared one stays out', () => {
		const p = [
			{ name: 'a', required: true, portType: 'String', declaredType: 'String' },
			{ name: 'b', required: true, portType: 'String' },
		];
		expect(headerPortSigs(p, p, undefined)).toEqual({
			sigs: [{ name: 'a', required: true, portType: 'String', rendered: 'String' }],
			reverted: [],
		});
	});
});

describe('copiedPortSigs', () => {
	it('a fresh copy renders its DECLARED spelling, not the original instance-resolved type', () => {
		// The original declares `x: T` and is wired, so it renders String;
		// the copy carries no wires, so its reparse renders `T`, and the
		// copy op must say so or the new node refuses wires the
		// declaration accepts until the round-trip lands.
		const ports = [{ name: 'x', required: true, portType: 'String', declaredType: 'T' }];
		expect(copiedPortSigs(ports, defaults)).toEqual([
			{ name: 'x', required: true, portType: 'T', rendered: 'T' },
		]);
	});
});

describe('partitionRemovals', () => {
	it('routes a provided name to a revert and a custom name to a removal', () => {
		// Deleting a PROVIDED port (catalog default or config-derived)
		// must not kill its wires: the next parse re-creates the port,
		// so the gesture reverts the override instead. A custom port
		// truly goes.
		// A GENERIC provided spelling (`T`, `T__key`) is dropped from the
		// revert: the projection's rendered type is the best answer until
		// the reparse re-runs inference. A CONCRETE spelling rides along,
		// or the projection would keep showing the deleted override's
		// type and the next gesture would read it as truth.
		expect(partitionRemovals(['data', 'haiku'], defaults)).toEqual({
			removed: ['haiku'],
			reverted: [{ name: 'data', required: false }],
		});
		const provided = [...defaults, { name: 'urgent', required: false, portType: 'Boolean' }];
		expect(partitionRemovals(['urgent'], provided)).toEqual({
			removed: [],
			reverted: [{ name: 'urgent', required: false, portType: 'Boolean' }],
		});
	});
});

describe('headerWorthy', () => {
	it('keeps a config-exposure input only when the source header declares it, never a carry ghost', () => {
		expect(headerWorthy({ name: 'title', exposure: 'config' })).toBe(false);
		expect(headerWorthy({ name: 'title', exposure: 'config', declaredType: 'String' })).toBe(true);
		expect(headerWorthy({ name: 'acc', synthesizedFromCarry: true })).toBe(false);
		expect(headerWorthy({ name: 'data', exposure: 'all' })).toBe(true);
	});
});

describe('declared config-exposure lines', () => {
	it('round-trip even when they restate the catalog default (the gate exempts config ports)', () => {
		// `HumanTrigger(title: String)` over a catalog config `title:
		// String`: the parser keeps the line with a diagnostic, and an
		// unrelated gesture must not delete it; the diagnostic is the
		// way out.
		const catalog = [{ name: 'title', required: false, portType: 'String', exposure: 'config' as const }];
		const p = [{ name: 'title', required: false, portType: 'String', declaredType: 'String', exposure: 'config' as const }];
		expect(headerPortSigs(p, p, catalog)).toEqual({
			sigs: [{ name: 'title', required: false, portType: 'String', rendered: 'String' }],
			reverted: [],
		});
	});
});

describe('partitionRemovals typing', () => {
	it('omits a nested generic, keeps MustOverride, omits an unparseable spelling', () => {
		const provided = [
			{ name: 'items', required: true, portType: 'List[T]' },
			{ name: 'response', required: false, portType: 'MustOverride' },
			{ name: 'odd', required: true, portType: 'Strng' },
		];
		expect(partitionRemovals(['items', 'response', 'odd'], provided).reverted).toEqual([
			{ name: 'items', required: true },
			{ name: 'response', required: false, portType: 'MustOverride' },
			{ name: 'odd', required: true },
		]);
	});
});

describe('shadow declarations', () => {
	it('a declared line over a config-derived port round-trips even when it restates the derived shape', () => {
		// The restates-the-default gate heals only against CATALOG
		// defaults; a derived port never reaches it (the caller passes
		// defaults alone), so a hand-authored shadow line is the
		// author's record and survives every unrelated gesture.
		const p = [{ name: 'photo', required: true, portType: 'Image', declaredType: 'Image' }];
		expect(headerPortSigs(p, p, defaults).sigs).toEqual([
			{ name: 'photo', required: true, portType: 'Image', rendered: 'Image' },
		]);
	});
});

describe('portDeleteAction', () => {
	it('reverts overrides on ANY node type, removes custom ports only where allowed, never offers on bare catalog ports', () => {
		// A bare catalog port: nothing to offer (the catalog re-creates it).
		expect(portDeleteAction({ name: 'data', portType: 'T' }, defaults, true)).toBeNull();
		// An override reverts, even on a node type with no custom-port
		// support: gating it there would leave a mistyped override with
		// no way back.
		expect(portDeleteAction({ name: 'data', portType: 'Number', declaredType: 'Number' }, defaults, false)).toBe('revert');
		// A custom port truly goes, where the type accepts custom ports.
		expect(portDeleteAction({ name: 'notes', portType: 'String' }, defaults, true)).toBe('remove');
		expect(portDeleteAction({ name: 'notes', portType: 'String' }, defaults, false)).toBeNull();
		// A DECLARED line is always deletable: a stale override whose
		// port left the catalog must not get stuck on a node type that
		// forbids custom ports.
		expect(portDeleteAction({ name: 'gone', portType: 'String', declaredType: 'String' }, defaults, false)).toBe('remove');
	});
});

describe('removedPortNames', () => {
	it('names only the ports that left the list', () => {
		const prev = [{ name: 'a' }, { name: 'b' }, { name: 'c' }];
		const next = [{ name: 'a' }, { name: 'c' }];
		expect(removedPortNames(prev, next)).toEqual(['b']);
	});

	it('an unchanged list removes nothing', () => {
		const prev = [{ name: 'a' }];
		expect(removedPortNames(prev, prev)).toEqual([]);
	});

	it('_should_flow is never removable, even off a filtered list', () => {
		const prev = [{ name: SHOULD_FLOW_PORT }, { name: 'a' }];
		expect(removedPortNames(prev, [])).toEqual(['a']);
	});
});
