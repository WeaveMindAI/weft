import { describe, expect, it } from 'vitest';
import { containerInputRequiredness, containerSpelledRequired, copiedPortSigs, declarationProblem, declarationRule, headerPortSigs, headerWorthy, nodeInputRequiredness, partitionRemovals, portDeleteAction, removedPortNames } from './header-ports';
import { ERROR_PORT, SHOULD_FLOW_PORT } from '../../../protocol';

// The bug this pins: adding one custom output through the editor once
// wrote the node type's ENTIRE resolved signature into source,
// `_should_flow` and placeholder types included; and a naive
// rendered-vs-default type compare would freeze inference-resolved
// generics into the header as if the author wrote them.

const defaults = [
	{ name: 'data', required: false, portType: 'T' },
	{ name: 'response', required: false, portType: 'T__response' },
	{ name: 'prompt', required: true, portType: 'String' },
];

const none = { sigs: [], reverted: [] };
const unwired = new Set<string>();
const wired = (...names: string[]) => new Set(names);

describe('headerPortSigs', () => {
	it('an untouched catalog port is not written', () => {
		const p = [{ name: 'data', required: false, portType: 'T' }];
		expect(headerPortSigs(p, p, defaults, unwired)).toEqual(none);
		expect(headerPortSigs(p, p, defaults, wired('data'))).toEqual(none);
	});

	it('an inference-resolved generic is NOT mistaken for an override', () => {
		// Catalog says `data: T`; wired to a String it renders as String,
		// with no declaredType because the header never declared it.
		const p = [{ name: 'data', required: false, portType: 'String' }];
		expect(headerPortSigs(p, p, defaults, wired('data'))).toEqual(none);
	});

	it('a declared override round-trips its DECLARED type, not the rendered one', () => {
		const p = [{ name: 'response', required: false, optional: true, portType: 'MyThing', declaredType: 'JsonDict' }];
		expect(headerPortSigs(p, p, defaults, unwired)).toEqual({
			sigs: [{ name: 'response', required: false, portType: 'JsonDict', rendered: 'MyThing', metadata: false }],
			reverted: [],
		});
	});

	it('a retype of an optional catalog port the program never wrote keeps its meaning at the current wiring', () => {
		// Wired, it already waits for its value, so `data: Number` (always
		// required) means the same; unwired, the node runs without it, so
		// `data?: Number` means the same.
		const prev = [{ name: 'data', required: false, portType: 'T' }];
		const next = [{ name: 'data', required: false, portType: 'Number' }];
		expect(headerPortSigs(prev, next, defaults, wired('data'))).toEqual({
			sigs: [{ name: 'data', required: true, portType: 'Number', rendered: 'Number', metadata: false }],
			reverted: [],
		});
		expect(headerPortSigs(prev, next, defaults, unwired)).toEqual({
			sigs: [{ name: 'data', required: false, portType: 'Number', rendered: 'Number', metadata: false }],
			reverted: [],
		});
	});

	it('a port the node requires is always written without `?`', () => {
		const prev = [{ name: 'prompt', required: true, portType: 'String' }];
		const retyped = [{ name: 'prompt', required: true, portType: 'String | Number' }];
		const askedOptional = [{ name: 'prompt', required: false, optional: true, portType: 'String | Number' }];
		for (const next of [retyped, askedOptional]) {
			expect(headerPortSigs(prev, next, defaults, unwired).sigs).toEqual([
				{ name: 'prompt', required: true, portType: 'String | Number', rendered: 'String | Number', metadata: false },
			]);
		}
	});

	it('a port the program wrote keeps its own spelling whatever its wiring', () => {
		const optional = [{ name: 'data', required: false, optional: true, portType: 'Number', declaredType: 'Number' }];
		expect(headerPortSigs(optional, optional, defaults, wired('data')).sigs).toEqual([
			{ name: 'data', required: false, portType: 'Number', rendered: 'Number', metadata: false },
		]);
		const required = [{ name: 'data', required: true, portType: 'Number', declaredType: 'Number' }];
		expect(headerPortSigs(required, required, defaults, unwired).sigs).toEqual([
			{ name: 'data', required: true, portType: 'Number', rendered: 'Number', metadata: false },
		]);
	});

	it('a required-only toggle keeps the CATALOG type, never the rendered one', () => {
		// The port renders inference-resolved (`String`), but only its
		// requiredness changed: writing the rendered type would freeze
		// the generic into source. The rendered value still rides along
		// so the projection keeps showing it.
		const prev = [{ name: 'data', required: false, portType: 'String' }];
		const next = [{ name: 'data', required: true, portType: 'String' }];
		expect(headerPortSigs(prev, next, defaults, wired('data'))).toEqual({
			sigs: [{ name: 'data', required: true, portType: 'T', rendered: 'String', metadata: false }],
			reverted: [],
		});
	});

	it('a required-only toggle on a DECLARED port keeps the declared spelling', () => {
		const prev = [{ name: 'x', required: false, optional: true, portType: 'String', declaredType: 'T' }];
		const next = [{ name: 'x', required: true, portType: 'String', declaredType: 'T' }];
		expect(headerPortSigs(prev, next, defaults, unwired)).toEqual({
			sigs: [{ name: 'x', required: true, portType: 'T', rendered: 'String', metadata: false }],
			reverted: [],
		});
	});

	it('a requiredness the gesture just set is written, even over the default type', () => {
		// `data?: T` means "runs without it even when wired", which the
		// catalog's own `data` does not: the author asked for it.
		const prev = [{ name: 'data', required: true, portType: 'String', declaredType: 'T' }];
		const next = [{ name: 'data', required: false, optional: true, portType: 'String', declaredType: 'T' }];
		expect(headerPortSigs(prev, next, defaults, unwired)).toEqual({
			sigs: [{ name: 'data', required: false, portType: 'T', rendered: 'String', metadata: false }],
			reverted: [],
		});
	});

	it('retyping back to the catalog default un-declares a line that means what the default means at this wiring', () => {
		const prev = [{ name: 'data', required: false, optional: true, portType: 'Number', declaredType: 'Number' }];
		const next = [{ name: 'data', required: false, optional: true, portType: 'T', declaredType: 'Number' }];
		expect(headerPortSigs(prev, next, defaults, unwired)).toEqual({
			sigs: [],
			reverted: [{ name: 'data', required: false, portType: 'T' }],
		});
		// Wired, `data?: T` would run without a closed wire's value where
		// the default skips, so the line stays.
		expect(headerPortSigs(prev, next, defaults, wired('data'))).toEqual({
			sigs: [{ name: 'data', required: false, portType: 'T', rendered: 'T', metadata: false }],
			reverted: [],
		});
		const prevReq = [{ name: 'data', required: true, portType: 'Number', declaredType: 'Number' }];
		const nextReq = [{ name: 'data', required: true, portType: 'T', declaredType: 'Number' }];
		expect(headerPortSigs(prevReq, nextReq, defaults, wired('data'))).toEqual({
			sigs: [],
			reverted: [{ name: 'data', required: false, portType: 'T' }],
		});
	});

	it('a newly added custom port is written', () => {
		const next = [{ name: 'haiku', required: false, portType: 'String' }];
		expect(headerPortSigs([], next, defaults, unwired)).toEqual({
			sigs: [{ name: 'haiku', required: false, portType: 'String', rendered: 'String', metadata: false }],
			reverted: [],
		});
	});

	it('a port with no type, new or declared, is written as its bare name', () => {
		// No type chosen yet: the header names the port alone and the port
		// holds its own variable until something is wired.
		const fresh = [
			{ name: 'haiku', required: false, portType: 'T__haiku', declaredType: '' },
			{ name: 'out', required: true, portType: 'T__out', declaredType: '' },
		];
		expect(headerPortSigs([], fresh, defaults, unwired).sigs).toEqual([
			{ name: 'haiku', required: false, portType: '', rendered: 'T__haiku', metadata: false },
			{ name: 'out', required: true, portType: '', rendered: 'T__out', metadata: false },
		]);
		// Declared with no type and resolved by a wire: still the bare name.
		const declared = [{ name: 'note', required: true, portType: 'String', declaredType: '' }];
		expect(headerPortSigs(declared, declared, defaults, wired('note')).sigs).toEqual([
			{ name: 'note', required: true, portType: '', rendered: 'String', metadata: false },
		]);
	});

	it('a header type that does not parse is written back as the author typed it', () => {
		const p = [{ name: 'note', required: true, portType: 'T__note', declaredType: 'Strng' }];
		expect(headerPortSigs(p, p, defaults, unwired).sigs).toEqual([
			{ name: 'note', required: true, portType: 'Strng', rendered: 'T__note', metadata: false },
		]);
	});

	it('a polluted header (declares its catalog defaults verbatim) heals to empty', () => {
		const p = [
			{ name: 'prompt', required: true, portType: 'String', declaredType: 'String' },
			{ name: SHOULD_FLOW_PORT, required: false, portType: 'T__should_flow', declaredType: 'T__should_flow' },
		];
		expect(headerPortSigs(p, p, defaults, unwired)).toEqual({
			sigs: [],
			reverted: [{ name: 'prompt', required: true, portType: 'String' }],
		});
	});

	it('an untouched `x?:` line over an optional catalog port is kept: it means more than the default', () => {
		const p = [{ name: 'data', required: false, optional: true, portType: 'T', declaredType: 'T' }];
		expect(headerPortSigs(p, p, defaults, unwired)).toEqual({
			sigs: [{ name: 'data', required: false, portType: 'T', rendered: 'T', metadata: false }],
			reverted: [],
		});
	});

	it('_error is never written into the header, even when the previous list lacked it', () => {
		const err = { name: ERROR_PORT, required: false, portType: 'String' };
		expect(headerPortSigs([], [err], [...defaults, err], unwired)).toEqual(none);
		expect(headerPortSigs([], [err], defaults, unwired)).toEqual(none);
	});

	it('a declared custom port whose generic got inference-resolved round-trips the authored generic', () => {
		const p = [{ name: 'x', required: true, portType: 'String', declaredType: 'T' }];
		expect(headerPortSigs(p, p, defaults, wired('x'))).toEqual({
			sigs: [{ name: 'x', required: true, portType: 'T', rendered: 'String', metadata: false }],
			reverted: [],
		});
	});

	it('with no defaults known, a declared port still round-trips and an undeclared one stays out', () => {
		const p = [
			{ name: 'a', required: true, portType: 'String', declaredType: 'String' },
			{ name: 'b', required: true, portType: 'String' },
		];
		expect(headerPortSigs(p, p, undefined, unwired)).toEqual({
			sigs: [{ name: 'a', required: true, portType: 'String', rendered: 'String', metadata: false }],
			reverted: [],
		});
	});
});

describe('nodeInputRequiredness', () => {
	it('names the four states a node input can be in', () => {
		const prompt = defaults[2];
		const data = defaults[0];
		expect(nodeInputRequiredness({ name: 'prompt', required: true, portType: 'String' }, prompt)).toBe('node_requires');
		expect(nodeInputRequiredness({ name: 'data', required: true, portType: 'T' }, data)).toBe('required');
		expect(nodeInputRequiredness({ name: 'data', required: false, optional: true, portType: 'T' }, data)).toBe('optional');
		expect(nodeInputRequiredness({ name: 'data', required: false, portType: 'T' }, data)).toBe('wiring');
		// A port the program added has no catalog side to require it.
		expect(nodeInputRequiredness({ name: 'extra', required: true, portType: 'String' }, undefined)).toBe('required');
	});

	it("reads an include's input as passing through, never as a toggle", () => {
		expect(nodeInputRequiredness({ name: 'q', required: true, passesThrough: true, portType: 'String' }, undefined)).toBe('passes_through');
	});
});

describe("a container's input requiredness", () => {
	it('passes through, or is what the program wrote', () => {
		expect(containerInputRequiredness({ name: 'x', required: true, passesThrough: true, portType: 'T' })).toBe('passes_through');
		// A port the program wrote `x?:` by mistake still passes through:
		// no gesture may toggle it back and forth.
		expect(containerInputRequiredness({ name: 'x', required: false, optional: true, passesThrough: true, portType: 'T' })).toBe('passes_through');
		// A loop's `over` or `carry` port launches its iterations.
		expect(containerInputRequiredness({ name: 'items', required: true, portType: 'T' })).toBe('required');
		expect(containerInputRequiredness({ name: 'items', required: false, optional: true, portType: 'T' })).toBe('optional');
	});

	it('is spelled without `?` whenever it passes through, and on every output', () => {
		expect(containerSpelledRequired({ name: 'x', required: false, passesThrough: true, portType: 'T' }, 'input')).toBe(true);
		expect(containerSpelledRequired({ name: 'items', required: false, portType: 'T' }, 'input')).toBe(false);
		expect(containerSpelledRequired({ name: 'items', required: true, portType: 'T' }, 'input')).toBe(true);
		expect(containerSpelledRequired({ name: 'out', required: false, portType: 'T' }, 'output')).toBe(true);
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
			{ name: 'x', required: true, portType: 'T', rendered: 'T', metadata: false },
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
	it('keeps every input but a carry ghost', () => {
		expect(headerWorthy({ name: 'title' })).toBe(true);
		expect(headerWorthy({ name: 'title', declaredType: 'String' })).toBe(true);
		expect(headerWorthy({ name: 'acc', synthesizedFromCarry: true })).toBe(false);
	});
});

describe('a declared line restating a catalog port', () => {
	it('is un-declared by the restates-the-default gate, whatever the port accepts', () => {
		// `HumanTrigger(title: String)` over a catalog `title: String`
		// the node requires: the line says nothing the catalog does not,
		// so the next gesture heals it out of the header.
		const catalog = [{ name: 'title', required: true, portType: 'String', accepts: ['literal'] as const }];
		const p = [{ name: 'title', required: true, portType: 'String', declaredType: 'String', accepts: ['literal'] as const }];
		expect(headerPortSigs(p, p, catalog, unwired)).toEqual({
			sigs: [],
			reverted: [{ name: 'title', required: true, portType: 'String' }],
		});
	});
});

describe('partitionRemovals typing', () => {
	it("omits a nested generic, a port's own variable, and an unparseable spelling", () => {
		const provided = [
			{ name: 'items', required: true, portType: 'List[T]' },
			{ name: 'response', required: false, portType: 'T__response' },
			{ name: 'odd', required: true, portType: 'Strng' },
		];
		expect(partitionRemovals(['items', 'response', 'odd'], provided).reverted).toEqual([
			{ name: 'items', required: true },
			{ name: 'response', required: false },
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
		expect(headerPortSigs(p, p, defaults, unwired).sigs).toEqual([
			{ name: 'photo', required: true, portType: 'Image', rendered: 'Image', metadata: false },
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

	it('_error is never removable, even off the card\'s output rows (which leave it out)', () => {
		const prev = [{ name: 'a' }, { name: ERROR_PORT }];
		expect(removedPortNames(prev, [{ name: 'a' }])).toEqual([]);
	});
});

// SYNC assertions with crates/weft-core/src/node.rs NodeMetadata::output_declaration_fits
describe('declarationRule / declarationProblem', () => {
	const out = (portType: string, narrowable?: true | string) =>
		({ name: 'out', portType, required: true, ...(narrowable !== undefined ? { narrowable } : {}) });

	it('a port the program added takes any type', () => {
		expect(declarationRule('output', undefined)).toEqual({ kind: 'free' });
		expect(declarationProblem({ kind: 'free' }, 'Verdict')).toBeNull();
	});

	it('a narrowable output takes what fits its own type, a stream item-wise', () => {
		const rule = declarationRule('output', out('JsonDict', true));
		expect(rule).toEqual({ kind: 'fits', bound: 'JsonDict', narrowable: true });
		expect(declarationProblem(rule, '{ verdict: String }')).toBeNull();
		expect(declarationProblem(rule, 'String')).toContain('does not fit in JsonDict');
		const stream = declarationRule('output', out('Generator[JsonDict]', true));
		expect(declarationProblem(stream, 'Generator[{ id: Number }]')).toBeNull();
	});

	it('a narrowable output with a wider bound takes what fits that bound', () => {
		const rule = declarationRule('output', out('JsonDict', 'JsonDict | String | File'));
		expect(declarationProblem(rule, 'String')).toBeNull();
		expect(declarationProblem(rule, 'Image')).toBeNull();
		expect(declarationProblem(rule, 'Number')).not.toBeNull();
	});

	it('any other concrete output is fixed to its own type', () => {
		const rule = declarationRule('output', out('String'));
		expect(rule).toEqual({ kind: 'fixed', type: 'String' });
		expect(declarationProblem(rule, 'String')).toBeNull();
		expect(declarationProblem(rule, 'Number')).not.toBeNull();
	});

	it('an output holding a variable and an input narrow within their own type', () => {
		expect(declarationRule('output', out('List[T]'))).toEqual({ kind: 'fits', bound: 'List[T]', narrowable: false });
		const input = declarationRule('input', { name: 'x', portType: 'Media', required: true });
		expect(declarationProblem(input, 'Image')).toBeNull();
		expect(declarationProblem(input, 'Blob')).not.toBeNull();
	});

	// SYNC assertions with crates/weft-compiler/src/validate.rs check_type_bounds (TypeParam::violation)
	it('a declaration pinning a bounded variable has to fit the bound', () => {
		const media = [{ name: 'T', bound: 'Media' }];
		const input = declarationRule('input', { name: 'file', portType: 'T', required: true }, media);
		expect(input).toEqual({ kind: 'fits', bound: 'T', narrowable: false, typeParams: media });
		expect(declarationProblem(input, 'String')).toBe('`T` has to fit in Media, and what is connected makes it String');
		expect(declarationProblem(input, 'Image')).toBeNull();
		expect(declarationProblem(input, 'Media')).toBeNull();
		// Bound through a container, on an output that holds the variable.
		const output = declarationRule('output', out('List[T]'), media);
		expect(declarationProblem(output, 'List[Number]')).toBe('`T` has to fit in Media, and what is connected makes it Number');
		expect(declarationProblem(output, 'List[Video]')).toBeNull();
	});

	it('a bound naming another variable reads that one from the same declaration or the node', () => {
		const params = [{ name: 'T', bound: 'Media' }, { name: 'T2', bound: 'List[T]' }];
		const rule = declarationRule('input', { name: 'pair', portType: '{ one: T, many: T2 }', required: true }, params);
		expect(declarationProblem(rule, '{ one: Image, many: List[Image] }')).toBeNull();
		expect(declarationProblem(rule, '{ one: Image, many: List[Video] }'))
			.toBe('`T2` has to fit in List[Image], and what is connected makes it List[Video]');
		const resolved = [{ name: 'T', bound: 'Media', resolved: 'Audio' }, { name: 'T2', bound: 'List[T]' }];
		const many = declarationRule('input', { name: 'many', portType: 'T2', required: true }, resolved);
		expect(declarationProblem(many, 'List[Audio]')).toBeNull();
		expect(declarationProblem(many, 'List[Image]')).toContain('`T2` has to fit in List[Audio]');
	});

	it('one declaration pinning a variable two ways is refused', () => {
		const rule = declarationRule('input', { name: 'pair', portType: '{ a: T, b: T }', required: true }, [{ name: 'T', bound: 'Media' }]);
		expect(declarationProblem(rule, '{ a: Image, b: Video }')).toContain("pin type 'T' to two different types (Image and Video)");
	});

	it('a port with no variable, or a node with no bounds, carries none', () => {
		const media = [{ name: 'T', bound: 'Media' }];
		expect(declarationRule('input', { name: 'x', portType: 'String', required: true }, media))
			.toEqual({ kind: 'fits', bound: 'String', narrowable: false });
		const unbounded = declarationRule('input', { name: 'x', portType: 'T', required: true }, []);
		expect(declarationProblem(unbounded, 'String')).toBeNull();
	});

	it("refuses a spelling the compiler would, but leaves a declared type's name to it", () => {
		expect(declarationProblem({ kind: 'free' }, 'Dict[Number, String]')).toContain('List[{key: Number, value: String}]');
		expect(declarationProblem(declarationRule('output', out('JsonDict', true)), 'Verdict')).toBeNull();
	});
});
