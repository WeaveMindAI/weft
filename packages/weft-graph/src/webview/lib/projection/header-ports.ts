// What a NODE's header should declare, and what a ports update removed.
//
// A node's header is NOT its whole port surface: the catalog provides
// the node type's own ports at enrich, and the header declares only the
// CUSTOM ones, plus a catalog port whose type or requiredness the
// author overrides (retyping a port from the context menu, making an
// optional one required). Writing the full rendered list instead freezes the
// node type's entire signature into source: internal machinery included
// (`_should_flow`), every structural type expanded, and catalog updates
// no longer reaching the node. Groups and Loops are different: their
// signature IS the whole surface, and they do not go through here.
//
// The header is rebuilt from `declaredType` (what the source actually
// says), never from the rendered `portType`: type inference substitutes
// generics per instance (a catalog `data: T` wired to a String renders
// as `data: String`), and comparing rendered types against catalog
// defaults would mistake every such instantiation for an author
// override and freeze it into source.

import type { EditPortSig, RevertedPortSig, OutputSpec, TypeParamWire, WeftType } from '../../../protocol';
import { isLanguagePort, containsUnresolvedLeaf, inputIsRequired, narrowingBound, parseWeftType, tryParseWeftType } from '../../../protocol';
import { collectTypeVarBindings, fitsWithin, isCompatible, typeParamBound, typeParamViolation, weftTypesEqual, weftTypeToString } from '../types';

export interface HeaderPortLike {
	name: string;
	required?: boolean;
	/** The program wrote it `x?:` (inputs only; see `InputDefinition.optional`). */
	optional?: boolean;
	/** A container's input that hands its value to the nodes inside
	 *  (inputs only; see `InputDefinition.passesThrough`). */
	passesThrough?: boolean;
	/** The rendered type. Every rendered port carries one. */
	portType: string;
	declaredType?: string;
	synthesizedFromCarry?: boolean;
}

/** Whether a port may appear in a node's HEADER signature at all.
 *  Carry-synthesized ghost inputs are derived from the loop's carry
 *  list, never written. The ONE predicate for every surface that builds
 *  header lists. */
export const headerWorthy = (p: Pick<HeaderPortLike, 'synthesizedFromCarry'>): boolean =>
	!p.synthesizedFromCarry;

/// A port's requiredness with an ABSENT flag read as not required,
/// matching the catalog side (`InputSpec`/`OutputSpec`, plain
/// `#[serde(default)]`), which is where a default with no flag comes
/// from. The WIRE sig's peer (`edit.rs PortSig`) has NO default and
/// refuses a missing flag, which is why every sig this module emits
/// carries it explicitly. Shared with the group signature writer.
export const portRequired = (p: HeaderPortLike | undefined): boolean => p?.required === true;

/** Whether a closed value on this port skips its node, given whether a
 *  wire feeds it (`inputIsRequired` over a port whose flags may be absent). */
const meansRequired = (p: HeaderPortLike, wired: boolean): boolean =>
	inputIsRequired({ required: portRequired(p), optional: p.optional }, wired);

/** Where an input's requiredness stands, which decides what a gesture
 *  may change it to:
 *   - `node_requires`: the node's metadata requires it, so its code
 *     needs it and the compiler refuses `x?:` on it.
 *   - `passes_through`: a container's input that hands its value to the
 *     nodes inside. It has no requiredness of its own (the compiler
 *     refuses `x?:` on it): the nodes inside decide, so no gesture
 *     changes it.
 *   - `required` / `optional`: the program wrote it `x:` / `x?:` (every
 *     other container port is one of these two).
 *   - `wiring`: the metadata calls it optional and the program wrote
 *     nothing: the node runs without it while unwired, and needs it once
 *     wired (a closed wire skips the node). */
export type InputRequiredness = 'node_requires' | 'passes_through' | 'required' | 'optional' | 'wiring';

/** A node input's `InputRequiredness`, `provided` being the node type's
 *  own port of that name (absent for a port the program added). */
export function nodeInputRequiredness(
	p: HeaderPortLike,
	provided: HeaderPortLike | undefined,
): InputRequiredness {
	// An include's opaque node is a node whose inputs pass through.
	if (p.passesThrough) return 'passes_through';
	if (portRequired(provided)) return 'node_requires';
	if (portRequired(p)) return 'required';
	return p.optional ? 'optional' : 'wiring';
}

/** A container input's `InputRequiredness`: it passes through, or the
 *  program wrote it `x:` / `x?:`. */
export function containerInputRequiredness(p: HeaderPortLike): InputRequiredness {
	if (p.passesThrough) return 'passes_through';
	return portRequired(p) ? 'required' : 'optional';
}

/** Whether a CONTAINER's signature spells this port without `?`. An
 *  output has no optionality at all (the language has no `?` on one,
 *  and the edit server refuses a sig asking for it), and a port that
 *  passes through has none of its own (the compiler refuses `x?:` on
 *  it), so both are always `x:`; any other input keeps what the program
 *  wrote. */
export function containerSpelledRequired(p: HeaderPortLike, side: 'input' | 'output'): boolean {
	return side === 'output' || p.passesThrough === true || portRequired(p);
}

/** Whether the header spells this port without `?`, chosen so the line
 *  means what the port means now: a port the node requires never takes
 *  `?` (the compiler refuses it); a port the program wrote keeps its own
 *  spelling; and a port the metadata calls optional that the program
 *  never wrote is spelled `x:` while wired (required, as wired it already
 *  is) and `x?:` while unwired (the node runs without it, as it already
 *  does). */
function spelledRequired(p: HeaderPortLike, provided: HeaderPortLike | undefined, wired: boolean): boolean {
	if (portRequired(provided) || portRequired(p)) return true;
	return p.optional ? false : wired;
}

/** The two requiredness flags as one comparable state. */
const requirednessState = (p: HeaderPortLike): string =>
	portRequired(p) ? 'required' : p.optional ? 'optional' : 'wiring';

/** A node's port surface as one gesture writes it, computed from the
 *  gesture's `next` list against the pre-gesture `previous` list, the
 *  node type's catalog `defaults`, and the input names a wire feeds
 *  (`wired`, empty for outputs and for a fresh copy).
 *   - `sigs`: the header-worthy ports. A port the GESTURE just added or
 *     changed (new name, or type / requiredness differs from `previous`)
 *     is written with its new values; a port the source header already
 *     declares (`declaredType` set) round-trips with the DECLARED type,
 *     the empty spelling of a port written with no type included. A
 *     brand new port writes its own `declaredType` when it carries one
 *     (`''`: the bare name, no type chosen yet). Each sig's `required`
 *     is the spelling that keeps the port's meaning (`spelledRequired`).
 *     Each sig also carries `rendered`, the port's inference-resolved
 *     type, which the optimistic projection shows (the header spelling
 *     may be a generic that inference instantiated per wire).
 *   - `reverted`: ports whose intended header line would restate the
 *     catalog default, so it leaves the header instead: this UN-declares
 *     a port the user just retyped back to the default instead of
 *     freezing the default into source, and heals a polluted header
 *     whose lines still spell the defaults. An untouched line restates
 *     the default only with the default's own requiredness; a retyped
 *     one also when its spelling means, at the current wiring, what the
 *     default means (the spelling `spelledRequired` picks for an
 *     unwritten port). A requiredness the gesture just set is the
 *     author's ask and is always written. A line polluted with an
 *     inference-RESOLVED spelling (`data: String` over a catalog
 *     `data: T`) is NOT healed: the declared string differs from the
 *     default's, and loosening the gate to a semantic compare would let
 *     a genuine override read as a revert. The projection needs the
 *     list to apply the revert's rendered effect; the server does not
 *     (its header rewrite already omits them).
 *  Everything else (catalog-provided, inference-typed, the language's
 *  own `_should_flow` and `_error`)
 *  appears in neither list. */
export function headerPortSigs(
	previous: HeaderPortLike[],
	next: HeaderPortLike[],
	defaults: HeaderPortLike[] | undefined,
	wired: ReadonlySet<string>,
): { sigs: EditPortSig[]; reverted: RevertedPortSig[] } {
	const prevByName = new Map(previous.map((p) => [p.name, p]));
	const defByName = new Map((defaults ?? []).map((d) => [d.name, d]));
	const sigs: EditPortSig[] = [];
	const reverted: RevertedPortSig[] = [];
	for (const p of next) {
		if (isLanguagePort(p.name)) continue;
		const prev = prevByName.get(p.name);
		const d = defByName.get(p.name);
		const isWired = wired.has(p.name);
		const typeChanged = !prev || prev.portType !== p.portType;
		const requiredChanged = !prev || requirednessState(prev) !== requirednessState(p);
		// The type the header WOULD spell for this port. A required-only
		// toggle must not carry the RENDERED type into source (an
		// inference-resolved generic would get frozen), so it keeps the
		// declared spelling, or the catalog's; only an actual retype (or
		// a brand new port with no spelling of its own, whose rendered
		// type IS its authored one) writes the rendered value.
		let portType: string;
		if (!prev) portType = p.declaredType ?? p.portType;
		else if (typeChanged) portType = p.portType;
		else if (p.declaredType !== undefined) portType = p.declaredType;
		else if (requiredChanged) portType = d?.portType ?? p.portType;
		else continue; // untouched and undeclared: the catalog provides it
		// ONE restates-the-default gate, gesture or not.
		const restatesDefault = d !== undefined && d.portType === portType && !requiredChanged
			&& (typeChanged
				? meansRequired(p, isWired) === meansRequired(d, isWired)
				: requirednessState(p) === requirednessState(d));
		if (restatesDefault) {
			reverted.push({ name: p.name, required: portRequired(d), portType: p.portType });
			continue;
		}
		sigs.push({ name: p.name, required: spelledRequired(p, d, isWired), portType, rendered: p.portType, metadata: false });
	}
	return { sigs, reverted };
}

/** The header a FRESH COPY of a node should declare. A copy carries no
 *  wires, so its reparse renders exactly what it declares: `rendered`
 *  is the declared spelling, not the ORIGINAL's inference-resolved type
 *  (stamping that would make the copy's ports refuse wires the
 *  declaration accepts, until the reparse lands). */
export function copiedPortSigs(
	ports: HeaderPortLike[],
	defaults: HeaderPortLike[] | undefined,
): EditPortSig[] {
	return headerPortSigs(ports, ports, defaults, new Set()).sigs.map((s) => ({ ...s, rendered: s.portType }));
}

/** What the delete gesture on this port would DO, or null when there is
 *  nothing to offer. The ONE rule for every delete surface (the port ×
 *  button, the context menu).
 *   - 'remove': a port this instance created (absent from the catalog:
 *     custom, config-created, or added locally and not yet
 *     round-tripped). The port and its wires go. Gated on the node type
 *     accepting custom ports on this side.
 *   - 'revert': an overridden catalog port. The header line goes and
 *     the port drops back to its catalog shape, wires kept
 *     (`partitionRemovals` routes it). Offered on EVERY node type,
 *     since every type accepts an override: gating it on custom-port
 *     support would leave a mistyped override with no way back.
 *   - null: a bare catalog port (the catalog re-creates it on every
 *     parse, so a delete could only orphan its wires). */
export function portDeleteAction(
	p: HeaderPortLike,
	// The ports something OTHER than the header provides for this side:
	// the catalog defaults plus the config-list-derived ports. A
	// provided port outlives its header line (the next parse re-creates
	// it), so deleting one can only mean dropping the override.
	provided: HeaderPortLike[],
	canAddPorts: boolean,
): 'remove' | 'revert' | null {
	const isProvided = provided.some((d) => d.name === p.name);
	// A DECLARED port is always deletable: the header line is the
	// author's text and the author can always take it back (a stale
	// override whose port left the catalog would otherwise be stuck on
	// a type that forbids custom ports). Provided-ness only picks
	// which action the deletion is.
	if (p.declaredType !== undefined) return isProvided ? 'revert' : 'remove';
	return !isProvided && canAddPorts ? 'remove' : null;
}

/** Split the names a gesture deleted into true removals and REVERTS.
 *  Deleting a PROVIDED port (catalog default or config-derived) can
 *  only mean "drop my override and its header line": the next parse
 *  re-creates the port, so routing it through `removed` would kill its
 *  wires and the port would come back bare. The revert sig asserts the
 *  name, the provided requiredness, and the provided type UNLESS that
 *  spelling holds a typevar anywhere (`T`, `T__in__key`, `List[T]`): there the
 *  projection's current rendered type is the best answer until the
 *  reparse re-runs inference, while for a concrete default keeping the
 *  deleted override's type on screen would let the next gesture read
 *  the stale value as truth (a re-apply of the same type would emit
 *  nothing). */
export function partitionRemovals(
	names: string[],
	provided: HeaderPortLike[],
): { removed: string[]; reverted: RevertedPortSig[] } {
	const byName = new Map(provided.map((d) => [d.name, d]));
	const removed: string[] = [];
	const reverted: RevertedPortSig[] = [];
	for (const name of names) {
		const d = byName.get(name);
		if (!d) {
			removed.push(name);
			continue;
		}
		// A spelling that does not parse is unknown, never asserted: the
		// projection keeps its rendered type rather than painting a string
		// nobody could read.
		const parsed = parseWeftType(d.portType);
		const concrete = parsed !== null && !containsUnresolvedLeaf(parsed);
		reverted.push({
			name,
			required: portRequired(d),
			...(concrete ? { portType: d.portType } : {}),
		});
	}
	return { removed, reverted };
}

/** The port names an update DELETED: present before, absent now. These
 *  are the only ports whose wires die (a port absent from the header
 *  may still be a catalog port with live wires). */
export function removedPortNames(
	previous: { name: string }[],
	next: { name: string }[],
): string[] {
	const kept = new Set(next.map((p) => p.name));
	// A language port (the gate, either spelling, or `_error`) is not a
	// removable port: a producer rendering a filtered list (the card's
	// output rows leave `_error` out) must never make Rust sweep a real
	// `n._should_flow = ...` or `y.x = n._error` wire (same guard
	// headerPortSigs has).
	return previous
		.filter((p) => !isLanguagePort(p.name) && !kept.has(p.name))
		.map((p) => p.name);
}

/** What the program may declare on a port its node type provides, the
 *  rule the compiler holds a header to:
 *   - `free`: any type (a port the program added).
 *   - `fits`: a type that fits `bound`. An input narrows within its own
 *     type, and so does an output whose type holds a variable (the
 *     declaration pins it, and each variable it pins has to fit the
 *     node's bound on it, from `typeParams`); a `narrowable` output
 *     narrows within its narrowing bound, a stream item-wise
 *     (`fitsWithin`).
 *   - `fixed`: an output that is the node's contract, so nothing but
 *     its own type.
 *  `provided` is the node type's own port of that name, absent for a
 *  port the program added; for an output it carries `narrowable`.
 *  `typeParams` is the node's bounds on its type variables. */
// SYNC: declarationRule <-> crates/weft-compiler/src/enrich.rs input_fits, crates/weft-core/src/node.rs NodeMetadata::output_declaration_fits
export type DeclarationRule =
	| { kind: 'free' }
	| { kind: 'fits'; bound: string; narrowable: boolean; typeParams?: readonly TypeParamWire[] }
	| { kind: 'fixed'; type: string };

export function declarationRule(
	side: 'input' | 'output',
	provided: (HeaderPortLike & Partial<Pick<OutputSpec, 'narrowable'>>) | undefined,
	typeParams?: readonly TypeParamWire[],
): DeclarationRule {
	if (!provided) return { kind: 'free' };
	const own = parseWeftType(provided.portType);
	// A type this side cannot read is the compiler's to judge.
	if (!own) return { kind: 'free' };
	if (side === 'input' || containsUnresolvedLeaf(own)) {
		return {
			kind: 'fits',
			bound: provided.portType,
			narrowable: false,
			...(typeParams?.length && containsUnresolvedLeaf(own) ? { typeParams } : {}),
		};
	}
	const bound = narrowingBound({ type: provided.portType, narrowable: provided.narrowable });
	return bound === null
		? { kind: 'fixed', type: provided.portType }
		: { kind: 'fits', bound, narrowable: true };
}

/** Why `declared` may not be written on a port under `rule`, or null
 *  when it may. A spelling that only names a type this side has no
 *  registry for (a type the project declares) is the compiler's to
 *  judge, so it passes; any other unreadable spelling is refused with
 *  the compiler's own message. */
export function declarationProblem(
	rule: DeclarationRule,
	declared: string,
): string | null {
	const parsed = tryParseWeftType(declared);
	if (!parsed.ok) return parsed.unknownName ? null : parsed.error;
	switch (rule.kind) {
		case 'free':
			return null;
		case 'fixed': {
			const own = parseWeftType(rule.type);
			return own && weftTypesEqual(parsed.type, own)
				? null
				: `this output always holds ${rule.type}`;
		}
		case 'fits': {
			const bound = parseWeftType(rule.bound);
			if (!bound) return null;
			const fits = rule.narrowable ? fitsWithin(parsed.type, bound) : isCompatible(parsed.type, bound);
			if (!fits) return `${declared} does not fit in ${weftTypeToString(bound)}`;
			return rule.typeParams ? pinnedBoundProblem(bound, parsed.type, rule.typeParams) : null;
		}
	}
}

/** Why the variables `declared` pins on a port typed `own` break the
 *  node's bounds, in the compiler's words, or null when every one fits:
 *  the declaration binds each variable it covers (enrich's declared
 *  narrowing), and each bound on a variable it bound is then held to
 *  that binding (validate's `type-bound`). */
// SYNC: pinnedBoundProblem <-> crates/weft-compiler/src/enrich.rs collect_type_var_bindings, crates/weft-compiler/src/validate.rs check_type_bounds
function pinnedBoundProblem(own: WeftType, declared: WeftType, typeParams: readonly TypeParamWire[]): string | null {
	const bindings: [string, WeftType][] = [];
	collectTypeVarBindings(own, declared, bindings);
	const pinned = new Map<string, WeftType>();
	for (const [name, ty] of bindings) {
		const first = pinned.get(name);
		if (first === undefined) pinned.set(name, ty);
		else if (!weftTypesEqual(first, ty)) {
			return `the declared ports pin type '${name}' to two different types `
				+ `(${weftTypeToString(first)} and ${weftTypeToString(ty)}); every port that carries '${name}' `
				+ `on one node has to name the same type`;
		}
	}
	for (const param of typeParams) {
		const resolved = pinned.get(param.name);
		const bound = resolved ? typeParamBound(param, typeParams, pinned) : null;
		const why = resolved && bound ? typeParamViolation(param.name, bound, resolved) : null;
		if (why) return why;
	}
	return null;
}
