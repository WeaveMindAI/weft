// What a NODE's header should declare, and what a ports update removed.
//
// A node's header is NOT its whole port surface: the catalog provides
// the node type's own ports at enrich, and the header declares only the
// CUSTOM ones, plus a catalog port whose type or requiredness the
// author overrides (filling a MustOverride output, retyping a port from
// the context menu). Writing the full rendered list instead freezes the
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

import type { EditPortSig, Exposure, RevertedPortSig } from '../../../protocol';
import { SHOULD_FLOW_PORT, containsTypevar, parseWeftType } from '../../../protocol';

export interface HeaderPortLike {
	name: string;
	required?: boolean;
	/** The rendered type. Every rendered port carries one. */
	portType: string;
	declaredType?: string;
	exposure?: Exposure;
	synthesizedFromCarry?: boolean;
}

/** Whether a port may appear in a node's HEADER signature at all.
 *  Carry-synthesized ghost inputs are derived from the loop's carry
 *  list; a `config`-exposure input is a metadata setting the emitter
 *  must never INVENT a line for. But a source header CAN already
 *  declare a config-exposure input (the enricher flags it with a
 *  diagnostic and keeps the merged port, declaredType stamped), and a
 *  declared line always round-trips: the diagnostic, not a silent
 *  deletion on the next gesture, is the way out. The ONE predicate for
 *  every surface that builds header lists. */
export const headerWorthy = (p: Pick<HeaderPortLike, 'exposure' | 'declaredType' | 'synthesizedFromCarry'>): boolean =>
	!p.synthesizedFromCarry
	&& (p.exposure !== 'config' || p.declaredType !== undefined);

/// A port's requiredness with an ABSENT flag read as not required,
/// matching the catalog side (`InputSpec`/`OutputSpec`, plain
/// `#[serde(default)]`), which is where a default with no flag comes
/// from. The WIRE sig's peer (`edit.rs PortSig`) has NO default and
/// refuses a missing flag, which is why every sig this module emits
/// carries it explicitly. Shared with the group signature writer.
export const portRequired = (p: HeaderPortLike | undefined): boolean => p?.required === true;

/** A node's port surface as one gesture writes it, computed from the
 *  gesture's `next` list against the pre-gesture `previous` list and the
 *  node type's catalog `defaults`.
 *   - `sigs`: the header-worthy ports. A port the GESTURE just added or
 *     changed (new name, or type / requiredness differs from `previous`)
 *     is written with its new values; a port the source header already
 *     declares (`declaredType` set) round-trips with the DECLARED type.
 *     Each sig also carries `rendered`, the port's inference-resolved
 *     type, which the optimistic projection shows (the header spelling
 *     may be a generic that inference instantiated per wire).
 *   - `reverted`: ports whose intended header line would restate the
 *     catalog default exactly (same type and requiredness), so it leaves
 *     the header instead: this UN-declares a port the user just reverted
 *     (a toggle-on-then-off, a retype back to the default) instead of
 *     freezing the default into source, and heals a polluted header
 *     whose lines still spell the defaults. A line polluted with an
 *     inference-RESOLVED spelling (`data: String` over a catalog
 *     `data: T`) is NOT healed: the declared string differs from the
 *     default's, and loosening the gate to a semantic compare would let
 *     a genuine override read as a revert. The projection needs the
 *     list to apply the revert's rendered effect; the server does not
 *     (its header rewrite already omits them).
 *  Everything else (catalog-provided, inference-typed, `_should_flow`)
 *  appears in neither list. */
export function headerPortSigs(
	previous: HeaderPortLike[],
	next: HeaderPortLike[],
	defaults: HeaderPortLike[] | undefined,
): { sigs: EditPortSig[]; reverted: RevertedPortSig[] } {
	const prevByName = new Map(previous.map((p) => [p.name, p]));
	const defByName = new Map((defaults ?? []).map((d) => [d.name, d]));
	const sigs: EditPortSig[] = [];
	const reverted: RevertedPortSig[] = [];
	for (const p of next) {
		if (p.name === SHOULD_FLOW_PORT) continue;
		const prev = prevByName.get(p.name);
		const typeChanged = !prev || prev.portType !== p.portType;
		const requiredChanged = !prev || portRequired(prev) !== portRequired(p);
		// The type the header WOULD spell for this port. A required-only
		// toggle must not carry the RENDERED type into source (an
		// inference-resolved generic would get frozen), so it keeps the
		// declared spelling, or the catalog's; only an actual retype (or
		// a brand new port, whose rendered type IS its authored one)
		// writes the rendered value.
		let portType: string;
		if (typeChanged) portType = p.portType;
		else if (p.declaredType !== undefined) portType = p.declaredType;
		else if (requiredChanged) portType = defByName.get(p.name)?.portType ?? p.portType;
		else continue; // untouched and undeclared: the catalog provides it
		// ONE restates-the-default gate, gesture or not. A
		// config-exposure port is EXEMPT: a catalog config port is never
		// implied into the header, so a line spelling it is not the
		// redundant pollution the gate heals but a distinct, diagnosed
		// authoring choice that must round-trip.
		const d = defByName.get(p.name);
		if (d && p.exposure !== 'config'
			&& d.portType === portType && portRequired(d) === portRequired(p)) {
			reverted.push({ name: p.name, required: portRequired(p), portType: p.portType });
			continue;
		}
		sigs.push({ name: p.name, required: portRequired(p), portType, rendered: p.portType });
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
	return headerPortSigs(ports, ports, defaults).sigs.map((s) => ({ ...s, rendered: s.portType }));
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
 *  spelling holds a typevar anywhere (`T`, `T__key`, `List[T]`): there the
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
		const concrete = parsed !== null && !containsTypevar(parsed);
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
	// `_should_flow` is language machinery, not a removable port: a
	// producer rendering a filtered list must never make Rust sweep a
	// real `n._should_flow = ...` wire (same guard headerPortSigs has).
	return previous
		.filter((p) => p.name !== SHOULD_FLOW_PORT && !kept.has(p.name))
		.map((p) => p.name);
}
