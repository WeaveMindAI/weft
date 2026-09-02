import type { PortDefinition, PortType } from '../types';
import { SHOULD_FLOW_PORT, type PortSpecWire, type SpecFieldWire, type PortTemplateWire } from '../../../protocol';
import { parseWeftType, weftTypeToWireString, type WeftType } from '../types';

// The wire types ARE the webview's types: `PortType` is a plain string,
// so nothing narrows. The shorter local names stay because this module
// is where the resolver logic reads most naturally; the one definition
// lives in `protocol.ts` (which carries the SYNC markers to the Rust
// side).
export type PortTemplate = PortTemplateWire;
export type SpecField = SpecFieldWire;
export type PortSpec = PortSpecWire;

/** One entry of the config list a node derives its ports from. The
 *  index signature carries whatever key the entry's spec names
 *  (`key`, `port`), plus a kind's own settings. */
export interface PortEntryDef {
	kind: string;
	key?: string;
	[other: string]: unknown;
}

/** Helper for constructing a PortTemplate inline (used by tests
 *  and by code paths that synthesize specs in TS rather than
 *  reading them from the dispatcher). */
export function port(nameTemplate: string, portType: PortType): PortTemplate {
	return { nameTemplate, portType };
}

/** The port name an entry carries, read through its spec's `keyField`.
 *  Empty when the entry has not named its port yet (the editor's
 *  half-filled add-field row). */
export function entryPortName(entry: PortEntryDef, spec: PortSpec): string {
	const value = entry[spec.keyField];
	return typeof value === 'string' ? value : '';
}

/** Is this a value at all? Exactly undefined (an untouched control)
 *  and null (a cleared one, the editor's unset convention) are not;
 *  everything else is, the empty string and the empty list included. */
export function isFilledIn(v: unknown): boolean {
	return v !== undefined && v !== null;
}

/** Does the draft carry a value for this spec field? See `isFilledIn`
 *  for what counts (an empty string is a value: a case matching "" is
 *  one the compiler accepts, so it must survive a save).
 *  SYNC: null-is-absent <->
 *        crates/weft-compiler/src/validate.rs (the spec-field loop),
 *        catalog/human/form_helpers.rs (build_form_fields) */
export function hasValue(draft: PortEntryDef, field: SpecField): boolean {
	return isFilledIn(draft[field.key]);
}

/** Is this field's value an empty CHOICE SET: an empty list where the
 *  list is the set of choices (`shape: valueList`, or `typed` with a
 *  list valueType)? Choosing nothing satisfies no requirement, so the
 *  commit gate refuses it on a required field, matching the compiler.
 *  Where a list is one VALUE being matched (`equals: []`), emptiness
 *  is legitimate and this answers false.
 *  SYNC: empty-list-is-nothing-chosen <->
 *        crates/weft-compiler/src/validate.rs (the spec-field loop),
 *        crates/weft-core/src/node.rs
 *        (SpecField::empty_list_means_nothing_chosen) */
export function isEmptyChoiceSet(draft: PortEntryDef, field: SpecField): boolean {
	const v = draft[field.key];
	if (!Array.isArray(v) || v.length > 0) return false;
	if (field.shape === 'valueList') return true;
	if (field.shape !== 'typed') return false;
	// STRUCTURAL, exactly like the Rust side's `WeftType::List(_)`
	// match: a string test would call a union like `List[String] |
	// String` a choice set while Rust does not. (A NAMED alias whose
	// body is a list reads false on BOTH sides; shared, deliberate.)
	return parseWeftType(field.valueType ?? '')?.kind === 'list';
}

/** The entry a draft commits to source: `kind`, the port name under the
 *  spec's key field, and only the spec fields the author actually filled
 *  in (a cleared control saves null, the editor's unset convention, and
 *  an entry written to source must not carry it). Building from
 *  `spec.fields` also drops values left over from a different kind the
 *  draft passed through. */
export function entryForSource(draft: PortEntryDef, spec: PortSpec, name: string): PortEntryDef {
	const entry: PortEntryDef = { kind: spec.kind, [spec.keyField]: name };
	for (const field of spec.fields ?? []) {
		if (hasValue(draft, field)) {
			entry[field.key] = draft[field.key];
		}
	}
	return entry;
}

/** Is `key` a legal weft bare identifier (`[A-Za-z_][A-Za-z0-9_]*`)?
 *  A field key becomes a PORT NAME (`{key}_approved`, etc.), and the
 *  parser rejects any port name outside that grammar, so the add-field
 *  form gates on this: a key with spaces or punctuation (`what do you
 *  want?`) is refused before it can emit an unparseable port.
 *  SYNC: bare-ident grammar <->
 *        crates/weft-core/src/lib.rs (is_rust_identifier, which the
 *        compiler's is_bare_ident re-exports) */
export function isValidFieldKey(key: string): boolean {
	return /^[A-Za-z_][A-Za-z0-9_]*$/.test(key);
}

function resolvePortName(template: string, key: string): string {
	return template.replace('{key}', key);
}

/** Sentinel TypeVar name used by form-field specs to request an auto-scoped
 *  per-port TypeVar.
 *  Catalog authors write `port('{key}', 'T_Auto')` for ports that should
 *  accept any type independently from sibling ports. Explicit TypeVars like
 *  `T`, `T1` are left alone, so authors can still express shared-type
 *  constraints when that is the right semantics. */
// SYNC: AUTO_TYPE_VAR_MARKER <-> crates/weft-core/src/node.rs AUTO_TYPE_VAR
const AUTO_TYPE_VAR_MARKER = 'T_Auto';

/** Recursively replace every `T_Auto` marker in a parsed WeftType with a
 *  TypeVar scoped to the field key. Every container arm recurses, so a
 *  nested placeholder scopes exactly like a top-level one.
 *  SYNC: materializeAutoTypeVars <-> crates/weft-core/src/node.rs materialize_auto_type_vars */
function materializeAutoTypeVars(t: WeftType, key: string): WeftType {
	switch (t.kind) {
		case 'typevar':
			if (t.name === AUTO_TYPE_VAR_MARKER) {
				return { kind: 'typevar', name: `T__${key}` };
			}
			return t;
		case 'list':
			return { kind: 'list', inner: materializeAutoTypeVars(t.inner, key) };
		case 'dict':
			return {
				kind: 'dict',
				key: materializeAutoTypeVars(t.key, key),
				value: materializeAutoTypeVars(t.value, key),
			};
		case 'union':
			return { kind: 'union', types: t.types.map(x => materializeAutoTypeVars(x, key)) };
		case 'record':
			return {
				kind: 'record',
				fields: t.fields.map(f => ({ ...f, ty: materializeAutoTypeVars(f.ty, key) })),
			};
		case 'generator':
			return { kind: 'generator', inner: materializeAutoTypeVars(t.inner, key) };
		// No 'named' case: a declared body is concrete by construction
		// (the Rust registry and wire parser both refuse a type
		// variable inside one), so there is never a T_Auto to
		// materialize beneath an alias.
		default:
			return t;
	}
}

/** Replace T_Auto markers in a port type string with key-scoped TypeVar names.
 *  Returns the original string if parsing fails or no markers are present. */
function resolveAutoTypeVars(portType: PortType, key: string): PortType {
	const parsed = parseWeftType(portType);
	if (!parsed) return portType;
	const materialized = materializeAutoTypeVars(parsed, key);
	// Wire rendering: the result flows back into `parseWeftType` (port
	// matching, colors), and only the self-contained form re-parses
	// when a named type is present (`weftTypeToString` prints the bare
	// name, which the frontend has no registry to resolve).
	return weftTypeToWireString(materialized);
}

export function buildSpecMap(specs: PortSpec[]): Record<string, PortSpec> {
	return Object.fromEntries(specs.map(s => [s.kind, s]));
}

export function deriveInputsFromEntries(
	entries: PortEntryDef[],
	specMap: Record<string, PortSpec>,
): PortDefinition[] {
	const ports: PortDefinition[] = [];
	for (const entry of entries) {
		const spec = specMap[entry.kind];
		if (!spec) continue;
		const key = entryPortName(entry, spec);
		if (!key) continue;
		for (const t of spec.addsInputs) {
			ports.push({
				name: resolvePortName(t.nameTemplate, key),
				portType: resolveAutoTypeVars(t.portType, key),
				// Derived inputs are required, the language default;
				// entries carry no per-port override (the compiler
				// admits only `kind`, the key field and spec fields).
				required: true,
			});
		}
	}
	return ports;
}

export function deriveOutputsFromEntries(
	entries: PortEntryDef[],
	specMap: Record<string, PortSpec>,
): PortDefinition[] {
	const ports: PortDefinition[] = [];
	for (const entry of entries) {
		const spec = specMap[entry.kind];
		if (!spec) continue;
		const key = entryPortName(entry, spec);
		if (!key) continue;
		for (const t of spec.addsOutputs) {
			ports.push({
				name: resolvePortName(t.nameTemplate, key),
				portType: resolveAutoTypeVars(t.portType, key),
				required: false,
			});
		}
	}
	return ports;
}

/** The port names `entry` would add that `others` already use.
 *  `others` is every entry the list will still hold beside this one, so
 *  editing an entry and keeping its name is not a clash with itself,
 *  while renaming it onto a neighbour's name is. Empty means the entry
 *  can be written. */
// Duplicate detection runs PER SIDE, matching the compiler's rule: a
// node legitimately has an input and an output sharing a name (a
// passthrough's `value`/`value`), so crossing the sides would
// false-positive every one of them.
// SYNC: entryPortCollisions <-> crates/weft-compiler/src/validate.rs duplicate-port
export function entryPortCollisions(
	entry: PortEntryDef,
	others: PortEntryDef[],
	specMap: Record<string, PortSpec>,
	// Names the node type itself already owns on each side (its catalog
	// ports): a derived port shadowing one would write a header the
	// build rejects, so it is refused at the door like an entry-vs-entry
	// collision. `_should_flow` is reserved on the input side of every
	// node (the enricher synthesizes it), so it is seeded here rather
	// than left to each caller.
	reserved: { inputs: string[]; outputs: string[] },
): string[] {
	const collide = (
		added: { name: string }[],
		existing: { name: string }[],
		reservedNames: string[],
	): string[] => {
		const taken = new Set([...reservedNames, ...existing.map((p) => p.name)]);
		return added.map((p) => p.name).filter((name) => taken.has(name));
	};
	return [
		...collide(
			deriveInputsFromEntries([entry], specMap),
			deriveInputsFromEntries(others, specMap),
			[...reserved.inputs, SHOULD_FLOW_PORT],
		),
		...collide(
			deriveOutputsFromEntries([entry], specMap),
			deriveOutputsFromEntries(others, specMap),
			reserved.outputs,
		),
	];
}
