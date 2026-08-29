import type { PortDefinition, PortType } from '../types';
import type {
	FormFieldRenderWire,
	PortSpecWire,
	SpecFieldWire,
	PortTemplateWire,
} from '../../../protocol';
import { parseWeftType, weftTypeToWireString, type WeftType } from '../types';

// The wire types ARE the webview's types: `PortType` is a plain string,
// so nothing narrows. The shorter local names stay because this module
// is where the resolver logic reads most naturally; the one definition
// lives in `protocol.ts` (which carries the SYNC markers to the Rust
// side).
export type FormFieldRender = FormFieldRenderWire;
export type PortTemplate = PortTemplateWire;
export type SpecField = SpecFieldWire;
export type PortSpec = PortSpecWire;

/** One entry of the config list a node derives its ports from. The
 *  index signature carries whatever key the entry's spec names
 *  (`key`, `port`), plus a kind's own settings. */
export interface PortEntryDef {
	kind: string;
	key?: string;
	render?: FormFieldRender;
	config?: Record<string, unknown>;
	required?: boolean;
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

/** Is `key` a legal weft bare identifier (`[A-Za-z_][A-Za-z0-9_]*`)?
 *  A field key becomes a PORT NAME (`{key}_approved`, etc.), and the
 *  parser rejects any port name outside that grammar, so the add-field
 *  form gates on this: a key with spaces or punctuation (`what do you
 *  want?`) is refused before it can emit an unparseable port.
 *  SYNC: keep in step with try_parse_port_decl in
 *  weft/crates/weft-compiler/src/weft_compiler.rs (the port-name grammar). */
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
				// Derived ports default to required (same as the language default).
				// Set "required": false explicitly to make a port optional.
				required: entry.required !== false,
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
export function entryPortCollisions(
	entry: PortEntryDef,
	others: PortEntryDef[],
	specMap: Record<string, PortSpec>,
): string[] {
	const added = [
		...deriveInputsFromEntries([entry], specMap),
		...deriveOutputsFromEntries([entry], specMap),
	].map((p) => p.name);
	const taken = new Set(
		[
			...deriveInputsFromEntries(others, specMap),
			...deriveOutputsFromEntries(others, specMap),
		].map((p) => p.name),
	);
	return added.filter((name) => taken.has(name));
}
