/**
 * Unified type definitions for Weft Dashboard
 *
 * This is the single source of truth for all types.
 */

import type {
	BusInspectorEvent,
	BusMeta,
	CallerInspectorEvent,
	CorruptionSite,
	LoopInspectorEvent,
	ScopeDecidedEvent,
	Frame,
	NodeFiringStatus,
	NodeFeaturesWire as NodeFeatures,
} from '../../../protocol';
// Node feature flags ARE the wire type `NodeFeaturesWire`, aliased to
// `NodeFeatures` and re-exported under the `lib/types` module so webview
// imports stay uniform and there is one definition of the concept (not a
// webview-local copy that drifts from the wire shape).
export type { NodeFiringStatus };
export type { NodeFeaturesWire as NodeFeatures } from '../../../protocol';
// The accepts/widget vocabulary IS the wire type, re-exported for the
// same one-definition reason.
import { ACCESS_MARKER_KEY, INFRA_MARKER_KEY, type Accepts, type CodeLanguage, type InputDefinition, type Widget, type WidgetKind } from '../../../protocol';
export type { Accepts, Widget, WidgetKind };

// =============================================================================
// PORT TYPE SYSTEM
//
// Python-style recursive types with strict enforcement. No Any type.
//
// Primitives:     String, Number, Boolean, Image, Video, Audio, Blob
// Parameterized:  List[T], Dict[String, V] (a dict's keys are always text)
// Records:        { name: String, age?: Number }, open with a last `..`
// Unions:         String | Number, List[String] | String
// Aliases:        Media = Image | Video | Audio;  File = Media | Blob
// Type variables: T, T1, T2..., node-scoped, same T on input and output = same type;
//                 a port written with no type holds its own (`T__in__name`), decided by its wire
//
// Port types describe what the node sees on its boundary. A port
// declared `List[T]` carries the list verbatim; to process elements
// one by one, wrap the call site in a `Loop(over: [...])`.
//
// USING PORT TYPES IN NODE DEFINITIONS (frontend.ts):
//   portType: 'String'
//   portType: 'List[String]'
//   portType: 'Dict[String, Number]'
//   portType: 'String | Number'
//   portType: 'T'                    type variable
// =============================================================================

// The primitive/alias tables live in the protocol leaf (protocol.ts is
// what `typeReferencesFile` derives its file-kind token set from, and
// this module already imports from it); re-exported here so webview
// imports stay uniform and there is exactly ONE definition.
import { ALL_PRIMITIVE_TYPES, FILE_PRIMITIVES, NAMED_UNIONS, STORED_FILE_MARKER_TYPES, containsUnresolvedLeaf, parseWeftType, tryParseWeftType, unionOf, weftTypesEqual, weftTypeToString, weftTypeToWireString, type RecordField, type WeftPrimitive, type WeftType } from '../../../protocol';
export { ALL_PRIMITIVE_TYPES, FILE_PRIMITIVES, NAMED_UNIONS, parseWeftType, tryParseWeftType, weftTypesEqual, weftTypeToString, weftTypeToWireString };
export type { RecordField, WeftPrimitive, WeftType };

/** A port type string. Supports recursive syntax: List[String], Dict[String, V], records, unions, type vars. */
export type PortType = string;

/** A lookup table with NO inherited entries, safe to index by any
 *  user-chosen name. A normal JS object silently answers `constructor`,
 *  `toString`, `valueOf` and friends from its prototype, so a node,
 *  port, field or file path with one of those names reads back a piece
 *  of JS machinery instead of "absent". Build every by-name table that
 *  is keyed by user data with this, and no reader has to remember. */
export function bareRecord<T>(...sources: (Record<string, T> | undefined)[]): Record<string, T> {
	return Object.assign(Object.create(null) as Record<string, T>, ...sources);
}

/** Own-property read on a wire-deserialized record. Deserialized JSON
 *  objects carry `Object.prototype`, so a key literally named
 *  'constructor' or 'toString' (a legal weft identifier) would
 *  otherwise resolve a prototype function instead of `undefined`. */
export function ownValue<T>(record: Record<string, T> | undefined, key: string): T | undefined {
	if (!record || !Object.prototype.hasOwnProperty.call(record, key)) return undefined;
	return record[key];
}

/** A NAME's stored value, from EITHER home, port literal winning: the
 *  compiler moves port-driven values into portLiterals, so a name whose
 *  home is unknown to the caller must be looked for there first. Use
 *  this when you hold a bare name; use `declaredHomeValue` when you
 *  hold a field that already declares its home. */
export function storedValueOf(
	portLiterals: Record<string, unknown>,
	config: unknown,
	key: string,
): unknown {
	return ownValue(portLiterals, key) ?? ownValue(config as Record<string, unknown> | undefined, key);
}

/** A FIELD's stored value, from the ONE home its `portDriven` flag
 *  declares (port literals or config, never the other). THE lookup
 *  every field renderer (node body and field strip alike) goes
 *  through; `storedValueOf` is the either-home variant for a bare
 *  name. */
export function declaredHomeValue(
	portLiterals: Record<string, unknown> | undefined,
	config: unknown,
	field: { portDriven?: boolean; key: string },
): unknown {
	return field.portDriven
		? ownValue(portLiterals, field.key)
		: ownValue(config as Record<string, unknown> | undefined, field.key);
}

/** Extract leaf primitive types from a parsed type (for color coding, etc.) */
export function extractPrimitives(t: WeftType): WeftPrimitive[] {
	switch (t.kind) {
		case 'primitive': return [t.value];
		case 'list': return extractPrimitives(t.inner);
		case 'dict': return extractPrimitives(t.value);
		case 'json_dict': return [];
		case 'bus': return [];
		case 'access': return [];
		case 'infra': return [];
		// A stream reads as its element for color/leaf purposes.
		case 'generator': return extractPrimitives(t.inner);
		case 'union': return t.types.flatMap(extractPrimitives);
		case 'record': return t.fields.flatMap(f => extractPrimitives(f.ty));
		case 'named': return extractPrimitives(t.body);
		case 'typevar': return [];
	}
}

/** Compile-time compatibility check: can source flow into target? */
export function isWeftTypeCompatible(source: PortType, target: PortType): boolean {
	const s = parseWeftType(source);
	const t = parseWeftType(target);
	if (!s || !t) return false;
	return isCompatible(s, t);
}

export function isCompatible(source: WeftType, target: WeftType): boolean {
	// A type variable on either side: can't check yet, assume ok
	if (source.kind === 'typevar') return true;
	if (target.kind === 'typevar') return true;
	// Empty (bottom type from empty containers) is compatible with anything as source
	if (source.kind === 'primitive' && source.value === 'Empty') return true;

	if (source.kind === 'primitive' && target.kind === 'primitive') {
		return source.value === target.value;
	}
	if (source.kind === 'list' && target.kind === 'list') {
		return isCompatible(source.inner, target.inner);
	}
	if (source.kind === 'dict' && target.kind === 'dict') {
		return isCompatible(source.value, target.value);
	}
	// JsonDict: compatible with any Dict[String, V] in both directions
	if (source.kind === 'json_dict' && target.kind === 'json_dict') return true;
	if (source.kind === 'json_dict' && target.kind === 'dict') return true;
	if (source.kind === 'dict' && target.kind === 'json_dict') return true;
	// A bus connects only to a bus; payloads are not type-checked.
	if (source.kind === 'bus' && target.kind === 'bus') return true;
	if (source.kind === 'access' && target.kind === 'access') return true;
	if (source.kind === 'infra' && target.kind === 'infra') return true;
	// A generator connects only to a same-element generator (invariant
	// in T, checked both ways). A plain T never accepts a Generator[T].
	// SYNC: generator compatibility <-> crates/weft-core/src/weft_type.rs WeftType::is_compatible
	if (source.kind === 'generator' && target.kind === 'generator') {
		return isCompatible(source.inner, target.inner) && isCompatible(target.inner, source.inner);
	}
	// No blanket generator rejection here: exactly like the Rust match,
	// a generator still flows through the named-decay arm below (an
	// alias whose body is a generator must behave like the spelled-out
	// stream), and the final fallthrough answers false for every
	// remaining pairing.
	// A NAMED type is nominal: only the same name flows in.
	// SYNC: named/record compatibility <-> crates/weft-core/src/weft_type.rs WeftType::is_compatible
	if (source.kind === 'named' && target.kind === 'named') {
		return source.name === target.name;
	}
	// Records: the target's contract decides. Every source field must be
	// declared on the target unless the target is open (it takes
	// undeclared keys), every required target field must be a required
	// source field, and field types must match pairwise. The SOURCE's
	// openness says nothing here: a value of an open record carries only
	// its declared fields once it was held to the type.
	if (source.kind === 'record' && target.kind === 'record') {
		return (target.open || source.fields.every(sf => target.fields.some(tf => tf.name === sf.name)))
			&& target.fields.every(tf => {
				const sf = source.fields.find(f => f.name === tf.name);
				if (!sf) return tf.optional;
				return (!sf.optional || tf.optional) && isCompatible(sf.ty, tf.ty);
			});
	}
	// A record forgets into the generic-object types; never the reverse
	// (the deliberate door is the Cast node).
	if (source.kind === 'record' && target.kind === 'json_dict') return true;
	// An optional field may hold null, so the dict's value type must
	// take null too.
	if (source.kind === 'record' && target.kind === 'dict') {
		return source.fields.every(f => isCompatible(f.ty, target.value)
			&& (!f.optional || isCompatible({ kind: 'primitive', value: 'Null' }, target.value)));
	}
	if (source.kind === 'json_dict' && target.kind === 'record') return false;
	if (source.kind === 'dict' && target.kind === 'record') return false;
	// Both unions: every source variant must match at least one target variant
	if (source.kind === 'union' && target.kind === 'union') {
		return source.types.every(s => target.types.some(t => isCompatible(s, t)));
	}
	// Single into union: source must match at least one variant
	if (target.kind === 'union') {
		return target.types.some(t => isCompatible(source, t));
	}
	// Union into single: all variants must be compatible
	if (source.kind === 'union') {
		return source.types.every(s => isCompatible(s, target));
	}
	// A named source decays into a structural target (after the union
	// arms, so `Named -> Named | Null` matched through the union above).
	if (source.kind === 'named') {
		return isCompatible(source.body, target);
	}
	if (target.kind === 'named') return false;
	return false;
}

/** May an author declare `declared` on a port whose type can be at
 *  most `bound`: a narrowable output's call-site type, or a type
 *  parameter's binding against its bound? Every value of `declared`
 *  must be a value of `bound`, which is `isCompatible` with one
 *  difference: a stream narrows ITEM-wise (`Generator[Signup]` fits
 *  `Generator[JsonDict]`), where wiring one stream into another stays
 *  invariant. A named bound stays nominal (only its own name fits it);
 *  a named declaration is read through its body. */
// SYNC: fitsWithin <-> crates/weft-core/src/weft_type.rs fits_within
export function fitsWithin(declared: WeftType, bound: WeftType): boolean {
	const d = structural(declared);
	if (d.kind === 'generator' && bound.kind === 'generator') return fitsWithin(d.inner, bound.inner);
	return isCompatible(declared, bound);
}

/** The structural type behind any chain of named types. */
// SYNC: structural <-> crates/weft-core/src/weft_type.rs WeftType::structural
function structural(t: WeftType): WeftType {
	while (t.kind === 'named') t = t.body;
	return t;
}

/** `t` with each type variable `replace` names swapped for what it
 *  returns, at any depth; a variable it answers `undefined` for stays.
 *  A named type's body is concrete by construction (the parser refuses
 *  a type variable inside one), so it is never walked. */
// SYNC: substituteTypeVars <-> crates/weft-core/src/weft_type.rs WeftType::substitute_var
export function substituteTypeVars(t: WeftType, replace: (name: string) => WeftType | undefined): WeftType {
	switch (t.kind) {
		case 'typevar': return replace(t.name) ?? t;
		case 'list': return { kind: 'list', inner: substituteTypeVars(t.inner, replace) };
		case 'generator': return { kind: 'generator', inner: substituteTypeVars(t.inner, replace) };
		case 'dict': return { kind: 'dict', value: substituteTypeVars(t.value, replace) };
		// `T | Null` with `T` a union is one flat union.
		case 'union': return unionOf(t.types.map((m) => substituteTypeVars(m, replace)));
		case 'record':
			return { ...t, fields: t.fields.map((f) => ({ ...f, ty: substituteTypeVars(f.ty, replace) })) };
		default: return t;
	}
}

/** Each type variable `port` holds, paired with what `other` has in
 *  its place, appended to `out` in walk order. Matched through named
 *  types, so an alias binds like its spelled-out body; what is bound is
 *  the unpeeled type, so the name survives. A variable is never bound
 *  to a whole stream, nor to a type still holding a variable. */
// SYNC: collectTypeVarBindings <-> crates/weft-compiler/src/enrich.rs collect_type_var_bindings
export function collectTypeVarBindings(port: WeftType, other: WeftType, out: [string, WeftType][]): void {
	const p = structural(port);
	const o = structural(other);
	if (p.kind === 'typevar') {
		if (!containsUnresolvedLeaf(other) && o.kind !== 'generator') out.push([p.name, other]);
	} else if ((p.kind === 'list' && o.kind === 'list') || (p.kind === 'generator' && o.kind === 'generator')) {
		collectTypeVarBindings(p.inner, o.inner, out);
	} else if (p.kind === 'dict' && o.kind === 'dict') {
		collectTypeVarBindings(p.value, o.value, out);
	} else if (p.kind === 'record' && o.kind === 'record') {
		for (const fa of p.fields) {
			const fb = o.fields.find((f) => f.name === fa.name);
			if (fb) collectTypeVarBindings(fa.ty, fb.ty, out);
		}
	}
}

/** Why the variable `name`, worked out to be `resolved`, breaks its
 *  `bound`, in the compiler's words; null when it fits, or when the
 *  bound still holds a variable (that variable's own port is what gets
 *  reported). */
// SYNC: typeParamViolation <-> crates/weft-core/src/weft_type.rs TypeParam::violation
export function typeParamViolation(name: string, bound: WeftType, resolved: WeftType): string | null {
	if (containsUnresolvedLeaf(bound) || fitsWithin(resolved, bound)) return null;
	return `\`${name}\` has to fit in ${weftTypeToString(bound)}, and what is connected makes it ${weftTypeToString(resolved)}`;
}

/** A node's bound on `param` with every variable `pinned` names, then
 *  every one the node already resolved, substituted in. Null when the
 *  bound is not a type this side can parse (the compiler's to judge). */
export function typeParamBound(
	param: import('../../../protocol').TypeParamWire,
	typeParams: readonly import('../../../protocol').TypeParamWire[],
	pinned: ReadonlyMap<string, WeftType> = new Map(),
): WeftType | null {
	const declared = parseWeftType(param.bound);
	if (!declared) return null;
	return substituteTypeVars(declared, (name) => {
		const fromPin = pinned.get(name);
		if (fromPin) return fromPin;
		const resolved = typeParams.find((p) => p.name === name)?.resolved;
		return resolved === undefined ? undefined : parseWeftType(resolved) ?? undefined;
	});
}

/** Whether a wire of `sourceType` must be refused by the bound on the
 *  type variable its target port is typed with: the target is a bare
 *  variable the node bounds, and the source does not fit that bound.
 *  A type this side cannot parse is the compiler's to judge. */
export function typeVarBoundRefuses(
	sourceType: string,
	targetType: string,
	typeParams: readonly import('../../../protocol').TypeParamWire[] | undefined,
): boolean {
	const target = parseWeftType(targetType);
	if (!target || target.kind !== 'typevar' || !typeParams) return false;
	const param = typeParams.find((p) => p.name === target.name);
	const source = parseWeftType(sourceType);
	const bound = param ? typeParamBound(param, typeParams) : null;
	if (!source || !bound) return false;
	return typeParamViolation(target.name, bound, source) !== null;
}

// ── Type inference from runtime values ──────────────────────────────────────

// The marker->type table lives in the protocol leaf beside
// parseFileValue (one definition; see STORED_FILE_MARKER_TYPES there).
// SYNC: FILE_HANDLE_KEYS <-> crates/weft-core/src/weft_type.rs detect_file_type
const FILE_HANDLE_KEYS = ['url', 'key'];

// The bus sentinel (`{"__weft_bus__": ...}`); the access sentinel is
// imported from the protocol module.
// SYNC: BUS_MARKER_KEY <-> crates/weft-core/src/bus.rs bus marker
const BUS_MARKER_KEY = '__weft_bus__';

/** Infer the WeftType of a JSON value. Mirrors WeftType::infer() in Rust. */
export function inferTypeFromValue(value: unknown): WeftType {
	if (value === null || value === undefined) return { kind: 'primitive', value: 'Null' };
	if (typeof value === 'boolean') return { kind: 'primitive', value: 'Boolean' };
	if (typeof value === 'number') return { kind: 'primitive', value: 'Number' };
	if (typeof value === 'string') return { kind: 'primitive', value: 'String' };
	if (Array.isArray(value)) {
		if (value.length === 0) return { kind: 'list', inner: { kind: 'primitive', value: 'Empty' } };
		const elementTypes = value.map(inferTypeFromValue);
		return { kind: 'list', inner: unifyTypes(elementTypes) };
	}
	if (typeof value === 'object') {
		const obj = value as Record<string, unknown>;
		// Detect a stored-file value by its CONCRETE marker key: the
		// marker IS the type. The payload must carry a handle (url/data/key).
		for (const [marker, prim] of STORED_FILE_MARKER_TYPES) {
			const payload = obj[marker];
			if (typeof payload === 'object' && payload !== null
				&& FILE_HANDLE_KEYS.some(k => k in (payload as Record<string, unknown>))) {
				return { kind: 'primitive', value: prim };
			}
		}
		// Same sentinel discipline for the other runtime handles, in the
		// backend's order: a bus channel and a credential grant are
		// their own types, never a generic dict. The bus marker's value
		// must itself be an object (the backend's detect_bus_type reads
		// it as one); the access marker is a bare key test there too.
		if (typeof obj[BUS_MARKER_KEY] === 'object' && obj[BUS_MARKER_KEY] !== null
			&& !Array.isArray(obj[BUS_MARKER_KEY])) {
			return { kind: 'bus' };
		}
		if (ACCESS_MARKER_KEY in obj) return { kind: 'access' };
		if (INFRA_MARKER_KEY in obj) return { kind: 'infra' };
		const values = Object.values(obj);
		if (values.length === 0) {
			return { kind: 'dict', value: { kind: 'primitive', value: 'Empty' } };
		}
		const valueTypes = values.map(inferTypeFromValue);
		return { kind: 'dict', value: unifyTypes(valueTypes) };
	}
	return { kind: 'primitive', value: 'String' };
}

/** Does this input take a wire? The compiler RESOLVES `accepts` onto
 *  every instance input and the CLI onto every catalog input, so the
 *  editor never re-derives it from the type; an absent list (a locally
 *  added port that has not round-tripped yet) means both. */
export function acceptsWire(port: PortDefinition): boolean {
	return port.accepts === undefined || port.accepts.includes('wire');
}

/** Does this input take a written value? See `acceptsWire`. */
export function acceptsLiteral(port: PortDefinition): boolean {
	return port.accepts === undefined || port.accepts.includes('literal');
}

/** Unify a list of inferred types: dedup by MUTUAL compatibility (each
 *  flows into the other), collapse a single survivor into itself.
 *  SYNC: unifyTypes <-> crates/weft-core/src/weft_type.rs WeftType::unify_types */
function unifyTypes(types: WeftType[]): WeftType {
	if (types.length === 0) return { kind: 'primitive', value: 'Empty' };
	const unique: WeftType[] = [];
	for (const t of types) {
		const members = t.kind === 'union' ? t.types : [t];
		for (const m of members) {
			if (!unique.some(u => isCompatible(m, u) && isCompatible(u, m))) unique.push(m);
		}
	}
	return unique.length === 1 ? unique[0] : { kind: 'union', types: unique };
}

/** The editor's ONE render-side port shape: the wire's enriched input
 *  (`InputDefinition`), re-exported rather than re-declared so it
 *  cannot drift from what the backend sends. Outputs and group
 *  interface ports ride the same shape with the input-only members
 *  absent, because every port renders through the same components. */
export type PortDefinition = InputDefinition;

// =============================================================================
// Field Types (for node configuration UI)
// =============================================================================

/// The RENDER shape of one input's editor field: the input's resolved
/// widget flattened for the controls (`type` mirrors `Widget.kind`
/// exactly, unknown-kind forward-compat included; there is
/// deliberately no parallel field-type union). Built from an
/// `InputDefinition` by `fieldForInput`; components never derive any
/// of it themselves.
export interface FieldDefinition {
	key: string;
	label: string;
	type: WidgetKind | string;
	/// This field edits an input's written value (an entry in the node's
	/// portLiterals, the one home a port's constant has). False only for
	/// a value that is not a port's: a loop's knob, an entry's own fields
	/// (the entry list is the value being edited). The strip routes the
	/// value read/write accordingly.
	portDriven?: boolean;
	placeholder?: string;
	options?: string[];
	defaultValue?: unknown;
	description?: string;
	accept?: string; // For file_drop fields: narrows the type-derived HTML-accept filter
	fileType?: string; // For file_drop fields: the declared weft file type (Image/Audio/.../File)
	multiple?: boolean; // For file_drop fields: the port holds several files, so the control keeps a list
	language?: CodeLanguage; // For code fields: the CodeMirror syntax
	min?: number; // For number fields: minimum allowed value (clamped on blur)
	max?: number; // For number fields: maximum allowed value (clamped on blur)
	step?: number; // For number fields: granularity of the input (used by slider/number)
	service?: string | null; // For access fields: the connected service (compiler-stamped)
	access?: string; // For remote_select fields: the Access input authenticating sources needing one
	sources?: import('../../../protocol').ResourceSource[]; // For remote_select fields: the fill sources, in preference order
	dependsOn?: string[]; // For remote_select fields: parent inputs for drill-down
	freeText?: boolean; // For select, multiselect and remote_select fields: the list is suggestions, a typed value outside it is legal
}

// =============================================================================
// Node Template Types (defines what a node TYPE looks like)
// =============================================================================


/** Record of a single run of a node. The `NodeFiringStatus`
 *  alias re-exported at the top of the file lets the wire and the UI
 *  share one source of truth and never drift on which states exist. */
export interface NodeFiring {
	id: string;
	nodeId: string;
	status: NodeFiringStatus;
	pulseId: string;
	error?: string;
	callbackId?: string;
	startedAt: number;
	completedAt?: number;
	input?: unknown;
	/// Wired input ports that arrived as CLOSURE markers for this
	/// firing (the upstream frame stack terminated without firing them).
	/// Disjoint from the keys of `input`; the inspector renders these
	/// as "(closed)" so a user-emitted null is not visually confused
	/// with a structural close.
	closedPorts?: string[];
	/// Why this firing did not run, on a skipped one.
	skipReason?: import('../../../protocol').SkipReason;
	output?: unknown;
	costUsd: number;
	/// Portion of a container's total belonging to reused member firings.
	inheritedCostUsd?: number;
	inheritedCostUnknown?: boolean;
	/// At least one of this firing's cost records could not be resolved
	/// to a figure (amount null). Rendered as an explicit "unknown" so an
	/// unresolved cost is never mistaken for a free call.
	costUnknown?: boolean;
	/// Whose key this firing's cost records spent; 'mixed' when records
	/// disagree (e.g. a group row aggregating both kinds of member).
	credentialOwner?: import('../../../protocol').CredentialOwnerKind | 'mixed';
	/// Identities of the cost records already folded into `costUsd` /
	/// `costUnknown`. The dispatcher re-streams journal events on every
	/// follow/reconnect (replay + live overlap), so the reducer dedups on
	/// these.
	costIds?: string[];
	/// The changes this firing made to stored files, in the order they
	/// landed (deduped on key + produced version across replay and live).
	fileEdits?: import('../../../protocol').FileEditWire[];
	logs: unknown[];
	runId: string;
	frames: Frame[];
	/// Frame stack serialized as JSON, used to correlate completion
	/// events to the right running row when several firings run
	/// in parallel. `[]` at root (outside any loop).
	framesKey: string;
	/// The run this firing was taken from (a seeded run reused it).
	inheritedFrom?: string;
	/// Input ports whose value the run was given (handed in, or sent by
	/// a node standing in for itself) rather than made by a node of it.
	// SYNC: input origins <-> crates/weft-core/src/live_event.rs DispatcherEvent, extension-vscode/src/runFollower.ts DispatcherEvent, packages/weft-graph/src/protocol.ts NodeFiringEvent
	providedPorts?: string[];
	inheritedPorts?: Record<string, string>;
}

/** Node firings keyed by node ID. */
export type NodeFiringTable = Record<string, NodeFiring[]>;

/** How the run ended, once it has: the state plus, for a cancel, the
 *  text and structured cause (a sibling run stopping it names the run
 *  and the tag). Undefined while the run is live. */
export interface RunTerminal {
	state: 'completed' | 'failed' | 'cancelled';
	reason?: string;
	cause?: import('../../../protocol').CancelCause;
}

/** Live run state the webview maintains from the extension
 *  host's SSE stream. Single source of truth: lifted here so
 *  `App.svelte` (which owns the state) and the editor components
 *  (which consume it) cannot drift on field shape. Webview-internal
 *  only: `Set<string>` does not survive the wire, so this type never
 *  appears in a `HostMessage`.
 */
export interface RunState {
	isRunning: boolean;
	/** Every place the followed run's program runs (`locatedKey`s);
	 *  `null` when the run's program could not be read (nothing is
	 *  dimmed), `undefined` before the run's birth arrived. A node
	 *  outside it, at the call on screen, is "not in this run": dimmed,
	 *  no status badge. */
	scope?: string[] | null;
	/** The run this one was seeded from and what it re-ran, or `null`
	 *  for a run from nothing. */
	seed?: import('../../../protocol').Seed | null;
	/** The version the run ran and the version the files on disk are,
	 *  when the host knows them (a run opened from the version tree). */
	version?: { version: string | null; diskVersion: string | null };
	/** The tags the run put on itself (`ctx.tag_run`), in claim
	 *  order, deduplicated. The handle a sibling's `ctx.stop_tagged`
	 *  selects on; shown on the run in the inspector. */
	tags: string[];
	terminal?: RunTerminal;
	nodeOutputs: Record<string, unknown>;
	nodeFirings: NodeFiringTable;
	/** Full bus log per `busId` (in arrival order). The inspector
	 *  renders one IRC-style panel per bus a node participates in. */
	busLogByBus: Record<string, BusInspectorEvent[]>;
	/** Per-bus metadata (mode), seeded from the first BusParticipant
	 *  edge the dispatcher derives from the bus marker JSON. */
	busMetaByBus: Record<string, BusMeta>;
	/** Participant set per `busId`. A node N gets a bus panel iff
	 *  `N` appears in the set for that bus. */
	busParticipantsByBus: Record<string, Set<string>>;
	/** Journal rows the dispatcher could not apply during fold.
	 *  Empty in the normal case; populated on replay if any row of
	 *  the journal was malformed. The inspector renders a muted
	 *  "N journal rows corrupted" collapsed disclosure when this
	 *  is non-empty, so the signal is visible without being
	 *  alarming. */
	journalCorruptions: Array<{
		site: CorruptionSite;
		reason: string;
	}>;
	/** Full ordered log of LoopInspectorEvents per loop group. Key is
	 *  the loop's `groupId`; the parentFrames stack lives on each
	 *  event so a card can split by nesting/sibling iteration. */
	loopEventsByGroup: Record<string, LoopInspectorEvent[]>;
	/** Every copy of a scope the run decided, by scope id (a group, an
	 *  include's call site, an included file's body), each copy once in
	 *  the order decided. A group is no node of the run, so these are
	 *  what its box paints from. */
	scopeDecisions: Record<string, ScopeDecidedEvent[]>;
	/** Full ordered log of the live caller exchange (connected /
	 *  inbound / outbound / errored / disconnected). One caller per
	 *  run, so this is a flat list, not keyed. Empty for runs
	 *  with no live connection. The inspector replays it as a single
	 *  caller panel on the run. */
	callerLog: CallerInspectorEvent[];
}

/** One line of a node's display. The authoritative definition lives
 *  in `protocol.ts`; this re-export keeps webview imports under the
 *  `lib/types` module. Adding a new kind: extend the union in
 *  protocol.ts, add the variant to `LiveItemKind` in
 *  `crates/weft-core/src/live.rs`, and add a branch in
 *  ProjectNode.svelte's render block.
 */
export type { LiveDataItem } from '../../../protocol';


/**
 * Validation levels:
 * - structural: the project is correctly wired (connections, required config for structure)
 * - runtime: the project can actually execute (API keys, credentials, file data)
 */
/** A node type's own output, as the template carries it: the render
 *  port plus how far a program may narrow it. */
export type TemplateOutputPort = PortDefinition & Pick<import('../../../protocol').OutputSpec, 'narrowable'>;

/**
 * NodeTemplate defines what a node TYPE looks like.
 * This is the schema/blueprint for nodes like "OpenRouterInference", "ExecPython", "Http".
 * Each node type has one template.
 */
export interface NodeTemplate {
	type: string;
	label: string;
	description: string;
	icon: import('svelte').Component;
	color: string;
	/** Free-form search tags from the node's metadata.json. The
	 *  command palette's scoreNode() reads this for tag-match
	 *  ranking. Always present (empty array when the node declares
	 *  none) so consumers don't have to optional-chain. */
	tags: string[];
	/// Mirrors the node's `metadata.requiresInfra` flag. The infra
	/// subgraph extractor + node-role helpers key off this to decide
	/// whether to seed from this node. Always present in templates
	/// built from the `weft describe-nodes` payload; defaults to false
	/// when the catalog entry doesn't declare it.
	requiresInfra: boolean;
	defaultInputs: PortDefinition[];
	defaultOutputs: TemplateOutputPort[];
	/** The node type's metadata ports, each with its hand-written value. */
	metadataPorts?: import('../../../protocol').MetadataPort[];
	/** The bound on each type variable the node type's ports use. */
	typeParams?: import('../../../protocol').TypeParamWire[];
	features?: NodeFeatures;
	/** The node's declared inline per-firing display (a media player,
	 *  a file card) and which port it shows. */
	display?: import('../../../protocol').DisplaySpecWire;
	/** Which config key this node's ports come from, and the entry
	 *  kinds it accepts. Undefined for a node whose ports are fixed. */
	portsFromConfig?: import('../../../protocol').PortsFromConfigWire;
	/** The service recipe, present ONLY on a personal access node;
	 *  drives the connect flow's forms and scope menu. */
	service?: import('../../../protocol').AccessSpecWire;
	/** The project's OAuth apps (inherited from the package root),
	 *  keyed by service name. An access node resolves its own app here
	 *  and sends it on connect. */
	accessApps?: Record<string, import('../../../protocol').AppRegistration>;
	/** Dynamically resolve port types based on current port definitions.
	 *  Returns overrides for input and output port types.
	 *  Only needed for nodes with dynamic type behavior (Pack, Unpack, etc.). */
	resolveTypes?: (inputs: PortDefinition[], outputs: PortDefinition[]) => {
		inputs?: Record<string, PortType>;
		outputs?: Record<string, PortType>;
	};
}

// =============================================================================
// Node Instance Types (a specific node in a project)
// =============================================================================

export interface Position {
	x: number;
	y: number;
}

/**
 * NodeInstance is a specific node placed in a project.
 * It has an id, position, and config values.
 * Multiple instances can exist of the same node type.
 */
export type GroupBoundaryRole = 'In' | 'Out';

export interface GroupBoundary {
	groupId: string;
	role: GroupBoundaryRole;
}

export interface NodeInstance {
	id: string;
	nodeType: string;
	label: string | null;
	config: Record<string, unknown>;
	/// Every constant written for a port, keyed by port name, whichever
	/// spelling wrote it. Mirrors the definition's `portLiterals`.
	portLiterals?: Record<string, unknown>;
	portLiteralSpans?: Record<string, import('../../../protocol').ConfigFieldSpan>;
	position: Position;
	parentId?: string;
	inputs: PortDefinition[];
	outputs: PortDefinition[];
	/// The ports whose value is written with the program (the wire's
	/// `metadataPorts`), declared in code only and never drawn on the
	/// canvas. A group's carry the value its body writes, shown in its
	/// config strip.
	metadataPorts?: import('../../../protocol').MetadataPort[];
	/// The inputs a metadata wire fills, by input name (`MetadataInput`).
	/// Kept apart from `portLiterals`, which holds only what the program
	/// writes on the input itself.
	metadataInputs?: Record<string, MetadataInput>;
	features: NodeFeatures;
	/// The bounds on the node's type variables, each with what it
	/// resolved to (the wire's `typeParams`). A group carries its
	/// boundaries' (`Group[T: Media]`).
	typeParams?: import('../../../protocol').TypeParamWire[];
	/// The compiler's word on whether this node brings up infra (it
	/// mirrors the catalog's metadata onto every parsed node). A node
	/// added in the editor and not yet parsed carries none, and the
	/// catalog answers for it (`nodeRequiresInfra`).
	requiresInfra?: boolean;
	/// Set when the node exists once per instance of the program: `marked`
	/// where the source says `@per_instance`, `derived` for a node the
	/// compiler found reached from one.
	perInstance?: import('../../../protocol').PerInstance;
	scope?: string[];
	groupBoundary?: GroupBoundary | null;
	// Source line where this node was declared in the weft code. Populated
	// by the parser and used by autoOrganize to keep siblings left-to-right
	// in the order the user wrote them, even though `project.nodes` ends up
	// sorted groups-first for SvelteFlow's parent-first requirement.
	sourceLine?: number;
	// Set on an opaque `@include` node: the included `.weft` file path. The
	// editor renders it as an expandable group that navigates into the file.
	includePath?: string;
	/// Set on an included file's own top-level group (the wire's
	/// `GroupDefinition.anonymous`): it draws open until a person closes it
	/// (`expandedOf` in `layout.ts`).
	anonymous?: boolean;
}

// =============================================================================
// Project Types
// =============================================================================

export interface Edge {
	id: string;
	source: string;
	target: string;
	sourceHandle: string | null;
	targetHandle: string | null;
	/// Keys read off the source value on the way; absent for a plain wire.
	path?: string[];
}

/// A metadata port named from somewhere else: the scoped id of the node
/// or group it belongs to, the port, and the keys read off its value
/// (`calls.tools.anthropic`). The same shape a gathered value writes it in.
export type MetadataRef = Extract<import('../../../protocol').MetadataExpr, { kind: 'port' }>;

/// An input a metadata wire fills: the compiler wrote `value` into it when
/// the program compiled, from the metadata port `from`. Read-only in the
/// editor: a person changes it where `from` is written.
export interface MetadataInput {
	from: MetadataRef;
	value: unknown;
}

export interface ProjectDefinition {
	id: string;
	// A project carries no name/description: identity is the manifest file name,
	// descriptions are per-group (the first plain `# ...` comment line of the
	// group body). (Matches the Rust wire type.)
	// Stored (source of truth)
	weftCode?: string | null;
	layoutCode?: string | null;
	// Derived in-memory from weftCode (not stored)
	nodes: NodeInstance[];
	edges: Edge[];
	createdAt: string;
	updatedAt: string;
}

// =============================================================================
// Node Update Types (for project editor callbacks)
// =============================================================================

/**
 * Updates that can be made to a node in the project editor.
 * Used by node components to communicate changes back to the editor.
 */
export interface NodeDataUpdates {
	label?: string | null;
	config?: Record<string, unknown>;
	/// Body-set PORT values (the node's portLiterals map), kept apart
	/// from `config`: one home per name. The handler diffs it into
	/// set_config/remove_config ops stamped with the value's written form.
	portLiterals?: Record<string, unknown>;
	/// Flip a port value's WRITTEN form (braces vs statement); becomes a
	/// `set_value_form` op.
	// SYNC: portValueForm.form <-> crates/weft-compiler/src/edit.rs ValueForm, packages/weft-graph/src/protocol.ts EditOp set_value_form.form
	portValueForm?: { key: string; form: 'inline' | 'connection' };
	inputs?: PortDefinition[];
	outputs?: PortDefinition[];
	/// A container's metadata port value, written in its body
	/// (`self.<port> = <value>`); null removes the line. Becomes a
	/// `set_metadata_value` op, after any header change in the same update.
	metadataValue?: { port: string; value: unknown };
	/// Set ONLY when the user dragged the resize handle. The host re-runs ELK on a
	/// resize (neighbours make room), so this must distinguish a real user resize
	/// from a programmatic dimension write (min-height auto-enforce, a rebuild after
	/// a move), which carry width/height too but must NOT trigger a relayout.
	resized?: boolean;
}

/** Resolve a container node's kind. Returns null for non-containers.
 *  The single source of truth for "is this a container, and which
 *  one": the two boolean helpers below are expressed through it so
 *  the kind set (Group, Loop) lives in exactly one place. Callers
 *  handle the null case (bail before mutating visual state). */
export function containerKindOf(nodeType: unknown): 'Group' | 'Loop' | null {
	if (nodeType === 'Group') return 'Group';
	if (nodeType === 'Loop') return 'Loop';
	return null;
}

/** A node whose `nodeType` is one of the language's structural
 *  containers (Group, Loop). Containers nest children; the visual
 *  editor treats them uniformly for layout, parent linking, and
 *  collapse/expand. The renderer picks distinct visuals per kind. */
export function isContainerNodeType(nodeType: unknown): boolean {
	return containerKindOf(nodeType) !== null;
}

/** True iff an expanded container draws its config strip: a loop always
 *  (its knobs live there), any container carrying values written on its
 *  interface ports or brought to them by a metadata wire, and any with a
 *  metadata port (its value shows there). Both the renderer and the layout engine ask
 *  this, so the strip and the space reserved for it cannot disagree. */
export function containerHasConfigStrip(
	nodeType: unknown,
	portLiterals: Record<string, unknown> | undefined,
	metadataPorts: readonly unknown[] | undefined,
	metadataInputs: Record<string, unknown> | undefined,
): boolean {
	if (!isContainerNodeType(nodeType)) return false;
	return isLoopNodeType(nodeType)
		|| Object.keys(portLiterals ?? {}).length > 0
		|| Object.keys(metadataInputs ?? {}).length > 0
		|| (metadataPorts?.length ?? 0) > 0;
}

/** The written form a port value takes on its FIRST write, when no
 *  span says where it already lives. A node's braces are its config,
 *  so a new value goes there and the toggle can move it. A group's or
 *  loop's braces hold its children and settings, and an include alias
 *  has no braces at all, so their ports take a value on a statement
 *  line only (`g.tone = "formal"`); the host refuses the braces form
 *  for them. */
export function portValueFirstForm(nodeType: unknown): import('../../../protocol').ConfigFieldSpan['origin'] {
	return isContainerNodeType(nodeType) || isIncludeNodeType(nodeType) ? 'connection' : 'inline';
}

/** True iff a node is a Loop container (used by renderer + visual
 *  differentiation; for structural checks prefer `isContainerNodeType`). */
export function isLoopNodeType(nodeType: unknown): boolean {
	return containerKindOf(nodeType) === 'Loop';
}

/** The opaque `@include` block: a node carrying only the included file's
 *  interface ports, whose body lives in that file. No catalog entry by
 *  design. */
// SYNC: INCLUDE_NODE_TYPE <-> crates/weft-core/src/project.rs INCLUDED_GROUP_TYPE
export const INCLUDE_NODE_TYPE = 'IncludedGroup';

export function isIncludeNodeType(nodeType: unknown): boolean {
	return nodeType === INCLUDE_NODE_TYPE;
}

/** A box that stands for a scope rather than a node: a container (its
 *  members run inside it at this level) or an include (the call site, whose
 *  body runs one call frame deeper). Neither is a place of a run; both
 *  are painted from their members and their scope's decisions. */
export function isScopeBoxNodeType(nodeType: unknown): boolean {
	return isContainerNodeType(nodeType) || isIncludeNodeType(nodeType);
}
