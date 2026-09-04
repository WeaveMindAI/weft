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
	LoopIteration,
	NodeExecutionStatus,
	NodeFeaturesWire as NodeFeatures,
} from '../../../protocol';
// Node feature flags ARE the wire type `NodeFeaturesWire`, aliased to
// `NodeFeatures` and re-exported under the `lib/types` module so webview
// imports stay uniform and there is one definition of the concept (not a
// webview-local copy that drifts from the wire shape).
export type { NodeExecutionStatus };
export type { NodeFeaturesWire as NodeFeatures } from '../../../protocol';
// The exposure/widget vocabulary IS the wire type, re-exported for the
// same one-definition reason.
import { ACCESS_MARKER_KEY, type Exposure, type InputDefinition, type Widget, type WidgetKind } from '../../../protocol';
export type { Exposure, Widget, WidgetKind };

// =============================================================================
// PORT TYPE SYSTEM
//
// Python-style recursive types with strict enforcement. No Any type.
//
// Primitives:     String, Number, Boolean, Image, Video, Audio, Blob
// Parameterized:  List[T], Dict[K, V]
// Unions:         String | Number, List[String] | String
// Aliases:        Media = Image | Video | Audio;  File = Media | Blob
// Type variables: T, T1, T2..., node-scoped, same T on input and output = same type
// MustOverride:   Node can't know the type, user/AI must declare it in Weft code
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
//   portType: 'MustOverride'         user must declare type in Weft
// =============================================================================

// The primitive/alias tables live in the protocol leaf (protocol.ts is
// what `typeReferencesFile` derives its file-kind token set from, and
// this module already imports from it); re-exported here so webview
// imports stay uniform and there is exactly ONE definition.
import { ALL_PRIMITIVE_TYPES, FILE_PRIMITIVES, NAMED_UNIONS, STORED_FILE_MARKER_TYPES, parseWeftType, weftTypesEqual, type WeftPrimitive, type WeftType } from '../../../protocol';
export { ALL_PRIMITIVE_TYPES, FILE_PRIMITIVES, NAMED_UNIONS, parseWeftType, weftTypesEqual };
export type { WeftPrimitive, WeftType };

/** A port type string. Supports recursive syntax: List[String], Dict[K, V], unions, type vars. */
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
export function ownValue(record: Record<string, unknown> | undefined, key: string): unknown {
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

/** The alias NAME of a structural union, if its members are exactly one
 *  alias's primitive set (order-insensitive). The rendering half of the
 *  alias round-trip; the type itself stays structural. */
function unionAliasName(types: WeftType[]): string | null {
	const prims: WeftPrimitive[] = [];
	for (const t of types) {
		if (t.kind !== 'primitive') return null;
		prims.push(t.value);
	}
	for (const [name, members] of NAMED_UNIONS) {
		if (members.length === prims.length && members.every((m) => prims.includes(m))) {
			return name;
		}
	}
	return null;
}

/** The AUTHORED rendering of a parsed type: a named type prints its
 *  bare name (what humans read/write; edit ops re-resolve it against
 *  the project registry backend-side), and an alias's member set
 *  renders under its NAME (`File`/`Media` survive a parse -> string
 *  round trip), matching the backend. NOT re-parseable frontend-side
 *  when a named type is present; a string that will be parsed again
 *  must use `weftTypeToWireString`.
 *  SYNC: weftTypeToString/weftTypeToWireString <-> crates/weft-core/src/weft_type.rs WeftType::fmt_with, wire_string */
export function weftTypeToString(t: WeftType): string {
	return renderType(t, false);
}

/** The SELF-CONTAINED rendering: a named type prints `Name=Body`, so
 *  the result re-parses without any registry. Use for every string that
 *  flows back into `parseWeftType`. */
export function weftTypeToWireString(t: WeftType): string {
	return renderType(t, true);
}

function renderType(t: WeftType, wire: boolean): string {
	switch (t.kind) {
		case 'primitive': return t.value;
		case 'list': return `List[${renderType(t.inner, wire)}]`;
		case 'dict': return `Dict[${renderType(t.key, wire)}, ${renderType(t.value, wire)}]`;
		case 'json_dict': return 'JsonDict';
		case 'bus': return 'Bus';
		case 'access': return 'Access';
		case 'generator': return `Generator[${renderType(t.inner, wire)}]`;
		case 'union': return unionAliasName(t.types) ?? t.types.map(m => renderType(m, wire)).join(' | ');
		case 'record':
			return `{${t.fields.map(f => `${f.name}${f.optional ? '?' : ''}: ${renderType(f.ty, wire)}`).join(', ')}}`;
		case 'named': {
			if (!wire) return t.name;
			// A union body is parenthesized so the rendered string is
			// unambiguous: `Kind=(A | B)` is one named type, `Kind=A | B`
			// re-parses as a union with a named member.
			const body = renderType(t.body, wire);
			return t.body.kind === 'union' ? `${t.name}=(${body})` : `${t.name}=${body}`;
		}
		case 'typevar': return t.name;
		case 'must_override': return 'MustOverride';
	}
}

/** Extract leaf primitive types from a parsed type (for color coding, etc.) */
export function extractPrimitives(t: WeftType): WeftPrimitive[] {
	switch (t.kind) {
		case 'primitive': return [t.value];
		case 'list': return extractPrimitives(t.inner);
		case 'dict': return [...extractPrimitives(t.key), ...extractPrimitives(t.value)];
		case 'json_dict': return [];
		case 'bus': return [];
		case 'access': return [];
		// A stream reads as its element for color/leaf purposes.
		case 'generator': return extractPrimitives(t.inner);
		case 'union': return t.types.flatMap(extractPrimitives);
		case 'record': return t.fields.flatMap(f => extractPrimitives(f.ty));
		case 'named': return extractPrimitives(t.body);
		case 'typevar': return [];
		case 'must_override': return [];
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
	// TypeVar or MustOverride on either side: can't check yet, assume ok
	if (source.kind === 'typevar' || source.kind === 'must_override') return true;
	if (target.kind === 'typevar' || target.kind === 'must_override') return true;
	// Empty (bottom type from empty containers) is compatible with anything as source
	if (source.kind === 'primitive' && source.value === 'Empty') return true;

	if (source.kind === 'primitive' && target.kind === 'primitive') {
		return source.value === target.value;
	}
	if (source.kind === 'list' && target.kind === 'list') {
		return isCompatible(source.inner, target.inner);
	}
	// JsonDict: compatible with any Dict[String, V] in both directions
	if (source.kind === 'json_dict' && target.kind === 'json_dict') return true;
	if (source.kind === 'json_dict' && target.kind === 'dict') {
		return target.key.kind === 'primitive' && target.key.value === 'String';
	}
	if (source.kind === 'dict' && target.kind === 'json_dict') {
		return source.key.kind === 'primitive' && source.key.value === 'String';
	}
	if (source.kind === 'dict' && target.kind === 'dict') {
		return isCompatible(source.key, target.key) && isCompatible(source.value, target.value);
	}
	// A bus connects only to a bus; payloads are not type-checked.
	if (source.kind === 'bus' && target.kind === 'bus') return true;
	if (source.kind === 'access' && target.kind === 'access') return true;
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
	// Records: the target's contract decides (strict unknown keys,
	// required target fields must be required source fields).
	if (source.kind === 'record' && target.kind === 'record') {
		return source.fields.every(sf => target.fields.some(tf => tf.name === sf.name))
			&& target.fields.every(tf => {
				const sf = source.fields.find(f => f.name === tf.name);
				if (!sf) return tf.optional;
				return (!sf.optional || tf.optional) && isCompatible(sf.ty, tf.ty);
			});
	}
	// A record forgets into the generic-object types; never the reverse
	// (the deliberate door is the Cast node).
	if (source.kind === 'record' && target.kind === 'json_dict') return true;
	if (source.kind === 'record' && target.kind === 'dict') {
		return target.key.kind === 'primitive' && target.key.value === 'String'
			&& source.fields.every(f => isCompatible(f.ty, target.value));
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

// ── Type inference from runtime values ──────────────────────────────────────

// The marker->type table lives in the protocol leaf beside
// parseFileValue (one definition; see STORED_FILE_MARKER_TYPES there).
const FILE_HANDLE_KEYS = ['url', 'data', 'key'];

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
		const values = Object.values(obj);
		if (values.length === 0) {
			return { kind: 'dict', key: { kind: 'primitive', value: 'String' }, value: { kind: 'primitive', value: 'Empty' } };
		}
		const valueTypes = values.map(inferTypeFromValue);
		return { kind: 'dict', key: { kind: 'primitive', value: 'String' }, value: unifyTypes(valueTypes) };
	}
	return { kind: 'primitive', value: 'String' };
}

/** An input's exposure. The compiler RESOLVES exposure onto every
 *  instance input and the CLI resolves it onto every catalog input, so
 *  the editor never re-derives it from the type. The fallback exists
 *  ONLY for a locally-added port that has not round-tripped through a
 *  parse yet: its type is the `MustOverride` placeholder, whose
 *  exposure is `assignment` (no braces literal can be typed for an
 *  unknown type). */
export function inputExposure(port: PortDefinition): Exposure {
	if (port.exposure !== undefined) return port.exposure;
	return String(port.portType) === 'MustOverride' ? 'assignment' : 'all';
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
	/// This field edits a WIREABLE input's body value (an entry in the
	/// node's portLiterals). A `config`-exposure input's field edits the
	/// config home instead (portDriven false). The strip routes the
	/// value read/write accordingly.
	portDriven?: boolean;
	placeholder?: string;
	options?: string[];
	defaultValue?: unknown;
	description?: string;
	accept?: string; // For file_drop fields: narrows the type-derived HTML-accept filter
	fileType?: string; // For file_drop fields: the declared weft file type (Image/Audio/.../File)
	multiple?: boolean; // For file_drop fields: the port holds several files, so the control keeps a list
	language?: string; // For code fields: the CodeMirror syntax ("python", "javascript", ...)
	min?: number; // For number fields: minimum allowed value (clamped on blur)
	max?: number; // For number fields: maximum allowed value (clamped on blur)
	step?: number; // For number fields: granularity of the input (used by slider/number)
	service?: string | null; // For access fields: the connected service (compiler-stamped)
	access?: string; // For remote_select fields: the Access input authenticating sources needing one
	sources?: import('../../../protocol').ResourceSource[]; // For remote_select fields: the fill sources, in preference order
	dependsOn?: string[]; // For remote_select fields: parent inputs for drill-down
	freeText?: boolean; // For remote_select fields: a typed value outside the fetched list is legal
}

// =============================================================================
// Node Template Types (defines what a node TYPE looks like)
// =============================================================================


/** Record of a single execution of a node. The `NodeExecutionStatus`
 *  alias re-exported at the top of the file lets the wire and the UI
 *  share one source of truth and never drift on which states exist. */
export interface NodeExecution {
	id: string;
	nodeId: string;
	status: NodeExecutionStatus;
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
	/// At least one of this firing's cost records could not be resolved
	/// to a figure (amount null). Rendered as an explicit "unknown" so an
	/// unresolved cost is never mistaken for a free call.
	costUnknown?: boolean;
	/// Whose key this firing's cost records spent; 'mixed' when records
	/// disagree (e.g. a group row aggregating both kinds of member).
	credentialOwner?: 'their-own' | 'ours' | 'mixed';
	/// Identities of the cost records already folded into `costUsd` /
	/// `costUnknown`. The dispatcher re-streams journal events on every
	/// follow/reconnect (replay + live overlap), so the reducer dedups on
	/// these.
	costIds?: string[];
	logs: unknown[];
	color: string;
	frames: LoopIteration[];
	/// Frame stack serialized as JSON, used to correlate completion
	/// events to the right running row when several firings run
	/// in parallel. `[]` at root (outside any loop).
	framesKey: string;
	/// Non-terminal per-port warnings raised on this firing. The only
	/// source is a runtime output-type mismatch: the node tried to emit a
	/// value whose type is incompatible with the port's declared type, so
	/// the engine closed the port instead. The node did NOT fail.
	// SYNC: PortWarning <-> crates/weft-core/src/exec/execution.rs PortWarning
	portWarnings?: PortWarning[];
}

/// A non-terminal, per-port problem on a single firing (output-type
/// mismatch). See `NodeExecution.portWarnings`.
// SYNC: PortWarning <-> crates/weft-core/src/exec/execution.rs PortWarning
export interface PortWarning {
	port: string;
	/// The port's declared type (what the node promised to emit).
	expected: string;
	/// The inferred type of the value the node actually tried to emit.
	actual: string;
}

/** Node executions keyed by node ID. */
export type NodeExecutionTable = Record<string, NodeExecution[]>;

/** How the run ended, once it has: the state plus, for a cancel, the
 *  text and structured cause (a sibling run stopping it names the run
 *  and the tag). Undefined while the run is live. */
export interface ExecutionTerminal {
	state: 'completed' | 'failed' | 'cancelled';
	reason?: string;
	cause?: import('../../../protocol').CancelCause;
}

/** Live execution state the webview maintains from the extension
 *  host's SSE stream. Single source of truth: lifted here so
 *  `App.svelte` (which owns the state) and the editor components
 *  (which consume it) cannot drift on field shape. Webview-internal
 *  only: `Set<string>` does not survive the wire, so this type never
 *  appears in a `HostMessage`.
 */
export interface ExecutionState {
	isRunning: boolean;
	/** The tags the run put on itself (`ctx.tag_execution`), in claim
	 *  order, deduplicated. The handle a sibling's `ctx.stop_tagged`
	 *  selects on; shown on the run in the inspector. */
	tags: string[];
	terminal?: ExecutionTerminal;
	nodeOutputs: Record<string, unknown>;
	nodeExecutions: NodeExecutionTable;
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
	/** Full ordered log of the live caller exchange (connected /
	 *  inbound / outbound / errored / disconnected). One caller per
	 *  execution, so this is a flat list, not keyed. Empty for runs
	 *  with no live connection. The inspector replays it as a single
	 *  caller panel on the execution. */
	callerLog: CallerInspectorEvent[];
}

/** A typed data item shown on a node's body-panel feed. The
 *  authoritative definition lives in `protocol.ts`; this
 *  re-export keeps webview imports under the `lib/types` module.
 *  Adding a new kind: extend the union in protocol.ts AND add a
 *  branch in ProjectNode.svelte's render block.
 */
export type { LiveDataItem } from '../../../protocol';


/**
 * Validation levels:
 * - structural: the project is correctly wired (connections, required config for structure)
 * - runtime: the project can actually execute (API keys, credentials, file data)
 */
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
	/// Mirrors the node's `metadata.requires_infra` flag. The infra
	/// subgraph extractor + node-role helpers key off this to decide
	/// whether to seed from this node. Always present in templates
	/// built from the `weft describe-nodes` payload; defaults to false
	/// when the catalog entry doesn't declare it.
	requiresInfra: boolean;
	defaultInputs: PortDefinition[];
	defaultOutputs: PortDefinition[];
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
	/// Body-set PORT values keyed by port name, the two-home twin of
	/// `config` (braces or statement form, per the port's literal
	/// placement). Mirrors the definition's `portLiterals`.
	portLiterals?: Record<string, unknown>;
	portLiteralSpans?: Record<string, import('../../../protocol').ConfigFieldSpan>;
	position: Position;
	parentId?: string;
	inputs: PortDefinition[];
	outputs: PortDefinition[];
	features: NodeFeatures;
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
	/// setConfig/removeConfig ops stamped with the value's written form.
	portLiterals?: Record<string, unknown>;
	/// Flip a port value's WRITTEN form (braces vs statement); becomes a
	/// `setValueForm` op.
	// SYNC: portValueForm.form <-> crates/weft-compiler/src/edit.rs ValueForm, packages/weft-graph/src/protocol.ts EditOp setValueForm.form
	portValueForm?: { key: string; form: 'inline' | 'connection' };
	inputs?: PortDefinition[];
	outputs?: PortDefinition[];
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
 *  (its knobs live there), and any container carrying values written on
 *  its interface ports. Both the renderer and the layout engine ask
 *  this, so the strip and the space reserved for it cannot disagree. */
export function containerHasConfigStrip(
	nodeType: unknown,
	portLiterals: Record<string, unknown> | undefined,
): boolean {
	if (!isContainerNodeType(nodeType)) return false;
	return isLoopNodeType(nodeType) || Object.keys(portLiterals ?? {}).length > 0;
}

/** True iff a node is a Loop container (used by renderer + visual
 *  differentiation; for structural checks prefer `isContainerNodeType`). */
export function isLoopNodeType(nodeType: unknown): boolean {
	return containerKindOf(nodeType) === 'Loop';
}
