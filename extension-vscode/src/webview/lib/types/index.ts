/**
 * Unified type definitions for Weft Dashboard
 *
 * This is the single source of truth for all types.
 */

import type {
	BusInspectorEvent,
	BusMeta,
	CorruptionSite,
	LoopInspectorEvent,
	LoopIteration,
	NodeExecutionStatus,
	NodeFeaturesWire as NodeFeatures,
} from '../../../shared/protocol';
// Node feature flags ARE the wire type `NodeFeaturesWire`, aliased to
// `NodeFeatures` and re-exported under the `$lib/types` alias so webview
// imports stay uniform and there is one definition of the concept (not a
// webview-local copy that drifts from the wire shape).
export type { NodeExecutionStatus };
export type { NodeFeaturesWire as NodeFeatures } from '../../../shared/protocol';

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

// SYNC: WeftPrimitive <-> crates/weft-core/src/weft_type.rs WeftPrimitive
export type WeftPrimitive =
	| "String" | "Number" | "Boolean" | "Null"
	| "Image" | "Video" | "Audio" | "Blob"
	| "Empty";

/** A port type string. Supports recursive syntax: List[String], Dict[K, V], unions, type vars. */
export type PortType = string;

/** All recognized primitive type names */
export const ALL_PRIMITIVE_TYPES: WeftPrimitive[] = [
	"String", "Number", "Boolean", "Null",
	"Image", "Video", "Audio", "Blob", "Empty",
];

// SYNC: NAMED_UNIONS <-> crates/weft-core/src/weft_type.rs WeftType::named_union
/** Named union aliases. The ONE place a union name expands (mirrors the
 *  backend registry). `Media` = media-proper (Image|Video|Audio); `File`
 *  = any stored file (Media + the Blob catch-all). Resolved generically,
 *  never a per-name branch in the parser; future user-defined unions
 *  register here. */
export const NAMED_UNIONS: Record<string, WeftPrimitive[]> = {
	Media: ["Image", "Video", "Audio"],
	File: ["Image", "Video", "Audio", "Blob"],
};

/** The primitive members of the `File` union: the single source of truth
 *  for "is this primitive a stored-file reference". */
export const FILE_PRIMITIVES: WeftPrimitive[] = NAMED_UNIONS.File;

// ── Parsed type representation ──────────────────────────────────────────────

export type WeftType =
	| { kind: 'primitive'; value: WeftPrimitive }
	| { kind: 'list'; inner: WeftType }
	| { kind: 'dict'; key: WeftType; value: WeftType }
	| { kind: 'json_dict' }
	// A message-bus handle: an in-process channel between co-alive nodes.
	// A Bus output connects only to a Bus input; the payloads are not
	// type-checked. Wired-only (a live runtime handle, never configurable).
	| { kind: 'bus' }
	| { kind: 'union'; types: WeftType[] }
	| { kind: 'typevar'; name: string }
	| { kind: 'must_override' };

/** Type variable names users can write: T, T1, T2, ... T99.
 *
 *  Also accepted (catalog-internal only, not user-facing):
 *    - `T_Auto`: sentinel used by form-field port specs to request a
 *      per-port-instance TypeVar. Replaced with `T__{key}` at enrichment time.
 *    - `T__scope` (e.g. `T__hook`): materialized form of a `T_Auto` marker.
 *      Must parse because port types round-trip through strings in the frontend.
 *
 *  These internal forms exist so catalog authors can express "this port
 *  accepts anything, independently from sibling ports" without forcing the
 *  same rule on nodes that want shared `T` semantics (Gate, etc.). */
function isTypeVarName(s: string): boolean {
	if (!s) return false;
	if (s === 'T_Auto') return true;
	if (!s.startsWith('T')) return false;
	if (s.length === 1) return true;
	const rest = s.slice(1);
	if (/^\d+$/.test(rest)) return true;
	if (rest.startsWith('__')) {
		const scope = rest.slice(2);
		return scope.length > 0 && /^[A-Za-z0-9_]+$/.test(scope);
	}
	return false;
}

/** Split string on delimiter, but only at top level (not inside []) */
function splitTopLevel(s: string, delimiter: string): string[] {
	const parts: string[] = [];
	let depth = 0;
	let start = 0;
	for (let i = 0; i < s.length; i++) {
		if (s[i] === '[') depth++;
		else if (s[i] === ']') depth--;
		else if (s[i] === delimiter && depth === 0) {
			parts.push(s.slice(start, i));
			start = i + 1;
		}
	}
	parts.push(s.slice(start));
	return parts;
}

function parseSingleType(s: string): WeftType | null {
	s = s.trim();
	// Named union aliases (Media, File, future user unions) all resolve
	// through the one registry, never a per-name branch.
	const alias = NAMED_UNIONS[s];
	if (alias !== undefined) {
		return { kind: 'union', types: alias.map(t => ({ kind: 'primitive', value: t })) };
	}
	if (s === 'JsonDict') return { kind: 'json_dict' };
	if (s === 'Bus') return { kind: 'bus' };
	if (s === 'MustOverride') return { kind: 'must_override' };

	// Parameterized: List[...], Dict[...]
	const bracketPos = s.indexOf('[');
	if (bracketPos !== -1) {
		if (!s.endsWith(']')) return null;
		const name = s.slice(0, bracketPos).trim();
		const inner = s.slice(bracketPos + 1, -1);

		if (name === 'List') {
			const innerType = parseWeftType(inner);
			return innerType ? { kind: 'list', inner: innerType } : null;
		}
		if (name === 'Dict') {
			const parts = splitTopLevel(inner, ',');
			if (parts.length !== 2) return null;
			const key = parseWeftType(parts[0].trim());
			const val = parseWeftType(parts[1].trim());
			return key && val ? { kind: 'dict', key, value: val } : null;
		}
		return null;
	}

	// Primitive
	if ((ALL_PRIMITIVE_TYPES as string[]).includes(s)) {
		return { kind: 'primitive', value: s as WeftPrimitive };
	}

	// Type variable
	if (isTypeVarName(s)) {
		return { kind: 'typevar', name: s };
	}

	return null;
}

/** Parse a port type string into a structured representation. */
export function parseWeftType(s: string): WeftType | null {
	const trimmed = s.trim();
	if (!trimmed) return null;

	// Split on top-level | for unions
	const parts = splitTopLevel(trimmed, '|');
	if (parts.length > 1) {
		const types: WeftType[] = [];
		for (const part of parts) {
			const parsed = parseSingleType(part.trim());
			if (!parsed) return null;
			types.push(parsed);
		}
		// Flatten nested unions, dedup
		const flat: WeftType[] = [];
		for (const t of types) {
			if (t.kind === 'union') flat.push(...t.types);
			else flat.push(t);
		}
		const seen = new Set<string>();
		const deduped: WeftType[] = [];
		for (const t of flat) {
			const key = weftTypeToString(t);
			if (!seen.has(key)) {
				seen.add(key);
				deduped.push(t);
			}
		}
		return deduped.length === 1 ? deduped[0] : { kind: 'union', types: deduped };
	}

	return parseSingleType(trimmed);
}

/** Convert a parsed type back to string form. */
export function weftTypeToString(t: WeftType): string {
	switch (t.kind) {
		case 'primitive': return t.value;
		case 'list': return `List[${weftTypeToString(t.inner)}]`;
		case 'dict': return `Dict[${weftTypeToString(t.key)}, ${weftTypeToString(t.value)}]`;
		case 'json_dict': return 'JsonDict';
		case 'bus': return 'Bus';
		case 'union': return t.types.map(weftTypeToString).join(' | ');
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
		case 'union': return t.types.flatMap(extractPrimitives);
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
	return false;
}

// ── Type inference from runtime values ──────────────────────────────────────

// SYNC: STORED_FILE_MARKER_TYPES <-> crates/weft-core/src/weft_type.rs FileKind
/** The per-kind sentinel key -> primitive type. A stored-file value
 *  carries its CONCRETE type as its marker key; the type is read from
 *  the marker, never re-derived from the mime string. */
const STORED_FILE_MARKER_TYPES: Record<string, WeftPrimitive> = {
	'__weft_image__': 'Image',
	'__weft_video__': 'Video',
	'__weft_audio__': 'Audio',
	'__weft_blob__': 'Blob',
};
const FILE_HANDLE_KEYS = ['url', 'data', 'key'];

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
		for (const [marker, prim] of Object.entries(STORED_FILE_MARKER_TYPES)) {
			const payload = obj[marker];
			if (typeof payload === 'object' && payload !== null
				&& FILE_HANDLE_KEYS.some(k => k in (payload as Record<string, unknown>))) {
				return { kind: 'primitive', value: prim };
			}
		}
		const values = Object.values(obj);
		if (values.length === 0) {
			return { kind: 'dict', key: { kind: 'primitive', value: 'String' }, value: { kind: 'primitive', value: 'Empty' } };
		}
		const valueTypes = values.map(inferTypeFromValue);
		return { kind: 'dict', key: { kind: 'primitive', value: 'String' }, value: unifyTypes(valueTypes) };
	}
	return { kind: 'primitive', value: 'String' };
}

/** Whether a port of this type should be configurable by default (i.e.,
 *  fillable from a same-named config field). Mirrors
 *  WeftType::is_default_configurable() in Rust. File types, TypeVar, and
 *  MustOverride are wired-only; everything else (primitives, lists, dicts,
 *  JsonDict, unions of configurable types) defaults to configurable. */
export function isDefaultConfigurable(t: WeftType): boolean {
	switch (t.kind) {
		case 'primitive':
			return !FILE_PRIMITIVES.includes(t.value);
		case 'list':
			return isDefaultConfigurable(t.inner);
		case 'dict':
			return isDefaultConfigurable(t.value);
		case 'union':
			return t.types.every(isDefaultConfigurable);
		case 'json_dict':
			return true;
		case 'bus':
			// A bus is a live runtime handle, never configurable.
			return false;
		case 'typevar':
			return false;
		case 'must_override':
			return false;
	}
}

/** Whether a port is configurable. Uses the explicit `configurable` field
 *  when set; falls back to the default determined by the port type. */
export function isPortConfigurable(port: PortDefinition): boolean {
	if (port.configurable !== undefined) return port.configurable;
	const parsed = parseWeftType(port.portType);
	if (!parsed) return false;
	return isDefaultConfigurable(parsed);
}

/** Unify a list of types. If all identical, return that type. Otherwise, return a union. */
function unifyTypes(types: WeftType[]): WeftType {
	if (types.length === 0) return { kind: 'primitive', value: 'Empty' };
	const seen = new Set<string>();
	const unique: WeftType[] = [];
	for (const t of types) {
		const key = weftTypeToString(t);
		if (!seen.has(key)) { seen.add(key); unique.push(t); }
	}
	return unique.length === 1 ? unique[0] : { kind: 'union', types: unique };
}

export interface PortDefinition {
	name: string;
	portType: PortType;
	required: boolean;
	description?: string;
	/** Whether this port can be filled by a same-named config field on the
	 *  node (in addition to being wired by an edge). Defaults to true unless
	 *  the type is a File type or otherwise non-configurable. Catalog
	 *  authors opt out per port. Edge wins over config when both are present. */
	configurable?: boolean;
	/** True iff this is the auto-synthesized input half of a loop carry port.
	 *  The editor renders it as a ghost mirror of the carry output; the user
	 *  edits the output's role to remove or rename it, never this side. */
	synthesizedFromCarry?: boolean;
}

// =============================================================================
// Field Types (for node configuration UI)
// =============================================================================

// TODO: add 'openai' and 'anthropic' providers when we support direct API keys for those
export type ApiKeyProvider = "openrouter" | "elevenlabs" | "tavily" | "apollo";

export type FieldType = "text" | "textarea" | "code" | "select" | "multiselect" | "number" | "checkbox" | "password" | "api_key" | "form_builder";

export interface FieldDefinition {
	key: string;
	label: string;
	type: FieldType;
	placeholder?: string;
	options?: string[];
	defaultValue?: unknown;
	description?: string;
	provider?: ApiKeyProvider; // For api_key fields: which platform key to use
	min?: number; // For number fields: minimum allowed value (clamped on blur)
	max?: number; // For number fields: maximum allowed value (clamped on blur)
	step?: number; // For number fields: granularity of the input (used by slider/number)
	maxLength?: number; // For text/textarea fields: max character count, enforced in UI with counter
	minLength?: number; // For text/textarea fields: min character count
	pattern?: string; // For text fields: HTML5 regex validation pattern
}

// =============================================================================
// Node Template Types (defines what a node TYPE looks like)
// =============================================================================

export type NodeCategory = "Triggers" | "AI" | "Data" | "Flow" | "Utility" | "Debug" | "Infrastructure";

/** Record of a single execution of a node. The `NodeExecutionStatus`
 *  alias re-exported at the top of the file lets the wire and the UI
 *  share one source of truth and never drift on which states exist. */
export interface NodeExecution {
	id: string;
	nodeId: string;
	status: NodeExecutionStatus;
	pulseIdsAbsorbed: string[];
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
	output?: unknown;
	costUsd: number;
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

/** Live execution state the webview maintains from the extension
 *  host's SSE stream. Single source of truth: lifted here so
 *  `App.svelte` (which owns the state) and the editor components
 *  (which consume it) cannot drift on field shape. Webview-internal
 *  only: `Set<string>` does not survive the wire, so this type never
 *  appears in a `HostMessage`.
 */
export interface ExecutionState {
	isRunning: boolean;
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
}

/** A typed data item shown on a node's body-panel feed. The
 *  authoritative definition lives in `shared/protocol.ts`; this
 *  re-export keeps webview imports under the `$lib/types` alias.
 *  Adding a new kind: extend the union in protocol.ts AND add a
 *  branch in ProjectNode.svelte's render block.
 */
export type { LiveDataItem } from '../../../shared/protocol';


/**
 * Validation levels:
 * - structural: the project is correctly wired (connections, required config for structure)
 * - runtime: the project can actually execute (API keys, credentials, file data)
 */
export type ValidationLevel = 'structural' | 'runtime';

/**
 * A single validation error for a node.
 */
export interface ValidationError {
	field?: string;
	port?: string;
	message: string;
	level: ValidationLevel;
}

/**
 * Function signature for node validation.
 * Each node can optionally implement this to validate its configuration.
 * Forward declaration - full context types defined below.
 */
export type NodeValidateFunction = (context: ValidationContext) => ValidationError[];

/**
 * NodeTemplate defines what a node TYPE looks like.
 * This is the schema/blueprint for nodes like "LlmInference", "ExecPython", "Http".
 * Each node type has one template.
 */
export interface NodeTemplate {
	type: string;
	label: string;
	description: string;
	icon: import('svelte').Component;
	color: string;
	category: NodeCategory;
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
	fields: FieldDefinition[];
	defaultInputs: PortDefinition[];
	defaultOutputs: PortDefinition[];
	features?: NodeFeatures;
	validate?: NodeValidateFunction;
	setupGuide?: string[];
	formFieldSpecs?: import('$lib/utils/form-field-specs').FormFieldSpec[];
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
	// descriptions are per-group (`# Description:`). (Matches the Rust wire type.)
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
	inputs?: PortDefinition[];
	outputs?: PortDefinition[];
	/// Set ONLY when the user dragged the resize handle. The host re-runs ELK on a
	/// resize (neighbours make room), so this must distinguish a real user resize
	/// from a programmatic dimension write (min-height auto-enforce, a rebuild after
	/// a move), which carry width/height too but must NOT trigger a relayout.
	resized?: boolean;
}

// =============================================================================
// Node Validation Types (ValidationContext defined here after NodeInstance/Edge)
// =============================================================================

/**
 * Context provided to a node's validate function.
 * Contains all information needed to validate the node's configuration.
 */
export interface ValidationContext {
	config: Record<string, unknown>;
	connectedInputs: Set<string>;
	allNodes: NodeInstance[];
	allEdges: Edge[];
	nodeId: string;
}

/**
 * Result of validating all nodes in a project.
 */
export interface ProjectValidationResult {
	valid: boolean;
	nodeErrors: Map<string, ValidationError[]>;
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

/** True iff a node is a Loop container (used by renderer + visual
 *  differentiation; for structural checks prefer `isContainerNodeType`). */
export function isLoopNodeType(nodeType: unknown): boolean {
	return containerKindOf(nodeType) === 'Loop';
}
