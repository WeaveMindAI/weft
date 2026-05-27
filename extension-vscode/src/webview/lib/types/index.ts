/**
 * Unified type definitions for Weft Dashboard
 *
 * This is the single source of truth for all types.
 */

// =============================================================================
// PORT TYPE SYSTEM
//
// Python-style recursive types with strict enforcement. No Any type.
//
// Primitives:     String, Number, Boolean, Image, Video, Audio, Document
// Parameterized:  List[T], Dict[K, V]
// Unions:         String | Number, List[String] | String
// Aliases:        Media = Image | Video | Audio | Document
// Type variables: T, T1, T2..., node-scoped, same T on input and output = same type
// MustOverride:   Node can't know the type, user/AI must declare it in Weft code
//
// Port types describe what the node sees post-operation:
//   Expand input (<): type is the element type T. Compiler validates List[T] arrives.
//   Gather input (>): type is List[T] (collected). Compiler validates stack context.
//   Stack depth tracked by compiler, not in the type system.
//
// USING PORT TYPES IN NODE DEFINITIONS (frontend.ts):
//   portType: 'String'
//   portType: 'List[String]'
//   portType: 'Dict[String, Number]'
//   portType: 'String | Number'
//   portType: 'T'                    type variable
//   portType: 'MustOverride'         user must declare type in Weft
// =============================================================================

export type WeftPrimitive =
	| "String" | "Number" | "Boolean" | "Null"
	| "Image" | "Video" | "Audio" | "Document"
	| "Empty";

/** A port type string. Supports recursive syntax: List[String], Dict[K, V], unions, type vars. */
export type PortType = string;

/** All recognized primitive type names */
export const ALL_PRIMITIVE_TYPES: WeftPrimitive[] = [
	"String", "Number", "Boolean", "Null",
	"Image", "Video", "Audio", "Document", "Empty",
];

/** Built-in alias: Media expands to Image | Video | Audio | Document */
export const MEDIA_TYPES: WeftPrimitive[] = ["Image", "Video", "Audio", "Document"];

// ── Parsed type representation ──────────────────────────────────────────────

export type WeftType =
	| { kind: 'primitive'; value: WeftPrimitive }
	| { kind: 'list'; inner: WeftType }
	| { kind: 'dict'; key: WeftType; value: WeftType }
	| { kind: 'json_dict' }
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
	if (s === 'Media') {
		return {
			kind: 'union',
			types: MEDIA_TYPES.map(t => ({ kind: 'primitive', value: t })),
		};
	}
	if (s === 'JsonDict') return { kind: 'json_dict' };
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

const MEDIA_KEYS = ['url', 'data'];
const MIME_PREFIXES: Record<string, WeftPrimitive> = {
	'image/': 'Image', 'video/': 'Video', 'audio/': 'Audio',
};

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
		// Detect media objects
		const hasUrl = MEDIA_KEYS.some(k => k in obj);
		const mime = (obj['mimeType'] ?? obj['mimetype']) as string | undefined;
		if (hasUrl && typeof mime === 'string') {
			for (const [prefix, prim] of Object.entries(MIME_PREFIXES)) {
				if (mime.startsWith(prefix)) return { kind: 'primitive', value: prim };
			}
			return { kind: 'primitive', value: 'Document' };
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
 *  WeftType::is_default_configurable() in Rust. Media types, TypeVar, and
 *  MustOverride are wired-only; everything else (primitives, lists, dicts,
 *  JsonDict, unions of configurable types) defaults to configurable. */
export function isDefaultConfigurable(t: WeftType): boolean {
	switch (t.kind) {
		case 'primitive':
			return t.value !== 'Image' && t.value !== 'Video' && t.value !== 'Audio' && t.value !== 'Document';
		case 'list':
			return isDefaultConfigurable(t.inner);
		case 'dict':
			return isDefaultConfigurable(t.value);
		case 'union':
			return t.types.every(isDefaultConfigurable);
		case 'json_dict':
			return true;
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

/** How a port interacts with the lane/stack system.
 * - "Single" (default): normal, one value per lane
 * - "Expand": this port carries a list that expands into N lanes downstream
 * - "Gather": this port collects values from all N lanes into a single list */
export type LaneMode = "Single" | "Expand" | "Gather";

export interface PortDefinition {
	name: string;
	portType: PortType;
	required: boolean;
	description?: string;
	laneMode?: LaneMode;
	/** Number of List[] levels to expand/gather. Default 1. */
	laneDepth?: number;
	/** Whether this port can be filled by a same-named config field on the
	 *  node (in addition to being wired by an edge). Defaults to true unless
	 *  the type is a Media type or otherwise non-configurable. Catalog
	 *  authors opt out per port. Edge wins over config when both are present. */
	configurable?: boolean;
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

/** Status of a single node execution. `suspended` is a real runtime state
 *  (the protocol's NodeExecutionStarted state union + status.ts + the node UI
 *  all handle it); it was missing from this type, so the UI's `=== 'suspended'`
 *  checks were dead branches the (never-run) webview typecheck flagged. */
export type NodeExecutionStatus = 'running' | 'completed' | 'failed' | 'waiting_for_input' | 'suspended' | 'skipped' | 'cancelled';

/** Record of a single execution of a node. */
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
	output?: unknown;
	costUsd: number;
	logs: unknown[];
	color: string;
	lane: Array<{ count: number; index: number }>;
	/// Stringified lane stack used to correlate completion
	/// events to the right running row when several lanes run
	/// in parallel. Empty for non-parallel runs.
	laneKey?: string;
}

/** Node executions keyed by node ID. */
export type NodeExecutionTable = Record<string, NodeExecution[]>;

/** A typed data item shown on a node's body-panel feed. The
 *  authoritative definition lives in `shared/protocol.ts`; this
 *  re-export keeps webview imports under the `$lib/types` alias.
 *  Adding a new kind: extend the union in protocol.ts AND add a
 *  branch in ProjectNode.svelte's render block.
 */
export type { LiveDataItem } from '../../../shared/protocol';

export interface NodeFeatures {
	isTrigger?: boolean;
	canAddInputPorts?: boolean;
	canAddOutputPorts?: boolean;
	hidden?: boolean;
	showRunLocationSelector?: boolean;
	showDebugPreview?: boolean;
	/** Node has a dynamic form schema. Ports are derived from config.fields via the node's formFieldSpecs. */
	hasFormSchema?: boolean;
	/** Names the endpoint serving the node's `/live` HTTP route the
	 * body panel polls. Unset for TCP-only infra (Postgres, Redis). */
	liveEndpoint?: string;
	/** Groups of ports where at least one must be non-null for the node to execute.
	 * If all ports in a group are null/missing, the node is skipped.
	 * e.g. [['text', 'media']] = at least one of text/media must be non-null. */
	oneOfRequired?: string[][];
}

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
	name: string;
	description: string | null;
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

