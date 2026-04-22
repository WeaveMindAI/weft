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

export type FieldType = "text" | "textarea" | "code" | "select" | "multiselect" | "number" | "checkbox" | "password" | "blob" | "api_key" | "form_builder";

export interface FileRef {
	file_id?: string;  // Only present for cloud-managed files
	url: string;
	filename: string;
	mime_type: string;
	size_bytes: number;
}

export interface FieldDefinition {
	key: string;
	label: string;
	type: FieldType;
	placeholder?: string;
	options?: string[];
	defaultValue?: unknown;
	description?: string;
	accept?: string; // For blob fields: mime type filter (e.g., 'audio/*', 'image/*')
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

/**
 * General trigger categories - fundamental ways a project can be triggered.
 * - Webhook: HTTP endpoint that receives POST requests (Discord, GitHub, Slack, custom, etc.)
 * - Polling: Periodically checks an external source for changes (RSS, API polling, etc.)
 * - Schedule: Time-based triggers using cron expressions
 * - Socket: Persistent connections (WebSocket, SSE, etc.)
 * - Local: Triggers that only work locally (file watcher, system events, etc.)
 * - Manual: Triggered manually by user action
 */
export type TriggerCategory = "Webhook" | "Polling" | "Schedule" | "Socket" | "Local" | "Manual";

export type Capability = "DurableKV";

export interface ReadinessCheck {
	TcpSocket?: { port: number };
	HttpGet?: { port: number; path: string };
}

export interface ActionEndpoint {
	port: number;
	path: string;
}

export interface KubeManifest {
	manifest: Record<string, unknown>;
}

export interface InfrastructureSpec {
	manifests: KubeManifest[];
	readinessCheck: ReadinessCheck;
	actionEndpoint: ActionEndpoint;
}

export type RunLocationConstraint = "local" | "cloud" | "any";

/** Status of a single node execution. */
export type NodeExecutionStatus = 'running' | 'completed' | 'failed' | 'waiting_for_input' | 'skipped' | 'cancelled';

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
}

/** Node executions keyed by node ID. */
export type NodeExecutionTable = Record<string, NodeExecution[]>;

/** A typed data item returned by the sidecar's /live endpoint for dashboard rendering. */
export interface LiveDataItem {
	type: 'text' | 'image' | 'progress';
	label: string;
	/** For text: the string to display. For image: a data URI. For progress: a number 0-1. */
	data: string | number;
}

export interface NodeFeatures {
	isTrigger?: boolean;
	triggerCategory?: TriggerCategory;
	runLocationConstraint?: RunLocationConstraint;
	canAddInputPorts?: boolean;
	canAddOutputPorts?: boolean;
	hidden?: boolean;
	showRunLocationSelector?: boolean;
	showDebugPreview?: boolean;
	/** Node is an infrastructure node (long-running, provides actions to other nodes) */
	isInfrastructure?: boolean;
	/** Node has a dynamic form schema. Ports are derived from config.fields via the node's formFieldSpecs. */
	hasFormSchema?: boolean;
	/** Infrastructure deployment specification (only for infrastructure nodes).
	 * Defines the K8s manifests, readiness check, and action endpoint.
	 * The node itself controls how it gets deployed. */
	infrastructureSpec?: InfrastructureSpec;
	/** Sidecar exposes a /live endpoint with typed data items for real-time dashboard display. */
	hasLiveData?: boolean;
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
	tags: string[];
	fields: FieldDefinition[];
	defaultInputs: PortDefinition[];
	defaultOutputs: PortDefinition[];
	features?: NodeFeatures;
	/** Whether this node is always included in the AI builder's context.
	 *  Non-base nodes are only available via the node shopping assistant. */
	isBase?: boolean;
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
	/** Return display items to show on the node (e.g. webhook URL after activation).
	 *  Called by the editor when project state changes. */
	getDisplayData?: (node: NodeInstance, context: DisplayDataContext) => LiveDataItem[];
}

/** Context passed to NodeTemplate.getDisplayData */
export interface DisplayDataContext {
	projectId: string;
	isProjectActive: boolean;
	apiBaseUrl: string;
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
}

// =============================================================================
// Setup Manifest Types (Builder/Runner mode)
// =============================================================================

/**
 * Audience a manifest element is shown to.
 * - 'admin': only the project owner sees it in the builder/admin runner view.
 * - 'visitor': only people who visit the published deploy page see it.
 * - 'both': everyone sees it (default for regular inputs/outputs).
 *
 * Sensitive field types (password, api_key) are forced to 'admin' by the
 * renderer regardless of what the DSL declares. Infra lifecycle controls
 * and trigger controls are also admin-only and not configurable from the DSL.
 */
export type Visibility = 'admin' | 'visitor' | 'both';

/** Render mode for the runner view. */
export type RunnerMode = 'admin' | 'visitor';

/**
 * Optional presentation variant for a field/output/live item.
 * The renderer picks a reasonable default based on the field/port type;
 * `as` lets the DSL override it. Unknown values fall back to the default.
 */
export type ItemVariant =
	// Text inputs
	| 'text' | 'textarea' | 'password' | 'email' | 'url'
	// Numeric
	| 'number' | 'slider'
	// Boolean
	| 'toggle' | 'checkbox'
	// Pickers (single-select)
	| 'radio' | 'select' | 'cards'
	// Pickers (multi-select)
	| 'multiselect' | 'tags' | 'multicards'
	// Specialized
	| 'date' | 'time' | 'datetime' | 'color' | 'file'
	// Output-only
	| 'markdown' | 'code' | 'json' | 'image' | 'gallery' | 'audio'
	| 'video' | 'download' | 'progress' | 'chart' | 'log';

/**
 * A single field exposed to the runner for configuration.
 * References a specific field on a specific node instance.
 */
export interface SetupItem {
	id: string;
	nodeId: string;
	fieldKey: string;
	label?: string;        // overrides the node field's default label
	description?: string;  // supplements the node's setupGuide
	visibility?: Visibility;
	as?: ItemVariant;
	/** Inline options for select/radio/cards/multiselect variants. Supplements
	 *  or overrides any options declared on the node field itself. Accepts
	 *  comma-separated values (`"a,b,c"`) or a JSON-encoded array. */
	options?: string[];
	// Sizing overrides (raw CSS or named presets). Used by height-sensitive
	// variants like textarea, markdown output, image/video, chart.
	height?: string;
	minHeight?: string;
	maxHeight?: string;
	width?: string;
	// Visual container: by default fields and outputs are chromeless (just
	// label + input) so they inherit the parent's box. Set to 'card' to force
	// a rounded bordered container, 'subtle' for a hairline separator only.
	chrome?: 'none' | 'subtle' | 'card';
}

/**
 * A single output port exposed to the runner for result display.
 * The runner sees the live value of this port after/during execution.
 */
export interface OutputItem {
	id: string;
	nodeId: string;
	portName: string;    // the output port name (e.g. "response", "data")
	label?: string;      // display label override
	description?: string;
	visibility?: Visibility;
	as?: ItemVariant;
	height?: string;
	minHeight?: string;
	maxHeight?: string;
	width?: string;
	chrome?: 'none' | 'subtle' | 'card';
	/** Placeholder text shown when the output is empty (before first run). */
	placeholder?: string;
}

/**
 * A live data display from an infrastructure node, shown in the runner view.
 * References a node whose sidecar exposes a /live endpoint with LiveDataItem[].
 */
export interface LiveItem {
	id: string;
	nodeId: string;
	label?: string;      // display label override
	description?: string;
	visibility?: Visibility;
	as?: ItemVariant;
	height?: string;
	minHeight?: string;
	maxHeight?: string;
	width?: string;
	chrome?: 'none' | 'subtle' | 'card';
}

/**
 * A logical group of setup items shown as a section in the runner view.
 * A phase can also contain nested bricks (columns, card, etc.) via `children`,
 * so you can build rich section layouts that mix fields with decoration.
 */
export interface SetupPhase {
	id: string;
	title: string;
	description?: string;
	items: SetupItem[];
	liveItems?: LiveItem[];
	visibility?: Visibility;
	/** Additional blocks rendered inside this phase after items/liveItems.
	 *  Used for layout bricks (columns, card) that wrap more content. */
	children?: Block[];
}

/**
 * Visual theme for the runner page.
 * All fields optional; renderer falls back to sensible defaults.
 *
 * The `skin` field selects a bundle of presets (fonts, surfaces, default
 * card chrome, spacing). Individual properties override whatever the skin
 * sets, so you can pick "studio" and then tweak only the primary color.
 */
export interface RunnerTheme {
	primary?: string;       // hex or tailwind color token
	accent?: string;
	background?: string;
	font?: 'inter' | 'serif' | 'mono' | 'display' | string;
	mode?: 'light' | 'dark' | 'auto';
	radius?: 'none' | 'sm' | 'md' | 'lg' | 'xl' | '2xl' | '3xl' | 'full';
	layout?: 'narrow' | 'centered' | 'wide' | 'ultrawide' | 'full';
	/** Aesthetic preset. See skins.ts for the full list. */
	skin?: 'default' | 'editorial' | 'studio' | 'brutalist' | 'terminal' | 'playful' | string;
	/** Page-wide background treatment. Overrides skin's default surface. */
	surface?: 'plain' | 'subtle' | 'gradient' | 'glass' | 'dark' | 'mesh';
	/** Vertical spacing multiplier for the main block stack. */
	density?: 'compact' | 'comfortable' | 'spacious';
	/** Explicit content width override (raw CSS) takes priority over layout preset. */
	contentWidth?: string;
	/** Page padding preset. */
	padding?: 'sm' | 'md' | 'lg' | 'xl' | '2xl';
}

/**
 * A decorative or structural brick in the runner page.
 * Bricks are non-interactive presentation elements (or simple CTAs) that
 * don't bind to a Weft node. The `kind` discriminates the shape of `props`.
 */
export interface Brick {
	id: string;
	kind: BrickKind;
	visibility?: Visibility;
	props: Record<string, unknown>;
	children?: Block[];
}

export type BrickKind =
	| 'hero'
	| 'navbar'
	| 'navlink'
	| 'logo'
	| 'banner'
	| 'text'
	| 'heading'
	| 'divider'
	| 'image'
	| 'video'
	| 'embed'
	| 'quote'
	| 'stat'
	| 'stats'
	| 'feature'
	| 'feature-grid'
	| 'faq'
	| 'qa'
	| 'testimonial'
	| 'badge'
	| 'spacer'
	| 'section'
	| 'columns'
	| 'card'
	| 'tabs'
	| 'tab'
	| 'cta'
	| 'footer';

/**
 * An ordered entry in the manifest's top-level block list.
 * Everything the page renders lives here, in order, so the LLM and the
 * renderer agree on layout without a second tree.
 */
export type Block =
	| { kind: 'phase'; phase: SetupPhase }
	| { kind: 'brick'; brick: Brick }
	| { kind: 'output'; output: OutputItem }
	| { kind: 'live'; live: LiveItem };

/**
 * Parsed view of a project's loom setup. Derived from `loomCode` by
 * `hydrateProject` on every store read, consumed by the runner view
 * to render the setup page. This is NEVER a write target: all edits
 * go through the raw `loomCode` text and re-parse. The serializer
 * that used to round-trip this shape back to text was deleted
 * because it corrupted multi-line text blocks and comments.
 *
 * `blocks`, when present, is the source-order block list the runner
 * walks when rendering (hero, section, cta, phase, output, etc.).
 * The current parser always populates it, so consumers produced by
 * `parseLoom` can rely on it being non-null. The field is marked
 * optional for legacy callers that construct a manifest without a
 * block list (programmatic tests, fallback synthesis).
 *
 * `phases`, `outputs`, and `liveItems` are flat convenience indexes
 * the parser also populates alongside `blocks` so filters,
 * validation, and visitor-access computation don't have to walk the
 * block tree. They may contain the same items referenced from
 * `blocks`; treat them as read-only projections.
 */
export interface SetupManifest {
	phases: SetupPhase[];
	outputs: OutputItem[];
	liveItems?: LiveItem[];
	theme?: RunnerTheme;
	blocks?: Block[];
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
	loomCode?: string | null;
	layoutCode?: string | null;
	// Derived in-memory from weftCode (not stored)
	nodes: NodeInstance[];
	edges: Edge[];
	// Derived in-memory from loomCode (not stored)
	setupManifest?: SetupManifest;
	// Weft editor state (persisted alongside weftCode)
	weftOpaqueBlocks?: unknown[];
	weftItemOrder?: string[];
	weftItemGaps?: number[];
	createdAt: string;
	updatedAt: string;
	/** True when this project row is a deployment snapshot (created via
	 *  Publish). Deployment projects are hidden from the builder list and
	 *  their code is read-only in the builder view, but the admin runner
	 *  path can still tweak field values. */
	isDeployment?: boolean;
	/** For deployments: the builder project this one was cloned from.
	 *  Null for builder rows. */
	originProjectId?: string | null;
}

export interface NodeInputs {
	[portName: string]: unknown;
}

export interface NodeOutputs {
	[portName: string]: unknown;
}

export interface MessagePayload {
	data: Record<string, unknown>;
	sourceNodeId: string | null;
	timestamp: string;
}

// =============================================================================
// Execution Types
// =============================================================================

export type ExecutionStatus =
	| "pending"
	| "running"
	| "waiting_for_input"
	| "paused"
	| "completed"
	| "failed"
	| "cancelled";

export interface ProjectExecution {
	id: string;
	projectId: string;
	status: ExecutionStatus;
	startedAt: string;
	completedAt: string | null;
	currentNode: string | null;
	state: Record<string, unknown>;
	error: string | null;
}

export interface TriggerConfig {
	id: string;
	triggerId: string;
	triggerCategory: TriggerCategory;
	nodeType: string;
	projectId: string;
	triggerNodeId: string;
	config: Record<string, unknown>;
	credentials?: Record<string, unknown>;
	enabled: boolean;
	runLocation: "cloud" | "local";
	createdAt: string;
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

