/**
 * Node Registry: VS Code extension build.
 *
 * v1's standalone dashboard ships one `.ts` file per node template
 * under this folder and auto-discovers them at build time. The VS
 * Code extension instead gets catalog entries from the local `weft`
 * CLI: `weft describe-nodes` for the full set (via setCatalog) and
 * `weft parse` for the referenced subset (via registerCatalog), both
 * reading the project's `nodes/` folder. We populate NODE_TYPE_CONFIG
 * at runtime, BEFORE App.svelte mounts ProjectEditor, so downstream
 * `$derived` reads on first render see a populated registry.
 */

import type { NodeTemplate, NodeCategory, FieldDefinition, PortDefinition, NodeFeatures } from '$lib/types';
import type { Component } from 'svelte';
import {
	Activity,
	AlertCircle,
	Archive,
	Bot,
	BrainCircuit,
	Braces,
	Bug,
	CheckCircle,
	ChevronRight,
	Clock,
	Code2,
	Cog,
	Database,
	FileText,
	Filter,
	Folder,
	GitBranch,
	GitFork,
	Globe,
	HardDrive,
	Hash,
	Image as ImageIcon,
	Info,
	Key,
	Keyboard,
	Layers,
	Link,
	List,
	Mail,
	MessageSquare,
	Mic,
	Network,
	Package,
	Pencil,
	Play,
	Plug,
	Puzzle,
	RefreshCw,
	Repeat,
	Search,
	Send,
	Server,
	Settings,
	Share2,
	Shield,
	Shuffle,
	Sliders,
	Sparkles,
	Split,
	Square,
	Terminal,
	Type,
	User,
	Users,
	Video,
	Volume2,
	Webhook,
	Wrench,
	Zap,
} from '@lucide/svelte';

export type { NodeTemplate, NodeCategory } from '$lib/types';

// Dispatcher icon-name → Lucide Svelte component. Names match the
// entries Rust-side metadata.json files use. Unknown names fall
// back to Square (generic node glyph).
const ICON_MAP: Record<string, Component> = {
	Activity,
	AlertCircle,
	Archive,
	Bot,
	BrainCircuit,
	Braces,
	Bug,
	CheckCircle,
	ChevronRight,
	Clock,
	Code2,
	Cog,
	Database,
	FileText,
	Filter,
	Folder,
	GitBranch,
	GitFork,
	Globe,
	HardDrive,
	Hash,
	Image: ImageIcon,
	Info,
	Key,
	Keyboard,
	Layers,
	Link,
	List,
	Mail,
	MessageSquare,
	Mic,
	Network,
	Package,
	Pencil,
	Play,
	Plug,
	Puzzle,
	RefreshCw,
	Repeat,
	Search,
	Send,
	Server,
	Settings,
	Share2,
	Shield,
	Shuffle,
	Sliders,
	Sparkles,
	Split,
	Square,
	Terminal,
	Type,
	User,
	Users,
	Video,
	Volume2,
	Webhook,
	Wrench,
	Zap,
};

function resolveIcon(name: string | undefined): Component {
	if (!name) return Square as Component;
	return (ICON_MAP[name] as Component) ?? (Square as Component);
}

// Dispatcher-side FieldDef shape. Rust's NodeMetadata nests the
// kind under field_type with serde(tag = "kind"), so textarea
// arrives as { kind: "textarea" } and select arrives as
// { kind: "select", options: [...] }. v1 components read flat
// properties (type, options, min, max, provider, ...) so
// we transform here.
interface RustFieldType {
	kind: string;
	options?: string[];
	min?: number;
	max?: number;
	provider?: string;
	placeholder?: string;
}

interface RustFieldDef {
	key: string;
	label: string;
	field_type: RustFieldType;
	default_value?: unknown;
	required?: boolean;
	description?: string;
	placeholder?: string;
}

// Shape sent per node type. Mirrors Rust NodeMetadata serialization
// plus the per-node FormFieldSpec[] inlined by `weft describe-nodes`.
export interface CatalogEntry {
	type: string;
	label: string;
	description: string;
	category: string;
	tags?: string[];
	icon?: string;
	color?: string;
	inputs?: PortDefinition[];
	outputs?: PortDefinition[];
	fields?: RustFieldDef[];
	entry?: unknown[];
	requires_infra?: boolean;
	features?: NodeFeatures;
	formFieldSpecs?: import('$lib/utils/form-field-specs').FormFieldSpec[];
}

function flattenField(f: RustFieldDef): FieldDefinition {
	const ft = f.field_type ?? { kind: 'text' };
	// eslint-disable-next-line @typescript-eslint/no-explicit-any
	const flat: any = {
		key: f.key,
		label: f.label,
		type: ft.kind,
		required: f.required,
		description: f.description,
	};
	if (ft.options) flat.options = ft.options;
	if (ft.min !== undefined) flat.min = ft.min;
	if (ft.max !== undefined) flat.max = ft.max;
	if (ft.provider) flat.provider = ft.provider;
	if (ft.placeholder) flat.placeholder = ft.placeholder;
	if (f.placeholder) flat.placeholder = f.placeholder;
	if (f.default_value !== undefined && f.default_value !== null)
		flat.defaultValue = f.default_value;
	return flat as FieldDefinition;
}

function toTemplate(entry: CatalogEntry): NodeTemplate {
	return {
		type: entry.type,
		label: entry.label,
		description: entry.description,
		icon: resolveIcon(entry.icon),
		color: entry.color ?? '#71717a',
		category: entry.category as NodeCategory,
		// Empty-array default keeps the command palette's
		// `tags.some(...)` call safe for nodes that declare no tags.
		tags: entry.tags ?? [],
		// Mirror the catalog metadata's `requires_infra` flag onto
		// the template. The webview's infra-subgraph extractor +
		// node-role helpers read this to decide which nodes are
		// infra-backed. Without it the subgraph "eye" finds no
		// seeds and shows an empty subgraph.
		requiresInfra: entry.requires_infra ?? false,
		fields: (entry.fields ?? []).map(flattenField),
		defaultInputs: entry.inputs ?? [],
		defaultOutputs: entry.outputs ?? [],
		features: entry.features,
		// `weft describe-nodes` ships the field-type vocabulary inline
		// for nodes whose features.hasFormSchema is true; the
		// form_builder editor reads it via `typeConfig.formFieldSpecs`.
		formFieldSpecs: entry.formFieldSpecs,
	};
}

// Reactive registry. Svelte 5's $state proxy tracks property
// mutations; components that read NODE_TYPE_CONFIG[type] or call
// getAllNodes() inside $derived get re-run when registerCatalog
// adds entries.
const registry: Record<string, NodeTemplate> = $state({});

export const NODE_TYPE_CONFIG = registry;

/** Reactive view over the registry. Svelte rejects exporting
 *  $derived directly from a module (runtime scope doesn't exist
 *  outside components) so we export accessors instead. Call inside
 *  a $derived to track updates. */
export function getAllNodes(): NodeTemplate[] {
	return Object.values(registry);
}

export function getAllNodeTypes(): string[] {
	return Object.keys(registry);
}

/** Back-compat aliases. v1 consumers import these directly and
 *  iterate them at render time. Expose as proxies so every read
 *  hits the live registry. */
export const ALL_NODES: NodeTemplate[] = new Proxy([] as NodeTemplate[], {
	get(_target, prop) {
		const arr = Object.values(registry);
		if (prop === 'length') return arr.length;
		if (prop === Symbol.iterator) return arr[Symbol.iterator].bind(arr);
		if (typeof prop === 'string' && /^\d+$/.test(prop)) return arr[Number(prop)];
		// eslint-disable-next-line @typescript-eslint/no-explicit-any
		const v = (arr as any)[prop];
		return typeof v === 'function' ? v.bind(arr) : v;
	},
}) as NodeTemplate[];

export const ALL_NODE_TYPES: string[] = new Proxy([] as string[], {
	get(_target, prop) {
		const arr = Object.keys(registry);
		if (prop === 'length') return arr.length;
		if (prop === Symbol.iterator) return arr[Symbol.iterator].bind(arr);
		if (typeof prop === 'string' && /^\d+$/.test(prop)) return arr[Number(prop)];
		// eslint-disable-next-line @typescript-eslint/no-explicit-any
		const v = (arr as any)[prop];
		return typeof v === 'function' ? v.bind(arr) : v;
	},
}) as string[];

export type NodeType = string;

/** Group and Loop are editor-only node types (not in the Rust
 *  catalog). Registered inline so the command palette can list them
 *  and addNode('Group' | 'Loop') resolves the NODE_TYPE_CONFIG lookup. */
function registerBuiltins(): void {
	if (!registry.Group) {
		registry.Group = {
			type: 'Group',
			label: 'Group',
			description: 'Wrap a subgraph. Interface ports flow in and out; children share a scope.',
			icon: resolveIcon('GitFork'),
			color: '#71717a',
			category: 'Flow' as NodeCategory,
			tags: ['group', 'container', 'scope'],
			requiresInfra: false,
			fields: [],
			defaultInputs: [],
			defaultOutputs: [],
			features: {},
		};
	}
	if (!registry.Loop) {
		registry.Loop = {
			type: 'Loop',
			label: 'Loop',
			description: 'Iterate over lists, fold via carry ports, or drive by self.done. Body sees one element per iteration. Pick port roles via right-click on each port.',
			icon: resolveIcon('RotateCw'),
			color: '#8b5cf6',
			category: 'Flow' as NodeCategory,
			tags: ['loop', 'iterate', 'container', 'scope'],
			requiresInfra: false,
			fields: [
				{
					key: 'parallel',
					label: 'Parallel',
					type: 'checkbox',
					description: 'Run all iterations concurrently (incompatible with carry / self.done).',
				},
				{
					key: 'max_iters',
					label: 'Max iterations',
					type: 'number',
					min: 0,
					description: 'Hard cap on iteration count. Leave blank for no cap.',
				},
				{
					key: 'trim_on_mismatch',
					label: 'Trim on length mismatch',
					type: 'checkbox',
					description: 'Zip iter inputs to the shortest length. Off = crash loud on mismatch.',
				},
			],
			defaultInputs: [],
			defaultOutputs: [],
			features: {},
		};
	}
}
registerBuiltins();

/** Merge catalog entries into the registry. Used by `parseResult`,
 *  which carries only the node types the current `main.weft`
 *  references: it augments the palette with freshly-parsed metadata
 *  but must never define the full set (it doesn't know it). Mutates
 *  the $state registry; readers re-run because the proxy tracks sets. */
export function registerCatalog(entries: Record<string, CatalogEntry>): void {
	for (const [type, entry] of Object.entries(entries)) {
		registry[type] = toTemplate(entry);
	}
	registerBuiltins();
}

/** Replace the registry with the authoritative full catalog. Used by
 *  `catalogAll` (the `weft describe-nodes` response), which is the
 *  complete set of known node types. Replacing (not merging) is what
 *  lets a deleted node disappear from the palette: a merge can never
 *  express removal. Builtins (Group) are re-seeded after the clear. */
export function setCatalog(entries: Record<string, CatalogEntry>): void {
	for (const type of Object.keys(registry)) {
		delete registry[type];
	}
	for (const [type, entry] of Object.entries(entries)) {
		registry[type] = toTemplate(entry);
	}
	registerBuiltins();
}
