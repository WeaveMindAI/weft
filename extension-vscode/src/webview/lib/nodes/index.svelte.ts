/**
 * Node Registry — VS Code extension build.
 *
 * v1's standalone dashboard ships one `.ts` file per node template
 * under this folder and auto-discovers them at build time. The VS
 * Code extension instead gets catalog entries from the dispatcher
 * at parse time (one request → metadata for every node type the
 * project references, covering both stdlib and any project-local
 * nodes/ folder). We populate NODE_TYPE_CONFIG at runtime, BEFORE
 * App.svelte mounts ProjectEditor, so downstream `$derived` reads
 * on first render see a populated registry.
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
// properties (type, options, min, max, provider, accept, ...) so
// we transform here.
interface RustFieldType {
	kind: string;
	options?: string[];
	min?: number;
	max?: number;
	provider?: string;
	accept?: string;
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

// Shape the dispatcher sends per node type. Mirrors Rust
// NodeMetadata serialization plus the per-node FormFieldSpec[]
// inlined by the dispatcher's `/describe/nodes` route.
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
	if (ft.accept) flat.accept = ft.accept;
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
		tags: entry.tags ?? [],
		fields: (entry.fields ?? []).map(flattenField),
		defaultInputs: entry.inputs ?? [],
		defaultOutputs: entry.outputs ?? [],
		features: entry.features,
		// The dispatcher's `/describe/nodes` ships the field-type
		// vocabulary inline for nodes whose features.hasFormSchema
		// is true; the form_builder editor reads it via
		// `typeConfig.formFieldSpecs`.
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

/** Group is the only editor-only node type (not in the Rust
 *  catalog). Registered inline so the command palette can list it
 *  and addNode('Group') resolves its NODE_TYPE_CONFIG lookup. */
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
			fields: [],
			defaultInputs: [],
			defaultOutputs: [],
			features: {},
		};
	}
}
registerBuiltins();

/** Called by App.svelte on every parseResult AND once on mount
 *  with the global /describe/nodes response. Mutates the $state
 *  registry; readers re-run because the proxy tracks set ops. */
export function registerCatalog(entries: Record<string, CatalogEntry>): void {
	for (const [type, entry] of Object.entries(entries)) {
		registry[type] = toTemplate(entry);
	}
	registerBuiltins();
}
