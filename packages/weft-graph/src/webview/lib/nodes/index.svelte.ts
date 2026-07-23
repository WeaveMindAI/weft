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

import type { NodeTemplate, NodeCategory, PortDefinition } from '../types';
import type { CatalogEntry, InputSpec, OutputSpec } from '../../../protocol';
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
	CloudDownload,
	Code2,
	Cog,
	Database,
	Download,
	Ear,
	FileText,
	Filter,
	Folder,
	GitBranch,
	GitFork,
	Globe,
	Hand,
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
	MessageCircle,
	MessageSquare,
	MessagesSquare,
	Mic,
	Network,
	Package,
	Pencil,
	Pin,
	Play,
	Plug,
	Puzzle,
	Radio,
	RefreshCw,
	Repeat,
	Rss,
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
	X,
	Zap,
} from '@lucide/svelte';

export type { NodeTemplate, NodeCategory } from '../types';

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
	CloudDownload,
	Code2,
	Cog,
	Database,
	Download,
	Ear,
	FileText,
	Filter,
	Folder,
	GitBranch,
	GitFork,
	Globe,
	Hand,
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
	MessageCircle,
	MessageSquare,
	MessagesSquare,
	Mic,
	Network,
	Package,
	Pencil,
	Pin,
	Play,
	Plug,
	Puzzle,
	Radio,
	RefreshCw,
	Repeat,
	Rss,
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
	X,
	Zap,
};

function resolveIcon(name: string | undefined): Component {
	if (!name) return Square as Component;
	const icon = ICON_MAP[name] as Component | undefined;
	if (!icon) {
		// A name that isn't in ICON_MAP silently degrades to the generic Square,
		// which is how a wrong icon name (e.g. an unimported lucide icon) hides.
		// Surface it loudly in dev so it gets caught, not shipped as a square.
		console.error(`resolveIcon: no icon mapped for '${name}'; add it to ICON_MAP. Falling back to Square.`);
		return Square as Component;
	}
	return icon;
}

// The catalog entry shape is the ONE wire definition in `protocol.ts`
// (`CatalogEntry`), matching Rust's `weft describe-nodes` serialization. This
// module consumes it directly (no local re-declaration) and transforms it into
// the webview's render types (`NodeTemplate` / `PortDefinition`): the wire
// names a port's type `type`, while the components read flat `portType`.
// Inputs arrive RESOLVED (exposure + widget always filled by the CLI), so the
// template carries them verbatim; the editor derives nothing.

/// Wire input (`{ name, type, exposure, widget, ... }`) -> render port.
function flattenInput(i: InputSpec): PortDefinition {
	return {
		name: i.name,
		portType: i.type,
		required: i.required ?? false,
		...(i.exposure !== undefined ? { exposure: i.exposure } : {}),
		...(i.widget !== undefined ? { widget: i.widget } : {}),
		...(i.default !== undefined ? { default: i.default } : {}),
		...(i.label !== undefined ? { label: i.label } : {}),
		...(i.placeholder !== undefined ? { placeholder: i.placeholder } : {}),
		...(i.description !== undefined ? { description: i.description } : {}),
	};
}

/// Wire output (`{ name, type, ... }`) -> render port.
function flattenOutput(p: OutputSpec): PortDefinition {
	return {
		name: p.name,
		portType: p.type,
		required: p.required ?? false,
		...(p.description !== undefined ? { description: p.description } : {}),
	};
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
		defaultInputs: (entry.inputs ?? []).map(flattenInput),
		defaultOutputs: (entry.outputs ?? []).map(flattenOutput),
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
			icon: resolveIcon('Repeat'),
			color: '#8b5cf6',
			category: 'Flow' as NodeCategory,
			tags: ['loop', 'iterate', 'container', 'scope'],
			requiresInfra: false,
			// The loop-config knobs (parallel / max_iters / trim_on_mismatch)
			// are editor-only UI owned by GroupNode.svelte, not template data.
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
