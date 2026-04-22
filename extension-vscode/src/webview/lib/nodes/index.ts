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

// Shape the dispatcher sends per node type. Mirrors Rust
// NodeMetadata serialization.
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
	fields?: FieldDefinition[];
	entry?: unknown[];
	requires_infra?: boolean;
	features?: NodeFeatures;
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
		fields: entry.fields ?? [],
		defaultInputs: entry.inputs ?? [],
		defaultOutputs: entry.outputs ?? [],
		features: entry.features,
	};
}

// Shared mutable registry. Consumers import the references below
// once and read through them. registerCatalog mutates the records
// in-place so the references stay stable.
const registry: Record<string, NodeTemplate> = {};

export const NODE_TYPE_CONFIG = registry;

export const ALL_NODES: NodeTemplate[] = [];

export const ALL_NODE_TYPES: string[] = [];

export type NodeType = string;

/** Called by App.svelte on every parseResult. Merges entries into
 *  the shared registry + rebuilds the ALL_NODES / ALL_NODE_TYPES
 *  snapshots in-place so existing imports stay valid. */
export function registerCatalog(entries: Record<string, CatalogEntry>): void {
	for (const [type, entry] of Object.entries(entries)) {
		registry[type] = toTemplate(entry);
	}
	ALL_NODES.length = 0;
	ALL_NODES.push(...Object.values(registry));
	ALL_NODE_TYPES.length = 0;
	ALL_NODE_TYPES.push(...Object.keys(registry));
}
