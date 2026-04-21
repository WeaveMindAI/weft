import { ALL_NODES } from '$lib/nodes';

export interface CatalogField {
	key: string;
	type: string;
	options?: string[];
	label?: string;
	placeholder?: string;
}

export interface CatalogPort {
	name: string;
	portType: string;
	required?: boolean;
}

export interface CatalogFormFieldSpec {
	fieldType: string;
	label: string;
	requiredConfig: string[];
	optionalConfig: string[];
	addsInputs: CatalogPort[];
	addsOutputs: CatalogPort[];
}

export interface CatalogNodeFeatures {
	canAddInputPorts?: boolean;
	canAddOutputPorts?: boolean;
	isTrigger?: boolean;
	triggerCategory?: string;
	isInfrastructure?: boolean;
	hasFormSchema?: boolean;
}

export interface CatalogNode {
	type: string;
	category: string;
	description: string;
	fields: CatalogField[];
	inputs: CatalogPort[];
	outputs: CatalogPort[];
	features: CatalogNodeFeatures;
	formFieldSpecs?: CatalogFormFieldSpec[];
	setupGuide?: string[];
}

export interface ShoppableNode {
	type: string;
	description: string;
}

export interface NodeCatalog {
	nodes: CatalogNode[];
}

function toCatalogNode(node: typeof ALL_NODES[number]): CatalogNode {
	return {
		type: node.type,
		category: node.category,
		description: node.description,
		fields: node.fields.map(f => ({
			key: f.key,
			type: f.type,
			...(f.type === 'select' && f.options ? { options: f.options } : {}),
			...(f.label ? { label: f.label } : {}),
			...(f.placeholder ? { placeholder: f.placeholder } : {}),
		})),
		inputs: node.defaultInputs.map(p => ({
			name: p.name,
			portType: p.portType,
			...(p.required ? { required: true } : {}),
			...(p.description ? { description: p.description } : {}),
		})),
		outputs: node.defaultOutputs.map(p => ({
			name: p.name,
			portType: p.portType,
			...(p.description ? { description: p.description } : {}),
		})),
		features: {
			...(node.features?.canAddInputPorts ? { canAddInputPorts: true } : {}),
			...(node.features?.canAddOutputPorts ? { canAddOutputPorts: true } : {}),
			...(node.features?.isTrigger ? { isTrigger: true, triggerCategory: node.features.triggerCategory } : {}),
			...(node.features?.isInfrastructure ? { isInfrastructure: true } : {}),
			...(node.features?.hasFormSchema ? { hasFormSchema: true } : {}),
		},
		...(node.formFieldSpecs ? {
			formFieldSpecs: node.formFieldSpecs.map(s => ({
				fieldType: s.fieldType,
				label: s.label,
				requiredConfig: s.requiredConfig,
				optionalConfig: s.optionalConfig,
				addsInputs: s.addsInputs.map(p => ({ name: p.name, portType: p.portType })),
				addsOutputs: s.addsOutputs.map(p => ({ name: p.name, portType: p.portType })),
			}))
		} : {}),
		...(node.setupGuide && node.setupGuide.length > 0 ? { setupGuide: node.setupGuide } : {}),
	};
}

/** Build the base node catalog (always sent to the builder) */
export function buildNodeCatalog(): NodeCatalog {
	const visibleNodes = ALL_NODES.filter(n => !n.features?.hidden);
	const baseNodes = visibleNodes.filter(n => n.isBase);
	return { nodes: baseNodes.map(toCatalogNode) };
}

/** Build the shoppable node list (compressed: type + description only) */
export function buildShoppableNodes(): ShoppableNode[] {
	const visibleNodes = ALL_NODES.filter(n => !n.features?.hidden);
	return visibleNodes.filter(n => !n.isBase).map(n => ({
		type: n.type,
		description: n.description,
	}));
}

/** Resolve full CatalogNode objects from a list of node type names */
export function resolveShoppedNodes(nodeTypes: string[]): CatalogNode[] {
	const typeSet = new Set(nodeTypes);
	const visibleNodes = ALL_NODES.filter(n => !n.features?.hidden);
	return visibleNodes.filter(n => typeSet.has(n.type)).map(toCatalogNode);
}

/** Format a CatalogNode into the same text format the backend uses for the builder prompt */
function formatNodeEntry(node: CatalogNode): string {
	const lines: string[] = [`**${node.type}**: ${node.description}`];

	if (node.fields.length > 0) {
		const parts = node.fields.map(f => {
			if (f.type === 'select' && f.options) return `${f.key} (${f.options.map(o => `"${o}"`).join(' | ')})`;
			if (f.type === 'checkbox') return `${f.key} (boolean)`;
			if (f.type === 'password') return `${f.key} (leave empty, user fills manually)`;
			if (f.type === 'code' && f.label) return `${f.key} (${f.label})`;
			if (f.placeholder) return `${f.key} (e.g. "${f.placeholder}")`;
			return f.key;
		});
		lines.push(`  config: ${parts.join(', ')}`);
	}

	if (node.inputs.length > 0) {
		const parts = node.inputs.map(p => {
			const opt = p.required ? '' : '?';
			const desc = (p as any).description ? ` (${(p as any).description})` : '';
			return `${p.name}: ${p.portType}${opt}${desc}`;
		});
		lines.push(`  in(${parts.join(', ')})`);
	}

	if (node.outputs.length > 0) {
		const parts = node.outputs.map(p => {
			const desc = (p as any).description ? ` (${(p as any).description})` : '';
			return `${p.name}:${p.portType}${desc}`;
		});
		lines.push(`  out: ${parts.join(', ')}`);
	}

	const fl: string[] = [];
	if (node.features.isTrigger) fl.push(`trigger (${node.features.triggerCategory || 'unknown'})`);
	if (node.features.canAddInputPorts) fl.push('customInputPorts');
	if (node.features.canAddOutputPorts) fl.push('customOutputPorts');
	if (node.features.isInfrastructure) fl.push('infrastructure');
	if (node.features.hasFormSchema) fl.push('hasFormSchema');
	if (fl.length > 0) lines.push(`  features: ${fl.join(', ')}`);

	if (node.formFieldSpecs?.length) {
		const parts = node.formFieldSpecs.map(s => {
			const info: string[] = [];
			if (s.addsInputs.length) info.push(`in=${s.addsInputs.map(p => `${p.name}:${p.portType}`).join('+')}`);
			if (s.addsOutputs.length) info.push(`out=${s.addsOutputs.map(p => `${p.name}:${p.portType}`).join('+')}`);
			return info.length ? `${s.fieldType} (${info.join(', ')})` : s.fieldType;
		});
		lines.push(`  formFields: ${parts.join('; ')}`);
	}

	if (node.setupGuide?.length) {
		lines.push(`  guide: ${node.setupGuide.join('. ')}`);
	}

	return lines.join('\n');
}

/** Format a list of CatalogNodes into text grouped by category */
export function formatNodeCatalog(nodes: CatalogNode[]): string {
	const categoryOrder = ['Data', 'AI', 'Triggers', 'Flow', 'Infrastructure', 'Utility', 'Debug'];
	const categories = new Map<string, CatalogNode[]>();
	for (const node of nodes) {
		const cat = node.category || 'Other';
		if (!categories.has(cat)) categories.set(cat, []);
		categories.get(cat)!.push(node);
	}

	const sections: string[] = [];
	for (const cat of categoryOrder) {
		const catNodes = categories.get(cat);
		if (!catNodes) continue;
		sections.push(`### ${cat}`);
		for (const node of catNodes) sections.push(formatNodeEntry(node));
	}
	// Any categories not in the order
	for (const [cat, catNodes] of categories) {
		if (categoryOrder.includes(cat)) continue;
		sections.push(`### ${cat}`);
		for (const node of catNodes) sections.push(formatNodeEntry(node));
	}
	return sections.join('\n');
}
