<script lang="ts">
	import type { ProjectDefinition, NodeInstance, FieldDefinition, PortDefinition, FileRef } from '$lib/types';
	import { NODE_TYPE_CONFIG } from '$lib/nodes';
	import { createFieldEditor } from '$lib/utils/field-editor.svelte';
	import { handleBlobFieldUpload, validateExternalUrl, formatBytes } from '$lib/utils/blob-upload';
	import FilePicker from './FilePicker.svelte';
	import { toast } from 'svelte-sonner';
	import BlobField from './BlobField.svelte';

	let { project, onUpdateNode, onUpdateNodePorts }: {
		project: ProjectDefinition;
		onUpdateNode: (nodeId: string, config: Record<string, unknown>) => void;
		onUpdateNodePorts?: (nodeId: string, inputs: PortDefinition[], outputs: PortDefinition[]) => void;
	} = $props();

	// Search filter
	let searchQuery = $state('');

	// Local port edits: nodeId -> { inputs: PortDefinition[], outputs: PortDefinition[] }
	let localPorts = $state<Record<string, { inputs: PortDefinition[]; outputs: PortDefinition[] }>>({});

	function getLocalPorts(node: NodeInstance): { inputs: PortDefinition[]; outputs: PortDefinition[] } {
		return localPorts[node.id] ?? { inputs: node.inputs, outputs: node.outputs };
	}

	function initLocalPorts(node: NodeInstance) {
		if (!localPorts[node.id]) {
			localPorts = { ...localPorts, [node.id]: { inputs: [...node.inputs], outputs: [...node.outputs] } };
		}
	}

	function isPortsDivergent(node: NodeInstance): boolean {
		const local = localPorts[node.id];
		if (!local) return false;
		const sameInputs = local.inputs.length === node.inputs.length &&
			local.inputs.every((p, i) => p.name === node.inputs[i]?.name);
		const sameOutputs = local.outputs.length === node.outputs.length &&
			local.outputs.every((p, i) => p.name === node.outputs[i]?.name);
		return !sameInputs || !sameOutputs;
	}

	function hasDuplicatePorts(node: NodeInstance): boolean {
		const local = localPorts[node.id];
		if (!local) return false;
		const inputNames = local.inputs.map(p => p.name);
		const outputNames = local.outputs.map(p => p.name);
		return new Set(inputNames).size !== inputNames.length || new Set(outputNames).size !== outputNames.length;
	}

	function applyPorts(node: NodeInstance) {
		const local = localPorts[node.id];
		if (!local || !onUpdateNodePorts || hasDuplicatePorts(node)) return;
		onUpdateNodePorts(node.id, local.inputs, local.outputs);
		localPorts = { ...localPorts, [node.id]: { inputs: [...local.inputs], outputs: [...local.outputs] } };
	}

	function resetPorts(node: NodeInstance) {
		localPorts = { ...localPorts, [node.id]: { inputs: [...node.inputs], outputs: [...node.outputs] } };
	}

	function updateLocalPortName(nodeId: string, side: 'inputs' | 'outputs', index: number, newName: string) {
		const current = localPorts[nodeId];
		if (!current) return;
		const ports = [...current[side]];
		ports[index] = { ...ports[index], name: newName };
		localPorts = { ...localPorts, [nodeId]: { ...current, [side]: ports } };
	}

	function addLocalPort(nodeId: string, side: 'inputs' | 'outputs') {
		const current = localPorts[nodeId];
		if (!current) return;
		const existing = current[side];
		const base = side === 'inputs' ? 'input' : 'output';
		const newPort: PortDefinition = { name: `${base}_${existing.length + 1}`, portType: 'MustOverride', required: false };
		localPorts = { ...localPorts, [nodeId]: { ...current, [side]: [...existing, newPort] } };
	}

	function removeLocalPort(nodeId: string, side: 'inputs' | 'outputs', index: number) {
		const current = localPorts[nodeId];
		if (!current) return;
		const ports = current[side].filter((_, i) => i !== index);
		localPorts = { ...localPorts, [nodeId]: { ...current, [side]: ports } };
	}

	// Track which nodes are expanded (collapsed by default)
	let expandedNodes = $state<Set<string>>(new Set());

	function toggleNode(node: NodeInstance) {
		const next = new Set(expandedNodes);
		if (next.has(node.id)) {
			next.delete(node.id);
		} else {
			next.add(node.id);
			if (!localPorts[node.id]) {
				initLocalPorts(node);
			}
		}
		expandedNodes = next;
	}

	// Only show nodes that have config fields or editable ports (deduplicated by id)
	let configurableNodes = $derived(
		project.nodes.filter((node, idx, arr) => {
			if (arr.findIndex(n => n.id === node.id) !== idx) return false;
			if (node.nodeType === 'Annotation') return false;
			const template = NODE_TYPE_CONFIG[node.nodeType];
			if (!template) return false;
			if (template.fields.length > 0) return true;
			if (template.features?.canAddInputPorts || template.features?.canAddOutputPorts) return true;
			return false;
		})
	);

	export function getConfigurableCount(): number {
		return configurableNodes.length;
	}

	let filteredNodes = $derived(
		searchQuery.trim() === ''
			? configurableNodes
			: configurableNodes.filter(node =>
					getNodeLabel(node).toLowerCase().includes(searchQuery.toLowerCase())
			  )
	);

	function getTemplate(nodeType: string) {
		return NODE_TYPE_CONFIG[nodeType];
	}

	function getNodeLabel(node: NodeInstance): string {
		return node.label || getTemplate(node.nodeType)?.label || node.nodeType;
	}

	function getNodeIcon(node: NodeInstance) {
		return getTemplate(node.nodeType)?.icon;
	}

	function getFieldValue(node: NodeInstance, field: FieldDefinition): unknown {
		const val = node.config[field.key];
		if (val !== undefined) return val;
		return field.defaultValue ?? '';
	}

	function updateField(node: NodeInstance, field: FieldDefinition, value: unknown) {
		let coerced = value;
		if (field.type === 'number' && typeof value === 'string' && value !== '') {
			const n = Number(value);
			if (!isNaN(n)) coerced = n;
		}
		const newConfig = { ...node.config, [field.key]: coerced };
		onUpdateNode(node.id, newConfig);
	}

	const fieldEditor = createFieldEditor();

	export function flushPendingEdits() {
		fieldEditor.flush();
	}

	function fieldEditKey(nodeId: string, fieldKey: string): string {
		return `${nodeId}:${fieldKey}`;
	}

	function valueToString(val: unknown): string {
		if (val === undefined || val === null) return '';
		if (typeof val === 'string') return val;
		if (typeof val === 'object') return JSON.stringify(val, null, 2);
		return String(val);
	}

	function stringToValue(str: string): unknown {
		const trimmed = str.trim();
		if ((trimmed.startsWith('{') && trimmed.endsWith('}')) || (trimmed.startsWith('[') && trimmed.endsWith(']'))) {
			try { return JSON.parse(trimmed); } catch { /* not valid JSON, keep as string */ }
		}
		return str;
	}

	function getFieldDisplayValue(node: NodeInstance, field: FieldDefinition): string {
		const key = fieldEditKey(node.id, field.key);
		const val = getFieldValue(node, field);
		return fieldEditor.display(key, valueToString(val));
	}

	function fieldSaveFn(node: NodeInstance, field: FieldDefinition): (value: string) => void {
		return (value: string) => updateField(node, field, stringToValue(value));
	}

	function fieldSaveClampedFn(node: NodeInstance, field: FieldDefinition): (value: string) => void {
		return (value: string) => {
			if (field.type === 'number' && value !== '') {
				let num = Number(value);
				if (!isNaN(num)) {
					if (field.min !== undefined && num < field.min) num = field.min;
					if (field.max !== undefined && num > field.max) num = field.max;
					updateField(node, field, num);
					return;
				}
			}
			updateField(node, field, value);
		};
	}

	const SENSITIVE_TYPES = new Set(['password', 'api_key']);
</script>

<div class="flex flex-col h-full overflow-hidden">
	<!-- Search bar -->
	<div class="px-2 py-1.5 border-b border-zinc-200 bg-[#f3f4f6] shrink-0">
		<div class="flex items-center gap-1.5 px-2 py-1 bg-white border border-zinc-200 rounded">
			<svg xmlns="http://www.w3.org/2000/svg" width="11" height="11" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2.5" class="text-zinc-400 shrink-0">
				<circle cx="11" cy="11" r="8"/><path d="m21 21-4.35-4.35"/>
			</svg>
			<input
				type="text"
				bind:value={searchQuery}
				placeholder="Filter nodes..."
				class="flex-1 text-[11px] bg-transparent text-zinc-700 placeholder-zinc-400 focus:outline-none"
			/>
			{#if searchQuery}
				<button
					onclick={() => searchQuery = ''}
					class="text-zinc-400 hover:text-zinc-600 transition-colors"
					aria-label="Clear search"
				>
					<svg xmlns="http://www.w3.org/2000/svg" width="10" height="10" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2.5"><path d="M18 6 6 18M6 6l12 12"/></svg>
				</button>
			{/if}
		</div>
	</div>
	<div class="flex-1 overflow-y-auto">
		{#if configurableNodes.length === 0}
			<div class="flex flex-col items-center justify-center h-full gap-2 px-4 text-center">
				<svg xmlns="http://www.w3.org/2000/svg" width="28" height="28" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.5" class="text-zinc-300">
					<circle cx="12" cy="12" r="3"/><path d="M19.07 4.93a10 10 0 0 1 0 14.14M4.93 4.93a10 10 0 0 0 0 14.14"/>
				</svg>
				<p class="text-xs text-zinc-400">No configurable nodes in this project.</p>
			</div>
		{:else if filteredNodes.length === 0}
			<div class="flex flex-col items-center justify-center h-32 gap-2 px-4 text-center">
				<p class="text-xs text-zinc-400">No nodes match "{searchQuery}".</p>
			</div>
		{:else}
			{#each filteredNodes as node (node.id)}
				{@const template = getTemplate(node.nodeType)}
				{@const isExpanded = expandedNodes.has(node.id)}
				<div class="border-b border-zinc-100 last:border-b-0">
					<!-- Node header (click to expand) -->
					<div
						role="button"
						tabindex="0"
						class="flex items-center gap-2 px-3 py-1.5 cursor-pointer hover:bg-zinc-100 transition-colors group"
						onclick={() => toggleNode(node)}
						onkeydown={(e) => { if (e.key === 'Enter' || e.key === ' ') { e.preventDefault(); toggleNode(node); } }}
					>
						<div class="text-zinc-400 group-hover:text-zinc-600 transition-transform duration-200" style="transform: {isExpanded ? 'rotate(90deg)' : 'rotate(0deg)'}">
							<svg xmlns="http://www.w3.org/2000/svg" width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="m9 18 6-6-6-6"/></svg>
						</div>
						<div class="w-4 h-4 flex items-center justify-center text-zinc-500">
							{#if getNodeIcon(node)}
								{@const NodeIcon = getNodeIcon(node)}
								<NodeIcon size={14} />
							{/if}
						</div>
						<span class="text-xs font-medium text-zinc-700 truncate flex-1">{getNodeLabel(node)}</span>
					</div>

					<!-- Expanded content -->
					{#if isExpanded && template}
						{@const canEditPorts = !!(NODE_TYPE_CONFIG[node.nodeType]?.features?.canAddInputPorts || NODE_TYPE_CONFIG[node.nodeType]?.features?.canAddOutputPorts) && !!onUpdateNodePorts}
						{@const localP = getLocalPorts(node)}
						{@const divergent = canEditPorts && isPortsDivergent(node)}
						{@const hasDups = canEditPorts && hasDuplicatePorts(node)}

						<!-- Config fields -->
						<div class="px-4 pb-3 pt-1 flex flex-col gap-3 border-b border-zinc-100 bg-white">
							{#each template.fields as field}
								<div class="flex flex-col gap-1.5">
									<div class="flex items-center justify-between">
										<label for="{node.id}-{field.key}" class="text-[11px] font-medium text-zinc-600">{field.label}</label>
										{#if field.description}
											<span class="text-[10px] text-zinc-400" title={field.description}>ⓘ</span>
										{/if}
									</div>
									{#if field.type === 'text' || field.type === 'password' || field.type === 'number'}
										<input
											id="{node.id}-{field.key}"
											type={field.type === 'text' ? 'text' : field.type}
											class="w-full px-2 py-1 text-xs bg-white border border-zinc-200 rounded text-zinc-800 placeholder-zinc-400 focus:outline-none focus:ring-1 focus:ring-blue-500 focus:border-blue-500 transition-shadow font-mono"
											placeholder={field.placeholder || ''}
											min={field.min}
											max={field.max}
											value={getFieldDisplayValue(node, field)}
											onfocus={() => fieldEditor.focus(fieldEditKey(node.id, field.key), valueToString(getFieldValue(node, field)))}
											oninput={(e) => fieldEditor.input((e.target as HTMLInputElement).value, fieldEditKey(node.id, field.key), fieldSaveFn(node, field))}
											onblur={(e) => {
												fieldEditor.blur(fieldEditKey(node.id, field.key), fieldSaveClampedFn(node, field));
												const el = e.target as HTMLInputElement;
												if (field.type === 'number' && el.value !== '') {
													let n = Number(el.value);
													if (!isNaN(n)) {
														if (field.min !== undefined && n < field.min) n = field.min;
														if (field.max !== undefined && n > field.max) n = field.max;
														el.value = String(n);
													}
												}
											}}
										/>
									{:else if field.type === 'textarea' || field.type === 'code'}
										<textarea
											id="{node.id}-{field.key}"
											class="w-full px-2 py-1.5 text-xs bg-white border border-zinc-200 rounded text-zinc-800 placeholder-zinc-400 focus:outline-none focus:ring-1 focus:ring-blue-500 focus:border-blue-500 transition-shadow font-mono resize-y min-h-[60px]"
											placeholder={field.placeholder || ''}
											rows="15"
											value={getFieldDisplayValue(node, field)}
											onfocus={() => fieldEditor.focus(fieldEditKey(node.id, field.key), valueToString(getFieldValue(node, field)))}
											oninput={(e) => fieldEditor.input((e.target as HTMLTextAreaElement).value, fieldEditKey(node.id, field.key), fieldSaveFn(node, field))}
											onblur={() => fieldEditor.blur(fieldEditKey(node.id, field.key), fieldSaveFn(node, field))}
										></textarea>
									{:else if field.type === 'select'}
										<select
											id="{node.id}-{field.key}"
											class="w-full px-2 py-1 text-xs bg-white border border-zinc-200 rounded text-zinc-800 focus:outline-none focus:ring-1 focus:ring-blue-500 focus:border-blue-500 transition-shadow"
											value={getFieldValue(node, field) as string}
											onchange={(e) => updateField(node, field, (e.target as HTMLSelectElement).value)}
										>
											{#each field.options || [] as opt}
												<option value={opt}>{opt}</option>
											{/each}
										</select>
									{:else if field.type === 'checkbox'}
										<label class="flex items-center gap-2 cursor-pointer">
											<input
												type="checkbox"
												class="w-3.5 h-3.5 text-blue-500 rounded border-zinc-300 focus:ring-blue-500 cursor-pointer"
												checked={!!getFieldValue(node, field)}
												onchange={(e) => updateField(node, field, (e.target as HTMLInputElement).checked)}
											/>
											<span class="text-xs text-zinc-600 cursor-pointer select-none">Enable</span>
										</label>
									{:else if field.type === 'multiselect'}
										<select
											id="{node.id}-{field.key}"
											multiple
											class="w-full px-2 py-1 text-xs bg-white border border-zinc-200 rounded text-zinc-800 focus:outline-none focus:ring-1 focus:ring-blue-500 focus:border-blue-500 transition-shadow"
											onchange={(e) => {
												const sel = Array.from((e.target as HTMLSelectElement).selectedOptions).map(o => o.value);
												updateField(node, field, sel);
											}}
										>
											{#each field.options || [] as opt}
												<option value={opt} selected={(getFieldValue(node, field) as string[] || []).includes(opt)}>{opt}</option>
											{/each}
										</select>
									{:else if field.type === 'blob'}
										<BlobField
											fileRef={getFieldValue(node, field) as import('$lib/types').FileRef | undefined}
											accept={field.accept}
											id={`cp-${node.id}-${field.key}`}
											placeholder={field.placeholder}
											onUpdate={(ref) => updateField(node, field, ref)}
										/>
									{/if}
									{#if field.description && field.type !== 'checkbox'}
										<p class="text-[10px] text-zinc-400 leading-tight">{field.description}</p>
									{/if}
								</div>
							{/each}
						</div>

						<!-- Port editor (bottom) -->
						{#if canEditPorts}
							<div class="px-4 pt-2 pb-3 bg-white flex flex-col gap-2">
								<span class="text-[10px] font-semibold text-zinc-500 uppercase tracking-wider">Ports</span>
								{#if NODE_TYPE_CONFIG[node.nodeType]?.features?.canAddInputPorts}
									<div class="flex flex-col gap-1">
										<span class="text-[10px] text-zinc-400">Inputs</span>
										{#each localP.inputs as port, i}
											<div class="flex items-center gap-1">
												<input
													type="text"
													value={port.name}
													oninput={(e) => updateLocalPortName(node.id, 'inputs', i, (e.target as HTMLInputElement).value)}
													class="flex-1 px-2 py-1 text-xs bg-white border border-zinc-200 rounded text-zinc-800 focus:outline-none focus:ring-1 focus:ring-blue-500 focus:border-blue-500 font-mono"
												/>
												<button
													onclick={() => removeLocalPort(node.id, 'inputs', i)}
													class="w-5 h-5 flex items-center justify-center rounded text-zinc-400 hover:text-red-500 hover:bg-red-50 transition-colors shrink-0"
													title="Remove port"
												>×</button>
											</div>
										{/each}
										<button
											onclick={() => addLocalPort(node.id, 'inputs')}
											class="mt-0.5 text-[10px] text-zinc-400 hover:text-zinc-600 text-left transition-colors"
										>+ add input</button>
									</div>
								{/if}
								{#if NODE_TYPE_CONFIG[node.nodeType]?.features?.canAddOutputPorts}
									<div class="flex flex-col gap-1">
										<span class="text-[10px] text-zinc-400">Outputs</span>
										{#each localP.outputs as port, i}
											<div class="flex items-center gap-1">
												<input
													type="text"
													value={port.name}
													oninput={(e) => updateLocalPortName(node.id, 'outputs', i, (e.target as HTMLInputElement).value)}
													class="flex-1 px-2 py-1 text-xs bg-white border border-zinc-200 rounded text-zinc-800 focus:outline-none focus:ring-1 focus:ring-blue-500 focus:border-blue-500 font-mono"
												/>
												<button
													onclick={() => removeLocalPort(node.id, 'outputs', i)}
													class="w-5 h-5 flex items-center justify-center rounded text-zinc-400 hover:text-red-500 hover:bg-red-50 transition-colors shrink-0"
													title="Remove port"
												>×</button>
											</div>
										{/each}
										<button
											onclick={() => addLocalPort(node.id, 'outputs')}
											class="mt-0.5 text-[10px] text-zinc-400 hover:text-zinc-600 text-left transition-colors"
										>+ add output</button>
									</div>
								{/if}
								{#if hasDups}
									<div class="flex items-center gap-2 mt-1 px-2 py-1.5 bg-red-50 border border-red-200 rounded text-[10px] text-red-700">
										<svg xmlns="http://www.w3.org/2000/svg" width="11" height="11" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2.5" class="shrink-0"><circle cx="12" cy="12" r="10"/><line x1="12" y1="8" x2="12" y2="12"/><line x1="12" y1="16" x2="12.01" y2="16"/></svg>
										<span class="flex-1 leading-tight">Duplicate port names, fix before applying</span>
										<button onclick={() => resetPorts(node)} class="px-1.5 py-0.5 rounded text-red-600 hover:bg-red-100 transition-colors shrink-0">Reset</button>
									</div>
								{:else if divergent}
									<div class="flex items-center gap-2 mt-1 px-2 py-1.5 bg-amber-50 border border-amber-200 rounded text-[10px] text-amber-700">
										<svg xmlns="http://www.w3.org/2000/svg" width="11" height="11" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2.5" class="shrink-0"><path d="M10.29 3.86 1.82 18a2 2 0 0 0 1.71 3h16.94a2 2 0 0 0 1.71-3L13.71 3.86a2 2 0 0 0-3.42 0z"/><line x1="12" y1="9" x2="12" y2="13"/><line x1="12" y1="17" x2="12.01" y2="17"/></svg>
										<span class="flex-1 leading-tight">Port config differs from project state</span>
										<div class="flex gap-1 shrink-0">
											<button onclick={() => resetPorts(node)} class="px-1.5 py-0.5 rounded text-amber-600 hover:bg-amber-100 transition-colors">Reset</button>
											<button onclick={() => applyPorts(node)} class="px-1.5 py-0.5 rounded bg-amber-600 text-white hover:bg-amber-700 transition-colors font-medium" disabled={hasDups}>Apply</button>
										</div>
									</div>
								{/if}
							</div>
						{/if}
					{/if}
				</div>
			{/each}
		{/if}
	</div>
</div>