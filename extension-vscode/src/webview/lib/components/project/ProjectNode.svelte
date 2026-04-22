<script lang="ts">
	import { Handle, Position, useEdges, NodeResizer, type ResizeParams } from "@xyflow/svelte";
	import { NODE_TYPE_CONFIG, type NodeType } from "$lib/nodes";
	import type { PortDefinition, PortType, NodeDataUpdates, LaneMode } from "$lib/types";
	import { parseWeftType } from "$lib/types";
	import { PORT_TYPE_COLORS, getPortTypeColor } from "$lib/constants/colors";
	import type { Edge } from "@xyflow/svelte";
	import CodeEditor from "$lib/components/CodeEditor.svelte";
	import { toast } from "svelte-sonner";
	import CopyButton from "$lib/components/ui/CopyButton.svelte";
	import { buildSpecMap, deriveInputsFromFields, deriveOutputsFromFields, type FormFieldDef, type FormFieldSpec } from '$lib/utils/form-field-specs';
	import { getStatusIcon } from "$lib/utils/status";
	import { BadgeQuestionMark, Maximize2, Minimize2 } from '@lucide/svelte';
	import { createFieldEditor } from '$lib/utils/field-editor.svelte';
	import { handleBlobFieldUpload, validateExternalUrl, formatBytes } from '$lib/utils/blob-upload';
	import FilePicker from './FilePicker.svelte';
	import type { FileRef } from '$lib/types';
	import BlobField from "./BlobField.svelte";
	import { createPortContextMenu, buildPortMenuItems } from "$lib/utils/port-context-menu";
	import { portMarkerStyle } from "$lib/utils/port-marker";
	import ExecutionInspector from './ExecutionInspector.svelte';
	
	const edgesState = useEdges();

	let { data, id, selected }: {
		data: {
			label: string | null;
			nodeType: NodeType;
			config: Record<string, unknown>;
			inputs?: PortDefinition[];
			outputs?: PortDefinition[];
			features?: import('$lib/types').NodeFeatures;
			onUpdate?: (updates: NodeDataUpdates) => void;
			infraNodeStatus?: string;
			debugData?: unknown;
			executions?: import('$lib/types').NodeExecution[];
			executionCount?: number;
			liveDataItems?: import('$lib/types').LiveDataItem[];
		};
		id: string;
		selected?: boolean;
	} = $props();

	const typeConfig = $derived(NODE_TYPE_CONFIG[data.nodeType as NodeType] ?? {
		type: data.nodeType,
		label: data.nodeType,
		description: 'Unknown node type',
		icon: BadgeQuestionMark,
		color: '#999',
		category: 'Logic' as const,
		tags: [],
		fields: [],
		defaultInputs: [],
		defaultOutputs: [],
	});

	const executions = $derived(data.executions ?? []);
	const latestExecution = $derived(executions[executions.length - 1]);
	const displayedStatus = $derived(latestExecution?.status ?? '');

	const nodeFormFieldSpecs: FormFieldSpec[] = $derived(typeConfig.formFieldSpecs ?? []);
	const nodeFormSpecMap: Record<string, FormFieldSpec> = $derived(buildSpecMap(nodeFormFieldSpecs));

	/** Ports that have an incoming edge. Used to hide synthesized config fields
	 *  for wired ports (the edge is the source of truth, config is redundant). */
	const wiredInputPorts: Set<string> = $derived.by(() => {
		const wired = new Set<string>();
		for (const e of edgesState.current) {
			if (e.target === id && e.targetHandle) wired.add(e.targetHandle);
		}
		return wired;
	});

	/** Input ports satisfied by a non-null config value and no edge. These
	 *  render with the 'empty-dotted' port marker to signal "filled from
	 *  code" without changing the declared port type. */
	const configFilledPorts: Set<string> = $derived.by(() => {
		const filled = new Set<string>();
		const cfg = (data.config as Record<string, unknown>) || {};
		const inputList = (data.inputs || typeConfig.defaultInputs || []) as Array<{ name: string; configurable?: boolean }>;
		for (const port of inputList) {
			if (port.configurable === false) continue;
			if (wiredInputPorts.has(port.name)) continue;
			const v = cfg[port.name];
			if (v !== undefined && v !== null && v !== '') filled.add(port.name);
		}
		return filled;
	});

	/** Fields rendered in the expanded view: catalog fields + synthesized
	 *  fields for configurable input ports whose config has a value and no
	 *  edge. Synthesized fields appear when the user/AI wrote `port: value`
	 *  in the weft source (or `node.port = value` on a connection line).
	 *  Removed when the source removes the value or adds an edge. */
	const displayedFields: import('$lib/types').FieldDefinition[] = $derived.by(() => {
		const catalogFields = typeConfig.fields ?? [];
		const result: import('$lib/types').FieldDefinition[] = [...catalogFields];
		const catalogFieldKeys = new Set(catalogFields.map(f => f.key));
		const cfg = (data.config as Record<string, unknown>) || {};
		const inputList = (data.inputs || typeConfig.defaultInputs || []);
		for (const port of inputList) {
			if (catalogFieldKeys.has(port.name)) continue; // catalog already defines a field
			if (port.configurable === false) continue;     // wired-only port
			if (wiredInputPorts.has(port.name)) continue;   // edge wins, don't show config
			const value = cfg[port.name];
			if (value === undefined || value === null) continue;
			// Multi-line string → textarea, otherwise single-line text input.
			const isMultiline = typeof value === 'string' && value.includes('\n');
			result.push({
				key: port.name,
				label: port.name,
				type: isMultiline ? 'textarea' : 'text',
			});
		}
		return result;
	});


	// Recursively remove _raw keys from objects
	function stripRawKeys(value: unknown): unknown {
		if (value === null || value === undefined) return value;
		if (Array.isArray(value)) {
			return value.map(stripRawKeys);
		}
		if (typeof value === 'object') {
			const obj = value as Record<string, unknown>;
			const result: Record<string, unknown> = {};
			for (const [key, val] of Object.entries(obj)) {
				if (key !== '_raw') {
					result[key] = stripRawKeys(val);
				}
			}
			return result;
		}
		return value;
	}

	// Get clean debug data as JSON string (exclude _raw recursively)
	const debugDataJson = $derived.by(() => {
		if (data.debugData === undefined || data.debugData === null) return null;
		const cleaned = stripRawKeys(data.debugData);
		return JSON.stringify(cleaned, null, 2);
	});

	// Check if node has expandable content (fields, run location option, debug preview, etc.)
	const hasExpandableContent = $derived.by(() => {
		// Has config fields (catalog-declared or synthesized from config-filled ports)
		if (displayedFields.length > 0) return true;
		// Has Run Location selector
		if (typeConfig.features?.showRunLocationSelector) return true;
		// Has debug preview (Debug node)
		if (typeConfig.features?.showDebugPreview) return true;
		// Has setup guide
		if (typeConfig.setupGuide && typeConfig.setupGuide.length > 0) return true;
		return false;
	});

	// Get expanded state from config (persisted), default collapsed for regular nodes
	const expanded = $derived((data.config?.expanded as boolean) ?? false);
	
	// Handle resize end - save dimensions to config and ensure expanded is true
	function handleResizeEnd(_event: unknown, params: ResizeParams) {
		if (data.onUpdate) {
			data.onUpdate({
				config: { ...data.config, width: params.width, height: params.height, expanded: true }
			});
		}
	}
	let showSetupGuide = $state(false);
	let editingLabel = $state(false);
	// svelte-ignore state_referenced_locally
	let labelInput = $state(data.label || '');
	let addingInputPort = $state(false);
	let addingOutputPort = $state(false);
	let newInputName = $state('');
	let newOutputName = $state('');
	let portContextMenu = $state<{ portName: string; side: 'input' | 'output'; x: number; y: number } | null>(null);
	let nodeElement: HTMLDivElement;

	function setPortType(portName: string, side: 'input' | 'output', newType: string) {
		if (side === 'input') {
			const newInputs = inputs.map((p: PortDefinition) =>
				p.name === portName ? { ...p, portType: newType } : { ...p }
			);
			data.onUpdate?.({ inputs: newInputs });
		} else {
			const newOutputs = baseOutputs.map((p: PortDefinition) =>
				p.name === portName ? { ...p, portType: newType } : { ...p }
			);
			data.onUpdate?.({ outputs: newOutputs });
		}
	}

	function togglePortRequired(portName: string, side: 'input' | 'output') {
		if (side === 'input') {
			const newInputs = inputs.map((p: PortDefinition) =>
				p.name === portName ? { ...p, required: !p.required } : { ...p }
			);
			data.onUpdate?.({ inputs: newInputs });
		} else {
			const newOutputs = baseOutputs.map((p: PortDefinition) =>
				p.name === portName ? { ...p, required: !p.required } : { ...p }
			);
			data.onUpdate?.({ outputs: newOutputs });
		}
	}
	
	// Port context menu rendered on document.body to avoid CSS transform positioning issues
	$effect(() => {
		if (!portContextMenu) return;
		const { portName, side, x, y } = portContextMenu;
		const port = side === 'input'
			? inputs.find((p) => p.name === portName)
			: baseOutputs.find((p) => p.name === portName);
		if (!port) return;

		const defaultPorts = side === 'input' ? typeConfig.defaultInputs : typeConfig.defaultOutputs;
		const isCustom = !defaultPorts.some((p) => p.name === portName);
		const canAddPorts = (side === 'input'
			? typeConfig.features?.canAddInputPorts
			: typeConfig.features?.canAddOutputPorts) ?? false;

		const items = buildPortMenuItems({
			port,
			side,
			isCustom,
			canAddPorts,
			onToggleRequired: () => togglePortRequired(portName, side),
			onSetType: (newType) => setPortType(portName, side, newType),
			onRemove: () => { if (side === 'input') removeInputPort(portName); else removeOutputPort(portName); },
		});

		return createPortContextMenu(x, y, items, () => { portContextMenu = null; });
	});


	// Blur any focused element inside the node when deselected
	// This prevents middle-click paste on Linux when panning
	$effect(() => {
		if (!selected && nodeElement) {
			const activeElement = document.activeElement;
			if (activeElement && nodeElement.contains(activeElement)) {
				(activeElement as HTMLElement).blur?.();
			}
		}
	});
	
	// Get textarea heights from config (persisted)
	const textareaHeights = $derived((data.config?.textareaHeights as Record<string, number>) || {});
	
	// Save textarea height to config when resized
	function handleTextareaResize(fieldKey: string, height: number) {
		if (data.onUpdate) {
			const currentHeights = (data.config?.textareaHeights as Record<string, number>) || {};
			if (currentHeights[fieldKey] !== height) {
				data.onUpdate({
					config: { 
						...data.config, 
						textareaHeights: { ...currentHeights, [fieldKey]: height } 
					}
				});
			}
		}
	}
	
	// Action to observe textarea resize
	function observeTextareaResize(node: HTMLTextAreaElement, fieldKey: string) {
		let lastHeight = node.clientHeight;
		const observer = new ResizeObserver(() => {
			const newHeight = node.clientHeight;
			if (newHeight !== lastHeight && newHeight >= 60) {
				lastHeight = newHeight;
				handleTextareaResize(fieldKey, newHeight);
			}
		});
		observer.observe(node);
		return {
			destroy() {
				observer.disconnect();
			}
		};
	};
	
	
	function getPortColor(portType: PortType): string {
		return getPortTypeColor(portType);
	}

	const inputs = $derived(data.inputs || typeConfig.defaultInputs);
	const baseOutputs = $derived(data.outputs || typeConfig.defaultOutputs);
	// _raw port is rendered separately as a square in the top-right corner
	const outputs = $derived(baseOutputs);

	// Dynamic min resize height: header + ports + fixed buffer for at least one config line
	// Accent bar (2) + header row (32) + content padding (16) + label (24) + ports gap (8) + port rows + buffer (100)
	const PORT_ROW_HEIGHT = 25;
	const minResizeHeight = $derived(
		2 + 32 + 16 + 24 + 8 + Math.max(inputs.length, outputs.length) * PORT_ROW_HEIGHT + 80
	);
	
	// Check if node allows adding ports based on its features
	const canAddInputPorts = $derived(typeConfig.features?.canAddInputPorts ?? false);
	const canAddOutputPorts = $derived(typeConfig.features?.canAddOutputPorts ?? false);
	const oneOfRequiredGroups: string[][] = $derived(
		[...(typeConfig.features?.oneOfRequired ?? []), ...(data.features?.oneOfRequired ?? [])]
	);
	const oneOfRequiredPorts: Set<string> = $derived(
		new Set(oneOfRequiredGroups.flat())
	);
	const canAddPorts = $derived(canAddInputPorts || canAddOutputPorts);
	// Check if _raw output is connected (any edge from this node's _raw handle)
	const rawConnected = $derived(
		edgesState.current.some((e: Edge) => e.source === id && e.sourceHandle === '_raw')
	);
	

	function startEditLabel(e: MouseEvent) {
		e.stopPropagation();
		labelInput = data.label || '';
		editingLabel = true;
	}

	function saveLabel() {
		editingLabel = false;
		if (data.onUpdate) {
			data.onUpdate({ label: labelInput || null });
		}
	}

	function handleLabelKeydown(e: KeyboardEvent) {
		if (e.key === 'Enter') {
			saveLabel();
		} else if (e.key === 'Escape') {
			editingLabel = false;
			labelInput = data.label || '';
		}
	}

	function updateConfig(key: string, value: string | string[] | number | FormFieldDef[] | FileRef | null) {
		if (data.onUpdate) {
			const newConfig = { ...data.config, [key]: value };
			if (typeConfig.features?.hasFormSchema && key === 'fields') {
				const fields = value as FormFieldDef[];
				data.onUpdate({
					config: newConfig,
					inputs: deriveInputsFromFields(fields, nodeFormSpecMap),
					outputs: deriveOutputsFromFields(fields, nodeFormSpecMap),
				});
			} else {
				data.onUpdate({ config: newConfig });
			}
		}
	}

	const fieldEditor = createFieldEditor();

	function getConfigDisplayValue(fieldKey: string): string {
		const v = (data.config as Record<string, unknown>)?.[fieldKey];
		const storeStr = (v === undefined || v === null) ? '' : (typeof v === 'string' ? v : JSON.stringify(v, null, 2));
		return fieldEditor.display(fieldKey, storeStr);
	}

	function configSaveFn(fieldKey: string): (value: string) => void {
		return (value: string) => updateConfig(fieldKey, value);
	}


	let addingFormField = $state(false);
	let newFormField = $state<FormFieldDef>({ fieldType: 'display', key: '', config: {} });
	let newOptionText = $state('');
	/** Set when the user clicks Add with an empty key so the key input
	 *  renders in an error state (red border + message) instead of
	 *  silently no-op'ing. Cleared on any keystroke in the key input. */
	let newFormFieldKeyError = $state(false);

	function getFormFields(): FormFieldDef[] {
		return ((data.config as Record<string, unknown>)?.fields as FormFieldDef[]) ?? [];
	}

	function updateFormFields(fields: FormFieldDef[]) {
		updateConfig('fields', fields);
	}

	function removeFormField(index: number) {
		const fields = getFormFields().filter((_, i) => i !== index);
		updateFormFields(fields);
	}

	function addFormField() {
		const f = newFormField;
		if (!f.key?.trim()) {
			newFormFieldKeyError = true;
			return;
		}
		const spec = nodeFormSpecMap[f.fieldType];
		const field: FormFieldDef = {
			fieldType: f.fieldType,
			key: f.key.trim().replace(/\s+/g, '_'),
			render: spec?.render,
			config: f.config ?? {},
		};

		// Compute port names the new field would generate
		const newInputNames = deriveInputsFromFields([field], nodeFormSpecMap).map(p => p.name);
		const newOutputNames = deriveOutputsFromFields([field], nodeFormSpecMap).map(p => p.name);
		const newPortNames = new Set([...newInputNames, ...newOutputNames]);

		// Compute all existing port names from current fields
		const existingFields = getFormFields();
		const existingInputNames = deriveInputsFromFields(existingFields, nodeFormSpecMap).map(p => p.name);
		const existingOutputNames = deriveOutputsFromFields(existingFields, nodeFormSpecMap).map(p => p.name);
		const existingPortNames = new Set([...existingInputNames, ...existingOutputNames]);

		const collisions = [...newPortNames].filter(n => existingPortNames.has(n));
		if (collisions.length > 0) {
			toast.error(`Port name conflict: "${collisions.join('", "')}" already exists. Choose a different key.`);
			return;
		}

		updateFormFields([...existingFields, field]);
		newFormField = { fieldType: 'display', key: '', config: {} };
		newOptionText = '';
		newFormFieldKeyError = false;
		addingFormField = false;
	}

	function addOption() {
		const opt = newOptionText.trim();
		if (!opt) return;
		const options = [...((newFormField.config?.options as string[]) ?? []), opt];
		newFormField = { ...newFormField, config: { ...newFormField.config, options } };
		newOptionText = '';
	}

	function removeOption(i: number) {
		const options = ((newFormField.config?.options as string[]) ?? []).filter((_, idx) => idx !== i);
		newFormField = { ...newFormField, config: { ...newFormField.config, options } };
	}

	function updateConfigBool(key: string, value: boolean) {
		if (data.onUpdate) {
			const newConfig = { ...data.config, [key]: value };
			data.onUpdate({ config: newConfig });
		}
	}

	function addInputPort() {
		const name = newInputName.trim();
		if (!name) return;
		// Check for duplicate name
		if (inputs.some((p: PortDefinition) => p.name === name)) {
			toast.error(`Input port "${name}" already exists`);
			return;
		}
		const newPort: PortDefinition = {
			name,
			portType: 'MustOverride',
			required: false,
		};
		const newInputs = [...inputs, newPort];
		if (data.onUpdate) {
			data.onUpdate({ inputs: newInputs });
		}
		newInputName = '';
		addingInputPort = false;
	}

	function addOutputPort() {
		const name = newOutputName.trim();
		if (!name) return;
		// Check for duplicate name (_raw is reserved for the raw output dock)
		if (name === '_raw') {
			toast.error(`"_raw" is a reserved port name`);
			return;
		}
		if (baseOutputs.some((p: PortDefinition) => p.name === name)) {
			toast.error(`Output port "${name}" already exists`);
			return;
		}
		const newPort: PortDefinition = {
			name,
			portType: 'MustOverride',
			required: false,
		};
		// Use baseOutputs (not outputs which includes _raw)
		const newOutputs = [...baseOutputs, newPort];
		if (data.onUpdate) {
			data.onUpdate({ outputs: newOutputs });
		}
		newOutputName = '';
		addingOutputPort = false;
	}

	function removeInputPort(portName: string) {
		const newInputs = inputs.filter((p: PortDefinition) => p.name !== portName);
		if (data.onUpdate) {
			data.onUpdate({ inputs: newInputs });
		}
	}

	function removeOutputPort(portName: string) {
		const newOutputs = baseOutputs.filter((p: PortDefinition) => p.name !== portName);
		if (data.onUpdate) {
			data.onUpdate({ outputs: newOutputs });
		}
	}

	function handlePortKeydown(e: KeyboardEvent, type: 'input' | 'output') {
		if (e.key === 'Enter') {
			if (type === 'input') addInputPort();
			else addOutputPort();
		} else if (e.key === 'Escape') {
			if (type === 'input') {
				addingInputPort = false;
				newInputName = '';
			} else {
				addingOutputPort = false;
				newOutputName = '';
			}
		}
	}

	function toggleExpand(e: MouseEvent) {
		if (!hasExpandableContent) return;
		
		const currentExpanded = (data.config?.expanded as boolean) ?? false;
		console.debug(`[toggleExpand] node=${id} currentExpanded=${currentExpanded} hasOnUpdate=${!!data.onUpdate} configKeys=${Object.keys(data.config || {}).join(',')}`);
		
		if (data.onUpdate) {
			if (currentExpanded) {
				// Collapsing - save current dimensions before collapsing (if node has been resized)
				// These will be restored when expanding again
				const currentWidth = nodeElement?.offsetWidth;
				const currentHeight = nodeElement?.offsetHeight;
				const existingWidth = (data.config?.width as number) || undefined;
				const existingHeight = (data.config?.height as number) || undefined;
				
				// Only save if we have actual dimensions and they're different from min size
				if (currentWidth && currentHeight && currentWidth > 200) {
					data.onUpdate({ 
						config: { 
							...data.config, 
							expanded: false,
							width: existingWidth || currentWidth,
							height: existingHeight || currentHeight,
						} 
					});
				} else {
					data.onUpdate({ config: { ...data.config, expanded: false } });
				}
			} else {
				// Expanding - just set expanded to true, dimensions will be applied by buildNodes
				data.onUpdate({ config: { ...data.config, expanded: true } });
			}
		}
	}

</script>

<!-- Node Resizer - only visible when selected AND expanded -->
{#if expanded}
<NodeResizer 
	minWidth={200} 
	minHeight={minResizeHeight}
	isVisible={selected}
	lineClass="node-resize-line"
	lineStyle="border-color: {typeConfig.color}; border-width: 1px; opacity: 0.5;"
	handleClass="node-resize-handle"
	handleStyle="background-color: {typeConfig.color}; width: 10px; height: 10px; border-radius: 2px;"
	onResizeEnd={handleResizeEnd}
/>
{/if}

<!-- svelte-ignore a11y_click_events_have_key_events -->
<!-- svelte-ignore a11y_no_static_element_interactions -->
<div
	bind:this={nodeElement}
	class="project-node rounded min-w-[200px] select-none transition-all duration-200 {displayedStatus === 'running' || displayedStatus === 'waiting_for_input' ? 'node-running-glow' : ''} {displayedStatus === 'failed' ? 'node-failed-glow' : displayedStatus === 'completed' ? 'node-completed-glow' : ''} {selected ? 'node-selected' : ''}"
	style="
		width: 100%;
		height: 100%;
		display: flex;
		flex-direction: column;
		overflow: hidden;
		background: rgba(255, 255, 255, 0.95);
		border: 1px solid {selected ? typeConfig.color : 'rgba(0, 0, 0, 0.08)'};
		box-shadow: 0 1px 3px rgba(0, 0, 0, 0.08), 0 4px 12px rgba(0, 0, 0, 0.05){selected ? `, 0 0 0 1px ${typeConfig.color}20` : ''};
		backdrop-filter: blur(8px);
	"
>
	<!-- Accent bar at top -->
	<div 
		class="h-0.5 rounded-t"
		style="background: {typeConfig.color};"
	></div>
	
	<!-- Header with type label and expand toggle -->
	<div
		class="px-3 py-2 flex items-center justify-between border-b border-black/5"
	>
		<div class="flex items-center gap-1.5">
			<span class="text-xs {displayedStatus === 'running' || displayedStatus === 'waiting_for_input' ? 'animate-pulse' : ''}" style="color: {typeConfig.color};">{getStatusIcon(displayedStatus)}</span>
			<span class="text-[11px] font-semibold tracking-wide uppercase" style="color: {typeConfig.color};">{typeConfig.label}</span>
			{#if data.infraNodeStatus}
				<span class="inline-flex items-center gap-1 px-1.5 py-0.5 rounded-full text-[9px] font-medium leading-none
					{data.infraNodeStatus === 'running' ? 'bg-green-100 text-green-700' : ''}
					{data.infraNodeStatus === 'starting' ? 'bg-blue-100 text-blue-700' : ''}
					{data.infraNodeStatus === 'stopped' ? 'bg-amber-100 text-amber-700' : ''}
					{data.infraNodeStatus === 'failed' ? 'bg-red-100 text-red-700' : ''}
					{data.infraNodeStatus === 'terminated' ? 'bg-zinc-100 text-zinc-500' : ''}
				">
					<span class="w-1.5 h-1.5 rounded-full
						{data.infraNodeStatus === 'running' ? 'bg-green-500' : ''}
						{data.infraNodeStatus === 'starting' ? 'bg-blue-500 animate-pulse' : ''}
						{data.infraNodeStatus === 'stopped' ? 'bg-amber-500' : ''}
						{data.infraNodeStatus === 'failed' ? 'bg-red-500' : ''}
						{data.infraNodeStatus === 'terminated' ? 'bg-zinc-400' : ''}
					"></span>
					{data.infraNodeStatus}
				</span>
			{/if}
		</div>
		<div class="flex items-center gap-0.5">
			<ExecutionInspector {executions} label={data.label || typeConfig.label} />
		{#if hasExpandableContent}
			<button
				class="w-5 h-5 flex items-center justify-center rounded hover:bg-black/5 cursor-pointer transition-colors text-zinc-400"
				onclick={toggleExpand}
				title={expanded ? 'Collapse' : 'Expand'}
			>
				{#if expanded}
					<Minimize2 size={12} />
				{:else}
					<Maximize2 size={12} />
				{/if}
			</button>
		{/if}
		</div>
	</div>

	<div class="px-3 py-2 flex-1 overflow-hidden min-h-0 nodrag nopan flex flex-col">
		<!-- Editable Label -->
		{#if editingLabel}
			<input
				type="text"
				class="w-full text-sm font-medium bg-zinc-100 text-zinc-900 px-2 py-1 rounded border border-zinc-200 outline-none focus:border-zinc-400"
				bind:value={labelInput}
				onblur={saveLabel}
				onkeydown={handleLabelKeydown}
				onclick={(e) => e.stopPropagation()}
			/>
		{:else}
			<p 
				class="text-sm font-medium text-zinc-800 cursor-text hover:bg-black/5 px-1 py-0.5 rounded -mx-1 truncate"
				ondblclick={startEditLabel}
				title="Double-click to edit"
			>
				{data.label || `${typeConfig.label} Node`}
			</p>
		{/if}
		
		<!-- Ports Section -->
		<div class="mt-2 flex justify-between text-[10px] text-zinc-500 w-full">
			<!-- Input Ports -->
			<div class="space-y-1 min-w-0 flex-1">
				{#each inputs as input}
					{@const pMarker = portMarkerStyle(input, oneOfRequiredPorts, configFilledPorts, getPortColor(input.portType), 'input')}
					<!-- svelte-ignore a11y_no_static_element_interactions -->
					<div
						class="relative flex items-center gap-1 group pl-3"
						title={!input.required && oneOfRequiredPorts.has(input.name) ? `At least one required: ${oneOfRequiredGroups.filter(g => g.includes(input.name)).map(g => g.join(' or ')).join('; ')}` : input.name}
						oncontextmenu={(e) => {
							e.preventDefault();
							e.stopPropagation();
							portContextMenu = { portName: input.name, side: 'input', x: e.clientX, y: e.clientY };
						}}
					>
						<Handle
							type="target"
							position={Position.Left}
							id={input.name}
							style="top: 50%; {pMarker.style}"
							class={pMarker.class}
							oncontextmenu={(e: MouseEvent) => { e.preventDefault(); e.stopPropagation(); portContextMenu = { portName: input.name, side: 'input', x: e.clientX, y: e.clientY }; }}
						/>
						<span class="truncate">{input.name}</span>
						{#if canAddInputPorts}
							<button 
								class="opacity-0 group-hover:opacity-100 text-destructive hover:text-destructive/80 ml-auto text-xs leading-none"
								onclick={(e) => { e.stopPropagation(); removeInputPort(input.name); }}
								title="Remove port"
							>×</button>
						{/if}
					</div>
				{/each}
				{#if canAddInputPorts}
					{#if addingInputPort}
						<div class="flex items-center gap-1">
							<input
								type="text"
								class="w-full text-[10px] bg-muted px-1 py-0.5 rounded border-none outline-none"
								placeholder="port name"
								bind:value={newInputName}
								onkeydown={(e) => handlePortKeydown(e, 'input')}
								onblur={() => { addingInputPort = false; newInputName = ''; }}
								onclick={(e) => e.stopPropagation()}
							/>
						</div>
					{:else}
						<button 
							class="flex items-center gap-0.5 text-muted-foreground/60 hover:text-muted-foreground transition-colors"
							onclick={(e) => { e.stopPropagation(); addingInputPort = true; }}
						>
							<span class="text-xs">+</span>
							<span>input</span>
						</button>
					{/if}
				{/if}
			</div>
			
			<!-- Output Ports -->
			<div class="space-y-1 text-right flex flex-col items-end min-w-0 flex-1">
				{#each outputs as output}
				{@const oMarker = portMarkerStyle(output, oneOfRequiredPorts, configFilledPorts, getPortColor(output.portType), 'output')}
				<!-- svelte-ignore a11y_no_static_element_interactions -->
				<div
					class="relative flex items-center gap-1 justify-end group pr-3"
					oncontextmenu={(e) => {
						e.preventDefault();
						e.stopPropagation();
						portContextMenu = { portName: output.name, side: 'output', x: e.clientX, y: e.clientY };
					}}
				>
					<Handle
						type="source"
						position={Position.Right}
						id={output.name}
						style="top: 50%; {oMarker.style}"
						class={oMarker.class}
						oncontextmenu={(e: MouseEvent) => { e.preventDefault(); e.stopPropagation(); portContextMenu = { portName: output.name, side: 'output', x: e.clientX, y: e.clientY }; }}
					/>
					{#if canAddOutputPorts}
						<button 
							class="opacity-0 group-hover:opacity-100 text-destructive hover:text-destructive/80 mr-auto text-xs leading-none"
							onclick={(e) => { e.stopPropagation(); removeOutputPort(output.name); }}
							title="Remove port"
						>×</button>
					{/if}
					<span class="truncate" title={output.name}>{output.name}</span>
				</div>
			{/each}
				{#if canAddOutputPorts}
					{#if addingOutputPort}
						<div class="flex items-center gap-1 justify-end">
							<input
								type="text"
								class="w-full text-[10px] bg-muted px-1 py-0.5 rounded border-none outline-none text-right"
								placeholder="port name"
								bind:value={newOutputName}
								onkeydown={(e) => handlePortKeydown(e, 'output')}
								onblur={() => { addingOutputPort = false; newOutputName = ''; }}
								onclick={(e) => e.stopPropagation()}
							/>
						</div>
					{:else}
						<button 
							class="flex items-center gap-0.5 text-muted-foreground/60 hover:text-muted-foreground transition-colors justify-end"
							onclick={(e) => { e.stopPropagation(); addingOutputPort = true; }}
						>
							<span>output</span>
							<span class="text-xs">+</span>
						</button>
					{/if}
				{/if}
			</div>
		</div>

		<!-- Live Data Items - always visible regardless of expanded state -->
		{#if data.liveDataItems && data.liveDataItems.length > 0}
			<div class="mt-2 pt-2 border-t live-data-container space-y-2">
				{#each data.liveDataItems as item}
					{#if item.type === 'image' && typeof item.data === 'string'}
						<div class="live-data-item">
							<span class="text-[10px] text-muted-foreground font-medium">{item.label}</span>
							<img src={item.data} alt={item.label} class="w-full rounded border border-zinc-200 mt-1" />
						</div>
					{:else if item.type === 'text'}
						<div class="live-data-item">
							<span class="text-[10px] text-muted-foreground font-medium block mb-1">{item.label}</span>
							<div class="relative">
								<div class="w-full text-[10px] font-mono bg-zinc-100 rounded px-2 py-1.5 pr-7 break-all border border-zinc-200 select-text cursor-text">{item.data}</div>
								<CopyButton text={String(item.data)} class="absolute top-1 right-1" />
							</div>
						</div>
					{:else if item.type === 'progress' && typeof item.data === 'number'}
						<div class="live-data-item">
							<span class="text-[10px] text-muted-foreground font-medium">{item.label}</span>
							<div class="w-full h-1.5 bg-zinc-200 rounded-full mt-1 overflow-hidden">
								<div class="h-full bg-emerald-500 rounded-full transition-all" style="width: {Math.round(item.data * 100)}%"></div>
							</div>
						</div>
					{/if}
				{/each}
			</div>
		{/if}

		<!-- Expanded Config Fields -->
		{#if expanded}
			<div class="mt-3 pt-3 border-t space-y-2 overflow-auto min-h-0 flex-1">
				<!-- Setup Guide -->
				{#if typeConfig.setupGuide && typeConfig.setupGuide.length > 0}
					<button
						class="w-full flex items-center gap-1.5 text-[10px] text-blue-500 hover:text-blue-600 font-medium transition-colors"
						onclick={(e) => { e.stopPropagation(); showSetupGuide = !showSetupGuide; }}
					>
						<span class="text-xs">{showSetupGuide ? '▾' : '▸'}</span>
						<span>Setup Guide</span>
					</button>
					{#if showSetupGuide}
						<div class="text-[10px] text-zinc-500 bg-blue-50 rounded px-2.5 py-2 space-y-1 leading-relaxed">
							{#each typeConfig.setupGuide as step}
								<p>{step}</p>
							{/each}
						</div>
					{/if}
				{/if}

				<!-- Run Location Override - any node can use this by setting features.showRunLocationSelector = true -->
				{#if typeConfig.features?.showRunLocationSelector}
					<div class="space-y-1">
						<label for={`${id}-run-location`} class="text-[10px] text-muted-foreground font-medium block">Run Location</label>
						<select
							id={`${id}-run-location`}
							class="w-full text-xs bg-muted px-2 py-1.5 rounded border-none outline-none"
							value={(data.config as Record<string, string>)?.runLocation || 'default'}
							onchange={(e) => updateConfig('runLocation', e.currentTarget.value)}
							onclick={(e) => e.stopPropagation()}
						>
							<option value="default">Use Default</option>
							<option value="cloud">Cloud</option>
							<option value="local">Local</option>
						</select>
					</div>
				{/if}
				
				{#each displayedFields as field}
					<div class="space-y-1">
						<div class="flex items-center justify-between">
							<label for={`${id}-field-${field.key}`} class="text-[10px] text-muted-foreground font-medium">{field.label}</label>
						</div>
						{#if field.type === "code"}
							<!-- Code editor field - any node can use this by setting field.type = 'code' -->
							<div class="nodrag nopan" onclick={(e) => e.stopPropagation()}
							onfocusin={(e) => e.currentTarget.classList.add('nowheel')}
							onfocusout={(e) => e.currentTarget.classList.remove('nowheel')}
						>
								<CodeEditor
									value={(data.config as Record<string, string>)?.[field.key] || ""}
									placeholder={field.placeholder}
									minHeight="120px"
									onchange={(newValue) => {
										updateConfig(field.key, newValue);
									}}
								/>
							</div>
						{:else if field.type === "textarea"}
							<textarea
								id={`${id}-field-${field.key}`}
								data-field-key={field.key}
								class="text-xs bg-muted px-2 py-1.5 rounded border-none outline-none font-mono nodrag nopan box-border block w-full"
							onfocusin={(e) => e.currentTarget.classList.add('nowheel')}
							onfocusout={(e) => e.currentTarget.classList.remove('nowheel')}
								style="resize: vertical; min-height: 60px; {textareaHeights[field.key] ? `height: ${textareaHeights[field.key]}px;` : ''}"
								placeholder={field.placeholder}
								value={getConfigDisplayValue(field.key)}
								onfocus={() => fieldEditor.focus(field.key, getConfigDisplayValue(field.key))}
								oninput={(e) => fieldEditor.input(e.currentTarget.value, field.key, configSaveFn(field.key))}
								onblur={() => fieldEditor.blur(field.key, configSaveFn(field.key))}
								onclick={(e) => e.stopPropagation()}
								use:observeTextareaResize={field.key}
							></textarea>
						{:else if field.type === "select" && field.options}
							<select
								id={`${id}-field-${field.key}`}
								class="w-full text-xs bg-muted px-2 py-1.5 rounded border-none outline-none"
								value={(data.config as Record<string, string>)?.[field.key] || field.options[0]}
								onchange={(e) => updateConfig(field.key, e.currentTarget.value)}
								onclick={(e) => e.stopPropagation()}
							>
								{#each field.options as option}
									<option value={option}>{option}</option>
								{/each}
							</select>
						{:else if field.type === "multiselect" && field.options}
							<div class="multiselect-container flex flex-wrap gap-1 p-1.5 bg-muted rounded">
								{#each field.options as option}
									{@const selectedValues = ((data.config as Record<string, string[]>)?.[field.key] || []) as string[]}
									{@const isSelected = selectedValues.includes(option)}
									<button
										type="button"
										class="text-[10px] px-1.5 py-0.5 rounded transition-colors whitespace-nowrap {isSelected ? 'bg-primary text-primary-foreground' : 'bg-background text-muted-foreground hover:bg-accent'}"
										onclick={(e) => {
											e.stopPropagation();
											const current = ((data.config as Record<string, string[]>)?.[field.key] || []) as string[];
											const newValues = isSelected
												? current.filter((v: string) => v !== option)
												: [...current, option];
											updateConfig(field.key, newValues);
										}}
									>
										{option}
									</button>
								{/each}
							</div>
						{:else if field.type === "checkbox"}
							<label class="flex items-center gap-2 cursor-pointer">
								<input
									type="checkbox"
									class="w-4 h-4 rounded border-muted-foreground/30"
									checked={(data.config as Record<string, unknown>)?.[field.key] === true}
									onchange={(e) => updateConfigBool(field.key, e.currentTarget.checked)}
									onclick={(e) => e.stopPropagation()}
								/>
								<span class="text-xs text-muted-foreground">{field.description || field.label}</span>
							</label>
						{:else if field.type === "api_key"}
							{@const currentValue = (data.config as Record<string, string>)?.[field.key] || ""}
							{@const isByok = currentValue !== "" && currentValue !== "__PLATFORM__"}
							<div class="space-y-1.5">
								<div class="flex justify-center">
									<div class="inline-flex rounded-md border border-border overflow-hidden">
										<button
											type="button"
											class="text-[10px] px-3 py-1 font-medium transition-colors {!isByok ? 'bg-emerald-500 text-white' : 'bg-background text-muted-foreground hover:text-foreground'}"
											onclick={(e) => { e.stopPropagation(); updateConfig(field.key, ''); }}
										>Credits</button>
										<button
											type="button"
											class="text-[10px] px-3 py-1 font-medium transition-colors border-l border-border {isByok ? 'bg-blue-500 text-white' : 'bg-background text-muted-foreground hover:text-foreground'}"
											onclick={(e) => { e.stopPropagation(); if (!isByok) updateConfig(field.key, '__BYOK__'); }}
										>Own key</button>
									</div>
								</div>
								{#if isByok}
									<input
										type="password"
										class="w-full text-xs bg-muted px-2 py-1.5 rounded border-none outline-none font-mono"
										placeholder="sk-or-v1-..."
										value={fieldEditor.display(field.key, currentValue === '__BYOK__' ? '' : currentValue)}
										onfocus={() => fieldEditor.focus(field.key, currentValue === '__BYOK__' ? '' : currentValue)}
										oninput={(e) => fieldEditor.input(e.currentTarget.value, field.key, (v) => updateConfig(field.key, v || '__BYOK__'))}
										onblur={() => fieldEditor.blur(field.key, (v) => updateConfig(field.key, v || '__BYOK__'))}
										onclick={(e) => e.stopPropagation()}
									/>
								{/if}
							</div>
						{:else if field.type === "password"}
							<input
								id={`${id}-field-${field.key}`}
								type="password"
								class="w-full text-xs bg-muted px-2 py-1.5 rounded border-none outline-none font-mono"
								placeholder={field.placeholder}
								value={getConfigDisplayValue(field.key)}
								onfocus={() => fieldEditor.focus(field.key, getConfigDisplayValue(field.key))}
								oninput={(e) => fieldEditor.input(e.currentTarget.value, field.key, configSaveFn(field.key))}
								onblur={() => fieldEditor.blur(field.key, configSaveFn(field.key))}
								onclick={(e) => e.stopPropagation()}
							/>
						{:else if field.type === "form_builder"}
							<div class="nodrag nopan space-y-1.5" onclick={(e) => e.stopPropagation()}>
								{#each getFormFields() as f, i}
									<div class="flex items-center gap-1.5 bg-zinc-50 border border-zinc-200 rounded px-2 py-1 text-[10px]">
										<span class="text-zinc-400 font-mono w-20 shrink-0 truncate">{f.fieldType}</span>
										<span class="flex-1 text-zinc-700 font-mono truncate">{f.key}</span>
										<button
											class="ml-1 text-zinc-400 hover:text-red-500 transition-colors leading-none"
											onclick={(e) => { e.stopPropagation(); removeFormField(i); }}
											title="Remove field"
										>×</button>
									</div>
								{/each}
								{#if addingFormField}
									<div class="border border-zinc-200 rounded p-2 space-y-1.5 bg-white">
										<select
											class="w-full text-[10px] bg-zinc-50 px-1.5 py-1 rounded border border-zinc-200 outline-none"
											bind:value={newFormField.fieldType}
										>
											{#each nodeFormFieldSpecs as spec}
												<option value={spec.fieldType}>{spec.label}</option>
											{/each}
										</select>
										<input
											type="text"
											class="w-full text-[10px] bg-zinc-50 px-1.5 py-1 rounded border outline-none font-mono {newFormFieldKeyError ? 'border-red-400' : 'border-zinc-200'}"
											placeholder="key (shown to reviewer + port name)"
											bind:value={newFormField.key}
											oninput={() => { newFormFieldKeyError = false; }}
										/>
										{#if newFormFieldKeyError}
											<p class="text-[10px] text-red-500 -mt-0.5">Key is required</p>
										{/if}
										{#if nodeFormSpecMap[newFormField.fieldType ?? 'display']?.requiredConfig.includes('options')}
											<div class="space-y-1">
												{#each ((newFormField.config?.options as string[]) ?? []) as opt, i}
													<div class="flex items-center gap-1">
														<span class="flex-1 text-[10px] text-zinc-600 truncate">{opt}</span>
														<button class="text-zinc-400 hover:text-red-500 text-xs" onclick={(e) => { e.stopPropagation(); removeOption(i); }}>×</button>
													</div>
												{/each}
												<div class="flex gap-1">
													<input
														type="text"
														class="flex-1 text-[10px] bg-zinc-50 px-1.5 py-1 rounded border border-zinc-200 outline-none"
														placeholder="Add option..."
														bind:value={newOptionText}
														onkeydown={(e) => { if (e.key === 'Enter') { e.preventDefault(); addOption(); } }}
													/>
													<button class="text-[10px] px-2 py-1 bg-zinc-100 hover:bg-zinc-200 rounded" onclick={(e) => { e.stopPropagation(); addOption(); }}>+</button>
												</div>
											</div>
										{/if}
										<div class="flex gap-1 pt-0.5">
											<button
												class="flex-1 text-[10px] py-1 bg-zinc-100 hover:bg-zinc-200 rounded transition-colors"
												onclick={(e) => { e.stopPropagation(); addingFormField = false; newFormField = { fieldType: 'display', key: '', config: {} }; newOptionText = ''; newFormFieldKeyError = false; }}
											>Cancel</button>
											<button
												class="flex-1 text-[10px] py-1 bg-zinc-800 hover:bg-zinc-700 text-white rounded transition-colors"
												onclick={(e) => { e.stopPropagation(); addFormField(); }}
											>Add</button>
										</div>
									</div>
								{:else}
									<button
										class="w-full text-[10px] py-1 border border-dashed border-zinc-300 hover:border-zinc-400 text-zinc-400 hover:text-zinc-600 rounded transition-colors"
										onclick={(e) => { e.stopPropagation(); addingFormField = true; newFormFieldKeyError = false; }}
									>+ Add field</button>
								{/if}
							</div>
						{:else if field.type === "blob"}
							<BlobField
								fileRef={(data.config as Record<string, unknown>)?.[field.key] as import('$lib/types').FileRef | undefined}
								accept={field.accept}
								id={`${id}-${field.key}`}
								placeholder={field.placeholder}
								onUpdate={(ref) => updateConfig(field.key, ref)}
							/>
						{:else}
							<input
								id={`${id}-field-${field.key}`}
								type="text"
								class="w-full text-xs bg-muted px-2 py-1.5 rounded border-none outline-none"
								placeholder={field.placeholder}
								value={getConfigDisplayValue(field.key)}
								onfocus={() => fieldEditor.focus(field.key, getConfigDisplayValue(field.key))}
								oninput={(e) => fieldEditor.input(e.currentTarget.value, field.key, configSaveFn(field.key))}
								onblur={() => fieldEditor.blur(field.key, configSaveFn(field.key))}
								onclick={(e) => e.stopPropagation()}
							/>
						{/if}
					</div>
				{/each}

			<!-- Debug Data Preview (expanded) - any node can use this by setting features.showDebugPreview = true -->
			{#if typeConfig.features?.showDebugPreview}
				{#if debugDataJson}
					<div class="relative">
						<CopyButton text={debugDataJson} class="absolute top-1 right-1 z-10 nodrag" />
						<pre class="debug-data-container nodrag nopan nowheel select-text cursor-text">{debugDataJson}</pre>
					</div>
				{:else if displayedStatus === 'completed'}
					<div class="debug-placeholder completed">
						<span>✓</span>
						<span>Execution complete</span>
					</div>
				{:else if displayedStatus === 'failed'}
					<div class="debug-placeholder completed" style="color: var(--color-red-500);">
						<span>✗</span>
						<span>Execution failed{latestExecution?.error ? `: ${latestExecution.error}` : ''}</span>
					</div>
				{:else if displayedStatus === 'running' || displayedStatus === 'waiting_for_input'}
					<div class="debug-placeholder running">
						<span class="debug-spinner"></span>
						<span>Processing...</span>
					</div>
				{:else}
					<div class="debug-placeholder waiting">
						<span>📥</span>
						<span>Waiting for data...</span>
					</div>
				{/if}
			{/if}

			</div>
		{/if}
	</div>
</div>

<!-- Raw output dock: Square handle in top-right corner for full output access -->
<Handle
	type="source"
	position={Position.Right}
	id="_raw"
	style="top: 18px; background: none; border: none; width: 10px; height: 10px;"
>
	<svg 
		width="10" 
		height="10" 
		viewBox="0 0 10 10" 
		style="pointer-events: none; position: absolute; left: 0; top: 0;"
	>
		<rect 
			x="1" 
			y="1" 
			width="8" 
			height="8" 
			fill={rawConnected ? '#18181b' : 'white'}
			stroke="#18181b"
			stroke-width="1.5"
		/>
	</svg>
</Handle>

<!-- Port context menu is rendered via $effect on document.body to avoid CSS transform issues -->

<style>
	:global(.blob-drag-over) {
		outline: 2px solid rgb(96, 165, 250);
		outline-offset: -2px;
		border-radius: 0.375rem;
		background-color: rgba(96, 165, 250, 0.08);
	}
	:global(.node-running-glow) {
		box-shadow: 0 1px 3px rgba(0, 0, 0, 0.08), 0 4px 12px rgba(0, 0, 0, 0.05), 0 0 0 2px rgba(245, 158, 11, 0.4) !important;
	}
	:global(.node-completed-glow) {
		box-shadow: 0 1px 3px rgba(0, 0, 0, 0.08), 0 4px 12px rgba(0, 0, 0, 0.05), 0 0 0 2px rgba(16, 185, 129, 0.3) !important;
	}
	:global(.node-failed-glow) {
		box-shadow: 0 1px 3px rgba(0, 0, 0, 0.08), 0 4px 12px rgba(0, 0, 0, 0.05), 0 0 0 2px rgba(239, 68, 68, 0.4) !important;
	}
	
	/* Multiselect should not expand the node - use a reasonable default width */
	.multiselect-container {
		width: 100%;
		max-width: 100%;
		box-sizing: border-box;
	}

	/* Debug node data display - single resizable box */
	.debug-data-container {
		margin: 0;
		background: #f8fafc;
		border: 1px solid #e2e8f0;
		border-radius: 6px;
		padding: 8px;
		min-height: 60px;
		max-height: 400px;
		overflow: auto;
		font-family: ui-monospace, 'SF Mono', Monaco, monospace;
		font-size: 10px;
		line-height: 1.4;
		white-space: pre-wrap;
		word-break: break-word;
		resize: vertical;
		color: #334155;
	}

	.debug-placeholder {
		display: flex;
		flex-direction: column;
		align-items: center;
		justify-content: center;
		gap: 4px;
		padding: 16px 8px;
		background: #f8fafc;
		border: 1px dashed #e2e8f0;
		border-radius: 6px;
		color: #94a3b8;
		font-size: 11px;
		text-align: center;
	}

	.debug-placeholder.completed {
		background: #f0fdf4;
		border-color: #bbf7d0;
		color: #22c55e;
	}

	.debug-placeholder.running {
		background: #fffbeb;
		border-color: #fde68a;
		color: #f59e0b;
	}

	.debug-spinner {
		width: 14px;
		height: 14px;
		border: 2px solid #fde68a;
		border-top-color: #f59e0b;
		border-radius: 50%;
		animation: debug-spin 0.8s linear infinite;
	}

	@keyframes debug-spin {
		to { transform: rotate(360deg); }
	}

	/* Widen resize line hit area: make the element itself thicker (transparent)
	   while keeping the visible border thin. The element IS the drag target. */
	:global(.node-resize-line.svelte-flow__resize-control.line.left),
	:global(.node-resize-line.svelte-flow__resize-control.line.right) {
		width: 12px !important;
		background: transparent;
	}
	:global(.node-resize-line.svelte-flow__resize-control.line.top),
	:global(.node-resize-line.svelte-flow__resize-control.line.bottom) {
		height: 12px !important;
		background: transparent;
	}

</style>
