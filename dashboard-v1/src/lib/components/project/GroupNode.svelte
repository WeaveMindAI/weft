<script lang="ts">
	import { Handle, Position, NodeResizer, type ResizeParams } from "@xyflow/svelte";
	import { Group, Maximize2, Minimize2 } from '@lucide/svelte';
	import type { NodeDataUpdates, PortDefinition, NodeExecution } from "$lib/types";
	import { getPortTypeColor } from "$lib/constants/colors";
	import { createPortContextMenu, buildPortMenuItems } from '$lib/utils/port-context-menu';
	import { portMarkerStyle } from '$lib/utils/port-marker';
	import ExecutionInspector from './ExecutionInspector.svelte';

	// Group interface ports cannot be config-filled (see the rule enforced in
	// enrichment's validate_required_ports). Pass an empty set to portMarkerStyle
	// so they never render as 'empty-dotted'.
	const noConfigFilled = new Set<string>();

	let { data, selected }: {
		data: {
			label: string | null;
			nodeType: string;
			config: Record<string, unknown>;
			inputs?: PortDefinition[];
			outputs?: PortDefinition[];
			features?: { oneOfRequired?: string[][] };
			onUpdate?: (updates: NodeDataUpdates) => void;
			executions?: NodeExecution[];
			executionCount?: number;
		};
		selected?: boolean
	} = $props();

	const inputs = $derived((data.inputs ?? []) as PortDefinition[]);
	const outputs = $derived((data.outputs ?? []) as PortDefinition[]);

	// One-of-required groups on the group's interface ports: parsed from
	// `@require_one_of(a, b)` directives in the group signature. Same shape
	// as regular node features.oneOfRequired.
	const oneOfRequiredGroups: string[][] = $derived(data.features?.oneOfRequired ?? []);
	const oneOfRequiredPorts: Set<string> = $derived(new Set(oneOfRequiredGroups.flat()));
	const isExpanded = $derived((data.config?.expanded as boolean) ?? true);
	const groupDescription = $derived((data.config?.description as string) ?? '');
	let descExpanded = $state(false);

	const executions = $derived(data.executions ?? []);

	const minExpandedHeight = $derived(computeMinHeight(inputs.length, outputs.length));

	// Auto-enforce minimum height when ports change or on load
	let lastEnforcedMinH = 0;
	$effect(() => {
		if (!isExpanded || !data.onUpdate) return;
		const currentH = (data.config?.height as number) || 0;
		const minH = minExpandedHeight;
		if (currentH < minH && minH !== lastEnforcedMinH) {
			lastEnforcedMinH = minH;
			data.onUpdate({ config: { ...data.config, height: minH } });
		}
	});

	function toggleExpand() {
		if (data.onUpdate) {
			data.onUpdate({
				config: { ...data.config, expanded: !isExpanded }
			});
		}
	}

	function handleResizeEnd(_event: unknown, params: ResizeParams) {
		if (data.onUpdate) {
			data.onUpdate({
				config: { ...data.config, width: params.width, height: params.height, expanded: true }
			});
		}
	}

	// Label editing
	let editingLabel = $state(false);
	let labelInput = $state('');

	function sanitizeLabel(val: string): string {
		// Groups use identifier-style names only: letters, digits, underscores
		// Spaces become underscores, everything else is stripped
		return val.replace(/\s+/g, '_').replace(/[^a-zA-Z0-9_]/g, '');
	}

	function startEditLabel(e: MouseEvent) {
		e.stopPropagation();
		labelInput = data.label || '';
		editingLabel = true;
	}

	function saveLabel() {
		editingLabel = false;
		let cleaned = sanitizeLabel(labelInput);
		// Must start with letter or underscore
		cleaned = cleaned.replace(/^[0-9]+/, '');
		if (cleaned && cleaned !== data.label && data.onUpdate) {
			data.onUpdate({ label: cleaned });
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

	// Port management
	let addingInputPort = $state(false);
	let addingOutputPort = $state(false);
	let newInputName = $state('');
	let newOutputName = $state('');
	let portContextMenu = $state<{ portName: string; side: 'input' | 'output'; x: number; y: number } | null>(null);

	function togglePortRequired(portName: string, side: 'input' | 'output') {
		if (side === 'input') {
			const newInputs = inputs.map((p: PortDefinition) =>
				p.name === portName ? { ...p, required: !p.required } : { ...p }
			);
			data.onUpdate?.({ inputs: newInputs });
		} else {
			const newOutputs = outputs.map((p: PortDefinition) =>
				p.name === portName ? { ...p, required: !p.required } : { ...p }
			);
			data.onUpdate?.({ outputs: newOutputs });
		}
	}

	function setPortType(portName: string, side: 'input' | 'output', newType: string) {
		if (side === 'input') {
			const newInputs = inputs.map((p: PortDefinition) =>
				p.name === portName ? { ...p, portType: newType } : { ...p }
			);
			data.onUpdate?.({ inputs: newInputs });
		} else {
			const newOutputs = outputs.map((p: PortDefinition) =>
				p.name === portName ? { ...p, portType: newType } : { ...p }
			);
			data.onUpdate?.({ outputs: newOutputs });
		}
	}

	// Port context menu rendered on document.body to avoid CSS transform positioning issues.
	// Group interface ports are always user-added and always support custom
	// add/remove, so isCustom=true and canAddPorts=true for every port.
	$effect(() => {
		if (!portContextMenu) return;
		const { portName, side, x, y } = portContextMenu;
		const port = side === 'input'
			? inputs.find((p: PortDefinition) => p.name === portName)
			: outputs.find((p: PortDefinition) => p.name === portName);
		if (!port) return;

		const items = buildPortMenuItems({
			port,
			side,
			isCustom: true,
			canAddPorts: true,
			onToggleRequired: () => togglePortRequired(portName, side),
			onSetType: (newType) => setPortType(portName, side, newType),
			onRemove: () => removePort(side, portName),
		});

		return createPortContextMenu(x, y, items, () => { portContextMenu = null; });
	});

	function computeMinHeight(numInputs: number, numOutputs: number): number {
		return 36 + 8 + Math.max(numInputs, numOutputs) * 30 + 24 + 128;
	}

	function addPort(side: 'input' | 'output', name: string) {
		if (!name.trim() || !data.onUpdate) return;
		const trimmed = name.trim();
		const currentInputs = [...inputs];
		const currentOutputs = [...outputs];
		if (side === 'input') {
			if (currentInputs.some(p => p.name === trimmed)) return;
			currentInputs.push({ name: trimmed, portType: 'MustOverride', required: false });
		} else {
			if (currentOutputs.some(p => p.name === trimmed)) return;
			currentOutputs.push({ name: trimmed, portType: 'MustOverride', required: false });
		}
		// Single update: ports + height enforcement
		const minH = computeMinHeight(currentInputs.length, currentOutputs.length);
		const currentH = (data.config?.height as number) || 300;
		const updates: NodeDataUpdates = { inputs: currentInputs, outputs: currentOutputs };
		if (currentH < minH) {
			updates.config = { ...data.config, height: minH };
		}
		data.onUpdate(updates);
	}

	function removePort(side: 'input' | 'output', name: string) {
		if (!data.onUpdate) return;
		const currentInputs = side === 'input' ? inputs.filter(p => p.name !== name) : [...inputs];
		const currentOutputs = side === 'output' ? outputs.filter(p => p.name !== name) : [...outputs];
		data.onUpdate({ inputs: currentInputs, outputs: currentOutputs });
	}

	function handlePortKeydown(e: KeyboardEvent, side: 'input' | 'output') {
		if (e.key === 'Enter') {
			const name = side === 'input' ? newInputName : newOutputName;
			addPort(side, name);
			if (side === 'input') { addingInputPort = false; newInputName = ''; }
			else { addingOutputPort = false; newOutputName = ''; }
		} else if (e.key === 'Escape') {
			if (side === 'input') { addingInputPort = false; newInputName = ''; }
			else { addingOutputPort = false; newOutputName = ''; }
		}
	}
</script>

{#if isExpanded}
<!-- ═══════════════ EXPANDED: container envelope ═══════════════ -->
<NodeResizer
	minWidth={250}
	minHeight={Math.max(200, minExpandedHeight)}
	isVisible={selected}
	lineStyle="border-color: hsl(var(--primary)); border-width: 2px;"
	handleStyle="background-color: hsl(var(--primary)); width: 10px; height: 10px; border-radius: 2px;"
	onResizeEnd={handleResizeEnd}
/>

<div class="expanded-container" class:selected>
	<div class="expanded-header">
		<span class="header-icon"><Group size={13} /></span>
		{#if editingLabel}
			<input
				type="text"
				class="label-input"
				bind:value={labelInput}
				onblur={saveLabel}
				onkeydown={handleLabelKeydown}
				onclick={(e) => e.stopPropagation()}
			/>
		{:else}
			<!-- svelte-ignore a11y_no_static_element_interactions -->
			<span class="header-label" ondblclick={startEditLabel} title="Double-click to rename">{data.label || 'Group'}</span>
		{/if}
		<div class="flex items-center gap-0.5" style="margin-left: auto;">
			<ExecutionInspector {executions} label={data.label || 'Group'} />
			<button class="expand-toggle" onclick={toggleExpand} title="Collapse group">
				<Minimize2 size={12} />
			</button>
		</div>
	</div>

	<!-- Left boundary ports -->
	<div class="expanded-side-ports expanded-side-left nodrag nopan">
		{#each inputs as input}
			{@const pMarker = portMarkerStyle(input, oneOfRequiredPorts, noConfigFilled, getPortTypeColor(input.portType), 'input', '!relative !inset-auto !transform-none')}
			<!-- svelte-ignore a11y_no_static_element_interactions -->
			<div class="expanded-port-block group" oncontextmenu={(e) => { e.preventDefault(); e.stopPropagation(); portContextMenu = { portName: input.name, side: 'input', x: e.clientX, y: e.clientY }; }}>
				<div class="expanded-port-label-row">
					<span class="expanded-port-label">{input.name}</span>
					<button
						class="opacity-0 group-hover:opacity-100 text-destructive hover:text-destructive/80 text-xs leading-none"
						onclick={(e) => { e.stopPropagation(); removePort('input', input.name); }}
						title="Remove port"
					>×</button>
				</div>
				<div class="expanded-port-dots">
					<!-- External handle (target), outside connections -->
					<Handle
						type="target"
						position={Position.Left}
						id={input.name}
						title={!input.required && oneOfRequiredPorts.has(input.name) ? `At least one required: ${oneOfRequiredGroups.filter(g => g.includes(input.name)).map(g => g.join(' or ')).join('; ')}` : input.name}
						style={pMarker.style}
						class={pMarker.class}
						oncontextmenu={(e: MouseEvent) => { e.preventDefault(); e.stopPropagation(); portContextMenu = { portName: input.name, side: 'input', x: e.clientX, y: e.clientY }; }}
					/>
					<!-- Internal handle (source), child connections -->
					<Handle
						type="source"
						position={Position.Right}
						id="{input.name}__inner"
						style="background-color: {getPortTypeColor(input.portType)};"
						class="!w-2.5 !h-2.5 !border !border-white !rounded-full !relative !inset-auto !transform-none"
						oncontextmenu={(e: MouseEvent) => { e.preventDefault(); e.stopPropagation(); portContextMenu = { portName: input.name, side: 'input', x: e.clientX, y: e.clientY }; }}
					/>
				</div>
			</div>
		{/each}
		{#if addingInputPort}
			<div class="expanded-port-block">
				<input
					type="text"
					class="port-name-input-inline"
					placeholder="port name"
					bind:value={newInputName}
					onkeydown={(e) => handlePortKeydown(e, 'input')}
					onblur={() => { addingInputPort = false; newInputName = ''; }}
					onclick={(e) => e.stopPropagation()}
				/>
			</div>
		{:else}
			<button 
				class="expanded-add-port-btn"
				onclick={(e) => { e.stopPropagation(); addingInputPort = true; }}
			>
				<span class="text-xs">+</span>
				<span>input</span>
			</button>
		{/if}
	</div>

	<!-- Right boundary ports -->
	<div class="expanded-side-ports expanded-side-right nodrag nopan">
		{#each outputs as output}
			{@const oMarker = portMarkerStyle(output, oneOfRequiredPorts, noConfigFilled, getPortTypeColor(output.portType), 'output', '!relative !inset-auto !transform-none')}
			<!-- svelte-ignore a11y_no_static_element_interactions -->
			<div class="expanded-port-block expanded-port-block-right group" oncontextmenu={(e) => { e.preventDefault(); e.stopPropagation(); portContextMenu = { portName: output.name, side: 'output', x: e.clientX, y: e.clientY }; }}>
				<div class="expanded-port-label-row expanded-port-label-row-right">
					<button
						class="opacity-0 group-hover:opacity-100 text-destructive hover:text-destructive/80 text-xs leading-none"
						onclick={(e) => { e.stopPropagation(); removePort('output', output.name); }}
						title="Remove port"
					>×</button>
					<span class="expanded-port-label">{output.name}</span>
				</div>
				<div class="expanded-port-dots expanded-port-dots-right">
					<!-- Internal handle (target), child connections -->
					<Handle
						type="target"
						position={Position.Left}
						id="{output.name}__inner"
						style="background-color: {getPortTypeColor(output.portType)};"
						class="!w-2.5 !h-2.5 !border !border-white !rounded-full !relative !inset-auto !transform-none"
						oncontextmenu={(e: MouseEvent) => { e.preventDefault(); e.stopPropagation(); portContextMenu = { portName: output.name, side: 'output', x: e.clientX, y: e.clientY }; }}
					/>
					<!-- External handle (source), outside connections -->
					<Handle
						type="source"
						position={Position.Right}
						id={output.name}
						style={oMarker.style}
						class={oMarker.class}
						oncontextmenu={(e: MouseEvent) => { e.preventDefault(); e.stopPropagation(); portContextMenu = { portName: output.name, side: 'output', x: e.clientX, y: e.clientY }; }}
					/>
				</div>
			</div>
		{/each}
		{#if addingOutputPort}
			<div class="expanded-port-block expanded-port-block-right">
				<input
					type="text"
					class="port-name-input-inline"
					placeholder="port name"
					bind:value={newOutputName}
					onkeydown={(e) => handlePortKeydown(e, 'output')}
					onblur={() => { addingOutputPort = false; newOutputName = ''; }}
					onclick={(e) => e.stopPropagation()}
				/>
			</div>
		{:else}
			<button 
				class="expanded-add-port-btn expanded-add-port-btn-right"
				onclick={(e) => { e.stopPropagation(); addingOutputPort = true; }}
			>
				<span>output</span>
				<span class="text-xs">+</span>
			</button>
		{/if}
	</div>
</div>

{:else}
<!-- ═══════════════ COLLAPSED: looks like a regular node ═══════════════ -->
<div class="collapsed-node" class:selected>
	<!-- Color accent bar -->
	<div class="collapsed-accent"></div>

	<!-- Header -->
	<div class="collapsed-header">
		<div class="collapsed-header-left">
			<span class="header-icon" style="color: #52525b;"><Group size={14} /></span>
			<span class="collapsed-type">GROUP</span>
		</div>
		<div class="flex items-center gap-0.5">
			<ExecutionInspector {executions} label={data.label || 'Group'} />
			<button class="expand-toggle" onclick={toggleExpand} title="Expand group">
				<Maximize2 size={12} />
			</button>
		</div>
	</div>

	<div class="px-3 py-2 overflow-hidden nodrag nopan flex flex-col">
		<!-- Label -->
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
			<!-- svelte-ignore a11y_no_static_element_interactions -->
			<p 
				class="text-sm font-medium text-zinc-800 cursor-text hover:bg-black/5 px-1 py-0.5 rounded -mx-1 truncate"
				ondblclick={startEditLabel}
				title="Double-click to rename"
			>
				{data.label || 'Group'}
			</p>
		{/if}

		<!-- Description (collapsed only) -->
		{#if groupDescription}
			<div class="mt-1 nodrag nopan">
				<p
					class="text-[11px] text-zinc-500 leading-snug whitespace-pre-wrap {descExpanded ? '' : 'line-clamp-2'}"
				>
					{groupDescription}
				</p>
				{#if groupDescription.length > 80 || groupDescription.includes('\n')}
					<button
						class="text-[10px] text-zinc-400 hover:text-zinc-600 transition-colors mt-0.5"
						onclick={(e) => { e.stopPropagation(); descExpanded = !descExpanded; }}
					>
						{descExpanded ? 'Show less' : 'Show more'}
					</button>
				{/if}
			</div>
		{/if}

		<!-- Ports Section -->
		<div class="mt-2 flex justify-between text-[10px] text-zinc-500 w-full">
			<!-- Input Ports -->
			<div class="space-y-1 min-w-0 flex-1">
				{#each inputs as input}
					{@const pMarker = portMarkerStyle(input, oneOfRequiredPorts, noConfigFilled, getPortTypeColor(input.portType), 'input')}
					<!-- svelte-ignore a11y_no_static_element_interactions -->
					<div class="relative flex items-center gap-1 group pl-3" oncontextmenu={(e) => { e.preventDefault(); e.stopPropagation(); portContextMenu = { portName: input.name, side: 'input', x: e.clientX, y: e.clientY }; }}>
						<Handle
							type="target"
							position={Position.Left}
							id={input.name}
							title={!input.required && oneOfRequiredPorts.has(input.name) ? `At least one required: ${oneOfRequiredGroups.filter(g => g.includes(input.name)).map(g => g.join(' or ')).join('; ')}` : input.name}
							style="top: 50%; {pMarker.style}"
							class={pMarker.class}
							oncontextmenu={(e: MouseEvent) => { e.preventDefault(); e.stopPropagation(); portContextMenu = { portName: input.name, side: 'input', x: e.clientX, y: e.clientY }; }}
						/>
						<span class="truncate" title={input.name}>{input.name}</span>
						<button 
							class="opacity-0 group-hover:opacity-100 text-destructive hover:text-destructive/80 ml-auto text-xs leading-none"
							onclick={(e) => { e.stopPropagation(); removePort('input', input.name); }}
							title="Remove port"
						>×</button>
					</div>
				{/each}
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
			</div>

			<!-- Output Ports -->
			<div class="space-y-1 text-right min-w-0 flex-1">
				{#each outputs as output}
					{@const oMarker = portMarkerStyle(output, oneOfRequiredPorts, noConfigFilled, getPortTypeColor(output.portType), 'output')}
					<!-- svelte-ignore a11y_no_static_element_interactions -->
					<div class="relative flex items-center gap-1 justify-end group pr-3" oncontextmenu={(e) => { e.preventDefault(); e.stopPropagation(); portContextMenu = { portName: output.name, side: 'output', x: e.clientX, y: e.clientY }; }}>
						<Handle
							type="source"
							position={Position.Right}
							id={output.name}
							style="top: 50%; {oMarker.style}"
							class={oMarker.class}
							oncontextmenu={(e: MouseEvent) => { e.preventDefault(); e.stopPropagation(); portContextMenu = { portName: output.name, side: 'output', x: e.clientX, y: e.clientY }; }}
						/>
						<button 
							class="opacity-0 group-hover:opacity-100 text-destructive hover:text-destructive/80 mr-auto text-xs leading-none"
							onclick={(e) => { e.stopPropagation(); removePort('output', output.name); }}
							title="Remove port"
						>×</button>
						<span class="truncate" title={output.name}>{output.name}</span>
					</div>
				{/each}
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
			</div>
		</div>
	</div>
</div>
{/if}

<style>
	/* ═══════════════ EXPANDED MODE ═══════════════ */
	.expanded-container {
		width: 100%;
		height: 100%;
		background: rgba(148, 163, 184, 0.06);
		border: 2px dashed rgba(148, 163, 184, 0.4);
		border-radius: 12px;
		min-width: 250px;
		min-height: 200px;
		position: relative;
	}

	.expanded-container.selected {
		border-color: hsl(var(--primary));
		border-style: solid;
		background: hsl(var(--primary) / 0.04);
	}

	.expanded-header {
		display: flex;
		align-items: center;
		gap: 6px;
		padding: 6px 10px;
		background: rgba(255, 255, 255, 0.85);
		border-radius: 10px 10px 0 0;
		border-bottom: 1px solid rgba(148, 163, 184, 0.25);
		font-size: 11px;
		font-weight: 600;
		color: #52525b;
		backdrop-filter: blur(4px);
	}

	.expanded-side-ports {
		position: absolute;
		top: 40px;
		display: flex;
		flex-direction: column;
		gap: 6px;
		z-index: 1;
		padding: 4px 0;
	}

	.expanded-side-left {
		left: 6px;
	}

	.expanded-side-right {
		right: 6px;
	}

	.expanded-port-block {
		display: flex;
		flex-direction: column;
		gap: 2px;
		font-size: 10px;
		color: #52525b;
		white-space: nowrap;
	}

	.expanded-port-label-row {
		display: flex;
		align-items: center;
		gap: 4px;
	}

	.expanded-port-label-row-right {
		justify-content: flex-end;
	}

	.expanded-port-dots {
		display: flex;
		align-items: center;
		gap: 4px;
	}

	.expanded-port-dots-right {
		justify-content: flex-end;
	}

	.expanded-port-label {
		font-weight: 500;
	}

	.expanded-add-port-btn {
		display: flex;
		align-items: center;
		gap: 2px;
		font-size: 10px;
		color: rgba(113, 113, 122, 0.6);
		background: none;
		border: none;
		cursor: pointer;
		padding: 2px 8px;
		transition: color 0.15s;
	}

	.expanded-add-port-btn:hover {
		color: #71717a;
	}

	.expanded-add-port-btn-right {
		justify-content: flex-end;
	}

	.port-name-input-inline {
		font-size: 10px;
		background: white;
		border: 1px solid #d4d4d8;
		border-radius: 4px;
		padding: 1px 4px;
		outline: none;
		width: 70px;
	}

	.port-name-input-inline:focus {
		border-color: hsl(var(--primary));
	}

	/* ═══════════════ COLLAPSED MODE ═══════════════ */
	.collapsed-node {
		background: white;
		border: 1px solid #e4e4e7;
		border-radius: 8px;
		min-width: 160px;
		width: 100%;
		box-shadow: 0 1px 3px rgba(0, 0, 0, 0.06);
		overflow: hidden;
	}

	.collapsed-node.selected {
		border-color: hsl(var(--primary));
		box-shadow: 0 0 0 2px hsl(var(--primary) / 0.15);
	}

	:global(.node-running) .collapsed-node {
		box-shadow: 0 1px 3px rgba(0, 0, 0, 0.08), 0 0 0 2px rgba(245, 158, 11, 0.4);
	}
	:global(.node-completed) .collapsed-node {
		box-shadow: 0 1px 3px rgba(0, 0, 0, 0.08), 0 0 0 2px rgba(16, 185, 129, 0.3);
	}
	:global(.node-failed) .collapsed-node {
		box-shadow: 0 1px 3px rgba(0, 0, 0, 0.08), 0 0 0 2px rgba(239, 68, 68, 0.4);
	}

	.collapsed-accent {
		height: 3px;
		background: #52525b;
		width: 100%;
	}

	.collapsed-header {
		display: flex;
		align-items: center;
		justify-content: space-between;
		padding: 6px 10px;
		border-bottom: 1px solid rgba(0, 0, 0, 0.05);
	}

	.collapsed-header-left {
		display: flex;
		align-items: center;
		gap: 5px;
	}

	.collapsed-type {
		font-size: 10px;
		font-weight: 700;
		letter-spacing: 0.05em;
		color: #52525b;
	}

	/* ═══════════════ SHARED ═══════════════ */
	.header-icon {
		display: flex;
		align-items: center;
		color: #71717a;
	}

	.header-label {
		color: #3f3f46;
		overflow: hidden;
		text-overflow: ellipsis;
		white-space: nowrap;
	}

	.expand-toggle {
		display: flex;
		align-items: center;
		justify-content: center;
		width: 18px;
		height: 18px;
		border-radius: 4px;
		border: none;
		background: transparent;
		color: #71717a;
		cursor: pointer;
		transition: background-color 0.15s, color 0.15s;
	}

	.expand-toggle:hover {
		background: rgba(0, 0, 0, 0.06);
		color: #3f3f46;
	}

	/* ═══════════════ LABEL EDITING ═══════════════ */
	.label-input {
		font-size: 11px;
		font-weight: 600;
		color: #3f3f46;
		background: rgba(255, 255, 255, 0.95);
		border: 1px solid #d4d4d8;
		border-radius: 4px;
		padding: 1px 4px;
		outline: none;
		min-width: 60px;
		flex: 1;
	}

	.label-input:focus {
		border-color: hsl(var(--primary));
	}


</style>
