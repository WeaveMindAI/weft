<script lang="ts">
	import { Handle, Position, NodeResizer, type ResizeParams } from "@xyflow/svelte";
	import { Group, RotateCw, Maximize2, Minimize2, ChevronDown, ChevronRight } from '@lucide/svelte';
	import { isLoopNodeType } from "../../types";
	import { NODE_TYPE_CONFIG } from '../../nodes';
	import type { NodeDataUpdates, PortDefinition, NodeExecution, FieldDefinition } from "../../types";
	import { getPortTypeColor } from "../../constants/colors";
	import { GROUP_PORTS_TOP_PX, LOOP_CONFIG_STRIP_BAR_PX, LOOP_CONFIG_STRIP_OPEN_PX } from "../../constants/loop-layout";
	import { createPortContextMenu, buildPortMenuItems } from '../../utils/port-context-menu';
	import { classifyInputPort, classifyOutputPort, removeFromOverAndCarry } from '../../utils/loop-port-roles';
	import { toast } from 'svelte-sonner';
	import { portMarkerStyle } from '../../utils/port-marker';
	import ExecutionInspector from './ExecutionInspector.svelte';
	import { SIMPLIFIED_IN_HANDLE, SIMPLIFIED_OUT_HANDLE, SIMPLIFIED_INNER_SOURCE_HANDLE, SIMPLIFIED_INNER_TARGET_HANDLE, SIMPLIFIED_LOOP_INDEX_HANDLE, SIMPLIFIED_LOOP_DONE_HANDLE, SIMPLIFIED_CONTENT_W_PX, SIMPLIFIED_SQUARE_PAD_PX, simplifiedDotStyle } from "../../constants/simplified-view";
	import { GROUP_COLOR, LOOP_COLOR } from "../../constants/colors";
	import FieldStrip from './FieldStrip.svelte';

	// Group interface ports cannot take a body-set literal (see the rule
	// enforced in enrichment's validate_required_ports). Pass an empty set to
	// portMarkerStyle so they never render as 'empty-dotted'.
	const noLiteralFilled = new Set<string>();

	let { data, id, selected }: {
		id: string;
		data: {
			label: string | null;
			nodeType: string;
			/// Simplified view: collapsed renders a square like a plain node;
			/// expanded keeps the box but collapses its interface to one dot
			/// per side (+ inner dots for children, + loop index/done).
			simplified?: boolean;
			config: Record<string, unknown>;
			inputs?: PortDefinition[];
			outputs?: PortDefinition[];
			features?: { oneOfRequired?: string[][] };
			onUpdate?: (updates: NodeDataUpdates) => void;
			executions?: NodeExecution[];
			executionCount?: number;
			/// Aggregated IRC logs over every node inside this group:
			/// one entry per bus any member node touched. Empty `[]`
			/// when no member node has used a bus.
			busLogs?: Array<{
				busId: string;
				events: import('../../../../shared/protocol').BusInspectorEvent[];
				meta?: import('../../../../shared/protocol').BusMeta;
			}>;
			/// Execution-wide journal corruptions. Empty in the normal
			/// case. The inspector renders a muted collapsed disclosure
			/// at the bottom when non-empty.
			journalCorruptions?: Array<{
				site: import('../../../../shared/protocol').CorruptionSite;
				reason: string;
			}>;
			/// Loop-specific inspector events for this loop group.
			/// Empty for ordinary groups.
			loopEvents?: import('../../../../shared/protocol').LoopInspectorEvent[];
		};
		selected?: boolean
	} = $props();

	const inputs = $derived((data.inputs ?? []) as PortDefinition[]);
	const outputs = $derived((data.outputs ?? []) as PortDefinition[]);
	const carryPortNames = $derived(new Set(
		isLoopNodeType(data.nodeType)
			? ((data.config?.carry as string[] | undefined) ?? [])
			: []
	));

	// One-of-required groups on the group's interface ports: parsed from
	// `@require_one_of(a, b)` directives in the group signature. Same shape
	// as regular node features.oneOfRequired.
	const oneOfRequiredGroups: string[][] = $derived(data.features?.oneOfRequired ?? []);
	const oneOfRequiredPorts: Set<string> = $derived(new Set(oneOfRequiredGroups.flat()));
	const isExpanded = $derived((data.config?.expanded as boolean) ?? true);
	const groupDescription = $derived((data.config?.description as string) ?? '');
	let descExpanded = $state(false);

	const executions = $derived(data.executions ?? []);
	// Aggregated bus logs over every node inside the group; the
	// group's inspector shows the combined IRC view per bus.
	const busLogs = $derived(data.busLogs ?? []);
	const journalCorruptions = $derived(data.journalCorruptions ?? []);
	const loopEvents = $derived(data.loopEvents ?? []);

	const isLoop = $derived(isLoopNodeType(data.nodeType));
	// Container identity color (violet for a Loop, zinc for a Group). One value for
	// the header, ports, and the simplified square; the scoped style block restates
	// the same hex (see the SYNC note on GROUP_COLOR/LOOP_COLOR in colors.ts).
	const containerColor = $derived(isLoop ? LOOP_COLOR : GROUP_COLOR);
	const configCollapsed = $derived((data.config?.configCollapsed as boolean) ?? false);

	/// Field definitions for the loop config strip. over and carry no
	/// longer live in the strip: their values are derived from the per-port
	/// role chosen via right-click on each port, so the strip only shows
	/// parallel, max_iters, trim_on_mismatch.
	const loopFields: FieldDefinition[] = $derived(
		isLoop ? ((NODE_TYPE_CONFIG.Loop?.fields ?? []) as FieldDefinition[]) : []
	);

	function updateLoopConfig(key: string, value: unknown) {
		if (!data.onUpdate) return;
		data.onUpdate({ config: { ...data.config, [key]: value } });
	}

	function toggleConfigCollapsed(e: MouseEvent) {
		e.stopPropagation();
		if (!data.onUpdate) return;
		// Toggling the config strip changes the loop's intrinsic minimum
		// size. Snap height to the NEW min and flag the update as a resize
		// so ELK reflows neighbours. The user's previous manual resize is
		// intentionally overridden: toggling is a deliberate request to
		// re-fit the container to its content.
		const nextCollapsed = !configCollapsed;
		const nextMinH = computeMinHeightFor(inputs.length, outputs.length, nextCollapsed);
		data.onUpdate({
			config: { ...data.config, configCollapsed: nextCollapsed, height: nextMinH },
			resized: true,
		});
	}

	const minExpandedHeight = $derived(computeMinHeight(inputs.length, outputs.length));

	/// Y offset of the side-port columns: the loop config strip (open or
	/// collapsed bar) pushes them below the header.
	const portsTop = $derived(
		GROUP_PORTS_TOP_PX +
			(isLoop && !data.simplified ? (configCollapsed ? LOOP_CONFIG_STRIP_BAR_PX : LOOP_CONFIG_STRIP_OPEN_PX) : 0),
	);

	// Auto-enforce minimum height when ports change or on load. This is a LAYOUT
	// change only: send just the `height` key, NOT the whole spread config. The
	// host update handler turns width/height/expanded into a layout-file write and
	// every OTHER config key into a source edit op; spreading the full config here
	// would emit a no-op `setConfig` for every existing value (needless source
	// churn) and split this into a source+layout action. A bare `{height}` is a
	// clean layout-only update that persists through the layout path.
	let lastEnforcedMinH = 0;
	$effect(() => {
		if (!isExpanded || !data.onUpdate) return;
		const currentH = (data.config?.height as number) || 0;
		const minH = minExpandedHeight;
		if (currentH < minH && minH !== lastEnforcedMinH) {
			lastEnforcedMinH = minH;
			data.onUpdate({ config: { height: minH } });
		}
	});

	function toggleExpand() {
		if (data.onUpdate) {
			data.onUpdate({
				config: { ...data.config, expanded: !isExpanded }
			});
		}
	}

	// Resize end: the user dragged the resize handle. `resized: true` tells the
	// host this is a real user resize (re-run ELK so neighbours make room), as
	// opposed to a programmatic dimension write like the min-height auto-enforce
	// below, which carries height too but must NOT trigger a relayout.
	function handleResizeEnd(_event: unknown, params: ResizeParams) {
		if (data.onUpdate) {
			data.onUpdate({
				config: { ...data.config, width: params.width, height: params.height },
				resized: true,
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

	/// Cycle a loop port's role. Inputs flip between broadcast and iter.
	/// Outputs flip between gather and carry. Each toggle plumbs both the
	/// port-list change (when needed) and the over / carry config change in
	/// a single onUpdate so the round-trip lands as one atomic batch.
	function cycleLoopRole(portName: string, side: 'input' | 'output') {
		if (!isLoop || !data.onUpdate) return;
		const lc = (data.config as Record<string, unknown>) ?? {};
		if (side === 'input') {
			const port = inputs.find(p => p.name === portName);
			if (!port) return;
			const current = classifyInputPort(port, lc);
			if (current.conflictReason) return;
			if (current.role === 'iter') {
				const over = (lc.over as string[] | undefined ?? []).filter(n => n !== portName);
				data.onUpdate({ config: { ...lc, over } });
			} else if (current.role === 'broadcast') {
				const over = [...(lc.over as string[] | undefined ?? [])];
				if (!over.includes(portName)) over.push(portName);
				data.onUpdate({ config: { ...lc, over } });
			}
		} else {
			const port = outputs.find(p => p.name === portName);
			if (!port) return;
			const current = classifyOutputPort(port, lc, inputs);
			if (current.conflictReason) return;
			if (current.role === 'carry') {
				// Dissolve the pairing: the name leaves the carry list AND the
				// paired input leaves the signature, whether it was a derived
				// ghost or a source-declared input (older sources wrote carry
				// inputs explicitly). The edit server drops the input's wires
				// with the port; the gather output itself stays.
				const carry = (lc.carry as string[] | undefined ?? []).filter(n => n !== portName);
				const newInputs = inputs.filter(p => p.name !== portName);
				data.onUpdate({ config: { ...lc, carry }, inputs: newInputs });
			} else if (current.role === 'gather') {
				// Only the carry list changes: the paired input is DERIVED (the
				// compiler synthesizes it on parse, the projection on apply), so
				// nothing is written into the signature. The ghost appears in the
				// same projection re-derive as the config flip, no flicker.
				const carry = [...(lc.carry as string[] | undefined ?? [])];
				if (!carry.includes(portName)) carry.push(portName);
				data.onUpdate({ config: { ...lc, carry } });
			}
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

		const loopRole = isLoop
			? (side === 'input'
				? (() => {
					const c = classifyInputPort(port, (data.config as Record<string, unknown>) ?? {});
					return {
						currentRole: c.role,
						conflictReason: c.conflictReason,
						onToggleRole: () => cycleLoopRole(portName, side),
					};
				})()
				: (() => {
					const c = classifyOutputPort(port, (data.config as Record<string, unknown>) ?? {}, inputs);
					return {
						currentRole: c.role,
						conflictReason: c.conflictReason,
						onToggleRole: () => cycleLoopRole(portName, side),
					};
				})())
			: undefined;

		const items = buildPortMenuItems({
			port,
			side,
			isCustom: true,
			canAddPorts: true,
			onToggleRequired: () => togglePortRequired(portName, side),
			onSetType: (newType) => setPortType(portName, side, newType),
			onRemove: () => removePort(side, portName),
			loopRole,
		});

		return createPortContextMenu(x, y, items, () => { portContextMenu = null; });
	});

	function computeMinHeightFor(numInputs: number, numOutputs: number, collapsed: boolean): number {
		// Loops always reserve room for one extra port row on each side
		// (the implicit index / done), and either a thin config bar or
		// the open config strip below the header. Without this, child nodes
		// drawn inside the body would overlap the strip. `collapsed` is the
		// config-strip state to compute FOR (may differ from current state
		// when called from the toggle handler).
		// Simplified view draws one interface dot per side and no config strip, so
		// the per-port rows and the strip reserve no space (matching the layout
		// engine's loopStripPx, which returns 0 when simplified). Counting them
		// here would floor the box at a height with phantom empty bands.
		const loopExtras = isLoop && !data.simplified;
		const visibleInputs = numInputs + (loopExtras ? 1 : 0);
		const visibleOutputs = numOutputs + (loopExtras ? 1 : 0);
		const portsBlock = data.simplified ? 0 : Math.max(visibleInputs, visibleOutputs) * 30 + 24;
		const headerArea = 44;
		const bodyMin = 220; // breathing room for at least a couple of child nodes
		const configArea = loopExtras ? (collapsed ? LOOP_CONFIG_STRIP_BAR_PX : LOOP_CONFIG_STRIP_OPEN_PX) : 0;
		return headerArea + configArea + portsBlock + bodyMin;
	}

	function computeMinHeight(numInputs: number, numOutputs: number): number {
		return computeMinHeightFor(numInputs, numOutputs, configCollapsed);
	}

	function addPort(side: 'input' | 'output', name: string) {
		if (!name.trim() || !data.onUpdate) return;
		const trimmed = name.trim();
		// Reserved port names on loops: `index` cannot be a user-declared
		// INPUT port (collides with the implicit self.index), `done` cannot
		// be a user-declared OUTPUT port (collides with the implicit
		// self.done). The other side is fine.
		if (isLoop && side === 'input' && trimmed === 'index') {
			toast.error(`"index" is a reserved loop input port name (auto-rendered from self.index).`);
			return;
		}
		if (isLoop && side === 'output' && trimmed === 'done') {
			toast.error(`"done" is a reserved loop output port name (auto-rendered from self.done).`);
			return;
		}
		const currentInputs = [...inputs];
		const currentOutputs = [...outputs];
		if (side === 'input') {
			if (currentInputs.some(p => p.name === trimmed)) {
				const existing = currentInputs.find(p => p.name === trimmed)!;
				if (existing.synthesizedFromCarry) {
					toast.error(`"${trimmed}" is already auto-generated from the carry output. Rename the output or pick a different name.`);
				} else {
					toast.error(`Input port "${trimmed}" already exists.`);
				}
				return;
			}
			currentInputs.push({ name: trimmed, portType: 'MustOverride', required: false });
		} else {
			if (currentOutputs.some(p => p.name === trimmed)) {
				toast.error(`Output port "${trimmed}" already exists.`);
				return;
			}
			currentOutputs.push({ name: trimmed, portType: 'MustOverride', required: false });
		}
		// Send the port change alone. If the extra port grows the group past its
		// current height, the min-height `$effect` above fires right after this
		// update re-renders the node and enforces the new height as its own clean
		// bare `{height}` layout write. Bundling height in here would re-spread the
		// whole config (emitting no-op source ops) and mix a layout change into a
		// structural op; keeping them separate reuses the one min-height mechanism.
		data.onUpdate({ inputs: currentInputs, outputs: currentOutputs });
	}

	function removePort(side: 'input' | 'output', name: string) {
		if (!data.onUpdate) return;
		// Deleting the synthesized carry-input is equivalent to converting
		// the matching carry OUTPUT back to gather. The synthesized side has
		// no existence of its own in the source; the cascade below drops it.
		if (side === 'input') {
			const port = inputs.find(p => p.name === name);
			if (port?.synthesizedFromCarry && isLoop) {
				const lc = (data.config as Record<string, unknown>) ?? {};
				const carry = (lc.carry as string[] | undefined ?? []).filter(n => n !== name);
				data.onUpdate({ config: { ...lc, carry } });
				return;
			}
			// `name_collision` case: a non-synthesized input shares its
			// name with a carry output. The user is resolving the
			// collision by deleting THIS input; the carry output (and
			// its config entry) must survive untouched. Without this
			// guard, `removeFromOverAndCarry` below would strip the
			// `carry: [..., name]` entry, silently demoting the
			// surviving carry output to gather.
			if (port && !port.synthesizedFromCarry && isLoop) {
				const lc = (data.config as Record<string, unknown>) ?? {};
				const carry = (lc.carry as string[] | undefined ?? []);
				if (carry.includes(name)) {
					const currentInputs = inputs.filter(p => p.name !== name);
					data.onUpdate({ inputs: currentInputs });
					return;
				}
			}
		}
		let currentInputs = side === 'input' ? inputs.filter(p => p.name !== name) : [...inputs];
		const currentOutputs = side === 'output' ? outputs.filter(p => p.name !== name) : [...outputs];
		// For loops, also propagate the port deletion into over / carry config
		// lists so the source stays consistent (no over: ["foo"] when foo no
		// longer exists). If the deleted port is a carry output, we also drop
		// the synthesized input that was mirroring it.
		if (isLoop) {
			const lc = (data.config as Record<string, unknown>) ?? {};
			const { over, carry } = removeFromOverAndCarry(lc, name);
			if (side === 'output') {
				// Deleting a carry output: also drop its synthesized input.
				currentInputs = currentInputs.filter(p => !(p.name === name && p.synthesizedFromCarry));
			}
			data.onUpdate({
				inputs: currentInputs,
				outputs: currentOutputs,
				config: { ...lc, over, carry },
			});
			return;
		}
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
		<span class="header-icon">
			{#if isLoopNodeType(data.nodeType)}
				<RotateCw size={13} color={LOOP_COLOR} />
			{:else}
				<Group size={13} />
			{/if}
		</span>
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
			<ExecutionInspector {executions} {busLogs} {journalCorruptions} {loopEvents} label={data.label || 'Group'} />
			<button class="expand-toggle" onclick={toggleExpand} title="Collapse group">
				<Minimize2 size={12} />
			</button>
		</div>
	</div>

	{#if isLoop && !data.simplified}
		<!-- Loop config strip: own collapse independent of the box. Hidden in
		     simplified view (no config there). -->
		<!-- svelte-ignore a11y_no_static_element_interactions -->
		<!-- svelte-ignore a11y_click_events_have_key_events -->
		<div class="loop-config-strip nodrag nopan" onclick={(e) => e.stopPropagation()}>
			<button class="loop-config-toggle" onclick={toggleConfigCollapsed} title={configCollapsed ? 'Show loop config' : 'Hide loop config'}>
				{#if configCollapsed}
					<ChevronRight size={11} />
				{:else}
					<ChevronDown size={11} />
				{/if}
				<span class="loop-config-toggle-label">Config</span>
			</button>
			{#if !configCollapsed}
				<div class="loop-config-fields">
					<FieldStrip
						fields={loopFields}
						config={(data.config as Record<string, unknown>) ?? {}}
						idPrefix={id}
						onUpdate={updateLoopConfig}
					/>
				</div>
			{/if}
		</div>
	{/if}

	<!-- Left boundary ports -->
	<div class="expanded-side-ports expanded-side-left nodrag nopan" style="top: {portsTop}px">
		{#if data.simplified}
			<!-- Single left interface: outer target (outside -> group inputs) +
			     inner source (group inputs -> children). Loop adds an `index`
			     inner-source dot below. -->
			<div class="expanded-port-block">
				<div class="expanded-port-dots">
					<Handle type="target" position={Position.Left} id={SIMPLIFIED_IN_HANDLE}
						style={simplifiedDotStyle(GROUP_COLOR)}
						class="!rounded-full !relative !inset-auto !transform-none" />
					<Handle type="source" position={Position.Right} id={SIMPLIFIED_INNER_SOURCE_HANDLE}
						style={simplifiedDotStyle(GROUP_COLOR)}
						class="!rounded-full !relative !inset-auto !transform-none" />
				</div>
			</div>
			{#if isLoop}
				<div class="expanded-port-block implicit-port" title="self.index (current iteration)">
					<div class="expanded-port-label-row"><span class="implicit-glyph">∗</span><span class="expanded-port-label">index</span></div>
					<div class="expanded-port-dots">
						<Handle type="source" position={Position.Right} id={SIMPLIFIED_LOOP_INDEX_HANDLE}
							style={simplifiedDotStyle(LOOP_COLOR)}
							class="!rounded-full !relative !inset-auto !transform-none" />
					</div>
				</div>
			{/if}
		{:else}
		{#each inputs as input}
			{@const pMarker = portMarkerStyle(input, oneOfRequiredPorts, noLiteralFilled, getPortTypeColor(input.portType), 'input', '!relative !inset-auto !transform-none')}
			{@const isGhost = !!input.synthesizedFromCarry}
			<!-- svelte-ignore a11y_no_static_element_interactions -->
			<div class="expanded-port-block group" class:port-ghost={isGhost} oncontextmenu={(e) => { e.preventDefault(); e.stopPropagation(); portContextMenu = { portName: input.name, side: 'input', x: e.clientX, y: e.clientY }; }}>
				<div class="expanded-port-label-row">
					{#if carryPortNames.has(input.name) || isGhost}
						<span class="carry-glyph" title={isGhost ? 'auto from carry output' : 'carry port'}>↻</span>
					{/if}
					<span class="expanded-port-label">{input.name}</span>
					{#if !isGhost}
						<button
							class="opacity-0 group-hover:opacity-100 text-destructive hover:text-destructive/80 text-xs leading-none"
							onclick={(e) => { e.stopPropagation(); removePort('input', input.name); }}
							title="Remove port"
						>×</button>
					{/if}
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

		{#if isLoop}
			<!-- Implicit `self.index`: inside-out only. Body nodes wire FROM
			     this handle. No outer handle: the index has no presence in
			     the loop's outer signature. -->
			<div class="expanded-port-block implicit-port" title="self.index (current iteration)">
				<div class="expanded-port-label-row">
					<span class="implicit-glyph">∗</span>
					<span class="expanded-port-label">index</span>
				</div>
				<div class="expanded-port-dots">
					<Handle
						type="source"
						position={Position.Right}
						id="index__inner"
						style="background-color: {LOOP_COLOR};"
						class="!w-2.5 !h-2.5 !border !border-white !rounded-full !relative !inset-auto !transform-none"
					/>
				</div>
			</div>
		{/if}
		{/if}
	</div>

	<!-- Right boundary ports -->
	<div class="expanded-side-ports expanded-side-right nodrag nopan" style="top: {portsTop}px">
		{#if data.simplified}
			<!-- Single right interface: inner target (children -> group outputs) +
			     outer source (group outputs -> outside). Loop adds a `done`
			     inner-target dot below. -->
			<div class="expanded-port-block expanded-port-block-right">
				<div class="expanded-port-dots expanded-port-dots-right">
					<Handle type="target" position={Position.Left} id={SIMPLIFIED_INNER_TARGET_HANDLE}
						style={simplifiedDotStyle(GROUP_COLOR)}
						class="!rounded-full !relative !inset-auto !transform-none" />
					<Handle type="source" position={Position.Right} id={SIMPLIFIED_OUT_HANDLE}
						style={simplifiedDotStyle(GROUP_COLOR)}
						class="!rounded-full !relative !inset-auto !transform-none" />
				</div>
			</div>
			{#if isLoop}
				<div class="expanded-port-block expanded-port-block-right implicit-port" title="self.done (write true to terminate loop)">
					<div class="expanded-port-label-row expanded-port-label-row-right"><span class="expanded-port-label">done</span><span class="implicit-glyph">∗</span></div>
					<div class="expanded-port-dots expanded-port-dots-right">
						<Handle type="target" position={Position.Left} id={SIMPLIFIED_LOOP_DONE_HANDLE}
							style={simplifiedDotStyle(LOOP_COLOR)}
							class="!rounded-full !relative !inset-auto !transform-none" />
					</div>
				</div>
			{/if}
		{:else}
		{#each outputs as output}
			{@const oMarker = portMarkerStyle(output, oneOfRequiredPorts, noLiteralFilled, getPortTypeColor(output.portType), 'output', '!relative !inset-auto !transform-none')}
			<!-- svelte-ignore a11y_no_static_element_interactions -->
			<div class="expanded-port-block expanded-port-block-right group" oncontextmenu={(e) => { e.preventDefault(); e.stopPropagation(); portContextMenu = { portName: output.name, side: 'output', x: e.clientX, y: e.clientY }; }}>
				<div class="expanded-port-label-row expanded-port-label-row-right">
					<button
						class="opacity-0 group-hover:opacity-100 text-destructive hover:text-destructive/80 text-xs leading-none"
						onclick={(e) => { e.stopPropagation(); removePort('output', output.name); }}
						title="Remove port"
					>×</button>
					<span class="expanded-port-label">{output.name}</span>
					{#if carryPortNames.has(output.name)}
						<span class="carry-glyph" title="carry port">↻</span>
					{/if}
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

		{#if isLoop}
			<!-- Implicit `self.done`: inside-in only. Body nodes wire TO
			     this handle to vote loop termination. No outer handle. -->
			<div class="expanded-port-block expanded-port-block-right implicit-port" title="self.done (write true to terminate loop after this iteration)">
				<div class="expanded-port-label-row expanded-port-label-row-right">
					<span class="expanded-port-label">done</span>
					<span class="implicit-glyph">∗</span>
				</div>
				<div class="expanded-port-dots expanded-port-dots-right">
					<Handle
						type="target"
						position={Position.Left}
						id="done__inner"
						style="background-color: {LOOP_COLOR};"
						class="!w-2.5 !h-2.5 !border !border-white !rounded-full !relative !inset-auto !transform-none"
					/>
				</div>
			</div>
		{/if}
		{/if}
	</div>
</div>

{:else if data.simplified}
<!-- ═══════════════ COLLAPSED + SIMPLIFIED: a square, like a plain node ═══════════════ -->
<Handle type="target" position={Position.Left} id={SIMPLIFIED_IN_HANDLE}
	style="top: 50%; {simplifiedDotStyle(containerColor)}" />
<div class="simplified-node rounded-lg select-none" class:selected
	style="width: 100%; height: 100%; display: flex; flex-direction: column; align-items: center; justify-content: center; gap: 4px; padding: {SIMPLIFIED_SQUARE_PAD_PX}px; background: rgba(255,255,255,0.95); border: 1px solid {selected ? containerColor : 'rgba(0,0,0,0.08)'}; box-shadow: 0 1px 3px rgba(0,0,0,0.08), 0 4px 12px rgba(0,0,0,0.05);">
	<div class="absolute top-1 right-1 flex items-center gap-0.5 nodrag nopan">
		<ExecutionInspector {executions} {busLogs} {journalCorruptions} {loopEvents} label={data.label || 'Group'} />
		<button class="expand-toggle" onclick={toggleExpand} title="Expand group"><Maximize2 size={12} /></button>
	</div>
	<!-- Fixed-width content column so a collapsed container measures as a uniform
	     square (the wrapper is `width: max-content`); a long label truncates. -->
	<div class="flex flex-col items-center gap-1" style="width: {SIMPLIFIED_CONTENT_W_PX}px;">
	{#if isLoop}
		<RotateCw size={26} color={containerColor} />
		<span class="text-[10px] font-semibold tracking-wide uppercase text-center leading-tight max-w-full truncate" style="color: {containerColor};">{data.label || 'Loop'}</span>
	{:else}
		<Group size={26} color={containerColor} />
		<span class="text-[10px] font-semibold tracking-wide uppercase text-center leading-tight max-w-full truncate" style="color: {containerColor};">{data.label || 'Group'}</span>
	{/if}
	</div>
</div>
<Handle type="source" position={Position.Right} id={SIMPLIFIED_OUT_HANDLE}
	style="top: 50%; {simplifiedDotStyle(containerColor)}" />
{:else}
<!-- ═══════════════ COLLAPSED: looks like a regular node ═══════════════ -->
<div class="collapsed-node" class:selected>
	<!-- Color accent bar -->
	<div class="collapsed-accent"></div>

	<!-- Header -->
	<div class="collapsed-header">
		<div class="collapsed-header-left">
			{#if isLoopNodeType(data.nodeType)}
				<span class="header-icon" style="color: {LOOP_COLOR};"><RotateCw size={14} /></span>
				<span class="collapsed-type" style="color: {LOOP_COLOR};">LOOP</span>
			{:else}
				<span class="header-icon" style="color: {GROUP_COLOR};"><Group size={14} /></span>
				<span class="collapsed-type">GROUP</span>
			{/if}
		</div>
		<div class="flex items-center gap-0.5">
			<ExecutionInspector {executions} {busLogs} {journalCorruptions} {loopEvents} label={data.label || 'Group'} />
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
					{@const pMarker = portMarkerStyle(input, oneOfRequiredPorts, noLiteralFilled, getPortTypeColor(input.portType), 'input')}
					{@const isGhost = !!input.synthesizedFromCarry}
					<!-- svelte-ignore a11y_no_static_element_interactions -->
					<div class="relative flex items-center gap-1 group pl-3" class:port-ghost={isGhost} oncontextmenu={(e) => { e.preventDefault(); e.stopPropagation(); portContextMenu = { portName: input.name, side: 'input', x: e.clientX, y: e.clientY }; }}>
						<Handle
							type="target"
							position={Position.Left}
							id={input.name}
							title={!input.required && oneOfRequiredPorts.has(input.name) ? `At least one required: ${oneOfRequiredGroups.filter(g => g.includes(input.name)).map(g => g.join(' or ')).join('; ')}` : input.name}
							style="top: 50%; {pMarker.style}"
							class={pMarker.class}
							oncontextmenu={(e: MouseEvent) => { e.preventDefault(); e.stopPropagation(); portContextMenu = { portName: input.name, side: 'input', x: e.clientX, y: e.clientY }; }}
						/>
						{#if carryPortNames.has(input.name) || isGhost}
							<span class="carry-glyph" title={isGhost ? 'auto from carry output' : 'carry port'}>↻</span>
						{/if}
						<span class="truncate" title={input.name}>{input.name}</span>
						{#if !isGhost}
							<button
								class="opacity-0 group-hover:opacity-100 text-destructive hover:text-destructive/80 ml-auto text-xs leading-none"
								onclick={(e) => { e.stopPropagation(); removePort('input', input.name); }}
								title="Remove port"
							>×</button>
						{/if}
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
					{@const oMarker = portMarkerStyle(output, oneOfRequiredPorts, noLiteralFilled, getPortTypeColor(output.portType), 'output')}
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
						{#if carryPortNames.has(output.name)}
							<span class="carry-glyph" title="carry port">↻</span>
						{/if}
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
	/* SYNC: container hex colors <-> ../../constants/colors.ts GROUP_COLOR (#52525b)
	   / LOOP_COLOR (#8b5cf6). Svelte scoped CSS can't read a TS const, so these
	   rules restate the hex (and the loop violet also appears alpha-blended as
	   rgba(139,92,246,...) in .loop-config-strip). Change one side, change both. */
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

	.loop-config-strip {
		background: rgba(139, 92, 246, 0.04);
		border-bottom: 1px solid rgba(139, 92, 246, 0.2);
		padding: 4px 10px 6px 10px;
		font-size: 11px;
	}

	.loop-config-toggle {
		display: flex;
		align-items: center;
		gap: 3px;
		color: #8b5cf6;
		font-size: 10px;
		font-weight: 600;
		padding: 2px 0;
		background: transparent;
		border: none;
		cursor: pointer;
	}

	.loop-config-toggle:hover {
		opacity: 0.8;
	}

	.loop-config-toggle-label {
		text-transform: uppercase;
		letter-spacing: 0.05em;
	}

	.loop-config-fields {
		display: flex;
		flex-direction: column;
		gap: 5px;
		margin-top: 4px;
	}

	/* `top` comes inline from `portsTop` (loop-layout constants), so the
	 * rendered strip offset and ELK's padding share one source. */
	.expanded-side-ports {
		position: absolute;
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

	.carry-glyph {
		color: #8b5cf6;
		font-weight: 600;
		font-size: 11px;
		line-height: 1;
	}

	/* Synthesized carry-input ports: shown as a ghost mirror of the
	 * matching carry output. They render the handle (so edges can attach)
	 * but the label is dimmed and the delete button is hidden. The user
	 * removes them via the carry output's role toggle. */
	:global(.port-ghost) {
		opacity: 0.55;
	}
	:global(.port-ghost .expanded-port-label) {
		font-style: italic;
	}

	/* Implicit loop ports (self.index, self.done): inside-only handles
	 * that body nodes wire to. Visually distinct from user-declared ports
	 * to communicate they are language-level, not part of the signature. */
	.implicit-port {
		opacity: 0.85;
	}
	.implicit-glyph {
		color: #8b5cf6;
		font-weight: 700;
		font-size: 11px;
		line-height: 1;
	}
	.implicit-port .expanded-port-label {
		font-style: italic;
		color: #8b5cf6;
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
