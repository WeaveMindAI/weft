<script lang="ts">
	import { untrack } from "svelte";
	import { Handle, Position, useEdges, useNodes, NodeResizer, type ResizeParams } from "@xyflow/svelte";
	import { NODE_TYPE_CONFIG, specForService, type NodeType } from "../../nodes";
	import type { PortDefinition, PortType, NodeDataUpdates, FieldDefinition, NodeFeatures, NodeExecution, LiveDataItem, NodeExecutionStatus } from "../../types";
	import { declaredHomeValue, inputExposure, ownValue, storedValueOf } from "../../types";
	import { PORT_TYPE_COLORS, getPortTypeColor } from "../../constants/colors";
	import type { Edge } from "@xyflow/svelte";
	import CodeEditor from "../CodeEditor.svelte";
	import { toast } from "svelte-sonner";
	import CopyButton from "../ui/CopyButton.svelte";
	import { buildSpecMap, deriveInputsFromEntries, deriveOutputsFromEntries, entryPortCollisions, entryPortName, isValidFieldKey, type PortEntryDef, type PortSpec } from '../../utils/port-specs';
	import { getStatusBadgeColor, getStatusIcon } from "../../utils/status";
	import type { ConfigFieldSpan, FileContent, BusInspectorEvent, BusMeta, CorruptionSite, NodeFeedState } from "../../../../protocol";
	import { BadgeQuestionMark, Eye, EyeOff, Maximize2, Minimize2, FileSymlink, Pencil } from '@lucide/svelte';
	import { createFieldEditor } from '../../utils/field-editor.svelte';
	import { useFieldEditorRegistry } from './field-editor-registry';
	import { emptyToUnset, isFileRefValue, type WeftFileRefValue } from '../../value-format';
	import { createPortContextMenu, buildPortMenuItems } from "../../utils/port-context-menu";
	import { portMarkerStyle } from "../../utils/port-marker";
	import { fieldForInput, fieldForSpecField, inputRendersField, nextPortLiterals, shouldFlowField } from "../../utils/input-field";
	import ExecutionInspector from './ExecutionInspector.svelte';
	import { SIMPLIFIED_IN_HANDLE, SIMPLIFIED_OUT_HANDLE, SIMPLIFIED_CONTENT_W_PX, SIMPLIFIED_SQUARE_PAD_PX, SIMPLIFIED_CARD_MAX_W_PX, simplifiedDotStyle } from "../../constants/simplified-view";
	import FieldStrip from './FieldStrip.svelte';
	import FileDropField from './FileDropField.svelte';
	import AccessField from './AccessField.svelte';
	import RemoteSelectField from './RemoteSelectField.svelte';
	import { grantsForService, grantsGeneration } from './grants-cache.svelte';
	import FilePreview from './FilePreview.svelte';
	import FlowDock from './FlowDock.svelte';
	import type { FileValueWire } from "../../../../protocol";
	import { parseFileValue, typeReferencesFile, SHOULD_FLOW_PORT } from "../../../../protocol";

	const edgesState = useEdges();
	const nodesState = useNodes();

	let { data, id, selected }: {
		data: {
			label: string | null;
			nodeType: NodeType;
			/// Simplified view: render as a fixed square (icon + type label,
			/// one in/out dot), no ports/config/body. Execution overlays
			/// (status glyph, inspector, glow) are kept.
			simplified?: boolean;
			/// The user aimed the run at this output node (right-click, "Set as
			/// target"). Renders as a breathing ring in the node's own colour,
			/// which is the only place the feature shows on the canvas.
			runTarget?: boolean;
			config: Record<string, unknown>;
			/// Body-set PORT values + their written forms, the two-home
			/// twin of `config` (see NodeInstance.portLiterals).
			portLiterals?: Record<string, unknown>;
			portLiteralSpans?: Record<string, ConfigFieldSpan>;
			inputs?: PortDefinition[];
			outputs?: PortDefinition[];
			features?: NodeFeatures;
			// Resolved state of @file targets, keyed by the marker's relative
			// path (content or read error). A config field whose value is a
			// `@file(...)` tag displays fileContents[path]; config itself never
			// holds resolved content.
			fileContents?: Record<string, FileContent>;
			includePath?: string;
			onUpdate?: (updates: NodeDataUpdates) => void;
			onSaveFileRef?: (path: string, content: string) => void;
			onOpenInclude?: (path: string, alias: string) => void;
			infraNodeStatus?: string;
			infraFailureStage?: string;
			infraFailureMessage?: string;
			debugData?: unknown;
			executions?: NodeExecution[];
			executionCount?: number;
			/// One IRC-style scrollable log per bus this node took part
			/// in (live + replay, identical shape). Empty `[]` for
			/// nodes that never touched a bus. Populated by ProjectEditorInner
			/// from `executionState.busLogByBus` filtered by participant set.
			busLogs?: Array<{
				busId: string;
				events: BusInspectorEvent[];
				meta?: BusMeta;
			}>;
			/// Execution-wide journal corruptions. Empty in the normal
			/// case. The inspector renders a muted collapsed
			/// disclosure at the bottom when non-empty; not alarming.
			journalCorruptions?: Array<{
				site: CorruptionSite;
				reason: string;
			}>;
			/// Body-panel feed for this node, set ONLY for infra
			/// (infra /live) and trigger (listener /display) nodes.
			/// Other nodes get undefined and render no body panel
			/// here. Distinct from `debugData` which is the JSON
			/// preview chip Debug-style nodes show under the body
			/// from the last execution's output.
			bodyFeed?: NodeFeedState;
		};
		id: string;
		selected?: boolean;
	} = $props();

	// Typed as NodeTemplate so the fallback and the registry entries are
	// ONE shape: every `typeConfig.<key>` read typechecks against the
	// template, never against an ad-hoc literal that silently lacks keys.
	const typeConfig = $derived<import('../../types').NodeTemplate>(
		NODE_TYPE_CONFIG[data.nodeType as NodeType] ?? {
			type: data.nodeType,
			label: data.nodeType,
			description: 'Unknown node type',
			icon: BadgeQuestionMark,
			color: '#999',
			tags: [],
			requiresInfra: false,
			defaultInputs: [],
			defaultOutputs: [],
		},
	);

	// Opaque `@include` block: carries a file path, navigates into the file
	// on Open. Renders ports + an Open affordance, no config/body.
	const isInclude = $derived(!!data.includePath);
	// Human-readable name of the included component, derived from its filename
	// (`components/my-cleaner.weft` -> "My Cleaner"): the basename without
	// `.weft`, `-`/`_` to spaces, each word capitalized. Matches the name the
	// component's own group shows when you navigate into it.
	const includeName = $derived.by(() => {
		const p = data.includePath;
		if (!p) return '';
		const stem = (p.split(/[\\/]/).pop() ?? p).replace(/\.weft$/, '');
		return stem
			.split(/[-_\s]+/)
			.filter(Boolean)
			.map((w) => w.charAt(0).toUpperCase() + w.slice(1))
			.join(' ');
	});

	const executions = $derived(data.executions ?? []);
	const latestExecution = $derived(executions[executions.length - 1]);
	// `undefined` means idle (no execution yet), NOT a status value.
	// The status helpers are exhaustive over the real statuses only;
	// the idle case is handled explicitly at each use site (no glyph,
	// the node's own type color) instead of a `''` sentinel that fell
	// through every switch and silently returned undefined.
	const displayedStatus = $derived<NodeExecutionStatus | undefined>(latestExecution?.status);
	// Per-bus IRC log this node took part in. Empty `[]` for nodes
	// that never touched a bus.
	const busLogs = $derived(data.busLogs ?? []);
	const journalCorruptions = $derived(data.journalCorruptions ?? []);

	const entryKinds: PortSpec[] = $derived(typeConfig.portsFromConfig?.specs ?? []);
	const entryKindByName: Record<string, PortSpec> = $derived(buildSpecMap(entryKinds));

	/** Input ports that have an incoming edge, so their field is hidden
	 *  (the edge is the value's source of truth). */
	const wiredInputPorts: Set<string> = $derived.by(() => {
		const wired = new Set<string>();
		for (const e of edgesState.current) {
			if (e.target === id && e.targetHandle) wired.add(e.targetHandle);
		}
		return wired;
	});

	/** The node's body-set port values and their written forms (the
	 *  `portLiterals` / `portLiteralSpans` maps of the definition: one
	 *  home per name, separate from config). */
	const portLiterals = $derived((data.portLiterals as Record<string, unknown>) ?? {});
	const portLiteralSpans = $derived(data.portLiteralSpans ?? {});

	/** Input ports satisfied by a body-set literal and no edge. These
	 *  render with the 'empty-dotted' port marker to signal "filled from
	 *  code" without changing the declared port type; docking onto one is
	 *  vetoed until the literal is unset (one driver per port). */
	const literalFilledPorts: Set<string> = $derived.by(() => {
		const filled = new Set<string>();
		for (const [name, v] of Object.entries(portLiterals)) {
			if (wiredInputPorts.has(name)) continue;
			if (v !== undefined && v !== null && v !== '') filled.add(name);
		}
		return filled;
	});

	/** Fields rendered in the expanded view: the node's INPUTS are the
	 *  field list, one field per input, flattened from each input's
	 *  RESOLVED widget (the editor derives nothing). Rendering rules by
	 *  exposure: 'wire' inputs never get a field; a wired input shows no
	 *  field (the edge is the driver); 'config' inputs edit the config
	 *  home; 'all'/'assignment' inputs edit the port-literal home
	 *  (portDriven), with 'assignment' locked to the statement form. */
	const displayedFields: FieldDefinition[] = $derived.by(() => {
		const inputList = (data.inputs || typeConfig.defaultInputs || []) as PortDefinition[];
		const result: FieldDefinition[] = [];
		for (const input of inputList) {
			const rendered = inputRendersField(input, {
				wired: wiredInputPorts.has(input.name),
				hasWrittenValue: portLiterals[input.name] !== undefined,
			});
			if (!rendered) continue;
			result.push(
				input.name === SHOULD_FLOW_PORT ? shouldFlowField('node') : fieldForInput(input),
			);
		}
		return result;
	});

	/** The written form of a port-driven field's value ('inline' = braces,
	 *  'connection' = statement). A value not yet in source defaults to
	 *  the braces form on its first write, except on an assignment-only
	 *  input, where the statement form is the only legal one. */
	function portFieldForm(key: string): 'inline' | 'connection' {
		const span = ownValue(portLiteralSpans, key) as ConfigFieldSpan | undefined;
		return span?.origin ?? (portFieldLocked(key) ? 'connection' : 'inline');
	}

	/** An assignment-only input's field is locked to the statement form:
	 *  the braces form cannot drive it, so the toggle is disabled. */
	function portFieldLocked(key: string): boolean {
		const inputList = (data.inputs || typeConfig.defaultInputs || []) as PortDefinition[];
		const input = inputList.find((p) => p.name === key);
		return input !== undefined && inputExposure(input) === 'assignment';
	}


	/// The debug preview's text: the node's latest output, as JSON.
	const debugDataJson = $derived.by(() => {
		if (data.debugData === undefined || data.debugData === null) return null;
		return JSON.stringify(data.debugData, null, 2);
	});

	// The node's declared inline file display (`features.display`):
	// the named port's value from the latest firing, read off the side
	// the declaration names (a sink shows what was wired IN, a
	// generator shows what it EMITTED). Only a concrete
	// `__weft_<kind>__` marker carrying a `key` or `url` handle
	// renders; data-backed values have no resolvable handle.
	const displayedFileValue = $derived.by<FileValueWire | null>(() => {
		const spec = typeConfig.display;
		if (!spec) return null;
		// The declaration names the port WITH its side, so a node with
		// a same-named input and output stays unambiguous.
		const side = spec.output !== undefined ? latestExecution?.output : latestExecution?.input;
		const port = spec.output ?? spec.input;
		if (typeof side !== 'object' || side === null || port === undefined) return null;
		return parseFileValue(ownValue(side as Record<string, unknown>, port));
	});

	// Check if node has expandable content (fields, run location option, debug preview, etc.)
	const hasExpandableContent = $derived.by(() => {
		// Has input fields (config/all/assignment inputs not currently wired)
		if (displayedFields.length > 0) return true;
		// Has debug preview (Debug node)
		if (typeConfig.features?.showDebugPreview) return true;
		// Has a declared file display (MediaDisplay / DownloadLink / a generator)
		if (typeConfig.display) return true;
		return false;
	});

	// The THREE live-display parts, each a single predicate that is the ONE source
	// of truth for "is this part present". Both the `hasLiveDisplay` boolean (which
	// decides square-vs-card) and the `liveDisplay` renderer gate on these exact
	// flags, so a part can never render without growing the card, or be counted
	// without rendering. Add a new live-display kind = add a flag here and a branch
	// in `liveDisplay`, and `hasLiveDisplay` picks it up for free.
	const showBodyFeed = $derived(
		!!data.bodyFeed &&
			(data.bodyFeed.state === 'error' ||
				data.bodyFeed.state === 'absent' ||
				(data.bodyFeed.state === 'ok' && data.bodyFeed.items.length > 0)),
	);
	const showDebugDisplay = $derived(!!(typeConfig.features?.showDebugPreview && debugDataJson));
	const showFileDisplay = $derived(!!(typeConfig.display && displayedFileValue));
	// Simplified view: a node with any live-display part (an infra/trigger feed, a
	// debug preview, an image/file preview) is drawn as a card showing that display
	// instead of a bare square.
	const hasLiveDisplay = $derived(showBodyFeed || showDebugDisplay || showFileDisplay);

	// Get expanded state from config (persisted), default collapsed for regular nodes
	const expanded = $derived((data.config?.expanded as boolean) ?? false);
	
	// Resize end: save the new dimensions (width/height only; `expanded` is
	// unchanged so we don't resend it). The host classifies resize vs collapse
	// by whether `expanded` actually changes value, so this stays a resize.
	function handleResizeEnd(_event: unknown, params: ResizeParams) {
		if (data.onUpdate) {
			data.onUpdate({
				config: { ...data.config, width: params.width, height: params.height },
				resized: true,
			});
		}
	}
	let editingLabel = $state(false);
	// Seed the editable label from the prop's initial value (deliberately a
	// one-time, non-reactive read via `untrack`: while editing, the input is the
	// user's local working copy, re-seeded explicitly on each edit start/cancel,
	// not bound live to the prop). `untrack` both states that and silences svelte
	// 5.56's `state_referenced_locally` warning.
	let labelInput = $state(untrack(() => data.label || ''));
	let addingInputPort = $state(false);
	let addingOutputPort = $state(false);
	let newInputName = $state('');
	let newOutputName = $state('');
	let portContextMenu = $state<{ portName: string; side: 'input' | 'output'; x: number; y: number } | null>(null);

	/// Per-secret-item reveal state, keyed by item label. A secret
	/// is hidden by default (••••); clicking the eye icon toggles
	/// visibility for that label only. Local to this node instance;
	/// closing/reopening the inspector resets to hidden, which is
	/// the desired security default.
	let revealedSecrets = $state<Record<string, boolean>>({});
	// The node's root element (`bind:this`). `$state` because an `$effect` below
	// reads it (the deselect-blur effect): when `bind:this` assigns it after
	// mount, the effect must re-run, which a plain `let` would not trigger.
	let nodeElement = $state<HTMLDivElement>();

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
	
	// Port context menu rendered on document.body to avoid CSS transform positioning issues
	$effect(() => {
		if (!portContextMenu) return;
		const { portName, side, x, y } = portContextMenu;
		const port = side === 'input'
			? inputs.find((p) => p.name === portName)
			: outputs.find((p) => p.name === portName);
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
			if (ownValue(currentHeights, fieldKey) !== height) {
				data.onUpdate({
					config: { 
						...data.config, 
						textareaHeights: { ...currentHeights, [fieldKey]: height } 
					}
				});
			}
		}
	}
	
	function getPortColor(portType: PortType): string {
		return getPortTypeColor(portType);
	}

	const inputs = $derived(data.inputs || typeConfig.defaultInputs);
	/** Inputs that render a PORT DOCK. A `config`-exposure input is a
	 *  design-time setting the graph never wires, so it gets a field in
	 *  the body but no handle on the edge rail. `inputs` stays the
	 *  COMPLETE list (edits round-trip the full set). */
	const wireableInputs = $derived(inputs.filter(
		(p: PortDefinition) => inputExposure(p) !== 'config' && p.name !== SHOULD_FLOW_PORT
	));
	const outputs = $derived(data.outputs || typeConfig.defaultOutputs);

	// Dynamic min resize height: header + ports + fixed buffer for at least one config line
	// Accent bar (2) + header row (32) + content padding (16) + label (24) + ports gap (8) + port rows + buffer (100)
	const PORT_ROW_HEIGHT = 25;
	const minResizeHeight = $derived(
		2 + 32 + 16 + 24 + 8 + Math.max(wireableInputs.length, outputs.length) * PORT_ROW_HEIGHT + 80
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
	// `_should_flow` decides whether the node runs at all, so it docks in
	// the top-left corner as a square instead of sitting in the port rail
	// among the node's own inputs. Filled means something answers it: a
	// wire, or a literal written straight into the braces.
	const flowConnected = $derived(
		edgesState.current.some((e: Edge) => e.target === id && e.targetHandle === SHOULD_FLOW_PORT)
			|| data.portLiterals?.[SHOULD_FLOW_PORT] !== undefined
	);
	// Simplified view draws a dot only when an edge attaches to it (the live
	// edges there are the merged __simp_* ones, flow wires included).
	const simplifiedInConnected = $derived(
		edgesState.current.some((e: Edge) => e.target === id && e.targetHandle === SIMPLIFIED_IN_HANDLE)
	);
	const simplifiedOutConnected = $derived(
		edgesState.current.some((e: Edge) => e.source === id && e.sourceHandle === SIMPLIFIED_OUT_HANDLE)
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

	/** If `config[key]` is a `@file`/`@asset` marker whose CONTENT is text the
	 *  host ships (both markers with text types), its ref. The per-field test
	 *  for "this field displays a referenced file's text". A file-typed
	 *  `@asset` is NOT: the marker itself is the field's value (the file-drop
	 *  field sets/clears it); nothing text-shaped exists to display, and
	 *  routing its edits into a file write would clobber the media file. */
	function fileRefOf(key: string): { path: string; type: string; marker: 'file' | 'asset' } | null {
		// CONFIG home only, and that is the whole story: the host bridge
		// puts every `@file`/`@asset` marker into config (never into a
		// port literal), and every consumer of this chain is config-
		// scoped too (FieldStrip asks only for non-port-driven fields,
		// `updateConfig` writes config, the chip renders in the config
		// branch). Reading the port literal first would let a
		// same-named port literal impose its marker on the config
		// field that legitimately shares its name.
		const v = ownValue(data.config as Record<string, unknown> | undefined, key);
		if (!isFileRefValue(v)) return null;
		return typeReferencesFile(v.__weftFileRef.type) ? null : v.__weftFileRef;
	}

	/** Resolved state of a file-backed field, from the host's fileContents
	 *  map. `loading` = content not yet delivered (brief, transient).
	 *  `error` = the file couldn't be read (fail loudly, no fallback). */
	function fileFieldState(key: string): { path: string; marker: 'file' | 'asset'; content?: string; error?: string; loading: boolean } | null {
		const ref = fileRefOf(key);
		if (!ref) return null;
		const entry = ownValue(data.fileContents, ref.path) as FileContent | undefined;
		// Undefined (not delivered) OR an explicit `{loading}` (bytes still being
		// fetched lazily) both render the non-interactive loading state.
		if (entry === undefined || 'loading' in entry) return { path: ref.path, marker: ref.marker, loading: true };
		if ('error' in entry) return { path: ref.path, marker: ref.marker, error: entry.error, loading: false };
		return { path: ref.path, marker: ref.marker, content: entry.content, loading: false };
	}

	/** A file-backed field is read-only when its content isn't loaded yet
	 *  (its display is a status string, not editable content) or when the ref
	 *  is an `@asset` (pull-only by contract: nothing ever writes back).
	 *  False for a normal field or a loaded `@file` field. The single
	 *  editability rule applied across every editable field branch. */
	function fileFieldReadonly(key: string): boolean {
		const fs = fileFieldState(key);
		return fs ? fs.content === undefined || fs.marker === 'asset' : false;
	}

	/// Field keys this node renders itself rather than delegating to the
	/// shared FieldStrip primitive renderer: only the exotic types (code,
	/// entry_list). File-backed primitives render through
	/// FieldStrip via its displayValueOf / readonlyKeys / headerBadge
	/// capabilities.
	const EXOTIC_FIELD_TYPES = new Set(['code', 'access', 'remote_select', 'entry_list', 'file_drop']);
	const customFieldKeys = $derived.by(() => {
		const keys = new Set<string>();
		for (const field of displayedFields) {
			if (EXOTIC_FIELD_TYPES.has(field.type)) keys.add(field.key);
		}
		return keys;
	});

	/// FieldStrip display override: for a file-backed field, the store
	/// value is the resolved file content (or a read status), never the
	/// `@file` marker that config holds. `undefined` for normal fields.
	function fileDisplayOverride(key: string): string | undefined {
		const fs = fileFieldState(key);
		if (!fs) return undefined;
		if (fs.content !== undefined) return fs.content;
		if (fs.error !== undefined) return `cannot read ${fs.path}: ${fs.error}`;
		return `loading ${fs.path}...`;
	}

	/// File-backed fields whose content isn't loaded (loading / read
	/// error) are read-only in FieldStrip so the status text can't be
	/// saved as content; `@asset`-backed fields are read-only by contract.
	const readonlyFieldKeys = $derived.by(() => {
		const keys = new Set<string>();
		for (const field of displayedFields) {
			if (fileFieldReadonly(field.key)) keys.add(field.key);
		}
		return keys;
	});

	/// Typing into a read-only file-backed field: explain WHY it's locked,
	/// throttled per field so held-down keys don't stack toasts.
	const readonlyToastAt = new Map<string, number>();
	function explainReadonlyField(key: string) {
		const fs = fileFieldState(key);
		if (!fs) return;
		const now = Date.now();
		const last = readonlyToastAt.get(key) ?? 0;
		if (now - last < 4000) return;
		readonlyToastAt.set(key, now);
		if (fs.marker === 'asset') {
			toast.error(`This field pulls its value from @asset(${fs.path}); it can't be edited here. Switch it to @file (the badge next to the label) to edit.`);
		} else {
			toast.error(`Cannot edit ${fs.path}: ${fs.error ?? 'still loading'}`);
		}
	}

	/// Flip a text-file-backed field between `@file` (editable, writes back)
	/// and `@asset` (pull-only). A DIRECT config write, deliberately not
	/// `updateConfig` (which routes a `@file` field's edit into the
	/// referenced file's content); switching the marker edits the marker.
	function switchFileMarker(key: string) {
		const v = ownValue(data.config as Record<string, unknown> | undefined, key);
		if (!isFileRefValue(v)) return;
		const r = v.__weftFileRef;
		const flipped = { __weftFileRef: { ...r, marker: r.marker === 'file' ? 'asset' as const : 'file' as const } };
		data.onUpdate?.({ config: { ...data.config, [key]: flipped } });
	}

	function updateConfig(key: string, value: string | string[] | number | boolean | PortEntryDef[] | Record<string, unknown> | WeftFileRefValue | WeftFileRefValue[] | null) {
		// File-backed field: the edit goes to the referenced file, never to the
		// weft source. The `@file(...)` marker in config (and source) is left
		// untouched; only the file's content changes.
		const fs = fileFieldState(key);
		if (fs) {
			// An `@asset` is pull-only: nothing ever writes back to the
			// referenced file, and the field is read-only. Guard loudly (the
			// readonly rendering should make this unreachable).
			if (fs.marker === 'asset') {
				toast.error(`${fs.path} is an @asset (read-only); use @file for an editable reference`);
				return;
			}
			// Only a loaded field is editable. Editing while loading or on a
			// read error must not write (it would clobber the file with a
			// status string); the field is read-only in those states, but
			// guard loudly here too.
			if (fs.content === undefined) {
				toast.error(`Cannot edit ${fs.path}: ${fs.error ?? 'still loading'}`);
				return;
			}
			// A cleared box (null) empties the file; there is no "unset" for
			// file-backed content, the file IS the value.
			const content =
				value === null || value === undefined
					? ''
					: typeof value === 'string'
						? value
						: JSON.stringify(value, null, 2);
			data.onSaveFileRef?.(fs.path, content);
			return;
		}
		if (data.onUpdate) {
			// config holds the `@file(...)` tag for file-backed fields (never the
			// resolved content), so serializing the whole config re-emits the
			// marker. No special handling needed for sibling file-backed fields.
			const newConfig = { ...data.config, [key]: value };
			if (typeConfig.portsFromConfig && key === typeConfig.portsFromConfig.field) {
				const fields = value as PortEntryDef[];
				data.onUpdate({
					config: newConfig,
					inputs: deriveInputsFromEntries(fields, entryKindByName),
					outputs: deriveOutputsFromEntries(fields, entryKindByName),
				});
			} else {
				data.onUpdate({ config: newConfig });
			}
		}
	}

	const fieldEditor = createFieldEditor();
	const fieldEditorRegistry = useFieldEditorRegistry();
	// Register this node's field-editor flush so a teardown (window hidden,
	// panel close) that doesn't fire a field `blur` still commits the last
	// <700ms of typing. $effect's cleanup unregisters on destroy.
	$effect(() => fieldEditorRegistry?.register(fieldEditor.flush));

	/** Write a PORT-DRIVEN field's value: the port's body literal, kept
	 *  apart from config (one home per name). An emptied control clears
	 *  the literal (the port goes back to unset/wireable): null is what
	 *  the strip's and the code editor's cleared boxes save, and an
	 *  empty string is accepted as cleared too so no control can ever
	 *  store a phantom "" literal. */
	function updatePortLiteral(key: string, value: unknown) {
		if (!data.onUpdate) return;
		data.onUpdate({ portLiterals: nextPortLiterals(portLiterals, key, value) });
	}

	/** Route a field edit. FILE-BACKING WINS over the value's home, and
	 *  that ordering is the whole rule: a `@file`/`@asset` marker always
	 *  lives in config, but the resolved CONTENT may have been moved
	 *  into the port literal (enrich does that for an `all`-exposure
	 *  input written in the body). Saving such an edit to the port
	 *  literal would rewrite the source line with the entire file and
	 *  destroy the reference, so a file-backed field always routes to
	 *  the file write; only a plain field goes to its declared home
	 *  (port literal vs config). THE one routing rule; every control
	 *  calls this. */
	function updateFieldValue(key: string, value: unknown, portDriven?: boolean) {
		if (fileFieldState(key)) updateConfig(key, value as Parameters<typeof updateConfig>[1]);
		else if (portDriven) updatePortLiteral(key, value);
		else updateConfig(key, value as Parameters<typeof updateConfig>[1]);
	}

	/** Flip a port-driven field's WRITTEN form (braces `key: value` vs
	 *  statement `node.key = value`); the host rewrites the source. */
	function togglePortValueForm(key: string) {
		if (portFieldLocked(key)) {
			toast.error(`'${key}' takes a literal only as an assignment: ${id}.${key} = ... is the one written form.`);
			return;
		}
		const next = portFieldForm(key) === 'inline' ? 'connection' : 'inline';
		data.onUpdate?.({ portValueForm: { key, form: next } });
	}

	/** The stored raw value behind a field, read from the home its
	 *  exposure routes to (port literal vs config). Every custom-field
	 *  renderer reads through this so no branch hardcodes a home. */
	function declaredValue(field: FieldDefinition): unknown {
		return declaredHomeValue(portLiterals, data.config, field);
	}

	/** Resolve a remote_select's authenticating access STRUCTURALLY.
	 *  Two homes, checked in order: the named input may be an access
	 *  widget on THIS node (the grant handle sits in this node's own
	 *  config, no edge exists), or an Access-typed port wired back to a
	 *  feeding access node whose config holds the handle. Either way we
	 *  read the grant id persisted when the user clicked Connect, plus
	 *  the service off the owning template's recipe. There is no data
	 *  flow between nodes at edit time; this structural read is what
	 *  makes the dropdown live with nothing running. */
	/** Whether the named access input is an access WIDGET on this node
	 *  (the connection is picked in this node's own config) rather than
	 *  an Access-typed port fed by a wire. Drives both the trace below
	 *  and the dropdown's connect-first wording (pick here vs wire in). */
	function accessInputIsOwnWidget(accessInput: string | undefined): boolean {
		if (!accessInput) return false;
		const ownInputs = (data.inputs || typeConfig.defaultInputs || []) as PortDefinition[];
		return ownInputs.some((i) => i.name === accessInput && i.widget?.kind === 'access');
	}

	function traceAccessRef(accessInput: string): { accessId: string; service: string } | null {
		if (accessInputIsOwnWidget(accessInput)) {
			const service = typeConfig.service?.service;
			if (!service) return null;
			const handle = ownValue(data.config as Record<string, unknown> | undefined, accessInput);
			const grantId =
				handle && typeof handle === 'object' ? (handle as { id?: unknown }).id : undefined;
			return typeof grantId === 'string' ? { accessId: grantId, service } : null;
		}
		const edge = edgesState.current.find(
			(e: Edge) => e.target === id && e.targetHandle === accessInput,
		);
		if (!edge) return null;
		const src = nodesState.current.find((n) => n.id === edge.source);
		if (!src) return null;
		const srcData = src.data as {
			nodeType?: string;
			config?: Record<string, unknown>;
			inputs?: PortDefinition[];
		};
		const tpl = NODE_TYPE_CONFIG[srcData.nodeType as NodeType];
		const service = tpl?.service?.service;
		if (!service) return null;
		const srcInputs = (srcData.inputs ?? tpl?.defaultInputs ?? []) as PortDefinition[];
		const connectInput = srcInputs.find((i) => i.widget?.kind === 'access');
		if (!connectInput) return null;
		const handle = ownValue(srcData.config as Record<string, unknown> | undefined, connectInput.name);
		const grantId =
			handle && typeof handle === 'object' ? (handle as { id?: unknown }).id : undefined;
		return typeof grantId === 'string' ? { accessId: grantId, service } : null;
	}

	/// The traced connections' summaries, keyed by the ACCESS INPUT
	/// name they were traced through: what the live permission check
	/// and the resource sources read. Refetched when the traced ids
	/// change (a rewire, a reconnect).
	let tracedGrants = $state<
		Record<
			string,
			{
				scopes: string[];
				verified: boolean;
				valueNames: string[];
				owner: string;
				service: string;
			}
		>
	>({});

	/// Which access inputs to watch: every Access-typed input declaring
	/// `requiresScopes` or `requiresValues`, plus every remote_select's
	/// authenticating input (its sources filter on the granted set).
	const watchedAccessInputs = $derived.by(() => {
		const names = new Set<string>();
		const inputList = (data.inputs || typeConfig.defaultInputs || []) as PortDefinition[];
		for (const i of inputList) {
			if (i.requiresScopes && i.requiresScopes.length > 0) names.add(i.name);
			if (i.requiresValues && i.requiresValues.length > 0) names.add(i.name);
			if (i.widget?.kind === 'remote_select' && i.widget.access) names.add(i.widget.access);
		}
		return [...names];
	});

	/// The traced (input name, access ref) pairs, hoisted out of the
	/// effect so the trace itself is a derivation.
	const tracedRefs = $derived(
		watchedAccessInputs
			.map((name) => ({ name, ref: traceAccessRef(name) }))
			.filter((x): x is { name: string; ref: { accessId: string; service: string } } => x.ref != null),
	);
	/// Stable string key of the traced pairs: the ONE thing the fetch
	/// effect tracks. Drags/hovers mutate edgesState/nodesState without
	/// changing the traced ids, so they change neither this key nor
	/// refire the HTTP checks.
	const tracedRefsKey = $derived(JSON.stringify(tracedRefs));

	/// Why the grant fetch behind the live permission/value checks
	/// failed, if it did; renders a muted "the check is dark" line so
	/// the user knows the banners' absence proves nothing. The check
	/// stays advisory: a failure never marks the node (the resolve-time
	/// backstop still holds).
	let grantCheckError = $state<string | null>(null);

	$effect(() => {
		void tracedRefsKey;
		// A Forget or a fresh connect bumps the generation, so an OPEN
		// node re-checks instead of showing the old answer until an
		// unrelated re-trace.
		void grantsGeneration();
		const refs = untrack(() => tracedRefs);
		let cancelled = false;
		(async () => {
			const next: Record<
				string,
				{
					scopes: string[];
					verified: boolean;
					valueNames: string[];
					owner: string;
					service: string;
				}
			> = {};
			let failure: string | null = null;
			for (const { name, ref } of refs) {
				try {
					const grants = await grantsForService(ref.service);
					const grant = grants.find((g) => g.id === ref.accessId);
					if (grant)
						next[name] = {
							scopes: grant.scopes,
							verified: grant.permissions_verified,
							valueNames: grant.value_names ?? [],
							owner: grant.owner,
							service: ref.service,
						};
				} catch (e) {
					failure = e instanceof Error ? e.message : String(e);
				}
			}
			if (!cancelled) {
				tracedGrants = next;
				grantCheckError = failure;
			}
		})();
		return () => {
			cancelled = true;
		};
	});

	/// The live permission check that replaces the old compile-time
	/// diagnostic: a picked connection VERIFIED to miss a permission
	/// this node requires marks the node immediately. Claimed/unknown
	/// sets never mark (nobody actually knows what they hold).
	const permissionShortfalls = $derived.by(() => {
		const out: string[] = [];
		const inputList = (data.inputs || typeConfig.defaultInputs || []) as PortDefinition[];
		for (const i of inputList) {
			const required = i.requiresScopes ?? [];
			if (required.length === 0) continue;
			const grant = tracedGrants[i.name];
			if (!grant || !grant.verified) continue;
			for (const r of required) {
				if (!grant.scopes.includes(r)) {
					out.push(`'${i.name}' needs permission ${r}; the picked connection does not hold it. Reconnect or upgrade it on the access node.`);
				}
			}
		}
		return out;
	});

	/// The live OWN-ACCOUNT check: a node requiring an own-account-only
	/// capability (it creates things inside the credential's account)
	/// with the SHARED (runtime-owned) connection picked. Marks
	/// immediately, whatever verification says: this is a policy of
	/// the capability, not a provider-reported scope. The resolve-time
	/// refusal is the backstop.
	const ownAccountShortfalls = $derived.by(() => {
		const out: { text: string; link?: string }[] = [];
		const inputList = (data.inputs || typeConfig.defaultInputs || []) as PortDefinition[];
		for (const i of inputList) {
			const required = i.requiresScopes ?? [];
			if (required.length === 0) continue;
			const grant = tracedGrants[i.name];
			if (!grant || grant.owner !== 'ours') continue;
			const catalogue = specForService(grant.service)?.permissions ?? [];
			for (const p of catalogue) {
				if (p.own_only && required.includes(p.id)) {
					out.push({
						text: `'${p.label}' only works on your own ${grant.service} account (the shared credential's account would hold the result); connect your own on the access node.`,
						link: p.guide?.link,
					});
				}
			}
		}
		return out;
	});

	/// The live VALUE check: a picked connection that does not store a
	/// value this node needs marks the node. Unlike permissions there
	/// is no unknown case (the connection stores it or it does not), so
	/// this marks whenever the grant was read.
	const valueShortfalls = $derived.by(() => {
		const out: string[] = [];
		const inputList = (data.inputs || typeConfig.defaultInputs || []) as PortDefinition[];
		for (const i of inputList) {
			const required = i.requiresValues ?? [];
			if (required.length === 0) continue;
			const grant = tracedGrants[i.name];
			if (!grant) continue;
			for (const r of required) {
				if (!grant.valueNames.includes(r)) {
					out.push(`'${i.name}' needs the connection's ${r}, which it does not store. Add it to the connection on the access node.`);
				}
			}
		}
		return out;
	});

	/** A remote_select's picked parent values (`dependsOn` drill-down):
	 *  each parent's stored id. A pick stores the bare id, so this is a
	 *  read, not an unwrap. */
	function remoteSelectParents(field: FieldDefinition): Record<string, string> {
		const out: Record<string, string> = {};
		for (const parent of field.dependsOn ?? []) {
			const v = storedValueOf(portLiterals, data.config, parent);
			if (typeof v === 'string' && v) out[parent] = v;
		}
		return out;
	}

	function fieldDisplayValue(field: FieldDefinition): string {
		const fs = fileFieldState(field.key);
		if (fs) {
			// File-backed: show resolved content (editable). While loading or on
			// a read error, show a status (read-only); never the marker as a
			// value, never a silent fall back to inline content.
			if (fs.content !== undefined) return fieldEditor.display(field.key, fs.content);
			if (fs.error !== undefined) return `cannot read ${fs.path}: ${fs.error}`;
			return `loading ${fs.path}...`;
		}
		// The EFFECTIVE value: the set value, else the input's declared
		// default (what the runtime would supply). Same rule FieldStrip
		// applies to the primitive controls.
		const v = declaredValue(field) ?? field.defaultValue;
		const storeStr = (v === undefined || v === null) ? '' : (typeof v === 'string' ? v : JSON.stringify(v, null, 2));
		return fieldEditor.display(field.key, storeStr);
	}

	let addingEntry = $state(false);
	/// The entry being built, in its final shape: `kind`, the port name
	/// under the spec's own key field, and whatever that kind asked for,
	/// all at the top level.
	let newEntry = $state<PortEntryDef>({ kind: '' });
	/** Set when the user clicks Add with an empty or unusable name so the
	 *  name input renders in an error state (red border + message)
	 *  instead of silently no-op'ing. Cleared on any keystroke. */
	let newEntryKeyError = $state(false);

	/// Entries open for editing, keyed by the port name they had when the
	/// pen was clicked. That key is the row's identity: port names are
	/// unique across the list, so removing another row (or reordering)
	/// never sends an edit to the wrong entry. Several can be open at
	/// once, each with its own draft and its own error flag.
	let editDrafts = $state<Record<string, PortEntryDef>>({});
	let editKeyErrors = $state<Record<string, boolean>>({});

	function startEditingEntry(entry: PortEntryDef, key: string) {
		editDrafts = { ...editDrafts, [key]: { ...entry } };
		editKeyErrors = { ...editKeyErrors, [key]: false };
	}

	function cancelEditingEntry(key: string) {
		const { [key]: _drop, ...rest } = editDrafts;
		editDrafts = rest;
		const { [key]: _dropErr, ...restErrors } = editKeyErrors;
		editKeyErrors = restErrors;
	}

	function setEditDraft(key: string, draft: PortEntryDef) {
		editDrafts = { ...editDrafts, [key]: draft };
	}

	/// Switch a draft's kind, keeping only the port name: the values
	/// belonged to the kind that asked for them.
	function draftWithKind(draft: PortEntryDef, kind: string): PortEntryDef {
		const from = entryKindByName[draft.kind] ?? entryKinds[0];
		const to = entryKindByName[kind];
		const name = from ? entryPortName(draft, from) : '';
		return { kind, ...(to && name ? { [to.keyField]: name } : {}) };
	}

	const newEntrySpec = $derived(entryKindByName[newEntry.kind] ?? entryKinds[0]);

	/// The config key holding the entry list, which the node's own
	/// metadata names (`fields` for a form, `cases` for a switch).
	const entryListKey = $derived(typeConfig.portsFromConfig?.field ?? '');

	function getEntries(): PortEntryDef[] {
		return ((data.config as Record<string, unknown>)?.[entryListKey] as PortEntryDef[]) ?? [];
	}

	function removeEntry(index: number) {
		updateConfig(entryListKey, getEntries().filter((_, i) => i !== index));
	}

	function startAddingEntry() {
		newEntry = { kind: entryKinds[0]?.kind ?? '' };
		newEntryKeyError = false;
		addingEntry = true;
	}

	/// Write a draft into the entry list, as a new entry (`replacing`
	/// null) or over the one at `replacing`. The one place an entry is
	/// checked, so the add form and every open edit form refuse the same
	/// things: an unusable port name, a missing required value, and a
	/// port name another entry already took. Returns whether it landed;
	/// the caller closes its form on true.
	function commitEntry(draft: PortEntryDef, replacing: number | null): boolean {
		const spec = entryKindByName[draft.kind] ?? entryKinds[0];
		if (!spec) return false;
		const name = entryPortName(draft, spec).trim();
		if (!name || !isValidFieldKey(name)) {
			return false; // the caller lights up its own name input
		}
		// Emptied counts as missing: a required text field the user typed
		// into and then cleared leaves '', and an entry with an empty
		// required value is as incomplete as one with none.
		const missing = (spec.fields ?? []).filter((f) => {
			if (!f.required) return false;
			const v = draft[f.key];
			return v === undefined || v === null || v === '' || (Array.isArray(v) && v.length === 0);
		});
		if (missing.length > 0) {
			toast.error(`A "${spec.label}" needs ${missing.map((f) => f.label).join(', ')}.`);
			return false;
		}
		const entry: PortEntryDef = { ...draft, kind: spec.kind, [spec.keyField]: name };

		// The ports this entry would add, against the ones already there.
		// An edit measures against every OTHER entry, so keeping a name
		// is not a conflict with itself.
		const existing = getEntries();
		const others = replacing === null ? existing : existing.filter((_, i) => i !== replacing);
		const collisions = entryPortCollisions(entry, others, entryKindByName);
		if (collisions.length > 0) {
			toast.error(`Port name conflict: "${collisions.join('", "')}" already exists. Choose a different name.`);
			return false;
		}

		updateConfig(
			entryListKey,
			replacing === null
				? [...existing, entry]
				: existing.map((e, i) => (i === replacing ? entry : e)),
		);
		return true;
	}

	function addEntry() {
		if (!commitEntry(newEntry, null)) {
			const spec = newEntrySpec;
			const name = spec ? entryPortName(newEntry, spec).trim() : '';
			newEntryKeyError = !name || !isValidFieldKey(name);
			return;
		}
		addingEntry = false;
		newEntryKeyError = false;
	}

	/// Save one open edit. `key` is the port name the row had when the
	/// pen was clicked; the row it points at may have been removed while
	/// the form was open, and that says so rather than writing over a
	/// neighbour.
	function saveEditingEntry(key: string) {
		const draft = editDrafts[key];
		if (!draft) return;
		const index = getEntries().findIndex((e) => {
			const spec = entryKindByName[e.kind];
			return spec ? entryPortName(e, spec) === key : false;
		});
		if (index === -1) {
			toast.error(`"${key}" is no longer in the list, so there is nothing to save it over.`);
			cancelEditingEntry(key);
			return;
		}
		if (!commitEntry(draft, index)) {
			const spec = entryKindByName[draft.kind] ?? entryKinds[0];
			const name = spec ? entryPortName(draft, spec).trim() : '';
			editKeyErrors = { ...editKeyErrors, [key]: !name || !isValidFieldKey(name) };
			return;
		}
		cancelEditingEntry(key);
	}

	function addInputPort() {
		const name = newInputName.trim();
		if (!name) return;
		// One namespace: a user-added port may not take ANY declared
		// input's name. `inputs` is the complete list (config-exposure
		// inputs included), so this one check covers what used to be two
		// (the port-dedup and the config-field collision). Same rule the
		// compiler enforces (enrich.rs config-input collision); vetoing
		// here keeps the invalid port out of the source entirely.
		if (inputs.some((p: PortDefinition) => p.name === name)) {
			toast.error(`"${name}" is already an input of this node`);
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
		if (outputs.some((p: PortDefinition) => p.name === name)) {
			toast.error(`Output port "${name}" already exists`);
			return;
		}
		const newPort: PortDefinition = {
			name,
			portType: 'MustOverride',
			required: false,
		};
		const newOutputs = [...outputs, newPort];
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
		const newOutputs = outputs.filter((p: PortDefinition) => p.name !== portName);
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

<!-- The live-display content (infra/trigger feed, debug preview, image/file
     preview) rendered the SAME way in the full body and the simplified card,
     so the two never drift. `actionBtn` is its helper. Defined at the top
     level so both render branches can call it. -->
{#snippet entryForm(
	draft: PortEntryDef,
	keyError: boolean,
	idPrefix: string,
	commitLabel: string,
	onDraft: (next: PortEntryDef) => void,
	onCancel: () => void,
	onCommit: () => void,
)}
	{@const spec = entryKindByName[draft.kind] ?? entryKinds[0]}
	{@const fields = (spec?.fields ?? []).map(fieldForSpecField)}
	<div class="rounded border border-border bg-background p-2 space-y-2">
		<div class="space-y-1">
			<span class="text-[10px] text-muted-foreground font-medium block">Kind</span>
			<select
				class="w-full text-xs bg-muted px-2 py-1.5 rounded border-none outline-none"
				value={draft.kind}
				onchange={(e) => onDraft(draftWithKind(draft, e.currentTarget.value))}
			>
				{#each entryKinds as option}
					<option value={option.kind}>{option.label}</option>
				{/each}
			</select>
		</div>
		<div class="space-y-1">
			<label for={`${idPrefix}-name`} class="text-[10px] text-muted-foreground font-medium block">
				Name {#if spec}<span class="font-normal">(becomes the port name)</span>{/if}
			</label>
			<input
				id={`${idPrefix}-name`}
				type="text"
				class="w-full text-xs bg-muted px-2 py-1.5 rounded outline-none font-mono {keyError ? 'ring-1 ring-destructive' : 'border-none'}"
				placeholder={spec ? spec.keyField : 'name'}
				value={spec ? entryPortName(draft, spec) : ''}
				oninput={(e) => { if (spec) onDraft({ ...draft, [spec.keyField]: e.currentTarget.value }); }}
			/>
			{#if keyError}
				<p class="text-[10px] text-destructive">Letters, digits and underscores only, starting with a letter or an underscore.</p>
			{/if}
		</div>
		{#if fields.length > 0}
			<FieldStrip
				{fields}
				config={draft as Record<string, unknown>}
				idPrefix={idPrefix}
				onUpdate={(key, value) => onDraft({ ...draft, [key]: value })}
			/>
		{/if}
		<div class="flex gap-1 pt-0.5">
			<button
				class="flex-1 text-[10px] py-1 rounded text-muted-foreground hover:bg-muted transition-colors"
				onclick={(e) => { e.stopPropagation(); onCancel(); }}
			>Cancel</button>
			<button
				class="flex-1 text-[10px] py-1 rounded bg-primary text-primary-foreground hover:opacity-90 transition-opacity"
				onclick={(e) => { e.stopPropagation(); onCommit(); }}
			>{commitLabel}</button>
		</div>
	</div>
{/snippet}

{#snippet actionBtn(item: LiveDataItem)}
	{#if item.action}
		<button
			type="button"
			class="mt-1 text-[10px] px-2 py-0.5 rounded border border-zinc-300 bg-white hover:bg-zinc-50 text-zinc-700"
			onclick={(e) => {
				e.stopPropagation();
				const action = item.action!;
				const ev = new CustomEvent('weft-signal-action', {
					detail: { nodeId: id, actionKind: action.actionKind, payload: action.payload, confirm: action.confirm },
					bubbles: true,
				});
				(e.currentTarget as HTMLElement).dispatchEvent(ev);
			}}
		>
			{item.action.label}
		</button>
	{/if}
{/snippet}

{#snippet bodyFeedDisplay()}
	{#if data.bodyFeed}
		{#if data.bodyFeed.state === 'error'}
			<div class="flex items-start gap-1.5 text-[10px] text-rose-600 bg-rose-50 border border-rose-200 rounded px-2 py-1.5">
				<span class="font-medium shrink-0">Error</span>
				<span class="break-all">{data.bodyFeed.error}</span>
			</div>
		{:else if data.bodyFeed.state === 'absent'}
			<div class="text-[10px] text-muted-foreground bg-zinc-50 border border-zinc-200 rounded px-2 py-1.5">
				Not running. Start it from the action bar.
			</div>
		{:else if data.bodyFeed.items.length > 0}
			<div class="space-y-2">
				{#each data.bodyFeed.items as item}
					{#if item.type === 'image' && typeof item.data === 'string'}
						<div class="live-data-item">
							<span class="text-[10px] text-muted-foreground font-medium">{item.label}</span>
							<img src={item.data} alt={item.label} class="w-full rounded border border-zinc-200 mt-1" />
							{@render actionBtn(item)}
						</div>
					{:else if item.type === 'text'}
						<div class="live-data-item">
							<span class="text-[10px] text-muted-foreground font-medium block mb-1">{item.label}</span>
							<div class="relative">
								<div class="w-full text-[10px] font-mono bg-zinc-100 rounded px-2 py-1.5 pr-7 break-all border border-zinc-200 select-text cursor-text">{item.data}</div>
								<CopyButton text={String(item.data)} class="absolute top-1 right-1" />
							</div>
							{@render actionBtn(item)}
						</div>
					{:else if item.type === 'secret'}
						{@const revealed = revealedSecrets[item.label] ?? false}
						<div class="live-data-item">
							<span class="text-[10px] text-muted-foreground font-medium block mb-1">{item.label}</span>
							<div class="relative">
								<div class="w-full text-[10px] font-mono bg-zinc-100 rounded px-2 py-1.5 pr-12 break-all border border-zinc-200 select-text cursor-text">
									{#if revealed}{item.data}{:else}{'•'.repeat(Math.min(String(item.data).length, 32))}{/if}
								</div>
								<button
									type="button"
									class="absolute top-1 right-7 p-0.5 text-zinc-500 hover:text-zinc-700"
									title={revealed ? 'Hide' : 'Reveal'}
									onclick={(e) => { e.stopPropagation(); revealedSecrets = { ...revealedSecrets, [item.label]: !revealed }; }}
								>
									{#if revealed}<EyeOff class="w-3 h-3" />{:else}<Eye class="w-3 h-3" />{/if}
								</button>
								<CopyButton text={String(item.data)} class="absolute top-1 right-1" />
							</div>
							{@render actionBtn(item)}
						</div>
					{:else if item.type === 'progress' && typeof item.data === 'number'}
						<div class="live-data-item">
							<span class="text-[10px] text-muted-foreground font-medium">{item.label}</span>
							<div class="w-full h-1.5 bg-zinc-200 rounded-full mt-1 overflow-hidden">
								<div class="h-full bg-emerald-500 rounded-full transition-all" style="width: {Math.round(item.data * 100)}%"></div>
							</div>
							{@render actionBtn(item)}
						</div>
					{:else}
						<!-- Unknown item type, or a known type whose data shape doesn't match
						     (image/text/secret expect a string, progress a number). Rendering
						     nothing here silently drops the value and hides the mismatch; show a
						     visible chip so a malformed feed item is caught, not swallowed. -->
						<div class="live-data-item">
							<span class="text-[10px] text-amber-600 font-medium block mb-1">{item.label} (unrenderable: {item.type})</span>
							<div class="w-full text-[10px] font-mono bg-amber-50 rounded px-2 py-1.5 break-all border border-amber-200 select-text">{String(item.data)}</div>
						</div>
					{/if}
				{/each}
			</div>
		{/if}
	{/if}
{/snippet}

<!-- The simplified-view live display: the body feed plus the debug/image
     previews, shown together under the card header. The full builder view
     renders bodyFeed (always) and debug/image (expanded-gated) separately; the
     bodyFeed markup is shared via {@render bodyFeedDisplay}. -->
{#snippet liveDisplay()}
	{#if showBodyFeed}{@render bodyFeedDisplay()}{/if}
	<!-- Gate on the nullable value itself (not just the flag) so the type narrows
	     to non-null at the use site; the flag stays the card-vs-square authority. -->
	{#if showDebugDisplay && debugDataJson}
		<div class="relative">
			<CopyButton text={debugDataJson} class="absolute top-1 right-1 z-10 nodrag" />
			<pre class="debug-data-container nodrag nopan nowheel select-text cursor-text">{debugDataJson}</pre>
		</div>
	{/if}
	{#if showFileDisplay && displayedFileValue}
		<FilePreview file={displayedFileValue} mode={typeConfig.display?.kind === 'media' ? 'media' : 'link'} />
	{/if}
{/snippet}

<!-- Flow dock: `_should_flow` decides whether this node runs at all, so it
     sits apart from the node's own inputs, as a square in the top-left
     corner. Filled means something answers it. -->
{#snippet flowDock()}
	<FlowDock top={18} subject="node" connected={flowConnected} />
{/snippet}

{#if data.simplified}
	<!-- Simplified view. A bare node is a square: icon, type, editable label, one
	     in/out dot. A node with LIVE DISPLAY content (infra feed, debug preview,
	     image) grows into a card that shows that display under the header instead
	     of staying square. Execution overlays (status glyph, inspector, glow) are
	     kept; ports/config are not shown; structure is not editable, but the
	     LABEL can still be renamed by double-click. -->
	{@const Icon = typeConfig.icon}
	<!-- A dot only exists when something attaches to it: structure is not
	     editable here, so an unwired dot would be pure decoration. Flow
	     wires collapse onto the in dot too (the edge builder maps them
	     there), so a flow-gated node still shows its in dot; the builder
	     view's separate flow dock is not drawn. -->
	{#if simplifiedInConnected}
		<Handle
			type="target"
			position={Position.Left}
			id={SIMPLIFIED_IN_HANDLE}
			style="top: 50%; z-index: 5; {simplifiedDotStyle(typeConfig.color)}"
		/>
	{/if}
	<!-- svelte-ignore a11y_no_static_element_interactions -->
	<div
		bind:this={nodeElement}
		class="project-node simplified-node rounded-lg select-none transition-all duration-200 {displayedStatus === 'running' ? 'node-running-glow' : ''} {displayedStatus === 'waiting_for_input' ? 'node-waiting-glow' : ''} {displayedStatus === 'failed' ? 'node-failed-glow' : displayedStatus === 'completed' ? 'node-completed-glow' : ''} {selected ? 'node-selected' : ''} {data.runTarget ? 'node-run-target' : ''}"
		style="
			width: 100%;
			height: 100%;
			{hasLiveDisplay ? `min-width: 220px; max-width: ${SIMPLIFIED_CARD_MAX_W_PX}px;` : ''}
			display: flex;
			flex-direction: column;
			align-items: {hasLiveDisplay ? 'stretch' : 'center'};
			justify-content: {hasLiveDisplay ? 'flex-start' : 'center'};
			gap: 4px;
			padding: {SIMPLIFIED_SQUARE_PAD_PX}px;
			background: rgba(255, 255, 255, 0.95);
			border: 1px solid {selected ? typeConfig.color : 'rgba(0, 0, 0, 0.08)'};
			box-shadow: 0 1px 3px rgba(0, 0, 0, 0.08), 0 4px 12px rgba(0, 0, 0, 0.05){selected ? `, 0 0 0 1px ${typeConfig.color}20` : ''};
			backdrop-filter: blur(8px);
			--run-target-color: {typeConfig.color};
		"
	>
		<!-- Status glyph (top-right) + inspector (magnifier), same as full view -->
		<div class="absolute top-1 right-1 flex items-center gap-0.5 nodrag nopan z-10">
			{#if displayedStatus}
				<span class="text-xs leading-none {displayedStatus === 'running' ? 'animate-pulse' : ''}" style="color: {getStatusBadgeColor(displayedStatus) ?? typeConfig.color};">{getStatusIcon(displayedStatus)}</span>
			{/if}
			<ExecutionInspector {executions} {busLogs} {journalCorruptions} label={data.label || typeConfig.label} />
		</div>
		<!-- Bare node: the content column is fixed to the square's inner width (the
		     square side minus the 8px padding each side) so the node measures as a
		     uniform square regardless of label length (the wrapper is `width:
		     max-content`). A live-display node stretches to its card width instead,
		     so the wrapper grows and the layout re-reads the measured size. -->
		<div class="flex flex-col items-center gap-1 {hasLiveDisplay ? 'self-center' : ''}" style={hasLiveDisplay ? '' : `width: ${SIMPLIFIED_CONTENT_W_PX}px;`}>
			{#if isInclude}
				<FileSymlink size={22} class="text-violet-500" />
			{:else}
				<Icon size={26} color={typeConfig.color} />
			{/if}
			<span class="text-[9px] font-semibold tracking-wide uppercase text-center leading-tight opacity-70 max-w-full truncate" style="color: {typeConfig.color};">
				{isInclude ? includeName : typeConfig.label}
			</span>
			<!-- Editable node label (double-click to rename), all nodes. Defaults to
			     the SAME label the builder view shows when none is set. -->
			{#if editingLabel}
				<input
					type="text"
					class="w-full text-[11px] font-medium text-center bg-zinc-100 text-zinc-900 px-1 py-0.5 rounded border border-zinc-200 outline-none focus:border-zinc-400 nodrag nopan"
					bind:value={labelInput}
					onblur={saveLabel}
					onkeydown={handleLabelKeydown}
					onclick={(e) => e.stopPropagation()}
				/>
			{:else}
				<!-- svelte-ignore a11y_no_static_element_interactions -->
				<p class="text-[11px] font-medium text-zinc-700 text-center leading-tight cursor-text hover:bg-black/5 px-1 rounded max-w-full truncate nodrag nopan" ondblclick={startEditLabel} title="Double-click to rename">{data.label || `${typeConfig.label} Node`}</p>
			{/if}
		</div>
		{#if hasLiveDisplay}
			<!-- svelte-ignore a11y_click_events_have_key_events a11y_no_static_element_interactions -->
			<div class="mt-1 pt-2 border-t border-black/5 nodrag nopan nowheel overflow-auto" style="max-height: 280px;" onclick={(e) => e.stopPropagation()}>
				{@render liveDisplay()}
			</div>
		{/if}
	</div>
	{#if simplifiedOutConnected}
		<Handle
			type="source"
			position={Position.Right}
			id={SIMPLIFIED_OUT_HANDLE}
			style="top: 50%; z-index: 5; {simplifiedDotStyle(typeConfig.color)}"
		/>
	{/if}
{:else}

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
	class="project-node rounded min-w-[200px] select-none transition-all duration-200 {displayedStatus === 'running' ? 'node-running-glow' : ''} {displayedStatus === 'waiting_for_input' ? 'node-waiting-glow' : ''} {displayedStatus === 'failed' ? 'node-failed-glow' : displayedStatus === 'completed' ? 'node-completed-glow' : ''} {selected ? 'node-selected' : ''} {data.runTarget ? 'node-run-target' : ''}"
	style="
		--run-target-color: {typeConfig.color};
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
			{#if displayedStatus}
				<span class="text-base leading-none {displayedStatus === 'running' ? 'animate-pulse' : ''}" style="color: {getStatusBadgeColor(displayedStatus) ?? typeConfig.color};">{getStatusIcon(displayedStatus)}</span>
			{/if}
			{#if isInclude}
				<FileSymlink size={12} class="text-violet-500" />
				<span class="text-[11px] font-semibold tracking-wide uppercase text-violet-600">{includeName}</span>
			{:else}
				<span class="text-[11px] font-semibold tracking-wide uppercase" style="color: {typeConfig.color};">{typeConfig.label}</span>
			{/if}
			{#if data.infraNodeStatus}
				<span
					class="inline-flex items-center gap-1 px-1.5 py-0.5 rounded-full text-[9px] font-medium leading-none
					{data.infraNodeStatus === 'running' ? 'bg-green-100 text-green-700' : ''}
					{data.infraNodeStatus === 'flaky' ? 'bg-amber-100 text-amber-700' : ''}
					{data.infraNodeStatus === 'failed' ? 'bg-rose-100 text-rose-700' : ''}
					{data.infraNodeStatus === 'stopped' ? 'bg-zinc-100 text-zinc-600' : ''}
					{data.infraNodeStatus === 'provisioning' || data.infraNodeStatus === 'stopping' || data.infraNodeStatus === 'terminating' ? 'bg-sky-100 text-sky-700' : ''}
					"
					title={data.infraFailureMessage
						? `${data.infraFailureStage ? data.infraFailureStage + ': ' : ''}${data.infraFailureMessage}`
						: undefined}
				>
					<span class="w-1.5 h-1.5 rounded-full
						{data.infraNodeStatus === 'running' ? 'bg-green-500' : ''}
						{data.infraNodeStatus === 'flaky' ? 'bg-amber-500' : ''}
						{data.infraNodeStatus === 'failed' ? 'bg-rose-500' : ''}
						{data.infraNodeStatus === 'stopped' ? 'bg-zinc-400' : ''}
						{data.infraNodeStatus === 'provisioning' || data.infraNodeStatus === 'stopping' || data.infraNodeStatus === 'terminating' ? 'bg-sky-500 animate-pulse' : ''}
					"></span>
					{data.infraNodeStatus}
				</span>
			{/if}
		</div>
		<div class="flex items-center gap-0.5">
			<ExecutionInspector {executions} {busLogs} {journalCorruptions} label={data.label || typeConfig.label} />
		{#if isInclude}
			<button
				class="px-1.5 h-5 flex items-center gap-1 rounded hover:bg-violet-100 cursor-pointer transition-colors text-violet-600 text-[10px] font-medium nodrag nopan"
				onclick={(e) => { e.stopPropagation(); if (data.includePath) data.onOpenInclude?.(data.includePath, id); }}
				title={`Open ${data.includePath} (edit its graph)`}
			>
				<FileSymlink size={11} /> Open
			</button>
		{/if}
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
		{:else if isInclude}
			<button
				class="text-sm font-medium text-violet-700 hover:underline cursor-pointer px-1 py-0.5 rounded -mx-1 truncate text-left font-mono nodrag nopan flex items-center gap-1"
				onclick={(e) => { e.stopPropagation(); if (data.includePath) data.onOpenInclude?.(data.includePath, id); }}
				title={`Open ${data.includePath}`}
			>
				<FileSymlink size={12} /> {data.includePath}
			</button>
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
			<!-- Input Ports (wireable inputs only; config-exposure inputs
			     live in the body as fields, never on the edge rail) -->
			<div class="space-y-1 min-w-0 flex-1">
				{#each wireableInputs as input}
					{@const pMarker = portMarkerStyle(input, oneOfRequiredPorts, literalFilledPorts, getPortColor(input.portType), 'input')}
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
				{@const oMarker = portMarkerStyle(output, oneOfRequiredPorts, literalFilledPorts, getPortColor(output.portType), 'output')}
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

		<!-- Live Data Items - always visible regardless of expanded state.
		     One render branch per item.type. The action button (if any) is
		     shared via the top-level {@render actionBtn} snippet so it doesn't
		     drift across kinds (and is reused by the simplified-view card). -->
		{#if showBodyFeed}
			<div class="mt-2 pt-2 border-t live-data-container">
				{@render bodyFeedDisplay()}
			</div>
		{/if}

		<!-- Live permission check: a picked connection VERIFIED to miss
		     a permission this node requires. Replaces the deleted
		     compile-time diagnostic; the resolve-time backstop remains. -->
		{#each permissionShortfalls as shortfall}
			<div class="mt-1.5 text-[10px] text-red-500 bg-red-50 rounded px-2 py-1">{shortfall}</div>
		{/each}

		<!-- Live own-account check: this node's capability creates
		     things inside the credential's account, so the shared
		     connection can never serve it. -->
		{#each ownAccountShortfalls as shortfall}
			<div class="mt-1.5 text-[10px] text-red-500 bg-red-50 rounded px-2 py-1">
				{shortfall.text}
				{#if shortfall.link}
					<a
						href={shortfall.link}
						target="_blank"
						rel="noreferrer"
						class="underline">Set-up guide</a
					>
				{/if}
			</div>
		{/each}

		<!-- Live value check: a picked connection missing a value this
		     node needs (a mailbox with no receiving server wired into a
		     mail trigger). Always marks; a stored value is knowable. -->
		{#each valueShortfalls as shortfall}
			<div class="mt-1.5 text-[10px] text-red-500 bg-red-50 rounded px-2 py-1">{shortfall}</div>
		{/each}

		<!-- The live checks went dark: the grant fetch failed, so the
		     absence of a shortfall banner proves nothing. Muted, not
		     alarming; the resolve-time backstop still holds. -->
		{#if grantCheckError}
			<div class="mt-1.5 text-[10px] text-muted-foreground bg-muted rounded px-2 py-1">
				Could not check this node's connection permissions: {grantCheckError}
			</div>
		{/if}

		<!-- Expanded Config Fields -->
		{#if expanded}
			<div class="mt-3 pt-3 border-t space-y-2 overflow-auto min-h-0 flex-1">
				<!-- Primitive fields (text / textarea / select / multiselect /
				     checkbox / number / password) render through the shared
				     FieldStrip, including file-backed ones (displayValueOf
				     supplies the resolved content, readonlyKeys locks unready
				     fields, headerBadge shows the path chip). The exotic kinds
				     (code / entry_list) are claimed via
				     customFieldKeys and drawn inline by the renderCustom
				     snippet below, in the same authored order. -->
				<FieldStrip
					fields={displayedFields}
					config={(data.config as Record<string, unknown>) ?? {}}
					portValues={portLiterals}
					idPrefix={id}
					onUpdate={(key, value, portDriven) => updateFieldValue(key, value, portDriven)}
					{customFieldKeys}
					heights={textareaHeights}
					onHeightChange={handleTextareaResize}
					displayValueOf={fileDisplayOverride}
					readonlyKeys={readonlyFieldKeys}
					{headerBadge}
					{renderCustom}
					onReadonlyEdit={explainReadonlyField}
					onClear={(key) => updatePortLiteral(key, null)}
				/>

				{#snippet headerBadge(field: FieldDefinition)}
					{#if field.portDriven}
						{@const locked = portFieldLocked(field.key)}
						{@const form = portFieldForm(field.key)}
						{@const hasValue = ownValue(portLiterals, field.key) !== undefined && ownValue(portLiterals, field.key) !== null}
						<!-- The form-toggle marker: which SOURCE FORM this port's
						     value is written in. Braces `{ }` vs statement `=`;
						     a wired-only port is locked to the statement form.
						     Its presence is also what distinguishes a port-driven
						     field from a plain config field at a glance. -->
						<button
							type="button"
							class="text-[9px] font-mono px-1 py-0.5 rounded nodrag transition-colors
								{locked ? 'bg-muted text-muted-foreground/60 cursor-default' : 'bg-muted text-muted-foreground hover:bg-accent'}"
							title={locked
								? `Wired-only port: only the statement form (${id}.${field.key} = ...) can set it.`
								: form === 'inline'
									? `Written inside the node body ({ ${field.key}: ... }). Click to move it to a statement line (${id}.${field.key} = ...).`
									: `Written as a statement (${id}.${field.key} = ...). Click to move it into the node body.`}
							aria-label={`Toggle source form for ${field.key}`}
							disabled={!hasValue}
							onclick={(e) => { e.stopPropagation(); if (hasValue) togglePortValueForm(field.key); }}
						><span aria-hidden="true">{form === 'inline' ? '{ }' : '='}</span></button>
						<!-- The clear (×) button for checkbox/select port fields
						     is FieldStrip's own, via onClear. -->
					{:else if fileRefOf(field.key)}
						{@const ref = fileRefOf(field.key)}
						<!-- The chip doubles as the marker toggle: @file (editable,
						     edits save to the file) <-> @asset (pull-only). Only
						     text-backed refs reach here (fileRefOf is null for
						     file-typed assets, which the file-drop field owns). -->
						<button
							type="button"
							class="text-[9px] font-mono px-1 py-0.5 rounded nodrag transition-colors
								{ref?.marker === 'asset' ? 'bg-amber-100 text-amber-800 hover:bg-amber-200' : 'bg-muted text-muted-foreground hover:bg-accent'}"
							title={ref?.marker === 'asset'
								? `@asset(${ref?.path}): pull-only, not editable here. Click to switch to @file (editable).`
								: `@file(${ref?.path}): edits save to this file. Click to switch to @asset (pull-only).`}
							aria-label={ref?.marker === 'asset'
								? `Switch ${ref?.path} to editable @file`
								: `Switch ${ref?.path} to pull-only @asset`}
							onclick={(e) => { e.stopPropagation(); switchFileMarker(field.key); }}
						><span aria-hidden="true">{ref?.marker === 'asset' ? '🔒' : '📄'}</span> {ref?.path}</button>
					{/if}
				{/snippet}

				{#snippet renderCustom(field: FieldDefinition)}
					<div class="space-y-1">
						<div class="flex items-center justify-between">
							<label for={`${id}-field-${field.key}`} class="text-[10px] text-muted-foreground font-medium">{field.label}</label>
							{@render headerBadge(field)}
						</div>
						{#if field.type === "code"}
							<!-- Code editor field - any node can use this by setting field.type = 'code' -->
							<div class="nodrag nopan" onclick={(e) => e.stopPropagation()}
							onfocusin={(e) => e.currentTarget.classList.add('nowheel')}
							onfocusout={(e) => e.currentTarget.classList.remove('nowheel')}
							onkeydown={(e) => {
								if (fileFieldReadonly(field.key) && (e.key.length === 1 || e.key === 'Backspace' || e.key === 'Delete' || e.key === 'Enter')) {
									explainReadonlyField(field.key);
								}
							}}
							onpaste={() => { if (fileFieldReadonly(field.key)) explainReadonlyField(field.key); }}
							ondrop={() => { if (fileFieldReadonly(field.key)) explainReadonlyField(field.key); }}
						>
								<CodeEditor
									value={fieldDisplayValue(field)}
									liveValue={!!fileFieldState(field.key)}
									readonly={fileFieldReadonly(field.key)}
									placeholder={field.placeholder}
									language={field.language}
									minHeight="120px"
									onchange={(newValue) => {
										// Direct, not via fieldEditor: CodeEditor has no blur to clear
										// the field editor's active key, which would strand the field
										// on its local value and mask external (file -> graph) updates.
										// An emptied editor means UNSET (null), same contract as the
										// strip's text boxes; a file-backed write turns it back into
										// an empty file. A file-backed field routes to the
										// (serialized) file write; otherwise the value goes to the
										// home the field's exposure routes to (port literal vs
										// config), same as every control.
										const value = emptyToUnset(newValue);
										updateFieldValue(field.key, value, field.portDriven);
									}}
								/>
							</div>
						{:else if field.type === "access"}
							<!-- The connect control on a personal ACCESS NODE. The
							     config value is only the small {id, identity} handle;
							     pasted secrets go editor -> store through the host's
							     access channel and never touch node config. -->
							{#if typeConfig.service}
								<AccessField
									spec={typeConfig.service}
									projectApp={typeConfig.accessApps?.[typeConfig.service.service]}
									nodeType={data.nodeType}
									value={declaredValue(field) as { id: string; identity?: string } | undefined}
									onUpdate={(v) => updateFieldValue(field.key, v, field.portDriven)}
								/>
							{:else}
								<div class="text-[10px] text-red-500">
									This node's metadata declares no `service` recipe; the access widget has nothing to connect.
								</div>
							{/if}
						{:else if field.type === "remote_select"}
							<!-- Pick a resource on the connected service. The lookup
							     runs through the dispatcher on the stored access,
							     found by tracing this node's access wire structurally
							     (no worker, no data flow at edit time). -->
							<RemoteSelectField
								{field}
								value={declaredValue(field) as string | undefined}
								accessRef={field.access ? traceAccessRef(field.access) : null}
								accessIsOwnField={accessInputIsOwnWidget(field.access)}
								grantedScopes={field.access ? (tracedGrants[field.access]?.scopes ?? null) : null}
								parents={remoteSelectParents(field)}
								onUpdate={(v) => updateFieldValue(field.key, v, field.portDriven)}
							/>
						{:else if field.type === "file_drop"}
							<!-- Writes an `@asset("<path-or-url>", <Type>)` ref into
							     config; the pre-build asset sync publishes the file and the
							     compile substitutes the stored-file value the runtime folds
							     onto the same-named input port (or the node reads from
							     config). Source never holds storage keys. -->
							<FileDropField
								value={declaredValue(field)}
								accept={field.accept}
								fileType={field.fileType}
								multiple={field.multiple ?? false}
								onUpdate={(ref) => updateFieldValue(field.key, ref, field.portDriven)}
							/>
						{:else if field.type === "entry_list"}
							<!-- The list a node's ports come from: one row per
							     entry, and an add row that asks whatever the
							     chosen kind declares it needs. Nothing here
							     knows any kind by name. The pen opens that same
							     form over the row it belongs to, filled in, and
							     several rows can be open at once. -->
							<div class="nodrag nopan space-y-1.5" onclick={(e) => e.stopPropagation()}>
								{#each getEntries() as entry, i}
									{@const spec = entryKindByName[entry.kind]}
									{@const rowKey = spec ? entryPortName(entry, spec) : ''}
									<!-- An entry whose kind the catalog no longer knows has
									     no form to open, so it stays a plain row you can
									     only remove. -->
									{#if rowKey && editDrafts[rowKey]}
										{@render entryForm(
											editDrafts[rowKey],
											editKeyErrors[rowKey] ?? false,
											`${id}-edit-${rowKey}`,
											'Edit',
											(draft) => setEditDraft(rowKey, draft),
											() => cancelEditingEntry(rowKey),
											() => saveEditingEntry(rowKey),
										)}
									{:else}
										<div class="group flex items-center gap-2 bg-muted rounded pl-2 pr-1 py-1 text-xs">
											<span class="flex-1 font-mono truncate">{rowKey}</span>
											<span class="shrink-0 text-[10px] text-muted-foreground truncate" title={entry.kind}>{spec?.label ?? entry.kind}</span>
											<div class="shrink-0 flex items-center gap-0.5 opacity-0 group-hover:opacity-100 transition-opacity">
												{#if spec && rowKey}
													<button
														class="w-4 h-4 grid place-items-center rounded text-muted-foreground hover:text-foreground hover:bg-background transition"
														onclick={(e) => { e.stopPropagation(); startEditingEntry(entry, rowKey); }}
														title="Edit"
														aria-label="Edit {rowKey}"
													>
														<Pencil size={10} />
													</button>
												{/if}
												<button
													class="w-4 h-4 grid place-items-center rounded text-muted-foreground hover:text-destructive hover:bg-background transition"
													onclick={(e) => { e.stopPropagation(); removeEntry(i); }}
													title="Remove"
													aria-label="Remove {rowKey}"
												>&times;</button>
											</div>
										</div>
									{/if}
								{/each}
								{#if addingEntry}
									{@render entryForm(
										newEntry,
										newEntryKeyError,
										`${id}-new-entry`,
										'Add',
										(draft) => { newEntry = draft; newEntryKeyError = false; },
										() => { addingEntry = false; newEntryKeyError = false; },
										() => addEntry(),
									)}
								{:else}
									<button
										class="w-full text-[10px] py-1 rounded border border-dashed border-border text-muted-foreground hover:text-foreground hover:bg-muted transition-colors"
										onclick={(e) => { e.stopPropagation(); startAddingEntry(); }}
									>+ Add</button>
								{/if}
							</div>
						{:else}
							<!-- Every customFieldKeys entry must have a branch above.
							     Reaching this means the claim set and the renderer
							     drifted; surface loud rather than silently rendering
							     a text input. -->
							<div class="text-[10px] px-1.5 py-1 rounded bg-destructive/10 text-destructive">
								ProjectNode: custom field type "{field.type}" has no renderer (key "{field.key}").
							</div>
						{/if}
					</div>
				{/snippet}

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
				{:else if displayedStatus === 'cancelled'}
					<div class="debug-placeholder completed" style="color: #71717a;">
						<span>■</span>
						<span>{latestExecution?.error || 'Cancelled by user'}</span>
					</div>
				{:else if displayedStatus === 'running' || displayedStatus === 'waiting_for_input'}
					<div class="debug-placeholder running">
						<span class="debug-spinner"></span>
						<span>{displayedStatus === 'waiting_for_input' ? 'Suspended' : 'Processing...'}</span>
					</div>
				{:else}
					<div class="debug-placeholder waiting">
						<span>📥</span>
						<span>Waiting for data...</span>
					</div>
				{/if}
			{/if}

			<!-- The declared file display (`features.display`): inline
			     media (MediaDisplay, the generators) or a download-link
			     card (DownloadLink), showing the declared port of the
			     latest firing. Key-backed files fetch through the
			     authenticated download handshake; url-backed ones
			     render/link their URL directly. -->
			{#if typeConfig.display}
				{#if displayedFileValue}
					<FilePreview
						file={displayedFileValue}
						mode={typeConfig.display?.kind === 'media' ? 'media' : 'link'}
					/>
				{:else if displayedStatus === 'completed'}
					<div class="debug-placeholder completed">
						<span>✓</span>
						<span>No file received</span>
					</div>
				{:else if displayedStatus === 'running' || displayedStatus === 'waiting_for_input'}
					<div class="debug-placeholder running">
						<span class="debug-spinner"></span>
						<span>Processing...</span>
					</div>
				{:else}
					<div class="debug-placeholder waiting">
						<span>Waiting for a file...</span>
					</div>
				{/if}
			{/if}

			</div>
		{/if}
	</div>
</div>

{@render flowDock()}

{/if}

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
	:global(.node-waiting-glow) {
		box-shadow: 0 1px 3px rgba(0, 0, 0, 0.08), 0 4px 12px rgba(0, 0, 0, 0.05), 0 0 0 2px rgba(6, 182, 212, 0.45) !important;
	}
	:global(.node-completed-glow) {
		box-shadow: 0 1px 3px rgba(0, 0, 0, 0.08), 0 4px 12px rgba(0, 0, 0, 0.05), 0 0 0 2px rgba(16, 185, 129, 0.3) !important;
	}
	:global(.node-failed-glow) {
		box-shadow: 0 1px 3px rgba(0, 0, 0, 0.08), 0 4px 12px rgba(0, 0, 0, 0.05), 0 0 0 2px rgba(239, 68, 68, 0.4) !important;
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

	/* An output node the user aimed the run at. A ring in the node's own
	   colour that breathes, so a targeted node is obvious on a busy canvas
	   without adding anything permanent to every output node. `!important`
	   because the inline box-shadow on the card would otherwise win. */
	:global(.node-run-target) {
		box-shadow:
			0 1px 3px rgba(0, 0, 0, 0.08),
			0 4px 12px rgba(0, 0, 0, 0.05),
			0 0 0 3px var(--run-target-color) !important;
		animation: run-target-breathe 2.4s ease-in-out infinite;
	}

	@keyframes run-target-breathe {
		0%, 100% {
			box-shadow:
				0 1px 3px rgba(0, 0, 0, 0.08),
				0 4px 12px rgba(0, 0, 0, 0.05),
				0 0 0 3px var(--run-target-color),
				0 0 6px 1px var(--run-target-color);
		}
		50% {
			box-shadow:
				0 1px 3px rgba(0, 0, 0, 0.08),
				0 4px 12px rgba(0, 0, 0, 0.05),
				0 0 0 3px var(--run-target-color),
				0 0 18px 5px var(--run-target-color);
		}
	}

	/* Somebody who set reduce-motion still gets the ring, without the pulse. */
	@media (prefers-reduced-motion: reduce) {
		:global(.node-run-target) {
			animation: none;
		}
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
