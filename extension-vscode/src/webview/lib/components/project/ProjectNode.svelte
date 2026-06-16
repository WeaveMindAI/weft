<script lang="ts">
	import { Handle, Position, useEdges, NodeResizer, type ResizeParams } from "@xyflow/svelte";
	import { NODE_TYPE_CONFIG, type NodeType } from "$lib/nodes";
	import type { PortDefinition, PortType, NodeDataUpdates, FieldDefinition, NodeFeatures, NodeExecution, LiveDataItem, NodeExecutionStatus } from "$lib/types";
	import { parseWeftType } from "$lib/types";
	import { PORT_TYPE_COLORS, getPortTypeColor } from "$lib/constants/colors";
	import type { Edge } from "@xyflow/svelte";
	import CodeEditor from "$lib/components/CodeEditor.svelte";
	import { toast } from "svelte-sonner";
	import CopyButton from "$lib/components/ui/CopyButton.svelte";
	import { buildSpecMap, deriveInputsFromFields, deriveOutputsFromFields, type FormFieldDef, type FormFieldSpec } from '$lib/utils/form-field-specs';
	import { getStatusBadgeColor, getStatusIcon } from "$lib/utils/status";
	import type { FileContent, BusInspectorEvent, BusMeta, CorruptionSite, NodeFeedState } from "../../../../shared/protocol";
	import { BadgeQuestionMark, Eye, EyeOff, Maximize2, Minimize2, FileSymlink } from '@lucide/svelte';
	import { createFieldEditor } from '$lib/utils/field-editor.svelte';
	import { useFieldEditorRegistry } from './field-editor-registry';
	import { isFileRefValue } from '$lib/value-format';
	import { createPortContextMenu, buildPortMenuItems } from "$lib/utils/port-context-menu";
	import { portMarkerStyle } from "$lib/utils/port-marker";
	import ExecutionInspector from './ExecutionInspector.svelte';
	import { SIMPLIFIED_IN_HANDLE, SIMPLIFIED_OUT_HANDLE, SIMPLIFIED_CONTENT_W_PX, SIMPLIFIED_SQUARE_PAD_PX, SIMPLIFIED_CARD_MAX_W_PX, simplifiedDotStyle } from "$lib/constants/simplified-view";
	import FieldStrip from './FieldStrip.svelte';
	import StoredFilePreview from './StoredFilePreview.svelte';
	import type { StoredFileWire } from "../../../../shared/protocol";
	import { parseStoredFile } from "../../../../shared/protocol";

	const edgesState = useEdges();

	let { data, id, selected }: {
		data: {
			label: string | null;
			nodeType: NodeType;
			/// Simplified view: render as a fixed square (icon + type label,
			/// one in/out dot), no ports/config/body. Execution overlays
			/// (status glyph, inspector, glow) are kept.
			simplified?: boolean;
			config: Record<string, unknown>;
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
	const displayedFields: FieldDefinition[] = $derived.by(() => {
		const catalogFields = typeConfig.fields ?? [];
		const result: FieldDefinition[] = [...catalogFields];
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

	// Stored-file preview (ImageDisplay / DownloadLink): these sink
	// nodes take a File value on an INPUT port and emit nothing, so
	// the preview reads the latest execution's input, not its output.
	// Returns the first stored-file value found (a concrete
	// `__weft_<kind>__` marker carrying a logical `key`; url/data file
	// values have no key and are skipped).
	const storedInputFile = $derived.by<StoredFileWire | null>(() => {
		const input = latestExecution?.input;
		if (typeof input !== 'object' || input === null) return null;
		for (const value of Object.values(input as Record<string, unknown>)) {
			const file = parseStoredFile(value);
			if (file) return file;
		}
		return null;
	});

	// Check if node has expandable content (fields, run location option, debug preview, etc.)
	const hasExpandableContent = $derived.by(() => {
		// Has config fields (catalog-declared or synthesized from config-filled ports)
		if (displayedFields.length > 0) return true;
		// Has debug preview (Debug node)
		if (typeConfig.features?.showDebugPreview) return true;
		// Has a stored-file preview (ImageDisplay / DownloadLink)
		if (typeConfig.features?.showImagePreview || typeConfig.features?.showDownloadLink) return true;
		// Has setup guide
		if (typeConfig.setupGuide && typeConfig.setupGuide.length > 0) return true;
		return false;
	});

	// The THREE live-display parts, each a single predicate that is the ONE source
	// of truth for "is this part present". Both the `hasLiveDisplay` boolean (which
	// decides square-vs-card) and the `liveDisplay` renderer gate on these exact
	// flags, so a part can never render without growing the card, or be counted
	// without rendering. Add a new live-display kind = add a flag here and a branch
	// in `liveDisplay`, and `hasLiveDisplay` picks it up for free.
	const showBodyFeed = $derived(!!data.bodyFeed && (data.bodyFeed.state === 'error' || data.bodyFeed.items.length > 0));
	const showDebugDisplay = $derived(!!(typeConfig.features?.showDebugPreview && debugDataJson));
	const showFileDisplay = $derived(!!((typeConfig.features?.showImagePreview || typeConfig.features?.showDownloadLink) && storedInputFile));
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
	let showSetupGuide = $state(false);
	let editingLabel = $state(false);
	// svelte-ignore state_referenced_locally
	let labelInput = $state(data.label || '');
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

	/** If `config[key]` is a `@file` marker, its {path, type}. The single
	 *  per-field test for "is this field file-backed". */
	function fileRefOf(key: string): { path: string; type: string } | null {
		const v = (data.config as Record<string, unknown>)?.[key];
		return isFileRefValue(v) ? v.__weftFileRef : null;
	}

	/** Resolved state of a file-backed field, from the host's fileContents
	 *  map. `loading` = content not yet delivered (brief, transient).
	 *  `error` = the file couldn't be read (fail loudly, no fallback). */
	function fileFieldState(key: string): { path: string; content?: string; error?: string; loading: boolean } | null {
		const ref = fileRefOf(key);
		if (!ref) return null;
		const entry = data.fileContents?.[ref.path];
		if (entry === undefined) return { path: ref.path, loading: true };
		if ('error' in entry) return { path: ref.path, error: entry.error, loading: false };
		return { path: ref.path, content: entry.content, loading: false };
	}

	/** A file-backed field whose content isn't loaded yet (loading or error)
	 *  is read-only: its display is a status string, not editable content.
	 *  False for a normal field or a loaded file-backed field. The single
	 *  editability rule applied across every editable field branch. */
	function fileFieldUnready(key: string): boolean {
		const fs = fileFieldState(key);
		return fs ? fs.content === undefined : false;
	}

	/// Field keys this node renders itself rather than delegating to the
	/// shared FieldStrip primitive renderer: only the exotic types (code,
	/// api_key, form_builder). File-backed primitives render through
	/// FieldStrip via its displayValueOf / readonlyKeys / headerBadge
	/// capabilities.
	const EXOTIC_FIELD_TYPES = new Set(['code', 'api_key', 'form_builder']);
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
	/// saved as content.
	const readonlyFieldKeys = $derived.by(() => {
		const keys = new Set<string>();
		for (const field of displayedFields) {
			if (fileFieldUnready(field.key)) keys.add(field.key);
		}
		return keys;
	});

	function updateConfig(key: string, value: string | string[] | number | boolean | FormFieldDef[] | null) {
		// File-backed field: the edit goes to the referenced file, never to the
		// weft source. The `@file(...)` marker in config (and source) is left
		// untouched; only the file's content changes.
		const fs = fileFieldState(key);
		if (fs) {
			// Only a loaded field is editable. Editing while loading or on a
			// read error must not write (it would clobber the file with a
			// status string); the field is read-only in those states, but
			// guard loudly here too.
			if (fs.content === undefined) {
				toast.error(`Cannot edit ${fs.path}: ${fs.error ?? 'still loading'}`);
				return;
			}
			const content = typeof value === 'string' ? value : JSON.stringify(value, null, 2);
			data.onSaveFileRef?.(fs.path, content);
			return;
		}
		if (data.onUpdate) {
			// config holds the `@file(...)` tag for file-backed fields (never the
			// resolved content), so serializing the whole config re-emits the
			// marker. No special handling needed for sibling file-backed fields.
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
	const fieldEditorRegistry = useFieldEditorRegistry();
	// Register this node's field-editor flush so a teardown (window hidden,
	// panel close) that doesn't fire a field `blur` still commits the last
	// <700ms of typing. $effect's cleanup unregisters on destroy.
	$effect(() => fieldEditorRegistry?.register(fieldEditor.flush));

	function getConfigDisplayValue(fieldKey: string): string {
		const fs = fileFieldState(fieldKey);
		if (fs) {
			// File-backed: show resolved content (editable). While loading or on
			// a read error, show a status (read-only); never the marker as a
			// value, never a silent fall back to inline content.
			if (fs.content !== undefined) return fieldEditor.display(fieldKey, fs.content);
			if (fs.error !== undefined) return `cannot read ${fs.path}: ${fs.error}`;
			return `loading ${fs.path}...`;
		}
		const v = (data.config as Record<string, unknown>)?.[fieldKey];
		const storeStr = (v === undefined || v === null) ? '' : (typeof v === 'string' ? v : JSON.stringify(v, null, 2));
		return fieldEditor.display(fieldKey, storeStr);
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
	{#if showFileDisplay && storedInputFile}
		<StoredFilePreview file={storedInputFile} mode={typeConfig.features?.showImagePreview ? 'image' : 'link'} />
	{/if}
{/snippet}

{#if data.simplified}
	<!-- Simplified view. A bare node is a square: icon, type, editable label, one
	     in/out dot. A node with LIVE DISPLAY content (infra feed, debug preview,
	     image) grows into a card that shows that display under the header instead
	     of staying square. Execution overlays (status glyph, inspector, glow) are
	     kept; ports/config are not shown; structure is not editable, but the
	     LABEL can still be renamed by double-click. -->
	{@const Icon = typeConfig.icon}
	<Handle
		type="target"
		position={Position.Left}
		id={SIMPLIFIED_IN_HANDLE}
		style="top: 50%; z-index: 5; {simplifiedDotStyle(typeConfig.color)}"
	/>
	<!-- svelte-ignore a11y_no_static_element_interactions -->
	<div
		bind:this={nodeElement}
		class="project-node simplified-node rounded-lg select-none transition-all duration-200 {displayedStatus === 'running' ? 'node-running-glow' : ''} {displayedStatus === 'waiting_for_input' ? 'node-waiting-glow' : ''} {displayedStatus === 'failed' ? 'node-failed-glow' : displayedStatus === 'completed' ? 'node-completed-glow' : ''} {selected ? 'node-selected' : ''}"
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
	<Handle
		type="source"
		position={Position.Right}
		id={SIMPLIFIED_OUT_HANDLE}
		style="top: 50%; z-index: 5; {simplifiedDotStyle(typeConfig.color)}"
	/>
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
	class="project-node rounded min-w-[200px] select-none transition-all duration-200 {displayedStatus === 'running' ? 'node-running-glow' : ''} {displayedStatus === 'waiting_for_input' ? 'node-waiting-glow' : ''} {displayedStatus === 'failed' ? 'node-failed-glow' : displayedStatus === 'completed' ? 'node-completed-glow' : ''} {selected ? 'node-selected' : ''}"
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

		<!-- Live Data Items - always visible regardless of expanded state.
		     One render branch per item.type. The action button (if any) is
		     shared via the top-level {@render actionBtn} snippet so it doesn't
		     drift across kinds (and is reused by the simplified-view card). -->
		{#if showBodyFeed}
			<div class="mt-2 pt-2 border-t live-data-container">
				{@render bodyFeedDisplay()}
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

				<!-- Primitive fields (text / textarea / select / multiselect /
				     checkbox / number / password) render through the shared
				     FieldStrip, including file-backed ones (displayValueOf
				     supplies the resolved content, readonlyKeys locks unready
				     fields, headerBadge shows the path chip). The exotic kinds
				     (code / api_key / form_builder) are claimed via
				     customFieldKeys and drawn inline by the renderCustom
				     snippet below, in the same authored order. -->
				<FieldStrip
					fields={displayedFields}
					config={(data.config as Record<string, unknown>) ?? {}}
					idPrefix={id}
					onUpdate={(key, value) => updateConfig(key, value as string | string[] | number | boolean | FormFieldDef[] | null)}
					{customFieldKeys}
					heights={textareaHeights}
					onHeightChange={handleTextareaResize}
					displayValueOf={fileDisplayOverride}
					readonlyKeys={readonlyFieldKeys}
					{headerBadge}
					{renderCustom}
				/>

				{#snippet headerBadge(field: FieldDefinition)}
					{#if fileRefOf(field.key)}
						{@const ref = fileRefOf(field.key)}
						<span
							class="text-[9px] text-muted-foreground font-mono px-1 py-0.5 rounded bg-muted"
							title={`Loaded from ${ref?.path} (edits save to this file)`}
						>📄 {ref?.path}</span>
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
						>
								<CodeEditor
									value={getConfigDisplayValue(field.key)}
									readonly={fileFieldUnready(field.key)}
									placeholder={field.placeholder}
									minHeight="120px"
									onchange={(newValue) => {
										// Direct, not via fieldEditor: CodeEditor has no blur to clear
										// the field editor's active key, which would strand the field
										// on its local value and mask external (file -> graph) updates.
										// updateConfig routes a file-backed field to the (serialized)
										// file write, so per-change writes are safe, just not debounced.
										updateConfig(field.key, newValue);
									}}
								/>
							</div>
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
						{:else if field.type === "form_builder"}
							<div class="nodrag nopan space-y-1.5" onclick={(e) => e.stopPropagation()}>
								{#each getFormFields() as f, i}
									<div class="flex items-center gap-1.5 bg-zinc-50 border border-zinc-200 rounded px-2 py-1 text-[10px]">
										<span class="text-zinc-400 font-mono shrink-0 truncate" title={f.fieldType}>{nodeFormSpecMap[f.fieldType]?.label ?? f.fieldType}</span>
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

			<!-- Stored-file preview: inline image (ImageDisplay) or a
			     download-link card (DownloadLink). Reads the latest
			     execution's INPUT (these nodes emit nothing). Both fetch
			     through the authenticated download handshake. -->
			{#if typeConfig.features?.showImagePreview || typeConfig.features?.showDownloadLink}
				{#if storedInputFile}
					<StoredFilePreview
						file={storedInputFile}
						mode={typeConfig.features?.showImagePreview ? 'image' : 'link'}
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
