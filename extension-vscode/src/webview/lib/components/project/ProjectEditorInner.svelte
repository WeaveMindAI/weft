<script lang="ts">
	import { SvelteFlow, Controls, Background, useSvelteFlow, useUpdateNodeInternals, type Node, type Edge, type Connection, SelectionMode, ConnectionLineType, MarkerType } from "@xyflow/svelte";
	import { untrack, tick } from "svelte";
	import "@xyflow/svelte/dist/style.css";
	import { browser } from "$app/environment";
	import ProjectNode from "./ProjectNode.svelte";
	import GroupNode from "./GroupNode.svelte";
	import AnnotationNode from "./AnnotationNode.svelte";
	import CommandPalette from "./CommandPalette.svelte";
	import CustomEdge from "./CustomEdge.svelte";
	import ActionBar from "./ActionBar.svelte";
	import { NODE_TYPE_CONFIG, type NodeType } from "$lib/nodes";
	import type { ProjectDefinition, PortDefinition, NodeFeatures } from "$lib/types";
	import { getApiUrl } from "$lib/config";
	import { PORT_TYPE_COLORS, getPortTypeColor } from "$lib/constants/colors";
	import { autoOrganize, parseWeft, type OpaqueBlock, type WeftParseError, type WeftWarning } from "$lib/ai/weft-parser";
	import { updateNodeConfig as weftUpdateConfig, updateNodeLabel as weftUpdateLabel, addNode as weftAddNode, addGroup as weftAddGroup, removeNode as weftRemoveNode, removeGroup as weftRemoveGroup, addEdge as weftAddEdge, removeEdge as weftRemoveEdge, updateNodePorts as weftUpdatePorts, updateGroupPorts as weftUpdateGroupPorts, updateProjectMeta as weftUpdateMeta, moveNodeScope as weftMoveNodeScope, moveGroupScope as weftMoveGroupScope, renameGroup as weftRenameGroup, updateLayoutEntry, removeLayoutEntry, parseLayoutCode, renameLayoutPrefix } from "$lib/ai/weft-editor";
	import { findSearchMatch } from "$lib/ai/weft-patch";
	import { extractInfraSubgraph } from "$lib/utils/infra-subgraph";
	import { extractTriggerSubgraph } from "$lib/utils/trigger-subgraph";
	import { toast } from "svelte-sonner";


	// Undo/Redo history
	// History stores snapshots. historyIndex points to current state.
	// Undo: go back one, Redo: go forward one
	// saveToHistory() should be called AFTER making changes to save the new state
	type HistoryState = { nodes: Node[]; edges: Edge[]; weftCode: string };
	const MAX_HISTORY = 50;
	let history = $state<HistoryState[]>([]);
	let historyIndex = $state(-1);
	let isUndoRedo = false;
	let lastPushTime = 0;
	const DEBOUNCE_MS = 100; // Prevent multiple pushes within 100ms

	let { project, onSave, onRun, onStop, onCancelBuild, executionState, triggerState, onToggleTrigger, onResyncTrigger, infraState, onCheckInfraStatus, onStartInfra, onStopInfra, onTerminateInfra, onForceRetry, validationErrors, autoOrganizeOnMount = false, fitViewAfterOrganize = false, infraLiveData, structuralLock = false }: {
		project: ProjectDefinition;
		onSave: (data: { name?: string; description?: string; weftCode?: string; loomCode?: string; layoutCode?: string }) => void;
		onRun?: () => void;
		onStop?: () => void;
		onCancelBuild?: () => void;
		executionState?: {
			isRunning: boolean;
			activeEdges: Set<string>;
			nodeOutputs: Record<string, unknown>;
			nodeStatuses: Record<string, string>;
			nodeExecutions: import('$lib/types').NodeExecutionTable;
		};
		triggerState?: {
			hasTriggers: boolean;
			hasTriggersInFrontend: boolean;
			hasTriggersInBackend: boolean;
			isActive: boolean;
			isLoading: boolean;
			hasError?: boolean;
			isStale?: boolean;
		};
		onToggleTrigger?: () => void;
		onResyncTrigger?: () => void;
		infraState?: {
			hasInfrastructure: boolean;
			hasInfraInFrontend: boolean;
			hasInfraInBackend: boolean;
			infraDiverged: boolean;
			status: string;
			nodes: Array<{ nodeId: string; nodeType: string; instanceId: string; status: string; backend?: string }>;
			isLoading: boolean;
		};
		onCheckInfraStatus?: () => void;
		onStartInfra?: () => void;
		onStopInfra?: () => void;
		onTerminateInfra?: () => void;
		onForceRetry?: () => void;
		validationErrors?: Map<string, import('$lib/types').ValidationError[]>;
		autoOrganizeOnMount?: boolean;
		fitViewAfterOrganize?: boolean;
		infraLiveData?: Record<string, import('$lib/types').LiveDataItem[]>;
		structuralLock?: boolean;
	} = $props();

	// VS Code embedding: dashboard chrome (right sidebar, code
	// panel, mobile notice, export dialog) is removed. The text
	// editor is the .weft tab in column 2; the activity-bar
	// Inspector handles node inspection.
	// Local editor state, intentionally captures initial value, not reactive to prop.
	// The editor owns these after init; saves flow outward via onSave.
	let weftCode = $state(untrack(() => project.weftCode) ?? '');
	let layoutCode = $state(untrack(() => project.layoutCode) ?? '');
	let weftOpaqueBlocks = $state<OpaqueBlock[]>([]);
	let weftParseErrors = $state<WeftParseError[]>([]);
	let saveStatus = $state<'idle' | 'saved'>('idle');
	let saveStatusTimer: ReturnType<typeof setTimeout> | null = null;

	/** Get the scoped layout key for a node. Both node IDs and group IDs are already
	 *  scoped by the parser (e.g., "GroupName.nodeId", "Outer.Inner"). */
	function getLayoutKey(node: Node): string {
		return node.id;
	}

	/** Update layout for a node, modifies layoutCode, NOT weftCode. */
	function layoutUpdateAny(node: Node) {
		const cfg = node.data.config as Record<string, unknown> | undefined;
		const key = getLayoutKey(node);
		layoutCode = updateLayoutEntry(layoutCode, key,
			node.position.x, node.position.y,
			cfg?.width as number | undefined, cfg?.height as number | undefined,
			cfg?.expanded as boolean | undefined ?? undefined);
	}

	/** Move a node or group to a different scope in weftCode. targetGroupLabel is the label of the target group, or undefined for top level. */
	/**
	 * Move a node or group to a different scope in weftCode and update layoutCode.
	 * targetGroupId is the scoped group ID (e.g., "Outer.Inner"), or undefined for top level.
	 * targetGroupLabel is the group's label as used by the weft text editor.
	 */
	function weftMoveScopeAny(node: Node, targetGroupLabel: string | undefined, targetGroupId?: string) {
		const oldKey = getLayoutKey(node);
		const isGroup = node.type === 'group' || node.type === 'groupCollapsed';
		if (isGroup && node.data.label) {
			weftCode = weftMoveGroupScope(weftCode, node.data.label as string, targetGroupLabel);
		} else {
			weftCode = weftMoveNodeScope(weftCode, node.id, targetGroupLabel);
		}
		// Update layoutCode: the scoped ID changes when moving between scopes.
		const localId = isGroup ? (node.data.label as string) : node.id.split('.').pop()!;
		const scopePrefix = targetGroupId || targetGroupLabel;
		const newKey = scopePrefix ? `${scopePrefix}.${localId}` : localId;
		if (oldKey !== newKey) {
			if (isGroup) {
				// Group + all children need renaming
				layoutCode = renameLayoutPrefix(layoutCode, oldKey, newKey);
			} else {
				const layoutMap = parseLayoutCode(layoutCode);
				const entry = layoutMap[oldKey];
				layoutCode = removeLayoutEntry(layoutCode, oldKey);
				if (entry) {
					layoutCode = updateLayoutEntry(layoutCode, newKey, entry.x, entry.y, entry.w, entry.h, entry.expanded);
				}
			}
		}
	}

	/**
	 * Convert xyflow edge endpoints to weft-local connection syntax.
	 * Returns { srcRef, srcPort, tgtRef, tgtPort, scopeGroupLabel } where refs are
	 * local to the scope (e.g. "self", "debug_1") and scopeGroupLabel is the group
	 * that should contain the connection line.
	 */
	function toWeftEdgeRef(
		srcId: string, srcHandle: string,
		tgtId: string, tgtHandle: string,
	): { srcRef: string; srcPort: string; tgtRef: string; tgtPort: string; scopeGroupLabel: string | undefined } {
		const srcPort = (srcHandle || 'value').replace(/__inner$/, '');
		const tgtPort = (tgtHandle || 'value').replace(/__inner$/, '');
		const isInnerSrc = srcHandle?.endsWith('__inner') ?? false;
		const isInnerTgt = tgtHandle?.endsWith('__inner') ?? false;

		const srcNode = nodes.find(n => n.id === srcId);
		const tgtNode = nodes.find(n => n.id === tgtId);
		const srcIsGroup = srcNode?.type === 'group' || srcNode?.type === 'groupCollapsed';
		const tgtIsGroup = tgtNode?.type === 'group' || tgtNode?.type === 'groupCollapsed';

		// Determine the scope: the parentId of regular nodes, or the group itself for inner ports
		const srcParent = (srcNode?.data.config as Record<string, string>)?.parentId;
		const tgtParent = (tgtNode?.data.config as Record<string, string>)?.parentId;

		// If source is a group with inner handle, the connection is inside that group
		// self.port syntax is used for group interface connections
		if (srcIsGroup && isInnerSrc) {
			const groupLabel = srcNode!.data.label as string;
			const localTgt = getLocalId(tgtId, srcId);
			return { srcRef: 'self', srcPort, tgtRef: localTgt, tgtPort, scopeGroupLabel: groupLabel };
		}

		// If target is a group with inner handle, the connection is inside that group
		if (tgtIsGroup && isInnerTgt) {
			const groupLabel = tgtNode!.data.label as string;
			const localSrc = getLocalId(srcId, tgtId);
			return { srcRef: localSrc, srcPort, tgtRef: 'self', tgtPort, scopeGroupLabel: groupLabel };
		}

		// Both are regular nodes, find common scope
		if (srcParent && srcParent === tgtParent) {
			const parentNode = nodes.find(n => n.id === srcParent);
			const parentLabel = parentNode?.data.label as string | undefined;
			const localSrc = getLocalId(srcId, srcParent);
			const localTgt = getLocalId(tgtId, srcParent);
			return { srcRef: localSrc, srcPort, tgtRef: localTgt, tgtPort, scopeGroupLabel: parentLabel };
		}

		// Top-level connection
		return { srcRef: srcId, srcPort, tgtRef: tgtId, tgtPort, scopeGroupLabel: undefined };
	}

	/** Strip scope prefix from an xyflow node ID to get the local name within a group scope. */
	function getLocalId(nodeId: string, scopeId: string): string {
		const prefix = scopeId + '.';
		if (nodeId.startsWith(prefix)) return nodeId.slice(prefix.length);
		return nodeId;
	}

	function generateNodeId(nodeType: string): string {
		const snake = nodeType.replace(/([a-z0-9])([A-Z])/g, '$1_$2').toLowerCase();
		const existingIds = new Set(nodes.map(n => n.id));
		let i = 1;
		while (existingIds.has(`${snake}_${i}`)) i++;
		return `${snake}_${i}`;
	}

	function stripWeftFences(code: string): string {
		return code
			.replace(/^````weft(?:-patch)?\s*\n/, '')
			.replace(/\n````\s*$/, '');
	}

	function parseWeftCode(rawCode: string): ReturnType<typeof parseWeft> {
		const result = parseWeft('````weft\n' + rawCode + '\n````');
		// Apply positions from layoutCode to parsed nodes. The
		// parser cache holds shared `n.position` / `n.config`
		// references; if we mutated them in place, two callers
		// reading the cache with different `layoutCode` values
		// would cross-contaminate. Clone the project + per-node
		// position/config so each caller sees its own layout.
		if (result.projects.length > 0) {
			const layoutMap = parseLayoutCode(layoutCode);
			result.projects = result.projects.map(w => ({
				...w,
				project: {
					...w.project,
					nodes: w.project.nodes.map(n => {
						const entry = layoutMap[n.id];
						if (!entry) {
							// Even unmatched nodes get fresh position
							// + config so callers can mutate freely.
							return { ...n, position: { ...n.position }, config: { ...(n.config as Record<string, unknown>) } };
						}
						const config = { ...(n.config as Record<string, unknown>) };
						if (entry.w !== undefined) config.width = entry.w;
						if (entry.h !== undefined) config.height = entry.h;
						if (entry.expanded !== undefined) config.expanded = entry.expanded;
						return {
							...n,
							position: { x: entry.x, y: entry.y },
							config,
						};
					}),
				},
			}));
		}
		return result;
	}

	function applyParseResult(w: { opaqueBlocks: OpaqueBlock[]; errors: WeftParseError[] }) {
		weftOpaqueBlocks = w.opaqueBlocks;
		weftParseErrors = w.errors;
	}

	function clearWeftParseState() {
		weftOpaqueBlocks = [];
		weftParseErrors = [];
	}

	let weftStreaming = $state(false);
	let weftSyncDirection: 'none' | 'to-code' | 'to-editor' = 'none';
	let weftSyncTimer: ReturnType<typeof setTimeout> | null = null;
	let codeEditInFlight = false;
	const WEFT_SYNC_DEBOUNCE_MS = 500;
	let editingName = $state(false);
	let editingNameValue = $state('');
	let saveProjectTimer: ReturnType<typeof setTimeout> | null = null;
	const SAVE_DEBOUNCE_MS = 1000;

	function focusOnMount(el: HTMLElement) {
		el.focus();
		if (el instanceof HTMLInputElement) {
			el.select();
		}
	}

	let weftInitialized = false;
	function initWeftCode() {
		if (weftInitialized) return;
		weftInitialized = true;
		// On first load, the weftCode comes from the project prop (already persisted).
		// If it's empty, we leave it empty, new projects start with no code.
		if (project.weftCode) {
			weftCode = project.weftCode;
			const result = parseWeftCode(weftCode);
			if (result.projects.length > 0) {
				applyParseResult(result.projects[0]);
				// Sync name/description from weft code if the DB has a generic name
				const parsed = result.projects[0].project;
				if (parsed.name && parsed.name !== 'Untitled Project' && project.name === 'Untitled project') {
					project.name = parsed.name;
				}
				if (parsed.description !== undefined && !project.description) {
					project.description = parsed.description;
				}
			}
		}
	}

	function handleWeftCodeChange(newCode: string) {
		if (weftSyncDirection === 'to-code') return;
		if (weftSyncTimer) clearTimeout(weftSyncTimer);
		weftCode = newCode;
		codeEditInFlight = true;
		weftSyncTimer = setTimeout(async () => {
			weftSyncDirection = 'to-editor';
			const result = parseWeftCode(weftCode);
			if (result.projects.length > 0) {
				const w = result.projects[0];
				applyParseResult(w);
				if (w.project.name) project.name = w.project.name;
				if (w.project.description !== undefined) project.description = w.project.description;
				const savedOpaqueBlocks = [...weftOpaqueBlocks];
				await patchFromProject(w.project);
				weftOpaqueBlocks = savedOpaqueBlocks;
				saveProject();
			} else {
				weftParseErrors = result.errors;
			}
			// Wait for Svelte's reactive cycle to settle before clearing the guard
			await tick();
			weftSyncDirection = 'none';
			codeEditInFlight = false;
		}, WEFT_SYNC_DEBOUNCE_MS);
	}

	// Sort/reorder removed, code ordering is user-controlled now

	const nodeTypes = { 
		project: ProjectNode,
		group: GroupNode,
		groupCollapsed: GroupNode,
		annotation: AnnotationNode,
	};
	
	const edgeTypes = {
		custom: CustomEdge,
	};


	function getEdgeColor(sourceNodeId: string, sourceHandle: string | null | undefined): string {
		const sourceNode = nodes.find(n => n.id === sourceNodeId);
		if (!sourceNode) return PORT_TYPE_COLORS.Any;
		
		const outputs = sourceNode.data.outputs as Array<{ name: string; portType: string }> | undefined;
		if (!outputs) return PORT_TYPE_COLORS.Any;
		
		const cleanHandle = sourceHandle?.endsWith('__inner') ? sourceHandle.slice(0, -7) : sourceHandle;
		const port = outputs.find(p => p.name === cleanHandle);
		return port ? (PORT_TYPE_COLORS[port.portType] || PORT_TYPE_COLORS.Any) : PORT_TYPE_COLORS.Any;
	}

	const defaultEdgeOptions = $derived({
		type: 'custom',
		animated: false,
		// NOTE: Do NOT set markerEnd here - it overrides individual edge settings
	});

	const isEdgeActive = (edgeId: string) => executionState?.activeEdges?.has(edgeId) ?? false;

	// Get the SvelteFlow instance for screenToFlowPosition and fitView
	const { screenToFlowPosition, fitView, getViewport, setViewport, zoomIn, zoomOut } = useSvelteFlow();
	const updateNodeInternals = useUpdateNodeInternals();

	// Track whether Ctrl is actually pressed on the keyboard (vs synthetic from pinch)
	let realCtrlDown = false;
	$effect(() => {
		const onKeyDown = (e: KeyboardEvent) => { if (e.key === 'Control' || e.key === 'Meta') realCtrlDown = true; };
		const onKeyUp = (e: KeyboardEvent) => { if (e.key === 'Control' || e.key === 'Meta') realCtrlDown = false; };
		window.addEventListener('keydown', onKeyDown);
		window.addEventListener('keyup', onKeyUp);
		return () => { window.removeEventListener('keydown', onKeyDown); window.removeEventListener('keyup', onKeyUp); };
	});

	// Pinch-to-zoom: browser sends synthetic Ctrl+wheel. Real Ctrl is NOT held.
	// Mouse wheel zoom: real Ctrl IS held. Use different sensitivity for each.
	function handleWheel(e: WheelEvent) {
		// Skip events we redispatched ourselves (see delta-mode normalization below).
		if ((e as WheelEvent & { __weftNormalized?: boolean }).__weftNormalized) return;
		if (e.ctrlKey || e.metaKey) {
			e.preventDefault();
			e.stopPropagation();
			const viewport = getViewport();
			// Pinch: amplify aggressively. Mouse wheel: gentle.
			const multiplier = realCtrlDown ? 0.002 : 0.03;
			const zoomDelta = -e.deltaY * multiplier;
			let newZoom = viewport.zoom * (1 + zoomDelta);
			newZoom = Math.max(0.05, Math.min(2, newZoom));
			const rect = (e.currentTarget as HTMLElement).getBoundingClientRect();
			const mouseX = e.clientX - rect.left;
			const mouseY = e.clientY - rect.top;
			const newX = mouseX - (mouseX - viewport.x) * (newZoom / viewport.zoom);
			const newY = mouseY - (mouseY - viewport.y) * (newZoom / viewport.zoom);
			setViewport({ x: newX, y: newY, zoom: newZoom }, { duration: 0 });
			return;
		}
		// Normalize line/page delta modes to pixels. After a page reload or when
		// the window loses focus, some browsers emit wheel events with
		// deltaMode = DOM_DELTA_LINE (1) or DOM_DELTA_PAGE (2) and large integer
		// deltas. xyflow's panOnScroll reads deltaX/Y as pixels, so those raw
		// values cause extremely fast panning. We intercept in capture phase,
		// stop the original, and redispatch a pixel-mode WheelEvent clone so
		// xyflow sees sensible values.
		if (e.deltaMode !== 0) {
			e.preventDefault();
			e.stopPropagation();
			const LINE_HEIGHT = 16;
			const PAGE_HEIGHT = 800;
			const scale = e.deltaMode === 1 ? LINE_HEIGHT : PAGE_HEIGHT;
			const normalized = new WheelEvent('wheel', {
				bubbles: true,
				cancelable: true,
				composed: true,
				view: e.view,
				detail: e.detail,
				screenX: e.screenX,
				screenY: e.screenY,
				clientX: e.clientX,
				clientY: e.clientY,
				ctrlKey: e.ctrlKey,
				shiftKey: e.shiftKey,
				altKey: e.altKey,
				metaKey: e.metaKey,
				button: e.button,
				buttons: e.buttons,
				relatedTarget: e.relatedTarget,
				deltaX: e.deltaX * scale,
				deltaY: e.deltaY * scale,
				deltaZ: e.deltaZ * scale,
				deltaMode: 0,
			});
			(normalized as WheelEvent & { __weftNormalized?: boolean }).__weftNormalized = true;
			(e.target as EventTarget).dispatchEvent(normalized);
		}
	}

	let configEditTimer: ReturnType<typeof setTimeout> | null = null;

	/** Parse width/height from an xyflow node's style string + measured fallback */
	function getNodeRect(n: Node): { width: number; height: number } {
		// Try explicit style first
		const wMatch = n.style?.match(/width:\s*(\d+)px/);
		const hMatch = n.style?.match(/height:\s*(\d+)px/);
		const w = wMatch ? parseInt(wMatch[1]) : (n.measured?.width ?? 200);
		const h = hMatch && !n.style?.includes('height: auto') ? parseInt(hMatch[1]) : (n.measured?.height ?? 60);
		return { width: w, height: h };
	}


	function createNodeUpdateHandler(nodeId: string) {
		return (updates: { label?: string | null; config?: Record<string, unknown>; inputs?: PortDefinition[]; outputs?: PortDefinition[] }) => {
			if ('config' in updates && 'expanded' in (updates.config || {})) {
				console.debug(`[nodeUpdateHandler] node=${nodeId} expanded=${updates.config!.expanded} incomingConfigKeys=${Object.keys(updates.config || {}).join(',')}`);
			}
			// Detect if this is an expand/collapse toggle
			const isExpandToggle = updates.config && 'expanded' in updates.config;

			// Capture old group label BEFORE updating (for rename in weft code)
			const oldGroupLabel = ('label' in updates) ? nodes.find(n => n.id === nodeId)?.data.label as string | undefined : undefined;

			// Capture old dimensions BEFORE updating, for anchor-point fix and neighbor shift
			let oldWidth = 0;
			let oldHeight = 0;
			let oldPosition = { x: 0, y: 0 };
			if (isExpandToggle) {
				const current = nodes.find(n => n.id === nodeId);
				if (current) {
					const rect = getNodeRect(current);
					oldWidth = rect.width;
					oldHeight = rect.height;
					oldPosition = getAbsolutePosition(current);
				}
			}

			nodes = nodes.map(n => {
				if (n.id !== nodeId) {
					return n;
				}

				const newData = { ...n.data };
				if ('label' in updates) newData.label = updates.label;
				if ('config' in updates) newData.config = updates.config;
				if ('inputs' in updates) newData.inputs = updates.inputs;
				if ('outputs' in updates) newData.outputs = updates.outputs;

				const newConfig = newData.config as Record<string, unknown> | undefined;
				const configWidth = newConfig?.width as number | undefined;
				const configHeight = newConfig?.height as number | undefined;
				// Annotations always use fixed dimensions
				if (n.type === 'annotation') {
					const w = configWidth || 250;
					const h = configHeight || 120;
					return { ...n, data: newData, style: `width: ${w}px; height: ${h}px;` };
				}

				// Groups: expanded = group type with container dimensions, collapsed = groupCollapsed type like regular node
				if (n.type === 'group' || n.type === 'groupCollapsed') {
					const groupExpanded = (newConfig?.expanded as boolean) ?? true;
					if (groupExpanded) {
						// Preserve current dimensions if config doesn't specify (avoids flash during streaming)
						const currentRect = getNodeRect(n);
						const w = configWidth || currentRect.width || 400;
						const h = configHeight || currentRect.height || 300;
						return { ...n, type: 'group', data: newData, zIndex: -1, style: `width: ${w}px; height: ${h}px;` };
					} else {
						const nodeInputs = (newData as Record<string, unknown>).inputs as PortDefinition[] | undefined;
						const nodeOutputs = (newData as Record<string, unknown>).outputs as PortDefinition[] | undefined;
						const minW = computeMinNodeWidth(nodeInputs, nodeOutputs);
						return { ...n, type: 'groupCollapsed', data: newData, zIndex: 4, style: `width: ${minW}px; height: auto;`, width: undefined, height: undefined };
					}
				}

				// Regular project nodes: use expanded state (default collapsed)
				const isExpanded = (newConfig?.expanded as boolean) ?? false;
				const nodeInputs = ((newData as Record<string, unknown>).inputs || (newConfig as Record<string, unknown> | undefined)?.inputs) as PortDefinition[] | undefined;
				const nodeOutputs = ((newData as Record<string, unknown>).outputs || (newConfig as Record<string, unknown> | undefined)?.outputs) as PortDefinition[] | undefined;
				const minW = computeMinNodeWidth(nodeInputs, nodeOutputs);

				if (!isExpanded) {
					return { 
						...n, 
						data: newData, 
						style: `width: ${minW}px; height: auto;`,
						width: undefined,
						height: undefined,
					};
				} else if (configWidth && configHeight) {
					const w = Math.max(configWidth, minW);
					return { 
						...n, 
						data: newData, 
						style: `width: ${w}px; height: ${configHeight}px;`,
						width: w,
						height: configHeight,
					};
				}
				const w = Math.max(320, minW);
				return { ...n, data: newData, style: `width: ${w}px; height: auto;`, width: undefined, height: undefined };
			});

			// Recompute visibility for all nodes and edges based on ancestor chain
			if (isExpandToggle) {
				// Build a lookup: nodeId -> config (for checking expanded state)
				const nodeById = new Map(nodes.map(n => [n.id, n]));

				// Check if any ancestor of a node is collapsed
				function isHiddenByAncestor(n: Node): boolean {
					let pid = (n.data.config as Record<string, string>)?.parentId;
					while (pid) {
						const parent = nodeById.get(pid);
						if (!parent) break;
						const parentExpanded = (parent.data.config as Record<string, boolean>)?.expanded ?? true;
						if (!parentExpanded) return true;
						pid = (parent.data.config as Record<string, string>)?.parentId;
					}
					return false;
				}

				const hiddenNodeIds = new Set<string>();
				nodes = nodes.map(n => {
					const rawParentId = (n.data.config as Record<string, string>)?.parentId;
					if (!rawParentId) return n;
					const hidden = isHiddenByAncestor(n);
					if (hidden) hiddenNodeIds.add(n.id);
					// Check if the direct parent is expanded (for xyflow parentId assignment)
					const directParent = nodeById.get(rawParentId);
					const directParentExpanded = directParent ? ((directParent.data.config as Record<string, boolean>)?.expanded ?? true) : false;
					const xyParentId = directParentExpanded && !hidden ? rawParentId : undefined;
					if (hidden) {
						return { ...n, parentId: undefined, style: 'display: none;' };
					} else {
						// Restore proper style based on node type
						const cfg = n.data.config as Record<string, unknown> | undefined;
						const cw = cfg?.width as number | undefined;
						const ch = cfg?.height as number | undefined;
						let style: string | undefined;
						if (n.type === 'group' || n.type === 'groupCollapsed') {
							const expanded = (cfg?.expanded as boolean) ?? true;
							style = expanded
								? `width: ${cw || 400}px; height: ${ch || 300}px;`
								: `width: ${computeMinNodeWidth(n.data.inputs as PortDefinition[], n.data.outputs as PortDefinition[])}px; height: auto;`;
						} else if (n.type === 'annotation') {
							style = `width: ${cw || 250}px; height: ${ch || 120}px;`;
						} else {
							const expanded = (cfg?.expanded as boolean) ?? false;
							const minW = computeMinNodeWidth(n.data.inputs as PortDefinition[], n.data.outputs as PortDefinition[]);
							if (!expanded) {
								style = `width: ${minW}px; height: auto;`;
							} else if (cw && ch) {
								style = `width: ${Math.max(cw, minW)}px; height: ${ch}px;`;
							} else {
								style = `width: ${Math.max(320, minW)}px; height: auto;`;
							}
						}
						return { ...n, parentId: xyParentId, ...(style ? { style } : {}) };
					}
				});

				// Hide/show edges touching hidden nodes
				edges = edges.map(e => {
					const touchesHidden = hiddenNodeIds.has(e.source) || hiddenNodeIds.has(e.target);
					if (touchesHidden) return { ...e, hidden: true };
					if (e.hidden) return { ...e, hidden: false };
					return e;
				});
			}

			// Expand/collapse: run ELK, then adjust viewport so the toggled node's
			// top-right corner stays at the same screen position (no node shifting)
			// Skip during streaming, the regular streaming auto-organize handles layout
			if (isExpandToggle && !weftStreaming) {
				const pinnedNodeId = nodeId;
				// Capture the top-right corner in flow coordinates before toggle
				const currentNode = nodes.find(n => n.id === nodeId);
				const absPos = currentNode ? getAbsolutePosition(currentNode) : oldPosition;
				const oldAbsTopRight = { x: absPos.x + oldWidth, y: absPos.y };
				// Convert to screen coordinates
				const vp = getViewport();
				const oldScreenX = oldAbsTopRight.x * vp.zoom + vp.x;
				const oldScreenY = oldAbsTopRight.y * vp.zoom + vp.y;

				tick().then(() => {
					requestAnimationFrame(() => {
						requestAnimationFrame(() => {
							runAutoOrganize(false).then(() => {
								// Compute new top-right corner in flow coordinates
								const postNode = nodes.find(n => n.id === pinnedNodeId);
								if (postNode) {
									const postAbs = getAbsolutePosition(postNode);
									const postRect = getNodeRect(postNode);
									const newAbsTopRight = { x: postAbs.x + postRect.width, y: postAbs.y };
									// Adjust viewport so the top-right stays at the same screen position
									const currentVp = getViewport();
									const newVpX = oldScreenX - newAbsTopRight.x * currentVp.zoom;
									const newVpY = oldScreenY - newAbsTopRight.y * currentVp.zoom;
									if (Math.abs(newVpX - currentVp.x) > 1 || Math.abs(newVpY - currentVp.y) > 1) {
										setViewport({ x: newVpX, y: newVpY, zoom: currentVp.zoom });
									}
								}

								// Re-hide edges touching hidden nodes (runAutoOrganize unhides all edges)
								const currentHidden = new Set(nodes.filter(n => n.style === 'display: none;').map(n => n.id));
								if (currentHidden.size > 0) {
									edges = edges.map(e => {
										const touchesHidden = currentHidden.has(e.source) || currentHidden.has(e.target);
										if (touchesHidden) return { ...e, hidden: true };
										if (e.hidden) return { ...e, hidden: false };
										return e;
									});
								}
								for (const n of nodes) {
									layoutUpdateAny(n);
								}
								saveToHistory();
								saveProject();
							});
						});
					});
				});
			}
			
			// When ports change, tell xyflow to re-scan handle bounds so new handles are connectable
			if ('inputs' in updates || 'outputs' in updates) {
				tick().then(() => updateNodeInternals(nodeId));
			}

			// Update weftCode surgically based on what changed
			if ('label' in updates) {
				const node = nodes.find(n => n.id === nodeId);
				const isGroup = node?.type === 'group' || node?.type === 'groupCollapsed';
				if (isGroup && oldGroupLabel && updates.label) {
					weftCode = weftRenameGroup(weftCode, oldGroupLabel, updates.label as string);
					// Rename layout entries: the old scoped ID (node.id) and all children
					// e.g., nodeId="Outer.Inner", oldGroupLabel="Inner", newLabel="Processing"
					// → old scoped ID is "Outer.Inner", new is "Outer.Processing"
					const parts = nodeId.split('.');
					parts[parts.length - 1] = updates.label as string;
					const newScopedId = parts.join('.');
					layoutCode = renameLayoutPrefix(layoutCode, nodeId, newScopedId);
				} else {
					weftCode = weftUpdateLabel(weftCode, nodeId, updates.label ?? null);
				}
			}
			if ('config' in updates) {
				const cfg = updates.config!;
				let layoutUpdated = false;
				for (const [key, value] of Object.entries(cfg)) {
					if (['parentId', 'textareaHeights', '_opaqueChildren'].includes(key)) continue;
					if (['width', 'height', 'expanded'].includes(key)) {
						// Layout-related: update @layout directive (once for all layout keys)
						if (!layoutUpdated) {
							const n = nodes.find(nd => nd.id === nodeId);
							if (n) {
								layoutUpdateAny({ ...n, data: { ...n.data, config: cfg } });
							}
							layoutUpdated = true;
						}
						continue;
					}
					weftCode = weftUpdateConfig(weftCode, nodeId, key, value);
				}
			}
			if ('inputs' in updates || 'outputs' in updates) {
				const node = nodes.find(n => n.id === nodeId);
				if (node?.data) {
					const inputs = (updates.inputs ?? node.data.inputs) as Array<{ name: string; required?: boolean; laneMode?: string; portType?: string }>;
					const outputs = (updates.outputs ?? node.data.outputs) as Array<{ name: string; laneMode?: string; portType?: string }>;
					const isGroup = node.type === 'group' || node.type === 'groupCollapsed';
					if (isGroup && node.data.label) {
						weftCode = weftUpdateGroupPorts(weftCode, node.data.label as string, inputs, outputs);
					} else {
						weftCode = weftUpdatePorts(weftCode, nodeId, inputs, outputs);
					}
				}
			}

			// Check if this is a resize operation (width/height in config)
			const isResize = updates.config && ('width' in updates.config || 'height' in updates.config);

			if ('config' in updates && !isResize) {
				// For text config changes (typing), debounce the history save
				if (configEditTimer) clearTimeout(configEditTimer);
				configEditTimer = setTimeout(() => {
					saveToHistory();
				}, 500);
				// Debounce the API save for typing (5s)
				if (saveProjectTimer) clearTimeout(saveProjectTimer);
				saveProjectTimer = setTimeout(() => {
					saveProjectTimer = null;
					saveProject();
				}, SAVE_DEBOUNCE_MS);
			} else {
				// For resize, ports, label - save immediately
				saveToHistory();
				saveProject();
			}
		};
	}

	function computeMinNodeWidth(inputs?: PortDefinition[], outputs?: PortDefinition[]): number {
		const MIN_WIDTH = 200;
		const CHAR_WIDTH = 6.5; // approximate px per char at text-[10px], slightly generous
		const PADDING = 60; // handles (12*2) + gaps + px padding
		const GAP = 20; // minimum gap between input and output labels

		const inputNames = (inputs || []).map(p => p.name + (p.required ? '*' : ''));
		const outputNames = (outputs || []).map(p => p.name);

		let maxRowWidth = 0;
		const rowCount = Math.max(inputNames.length, outputNames.length);
		for (let i = 0; i < rowCount; i++) {
			const leftLen = i < inputNames.length ? inputNames[i].length : 0;
			const rightLen = i < outputNames.length ? outputNames[i].length : 0;
			const rowWidth = (leftLen + rightLen) * CHAR_WIDTH + GAP;
			if (rowWidth > maxRowWidth) maxRowWidth = rowWidth;
		}

		return Math.max(MIN_WIDTH, Math.ceil(maxRowWidth + PADDING));
	}

	// Node types that have their own SvelteFlow components (not in NODE_TYPE_CONFIG)
	const SPECIAL_NODE_TYPES = new Set(['Group', 'Annotation']);

	function buildNodes(projectNodes: typeof project.nodes, projectEdges: typeof project.edges, layoutMap?: Record<string, { x: number; y: number; w?: number; h?: number; expanded?: boolean }>): Node[] {
		// Unknown node types are already handled by the parser as opaque blocks
		// (they never reach project.nodes), so we only need to filter for known types.
		const validNodes = projectNodes.filter(n =>
			SPECIAL_NODE_TYPES.has(n.nodeType) || NODE_TYPE_CONFIG[n.nodeType]
		);

		// xyflow requires parent nodes to appear before children in the array.
		// Topologically sort groups so parent groups come first, then non-group nodes.
		const groupNodes = validNodes.filter(n => n.nodeType === 'Group');
		const otherNodes = validNodes.filter(n => n.nodeType !== 'Group');
		const groupById = new Map(groupNodes.map(g => [g.id, g]));
		const sortedGroups: typeof groupNodes = [];
		const visited = new Set<string>();
		function visitGroup(g: typeof groupNodes[0]) {
			if (visited.has(g.id)) return;
			visited.add(g.id);
			const pid = (g.config as Record<string, string>)?.parentId;
			if (pid && groupById.has(pid)) {
				visitGroup(groupById.get(pid)!);
			}
			sortedGroups.push(g);
		}
		for (const g of groupNodes) visitGroup(g);
		const sortedNodes = [...sortedGroups, ...otherNodes];
		
		return sortedNodes.map((n) => {
			const isGroup = n.nodeType === 'Group';
			const isAnnotation = n.nodeType === 'Annotation';
			const rawParentId = (n.config as Record<string, string>)?.parentId;
			// Walk up the ancestor chain: hide if any ancestor is collapsed
			let hiddenByCollapsedGroup = false;
			let parentGroupExpanded = true;
			if (rawParentId) {
				const directParent = projectNodes.find(g => g.id === rawParentId);
				parentGroupExpanded = directParent ? ((directParent.config as Record<string, boolean>)?.expanded ?? true) : false;
				// Check full ancestor chain
				let pid: string | undefined = rawParentId;
				while (pid) {
					const ancestor = projectNodes.find(g => g.id === pid);
					if (!ancestor) break;
					if ((ancestor.config as Record<string, boolean>)?.expanded === false) {
						hiddenByCollapsedGroup = true;
						break;
					}
					pid = (ancestor.config as Record<string, string>)?.parentId;
				}
			}
			const parentId = (rawParentId && parentGroupExpanded && !hiddenByCollapsedGroup) ? rawParentId : undefined;

			// Build style for resizable nodes (groups, annotations, and project nodes)
			const configWidth = (n.config as Record<string, number>)?.width;
			const configHeight = (n.config as Record<string, number>)?.height;
			const isExpanded = (n.config as Record<string, boolean>)?.expanded ?? (isGroup ? true : false);
			
			const nodeType = isGroup ? (isExpanded ? 'group' : 'groupCollapsed') : isAnnotation ? 'annotation' : 'project';

			// Z-index order: expanded groups/annotations at bottom, nested groups above parents.
			// Compute nesting depth so child groups render above parent groups.
			let nestingDepth = 0;
			if (isGroup && isExpanded && rawParentId) {
				let pid: string | undefined = rawParentId;
				while (pid) {
					nestingDepth++;
					const p = projectNodes.find(g => g.id === pid);
					pid = p ? (p.config as Record<string, string>)?.parentId : undefined;
				}
			}
			const zIndex = isAnnotation ? -1 : isGroup ? (isExpanded ? -1 + nestingDepth : 4) : 4;
			let nodeStyle: string | undefined;
			
			if (isAnnotation) {
				nodeStyle = `width: ${configWidth || 250}px; height: ${configHeight || 120}px;`;
			} else if (isGroup) {
				if (isExpanded) {
					// Check layoutMap for saved dimensions, then config, then default
					const layoutEntry = layoutMap?.[n.id];
					const w = configWidth || layoutEntry?.w || 400;
					const h = configHeight || layoutEntry?.h || 300;
					nodeStyle = `width: ${w}px; height: ${h}px;`;
				} else {
					const minW = computeMinNodeWidth(n.inputs, n.outputs);
					nodeStyle = `width: ${minW}px; height: auto;`;
				}
			}
			
			// For regular project nodes: collapsed = fit content, expanded = saved dimensions or fit content
			let nodeWidth: number | undefined;
			let nodeHeight: number | undefined;
			if (!isGroup && !isAnnotation) {
				const minW = computeMinNodeWidth(n.inputs, n.outputs);
				if (!isExpanded) {
					nodeStyle = `width: ${minW}px; height: auto;`;
					nodeWidth = undefined;
					nodeHeight = undefined;
				} else if (configWidth && configHeight) {
					const w = Math.max(configWidth, minW);
					nodeStyle = `width: ${w}px; height: ${configHeight}px;`;
					nodeWidth = w;
					nodeHeight = configHeight;
				} else {
					// Expanded but no saved dimensions - fit to content
					const w = Math.max(320, minW);
					nodeStyle = `width: ${w}px; height: auto;`;
				}
			}
			
			return {
				id: n.id,
				type: nodeType,
				position: n.position,
				zIndex,
				...(nodeWidth !== undefined ? { width: nodeWidth } : {}),
				...(nodeHeight !== undefined ? { height: nodeHeight } : {}),
				data: {
					label: n.label,
					nodeType: n.nodeType,
					config: n.config,
					inputs: n.inputs,
					outputs: n.outputs,
					features: n.features,
					sourceLine: (n as typeof n & { sourceLine?: number }).sourceLine,
					onUpdate: createNodeUpdateHandler(n.id),
					infraNodeStatus: infraState?.nodes.find(inf => inf.nodeId === n.id)?.status,
				},
				...(hiddenByCollapsedGroup
					? { style: 'display: none;' }
					: nodeStyle ? { style: nodeStyle } : {}),
				parentId,
			};
		});
	}

	// svelte-ignore state_referenced_locally
	let nodes = $state.raw<Node[]>(buildNodes(project.nodes, project.edges, parseLayoutCode(layoutCode)));

	function buildEdges(projectEdges: typeof project.edges, projectNodes: typeof project.nodes): Edge[] {
		// Deduplicate edges - only keep one edge per target+targetHandle (last one wins)
		const seenTargets = new Map<string, typeof projectEdges[0]>();
		for (const e of projectEdges) {
			const key = `${e.target}:${e.targetHandle || 'default'}`;
			seenTargets.set(key, e);
		}
		const deduplicatedEdges = Array.from(seenTargets.values());
		
		
		return deduplicatedEdges.map((e) => {
			const sourceNode = projectNodes.find(n => n.id === e.source);
			const edgeColor = getEdgeColor(e.source, e.sourceHandle);

			const active = isEdgeActive(e.id);

			// Group interface port handles: __inner suffix is set by the parser for self-references
			// (in.port -> __inner source handle, out.port -> __inner target handle)
			const sourceHandle = e.sourceHandle;
			const targetHandle = e.targetHandle;

			return {
				id: e.id,
				source: e.source,
				target: e.target,
				sourceHandle,
				targetHandle,
				type: 'custom',
				animated: active,
				zIndex: 5,
				style: `stroke-width: ${active ? 3 : 2}px; stroke: ${edgeColor};`,
				markerEnd: {
					type: MarkerType.ArrowClosed,
					width: 20,
					height: 20,
					color: edgeColor,
				},
				className: active ? 'edge-active' : '',
			};
		});
	}

	// svelte-ignore state_referenced_locally
	let edges = $state.raw<Edge[]>(buildEdges(project.edges, project.nodes));

	$effect(() => {
		const state = executionState;
		if (state) {
			const activeEdges = state.activeEdges;
			const nodeOutputs = state.nodeOutputs || {};
			const nodeExecutions = state.nodeExecutions || {};

			untrack(() => {
				nodes = nodes.map(n => {
					const nodeType = n.data.nodeType as string;
					const nodeTypeConfig = NODE_TYPE_CONFIG[nodeType];
					// Debug-style preview: show the node's output if
					// it has one; otherwise (Debug, any future sink)
					// show what flowed IN, since that's what makes
					// the preview meaningful. Read the latest exec
					// row's input payload when no outputs exist on
					// the node type.
					let debugData: unknown = undefined;
					if (nodeTypeConfig?.features?.showDebugPreview) {
						const hasOutputs = (nodeTypeConfig.outputs?.length ?? 0) > 0;
						if (hasOutputs) {
							debugData = nodeOutputs[n.id];
						} else {
							const rows = nodeExecutions[n.id];
							const latest = rows?.[rows.length - 1];
							debugData = latest?.input;
						}
					}

					let executions: import('$lib/types').NodeExecution[];

					if (nodeType === 'Group') {
						const groupId = n.id;

						// Boundary passthrough executions (compiled IDs follow {groupId}__in / {groupId}__out)
						const inExecs = nodeExecutions[`${groupId}__in`] || [];
						const outExecs = nodeExecutions[`${groupId}__out`] || [];

						// Collect internal node executions via scope field
						const internalExecs: import('$lib/types').NodeExecution[] = [];
						for (const projNode of project.nodes) {
							if (projNode.scope?.includes(groupId) && nodeExecutions[projNode.id]) {
								internalExecs.push(...nodeExecutions[projNode.id]);
							}
						}

						// Build synthetic execution: one per __in execution
						executions = inExecs.map((inExec, i) => {
							const outExec = outExecs[i];
							// Derive status from all children + in/out
							const allRelated = [...internalExecs, ...inExecs, ...outExecs];
							const hasRunning = allRelated.some(e => e.status === 'running' || e.status === 'waiting_for_input');
							const hasFailed = allRelated.some(e => e.status === 'failed');
							const allTerminal = allRelated.length > 0 && allRelated.every(e =>
								e.status === 'completed' || e.status === 'skipped' || e.status === 'failed' || e.status === 'cancelled'
							);
							const status: import('$lib/types').NodeExecutionStatus = hasRunning ? 'running'
								: hasFailed ? 'failed'
								: allTerminal ? 'completed'
								: inExec.status;

							return {
								id: `${groupId}-synth-${i}`,
								nodeId: groupId,
								status,
								pulseIdsAbsorbed: inExec.pulseIdsAbsorbed,
								pulseId: inExec.pulseId,
								error: outExec?.error ?? inExec.error,
								startedAt: inExec.startedAt,
								completedAt: outExec?.completedAt ?? inExec.completedAt,
								input: inExec.output, // __in output = what flows into the group
								output: outExec?.output, // __out output = what the group produces
								costUsd: allRelated.reduce((sum, e) => sum + (e.costUsd || 0), 0),
								logs: [],
								color: inExec.color,
								lane: inExec.lane,
							};
						});
					} else {
						executions = nodeExecutions[n.id] || [];
					}

					// Derive SvelteFlow wrapper class from the latest execution status
					const latestExec = executions[executions.length - 1];
					const execStatus = latestExec?.status;
					const nodeClass = execStatus === 'running' || execStatus === 'waiting_for_input' ? 'node-running'
						: execStatus === 'failed' ? 'node-failed'
						: execStatus === 'completed' || execStatus === 'skipped' ? 'node-completed'
						: '';
					return {
						...n,
						data: {
							...n.data,
							debugData,
							executions,
							executionCount: executions.length,
						},
						class: nodeClass,
					};
				});
				
				edges = edges.map(e => ({
					...e,
					animated: activeEdges.has(e.id),
					style: activeEdges.has(e.id) 
						? e.style?.replace(/stroke-width: \d+px/, 'stroke-width: 3px') 
						: e.style?.replace(/stroke-width: \d+px/, 'stroke-width: 2px'),
					class: activeEdges.has(e.id) ? 'edge-active' : '',
				}));
			});
		}
	});

	// Keep per-node infra status badges in sync with backend state
	$effect(() => {
		const infraNodes = infraState?.nodes;
		if (!infraNodes) return;
		untrack(() => {
			nodes = nodes.map(n => {
				const backendNode = infraNodes.find(inf => inf.nodeId === n.id);
				const newStatus = backendNode?.status;
				if (n.data.infraNodeStatus !== newStatus) {
					return { ...n, data: { ...n.data, infraNodeStatus: newStatus } };
				}
				return n;
			});
		});
	});

	// Keep per-node live data in sync (infra live data + node display data)
	$effect(() => {
		const liveMap = infraLiveData;
		const isActive = triggerState?.isActive ?? false;
		const pid = project?.id ?? '';
		untrack(() => {
			const ctx = { projectId: pid, isProjectActive: isActive, apiBaseUrl: getApiUrl() };
			nodes = nodes.map(n => {
				const d = n.data as Record<string, unknown>;
				const infraItems = liveMap?.[n.id];
				// Compute display data from node template
				const nodeType = d.nodeType as string;
				const template = NODE_TYPE_CONFIG[nodeType as NodeType];
				const displayItems = template?.getDisplayData?.(
					{ id: n.id, nodeType, label: d.label as string | null, config: (d.config as Record<string, unknown>) ?? {}, position: n.position, inputs: (d.inputs as import('$lib/types').PortDefinition[]) ?? [], outputs: (d.outputs as import('$lib/types').PortDefinition[]) ?? [], features: d.features as NodeFeatures },
					ctx,
				);
				const items = [...(infraItems ?? []), ...(displayItems ?? [])];
				const prev = d.liveDataItems as import('$lib/types').LiveDataItem[] | undefined;
				if (items.length === 0 && (!prev || prev.length === 0)) return n;
				return { ...n, data: { ...n.data, liveDataItems: items.length > 0 ? items : undefined } };
			});
		});
	});

	// Subgraph highlighting: shared logic for infra and trigger subgraphs
	function applySubgraphHighlight(
		show: boolean,
		extractFn: (projectNodes: any, projectEdges: any) => import('$lib/utils/subgraph').SubgraphResult,
		highlightedClass: string,
		dimmedClass: string,
	) {
		if (!show) {
			untrack(() => {
				nodes = nodes.map(n => ({ ...n, class: '' }));
				edges = edges.map(e => {
					const active = executionState?.activeEdges?.has(e.id) ?? false;
					return { ...e, class: active ? 'edge-active' : '' };
				});
			});
			return;
		}
		untrack(() => {
			const projectNodes = nodes.map(n => ({
				id: n.id,
				nodeType: n.data.nodeType as string,
				label: n.data.label as string | null,
				config: n.data.config as Record<string, unknown>,
				position: n.position,
				inputs: n.data.inputs as any[],
				outputs: n.data.outputs as any[],
				features: NODE_TYPE_CONFIG[n.data.nodeType as string]?.features || {},
			}));
			const projectEdges = edges.map(e => ({
				id: e.id,
				source: e.source,
				target: e.target,
				sourceHandle: e.sourceHandle || '',
				targetHandle: e.targetHandle || '',
			}));
			const result = extractFn(projectNodes as any, projectEdges as any);
			const subgraphNodeIds = result.nodeIds;
			const subgraphEdgeIds = new Set(result.edges.map(e => e.id));
			nodes = nodes.map(n => ({
				...n, class: subgraphNodeIds.has(n.id) ? highlightedClass : dimmedClass,
			}));
			edges = edges.map(e => ({
				...e, class: subgraphEdgeIds.has(e.id) ? highlightedClass : dimmedClass,
			}));
		});
	}

	// Infra subgraph highlighting
	let showInfraSubgraph = $state(false);
	$effect(() => {
		applySubgraphHighlight(showInfraSubgraph, extractInfraSubgraph, 'infra-highlighted', 'infra-dimmed');
	});

	// Trigger subgraph highlighting
	let showTriggerSubgraph = $state(false);
	$effect(() => {
		applySubgraphHighlight(showTriggerSubgraph, extractTriggerSubgraph, 'trigger-highlighted', 'trigger-dimmed');
	});

	let selectedNodeId = $state<string | null>(null);

	let contextMenu = $state<{ x: number; y: number; flowX: number; flowY: number; nodeId: string | null } | null>(null);
	let commandPaletteOpen = $state(false);
	
	// Flow position saved from the context menu (right-click) for placing nodes
	let contextMenuFlowPos = $state<{ x: number; y: number } | null>(null);
	
	// Track pending connection for "drop on empty" feature
	let pendingConnection = $state<{ sourceNodeId: string; sourceHandle: string | null } | null>(null);
	
	let preDragPositions = new Map<string, { x: number; y: number }>();

	function cloneState(): HistoryState {
		return {
			nodes: JSON.parse(JSON.stringify(nodes)),
			edges: JSON.parse(JSON.stringify(edges)),
			weftCode,
		};
	}

	/** Fast content hash (djb2) to avoid serializing the entire graph twice for equality checks. */
	function hashState(state: HistoryState): string {
		// Hash the weft code (source of truth) + node count + edge count as a fast fingerprint.
		// Full JSON equality is too expensive for large graphs.
		const s = state.weftCode + ':' + state.nodes.length + ':' + state.edges.length;
		let hash = 5381;
		for (let i = 0; i < s.length; i++) {
			hash = ((hash * 33) ^ s.charCodeAt(i)) >>> 0;
		}
		return hash.toString(16);
	}

	let lastHistoryHash = '';

	// Called AFTER an action to save the new state
	function saveToHistory() {
		if (isUndoRedo) return;

		// Debounce: prevent multiple saves within DEBOUNCE_MS
		const now = Date.now();
		if (now - lastPushTime < DEBOUNCE_MS) return;
		lastPushTime = now;

		const currentState = cloneState();

		// Don't save if state hasn't changed (fast hash comparison)
		const currentHash = hashState(currentState);
		if (currentHash === lastHistoryHash) return;
		lastHistoryHash = currentHash;

		// Truncate any redo history and add new state
		const newHistory = history.slice(0, historyIndex + 1);
		newHistory.push(currentState);
		if (newHistory.length > MAX_HISTORY) {
			newHistory.shift();
		}
		history = newHistory;
		historyIndex = history.length - 1;
	}
	

	function restoreFromHistory(state: HistoryState) {
		const restoredNodes: Node[] = JSON.parse(JSON.stringify(state.nodes));
		// Re-attach function callbacks lost during JSON serialization
		for (const n of restoredNodes) {
			n.data.onUpdate = createNodeUpdateHandler(n.id);
		}
		nodes = restoredNodes;
		edges = JSON.parse(JSON.stringify(state.edges));
		weftCode = state.weftCode;
	}

	function undo() {
		if (historyIndex <= 0) return;
		isUndoRedo = true;
		historyIndex--;
		restoreFromHistory(history[historyIndex]);
		isUndoRedo = false;
		saveProject();
	}

	function redo() {
		if (historyIndex >= history.length - 1) return;
		isUndoRedo = true;
		historyIndex++;
		restoreFromHistory(history[historyIndex]);
		isUndoRedo = false;
		saveProject();
	}

	// Initialize weftCode from project prop and history with current state
	$effect(() => {
		if (history.length === 0) {
			initWeftCode();
			history = [cloneState()];
			historyIndex = 0;
		}
	});

	function doFitView(padding = 0.2) {
		const flowContainer = document.querySelector('.svelte-flow');
		if (!flowContainer) return;
		const rect = flowContainer.getBoundingClientRect();
		const containerW = rect.width;
		const containerH = rect.height;
		if (containerW === 0 || containerH === 0) return;

		// Compute bounding box of visible, measured nodes (using absolute positions)
		const visibleNodes = nodes.filter(n => n.style !== 'display: none;' && n.measured?.width && n.measured?.height);
		if (visibleNodes.length === 0) return;

		// For child nodes, compute absolute position by walking up parentId chain
		function getAbsPos(node: Node): { x: number; y: number } {
			let x = node.position.x;
			let y = node.position.y;
			if (node.parentId) {
				const parent = nodes.find(n => n.id === node.parentId);
				if (parent) {
					const parentAbs = getAbsPos(parent);
					x += parentAbs.x;
					y += parentAbs.y;
				}
			}
			return { x, y };
		}

		let minX = Infinity, minY = Infinity, maxX = -Infinity, maxY = -Infinity;
		for (const n of visibleNodes) {
			const abs = getAbsPos(n);
			minX = Math.min(minX, abs.x);
			minY = Math.min(minY, abs.y);
			maxX = Math.max(maxX, abs.x + (n.measured!.width ?? 0));
			maxY = Math.max(maxY, abs.y + (n.measured!.height ?? 0));
		}

		const contentW = maxX - minX;
		const contentH = maxY - minY;
		if (contentW === 0 || contentH === 0) return;

		const padW = containerW * padding;
		const padH = containerH * padding;
		const zoom = Math.min(
			(containerW - padW) / contentW,
			(containerH - padH) / contentH,
			2  // maxZoom
		);
		const clampedZoom = Math.max(0.05, Math.min(zoom, 2));
		const centerX = (minX + maxX) / 2;
		const centerY = (minY + maxY) / 2;
		const x = containerW / 2 - centerX * clampedZoom;
		const y = containerH / 2 - centerY * clampedZoom;

		setViewport({ x, y, zoom: clampedZoom });
	}

	/** Measure actual handle Y positions from the DOM for a given node element. */
	function measurePortPositions(nodeId: string): Map<string, number> {
		const portYMap = new Map<string, number>();
		const nodeEl = document.querySelector(`[data-id="${nodeId}"]`) as HTMLElement | null;
		if (!nodeEl) return portYMap;
		const nodeRect = nodeEl.getBoundingClientRect();
		// Find all handles inside this node
		const handles = nodeEl.querySelectorAll('.svelte-flow__handle');
		for (const handle of handles) {
			const handleId = handle.getAttribute('data-handleid');
			if (!handleId) continue;
			const handleRect = handle.getBoundingClientRect();
			// Y relative to node top
			const relativeY = handleRect.top + handleRect.height / 2 - nodeRect.top;
			portYMap.set(handleId, relativeY);
		}
		return portYMap;
	}

	function runAutoOrganize(andFitView = false): Promise<void> {
		const sizes = new Map<string, { width: number; height: number }>();
		let measuredCount = 0;
		let unmeasuredCount = 0;
		for (const n of nodes) {
			if (n.measured?.width && n.measured?.height) {
				sizes.set(n.id, { width: n.measured.width, height: n.measured.height });
				measuredCount++;
			} else {
				unmeasuredCount++;
			}
		}

		// Measure actual port Y positions from the DOM
		const portPositions = new Map<string, Map<string, number>>();
		for (const n of nodes) {
			if (n.style === 'display: none;') continue;
			const portYs = measurePortPositions(n.id);
			if (portYs.size > 0) {
				portPositions.set(n.id, portYs);
			}
		}
		// Build current node/edge data from SvelteFlow state (not stale project prop)
		const currentNodes = nodes.map(n => ({
			id: n.id,
			nodeType: n.data.nodeType as string,
			label: (n.data.label as string | null) || null,
			config: (n.data.config as Record<string, unknown>) || {},
			position: n.position,
			parentId: (n.data.config as Record<string, string>)?.parentId,
			inputs: (n.data.inputs as PortDefinition[]) || [],
			outputs: (n.data.outputs as PortDefinition[]) || [],
			features: { ...(NODE_TYPE_CONFIG[n.data.nodeType as string]?.features || {}), ...((n.data.features as NodeFeatures) || {}) },
			sourceLine: n.data.sourceLine as number | undefined,
		}));
		const currentEdges = edges.map(e => ({
			id: e.id,
			source: e.source,
			target: e.target,
			sourceHandle: e.sourceHandle || null,
			targetHandle: e.targetHandle || null,
		}));
		return autoOrganize(currentNodes, currentEdges, sizes, portPositions).then(({ positions, groupSizes }) => {
			nodes = nodes.map((n) => {
				const pos = positions.get(n.id);
				const groupSize = groupSizes.get(n.id);
				// Remove pending-layout class from nodes that were hidden during patch wait
				const nodeClass = typeof n.class === 'string' ? n.class : '';
				const hasPending = nodeClass.includes('node-pending-layout');
				let updated = hasPending ? { ...n, class: nodeClass.replace('node-pending-layout', '').trim() || undefined } : n;
				if (pos) updated = { ...updated, position: pos };
				if (groupSize) {
					const existingConfig = updated.data.config as Record<string, unknown>;
					const w = groupSize.width;
					const h = groupSize.height;
					const newConfig = { ...existingConfig, width: w, height: h };
					updated = { ...updated, style: `width: ${w}px; height: ${h}px;`, data: { ...updated.data, config: newConfig } };
				}
				return updated;
			});
			// Unhide edges that were hidden during streaming/patch wait (marked with pendingLayout)
			if (edges.some(e => (e.data as Record<string, unknown>)?.pendingLayout)) {
				edges = edges.map(e => {
					if ((e.data as Record<string, unknown>)?.pendingLayout) {
						const { pendingLayout, ...rest } = e.data as Record<string, unknown>;
						return { ...e, hidden: false, data: rest };
					}
					return e;
				});
			}
			// Write ELK-computed positions to layoutCode (separate from weftCode)
			for (const n of nodes) {
				if (positions.has(n.id) || groupSizes.has(n.id)) {
					layoutUpdateAny(n);
				}
			}
			saveToHistory();
			saveProject();
			if (andFitView) setTimeout(() => doFitView(), 50);
		});
	}

	// Surgical patch: apply a new project definition without remounting the editor.
	// Preserves positions of unchanged nodes, then re-runs ELK (same path as the palette auto-organize).
	/** VS Code embedding: the host owns the authoritative .weft text.
	 *  Whenever it sends us a parseResult we may need to overwrite
	 *  our local weftCode + layoutCode copy (e.g. the user edited
	 *  the file directly in the text editor).
	 *
	 *  Race window: in-graph config edits update `weftCode`
	 *  locally, then debounce `saveProject` for ~1s. During that
	 *  window, anything that triggers a host-side `triggerParse`
	 *  (a focus-change, an iframe rebuild's `ready` re-emit, a
	 *  background tick) reads the OLD source from disk and posts
	 *  it back as a parseResult. If we naively accept that, the
	 *  in-memory edit reverts to the disk version. We avoid that
	 *  by treating any unsaved edit (saveProjectTimer pending OR
	 *  fieldEditor with an active key) as authoritative and
	 *  dropping the host echo until the saveWeft round-trip
	 *  completes. */
	export function applyExternalSource(newWeftCode: string, newLayoutCode: string): void {
		if (newLayoutCode !== layoutCode) layoutCode = newLayoutCode;
		// Local copy is ahead of the host's view of the .weft. A
		// pending saveProjectTimer is the canonical signal: a
		// debounced saveWeft is in flight, the local weftCode
		// reflects the user's most recent edits, and the host
		// echo is necessarily stale. Drop the echo; the next
		// round-trip after saveProjectTimer fires will reconcile.
		if (saveProjectTimer !== null && newWeftCode !== weftCode) {
			return;
		}
		// Fast path: if the source matches what we already have,
		// the user-visible structure didn't change — but the
		// COMPILER may have re-inferred port metadata that isn't
		// captured in the source text (laneMode for implicit
		// Gather/Expand, inferred concrete portTypes from edges).
		// We need to refresh those without re-running ELK or
		// rebuilding the node list. mergeInferredPortMetadata
		// patches the existing nodes in place.
		if (newWeftCode === weftCode) {
			const cached = parseWeftCode(weftCode);
			if (cached.projects.length > 0) {
				mergeInferredPortMetadata(cached.projects[0].project);
			}
			return;
		}
		weftCode = newWeftCode;
		const result = parseWeftCode(weftCode);
		if (result.projects.length > 0) {
			const w = result.projects[0];
			applyParseResult(w);
			if (w.project.name) project.name = w.project.name;
			if (w.project.description !== undefined) project.description = w.project.description;
			void patchFromProject(w.project);
		} else {
			weftParseErrors = result.errors;
		}
	}

	/// Update each existing graph node's `data.inputs` / `data.outputs`
	/// to match the freshly-parsed project's port metadata. Used when
	/// the source text didn't change but the compiler may have
	/// re-inferred laneMode (implicit Gather/Expand) or concrete
	/// portTypes. Only touches port arrays — positions, configs,
	/// edge IDs are untouched, so xyflow doesn't relayout.
	function mergeInferredPortMetadata(parsed: ProjectDefinition): void {
		const byId = new Map(parsed.nodes.map(n => [n.id, n]));
		let changed = false;
		const next = nodes.map(n => {
			const fresh = byId.get(n.id);
			if (!fresh) return n;
			const oldInputs = (n.data.inputs as PortDefinition[] | undefined) ?? [];
			const oldOutputs = (n.data.outputs as PortDefinition[] | undefined) ?? [];
			if (portsEqual(oldInputs, fresh.inputs) && portsEqual(oldOutputs, fresh.outputs)) {
				return n;
			}
			changed = true;
			return { ...n, data: { ...n.data, inputs: fresh.inputs, outputs: fresh.outputs } };
		});
		if (changed) {
			nodes = next;
		}
	}

	function portsEqual(a: PortDefinition[], b: PortDefinition[]): boolean {
		if (a.length !== b.length) return false;
		for (let i = 0; i < a.length; i++) {
			const pa = a[i]; const pb = b[i];
			if (pa.name !== pb.name) return false;
			if (pa.portType !== pb.portType) return false;
			if (pa.laneMode !== pb.laneMode) return false;
			if (!!pa.required !== !!pb.required) return false;
		}
		return true;
	}

	export async function patchFromProject(newProject: ProjectDefinition, andFitView = false): Promise<void> {
		// Reset opaque blocks since we're applying a new project (errors are preserved, set by applyParseResult)
		weftOpaqueBlocks = [];
		// Build a position map from the current editor state so unchanged nodes keep their spot
		const currentPositions = new Map(nodes.map(n => [n.id, n.position]));

		// Rebuild nodes from the new project, injecting preserved positions where available.
		// New nodes get class 'node-pending-layout' (opacity:0) so they render+measure but stay invisible.
		// runAutoOrganize removes this class after ELK completes.
		const newNodes = buildNodes(newProject.nodes, newProject.edges, parseLayoutCode(layoutCode)).map(n => {
			const existingPos = currentPositions.get(n.id);
			if (existingPos) return { ...n, position: existingPos };
			// New node: invisible but rendered so SvelteFlow can measure it
			return { ...n, class: ((n.class ?? '') + ' node-pending-layout').trim() };
		});
		nodes = newNodes;

		// New edges get hidden so they don't flash in wrong positions before ELK runs.
		const currentEdgeIds = new Set(edges.map(e => e.id));
		edges = buildEdges(newProject.edges, newProject.nodes).map(e =>
			currentEdgeIds.has(e.id) ? e : { ...e, hidden: true, data: { ...e.data, pendingLayout: true } }
		);

		// Wait for SvelteFlow to measure the new nodes before running ELK.
		// Without this, n.measured is empty and ELK uses wrong size estimates.
		await tick();
		await new Promise(resolve => setTimeout(resolve, 300));
		await runAutoOrganize(andFitView);
	}

	// Weft streaming: AI streams raw weft text into the code editor
	let streamLastNodeIds = new Set<string>();
	let streamLastEdgeIds = new Set<string>();
	let streamParseTimer: ReturnType<typeof setTimeout> | null = null;
	let streamOrganizePending = false;

	function streamSyncVisual(parsed: ProjectDefinition) {
		weftSyncDirection = 'to-editor';
		const currentPositions = new Map(nodes.map(n => [n.id, n.position]));
		// Preserve existing styles for nodes that are already rendered, so groups
		// keep their ELK-computed sizes instead of flashing back to defaults.
		const currentStyles = new Map(nodes.map(n => [n.id, n.style]));
		// Also preserve current group configs (width/height from ELK) for buildNodes
		const currentGroupConfigs = new Map<string, { width?: number; height?: number }>();
		for (const n of nodes) {
			if (n.type === 'group') {
				const cfg = n.data.config as Record<string, unknown> | undefined;
				const rect = getNodeRect(n);
				currentGroupConfigs.set(n.id, {
					width: (cfg?.width as number) || rect.width || undefined,
					height: (cfg?.height as number) || rect.height || undefined,
				});
			}
		}
		// Inject preserved group dimensions into parsed nodes before buildNodes
		for (const pn of parsed.nodes) {
			if (pn.nodeType === 'Group') {
				const saved = currentGroupConfigs.get(pn.id);
				if (saved) {
					const cfg = (pn.config as Record<string, unknown>) || {};
					if (!cfg.width && saved.width) cfg.width = saved.width;
					if (!cfg.height && saved.height) cfg.height = saved.height;
					(pn as any).config = cfg;
				}
			}
		}
		// Compute the right edge of existing content so new nodes can be placed
		// to the right instead of on top of existing nodes.
		let existingRightEdge = 0;
		for (const n of nodes) {
			if (n.style === 'display: none;') continue;
			const w = n.measured?.width ?? 200;
			existingRightEdge = Math.max(existingRightEdge, n.position.x + w);
		}
		let newNodeOffsetY = 0;
		const newNodes = buildNodes(parsed.nodes, parsed.edges, parseLayoutCode(layoutCode)).map(n => {
			const existingPos = currentPositions.get(n.id);
			const existingStyle = currentStyles.get(n.id);
			if (existingPos) {
				// Keep existing style if available (prevents group resize flicker)
				const style = existingStyle && existingStyle !== 'display: none;' ? existingStyle : n.style;
				return { ...n, position: existingPos, style };
			}
			// New node: place to the right of existing content, stacked vertically
			const pos = { x: existingRightEdge + 100, y: newNodeOffsetY };
			newNodeOffsetY += 80;
			return { ...n, position: pos, class: ((n.class ?? '') + ' node-pending-layout').trim() };
		});
		nodes = newNodes;
		// Track which nodes are truly "unanchored" (pending layout AND not inside
		// an already-positioned group). Children of existing groups inherit position
		// from the parent, so their edges should be visible immediately.
		const pendingNodeIds = new Set<string>();
		for (const n of newNodes) {
			if (typeof n.class !== 'string' || !n.class.includes('node-pending-layout')) continue;
			// If this node's parent group is already positioned, don't consider it pending
			if (n.parentId && currentPositions.has(n.parentId)) continue;
			pendingNodeIds.add(n.id);
		}
		const currentEdgeIds = new Set(edges.map(e => e.id));
		edges = buildEdges(parsed.edges, parsed.nodes).map(e => {
			if (currentEdgeIds.has(e.id)) return e;
			// Hide new edges until ELK positions everything (prevents flash to wrong position)
			return { ...e, hidden: true, data: { ...e.data, pendingLayout: true } };
		});
		weftSyncDirection = 'none';
		// Debounce auto-organize so we don't run ELK on every single item
		if (!streamOrganizePending) {
			streamOrganizePending = true;
			tick().then(() => {
				setTimeout(() => {
					streamOrganizePending = false;
					runAutoOrganize(false);
				}, 400);
			});
		}
	}

	let streamLastWeftContent = '';
	function streamTryIncrementalParse() {
		const fenced = '````weft\n' + weftCode + '\n````';
		const result = parseWeft(fenced);
		if (result.projects.length === 0) return;
		const { project: parsed } = result.projects[0];

		// Check if the set of node/edge IDs changed (handles add, remove, and modify)
		const newNodeIds = new Set(parsed.nodes.map(n => n.id));
		const newEdgeIds = new Set(parsed.edges.map(e => e.id));
		const nodesDiffer = newNodeIds.size !== streamLastNodeIds.size || [...newNodeIds].some(id => !streamLastNodeIds.has(id));
		const edgesDiffer = newEdgeIds.size !== streamLastEdgeIds.size || [...newEdgeIds].some(id => !streamLastEdgeIds.has(id));
		// Detect any content change (length OR content) so same-length patches still sync
		const contentChanged = weftCode !== streamLastWeftContent;

		if (nodesDiffer || edgesDiffer || contentChanged) {
			streamLastNodeIds = newNodeIds;
			streamLastEdgeIds = newEdgeIds;
			streamLastWeftContent = weftCode;
			streamSyncVisual(parsed);
		}
	}

	export function weftStreamStart(mode: 'weft' | 'weft-patch' | 'weft-continue') {
		weftStreaming = true;
		weftSyncDirection = 'to-code';
		streamOrganizePending = false;
		if (streamParseTimer) { clearTimeout(streamParseTimer); streamParseTimer = null; }
		// Cancel any pending code-edit debounce so stale user edits don't fire mid-stream
		if (weftSyncTimer) { clearTimeout(weftSyncTimer); weftSyncTimer = null; codeEditInFlight = false; }
		// Force-blur the code panel to prevent focus-related sync bugs during AI edits
		if (document.activeElement instanceof HTMLElement) document.activeElement.blur();
		if (mode === 'weft') {
			// Full weft: clear editor for incoming content
			weftCode = '';
			clearWeftParseState();
			streamLastNodeIds = new Set();
			streamLastEdgeIds = new Set();
			streamLastWeftContent = '';
		} else {
			// weft-patch and weft-continue: preserve existing code
			if (mode === 'weft-continue' && weftCode.length > 0 && !weftCode.endsWith('\n')) {
				weftCode += '\n';
			}
			streamLastNodeIds = new Set(nodes.map(n => n.id));
			streamLastEdgeIds = new Set(edges.map(e => e.id));
			streamLastWeftContent = weftCode;
		}
	}

	export function weftStreamDelta(delta: string, mode: 'weft' | 'weft-patch' | 'weft-continue', at?: number) {
		if (at !== undefined && at >= 0) {
			// Positional insertion: splice delta at the given character offset
			weftCode = weftCode.slice(0, at) + delta + weftCode.slice(at);
		} else {
			// Append mode (weft / weft-continue)
			weftCode += delta;
		}
		// Try incremental parse (debounced to avoid parsing on every token)
		if (streamParseTimer) clearTimeout(streamParseTimer);
		streamParseTimer = setTimeout(() => {
			streamParseTimer = null;
			streamTryIncrementalParse();
		}, 100);
	}

	export function getWeftCode(): string {
		return weftCode;
	}

	export function getRawWeftCode(): string {
		return weftCode;
	}

	export function getLayoutCode(): string {
		return layoutCode;
	}

	export function isStreaming(): boolean {
		return weftStreaming;
	}

	export function updateNodeConfigs(configUpdates: Array<{ nodeId: string; fieldKey: string; value: unknown }>) {
		nodes = nodes.map(n => {
			const updates = configUpdates.filter(u => u.nodeId === n.id);
			if (updates.length === 0) return n;
			let newConfig = { ...(n.data.config as Record<string, unknown>) };
			for (const u of updates) newConfig[u.fieldKey] = u.value;
			return { ...n, data: { ...n.data, config: newConfig } };
		});
		// Surgically update weftCode for each config change
		for (const u of configUpdates) {
			weftCode = weftUpdateConfig(weftCode, u.nodeId, u.fieldKey, u.value);
		}
		saveProject();
	}

	/** Find a SEARCH block match in weftCode, erase the matched region, return insert offset. */
	export function weftStreamPatchSearch(searchText: string): { insertAt: number } | { error: string } {
		const match = findSearchMatch(weftCode, searchText);
		if ('error' in match) return match;
		weftCode = weftCode.slice(0, match.offset) + weftCode.slice(match.offset + match.length);
		return { insertAt: match.offset };
	}

	export async function weftStreamEnd(): Promise<{ errors: WeftParseError[]; warnings: WeftWarning[]; opaqueBlocks: OpaqueBlock[] }> {
		if (streamParseTimer) { clearTimeout(streamParseTimer); streamParseTimer = null; }
		// Safety: strip any fences from weftCode before wrapping
		weftCode = stripWeftFences(weftCode);
		// Final parse and sync
		const result = parseWeftCode(weftCode);
		if (result.projects.length > 0) {
			const { project: parsed, opaqueBlocks, errors, warnings } = result.projects[0];
			applyParseResult(result.projects[0]);

			// Sync name/description so saveProject uses the updated values
			if (parsed.name) project.name = parsed.name;
			if (parsed.description !== undefined) project.description = parsed.description;

			// Visual sync and auto-organize
			streamSyncVisual(parsed);
			weftStreaming = false;
			// Wait for auto-organize to settle (ELK writes @layout back to weftCode)
			await tick();
			await new Promise(resolve => setTimeout(resolve, 600));
			await tick();
			// Save after auto-organize so @layout directives reflect ELK positions
			saveProject();

			return { errors, warnings, opaqueBlocks };
		}

		weftStreaming = false;
		weftSyncDirection = 'none';
		return { errors: result.errors, warnings: [], opaqueBlocks: [] };
	}

	// Fit view to graph on initial load
	let hasFitView = $state(false);
	let hasAutoOrganized = $state(false);
	// Hide canvas until initial ELK layout completes to avoid flash of ugly unorganized positions
	let canvasReady = $state(false);
	$effect(() => {
		if (!hasFitView && nodes.length > 0) {
			hasFitView = true;
			if (!layoutCode || autoOrganizeOnMount) {
				// No saved layout or explicitly requested: run ELK to compute positions.
				// Wait until SvelteFlow has measured every node before firing ELK,
				// otherwise ELK uses zero-sized fallbacks and produces garbage layouts
				// (same issue patchFromProject works around below). Cap at 2s so a
				// pathological case doesn't wedge the canvas forever.
				hasAutoOrganized = true;
				void (async () => {
					const deadline = Date.now() + 2000;
					while (Date.now() < deadline) {
						await tick();
						if (nodes.every(n => n.measured?.width && n.measured?.height)) break;
						await new Promise(resolve => setTimeout(resolve, 50));
					}
					await runAutoOrganize(true);
					canvasReady = true;
				})();
			} else {
				// Saved layout exists: just fit the view, don't reorganize
				setTimeout(() => { doFitView(); canvasReady = true; }, 100);
			}
		} else if (!hasFitView && nodes.length === 0) {
			canvasReady = true;
		}
	});

	// Handle actions from command palette
	function handlePaletteAction(action: string) {
		switch (action) {
			case 'save':
				saveProject();
				break;
			case 'run':
				onRun?.();
				break;
			case 'add_group':
				addNode('Group');
				break;
			case 'undo':
				undo();
				break;
			case 'redo':
				redo();
				break;
			case 'selectAll':
				nodes = nodes.map(n => ({ ...n, selected: true }));
				break;
			case 'fitView':
				doFitView();
				break;
			case 'duplicate':
				// Duplicate selected node(s)
				if (selectedNodeId) {
					duplicateNode(selectedNodeId);
				} else {
					const selectedNodes = nodes.filter(n => n.selected);
					if (selectedNodes.length > 0) {
						duplicateNode(selectedNodes[0].id);
					}
				}
				break;
			case 'delete':
				// Delete selected node(s)
				if (selectedNodeId) {
					deleteNode(selectedNodeId);
				} else {
					const selectedNodes = nodes.filter(n => n.selected);
					if (selectedNodes.length > 0) {
						deleteNodes(selectedNodes.map(n => n.id));
					}
				}
				break;
			case 'autoOrganize': {
				// Re-parse current code and re-layout visually
				const result = parseWeftCode(weftCode);
				if (result.projects.length > 0) {
					const w = result.projects[0];
					applyParseResult(w);
					patchFromProject(w.project, true).then(() => {
						// After auto-organize, update @layout directives in weftCode
						for (const n of nodes) {
							layoutUpdateAny(n);
						}
						saveProject();
					});
				}
				break;
			}
		}
	}

	let currentViewport = $state({ x: 100, y: 100, zoom: 1 });

	function wouldCreateCycle(source: string, target: string): boolean {
		const adjacency = new Map<string, string[]>();
		for (const edge of edges) {
			// Skip group interface pass-through edges (inner handles), they represent
			// data flowing through the group, not actual dependency cycles
			if (edge.sourceHandle?.endsWith('__inner') || edge.targetHandle?.endsWith('__inner')) continue;
			if (!adjacency.has(edge.source)) adjacency.set(edge.source, []);
			adjacency.get(edge.source)!.push(edge.target);
		}
		if (!adjacency.has(source)) adjacency.set(source, []);
		adjacency.get(source)!.push(target);

		const visited = new Set<string>();
		const stack = new Set<string>();

		function dfs(node: string): boolean {
			if (stack.has(node)) return true;
			if (visited.has(node)) return false;
			visited.add(node);
			stack.add(node);
			for (const neighbor of adjacency.get(node) || []) {
				if (dfs(neighbor)) return true;
			}
			stack.delete(node);
			return false;
		}

		for (const node of nodes) {
			if (dfs(node.id)) return true;
		}
		return false;
	}

	// Scope-based connection validation: inner handles connect within the group,
	// outer handles connect in the parent scope, regular nodes connect in their parent scope.
	function getHandleScope(nodeId: string, handleId: string | null | undefined): string | null {
		const node = nodes.find(n => n.id === nodeId);
		if (!node) return null;
		const isGroup = node.type === 'group' || node.type === 'groupCollapsed';
		const parentId = (node.data.config as Record<string, string>)?.parentId;
		if (isGroup && handleId?.endsWith('__inner')) {
			// Inner handle: scope is inside this group
			return nodeId;
		}
		// Outer handle or regular node: scope is the parent group (or '__root__' for top-level)
		return parentId || '__root__';
	}

	function isValidConnection(connection: Edge | Connection): boolean {
		const sourceScope = getHandleScope(connection.source!, connection.sourceHandle);
		const targetScope = getHandleScope(connection.target!, connection.targetHandle);
		if (sourceScope === null || targetScope === null) return false;
		return sourceScope === targetScope;
	}

	// Track current connection line color based on source handle
	let currentConnectionColor = $state('#9ca3af');
	
	function onConnectStart(event: MouseEvent | TouchEvent, params: { nodeId: string | null; handleId: string | null; handleType: 'source' | 'target' | null }) {
		// Set connection line color based on source port
		if (params.nodeId && params.handleType === 'source') {
			const color = getEdgeColor(params.nodeId, params.handleId);
			currentConnectionColor = color;
		}
	}

	// Track if reconnection was successful (dropped on valid handle)
	let reconnectSuccessful = false;
	
	// eslint-disable-next-line @typescript-eslint/no-explicit-any
	function onReconnectStart(event: MouseEvent | TouchEvent, edge: any) {
		reconnectSuccessful = false;
		// Set connection line color based on the edge being reconnected
		if (edge?.source) {
			currentConnectionColor = getEdgeColor(edge.source, edge.sourceHandle);
		}
	}
	
	// eslint-disable-next-line @typescript-eslint/no-explicit-any
	function onReconnect(oldEdge: any, newConnection: any) {
		if (structuralLock) return;
		reconnectSuccessful = true;

		// Remove old edge from weft code, add new one
		const oldRef = toWeftEdgeRef(oldEdge.source, oldEdge.sourceHandle || 'value', oldEdge.target, oldEdge.targetHandle || 'value');
		weftCode = weftRemoveEdge(weftCode, oldRef.srcRef, oldRef.srcPort, oldRef.tgtRef, oldRef.tgtPort);

		const newRef = toWeftEdgeRef(newConnection.source, newConnection.sourceHandle || 'value', newConnection.target, newConnection.targetHandle || 'value');
		weftCode = weftAddEdge(weftCode, newRef.srcRef, newRef.srcPort, newRef.tgtRef, newRef.tgtPort, newRef.scopeGroupLabel);

		// Update the edge with new connection
		edges = edges.map(e => {
			if (e.id === oldEdge.id) {
				return {
					...e,
					source: newConnection.source,
					sourceHandle: newConnection.sourceHandle,
					target: newConnection.target,
					targetHandle: newConnection.targetHandle,
				};
			}
			return e;
		});
		saveToHistory();
	}

	// eslint-disable-next-line @typescript-eslint/no-explicit-any
	function onReconnectEnd(event: MouseEvent | TouchEvent, edge: any) {
		// If reconnection wasn't successful (dropped on empty space), delete the edge
		if (!reconnectSuccessful && !structuralLock) {
			const ref = toWeftEdgeRef(edge.source, edge.sourceHandle || 'value', edge.target, edge.targetHandle || 'value');
			weftCode = weftRemoveEdge(weftCode, ref.srcRef, ref.srcPort, ref.tgtRef, ref.tgtPort);
			edges = edges.filter(e => e.id !== edge.id);
			saveToHistory();
		}
		reconnectSuccessful = false;
	}

	// Flag to prevent click handler from immediately closing the context menu after drop
	let justOpenedContextMenu = false;
	
	// eslint-disable-next-line @typescript-eslint/no-explicit-any
	function onConnectEnd(event: MouseEvent | TouchEvent, connectionState: any) {
			
		// When a connection is dropped on the pane it's not valid
		// Based on React Flow example: https://reactflow.dev/examples/nodes/add-node-on-edge-drop
		if (!connectionState.isValid) {
			// Get coordinates - handle both mouse and touch events
			const { clientX, clientY } = 'changedTouches' in event 
				? (event as TouchEvent).changedTouches[0] 
				: (event as MouseEvent);
			
			// Use screenToFlowPosition for accurate flow coordinates
			const flowPos = screenToFlowPosition({ x: clientX, y: clientY });
			
			// Store the source info from connectionState
			if (connectionState.fromNode) {
				pendingConnection = {
					sourceNodeId: connectionState.fromNode.id,
					sourceHandle: connectionState.fromHandle?.id || null,
				};
			}
			
			// Set flag to prevent the click handler from immediately closing the menu
			justOpenedContextMenu = true;
			setTimeout(() => { justOpenedContextMenu = false; }, 100);
			
			contextMenu = {
				x: clientX,
				y: clientY,
				flowX: flowPos.x,
				flowY: flowPos.y,
				nodeId: null, // null means "add node" mode, not "edit node" mode
			};
		} else {
			// Connection was valid - clear pending
			pendingConnection = null;
		}
	}

	function onBeforeConnect(connection: Connection): Edge | null {
		// Clear pending connection since we're making a real connection
		pendingConnection = null;
		if (structuralLock) return null;

		if (wouldCreateCycle(connection.source!, connection.target!)) {
			alert("Cannot create this connection - it would create a cycle (infinite loop)");
			return null;
		}

		const sourceHandle = connection.sourceHandle;
		const targetHandle = connection.targetHandle;
		
		// Remove any existing edge TO the same input port (only one edge per input allowed)
		const targetNode = connection.target;
		const targetPort = targetHandle || 'default';
		
		edges = edges.filter(e => {
			const eTargetPort = e.targetHandle || 'default';
			return !(e.target === targetNode && eTargetPort === targetPort);
		});

		const edgeColor = getEdgeColor(connection.source!, sourceHandle);

		const newEdge: Edge = {
			id: `e-${connection.source}-${sourceHandle}-${connection.target}-${targetHandle}`,
			source: connection.source!,
			target: connection.target!,
			sourceHandle,
			targetHandle,
			type: 'custom',
			zIndex: 5,
			style: `stroke-width: 2px; stroke: ${edgeColor};`,
			markerEnd: {
				type: MarkerType.ArrowClosed,
				width: 20,
				height: 20,
				color: edgeColor,
			},
		};
		
		// Schedule save after the edge is added
		setTimeout(() => {
			// Add edge to weftCode with proper scoping
			const ref = toWeftEdgeRef(connection.source!, sourceHandle || 'value', connection.target!, targetHandle || 'value');
			weftCode = weftAddEdge(weftCode, ref.srcRef, ref.srcPort, ref.tgtRef, ref.tgtPort, ref.scopeGroupLabel);
			saveToHistory();
			saveProject();
		}, 0);

		return newEdge;
	}

	function getViewportCenter(): { x: number; y: number } {
		const flowContainer = document.querySelector('.svelte-flow');
		if (flowContainer) {
			const rect = flowContainer.getBoundingClientRect();
			return screenToFlowPosition({ x: rect.left + rect.width / 2, y: rect.top + rect.height / 2 });
		}
		return { x: 250, y: 150 };
	}

	function generateUniqueGroupLabel(baseLabel: string): string {
		const existingLabels = new Set(
			nodes.filter(n => n.type === 'group' || n.type === 'groupCollapsed').map(n => (n.data.label as string) || '')
		);
		if (!existingLabels.has(baseLabel)) return baseLabel;
		let i = 2;
		while (existingLabels.has(`${baseLabel}_${i}`)) i++;
		return `${baseLabel}_${i}`;
	}

	function addNode(type: NodeType) {
		if (structuralLock) return;
		const id = generateNodeId(type);
		const typeConfig = NODE_TYPE_CONFIG[type];
		const isGroup = type === 'Group';
		const isAnnotation = type === 'Annotation';
		const pos = contextMenuFlowPos ?? getViewportCenter();
		contextMenuFlowPos = null;
		const newNode: Node = {
			id,
			type: isGroup ? 'group' : isAnnotation ? 'annotation' : 'project',
			position: { x: pos.x, y: pos.y },
			selected: true, // Select the new node
			data: {
				label: isGroup ? generateUniqueGroupLabel(typeConfig.label) : null,
				nodeType: type,
				config: isGroup ? { width: 400, height: 300, expanded: true } : isAnnotation ? { width: 250, height: 120, content: '' } : {},
				inputs: [...typeConfig.defaultInputs],
				outputs: [...typeConfig.defaultOutputs],
				features: typeConfig.features || {},
				onUpdate: createNodeUpdateHandler(id),
			},
			...((isGroup || isAnnotation) ? { style: `width: ${isAnnotation ? 250 : 400}px; height: ${isAnnotation ? 120 : 300}px;` } : {}),
		};
		
		// Deselect all existing nodes before adding the new one
		const deselectedNodes = nodes.map(n => ({ ...n, selected: false }));
		
		if (isGroup || isAnnotation) {
			const specialNodes = deselectedNodes.filter(n => n.type === 'group' || n.type === 'groupCollapsed' || n.type === 'annotation');
			const otherNodes = deselectedNodes.filter(n => n.type !== 'group' && n.type !== 'groupCollapsed' && n.type !== 'annotation');
			nodes = [...specialNodes, newNode, ...otherNodes];
		} else {
			nodes = [...deselectedNodes, newNode];
		}
		selectedNodeId = id;
		// Add node/group to weftCode
		if (isGroup) {
			const groupLabel = newNode.data.label as string;
			weftCode = weftAddGroup(weftCode, groupLabel);
			layoutCode = updateLayoutEntry(layoutCode, groupLabel, pos.x, pos.y, 400, 300);
		} else {
			weftCode = weftAddNode(weftCode, type, id);
			layoutCode = updateLayoutEntry(layoutCode, id, pos.x, pos.y);
		}
		saveToHistory();
		saveProject();
	}

	function deleteNodes(nodeIds: string[]) {
		if (nodeIds.length === 0) return;
		if (structuralLock) return;

		// Capture group labels before visual deletion removes them from the nodes array
		const groupLabels = new Map<string, string>();
		for (const nodeId of nodeIds) {
			const n = nodes.find(nd => nd.id === nodeId);
			if (n && (n.type === 'group' || n.type === 'groupCollapsed') && n.data.label) {
				groupLabels.set(nodeId, n.data.label as string);
			}
		}

		for (const nodeId of nodeIds) {
			const nodeBeingDeleted = nodes.find(n => n.id === nodeId);
			const isGroup = nodeBeingDeleted?.type === 'group' || nodeBeingDeleted?.type === 'groupCollapsed';
			
			if (isGroup && nodeBeingDeleted) {
				const deletedGroup = nodeBeingDeleted;
				const deletedGroupConfig = deletedGroup.data.config as Record<string, string> | undefined;
				const grandparentId = deletedGroupConfig?.parentId;
				nodes = nodes
					.filter((n) => n.id !== nodeId)
					.map(n => {
						if (n.parentId === nodeId) {
							const newConfig = { ...(n.data.config as Record<string, unknown>) };
							if (grandparentId) {
								// Re-parent to grandparent: convert position relative to grandparent
								newConfig.parentId = grandparentId;
								return {
									...n,
									position: { x: deletedGroup.position.x + n.position.x, y: deletedGroup.position.y + n.position.y },
									parentId: grandparentId,
									data: { ...n.data, config: newConfig },
								};
							} else {
								// No grandparent: move to root with absolute position
								delete newConfig.parentId;
								const absoluteX = deletedGroup.position.x + n.position.x;
								const absoluteY = deletedGroup.position.y + n.position.y;
								return {
									...n,
									position: { x: absoluteX, y: absoluteY },
									parentId: undefined,
									data: { ...n.data, config: newConfig },
								};
							}
						}
						return n;
					});
				edges = edges.filter((e) => e.source !== nodeId && e.target !== nodeId);
			} else {
				nodes = nodes.filter((n) => n.id !== nodeId);
				edges = edges.filter((e) => e.source !== nodeId && e.target !== nodeId);
			}
		}
		
		if (selectedNodeId && nodeIds.includes(selectedNodeId)) {
			selectedNodeId = null;
		}
		contextMenu = null;
		// Remove deleted nodes/groups from weftCode and layoutCode
		// Delete non-group nodes first so children are removed while still inside their group scope
		for (const nodeId of nodeIds) {
			if (!groupLabels.has(nodeId)) {
				weftCode = weftRemoveNode(weftCode, nodeId);
				layoutCode = removeLayoutEntry(layoutCode, nodeId);
			}
		}
		for (const nodeId of nodeIds) {
			const groupLabel = groupLabels.get(nodeId);
			if (groupLabel) {
				weftCode = weftRemoveGroup(weftCode, groupLabel);
				layoutCode = removeLayoutEntry(layoutCode, groupLabel);
			}
		}
		saveToHistory();
		saveProject();
	}

	function handleKeyDown(event: KeyboardEvent) {
		const target = event.target as HTMLElement;
		// Skip if user is typing in an input, textarea, or contenteditable element
		// This allows native text editing shortcuts (Ctrl+A, Ctrl+Z, etc.) to work
		const isEditableElement = 
			target.tagName === 'INPUT' || 
			target.tagName === 'TEXTAREA' || 
			target.isContentEditable ||
			target.closest('[role="dialog"]') ||
			target.closest('.edit-textarea') ||
			target.closest('.annotation-node.editing');
		
		// Ctrl+S: save (works from anywhere, including code editors)
		if ((event.ctrlKey || event.metaKey) && event.key === 's') {
			event.preventDefault();
			// Flush pending code panel debounce: parse + update diagram + save
			if (weftSyncTimer && codeEditInFlight) {
				clearTimeout(weftSyncTimer);
				weftSyncTimer = null;
				codeEditInFlight = false;
				const result = parseWeftCode(weftCode);
				if (result.projects.length > 0) {
					const w = result.projects[0];
					applyParseResult(w);
					if (w.project.name) project.name = w.project.name;
					if (w.project.description !== undefined) project.description = w.project.description;
					patchFromProject(w.project);
				}
			}
			saveProject();
			return;
		}

		if (isEditableElement) return;

		// Escape: close context menu and cancel pending connection
		if (event.key === 'Escape') {
			if (contextMenu || pendingConnection) {
				event.preventDefault();
				contextMenu = null;
				pendingConnection = null;
				return;
			}
		}
		
		// Undo: Ctrl+Z
		if ((event.ctrlKey || event.metaKey) && event.key === 'z' && !event.shiftKey) {
			event.preventDefault();
			undo();
			return;
		}
		
		// Redo: Ctrl+Y or Ctrl+Shift+Z
		if ((event.ctrlKey || event.metaKey) && (event.key === 'y' || (event.key === 'z' && event.shiftKey))) {
			event.preventDefault();
			redo();
			return;
		}
		
		// Delete
		if (event.key === 'Delete' || event.key === 'Backspace') {
			const selectedEdges = edges.filter(e => e.selected);
			if (selectedEdges.length > 0) {
				event.preventDefault();
				// Remove edges from weftCode
				for (const e of selectedEdges) {
					const ref = toWeftEdgeRef(e.source, e.sourceHandle || 'value', e.target, e.targetHandle || 'value');
					weftCode = weftRemoveEdge(weftCode, ref.srcRef, ref.srcPort, ref.tgtRef, ref.tgtPort);
				}
				edges = edges.filter(e => !e.selected);
				saveToHistory();
				saveProject();
				return;
			}
			
			const selectedNodes = nodes.filter(n => n.selected);
			if (selectedNodes.length > 0) {
				event.preventDefault();
				deleteNodes(selectedNodes.map(n => n.id));
			} else if (selectedNodeId) {
				event.preventDefault();
				deleteNodes([selectedNodeId]);
			}
		}
	}

	// Counter for bringing clicked nodes to front, must start above edge default zIndex (5)
	let nextNodeZ = 6;

	function onNodeClick({ node }: { node: Node; event: MouseEvent | TouchEvent }) {
		selectedNodeId = node.id;
		contextMenu = null;
		// Bring clicked node to front using same zIndex pattern as buildNodes
		// Defer to next tick so we don't overwrite Svelte Flow's selection state
		tick().then(() => {
			nodes = nodes.map(n => n.id === node.id ? { ...n, zIndex: nextNodeZ } : n);
			// Raise connected edges so their reconnect anchors stay above the raised node
			edges = edges.map(e =>
				(e.source === node.id || e.target === node.id)
					? { ...e, zIndex: nextNodeZ + 1 }
					: e
			);
			nextNodeZ++;
		});
	}

	function onPaneClick() {
		selectedNodeId = null;
		contextMenu = null;
	}

	function onEdgeClick(_event: { event: MouseEvent; edge: Edge }) {
		// No-op: kept for SvelteFlow binding
	}

	function getGroupDimensions(group: Node): { width: number; height: number } {
		const measured = (group as unknown as { measured?: { width?: number; height?: number } }).measured;
		if (measured?.width && measured?.height) {
			return { width: measured.width, height: measured.height };
		}
		const widthMatch = group.style?.match(/width:\s*(\d+)px/);
		const heightMatch = group.style?.match(/height:\s*(\d+)px/);
		return {
			width: widthMatch ? parseInt(widthMatch[1]) : 400,
			height: heightMatch ? parseInt(heightMatch[1]) : 300,
		};
	}

	function onNodeDragStart({ targetNode, nodes: draggedNodes }: { targetNode: Node | null; event: MouseEvent | TouchEvent; nodes: Node[] }) {
		// Capture state before drag - will be saved when drag stops
		lastPushTime = 0; // Reset debounce to allow save
		// Store pre-drag positions for all dragged nodes (for scope-blocked revert)
		preDragPositions.clear();
		for (const dn of draggedNodes) {
			preDragPositions.set(dn.id, { ...dn.position });
		}
		// Bring dragged node to front
		if (targetNode) {
			nodes = nodes.map(n => n.id === targetNode.id ? { ...n, zIndex: nextNodeZ } : n);
			nextNodeZ++;
		}
	}
	
	function onNodeDragStop({ targetNode, nodes: draggedNodes }: { targetNode: Node | null; nodes: Node[] }) {
		if (!targetNode) return;

		// Re-read from current nodes state after each step to avoid stale references.
		let currentNode = nodes.find(n => n.id === targetNode.id);
		if (currentNode) {
			if (currentNode.parentId) {
				checkNodeLeavesGroup(currentNode);
				currentNode = nodes.find(n => n.id === targetNode.id);
			}
			if (currentNode) {
				checkNodeCapturedByGroup(currentNode);
				currentNode = nodes.find(n => n.id === targetNode.id);
			}
			if (currentNode?.type === 'group' || currentNode?.type === 'groupCollapsed') {
				checkGroupCapturesNodes(currentNode);
			}
		}

		// Update weftCode with new positions for all dragged nodes
		for (const dn of draggedNodes) {
			const n = nodes.find(nd => nd.id === dn.id);
			if (!n) continue;
			layoutUpdateAny(n);
		}
		saveToHistory();
		saveProject();
	}

	function onSelectionDragStop(_event: MouseEvent, selectedNodes: Node[]) {
		// Check group captures for each selected node.
		// Re-read from current nodes state to avoid stale parentId.
		for (const selectedNode of selectedNodes) {
			let node = nodes.find(n => n.id === selectedNode.id);
			if (!node) continue;
			if (node.parentId) {
				checkNodeLeavesGroup(node);
				node = nodes.find(n => n.id === selectedNode.id);
			}
			if (node) {
				checkNodeCapturedByGroup(node);
				node = nodes.find(n => n.id === selectedNode.id);
			}
			if (node && (node.type === 'group' || node.type === 'groupCollapsed')) {
				checkGroupCapturesNodes(node);
			}
		}

		// Update weftCode with new positions
		for (const sn of selectedNodes) {
			const n = nodes.find(nd => nd.id === sn.id);
			if (!n) continue;
			layoutUpdateAny(n);
		}
		saveToHistory();
		saveProject();
	}

	let lastScopeBlockToastTime = 0;
	function showScopeBlockedToast() {
		const now = Date.now();
		if (now - lastScopeBlockToastTime < 3000) return;
		lastScopeBlockToastTime = now;
		toast.warning('Cannot change scope', {
			description: 'Disconnect this node from other nodes in its current scope first.',
			duration: 3000,
		});
	}

	function nodeHasConnectionsInScope(nodeId: string, scopeParentId: string | undefined): boolean {
		const sameScope = new Set(
			nodes
				.filter(n => n.id !== nodeId && n.parentId === scopeParentId)
				.map(n => n.id)
		);
		if (scopeParentId) sameScope.add(scopeParentId);
		for (const edge of edges) {
			if (edge.source === nodeId && sameScope.has(edge.target)) return true;
			if (edge.target === nodeId && sameScope.has(edge.source)) return true;
		}
		return false;
	}

	function checkNodeLeavesGroup(node: Node) {
		const parentGroup = nodes.find(n => n.id === node.parentId);
		if (!parentGroup) return;
		
		const { width: groupWidth, height: groupHeight } = getGroupDimensions(parentGroup);
		
		const stillInGroup = 
			node.position.x >= 0 &&
			node.position.x <= groupWidth &&
			node.position.y >= 0 &&
			node.position.y <= groupHeight;
		
		if (!stillInGroup) {
			if (nodeHasConnectionsInScope(node.id, node.parentId)) {
				// Revert to pre-drag position
				const savedPos = preDragPositions.get(node.id);
				if (savedPos) {
					nodes = nodes.map(n => n.id === node.id ? { ...n, position: { ...savedPos } } : n);
				}
				showScopeBlockedToast();
				return;
			}

			const parentAbs = getAbsolutePosition(parentGroup);
			const absoluteX = parentAbs.x + node.position.x;
			const absoluteY = parentAbs.y + node.position.y;
			
			nodes = nodes.map(n => {
				if (n.id !== node.id) return n;
				const newConfig = { ...(n.data.config as Record<string, unknown>) };
				delete newConfig.parentId;
				return {
					...n,
					position: { x: absoluteX, y: absoluteY },
					parentId: undefined,
					extent: undefined,
					data: { ...n.data, config: newConfig },
				};
			});
			weftMoveScopeAny(node, undefined);
		}
	}

	function getAbsolutePosition(n: Node): { x: number; y: number } {
		if (!n.parentId) return { x: n.position.x, y: n.position.y };
		const parent = nodes.find(p => p.id === n.parentId);
		if (!parent) return { x: n.position.x, y: n.position.y };
		const parentAbs = getAbsolutePosition(parent);
		return { x: parentAbs.x + n.position.x, y: parentAbs.y + n.position.y };
	}

	function isDescendantOf(candidateId: string, ancestorId: string): boolean {
		let current = nodes.find(n => n.id === candidateId);
		while (current?.parentId) {
			if (current.parentId === ancestorId) return true;
			current = nodes.find(n => n.id === current!.parentId);
		}
		return false;
	}

	function getGroupDepth(group: Node): number {
		let depth = 0;
		let current: Node | undefined = group;
		while (current?.parentId) {
			depth++;
			current = nodes.find(n => n.id === current!.parentId);
		}
		return depth;
	}

	function checkNodeCapturedByGroup(node: Node) {
		const nodeAbs = getAbsolutePosition(node);

		let bestGroup: Node | null = null;
		let bestDepth = -1;
		let bestArea = Infinity;

		for (const group of nodes) {
			if (group.type !== 'group') continue;
			if (group.id === node.id) continue;
			if (isDescendantOf(group.id, node.id)) continue;
			// Only expanded groups can capture nodes
			if (!((group.data.config as Record<string, unknown>)?.expanded ?? true)) continue;
			// Skip hidden nodes (children of collapsed ancestors)
			if (group.style?.includes('display: none')) continue;

			const groupAbs = getAbsolutePosition(group);
			const { width: groupWidth, height: groupHeight } = getGroupDimensions(group);

			const nodeInGroup =
				nodeAbs.x >= groupAbs.x &&
				nodeAbs.x <= groupAbs.x + groupWidth &&
				nodeAbs.y >= groupAbs.y &&
				nodeAbs.y <= groupAbs.y + groupHeight;

			if (nodeInGroup) {
				const depth = getGroupDepth(group);
				const area = groupWidth * groupHeight;
				// Prefer deepest nesting, break ties by smallest area
				if (depth > bestDepth || (depth === bestDepth && area < bestArea)) {
					bestDepth = depth;
					bestArea = area;
					bestGroup = group;
				}
			}
		}

		if (!bestGroup) return;

		// Already in this group, nothing to do
		if (bestGroup.id === node.parentId) return;

		if (nodeHasConnectionsInScope(node.id, node.parentId)) {
			const savedPos = preDragPositions.get(node.id);
			if (savedPos) {
				nodes = nodes.map(n => n.id === node.id ? { ...n, position: { ...savedPos } } : n);
			}
			showScopeBlockedToast();
			return;
		}

		const groupAbs = getAbsolutePosition(bestGroup);
		const relativeX = nodeAbs.x - groupAbs.x;
		const relativeY = nodeAbs.y - groupAbs.y;

		nodes = nodes.map(n => {
			if (n.id !== node.id) return n;
			const existingConfig = (n.data.config as Record<string, unknown>) || {};
			return {
				...n,
				position: { x: relativeX, y: relativeY },
				parentId: bestGroup!.id,
				data: { ...n.data, config: { ...existingConfig, parentId: bestGroup!.id } },
			};
		});
		weftMoveScopeAny(node, bestGroup!.data.label as string, bestGroup!.id);
		ensureParentBeforeChild();
	}

	function ensureParentBeforeChild() {
		// xyflow requires parent nodes to appear before children in the array.
		// Topologically sort: nodes without parentId first, then children after their parents.
		const indexed = new Map(nodes.map((n, i) => [n.id, i]));
		let needsSort = false;
		for (const n of nodes) {
			if (n.parentId) {
				const parentIdx = indexed.get(n.parentId);
				const childIdx = indexed.get(n.id);
				if (parentIdx !== undefined && childIdx !== undefined && parentIdx > childIdx) {
					needsSort = true;
					break;
				}
			}
		}
		if (!needsSort) return;
		const sorted: Node[] = [];
		const placed = new Set<string>();
		const nodeMap = new Map(nodes.map(n => [n.id, n]));
		function place(n: Node) {
			if (placed.has(n.id)) return;
			if (n.parentId && nodeMap.has(n.parentId) && !placed.has(n.parentId)) {
				place(nodeMap.get(n.parentId)!);
			}
			sorted.push(n);
			placed.add(n.id);
		}
		for (const n of nodes) place(n);
		nodes = sorted;
	}

	function checkGroupCapturesNodes(group: Node) {
		// Collapsed groups don't capture nodes
		if (!((group.data.config as Record<string, unknown>)?.expanded ?? true)) return;

		const groupAbs = getAbsolutePosition(group);
		const { width: groupWidth, height: groupHeight } = getGroupDimensions(group);

		let blocked = false;
		const capturedNodeIds: string[] = [];
		nodes = nodes.map(n => {
			if (n.parentId || n.type === 'group' || n.type === 'groupCollapsed' || n.id === group.id) return n;

			const nodeAbs = getAbsolutePosition(n);
			const nodeInGroup =
				nodeAbs.x >= groupAbs.x &&
				nodeAbs.x <= groupAbs.x + groupWidth &&
				nodeAbs.y >= groupAbs.y &&
				nodeAbs.y <= groupAbs.y + groupHeight;

			if (nodeInGroup) {
				if (nodeHasConnectionsInScope(n.id, n.parentId)) {
					blocked = true;
					return n;
				}
				capturedNodeIds.push(n.id);
				const existingConfig = (n.data.config as Record<string, unknown>) || {};
				return {
					...n,
					position: { x: nodeAbs.x - groupAbs.x, y: nodeAbs.y - groupAbs.y },
					parentId: group.id,
					data: { ...n.data, config: { ...existingConfig, parentId: group.id } },
				};
			}
			return n;
		});
		const groupLabel = group.data.label as string;
		for (const id of capturedNodeIds) {
			const capturedNode = nodes.find(n => n.id === id);
			if (capturedNode) {
				weftMoveScopeAny(capturedNode, groupLabel, group.id);
			}
		}
		if (blocked) showScopeBlockedToast();
		ensureParentBeforeChild();
	}

	function onContextMenu(event: MouseEvent) {
		event.preventDefault();
		
		const flowPos = screenToFlowPosition({ x: event.clientX, y: event.clientY });
		const clickedNodeId = findNodeAtPosition(event.clientX, event.clientY);
		
		contextMenu = {
			x: event.clientX,
			y: event.clientY,
			flowX: flowPos.x,
			flowY: flowPos.y,
			nodeId: clickedNodeId,
		};
	}

	function findNodeAtPosition(clientX: number, clientY: number): string | null {
		const nodeElements = document.querySelectorAll('.svelte-flow__node');
		for (const nodeEl of nodeElements) {
			const rect = nodeEl.getBoundingClientRect();
			if (clientX >= rect.left && clientX <= rect.right && 
				clientY >= rect.top && clientY <= rect.bottom) {
				const nodeId = nodeEl.getAttribute('data-id');
				if (nodeId) return nodeId;
			}
		}
		return null;
	}

	function deleteNode(nodeId: string) {
		deleteNodes([nodeId]);
	}

	function duplicateNode(nodeId: string) {
		if (structuralLock) return;
		const nodeToDuplicate = nodes.find((n) => n.id === nodeId);
		if (!nodeToDuplicate) return;

		const nodeType = nodeToDuplicate.data.nodeType as string;
		const newId = generateNodeId(nodeType);
		const isGroup = nodeToDuplicate.type === 'group' || nodeToDuplicate.type === 'groupCollapsed';
		const newPos = { x: nodeToDuplicate.position.x + 50, y: nodeToDuplicate.position.y + 50 };

		const newNode: Node = {
			...nodeToDuplicate,
			id: newId,
			position: newPos,
			data: {
				...nodeToDuplicate.data,
				label: isGroup ? generateUniqueGroupLabel((nodeToDuplicate.data.label as string) || 'Group') : nodeToDuplicate.data.label,
				onUpdate: createNodeUpdateHandler(newId),
			},
		};
		nodes = [...nodes, newNode];
		selectedNodeId = newId;
		contextMenu = null;

		// Sync to weftCode
		if (isGroup) {
			const groupLabel = newNode.data.label as string;
			weftCode = weftAddGroup(weftCode, groupLabel);
			layoutCode = updateLayoutEntry(layoutCode, groupLabel, newPos.x, newPos.y,
				(newNode.data.config as Record<string, number>)?.width,
				(newNode.data.config as Record<string, number>)?.height);
		} else {
			weftCode = weftAddNode(weftCode, nodeType, newId);
			layoutCode = updateLayoutEntry(layoutCode, newId, newPos.x, newPos.y);
			// Copy config fields from the original node
			const config = nodeToDuplicate.data.config as Record<string, unknown> | undefined;
			if (config) {
				for (const [key, value] of Object.entries(config)) {
					if (['parentId', 'textareaHeights', 'width', 'height', 'expanded'].includes(key)) continue;
					if (value === undefined || value === null || value === '') continue;
					weftCode = weftUpdateConfig(weftCode, newId, key, value);
				}
			}
		}
		saveToHistory();
		saveProject();
	}
	
	function saveProject() {
		onSave({ name: project.name, description: project.description ?? undefined, weftCode, layoutCode });
		flashSaveStatus();
	}

	function flushPendingEdits() {
		if (weftSyncTimer && codeEditInFlight) {
			clearTimeout(weftSyncTimer);
			weftSyncTimer = null;
			codeEditInFlight = false;
			saveProject();
		}
	}

	/// Flush every pending debounced save. Called before the host
	/// kicks off Run / Activate / InfraStart so the build always
	/// sees the user's latest edits, even if the 1s saveProjectTimer
	/// debounce hasn't elapsed yet.
	export function flushAllPendingSaves(): void {
		// Code panel (raw .weft text edits): commit pending sync.
		flushPendingEdits();
		// Config-edit history timer: just save state, no saveWeft.
		if (configEditTimer) {
			clearTimeout(configEditTimer);
			configEditTimer = null;
			saveToHistory();
		}
		// Debounced saveProject from createNodeUpdateHandler. This
		// is the one that actually calls onSave({ weftCode }) which
		// posts saveWeft to the host. Fire it now.
		if (saveProjectTimer) {
			clearTimeout(saveProjectTimer);
			saveProjectTimer = null;
			saveProject();
		}
	}

	// Flush pending edits when the component is destroyed
	$effect(() => {
		return () => { flushPendingEdits(); };
	});

	function flashSaveStatus() {
		saveStatus = 'saved';
		if (saveStatusTimer) clearTimeout(saveStatusTimer);
		saveStatusTimer = setTimeout(() => { saveStatus = 'idle'; }, 2000);
	}

	</script>

<svelte:window onkeydown={handleKeyDown} onbeforeunload={() => { flushPendingEdits(); }} onvisibilitychange={() => { if (document.hidden) { flushPendingEdits(); } }} />

<!-- Command Palette -->
<CommandPalette
	bind:open={commandPaletteOpen}
	onAddNode={addNode}
	onAction={handlePaletteAction}
/>


<!-- VS Code embedding: mobile notice + IDE header toolbar removed.
     The extension host owns title, save status, run/stop controls;
     the webview renders only the graph canvas + context menu +
     right-panel area. -->
<div class="flex flex-col h-full w-full">
	<!-- svelte-ignore a11y_click_events_have_key_events, a11y_no_static_element_interactions -->
	<div 
		class="flex flex-1 relative overflow-hidden"
		oncontextmenu={onContextMenu}
		onclick={() => { if (!justOpenedContextMenu) { contextMenu = null; pendingConnection = null; } }}
	>
	<!-- Main Canvas (code panel removed for VS Code embedding) -->
	<div class="flex-1 relative" oncontextmenucapture={(e: MouseEvent) => {
		const target = e.target as HTMLElement | null;
		if (!target?.closest('.svelte-flow__edgeupdater')) return;
		// Right-click on edge reconnect overlay, find the actual handle underneath
		const els = document.elementsFromPoint(e.clientX, e.clientY);
		const handleEl = els.find(el => el.classList.contains('svelte-flow__handle'));
		if (handleEl) {
			e.preventDefault();
			e.stopPropagation();
			handleEl.dispatchEvent(new MouseEvent('contextmenu', { bubbles: true, clientX: e.clientX, clientY: e.clientY }));
		}
	}}>
		{#if browser}
			<!-- svelte-ignore a11y_no_static_element_interactions -->
			<div class="svelte-flow-wrapper" style="width: 100%; height: 100%;" onwheelcapture={handleWheel}>
			<SvelteFlow
				bind:nodes
				bind:edges
				{nodeTypes}
				{edgeTypes}
				{defaultEdgeOptions}
				{isValidConnection}
				proOptions={{ hideAttribution: true }}
				onconnectstart={onConnectStart}
				onconnectend={onConnectEnd}
				onbeforeconnect={onBeforeConnect}
				onreconnectstart={onReconnectStart}
				onreconnect={onReconnect}
				onreconnectend={onReconnectEnd}
				onnodeclick={onNodeClick}
				onpaneclick={onPaneClick}
				onnodedragstart={onNodeDragStart}
				onnodedragstop={onNodeDragStop}
				onselectiondragstart={(_event, selectedNodes) => { if (selectedNodes.length > 0) onNodeDragStart({ targetNode: selectedNodes[0], event: _event, nodes: selectedNodes }); }}
				onselectiondragstop={onSelectionDragStop}
				onedgeclick={onEdgeClick}
				bind:viewport={currentViewport}
				minZoom={0.05}
				maxZoom={2}
				deleteKey={null}
				selectionKey="Shift"
				multiSelectionKey="Shift"
				zoomActivationKey={null}
				panActivationKey={null}
				selectionOnDrag={false}
				selectionMode={SelectionMode.Partial}
				elementsSelectable={true}
				panOnDrag={true}
				panOnScroll
				zoomOnScroll={false}
				zoomOnPinch={false}
				preventScrolling
				connectionLineType={ConnectionLineType.Straight}
				connectionLineStyle={`stroke-width: 2px; stroke: ${currentConnectionColor};`}
				style={canvasReady ? 'background: #fafafa;' : 'background: #fafafa; opacity: 0; pointer-events: none;'}
			>
				<Controls position="bottom-left" class="!bg-white/90 !border-zinc-200 !rounded [&>button]:!bg-white [&>button]:!border-zinc-200 [&>button]:!text-zinc-500 [&>button:hover]:!bg-zinc-50" />
				<Background bgColor="#fafafa" gap={24} size={1} />
			</SvelteFlow>
			</div>
		{:else}
			<div class="flex items-center justify-center h-full text-muted-foreground">
				Loading editor...
			</div>
		{/if}

		<!-- Divergence warnings -->
		{#if infraState?.infraDiverged}
			<div class="absolute bottom-20 left-1/2 -translate-x-1/2 flex flex-col gap-1.5 items-center z-10">
				<div class="flex items-center gap-2 px-4 py-2 bg-amber-500/90 text-white rounded-lg shadow-lg text-xs font-medium backdrop-blur-sm">
					<svg xmlns="http://www.w3.org/2000/svg" width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><path d="M10.29 3.86L1.82 18a2 2 0 0 0 1.71 3h16.94a2 2 0 0 0 1.71-3L13.71 3.86a2 2 0 0 0-3.42 0z"/><line x1="12" y1="9" x2="12" y2="13"/><line x1="12" y1="17" x2="12.01" y2="17"/></svg>
					Infrastructure has changed. Stop and restart to apply.
				</div>
			</div>
		{/if}

		<!-- Floating action bar. In the VS Code build, 'playground'
		     only hides SaaS-specific controls in CommandPalette and
		     other surfaces; the action bar itself (Run/Stop/infra/
		     trigger controls) is always available. -->
		<ActionBar
			variant="floating"
			{infraState}
			{triggerState}
			{executionState}
			{onCheckInfraStatus}
			{onStartInfra}
			{onStopInfra}
			{onTerminateInfra}
			{onForceRetry}
			{onToggleTrigger}
			{onResyncTrigger}
			{onRun}
			{onStop}
			{onCancelBuild}
			onToggleInfraSubgraph={() => { showInfraSubgraph = !showInfraSubgraph; if (showInfraSubgraph) showTriggerSubgraph = false; }}
			{showInfraSubgraph}
			onToggleTriggerSubgraph={() => { showTriggerSubgraph = !showTriggerSubgraph; if (showTriggerSubgraph) showInfraSubgraph = false; }}
			{showTriggerSubgraph}
			nodeCount={nodes.length}
		/>
	</div>

	<!-- Context Menu -->
	{#if contextMenu}
		<!-- svelte-ignore a11y_click_events_have_key_events, a11y_no_static_element_interactions -->
		<div 
			class="fixed bg-popover border rounded-xl shadow-xl py-1 z-50 min-w-[180px] backdrop-blur-sm"
			style="left: {contextMenu.x}px; top: {contextMenu.y}px;"
			onclick={(e) => e.stopPropagation()}
		>
			{#if contextMenu.nodeId}
				{@const targetNodeId = contextMenu.nodeId}
				{@const nodeToEdit = nodes.find(n => n.id === targetNodeId)}
				{@const nodeConfig = nodeToEdit ? NODE_TYPE_CONFIG[nodeToEdit.data.nodeType as NodeType] : null}
				
				{#if nodeToEdit && nodeConfig}
					<div class="px-1">
						<button
							class="w-full flex items-center gap-2 px-3 py-2 rounded-lg hover:bg-muted text-sm text-left transition-colors"
							onclick={() => duplicateNode(contextMenu!.nodeId!)}
						>
							<span class="text-muted-foreground text-xs">Ctrl+D</span>
							<span>Duplicate</span>
						</button>
						<button
							class="w-full flex items-center gap-2 px-3 py-2 rounded-lg hover:bg-destructive/10 text-sm text-left transition-colors text-destructive"
							onclick={() => deleteNode(contextMenu!.nodeId!)}
						>
							<span class="text-xs">Del</span>
							<span>Delete</span>
						</button>
					</div>
				{/if}
			{:else}
				<!-- Quick Add Menu -->
				<div class="px-1">
					<button
						class="w-full flex items-center gap-2 px-3 py-2 rounded-lg hover:bg-muted text-sm text-left transition-colors"
						onclick={() => { contextMenuFlowPos = contextMenu ? { x: contextMenu.flowX, y: contextMenu.flowY } : null; contextMenu = null; commandPaletteOpen = true; }}
					>
						<span class="text-muted-foreground text-xs">Ctrl+P</span>
						<span>Add Node...</span>
					</button>
					<div class="my-1 mx-2 border-t"></div>
					<button
						class="w-full flex items-center gap-2 px-3 py-2 rounded-lg hover:bg-muted text-sm text-left transition-colors"
						onclick={() => { contextMenu = null; undo(); }}
					>
						<span class="text-muted-foreground text-xs">Ctrl+Z</span>
						<span>Undo</span>
					</button>
					<button
						class="w-full flex items-center gap-2 px-3 py-2 rounded-lg hover:bg-muted text-sm text-left transition-colors"
						onclick={() => { contextMenu = null; redo(); }}
					>
						<span class="text-muted-foreground text-xs">Ctrl+Shift+Z</span>
						<span>Redo</span>
					</button>
				</div>
			{/if}
		</div>
	{/if}

	<!-- VS Code embedding: right-sidebar (Config/Runs/History panels)
	     deleted. The activity-bar Inspector covers node inspection;
	     editing config happens inline on the node body. -->
</div>
</div>

<style>
	/* Z-index order: groups (0) < edge paths (1) < normal nodes (2) < edge labels/anchors (3+) */
	:global(.svelte-flow .svelte-flow__edges) {
		z-index: 1 !important;
	}
	
	:global(.svelte-flow .svelte-flow__node) {
		z-index: 2;
	}

	:global(.svelte-flow .svelte-flow__edge-labels) {
		pointer-events: none;
	}

	:global(.svelte-flow .svelte-flow__edgeupdater) {
		pointer-events: all !important;
	}
	
	:global(.svelte-flow .svelte-flow__node-group) {
		z-index: 0 !important;
		background: transparent !important;
		border: none !important;
		box-shadow: none !important;
		padding: 0 !important;
		text-align: left !important;
	}
	:global(.svelte-flow .svelte-flow__node-group.selected) {
		background: transparent !important;
		border: none !important;
		box-shadow: none !important;
	}
	
	/* Edge styling improvements */
	:global(.svelte-flow .svelte-flow__edge-path) {
		stroke-linecap: round;
		stroke-linejoin: round;
	}
	
	/* Active edge animation */
	:global(.svelte-flow .edge-active .svelte-flow__edge-path) {
		animation: edge-flow 1s linear infinite;
	}
	
	@keyframes edge-flow {
		from {
			stroke-dashoffset: 24;
		}
		to {
			stroke-dashoffset: 0;
		}
	}

	/* New nodes during patch: rendered for measurement but invisible until ELK positions them */
	:global(.svelte-flow__node.node-pending-layout) {
		opacity: 0 !important;
		pointer-events: none !important;
	}

	/* Infrastructure subgraph highlighting */
	:global(.svelte-flow__node.infra-dimmed) {
		opacity: 0.15 !important;
		transition: opacity 0.2s ease;
		pointer-events: none;
	}
	:global(.svelte-flow__node.infra-highlighted) {
		box-shadow: 0 0 0 2px rgba(59, 130, 246, 0.5), 0 0 12px rgba(59, 130, 246, 0.25) !important;
		transition: box-shadow 0.2s ease;
	}
	:global(.svelte-flow .svelte-flow__edge.infra-dimmed) {
		opacity: 0.1 !important;
	}
	:global(.svelte-flow .svelte-flow__edge.infra-highlighted) {
		opacity: 1;
	}

	/* Trigger subgraph highlighting */
	:global(.svelte-flow__node.trigger-dimmed) {
		opacity: 0.15 !important;
		transition: opacity 0.2s ease;
		pointer-events: none;
	}
	:global(.svelte-flow__node.trigger-highlighted) {
		box-shadow: 0 0 0 2px rgba(16, 185, 129, 0.5), 0 0 12px rgba(16, 185, 129, 0.25) !important;
		transition: box-shadow 0.2s ease;
	}
	:global(.svelte-flow .svelte-flow__edge.trigger-dimmed) {
		opacity: 0.1 !important;
	}
	:global(.svelte-flow .svelte-flow__edge.trigger-highlighted) {
		opacity: 1;
	}

</style>
