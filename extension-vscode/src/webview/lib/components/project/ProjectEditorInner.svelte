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
	import type { ProjectDefinition, PortDefinition, NodeFeatures, NodeDataUpdates } from "$lib/types";
	import { isContainerNodeType, isLoopNodeType, containerKindOf } from "$lib/types";
	import type { EditOp, TextEdit } from "../../../../shared/protocol";
	import { PORT_TYPE_COLORS, getPortTypeColor } from "$lib/constants/colors";
	import { autoOrganize } from "$lib/auto-organize";
	import { updateLayoutEntry, removeLayoutEntry, parseLayoutCode, renameLayoutSubtree, applyLayoutOps, diffLayoutOps, type LayoutOp } from "$lib/layout";
	import { formatConfigValue } from "$lib/value-format";
	import { provideFieldEditorRegistry } from "./field-editor-registry";
	import { extractInfraSubgraph } from "$lib/utils/infra-subgraph";
	import { extractTriggerSubgraph } from "$lib/utils/trigger-subgraph";
	import { nodeBodyFeedKind } from "$lib/utils/node-roles";
	import { toast } from "svelte-sonner";


	// Undo/Redo: two stacks of reversible actions (Monaco's model). An action is
	// the inverse edits needed to go one step in a direction: a source TextEdit
	// (applied via the Rust edit-server) and/or a layout LayoutOp batch (applied
	// locally). Applying an undo action yields the redo action (its inverse) and
	// vice-versa. No graph snapshots, no raw-source replay: source actions are
	// minimal text-edit hunks that restore exact bytes (so `@file` survives).
	type ReversibleAction = {
		source?: import('../../../../shared/protocol').TextEdit;
		layout?: import('$lib/layout').LayoutOp[];
	};
	const MAX_HISTORY = 100;
	let undoStack = $state<ReversibleAction[]>([]);
	let redoStack = $state<ReversibleAction[]>([]);

	let {
		project,
		onSave,
		onApplyEdits,
		onApplyTextEdit,
		onRun,
		onStop,
		onDismissError,
		onActivate,
		onCancelActivate,
		onDeactivate,
		onReactivate,
		onCancelRunning,
		onResumeActive,
		onResync,
		onStartInfra,
		onStopInfra,
		onTerminateInfra,
		onInfraNodeStop,
		onInfraNodeTerminate,
		onUpgradeInfra,
		actionBarState,
		drift,
		infraNodes,
		hasInfraInGraph = false,
		hasTriggersInGraph = false,
		executionState,
		autoOrganizeOnMount = false,
		infraFeedByNode,
		signalFeedByNode,
		structuralLock = false,
		onOpenInclude = () => {},
		execPrefix = '',
		fileContents = {},
	}: {
		project: ProjectDefinition;
		onSave: (data: { layoutCode?: string; fileRef?: { path: string; content: string } }) => void;
		// Graph (GUI) edits: emit structured intents; Rust rewrites the source and
		// returns the inverse text edit (the action's undo), or null on failure.
		onApplyEdits: (ops: import('../../../../shared/protocol').EditOp[]) => Promise<import('../../../../shared/protocol').TextEdit | null>;
		// Replay a raw source text edit (undo/redo); returns its inverse.
		onApplyTextEdit: (edit: import('../../../../shared/protocol').TextEdit) => Promise<import('../../../../shared/protocol').TextEdit | null>;
		// Navigate into an @include'd file; host opens it + pushes a back-stack.
		onOpenInclude?: (path: string, alias: string) => void;
		// Dotted alias chain (e.g. `c.`) prepended to node ids for execution
		// value lookup when navigated into an included file. The Return
		// button itself lives in the GraphToolbar (App level), not here.
		execPrefix?: string;
		// Resolved content of @file targets, keyed by the marker's relative
		// path. The display value for file-backed fields (config holds only
		// the `@file(...)` marker, never the resolved content).
		fileContents?: Record<string, import('../../../../shared/protocol').FileContent>;
		// Action-bar verb callbacks. The webview emits these; the
		// host translates each into a CLI shell-out.
		onRun?: () => void;
		onStop?: () => void;
		onDismissError?: () => void;
		onActivate?: () => void;
		onCancelActivate?: () => void;
		onDeactivate?: () => void;
		onReactivate?: () => void;
		onCancelRunning?: () => void;
		onResumeActive?: () => void;
		onResync?: () => void;
		onStartInfra?: () => void;
		onStopInfra?: () => void;
		onTerminateInfra?: () => void;
		/// Per-node infra lifecycle. The graph's node context-menu
		/// emits these when the user right-clicks an infra node.
		/// Routed through the host's CLI verb path so the action
		/// bar's `cli_running` overlay covers them.
		onInfraNodeStop?: (nodeId: string) => void;
		onInfraNodeTerminate?: (nodeId: string) => void;
		onUpgradeInfra?: () => void;
		// Action-bar state machine (host-owned single source of
		// truth) and drift snapshot. Passed straight through to
		// the ActionBar component; nothing in this file decides.
		actionBarState: import('../../../../shared/protocol').ActionBarState;
		drift: import('../../../../shared/protocol').ActionAvailability | undefined;
		// Per-node infra status, keyed by node_id. Used by the
		// graph node decorations (badge under each infra node);
		// independent of the action bar's infra rollup.
		infraNodes?: Array<{ nodeId: string; nodeType: string; status: string; failureStage?: string; failureMessage?: string }>;
		// Source-derived flags from the parsed project: does this
		// graph DECLARE infra / trigger nodes. Drives bar-section
		// visibility (don't show Infra section on a project with
		// no infra nodes).
		hasInfraInGraph?: boolean;
		hasTriggersInGraph?: boolean;
		// Per-execution state for graph decorations: which edges
		// are pulsing right now, last-observed node outputs, etc.
		// Independent of action-bar state.
		executionState?: import('$lib/types').ExecutionState;
		autoOrganizeOnMount?: boolean;
		/// Per-node infra /live tick state. Read for nodes whose
		/// `requiresInfra` flag is true; ignored otherwise.
		infraFeedByNode?: Record<string, import('../../../../shared/protocol').NodeFeedState>;
		/// Per-node listener /display tick state. Read for nodes whose
		/// `features.isTrigger` flag is true; ignored otherwise.
		signalFeedByNode?: Record<string, import('../../../../shared/protocol').NodeFeedState>;
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
	// Child ProjectNodes register their field-editor flushes here so
	// flushAllPendingSaves can commit in-progress typing on teardown.
	const fieldEditorRegistry = provideFieldEditorRegistry();
	let saveStatus = $state<'idle' | 'saved'>('idle');
	let saveStatusTimer: ReturnType<typeof setTimeout> | null = null;

	/** The one scoped-layout-key idiom: a node/group is keyed by its enclosing
	 *  scope + its local id. The single place this string is shaped, so the
	 *  position writer and the scope-move rename can never disagree on it. */
	function scopedLayoutKey(localId: string, parentId: string | undefined): string {
		return parentId ? `${parentId}.${localId}` : localId;
	}

	/** The scoped layout key for a node, from its CURRENT scope (`parentId`) +
	 *  local id, NOT its raw `node.id`. Normally these agree (the parser hands
	 *  back already-scoped ids); they diverge for one frame after a drag
	 *  reparents a node (the capture handler sets the new `parentId` while the
	 *  optimistic `node.id` stays bare until the round-trip re-ids it). Keying
	 *  off `parentId` keeps every writer agreeing on ONE key, so a reparented
	 *  node's position can't orphan itself under a stale key. */
	function getLayoutKey(node: Node): string {
		const parentId = (node.data.config as Record<string, string> | undefined)?.parentId;
		return scopedLayoutKey(node.id.split('.').pop()!, parentId);
	}

	/** Update layout for a node, modifies layoutCode, NOT weftCode. */
	function layoutUpdateAny(node: Node) {
		const cfg = node.data.config as Record<string, unknown> | undefined;
		const key = getLayoutKey(node);
		layoutCode = updateLayoutEntry(layoutCode, key,
			node.position.x, node.position.y,
			cfg?.width as number | undefined, cfg?.height as number | undefined,
			cfg?.expanded as boolean | undefined ?? undefined,
			cfg?.configCollapsed as boolean | undefined ?? undefined);
	}

	/** Move a node or group to a different scope: emit the move intent (Rust
	 *  rewrites the source) and re-key its layout entries (the moved decl AND, for
	 *  a group, its whole subtree) to the new scoped address, then set the moved
	 *  decl's new position. `targetGroupLabel` is the target group's label (undefined
	 *  = top level); `targetGroupId` is its scoped id for the layout key.
	 *
	 *  Layout view-state is the single source of truth for positions/sizes; the
	 *  re-key here is the ONLY layout change a move makes, and `commit` runs it
	 *  before the source round-trip so the pure-merge re-render reads it. No
	 *  carry/remap is needed: the freshly-parsed nodes come back at the new scoped
	 *  ids and the merge finds their (re-keyed) layout entries directly.
	 *
	 *  PRECONDITION: `node` is the node's state BEFORE its `parentId` was reparented,
	 *  so `getLayoutKey(node)` yields the OLD key. The new POSITION is read from the
	 *  live `nodes` array (the caller already wrote the reparented coords there). */
	function weftMoveScopeAny(node: Node, targetGroupLabel: string | undefined, targetGroupId?: string) {
		const oldKey = getLayoutKey(node);
		const isContainer = node.type === 'group' || node.type === 'groupCollapsed';
		// Group/Loop have distinct move ops; the Rust side rejects a
		// mismatched-kind move loudly. The kind check has to happen
		// in this function (callers already reparented the visual)
		// so an unset kind is a return-and-warn (no source op) rather
		// than a throw mid-handler. The visual reparent is left in
		// place; the user can re-trigger the move once the node tag
		// re-hydrates.
		let op: EditOp;
		if (isContainer && node.data.label) {
			const kind = containerKindOf(node.data?.nodeType);
			if (kind === null) {
				console.warn(
					`[move] container ${node.id} reached weftMoveScopeAny with nodeType=${JSON.stringify(node.data?.nodeType)};`,
					'skipping source op until the node re-hydrates'
				);
				return;
			}
			op = kind === 'Loop'
				? { op: 'moveLoopScope', loopId: node.data.label as string, targetGroup: targetGroupLabel ?? null }
				: { op: 'moveGroupScope', group: node.data.label as string, targetGroup: targetGroupLabel ?? null };
		} else {
			op = { op: 'moveNodeScope', node: node.id, targetGroup: targetGroupLabel ?? null };
		}
		const localId = isContainer ? (node.data.label as string) : node.id.split('.').pop()!;
		const newKey = scopedLayoutKey(localId, targetGroupId || targetGroupLabel);
		// The reparented (relative-to-new-parent) position the caller just committed.
		const pos = (nodes.find(n => n.id === node.id) ?? node).position;
		recordEdit([op], () => {
			if (oldKey === newKey) return;
			// Re-key the moved subtree's layout entries to the new address (exact, not
			// a regex prefix-sweep), then set the moved decl's own new position.
			// Descendants keep their (parent-relative) coords under their re-keyed ids.
			layoutCode = renameLayoutSubtree(layoutCode, oldKey, newKey);
			const entry = parseLayoutCode(layoutCode)[newKey];
			layoutCode = updateLayoutEntry(layoutCode, newKey, pos.x, pos.y, entry?.w, entry?.h, entry?.expanded ?? null);
		});
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

	let saveProjectTimer: ReturnType<typeof setTimeout> | null = null;
	const SAVE_DEBOUNCE_MS = 1000;

	let weftInitialized = false;
	function initWeftCode() {
		if (weftInitialized) return;
		weftInitialized = true;
		// The project prop already came from the host's authoritative parse
		// (translateProject), so the graph is rendered; just sync the raw text.
		if (project.weftCode) weftCode = project.weftCode;
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

	/** Parse width/height from an xyflow node's style string + measured fallback */
	function getNodeRect(n: Node): { width: number; height: number } {
		// Try explicit style first
		const wMatch = n.style?.match(/width:\s*(\d+)px/);
		const hMatch = n.style?.match(/height:\s*(\d+)px/);
		const w = wMatch ? parseInt(wMatch[1]) : (n.measured?.width ?? 200);
		const h = hMatch && !n.style?.includes('height: auto') ? parseInt(hMatch[1]) : (n.measured?.height ?? 60);
		return { width: w, height: h };
	}


	/** Write-back for a file-backed config field. The content goes to the
	 *  referenced file via `onSave`, not to the weft source. The `@file(...)`
	 *  token in the source is left untouched, so no surgical weft edit runs. */
	function saveFileRef(path: string, content: string) {
		onSave({ fileRef: { path, content } });
	}

	/** Navigate into an `@include`d file: the host opens that file's graph in
	 *  this panel and pushes the current view onto the back-stack. `alias` is
	 *  the include node id, accumulated into the execution-id prefix. */
	function openInclude(path: string, alias: string) {
		onOpenInclude(path, alias);
	}

	/** Map a local node id to its execution-journal key. When navigated into
	 *  an included file, journal events are keyed by the fully-qualified id
	 *  (e.g. `c.strip`) while this view's nodes are bare (`strip`), so prepend
	 *  the accumulated alias prefix. No-op at the top level (empty prefix). */
	function execKey(localId: string): string {
		return execPrefix + localId;
	}

	function createNodeUpdateHandler(nodeId: string) {
		return (updates: NodeDataUpdates) => {
			// A collapse/expand TOGGLE is `expanded` actually CHANGING value, not
			// merely present. Resize senders spread the whole config (which always
			// carries the node's current `expanded`), so a key-presence test would
			// classify every resize as a toggle and run the heavy collapse path
			// (visibility + viewport pin). Compare against the live value instead:
			// a resize spreads the unchanged flag (not a toggle => falls to resize),
			// a real toggle flips it.
			const priorExpanded = (nodes.find(n => n.id === nodeId)?.data.config as Record<string, boolean> | undefined)?.expanded;
			const nextExpanded = (updates.config as Record<string, boolean> | undefined)?.expanded;
			const isExpandToggle = nextExpanded !== undefined && nextExpanded !== priorExpanded;

			// Capture old group label BEFORE updating (for rename in weft code)
			const oldGroupLabel = ('label' in updates) ? nodes.find(n => n.id === nodeId)?.data.label as string | undefined : undefined;

			// Container-kind precondition. If this update would build a
			// Loop-vs-Group-distinct op (rename / updatePorts) AND the
			// node is a container, resolve the kind BEFORE any visual
			// mutation. A null kind here means the container reached
			// the dispatch with no kind tag (hydration race); bail with
			// a console warning and no source op, leaving the visual
			// state untouched so the user can retry once the node
			// re-hydrates. The earlier shape did this check inline
			// AFTER mutating `nodes`, which left the UI desynced from
			// source on the rare-but-possible bad-state.
			const wouldDispatchKindDistinct = ('label' in updates) || ('inputs' in updates) || ('outputs' in updates);
			if (wouldDispatchKindDistinct) {
				const current = nodes.find(n => n.id === nodeId);
				const isContainer = current?.type === 'group' || current?.type === 'groupCollapsed';
				if (isContainer && containerKindOf(current?.data?.nodeType) === null) {
					console.warn(
						`[edit] container ${nodeId} reached dispatch with nodeType=${JSON.stringify(current?.data?.nodeType)};`,
						'aborting edit until the node re-hydrates'
					);
					return;
				}
			}

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
				return applyNodeSizing(n, newData);
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
					}
					// Visible: restyle through the one sizing ladder, then layer the
					// xyflow parentId (collapsed-parent children detach to top level).
					return { ...applyNodeSizing(n, n.data as Record<string, unknown>), parentId: xyParentId };
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
			// top-right corner stays at the same screen position (no node shifting).
			if (isExpandToggle) {
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

								// Keep edges touching collapsed-group-hidden nodes hidden.
								const currentHidden = new Set(nodes.filter(n => n.style === 'display: none;').map(n => n.id));
								if (currentHidden.size > 0) {
									edges = edges.map(e => {
										const touchesHidden = currentHidden.has(e.source) || currentHidden.has(e.target);
										if (touchesHidden) return { ...e, hidden: true };
										if (e.hidden) return { ...e, hidden: false };
										return e;
									});
								}
								// runAutoOrganize already persisted layout + history.
							});
						});
					});
				});
			}
			
			// When ports change, tell xyflow to re-scan handle bounds so new handles are connectable
			if ('inputs' in updates || 'outputs' in updates) {
				tick().then(() => updateNodeInternals(nodeId));
			}

			// Build structured edit intents from what changed. Rust applies
			// them to the source (the webview never edits `.weft` text). Layout
			// keys (width/height/expanded) are NOT source edits: they go to the
			// companion layout file via layoutUpdateAny.
			const ops: import('../../../../shared/protocol').EditOp[] = [];

			// Layout mutations are deferred into a closure so recordEdit captures
			// them in its before/after diff (one reversible action with the source
			// ops). Layout-only changes (resize/collapse) carry no source ops.
			let mutateLayout: () => void = () => {};
			let hasLayout = false;

			if ('label' in updates) {
				const node = nodes.find(n => n.id === nodeId);
				const isContainer = node?.type === 'group' || node?.type === 'groupCollapsed';
				if (isContainer && oldGroupLabel && updates.label) {
					// Group/Loop have distinct rename ops. The Rust side
					// rejects RenameGroup on a Loop (and vice versa) loudly;
					// routing by container kind is required, not just nicer.
					// Precondition (above) already verified kind is set;
					// `containerKindOf` returns non-null here.
					const kind = containerKindOf(node?.data?.nodeType);
					if (kind === 'Loop') {
						ops.push({ op: 'renameLoop', oldLabel: oldGroupLabel, newLabel: updates.label as string });
					} else {
						ops.push({ op: 'renameGroup', oldLabel: oldGroupLabel, newLabel: updates.label as string });
					}
					// Re-key the group's layout subtree (its own entry + descendants) from
					// the old scoped address to the new one. Same exact, non-compounding
					// re-key a move uses; a rename only changes the last path segment.
					const parts = nodeId.split('.');
					parts[parts.length - 1] = updates.label as string;
					const newPrefix = parts.join('.');
					mutateLayout = () => { layoutCode = renameLayoutSubtree(layoutCode, nodeId, newPrefix); };
					hasLayout = true;
				} else {
					ops.push({ op: 'setLabel', node: nodeId, label: (updates.label as string | null) ?? null });
				}
			}
			if ('config' in updates) {
				const cfg = updates.config!;
				let needsLayout = false;
				for (const [key, value] of Object.entries(cfg)) {
					if (['parentId', 'textareaHeights', '_opaqueChildren'].includes(key)) continue;
					if (['width', 'height', 'expanded', 'configCollapsed'].includes(key)) {
						needsLayout = true;
						continue;
					}
					if (value === undefined || value === null) {
						ops.push({ op: 'removeConfig', node: nodeId, key });
					} else {
						ops.push({ op: 'setConfig', node: nodeId, key, value: formatConfigValue(value) });
					}
				}
				if (needsLayout) {
					mutateLayout = () => {
						const n = nodes.find(nd => nd.id === nodeId);
						if (n) layoutUpdateAny({ ...n, data: { ...n.data, config: cfg } });
					};
					hasLayout = true;
				}
			}
			if ('inputs' in updates || 'outputs' in updates) {
				const node = nodes.find(n => n.id === nodeId);
				if (node?.data) {
					const inputs = toPortSigs((updates.inputs ?? node.data.inputs) as PortLike[]);
					const outputs = toPortSigs((updates.outputs ?? node.data.outputs) as PortLike[]);
					const isContainer = node.type === 'group' || node.type === 'groupCollapsed';
					if (isContainer && node.data.label) {
						// Group/Loop have distinct port-update ops to keep the
						// protocol surface symmetric with rename/remove (the
						// Rust impls share a generic helper but the dispatch
						// validates the decl kind matches the op). Precondition
						// at handler entry already verified kind is set.
						const kind = containerKindOf(node.data.nodeType);
						if (kind === 'Loop') {
							ops.push({ op: 'updateLoopPorts', loopId: node.data.label as string, inputs, outputs });
						} else {
							ops.push({ op: 'updateGroupPorts', group: node.data.label as string, inputs, outputs });
						}
					} else {
						ops.push({ op: 'updateNodePorts', node: nodeId, inputs, outputs });
					}
				}
			}

			// Resize vs collapse/expand are distinct events (see the value-change
			// classifier above). A TOGGLE flips `expanded` and is handled by the
			// isExpandToggle block above (visual restyle + visibility + viewport
			// pin + ELK, which records the layout incl. the toggled flag). A RESIZE
			// changes width/height with `expanded` unchanged: it reflows neighbours
			// via ELK with NO viewport pin.
			// A resize is the USER dragging the resize handle (flagged `resized`), the
			// only dimension change that should re-run ELK so neighbours make room. A
			// programmatic dimension write (min-height auto-enforce, a rebuild after a
			// move) carries width/height too but is NOT a user resize, so it must not
			// reshuffle the graph. Gate on the explicit flag, never on key-presence.
			const isResize = !!updates.resized && !isExpandToggle;

			if (isExpandToggle) {
				// Layout (incl. the toggled `expanded` flag) is recorded by the
				// isExpandToggle block's ELK pass. Only flush any text ops that
				// rode along (rare).
				if (ops.length > 0) { flushPendingConfigOps(); recordEdit(ops); }
			} else if (isResize) {
				// Resize: a new footprint means neighbours should make room, so
				// re-run ELK (no viewport pin); runAutoOrganize records the layout.
				void runAutoOrganize(false);
			} else if ('config' in updates) {
				// Text config typing: accumulate ops across keystrokes and flush as
				// one atomic batch (one undo unit) once typing pauses. The buffer
				// survives handler calls so edits to different fields aren't lost.
				if (ops.length > 0) {
					pendingConfigOps.push(...ops);
					if (saveProjectTimer) clearTimeout(saveProjectTimer);
					saveProjectTimer = setTimeout(() => {
						saveProjectTimer = null;
						flushPendingConfigOps();
					}, SAVE_DEBOUNCE_MS);
				}
			} else if (ops.length > 0 || hasLayout) {
				// Ports/label (structural): flush pending typing first (order), then
				// apply as one reversible action with its layout part.
				flushPendingConfigOps();
				recordEdit(ops, mutateLayout);
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
	// 'IncludedGroup' is the opaque @include block: no catalog entry by
	// design (its ports come from the included file's Group header), so it
	// must be allowed through the catalog filter explicitly.
	const SPECIAL_NODE_TYPES = new Set(['Group', 'Annotation', 'IncludedGroup']);

	function buildNodes(projectNodes: typeof project.nodes, projectEdges: typeof project.edges, layoutMap?: Record<string, { x: number; y: number; w?: number; h?: number; expanded?: boolean; configCollapsed?: boolean }>): Node[] {
		// Pure merge step: overlay each node's layout entry (width/height/expanded)
		// onto its config UP FRONT, so the structural parse (which carries none of
		// this view-state) plus the layout file produce one merged node list. The
		// rest of buildNodes (visibility walk, sizing, the returned config) reads the
		// merged config, and the ancestor-collapse walk sees merged `expanded` too.
		// Position is applied per-node below (also from the layout entry).
		if (layoutMap) {
			projectNodes = projectNodes.map(n => {
				const e = layoutMap[n.id];
				if (!e) return n;
				const cfg = { ...(n.config as Record<string, unknown>) };
				if (e.w !== undefined) cfg.width = e.w;
				if (e.h !== undefined) cfg.height = e.h;
				if (e.expanded !== undefined) cfg.expanded = e.expanded;
				if (e.configCollapsed !== undefined) cfg.configCollapsed = e.configCollapsed;
				return { ...n, config: cfg };
			}) as typeof projectNodes;
		}
		// Unknown node types are already handled by the parser as opaque blocks
		// (they never reach project.nodes), so we only need to filter for known types.
		const validNodes = projectNodes.filter(n =>
			SPECIAL_NODE_TYPES.has(n.nodeType) || NODE_TYPE_CONFIG[n.nodeType]
		);

		// xyflow requires parent nodes to appear before children in the array.
		// Topologically sort groups so parent groups come first, then non-group nodes.
		const groupNodes = validNodes.filter(n => isContainerNodeType(n.nodeType));
		const otherNodes = validNodes.filter(n => !isContainerNodeType(n.nodeType));
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
			const isGroup = isContainerNodeType(n.nodeType);
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

			const configWidth = (n.config as Record<string, number>)?.width;
			const configHeight = (n.config as Record<string, number>)?.height;
			const isExpanded = (n.config as Record<string, boolean>)?.expanded ?? (isGroup ? true : false);

			// Nesting depth so child groups render above parent groups.
			let nestingDepth = 0;
			if (isGroup && isExpanded && rawParentId) {
				let pid: string | undefined = rawParentId;
				while (pid) {
					nestingDepth++;
					const p = projectNodes.find(g => g.id === pid);
					pid = p ? (p.config as Record<string, string>)?.parentId : undefined;
				}
			}
			// One sizing ladder (shared with applyNodeSizing). Group expanded dims
			// fall back to the saved layout entry when config doesn't specify.
			const layoutEntry = layoutMap?.[n.id];
			const sizing = computeSizing({
				isGroup,
				isAnnotation,
				isExpanded,
				configWidth,
				configHeight,
				fallbackWidth: layoutEntry?.w,
				fallbackHeight: layoutEntry?.h,
				inputs: n.inputs,
				outputs: n.outputs,
				nestingDepth,
			});

			// Position is layout's job: the merge places a node at its saved layout
			// entry when present, else at the node's own position (the structural
			// parse emits 0,0; a just-typed node with no layout falls to the caller's
			// placement). This is what makes buildNodes(project, layout) a pure merge.
			const position = layoutEntry ? { x: layoutEntry.x, y: layoutEntry.y } : n.position;

			return {
				id: n.id,
				type: sizing.type,
				position,
				zIndex: sizing.zIndex,
				...(sizing.width !== undefined ? { width: sizing.width } : {}),
				...(sizing.height !== undefined ? { height: sizing.height } : {}),
				data: {
					label: n.label,
					nodeType: n.nodeType,
					config: n.config,
					inputs: n.inputs,
					outputs: n.outputs,
					features: n.features,
					fileContents,
					includePath: (n as typeof n & { includePath?: string }).includePath,
					sourceLine: (n as typeof n & { sourceLine?: number }).sourceLine,
					onUpdate: createNodeUpdateHandler(n.id),
					onSaveFileRef: saveFileRef,
					onOpenInclude: openInclude,
					infraNodeStatus: infraNodes?.find(inf => inf.nodeId === n.id)?.status,
					infraFailureStage: infraNodes?.find(inf => inf.nodeId === n.id)?.failureStage,
					infraFailureMessage: infraNodes?.find(inf => inf.nodeId === n.id)?.failureMessage,
				},
				...(hiddenByCollapsedGroup
					? { style: 'display: none;' }
					: { style: sizing.style }),
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
				animated: false,
				zIndex: 5,
				style: `stroke-width: 2px; stroke: ${edgeColor};`,
				markerEnd: {
					type: MarkerType.ArrowClosed,
					width: 20,
					height: 20,
					color: edgeColor,
				},
			};
		});
	}

	// svelte-ignore state_referenced_locally
	let edges = $state.raw<Edge[]>(buildEdges(project.edges, project.nodes));

	// Memoize the participants→buses inversion across $effect ticks.
	// `busParticipantsByBus` is replaced by reference only when a new
	// participant lands (see App.svelte's bus_participant handler), so
	// reference equality with the prior tick means the inversion is
	// unchanged. Without this, every nodeOutputs/nodeExecutions tick
	// (which happens on every SSE event) re-inverts and re-allocates
	// the whole nodes array, quadratic with execution age.
	let busesByNodeCache: { ref: unknown; value: Record<string, string[]> } = {
		ref: undefined,
		value: {},
	};

	$effect(() => {
		const state = executionState;
		if (state) {
			const nodeOutputs = state.nodeOutputs || {};
			const nodeExecutions = state.nodeExecutions || {};
			const busLogByBus = state.busLogByBus || {};
			const busParticipantsByBus = state.busParticipantsByBus || {};
			// Read these THREE in the tracked region (above the untrack
			// below) so the effect re-runs when they change. App.svelte
			// mutates them in place (executionState.loopEventsByGroup =
			// {...}, etc.) without replacing the whole executionState
			// object, so a read buried inside untrack() would never
			// register the dependency: the inspector would show stale
			// loop events / corruption markers / bus metadata until an
			// unrelated tick replaced executionState wholesale.
			const loopEventsByGroup = state.loopEventsByGroup ?? {};
			const journalCorruptions = state.journalCorruptions ?? [];
			const busMetaByBus = state.busMetaByBus ?? {};
			let busesByNode: Record<string, string[]>;
			if (busesByNodeCache.ref === busParticipantsByBus) {
				busesByNode = busesByNodeCache.value;
			} else {
				busesByNode = {};
				for (const [busId, participants] of Object.entries(busParticipantsByBus)) {
					for (const nodeId of participants) {
						(busesByNode[nodeId] ??= []).push(busId);
					}
				}
				busesByNodeCache = { ref: busParticipantsByBus, value: busesByNode };
			}

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
						const hasOutputs = (nodeTypeConfig.defaultOutputs?.length ?? 0) > 0;
						if (hasOutputs) {
							debugData = nodeOutputs[execKey(n.id)];
						} else {
							const rows = nodeExecutions[execKey(n.id)];
							const latest = rows?.[rows.length - 1];
							debugData = latest?.input;
						}
					}

					let executions: import('$lib/types').NodeExecution[];

					if (isContainerNodeType(nodeType)) {
						const groupId = n.id;

						// Boundary passthrough executions (compiled IDs follow {groupId}__in / {groupId}__out)
						const inExecs = nodeExecutions[execKey(`${groupId}__in`)] || [];
						const outExecs = nodeExecutions[execKey(`${groupId}__out`)] || [];

						// Collect internal node executions via scope field
						const internalExecs: import('$lib/types').NodeExecution[] = [];
						for (const projNode of project.nodes) {
							if (projNode.scope?.includes(groupId) && nodeExecutions[execKey(projNode.id)]) {
								internalExecs.push(...nodeExecutions[execKey(projNode.id)]);
							}
						}

						// Pair the __out boundary execution to its __in by
						// FRAME STACK, not array index. A parallel loop fires
						// many iterations whose __in / __out events interleave
						// and arrive out of order, so the i-th __in is not the
						// i-th __out; matching by framesKey ties each iteration's
						// in to its own out. (Sequential loops and plain groups
						// happen to line up by index, but framesKey is correct
						// for all of them.)
						const outByFrames = new Map(outExecs.map((e) => [e.framesKey, e]));

						// Build synthetic execution: one per __in execution
						executions = inExecs.map((inExec) => {
							const outExec = outByFrames.get(inExec.framesKey);
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
								// Frame-stack-keyed id so an iteration's card keeps
								// a stable identity across ticks even as sibling
								// iterations' events interleave (index would shuffle).
								id: `${groupId}-synth-${inExec.framesKey}`,
								nodeId: groupId,
								status,
								pulseIdsAbsorbed: inExec.pulseIdsAbsorbed,
								pulseId: inExec.pulseId,
								error: outExec?.error ?? inExec.error,
								startedAt: inExec.startedAt,
								completedAt: outExec?.completedAt ?? inExec.completedAt,
								input: inExec.output, // __in output = what flows into the group
								output: outExec?.output, // __out output = what the group produces
								// Closed input ports surface from the `__in` boundary
								// (its own `closedPorts` is the set of group-level
								// inputs that arrived as closures). Without this, a
								// group inspector with closed inputs renders a
								// blank Input panel where the per-node inspector
								// shows `port: (closed)` rows.
								closedPorts: inExec.closedPorts,
								costUsd: allRelated.reduce((sum, e) => sum + (e.costUsd || 0), 0),
								logs: [],
								color: inExec.color,
								frames: inExec.frames,
								framesKey: inExec.framesKey,
							};
						});
					} else {
						executions = nodeExecutions[execKey(n.id)] || [];
					}

					// Derive SvelteFlow wrapper class from the latest execution status
					const latestExec = executions[executions.length - 1];
					const execStatus = latestExec?.status;
					const nodeClass = execStatus === 'running' || execStatus === 'waiting_for_input' ? 'node-running'
						: execStatus === 'failed' ? 'node-failed'
						: execStatus === 'completed' || execStatus === 'skipped' ? 'node-completed'
						: '';

					// Bus panels: for an ordinary node, the buses this node
					// participated in directly. For a group, the union of
					// every internal node's bus participation (so the
					// group's modal shows the conversations its members
					// had with each other or with the outside).
					const busIdsForNode = new Set<string>(busesByNode[execKey(n.id)] ?? []);
					if (isContainerNodeType(n.data?.nodeType)) {
						for (const projNode of project.nodes) {
							if (projNode.scope?.includes(n.id)) {
								for (const busId of busesByNode[execKey(projNode.id)] ?? []) {
									busIdsForNode.add(busId);
								}
							}
						}
					}
					// Sort by busId so panel order is stable across SSE
					// ticks. Without this, every new participant could
					// shuffle the order in which panels render (Set
					// insertion order leaks to the user).
					const busLogs = Array.from(busIdsForNode).sort().map((busId) => ({
						busId,
						events: busLogByBus[busId] ?? [],
						meta: busMetaByBus[busId],
					}));

					const loopEvents = isLoopNodeType(n.data?.nodeType)
						? (loopEventsByGroup[execKey(n.id)] ?? [])
						: [];

					return {
						...n,
						data: {
							...n.data,
							debugData,
							executions,
							executionCount: executions.length,
							busLogs,
							loopEvents,
							journalCorruptions,
						},
						class: nodeClass,
					};
				});
				
			});
		}
	});

	// Keep per-node infra status badges in sync with backend state
	$effect(() => {
		const list = infraNodes;
		if (!list) return;
		untrack(() => {
			nodes = nodes.map(n => {
				const backendNode = list.find(inf => inf.nodeId === n.id);
				const newStatus = backendNode?.status;
				const newStage = backendNode?.failureStage;
				const newMsg = backendNode?.failureMessage;
				if (
					n.data.infraNodeStatus !== newStatus ||
					n.data.infraFailureStage !== newStage ||
					n.data.infraFailureMessage !== newMsg
				) {
					return {
						...n,
						data: {
							...n.data,
							infraNodeStatus: newStatus,
							infraFailureStage: newStage,
							infraFailureMessage: newMsg,
						},
					};
				}
				return n;
			});
		});
	});

	// buildNodes bakes the current fileContents into each node at construction.
	// This effect handles the LATE-ARRIVAL case: fileContents is its own
	// message (it lands after parseResult, and again on external file change
	// with no rebuild), so patch the already-built nodes when it updates. Only
	// reassign `nodes` if some node actually needed patching, so a no-op reship
	// (common: periodic external-file reships) doesn't allocate a fresh array
	// and retrigger every other derivation that reads `nodes`.
	$effect(() => {
		const contents = fileContents;
		untrack(() => {
			let changed = false;
			const next = nodes.map(n => {
				if (n.data.fileContents === contents) return n;
				changed = true;
				return { ...n, data: { ...n.data, fileContents: contents } };
			});
			if (changed) nodes = next;
		});
	});

	// Per-node body-panel feed. Each node consumes AT MOST ONE feed
	// based on its role: infra nodes get infra /live ticks, trigger
	// nodes get listener /display ticks, anything else gets nothing.
	// Roles are mutually exclusive (see lib/utils/node-roles); the
	// `bodyFeed` field on node.data is what ProjectNode.svelte renders.
	$effect(() => {
		const infra = infraFeedByNode;
		const signal = signalFeedByNode;
		untrack(() => {
			nodes = nodes.map(n => {
				const d = n.data as Record<string, unknown>;
				const role = nodeBodyFeedKind({
					nodeType: d.nodeType as string,
					features: d.features as { isTrigger?: boolean } | undefined,
				});
				const feed =
					role === 'infra' ? infra?.[n.id]
					: role === 'signal' ? signal?.[n.id]
					: undefined;
				const prev = d.bodyFeed as import('../../../../shared/protocol').NodeFeedState | undefined;
				if (feed === prev) return n;
				return { ...n, data: { ...n.data, bodyFeed: feed } };
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
				edges = edges.map(e => ({ ...e, class: '' }));
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

	// Commit one reversible action: mutate layout (the in-memory layoutCode edits)
	// and apply a source change, then return the INVERSE action. The layout inverse
	// is a diff of layoutCode after-vs-before; the source inverse is the text edit
	// the edit-server returns. Backs new edits (recordEdit) and undo/redo replay.
	//
	// Layout is applied BEFORE the source round-trip, not after. The round-trip's
	// re-parse re-renders the graph as a pure merge of (parsed source, layoutCode);
	// it reads the IN-MEMORY layoutCode, so a move's layout re-key must already be
	// in place or the moved node would render at a stale position for a frame (the
	// old source-first ordering, plus carry/remap compensations, was exactly this
	// race). Failure safety is kept by snapshotting layoutCode and rolling it back
	// if the source op throws, so a rejected edit never strands a layout entry.
	async function commit(
		applySource: () => Promise<TextEdit | null>,
		mutateLayout: () => void,
	): Promise<ReversibleAction> {
		const layoutBefore = layoutCode;
		mutateLayout();
		// While a GUI edit is in flight, the webview's in-memory layoutCode is the
		// authoritative copy (it already holds this edit's re-key). The round-trip's
		// parseResult echoes the host's disk layout, which lags; `applyExternalSource`
		// checks this flag and keeps our copy instead of clobbering it.
		editInFlight++;
		let source: TextEdit | undefined;
		try {
			source = (await applySource()) ?? undefined;
		} catch (err) {
			layoutCode = layoutBefore; // roll back the optimistic layout on a rejected source op
			throw err;
		} finally {
			editInFlight--;
		}
		if (layoutCode !== layoutBefore) saveLayout();
		const layout = diffLayoutOps(layoutCode, layoutBefore); // ops: after -> before
		return { source, layout: layout.length > 0 ? layout : undefined };
	}
	// >0 while a GUI edit round-trip is in flight (see `commit`). Guards layout
	// ownership in `applyExternalSource`.
	let editInFlight = 0;

	// All graph mutations (new edits + undo/redo replay) run through this one
	// queue. They share mutable state (layoutCode, the undo/redo stacks) and each
	// awaits an async source round-trip, so serializing keeps that state
	// consistent under rapid edits / ctrl-z held down (no interleaving).
	let historyChain: Promise<void> = Promise.resolve();
	function enqueue(fn: () => Promise<void>): void {
		historyChain = historyChain.then(fn, fn);
	}

	// When a gesture spans several recordEdit calls (e.g. a drag that also
	// reparents nodes), they accumulate here so the whole gesture is ONE undo
	// unit. `transaction(fn)` opens it; recordEdit calls inside fn buffer their
	// ops + layout closures; on close they commit as a single action.
	type BufferedEdit = { ops: EditOp[]; mutateLayout: () => void };
	let txBuffer: BufferedEdit[] | null = null;
	function transaction(fn: () => void): void {
		if (txBuffer) { fn(); return; } // already inside one: just nest
		const buffer: BufferedEdit[] = [];
		txBuffer = buffer;
		try {
			fn();
		} finally {
			txBuffer = null;
		}
		if (buffer.length === 0) return;
		const ops = buffer.flatMap(e => e.ops);
		const mutateLayout = () => { for (const e of buffer) e.mutateLayout(); };
		pushAction(ops, mutateLayout);
	}

	// Record a user edit as a reversible action (or buffer it into the open
	// transaction). Callers pass the source ops (Rust applies them, returns the
	// inverse) and optionally a layout mutation closure (the existing inline
	// layoutCode edits). History bookkeeping is automatic: no saveToHistory.
	function recordEdit(ops: EditOp[], mutateLayout: () => void = () => {}): void {
		if (txBuffer) { txBuffer.push({ ops, mutateLayout }); return; }
		pushAction(ops, mutateLayout);
	}

	function pushAction(ops: EditOp[], mutateLayout: () => void): void {
		enqueue(async () => {
			const undoAction = await commit(() => (ops.length > 0 ? onApplyEdits(ops) : Promise.resolve(null)), mutateLayout);
			if (undoAction.source || undoAction.layout) {
				undoStack = [...undoStack, undoAction].slice(-MAX_HISTORY);
				redoStack = [];
			}
		});
	}

	// Replay a stored action (undo/redo): source half is a text edit replayed
	// through the edit-server, layout half is layout ops applied locally.
	function applyAction(a: ReversibleAction): Promise<ReversibleAction> {
		return commit(
			() => (a.source ? onApplyTextEdit(a.source) : Promise.resolve(null)),
			() => {
				if (!a.layout) return;
				layoutCode = applyLayoutOps(layoutCode, a.layout);
				// A layout undo has no reparse to re-render from, so push the new
				// layoutCode onto the live node positions/sizes.
				reconcileNodesFromLayout();
			},
		);
	}

	// Compute a node's type/style/zIndex/dimensions from its `data` (which holds
	// config.expanded + config.width/height). The single source of truth for
	// node sizing: used by the optimistic config-edit update AND by layout
	// undo/redo reconciliation, so a collapsed node always renders collapsed (it
	// never gets blown up to its saved expanded size) regardless of which path
	// last touched it.
	// The ONE node-sizing ladder: given what a node IS (group/annotation/node),
	// whether it's expanded, its config + fallback dimensions, and its inputs/
	// outputs, decide its xyflow type / zIndex / style / explicit w-h. Both
	// buildNodes (constructing from the parse) and applyNodeSizing (restyling a
	// live node) route through this, so a collapsed node renders min-width/auto
	// no matter which path touched it last (no second ladder to drift).
	type SizingInput = {
		isGroup: boolean;
		isAnnotation: boolean;
		isExpanded: boolean;
		configWidth?: number;
		configHeight?: number;
		fallbackWidth?: number;  // group expanded dims when config has none (layout entry / live rect)
		fallbackHeight?: number;
		inputs?: PortDefinition[];
		outputs?: PortDefinition[];
		nestingDepth?: number;   // expanded groups stack above their parents
	};
	type Sizing = { type: 'group' | 'groupCollapsed' | 'annotation' | 'project'; zIndex: number; style: string; width?: number; height?: number };
	function computeSizing(s: SizingInput): Sizing {
		if (s.isAnnotation) {
			return { type: 'annotation', zIndex: -1, style: `width: ${s.configWidth || 250}px; height: ${s.configHeight || 120}px;` };
		}
		if (s.isGroup) {
			if (s.isExpanded) {
				const w = s.configWidth || s.fallbackWidth || 400;
				const h = s.configHeight || s.fallbackHeight || 300;
				return { type: 'group', zIndex: -1 + (s.nestingDepth ?? 0), style: `width: ${w}px; height: ${h}px;` };
			}
			const minW = computeMinNodeWidth(s.inputs, s.outputs);
			return { type: 'groupCollapsed', zIndex: 4, style: `width: ${minW}px; height: auto;` };
		}
		// Regular node: collapsed (default) = min-width/auto; expanded = saved
		// w/h (>= minW), else fit. A collapsed node ignores any saved w/h.
		const minW = computeMinNodeWidth(s.inputs, s.outputs);
		if (!s.isExpanded) {
			return { type: 'project', zIndex: 4, style: `width: ${minW}px; height: auto;` };
		}
		if (s.configWidth && s.configHeight) {
			const w = Math.max(s.configWidth, minW);
			return { type: 'project', zIndex: 4, style: `width: ${w}px; height: ${s.configHeight}px;`, width: w, height: s.configHeight };
		}
		return { type: 'project', zIndex: 4, style: `width: ${Math.max(320, minW)}px; height: auto;` };
	}

	// Restyle a live xyflow node from its (possibly just-edited) data, via the
	// one ladder. Used by the optimistic config-edit update + layout undo/redo.
	function applyNodeSizing(n: Node, newData: Record<string, unknown>): Node {
		const cfg = newData.config as Record<string, unknown> | undefined;
		const isGroup = n.type === 'group' || n.type === 'groupCollapsed';
		const rect = isGroup ? getNodeRect(n) : undefined; // preserve current group dims if config has none (no flash)
		const sizing = computeSizing({
			isGroup,
			isAnnotation: n.type === 'annotation',
			isExpanded: (cfg?.expanded as boolean) ?? (isGroup ? true : false),
			configWidth: cfg?.width as number | undefined,
			configHeight: cfg?.height as number | undefined,
			fallbackWidth: rect?.width,
			fallbackHeight: rect?.height,
			inputs: (newData.inputs ?? cfg?.inputs) as PortDefinition[] | undefined,
			outputs: (newData.outputs ?? cfg?.outputs) as PortDefinition[] | undefined,
		});
		return {
			...n,
			type: sizing.type,
			data: newData,
			zIndex: sizing.zIndex,
			style: sizing.style,
			width: sizing.width,
			height: sizing.height,
		};
	}

	// Push the current layoutCode (positions + sizes + expanded state) onto the
	// live xyflow nodes. A layout undo/redo has no reparse to re-render from, so
	// reconcile here, routing sizing through applyNodeSizing so a collapsed node
	// stays collapsed (its saved expanded w/h is remembered, not applied).
	function reconcileNodesFromLayout() {
		const map = parseLayoutCode(layoutCode);
		nodes = nodes.map((n) => {
			const e = map[n.id];
			if (!e) return n;
			const config = { ...(n.data.config as Record<string, unknown>) };
			if (e.w !== undefined) config.width = e.w;
			if (e.h !== undefined) config.height = e.h;
			if (e.expanded !== undefined) config.expanded = e.expanded;
			const sized = applyNodeSizing(n, { ...n.data, config });
			return { ...sized, position: { x: e.x, y: e.y } };
		});
	}

	function undo() {
		enqueue(async () => {
			const a = undoStack[undoStack.length - 1];
			if (!a) return;
			const inverse = await applyAction(a); // may throw; swap stacks only on success
			undoStack = undoStack.slice(0, -1);
			redoStack = [...redoStack, inverse];
		});
	}
	function redo() {
		enqueue(async () => {
			const a = redoStack[redoStack.length - 1];
			if (!a) return;
			const inverse = await applyAction(a);
			redoStack = redoStack.slice(0, -1);
			undoStack = [...undoStack, inverse];
		});
	}

	// Seed the working source copy from the prop on first mount.
	$effect(() => { initWeftCode(); });

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
				let updated = n;
				if (pos) updated = { ...updated, position: pos };
				if (groupSize) {
					const w = groupSize.width;
					const h = groupSize.height;
					const newConfig = { ...(updated.data.config as Record<string, unknown>), width: w, height: h };
					updated = { ...updated, style: `width: ${w}px; height: ${h}px;`, data: { ...updated.data, config: newConfig } };
				}
				return updated;
			});
			// ELK positions are LAYOUT, not source: record as one reversible
			// layout action (recordEdit persists + captures the diff for undo).
			// Persist EVERY visible node, not only the ones ELK repositioned:
			// ELK may leave a node in place (no `positions` entry), and a
			// collapse/expand toggle drives this path precisely to persist the
			// toggled node's `expanded` flag. Gating on ELK movement dropped that
			// flag for untouched nodes, so they snapped back to their default on
			// the next rebuild (the intermittent recollapse). Hidden nodes
			// (display:none under a collapsed ancestor) keep their stored entry.
			recordEdit([], () => {
				for (const n of nodes) {
					if (n.style === 'display: none;') continue;
					layoutUpdateAny(n);
				}
			});
			if (andFitView) setTimeout(() => doFitView(), 50);
		});
	}

	/** Apply a host parseResult in place (no remount): the host owns the
	 *  authoritative .weft text and re-sends it on direct file edits, focus
	 *  changes, background ticks, etc.
	 *
	 *  Staleness guard: GUI config edits are buffered (pendingConfigOps) and
	 *  flushed on a ~1s `saveProjectTimer`. While that timer is pending, a
	 *  host-side reparse triggered by something else (a focus-change, a `ready`
	 *  re-emit) can echo the PRE-edit source back. Accepting it would revert the
	 *  not-yet-flushed edit, so we drop any echo that differs from our working
	 *  copy while a flush is pending; the round-trip after the flush reconciles. */
	export function applyExternalSource(newProject: ProjectDefinition, newWeftCode: string, newLayoutCode: string): void {
		if (saveProjectTimer !== null && newWeftCode !== weftCode) {
			return;
		}
		// Layout ownership: the webview owns layout view-state. During a GUI edit
		// round-trip (`editInFlight > 0`) our in-memory layoutCode already holds this
		// edit's re-key, while the host's echoed copy is read from disk and lags, so
		// we keep ours. We adopt the host's layout only for a genuinely external
		// change (a direct `.weft`/`.layout` edit with no GUI edit in flight), where
		// the host is authoritative. This is what lets a move's re-key survive the
		// round-trip without any carry/remap compensation.
		if (editInFlight === 0 && newLayoutCode !== layoutCode) {
			layoutCode = newLayoutCode;
		}
		// Fast path: source text unchanged, but the compiler may have re-inferred
		// port metadata (concrete portTypes resolved from TypeVars).
		// Patch ports in place without rebuilding the node list or re-running ELK.
		if (newWeftCode === weftCode) {
			mergeInferredPortMetadata(newProject);
			return;
		}
		weftCode = newWeftCode;
		void patchFromProject(newProject);
	}

	/// Update each existing graph node's `data.inputs` / `data.outputs`
	/// to match the freshly-parsed project's port metadata. Used when
	/// the source text didn't change but the compiler may have
	/// resolved TypeVars to concrete
	/// portTypes. Only touches port arrays; positions, configs,
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
			if (!!pa.required !== !!pb.required) return false;
		}
		return true;
	}

	/// Re-render the graph from a structural parse (the round-trip after an
	/// edit). This NEVER auto-organizes: a structural edit (field change,
	/// delete, add-edge, move, hand-placed add) must not reshuffle the graph.
	/// ELK relayout is a separate, explicit thing driven by the layout side
	/// (resize, expand/collapse) or the toolbar Auto-organize button, which
	/// call `runAutoOrganize` directly. Fresh-mount layout is the mount $effect.
	export async function patchFromProject(newProject: ProjectDefinition): Promise<void> {
		// Pure merge: the rendered graph is a function of (parsed source, layout).
		// `buildNodes` already merges each node with its layout entry (position,
		// size, expanded) by scoped id. Because a move re-keys the layout to the new
		// scoped address BEFORE this runs (commit applies layout, then the source
		// round-trip), every freshly-parsed node finds its layout entry directly.
		// No state is carried across the re-parse, so there is nothing to reconcile,
		// no id-remap, no size-hack: the merge IS the truth. A node with no layout
		// entry (e.g. one just typed into the code panel) is placed below existing
		// content so it's visible without an ELK pass shuffling everything.
		const layoutMap = parseLayoutCode(layoutCode);
		let nextFreeY = 0;
		for (const n of nodes) nextFreeY = Math.max(nextFreeY, n.position.y + (n.measured?.height ?? 100) + 40);
		nodes = buildNodes(newProject.nodes, newProject.edges, layoutMap).map(n => {
			if (layoutMap[n.id]) return n; // buildNodes already applied the layout entry
			const pos = { x: 0, y: nextFreeY };
			nextFreeY += 140;
			return { ...n, position: pos };
		});

		const currentEdgeIds = new Set(edges.map(e => e.id));
		edges = buildEdges(newProject.edges, newProject.nodes).map(e =>
			currentEdgeIds.has(e.id) ? e : { ...e }
		);
		await tick();
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
			case 'delete': {
				// Selected edges take priority over nodes (matches canvas Delete).
				const selectedEdges = edges.filter(e => e.selected);
				if (selectedEdges.length > 0) {
					recordEdit(selectedEdges.map(e => {
						const ref = toWeftEdgeRef(e.source, e.sourceHandle || 'value', e.target, e.targetHandle || 'value');
						return { op: 'removeEdge' as const, source: ref.srcRef, sourcePort: ref.srcPort, target: ref.tgtRef, targetPort: ref.tgtPort, scopeGroup: ref.scopeGroupLabel ?? null };
					}));
					edges = edges.filter(e => !e.selected);
					break;
				}
				const selectedNodes = nodes.filter(n => n.selected);
				if (selectedNodes.length > 0) deleteNodes(selectedNodes.map(n => n.id));
				else if (selectedNodeId) deleteNodes([selectedNodeId]);
				break;
			}
			case 'escape':
				contextMenu = null;
				pendingConnection = null;
				break;
			case 'autoOrganize': {
				// Re-layout the current graph with ELK. No re-parse (rendered
				// nodes are current); runAutoOrganize persists the layout itself.
				void runAutoOrganize(true);
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

		// Remove old edge, add new one: one atomic batch.
		const oldRef = toWeftEdgeRef(oldEdge.source, oldEdge.sourceHandle || 'value', oldEdge.target, oldEdge.targetHandle || 'value');
		const newRef = toWeftEdgeRef(newConnection.source, newConnection.sourceHandle || 'value', newConnection.target, newConnection.targetHandle || 'value');
		recordEdit([
			{ op: 'removeEdge', source: oldRef.srcRef, sourcePort: oldRef.srcPort, target: oldRef.tgtRef, targetPort: oldRef.tgtPort, scopeGroup: oldRef.scopeGroupLabel ?? null },
			{ op: 'addEdge', source: newRef.srcRef, sourcePort: newRef.srcPort, target: newRef.tgtRef, targetPort: newRef.tgtPort, scopeGroup: newRef.scopeGroupLabel ?? null },
		]);

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
	}

	// eslint-disable-next-line @typescript-eslint/no-explicit-any
	function onReconnectEnd(event: MouseEvent | TouchEvent, edge: any) {
		// If reconnection wasn't successful (dropped on empty space), delete the edge
		if (!reconnectSuccessful && !structuralLock) {
			const ref = toWeftEdgeRef(edge.source, edge.sourceHandle || 'value', edge.target, edge.targetHandle || 'value');
			recordEdit([{ op: 'removeEdge', source: ref.srcRef, sourcePort: ref.srcPort, target: ref.tgtRef, targetPort: ref.tgtPort, scopeGroup: ref.scopeGroupLabel ?? null }]);
			edges = edges.filter(e => e.id !== edge.id);
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
		
		// Emit the add-edge intent after the optimistic edge is added.
		setTimeout(() => {
			const ref = toWeftEdgeRef(connection.source!, sourceHandle || 'value', connection.target!, targetHandle || 'value');
			recordEdit([{ op: 'addEdge', source: ref.srcRef, sourcePort: ref.srcPort, target: ref.tgtRef, targetPort: ref.tgtPort, scopeGroup: ref.scopeGroupLabel ?? null }]);
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
		const typeConfig = NODE_TYPE_CONFIG[type];
		const isGroup = type === 'Group';
		const isLoop = type === 'Loop';
		const isContainer = isGroup || isLoop;
		const isAnnotation = type === 'Annotation';
		// A container is declared in source by its label (`MyGroup = Group()...`
		// or `MyLoop = Loop()...`), so the label IS its node id once parsed.
		// Seed names from a safe default, NOT the type's display label:
		// "Group" / "Loop" are reserved type keywords. "MyGroup" / "MyLoop"
		// are valid, non-reserved starting points.
		const containerLabel = isContainer
			? generateUniqueGroupLabel(isLoop ? 'MyLoop' : 'MyGroup')
			: null;
		const id = isContainer ? containerLabel! : generateNodeId(type);
		const pos = contextMenuFlowPos ?? getViewportCenter();
		contextMenuFlowPos = null;
		// Default container size: groups get 500x350, loops get a wider /
		// taller default to fit the config strip plus a few body nodes.
		// The autonomous min-height logic in GroupNode bumps it higher
		// once ports are added, but a generous starting size keeps the
		// first paint pleasant.
		const containerConfig: Record<string, unknown> = isContainer
			? (isLoop
				? { width: 600, height: 500, expanded: true }
				: { width: 500, height: 350, expanded: true })
			: {};
		// Loop config left empty: parallel defaults to false (sequential),
		// over and carry default to empty lists. No need to seed them.
		const newNode: Node = {
			id,
			type: isContainer ? 'group' : isAnnotation ? 'annotation' : 'project',
			position: { x: pos.x, y: pos.y },
			selected: true,
			data: {
				label: containerLabel,
				nodeType: type,
				config: isContainer
					? containerConfig
					: isAnnotation
						? { width: 250, height: 120, content: '' }
						: {},
				inputs: [...typeConfig.defaultInputs],
				outputs: [...typeConfig.defaultOutputs],
				features: typeConfig.features || {},
				onUpdate: createNodeUpdateHandler(id),
			},
			...((isContainer || isAnnotation)
				? {
					style: isAnnotation
						? `width: 250px; height: 120px;`
						: isLoop
							? `width: 600px; height: 500px;`
							: `width: 500px; height: 350px;`,
				}
				: {}),
		};

		const deselectedNodes = nodes.map(n => ({ ...n, selected: false }));

		if (isContainer || isAnnotation) {
			const specialNodes = deselectedNodes.filter(n => n.type === 'group' || n.type === 'groupCollapsed' || n.type === 'annotation');
			const otherNodes = deselectedNodes.filter(n => n.type !== 'group' && n.type !== 'groupCollapsed' && n.type !== 'annotation');
			nodes = [...specialNodes, newNode, ...otherNodes];
		} else {
			nodes = [...deselectedNodes, newNode];
		}
		selectedNodeId = id;
		const op: EditOp = isLoop
			? { op: 'addLoop', label: newNode.data.label as string, parentGroup: null }
			: isGroup
				? { op: 'addGroup', label: newNode.data.label as string, parentGroup: null }
				: { op: 'addNode', id, nodeType: type, parentGroup: null };
		recordEdit([op], () => {
			if (isContainer) {
				const w = isLoop ? 600 : 500;
				const h = isLoop ? 500 : 350;
				layoutCode = updateLayoutEntry(layoutCode, newNode.data.label as string, pos.x, pos.y, w, h);
			}
			else layoutCode = updateLayoutEntry(layoutCode, id, pos.x, pos.y);
		});
	}

	function deleteNodes(nodeIds: string[]) {
		if (nodeIds.length === 0) return;
		if (structuralLock) return;

		// Capture container labels (groups + loops) and their kind
		// before visual deletion removes them from the nodes array.
		// The kind decides which EditOp to emit: `removeLoop` for
		// loops, `removeGroup` for groups. The Rust side rejects
		// RemoveGroup on a Loop (and vice versa) loudly, so routing
		// by container kind is required.
		const groupLabels = new Map<string, string>();
		const loopLabels = new Set<string>();
		for (const nodeId of nodeIds) {
			const n = nodes.find(nd => nd.id === nodeId);
			if (n && (n.type === 'group' || n.type === 'groupCollapsed') && n.data.label) {
				// Precondition before any visual mutation: a container
				// with no kind tag means a hydration race. Bail the
				// whole delete batch with a warning so the user can
				// retry once nodes re-hydrate. Half-deleting (visually
				// remove some, can't route the source op) would leave
				// the UI desynced from source.
				const kind = containerKindOf(n.data.nodeType);
				if (kind === null) {
					console.warn(
						`[delete] container ${nodeId} has nodeType=${JSON.stringify(n.data.nodeType)};`,
						'aborting delete batch until the node re-hydrates'
					);
					return;
				}
				groupLabels.set(nodeId, n.data.label as string);
				if (kind === 'Loop') {
					loopLabels.add(nodeId);
				}
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
		// Emit removals as one atomic batch. Non-group nodes first so children
		// are removed while still inside their group scope; layout entries clear
		// locally.
		const ops: EditOp[] = [];
		const layoutKeysToDrop: string[] = [];
		for (const nodeId of nodeIds) {
			if (!groupLabels.has(nodeId)) {
				ops.push({ op: 'removeNode', node: nodeId });
				layoutKeysToDrop.push(nodeId);
			}
		}
		for (const nodeId of nodeIds) {
			const groupLabel = groupLabels.get(nodeId);
			if (groupLabel) {
				if (loopLabels.has(nodeId)) {
					ops.push({ op: 'removeLoop', loopId: groupLabel });
				} else {
					ops.push({ op: 'removeGroup', group: groupLabel });
				}
				layoutKeysToDrop.push(groupLabel);
			}
		}
		recordEdit(ops, () => {
			for (const key of layoutKeysToDrop) layoutCode = removeLayoutEntry(layoutCode, key);
		});
	}

	// The single canvas keymap: chord -> action name, dispatched through
	// handlePaletteAction (the one action dispatcher; the palette's command list
	// routes through it too). One declaration site, so no two handlers can claim
	// the same chord (the double-undo bug). `inEditable` entries fire even while
	// typing in a field (only Ctrl+S); everything else is canvas-only.
	// `when` gates a chord on editor state (e.g. Escape only acts when there's a
	// menu/connection to close, so it otherwise falls through to xyflow's own
	// Escape handling). `inEditable` lets a chord fire while typing in a field
	// (only Ctrl+S); everything else is canvas-only.
	type Chord = { ctrl?: boolean; shift?: boolean; key: string; action: string; inEditable?: boolean; when?: () => boolean };
	const KEYMAP: Chord[] = [
		{ ctrl: true, key: 's', action: 'save', inEditable: true },
		{ ctrl: true, shift: false, key: 'z', action: 'undo' },
		{ ctrl: true, key: 'y', action: 'redo' },
		{ ctrl: true, shift: true, key: 'z', action: 'redo' },
		{ ctrl: true, key: 'a', action: 'selectAll' },
		{ ctrl: true, key: 'd', action: 'duplicate' },
		{ ctrl: true, key: 'Enter', action: 'run' },
		{ key: 'Delete', action: 'delete' },
		{ key: 'Backspace', action: 'delete' },
		{ key: 'Escape', action: 'escape', when: () => contextMenu !== null || pendingConnection !== null },
	];

	function handleKeyDown(event: KeyboardEvent) {
		const target = event.target as HTMLElement;
		const isEditableElement =
			target.tagName === 'INPUT' ||
			target.tagName === 'TEXTAREA' ||
			target.isContentEditable ||
			target.closest('[role="dialog"]') ||
			target.closest('.edit-textarea') ||
			target.closest('.annotation-node.editing');

		const ctrl = event.ctrlKey || event.metaKey;
		for (const c of KEYMAP) {
			if (c.key !== event.key) continue;
			if ((c.ctrl ?? false) !== ctrl) continue;
			if (c.shift !== undefined && c.shift !== event.shiftKey) continue;
			if (isEditableElement && !c.inEditable) return; // let the field handle it
			if (c.when && !c.when()) return; // precondition not met: don't claim the key
			event.preventDefault();
			handlePaletteAction(c.action);
			return;
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

		// One reversible action for the whole gesture: any reparent (move ops
		// from the capture checks) + the final positions are committed together,
		// so a single undo reverts the entire drag, not piece by piece.
		transaction(() => {
			// Re-read from current nodes state after each step to avoid stale refs.
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
			recordEdit([], () => {
				for (const dn of draggedNodes) {
					const n = nodes.find(nd => nd.id === dn.id);
					if (n) layoutUpdateAny(n);
				}
			});
		});
	}

	function onSelectionDragStop(_event: MouseEvent, selectedNodes: Node[]) {
		// One reversible action for the whole multi-select drag (reparents + the
		// final positions), so a single undo reverts the entire gesture.
		transaction(() => {
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
			recordEdit([], () => {
				for (const sn of selectedNodes) {
					const n = nodes.find(nd => nd.id === sn.id);
					if (n) layoutUpdateAny(n);
				}
			});
		});
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
		const isGroup = nodeToDuplicate.type === 'group' || nodeToDuplicate.type === 'groupCollapsed';
		// A group's id is its label in source; keep optimistic id == label so the
		// round-tripped node matches (no flash, position preserved). See addNode.
		const newLabel = isGroup ? generateUniqueGroupLabel((nodeToDuplicate.data.label as string) || 'MyGroup') : (nodeToDuplicate.data.label as string | null);
		const newId = isGroup ? (newLabel as string) : generateNodeId(nodeType);
		const newPos = { x: nodeToDuplicate.position.x + 50, y: nodeToDuplicate.position.y + 50 };

		const newNode: Node = {
			...nodeToDuplicate,
			id: newId,
			position: newPos,
			data: {
				...nodeToDuplicate.data,
				label: newLabel,
				onUpdate: createNodeUpdateHandler(newId),
			},
		};
		nodes = [...nodes, newNode];
		selectedNodeId = newId;
		contextMenu = null;

		// The duplication as one reversible action (add + copy config fields,
		// source) with the new node's drop position (layout).
		const ops: EditOp[] = [];
		let mutateLayout: () => void;
		if (isGroup) {
			const groupLabel = newNode.data.label as string;
			const cfg = newNode.data.config as Record<string, number>;
			ops.push({ op: 'addGroup', label: groupLabel, parentGroup: null });
			mutateLayout = () => { layoutCode = updateLayoutEntry(layoutCode, groupLabel, newPos.x, newPos.y, cfg?.width, cfg?.height); };
		} else {
			ops.push({ op: 'addNode', id: newId, nodeType, parentGroup: null });
			mutateLayout = () => { layoutCode = updateLayoutEntry(layoutCode, newId, newPos.x, newPos.y); };
			const config = nodeToDuplicate.data.config as Record<string, unknown> | undefined;
			if (config) {
				for (const [key, value] of Object.entries(config)) {
					if (['parentId', 'textareaHeights', 'width', 'height', 'expanded'].includes(key)) continue;
					// Copy every set field, incl a deliberately-empty string (the
					// live-edit path emits "", so duplicate must too or it drops
					// an intentionally-blank field).
					if (value === undefined || value === null) continue;
					ops.push({ op: 'setConfig', node: newId, key, value: formatConfigValue(value) });
				}
			}
		}
		recordEdit(ops, mutateLayout);
	}

	// Explicit save (Ctrl+S / palette): the source is already the host's via the
	// edit-server, so this only flushes pending GUI edits and persists layout.
	function saveProject() {
		flushAllPendingSaves();
		saveLayout();
		flashSaveStatus();
	}

	/// Persist ONLY the layout (positions/sizes), not the source. Graph edits
	/// send the source via onApplyEdits; their layout side (drop position of a
	/// new node, a drag, a resize) persists through here so it survives a fresh
	/// reload. Without this a GUI-placed node would lose its position.
	function saveLayout() {
		onSave({ layoutCode });
	}

	// GUI edits buffered while the user types in a config field; flushed as one
	// atomic batch once typing pauses (see createNodeUpdateHandler).
	let pendingConfigOps: import('../../../../shared/protocol').EditOp[] = [];

	function flushPendingConfigOps() {
		if (pendingConfigOps.length === 0) return;
		const batch = pendingConfigOps;
		pendingConfigOps = [];
		recordEdit(batch);
		flashSaveStatus();
	}

	type PortLike = { name: string; required?: boolean; portType?: string };
	function toPortSigs(ports: PortLike[]): import('../../../../shared/protocol').EditPortSig[] {
		return (ports ?? []).map(p => ({ name: p.name, required: p.required !== false, portType: p.portType }));
	}

	/// Flush every pending debounced edit. Called before the host kicks off
	/// Run / Activate / InfraStart (so the build sees the user's latest edits)
	/// and on teardown. Commits mid-typing field editors (their flush pushes the
	/// pending value into pendingConfigOps), cancels the debounce, and drains the
	/// op buffer as one reversible action via flushPendingConfigOps.
	export function flushAllPendingSaves(): void {
		fieldEditorRegistry.flushAll();
		if (saveProjectTimer) {
			clearTimeout(saveProjectTimer);
			saveProjectTimer = null;
		}
		flushPendingConfigOps();
	}

	// Flush buffered GUI config ops when the component is destroyed (panel
	// close, background) so an in-flight edit isn't lost. NOTE: navigation
	// flushes in the nav handlers (before the host swaps the watched doc).
	$effect(() => {
		return () => { flushAllPendingSaves(); };
	});

	function flashSaveStatus() {
		saveStatus = 'saved';
		if (saveStatusTimer) clearTimeout(saveStatusTimer);
		saveStatusTimer = setTimeout(() => { saveStatus = 'idle'; }, 2000);
	}

	</script>

<svelte:window onkeydown={handleKeyDown} onbeforeunload={() => { flushAllPendingSaves(); }} onvisibilitychange={() => { if (document.hidden) { flushAllPendingSaves(); } }} />

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

		<!-- Drift banner. A redundant surfacing of the action-bar
		     affordance, in case the user misses the button. The verb
		     it names must match whichever button is actually showing:
		     if infra is running + drifted the button is Upgrade; if
		     it's stopped + drifted the button is Start (Start re-applies
		     the changed spec and reaps orphans on the way up). Keyed on
		     `available_actions` so banner text and button agree. -->
		{#if drift?.infraDrift && (drift?.availableActions ?? []).includes('infra_upgrade')}
			<div class="absolute bottom-20 left-1/2 -translate-x-1/2 flex flex-col gap-1.5 items-center z-10">
				<div class="flex items-center gap-2 px-4 py-2 bg-amber-500/90 text-white rounded-lg shadow-lg text-xs font-medium backdrop-blur-sm">
					<svg xmlns="http://www.w3.org/2000/svg" width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><path d="M10.29 3.86L1.82 18a2 2 0 0 0 1.71 3h16.94a2 2 0 0 0 1.71-3L13.71 3.86a2 2 0 0 0-3.42 0z"/><line x1="12" y1="9" x2="12" y2="13"/><line x1="12" y1="17" x2="12.01" y2="17"/></svg>
					Infrastructure has changed. Click Upgrade to apply.
				</div>
			</div>
		{:else if drift?.infraDrift && (drift?.availableActions ?? []).includes('infra_start')}
			<div class="absolute bottom-20 left-1/2 -translate-x-1/2 flex flex-col gap-1.5 items-center z-10">
				<div class="flex items-center gap-2 px-4 py-2 bg-amber-500/90 text-white rounded-lg shadow-lg text-xs font-medium backdrop-blur-sm">
					<svg xmlns="http://www.w3.org/2000/svg" width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><path d="M10.29 3.86L1.82 18a2 2 0 0 0 1.71 3h16.94a2 2 0 0 0 1.71-3L13.71 3.86a2 2 0 0 0-3.42 0z"/><line x1="12" y1="9" x2="12" y2="13"/><line x1="12" y1="17" x2="12.01" y2="17"/></svg>
					Infrastructure has changed. Click Start to apply.
				</div>
			</div>
		{/if}

		<!-- Floating action bar. State + drift come from the host's
		     ActionBarStore (single source of truth). The bar reads
		     `state.backend` for at-rest facts and `state.overlay`
		     for the in-flight user-action; `drift` lights
		     Resync/Upgrade indicators. -->
		<ActionBar
			state={actionBarState}
			{drift}
			{onRun}
			{onStop}
			{onDismissError}
			{onActivate}
			{onCancelActivate}
			{onDeactivate}
			{onReactivate}
			{onCancelRunning}
			{onResumeActive}
			{onResync}
			{onStartInfra}
			{onStopInfra}
			{onTerminateInfra}
			{onUpgradeInfra}
			hasInfra={hasInfraInGraph}
			hasTriggers={hasTriggersInGraph}
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
				{@const infraInfo = infraNodes?.find(n => n.nodeId === targetNodeId)}
				{@const canNodeStop = !!infraInfo && (infraInfo.status === 'running' || infraInfo.status === 'flaky')}
				{@const canNodeTerminate = !!infraInfo && infraInfo.status !== 'terminating'}

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
						{#if infraInfo && (canNodeStop || canNodeTerminate)}
							<div class="my-1 mx-2 border-t"></div>
							<div class="px-3 py-1 text-xs text-muted-foreground uppercase tracking-wide">
								Infra ({infraInfo.status})
							</div>
							{#if canNodeStop && onInfraNodeStop}
								<button
									class="w-full flex items-center gap-2 px-3 py-2 rounded-lg hover:bg-muted text-sm text-left transition-colors"
									onclick={() => { const id = contextMenu!.nodeId!; contextMenu = null; onInfraNodeStop?.(id); }}
								>
									<span class="text-muted-foreground text-xs">⏸</span>
									<span>Stop this node</span>
								</button>
							{/if}
							{#if canNodeTerminate && onInfraNodeTerminate}
								<button
									class="w-full flex items-center gap-2 px-3 py-2 rounded-lg hover:bg-destructive/10 text-sm text-left transition-colors text-destructive"
									onclick={() => { const id = contextMenu!.nodeId!; contextMenu = null; onInfraNodeTerminate?.(id); }}
								>
									<span class="text-xs">✕</span>
									<span>Terminate this node</span>
								</button>
							{/if}
						{/if}
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
