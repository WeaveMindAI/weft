<script lang="ts">
	import { SvelteFlow, Controls, Background, useSvelteFlow, useUpdateNodeInternals, type Node, type Edge, type Connection, SelectionMode, ConnectionLineType, MarkerType } from "@xyflow/svelte";
	import { untrack, tick, onDestroy } from "svelte";
	import "@xyflow/svelte/dist/style.css";
	import { browser } from "$app/environment";
	import ProjectNode from "./ProjectNode.svelte";
	import GroupNode from "./GroupNode.svelte";
	import AnnotationNode from "./AnnotationNode.svelte";
	import CommandPalette from "./CommandPalette.svelte";
	import CustomEdge from "./CustomEdge.svelte";
	import ActionBar from "./ActionBar.svelte";
	import NodeTagsEditor from "./NodeTagsEditor.svelte";
	import { nodeTags, TAGS_CONFIG_KEY } from "../../node-tags";
	import { NODE_TYPE_CONFIG, type NodeType } from "../../nodes";
	import type { ProjectDefinition, PortDefinition, NodeFeatures, NodeDataUpdates } from "../../types";
	import { isContainerNodeType, isLoopNodeType, containerKindOf, inputExposure } from "../../types";
	import type { EditOp, TextEdit } from "../../../../shared/protocol";
	import { PORT_TYPE_COLORS } from "../../constants/colors";
	import { autoOrganize } from "../../auto-organize";
	import { updateLayoutEntry, removeLayoutEntry, parseLayoutCode, renameLayoutSubtree, computeContainmentFloors, parseViewMode, setViewMode, LAYOUT_VERB, SIMPLIFIED_LAYOUT_VERB, type ViewMode, type LayoutVerb } from "../../layout";
	import { SIMPLIFIED_IN_HANDLE, SIMPLIFIED_OUT_HANDLE, SIMPLIFIED_INNER_SOURCE_HANDLE, SIMPLIFIED_INNER_TARGET_HANDLE, SIMPLIFIED_LOOP_INDEX_HANDLE, SIMPLIFIED_LOOP_DONE_HANDLE, SIMPLIFIED_SQUARE_PX, SIMPLIFIED_CARD_MAX_W_PX } from "../../constants/simplified-view";
	import { Boxes } from "@lucide/svelte";
	import { formatConfigValue } from "../../value-format";
	import { measureTextWidth, nodeLabelFont } from "../../utils/measure-text";
	import { foldOps } from "../../projection/apply";
	import { diffConfigOps, diffPortLiteralOps, sameConfigValue, VIEW_KEYS, NON_SOURCE_KEYS } from "../../projection/config-diff";
	import { ProjectionEngine } from "../../projection/engine.svelte";
	import { provideFieldEditorRegistry } from "./field-editor-registry";
	import { extractInfraSubgraph } from "../../utils/infra-subgraph";
	import { extractTriggerSubgraph } from "../../utils/trigger-subgraph";
	import { nodeBodyFeedKind } from "../../utils/node-roles";
	import { toast } from "svelte-sonner";

	let {
		project,
		onSave,
		onApplyEdits,
		onApplyTextEdit,
		onResyncSource,
		onRun,
		onStop,
		onDismissError,
		onActivate,
		onCancelActivate,
		onCancelBuild,
		onCancelInfra,
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
		onOpenInclude = () => {},
		execPrefix = '',
		fileContents = {},
	}: {
		project: ProjectDefinition;
		onSave: (data: { layoutCode?: string; fileRef?: { path: string; content: string } }) => void;
		// Graph (GUI) edits: emit structured intents; Rust rewrites the source and
		// the reply carries the inverse text edit (the action's undo) PLUS the
		// post-edit truth. Rejects with the server's reason; the editor rolls the
		// optimistic op back through the one rejection path.
		onApplyEdits: (ops: import('../../../../shared/protocol').EditOp[]) => Promise<import('../../projection/types').EditRpcResult>;
		// Replay a raw source text edit (undo/redo); same reply shape.
		onApplyTextEdit: (edit: import('../../../../shared/protocol').TextEdit) => Promise<import('../../projection/types').EditRpcResult>;
		// Fetch the host's current truth after a rejected edit (the authoritative
		// post-rejection state). Resolves null when the source doesn't parse
		// right now (the editor keeps its previous truth).
		onResyncSource: () => Promise<{ project: ProjectDefinition; weftCode: string } | null>;
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
		onCancelBuild?: () => void;
		onCancelInfra?: () => void;
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
		executionState?: import('../../types').ExecutionState;
		autoOrganizeOnMount?: boolean;
		/// Per-node infra /live tick state. Read for nodes whose
		/// `requiresInfra` flag is true; ignored otherwise.
		infraFeedByNode?: Record<string, import('../../../../shared/protocol').NodeFeedState>;
		/// Per-node listener /display tick state. Read for nodes whose
		/// `features.isTrigger` flag is true; ignored otherwise.
		signalFeedByNode?: Record<string, import('../../../../shared/protocol').NodeFeedState>;
	} = $props();

	// VS Code embedding: dashboard chrome (right sidebar, code
	// panel, mobile notice, export dialog) is removed. The text
	// editor is the .weft tab in column 2; the activity-bar
	// Inspector handles node inspection.

	// ── The projection engine ──────────────────────────────────────────────
	// The visible graph is a pure function of (truth, pendingOps, layoutCode),
	// all owned by the engine (../../projection/engine.svelte.ts): it records
	// gestures, sends them, advances truth on confirmation, rolls back on
	// rejection, and runs undo/redo. This component is the binding: it renders
	// the projection and routes gestures in. Initial state intentionally
	// captures the prop's first value; truth advances through the engine.
	const engine = new ProjectionEngine(
		{
			applyEdits: (ops) => onApplyEdits(ops),
			applyTextEdit: (edit) => onApplyTextEdit(edit),
			resyncSource: () => onResyncSource(),
			persistLayout: (committedLayout) => saveLayout(committedLayout),
			notify: (title, description) => toast.warning(title, { description, duration: 5000 }),
			snapBack: () => rebuildFromProjection(),
			flashSave: () => flashSaveStatus(),
			now: () => Date.now(),
		},
		NODE_TYPE_CONFIG,
		untrack(() => ({ project, weftCode: project.weftCode ?? '' })),
		untrack(() => project.layoutCode ?? ''),
	);
	const layoutCode = $derived(engine.layoutCode);
	// Per-project view mode (builder = full ports/config/body; simplified =
	// square nodes, one in/out dot, edges collapsed per node-pair). Persisted as
	// a `@view` header in the layout file, so it survives reload and is per
	// project. `simplified` feeds buildNodes/buildEdges/computeSizing so the
	// whole render derives from it; the toggle is a layout-only edit (undoable).
	const simplified = $derived(parseViewMode(layoutCode) === 'simplified');
	// Which view's position block to read/write. Builder and simplified keep
	// SEPARATE positions in the same layout file (a node is a wide box vs a small
	// square), so every layout read/write picks the verb for the active view.
	const layoutVerb = $derived<LayoutVerb>(simplified ? SIMPLIFIED_LAYOUT_VERB : LAYOUT_VERB);
	function toggleSimplified(): void {
		const next: ViewMode = simplified ? 'builder' : 'simplified';
		recordEdit([], (layout) => setViewMode(layout, next));
	}
	const fold = $derived(foldOps(engine.truth.project, engine.pendingOps, NODE_TYPE_CONFIG));
	// A gesture's layout half is PURE: `(currentLayout) => newLayout`. The
	// engine runs it against the right layout and captures the diff, so the
	// binding never reaches into engine state (no `engine.layoutCode = ...`).
	function recordEdit(ops: EditOp[], mutateLayout: import('../../projection/engine.svelte').LayoutMutator = (l) => l, typingKey?: string): void {
		engine.recordEdit(ops, mutateLayout, typingKey);
	}
	/** Persist a layout-only change with NO undo entry (automatic re-flows the user
	 *  didn't author, e.g. a content-driven re-organize). */
	function persistLayoutEdit(mutateLayout: import('../../projection/engine.svelte').LayoutMutator): void {
		engine.persistLayoutEdit(mutateLayout);
	}
	function transaction(fn: () => void): void {
		engine.transaction(fn);
	}
	function undo(): void {
		engine.undo();
	}
	function redo(): void {
		engine.redo();
	}

	// Session-local home for per-node textarea heights (which field, how tall
	// the user dragged it). View-state with no source op and no slot in the
	// layout file's scalar format, so buildNodes merges it back on every
	// structural rebuild (else a rebuild from the projection would wipe it).
	const textareaHeightsByNode = new Map<string, Record<string, number>>();

	// Graph-logic lock plumbing (gate 1: sliding auto-lock on external code
	// keystrokes; gate 2: explicit lock with banner). Routed from App.svelte.
	export function setCodeEditTouched(): void {
		engine.setCodeEditTouched();
	}
	export function setGraphLogicLock(locked: boolean, reason?: string): void {
		engine.setGraphLogicLock(locked, reason);
	}

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
	 *  local id. With the projection model these always agree with `node.id`
	 *  (nodes are never optimistically re-parented), but keying off `parentId`
	 *  keeps every writer agreeing on the ONE key-shaping rule. */
	function getLayoutKey(node: Node): string {
		const parentId = (node.data.config as Record<string, string> | undefined)?.parentId;
		return scopedLayoutKey(node.id.split('.').pop()!, parentId);
	}

	/** A PURE layout mutator writing a node's current position/size/state into
	 *  `layout`. Never touches source. Returned for composition into a gesture's
	 *  `mutateLayout` (the engine runs it against the right layout). */
	function layoutUpdateAny(node: Node): (layout: string) => string {
		const cfg = node.data.config as Record<string, unknown> | undefined;
		const key = getLayoutKey(node);
		const verb = layoutVerb;
		return (layout) => updateLayoutEntry(layout, key,
			node.position.x, node.position.y,
			cfg?.width as number | undefined, cfg?.height as number | undefined,
			cfg?.expanded as boolean | undefined ?? undefined,
			cfg?.configCollapsed as boolean | undefined ?? undefined,
			verb);
	}

	/** Move a node or group to a different scope: emit the move intent (Rust
	 *  rewrites the source) and re-key its layout entries (the moved decl AND, for
	 *  a group, its whole subtree) to the new scoped address, then set the moved
	 *  decl's new position. `targetGroupId` is the target group's SCOPED id
	 *  (undefined = top level).
	 *
	 *  Both the moved decl and the target are identified to Rust by their SCOPED id
	 *  (`node.id`, `targetGroupId`), never a bare label. A bare label is ambiguous
	 *  across scopes and, crucially, can't be compared against a scoped parent path:
	 *  the old label-based op made the Rust no-op guard misfire at nesting depth >= 2
	 *  (a no-op move was applied destructively). Scoped-id everywhere makes the
	 *  resolve and the no-op comparison exact at any depth.
	 *
	 *  Layout view-state is the single source of truth for positions/sizes; the
	 *  re-key + position write here is the ONLY layout change a move makes, and
	 *  it runs at record time so the projection's pure merge reads it. No
	 *  carry/remap is needed: the projected (and later re-parsed) nodes carry
	 *  the new scoped ids and the merge finds their re-keyed entries directly.
	 *
	 *  The visual reparent comes from the projection re-derive; nothing mutates
	 *  live nodes, so `getLayoutKey(node)` always reads the OLD key. `newPos` is
	 *  the node's position RELATIVE to the new parent (the caller computes it
	 *  from the drag's absolute drop point). */
	function weftMoveScopeAny(node: Node, targetGroupId: string | undefined, newPos: { x: number; y: number }) {
		const oldKey = getLayoutKey(node);
		const isContainer = node.type === 'group' || node.type === 'groupCollapsed';
		// Group and Loop have distinct move ops; the Rust side rejects a
		// mismatched-kind move loudly. Projection-built containers always carry
		// their kind in data.nodeType, so the dispatch is total.
		const op: EditOp = isContainer
			? (containerKindOf(node.data?.nodeType) === 'Loop'
				? { op: 'moveLoopScope', loopId: node.id, targetGroup: targetGroupId ?? null }
				: { op: 'moveGroupScope', group: node.id, targetGroup: targetGroupId ?? null })
			: { op: 'moveNodeScope', node: node.id, targetGroup: targetGroupId ?? null };
		const localId = node.id.split('.').pop()!;
		const newKey = scopedLayoutKey(localId, targetGroupId);
		recordEdit([op], (layout) => {
			let next = layout;
			if (oldKey !== newKey) {
				// Re-key the moved subtree's layout entries to the new address (exact,
				// not a regex prefix-sweep). Descendants keep their (parent-relative)
				// coords under their re-keyed ids.
				next = renameLayoutSubtree(next, oldKey, newKey);
			}
			const entry = parseLayoutCode(next)[newKey];
			return updateLayoutEntry(next, newKey, newPos.x, newPos.y, entry?.w, entry?.h, entry?.expanded ?? null);
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

	const nodeTypes = {
		project: ProjectNode,
		group: GroupNode,
		groupCollapsed: GroupNode,
		annotation: AnnotationNode,
	};
	
	const edgeTypes = {
		custom: CustomEdge,
	};


	/** Edge color from the SOURCE port's type. `outputsOf` resolves a node id
	 *  to its outputs; it defaults to the live xyflow `nodes`, but buildEdges
	 *  passes a resolver over the PROJECTED nodes it is rendering, because at
	 *  rebuild time `nodes` still holds the PREVIOUS render (a freshly-added
	 *  wired node would otherwise color gray until the next change). */
	function getEdgeColor(
		sourceNodeId: string,
		sourceHandle: string | null | undefined,
		outputsOf: (id: string) => Array<{ name: string; portType: string }> | undefined
			= (id) => nodes.find(n => n.id === id)?.data.outputs as Array<{ name: string; portType: string }> | undefined,
	): string {
		const outputs = outputsOf(sourceNodeId);
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
		// Read a DEFINITE `width: Npx` / `height: Npx` from the style, but ignore
		// `min-width`/`min-height` (the `(?<![a-z-])` guard) and `max-content` /
		// `auto` sizes: those aren't the rendered size, so fall back to the real
		// measured DOM size. Simplified nodes style as `min-width: 96px; width:
		// max-content`, so without this guard the regex matched `min-width` and
		// reported every node as 96px wide, breaking the expand viewport math.
		const wMatch = n.style?.match(/(?<![a-z-])width:\s*(\d+)px/);
		const hMatch = n.style?.match(/(?<![a-z-])height:\s*(\d+)px/);
		const styleW = wMatch && !n.style?.includes('width: max-content') && !n.style?.includes('width: auto');
		const styleH = hMatch && !n.style?.includes('height: max-content') && !n.style?.includes('height: auto');
		const w = styleW ? parseInt(wMatch![1]) : (n.measured?.width ?? 200);
		const h = styleH ? parseInt(hMatch![1]) : (n.measured?.height ?? 60);
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

	/** Whose key a group's members spent, folded for the group's synthetic
	 *  execution row: one distinct origin passes through, disagreeing
	 *  members read 'mixed', no cost records means no origin. */
	function groupCostOrigin(
		members: { costOrigin?: 'user-provided' | 'runtime' | 'mixed' }[],
	): 'user-provided' | 'runtime' | 'mixed' | undefined {
		const origins = new Set(
			members.map((e) => e.costOrigin).filter((o) => o !== undefined),
		);
		if (origins.size === 0) return undefined;
		return origins.size === 1 ? [...origins][0] : 'mixed';
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

			// LIVE merge for LAYOUT/VIEW keys only (VIEW_KEYS): these are
			// view-state, never source, and resize/collapse/textarea need
			// immediate visual feedback. Source-side fields (label, config
			// values, ports) repaint through the projection when their op lands
			// in pendingOps; merging them here too would be a second data path.
			if ('config' in updates) {
				const viewPatch: Record<string, unknown> = {};
				for (const key of VIEW_KEYS) {
					if (updates.config && key in updates.config) viewPatch[key] = (updates.config as Record<string, unknown>)[key];
				}
				// textareaHeights has no durable home (not source, not in the
				// layout file's scalar format), so a structural rebuild from the
				// projection would wipe it. Stash it in a session-local map that
				// buildNodes merges back, so dragging a textarea taller survives
				// the next keystroke's rebuild.
				if (updates.config && 'textareaHeights' in updates.config) {
					textareaHeightsByNode.set(nodeId, (updates.config as Record<string, Record<string, number>>).textareaHeights);
				}
				if (Object.keys(viewPatch).length > 0) {
					nodes = nodes.map(n => {
						if (n.id !== nodeId) return n;
						const newData = { ...n.data, config: { ...(n.data.config as Record<string, unknown>), ...viewPatch } };
						return applyNodeSizing(n, newData);
					});
				}
			}

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
							runAutoOrganize(false).then((outcome) => {
								// If the organize bailed (view toggled away, or the editor
								// closed) nothing was repositioned, so the viewport-pin math is
								// meaningless and touching the viewport on a destroyed component
								// could throw. Only adjust when the layout actually applied.
								if (outcome !== 'applied') return;
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

			// Layout mutations are a PURE `(layout) => layout` transform the engine
			// runs and diffs (one reversible action with the source ops). Layout-
			// only changes (resize/collapse) carry no source ops.
			let mutateLayout: import('../../projection/engine.svelte').LayoutMutator = (l) => l;
			let hasLayout = false;

			if ('label' in updates) {
				const node = nodes.find(n => n.id === nodeId);
				const isContainer = node?.type === 'group' || node?.type === 'groupCollapsed';
				if (isContainer && oldGroupLabel && updates.label) {
					// Group and Loop have distinct rename ops (the Rust dispatch
					// rejects a kind mismatch loudly), but both carry the SCOPED id
					// (`nodeId`), so a rename is unambiguous when two containers share a
					// local label in different scopes; Rust derives the old bare label
					// from the resolved decl. Identical shape, so route by kind into one
					// push.
					const kind = containerKindOf(node?.data?.nodeType);
					ops.push(kind === 'Loop'
						? { op: 'renameLoop', loopId: nodeId, newLabel: updates.label as string }
						: { op: 'renameGroup', group: nodeId, newLabel: updates.label as string });
					// Re-key the group's layout subtree (its own entry + descendants) from
					// the old scoped address to the new one. Same exact, non-compounding
					// re-key a move uses; a rename only changes the last path segment.
					const parts = nodeId.split('.');
					parts[parts.length - 1] = updates.label as string;
					const newPrefix = parts.join('.');
					mutateLayout = (layout) => renameLayoutSubtree(layout, nodeId, newPrefix);
					hasLayout = true;
				} else {
					ops.push({ op: 'setLabel', node: nodeId, label: (updates.label as string | null) ?? null });
				}
			}
			if ('config' in updates) {
				const cfg = updates.config!;
				// `needsLayout` means a layout key (width/height/expanded) actually
				// CHANGED vs what's persisted, not merely that it's present. A field
				// edit re-sends the full config including the UNCHANGED dims; treating
				// mere presence as a change would churn the layout file (and history)
				// on every keystroke. We compare against the PERSISTED layout entry
				// (the source of truth for dims) so a bundled config+ops update can
				// safely persist its layout part only when the dims really moved,
				// closing the "layout dropped when bundled with ops" gap.
				// Look up by the node's live SCOPED layout key (the exact key the writer
				// `layoutUpdateAny` uses), not the raw `nodeId`: the two diverge for one
				// frame after a drag reparents a node, and keying off `nodeId` there would
				// miss the entry (read undefined -> every dim looks "changed" -> a spurious
				// layout write + history churn on a keystroke).
				const liveNode = nodes.find(n => n.id === nodeId);
				const layoutEntry = liveNode ? parseLayoutCode(layoutCode, layoutVerb)[getLayoutKey(liveNode)] : undefined;
				const persistedDim = (key: string): unknown =>
					key === 'width' ? layoutEntry?.w
						: key === 'height' ? layoutEntry?.h
						: key === 'configCollapsed' ? layoutEntry?.configCollapsed
						: layoutEntry?.expanded;
				// A Loop's config fields (`parallel`, `over`, `carry`, ...) live in the
				// loop config block, not a node config block, so they route to the
				// loop-specific ops; the Rust dispatch rejects a generic SetConfig /
				// RemoveConfig against a Loop decl. Both carry the SCOPED id (`nodeId`).
				const isLoopConfig = containerKindOf(liveNode?.data?.nodeType) === 'Loop';
				let needsLayout = false;
				for (const [key, value] of Object.entries(cfg)) {
					if (['width', 'height', 'expanded', 'configCollapsed'].includes(key)) {
						// Only a CHANGED layout key needs a layout write: a field edit
						// re-sends the full config including unchanged dims, and treating
						// mere presence as a change churns the layout file + history on a
						// keystroke. `configCollapsed` (the loop config strip) is a layout
						// key too, so it rides the same changed-not-present gate.
						if (value !== persistedDim(key)) needsLayout = true;
					}
				}
				// Source ops: only the keys whose value actually CHANGED vs the
				// node's projected config. Update senders spread the full config,
				// and emitting unchanged keys would turn a pure layout gesture
				// (expand/collapse spreads config too) into phantom source ops
				// whose round-trip races the layout persist and reverts the toggle.
				ops.push(...diffConfigOps(
					nodeId, cfg as Record<string, unknown>,
					(liveNode?.data.config as Record<string, unknown> | undefined) ?? {},
					isLoopConfig,
				));
				if (needsLayout) {
					// The node in `nodes` was already updated (merged config + new
					// dims) by the synchronous map above, so persist its live state
					// directly. (Previously this overrode config with the update's
					// `cfg`, which is fine for a full-config update but wrong for a
					// partial one like `{height}` now that config merges.)
					mutateLayout = (layout) => {
						const n = nodes.find(nd => nd.id === nodeId);
						return n ? layoutUpdateAny(n)(layout) : layout;
					};
					hasLayout = true;
				}
			}
			if ('portLiterals' in updates && updates.portLiterals) {
				// Port-driven values: diff against the PROJECTION's portLiterals
				// (the same truth the fields render from) through the shared
				// producer, so every surface emits identical ops for this home.
				const foldNode = fold.project.nodes.find((n) => n.id === nodeId);
				ops.push(...diffPortLiteralOps(
					nodeId,
					updates.portLiterals,
					(foldNode?.portLiterals as Record<string, unknown> | undefined) ?? {},
					(foldNode?.portLiteralSpans as Record<string, { origin: 'inline' | 'connection' }> | undefined) ?? {},
					foldNode?.inputs ?? [],
				));
			}
			if ('portValueForm' in updates && updates.portValueForm) {
				const { key, form } = updates.portValueForm;
				ops.push({ op: 'setValueForm', node: nodeId, key, form });
			}
			if ('inputs' in updates || 'outputs' in updates) {
				const node = nodes.find(n => n.id === nodeId);
				// A form-schema node's ports are DERIVED from its `fields` config
				// (the enricher re-materializes them on every parse), so they must
				// NEVER be written into the source signature. Emitting them makes the
				// `.weft` declare `-> (test_approved: Boolean?, ...)` on a node type
				// with `canAddOutputPorts: false`, which strict enrich (the build
				// path) rejects as "custom output port on a node that does not
				// support custom ports". The `fields` config, written by the config
				// ops above, is the single source of truth; the ports round-trip
				// through it, not through the header.
				const isFormSchema = (node?.data?.features as { hasFormSchema?: boolean } | undefined)?.hasFormSchema === true;
				if (node?.data && !isFormSchema) {
					// Carry-synthesized ghost inputs are DERIVED from the loop's carry
					// list (by the compiler on parse, by the projection on apply);
					// writing one into the source signature would turn it into a real
					// declared port that no longer dies with its carry. Strip them
					// from every signature we emit.
					// `config`-exposure inputs are metadata-derived settings,
					// never source-declared ports: writing one into the header
					// signature would collide with the compiler's config-input
					// rule. Strip them, like the carry-synthesized ghosts.
					const inputs = toPortSigs(((updates.inputs ?? node.data.inputs) as PortDefinition[])
						.filter(p => !p.synthesizedFromCarry && inputExposure(p) !== 'config'));
					const outputs = toPortSigs((updates.outputs ?? node.data.outputs) as PortLike[]);
					const isContainer = node.type === 'group' || node.type === 'groupCollapsed';
					if (isContainer) {
						// Group and Loop have distinct port-update ops (the Rust dispatch
						// validates the decl kind matches the op). Both carry the SCOPED id
						// (`nodeId`), not the bare label, so ports resolve unambiguously
						// even when two containers share a local label in different scopes.
						const kind = containerKindOf(node.data.nodeType);
						if (kind === 'Loop') {
							ops.push({ op: 'updateLoopPorts', loopId: nodeId, inputs, outputs });
						} else {
							ops.push({ op: 'updateGroupPorts', group: nodeId, inputs, outputs });
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

			// Dispatch. `hasLayout` now means a layout dim ACTUALLY changed (not just
			// present), so the only thing that should debounce is pure config-value
			// typing: source `config` ops, no structural ports/label, no real layout
			// change. Everything else (structural ops, a real layout change, or a
			// bundle of both like add-port-which-grows-the-group) commits immediately
			// with BOTH its source ops and its layout mutation, so the layout part is
			// never dropped regardless of how the update is bundled.
			const isPureConfigTyping = 'config' in updates && ops.length > 0 && !hasLayout
				&& !('inputs' in updates) && !('outputs' in updates) && !('label' in updates);
			if (isExpandToggle) {
				// Collapse/expand: the ELK pass below records layout incl. the toggled
				// flag. Record any source ops that rode along (rare: a config typed in
				// the same frame as a toggle).
				if (ops.length > 0) recordEdit(ops);
			} else if (isResize) {
				// User resize: a new footprint means neighbours reflow, so re-run ELK
				// (no viewport pin); runAutoOrganize records the layout.
				void runAutoOrganize(false);
			} else if (isPureConfigTyping) {
				// Config-value typing: one pending op per node, its value replaced in
				// place on every keystroke (the projection repaints instantly), sent
				// to the host after a short debounce. One queue, one undo unit per
				// typing burst.
				recordEdit(ops, undefined, `cfg:${nodeId}`);
			} else if (ops.length > 0 || hasLayout) {
				// Structural (ports/label) ops, and/or a LAYOUT-only change with no
				// source ops (the min-height auto-enforce sends just `{height}`).
				// Both persist immediately as one reversible action.
				recordEdit(ops, mutateLayout);
			}
		};
	}

	function computeMinNodeWidth(inputs?: PortDefinition[], outputs?: PortDefinition[]): number {
		const MIN_WIDTH = 200;
		const PADDING = 60; // handles (12*2) + gaps + px padding
		const GAP = 20; // minimum gap between input and output labels
		// Port labels render at text-[10px] in the app's sans font; measure the real
		// pixel width of each label in that exact font instead of guessing chars*px
		// (which clips wide labels like WWWW and over-reserves narrow ones like iiii).
		const font = nodeLabelFont(10);

		const inputNames = (inputs || []).map(p => p.name + (p.required ? '*' : ''));
		const outputNames = (outputs || []).map(p => p.name);

		let maxRowWidth = 0;
		const rowCount = Math.max(inputNames.length, outputNames.length);
		for (let i = 0; i < rowCount; i++) {
			const leftW = i < inputNames.length ? measureTextWidth(inputNames[i], font) : 0;
			const rightW = i < outputNames.length ? measureTextWidth(outputNames[i], font) : 0;
			const rowWidth = leftW + rightW + GAP;
			if (rowWidth > maxRowWidth) maxRowWidth = rowWidth;
		}

		return Math.max(MIN_WIDTH, Math.ceil(maxRowWidth + PADDING));
	}

	// Node types that have their own SvelteFlow components (not in NODE_TYPE_CONFIG)
	// 'IncludedGroup' is the opaque @include block: no catalog entry by
	// design (its ports come from the included file's Group header), so it
	// must be allowed through the catalog filter explicitly.
	const SPECIAL_NODE_TYPES = new Set(['Group', 'Annotation', 'IncludedGroup']);

	// `liveNodes` is the CURRENTLY-rendered xyflow array, read only for last-render
	// measured sizes (the simplified containment floor). It is passed explicitly
	// rather than read from the module-level `nodes` because buildNodes runs inside
	// the `let nodes = $state.raw(buildNodes(...))` initializer, where reading
	// `nodes` would hit its temporal dead zone (the "Loading graph..." crash). At
	// init there is nothing measured yet, so the caller passes `[]`.
	function buildNodes(projectNodes: typeof project.nodes, projectEdges: typeof project.edges, layoutMap?: Record<string, { x: number; y: number; w?: number; h?: number; expanded?: boolean; configCollapsed?: boolean }>, liveNodes: Node[] = []): Node[] {
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

		// Placement for nodes with NO layout entry (e.g. just typed into the code
		// panel, or a fresh file before ELK): stack them BELOW the laid-out content
		// so they're visible without overlapping at the origin, instead of all
		// landing at (0,0). This is the ONE placement rule, shared by the initial
		// mount and every post-edit re-render (both go through buildNodes), so the
		// two paths can't diverge (mount used to leave such nodes stacked at 0,0).
		// Top-level only: a child with no entry stays at its own (parent-relative)
		// position, which a later ELK/auto-organize resolves.
		let nextFreeY = 0;
		for (const n of sortedNodes) {
			const e = layoutMap?.[n.id];
			if (e) nextFreeY = Math.max(nextFreeY, e.y + (e.h ?? 120) + 40);
		}

		// Containment floors: an expanded container's drawn box grows to enclose
		// its children, recursively, so a container child (a Loop inside a
		// Group) can never render ejected outside its parent. Saved sizes win
		// when already large enough; a full auto-organize recomputes properly.
		// Measured DOM size of the currently-rendered node, if any. Used in
		// simplified view so the containment floor reflects a node's REAL drawn size
		// (a live-display card is bigger than the base square), keeping a grown child
		// inside its parent. The square is only the lower bound before measurement.
		const measuredOf = (id: string) => {
			const live = liveNodes.find(ln => ln.id === id)?.measured;
			return live?.width && live?.height ? { w: live.width, h: live.height } : undefined;
		};
		const floors = computeContainmentFloors(
			sortedNodes.map(n => {
				const cfg = n.config as Record<string, unknown>;
				const entry = layoutMap?.[n.id];
				const isContainer = isContainerNodeType(n.nodeType);
				// ContainmentItem.w/h is the DRAWN size. A collapsed node keeps its
				// saved expanded dims in config (restored on re-expand) but draws as a
				// min-width chip; feeding the saved dims here would floor the parent at
				// the node's pre-collapse footprint, so the parent never shrinks.
				const drawsAtConfigDims = n.nodeType === 'Annotation'
					|| ((cfg?.expanded as boolean | undefined) ?? isContainer) !== false;
				// Simplified leaf: prefer the MEASURED size (real drawn footprint of the
				// square or the live-display card), falling back to the base square as a
				// lower bound before the DOM is measured. NEVER the builder min-width
				// (>= 200px), which inflated the group's right edge on the rightmost
				// node (the lopsided-gap bug). Builder leaf keeps its min-width.
				const measured = simplified ? measuredOf(n.id) : undefined;
				return {
					id: n.id,
					parentId: cfg?.parentId as string | undefined,
					container: isContainer && (cfg?.expanded as boolean ?? true) !== false,
					x: entry?.x ?? n.position.x,
					y: entry?.y ?? n.position.y,
					w: drawsAtConfigDims
						? cfg?.width as number | undefined
						: (simplified ? Math.max(SIMPLIFIED_SQUARE_PX, measured?.w ?? 0) : computeMinNodeWidth(n.inputs, n.outputs)),
					h: drawsAtConfigDims
						? cfg?.height as number | undefined
						: (simplified ? Math.max(SIMPLIFIED_SQUARE_PX, measured?.h ?? 0) : undefined),
				};
			}),
			{ w: 280, h: 120 },
			{ right: 40, bottom: 40 },
		);

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
			// fall back to the saved layout entry when config doesn't specify, and
			// are floored by the children-containment minimum (see above).
			const layoutEntry = layoutMap?.[n.id];
			const floor = isGroup && isExpanded ? floors.get(n.id) : undefined;
			const sizing = computeSizing({
				isGroup,
				isAnnotation,
				isExpanded,
				configWidth: floor && configWidth !== undefined ? Math.max(configWidth, floor.w) : configWidth,
				configHeight: floor && configHeight !== undefined ? Math.max(configHeight, floor.h) : configHeight,
				fallbackWidth: floor ? Math.max(layoutEntry?.w ?? 0, floor.w) : layoutEntry?.w,
				fallbackHeight: floor ? Math.max(layoutEntry?.h ?? 0, floor.h) : layoutEntry?.h,
				inputs: n.inputs,
				outputs: n.outputs,
				nestingDepth,
				simplified,
			});

			// Position is layout's job: the merge places a node at its saved layout
			// entry when present. A node with NO entry (just typed, fresh file) is
			// placed below existing laid-out content (top-level) so it's visible, not
			// stacked at the origin; a child with no entry keeps its parent-relative
			// position. This single placement rule lives here so mount and re-render
			// agree. (`n.position` is the structural parse's 0,0.)
			let position: { x: number; y: number };
			if (layoutEntry) {
				position = { x: layoutEntry.x, y: layoutEntry.y };
			} else if (!rawParentId) {
				position = { x: 0, y: nextFreeY };
				nextFreeY += 140;
			} else {
				position = n.position;
			}

			return {
				id: n.id,
				type: sizing.type,
				position,
				zIndex: sizing.zIndex,
				...(sizing.width !== undefined ? { width: sizing.width } : {}),
				...(sizing.height !== undefined ? { height: sizing.height } : {}),
				// Dynamic per-node data (fileContents, infra badges, execution
				// state, body feeds) is painted by `decorate`, the ONE decoration
				// pass; buildNodes carries only the structural fields.
				data: {
					label: n.label,
					nodeType: n.nodeType,
					// Per-project view mode flows into every node so ProjectNode can
					// render the simplified square (icon + type, single in/out dot)
					// instead of the full ports/config/body.
					simplified,
					// Overlay the session-local textarea heights (view-state with
					// no source/layout home) so they survive this rebuild.
					config: textareaHeightsByNode.has(n.id)
						? { ...(n.config as Record<string, unknown>), textareaHeights: textareaHeightsByNode.get(n.id) }
						: n.config,
					inputs: n.inputs,
					outputs: n.outputs,
					// Body-set port values + their written forms, the two-home
					// twin of `config` (ProjectNode renders these as the
					// marker-carrying port fields).
					portLiterals: (n as typeof n & { portLiterals?: Record<string, unknown> }).portLiterals,
					portLiteralSpans: (n as typeof n & { portLiteralSpans?: Record<string, unknown> }).portLiteralSpans,
					features: n.features,
					includePath: (n as typeof n & { includePath?: string }).includePath,
					sourceLine: (n as typeof n & { sourceLine?: number }).sourceLine,
					onUpdate: createNodeUpdateHandler(n.id),
					onSaveFileRef: saveFileRef,
					onOpenInclude: openInclude,
				},
				...(hiddenByCollapsedGroup
					? { style: 'display: none;' }
					: { style: sizing.style }),
				parentId,
			};
		});
	}

	// The INITIAL graph snapshot, seeded ONCE from the props. The live
	// reconciliation against later prop changes is driven by the effects below,
	// not by re-running this initializer, so reading the props here is
	// intentional, NOT a missed reactive dependency. `untrack` states that
	// explicitly (the official "read without creating a dependency" API), which is
	// also what silences svelte 5.56's `state_referenced_locally` warning.
	let nodes = $state.raw<Node[]>(
		untrack(() => buildNodes(project.nodes, project.edges, parseLayoutCode(layoutCode, layoutVerb)))
	);

	function buildEdges(projectEdges: typeof project.edges, projectNodes: typeof project.nodes): Edge[] {
		// Resolve a node's ports / kind against the PROJECTED nodes being rendered,
		// not the live `nodes` (still the previous render at rebuild time).
		const nodeOf = (id: string) => projectNodes.find(n => n.id === id);
		const outputsOf = (id: string) =>
			nodeOf(id)?.outputs as Array<{ name: string; portType: string }> | undefined;
		const inputsOf = (id: string) =>
			nodeOf(id)?.inputs as Array<{ name: string; portType: string }> | undefined;
		const isLoopNode = (id: string) => isLoopNodeType(nodeOf(id)?.nodeType ?? '');

		// Simplified view: every connection between two nodes collapses onto the
		// single in/out dots, and all the lines between the same pair merge into
		// ONE edge. Color: the shared type's color if every merged connection is
		// the same type, else the mixed (TypeVar) color. Self-reference handles
		// (`__inner`) resolve to the container's own simplified dots so a group's
		// interface still connects in this view.
		if (simplified) {
			// An edge's source handle attaches to the source node's single OUT dot,
			// unless the parser marked it `__inner` (an expanded container feeding
			// one of its own inputs to a child), which attaches to the container's
			// inner SOURCE dot instead. Symmetrically for targets and the inner
			// TARGET dot (a child writing the container's output). This keeps a
			// group's interface wiring visible in simplified view.
			// xyflow scopes a handle id to its node (it pairs (node, handleId)), so
			// these are the BARE handle ids the node renders, NOT prefixed with the
			// node id. The source node's single OUT dot, unless the parser marked
			// the edge `__inner` (an expanded container feeding its own input to a
			// child) which uses the inner SOURCE dot. Symmetrically for targets.
			// `index`/`done` are reserved implicit ports ONLY on loops; a plain Group
			// may legitimately declare an interface port literally named `index` or
			// `done`. So the loop-dot special-case is gated on the SOURCE/TARGET node
			// actually being a loop (read from its nodeType), never inferred from the
			// handle string alone, or a Group's `index__inner` edge would be routed to
			// a `__simp_index` dot that a non-loop GroupNode never renders (the edge
			// would point at a nonexistent handle and vanish).
			const srcHandleId = (e: typeof projectEdges[0]) => {
				if (e.sourceHandle === 'index__inner' && isLoopNode(e.source)) return SIMPLIFIED_LOOP_INDEX_HANDLE;
				return e.sourceHandle?.endsWith('__inner') ? SIMPLIFIED_INNER_SOURCE_HANDLE : SIMPLIFIED_OUT_HANDLE;
			};
			const tgtHandleId = (e: typeof projectEdges[0]) => {
				if (e.targetHandle === 'done__inner' && isLoopNode(e.target)) return SIMPLIFIED_LOOP_DONE_HANDLE;
				return e.targetHandle?.endsWith('__inner') ? SIMPLIFIED_INNER_TARGET_HANDLE : SIMPLIFIED_IN_HANDLE;
			};
			// Merge every connection that lands on the SAME pair of dots into one
			// edge. The key is the resolved dot ids (not just the node ids) so an
			// inner and an outer edge between the same two nodes stay distinct.
			const byPair = new Map<string, { source: string; target: string; sh: string; th: string; types: Set<string> }>();
			for (const e of projectEdges) {
				const sh = srcHandleId(e);
				const th = tgtHandleId(e);
				// Key includes the node ids (handle ids are bare/shared), so edges
				// only merge when they land on the SAME dot pair of the SAME nodes.
				const key = `${e.source}.${sh}->${e.target}.${th}`;
				let entry = byPair.get(key);
				if (!entry) { entry = { source: e.source, target: e.target, sh, th, types: new Set() }; byPair.set(key, entry); }
				// Resolve the connection's type to color the merged edge. A normal
				// source handle names one of the source node's OUTPUTS; an `__inner`
				// source (an expanded container feeding its own input to a child) names
				// one of the container's INPUTS (see dropDanglingPortEdges in apply.ts:
				// an inner source feeds an IN port). Resolving against the wrong list
				// returned undefined -> every inner-source edge fell back to 'Any'.
				const isInnerSrc = e.sourceHandle?.endsWith('__inner') ?? false;
				const cleanHandle = isInnerSrc ? e.sourceHandle!.slice(0, -'__inner'.length) : e.sourceHandle;
				const port = (isInnerSrc ? inputsOf(e.source) : outputsOf(e.source))?.find(p => p.name === cleanHandle);
				entry.types.add(port?.portType ?? 'Any');
			}
			return Array.from(byPair.values()).map(({ source, target, sh, th, types }) => {
				const edgeColor = types.size === 1
					? (PORT_TYPE_COLORS[[...types][0]] || PORT_TYPE_COLORS.TypeVar)
					: PORT_TYPE_COLORS.TypeVar;
				return {
					id: `simplified:${source}.${sh}->${target}.${th}`,
					source,
					target,
					sourceHandle: sh,
					targetHandle: th,
					type: 'custom',
					animated: false,
					zIndex: 5,
					// A simplified edge merges many real connections, so it is
					// non-interactive: not selectable, not deletable. Reconnect is
					// disabled where it actually matters: CustomEdge.svelte omits the
					// <EdgeReconnectAnchor> in simplified view (no grab zone, so the
					// gesture can't start), and the onReconnect*/onConnectEnd handlers
					// self-guard on `simplified` as defense. (Svelte Flow has no
					// per-edge `reconnectable` field, unlike React Flow, so the anchor
					// is the real lever.) Without this, dragging a merged edge's end
					// would record a removeEdge against the SYNTHETIC `__simp_*` handle.
					selectable: false,
					deletable: false,
					data: { simplified: true },
					style: `stroke-width: 2px; stroke: ${edgeColor};`,
					markerEnd: { type: MarkerType.ArrowClosed, width: 20, height: 20, color: edgeColor },
				};
			});
		}

		// Deduplicate edges - only keep one edge per target+targetHandle (last one wins)
		const seenTargets = new Map<string, typeof projectEdges[0]>();
		for (const e of projectEdges) {
			const key = `${e.target}:${e.targetHandle || 'default'}`;
			seenTargets.set(key, e);
		}
		const deduplicatedEdges = Array.from(seenTargets.values());

		return deduplicatedEdges.map((e) => {
			const edgeColor = getEdgeColor(e.source, e.sourceHandle, outputsOf);

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

	// Initial edge snapshot, seeded once from the props (same intent + `untrack`
	// rationale as the `nodes` seed above): reconciliation is handled by the
	// effects, so the prop read here is deliberately non-reactive.
	let edges = $state.raw<Edge[]>(untrack(() => buildEdges(project.edges, project.nodes)));

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

	// Everything dynamic painted onto the projected graph, gathered in one
	// place so there is ONE decoration pass (no per-source effects mutating
	// `nodes` in place). `readOverlayCtx()` reads every reactive source, so
	// calling it in an effect's tracked region registers all of them.
	type OverlayCtx = {
		nodeOutputs: Record<string, unknown>;
		nodeExecutions: Record<string, import('../../types').NodeExecution[]>;
		busLogByBus: Record<string, import('../../../../shared/protocol').BusInspectorEvent[]>;
		busesByNode: Record<string, string[]>;
		busMetaByBus: Record<string, import('../../../../shared/protocol').BusMeta>;
		loopEventsByGroup: Record<string, import('../../../../shared/protocol').LoopInspectorEvent[]>;
		journalCorruptions: Array<{ site: import('../../../../shared/protocol').CorruptionSite; reason: string }>;
		infraNodes: typeof infraNodes;
		fileContents: typeof fileContents;
		infraFeedByNode: typeof infraFeedByNode;
		signalFeedByNode: typeof signalFeedByNode;
		showInfraSubgraph: boolean;
		showTriggerSubgraph: boolean;
		execPrefix: string;
		projectNodes: import('../../types').NodeInstance[];
	};

	function readOverlayCtx(): OverlayCtx {
		const state = executionState;
		const busParticipantsByBus = state?.busParticipantsByBus || {};
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
		return {
			nodeOutputs: state?.nodeOutputs || {},
			nodeExecutions: state?.nodeExecutions || {},
			busLogByBus: state?.busLogByBus || {},
			busesByNode,
			busMetaByBus: state?.busMetaByBus ?? {},
			loopEventsByGroup: state?.loopEventsByGroup ?? {},
			journalCorruptions: state?.journalCorruptions ?? [],
			infraNodes,
			fileContents,
			infraFeedByNode,
			signalFeedByNode,
			showInfraSubgraph,
			showTriggerSubgraph,
			// Touch execPrefix in the tracked region so a navigation that only
			// changes the exec-id prefix re-decorates (decorate reads it via
			// execKey). Untrack the projection: the STRUCTURAL effect already
			// owns repaint-on-projection-change, so reading fold here too would
			// make every keystroke run BOTH effects (a redundant second full
			// decorate pass on the hottest path).
			execPrefix,
			projectNodes: untrack(() => fold.project.nodes),
		};
	}

	/** Paint every dynamic decoration (execution state, infra badges, file
	 *  contents, body feeds, subgraph highlight) onto a node/edge array.
	 *  Pure: spreads each node, never reorders or repositions. Used by BOTH
	 *  the structural rebuild and the overlay-tick refresh, so the two render
	 *  paths cannot drift. */
	function decorate(ns: Node[], es: Edge[], ctx: OverlayCtx): { nodes: Node[]; edges: Edge[] } {
		const {
			nodeOutputs, nodeExecutions, busLogByBus, busesByNode, busMetaByBus,
			loopEventsByGroup, journalCorruptions,
		} = ctx;
		// Subgraph highlight classes are computed over the CURRENT arrays so
		// highlighted/dimmed always reflects what is on screen.
		let subgraphNodeIds: Set<string> | null = null;
		let subgraphEdgeIds: Set<string> | null = null;
		let highlightedClass = '';
		let dimmedClass = '';
		if (ctx.showInfraSubgraph || ctx.showTriggerSubgraph) {
			const extractFn = ctx.showInfraSubgraph ? extractInfraSubgraph : extractTriggerSubgraph;
			highlightedClass = ctx.showInfraSubgraph ? 'infra-highlighted' : 'trigger-highlighted';
			dimmedClass = ctx.showInfraSubgraph ? 'infra-dimmed' : 'trigger-dimmed';
			const projectNodes = ns.map(n => ({
				id: n.id,
				nodeType: n.data.nodeType as string,
				label: n.data.label as string | null,
				config: n.data.config as Record<string, unknown>,
				position: n.position,
				inputs: n.data.inputs as any[],
				outputs: n.data.outputs as any[],
				features: NODE_TYPE_CONFIG[n.data.nodeType as string]?.features || {},
			}));
			const projectEdges = es.map(e => ({
				id: e.id,
				source: e.source,
				target: e.target,
				sourceHandle: e.sourceHandle || '',
				targetHandle: e.targetHandle || '',
			}));
			// eslint-disable-next-line @typescript-eslint/no-explicit-any
			const result = (extractFn as (n: any, e: any) => import('../../utils/subgraph').SubgraphResult)(projectNodes as any, projectEdges as any);
			subgraphNodeIds = result.nodeIds;
			subgraphEdgeIds = new Set(result.edges.map(e => e.id));
		}

		const decoratedNodes = ns.map(n => {
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

					let executions: import('../../types').NodeExecution[];

					if (isContainerNodeType(nodeType)) {
						const groupId = n.id;

						// Boundary passthrough executions (compiled IDs follow {groupId}__in / {groupId}__out)
						const inExecs = nodeExecutions[execKey(`${groupId}__in`)] || [];
						const outExecs = nodeExecutions[execKey(`${groupId}__out`)] || [];

						// Collect internal node executions via scope field (against the
						// CURRENT projected nodes, not the stale initial prop).
						const internalExecs: import('../../types').NodeExecution[] = [];
						for (const projNode of ctx.projectNodes) {
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
							const status: import('../../types').NodeExecutionStatus = hasRunning ? 'running'
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
								// An unknown member cost keeps the group honest too: the
								// summed figure alone would read as the full price.
								costUnknown: allRelated.some((e) => e.costUnknown),
								costOrigin: groupCostOrigin(allRelated),
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
						for (const projNode of ctx.projectNodes) {
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

					// Per-node infra badge (status snapshot from the host).
					const backendNode = ctx.infraNodes?.find(inf => inf.nodeId === n.id);

					// Per-node body-panel feed. Each node consumes AT MOST ONE feed
					// based on its role: infra nodes get infra /live ticks, trigger
					// nodes get listener /display ticks, anything else gets nothing.
					const role = nodeBodyFeedKind({
						nodeType,
						features: n.data.features as { isTrigger?: boolean } | undefined,
					});
					const bodyFeed =
						role === 'infra' ? ctx.infraFeedByNode?.[n.id]
						: role === 'signal' ? ctx.signalFeedByNode?.[n.id]
						: undefined;

					// Subgraph highlight wins the class slot while active; the
					// execution-status class paints otherwise.
					const cls = subgraphNodeIds
						? (subgraphNodeIds.has(n.id) ? highlightedClass : dimmedClass)
						: nodeClass;

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
							fileContents: ctx.fileContents,
							bodyFeed,
							infraNodeStatus: backendNode?.status,
							infraFailureStage: backendNode?.failureStage,
							infraFailureMessage: backendNode?.failureMessage,
						},
						class: cls,
					};
				});

		const decoratedEdges = subgraphEdgeIds
			? es.map(e => ({ ...e, class: subgraphEdgeIds.has(e.id) ? highlightedClass : dimmedClass }))
			: es.map(e => (e.class ? { ...e, class: '' } : e));

		return { nodes: decoratedNodes, edges: decoratedEdges };
	}

	// Infra / trigger subgraph highlighting toggles (decorate paints the classes).
	let showInfraSubgraph = $state(false);
	let showTriggerSubgraph = $state(false);

	// ── The render path ────────────────────────────────────────────────────
	/** Rebuild nodes/edges wholesale from the projected project + layout and
	 *  decorate. xyflow ephemeral view-state (selection) is carried over by
	 *  id; positions come from the layout merge inside buildNodes. Called by
	 *  the structural effect below AND by the engine's snapBack (a preflight
	 *  rejection where xyflow already moved nodes mid-gesture). */
	function rebuildFromProjection(): void {
		const f = fold;
		// Carry selection over by id: nodes selected before, the inspector's
		// focused node, AND any ids a gesture asked to select on its next
		// rebuild (new nodes that don't exist in the OLD `nodes` array yet, e.g.
		// freshly duplicated copies). The request set is consumed here.
		const prevSelected = new Set(nodes.filter(n => n.selected).map(n => n.id));
		const want = (id: string) => prevSelected.has(id) || id === selectedNodeId || selectOnNextRebuild.has(id);
		const built = buildNodes(f.project.nodes, f.project.edges, parseLayoutCode(engine.layoutCode, layoutVerb), nodes)
			.map(n => (want(n.id) ? { ...n, selected: true } : n));
		selectOnNextRebuild.clear();
		const decorated = decorate(built, buildEdges(f.project.edges, f.project.nodes), readOverlayCtx());
		nodes = decorated.nodes;
		edges = decorated.edges;
	}
	// Ids a just-recorded gesture wants selected once the projection rebuilds
	// (new nodes absent from the live `nodes` array at gesture time).
	const selectOnNextRebuild = new Set<string>();

	// STRUCTURAL rebuild: whenever the projection (truth + pendingOps) or the
	// layout changes, re-render from scratch.
	$effect(() => {
		const f = fold;
		void layoutCode; // tracked: layout-only changes re-render too
		untrack(() => {
			if (f.dropped.length > 0) {
				// An op stopped applying (it should have been pruned at truth-advance
				// time; this is the converging backstop). Route it through the one
				// failure path; the queue change re-derives and re-runs this effect.
				for (const d of f.dropped) engine.failPendingOp(d.op, d.reason);
				return;
			}
			rebuildFromProjection();
		});
	});

	// OVERLAY refresh: when any dynamic source ticks (execution SSE, infra
	// badges, file contents, body feeds, subgraph toggles), re-decorate the
	// CURRENT arrays in place (positions, selection, mid-gesture state are
	// preserved because decorate only spreads).
	$effect(() => {
		const ctx = readOverlayCtx();
		untrack(() => {
			const decorated = decorate(nodes, edges, ctx);
			nodes = decorated.nodes;
			edges = decorated.edges;
		});
	});

	// Simplified view sizes every node to its measured content (see simplifiedSizing),
	// so when a node gains a live display (an image loads, a feed grows) it RESIZES.
	// A resize can make it overlap a neighbour, so we re-run auto-organize to re-flow
	// around the new measured size (a full re-layout: in simplified view that is
	// accepted over preserving manual positions). Keyed off the REAL measured sizes
	// of leaf (non-container) nodes, so it fires on any actual resize (image, feed,
	// label) without duplicating the "has live display" predicate, and never loops:
	// auto-organize changes positions, not a leaf's content-driven measured size.
	// Debounced so a burst of streaming updates triggers one re-flow, not dozens.
	let leafSizeSig = '';
	let resizeReflowTimer: ReturnType<typeof setTimeout> | null = null;
	// True once the component is torn down, so an in-flight async organize that
	// resolves after teardown does not write `nodes` / persist on dead state.
	let destroyed = false;
	// Cancel a pending reflow: on teardown (the timer must not fire on dead state)
	// and when leaving simplified view (a stale reflow would run in builder view and
	// silently rewrite builder positions with no undo entry). This clears the
	// debounce timer; an organize ALREADY in flight (mid measure-wait) is caught
	// separately by runAutoOrganize re-checking the active view + `destroyed` before
	// it writes (see runAutoOrganize's apply guard), so neither window can persist
	// the wrong view's positions or touch a destroyed component.
	function cancelResizeReflow(): void {
		if (resizeReflowTimer) { clearTimeout(resizeReflowTimer); resizeReflowTimer = null; }
	}
	onDestroy(() => { destroyed = true; cancelResizeReflow(); });
	$effect(() => {
		if (!simplified) { leafSizeSig = ''; cancelResizeReflow(); return; }
		// Read measured sizes reactively so this re-runs when xyflow re-measures.
		const sig = nodes
			.filter(n => n.type === 'project' || n.type === 'groupCollapsed')
			.map(n => `${n.id}:${n.measured?.width ?? 0}x${n.measured?.height ?? 0}`)
			.join('|');
		untrack(() => {
			// Skip until the active view has had its initial organize (the mount /
			// view-switch effects own that); only react to LATER resizes.
			if (!organizedVerbs.has(layoutVerb)) { leafSizeSig = sig; return; }
			if (sig === leafSizeSig) return;
			leafSizeSig = sig;
			if (resizeReflowTimer) clearTimeout(resizeReflowTimer);
			// No fitView (don't yank the camera mid-execution); non-undoable (an
			// automatic re-flow is not a user action, must not pollute the undo stack).
			resizeReflowTimer = setTimeout(() => { resizeReflowTimer = null; void runAutoOrganize(false, false); }, 250);
		});
	});

	let selectedNodeId = $state<string | null>(null);

	let contextMenu = $state<{ x: number; y: number; flowX: number; flowY: number; nodeId: string | null } | null>(null);
	// An OPEN node menu in simplified view shows only infra lifecycle actions; if
	// the node's infra actions disappear while the menu is open (an execution tick
	// terminates the node), the menu would render empty. Close it instead, using the
	// SAME predicate that gates opening (nodeInfraActions), so open-time and
	// stay-open agree. The empty-area menu (no nodeId) always has Undo/Redo, never
	// empties.
	$effect(() => {
		if (contextMenu?.nodeId && simplified && !nodeInfraActions(contextMenu.nodeId).has) {
			contextMenu = null;
		}
	});
	let commandPaletteOpen = $state(false);

	// The node whose Tags editor is open (a small popover the node context menu
	// launches), plus where to anchor it. Null = closed. Tags are a source edit
	// (they change `config._tags`), so this is gated to non-simplified view.
	let tagEditor = $state<{ nodeId: string; x: number; y: number } | null>(null);

	// Apply a `_tags` change to a node through the SAME edit path config edits
	// use (createNodeUpdateHandler diffs it into a setConfig op + autosave).
	function setNodeTags(nodeId: string, next: string[]) {
		// Read the CURRENT config from projection truth (`fold`), NOT the render
		// `nodes` array. `_tags` is a source key, so it only repaints into `nodes`
		// after the projection round-trip; reading `nodes` here would see a stale
		// pre-pending-op value, so two rapid tag edits would both build on the same
		// old base and the second would drop the first's tag. `fold.project.nodes`
		// folds pending ops synchronously, so each edit builds on the latest.
		const node = fold.project.nodes.find((n) => n.id === nodeId);
		if (!node) return;
		// An empty tag list means "no tags", i.e. REMOVE the key, not `_tags: []`
		// (which would leave a `_tags = []` cruft line in source). diffConfigOps
		// emits a removeConfig only for a key that is PRESENT-but-undefined in the
		// update (a key merely absent is never visited), so pass `undefined` for the
		// empty case rather than omitting or writing `[]`.
		const config = {
			...((node.config as Record<string, unknown>) ?? {}),
			[TAGS_CONFIG_KEY]: next.length > 0 ? next : undefined,
		};
		createNodeUpdateHandler(nodeId)({ config });
	}

	// Flow position saved from the context menu (right-click) for placing nodes
	let contextMenuFlowPos = $state<{ x: number; y: number } | null>(null);

	// Track pending connection for "drop on empty" feature
	let pendingConnection = $state<{ sourceNodeId: string; sourceHandle: string | null } | null>(null);


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
		simplified?: boolean;    // square nodes, fixed size, no port/config sizing
	};
	type Sizing = { type: 'group' | 'groupCollapsed' | 'annotation' | 'project'; zIndex: number; style: string; width?: number; height?: number };
	// Simplified-view node sizing: SIZE TO CONTENT, never a pinned size. A bare
	// node's content is constrained to a square in ProjectNode/GroupNode, so it
	// measures as a SIMPLIFIED_SQUARE_PX square; a node with a live display (image,
	// feed, debug preview) grows its card and measures bigger. We pass NO width/
	// height prop so xyflow reads the real DOM size into `n.measured`, which is what
	// the layout engine consumes. `min-width`/`min-height` floor the empty square;
	// `max-width` caps a runaway card. When a live display appears and the node
	// resizes, the overlay effect re-runs auto-organize so the layout re-reads the
	// new measured size (a full re-flow, accepted over preserving manual positions).
	// A `function` (hoisted), not a const arrow: computeSizing runs during the
	// `$state.raw(buildNodes(...))` initializer above, so a const declared here
	// would be in its temporal dead zone (the "Loading graph..." crash).
	function simplifiedSizing(type: 'project' | 'groupCollapsed'): Sizing {
		return {
			type, zIndex: 4,
			style: `width: max-content; min-width: ${SIMPLIFIED_SQUARE_PX}px; max-width: ${SIMPLIFIED_CARD_MAX_W_PX}px; min-height: ${SIMPLIFIED_SQUARE_PX}px; height: auto;`,
		};
	}
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
			if (s.simplified) return simplifiedSizing('groupCollapsed');
			const minW = computeMinNodeWidth(s.inputs, s.outputs);
			return { type: 'groupCollapsed', zIndex: 4, style: `width: ${minW}px; height: auto;` };
		}
		if (s.simplified) return simplifiedSizing('project');
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
			simplified: newData.simplified as boolean | undefined,
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
		// getBoundingClientRect is SCREEN space (scaled by the canvas zoom), but the
		// node sizes we feed ELK come from `n.measured`, which xyflow stores in GRAPH
		// space (unscaled). Mixing the two means that when auto-organize runs while
		// zoomed out, every port Y shrinks by the zoom factor and the ports collapse
		// into a tiny band at the node's top, so ELK sees near-coincident anchors and
		// can't separate the edges (the tangled-layout regression). Divide the
		// relative offset by the live zoom to bring port Ys back into graph space.
		const zoom = getViewport().zoom || 1;
		// Find all handles inside this node
		const handles = nodeEl.querySelectorAll('.svelte-flow__handle');
		for (const handle of handles) {
			const handleId = handle.getAttribute('data-handleid');
			if (!handleId) continue;
			const handleRect = handle.getBoundingClientRect();
			// Y relative to node top, converted screen -> graph space.
			const relativeY = (handleRect.top + handleRect.height / 2 - nodeRect.top) / zoom;
			portYMap.set(handleId, relativeY);
		}
		// Simplified view: the node renders only ONE input dot and ONE output dot,
		// but the layout engine (shared with the builder view) looks up each REAL
		// port by name. So alias every real input port name to the single in-dot's
		// measured Y and every real output (plus `_raw`) to the out-dot's. The
		// engine then naturally collapses all of a node's ports onto its two dots,
		// using the exact positions measured on screen. This keeps ONE layout path
		// for both views: it always reads "the dots that actually exist".
		if (simplified) {
			const inY = portYMap.get(SIMPLIFIED_IN_HANDLE);
			const outY = portYMap.get(SIMPLIFIED_OUT_HANDLE);
			const node = nodes.find(n => n.id === nodeId);
			const inputs = (node?.data.inputs as PortDefinition[] | undefined) ?? [];
			const outputs = (node?.data.outputs as PortDefinition[] | undefined) ?? [];
			if (inY !== undefined) for (const p of inputs) portYMap.set(p.name, inY);
			if (outY !== undefined) {
				for (const p of outputs) portYMap.set(p.name, outY);
				portYMap.set('_raw', outY);
			}
		}
		return portYMap;
	}

	// `undoable`: a user-initiated organize (palette button, collapse/expand,
	// first-show) records an undo entry; an AUTOMATIC re-flow (a node resized
	// because live-display content arrived) persists without one, so a streaming
	// execution can't bury the user's real edits under reflow frames.
	// Outcome of an organize: it APPLIED, or it bailed because the active view
	// CHANGED mid-run (a later toggle back will re-fire and re-organize), or because
	// the component was DESTROYED. Callers that claimed a verb in `organizedVerbs`
	// un-claim ONLY on 'view-changed' (so the toggle-back recovers); 'destroyed'
	// needs no recovery, and re-inferring the reason at the call site is fragile, so
	// it's returned explicitly.
	type OrganizeOutcome = 'applied' | 'view-changed' | 'destroyed';
	async function runAutoOrganize(andFitView = false, undoable = true): Promise<OrganizeOutcome> {
		// The view this organize is FOR. ELK runs across an up-to-2s measure-wait,
		// during which the user can toggle views or close the editor. The result is
		// only valid for the view that was active at entry, so the apply step below
		// bails if the active view changed (or the component was destroyed) by the
		// time ELK resolves. Without this, a reflow scheduled in simplified view
		// could resolve after a toggle and persist BUILDER positions (wrong verb,
		// non-undoable), or write `nodes` on a torn-down component.
		const startVerb = layoutVerb;
		// Wait until every visible node has a measured DOM size before reading
		// sizes. ELK is only as good as the sizes it's fed: a node measured at a
		// stale (pre-toggle) size makes ELK place neighbours against the wrong
		// edge (the "node stuck to the collapsed group" bug). The palette path got
		// this right by waiting externally; doing it HERE makes every caller
		// correct (collapse/expand, mount, manual) with one wait. Cap at 2s.
		{
			// Wait until sizes are not just present but STABLE across two polls. A
			// bare "every node has a measured size" check breaks too early right after
			// a view toggle: the nodes still carry the PREVIOUS view's measured sizes
			// (builder boxes) until Svelte re-renders the squares and the ResizeObserver
			// catches up, so ELK would run on stale sizes and the first simplified open
			// looks wrong. Requiring the size signature to repeat means the DOM has
			// settled into the CURRENT view before we read it.
			const deadline = Date.now() + 2000;
			let prevSig = '';
			let settled = false;
			while (Date.now() < deadline) {
				await tick();
				const allMeasured = nodes.every(n => n.style === 'display: none;' || (n.measured?.width && n.measured?.height));
				const sig = nodes.map(n => `${n.id}:${n.measured?.width ?? 0}x${n.measured?.height ?? 0}`).join('|');
				if (allMeasured && sig === prevSig) { settled = true; break; }
				prevSig = sig;
				await new Promise(resolve => setTimeout(resolve, 50));
			}
			// Fail loud (breadcrumb): the wait's whole point is to feed ELK a SETTLED
			// DOM. If sizes never stabilized within the cap, ELK runs on whatever's
			// there, which can be a wrong layout; surface it instead of failing silent.
			if (!settled) console.warn(`[auto-organize] node sizes never settled within 2s (${nodes.length} nodes); laying out on unstable sizes.`);
		}
		const sizes = new Map<string, { width: number; height: number }>();
		for (const n of nodes) {
			if (n.measured?.width && n.measured?.height) {
				sizes.set(n.id, { width: n.measured.width, height: n.measured.height });
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
		// Feed ELK the RAW edges (original `__inner` handles), not the live xyflow
		// edges: in simplified view those are collapsed to `__simp_*` handles, but
		// autoOrganize's simplified mapping needs the real direction/inner markers
		// to place endpoints. It collapses topology to one in/out port per node
		// itself, so parallel raw edges between a pair are harmless.
		const rawEdges = fold.project.edges.map(e => ({
			id: e.id,
			source: e.source,
			target: e.target,
			sourceHandle: e.sourceHandle ?? null,
			targetHandle: e.targetHandle ?? null,
		}));
		return autoOrganize(currentNodes, rawEdges, sizes, portPositions, simplified).then(({ positions, groupSizes }) => {
			// The view changed (toggle) or the editor closed while ELK ran: this
			// result is stale, discard it rather than write the wrong view's layout
			// or mutate a destroyed component. (layoutVerb is the view identity:
			// @slayout in simplified, @layout in builder.)
			if (destroyed) return 'destroyed';
			if (layoutVerb !== startVerb) return 'view-changed';
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
			// ELK positions are LAYOUT, not source. Persist EVERY visible node, not
			// only the ones ELK repositioned: ELK may leave a node in place (no
			// `positions` entry), and a collapse/expand toggle drives this path
			// precisely to persist the toggled node's `expanded` flag. Gating on ELK
			// movement dropped that flag for untouched nodes, so they snapped back to
			// their default on the next rebuild (the intermittent recollapse). Hidden
			// nodes (display:none under a collapsed ancestor) keep their stored entry.
			// Undoable (user-initiated) captures one reversible layout action; an
			// automatic re-flow persists with no undo entry.
			const persistAll = (layout: string) => {
				let next = layout;
				for (const n of nodes) {
					if (n.style === 'display: none;') continue;
					next = layoutUpdateAny(n)(next);
				}
				return next;
			};
			if (undoable) recordEdit([], persistAll);
			else persistLayoutEdit(persistAll);
			if (andFitView) setTimeout(() => doFitView(), 50);
			return 'applied';
		});
	}

	/** Apply a host parseResult (a text-tab edit, focus change, background
	 *  tick): truth replacement always wins. There is no bail and no staleness
	 *  guard anymore: pending ops re-apply ON TOP of any incoming truth (the
	 *  race the old guards protected against is structurally gone), and ops the
	 *  new truth invalidated drop through the one rollback path with a toast. */
	export function applyExternalSource(newProject: ProjectDefinition, newWeftCode: string, newLayoutCode: string): void {
		engine.applyExternalSource(newProject, newWeftCode, newLayoutCode);
	}

	// Fit view to graph on initial load
	let hasFitView = $state(false);
	let hasAutoOrganized = $state(false);
	// Hide canvas until initial ELK layout completes to avoid flash of ugly unorganized positions
	let canvasReady = $state(false);
	// The set of view verbs (@layout / @slayout) whose positions have already been
	// established (saved on disk OR organized-on-first-show), so neither the mount
	// effect nor the view-switch effect organizes the same view twice. Seeded by
	// BOTH effects: whichever first handles a verb claims it, so they can't both
	// fire runAutoOrganize for one view (the double-organize race: two concurrent
	// recordEdits + racing `nodes=`). A plain Set (not $state) on purpose: it's a
	// write-only ledger; making it reactive would re-run the effects on their own
	// mutation. The wait-for-measured + ELK run lives solely in runAutoOrganize, so
	// both effects just `await runAutoOrganize(true)` (no duplicated wait loop).
	let organizedVerbs = new Set<LayoutVerb>();
	$effect(() => {
		if (!hasFitView && nodes.length > 0) {
			hasFitView = true;
			// Auto-organize if the ACTIVE VIEW has no saved positions (the builder
			// and simplified views keep separate position blocks; a project laid out
			// in builder has none for simplified, so it must organize on first show).
			const activeViewHasPositions = Object.keys(parseLayoutCode(layoutCode, layoutVerb)).length > 0;
			// Claim this verb so the view-switch effect below doesn't ALSO organize it
			// on this same mount (both would otherwise see empty positions and race).
			const claimedVerb = layoutVerb;
			organizedVerbs.add(claimedVerb);
			if (!activeViewHasPositions || autoOrganizeOnMount) {
				// No saved layout or explicitly requested: run ELK to compute positions.
				// runAutoOrganize waits for measured sizes internally (see its body).
				hasAutoOrganized = true;
				void (async () => {
					// Un-claim ONLY on 'view-changed' (the user switched away mid-run): a
					// toggle BACK then re-fires the view-switch effect, which re-claims and
					// re-organizes. 'destroyed' needs no recovery (component is gone), so
					// leave the claim.
					if (await runAutoOrganize(true) === 'view-changed') organizedVerbs.delete(claimedVerb);
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

	// Switching to a view whose position block is empty (first time the simplified
	// view is shown for a project laid out only in builder, or vice versa)
	// organizes it once, then fits. Guarded by `organizedVerbs` so it can't loop,
	// double-fire with the mount effect, or fight a user who then drags things.
	$effect(() => {
		const verb = layoutVerb;
		void layoutCode;
		untrack(() => {
			if (!hasFitView || organizedVerbs.has(verb)) return;
			if (Object.keys(parseLayoutCode(layoutCode, verb)).length > 0) {
				organizedVerbs.add(verb);
				return;
			}
			if (nodes.length === 0) return;
			organizedVerbs.add(verb);
			// Un-claim ONLY on 'view-changed' (switched away mid-run): a toggle back
			// re-fires this effect and re-organizes. 'destroyed' needs no recovery.
			void runAutoOrganize(true).then((outcome) => { if (outcome === 'view-changed') organizedVerbs.delete(verb); });
		});
	});

	// Handle actions from command palette
	// Actions that mutate the graph's STRUCTURE (source). Blocked in simplified
	// view, where the only allowed interactions are visual: expand/collapse and
	// reposition. Creating or deleting would be ambiguous against collapsed
	// nodes and merged edges, so it is refused here (toggle back to builder).
	const STRUCTURAL_ACTIONS = new Set(['duplicate', 'delete', 'addNode', 'paste', 'group', 'ungroup']);
	function handlePaletteAction(action: string) {
		if (simplified && STRUCTURAL_ACTIONS.has(action)) {
			toast.info('Switch to the builder view to edit the graph', { description: 'Simplified view is read-only (you can still move, expand, and collapse).', duration: 4000 });
			return;
		}
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
			case 'duplicate': {
				// Duplicate the whole selection as one batch (one undo unit).
				const selected = nodes.filter(n => n.selected).map(n => n.id);
				const ids = selected.length > 0 ? selected : (selectedNodeId ? [selectedNodeId] : []);
				if (ids.length > 0) duplicateNodes(ids);
				break;
			}
			case 'delete': {
				// Selected edges take priority over nodes (matches canvas Delete).
				const selectedEdges = edges.filter(e => e.selected);
				if (selectedEdges.length > 0) {
					// The projection paints the removal; no live edge mutation.
					recordEdit(selectedEdges.map(e => {
						const ref = toWeftEdgeRef(e.source, e.sourceHandle || 'value', e.target, e.targetHandle || 'value');
						return { op: 'removeEdge' as const, source: ref.srcRef, sourcePort: ref.srcPort, target: ref.tgtRef, targetPort: ref.tgtPort, scopeGroup: ref.scopeGroupLabel ?? null };
					}));
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
		// Simplified view is look-only for wiring: the single in/out dots don't
		// map to real ports, so no edge can be authored or reconnected here. The
		// user toggles back to the builder view to change connections.
		if (simplified) return false;
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
		if (simplified) return; // simplified edges are merged + non-interactive
		reconnectSuccessful = false;
		// Set connection line color based on the edge being reconnected
		if (edge?.source) {
			currentConnectionColor = getEdgeColor(edge.source, edge.sourceHandle);
		}
	}
	
	// The reconnect twin of onBeforeConnect: xyflow calls this BEFORE it
	// rewires the edge in its (two-way-bound) edges array, so returning
	// null here is the only veto point that leaves the old edge painted.
	// A veto also marks the gesture handled: with the rewire blocked,
	// onreconnect never runs, and without the flag onReconnectEnd would
	// read the drop as landed-on-empty and delete the edge.
	// eslint-disable-next-line @typescript-eslint/no-explicit-any
	function onBeforeReconnect(newEdge: any): any {
		if (vetoLiteralDrivenTarget(newEdge.target, newEdge.targetHandle)) {
			reconnectSuccessful = true;
			return null;
		}
		return newEdge;
	}

	// eslint-disable-next-line @typescript-eslint/no-explicit-any
	function onReconnect(oldEdge: any, newConnection: any) {
		if (simplified) return; // no wiring edits in simplified view
		reconnectSuccessful = true;

		// Remove old edge, add new one: one atomic batch. The projection paints
		// the swap as soon as the op is appended; no live edge mutation.
		const oldRef = toWeftEdgeRef(oldEdge.source, oldEdge.sourceHandle || 'value', oldEdge.target, oldEdge.targetHandle || 'value');
		const newRef = toWeftEdgeRef(newConnection.source, newConnection.sourceHandle || 'value', newConnection.target, newConnection.targetHandle || 'value');
		recordEdit([
			{ op: 'removeEdge', source: oldRef.srcRef, sourcePort: oldRef.srcPort, target: oldRef.tgtRef, targetPort: oldRef.tgtPort, scopeGroup: oldRef.scopeGroupLabel ?? null },
			{ op: 'addEdge', source: newRef.srcRef, sourcePort: newRef.srcPort, target: newRef.tgtRef, targetPort: newRef.tgtPort, scopeGroup: newRef.scopeGroupLabel ?? null },
		]);
	}

	// eslint-disable-next-line @typescript-eslint/no-explicit-any
	function onReconnectEnd(event: MouseEvent | TouchEvent, edge: any) {
		if (simplified) return; // no wiring edits in simplified view
		// Reconnect dropped on empty space = remove the edge (the gesture's
		// meaning, expressed as the same removeEdge op a delete uses).
		if (!reconnectSuccessful) {
			const ref = toWeftEdgeRef(edge.source, edge.sourceHandle || 'value', edge.target, edge.targetHandle || 'value');
			recordEdit([{ op: 'removeEdge', source: ref.srcRef, sourcePort: ref.srcPort, target: ref.tgtRef, targetPort: ref.tgtPort, scopeGroup: ref.scopeGroupLabel ?? null }]);
		}
		reconnectSuccessful = false;
	}

	// Flag to prevent click handler from immediately closing the context menu after drop
	let justOpenedContextMenu = false;
	
	// eslint-disable-next-line @typescript-eslint/no-explicit-any
	function onConnectEnd(event: MouseEvent | TouchEvent, connectionState: any) {
		if (simplified) return; // no new connections / add-on-drop in simplified view
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

	// One driver per port: a target port already driven by a body-set
	// literal cannot ALSO take a wire. The compile rejects it
	// (`double-driven-port`); every wire-landing gesture (fresh connect,
	// reconnect) vetoes through here with a toast so the user never
	// authors the error. A container's self-referencing input handle
	// carries an `__inner` suffix; portLiterals is keyed by bare port
	// name. Returns true when the drop was vetoed.
	function vetoLiteralDrivenTarget(target: string, targetHandle: string | null | undefined): boolean {
		const targetPort = (targetHandle || 'value').replace(/__inner$/, '');
		const targetNode = fold.project.nodes.find((n) => n.id === target);
		const literal = (targetNode?.portLiterals as Record<string, unknown> | undefined)?.[targetPort];
		if (literal === undefined || literal === null) return false;
		toast.error(`'${targetPort}' is driven by a config assignment; unset it first to drive it with an edge.`);
		return true;
	}

	function onBeforeConnect(connection: Connection): Edge | null {
		// Clear pending connection since we're making a real connection
		pendingConnection = null;

		if (vetoLiteralDrivenTarget(connection.target!, connection.targetHandle)) return null;

		// Record the intent and let the PROJECTION add the edge: preflight
		// rejects cycles/locks with a toast, the apply mirrors the server's
		// replace-existing-driver semantics, and a later rejection snaps back
		// through the one rollback path. Returning null tells xyflow not to add
		// its own optimistic edge (the projection's re-derive paints it).
		const ref = toWeftEdgeRef(connection.source!, connection.sourceHandle || 'value', connection.target!, connection.targetHandle || 'value');
		recordEdit([{ op: 'addEdge', source: ref.srcRef, sourcePort: ref.srcPort, target: ref.tgtRef, targetPort: ref.tgtPort, scopeGroup: ref.scopeGroupLabel ?? null }]);
		return null;
	}

	function getViewportCenter(): { x: number; y: number } {
		const flowContainer = document.querySelector('.svelte-flow');
		if (flowContainer) {
			const rect = flowContainer.getBoundingClientRect();
			return screenToFlowPosition({ x: rect.left + rect.width / 2, y: rect.top + rect.height / 2 });
		}
		return { x: 250, y: 150 };
	}

	function addNode(type: NodeType) {
		// Simplified view is read-only for structure: refuse node creation (the
		// command palette / context menu reach this directly, not via the gate).
		if (simplified) {
			toast.info('Switch to the builder view to add nodes', { description: 'Simplified view is read-only (you can still move, expand, and collapse).', duration: 4000 });
			return;
		}
		const isGroup = type === 'Group';
		const isLoop = type === 'Loop';
		const isContainer = isGroup || isLoop;
		// A container is declared in source by its label (`MyGroup = Group()...`
		// or `MyLoop = Loop()...`), so the label IS its node id once parsed.
		// Seed names from a safe default, NOT the type's display label:
		// "Group" / "Loop" are reserved type keywords. "MyGroup" / "MyLoop"
		// are valid, non-reserved starting points.
		// New top-level node: collision-safe id, via the SAME minting helpers
		// duplicate uses (root scope = parentId undefined).
		const taken = new Set(fold.project.nodes.map(n => n.id));
		const containerLabel = isContainer
			? freshScopedLabel(isLoop ? 'MyLoop' : 'MyGroup', undefined, taken).label
			: null;
		const id = isContainer ? containerLabel! : freshScopedNodeId(type, undefined, taken).localId;
		const pos = contextMenuFlowPos ?? getViewportCenter();
		contextMenuFlowPos = null;
		// Select the new node; the structural rebuild marks it selected by id.
		nodes = nodes.map(n => (n.selected ? { ...n, selected: false } : n));
		selectedNodeId = id;
		const op: EditOp = isLoop
			? { op: 'addLoop', label: containerLabel!, parentGroup: null }
			: isGroup
				? { op: 'addGroup', label: containerLabel!, parentGroup: null }
				: { op: 'addNode', id, nodeType: type, parentGroup: null };
		recordEdit([op], (layout) => {
			if (isContainer) {
				// Default container size: groups get 500x350, loops a taller box to
				// fit the config strip plus a few body nodes. GroupNode's min-height
				// logic bumps it once ports are added; a generous start keeps the
				// first paint pleasant. Loop config left empty: parallel defaults to
				// false, over/carry to empty lists.
				const w = isLoop ? 600 : 500;
				const h = isLoop ? 500 : 350;
				return updateLayoutEntry(layout, containerLabel!, pos.x, pos.y, w, h, true);
			}
			return updateLayoutEntry(layout, id, pos.x, pos.y);
		});
	}

	function deleteNodes(nodeIds: string[]) {
		if (nodeIds.length === 0) return;

		// Classify each id: containers route by kind (`removeLoop` vs
		// `removeGroup`; the Rust side rejects a mismatch loudly), everything
		// else is `removeNode`. Projection-built containers always carry their
		// kind in data.nodeType. The visual removal (and a removed group's
		// children climbing into the parent scope) comes from the projection.
		const containers = new Map<string, 'Group' | 'Loop'>();
		for (const nodeId of nodeIds) {
			const n = nodes.find(nd => nd.id === nodeId);
			if (n && (n.type === 'group' || n.type === 'groupCollapsed')) {
				containers.set(nodeId, containerKindOf(n.data.nodeType) === 'Loop' ? 'Loop' : 'Group');
			}
		}

		if (selectedNodeId && nodeIds.includes(selectedNodeId)) {
			selectedNodeId = null;
		}
		contextMenu = null;
		// One atomic batch. Non-container nodes first so children are removed
		// while still inside their group scope; layout entries clear locally.
		// Containers ride their SCOPED id (it's unambiguous at any depth).
		const ops: EditOp[] = [];
		const layoutKeysToDrop: string[] = [];
		for (const nodeId of nodeIds) {
			if (!containers.has(nodeId)) {
				ops.push({ op: 'removeNode', node: nodeId });
				layoutKeysToDrop.push(nodeId);
			}
		}
		// Container removals DEEPEST-FIRST: ungrouping a container re-keys its
		// children (a nested `outer.inner` becomes `inner` when `outer`
		// dissolves), so removing the parent first would leave a later
		// `removeGroup outer.inner` unresolvable ("node not found"). Removing the
		// deepest container first never touches a shallower one's id (children
		// climb INTO the shallower container, whose id is unchanged), so every op
		// resolves and the final ids match computeUngroupLayoutMoves' mapping.
		const containersDeepFirst = [...containers].sort(
			(a, b) => b[0].split('.').length - a[0].split('.').length,
		);
		for (const [nodeId, kind] of containersDeepFirst) {
			ops.push(kind === 'Loop' ? { op: 'removeLoop', loopId: nodeId } : { op: 'removeGroup', group: nodeId });
			layoutKeysToDrop.push(nodeId);
		}
		// A removed container UNGROUPS: every node under it climbs out, its
		// scoped id losing each removed-container segment (`outer.inner.child`
		// with both `outer` and `outer.inner` removed -> `child`). The layout
		// entries must follow, re-keyed to the final id and offset so the node
		// stays put visually (a child's coords were relative to its dissolved
		// parent, which itself sat somewhere in the grandparent frame; summing
		// every dissolved ancestor's position gives the total shift). Computing
		// each node's FINAL mapping in one pass (not one hop per container)
		// composes correctly at any nesting depth, the failure of the naive
		// per-container version. Without this, children strand under dead keys
		// and buildNodes re-stacks them at the origin.
		const childMoves = computeUngroupLayoutMoves(containers, fold.project.nodes, parseLayoutCode(engine.layoutCode));
		recordEdit(ops, (layout) => {
			let next = layout;
			for (const key of layoutKeysToDrop) next = removeLayoutEntry(next, key);
			// Rename deepest keys first so a parent's rename can't swallow a
			// child's not-yet-processed entry (renameLayoutSubtree re-keys a
			// whole subtree; deepest-first keeps each move exact).
			for (const m of childMoves) {
				next = renameLayoutSubtree(next, m.oldKey, m.newKey);
				if (m.dx !== 0 || m.dy !== 0) {
					const e = parseLayoutCode(next)[m.newKey];
					if (e) next = updateLayoutEntry(next, m.newKey, e.x + m.dx, e.y + m.dy, e.w, e.h, e.expanded ?? null, e.configCollapsed ?? null);
				}
			}
			return next;
		});
	}

	/** The layout re-key moves for ungrouping `removed` containers: for every
	 *  node that climbs out, its FINAL new scoped id (removed-container segments
	 *  stripped) and the cumulative position offset (sum of each dissolved
	 *  ancestor's layout position). Pure geometry over the projected nodes +
	 *  the current layout map; mirrors apply.ts's ungroup id semantics. Moves
	 *  are returned DEEPEST-key-first so subtree renames don't clobber. */
	function computeUngroupLayoutMoves(
		removed: Map<string, 'Group' | 'Loop'>,
		projectNodes: import('../../types').NodeInstance[],
		layoutMap: Record<string, import('../../layout').LayoutEntry>,
	): Array<{ oldKey: string; newKey: string; dx: number; dy: number }> {
		const byId = new Map(projectNodes.map(n => [n.id, n] as const));
		const moves: Array<{ oldKey: string; newKey: string; dx: number; dy: number }> = [];
		for (const node of projectNodes) {
			if (removed.has(node.id)) continue; // the container itself is dropped, not re-keyed
			// Walk up from the node's parent collecting the CONTIGUOUS removed
			// ancestors (the ones whose frames the node falls through), stopping
			// at the first surviving ancestor. Removed ancestors ABOVE a
			// surviving intermediate parent don't move THIS node (its surviving
			// parent does), so they must not count toward its offset.
			const dissolved: string[] = [];
			let pid = node.parentId;
			while (pid && removed.has(pid)) {
				dissolved.push(pid);
				pid = byId.get(pid)?.parentId;
			}
			if (dissolved.length === 0) continue; // direct parent survived: no climb
			const survivingParent = pid; // first non-removed ancestor (or undefined = root)
			const localId = node.id.split('.').pop()!;
			const newKey = scopedLayoutKey(localId, survivingParent);
			// Offset: sum of the fallen-through frames' parent-relative positions.
			let dx = 0, dy = 0;
			for (const anc of dissolved) {
				const e = layoutMap[anc];
				dx += e?.x ?? 0;
				dy += e?.y ?? 0;
			}
			moves.push({ oldKey: node.id, newKey, dx, dy });
		}
		// Deepest old key first (more dots = deeper) so a renameLayoutSubtree of
		// an ancestor doesn't pre-empt a descendant's own move.
		moves.sort((a, b) => b.oldKey.split('.').length - a.oldKey.split('.').length);
		return moves;
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
	// The key name we pass to xyflow's `deleteKey`/`zoomActivationKey`/
	// `panActivationKey` to disable those built-in bindings. The documented
	// disable is `null`, but xyflow (<= 1.6.1) turns a nulled binding into an
	// ACTIVE trigger with an empty key, and its keyboard helper
	// (@svelte-put/shortcut) then warns "Trigger should have either `key` or
	// `code`" on every keydown. A key name no keyboard event can ever produce
	// registers a valid trigger that simply never matches: same disabled
	// behavior, no warning, no library patch.
	const UNBOUND_KEY = 'WeftUnboundKey';
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

	/** Is `node`'s CENTRE inside `group`'s box? The ONE containment test shared by
	 *  every drag in/out decision (leave-group, captured-by-group, group-captures).
	 *  Keying on the node's centre (not its top-left corner) makes "leaves" and
	 *  "enters" symmetric about the same boundary: a node leaves exactly when its
	 *  centre exits and is captured exactly when its centre enters. The old
	 *  top-left-corner tests disagreed by the node's own width/height, so a node
	 *  could be dragged visually out of (or into) a group without its corner
	 *  crossing the edge, and no move op was emitted (a silent non-move). */
	function nodeCentreInGroup(node: Node, group: Node): boolean {
		const nodeAbs = getAbsolutePosition(node);
		const nodeDims = getNodeRect(node);
		const cx = nodeAbs.x + nodeDims.width / 2;
		const cy = nodeAbs.y + nodeDims.height / 2;
		const groupAbs = getAbsolutePosition(group);
		const { width: gw, height: gh } = getGroupDimensions(group);
		return cx >= groupAbs.x && cx <= groupAbs.x + gw && cy >= groupAbs.y && cy <= groupAbs.y + gh;
	}

	function onNodeDragStart({ targetNode }: { targetNode: Node | null; event: MouseEvent | TouchEvent; nodes: Node[] }) {
		// Bring dragged node to front. No pre-drag position snapshot: a rejected
		// drop snaps back by re-deriving from the projection (one rollback path).
		if (targetNode) {
			nodes = nodes.map(n => n.id === targetNode.id ? { ...n, zIndex: nextNodeZ } : n);
			nextNodeZ++;
		}
	}
	
	function onNodeDragStop({ targetNode, nodes: draggedNodes }: { targetNode: Node | null; nodes: Node[] }) {
		if (!targetNode) return;

		// One pending op + one undo unit for the whole gesture: any reparent
		// (move ops from the drop/capture resolution) + the final positions are
		// recorded together. A move the preflight rejects (e.g. the node still
		// has in-scope wires) rejects the WHOLE gesture: the projection
		// re-derive snaps every dragged node back, one toast explains why.
		const draggedIds = new Set(draggedNodes.map(dn => dn.id));
		transaction(() => {
			const movedIds = new Set<string>();
			// Simplified view is READ-ONLY for structure: a drag may REPOSITION but
			// must not REPARENT. Re-scoping (dropping a node into a group) rewrites
			// the .weft source, so it is suppressed here; only the position write
			// below runs. (Builder view does both.)
			const currentNode = simplified ? undefined : nodes.find(n => n.id === targetNode.id);
			if (currentNode) {
				if (applyNodeScopeChange(currentNode)) movedIds.add(currentNode.id);
				if (currentNode.type === 'group' || currentNode.type === 'groupCollapsed') {
					checkGroupCapturesNodes(currentNode, draggedIds);
				}
			}
			recordEdit([], (layout) => {
				let next = layout;
				for (const dn of draggedNodes) {
					if (skipGenericPositionWrite(dn.id, movedIds)) continue;
					const n = nodes.find(nd => nd.id === dn.id);
					if (n) next = layoutUpdateAny(n)(next);
				}
				return next;
			});
		});
	}

	/** A co-dragged node's generic position write must be SKIPPED when the node
	 *  itself OR any ancestor changed scope: the move closure's subtree re-key
	 *  already owns its position under the NEW scoped key, and a generic write
	 *  keys off the still-old parent, stranding a stale layout entry. */
	function skipGenericPositionWrite(nodeId: string, movedIds: Set<string>): boolean {
		if (movedIds.has(nodeId)) return true;
		let pid = (nodes.find(n => n.id === nodeId)?.data.config as Record<string, string> | undefined)?.parentId;
		while (pid) {
			if (movedIds.has(pid)) return true;
			pid = (nodes.find(n => n.id === pid)?.data.config as Record<string, string> | undefined)?.parentId;
		}
		return false;
	}

	function onSelectionDragStop(_event: MouseEvent, selectedNodes: Node[]) {
		// One pending op + one undo unit for the whole multi-select drag.
		const draggedIds = new Set(selectedNodes.map(sn => sn.id));
		transaction(() => {
			const movedIds = new Set<string>();
			// Simplified view is READ-ONLY for structure: reposition only, no reparent
			// (see onNodeDragStop). The scope-change scan is skipped; positions persist.
			for (const selectedNode of (simplified ? [] : selectedNodes)) {
				const node = nodes.find(n => n.id === selectedNode.id);
				if (!node) continue;
				if (applyNodeScopeChange(node)) movedIds.add(node.id);
				if (node.type === 'group' || node.type === 'groupCollapsed') {
					checkGroupCapturesNodes(node, draggedIds);
				}
			}
			recordEdit([], (layout) => {
				let next = layout;
				for (const sn of selectedNodes) {
					if (skipGenericPositionWrite(sn.id, movedIds)) continue;
					const n = nodes.find(nd => nd.id === sn.id);
					if (n) next = layoutUpdateAny(n)(next);
				}
				return next;
			});
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

	/** The group a dragged node should END UP in, given where its centre landed:
	 *  the deepest, then smallest, EXPANDED, visible group whose box contains the
	 *  centre (excluding the node itself and its own descendants), or `undefined`
	 *  for top level. This single resolver subsumes both "leaves its group" (the
	 *  centre escaped the parent, so the parent is no longer the best container)
	 *  and "captured by a group" (the centre entered one). Computing the NET
	 *  destination once is what lets a drag emit exactly one move instead of a
	 *  leave-then-enter pair whose second op referenced the node's stale id. */
	function resolveDropGroup(node: Node): string | undefined {
		let bestGroup: Node | undefined;
		let bestDepth = -1;
		let bestArea = Infinity;
		for (const group of nodes) {
			if (group.type !== 'group') continue; // expanded groups only (collapsed = 'groupCollapsed')
			if (group.id === node.id) continue;
			if (isDescendantOf(group.id, node.id)) continue;
			if (group.style?.includes('display: none')) continue;
			if (!nodeCentreInGroup(node, group)) continue;
			const { width: gw, height: gh } = getGroupDimensions(group);
			const depth = getGroupDepth(group);
			const area = gw * gh;
			if (depth > bestDepth || (depth === bestDepth && area < bestArea)) {
				bestDepth = depth;
				bestArea = area;
				bestGroup = group;
			}
		}
		return bestGroup?.id;
	}

	/** Apply the net scope change for a single dragged node: ONE move op to the
	 *  group its centre landed in (or top level). No live-node mutation: the
	 *  visual reparent comes from the projection re-derive, and a blocked move
	 *  (in-scope wires) is rejected by the gesture's preflight (whole-gesture
	 *  snap-back, one toast). Returns true when a move op was recorded, so the
	 *  caller's generic position write skips this node (the move's layout
	 *  closure already wrote its position under the NEW scoped key; a generic
	 *  write would key off the still-old parent and strand a stale entry). */
	function applyNodeScopeChange(node: Node): boolean {
		const targetGroupId = resolveDropGroup(node);
		const currentParentId = node.parentId ?? undefined;
		if (targetGroupId === currentParentId) return false; // net scope unchanged

		// New position: relative to the new parent when entering a group, absolute
		// when moving to top level. Computed from the node's current absolute spot.
		const nodeAbs = getAbsolutePosition(node);
		const targetGroup = targetGroupId ? nodes.find(n => n.id === targetGroupId) : undefined;
		const groupAbs = targetGroup ? getAbsolutePosition(targetGroup) : { x: 0, y: 0 };
		weftMoveScopeAny(node, targetGroupId, { x: nodeAbs.x - groupAbs.x, y: nodeAbs.y - groupAbs.y });
		return true;
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

	/** A dragged GROUP swallows stationary top-level nodes its box now covers.
	 *  `draggedIds` are the nodes in the SAME drag gesture; they must be excluded:
	 *  a co-dragged node is going to its OWN drop target (handled by its own
	 *  `applyNodeScopeChange`), so letting the group passively capture it would
	 *  steal it into the wrong scope.
	 *
	 *  A coverable node that still has wires in its scope is SKIPPED (with a
	 *  toast), not captured: its capture is a passive side effect of the
	 *  group's drag, and folding a doomed move into the batch would reject the
	 *  user's whole (otherwise valid) gesture. This is gesture shaping, before
	 *  preflight; the rejected-gesture path stays singular. */
	function checkGroupCapturesNodes(group: Node, draggedIds: Set<string> = new Set()) {
		// Collapsed groups don't capture nodes
		if (!((group.data.config as Record<string, unknown>)?.expanded ?? true)) return;

		const groupAbs = getAbsolutePosition(group);
		let blocked = false;
		for (const n of nodes) {
			if (n.parentId || n.type === 'group' || n.type === 'groupCollapsed' || n.id === group.id) continue;
			if (draggedIds.has(n.id)) continue; // co-dragged: goes to its own drop target, not swallowed
			if (!nodeCentreInGroup(n, group)) continue;
			if (nodeHasConnectionsInScope(n.id, n.parentId)) {
				blocked = true;
				continue;
			}
			const nodeAbs = getAbsolutePosition(n);
			weftMoveScopeAny(n, group.id, { x: nodeAbs.x - groupAbs.x, y: nodeAbs.y - groupAbs.y });
		}
		if (blocked) {
			toast.warning('Node not captured', {
				description: 'A covered node keeps its scope: disconnect it from its current scope first.',
				duration: 3000,
			});
		}
	}

	/** Whether a node's context menu has any INFRA lifecycle action (stop/terminate).
	 *  The one source of truth for "infra actions present", used both to decide
	 *  whether to open the menu in simplified view and to render the infra section. */
	function nodeInfraActions(nodeId: string | null): { stop: boolean; terminate: boolean; has: boolean } {
		const infra = nodeId ? infraNodes?.find(n => n.nodeId === nodeId) : undefined;
		const stop = !!infra && (infra.status === 'running' || infra.status === 'flaky');
		const terminate = !!infra && infra.status !== 'terminating';
		return { stop, terminate, has: stop || terminate };
	}

	function onContextMenu(event: MouseEvent) {
		event.preventDefault();

		const flowPos = screenToFlowPosition({ x: event.clientX, y: event.clientY });
		const clickedNodeId = findNodeAtPosition(event.clientX, event.clientY);

		// Simplified view hides the structural node-menu items (Duplicate/Delete),
		// leaving only infra lifecycle actions. A node with no infra actions would
		// open an empty popover, so don't open the menu at all in that case. (The
		// empty-area menu still has Undo/Redo, so it's never empty.)
		if (simplified && clickedNodeId && !nodeInfraActions(clickedNodeId).has) {
			contextMenu = null;
			return;
		}

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

	/** A fresh local id of `nodeType`'s shape, unique against existing scoped
	 *  ids AND `taken` (ids minted earlier in the same batch). The new id is
	 *  scoped under `parentId` so the duplicate lands as a SIBLING of its
	 *  original. Mutates `taken`. */
	function freshScopedNodeId(nodeType: string, parentId: string | undefined, taken: Set<string>): { localId: string; scopedId: string } {
		const snake = nodeType.replace(/([a-z0-9])([A-Z])/g, '$1_$2').toLowerCase();
		let i = 1;
		let scoped = scopedLayoutKey(`${snake}_${i}`, parentId);
		while (taken.has(scoped)) { i++; scoped = scopedLayoutKey(`${snake}_${i}`, parentId); }
		taken.add(scoped);
		return { localId: `${snake}_${i}`, scopedId: scoped };
	}

	/** A fresh container label unique against existing scoped ids AND `taken`,
	 *  scoped under `parentId` (so the duplicate is a sibling). Mutates `taken`. */
	function freshScopedLabel(base: string, parentId: string | undefined, taken: Set<string>): { label: string; scopedId: string } {
		let candidate = base;
		let scoped = scopedLayoutKey(candidate, parentId);
		let i = 2;
		while (taken.has(scoped)) { candidate = `${base}_${i}`; scoped = scopedLayoutKey(candidate, parentId); i++; }
		taken.add(scoped);
		return { label: candidate, scopedId: scoped };
	}

	function duplicateNode(nodeId: string) {
		duplicateNodes([nodeId]);
	}

	/** Duplicate one or more nodes as SIBLINGS (each copy lands in the same
	 *  scope as its original), in ONE atomic batch (one undo unit). Ids are
	 *  collision-safe against existing nodes AND against every other copy in
	 *  the same batch. A container duplicates as an empty shell of the same
	 *  kind (its children are NOT deep-copied); a node copies its source config
	 *  and label. The projection paints every copy as soon as the ops land. */
	function duplicateNodes(nodeIds: string[]) {
		const originals = nodeIds
			.map(id => nodes.find(n => n.id === id))
			.filter((n): n is Node => n !== undefined);
		if (originals.length === 0) return;
		contextMenu = null;

		// Seed the collision set from the PROJECTION truth (the same source the
		// delete path uses), so the batch's new ids can't collide with the graph
		// OR with each other.
		const taken = new Set(fold.project.nodes.map(n => n.id));
		const ops: EditOp[] = [];
		const layoutWrites: Array<(layout: string) => string> = [];
		const newIds: string[] = [];

		// Copy a decl's SOURCE config (skip view/layout keys) onto its copy.
		// Loop config routes to the loop-specific op (the Rust dispatch rejects
		// a generic SetConfig on a Loop decl); everything else is SetConfig.
		const copyConfig = (config: Record<string, unknown> | undefined, scopedId: string, isLoop: boolean) => {
			if (!config) return;
			for (const [key, value] of Object.entries(config)) {
				if (NON_SOURCE_KEYS.has(key)) continue;
				// Copy every set field, incl a deliberately-empty string (the
				// live-edit path emits "", so duplicate must too or it drops an
				// intentionally-blank field).
				if (value === undefined || value === null) continue;
				ops.push(isLoop
					? { op: 'setLoopConfig', loopId: scopedId, key, value: formatConfigValue(value) }
					: { op: 'setConfig', node: scopedId, key, value: formatConfigValue(value) });
			}
		};

		for (const orig of originals) {
			const nodeType = orig.data.nodeType as string;
			const isContainer = orig.type === 'group' || orig.type === 'groupCollapsed';
			const isLoop = isContainer && containerKindOf(nodeType) === 'Loop';
			const parentId = (orig.data.config as Record<string, string> | undefined)?.parentId;
			const newPos = { x: orig.position.x + 50, y: orig.position.y + 50 };
			const config = orig.data.config as Record<string, unknown> | undefined;

			if (isContainer) {
				const base = (orig.data.label as string) || (isLoop ? 'MyLoop' : 'MyGroup');
				const { label, scopedId } = freshScopedLabel(base, parentId, taken);
				newIds.push(scopedId);
				const cfg = config as Record<string, number> | undefined;
				ops.push(isLoop
					? { op: 'addLoop', label, parentGroup: parentId ?? null }
					: { op: 'addGroup', label, parentGroup: parentId ?? null });
				// Copy the container's boundary SIGNATURE: it is part of the decl,
				// and a Loop's `over`/`carry` config references its ports, so the
				// shell must declare them or the next build hard-errors
				// (loop-over/carry-unknown-port). Carry GHOST inputs are stripped
				// (they re-derive from the copied carry list on apply). Then copy
				// the source config AFTER the ports exist. (Children are NOT
				// deep-copied: the shell duplicates.)
				const sigInputs = toPortSigs((orig.data.inputs as PortDefinition[]).filter(p => !p.synthesizedFromCarry));
				const sigOutputs = toPortSigs(orig.data.outputs as PortLike[]);
				if (sigInputs.length > 0 || sigOutputs.length > 0) {
					ops.push(isLoop
						? { op: 'updateLoopPorts', loopId: scopedId, inputs: sigInputs, outputs: sigOutputs }
						: { op: 'updateGroupPorts', group: scopedId, inputs: sigInputs, outputs: sigOutputs });
				}
				if (isLoop) copyConfig(config, scopedId, true);
				layoutWrites.push((layout) => updateLayoutEntry(layout, scopedId, newPos.x, newPos.y, cfg?.width, cfg?.height));
			} else {
				const { localId, scopedId } = freshScopedNodeId(nodeType, parentId, taken);
				newIds.push(scopedId);
				ops.push({ op: 'addNode', id: localId, nodeType, parentGroup: parentId ?? null });
				layoutWrites.push((layout) => updateLayoutEntry(layout, scopedId, newPos.x, newPos.y));
				copyConfig(config, scopedId, false);
				if (orig.data.label) {
					ops.push({ op: 'setLabel', node: scopedId, label: orig.data.label as string });
				}
			}
		}

		// Select the new copies once the projection rebuilds (they don't exist
		// in the live `nodes` array yet). Clear the current selection now.
		selectedNodeId = newIds.length === 1 ? newIds[0] : null;
		for (const id of newIds) selectOnNextRebuild.add(id);
		nodes = nodes.map(n => (n.selected ? { ...n, selected: false } : n));

		recordEdit(ops, (layout) => layoutWrites.reduce((l, w) => w(l), layout));
	}

	// Explicit save (Ctrl+S / palette): the source is already the host's via the
	// edit-server, so this only flushes pending GUI edits and persists layout.
	function saveProject() {
		flushAllPendingSaves();
		// Persist the COMMITTED layout. flushAllPendingSaves() above commits pending
		// GUI edits, so layoutBase now holds the authoritative layout; using the
		// derived layoutCode (base + any still-optimistic ops) could persist a layout
		// the engine hasn't committed.
		saveLayout(engine.layoutBase);
		flashSaveStatus();
	}

	/// Persist ONLY the layout (positions/sizes), not the source. Graph edits
	/// send the source via onApplyEdits; their layout side (drop position of a
	/// new node, a drag, a resize) persists through here so it survives a fresh
	/// reload. Without this a GUI-placed node would lose its position.
	///
	/// Takes the layout string to persist. It must be the engine's COMMITTED
	/// `layoutBase`, NOT the derived `layoutCode` (base + in-flight optimistic
	/// ops): persisting the optimistic view would write a layout the engine has
	/// not yet committed, so a rejected/rebased layout op would leak to disk.
	function saveLayout(layoutToPersist: string) {
		onSave({ layoutCode: layoutToPersist });
	}

	type PortLike = { name: string; required?: boolean; portType?: string };
	function toPortSigs(ports: PortLike[]): import('../../../../shared/protocol').EditPortSig[] {
		return (ports ?? []).map(p => ({ name: p.name, required: p.required !== false, portType: p.portType }));
	}

	/// Flush every pending debounced edit. Called before the host kicks off
	/// Run / Activate / InfraStart (so the build sees the user's latest edits)
	/// and on teardown. Commits mid-typing field editors (their flush records
	/// the in-progress value as a typing op), then sends every still-pending
	/// typing op immediately. The sends queue on the doc's host-side write
	/// chain ahead of the verb, so the build reads post-edit source.
	export function flushAllPendingSaves(): void {
		fieldEditorRegistry.flushAll();
		engine.flushTypingOps();
	}

	// Flush buffered GUI config ops when the component is destroyed (panel
	// close, background) so an in-flight edit isn't lost. NOTE: navigation
	// flushes in the nav handlers (before the host swaps the watched doc).
	$effect(() => {
		return () => {
			flushAllPendingSaves();
			if (saveStatusTimer) clearTimeout(saveStatusTimer);
		};
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
		onclick={() => { if (!justOpenedContextMenu) { contextMenu = null; pendingConnection = null; tagEditor = null; } }}
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
			<!-- View-mode toggle (top-right). An explicit labelled on/off switch so
			     it always reads as "Simplified view: off/on", never an opaque icon.
			     Per-project, persisted. -->
			<div class="absolute top-3 right-3 z-30 pointer-events-auto">
				<button
					type="button"
					role="switch"
					aria-checked={simplified}
					onclick={toggleSimplified}
					class="flex items-center gap-2 pl-2.5 pr-2 py-1.5 rounded-md border bg-white text-zinc-700 border-zinc-200 shadow-sm text-xs font-medium hover:bg-zinc-50 transition"
					title={simplified
						? 'Simplified view is ON: nodes are squares with one in/out dot, read-only. Click to turn off (back to the full builder view).'
						: 'Simplified view is OFF: full builder view with every port and config. Click to turn on a clean square overview.'}
				>
					<Boxes class="w-3.5 h-3.5 {simplified ? 'text-violet-600' : 'text-zinc-400'}" />
					<span>Simplified view</span>
					<!-- on/off track -->
					<span class="relative inline-flex h-4 w-7 items-center rounded-full transition-colors {simplified ? 'bg-violet-600' : 'bg-zinc-300'}">
						<span class="inline-block h-3 w-3 transform rounded-full bg-white shadow transition-transform {simplified ? 'translate-x-3.5' : 'translate-x-0.5'}"></span>
					</span>
				</button>
			</div>
			<!-- svelte-ignore a11y_no_static_element_interactions -->
			<div class="svelte-flow-wrapper" style="width: 100%; height: 100%;" onwheelcapture={handleWheel}>
			<!-- The `...Key={UNBOUND_KEY}` props below DISABLE xyflow's built-in
			     key bindings (delete is ours via the KEYMAP in handleKeyDown;
			     zoom/pan need no modifier). See UNBOUND_KEY for why we don't
			     pass the documented `null`. -->
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
				onbeforereconnect={onBeforeReconnect}
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
				deleteKey={UNBOUND_KEY}
				selectionKey="Shift"
				multiSelectionKey="Shift"
				zoomActivationKey={UNBOUND_KEY}
				panActivationKey={UNBOUND_KEY}
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

		<!-- Graph-logic lock banner (explicit lock only; the sliding auto-lock
		     surfaces through gesture-rejection toasts instead, since it lives
		     sub-second). The release button clears the lock locally. -->
		{#if engine.lockGraphLogic}
			<div class="absolute top-3 left-1/2 -translate-x-1/2 z-10">
				<div class="flex items-center gap-3 px-4 py-2 bg-indigo-600/90 text-white rounded-lg shadow-lg text-xs font-medium backdrop-blur-sm">
					<svg xmlns="http://www.w3.org/2000/svg" width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><rect x="3" y="11" width="18" height="11" rx="2" ry="2"/><path d="M7 11V7a5 5 0 0 1 10 0v4"/></svg>
					<span>Graph locked{engine.lockReason ? ` while ${engine.lockReason}` : ''}. Layout still works; logic edits are paused.</span>
					<button
						class="px-2 py-0.5 rounded bg-white/20 hover:bg-white/30 transition-colors"
						onclick={() => setGraphLogicLock(false)}
					>
						Deactivate lock
					</button>
				</div>
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
			{onCancelBuild}
			{onCancelInfra}
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
				{@const infraActions = nodeInfraActions(targetNodeId)}
				{@const canNodeStop = infraActions.stop}
				{@const canNodeTerminate = infraActions.terminate}
				{@const hasInfraActions = infraActions.has}
				<!-- In simplified view the only node-menu items are infra lifecycle actions
				     (structural Duplicate/Delete are hidden), so render the menu only when
				     there's something to show, never an empty popover. -->
				{#if nodeToEdit && nodeConfig && (!simplified || hasInfraActions)}
					<div class="px-1">
						<!-- Duplicate/Delete are STRUCTURAL edits: hidden in simplified view
						     (read-only for structure). Infra Stop/Terminate below are runtime
						     lifecycle actions (not source edits), so they stay available. This
						     is the same gate STRUCTURAL_ACTIONS applies to the palette/keyboard;
						     the context menu is the other entry point and must gate too. -->
						{#if !simplified}
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
							<button
								class="w-full flex items-center gap-2 px-3 py-2 rounded-lg hover:bg-muted text-sm text-left transition-colors"
								onclick={() => { const id = contextMenu!.nodeId!; const x = contextMenu!.x; const y = contextMenu!.y; contextMenu = null; tagEditor = { nodeId: id, x, y }; }}
							>
								<span class="text-muted-foreground text-xs">#</span>
								<span>Tags…</span>
							</button>
						{/if}
						{#if hasInfraActions}
							{#if !simplified}<div class="my-1 mx-2 border-t"></div>{/if}
							<div class="px-3 py-1 text-xs text-muted-foreground uppercase tracking-wide">
								Infra ({infraInfo!.status})
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
					<!-- Add Node is a STRUCTURAL edit: hidden in simplified view. Undo/Redo
					     stay (they only reverse layout/structure the user already did). -->
					{#if !simplified}
						<button
							class="w-full flex items-center gap-2 px-3 py-2 rounded-lg hover:bg-muted text-sm text-left transition-colors"
							onclick={() => { contextMenuFlowPos = contextMenu ? { x: contextMenu.flowX, y: contextMenu.flowY } : null; contextMenu = null; commandPaletteOpen = true; }}
						>
							<span class="text-muted-foreground text-xs">Ctrl+P</span>
							<span>Add Node...</span>
						</button>
						<div class="my-1 mx-2 border-t"></div>
					{/if}
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

	<!-- Node Tags popover: launched from the node context menu. Edits the node's
	     `_tags` config key through the normal edit path. Click-away closes it. -->
	{#if tagEditor}
		<!-- Feed the popover from projection truth (fold), not the render `nodes`
		     array: `_tags` repaints into `nodes` only after the projection
		     round-trip, so reading `nodes` would show a stale tag set between a rapid
		     edit and its repaint. `fold` folds pending ops synchronously. -->
		{@const tagNode = fold.project.nodes.find(n => n.id === tagEditor!.nodeId)}
		{#if tagNode}
			<!-- svelte-ignore a11y_click_events_have_key_events, a11y_no_static_element_interactions -->
			<div
				class="fixed z-50 w-72 rounded-xl border bg-popover p-3 shadow-xl backdrop-blur-sm"
				style="left: {tagEditor.x}px; top: {tagEditor.y}px;"
				onclick={(e) => e.stopPropagation()}
			>
				<div class="mb-2 flex items-center justify-between">
					<span class="text-xs font-semibold text-foreground">Tags</span>
					<button class="text-muted-foreground hover:text-foreground text-xs" onclick={() => (tagEditor = null)} aria-label="Close">✕</button>
				</div>
				<NodeTagsEditor
					tags={nodeTags({ config: tagNode.config as Record<string, unknown> })}
					onChange={(next) => setNodeTags(tagEditor!.nodeId, next)}
					note="A listener authorized for a tag reaches every node carrying that tag, across all your projects."
				/>
			</div>
		{/if}
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
