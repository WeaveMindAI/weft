// Auto-layout (ELK). Pure graph layout: given V1 nodes/edges + measured sizes,
// it returns positions + group sizes. NOT part of the Weft language (it doesn't
// read or write `.weft` source), so it lives on the frontend and was lifted out
// of the deleted webview parser verbatim.

import ELK from 'elkjs/lib/elk.bundled.js';
import type { NodeInstance, Edge } from './types';
import { isContainerNodeType, isLoopNodeType, containerHasConfigStrip, inputExposure } from './types';
import { CONFIG_STRIP_BAR_PX, configStripOpenPx } from './constants/container-layout';
import { LOOP_CONFIG_FIELDS } from './utils/input-field';
import { SHOULD_FLOW_PORT } from '../../protocol';

export interface AutoOrganizeResult {
	positions: Map<string, { x: number; y: number }>;
	groupSizes: Map<string, { width: number; height: number }>;
}

export async function autoOrganize(
	projectNodes: NodeInstance[],
	projectEdges: Edge[],
	nodeSizes?: Map<string, { width: number; height: number }>,
	portPositions?: Map<string, Map<string, number>>,
	simplified = false,
): Promise<AutoOrganizeResult> {
	const positions = new Map<string, { x: number; y: number }>();
	const groupSizes = new Map<string, { width: number; height: number }>();
	if (projectNodes.length === 0) return { positions, groupSizes };

	const elk = new ELK();

	const NODE_BASE_HEIGHT = 90;
	const PORT_ROW_HEIGHT = 22;
	const NODE_WIDTH = 280;
	const GROUP_PADDING = 40;

	// Build parent->children map for all nodes (including nested groups)
	const childrenOf = new Map<string, NodeInstance[]>();
	for (const node of projectNodes) {
		if (node.parentId) {
			const arr = childrenOf.get(node.parentId) ?? [];
			arr.push(node);
			childrenOf.set(node.parentId, arr);
		}
	}

	// Only expanded groups are ELK containers; collapsed groups are leaf nodes
	const groupIds = new Set(
		projectNodes
			.filter(n => isContainerNodeType(n.nodeType) && (n.config as Record<string, unknown>)?.expanded !== false)
			.map(n => n.id)
	);
	const collapsedGroupIds = new Set(
		projectNodes
			.filter(n => isContainerNodeType(n.nodeType) && (n.config as Record<string, unknown>)?.expanded === false)
			.map(n => n.id)
	);

	// Pre-compute annotation sizes from content so ELK reserves space for them
	const ANNOTATION_CHAR_WIDTH = 7.5;
	const ANNOTATION_LINE_HEIGHT = 20;
	const ANNOTATION_PADDING = 24;
	const ANNOTATION_MIN_W = 200;
	const ANNOTATION_MAX_W = 420;
	const ANNOTATION_MIN_H = 80;
	const ANNOTATION_MAX_H = 320;
	const ANNOTATION_TARGET_W = 280;

	const annotationIds = new Set<string>();
	for (const node of projectNodes) {
		if (node.nodeType !== 'Annotation') continue;
		annotationIds.add(node.id);
		const content = (node.config?.content as string) || '';
		const existingW = nodeSizes?.get(node.id)?.width;
		const existingH = nodeSizes?.get(node.id)?.height;
		if (existingW && existingH) {
			// Already measured from DOM, use actual size
			groupSizes.set(node.id, { width: existingW, height: existingH });
		} else if (content) {
			const charsPerLine = Math.floor((ANNOTATION_TARGET_W - ANNOTATION_PADDING * 2) / ANNOTATION_CHAR_WIDTH);
			let totalLines = 0;
			for (const line of content.split('\n')) {
				totalLines += Math.max(1, Math.ceil((line.length || 1) / charsPerLine));
			}
			const w = Math.min(ANNOTATION_MAX_W, Math.max(ANNOTATION_MIN_W, ANNOTATION_TARGET_W));
			const h = Math.min(ANNOTATION_MAX_H, Math.max(ANNOTATION_MIN_H, totalLines * ANNOTATION_LINE_HEIGHT + ANNOTATION_PADDING * 2));
			groupSizes.set(node.id, { width: w, height: h });
		} else {
			groupSizes.set(node.id, { width: ANNOTATION_TARGET_W, height: ANNOTATION_MIN_H });
		}
	}

	const topLevelNodes = projectNodes.filter(n => !n.parentId);

	// Source-code rank of every node, from the `sourceLine` the parser
	// attaches to each NodeInstance. We can't rely on the order of the
	// `projectNodes` array itself because SvelteFlow's buildNodes sorts
	// groups before non-groups (xyflow parent-first requirement), which
	// would make every group look like it was written first in the source.
	const sourceOrder = new Map<string, number>();
	for (const n of projectNodes) {
		const line = (n as NodeInstance & { sourceLine?: number }).sourceLine;
		if (typeof line === 'number') sourceOrder.set(n.id, line);
	}
	// Fallback rank for any node missing sourceLine: use array index so we
	// still get a stable, deterministic order.
	for (let i = 0; i < projectNodes.length; i++) {
		if (!sourceOrder.has(projectNodes[i].id)) {
			sourceOrder.set(projectNodes[i].id, 1_000_000 + i);
		}
	}
	const sourceRank = (id: string) => sourceOrder.get(id) ?? Number.MAX_SAFE_INTEGER;

	// Build set of node IDs that are visible in the ELK tree
	// (not hidden inside collapsed groups)
	const elkVisibleNodeIds = new Set<string>();
	function collectVisible(nodes: NodeInstance[]) {
		for (const n of nodes) {
			elkVisibleNodeIds.add(n.id);
			// Only recurse into expanded groups
			if (groupIds.has(n.id)) {
				const children = childrenOf.get(n.id) ?? [];
				collectVisible(children);
			}
		}
	}
	collectVisible(topLevelNodes);

	// With SEPARATE_CHILDREN, edges must be placed at the correct scope level.
	// An edge between two nodes in the same scope goes on that scope's edge list.
	// An edge crossing a group boundary (using __inner handles) goes on the group's edge list.
	const nodeById = new Map(projectNodes.map(n => [n.id, n]));

	// Determine which scope a node belongs to (its parentId, or 'root' for top-level)
	function getScope(nodeId: string): string {
		const node = nodeById.get(nodeId);
		return node?.parentId || 'root';
	}

	// An edge whose target input is an `Access` port is plumbing: a
	// credential hookup, not a step of the story. Plumbing does not bind
	// two nodes into one component, so a side path that only shares an
	// access node with the main flow lays out as its own row instead of
	// being woven through the big one. (The wires still render; they are
	// only ignored for component discovery.)
	const plumbingEdgeIds = new Set<string>();
	for (const e of projectEdges) {
		const tgt = nodeById.get(e.target);
		const input = tgt?.inputs?.find(i => i.name === e.targetHandle);
		if (input && String(input.portType) === 'Access') plumbingEdgeIds.add(e.id);
	}

	// ---- Satellites ("moons") -----------------------------------------
	// A leaf node with no incoming wires whose outputs all feed ONE
	// same-scope consumer, and only through its hookup inputs (an `Access`
	// port or a wire-exposure input like an LLM's `provider` / `params`),
	// is an accessory of that consumer. Layered layout would slam it into
	// the leftmost layer as a "source" and drag a wire across the whole
	// canvas; instead it is kept out of ELK entirely and parked under its
	// consumer afterwards, the way a person lays these out. A node feeding
	// a plain DATA input (a Text heading a pipeline) is a story start and
	// stays in the layering, as does a trigger.
	const SAT_GAP = 30;
	const incomingCount = new Map<string, number>();
	const outEdges = new Map<string, Edge[]>();
	for (const e of projectEdges) {
		incomingCount.set(e.target, (incomingCount.get(e.target) ?? 0) + 1);
		if (!outEdges.has(e.source)) outEdges.set(e.source, []);
		outEdges.get(e.source)!.push(e);
	}
	/** Is this edge a hookup: does it land on an Access port or a
	 *  wire-exposure input of its target? */
	function isHookupEdge(e: Edge): boolean {
		const input = nodeById.get(e.target)?.inputs?.find(i => i.name === e.targetHandle);
		if (!input) return false;
		return String(input.portType) === 'Access' || inputExposure(input) === 'wire';
	}
	/** Cached path split of one scope's children (plumbing-blind, the
	 *  same split the banding uses). */
	const scopeCompsCache = new Map<string, string[][]>();
	function scopeComps(scopeId: string): string[][] {
		let comps = scopeCompsCache.get(scopeId);
		if (!comps) {
			const ids = scopeId === 'root'
				? new Set(topLevelNodes.map(nn => nn.id))
				: new Set((childrenOf.get(scopeId) ?? []).map(nn => nn.id));
			comps = findConnectedComponents(ids, scopeId);
			scopeCompsCache.set(scopeId, comps);
		}
		return comps;
	}
	/** The consumer a moon parks with: the earliest consumer inside the
	 *  path holding most of the moon's consumers. */
	function pickMoonConsumer(n: NodeInstance, consumers: string[]): string {
		if (consumers.length === 1) return consumers[0];
		const comps = scopeComps(n.parentId ?? 'root');
		const compOf = new Map<string, number>();
		comps.forEach((comp, i) => comp.forEach(id => compOf.set(id, i)));
		const votes = new Map<number, number>();
		for (const c of consumers) {
			const i = compOf.get(c);
			if (i !== undefined) votes.set(i, (votes.get(i) ?? 0) + 1);
		}
		let best = -1, bestVotes = -1;
		for (const [i, v] of votes) {
			if (v > bestVotes) { best = i; bestVotes = v; }
		}
		// consumers is sourceRank-sorted, so the first hit in the winning
		// path is the earliest one there; a vote tie keeps the earliest
		// consumer overall (Map iteration saw its path first).
		return consumers.find(c => compOf.get(c) === best) ?? consumers[0];
	}

	/** sat id -> consumer id */
	const satelliteConsumer = new Map<string, string>();
	/** consumer id -> sat ids, source order */
	const satellitesByConsumer = new Map<string, string[]>();
	for (const n of projectNodes) {
		if (isContainerNodeType(n.nodeType) || n.nodeType === 'Annotation') continue;
		if (n.features?.isTrigger) continue;
		if ((incomingCount.get(n.id) ?? 0) > 0) continue;
		const edges = outEdges.get(n.id);
		if (!edges || edges.length === 0 || !edges.every(isHookupEdge)) continue;
		// Several consumers (one access node, many users) still moon: it
		// parks under one of them, and only the wires to the others stay
		// long. Keeping such a node in the layering would thread its long
		// wires through every layer as invisible spacers and push the real
		// rows apart. It parks with the PATH that holds most of its
		// consumers (the earliest consumer there), so a database shared by
		// a main flow and a two-node maintenance branch sits with the
		// branch, next to the bulk of its wires.
		const consumers = [...new Set(edges.map(e => e.target))]
			.filter(t => (nodeById.get(t)?.parentId ?? undefined) === (n.parentId ?? undefined))
			.sort((a, b) => sourceRank(a) - sourceRank(b));
		if (consumers.length === 0) continue;
		const consumer = pickMoonConsumer(n, consumers);
		const cNode = nodeById.get(consumer);
		if (!cNode) continue;
		satelliteConsumer.set(n.id, consumer);
		if (!satellitesByConsumer.has(consumer)) satellitesByConsumer.set(consumer, []);
		satellitesByConsumer.get(consumer)!.push(n.id);
	}
	for (const list of satellitesByConsumer.values()) {
		list.sort((a, b) => sourceRank(a) - sourceRank(b));
	}
	// Build edges grouped by scope
	const edgesByScope = new Map<string, any[]>();
	function addEdgeToScope(scope: string, edge: any) {
		if (!edgesByScope.has(scope)) edgesByScope.set(scope, []);
		edgesByScope.get(scope)!.push(edge);
	}

	// Every port id some edge anchors on. ELK gets ONLY these: a node can
	// declare far more inputs than it renders (config-only fields draw no
	// handle), and its fallback port Ys assume one row per declared port,
	// which lands phantom FIXED_POS anchors way below the node's real
	// bottom. ELK then spaces neighbours and sizes groups around anchors
	// that do not exist on screen (the inflated-group bug). A port no edge
	// touches has no bearing on layout, so it is simply not declared.
	const usedPortIds = new Set<string>();

	let edgeIdx = 0;
	for (const e of projectEdges) {
		if (!elkVisibleNodeIds.has(e.source) || !elkVisibleNodeIds.has(e.target)) continue;
		const rawSrc = e.sourceHandle || 'output';
		const rawTgt = e.targetHandle || 'input';
		const srcIsInner = rawSrc.endsWith('__inner');
		const tgtIsInner = rawTgt.endsWith('__inner');
		const srcHandle = srcIsInner ? rawSrc.slice(0, -7) : rawSrc;
		const tgtHandle = tgtIsInner ? rawTgt.slice(0, -7) : rawTgt;
		const srcDir = srcIsInner ? 'in' : 'out';
		const tgtDir = tgtIsInner ? 'out' : 'in';

		const elkEdge = {
			id: `e${edgeIdx++}`,
			sources: [`${e.source}__${srcDir}__${srcHandle}`],
			targets: [`${e.target}__${tgtDir}__${tgtHandle}`],
		};
		usedPortIds.add(elkEdge.sources[0]);
		usedPortIds.add(elkEdge.targets[0]);

		// Determine scope: if both nodes are in the same scope, edge goes there.
		// If one is a group and the handle is __inner, it's an internal edge of that group.
		if (srcIsInner) {
			// Source is a group, edge goes inside that group's scope
			addEdgeToScope(e.source, elkEdge);
		} else if (tgtIsInner) {
			// Target is a group, edge goes inside that group's scope
			addEdgeToScope(e.target, elkEdge);
		} else {
			// Normal edge: goes to the common parent scope
			const srcScope = getScope(e.source);
			const tgtScope = getScope(e.target);
			// If same scope, add there. Otherwise add to root (cross-scope edges).
			addEdgeToScope(srcScope === tgtScope ? srcScope : 'root', elkEdge);
		}
	}

	// Builder groups need room for the header plus per-port label rows on
	// both sides; simplified groups draw a slim header and bare dots, so the
	// same padding would wrap a 96px square in a sea of empty box (the
	// giant-group look). Sized to each view's real chrome.
	const GROUP_TOP_PADDING = simplified ? 48 : 80;
	const GROUP_SIDE_PADDING = simplified ? 28 : 60;
	const GROUP_BOTTOM_PADDING = simplified ? 28 : 60;
	const COLLAPSED_GROUP_WIDTH = 200;
	const COLLAPSED_GROUP_HEIGHT = 80;

	/** Extra vertical space the config strip (open or collapsed bar)
	 *  occupies below a container's header. 0 when it draws none. Both ELK
	 *  padding and side-port Y offsets add this so children never sit on
	 *  the strip. */
	function configStripPx(nodeId: string): number {
		// In simplified view the config strip is not drawn (GroupNode gates it
		// on !data.simplified), so it occupies zero vertical space. Reserving the
		// open-strip height here would push the first child row down by ~220px with
		// nothing to fill it: the phantom top gap above a simplified loop's body.
		if (simplified) return 0;
		const node = projectNodes.find(n => n.id === nodeId);
		const literals = (node as { portLiterals?: Record<string, unknown> } | undefined)?.portLiterals;
		if (!node || !containerHasConfigStrip(node.nodeType, literals)) return 0;
		const configCollapsed = (node.config as Record<string, unknown> | undefined)?.configCollapsed === true;
		if (configCollapsed) return CONFIG_STRIP_BAR_PX;
		// The open strip grows with its field list: one row per written
		// port literal, plus the loop knob rows. Same derivation as
		// GroupNode's stripFields, so the reserved space follows the
		// rendered strip.
		const fieldCount =
			Object.keys(literals ?? {}).length +
			(isLoopNodeType(node.nodeType) ? LOOP_CONFIG_FIELDS.length : 0);
		return configStripOpenPx(fieldCount);
	}

	function paddingForGroup(groupId: string): string {
		return `[top=${GROUP_TOP_PADDING + configStripPx(groupId)},left=${GROUP_SIDE_PADDING},bottom=${GROUP_BOTTOM_PADDING},right=${GROUP_SIDE_PADDING}]`;
	}

	// Port Y position constants (must match CSS in GroupNode.svelte and ProjectNode.svelte)
	// Expanded group: ports start at top:40px + 4px padding, each ~30px tall with 6px gap
	const GROUP_PORT_START_Y = 44;  // top(40) + padding(4)
	const GROUP_PORT_HEIGHT = 30;   // label row + dots
	const GROUP_PORT_GAP = 6;
	// Regular/collapsed node: header ~50px, then ports ~25px each with ~1px gap
	const NODE_PORT_START_Y = 58;   // accent(2) + header(32) + content-padding(16) + label-area(8)
	const NODE_PORT_HEIGHT = 25;    // PORT_ROW_HEIGHT from ProjectNode.svelte
	const NODE_PORT_GAP = 4;       // space-y-1

	/** Compute port Y position for regular/collapsed nodes */
	function nodePortY(portIndex: number): number {
		return NODE_PORT_START_Y + portIndex * (NODE_PORT_HEIGHT + NODE_PORT_GAP) + NODE_PORT_HEIGHT / 2;
	}

	/** Compute port Y position for expanded groups (side ports). Loop
	 *  containers push side ports down by the config-strip height. */
	function groupPortY(nodeId: string, portIndex: number): number {
		return GROUP_PORT_START_Y + configStripPx(nodeId) + portIndex * (GROUP_PORT_HEIGHT + GROUP_PORT_GAP) + GROUP_PORT_HEIGHT / 2;
	}

	/** Get the actual measured port Y, falling back to computed position if DOM isn't available (e.g. during streaming). */
	function getPortY(nodeId: string, handleId: string, isGroup: boolean, portIndex: number): number {
		const measured = portPositions?.get(nodeId)?.get(handleId);
		if (measured !== undefined) return measured;
		// Fallback: compute from constants (used during streaming when DOM isn't rendered)
		return isGroup ? groupPortY(nodeId, portIndex) : nodePortY(portIndex);
	}

	/** The flow dock's Y on a node's header (the yellow triangle). */
	const FLOW_PORT_Y = 16;

	/** The ELK ports for one box: the implicit `_should_flow` dock (not in
	 *  any declared `inputs`, but a wire into it is a real dependency edge;
	 *  without it those wires get dropped and the gated chains come out
	 *  sideways) plus every declared port an edge actually anchors on,
	 *  inputs west, outputs east. Ports no edge touches are NOT declared,
	 *  and every Y is clamped inside the box: a node can declare far more
	 *  inputs than it renders, and the row-index fallback would otherwise
	 *  put FIXED_POS anchors below the node's real bottom, which ELK then
	 *  reserves space for (the inflated-group bug). */
	function elkPorts(
		nodeId: string,
		inputs: string[],
		outputs: string[],
		width: number,
		height: number,
		isGroup: boolean,
	) {
		const clamp = (y: number) => Math.max(4, Math.min(y, height - 4));
		const ports: any[] = [];
		if (usedPortIds.has(`${nodeId}__in__${SHOULD_FLOW_PORT}`)) {
			ports.push({
				id: `${nodeId}__in__${SHOULD_FLOW_PORT}`,
				x: 0,
				y: clamp(portPositions?.get(nodeId)?.get(SHOULD_FLOW_PORT) ?? FLOW_PORT_Y),
				width: 1, height: 1,
				// Above every declared port (the dock sits on the header).
				properties: { 'port.side': 'WEST', 'port.index': '-1' },
			});
		}
		inputs.forEach((name, i) => {
			const id = `${nodeId}__in__${name}`;
			if (!usedPortIds.has(id)) return;
			ports.push({
				id,
				x: 0,
				y: clamp(getPortY(nodeId, name, isGroup, i)),
				width: 1, height: 1,
				properties: { 'port.side': 'WEST', 'port.index': String(i) },
			});
		});
		outputs.forEach((name, i) => {
			const id = `${nodeId}__out__${name}`;
			if (!usedPortIds.has(id)) return;
			ports.push({
				id,
				x: width - 1,
				y: clamp(getPortY(nodeId, name, isGroup, i)),
				width: 1, height: 1,
				properties: { 'port.side': 'EAST', 'port.index': String(i) },
			});
		});
		return ports;
	}

	// --- Shared ELK layout options ---
	// We rely on model order (the order of children in the input array) to
	// pin siblings left-to-right the way the user wrote them in weft source.
	// `considerModelOrder` + `crossingCounterNodeInfluence > 0` makes ELK treat
	// source order as a strong tiebreaker during crossing minimization, and
	// `nodePromotion.strategy` + tighter spacing keep layers compact.
	const elkLayoutOptions: Record<string, string> = {
		'elk.algorithm': 'layered',
		'elk.direction': 'RIGHT',
		'elk.layered.spacing.nodeNodeBetweenLayers': '50',
		'elk.spacing.nodeNode': '25',
		'elk.layered.spacing.edgeNodeBetweenLayers': '15',
		'elk.layered.nodePlacement.strategy': 'NETWORK_SIMPLEX',
		'elk.layered.crossingMinimization.strategy': 'LAYER_SWEEP',
		'elk.layered.crossingMinimization.greedySwitch.type': 'TWO_SIDED',
		'elk.layered.crossingMinimization.thoroughness': '100',
		'elk.layered.considerModelOrder.strategy': 'NODES_AND_EDGES',
		'elk.layered.considerModelOrder.crossingCounterNodeInfluence': '0.5',
		'elk.layered.considerModelOrder.crossingCounterPortInfluence': '0.5',
		'elk.layered.crossingMinimization.forceNodeModelOrder': 'true',
		'elk.layered.nodePromotion.strategy': 'DUMMYNODE_PERCENTAGE',
		'elk.separateConnectedComponents': 'true',
		'elk.layered.compaction.connectedComponents': 'true',
		'elk.spacing.componentComponent': '60',
		// Wide-and-flat target for the component packing: flows read
		// left-to-right, so a side path belongs below the main one, not
		// squeezed beside it.
		'elk.aspectRatio': '2.4',
		// One-dimensional post-compaction closes the dead width that big
		// boxes (expanded groups) tear open in their layer: nodes slide
		// toward their edge partners after placement. QUADRATIC
		// constraints on purpose: the default scanline constraint
		// calculation throws "Invalid hitboxes" on hierarchical graphs
		// (the moon clusters), quadratic handles them.
		'elk.layered.compaction.postCompaction.strategy': 'EDGE_LENGTH',
		'elk.layered.compaction.postCompaction.constraints': 'QUADRATIC',
	};
	const baseOptions = elkLayoutOptions;

	// --- Helper: find connected components among a set of node IDs ---
	function findConnectedComponents(nodeIds: Set<string>, scopeId: string): string[][] {
		const adj = new Map<string, Set<string>>();
		for (const id of nodeIds) adj.set(id, new Set());

		const resolveToScope = (id: string): string | null => {
			if (nodeIds.has(id)) return id;
			let current = id;
			let parent = nodeById.get(current)?.parentId;
			while (parent && !nodeIds.has(current) && nodeById.has(parent)) {
				current = parent;
				parent = nodeById.get(current)?.parentId;
			}
			return nodeIds.has(current) ? current : null;
		};

		const portPeers = new Map<string, Set<string>>();
		for (const e of projectEdges) {
			// Plumbing (a wire into an Access port) does not bind two nodes
			// into one component: a side path that only shares a credential
			// source with the main flow is still its own path.
			if (plumbingEdgeIds.has(e.id)) continue;
			const src = resolveToScope(e.source);
			const tgt = resolveToScope(e.target);
			if (src && tgt && src !== tgt && nodeIds.has(src) && nodeIds.has(tgt)) {
				adj.get(src)!.add(tgt);
				adj.get(tgt)!.add(src);
			} else if (e.source === scopeId && tgt && nodeIds.has(tgt)) {
				const portKey = e.sourceHandle || 'default';
				if (!portPeers.has(portKey)) portPeers.set(portKey, new Set());
				portPeers.get(portKey)!.add(tgt);
			} else if (e.target === scopeId && src && nodeIds.has(src)) {
				const portKey = e.targetHandle || 'default';
				if (!portPeers.has(portKey)) portPeers.set(portKey, new Set());
				portPeers.get(portKey)!.add(src);
			}
		}
		for (const peers of portPeers.values()) {
			const arr = [...peers];
			for (let i = 0; i < arr.length; i++) {
				for (let j = i + 1; j < arr.length; j++) {
					adj.get(arr[i])!.add(arr[j]);
					adj.get(arr[j])!.add(arr[i]);
				}
			}
		}

		// Walk nodes in weft source order so component discovery is deterministic
		// across runs. Each component inherits the rank of its earliest node, so
		// sorting components by "min rank" below gives left-to-right order that
		// matches the user's source.
		const sorted = [...nodeIds].sort((a, b) => sourceRank(a) - sourceRank(b));
		const visited = new Set<string>();
		const comps: string[][] = [];
		for (const id of sorted) {
			if (visited.has(id)) continue;
			const comp: string[] = [];
			const stack = [id];
			while (stack.length > 0) {
				const cur = stack.pop()!;
				if (visited.has(cur)) continue;
				visited.add(cur);
				comp.push(cur);
				for (const nb of (adj.get(cur) ?? [])) {
					if (!visited.has(nb)) stack.push(nb);
				}
			}
			comps.push(comp);
		}

		// A component whose every member only SOURCES plumbing (an access
		// node all of whose wires were ignored above) is not a path of its
		// own; each member joins the component of its earliest consumer,
		// so a plumbing-only cluster (a credential source and nothing
		// else) is packed with the path that actually uses it instead of
		// floating as its own island.
		const compOf = new Map<string, number>();
		comps.forEach((comp, i) => comp.forEach(id => compOf.set(id, i)));
		const merged: string[][] = comps.map(() => []);
		comps.forEach((comp, i) => {
			const onlyPlumbs = comp.every(id => (adj.get(id)?.size ?? 0) === 0
				&& projectEdges.some(e => plumbingEdgeIds.has(e.id) && resolveToScope(e.source) === id));
			if (!onlyPlumbs) {
				merged[i].push(...comp);
				return;
			}
			for (const id of comp) {
				// This runs WHILE the satellite map is still being built
				// (it is filled later, in the moon pass), so the vote is
				// always the earliest consumer by rank.
				const anchor = projectEdges
					.filter(e => plumbingEdgeIds.has(e.id) && resolveToScope(e.source) === id)
					.map(e => resolveToScope(e.target))
					.filter((t): t is string => !!t && nodeIds.has(t) && !comp.includes(t))
					.sort((a, b) => sourceRank(a) - sourceRank(b))[0];
				const home = anchor !== undefined ? compOf.get(anchor) : undefined;
				merged[home !== undefined && home !== i ? home : i].push(id);
			}
		});
		return merged.filter(c => c.length > 0);
	}

	// --- Build ELK node for a single scope (flat, no children for groups) ---
	/** THE sizer for a leaf's box: measured DOM size first, then config,
	 *  then the row estimate. `buildElkLeafBase` builds its ELK node
	 *  from this, so there is exactly one place a box size comes from. */
	function leafSize(node: NodeInstance): { width: number; height: number } {
		const override = nodeSizes?.get(node.id);
		if (collapsedGroupIds.has(node.id)) {
			return {
				width: override?.width ?? COLLAPSED_GROUP_WIDTH,
				height: override?.height ?? COLLAPSED_GROUP_HEIGHT,
			};
		}
		if (annotationIds.has(node.id)) {
			return groupSizes.get(node.id) ?? { width: ANNOTATION_TARGET_W, height: ANNOTATION_MIN_H };
		}
		if (groupIds.has(node.id)) {
			return groupSizes.get(node.id) ?? { width: 400, height: 300 };
		}
		const cfg = node.config as Record<string, unknown>;
		const portCount = Math.max((node.inputs || []).length, (node.outputs || []).length, 1);
		return {
			width: override?.width ?? (cfg?.width as number | undefined) ?? NODE_WIDTH,
			height: override?.height
				?? (cfg?.height as number | undefined)
				?? (NODE_BASE_HEIGHT + portCount * PORT_ROW_HEIGHT),
		};
	}

	/** The node id a port id anchors on (`x__in__p` / `x__out__p` -> `x`). */
	function portNode(portId: string): string {
		return portId.split('__in__')[0].split('__out__')[0];
	}

	/** One ELK child for a scope: the node itself, or, when the node has
	 *  moons, a real ELK subgraph holding the node and its moons with the
	 *  hookup wires inside. ELK lays the cluster out itself (moons land
	 *  in the layer left of their consumer, boxed tight) and the scope
	 *  run places the whole box; nothing is positioned by hand. */
	function buildElkUnit(node: NodeInstance, scopeEdges: any[]): any {
		const base = buildElkLeafBase(node);
		const sats = satellitesByConsumer.get(node.id);
		if (!sats || sats.length === 0) return base;
		// Move the moons' wires INTO the cluster: an edge whose source is
		// one of this consumer's moons and whose target is the consumer
		// belongs to the cluster's own layout, not the scope's.
		const innerEdges: any[] = [];
		for (let i = scopeEdges.length - 1; i >= 0; i--) {
			const e = scopeEdges[i];
			const src = portNode(e.sources[0]);
			if (sats.includes(src) && portNode(e.targets[0]) === node.id) {
				innerEdges.push(e);
				scopeEdges.splice(i, 1);
			}
		}
		return {
			id: `__moons_${node.id}`,
			layoutOptions: {
				'elk.algorithm': 'layered',
				'elk.direction': 'RIGHT',
				'elk.padding': '[top=0,left=0,bottom=0,right=0]',
				'elk.spacing.nodeNode': String(SAT_GAP),
				'elk.layered.spacing.nodeNodeBetweenLayers': '40',
			},
			children: [base, ...sats.map(sid => buildElkLeafBase(nodeById.get(sid)!))],
			edges: innerEdges,
		};
	}

	function buildElkLeafBase(node: NodeInstance): any {
		const { width, height } = leafSize(node);
		if (annotationIds.has(node.id)) {
			return ({
				id: node.id,
				width,
				height,
				layoutOptions: { 'elk.portConstraints': 'FREE' },
			});
		}
		const inputs = (node.inputs || []).map(p => p.name);
		const outputs = (node.outputs || []).map(p => p.name);
		if (groupIds.has(node.id) && !collapsedGroupIds.has(node.id)) {
			// Groups are leaf nodes here, their children are laid out in a
			// separate pass; the resolved size (from the bottom-up layout)
			// is a floor ELK may grow.
			return ({
				id: node.id,
				width,
				height,
				ports: elkPorts(node.id, inputs, outputs, width, height, true),
				layoutOptions: {
					'elk.portConstraints': 'FIXED_POS',
					'elk.nodeSize.constraints': 'MINIMUM_SIZE',
					'elk.nodeSize.minimum': `(${width},${height})`,
				},
			});
		}
		return ({
			id: node.id,
			width,
			height,
			ports: elkPorts(node.id, inputs, outputs, width, height, false),
			layoutOptions: { 'elk.portConstraints': 'FIXED_POS' },
		});
	}

	// --- Run ELK for a single scope and extract positions ---
	// For group scopes: wrap in a parent graph with SEPARATE_CHILDREN so ELK
	// handles the group's own ports natively. For root scope: run directly.
	async function layoutScope(scopeId: string, children: NodeInstance[], padding: string) {
		// Feed children in weft source order so ELK's model-order machinery can
		// use it as a strong tiebreaker, keeping siblings left-to-right.
		// Satellites stay out: their consumer's box already reserves their
		// room and the final pass parks them under it.
		const orderedChildren = [...children]
			.filter(c => !satelliteConsumer.has(c.id))
			.sort((a, b) => sourceRank(a.id) - sourceRank(b.id));

		// Collect all valid port IDs from children (and group ports if
		// applicable), moons inside their clusters included.
		const collectPorts = (child: any, into: Set<string>) => {
			for (const port of (child.ports || [])) into.add(port.id);
			for (const c of (child.children || [])) collectPorts(c, into);
		};

		// The scope's edges, filtered below to what this run can anchor.
		// Built BEFORE the units: buildElkUnit MOVES a moon's hookup wires
		// out of this list into its cluster.
		const allScopeEdges = edgesByScope.get(scopeId) || [];
		const scopeEdges = [...allScopeEdges];
		const elkChildren = orderedChildren.map(c => buildElkUnit(c, scopeEdges));

		const validPortIds = new Set<string>();
		for (const child of elkChildren) collectPorts(child, validPortIds);

		// Also include group's own ports (for edges from/to group interface)
		if (groupIds.has(scopeId)) {
			const scopeNode = nodeById.get(scopeId);
			if (scopeNode) {
				for (const p of (scopeNode.inputs || [])) validPortIds.add(`${scopeId}__in__${p.name}`);
				for (const p of (scopeNode.outputs || [])) validPortIds.add(`${scopeId}__out__${p.name}`);
				validPortIds.add(`${scopeId}__in__${SHOULD_FLOW_PORT}`);
			}
		}

		// Only edges this run can anchor on both ends survive. A moon's
		// leftover wires (to consumers other than the one it is boxed
		// with) stay in: they are real dependencies, and layering needs
		// them so a shared feeder's OTHER consumers land to its right.
		// Withholding them left ELK blind to the link between two paths
		// sharing one feeder (a database feeding both the main flow and a
		// side path), and the unconstrained path could come out to the
		// right of the feeder, drawing its wire backward.
		const anchoredEdges = scopeEdges.filter((e: any) => {
			const srcId = e.sources?.[0] as string;
			const tgtId = e.targets?.[0] as string;
			return validPortIds.has(srcId) && validPortIds.has(tgtId);
		});

		if (groupIds.has(scopeId)) {
			const scopeNode = nodeById.get(scopeId)!;
			const inputs = (scopeNode.inputs || []).map(p => p.name);
			const outputs = (scopeNode.outputs || []).map(p => p.name);
			// Floor only: ELK grows the group to fit its children, so this minimum
			// just sets how small an (almost) empty group may get. Keep it small so
			// the group hugs its content instead of leaving a big empty band at the
			// bottom/right when the content is short (common with simplified-view
			// squares). Don't use measured DOM size, it would block shrinking.
			const minW = 120;
			const minH = 100;
			// Port positions on the east side need a reference width.
			// Use a large value; ELK will place the east ports at the final computed width.
			const portRefW = 400;

			// Wrap the group as a child of a dummy root, using SEPARATE_CHILDREN
			const graph = {
				id: `__wrapper_${scopeId}`,
				layoutOptions: {
					'elk.algorithm': 'layered',
					// One layered problem across the hierarchy, so wires into a
					// moon cluster's consumer still shape the outer layering.
					'elk.hierarchyHandling': 'INCLUDE_CHILDREN',
				},
				children: [{
					id: scopeId,
					width: minW,
					height: minH,
					layoutOptions: {
						...baseOptions,
						'elk.padding': padding,
						'elk.portConstraints': 'FIXED_POS',
						'elk.nodeSize.constraints': 'MINIMUM_SIZE',
						'elk.nodeSize.minimum': `(${minW},${minH})`,
					},
					// `portRefW` as the width so the east ports sit at its edge;
					// ELK grows the box and keeps FIXED_POS ports where placed.
					// Height for the clamp: the group's own ports hug the header,
					// so the min height bounds them fine.
					ports: elkPorts(scopeId, inputs, outputs, portRefW, minH, true),
					children: elkChildren,
					edges: anchoredEdges,
				}],
				edges: [],
			};

			const result = await elk.layout(graph);
			const groupResult = result.children?.[0];
			if (groupResult) {
				// Store the ELK-computed group size
				if (groupResult.width && groupResult.height) {
					groupSizes.set(scopeId, { width: groupResult.width, height: groupResult.height });
				}
				harvestPositions(groupResult, 0, 0);
			}
			return result;
		}

		// Root scope, run directly
		const graph = {
			id: scopeId,
			layoutOptions: {
				...baseOptions,
				'elk.hierarchyHandling': 'INCLUDE_CHILDREN',
				'elk.padding': padding,
			},
			children: elkChildren,
			edges: anchoredEdges,
		};

		const result = await elk.layout(graph);
		harvestPositions(result, 0, 0);
		return result;
	}

	/** Walk an ELK result depth-first, turning nested (cluster-relative)
	 *  coordinates into scope coordinates. Cluster wrappers themselves are
	 *  bookkeeping, not nodes, so only real ids land in `positions`. */
	function harvestPositions(elkNode: any, ox: number, oy: number): void {
		for (const child of (elkNode.children || [])) {
			const x = ox + (child.x ?? 0);
			const y = oy + (child.y ?? 0);
			if (!String(child.id).startsWith('__moons_')) {
				positions.set(child.id, { x, y });
				if (groupIds.has(child.id) && child.width && child.height && !groupSizes.has(child.id)) {
					groupSizes.set(child.id, { width: child.width, height: child.height });
				}
			}
			harvestPositions(child, x, y);
		}
	}

	// --- Bottom-up scope resolution ---
	// 1. Compute depth of each group
	function getGroupDepth(groupId: string): number {
		let depth = 0;
		const children = childrenOf.get(groupId) ?? [];
		for (const child of children) {
			if (groupIds.has(child.id)) {
				depth = Math.max(depth, 1 + getGroupDepth(child.id));
			}
		}
		return depth;
	}

	const groupsByDepth = new Map<number, string[]>();
	let maxDepth = 0;
	for (const groupId of groupIds) {
		if (collapsedGroupIds.has(groupId)) continue;
		const depth = getGroupDepth(groupId);
		maxDepth = Math.max(maxDepth, depth);
		if (!groupsByDepth.has(depth)) groupsByDepth.set(depth, []);
		groupsByDepth.get(depth)!.push(groupId);
	}

	try {
		// 2. Layout from deepest groups up to shallowest
		for (let depth = 0; depth <= maxDepth; depth++) {
			const groups = groupsByDepth.get(depth) ?? [];
			for (const groupId of groups) {
				const children = (childrenOf.get(groupId) ?? []).filter(c => elkVisibleNodeIds.has(c.id));
				if (children.length === 0) continue;

				const padding = paddingForGroup(groupId);
				// One run: with the plumbing withheld from the edge set,
				// ELK's own `separateConnectedComponents` splits and packs
				// the group's independent pieces itself.
				await layoutScope(groupId, children, padding);
			}
		}

		// 3. Layout the root scope, groups now at their final sizes. One
		// run here too: the paths (plumbing-blind components) are
		// separated and packed by ELK, not by hand.
		const rootPadding = `[top=${GROUP_PADDING},left=${GROUP_PADDING},bottom=${GROUP_PADDING},right=${GROUP_PADDING}]`;
		await layoutScope('root', topLevelNodes, rootPadding);
	} catch (e) {
		// ELK failing is a real bug (a malformed graph we fed it, a bad size, an
		// unsupported option), not an expected outcome. Scattering nodes into a fixed
		// grid here USED to hide that: it produced a plausible-looking layout that
		// masked the input problem and destroyed the user's existing arrangement.
		// Surface it loudly and return an empty positions map instead: the caller
		// applies positions only where present, so every node keeps its current
		// position and the broken layout is visible rather than silently papered over.
		console.error('[autoOrganize] ELK layout failed; leaving node positions untouched:', e);
		positions.clear();
	}

	return { positions, groupSizes };
}
