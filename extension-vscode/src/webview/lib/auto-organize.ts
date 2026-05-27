// Auto-layout (ELK). Pure graph layout: given V1 nodes/edges + measured sizes,
// it returns positions + group sizes. NOT part of the Weft language (it doesn't
// read or write `.weft` source), so it lives on the frontend and was lifted out
// of the deleted webview parser verbatim.

import ELK from 'elkjs/lib/elk.bundled.js';
import type { NodeInstance, Edge } from './types';

export interface AutoOrganizeResult {
	positions: Map<string, { x: number; y: number }>;
	groupSizes: Map<string, { width: number; height: number }>;
}

export async function autoOrganize(
	projectNodes: NodeInstance[],
	projectEdges: Edge[],
	nodeSizes?: Map<string, { width: number; height: number }>,
	portPositions?: Map<string, Map<string, number>>,
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
			.filter(n => n.nodeType === 'Group' && (n.config as Record<string, unknown>)?.expanded !== false)
			.map(n => n.id)
	);
	const collapsedGroupIds = new Set(
		projectNodes
			.filter(n => n.nodeType === 'Group' && (n.config as Record<string, unknown>)?.expanded === false)
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

	// Build edges grouped by scope
	const edgesByScope = new Map<string, any[]>();
	function addEdgeToScope(scope: string, edge: any) {
		if (!edgesByScope.has(scope)) edgesByScope.set(scope, []);
		edgesByScope.get(scope)!.push(edge);
	}

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

	// For each node, record the lowest port index it connects to on any target,
	// AND the lowest source port index it connects from.
	// Used to pre-sort sibling nodes to reduce crossings.
	const nodeTargetPortOrder = new Map<string, number>();
	const nodeSourcePortOrder = new Map<string, number>();
	for (const edge of projectEdges) {
		const targetNode = projectNodes.find(n => n.id === edge.target);
		if (targetNode) {
			const tgtHandle = (edge.targetHandle || '').replace(/__inner$/, '');
			const portIndex = (targetNode.inputs || []).findIndex(p => p.name === tgtHandle);
			if (portIndex !== -1) {
				const existing = nodeTargetPortOrder.get(edge.source);
				if (existing === undefined || portIndex < existing) {
					nodeTargetPortOrder.set(edge.source, portIndex);
				}
			}
		}
		// Also track which source port feeds into each target node
		const sourceNode = projectNodes.find(n => n.id === edge.source);
		if (sourceNode) {
			const srcHandle = (edge.sourceHandle || '').replace(/__inner$/, '');
			const portIndex = (sourceNode.outputs || []).findIndex(p => p.name === srcHandle);
			if (portIndex !== -1) {
				const existing = nodeSourcePortOrder.get(edge.target);
				if (existing === undefined || portIndex < existing) {
					nodeSourcePortOrder.set(edge.target, portIndex);
				}
			}
		}
	}

	function sortByTargetPortOrder(nodes: NodeInstance[]): NodeInstance[] {
		return [...nodes].sort((a, b) => {
			// Primary: sort by which target port they feed into (lower port index = higher in layout)
			const tgtA = nodeTargetPortOrder.get(a.id) ?? Infinity;
			const tgtB = nodeTargetPortOrder.get(b.id) ?? Infinity;
			if (tgtA !== tgtB) return tgtA - tgtB;
			// Secondary: sort by which source port feeds them (lower port index = higher in layout)
			const srcA = nodeSourcePortOrder.get(a.id) ?? Infinity;
			const srcB = nodeSourcePortOrder.get(b.id) ?? Infinity;
			return srcA - srcB;
		});
	}

	const GROUP_TOP_PADDING = 80;   // header + port labels
	const GROUP_SIDE_PADDING = 60;  // port labels on sides
	const GROUP_BOTTOM_PADDING = 40;
	const COLLAPSED_GROUP_WIDTH = 200;
	const COLLAPSED_GROUP_HEIGHT = 80;

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

	/** Compute port Y position for expanded groups (side ports) */
	function groupPortY(portIndex: number): number {
		return GROUP_PORT_START_Y + portIndex * (GROUP_PORT_HEIGHT + GROUP_PORT_GAP) + GROUP_PORT_HEIGHT / 2;
	}

	/** Get the actual measured port Y, falling back to computed position if DOM isn't available (e.g. during streaming). */
	function getPortY(nodeId: string, handleId: string, isGroup: boolean, portIndex: number): number {
		const measured = portPositions?.get(nodeId)?.get(handleId);
		if (measured !== undefined) return measured;
		// Fallback: compute from constants (used during streaming when DOM isn't rendered)
		return isGroup ? groupPortY(portIndex) : nodePortY(portIndex);
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
		return comps;
	}

	// --- Helper: arrange disconnected components side by side ---
	function arrangeDisconnectedComponents(
		comps: string[][],
		padding: { top: number; left: number; bottom: number; right: number },
	): { width: number; height: number } | null {
		if (comps.length <= 1) return null;

		const GAP = 80;
		const compBBoxes: { minX: number; maxX: number; minY: number; maxY: number; ids: string[] }[] = [];
		for (const comp of comps) {
			let minX = Infinity, maxX = -Infinity, minY = Infinity, maxY = -Infinity;
			for (const id of comp) {
				const pos = positions.get(id);
				if (!pos) continue;
				const w = groupSizes.get(id)?.width ?? nodeSizes?.get(id)?.width ?? NODE_WIDTH;
				const h = groupSizes.get(id)?.height ?? nodeSizes?.get(id)?.height ?? NODE_BASE_HEIGHT;
				minX = Math.min(minX, pos.x);
				maxX = Math.max(maxX, pos.x + w);
				minY = Math.min(minY, pos.y);
				maxY = Math.max(maxY, pos.y + h);
			}
			compBBoxes.push({ minX, maxX, minY, maxY, ids: comp });
		}
		// Preserve the caller's ordering (connectivity-based for groups,
		// X-position-based for root level)

		let cursor = padding.left;
		for (const comp of compBBoxes) {
			if (comp.minX === Infinity) continue;
			const shiftX = cursor - comp.minX;
			const shiftY = padding.top - comp.minY;
			for (const id of comp.ids) {
				const pos = positions.get(id);
				if (pos) positions.set(id, { x: pos.x + shiftX, y: pos.y + shiftY });
			}
			cursor += (comp.maxX - comp.minX) + GAP;
		}

		// Compute final bounding box
		let totalMaxX = 0, totalMaxY = 0;
		for (const comp of compBBoxes) {
			for (const id of comp.ids) {
				const pos = positions.get(id);
				if (!pos) continue;
				const w = groupSizes.get(id)?.width ?? nodeSizes?.get(id)?.width ?? NODE_WIDTH;
				const h = groupSizes.get(id)?.height ?? nodeSizes?.get(id)?.height ?? NODE_BASE_HEIGHT;
				totalMaxX = Math.max(totalMaxX, pos.x + w);
				totalMaxY = Math.max(totalMaxY, pos.y + h);
			}
		}
		return {
			width: totalMaxX + padding.right,
			height: totalMaxY + padding.bottom,
		};
	}

	// --- Build ELK node for a single scope (flat, no children for groups) ---
	function buildElkLeafNode(node: NodeInstance): any {
		if (collapsedGroupIds.has(node.id)) {
			const override = nodeSizes?.get(node.id);
			const inputs = (node.inputs || []).map(p => p.name);
			const outputs = (node.outputs || []).map(p => p.name);
			const w = override?.width ?? COLLAPSED_GROUP_WIDTH;
			return ({
				id: node.id,
				width: w,
				height: override?.height ?? COLLAPSED_GROUP_HEIGHT,
				ports: [
					...inputs.map((name, i) => ({
						id: `${node.id}__in__${name}`,
						x: 0,
						y: getPortY(node.id, name, false, i),
						width: 1, height: 1,
						properties: { 'port.side': 'WEST', 'port.index': String(i) },
					})),
					...outputs.map((name, i) => ({
						id: `${node.id}__out__${name}`,
						x: w - 1,
						y: getPortY(node.id, name, false, i),
						width: 1, height: 1,
						properties: { 'port.side': 'EAST', 'port.index': String(i) },
					})),
				],
				layoutOptions: { 'elk.portConstraints': 'FIXED_POS' },
			});
		}

		if (groupIds.has(node.id)) {
			// Groups are leaf nodes here, their children are laid out in a separate pass.
			// Use the resolved size from groupSizes (set by bottom-up layout).
			const inputs = (node.inputs || []).map(p => p.name);
			const outputs = (node.outputs || []).map(p => p.name);
			const size = groupSizes.get(node.id) ?? { width: 400, height: 300 };
			return ({
				id: node.id,
				width: size.width,
				height: size.height,
				ports: [
					...inputs.map((name, i) => ({
						id: `${node.id}__in__${name}`,
						x: 0,
						y: getPortY(node.id, name, true, i),
						width: 1, height: 1,
						properties: { 'port.side': 'WEST', 'port.index': String(i) },
					})),
					...outputs.map((name, i) => ({
						id: `${node.id}__out__${name}`,
						x: size.width - 1,
						y: getPortY(node.id, name, true, i),
						width: 1, height: 1,
						properties: { 'port.side': 'EAST', 'port.index': String(i) },
					})),
				],
				layoutOptions: {
					'elk.portConstraints': 'FIXED_POS',
					'elk.nodeSize.constraints': 'MINIMUM_SIZE',
					'elk.nodeSize.minimum': `(${size.width},${size.height})`,
				},
			});
		}

		if (annotationIds.has(node.id)) {
			const size = groupSizes.get(node.id) ?? { width: ANNOTATION_TARGET_W, height: ANNOTATION_MIN_H };
			return ({
				id: node.id,
				width: size.width,
				height: size.height,
				layoutOptions: { 'elk.portConstraints': 'FREE' },
			});
		}

		const inputs = (node.inputs || []).map(p => p.name);
		const outputs = (node.outputs || []).map(p => p.name);
		const portCount = Math.max(inputs.length, outputs.length, 1);
		const override = nodeSizes?.get(node.id);
		const cfg = node.config as Record<string, unknown>;
		const configW = cfg?.width as number | undefined;
		const configH = cfg?.height as number | undefined;
		const width = override?.width ?? configW ?? NODE_WIDTH;
		const height = override?.height ?? configH ?? (NODE_BASE_HEIGHT + portCount * PORT_ROW_HEIGHT);

		return ({
			id: node.id,
			width,
			height,
			ports: [
				...inputs.map((name, i) => ({
					id: `${node.id}__in__${name}`,
					x: 0,
					y: getPortY(node.id, name, false, i),
					width: 1, height: 1,
					properties: { 'port.side': 'WEST', 'port.index': String(i) },
				})),
				...outputs.map((name, i) => ({
					id: `${node.id}__out__${name}`,
					x: width - 1,
					y: getPortY(node.id, name, false, i),
					width: 1, height: 1,
					properties: { 'port.side': 'EAST', 'port.index': String(i) },
				})),
				{
					id: `${node.id}__out___raw`,
					x: width - 1,
					y: getPortY(node.id, '_raw', false, outputs.length),
					width: 1, height: 1,
					properties: { 'port.side': 'EAST', 'port.index': String(outputs.length) },
				},
			],
			layoutOptions: { 'elk.portConstraints': 'FIXED_POS' },
		});
	}

	// --- Run ELK for a single scope and extract positions ---
	// For group scopes: wrap in a parent graph with SEPARATE_CHILDREN so ELK
	// handles the group's own ports natively. For root scope: run directly.
	async function layoutScope(scopeId: string, children: NodeInstance[], padding: string) {
		// Feed children in weft source order so ELK's model-order machinery can
		// use it as a strong tiebreaker, keeping siblings left-to-right.
		const orderedChildren = [...children].sort((a, b) => sourceRank(a.id) - sourceRank(b.id));
		const elkChildren = orderedChildren.map(c => buildElkLeafNode(c));

		// Collect all valid port IDs from children (and group ports if applicable)
		const validPortIds = new Set<string>();
		for (const child of elkChildren) {
			for (const port of (child.ports || [])) {
				validPortIds.add(port.id);
			}
		}

		// Also include group's own ports (for edges from/to group interface)
		if (groupIds.has(scopeId)) {
			const scopeNode = nodeById.get(scopeId);
			if (scopeNode) {
				for (const p of (scopeNode.inputs || [])) validPortIds.add(`${scopeId}__in__${p.name}`);
				for (const p of (scopeNode.outputs || [])) validPortIds.add(`${scopeId}__out__${p.name}`);
			}
		}

		// Filter edges to only those whose source AND target ports exist in this layout
		const allScopeEdges = edgesByScope.get(scopeId) || [];
		const scopeEdges = allScopeEdges.filter((e: any) => {
			const srcId = e.sources?.[0] as string;
			const tgtId = e.targets?.[0] as string;
			return validPortIds.has(srcId) && validPortIds.has(tgtId);
		});

		if (groupIds.has(scopeId)) {
			const scopeNode = nodeById.get(scopeId)!;
			const inputs = (scopeNode.inputs || []).map(p => p.name);
			const outputs = (scopeNode.outputs || []).map(p => p.name);
			// Use a small default size, ELK will grow the group to fit children.
			// Don't use measured DOM size as minimum, it would prevent ELK from shrinking.
			const minW = 400;
			const minH = 300;
			// Port positions on the east side need a reference width.
			// Use a large value; ELK will place the east ports at the final computed width.
			const portRefW = 400;

			// Wrap the group as a child of a dummy root, using SEPARATE_CHILDREN
			const graph = {
				id: `__wrapper_${scopeId}`,
				layoutOptions: {
					'elk.algorithm': 'layered',
					'elk.hierarchyHandling': 'SEPARATE_CHILDREN',
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
					ports: [
						...inputs.map((name, i) => ({
							id: `${scopeId}__in__${name}`,
							x: 0,
							y: getPortY(scopeId, name, true, i),
							width: 1, height: 1,
							properties: { 'port.side': 'WEST', 'port.index': String(i) },
						})),
						...outputs.map((name, i) => ({
							id: `${scopeId}__out__${name}`,
							x: portRefW - 1,
							y: getPortY(scopeId, name, true, i),
							width: 1, height: 1,
							properties: { 'port.side': 'EAST', 'port.index': String(i) },
						})),
					],
					children: elkChildren,
					edges: scopeEdges,
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
				for (const child of (groupResult.children || [])) {
					positions.set(child.id, { x: child.x ?? 0, y: child.y ?? 0 });
					if (groupIds.has(child.id) && child.width && child.height && !groupSizes.has(child.id)) {
						groupSizes.set(child.id, { width: child.width, height: child.height });
					}
				}
			}
			return result;
		}

		// Root scope, run directly
		const graph = {
			id: scopeId,
			layoutOptions: {
				...baseOptions,
				'elk.padding': padding,
			},
			children: elkChildren,
			edges: scopeEdges,
		};

		const result = await elk.layout(graph);
		for (const child of (result.children || [])) {
			positions.set(child.id, { x: child.x ?? 0, y: child.y ?? 0 });
			if (groupIds.has(child.id) && child.width && child.height && !groupSizes.has(child.id)) {
				groupSizes.set(child.id, { width: child.width, height: child.height });
			}
		}
		return result;
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

				const padding = `[top=${GROUP_TOP_PADDING},left=${GROUP_SIDE_PADDING},bottom=${GROUP_BOTTOM_PADDING},right=${GROUP_SIDE_PADDING}]`;

				// Find disconnected components first
				const childIds = new Set(children.map(c => c.id));
				const comps = findConnectedComponents(childIds, groupId);

				// Lay out each component independently so ELK can't spread
				// disconnected nodes across connected component's layers.
				for (const comp of comps) {
					const compChildren = children.filter(c => comp.includes(c.id));
					await layoutScope(groupId, compChildren, padding);
				}

				// Sort components for arrangement:
				// - Connected to group input ports → leftmost (score 0)
				// - Connected to both → leftmost (score 0)
				// - Not connected to any group port → middle (score 1)
				// - Connected to group output ports only → rightmost (score 2)
				if (comps.length > 1) {
					const sortedComps = comps.map(comp => {
						const compSet = new Set(comp);
						let connectsToInput = false;
						let connectsToOutput = false;
						for (const e of projectEdges) {
							if (e.source === groupId && compSet.has(e.target)) connectsToInput = true;
							if (e.target === groupId && compSet.has(e.source)) connectsToOutput = true;
						}
						const score = connectsToInput ? 0 : connectsToOutput ? 2 : 1;
						const minRank = Math.min(...comp.map(sourceRank));
						return { comp, score, minRank };
					});
					// Score groups components by port role (input-connected first,
					// output-connected last). Within the same score, weft source
					// order wins so siblings stay left-to-right as the user wrote.
					sortedComps.sort((a, b) => a.score - b.score || a.minRank - b.minRank);

					const newSize = arrangeDisconnectedComponents(
						sortedComps.map(c => c.comp),
						{
							top: GROUP_TOP_PADDING,
							left: GROUP_SIDE_PADDING,
							bottom: GROUP_BOTTOM_PADDING,
							right: GROUP_SIDE_PADDING,
						},
					);
					if (newSize) {
						groupSizes.set(groupId, newSize);
					}
				}
			}
		}

		// 3. Layout root scope (top-level nodes, groups now have final sizes)
		const rootPadding = `[top=${GROUP_PADDING},left=${GROUP_PADDING},bottom=${GROUP_PADDING},right=${GROUP_PADDING}]`;
		await layoutScope('root', topLevelNodes, rootPadding);

		// 4. Arrange disconnected components at root level
		const topIds = new Set(topLevelNodes.map(n => n.id));
		const comps = findConnectedComponents(topIds, 'root');
		arrangeDisconnectedComponents(comps, { top: 0, left: 0, bottom: 0, right: 0 });
	} catch (e) {
		console.warn('[autoOrganize] ELK layout failed:', e);
		for (let i = 0; i < projectNodes.length; i++) {
			positions.set(projectNodes[i].id, { x: 100 + (i % 4) * 350, y: 100 + Math.floor(i / 4) * 250 });
		}
	}

	return { positions, groupSizes };
}
