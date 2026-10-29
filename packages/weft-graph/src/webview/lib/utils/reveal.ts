// Following a "comes from" link: from the scoped id of the node or group a
// value comes from, to what the editor has to do to show it. Pure, so the
// decision is tested apart from the canvas that carries it out.

/** What it takes to show a node: the groups to open, outermost first
 *  (every group it sits in, open or not: the caller opens the closed
 *  ones), and the node to select and pan to. */
export interface RevealPlan {
	open: string[];
	select: string;
}

/** The plan for showing `id`, read off the drawn nodes. An id the editor
 *  does not draw on its own (a node inside an `@include`, which the editor
 *  shows as one box) is shown by the closest drawn node holding it: each
 *  `.segment` dropped off its end is one level out. Null when nothing
 *  drawn holds it. */
export function revealPlan(nodes: readonly { id: string; parentId?: string }[], id: string): RevealPlan | null {
	const byId = new Map(nodes.map((n) => [n.id, n]));
	let select = id;
	while (!byId.has(select)) {
		const dot = select.lastIndexOf('.');
		if (dot < 0) return null;
		select = select.slice(0, dot);
	}
	const open: string[] = [];
	for (let parent = byId.get(select)?.parentId; parent; parent = byId.get(parent)?.parentId) {
		open.unshift(parent);
	}
	return { open, select };
}
