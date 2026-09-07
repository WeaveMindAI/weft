// Draws a layout result as a plain SVG so a corpus run can be LOOKED at, not
// only asserted on. Boxes for nodes, outlined boxes for groups, one straight
// line per edge between the boxes' port sides. Not a rendering of the real
// graph view (no ports, no handles, no styling): a picture of where the layout
// put things, good enough to tell a readable layout from a mess. The corpus
// test writes one per fixture into `WEFT_LAYOUT_SVG_DIR` when that is set.

import type { NodeInstance, Edge } from '../types';

export interface LayoutForSvg {
	nodes: NodeInstance[];
	edges: Edge[];
	positions: Map<string, { x: number; y: number }>;
	groupSizes: Map<string, { width: number; height: number }>;
	/** Leaf box size, when the layout was fed one; else the renderer's default. */
	leafSize?: (id: string) => { width: number; height: number } | undefined;
}

const LEAF_W = 280;
const LEAF_H = 90;

export function layoutToSvg(l: LayoutForSvg): string {
	// Absolute position: a child's ELK position is relative to its group.
	const byId = new Map(l.nodes.map(n => [n.id, n]));
	const abs = new Map<string, { x: number; y: number }>();
	const absOf = (id: string): { x: number; y: number } => {
		const cached = abs.get(id);
		if (cached) return cached;
		const p = l.positions.get(id) ?? { x: 0, y: 0 };
		const parent = byId.get(id)?.parentId;
		const base = parent ? absOf(parent) : { x: 0, y: 0 };
		const r = { x: base.x + p.x, y: base.y + p.y };
		abs.set(id, r);
		return r;
	};
	const sizeOf = (id: string) => {
		const g = l.groupSizes.get(id);
		if (g) return { width: g.width, height: g.height };
		return l.leafSize?.(id) ?? { width: LEAF_W, height: LEAF_H };
	};
	let maxX = 0, maxY = 0;
	const boxes = l.nodes.map(n => {
		const p = absOf(n.id);
		const s = sizeOf(n.id);
		maxX = Math.max(maxX, p.x + s.width);
		maxY = Math.max(maxY, p.y + s.height);
		return { n, p, s, isGroup: l.groupSizes.has(n.id) };
	});
	const esc = (t: string) => t.replace(/&/g, '&amp;').replace(/</g, '&lt;');
	const parts: string[] = [];
	parts.push(`<svg xmlns="http://www.w3.org/2000/svg" width="${maxX + 40}" height="${maxY + 40}" viewBox="-20 -20 ${maxX + 40} ${maxY + 40}" font-family="sans-serif" font-size="11">`);
	parts.push(`<rect x="-20" y="-20" width="${maxX + 40}" height="${maxY + 40}" fill="#fafafa"/>`);
	// Groups first so leaves paint over them.
	for (const b of boxes.filter(b => b.isGroup)) {
		parts.push(`<rect data-id="${esc(b.n.id)}" x="${b.p.x}" y="${b.p.y}" width="${b.s.width}" height="${b.s.height}" fill="#eef2ff" stroke="#6366f1" stroke-dasharray="4 3"/>`);
		parts.push(`<text x="${b.p.x + 6}" y="${b.p.y + 14}" fill="#4338ca" font-weight="bold">${esc(b.n.id)}</text>`);
	}
	for (const e of l.edges) {
		const s = boxes.find(b => b.n.id === e.source);
		const t = boxes.find(b => b.n.id === e.target);
		if (!s || !t) continue;
		const x1 = s.p.x + s.s.width, y1 = s.p.y + s.s.height / 2;
		const x2 = t.p.x, y2 = t.p.y + t.s.height / 2;
		parts.push(`<line x1="${x1}" y1="${y1}" x2="${x2}" y2="${y2}" stroke="#94a3b8" stroke-opacity="0.7"/>`);
	}
	for (const b of boxes.filter(b => !b.isGroup)) {
		parts.push(`<rect data-id="${esc(b.n.id)}" x="${b.p.x}" y="${b.p.y}" width="${b.s.width}" height="${b.s.height}" rx="6" fill="#fff" stroke="#334155"/>`);
		parts.push(`<text x="${b.p.x + 8}" y="${b.p.y + 18}" fill="#0f172a" font-weight="bold">${esc(b.n.id)}</text>`);
		parts.push(`<text x="${b.p.x + 8}" y="${b.p.y + 34}" fill="#64748b">${esc(b.n.nodeType)}</text>`);
	}
	parts.push('</svg>');
	return parts.join('\n');
}
