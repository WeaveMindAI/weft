import { parseWeftType, type WeftType } from '$lib/types';

export const PORT_TYPE_COLORS: Record<string, string> = {
	String: '#6b7280',   // Neutral gray
	Number: '#5a9eb8',   // Monokai cyan
	Boolean: '#b05574',  // Monokai pink
	Null: '#a1a1aa',     // Zinc 400 (muted)
	Image: '#c4a35a',    // Warm gold
	Video: '#8b6fc0',    // Rich purple
	Audio: '#4a9e6f',    // Forest green
	Document: '#9e7c5a', // Warm brown
	List: '#5a8a8a',     // Monokai teal
	Dict: '#7c6f9f',     // Monokai purple
	TypeVar: '#6366f1',  // Indigo
	MustOverride: '#ef4444', // Red (needs attention)
};

const FALLBACK_COLOR = '#52525b'; // Dark gray

function colorForParsed(t: WeftType): string {
	switch (t.kind) {
		case 'primitive': return PORT_TYPE_COLORS[t.value] ?? FALLBACK_COLOR;
		case 'list': return PORT_TYPE_COLORS.List;
		case 'dict': return PORT_TYPE_COLORS.Dict;
		case 'json_dict': return PORT_TYPE_COLORS.Dict;
		case 'union': return colorForParsed(t.types[0]);
		case 'typevar': return PORT_TYPE_COLORS.TypeVar;
		case 'must_override': return PORT_TYPE_COLORS.MustOverride;
	}
}

export function getPortTypeColor(portType: string): string {
	if (!portType) return FALLBACK_COLOR;
	// Direct match for single primitives (fast path)
	if (PORT_TYPE_COLORS[portType]) return PORT_TYPE_COLORS[portType];
	if (portType === 'Media') return PORT_TYPE_COLORS.Image;
	// Parse and resolve
	const parsed = parseWeftType(portType);
	return parsed ? colorForParsed(parsed) : FALLBACK_COLOR;
}
