// Layout constants for a container's config strip. Shared by
// GroupNode.svelte (side-port top offset, min-height) and
// auto-organize.ts (ELK padding) so the rendered strip and the layout
// engine always agree on how far the strip pushes the body down.

/// Estimated height of the open config strip below the container
/// header: its toggle row plus one row per field. The strip's real
/// height varies with the control types (a textarea is taller than a
/// checkbox), so anything deciding WHERE to paint against the strip
/// (the side-port offset) measures the rendered strip instead; this
/// estimate serves the layout engine, which has no DOM to measure.
export function configStripOpenPx(fieldCount: number): number {
	return CONFIG_STRIP_BAR_PX + fieldCount * 64;
}

/// Height of the collapsed config bar below the container header.
export const CONFIG_STRIP_BAR_PX = 28;

/// Top of the side-port column in an expanded container, measured from
/// the container top (just below the header). The config strip adds on
/// top of this.
export const GROUP_PORTS_TOP_PX = 40;

import { SIMPLIFIED_SQUARE_PX } from './simplified-view';

/// The room an expanded container keeps between its border and its
/// children, per view. The layout engine feeds it to ELK as the scope's
/// padding; the builder view needs the header plus one label row per
/// side port, the simplified view only a slim header and bare dots.
export function containerPaddingPx(simplified: boolean): { top: number; side: number; bottom: number } {
	return simplified ? { top: 48, side: 28, bottom: 28 } : { top: 80, side: 60, bottom: 60 };
}

/// The smallest box an expanded container may draw at, per view. ONE
/// value for the layout engine's floor, the container's CSS floor, its
/// resize handle and its min-height enforcement, so a size the engine
/// hands out is always a size the renderer draws. They used to disagree
/// in simplified view: the engine wrapped one 96px square in its slim
/// padding (156x172) while the renderer floored every container at the
/// builder's 250x200, so a small loop drew past its parent's bottom and
/// right edges. Simplified: the padding around one square. Builder: the
/// historical floor, below what the builder padding plus any node
/// reaches anyway.
export function expandedContainerMinPx(simplified: boolean): { w: number; h: number } {
	if (!simplified) return { w: 250, h: 200 };
	const pad = containerPaddingPx(true);
	return { w: pad.side * 2 + SIMPLIFIED_SQUARE_PX, h: pad.top + SIMPLIFIED_SQUARE_PX + pad.bottom };
}
