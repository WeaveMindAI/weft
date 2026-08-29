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
