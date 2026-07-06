// Layout constants for the Loop container's config strip. Shared by
// GroupNode.svelte (side-port top offset, min-height) and
// auto-organize.ts (ELK padding) so the rendered strip and the layout
// engine always agree on how far the strip pushes the body down.

/// Height of the open config strip (a few rows of controls) below the
/// loop header.
export const LOOP_CONFIG_STRIP_OPEN_PX = 220;

/// Height of the collapsed config bar below the loop header.
export const LOOP_CONFIG_STRIP_BAR_PX = 28;

/// Top of the side-port column in an expanded group, measured from the
/// container top (just below the header). Loop strips add on top of
/// this.
export const GROUP_PORTS_TOP_PX = 40;
