// Shared handle ids for the simplified graph view. A node in simplified view
// renders exactly one target dot (left) and one source dot (right); every edge
// is rewritten to attach to these. Defined once so the node renderer
// (ProjectNode.svelte) and the edge builder (ProjectEditorInner.svelte) can
// never disagree on the id string.
//
// The ids are SUFFIXES appended to the node id (e.g. `agent__simp_out`), the
// same shape xyflow handle ids already use elsewhere.

// A simplified-view plain node or collapsed container draws as a fixed square
// (icon + type label, one in/out dot); expanded containers keep their box so
// children stay visible. This is the square's side length.
export const SIMPLIFIED_SQUARE_PX = 96;

// Inner padding of a simplified square (each side). The bare content column is
// pinned to the square minus both paddings so the node measures as a uniform
// square regardless of label length (the wrapper is `width: max-content`).
export const SIMPLIFIED_SQUARE_PAD_PX = 8;
export const SIMPLIFIED_CONTENT_W_PX = SIMPLIFIED_SQUARE_PX - 2 * SIMPLIFIED_SQUARE_PAD_PX;

// A node with a live display (image/feed/debug) grows past the square into a
// card. This is the shared upper bound, used BOTH as the xyflow node-level cap
// (simplifiedSizing) and the inner card cap (ProjectNode); the two must agree.
export const SIMPLIFIED_CARD_MAX_W_PX = 320;

// The single in/out interface dot. ONE size for every site (plain node, collapsed
// container, expanded container) so an expanded group's dots can't render a
// different size than a collapsed one's (the 19-vs-24 drift). `simplifiedDotStyle`
// returns the inline style; the dot is white-bordered and `color`-filled.
export const SIMPLIFIED_DOT_PX = 19;
export const SIMPLIFIED_DOT_BORDER_PX = 3;
export function simplifiedDotStyle(color: string): string {
  return `width: ${SIMPLIFIED_DOT_PX}px; height: ${SIMPLIFIED_DOT_PX}px; background: ${color}; border: ${SIMPLIFIED_DOT_BORDER_PX}px solid white;`;
}


export const SIMPLIFIED_IN_HANDLE = '__simp_in';
export const SIMPLIFIED_OUT_HANDLE = '__simp_out';

// Expanded containers (Group/Loop) also expose their interface to their
// CHILDREN. A child reads the container's inputs from an inner source dot on
// the left-inside edge, and writes the container's outputs to an inner target
// dot on the right-inside edge. Plain nodes never use these. The parser marks
// such self-reference edges with an `__inner` suffix on the port handle; the
// edge builder maps those to these inner dots instead of the outer ones.
export const SIMPLIFIED_INNER_SOURCE_HANDLE = '__simp_inner_src';
export const SIMPLIFIED_INNER_TARGET_HANDLE = '__simp_inner_tgt';

// Expanded Loop containers expose a second pair in simplified view for the
// loop's index (read) and done (write) body ports, so loop wiring is still
// visible. Plain nodes and Groups never use these.
export const SIMPLIFIED_LOOP_INDEX_HANDLE = '__simp_index';
export const SIMPLIFIED_LOOP_DONE_HANDLE = '__simp_done';
