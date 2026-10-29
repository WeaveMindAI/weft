import type { PortDefinition } from '../types';
import { inputIsRequired } from '../../../protocol';

/** Visual state of a port marker.
 *  - 'full': required + not satisfied from code (fully filled)
 *  - 'empty': optional (outline only)
 *  - 'half': in a @require_one_of group (half-filled)
 *  - 'empty-dotted': satisfied from code via a body-set literal (dotted
 *    outline, no fill). Overrides the declared required/oneOfRequired state
 *    visually because the value is already provided, but the port type is
 *    unchanged (a user can still wire an edge to override).
 *  - 'pass-through': a container's input that holds nothing and hands
 *    its value to the nodes inside (`InputDefinition.passesThrough`): a
 *    ring with a small dot in the middle. It is neither required nor
 *    optional itself, so it never draws as one of those.
 */
export type PortMarkerState = 'full' | 'empty' | 'half' | 'empty-dotted' | 'pass-through';

/** What an input's requiredness means right now, in words, for its
 *  hover text: null for a required input (its full marker says it), and
 *  for the other two states what a missing value does. An input with
 *  neither flag changes with its wiring, so its text names both halves. */
export function requirednessNote(port: PortDefinition, isWired: boolean): string | null {
	if (port.passesThrough) return `${port.name}: passes its value straight to the nodes inside; it has to be connected only when a node inside needs it`;
	if (port.required) return null;
	if (port.optional) return `${port.name}: optional, even when wired (if its wire delivers nothing, this node runs without it)`;
	return isWired
		? `${port.name}: required while wired (if its wire delivers nothing, this node skips); optional once unwired`
		: `${port.name}: optional while unwired; once wired, required (if its wire delivers nothing, this node skips)`;
}

/** Pick the marker state for an input port.
 *  A literal fill takes visual precedence over everything else: if the
 *  port has a non-null body-set literal and no edge, it renders as
 *  'empty-dotted' (a value written there is still worth showing). Next a
 *  port that passes through, which has no requiredness of its own.
 */
export function inputMarkerState(
	required: boolean,
	inOneOfRequired: boolean,
	isLiteralFilled: boolean = false,
	passesThrough: boolean = false,
): PortMarkerState {
	if (isLiteralFilled) return 'empty-dotted';
	if (passesThrough) return 'pass-through';
	if (required) return 'full';
	if (inOneOfRequired) return 'half';
	return 'empty';
}

/** Compute the { style, class } pair for a `<Handle>` that renders a port
 *  marker. Single source of truth for every port rendering in the project
 *  graph (ProjectNode inputs/outputs, GroupNode external inputs/outputs, in
 *  expanded and collapsed modes).
 *
 *  Parameters:
 *  - port: the port definition (carries required, portType)
 *  - oneOfRequiredPorts: set of input port names in a @require_one_of group
 *  - literalFilledPorts: set of input port names filled by a non-null
 *    body-set literal AND no incoming edge. These render as 'empty-dotted'
 *    to signal "satisfied from code" regardless of their declared required
 *    state.
 *  - color: the port's type color
 *  - side: 'input' (honors state) or 'output' (always full)
 *  - extraClass: optional extra Tailwind utilities
 *  - isWired: whether a wire feeds this input: an input with neither
 *    `required` nor `optional` draws required exactly while wired
 *    (`inputIsRequired`), so its look follows connect and disconnect
 */
export function portMarkerStyle(
	port: PortDefinition,
	oneOfRequiredPorts: Set<string>,
	literalFilledPorts: Set<string>,
	color: string,
	side: 'input' | 'output',
	extraClass: string = '',
	isWired: boolean = false,
): { style: string; class: string } {
	// Outputs are always `full`, regardless of the port's `required` flag.
	const state: PortMarkerState = side === 'input'
		? inputMarkerState(inputIsRequired(port, isWired), oneOfRequiredPorts.has(port.name), literalFilledPorts.has(port.name), port.passesThrough === true)
		: 'full';

	if (state === 'full') return filledMarkerStyle(color, 'port', extraClass);

	let style: string;
	if (state === 'half') {
		style = `background: linear-gradient(to right, ${color} 50%, white 50%); ${ring(color)}`;
	} else if (state === 'empty-dotted') {
		style = dottedRing(color);
	} else if (state === 'pass-through') {
		style = passThroughRing(color);
	} else {
		style = `background-color: white; ${ring(color)}`;
	}

	return { style, class: markerClass(SIZE_CLASS.port, extraClass) };
}

/** A filled marker in the port's own colour, at one of two sizes.
 *  - 'port' (12px): what an output draws and what a required input draws.
 *  - 'inner' (10px): what a container's INNER boundary handle draws (the
 *    dot a child wires to, and a loop's implicit `index` / `done`). Being
 *    smaller is what says "this is the inside of the port", so the two
 *    sizes have to stay apart.
 *
 *  The ring is the colour too. A white ring reads as a smaller port,
 *  because the marker is border-box and the ring eats 2px in from each
 *  edge: that is what made an output draw 8px beside a 12px input. Which
 *  side a port is on is already said by where it sits. */
export function filledMarkerStyle(
	color: string,
	size: MarkerSize,
	extraClass: string = '',
): { style: string; class: string } {
	return {
		style: `background-color: ${color}; ${ring(color)}`,
		class: markerClass(SIZE_CLASS[size], extraClass),
	};
}

export type MarkerSize = 'port' | 'inner';

const SIZE_CLASS: Record<MarkerSize, string> = {
	port: '!w-3 !h-3',
	inner: '!w-2.5 !h-2.5',
};

/** The whole ring, WIDTH INCLUDED, as one inline `border` shorthand.
 *
 *  The width used to come from Tailwind's `!border-2` instead, and that
 *  is what broke the dotted marker: in Tailwind 4 a width utility also
 *  emits `border-style: var(--tw-border-style)`, which defaults to
 *  `solid`, and the `!` makes it important. An important declaration
 *  beats an inline style, so `border-style: dotted` never landed and a
 *  port filled from code drew a solid ring around a white centre,
 *  looking exactly like an ordinary optional port.
 *
 *  2px, because at 12px a 1px dotted ring is indistinguishable from a
 *  solid hollow one; the weight is the same on every marker so the one
 *  dotted port does not look like a bug. */
function ring(color: string): string {
	return `border: 2px solid ${color}`;
}

/** The ring of an input filled from code: eight even dashes. A CSS
 *  `dotted` border cannot do it on a 12px circle (the browser spaces the
 *  dots unevenly round the curve), so the ring is a transparent border
 *  painted by a conic gradient, with a white disc over the middle. Same
 *  2px width and box as every other marker. */
function dottedRing(color: string): string {
	return `border: 2px solid transparent; background: linear-gradient(white, white) padding-box, `
		+ `repeating-conic-gradient(${color} 0deg 22.5deg, transparent 22.5deg 45deg) border-box`;
}

/** The marker of a port that passes through: the ring every marker
 *  has, around a white middle holding a small dot in the port's colour.
 *  The gradient paints the 8px inside the ring, so a 2px dot leaves 2px
 *  of white between it and the ring, enough to read as a dot at 12px. */
function passThroughRing(color: string): string {
	return `background: radial-gradient(circle, ${color} 0 2px, white 2.5px); ${ring(color)}`;
}

function markerClass(sizeClass: string, extraClass: string): string {
	return [sizeClass, '!rounded-full', extraClass].filter(Boolean).join(' ');
}
