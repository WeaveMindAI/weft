import type { PortDefinition } from '../types';

/** Visual state of a port marker.
 *  - 'full': required + not satisfied from code (fully filled)
 *  - 'empty': optional (outline only)
 *  - 'half': in a @require_one_of group (half-filled)
 *  - 'empty-dotted': satisfied from code via a body-set literal (dotted
 *    outline, no fill). Overrides the declared required/oneOfRequired state
 *    visually because the value is already provided, but the port type is
 *    unchanged (a user can still wire an edge to override).
 */
export type PortMarkerState = 'full' | 'empty' | 'half' | 'empty-dotted';

/** Pick the marker state for an input port.
 *  A literal fill takes visual precedence over required/oneOfRequired:
 *  if the port has a non-null body-set literal and no edge, it renders
 *  as 'empty-dotted' regardless of declared required state.
 */
export function inputMarkerState(
	required: boolean,
	inOneOfRequired: boolean,
	isLiteralFilled: boolean = false,
): PortMarkerState {
	if (isLiteralFilled) return 'empty-dotted';
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
 */
export function portMarkerStyle(
	port: PortDefinition,
	oneOfRequiredPorts: Set<string>,
	literalFilledPorts: Set<string>,
	color: string,
	side: 'input' | 'output',
	extraClass: string = '',
): { style: string; class: string } {
	// Outputs are always `full`, regardless of the port's `required` flag.
	const state: PortMarkerState = side === 'input'
		? inputMarkerState(port.required, oneOfRequiredPorts.has(port.name), literalFilledPorts.has(port.name))
		: 'full';

	if (state === 'full') return fullMarkerStyle(color, extraClass);

	let style: string;
	if (state === 'half') {
		style = `background: linear-gradient(to right, ${color} 50%, white 50%); ${ring('solid', color)}`;
	} else if (state === 'empty-dotted') {
		style = `background-color: white; ${ring('dotted', color)}`;
	} else {
		style = `background-color: white; ${ring('solid', color)}`;
	}

	return { style, class: markerClass(PORT_SIZE_CLASS, extraClass) };
}

/** A filled marker in the port's own colour: what an output draws and
 *  what a required input draws, at the full port size.
 *
 *  The ring is the colour too. A white ring reads as a smaller port,
 *  because the marker is border-box and the ring eats 2px in from each
 *  edge: that is what made an output draw 8px beside a 12px input. Which
 *  side a port is on is already said by where it sits. */
export function fullMarkerStyle(color: string, extraClass: string = ''): { style: string; class: string } {
	return {
		style: `background-color: ${color}; ${ring('solid', color)}`,
		class: markerClass(PORT_SIZE_CLASS, extraClass),
	};
}

/** The same filled marker, one step smaller: what a container's INNER
 *  boundary handle draws (the dot a child wires to, and a loop's implicit
 *  `index` / `done`). Being smaller is what says "this is the inside of
 *  the port", so this size and the full one have to stay apart. */
export function innerMarkerStyle(color: string, extraClass: string = ''): { style: string; class: string } {
	return {
		style: `background-color: ${color}; ${ring('solid', color)}`,
		class: markerClass(INNER_SIZE_CLASS, extraClass),
	};
}

/// 12px for a port, 10px for the inside of one.
const PORT_SIZE_CLASS = '!w-3 !h-3';
const INNER_SIZE_CLASS = '!w-2.5 !h-2.5';

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
function ring(style: 'solid' | 'dotted', color: string): string {
	return `border: 2px ${style} ${color}`;
}

function markerClass(sizeClass: string, extraClass: string): string {
	return [sizeClass, '!rounded-full', extraClass].filter(Boolean).join(' ');
}
