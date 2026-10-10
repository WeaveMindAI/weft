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

/** Whether an input draws as required: a required input always, and one
 *  required only once wired (`requiredWhenWired`) exactly while a wire
 *  feeds it, so its look follows connect and disconnect. */
export function portLooksRequired(port: PortDefinition, isWired: boolean): boolean {
	return port.required || (isWired && !!port.requiredWhenWired);
}

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
 *  - isWired: whether a wire feeds this input (see `portLooksRequired`)
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
		? inputMarkerState(portLooksRequired(port, isWired), oneOfRequiredPorts.has(port.name), literalFilledPorts.has(port.name))
		: 'full';

	if (state === 'full') return filledMarkerStyle(color, 'port', extraClass);

	let style: string;
	if (state === 'half') {
		style = `background: linear-gradient(to right, ${color} 50%, white 50%); ${ring(color)}`;
	} else if (state === 'empty-dotted') {
		style = dottedRing(color);
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

function markerClass(sizeClass: string, extraClass: string): string {
	return [sizeClass, '!rounded-full', extraClass].filter(Boolean).join(' ');
}
