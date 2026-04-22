import type { LaneMode, PortDefinition } from '$lib/types';

/** Visual state of a port marker.
 *  - 'full': required + not satisfied from code (fully filled)
 *  - 'empty': optional (outline only)
 *  - 'half': in a @require_one_of group (half-filled)
 *  - 'empty-dotted': satisfied from code via a config-fill literal (dotted
 *    outline, no fill). Overrides the declared required/oneOfRequired state
 *    visually because the value is already provided, but the port type is
 *    unchanged (a user can still wire an edge to override).
 */
export type PortMarkerState = 'full' | 'empty' | 'half' | 'empty-dotted';

/** Shape of a port marker, determined from laneMode. */
export type PortMarkerShape = 'circle' | 'gather' | 'expand';

export function portMarkerShape(laneMode: LaneMode | undefined): PortMarkerShape {
	if (laneMode === 'Gather') return 'gather';
	if (laneMode === 'Expand') return 'expand';
	return 'circle';
}

/** Pick the marker state for an input port.
 *  Config-fill takes visual precedence over required/oneOfRequired:
 *  if the port has a non-null config value and no edge, it renders as
 *  'empty-dotted' regardless of declared required state.
 */
export function inputMarkerState(
	required: boolean,
	inOneOfRequired: boolean,
	isConfigFilled: boolean = false,
): PortMarkerState {
	if (isConfigFilled) return 'empty-dotted';
	if (required) return 'full';
	if (inOneOfRequired) return 'half';
	return 'empty';
}

/** Build an inline-SVG background image for a gather/expand triangle port
 *  with a visible 1px outline on all edges (including the slanted hypotenuses)
 *  and the correct full/empty/half state. CSS clip-path cannot draw a stroke
 *  along the slanted edges because borders paint on the bounding box, so we
 *  render the triangle entirely inside an SVG background image.
 *
 *  The outline is always drawn as a separate non-filled polygon on top of
 *  any fill, so the half-fill state composes cleanly without depending on
 *  gradient stops.
 */
function trianglePortBackground(
	shape: 'gather' | 'expand',
	state: PortMarkerState,
	color: string,
): string {
	// 12x12 viewBox. 1px inset so the stroke has room on all sides without
	// being clipped. Stroke width 1 matches the circle's 1px border.
	const pts = shape === 'gather'
		? '1,1 11,6 1,11' // base at x=1 (left), point at x=11 (right), `>`
		: '11,1 1,6 11,11'; // base at x=11 (right), point at x=1 (left), `<`

	// For half-fill, the colored half covers the fat-base side of the triangle:
	//   gather: clip to left half (x=0..6)
	//   expand: clip to right half (x=6..12)
	const halfRect = shape === 'gather'
		? '<rect x="0" y="0" width="6" height="12"/>'
		: '<rect x="6" y="0" width="6" height="12"/>';

	let body: string;
	if (state === 'full') {
		body = `<polygon points="${pts}" fill="${color}" stroke="${color}" stroke-width="1" stroke-linejoin="round"/>`;
	} else if (state === 'empty') {
		body = `<polygon points="${pts}" fill="white" stroke="${color}" stroke-width="1" stroke-linejoin="round"/>`;
	} else if (state === 'empty-dotted') {
		// Dotted outline, white fill. Signals "satisfied from code config
		// value". `stroke-dasharray` draws the same polygon outline as 'empty'
		// but with short dashes so it's visually distinct from the solid
		// required/optional outlines.
		body = `<polygon points="${pts}" fill="white" stroke="${color}" stroke-width="1" stroke-linejoin="round" stroke-dasharray="1.5 1.2"/>`;
	} else {
		// half: white base + colored half-fill clipped to fat-base side + full outline on top.
		body =
			`<defs><clipPath id="h">${halfRect}</clipPath></defs>` +
			`<polygon points="${pts}" fill="white"/>` +
			`<polygon points="${pts}" fill="${color}" clip-path="url(#h)"/>` +
			`<polygon points="${pts}" fill="none" stroke="${color}" stroke-width="1" stroke-linejoin="round"/>`;
	}

	const svg = `<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 12 12">${body}</svg>`;
	return `url("data:image/svg+xml,${encodeURIComponent(svg)}") center/contain no-repeat`;
}

/** Compute the { style, class } pair for a `<Handle>` that renders a port
 *  marker. Single source of truth for every port rendering in the project
 *  graph (ProjectNode inputs/outputs, GroupNode external inputs/outputs, in
 *  expanded and collapsed modes).
 *
 *  Parameters:
 *  - port: the port definition (carries required, laneMode, portType)
 *  - oneOfRequiredPorts: set of input port names in a @require_one_of group
 *  - configFilledPorts: set of input port names that have a non-null config
 *    value AND no incoming edge. These render as 'empty-dotted' to signal
 *    "satisfied from code" regardless of their declared required state.
 *  - color: the port's type color
 *  - side: 'input' (honors state) or 'output' (always full)
 *  - extraClass: optional extra Tailwind utilities
 */
export function portMarkerStyle(
	port: PortDefinition,
	oneOfRequiredPorts: Set<string>,
	configFilledPorts: Set<string>,
	color: string,
	side: 'input' | 'output',
	extraClass: string = '',
): { style: string; class: string } {
	const shape = portMarkerShape(port.laneMode);
	// Outputs are always `full`, regardless of the port's `required` flag.
	const state: PortMarkerState = side === 'input'
		? inputMarkerState(port.required, oneOfRequiredPorts.has(port.name), configFilledPorts.has(port.name))
		: 'full';

	let style: string;
	if (shape === 'circle') {
		if (state === 'full') {
			const borderColor = side === 'output' ? 'white' : color;
			style = `background-color: ${color}; border-color: ${borderColor}`;
		} else if (state === 'half') {
			style = `background: linear-gradient(to right, ${color} 50%, white 50%); border-color: ${color}`;
		} else if (state === 'empty-dotted') {
			// Dotted border via border-style; border color still the port type color.
			style = `background-color: white; border-color: ${color}; border-style: dotted`;
		} else {
			style = `background-color: white; border-color: ${color}`;
		}
	} else {
		// Gather or Expand triangle: rendered via inline SVG background image.
		// No border (the SVG stroke provides the outline).
		style = `background: ${trianglePortBackground(shape, state, color)}; border: none`;
	}

	const baseClass = '!w-3 !h-3';
	const shapeClass = shape === 'circle' ? '!border !rounded-full' : '';
	const cls = [baseClass, shapeClass, extraClass].filter(Boolean).join(' ');
	return { style, class: cls };
}
