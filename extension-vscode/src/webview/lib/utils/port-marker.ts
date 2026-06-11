import type { PortDefinition } from '$lib/types';

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

/** Compute the { style, class } pair for a `<Handle>` that renders a port
 *  marker. Single source of truth for every port rendering in the project
 *  graph (ProjectNode inputs/outputs, GroupNode external inputs/outputs, in
 *  expanded and collapsed modes).
 *
 *  Parameters:
 *  - port: the port definition (carries required, portType)
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
	// Outputs are always `full`, regardless of the port's `required` flag.
	const state: PortMarkerState = side === 'input'
		? inputMarkerState(port.required, oneOfRequiredPorts.has(port.name), configFilledPorts.has(port.name))
		: 'full';

	let style: string;
	if (state === 'full') {
		const borderColor = side === 'output' ? 'white' : color;
		style = `background-color: ${color}; border-color: ${borderColor}`;
	} else if (state === 'half') {
		style = `background: linear-gradient(to right, ${color} 50%, white 50%); border-color: ${color}`;
	} else if (state === 'empty-dotted') {
		style = `background-color: white; border-color: ${color}; border-style: dotted`;
	} else {
		style = `background-color: white; border-color: ${color}`;
	}

	const cls = ['!w-3 !h-3', '!border !rounded-full', extraClass].filter(Boolean).join(' ');
	return { style, class: cls };
}
