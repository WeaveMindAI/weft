/**
 * Loop port role helpers.
 *
 * A loop has a list of input ports, a list of output ports, and two config
 * lists (over: string[], carry: string[]). From these we derive a discrete
 * role per port. The visual editor exposes the role via a right-click cycle
 * button so the user never edits over/carry directly: they pick a role on
 * the port itself and we propagate to the config lists.
 *
 * Tricky cases the helpers below handle explicitly:
 *   - A user-declared output port and a same-named user-declared input port
 *     with mismatching types: switching the output to carry would conflict
 *     because the carry rule requires both sides to share a type.
 *   - The synthesized carry input shows a read-only menu: the user must
 *     edit the source carry output, not the ghost.
 */

import type { PortDefinition } from '$lib/types';
import type { LoopPortRole } from './port-context-menu';

export interface RoleContext {
	role: LoopPortRole;
	conflictReason?: string;
}

/// The current role of an INPUT port plus any conflict that blocks toggling
/// it to the alternative role.
export function classifyInputPort(
	port: PortDefinition,
	loopConfig: Record<string, unknown> | undefined,
): RoleContext {
	const over = readStringArray(loopConfig, 'over');
	const carry = readStringArray(loopConfig, 'carry');
	// Ghost classification is keyed on the `synthesizedFromCarry` flag, NOT
	// on a name match against `carry`. A user-declared input that happens to
	// share its name with a carry output is a name collision the user must
	// resolve; presenting it as a read-only ghost would hide the real input
	// and remove the user's delete handle.
	if (port.synthesizedFromCarry) return { role: 'synthesized_carry_input' };
	if (carry.includes(port.name)) {
		return {
			role: 'name_collision',
			conflictReason: `Carry output \`${port.name}\` shadows this input. Rename either the input or the carry output.`,
		};
	}
	if (over.includes(port.name)) {
		// Currently iter. Toggling to broadcast: always safe.
		return { role: 'iter' };
	}
	// Currently broadcast. The same-name + carry case was handled by the
	// `name_collision` branch above (a same-name carry output puts
	// `port.name` in the carry list directly), so this path has no
	// remaining conflict to surface.
	return { role: 'broadcast' };
}

/// The current role of an OUTPUT port plus any conflict that blocks toggling
/// it to the alternative role.
export function classifyOutputPort(
	port: PortDefinition,
	loopConfig: Record<string, unknown> | undefined,
	siblingInputs: PortDefinition[],
): RoleContext {
	const carry = readStringArray(loopConfig, 'carry');
	const isCarry = carry.includes(port.name);
	if (isCarry) return { role: 'carry' };
	const role: LoopPortRole = 'gather';
	const conflict = wouldCarryConflict(port, siblingInputs, loopConfig);
	return conflict ? { role, conflictReason: conflict } : { role };
}

/// If toggling an output to carry would conflict with an existing
/// user-declared input port of the same name, return the human reason; else
/// undefined. We block on the existence of the input, not just type mismatch:
/// even a same-type input is a problem because making the output a carry
/// would synthesize a ghost input that shadows the user's declared one. The
/// only safe path is to remove or rename the input first. Synthesized ghosts
/// don't count: they ARE the carry pair we'd produce.
export function wouldCarryConflict(
	output: PortDefinition,
	siblingInputs: PortDefinition[],
	loopConfig: Record<string, unknown> | undefined,
): string | undefined {
	const sameName = siblingInputs.find(p => p.name === output.name);
	if (!sameName) return undefined;
	if (sameName.synthesizedFromCarry) return undefined;
	// A user-declared input with the same name as the would-be carry: block
	// regardless of type. Two situations the user must resolve first:
	//   1. The input is iter (name is in over): carry can't coexist with over.
	//   2. The input is broadcast: carry would shadow it with a synthesized
	//      ghost of the same name.
	const over = readStringArray(loopConfig, 'over');
	if (over.includes(sameName.name)) {
		return `Input \`${sameName.name}\` is already iter (in 'over'); a port can't be both iter and carry. Remove or rename it first.`;
	}
	return `Input \`${sameName.name}\` already exists; carry would shadow it. Remove or rename the input first.`;
}

function readStringArray(loopConfig: Record<string, unknown> | undefined, key: string): string[] {
	if (!loopConfig) return [];
	const v = loopConfig[key];
	if (!Array.isArray(v)) return [];
	return v.filter((x): x is string => typeof x === 'string');
}

/// Remove a port name from BOTH over and carry. Used when a port is deleted
/// from the visible list. The caller plumbs the resulting config ops back to
/// the host via data.onUpdate.
export function removeFromOverAndCarry(
	loopConfig: Record<string, unknown> | undefined,
	name: string,
): { over: string[]; carry: string[] } {
	const over = readStringArray(loopConfig, 'over').filter(n => n !== name);
	const carry = readStringArray(loopConfig, 'carry').filter(n => n !== name);
	return { over, carry };
}
