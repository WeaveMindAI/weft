// Node statuses (mirror Rust `weft_core::exec::NodeExecutionStatus`):
// running, waiting_for_input, completed, skipped, failed, cancelled.
// The TS union (`NodeExecutionStatus` in protocol.ts) is the
// single source of truth for the closed set; helpers here only need
// to handle those values.

import type { NodeExecutionStatus } from '../types';
import type { SkipReason } from '../../../protocol';

export function getStatusIcon(status: NodeExecutionStatus): string {
	switch (status) {
		case 'completed': return '✓';
		case 'running': return '●';
		case 'waiting_for_input': return '◉';
		case 'failed': return '✕';
		case 'cancelled': return '■';
		case 'skipped': return '⊘';
	}
}

/// Override color for status badges so waiting reads "frozen /
/// parked" (cyan), distinct from running (the node's own color) and
/// from completed/failed which carry semantic glyph coloring
/// elsewhere. Returns `undefined` for statuses that should keep the
/// node's own color.
export function getStatusBadgeColor(status: NodeExecutionStatus): string | undefined {
	switch (status) {
		case 'waiting_for_input':
			return '#06b6d4';
		case 'cancelled':
			return '#71717a';
		default:
			return undefined;
	}
}

/// Why a firing did not run, in words. A DECISION (its `_should_flow`
/// said no) reads differently from a CONSEQUENCE (an input it needed
/// never arrived), and the inspector says which.
///
/// An event that carries no reason predates this field: the run is
/// older than the journal shape, so the honest answer is that we do not
/// know rather than a guessed one.
export function skipReasonText(reason: SkipReason | undefined): string {
	if (!reason) return 'reason not recorded';
	switch (reason.kind) {
		case 'did_not_flow': return 'its `_should_flow` said no';
		case 'flow_closed': return 'nothing ever answered its `_should_flow`';
		case 'required_input_closed': return `the required input '${reason.port}' closed`;
		case 'every_input_closed': return 'every input closed';
		case 'one_of_group_closed':
			return `every input of the group (${reason.ports.join(', ')}) closed`;
		case 'outside_this_run': return 'it is outside the part of the graph this execution runs';
		default: {
			// Compile-time exhaustiveness; at runtime (a dispatcher newer
			// than this webview sending a new kind) say so honestly
			// instead of rendering '[object Object]'.
			const unhandled: never = reason;
			return `skipped for an unrecognized reason (${JSON.stringify(unhandled)})`;
		}
	}
}

export function displayStatus(status: NodeExecutionStatus): string {
	switch (status) {
		case 'completed': return 'Completed';
		case 'running': return 'Running';
		case 'failed': return 'Failed';
		case 'cancelled': return 'Cancelled';
		case 'waiting_for_input': return 'Waiting';
		case 'skipped': return 'Skipped';
	}
}
