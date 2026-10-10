// Node statuses (mirror Rust `weft_core::exec::NodeExecutionStatus`):
// running, waiting_for_input, completed, skipped, failed, cancelled.
// The TS union (`NodeExecutionStatus` in protocol.ts) is the
// single source of truth for the closed set; helpers here only need
// to handle those values.

import type { NodeExecutionStatus } from '../types';
import type { Failure, SkipReason } from '../../../protocol';

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
/// The tail a reason's text grows when the closure it names carried a
/// failure: the same words as the Rust `Display`.
function afterFailure(failure: Failure | undefined): string {
	return failure === undefined ? '' : ` because ${failureText(failure)}`;
}

/// SYNC: failureText <-> crates/weft-core/src/pulse.rs `Display for Failure`
function failureText(failure: Failure): string {
	return `'${failure.node}' failed: ${failure.error}`;
}

export function skipReasonText(reason: SkipReason | undefined): string {
	if (!reason) return 'reason not recorded';
	switch (reason.kind) {
		case 'did_not_flow': return 'its `_should_flow` said no';
		case 'flow_closed': return `nothing ever answered its \`_should_flow\`${afterFailure(reason.failure)}`;
		case 'did_flow': return 'its `_should_not_flow` saw a value';
		case 'watched_node_failed':
			return `what its \`_should_not_flow\` watches did not finish (${failureText(reason.failure)}), which is not the absence this node runs on`;
		case 'required_input_closed':
			return `the required input '${reason.port}' closed${afterFailure(reason.failure)}`;
		case 'every_input_closed': return `every input closed${afterFailure(reason.failure)}`;
		case 'one_of_group_closed':
			return `every input of the group (${reason.ports.join(', ')}) closed${afterFailure(reason.failure)}`;
		case 'scope_skipped': return `the scope '${reason.scope}' it lives in did not run`;
		case 'wait_skipped': return 'a person skipped what it was waiting for';
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
