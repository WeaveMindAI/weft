// Firing statuses (mirror Rust `weft_core::program::NodeStatus`, less
// `pending`): running, waiting, paused, completed, skipped, failed,
// cancelled.
// The TS union (`NodeFiringStatus` in protocol.ts) is the
// single source of truth for the closed set; helpers here only need
// to handle those values.

import type { NodeFiringStatus } from '../types';
import type { Failure, SkipReason } from '../../../protocol';

export function getStatusIcon(status: NodeFiringStatus): string {
	switch (status) {
		case 'completed': return '✓';
		case 'running': return '●';
		case 'waiting': return '◉';
		case 'paused': return '◌';
		case 'failed': return '✕';
		case 'cancelled': return '■';
		case 'skipped': return '⊘';
	}
}

/// Override color for status badges so waiting reads "frozen /
/// parked" (cyan) and paused reads "held with its run" (slate),
/// distinct from running (the node's own color) and from
/// completed/failed which carry semantic glyph coloring elsewhere.
/// Returns `undefined` for statuses that should keep the node's own
/// color.
export function getStatusBadgeColor(status: NodeFiringStatus): string | undefined {
	switch (status) {
		case 'waiting':
			return '#06b6d4';
		case 'paused':
			return '#94a3b8';
		case 'cancelled':
			return '#71717a';
		default:
			return undefined;
	}
}

/// The tail a reason's text grows when the closure it names carried a
/// failure: the same words as the Rust `Display`.
function afterFailure(failure: Failure | undefined): string {
	return failure === undefined ? '' : ` because ${failureText(failure)}`;
}

/// SYNC: failureText <-> crates/weft-core/src/pulse.rs `Display for Failure`
function failureText(failure: Failure): string {
	return `'${failure.node}' failed: ${failure.error}`;
}

/// Why a firing did not run, in words. A DECISION (its `_should_flow`
/// said no) reads differently from a CONSEQUENCE (an input it needed
/// never arrived), and the inspector says which.
///
/// An event that carries no reason predates this field: the run is
/// older than the journal shape, so the honest answer is that we do not
/// know rather than a guessed one.
/// SYNC: skipReasonText <-> crates/weft-core/src/run/skip.rs `Display for SkipReason`
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
		case 'nothing_joined': return `none of the branches it joins delivered${afterFailure(reason.failure)}`;
		case 'every_input_closed': return `every wire into it closed with nothing on it${afterFailure(reason.failure)}`;
		case 'one_of_group_closed':
			return `every input of the group (${reason.ports.join(', ')}) closed${afterFailure(reason.failure)}`;
		case 'scope_skipped': return `the scope '${reason.scope}' it lives in did not run`;
		case 'wait_skipped': return 'a person skipped what it was waiting for';
		case 'unfilled':
			return reason.service
				? `not connected: this instance has no ${reason.service} connection for '${reason.field}'`
				: `not filled: this instance left '${reason.field}' empty`;
		default: {
			// Compile-time exhaustiveness; at runtime (a dispatcher newer
			// than this webview sending a new kind) say so honestly
			// instead of rendering '[object Object]'.
			const unhandled: never = reason;
			return `skipped for an unrecognized reason (${JSON.stringify(unhandled)})`;
		}
	}
}

export function displayStatus(status: NodeFiringStatus): string {
	switch (status) {
		case 'completed': return 'Completed';
		case 'running': return 'Running';
		case 'failed': return 'Failed';
		case 'cancelled': return 'Cancelled';
		case 'waiting': return 'Waiting';
		case 'paused': return 'Paused';
		case 'skipped': return 'Skipped';
	}
}
