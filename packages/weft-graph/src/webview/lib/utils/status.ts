// Node statuses (mirror Rust `weft_core::exec::NodeExecutionStatus`):
// running, waiting_for_input, completed, skipped, failed, cancelled.
// The TS union (`NodeExecutionStatus` in shared/protocol.ts) is the
// single source of truth for the closed set; helpers here only need
// to handle those values.

import type { NodeExecutionStatus } from '../types';

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
