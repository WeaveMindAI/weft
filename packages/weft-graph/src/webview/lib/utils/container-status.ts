import type { NodeExecutionStatus } from '../types';

/// The status a group or loop card shows for one launch of it, read off
/// the boundary firings and every member execution at play.
///
/// A skipped In boundary is the whole story: the container did not run
/// (its `_should_flow` said no, or a loop had nothing to iterate), and
/// its members carry the same skip, so the card says skipped rather
/// than "completed" for a set of rows that are all terminal.
export function containerStatus(
	inStatus: NodeExecutionStatus,
	related: ReadonlyArray<{ status: NodeExecutionStatus }>,
): NodeExecutionStatus {
	if (related.some((e) => e.status === 'running' || e.status === 'waiting_for_input')) return 'running';
	if (related.some((e) => e.status === 'failed')) return 'failed';
	if (inStatus === 'skipped') return 'skipped';
	const allTerminal =
		related.length > 0 &&
		related.every(
			(e) =>
				e.status === 'completed' ||
				e.status === 'skipped' ||
				e.status === 'failed' ||
				e.status === 'cancelled',
		);
	return allTerminal ? 'completed' : inStatus;
}
