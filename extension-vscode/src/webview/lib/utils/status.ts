// Node statuses: pending, running, waiting_for_input, accumulating, completed, skipped, failed
// Execution statuses: pending, running, waiting_for_input, completed, failed, cancelled

/** Format a date string as relative time (e.g., "5m ago", "2h ago", "3d ago"). */
export function formatTimeAgo(dateStr: string | null): string {
	if (!dateStr) return 'never';
	const ms = Date.now() - new Date(dateStr).getTime();
	const min = Math.floor(ms / 60000);
	if (min < 1) return 'just now';
	if (min < 60) return `${min}m ago`;
	const hr = Math.floor(min / 60);
	if (hr < 24) return `${hr}h ago`;
	return `${Math.floor(hr / 24)}d ago`;
}

/** Format a date string as localized date+time. */
export function formatDate(dateStr: string): string {
	return new Date(dateStr).toLocaleString();
}

/** Get CSS style classes for an execution/node status badge. */
export function getStatusStyle(status: string): { bg: string; text: string; border: string } {
	switch (status) {
		case 'completed': return { bg: 'bg-emerald-500/10', text: 'text-emerald-600', border: 'border-emerald-500/20' };
		case 'running': return { bg: 'bg-blue-500/10', text: 'text-blue-600', border: 'border-blue-500/20' };
		case 'waiting_for_input': return { bg: 'bg-purple-500/10', text: 'text-purple-600', border: 'border-purple-500/20' };
		case 'failed': return { bg: 'bg-red-500/10', text: 'text-red-600', border: 'border-red-500/20' };
		case 'cancelled': return { bg: 'bg-orange-500/10', text: 'text-orange-600', border: 'border-orange-500/20' };
		case 'pending': return { bg: 'bg-slate-500/10', text: 'text-slate-500', border: 'border-slate-500/20' };
		default: return { bg: 'bg-zinc-100', text: 'text-zinc-500', border: 'border-zinc-200' };
	}
}

/** Strip _raw keys from output objects for display. */
export function cleanOutput(output: unknown): unknown {
	if (output && typeof output === 'object' && !Array.isArray(output)) {
		const obj = output as Record<string, unknown>;
		const cleaned: Record<string, unknown> = {};
		for (const [k, v] of Object.entries(obj)) {
			if (k !== '_raw') cleaned[k] = v;
		}
		return Object.keys(cleaned).length > 0 ? cleaned : obj;
	}
	return output;
}
export function getStatusIcon(status: string): string {
	switch (status) {
		case 'completed': return '✓';
		case 'running': return '●';
		case 'waiting_for_input': return '◉';
		case 'failed': return '✕';
		case 'cancelled': return '◼';
		case 'skipped': return '⊘';
		case 'accumulating': return '◎';
		default: return '○';
	}
}

export function displayStatus(status: string): string {
	switch (status) {
		case 'completed': return 'Completed';
		case 'running': return 'Running';
		case 'failed': return 'Failed';
		case 'cancelled': return 'Cancelled';
		case 'pending': return 'Pending';
		case 'waiting_for_input': return 'Waiting for Input';
		case 'skipped': return 'Skipped';
		case 'accumulating': return 'Accumulating';
		default: return status;
	}
}
