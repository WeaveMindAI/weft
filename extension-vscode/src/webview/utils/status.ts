// Ported from dashboard-v1/src/lib/utils/status.ts + inline helpers
// from ExecutionInspector.svelte (formatDuration, formatCost). Used
// by ProjectNode header, status glows, execution inspector, and
// execution footer.

export type NodeStatus =
  | 'pending'
  | 'running'
  | 'waiting_for_input'
  | 'accumulating'
  | 'completed'
  | 'skipped'
  | 'failed'
  | 'cancelled'
  | '';

export function getStatusIcon(status: string): string {
  switch (status) {
    case 'completed':
      return '✓';
    case 'running':
      return '●';
    case 'waiting_for_input':
      return '◉';
    case 'failed':
      return '✕';
    case 'cancelled':
      return '◼';
    case 'skipped':
      return '⊘';
    case 'accumulating':
      return '◎';
    default:
      return '○';
  }
}

export function displayStatus(status: string): string {
  switch (status) {
    case 'completed':
      return 'Completed';
    case 'running':
      return 'Running';
    case 'failed':
      return 'Failed';
    case 'cancelled':
      return 'Cancelled';
    case 'pending':
      return 'Pending';
    case 'waiting_for_input':
      return 'Waiting for Input';
    case 'skipped':
      return 'Skipped';
    case 'accumulating':
      return 'Accumulating';
    default:
      return status;
  }
}

export function getStatusStyle(status: string): { bg: string; text: string; border: string } {
  switch (status) {
    case 'completed':
      return { bg: 'bg-emerald-500/10', text: 'text-emerald-600', border: 'border-emerald-500/20' };
    case 'running':
      return { bg: 'bg-blue-500/10', text: 'text-blue-600', border: 'border-blue-500/20' };
    case 'waiting_for_input':
      return { bg: 'bg-purple-500/10', text: 'text-purple-600', border: 'border-purple-500/20' };
    case 'failed':
      return { bg: 'bg-red-500/10', text: 'text-red-600', border: 'border-red-500/20' };
    case 'cancelled':
      return { bg: 'bg-orange-500/10', text: 'text-orange-600', border: 'border-orange-500/20' };
    case 'pending':
      return { bg: 'bg-slate-500/10', text: 'text-slate-500', border: 'border-slate-500/20' };
    default:
      return { bg: 'bg-zinc-100', text: 'text-zinc-500', border: 'border-zinc-200' };
  }
}

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

export function formatDuration(startMs: number, endMs?: number): string {
  if (!endMs) return 'running...';
  const ms = endMs - startMs;
  if (ms < 1000) return `${ms}ms`;
  if (ms < 60000) return `${(ms / 1000).toFixed(1)}s`;
  return `${Math.floor(ms / 60000)}m ${Math.round((ms % 60000) / 1000)}s`;
}

export function formatCost(usd: number): string {
  if (usd === 0) return '$0';
  if (usd < 0.001) return `$${usd.toFixed(6)}`;
  if (usd < 0.01) return `$${usd.toFixed(4)}`;
  return `$${usd.toFixed(2)}`;
}
