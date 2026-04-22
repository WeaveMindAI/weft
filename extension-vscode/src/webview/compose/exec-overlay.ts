// Execution state overlay. Turns a per-source-id NodeExecution
// history into:
//   • A per-view-node execution history (groups synthesised from
//     __in/__out/children).
//   • The glow class each xyflow node should wear
//     (node-running / node-completed / node-failed).
//
// v1: ProjectEditorInner.svelte:1018-1111.

import type { NodeExecution } from '../components/exec-types';
import type {
  NodeExecEvent,
  NodeExecutionStatus,
  ProjectDefinition,
} from '../../shared/protocol';
import type { ExecMap, ViewNode } from './types';

// Turn a live NodeExecEvent stream into per-id NodeExecution[].
// Each `started` event pushes a running execution; subsequent events
// update it in-place (v1 also treats an execution as mutable
// until a terminal status).
export function applyExecEvent(prev: ExecMap, event: NodeExecEvent): ExecMap {
  const list = prev[event.node_id]?.slice() ?? [];
  const status = normaliseStatus(event.kind);

  if (status === 'running' || event.kind === 'started') {
    list.push({
      id: event.id ?? `${event.node_id}-${list.length}`,
      nodeId: event.node_id,
      status: 'running',
      pulseIdsAbsorbed: event.pulse_ids_absorbed ?? [],
      pulseId: event.pulse_id ?? '',
      startedAt: event.at_unix * 1000,
      input: event.input,
      costUsd: event.cost_usd ?? 0,
      logs: [],
      color: event.color,
      lane: [],
    });
  } else if (list.length > 0) {
    const latest = { ...list[list.length - 1] };
    latest.status = status;
    if (event.output !== undefined) latest.output = event.output;
    if (event.error !== undefined) latest.error = event.error;
    if (event.completed_at_unix != null) {
      latest.completedAt = event.completed_at_unix * 1000;
    }
    if (event.cost_usd != null) latest.costUsd = event.cost_usd;
    list[list.length - 1] = latest;
  }

  return { ...prev, [event.node_id]: list };
}

function normaliseStatus(
  kind: NodeExecEvent['kind'],
): NodeExecutionStatus {
  if (kind === 'started') return 'running';
  return kind;
}

// v1 group execution synthesis — execution.md lines 81-127.
export function synthesizeGroupExecutions(
  groupId: string,
  exec: ExecMap,
  project: ProjectDefinition,
): NodeExecution[] {
  const inExecs = exec[`${groupId}__in`] ?? [];
  const outExecs = exec[`${groupId}__out`] ?? [];
  const internal: NodeExecution[] = [];
  for (const n of project.nodes) {
    if (!n.scope.includes(groupId)) continue;
    const list = exec[n.id];
    if (list) internal.push(...list);
  }

  return inExecs.map((inExec, i) => {
    const outExec = outExecs[i];
    const related = [...internal, ...inExecs, ...outExecs];
    const hasRunning = related.some(
      (e) => e.status === 'running' || e.status === 'waiting_for_input',
    );
    const hasFailed = related.some((e) => e.status === 'failed');
    const allTerminal =
      related.length > 0 &&
      related.every((e) =>
        ['completed', 'skipped', 'failed', 'cancelled'].includes(e.status),
      );
    const status: NodeExecutionStatus = hasRunning
      ? 'running'
      : hasFailed
      ? 'failed'
      : allTerminal
      ? 'completed'
      : inExec.status;

    return {
      id: `${groupId}-synth-${i}`,
      nodeId: groupId,
      status,
      pulseIdsAbsorbed: inExec.pulseIdsAbsorbed,
      pulseId: inExec.pulseId,
      error: outExec?.error ?? inExec.error,
      startedAt: inExec.startedAt,
      completedAt: outExec?.completedAt ?? inExec.completedAt,
      input: inExec.output,
      output: outExec?.output,
      costUsd: related.reduce((sum, e) => sum + (e.costUsd || 0), 0),
      logs: [],
      color: inExec.color,
      lane: inExec.lane,
    };
  });
}

// Per-node execution history for every view node (real + virtual).
export function executionsByViewNode(
  viewNodes: readonly ViewNode[],
  exec: ExecMap,
  project: ProjectDefinition,
): Map<string, NodeExecution[]> {
  const out = new Map<string, NodeExecution[]>();
  for (const n of viewNodes) {
    if (n.kind === 'group') {
      out.set(n.id, synthesizeGroupExecutions(n.id, exec, project));
    } else {
      out.set(n.id, exec[n.id] ?? []);
    }
  }
  return out;
}

export function glowClassForLatest(execs: readonly NodeExecution[]): string {
  const latest = execs[execs.length - 1];
  if (!latest) return '';
  if (latest.status === 'running' || latest.status === 'waiting_for_input')
    return 'node-running';
  if (latest.status === 'failed') return 'node-failed';
  if (latest.status === 'completed' || latest.status === 'skipped')
    return 'node-completed';
  return '';
}
