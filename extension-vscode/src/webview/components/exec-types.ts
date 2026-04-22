import type { NodeExecutionStatus, LaneFrame } from '../../shared/protocol';

// v1 renders based on the latest of a NodeExecution history. We
// mirror that shape exactly so synth Group executions, pager math,
// and inspector columns stay straightforward.
export interface NodeExecution {
  id: string;
  nodeId: string;
  status: NodeExecutionStatus;
  pulseIdsAbsorbed: string[];
  pulseId: string;
  error?: string;
  startedAt: number;
  completedAt?: number;
  input?: unknown;
  output?: unknown;
  costUsd: number;
  logs: unknown[];
  color: string;
  lane: LaneFrame[];
}
