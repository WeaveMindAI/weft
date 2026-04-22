// Shared types for the composition layer. The layer turns a
// ProjectDefinition (compiler output) + layout sidecar + execution
// state into the {nodes, edges} arrays xyflow renders. Everything
// here is a pure value — no Svelte state, no side effects.

import type {
  GroupDefinition,
  NodeDefinition,
  PortDefinition,
} from '../../shared/protocol';
import type { NodeExecution } from '../components/exec-types';

// A view node is either a real NodeDefinition or a Group synthesized
// from a GroupDefinition. The rest of the composition layer treats
// both uniformly.
export type NodeKind = 'regular' | 'group' | 'annotation';

export interface ViewNode {
  id: string;
  kind: NodeKind;
  label: string | null;
  nodeType: string;
  // Parent GROUP id in the source (pre-visibility walk). Visibility
  // walks may override this to `null` when an ancestor is collapsed.
  rawParentId: string | null;
  inputs: PortDefinition[];
  outputs: PortDefinition[];
  config: Record<string, unknown>;
  features: NodeDefinition['features'];
  groupDef: GroupDefinition | null;
  source: NodeDefinition | null;
}

export interface LayoutEntry {
  x: number;
  y: number;
  w?: number;
  h?: number;
  expanded?: boolean;
}

export type LayoutMap = Record<string, LayoutEntry>;

// Executions keyed by source id. Group virtual nodes carry synthetic
// executions derived from __in/__out passthroughs + internal children.
export type ExecMap = Record<string, NodeExecution[]>;
