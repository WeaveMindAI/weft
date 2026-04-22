// Types shared between extension host and webview. Both import from
// this file so any change propagates.

export interface Span {
  start_line: number;
  start_col: number;
  end_line: number;
  end_col: number;
}

export interface PortDefinition {
  name: string;
  portType: string;
  required: boolean;
  laneMode: 'Single' | 'Expand' | 'Gather';
  laneDepth: number;
  configurable: boolean;
  description?: string;
}

export interface ConfigFieldSpan {
  startLine: number;
  endLine: number;
  origin: 'inline' | 'connection';
}

export interface NodeDefinition {
  id: string;
  nodeType: string;
  label: string | null;
  config: Record<string, unknown>;
  position: { x: number; y: number };
  scope: string[];
  groupBoundary: { groupId: string; role: 'In' | 'Out' } | null;
  inputs: PortDefinition[];
  outputs: PortDefinition[];
  features: {
    oneOfRequired: string[][];
    correlatedPorts: string[][];
    canAddInputPorts: boolean;
    canAddOutputPorts: boolean;
    hasFormSchema: boolean;
  };
  entry: unknown[];
  span?: Span;
  header_span?: Span;
  configSpans?: Record<string, ConfigFieldSpan>;
}

export interface Edge {
  id: string;
  source: string;
  target: string;
  sourceHandle: string | null;
  targetHandle: string | null;
  span?: Span;
}

export interface GroupDefinition {
  id: string;
  label: string | null;
  inPorts: PortDefinition[];
  outPorts: PortDefinition[];
  oneOfRequired: string[][];
  parentGroupId: string | null;
  childGroupIds: string[];
  nodeIds: string[];
  span?: Span;
  headerSpan?: Span;
}

export interface ProjectDefinition {
  id: string;
  name: string;
  description: string | null;
  nodes: NodeDefinition[];
  edges: Edge[];
  groups: GroupDefinition[];
}

export type Severity = 'error' | 'warning' | 'info' | 'hint';

export interface Diagnostic {
  line: number;
  column: number;
  severity: Severity;
  message: string;
  code?: string;
}

// v1 parity: FieldDefinition carries per-kind extras. We serialize the
// full set so the webview can render min/max/pattern/options/etc.
export type ApiKeyProvider = 'openrouter' | 'elevenlabs' | 'tavily' | 'apollo';
export type FieldKind =
  | 'text'
  | 'textarea'
  | 'code'
  | 'select'
  | 'multiselect'
  | 'number'
  | 'checkbox'
  | 'password'
  | 'blob'
  | 'api_key'
  | 'form_builder';

export interface FieldType {
  kind: FieldKind | string;
  language?: string;
  options?: string[];
  accept?: string;
  provider?: ApiKeyProvider;
  min?: number;
  max?: number;
  step?: number;
  maxLength?: number;
  minLength?: number;
  pattern?: string;
  [key: string]: unknown;
}

export interface FieldDef {
  key: string;
  label: string;
  field_type: FieldType;
  default_value?: unknown;
  required?: boolean;
  description?: string;
  placeholder?: string;
}

export interface PortDef {
  name: string;
  type: string;
  required?: boolean;
  configurable?: boolean;
  description?: string;
}

export type NodeCategory =
  | 'Triggers'
  | 'AI'
  | 'Data'
  | 'Flow'
  | 'Utility'
  | 'Debug'
  | 'Infrastructure';

export interface CatalogEntry {
  type: string;
  label: string;
  description: string;
  category: NodeCategory | string;
  tags: string[];
  icon?: string;
  color?: string;
  inputs: PortDef[];
  outputs: PortDef[];
  fields: FieldDef[];
  entry: unknown[];
  requires_infra?: boolean;
  features?: {
    oneOfRequired?: string[][];
    correlatedPorts?: string[][];
    canAddInputPorts?: boolean;
    canAddOutputPorts?: boolean;
    hasFormSchema?: boolean;
    hasLiveData?: boolean;
    isTrigger?: boolean;
    showDebugPreview?: boolean;
    hidden?: boolean;
  };
}

export interface ParseResponse {
  project: ProjectDefinition;
  catalog: Record<string, CatalogEntry>;
  diagnostics: Diagnostic[];
}

export type NodeExecutionStatus =
  | 'pending'
  | 'running'
  | 'waiting_for_input'
  | 'accumulating'
  | 'completed'
  | 'skipped'
  | 'failed'
  | 'cancelled';

export interface LaneFrame {
  count: number;
  index: number;
}

export interface NodeExecEvent {
  id: string;
  color: string;
  node_id: string;
  lane: string;
  kind: NodeExecutionStatus | 'started';
  input?: unknown;
  output?: unknown;
  error?: string;
  at_unix: number;
  completed_at_unix?: number;
  cost_usd?: number;
  pulse_id?: string;
  pulse_ids_absorbed?: string[];
}

export interface LiveDataItem {
  type: 'text' | 'image' | 'progress';
  label: string;
  data: string | number;
}

// Active-edge events for the pulse-in-transit animation.
export interface EdgeActiveEvent {
  edgeId: string;
  color?: string;
  active: boolean;
}

// ─── Messages: extension host -> webview ────────────────────────────────

export type HostMessage =
  | { kind: 'parseResult'; response: ParseResponse }
  | { kind: 'parseError'; error: string }
  | { kind: 'layoutHint'; positions: Record<string, { x: number; y: number }> }
  | { kind: 'settings'; parseDebounceMs: number; layoutDebounceMs: number }
  | { kind: 'execEvent'; event: NodeExecEvent }
  | { kind: 'edgeActive'; event: EdgeActiveEvent }
  | { kind: 'liveData'; nodeId: string; items: LiveDataItem[] }
  | { kind: 'execReset' };

// ─── Messages: webview -> extension host ────────────────────────────────

export type WebviewMessage =
  | { kind: 'ready' }
  | { kind: 'mutation'; mutation: GraphMutation }
  | { kind: 'positionsChanged'; positions: Record<string, { x: number; y: number }> }
  | { kind: 'layoutChanged'; layout: LayoutSnapshot }
  | { kind: 'log'; level: 'info' | 'warn' | 'error'; message: string };

// Layout sidecar persisted to .layout.json. Keyed by node/group id.
export interface LayoutEntry {
  x: number;
  y: number;
  w?: number;
  h?: number;
  expanded?: boolean;
}

export type LayoutSnapshot = Record<string, LayoutEntry>;

// Graph mutations the webview can request. Extension host translates
// each into a surgical TextEditor.edit() using the stored spans.

export type GraphMutation =
  | { kind: 'addNode'; id: string; nodeType: string; parentGroupLabel?: string | null }
  | { kind: 'removeNode'; id: string }
  | { kind: 'addEdge'; source: string; sourcePort: string; target: string; targetPort: string; scopeGroupLabel?: string | null }
  | { kind: 'removeEdge'; source: string; sourcePort: string; target: string; targetPort: string }
  | { kind: 'updateConfig'; nodeId: string; key: string; value: unknown }
  | { kind: 'updateLabel'; nodeId: string; label: string | null }
  | { kind: 'duplicateNode'; nodeId: string }
  | { kind: 'addGroup'; label: string; parentGroupLabel?: string | null }
  | { kind: 'removeGroup'; label: string }
  | { kind: 'renameGroup'; oldLabel: string; newLabel: string }
  | { kind: 'updateGroupPorts'; groupLabel: string; inputs: PortDefinition[]; outputs: PortDefinition[] }
  | { kind: 'updateNodePorts'; nodeId: string; inputs: PortDefinition[]; outputs: PortDefinition[] }
  | { kind: 'moveNodeScope'; nodeId: string; targetGroupLabel: string | null }
  | { kind: 'moveGroupScope'; groupLabel: string; targetGroupLabel: string | null }
  | { kind: 'updateProjectMeta'; name?: string | null; description?: string | null };
