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
  | { kind: 'parseResult'; response: ParseResponse; source: string; layoutCode: string }
  | { kind: 'parseError'; error: string }
  | { kind: 'catalogAll'; catalog: Record<string, CatalogEntry> }
  | { kind: 'layoutHint'; positions: Record<string, { x: number; y: number }> }
  | { kind: 'settings'; parseDebounceMs: number; layoutDebounceMs: number }
  | { kind: 'execEvent'; event: NodeExecEvent }
  | { kind: 'edgeActive'; event: EdgeActiveEvent }
  | { kind: 'liveData'; nodeId: string; items: LiveDataItem[] }
  | { kind: 'execReset' };

// ─── Messages: webview -> extension host ────────────────────────────────

export type WebviewMessage =
  | { kind: 'ready' }
  | { kind: 'saveWeft'; source: string }
  | { kind: 'saveLayout'; layoutCode: string }
  | { kind: 'log'; level: 'info' | 'warn' | 'error'; message: string };

// The v1 editor performs all text surgery in-process and sends the
// resulting source with `saveWeft`. No semantic mutation protocol
// is needed — the extension host just applies the full-range
// TextEdit and re-parses.
