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
  configSpans?: Record<string, Span>;
}

export interface Edge {
  id: string;
  source: string;
  target: string;
  sourceHandle: string | null;
  targetHandle: string | null;
  span?: Span;
}

export interface ProjectDefinition {
  id: string;
  name: string;
  description: string | null;
  nodes: NodeDefinition[];
  edges: Edge[];
}

export type Severity = 'error' | 'warning' | 'info' | 'hint';

export interface Diagnostic {
  line: number;
  column: number;
  severity: Severity;
  message: string;
  code?: string;
}

export interface FieldType {
  kind: string;
  // fields vary by kind (select: options, code: language, etc)
  [key: string]: unknown;
}

export interface FieldDef {
  key: string;
  label: string;
  field_type: FieldType;
  default_value?: unknown;
  required?: boolean;
  description?: string;
}

export interface PortDef {
  name: string;
  type: string;
  required?: boolean;
  configurable?: boolean;
}

export interface CatalogEntry {
  type: string;
  label: string;
  description: string;
  category: string;
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
  };
}

export interface ParseResponse {
  project: ProjectDefinition;
  catalog: Record<string, CatalogEntry>;
  diagnostics: Diagnostic[];
}

export interface NodeExecEvent {
  color: string;
  node_id: string;
  lane: string;
  kind: 'started' | 'completed' | 'failed' | 'skipped';
  input?: unknown;
  output?: unknown;
  error?: string;
  at_unix: number;
}

// ─── Messages: extension host -> webview ────────────────────────────────

export type HostMessage =
  | { kind: 'parseResult'; response: ParseResponse }
  | { kind: 'parseError'; error: string }
  | { kind: 'layoutHint'; positions: Record<string, { x: number; y: number }> }
  | { kind: 'settings'; parseDebounceMs: number; layoutDebounceMs: number }
  | { kind: 'execEvent'; event: NodeExecEvent }
  | { kind: 'execReset' };

// ─── Messages: webview -> extension host ────────────────────────────────

export type WebviewMessage =
  | { kind: 'ready' }
  | { kind: 'mutation'; mutation: GraphMutation }
  | { kind: 'positionsChanged'; positions: Record<string, { x: number; y: number }> }
  | { kind: 'log'; level: 'info' | 'warn' | 'error'; message: string };

// Graph mutations the webview can request. Extension host translates
// each into a surgical TextEditor.edit() using the stored spans.

export type GraphMutation =
  | { kind: 'addNode'; id: string; nodeType: string }
  | { kind: 'removeNode'; id: string }
  | { kind: 'addEdge'; source: string; sourcePort: string; target: string; targetPort: string }
  | { kind: 'removeEdge'; edgeId: string }
  | { kind: 'updateConfig'; nodeId: string; key: string; value: unknown };
