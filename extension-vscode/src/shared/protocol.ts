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
  // KEEP IN SYNC with `NodeFeatures` in
  // `weft/crates/weft-core/src/node.rs`. A field that appears in
  // one side but not the other will silently round-trip as
  // undefined (Rust serde drops unknown metadata fields;
  // JS reads missing fields as undefined). See the sync comment
  // over NodeFeatures in node.rs for the full checklist.
  features: {
    oneOfRequired: string[][];
    canAddInputPorts: boolean;
    canAddOutputPorts: boolean;
    hasFormSchema: boolean;
    isTrigger?: boolean;
    hasLiveData?: boolean;
    showDebugPreview?: boolean;
    isOutputDefault?: boolean;
  };
  requiresInfra?: boolean;
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
  // KEEP IN SYNC with `NodeFeatures` in
  // `weft/crates/weft-core/src/node.rs`.
  features?: {
    oneOfRequired?: string[][];
    canAddInputPorts?: boolean;
    canAddOutputPorts?: boolean;
    hasFormSchema?: boolean;
    hasLiveData?: boolean;
    isTrigger?: boolean;
    showDebugPreview?: boolean;
    hidden?: boolean;
  };
  /** Form-field vocabulary for nodes whose `features.hasFormSchema`
   *  is true. Empty/undefined for everything else. The dispatcher's
   *  `/describe/nodes` endpoint inlines this from each node's
   *  `form_field_specs.json` so the form_builder editor can drive
   *  the field-type dropdown without a separate fetch. */
  formFieldSpecs?: FormFieldSpecWire[];
}

/** Render hint for one form field. Opaque to the host; the
 *  consumer (browser extension, dashboard) reads `component` to
 *  pick a UI primitive. */
export interface FormFieldRenderWire {
  component: string;
  source?: 'static' | 'input';
  multiple?: boolean;
  prefilled?: boolean;
}

/** Port template emitted by a field type. */
export interface FormFieldPortWire {
  nameTemplate: string;
  portType: string;
}

/** Wire shape of one `FormFieldSpec`. Mirrors `weft-core::node::
 *  FormFieldSpec` (camelCase). The webview narrows this further
 *  via its own `FormFieldSpec` interface in
 *  `lib/utils/form-field-specs`. */
export interface FormFieldSpecWire {
  fieldType: string;
  label: string;
  render: FormFieldRenderWire;
  requiredConfig: string[];
  optionalConfig: string[];
  addsInputs: FormFieldPortWire[];
  addsOutputs: FormFieldPortWire[];
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

// Minimal shape the webview needs to paint node state. Richer
// journal fields (cost_usd, pulse_id) flow directly in follow-up
// messages when we need them.
//
// `input` and `output` carry the per-node payload verbatim from
// the dispatcher event when the exec follower has it. The modal
// inspector renders them as JSON trees; without these filled in
// it falls back to "(none)" for every node, even when the
// execution actually moved data.
export interface NodeExecEvent {
  nodeId: string;
  state: NodeExecutionStatus | 'started' | 'running' | 'suspended' | 'cancelled';
  /// Lane identity from the dispatcher's NodeStarted /
  /// NodeCompleted events. Stringified JSON of the lane stack
  /// (e.g. `[{"count":5,"index":2}]`). The webview uses this to
  /// match a `completed` event to the SAME lane's `running`
  /// row, so parallel fan-outs don't cross-correlate inputs to
  /// outputs.
  lane: string;
  error?: string;
  input?: unknown;
  output?: unknown;
  /// Wake-signal token. Set on Suspended/Resumed.
  token?: string;
  /// Delivered value. Set on Resumed.
  resumeValue?: unknown;
  /// Reason. Set on Retried.
  retryReason?: string;
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

export type InfraNodeStatus = 'running' | 'stopped';

export interface InfraStatusSnapshot {
  /// One entry per provisioned infra node. Empty array = nothing
  /// provisioned (fresh project / after terminate).
  nodes: Array<{ nodeId: string; status: InfraNodeStatus; endpointUrl: string | null }>;
  /// Coarse rollup for the ActionBar: 'running' if all nodes are
  /// running, 'stopped' if all are stopped, 'mixed' otherwise, or
  /// 'none' if no nodes are provisioned.
  rollup: 'running' | 'stopped' | 'mixed' | 'none';
}

export interface TriggerStatusSnapshot {
  /// Dispatcher's ProjectStatus for this project: 'registered' |
  /// 'active' | 'inactive'. The ActionBar reads 'active' as "the
  /// trigger URLs are minted and listener is running."
  projectStatus: 'registered' | 'active' | 'inactive' | 'unknown';
}

export type HostMessage =
  | { kind: 'parseResult'; response: ParseResponse; source: string; layoutCode: string }
  | { kind: 'parseError'; error: string }
  | { kind: 'execTerminal'; color: string; state: 'completed' | 'failed' }
  | { kind: 'catalogAll'; catalog: Record<string, CatalogEntry> }
  | { kind: 'layoutHint'; positions: Record<string, { x: number; y: number }> }
  | { kind: 'settings'; parseDebounceMs: number; layoutDebounceMs: number }
  | { kind: 'execEvent'; event: NodeExecEvent }
  | { kind: 'edgeActive'; event: EdgeActiveEvent }
  | { kind: 'liveData'; nodeId: string; items: LiveDataItem[] }
  | { kind: 'infraStatus'; snapshot: InfraStatusSnapshot }
  | { kind: 'triggerStatus'; snapshot: TriggerStatusSnapshot }
  | { kind: 'actionFailed'; action: 'infraStart' | 'infraStop' | 'infraTerminate' | 'activate' | 'deactivate'; message: string }
  | { kind: 'followStatus'; status: FollowStatus }
  | { kind: 'execReset' }
  /// Whether the watched .weft source is currently visible in
  /// some editor tab. The webview uses this to swap the "Source"
  /// button into an active/dark state when the source is on
  /// screen, so the user can see at a glance whether clicking it
  /// reveals an existing tab vs opens a new one.
  | { kind: 'sourceState'; open: boolean }
  /// Host signals the worker image is being rebuilt right now.
  /// `verb` identifies which graph-bar action triggered it so
  /// the ActionBar can show "Building..." in place of
  /// "Running..." / "Starting..." / "Starting Infra..." until
  /// the build completes. `verb: undefined` resets the state.
  | { kind: 'buildState'; active: boolean; verb?: 'run' | 'activate' | 'infraStart' };

export interface FollowStatus {
  mode: 'latest' | 'pinned';
  color: string | undefined;
  pendingCount: number;
}

// ─── Messages: webview -> extension host ────────────────────────────────

export type WebviewMessage =
  | { kind: 'ready' }
  | { kind: 'saveWeft'; source: string }
  | { kind: 'saveLayout'; layoutCode: string }
  | { kind: 'log'; level: 'info' | 'warn' | 'error'; message: string }
  | { kind: 'runProject' }
  | { kind: 'stopProject' }
  | { kind: 'nodeSelected'; nodeId: string | null }
  | { kind: 'infraStart' }
  | { kind: 'infraStop' }
  | { kind: 'infraTerminate' }
  | { kind: 'activateProject' }
  | { kind: 'deactivateProject' }
  | { kind: 'followTogglePin' }
  | { kind: 'followCatchUp' }
  /// User clicked the "open .weft source" button on the graph.
  /// Host opens the watched document in a side editor.
  | { kind: 'openSource' }
  /// User clicked the Stop button while a `weft build` was in
  /// flight (Run / Activate / InfraStart pending). Host kills
  /// the child process. The cache key is NOT updated because
  /// the build never completed, so the next attempt rebuilds.
  | { kind: 'cancelBuild' };

// The v1 editor performs all text surgery in-process and sends the
// resulting source with `saveWeft`. No semantic mutation protocol
// is needed — the extension host just applies the full-range
// TextEdit and re-parses.
