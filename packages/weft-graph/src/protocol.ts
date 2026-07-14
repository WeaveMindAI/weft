// Types shared between extension host and webview. Both import from
// this file so any change propagates.

export interface Span {
  startLine: number;
  startColumn: number;
  endLine: number;
  endColumn: number;
}

export interface PortDefinition {
  name: string;
  portType: string;
  required: boolean;
  configurable: boolean;
  description?: string;
  /// True iff this port was auto-synthesized by the loop-lowering pass
  /// (the input side of a carry port). The editor renders it as a ghost
  /// mirror of the matching carry output. Never user-editable; the user
  /// changes the output's role to remove the synthesized input.
  synthesizedFromCarry?: boolean;
}

/// Source span of one config field plus how it was written. `origin` tells
/// the editor how to rewrite the field in place: an inline field
/// (`n = Type { k: v }`) becomes `k: v`; a connection-line field (`n.k = v`)
/// keeps its `n.k = ` prefix. Matches the Rust `ConfigFieldSpan`.
export interface ConfigFieldSpan {
  span: Span;
  origin: 'inline' | 'connection';
}

/// A `@file("path", Type)` reference on a config field. `config[field]`
/// holds the resolved value; this says the value came from a file, so the
/// editor renders the field as file-backed and writes edits to `path`
/// instead of rewriting the `@file(...)` token in the source.
export interface FileRef {
  path: string;
  type: string;
}

/// The resolved state of a `@file` target: its content, a read error, or
/// `loading` while its bytes are still being fetched (lazy load: the editor opens
/// the graph before every asset has arrived). A file-backed field is always
/// file-backed; if the file can't be read it fails loudly (`error`). There is no
/// "fall back to the marker" state. A `loading` field renders a NON-INTERACTIVE
/// skeleton so it can't be edited before its real content lands (which would
/// clobber the file).
export type FileContent = { content: string } | { error: string } | { loading: true };

// SYNC: NodeFeaturesWire <-> crates/weft-core/src/node.rs NodeFeatures
// A field that appears in one side but not the other will silently
// round-trip as undefined (Rust serde drops unknown metadata fields;
// JS reads missing fields as undefined). See the sync comment
// over NodeFeatures in node.rs for the full checklist.
export interface NodeFeaturesWire {
  oneOfRequired?: string[][];
  canAddInputPorts?: boolean;
  canAddOutputPorts?: boolean;
  hasFormSchema?: boolean;
  isTrigger?: boolean;
  showDebugPreview?: boolean;
  // SYNC: interruptGrace <-> crates/weft-core/src/node.rs NodeFeatures.interrupt_grace
  // On cancellation, give the node a short grace window to observe the
  // cancellation flag and wrap up before its future is aborted.
  interruptGrace?: boolean;
  // SYNC: showImagePreview <-> crates/weft-core/src/node.rs NodeFeatures.show_image_preview
  showImagePreview?: boolean;
  // SYNC: showDownloadLink <-> crates/weft-core/src/node.rs NodeFeatures.show_download_link
  showDownloadLink?: boolean;
  isOutputDefault?: boolean;
  /// Names the endpoint serving the node's `/live` HTTP route the
  /// body panel polls. Unset for TCP-only infra (Postgres, Redis)
  /// so the panel doesn't show a broken eye.
  liveEndpoint?: string;
  hidden?: boolean;
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
  features: NodeFeaturesWire;
  requiresInfra?: boolean;
  span?: Span;
  headerSpan?: Span;
  configSpans?: Record<string, ConfigFieldSpan>;
  fileRefs?: Record<string, FileRef>;
  /// Set on an opaque `@include` node: the included `.weft` file path. The
  /// editor renders this as an expandable group that navigates into the file.
  includePath?: string;
}

export interface Edge {
  id: string;
  source: string;
  target: string;
  sourceHandle: string | null;
  targetHandle: string | null;
  span?: Span;
}

// SYNC: GroupDefinition (kind + loopConfig) <-> crates/weft-core/src/project.rs GroupKind
export interface GroupDefinition {
  id: string;
  /// `group` or `loop`. The visual editor renders a loop differently
  /// (distinct color/glyph + carry-port double rendering) even
  /// though both flatten through the same boundary-pair shape.
  /// On the Rust side `kind` + `loopConfig` are ONE flattened tagged
  /// enum (GroupKind), so a loop always carries its config and a
  /// payload missing `kind` fails Rust deserialization.
  kind: 'group' | 'loop';
  /// Loop config fields (parallel/over/carry/max_iters/trim_on_mismatch).
  /// Always present when `kind === 'loop'`, never for `group`.
  loopConfig?: Record<string, unknown> | null;
  label: string | null;
  inPorts: PortDefinition[];
  outPorts: PortDefinition[];
  oneOfRequired: string[][];
  parentGroupId: string | null;
  childGroupIds: string[];
  nodeIds: string[];
  span?: Span;
  headerSpan?: Span;
  /// The group's description: the plain `# ...` comment on the first body
  /// line of the group (text without the `# `).
  // SYNC: GroupDefinition <-> crates/weft-core/src/project.rs GroupDefinition
  description?: string | null;
}

export interface ProjectDefinition {
  id: string;
  // The parsed graph carries no name/description: a project's name comes from
  // the manifest (`weft.toml` `[package] name`), and descriptions are per-group
  // (first plain `# ...` body line). The Rust `ProjectDefinition` mirrors this.
  nodes: NodeDefinition[];
  edges: Edge[];
  groups: GroupDefinition[];
}

export type Severity = 'error' | 'warning' | 'info' | 'hint';

export interface Diagnostic {
  // 1-based start line, 0-based start char column.
  line: number;
  column: number;
  // End of the culprit's range (1-based line, 0-based char column, exclusive).
  // Optional for back-compat with older payloads; absent => a 1-char caret.
  endLine?: number;
  endColumn?: number;
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
  // SYNC: FieldKind 'file_drop' <-> crates/weft-core/src/node.rs FieldType::FileDrop
  | 'file_drop'
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

// SYNC: CatalogEntry/FieldDef/FieldType/PortDef <-> crates/weft-core/src/node.rs
//       NodeMetadata (the `weft describe-nodes` serialization these mirror)
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
  requires_infra?: boolean;
  features?: NodeFeaturesWire;
  /** Form-field vocabulary for nodes whose `features.hasFormSchema`
   *  is true. Empty/undefined for everything else. A metadata key,
   *  declared once in the package root's partial `metadata.json` and
   *  inherited by every member, so the form_builder editor can drive
   *  the field-type dropdown without a separate fetch. */
  formFieldSpecs?: FormFieldSpecWire[];
  /** The paid service this node calls on a deployment-granted access.
   *  Present only on nodes that declare one (a metadata key, inherited
   *  from the package root). `deny_unknown_fields` Rust-side, so the
   *  wire shape is exactly `{ name, base_url }`. */
  // SYNC: provider <-> crates/weft-core/src/node.rs NodeMetadata.provider (ProviderDecl)
  provider?: { name: string; base_url: string };
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

/** Wire shape of one `FormFieldSpec` (camelCase). The webview narrows
 *  this further via its own `FormFieldSpec` interface in
 *  `lib/utils/form-field-specs`. */
// SYNC: FormFieldSpecWire <-> crates/weft-core/src/node.rs FormFieldSpec
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

// SYNC: NodeExecutionStatus <-> crates/weft-core/src/exec/execution.rs NodeExecutionStatus
/// Adding a state requires adding it on both sides; the UI lookup
/// tables in `webview/lib/utils/status.ts` exhaust this union so
/// drift compiles as an error. The earlier shape had ghost variants
/// (`pending` / `suspended` / `accumulating`) the dispatcher never
/// emitted; they painted states the engine could not produce and
/// `suspended` doubled-up with the real `waiting_for_input` event.
export type NodeExecutionStatus =
  | 'running'
  | 'waiting_for_input'
  | 'completed'
  | 'skipped'
  | 'failed'
  | 'cancelled';

// Minimal shape the webview needs to paint node state. Richer
// journal fields (cost_usd, pulse_id) flow directly in follow-up
// messages when we need them.
//
// `input` and `output` carry the per-node payload verbatim from
// the dispatcher event when the exec follower has it. The modal
// inspector renders them as JSON trees; without these filled in
// it falls back to "(none)" for every node, even when the
// execution actually moved data.

/// Frame stack identifying which iteration of which (nested) loop this
/// firing belongs to. Empty for firings outside any loop.
// SYNC: LoopIteration <-> crates/weft-core/src/frames.rs LoopIteration
export interface LoopIteration {
  index: number;
}

export interface NodeExecEvent {
  nodeId: string;
  state: NodeExecutionStatus;
  /// Frame stack: empty when the firing is not inside any loop;
  /// `[{index:2}]` when inside iteration 2 of a single loop; nested
  /// loops extend the array. Used as part of the execution-card key so
  /// parallel iterations don't cross-correlate.
  frames: LoopIteration[];
  error?: string;
  input?: unknown;
  /// Wired input ports that arrived as CLOSURE markers for this firing
  /// (the upstream frame stack terminated without firing them). Disjoint from
  /// the keys present in `input`; the inspector renders these as
  /// "(closed)" to distinguish them from user-emitted nulls. Present
  /// only on `running` (NodeStarted) and `skipped` (NodeSkipped)
  /// events; other state transitions don't carry the per-port closed
  /// info because it's the same set the firing started with.
  closedPorts?: string[];
  output?: unknown;
}

/// Live loop event surfaced through the dispatcher SSE stream. Mirrors
/// the four `LoopInstantiated` / `LoopIterationLaunched` / `LoopOutFired`
/// / `LoopTerminated` journal events. The inspector groups by `groupId`
/// + `parentFrames` so nested loops and parallel sibling iterations
/// each render under their own card.
// SYNC: LoopTerminationReason <-> crates/weft-core/src/primitive.rs LoopTerminationReason
export type LoopTerminationReason =
  | 'over_exhausted'
  | 'done_voted'
  | 'max_iters_reached'
  | 'cancelled'
  | 'failed';

export type LoopInspectorEvent =
  | {
      kind: 'instantiated';
      groupId: string;
      parentFrames: LoopIteration[];
      iterCount: number;
      parallel: boolean;
    }
  | {
      kind: 'iteration_launched';
      groupId: string;
      parentFrames: LoopIteration[];
      index: number;
    }
  | {
      kind: 'out_fired';
      groupId: string;
      parentFrames: LoopIteration[];
      index: number;
      doneVote?: boolean | null;
    }
  | {
      kind: 'terminated';
      groupId: string;
      parentFrames: LoopIteration[];
      reason: LoopTerminationReason;
    };

/// One line in a node's bus inspector panel. IRC-shaped: `joined` /
/// `left` render as `* name joined` / `* name left`; `message`
/// renders as `from: <payload-pretty>` for journaled buses or
/// `from sent <kind> of <size> bytes [hash: <hex>]` for ephemeral
/// buses (where `payload` is null). `closed` renders an explicit
/// `* the bus closed here` marker. `busId` groups lines by channel
/// so a node attached to multiple buses gets one scrollable section
/// per bus. Replay orders lines by arrival from the SSE stream (the
/// dispatcher already orders them by journal row id server-side).
// SYNC: JournaledPayload <-> crates/weft-core/src/primitive.rs JournaledPayload
/// Tagged payload. Default `Option<Value>` would collapse `Some(Value::Null)` with
/// `None` at the JSON boundary; the tag preserves the distinction
/// so a journaled bus that sends literal `null` doesn't render as
/// if it were ephemeral.
export type JournaledPayload =
  | { kind: 'journaled'; value: unknown }
  | { kind: 'ephemeral' };

export type BusInspectorEvent =
  | { kind: 'joined'; busId: string; offset: number; name: string; atUnix: number }
  | { kind: 'left'; busId: string; offset: number; name: string; atUnix: number }
  | {
      kind: 'message';
      busId: string;
      offset: number;
      from: string;
      msgKind: string;
      payload: JournaledPayload;
      payloadByteSize: number;
      payloadSha256Prefix: string;
      atUnix: number;
    }
  | { kind: 'closed'; busId: string; offset: number; atUnix: number };

/// One live-caller-connection event the inspector replays. One caller
/// per execution (no busId; the execution color is the identity).
/// `payload` reuses `JournaledPayload` so a high-volume stream renders
/// metadata-only exactly like an ephemeral bus.
export type CallerInspectorEvent =
  | { kind: 'connected'; offset: number; protocol: string; atUnix: number }
  | {
      kind: 'inbound';
      offset: number;
      payload: JournaledPayload;
      payloadByteSize: number;
      payloadSha256Prefix: string;
      atUnix: number;
    }
  | {
      kind: 'outbound';
      offset: number;
      payload: JournaledPayload;
      payloadByteSize: number;
      payloadSha256Prefix: string;
      terminal: boolean;
      atUnix: number;
    }
  | { kind: 'errored'; offset: number; message: string; atUnix: number }
  | { kind: 'disconnected'; offset: number; reason: string; atUnix: number };

/// Per-bus metadata derived dispatcher-side from the bus marker JSON
/// (`{"__weft_bus__": {"id":..., "mode":"journaled"|"ephemeral"}}`).
/// The dispatcher attaches `ephemeral` to every `BusParticipant` edge
/// it derives from a `PulseEmitted`, so the webview learns mode the
/// same time it learns about the bus. Stored keyed by `busId`.
export interface BusMeta {
  ephemeral: boolean;
}

// SYNC: CorruptionSite <-> crates/weft-core/src/primitive.rs CorruptionSite
/// Names the fold step that rejected a journal row.
/// Kept as a closed union so the inspector renders a stable label;
/// adding a fold branch that can fail requires adding a variant
/// both here and on the Rust side.
export type CorruptionSite =
  | 'PulseEmitted'
  | 'NodeStarted'
  | 'NodeResumed'
  | 'LoopIterationLaunched'
  | 'LoopOutFired'
  | 'LoopTerminated'
  | 'NodeCompleted'
  | 'NodeFailed'
  | 'NodeSkipped'
  | 'NodeCancelled';

/// One item rendered in a node's body panel. Two distinct feeds
/// produce items: infra `/live` (infra-pod telemetry) and signal
/// `/display` (trigger URL + auth metadata). The two feeds flow
/// through SEPARATE message channels (`infraLive`, `signalDisplay`)
/// keyed by node id; they never cross. Adding a new presentation
/// kind: add a string to the union and a branch in ProjectNode's
/// rendering.
export interface LiveDataItem {
  /// - `text`: plain copyable string in a code-style box.
  /// - `image`: `data` is a data URI; rendered inline.
  /// - `progress`: `data` is a 0..1 number; rendered as a bar.
  /// - `secret`: hidden behind a `••••` mask until the user
  ///   clicks the eye icon to reveal. Copy still works on the
  ///   underlying value. Use for API keys, signed URLs, anything
  ///   that shouldn't sit on screen by default.
  type: 'text' | 'image' | 'progress' | 'secret';
  label: string;
  data: string | number;
  /// Optional action button rendered next to the item. Click
  /// posts a `signalAction` message; the host routes through
  /// `/projects/{id}/signals/{node_id}/action`. The listener's
  /// kind impl owns the action's payload schema. Use for
  /// regenerate-api-key, future "rotate", etc. Generic so node
  /// authors can add buttons without changing the inspector.
  action?: {
    label: string;
    actionKind: string;
    payload?: unknown;
    confirm?: string;
  };
}

/// State of one node's body feed. Pollers emit one of these per
/// tick: `ok` carries the rendered items, `error` carries a
/// short user-facing message the webview shows in place of the
/// items. There is NO silent fallback: if the poller can't reach
/// the backend, the user sees the error verbatim.
export type NodeFeedState =
  | { state: 'ok'; items: LiveDataItem[] }
  | { state: 'error'; error: string };

// ─── Messages: extension host -> webview ────────────────────────────────
//
// Backend state flows to the webview through exactly two channels:
//   - actionBarState: the host's state-machine projection (idle /
//     cli_running / execution_running / error). Driven by status
//     fetches + CLI NDJSON events.
//   - statusSnapshot: the latest `weft status --json` payload
//     (drift bits, available actions, per-node infra status). Used
//     by the action bar AND graph decorations.
//
// Adding a new graph-decoration source means folding it into
// statusSnapshot, not adding a new message type.

/// Snapshot of backend state from `weft status --json`. Refreshed
/// on graph open, after every CLI verb, on SSE-triggered events
/// (debounced 500ms), on file-change debounce, and on the Refresh
/// button. Drives both the action bar (state + drift) AND graph
/// decorations (per-node infra status).
/// The BUILD-transition axis on the project row, orthogonal to
/// `projectStatus`. While not 'none', the only offered action is
/// cancel_build (the master transitional rule).
// SYNC: ProjectTransition <-> crates/weft-dispatcher/src/project_store.rs ProjectTransition, crates/weft-dispatcher/src/api/project.rs ProjectStatusResponse.transition, packages/weft-graph/src/status.ts VALID_TRANSITIONS
export type ProjectTransition = 'none' | 'building' | 'cancelling_build';

/// Trigger-deactivation spec the shared picker produces and the verbs
/// that take triggers down consume (deactivate; infra stop/terminate/
/// upgrade on an Active project). `wipe` forces `runningPolicy:
/// 'cancel'` (waiting before wiping is contradictory); `graceMinutes`
/// only applies to `hibernate`.
// SYNC: DeactivationSpec <-> crates/weft-broker-client/src/protocol.rs DeactivateSpec
export interface DeactivationSpec {
  mode: 'wipe' | 'hibernate' | 'park';
  runningPolicy: 'wait' | 'cancel';
  graceMinutes?: number;
  /// Cap in seconds on a `wait` drain: "wait at most N, then proceed"
  /// (the deactivation cancels the stragglers and lands; a worker
  /// replacement kills them with the old workers). One cap for the
  /// whole operation: it rides inside the DeactivateSpec wire object
  /// AND at the request top level for the verbs whose supervisor/
  /// worker drains read it there. Absent = the server default.
  drainTimeoutSecs?: number;
}

/// The server's default `wait` drain cap, mirrored so the picker can
/// prefill its input.
// SYNC: DEFAULT_DRAIN_TIMEOUT_SECS <-> crates/weft-broker-client/src/protocol.rs DEFAULT_DRAIN_TIMEOUT_SECS
export const DEFAULT_DRAIN_TIMEOUT_SECS = 600;

// SYNC: ActionAvailability <-> crates/weft-dispatcher/src/api/project.rs ProjectStatusResponse
export interface ActionAvailability {
  /// Verbs the dispatcher will currently accept.
  availableActions: ActionVerb[];
  /// Drift bits. Lit independently; each resolved by its own verb.
  /// - `binaryDrift`: worker binary inputs changed (engine, node
  ///   implementations, node-type set, `weft.toml` build section).
  ///   Resolved by Rebuild + a fresh worker spawn.
  /// - `definitionDrift`: runtime project shape changed (topology,
  ///   per-node configs). Resolved by Resync (a new project_definition
  ///   row + pointer advance; the running worker keeps the old
  ///   shape, the next execution picks up the new).
  /// - `infraDrift`: infra-closure changed. Resolved by Upgrade.
  binaryDrift: boolean;
  definitionDrift: boolean;
  infraDrift: boolean;
  /// Project lifecycle status: registered | activating | active |
  /// deactivating | inactive. Drives action-bar primary slot
  /// ("Activate" vs "Activating + Cancel" vs "Deactivate" vs
  /// "Cancel running / Resume" while deactivating).
  // SYNC: projectStatus <-> crates/weft-broker-client/src/protocol.rs ProjectStatus, crates/weft-dispatcher/src/api/project.rs ProjectStatusResponse.status
  projectStatus:
    | 'registered'
    | 'activating'
    | 'active'
    | 'deactivating'
    | 'inactive'
    | 'unknown';
  /// The build-transition axis. 'building'/'cancelling_build' render
  /// the unified transitional pattern (the launching button shows
  /// "Building... (cancel)") and gate every other verb.
  transition: ProjectTransition;
  /// Live infra rows exist whose node the current source no longer
  /// declares (the user deleted the node while it was deployed).
  /// Never gates run/activate; keeps the infra controls visible so
  /// the user never loses track of live (billed) infra.
  orphanedInfra: boolean;
  /// User-facing lifecycle label derived from accepting/visible/
  /// deadline. Possible values: "registered" | "active" |
  /// "deactivating" | "wipe" | "hibernate" | "park". Action bar
  /// renders this verbatim under the project name.
  mode: string;
  /// Unix-second deadline after which fires flip from parked to
  /// refused. Only set during hibernate's grace window.
  firesDeadlineUnix?: number;
  /// Count of running, non-suspended executions. Drives the
  /// deactivating-state UI: shows "draining N executions...".
  runningCount: number;
  /// Infra rollup.
  infraRollup:
    | 'none'
    | 'stopped'
    | 'partial'
    | 'running'
    | 'failed'
    | 'flaky'
    | 'stopping'
    | 'terminating'
    | 'provisioning';
  /// Per-node infra status. Used by graph decorations (badges
  /// under each infra node), independent of the rollup.
  infraNodes: Array<{
    nodeId: string;
    nodeType: string;
    /// Possible values:
    ///   "provisioning" | "running" | "stopped" | "flaky" | "failed"
    ///   | "stopping"   | "terminating"
    status: string;
    /// Set when status=failed: which stage of the apply pipeline
    /// failed (`provision` | `apply` | `execute` | `apply_lifecycle`).
    failureStage?: string;
    failureMessage?: string;
  }>;
  /// Counts of preserved state, for the reactivate-time dialog.
  preservation: {
    /// Resume signals with parked_payload set (queued submissions).
    parked: number;
    /// Resume signals registered but with no parked submission yet.
    suspended: number;
  };
}

/// Every action-bar verb. Mirrors the dispatcher's
/// `compute_available_actions` output union AND the CLI's
/// `ActionVerb` enum (snake_case). Some verbs come from the
/// dispatcher's `/status` (reactivate, resume_active) and some
/// from the CLI's progress stream (build, rm); the bar consumes
/// both, so the type is the superset.
// SYNC: ActionVerb <-> crates/weft-dispatcher/src/api/project.rs compute_available_actions, crates/weft-cli/src/progress.rs ActionVerb
export type ActionVerb =
  | 'run'
  | 'activate'
  | 'cancel_activate'
  | 'cancel_build'
  | 'reactivate'
  | 'deactivate'
  | 'cancel_running'
  | 'resume_active'
  | 'resync'
  | 'build'
  | 'rm'
  | 'infra_start'
  | 'infra_stop'
  | 'infra_terminate'
  | 'infra_upgrade'
  | 'infra_cancel'
  | 'infra_node_stop'
  | 'infra_node_terminate';

/// CLI progress phase. Matches the CLI's Phase enum. Closed set so
/// the reducer's match is exhaustive at the type level.
export type CliPhase =
  | 'build_start'
  | 'build_skip'
  | 'build_done'
  | 'image_push_start'
  | 'image_push_done'
  | 'dispatcher_call_start'
  | 'dispatcher_call_done'
  | 'infra_provision_start'
  | 'infra_provision_done'
  | 'trigger_register_start'
  | 'trigger_register_done'
  | 'complete'
  | 'error';

/// One NDJSON line emitted by the CLI in --json mode.
export interface CliEvent {
  ts_unix: number;
  verb: ActionVerb;
  phase: CliPhase;
  detail?: Record<string, unknown>;
}

/// Action-bar state. Three orthogonal concerns:
///
///   1. `backend`: at-rest facts from `weft status --json`. Always
///      present (defaults until the first fetch lands). Sections
///      that reflect backend state (infra rollup, trigger lifecycle)
///      read from here regardless of what the user is doing.
///
///   2. `overlay`: what user-action the bar is currently locked
///      into. `idle` when waiting for input, `cli_running` while a
///      CLI verb is in flight, `execution_running` when the user is
///      following a live execution, `pending` while an HTTP verb
///      (Stop) awaits backend confirmation. Exactly one slot owns
///      the spinner per overlay; sibling slots stay readable but
///      may disable their actions to avoid conflicts.
///
///   3. `error`: sticky banner shown above the bar. Independent of
///      both above. Cleared by user dismiss or by the next
///      successful idle push.
///
/// Transitions:
///   - status fetch result      -> backend updated, overlay unchanged
///   - SSE execution_started    -> overlay may flip to execution_running
///   - SSE execution_finished   -> overlay flips back to idle
///   - CLI start                -> overlay = cli_running
///   - CLI complete             -> overlay = idle
///   - CLI error                -> overlay = idle, error set
///   - user clicks Stop         -> overlay = pending, until SSE confirms
export type ActionBarState = {
  backend: BackendSnapshot;
  overlay: ActionBarOverlay;
  error?: ActionBarError;
};

/// Verbs that can carry an error to the action bar. Includes every
/// CLI verb the user can click PLUS the system-side error sources
/// (parse / catalog) that the graph view raises without any user
/// click. Surfaced as a wider union than `ActionVerb` so the modal
/// renders an honest headline ("Parse failed", "Catalog failed")
/// instead of pretending a CLI verb crashed when none did.
export type ErrorVerb = ActionVerb | 'parse' | 'catalog';

/// User-visible failure for the action bar. The banner shows `message`
/// (one-line); clicking the banner opens a modal that renders `details`
/// in full. Keep `details` optional so legacy paths that haven't been
/// migrated still produce a usable (if sparse) modal.
export interface ActionBarError {
  verb: ErrorVerb;
  message: string;
  details?: ActionErrorDetails;
}

export interface ActionErrorDetails {
  /// One-line description of what was being attempted. Plain English.
  /// "Running project 'foo'" / "Compiling main.weft" / "Applying edit".
  what: string;
  /// Stage where the failure happened. Drives the modal's icon and
  /// helps the user understand which subsystem reported the error.
  /// "compile" | "spawn" | "runtime" | "dispatch" | "edit" | "parse"
  /// | "catalog" | "cli" | "unknown"
  stage: string;
  /// Per-diagnostic items. A compile failure fans out into many; an
  /// exit-code failure produces one item with the stderr blob in raw.
  diagnostics: ActionErrorDiagnostic[];
  /// Free-form text the modal renders inside a collapsible `<pre>`.
  /// Stderr / stdout / log dump.
  raw?: string;
  /// Process exit code, when available.
  exitCode?: number;
  /// The shell command that was run, when available.
  /// "weft --json run --color foo".
  command?: string;
}

export interface ActionErrorDiagnostic {
  severity: 'error' | 'warning' | 'info';
  /// Diagnostic code like `loop-parallel-not-boolean`. Optional.
  code?: string;
  message: string;
  /// Optional source location. The modal renders "main.weft:12:5"
  /// and offers click-to-jump.
  location?: { file: string; line: number; column: number };
  /// Optional extended explanation shown below the message.
  hint?: string;
}

export type BackendSnapshot = {
  /// Verbs the dispatcher will currently accept. Inherited from
  /// the most recent status fetch; stale-but-known is preferable
  /// to blank during overlays so disabled-state derivation works.
  available: ActionVerb[];
  /// Project lifecycle status.
  status:
    | 'registered'
    | 'activating'
    | 'active'
    | 'deactivating'
    | 'inactive'
    | 'unknown';
  /// The build-transition axis (see `ActionAvailability.transition`).
  transition: ProjectTransition;
  /// Live orphaned infra exists (see `ActionAvailability.orphanedInfra`).
  orphanedInfra: boolean;
  /// Mode label: "active" | "wipe" | "hibernate" | "park" |
  /// "deactivating" | "registered". Rendered as a chip and used
  /// by the trigger slot to pick the Reactivate / Activate variant.
  mode: string;
  infraRollup:
    | 'none'
    | 'stopped'
    | 'partial'
    | 'running'
    | 'failed'
    | 'flaky'
    | 'stopping'
    | 'terminating'
    | 'provisioning';
  /// Drain progress when status='deactivating'.
  runningCount: number;
  /// Hibernate-grace deadline, when present.
  firesDeadlineUnix?: number;
};

export type ActionBarOverlay =
  | { kind: 'idle' }
  | { kind: 'cli_running'; verb: ActionVerb; phase: CliPhase; detail?: Record<string, unknown> }
  | { kind: 'execution_running'; color: string }
  | { kind: 'pending'; verb: ActionVerb; message: string };

export type HostMessage =
  | { kind: 'parseResult'; response: ParseResponse; source: string; layoutCode: string; freshMount?: boolean }
  | { kind: 'parseError'; error: string }
  /// Reply to `applyEdits` / `applyTextEdit`. Success carries the inverse text
  /// edit (the action's undo) and, NORMALLY, the post-edit truth (parse +
  /// source) so the webview advances its truth in one message with no second
  /// round-trip. `response`/`source` are ABSENT when the user switched `.weft`
  /// tabs mid-round-trip: the write landed on the right (now-background) doc,
  /// but the truth belongs to a graph the webview is no longer showing, so the
  /// webview resolves the inverse for undo bookkeeping WITHOUT advancing truth
  /// (the new doc's `parseResult` is its truth). Failure carries a user-
  /// readable `reason` for the rollback toast: the edit-server's message, or
  /// the `'code-was-edited'` sentinel when the doc changed under the edit.
  | { kind: 'editApplied'; requestId: number; ok: true; inverse?: TextEdit; response?: ParseResponse; source?: string }
  | { kind: 'editApplied'; requestId: number; ok: false; reason: string }
  /// Reply to `resyncSource`: the host's current truth, parsed fresh. Sent
  /// after a rejected edit so the webview can snap back to the authoritative
  /// state instead of mirroring server semantics locally. `ok:false` means
  /// the current source doesn't parse (the webview keeps its previous truth).
  | { kind: 'sourceResynced'; requestId: number; ok: true; response: ParseResponse; source: string }
  | { kind: 'sourceResynced'; requestId: number; ok: false; error: string }
  /// An EXTERNAL change landed on the watched `.weft` doc (user typing in the
  /// text tab, AI streaming edits): the webview engages its 1s auto-lock on
  /// source-mutating graph gestures. Re-posted on every keystroke; the lock
  /// deadline slides forward and expires on its own.
  | { kind: 'codeEditTouched' }
  /// Engage / release the explicit graph-logic lock (AI assistant integration;
  /// the webview also renders a banner with `reason` and a release button).
  | { kind: 'setGraphLogicLock'; locked: boolean; reason?: string }
  /// Resolved state of every `@file`-referenced file in the current view,
  /// keyed by the marker's relative path. Each entry is either the file's
  /// content or a read error (unreadable/missing). The webview displays a
  /// file-backed field from this (config holds only the `@file(...)` marker);
  /// a missing key means "still loading", an `error` entry fails loudly (no
  /// fallback to showing the marker as the value). Resent on backing-file
  /// change (file -> graph) so the display stays live without a reparse.
  | { kind: 'fileContents'; contents: Record<string, FileContent> }
  /// Navigation depth in the include back-stack. `depth > 0` means the user
  /// navigated into an included file; the webview shows a Return button.
  /// `fileName` is the current file's display name for the navigation bar.
  /// `execPrefix` is the dotted alias chain descended through (e.g. `c.` or
  /// `c.inner.`), prepended to node ids when looking up execution values so
  /// the journal's qualified keys match the sub-graph's bare node ids.
  | { kind: 'navState'; depth: number; fileName: string; execPrefix: string }
  | { kind: 'execTerminal'; color: string; state: 'completed' | 'failed' | 'cancelled' }
  | { kind: 'catalogAll'; catalog: Record<string, CatalogEntry> }
  /// The node catalog (full set, from `weft describe-nodes`) failed to
  /// load, or loaded with soft warnings. Distinct from `parseError`:
  /// the source may parse fine while the catalog is unavailable
  /// (weft not on PATH, a project error) or partial (a node mid-rename
  /// with bad metadata.json). Rendered as a non-blocking banner so it
  /// isn't erased by an unrelated successful parse. `error` set means
  /// the whole catalog is missing; `warnings` carries per-node soft
  /// failures when the catalog loaded but some nodes were skipped.
  | { kind: 'catalogError'; error?: string; warnings?: string[] }
  | { kind: 'execEvent'; event: NodeExecEvent }
  /// A non-terminal output-type mismatch on one firing: the node emitted a
  /// value whose type is incompatible with the port's declared (possibly
  /// narrowed) type, so the engine closed the port instead of forwarding
  /// the value. Attaches a warning to the matching execution row WITHOUT
  /// changing its state (the node did not fail). Kept separate from
  /// `execEvent`, which is purely a state transition.
  | { kind: 'execPortWarning'; nodeId: string; frames: LoopIteration[]; port: string; expected: string; actual: string }
  /// One bus event (live or replay). Carries only what the bus layer
  /// recorded: join / left / message / closed keyed by `busId`.
  /// Routing to node inspector panels is a SEPARATE signal,
  /// `busParticipant`, because participation is a property of the
  /// graph, not of the live bus stream.
  | { kind: 'busEvent'; event: BusInspectorEvent }
  /// One live-caller-connection event (live or replay). One caller per
  /// execution, so unlike `busEvent` there is no busId; the inspector
  /// renders a single "caller" panel for the run replaying what the
  /// program said to and heard from the caller.
  | { kind: 'callerEvent'; event: CallerInspectorEvent }
  /// One loop event (live or replay). Routes by groupId + parentFrames
  /// so the inspector card for each LoopOut node groups its
  /// iterations together.
  | { kind: 'loopEvent'; event: LoopInspectorEvent }
  /// "Node N participates in bus B." Derived dispatcher-side from
  /// PulseEmitted events whose value carries a bus marker. The
  /// webview unions these into a per-bus participant set; the
  /// inspector for each participant node renders the bus's IRC log.
  | { kind: 'busParticipant'; busId: string; nodeId: string; meta: BusMeta }
  /// One journal row the dispatcher could not apply during fold
  /// (replay-time corruption check). The inspector aggregates these
  /// into a muted "N journal rows corrupted" line; not alarming,
  /// not red, just visible if the user looks. Per-execution; the
  /// webview groups by execution color and renders the list
  /// behind a collapsed disclosure.
  | { kind: 'journalCorruption'; site: CorruptionSite; reason: string }
  /// Infra `/live` poll result for one infra node. Routed to
  /// the node's body panel iff the node has `requiresInfra: true`.
  | ({ kind: 'infraLive'; nodeId: string } & NodeFeedState)
  /// Listener `/display` poll result for one trigger node. Routed
  /// to the node's body panel iff `features.isTrigger: true`.
  | ({ kind: 'signalDisplay'; nodeId: string } & NodeFeedState)
  | { kind: 'followStatus'; status: FollowStatus }
  /// The live execution SSE stream ended or broke before the
  /// execution reached a terminal state. `reason` is 'closed' (server
  /// cleanly ended the stream) or 'error' (connection/read failure).
  /// The webview stops presenting the execution as live (so it isn't
  /// stuck showing "running" forever) WITHOUT falsely marking nodes
  /// completed: the per-node rows keep their last known state, the
  /// run is just no longer being followed. Distinct from execTerminal
  /// (which IS the run finishing) and from execReset (a fresh follow).
  | { kind: 'followLost'; color: string; reason: 'closed' | 'error' }
  | { kind: 'execReset' }
  /// Whether the watched .weft source is currently visible in
  /// some editor tab. The webview uses this to swap the "Source"
  /// button into an active/dark state when the source is on
  /// screen, so the user can see at a glance whether clicking it
  /// reveals an existing tab vs opens a new one.
  | { kind: 'sourceState'; open: boolean }
  /// Pushed from the host whenever the action-bar state machine
  /// transitions. The webview is a pure renderer that reads the
  /// latest state from this message. State transitions come from
  /// either status fetches (idle/execution_running) or live CLI
  /// events (cli_running/error/complete).
  | { kind: 'actionBarState'; state: ActionBarState }
  /// Latest `weft status --json` snapshot. Drives the action bar's
  /// drift indicators (Resync/Upgrade lights) and the graph's
  /// per-node infra badges. Stays current across cli_running so
  /// the bar can show "Resync available" while a different verb
  /// is in flight without flickering.
  | { kind: 'statusSnapshot'; snapshot: ActionAvailability }
  /// Reply to `resolveStoredFileUrl`. `url` present = the box's
  /// public URL (carrying a short-lived capability) the <img>/<video>
  /// streams directly from; `error` present = the file is
  /// expired/deleted or the handshake failed (preview shows fallback).
  | { kind: 'storedFileUrl'; requestId: number; url?: string; error?: string };

export interface FollowStatus {
  mode: 'latest' | 'pinned';
  color: string | undefined;
  pendingCount: number;
}

// ─── Messages: webview -> extension host ────────────────────────────────

export type WebviewMessage =
  | { kind: 'ready' }
  /// Apply a batch of structured edit intents to the source. The host runs
  /// them through the Rust edit-server (the single place that knows how to
  /// rewrite `.weft`), writes the resulting source to the document, and the
  /// normal parse round-trip re-renders the graph. The webview never edits
  /// `.weft` text itself; it only expresses intent. This is what makes the
  /// editor logic reusable across frontends (VS Code, Cursor, dashboard).
  /// `requestId` correlates the host's `editApplied` reply, which carries the
  /// inverse text edit the webview stores as this action's undo. The webview
  /// owns the undo stack (source + layout uniformly).
  | { kind: 'applyEdits'; ops: EditOp[]; requestId: number }
  /// Replay a raw source text edit (undo/redo of a source action). Same
  /// reply shape as `applyEdits` (the inverse undoes THIS replay).
  | { kind: 'applyTextEdit'; edit: TextEdit; requestId: number }
  /// Ask the host for its current truth (fresh parse of the open doc).
  /// Sent after a rejected edit; answered with `sourceResynced`.
  | { kind: 'resyncSource'; requestId: number }
  /// The user edited the ACTIVE file's `.weft` source DIRECTLY (a code-view text
  /// edit from a host's editable code panel). The host adopts `source` as the
  /// active file's new text, re-parses it, and answers with a NON-fresh
  /// `parseResult` so the editor adopts it as external truth (the canvas updates,
  /// pending ops re-apply) instead of rebuilding. The inverse of a graph gesture:
  /// graph edit writes the source, this is the source writing the graph. Mirrors
  /// the VS Code text-tab -> parse-server -> parseResult flow, where the code
  /// panel IS the text surface. `requestId` is unused (no reply correlation; the
  /// host's parseResult is the truth), kept absent.
  | { kind: 'editActiveSource'; source: string }
  | { kind: 'saveLayout'; layoutCode: string }
  /// Write-back for a file-backed config field (`@file("path", Type)`).
  /// The edit goes to the referenced file, not the `@file(...)` token in
  /// the source. `path` is project-root-relative.
  | { kind: 'saveFileRef'; path: string; content: string }
  /// Navigate into an `@include`d file (project-root-relative path). `alias`
  /// is the include node's id (the call-site name), accumulated into the
  /// execution-id prefix so journal values for the sub-graph (keyed
  /// `alias.node`) render when navigated in. The host opens that file's graph
  /// in the same panel and pushes the current view onto a back-stack.
  | { kind: 'openInclude'; path: string; alias: string }
  /// Pop the navigation back-stack (Return button), reopening the previous
  /// file's graph in the panel.
  | { kind: 'navigateBack' }
  | { kind: 'log'; level: 'info' | 'warn' | 'error'; message: string }
  | { kind: 'runProject' }
  | { kind: 'infraStart' }
  /// Project-level infra Stop / Terminate. `deactivation` is set iff
  /// the project is Active: the shared picker (in the webview) chose
  /// how triggers come down; the host forwards it verbatim. Absent
  /// when the project is not Active (nothing to deactivate).
  | { kind: 'infraStop'; deactivation?: DeactivationSpec }
  | { kind: 'infraTerminate'; deactivation?: DeactivationSpec }
  /// Cancel in-flight infra work (provisioning / stopping /
  /// terminating): HALT, per-node partial state stays visible.
  | { kind: 'infraCancel' }
  /// Cancel the in-flight server-side build (transition=building).
  | { kind: 'cancelBuild' }
  /// Per-node Stop (graph menu, partial-state recovery).
  | { kind: 'infraNodeStop'; nodeId: string }
  /// Per-node Terminate (graph menu, partial-state recovery).
  | { kind: 'infraNodeTerminate'; nodeId: string }
  | { kind: 'activateProject' }
  /// Deactivate with the shared picker's spec (mode + runningPolicy +
  /// grace). The picker lives in the SHARED webview so both hosts get
  /// the exact same UX; hosts just forward the spec.
  | { kind: 'deactivateProject'; spec: DeactivationSpec }
  /// User clicked Reactivate (project is Inactive WITH preserved
  /// state). Host opens the reactivate-choice dialog (3-option
  /// VS Code QuickPick) and POSTs `/activate` with the chosen
  /// `reactivateChoice`.
  | { kind: 'reactivateProject' }
  /// User clicked Cancel Running while the project is in the
  /// `deactivating` state. Host shells out to `weft cancel-running`,
  /// which POSTs the dispatcher's dedicated `/cancel-running`
  /// endpoint. That cancels every running, non-suspended execution;
  /// the lifecycle target the original deactivate wrote stays in
  /// place; the drain-watcher CASes status to inactive once the
  /// running set empties.
  | { kind: 'cancelRunning' }
  /// User clicked Cancel during status=Activating. Host shells out
  /// to `weft cancel-activate`, which POSTs the dispatcher's
  /// `/cancel-activate` endpoint. That cancels the TriggerSetup
  /// color, wipes every signal row registered so far, CAS-flips
  /// status Activating → Inactive.
  | { kind: 'cancelActivate' }
  /// User clicked Resume Active while in `deactivating`. Host POSTs
  /// `/activate` (no choice prompt: rolling back to live with no
  /// drain). The dispatcher's activate handler resets accepting/
  /// visible to active values and runs the drain pass against
  /// anything that parked during the transient.
  | { kind: 'resumeActive' }
  /// User clicked Resync. Deactivate + reactivate against the current
  /// source. `spec` is set iff the project is Active (the shared
  /// picker chose how triggers come down, exactly like Deactivate).
  | { kind: 'resyncProject'; spec?: DeactivationSpec }
  /// User clicked Upgrade Infra. atomic infra stop + image
  /// rebuild + start. `deactivation` set iff the project is Active
  /// (same shared-picker contract as infraStop).
  | { kind: 'infraUpgrade'; deactivation?: DeactivationSpec }
  /// User clicked the Refresh Status button on the graph header.
  /// Forces a `weft status --json` recheck without waiting for
  /// the file-change debounce. Useful after editing source
  /// outside the IDE or when the user wants to confirm state.
  | { kind: 'refreshStatus' }
  | { kind: 'followTogglePin' }
  | { kind: 'followCatchUp' }
  /// Replay a PAST execution onto the canvas: the host loads that execution's
  /// recorded events and feeds them as the editor's execution state (so the
  /// graph shows that run's final node statuses + outputs). `color` is the
  /// execution to replay, or `null` to drop the replay and return to live
  /// follow. A host that surfaces past executions another way (the VS Code
  /// extension has its own history) leaves this unhandled.
  | { kind: 'replayExecution'; color: string | null }
  /// User clicked the "open .weft source" button on the graph.
  /// Host opens the watched document in a side editor.
  | { kind: 'openSource' }
  /// User clicked the action bar's Stop / Cancel affordance. The
  /// host inspects the current ActionBarState to decide:
  ///   - cli_running       -> SIGTERM the spawned CLI process group.
  ///   - execution_running -> POST /executions/{color}/cancel.
  ///   - any other state   -> ignored (button shouldn't be shown).
  | { kind: 'stopAction' }
  /// User clicked a per-signal action button on a trigger node's
  /// inspector (e.g. "Regenerate API key"). The host POSTs to
  /// `/projects/{id}/signals/{node_id}/action` with this payload.
  /// Action `kind` strings are listener-defined per signal kind.
  /// `confirm`, when set, is the host's prompt text for a VS Code
  /// QuickPick; the action runs only on explicit confirmation.
  | { kind: 'signalAction'; nodeId: string; actionKind: string; payload?: unknown; confirm?: string }
  /// User dismissed the action-bar error banner. The host clears
  /// the slot's `error` field; the bar stops rendering the banner.
  /// Errors otherwise survive auto-refreshes so the user has time
  /// to read them.
  | { kind: 'dismissError' }
  /// User clicked Download on a stored-file value in the replay
  /// inspector. The host runs the brokered handshake (POST
  /// `/storage/files/download`; the dispatcher authenticates + asks
  /// the tenant's storage box to mint a short-lived capability) and
  /// opens the returned box URL externally: the BYTES stream
  /// browser<->box directly, never through the dispatcher. A 404
  /// surfaces as "expired or deleted" (the metadata in the value
  /// stays readable; the bytes are gone).
  | { kind: 'downloadStoredFile'; key: string }
  /// Inline image preview: the webview asks the host to run the
  /// brokered handshake and return the box's public URL (carrying a
  /// short-lived capability). The host replies with a correlated
  /// `storedFileUrl`; the <img> streams directly from the box (the
  /// CSP admits the storage origin).
  | { kind: 'resolveStoredFileUrl'; key: string; requestId: number };

// SYNC: StoredFileWire <-> crates/weft-core/src/storage/mod.rs StoredFile
/// The payload INSIDE a concrete stored-file marker: a logical key +
/// self-describing metadata, NO url (bytes are fetched via the
/// authenticated handshake above). url/data file values are the other
/// two handle forms and don't carry `key`.
export interface StoredFileWire {
  key: string;
  mimeType: string;
  sizeBytes: number;
  filename: string;
}

// SYNC: STORED_FILE_MARKERS <-> crates/weft-core/src/weft_type.rs FileKind::marker_key
/// The per-kind sentinel keys a stored-file value can carry. The marker
/// IS the value's concrete type; there is no `__weft_media__` umbrella.
const STORED_FILE_MARKERS = [
  '__weft_image__',
  '__weft_video__',
  '__weft_audio__',
  '__weft_blob__',
] as const;

// SYNC: parseStoredFile <-> crates/weft-core/src/storage/mod.rs StoredFile::from_value
/// The ONE place the webview parses a stored-file value, regardless of
/// which concrete marker (image/video/audio/blob) it carries. Returns
/// null for anything that is not a key-backed stored-file value. Every
/// consumer (inspector card, node preview) routes through here so the
/// shape is validated identically.
export function parseStoredFile(value: unknown): StoredFileWire | null {
  if (typeof value !== 'object' || value === null) return null;
  const obj = value as Record<string, unknown>;
  const marker = STORED_FILE_MARKERS.find((m) => m in obj);
  if (marker === undefined) return null;
  const payload = obj[marker];
  if (typeof payload !== 'object' || payload === null) return null;
  const p = payload as Record<string, unknown>;
  if (typeof p.key !== 'string' || typeof p.mimeType !== 'string') return null;
  return {
    key: p.key,
    mimeType: p.mimeType,
    sizeBytes: typeof p.sizeBytes === 'number' ? p.sizeBytes : 0,
    filename: typeof p.filename === 'string' ? p.filename : '',
  };
}

// SYNC: EditOp <-> crates/weft-compiler/src/edit.rs EditOp
/// A structured edit intent (serde tag `op`, camelCase fields).
/// The frontend emits these; the Rust edit-server applies
/// them to the source. All graph edits go through `applyEdits` so the language
/// logic lives in Rust only, reusable by any frontend.
export type EditOp =
  | { op: 'setConfig'; node: string; key: string; value: string }
  | { op: 'removeConfig'; node: string; key: string }
  | { op: 'setLabel'; node: string; label: string | null }
  | { op: 'addNode'; id: string; nodeType: string; parentGroup: string | null }
  | { op: 'removeNode'; node: string }
  | { op: 'addEdge'; source: string; sourcePort: string; target: string; targetPort: string; scopeGroup: string | null }
  | { op: 'removeEdge'; source: string; sourcePort: string; target: string; targetPort: string; scopeGroup: string | null }
  | { op: 'addGroup'; label: string; parentGroup: string | null }
  | { op: 'removeGroup'; group: string }
  | { op: 'renameGroup'; group: string; newLabel: string }
  | { op: 'moveNodeScope'; node: string; targetGroup: string | null }
  | { op: 'moveGroupScope'; group: string; targetGroup: string | null }
  | { op: 'updateNodePorts'; node: string; inputs: EditPortSig[]; outputs: EditPortSig[] }
  | { op: 'updateGroupPorts'; group: string; inputs: EditPortSig[]; outputs: EditPortSig[] }
  // A group's description is the plain `# ...` comment on its first body line
  // (the single description concept; the old single-file `# Project:` header is
  // dropped, a file's identity is its filename). `description: null` clears it.
  | { op: 'setGroupDescription'; group: string; description: string | null }
  // Loop ops mirror the Rust EditOp variants.
  | { op: 'addLoop'; label: string; parentGroup: string | null }
  | { op: 'removeLoop'; loopId: string }
  | { op: 'renameLoop'; loopId: string; newLabel: string }
  | { op: 'moveLoopScope'; loopId: string; targetGroup: string | null }
  | { op: 'updateLoopPorts'; loopId: string; inputs: EditPortSig[]; outputs: EditPortSig[] }
  | { op: 'setLoopConfig'; loopId: string; key: string; value: string }
  | { op: 'removeLoopConfig'; loopId: string; key: string };

export interface EditPortSig {
  name: string;
  required: boolean;
  portType?: string;
}

/// A minimal source text edit (mirrors the Rust `TextEdit`): replace the byte
/// range `[start, end)` with `text`. The reversible-action unit for source: an
/// applied edit yields its inverse edit, and the webview's undo stack stores
/// inverses (source) alongside layout-op inverses. Byte offsets so empty
/// replacements and trailing newlines are unambiguous.
export interface TextEdit {
  start: number;
  end: number;
  text: string;
}
