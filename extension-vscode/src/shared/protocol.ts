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

/// The resolved state of a `@file` target: either its content, or a read
/// error. No third "fall back to the marker" state by design: a file-backed
/// field is always file-backed; if the file can't be read it fails loudly.
export type FileContent = { content: string } | { error: string };

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
    showDebugPreview?: boolean;
    isOutputDefault?: boolean;
    /// Names the endpoint serving the node's `/live` HTTP route the
    /// body panel polls. Unset for TCP-only infra (Postgres, Redis)
    /// so the panel doesn't show a broken eye.
    liveEndpoint?: string;
  };
  requiresInfra?: boolean;
  entry: unknown[];
  span?: Span;
  header_span?: Span;
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
    isTrigger?: boolean;
    showDebugPreview?: boolean;
    liveEndpoint?: string;
    hidden?: boolean;
  };
  /** Form-field vocabulary for nodes whose `features.hasFormSchema`
   *  is true. Empty/undefined for everything else. `weft describe-nodes`
   *  inlines this from each node's `form_field_specs.json` so the
   *  form_builder editor can drive the field-type dropdown without a
   *  separate fetch. */
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
export interface ActionAvailability {
  /// Verbs the dispatcher will currently accept.
  availableActions: ActionVerb[];
  /// Drift bits. Lit independently; resolved by Upgrade and Resync
  /// respectively.
  infraDrift: boolean;
  sourceDrift: boolean;
  /// Project lifecycle status: registered | activating | active |
  /// deactivating | inactive. Drives action-bar primary slot
  /// ("Activate" vs "Activating + Cancel" vs "Deactivate" vs
  /// "Cancel running / Resume" while deactivating).
  projectStatus:
    | 'registered'
    | 'activating'
    | 'active'
    | 'deactivating'
    | 'inactive'
    | 'unknown';
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
export type ActionVerb =
  | 'run'
  | 'activate'
  | 'cancel_activate'
  | 'reactivate'
  | 'deactivate'
  | 'cancel_running'
  | 'resume_active'
  | 'resync'
  | 'build'
  | 'rm'
  | 'infra_start'
  | 'infra_restart'
  | 'infra_stop'
  | 'infra_terminate'
  | 'infra_upgrade'
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
  error?: { verb: ActionVerb; message: string };
};

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
  /// Reply to `applyEdits` / `applyTextEdit`: `inverse` is the text edit that
  /// undoes what was just applied (the webview stores it on its undo stack).
  /// `ok:false` means the edit failed (the webview drops the pending action).
  | { kind: 'editApplied'; requestId: number; ok: boolean; inverse?: TextEdit }
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
  /// Infra `/live` poll result for one infra node. Routed to
  /// the node's body panel iff the node has `requiresInfra: true`.
  | ({ kind: 'infraLive'; nodeId: string } & NodeFeedState)
  /// Listener `/display` poll result for one trigger node. Routed
  /// to the node's body panel iff `features.isTrigger: true`.
  | ({ kind: 'signalDisplay'; nodeId: string } & NodeFeedState)
  | { kind: 'followStatus'; status: FollowStatus }
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
  | { kind: 'statusSnapshot'; snapshot: ActionAvailability };

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
  | { kind: 'infraRestart' }
  | { kind: 'infraStop' }
  | { kind: 'infraTerminate' }
  /// Per-node Stop (graph menu, partial-state recovery).
  | { kind: 'infraNodeStop'; nodeId: string }
  /// Per-node Terminate (graph menu, partial-state recovery).
  | { kind: 'infraNodeTerminate'; nodeId: string }
  | { kind: 'activateProject' }
  | { kind: 'deactivateProject' }
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
  /// User clicked Resync. Atomic deactivate + reactivate against
  /// the current source.
  | { kind: 'resyncProject' }
  /// User clicked Upgrade Infra. atomic infra stop + image
  /// rebuild + start.
  | { kind: 'infraUpgrade' }
  /// User clicked the Refresh Status button on the graph header.
  /// Forces a `weft status --json` recheck without waiting for
  /// the file-change debounce. Useful after editing source
  /// outside the IDE or when the user wants to confirm state.
  | { kind: 'refreshStatus' }
  | { kind: 'followTogglePin' }
  | { kind: 'followCatchUp' }
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
  | { kind: 'dismissError' };

/// A structured edit intent. Mirrors the Rust `EditOp` enum (serde tag `op`,
/// camelCase fields). The frontend emits these; the Rust edit-server applies
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
  | { op: 'renameGroup'; oldLabel: string; newLabel: string }
  | { op: 'moveNodeScope'; node: string; targetGroup: string | null }
  | { op: 'moveGroupScope'; group: string; targetGroup: string | null }
  | { op: 'updateNodePorts'; node: string; inputs: EditPortSig[]; outputs: EditPortSig[] }
  | { op: 'updateGroupPorts'; group: string; inputs: EditPortSig[]; outputs: EditPortSig[] }
  | { op: 'setProjectMeta'; name: string | null; description: string | null };

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
