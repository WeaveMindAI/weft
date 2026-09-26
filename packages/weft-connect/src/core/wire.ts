// The connection vocabulary: a service's recipe as the connect surfaces
// read it, a stored connection as the store lists it, and whose credential
// a connection is. Shared by the editor's connection picker (through the
// graph protocol, which re-exports it) and a member's connect page.

/** The credentials of a service's OAuth app: a mandatory display
 *  label (the connection list's middle column), client id, secret
 *  (absent for a public PKCE client, and FORBIDDEN on project-declared
 *  apps: metadata is source), and any registration_fields extras. */
// SYNC: AppRegistration <-> crates/weft-core/src/access/spec.rs AppRegistration
export interface AppRegistration {
  label: string;
  client_id: string;
  client_secret?: string;
  /** registration_fields extras, flattened alongside id + secret. */
  [key: string]: string | undefined;
}

/** How grants coexist across projects (a provider property). */
// SYNC: GrantCoexistence <-> crates/weft-core/src/access/spec.rs GrantCoexistence
export type GrantCoexistence = 'coexisting' | 'exclusive';

/** One pasted credential field on the connect form. */
// SYNC: CredentialFieldWire <-> crates/weft-core/src/access/spec.rs CredentialField
export interface CredentialFieldWire {
  name: string;
  label?: string;
  /** The connect accepts this field empty (an app-level token only
   *  some uses of the service need); default false. */
  optional?: boolean;
  /** Render as a password field; default true. */
  secret?: boolean;
  placeholder?: string;
}

/** One connect door. */
// SYNC: Door <-> crates/weft-core/src/access/spec.rs Door
export type Door = 'shared' | 'own';

/** One entry of a service's permission catalogue. */
// SYNC: Permission <-> crates/weft-core/src/access/spec.rs Permission
export interface Permission {
  id: string;
  label: string;
  description: string;
  default?: boolean;
  /** This capability creates or reads things INSIDE the credential's
   *  own account, so a runtime-supplied (shared) credential can never
   *  serve it; the editor greys the shared option and resolution
   *  refuses it. */
  own_only?: boolean;
  /** The set-up tutorial for this capability, shown as its own
   *  foldable section on the "Your own" page. */
  guide?: { link?: string; steps: string[] };
}

// SYNC: VerificationRung <-> crates/weft-core/src/access/spec.rs VerificationRung
export type VerificationRung =
  | 'reports_permissions'
  | 'self_introspect'
  | 'reports_validity'
  | 'probe'
  | 'silent';

// SYNC: VerificationCost <-> crates/weft-core/src/access/spec.rs VerificationCost
export type VerificationCost = 'free' | 'ambiguous' | 'paid';

/** The optional parts of the "Your own" page beyond its paste fields
 *  (which derive from the acquisition; see `ownFields`). */
// SYNC: OwnPage <-> crates/weft-core/src/access/spec.rs OwnPage
export interface OwnPageWire {
  mint?: { url: string; payload: unknown; captures: unknown[] };
  guide?: { link?: string; steps: string[] };
  /** "I already have a credential": paste it, no app; the server
   *  stores a static-acquisition connection over these fields. */
  paste?: { fields: CredentialFieldWire[] };
}

/** The wire shape of an `AccessSpec` (the parts the editor reads;
 *  auth steps/test calls pass through opaquely to the store). */
// SYNC: AccessSpecWire <-> crates/weft-core/src/access/spec.rs AccessSpec
export interface AccessSpecWire {
  service: string;
  label?: string;
  /** The node runs without a connection picked (a possibly
   *  unauthenticated custom endpoint): no synthesized "no connection
   *  picked" rule, no pinned-open node. Default false: every access
   *  node requires a connection unless it says otherwise. */
  connection_optional?: boolean;
  grants?: GrantCoexistence;
  /** The connect doors this service offers; default ['own']. */
  doors?: Door[];
  own_page?: OwnPageWire;
  permissions?: Permission[];
  /** Where the provider's COMPLETE permission list lives, when
   *  `permissions` is a curated subset (Google class); drives the
   *  picker's "missing one? add it to this node's metadata" hint.
   *  Absent = the catalogue is the complete set, no hint. */
  all_permissions_url?: string;
  verification?: { rung?: VerificationRung; cost?: VerificationCost };
  acquisition: {
    // SYNC: kind <-> crates/weft-core/src/access/spec.rs Acquisition
    kind: 'static' | 'oauth2' | 'runtime' | 'mint_jwt';
    fields?: CredentialFieldWire[];
    registration_fields?: CredentialFieldWire[];
    grant?: { kind: 'authorization_code' | 'client_credentials'; [key: string]: unknown };
    [key: string]: unknown;
  };
  auth?: unknown[];
  test?: unknown;
  identity?: string;
  /** How the service REPORTS events, by named topic. The editor never
   *  reads inside; the blob rides to the server verbatim (the door
   *  probe records it so the events receiver can verify pushes). */
  events?: Record<string, unknown>;
  /** How a CALLER presenting a connection of this service is checked
   *  when a live route is gated by it (an auth access node's recipe).
   *  The editor never reads inside; the blob rides to the server
   *  verbatim and the broker runs it. */
  verify?: Record<string, unknown>;
  /** All-or-nothing groups of optional fields, at least one of which a
   *  connect must fill (a mailbox's receiving vs sending servers). The
   *  connect refuses a half-filled or empty choice, naming the fix. */
  // SYNC: Capability <-> crates/weft-core/src/access/spec.rs Capability
  capabilities?: { label: string; fields: string[] }[];
}

/** A connection row as the store lists it: everything the connection
 *  list renders, never a stored value. */
// SYNC: GrantSummary <-> crates/weft-core/src/access/wire.rs GrantSummary
export interface GrantSummary {
  id: string;
  service: string;
  project_id?: string | null;
  identity?: string | null;
  /** The list's middle column: the app's label, or the user's name for
   *  a pasted credential. */
  label?: string | null;
  scopes: string[];
  /** Whether `scopes` came from the provider (verified) or the user's
   *  ticks (claimed); only a verified shortfall marks a node. */
  permissions_verified: boolean;
  /** Whose credential the row resolves to; 'platform' rows spend credits. */
  owner: CredentialOwner;
  /** The member of `project_id` whose connection this is; absent for
   *  the author's. */
  member?: string;
  /** Which door created it; drives the shared-door one-time warning. */
  door: Door;
  expires_at?: string | null;
  /** The NAMES of the values this connection stores (never the values).
   *  What the live `requiresValues` check compares against. */
  value_names?: string[];
  /** Whether a credential stands behind the row right now: always for
   *  a stored credential; for a 'platform' row, whether the runtime holds
   *  the key it resolves to. False = picked, nothing behind it. */
  has_credential: boolean;
}

/// Whose credential a measured call spent: the runtime's own key, the
/// author's own connection, or a member's own connection.
// SYNC: CredentialOwner <-> crates/weft-core/src/access/mod.rs CredentialOwner
export type CredentialOwner = 'platform' | 'author' | { member: string };

/// The kind of a `CredentialOwner`, member id dropped: what a firing's
/// cost row says ("own key", "platform key", "member's key").
export type CredentialOwnerKind = 'platform' | 'author' | 'member';

export function credentialOwnerKind(owner: CredentialOwner): CredentialOwnerKind {
  return typeof owner === 'string' ? owner : 'member';
}

/** One registered app the shared door offers: its label and the FIXED
 *  permission set it covers. A person picks an option; they never tick
 *  permissions on the shared door. */
// SYNC: SharedAppChoice <-> crates/weft-core/src/access/wire.rs SharedAppChoice
export interface SharedAppChoice {
  label: string;
  covers: string[];
}

/** Which doors a connect page offers for a service right now: the
 *  shared-door options backed by something (a door with nothing behind
 *  it is hidden), the runtime-credential flag, and the consent facts. */
// SYNC: DoorsStatus <-> crates/weft-core/src/access/wire.rs DoorsStatus, crates/weft-core/src/access/wire.rs DoorsAnswer (its flattened core)
export interface DoorsStatus {
  shared_apps: SharedAppChoice[];
  shared_credential: boolean;
  redirect_uri: string | null;
  /** Why no browser consent can run (the provider only accepts https
   *  callbacks and this weft has none); paste connects still work. */
  consent_blocked?: string;
}

/** A connection a connect finished with. */
// SYNC: CompletedConnect <-> crates/weft-core/src/access/wire.rs CompletedConnect
export interface CompletedConnect {
  grant: GrantSummary;
}

/** A browser consent started: the page to open and the nonce to poll. */
// SYNC: StartedConsent <-> crates/weft-core/src/access/wire.rs StartedOAuth
export interface StartedConsent {
  consent_url: string;
  state: string;
}

/** A consent's outcome once it landed (`null` while still pending; the
 *  poll answers 410 once nothing is live under the state). */
// SYNC: ConsentOutcome <-> crates/weft-access-store/src/flows.rs complete_oauth (the parked result_json)
export type ConsentOutcome = { grant?: GrantSummary; error?: string } | null;

// The ways a `remote_select` field can be filled, in preference order.
// SYNC: ResourceSource <-> crates/weft-core/src/node.rs ResourceSource
export type ResourceSource =
  /** Options recorded on the connection during sign-in; free. */
  | { kind: 'granted'; from: string; label: string; value: string }
  /** Call the service and enumerate; needs `requires` on the
   *  connection, unless the lookup is `public` (credential-free, stands
   *  with no connection). */
  | ({ kind: 'list'; requires?: string[] } & Lookup)
  /** The provider's own chooser, declared entirely by the node, run on a
   *  weft-served page in the person's browser. Choosing GRANTS the
   *  picked resource. */
  | { kind: 'picker'; script: string; code: string; grants?: string[]; mime_types?: string[] }
  /** Paste a link; the pattern's first capture group is the id. */
  | { kind: 'from_url'; pattern: string };

// The declarative list request behind a `list` source.
// SYNC: Lookup <-> crates/weft-core/src/node.rs Lookup
export interface Lookup {
  /** GET URL; `{query}` interpolates the search text, `{<parent>}` a
   *  depends_on parent's picked id. */
  get: string;
  /** Dotted path to the items array in the response. */
  items: string;
  /** Dotted path (per item) for the display label. */
  label: string;
  /** Dotted path (per item) for the stored id. */
  value: string;
  page?: PageSpec;
  /** The endpoint is public: called with no credential. */
  public?: boolean;
}

// SYNC: PageSpec <-> crates/weft-core/src/node.rs PageSpec
export interface PageSpec {
  cursor_param: string;
  cursor_path: string;
}

/** One option a lookup offers: the id stored, the label shown. */
// SYNC: LookupItem <-> crates/weft-core/src/access/lookup.rs LookupItem
export interface LookupItem {
  id: string;
  label: string;
}

/** One page of a lookup, and where the next one starts. */
// SYNC: LookupPage <-> crates/weft-core/src/access/lookup.rs LookupPage
export interface LookupPage {
  items: LookupItem[];
  next_cursor: string | null;
}

/** A provider chooser's outcome once the person finished it (`null`
 *  while it is still open; the poll answers 410 once nothing is live
 *  under the state). */
// SYNC: PickerOutcome <-> crates/weft-dispatcher/src/api/access.rs picker_result (the parked shapes)
export type PickerOutcome = { picked?: LookupItem; cancelled?: boolean; error?: string } | null;

/** Whose connection signs a `remote_select` field. A `list` lookup runs
 *  server-side, so any connection signs it; a `picker` or `granted`
 *  source hands the connection to the person's own browser, so it only
 *  runs on their OWN connection. */
// SYNC: FieldConnection <-> crates/weft-core/src/member_door.rs FieldConnection
export type FieldConnection = 'none' | 'own' | 'shared';

/** One field a member fills, at one place of the program, as the member
 *  door lists it. */
// SYNC: MemberField <-> crates/weft-core/src/member_door.rs MemberField
export interface MemberField {
  /** The step, spelled the way the program writes it; with `field`, the
   *  key the member's value is stored under. */
  step: string;
  field: string;
  nodeType: string;
  label?: string;
  /** The input as its node declares it. */
  input: InputDefinition;
  /** What a member who gives nothing gets. */
  fallback?: unknown;
  /** Whether a run needs the member's value here. */
  needed: boolean;
  /** For a connection field, the service's recipe. */
  spec?: AccessSpecWire;
  /** For a `remote_select` field, the connection wired to it for this
   *  member and whose it is; `none` for any other field. */
  connection: FieldConnection;
  /** What the member gave (a connection reads as its handle). */
  value?: unknown;
}

/** A value to give one field. */
// SYNC: MemberValueInput <-> crates/weft-core/src/run_spec.rs MemberValueInput
export interface MemberValueInput {
  step: string;
  field: string;
  value: unknown;
}

/** One member-filled field, named: `field` of the step at `step`. */
// SYNC: MemberFieldRef <-> crates/weft-core/src/run_spec.rs MemberFieldRef
export interface MemberFieldRef {
  step: string;
  field: string;
}

/** `PUT /member/values`: values to give, and fields to clear, at once. */
// SYNC: ValuesRequest <-> crates/weft-dispatcher/src/api/member_door.rs ValuesRequest
export interface ValuesRequest {
  set: MemberValueInput[];
  clear: MemberFieldRef[];
}

/** `POST /member/lookup`: which list of which field, by the position of
 *  its source in the field's `sources`. */
// SYNC: MemberLookupRequest <-> crates/weft-dispatcher/src/api/member_door.rs LookupRequest
export interface MemberLookupRequest {
  step: string;
  field: string;
  source: number;
  query?: string;
  parents?: Record<string, string>;
  cursor?: string | null;
}

/** `POST /member/picker`: which field's chooser to open, by the position
 *  of its `picker` source. */
// SYNC: MemberPickerRequest <-> crates/weft-dispatcher/src/api/member_door.rs PickerRequest
export interface MemberPickerRequest {
  step: string;
  field: string;
  source: number;
}

/** What a change of a member's values did beyond storing: the member's
 *  triggers set up again because they read a changed value. */
// SYNC: ValuesChanged <-> crates/weft-core/src/member_door.rs ValuesChanged
export interface ValuesChanged {
  rearmed: string[];
}

/// One of the two things that can drive an input: a constant written in
/// the source (any spelling: braces, statement, `@file`, `@asset`), or a
/// value another node produces at run time (an edge, a dotted value in
/// the braces, an inline node).
// SYNC: AcceptedForm <-> crates/weft-core/src/node.rs AcceptedForm
export type AcceptedForm = 'literal' | 'wire';

/// Which drivers an input takes, as the list of accepted forms. Absent
/// on an input means both. A port never adds a form; it only removes
/// one, and the compiler resolves the list onto every instance input.
// SYNC: Accepts <-> crates/weft-core/src/node.rs Accepts
export type Accepts = AcceptedForm[];

/// A pure WIRE port on a node instance's output side or a group/loop
/// interface. Inputs are the richer `InputDefinition`.
// SYNC: PortDefinition <-> crates/weft-core/src/project.rs PortDefinition
export interface PortDefinition {
  name: string;
  portType: string;
  /// Whether the node waits for a value here. Inputs only: an output
  /// carries no optionality and is always `true` here.
  required: boolean;
  description?: string;
  /// True iff this port was auto-synthesized by the loop-lowering pass
  /// (the input side of a carry port). The editor renders it as a ghost
  /// mirror of the matching carry output. Never user-editable; the user
  /// changes the output's role to remove the synthesized input.
  synthesizedFromCarry?: boolean;
  /// The type the SOURCE header declares for this port; absent when the
  /// header does not declare it (a catalog port, a config-derived one,
  /// a synthesized one). The editor rewrites the header from THIS,
  /// never from `portType`: the rendered type may be an
  /// inference-resolved instantiation of a generic, which must not get
  /// frozen into source as if the author wrote it.
  // Declared once here and inherited by InputDefinition (Rust flattens
  // its PortDefinition into InputDefinition the same way).
  // SYNC: PortDefinition.declaredType <-> crates/weft-core/src/project.rs PortDefinition.declared_type
  declaredType?: string;
}

/// One INPUT on a node instance, enriched: accepted drivers resolved and
/// the editor surface (widget/default/label/placeholder) stamped by the
/// compiler, so the editor never re-derives any of it. The optional
/// members are only absent on a locally-added port that has not
/// round-tripped through a parse yet.
// SYNC: InputDefinition <-> crates/weft-core/src/project.rs InputDefinition
export interface InputDefinition extends PortDefinition {
  // SYNC: InputDefinition.accepts <-> crates/weft-core/src/project.rs InputDefinition.accepts
  accepts?: Accepts;
  // SYNC: InputDefinition.widget <-> crates/weft-core/src/project.rs InputDefinition.widget
  widget?: Widget;
  default?: unknown;
  label?: string;
  placeholder?: string;
  /// True when the input comes from the node type's own spec (a
  /// setting), absent for instance-added ports (custom header ports,
  /// form-derived ports).
  // SYNC: InputDefinition.fromSpec <-> crates/weft-core/src/project.rs InputDefinition.from_spec
  fromSpec?: boolean;
  /// The permissions THIS consumer needs on the wired connection
  /// (Access-typed inputs only). The editor's live check compares them
  /// against the picked connection's granted set; the runtime stamps
  /// them onto the marker for the resolve-time backstop.
  // SYNC: InputDefinition.requiresScopes <-> crates/weft-core/src/project.rs InputDefinition.requires_scopes
  requiresScopes?: string[];
  /// The stored VALUES this input needs on the wired connection
  /// (Access-typed inputs only), for a service whose optional fields
  /// decide what a connection can do. Same three check points; unlike
  /// permissions a shortfall is never "unknown", so it always marks.
  // SYNC: InputDefinition.requiresValues <-> crates/weft-core/src/project.rs InputDefinition.requires_values
  requiresValues?: string[];
}

// The editor control an input renders. Every key a widget object may
// carry, one per Rust variant payload. No index signature: the Rust
// side rejects unknown keys, so a key that is not listed here cannot
// survive a metadata load and must not typecheck.
// A DISCRIMINATED union mirroring the Rust tagged enum, one member per
// variant with only its own payload: an options-less select or a
// sources-less remote_select cannot typecheck (Rust already refuses
// them at metadata load), and adding a Rust variant without a member
// here breaks every exhaustive switch instead of shipping unhandled.
// SYNC: Widget <-> crates/weft-core/src/node.rs Widget
export type Widget =
  | { kind: 'text' }
  | { kind: 'textarea' }
  /// Syntax highlighting language ("python", "javascript", ...).
  | { kind: 'code'; language: string }
  /// `step` is the input's granularity (arrow/slider increment).
  | { kind: 'number'; min?: number | null; max?: number | null; step?: number | null }
  | { kind: 'checkbox' }
  /// A calendar-and-clock picker; the stored String is ISO-8601 with
  /// the picker's own zone offset.
  | { kind: 'datetime' }
  | { kind: 'select'; options: string[] }
  | { kind: 'multiselect'; options: string[] }
  | { kind: 'password' }
  /// The connection picker; `service` and `optional` are
  /// compiler-stamped from the node metadata's recipe
  /// (`service.service` / `service.connection_optional`). `optional` =
  /// the node runs without a connection, so the editor neither pins
  /// the unconnected node open nor gates the run on it.
  | { kind: 'access'; service?: string | null; optional?: boolean }
  /// Pick a resource on the connected service. `access` names this
  /// node's Access input; `sources` are the fill ways in preference
  /// order; `depends_on` are parent inputs for drill-down.
  | {
      kind: 'remote_select';
      access: string;
      sources: ResourceSource[];
      depends_on?: string[];
      /// The user may type a value the sources never listed (the
      /// fetched list is suggestions, not a closed set).
      free_text?: boolean;
    }
  /// Build the list of config entries a node's ports come from.
  | { kind: 'entry_list' }
  /// A list of short text values, added and removed one at a time.
  | { kind: 'text_list' }
  /// Editor file picker. `type` is the declared weft file type
  /// (Image/Audio/Video/Blob/File); `accept` optionally narrows the
  /// derived filter; `multiple` means the port holds several files, so
  /// the control keeps a list and writes one marker per file.
  // SYNC: Widget.type <-> crates/weft-core/src/node.rs Widget::FileDrop file_type
  | { kind: 'file_drop'; accept?: string | null; type: string; multiple?: boolean };
