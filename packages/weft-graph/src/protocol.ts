// Types shared between extension host and webview. Both import from
// this file so any change propagates.

export interface Span {
  startLine: number;
  startColumn: number;
  endLine: number;
  endColumn: number;
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

/// Source span of one config field plus how it was written. `origin` tells
/// the editor how to rewrite the field in place: an inline field
/// (`n = Type { k: v }`) becomes `k: v`; a connection-line field (`n.k = v`)
/// keeps its `n.k = ` prefix.
// SYNC: ConfigFieldSpan <-> crates/weft-core/src/project.rs ConfigFieldSpan
export interface ConfigFieldSpan {
  span: Span;
  origin: 'inline' | 'connection';
  /// The file the span's coordinates live in; absent = the compiled
  /// source. An interface-port fill written in an including file
  /// carries its own file, which may differ from its node's.
  sourceFile?: string;
}

/// A `@file("path", Type)` / `@asset("path", Type)` reference on a config
/// field. The marker is the edit contract:
///
/// `marker: 'file'` (bidirectional): `config[field]` holds the resolved text
/// content; the editor renders the field as file-backed and writes edits to
/// `path` instead of rewriting the marker token in the source.
///
/// `marker: 'asset'` (pull-only): nothing writes back. A file-typed ref
/// (`typeReferencesFile(type)`) defers to the build (the marker itself is
/// the field's value; the file-drop field sets/clears it); a text-typed one
/// resolved at parse and renders read-only.
/// SYNC: FileRef <-> crates/weft-core/src/project.rs FileRef, FileMarker
export interface FileRef {
  path: string;
  type: string;
  marker: 'file' | 'asset';
}

// SYNC: WeftPrimitive <-> crates/weft-core/src/weft_type.rs WeftPrimitive
export type WeftPrimitive =
  | 'String' | 'Number' | 'Boolean' | 'Null'
  | 'Image' | 'Video' | 'Audio' | 'Blob'
  | 'Empty';

/** All recognized primitive type names */
export const ALL_PRIMITIVE_TYPES: WeftPrimitive[] = [
  'String', 'Number', 'Boolean', 'Null',
  'Image', 'Video', 'Audio', 'Blob', 'Empty',
];

// SYNC: NAMED_UNIONS <-> crates/weft-core/src/weft_type.rs WeftType::UNION_ALIASES
/** Named union aliases. The ONE table both name resolution (parse) and
 *  rendering read, so an alias round-trips to its NAME instead of
 *  leaking the structural expansion (mirrors the backend registry).
 *  `Media` = media-proper (Image|Video|Audio); `File` = any stored file
 *  (Media + the Blob catch-all). A Map, not a plain object, so a name
 *  that happens to be an Object prototype key ('toString', ...) can
 *  never resolve through the prototype chain. */
export const NAMED_UNIONS: Map<string, WeftPrimitive[]> = new Map([
  ['Media', ['Image', 'Video', 'Audio']],
  ['File', ['Image', 'Video', 'Audio', 'Blob']],
]);

/** The primitive members of the `File` union: the single source of
 *  truth for "is this primitive a stored-file reference". */
export const FILE_PRIMITIVES: WeftPrimitive[] = (() => {
  const file = NAMED_UNIONS.get('File');
  if (!file) throw new Error('NAMED_UNIONS must declare File');
  return file;
})();


// ── Parsed type representation ──────────────────────────────────────────────

export type WeftType =
  | { kind: 'primitive'; value: WeftPrimitive }
  | { kind: 'list'; inner: WeftType }
  | { kind: 'dict'; key: WeftType; value: WeftType }
  | { kind: 'json_dict' }
  // A message-bus handle: an in-process channel between co-alive nodes.
  // A Bus output connects only to a Bus input; the payloads are not
  // type-checked. Wires only (a live runtime handle takes no literal).
  | { kind: 'bus' }
  // An access handle (a stored credential grant). Access connects only
  // to Access; the grant itself is opaque to the type system.
  // SYNC: access <-> crates/weft-core/src/weft_type.rs WeftType::Access
  | { kind: 'access' }
  // A typed, one-directional, terminating stream: `Generator[T]`. The
  // port accepts being emitted into repeatedly (each emission one item
  // of T); exactly one producer feeds exactly one consumer. Wires only.
  // SYNC: generator <-> crates/weft-core/src/weft_type.rs WeftType::Generator
  | { kind: 'generator'; inner: WeftType }
  | { kind: 'union'; types: WeftType[] }
  // Dict with KNOWN field names: `{ role: String, name?: String }`.
  // SYNC: record/named <-> crates/weft-core/src/weft_type.rs WeftType::Record/Named
  | { kind: 'record'; fields: { name: string; ty: WeftType; optional: boolean }[] }
  // A user-declared NOMINAL type: compatibility is by name. The backend
  // serializes it self-contained (`Name=Body`), so the parser here never
  // needs a registry; display renders the bare name.
  | { kind: 'named'; name: string; body: WeftType }
  | { kind: 'typevar'; name: string }
  | { kind: 'must_override' };

/** Type variable names users can write: T, T1, T2, ... T99.
 *
 *  Also accepted (catalog-internal only, not user-facing):
 *    - `T_Auto`: sentinel used by form-field port specs to request a
 *      per-port-instance TypeVar. Replaced with `T__{key}` at enrichment time.
 *    - `T__scope` (e.g. `T__hook`): materialized form of a `T_Auto` marker.
 *      Must parse because port types round-trip through strings in the frontend.
 *
 *  These internal forms exist so catalog authors can express "this port
 *  accepts anything, independently from sibling ports" without forcing the
 *  same rule on nodes that want shared `T` semantics (FirstInOrder, etc.). */
function isTypeVarName(s: string): boolean {
  if (!s) return false;
  if (s === 'T_Auto') return true;
  if (!s.startsWith('T')) return false;
  if (s.length === 1) return true;
  const rest = s.slice(1);
  if (/^\d+$/.test(rest)) return true;
  if (rest.startsWith('__')) {
    const scope = rest.slice(2);
    return scope.length > 0 && /^[A-Za-z0-9_]+$/.test(scope);
  }
  return false;
}

/** Split string on delimiter, but only at top level (not inside [] or {}) */
function splitTopLevel(s: string, delimiter: string): string[] {
  const parts: string[] = [];
  let depth = 0;
  let start = 0;
  for (let i = 0; i < s.length; i++) {
    if (s[i] === '[' || s[i] === '{' || s[i] === '(') depth++;
    else if (s[i] === ']' || s[i] === '}' || s[i] === ')') depth--;
    else if (s[i] === delimiter && depth === 0) {
      parts.push(s.slice(start, i));
      start = i + 1;
    }
  }
  parts.push(s.slice(start));
  return parts;
}

/** Index of the first top-level (outside []/{}) occurrence of `delimiter`. */
function findTopLevel(s: string, delimiter: string): number {
  let depth = 0;
  for (let i = 0; i < s.length; i++) {
    if (s[i] === '[' || s[i] === '{' || s[i] === '(') depth++;
    else if (s[i] === ']' || s[i] === '}' || s[i] === ')') depth--;
    else if (s[i] === delimiter && depth === 0) return i;
  }
  return -1;
}

/** Whether any LEAF of the type satisfies `leaf`. The one tree walk
 *  behind every "does this type still hold an X" question. */
function anyLeaf(t: WeftType, leaf: (kind: WeftType['kind']) => boolean): boolean {
  switch (t.kind) {
    case 'list':
    case 'generator':
      return anyLeaf(t.inner, leaf);
    case 'dict':
      return anyLeaf(t.key, leaf) || anyLeaf(t.value, leaf);
    case 'union':
      return t.types.some(u => anyLeaf(u, leaf));
    case 'record':
      return t.fields.some(f => anyLeaf(f.ty, leaf));
    case 'named':
      return anyLeaf(t.body, leaf);
    default:
      return leaf(t.kind);
  }
}

/** Any unresolved leaf (`typevar`, `must_override`) anywhere in the type. */
// SYNC: containsUnresolvedLeaf <-> crates/weft-core/src/weft_type.rs contains_unresolved_leaf
function containsUnresolvedLeaf(t: WeftType): boolean {
  return anyLeaf(t, kind => kind === 'typevar' || kind === 'must_override');
}

/** Whether a type still holds a TYPEVAR anywhere (`T`, `List[T]`,
 *  `Dict[String, T]`), meaning inference resolves it per instance and
 *  its spelling is not what a reparse renders. Deliberately EXCLUDES
 *  `must_override`, unlike `containsUnresolvedLeaf`: a wired
 *  MustOverride port is an error, never resolved, so its rendered type
 *  stays MustOverride and the spelling is honest. */
export function containsTypevar(t: WeftType): boolean {
  return anyLeaf(t, kind => kind === 'typevar');
}

function parseSingleType(s: string): WeftType | null {
  s = s.trim();
  // Parenthesized group: `(A | B)` is the type inside. Exists so a
  // named type's union body has an unambiguous wire form
  // (`Kind=(A | B)` vs the union `Kind=A | B`). Only strip when the
  // opening paren closes at the very end.
  // SYNC: paren group <-> crates/weft-core/src/weft_type.rs parse_single_type
  if (s.startsWith('(') && s.endsWith(')') && findTopLevel(s.slice(1, -1), ')') === -1) {
    return parseWeftType(s.slice(1, -1));
  }
  // Named union aliases (Media, File) resolve through the one table,
  // never a per-name branch.
  // A Map, not a plain object, so a name that happens to be an Object
  // prototype key ('toString', 'constructor', ...) can never resolve
  // through the prototype chain and crash the parse.
  const alias = NAMED_UNIONS.get(s);
  if (alias !== undefined) {
    return { kind: 'union', types: alias.map(t => ({ kind: 'primitive', value: t })) };
  }
  // Self-contained named form (`Name=Body`): the wire encoding of a
  // user-declared type. The body rides inline, so no registry here.
  const eq = findTopLevel(s, '=');
  if (eq !== -1) {
    const name = s.slice(0, eq).trim();
    // Same rule as the backend's check_declarable_name: an
    // uppercase-starting identifier that is not something the type
    // language already means (a primitive, a container keyword, a
    // special type, an alias, a TypeVar shape).
    // SYNC: named-name rule <-> crates/weft-core/src/weft_type.rs TypeRegistry::check_declarable_name
    if (!/^[A-Z][A-Za-z0-9_]*$/.test(name)) return null;
    if (
      (ALL_PRIMITIVE_TYPES as string[]).includes(name)
      || ['List', 'Dict', 'JsonDict', 'Bus', 'Access', 'Generator', 'MustOverride'].includes(name)
      || NAMED_UNIONS.has(name)
      || isTypeVarName(name)
    ) return null;
    const body = parseWeftType(s.slice(eq + 1));
    // A declared body must be CONCRETE, mirroring the backend's
    // wire-parser gate: a type variable under an alias would let
    // two same-named types carry different bodies, which name-only
    // nominal compatibility must never see.
    // SYNC: named-body concreteness <-> crates/weft-core/src/weft_type.rs parse_single_type (Name=Body arm)
    if (!body || containsUnresolvedLeaf(body)) return null;
    return { kind: 'named', name, body };
  }
  // Record: `{ field: Type, field?: Type }` (at least one field).
  if (s.startsWith('{') && s.endsWith('}')) {
    const inner = s.slice(1, -1);
    const fields: { name: string; ty: WeftType; optional: boolean }[] = [];
    for (const part of splitTopLevel(inner, ',')) {
      const p = part.trim();
      if (!p) return null;
      const colon = findTopLevel(p, ':');
      if (colon === -1) return null;
      let name = p.slice(0, colon).trim();
      const optional = name.endsWith('?');
      if (optional) name = name.slice(0, -1).trimEnd();
      if (!/^[A-Za-z0-9_]+$/.test(name) || fields.some(f => f.name === name)) return null;
      const ty = parseWeftType(p.slice(colon + 1));
      if (!ty) return null;
      fields.push({ name, ty, optional });
    }
    return fields.length > 0 ? { kind: 'record', fields } : null;
  }
  if (s === 'JsonDict') return { kind: 'json_dict' };
  if (s === 'Bus') return { kind: 'bus' };
  if (s === 'Access') return { kind: 'access' };
  if (s === 'MustOverride') return { kind: 'must_override' };

  // Parameterized: List[...], Dict[...]
  const bracketPos = s.indexOf('[');
  if (bracketPos !== -1) {
    if (!s.endsWith(']')) return null;
    const name = s.slice(0, bracketPos).trim();
    const inner = s.slice(bracketPos + 1, -1);

    if (name === 'List') {
      const innerType = parseWeftType(inner);
      return innerType ? { kind: 'list', inner: innerType } : null;
    }
    if (name === 'Generator') {
      const innerType = parseWeftType(inner);
      return innerType ? { kind: 'generator', inner: innerType } : null;
    }
    if (name === 'Dict') {
      const parts = splitTopLevel(inner, ',');
      if (parts.length !== 2) return null;
      const key = parseWeftType(parts[0].trim());
      const val = parseWeftType(parts[1].trim());
      return key && val ? { kind: 'dict', key, value: val } : null;
    }
    return null;
  }

  // Primitive
  if ((ALL_PRIMITIVE_TYPES as string[]).includes(s)) {
    return { kind: 'primitive', value: s as WeftPrimitive };
  }

  // Type variable
  if (isTypeVarName(s)) {
    return { kind: 'typevar', name: s };
  }

  return null;
}

/** Parse a port type string into a structured representation. */
// SYNC: parseWeftType <-> crates/weft-core/src/weft_type.rs WeftType::parse
export function parseWeftType(s: string): WeftType | null {
  const trimmed = s.trim();
  if (!trimmed) return null;

  // Split on top-level | for unions
  const parts = splitTopLevel(trimmed, '|');
  if (parts.length > 1) {
    const types: WeftType[] = [];
    for (const part of parts) {
      const parsed = parseSingleType(part.trim());
      if (!parsed) return null;
      types.push(parsed);
    }
    // Flatten nested unions, then normalize like the backend's
    // union builder: dedup by structural equality, drop `Empty`
    // whenever another member remains (`Number | Empty` IS
    // `Number`; the bottom type adds nothing), collapse a single
    // survivor.
    // SYNC: union normalization <-> crates/weft-core/src/weft_type.rs WeftType::union
    const flat: WeftType[] = [];
    for (const t of types) {
      if (t.kind === 'union') flat.push(...t.types);
      else flat.push(t);
    }
    const deduped: WeftType[] = [];
    for (const t of flat) {
      if (!deduped.some(u => weftTypesEqual(u, t))) deduped.push(t);
    }
    const nonEmpty = deduped.length > 1
      ? deduped.filter(t => !(t.kind === 'primitive' && t.value === 'Empty'))
      : deduped;
    return nonEmpty.length === 1 ? nonEmpty[0] : { kind: 'union', types: nonEmpty };
  }

  return parseSingleType(trimmed);
}


/** Structural equality, mirroring the backend's hand-written rules
 *  exactly: record fields AND union members compare as unordered sets,
 *  and two NAMED types are equal by name alone (name-only comparison
 *  is the deliberate nominal choice: same-named bodies are made to
 *  agree by the compiler's `named-type-conflict` rule, never by the
 *  parser, so a stored string can carry an out-of-date body). A
 *  rendered-string compare would reintroduce exactly what those rules
 *  ignore.
 *  SYNC: weftTypesEqual <-> crates/weft-core/src/weft_type.rs impl PartialEq for WeftType */
export function weftTypesEqual(a: WeftType, b: WeftType): boolean {
  if (a.kind !== b.kind) return false;
  switch (a.kind) {
    case 'primitive':
      return b.kind === 'primitive' && a.value === b.value;
    case 'list':
      return b.kind === 'list' && weftTypesEqual(a.inner, b.inner);
    case 'dict':
      return b.kind === 'dict' && weftTypesEqual(a.key, b.key) && weftTypesEqual(a.value, b.value);
    case 'union':
      return b.kind === 'union' && a.types.length === b.types.length
        && a.types.every(at => (b as Extract<WeftType, { kind: 'union' }>).types.some(bt => weftTypesEqual(at, bt)));
    case 'record':
      return b.kind === 'record' && a.fields.length === b.fields.length
        && a.fields.every(af => (b as Extract<WeftType, { kind: 'record' }>).fields.some(
          bf => bf.name === af.name && bf.optional === af.optional && weftTypesEqual(af.ty, bf.ty)));
    case 'generator':
      return b.kind === 'generator' && weftTypesEqual(a.inner, b.inner);
    case 'named':
      return b.kind === 'named' && a.name === (b as Extract<WeftType, { kind: 'named' }>).name;
    case 'typevar':
      return b.kind === 'typevar' && a.name === (b as Extract<WeftType, { kind: 'typevar' }>).name;
    default:
      // json_dict / bus / access / must_override carry no payload.
      return true;
  }
}


/// Does a ref's declared type make it a stored-file ref (its value is a
/// stored file, not text content)? Answered structurally over the
/// parsed type, so composite types (`List[Image]`, `Image | Null`)
/// count and non-value positions (a dict KEY, a generator element)
/// do not.
/// SYNC: typeReferencesFile <-> crates/weft-core/src/weft_type.rs references_file
export function typeReferencesFile(type: string): boolean {
  // STRUCTURAL, arm for arm with the Rust match (a token regex could
  // not say "a dict's KEY doesn't count" or "a generator's element
  // never counts"). A string that does not parse is not provably a
  // file ref, so it answers false, the same conservative answer the
  // Rust side gives every non-file arm.
  const parsed = parseWeftType(type);
  return parsed !== null && weftTypeReferencesFile(parsed);
}

function weftTypeReferencesFile(t: WeftType): boolean {
  switch (t.kind) {
    case 'primitive':
      return FILE_PRIMITIVES.includes(t.value);
    case 'list':
      return weftTypeReferencesFile(t.inner);
    // A dict's KEY is an index, never a stored value; only the value
    // side can make the type file-referencing (mirrors Rust).
    case 'dict':
      return weftTypeReferencesFile(t.value);
    case 'union':
      return t.types.some(weftTypeReferencesFile);
    case 'record':
      return t.fields.some((f) => weftTypeReferencesFile(f.ty));
    case 'named':
      return weftTypeReferencesFile(t.body);
    // A generator's port value is a live handle; its ITEMS may
    // reference files, but nothing about the port value itself is a
    // file to cast or pick (mirrors Rust's Generator arm).
    default:
      return false;
  }
}

/// The resolved state of a `@file` target: its content, a read error, or
/// `loading` while its bytes are still being fetched (lazy load: the editor opens
/// the graph before every asset has arrived). A file-backed field is always
/// file-backed; if the file can't be read it fails loudly (`error`). There is no
/// "fall back to the marker" state. A `loading` field renders a NON-INTERACTIVE
/// skeleton so it can't be edited before its real content lands (which would
/// clobber the file).
export type FileContent = { content: string } | { error: string } | { loading: true };

/** The inline per-firing display a node declares: which renderer
 *  (`media` plays the file by mime, `link` shows the file card), and
 *  which PORT it shows, named with its side (exactly one of
 *  `input`/`output`). */
// SYNC: DisplaySpecWire <-> crates/weft-core/src/node.rs DisplaySpec
export interface DisplaySpecWire {
  kind: 'media' | 'link';
  input?: string;
  output?: string;
}

// SYNC: NodeFeaturesWire <-> crates/weft-core/src/node.rs NodeFeatures
// This mirrors ONLY the features the editor reads. Backend-only
// features (cast ports, hidden filtering, ...) stay out: an unread
// field in a wire type advertises UI behavior that does not exist.
// The wire may still carry them (JS ignores unknown keys).
export interface NodeFeaturesWire {
  oneOfRequired?: string[][];
  canAddInputPorts?: boolean;
  canAddOutputPorts?: boolean;
  isTrigger?: boolean;
  showDebugPreview?: boolean;
  /// Names the endpoint serving the node's `/live` HTTP route the
  /// body panel polls. Unset for TCP-only infra (Postgres, Redis)
  /// so the panel doesn't show a broken eye.
  liveEndpoint?: string;
}

// SYNC: NodeDefinition (the editor-visible subset; backend-only fields like
// `images` and `publishedService` live only on the peer) <-> crates/weft-core/src/project.rs NodeDefinition
export interface NodeDefinition {
  id: string;
  nodeType: string;
  label: string | null;
  config: Record<string, unknown>;
  position: { x: number; y: number };
  scope: string[];
  groupBoundary: { groupId: string; role: 'In' | 'Out' } | null;
  inputs: InputDefinition[];
  outputs: PortDefinition[];
  features: NodeFeaturesWire;
  requiresInfra?: boolean;
  span?: Span;
  headerSpan?: Span;
  configSpans?: Record<string, ConfigFieldSpan>;
  /// Every constant written for an INPUT PORT, keyed by port name,
  /// whichever spelling wrote it (braces or statement, a `@file`/`@asset`
  /// marker included). The one home for a port's value; `config` keeps
  /// only what is not a port (the `_` keys, a loop's knobs). The paired
  /// spans carry each entry's source range + written form (`origin`:
  /// 'inline' = braces, 'connection' = statement), which is what the
  /// editor's form toggle rewrites.
  // SYNC: portLiterals/portLiteralSpans <-> crates/weft-core/src/project.rs NodeDefinition.port_literals/port_literal_spans
  portLiterals?: Record<string, unknown>;
  portLiteralSpans?: Record<string, ConfigFieldSpan>;
  fileRefs?: Record<string, FileRef>;
  /// Set on an opaque `@include` node: the included `.weft` file path. The
  /// editor renders this as an expandable group that navigates into the file.
  includePath?: string;
  /// The file this node was written in; absent = the compiled source.
  /// The editor never edits by span (every EditOp addresses by id/key,
  /// and the Rust engine resolves the decl in the buffer it is editing,
  /// failing loudly on an id it cannot find), so this is not consumed
  /// by the editor; the Problems panel routes diagnostics by it.
  sourceFile?: string;
}

// SYNC: Edge <-> crates/weft-core/src/project.rs Edge
export interface Edge {
  id: string;
  source: string;
  target: string;
  sourceHandle: string | null;
  targetHandle: string | null;
  /// The keys read off the source value before it lands
  /// (`w.seconds = x.profile.wpm` carries `["wpm"]`). Absent or empty
  /// for a plain wire. The graph draws such a wire dotted with the
  /// path at its target end.
  path?: string[];
  span?: Span;
  /// The file the span lives in; absent = the compiled source.
  sourceFile?: string;
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
  /// Always null today: no group-like decl carries a user label in the
  /// language; the editor derives the display name from the id's local
  /// segment. The slot exists so a future label syntax needs no wire
  /// change.
  label: string | null;
  inPorts: PortDefinition[];
  outPorts: PortDefinition[];
  parentGroupId: string | null;
  childGroupIds: string[];
  nodeIds: string[];
  span?: Span;
  headerSpan?: Span;
  /// The group's description: the plain `# ...` comment on the first body
  /// line of the group (text without the `# `).
  // SYNC: GroupDefinition <-> crates/weft-core/src/project.rs GroupDefinition
  description?: string | null;
  /// Literals written on the container's interface ports: `g.x = "hi"` from
  /// outside, or `_should_flow: false` in the braces. A wired value is an
  /// ordinary edge and never appears here.
  portLiterals?: Record<string, unknown>;
  /// Where each `portLiterals` entry was written, and in which form, so an
  /// edit rewrites the value where it already lives.
  portLiteralSpans?: Record<string, ConfigFieldSpan>;
  /// The file the group's spans live in; absent = the compiled source.
  sourceFile?: string;
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

// SYNC: Diagnostic <-> crates/weft-core/src/node.rs Diagnostic
export interface Diagnostic {
  // 1-based start line, 0-based start char column.
  line: number;
  column: number;
  // End of the culprit's range (1-based line, 0-based char column,
  // exclusive). Always present (Rust serializes 0 when the producer
  // only knew a point); endLine 0 renders as a 1-char caret.
  endLine: number;
  endColumn: number;
  severity: Severity;
  message: string;
  code?: string;
  // The file the coordinates live in, when it is NOT the source that
  // was compiled (an @include splices other files' nodes in with their
  // own line numbers). Absent = the compiled source itself.
  file?: string;
}

/// Derived from the Widget union below (never a second hand-kept list).
export type WidgetKind = Widget['kind'];

// One way a `remote_select` field can be filled, in preference order;
// the editor uses the richest source the chosen connection supports
// and silently drops each source whose requirement is not met.
// SYNC: ResourceSource <-> crates/weft-core/src/node.rs ResourceSource
export type ResourceSource =
  /// Options recorded on the connection during sign-in; free.
  | { kind: 'granted'; from: string; label: string; value: string }
  /// Call the service and enumerate; needs `requires` on the
  /// connection, unless the lookup is `public` (credential-free,
  /// stands with no connection; `public` + `requires` is refused at
  /// metadata load).
  | ({ kind: 'list'; requires?: string[] } & Lookup)
  /// The provider's own chooser, declared entirely by the NODE: the
  /// chooser script's address and the author's glue, run on a
  /// weft-served page (embedded, or a browser tab), never in the
  /// editor. Choosing GRANTS the picked resource.
  | { kind: 'picker'; script: string; code: string; grants?: string[]; mime_types?: string[] }
  /// Paste a link; the pattern's first capture group is the id.
  | { kind: 'from_url'; pattern: string };

// The declarative list request behind a `remote_select` list source.
// SYNC: Lookup <-> crates/weft-core/src/node.rs Lookup
export interface Lookup {
  /// GET URL; `{query}` interpolates the search text, `{<parent>}` a
  /// depends_on parent's picked id.
  get: string;
  /// Dotted path to the items array in the response.
  items: string;
  /// Dotted path (per item) for the display label.
  label: string;
  /// Dotted path (per item) for the stored id.
  value: string;
  page?: PageSpec;
  /// The endpoint is public: called with no credential, so the source
  /// works with no connection picked (and never signs even with one).
  public?: boolean;
}

// SYNC: PageSpec <-> crates/weft-core/src/node.rs PageSpec
export interface PageSpec {
  cursor_param: string;
  cursor_path: string;
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

// One declared INPUT of a node type, as authored in metadata.json and
// RESOLVED by the CLI before it ships (accepts + widget always filled
// with their effective values on this wire).
// SYNC: InputSpec <-> crates/weft-core/src/node.rs InputSpec
export interface InputSpec {
  name: string;
  type: string;
  required?: boolean;
  // SYNC: InputSpec.accepts <-> crates/weft-core/src/node.rs InputSpec.accepts
  accepts?: Accepts;
  widget?: Widget;
  default?: unknown;
  label?: string;
  placeholder?: string;
  description?: string;
  /// The permissions THIS consumer needs on the wired connection
  /// (Access-typed inputs only); checked live against the picked
  /// connection, at connect, and at resolve, never by the compiler.
  // SYNC: InputSpec.requiresScopes <-> crates/weft-core/src/node.rs InputSpec.requires_scopes
  requiresScopes?: string[];
  /// The stored VALUES this consumer needs on the wired connection
  /// (Access-typed inputs only); checked the same three places.
  // SYNC: InputSpec.requiresValues <-> crates/weft-core/src/node.rs InputSpec.requires_values
  requiresValues?: string[];
}

// SYNC: OutputSpec <-> crates/weft-core/src/node.rs OutputSpec
export interface OutputSpec {
  name: string;
  type: string;
  description?: string;
}

// SYNC: CatalogEntry/InputSpec/Widget/OutputSpec <-> crates/weft-core/src/node.rs
//       NodeMetadata (the `weft describe-nodes` serialization these mirror)
export interface CatalogEntry {
  type: string;
  label: string;
  description: string;
  tags: string[];
  icon?: string;
  color?: string;
  inputs: InputSpec[];
  outputs: OutputSpec[];
  requires_infra?: boolean;
  features?: NodeFeaturesWire;
  /** The node's declared inline per-firing display (a media player, a
   *  file card) and which port it shows. */
  // SYNC: display <-> crates/weft-core/src/node.rs NodeMetadata.display
  display?: DisplaySpecWire;
  /** Which config key this node's ports come from, and the entry kinds
   *  that key accepts. Undefined for a node whose ports are fixed. A
   *  metadata key, declared once in the package root's partial
   *  `metadata.json` and inherited by every member, so the
   *  entry-list editor can drive the kind dropdown without a
   *  separate fetch. */
  portsFromConfig?: PortsFromConfigWire;
  /** The service recipe, present ONLY on an access node. Drives the
   *  connection picker (doors, permission catalogue, own page). */
  // SYNC: AccessSpecWire <-> crates/weft-core/src/access/spec.rs AccessSpec
  service?: AccessSpecWire;
  /** The OAuth apps this project uses, keyed by service name. Declared
   *  once at a package root and inherited by every member node; an
   *  access node resolves its own app by its service name. Each value
   *  is the app's credentials. Empty in the shipped catalog. */
  // SYNC: accessApps <-> crates/weft-core/src/node.rs NodeMetadata.access_apps
  accessApps?: Record<string, AppRegistration>;
}

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
  /** Whose credential the row resolves to; 'ours' rows spend credits. */
  owner: CredentialOwner;
  /** Which door created it; drives the shared-door one-time warning. */
  door: Door;
  expires_at?: string | null;
  /** The NAMES of the values this connection stores (never the values).
   *  What the live `requiresValues` check compares against. */
  value_names?: string[];
}

/** The on-wire sentinel key tagging an Access value. */
// SYNC: ACCESS_MARKER_KEY <-> crates/weft-core/src/access/value.rs ACCESS_MARKER_KEY
export const ACCESS_MARKER_KEY = '__weft_access__';

/** Render hint for one form field. Opaque to the host; the
 *  consumer (browser extension, dashboard) reads `component` to
 *  pick a UI primitive. */
// SYNC: FormFieldRenderWire <-> crates/weft-core/src/node.rs FormFieldRender,
//       extension-browser/src/lib/api.ts FormFieldRender (a separate pnpm
//       workspace with no dependency between the two, so the shape is
//       restated there rather than imported)
export interface FormFieldRenderWire {
  component: string;
  source?: 'static' | 'input';
  multiple?: boolean;
  prefilled?: boolean;
}

/** Port template one entry kind adds. */
// SYNC: PortTemplateWire <-> crates/weft-core/src/node.rs PortTemplate
export interface PortTemplateWire {
  nameTemplate: string;
  portType: string;
}

/** Wire shape of one `PortSpec` (camelCase): how one kind of config
 *  entry contributes ports. The webview narrows this further via its
 *  own `PortSpec` interface in `lib/utils/port-specs`. */
/** What a [kind] asks the author to fill in, and what that value has to
 *  be: a plain weft type, or a shape measured against the input the
 *  entries are matched on. */
// SYNC: SpecFieldWire <-> crates/weft-core/src/node.rs SpecField, SpecShape
export type SpecFieldWire = {
  /** The entry key this value lives under (`options`, `value`, `min`). */
  key: string;
  label: string;
  /** An entry of this kind is incomplete without it. */
  required?: boolean;
  /** The control the editor draws, always filled by the time it ships. */
  widget: Widget;
} & (
  | { shape: 'typed'; valueType: string }
  | { shape: 'value' }
  | { shape: 'valueList' }
  | { shape: 'number' }
  | { shape: 'element' }
  | { shape: 'regex' }
);

// SYNC: PortSpecWire <-> crates/weft-core/src/node.rs PortSpec
export interface PortSpecWire {
  kind: string;
  /** The entry key holding the port name (`key` for a form field,
   *  `port` for a switch case). */
  keyField: string;
  label: string;
  /** Absent for a kind nothing renders (a switch case). */
  render?: FormFieldRenderWire;
  /** What this kind asks the author to fill in. A metadata author may
   *  write an entry as a bare name (`"placeholder"`); by the time it
   *  reaches the editor it is always the whole declaration. */
  fields?: SpecFieldWire[];
  /** This kind takes anything, so it is the branch reached when no
   *  earlier entry matched. At most one per list, and it goes last. */
  catchAll?: boolean;
  addsInputs: PortTemplateWire[];
  addsOutputs: PortTemplateWire[];
}

/** Where a node's ports come from when they come from its own config. */
// SYNC: PortsFromConfigWire <-> crates/weft-core/src/node.rs PortsFromConfig
// This mirrors only what the editor reads; the wire also carries
// `matchInput` (the compiler's value-check anchor), which no UI path
// consumes and so stays out, same rule as NodeFeaturesWire.
export interface PortsFromConfigWire {
  field: string;
  specs: PortSpecWire[];
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
/// Why a firing did not run. A DECISION (the author's `_should_flow` said no)
/// reads differently from a CONSEQUENCE (an input it needed never arrived), so
/// the journal carries which one it was.
// SYNC: SkipReason <-> crates/weft-core/src/exec/skip.rs SkipReason
export type SkipReason =
  | { kind: 'did_not_flow' }
  | { kind: 'flow_closed' }
  | { kind: 'required_input_closed'; port: string }
  | { kind: 'every_input_closed' }
  | { kind: 'one_of_group_closed'; ports: string[] }
  | { kind: 'scope_skipped'; scope: string };

/// Why an execution was cancelled: a person, a sibling run's
/// `ctx.stop_tagged` (naming the run and the tag), the live caller
/// dropping, or the runtime itself. Rides the `execution_cancelled`
/// event beside its text `reason`; absent only on a journal row written
/// before the field existed.
// SYNC: CancelCause <-> crates/weft-core/src/exec/cancel.rs CancelCause
export type CancelCause =
  | { kind: 'user' }
  | { kind: 'execution'; by: string; tag: string }
  | { kind: 'caller_gone' }
  | { kind: 'runtime'; detail: string };

/// The port every node carries, deciding whether it runs at all.
// SYNC: SHOULD_FLOW_PORT <-> crates/weft-core/src/exec/skip.rs SHOULD_FLOW_PORT,
// docs/src/language/syntax.md (reserved keys),
// packages/weft-syntax/weft.tmLanguage.json (reserved-key rule; see its README)
export const SHOULD_FLOW_PORT = '_should_flow';

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

/// Whose credential a measured call spent.
// SYNC: CredentialOwner <-> crates/weft-core/src/access/mod.rs CredentialOwner
export type CredentialOwner = 'their-own' | 'ours';

export interface NodeExecEvent {
  nodeId: string;
  state: NodeExecutionStatus;
  /// The journal's stamp for this transition (unix seconds), so a
  /// replay renders when the firing started and ended rather than
  /// when it was read.
  atUnix: number;
  /// This `running` transition is a RESUME of the same firing (a
  /// crash re-dispatch or a suspension waking), not a fresh attempt.
  /// Per-firing state accumulated before the resume (port warnings)
  /// belongs to this attempt and must survive it; only a fresh firing
  /// resets it.
  resumed?: boolean;
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
  /// Why the firing did not run, on a `skipped` event. The inspector
  /// tells a DECISION (`did_not_flow`) from a CONSEQUENCE (an input it
  /// needed closed) with it.
  skipReason?: SkipReason;
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
  // SYNC: LoopInspectorEvent 'instantiated' <-> crates/weft-dispatcher/src/events.rs LoopInstantiated, extension-vscode/src/execFollower.ts loop_instantiated
  | {
      kind: 'instantiated';
      groupId: string;
      parentFrames: LoopIteration[];
      /// Effective iteration cap; null for an uncapped loop (done- or
      /// stream-driven with no max_iters), whose count is unknowable
      /// up front.
      iterCap: number | null;
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
/// renders as `from: <payload-pretty>` (or a byte count for a binary
/// payload); `window` renders an ephemeral window's rollup ("N frames,
/// M bytes"); `closed` renders an explicit `* the bus closed here`
/// marker. `busId` groups lines by channel so a node attached to
/// multiple buses gets one scrollable section per bus. Replay orders
/// lines by arrival from the SSE stream (the dispatcher already orders
/// them by journal row id).
/// A journaled message's payload, in its declared kind: a JSON value,
/// or raw bytes carried as base64 (the wire form of a media frame; the
/// panel renders its size, not the base64). The ONE wire payload
/// vocabulary for every journaled exchange (bus window messages and the
/// live caller's events).
// SYNC: WirePayload <-> crates/weft-core/src/bus.rs WirePayload
export type WirePayload =
  | { kind: 'json'; data: unknown }
  | { kind: 'bytes'; data: string };

// SYNC: BusInspectorEvent 'message' <-> crates/weft-core/src/bus.rs WindowedBusMessage, extension-vscode/src/execFollower.ts DispatcherEvent 'bus_window' messages
// SYNC: BusInspectorEvent 'window' totals <-> crates/weft-core/src/bus.rs BusWindowTotal, extension-vscode/src/execFollower.ts DispatcherEvent 'bus_window' totals
export type BusInspectorEvent =
  | { kind: 'joined'; busId: string; offset: number; name: string; atUnix: number }
  | { kind: 'left'; busId: string; offset: number; name: string; atUnix: number }
  | {
      kind: 'message';
      busId: string;
      offset: number;
      from: string;
      msgKind: string;
      payload: WirePayload;
      payloadByteSize: number;
      atUnix: number;
    }
  /// One journal window of an EPHEMERAL bus: the payloads never reach
  /// the journal, so the panel renders the rollup ("N frames, M bytes
  /// from <sender>"). `offset` is the window's first offset (the
  /// dedup key the log store uses). Journaled buses never produce
  /// this: their windows unpack into per-message `message` events.
  | {
      kind: 'window';
      busId: string;
      offset: number;
      lastOffset: number;
      totals: Array<{ from: string; msgKind: string; count: number; bytes: number }>;
      atUnix: number;
    }
  | { kind: 'closed'; busId: string; offset: number; atUnix: number };

/// One live-caller-connection event the inspector replays. One caller
/// per execution (no busId; the execution color is the identity).
/// `payload` is the same tagged `WirePayload` shape a bus window's
/// messages carry.
// SYNC: CallerInspectorEvent 'inbound'/'outbound' <-> crates/weft-journal/src/events.rs CallerInbound/CallerOutbound, crates/weft-dispatcher/src/events.rs CallerInbound/CallerOutbound, extension-vscode/src/execFollower.ts DispatcherEvent 'caller_inbound'/'caller_outbound'
export type CallerInspectorEvent =
  | { kind: 'connected'; offset: number; protocol: string; atUnix: number }
  | {
      kind: 'inbound';
      offset: number;
      payload: WirePayload;
      payloadByteSize: number;
      atUnix: number;
    }
  | {
      kind: 'outbound';
      offset: number;
      payload: WirePayload;
      payloadByteSize: number;
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
  | 'NodeCancelled'
  | 'PulsesConsumed'
  | 'LoopStreamEnded'
  | 'UndecodableRow';

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
  /// posts a `signalAction` message; the host routes it to
  /// `/projects/{id}/infra/nodes/{node_id}/action` for an infra
  /// node (the container behind `/live` serves `/action`) and to
  /// `/projects/{id}/signals/{node_id}/action` for a trigger (the
  /// listener's kind impl). Whoever serves the action owns its
  /// payload schema. Generic so node authors can add buttons
  /// without changing the inspector.
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
  // The feed's SOURCE does not exist yet (infra not provisioned, the
  // signal not registered): a resting state with its own affordance,
  // distinct from a healthy-but-empty list AND from a failure.
  | { state: 'absent' }
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

/// CLI progress phase. Closed set so the reducer's match is
/// exhaustive at the type level.
// SYNC: CliPhase <-> crates/weft-cli/src/progress.rs Phase
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
  /// Periodic heartbeat while an infra verb waits on the supervisor
  /// (unbounded: draining executions). Detail carries `elapsedSeconds`.
  | 'infra_wait'
  | 'complete'
  | 'error';

/// What the action bar's working label can show: every CLI phase, plus
/// the extension-side `preflight` window (the saved-state check that
/// runs between the click and the CLI spawn). Extension-only: the CLI
/// never emits it, so it is NOT part of the CliPhase wire SYNC.
export type BarPhase = CliPhase | 'preflight';

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
/// in full. `details` is optional: an error without one still renders
/// a usable (if sparse) modal.
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
  /// | "catalog" | "cli" | "preflight" | "unknown"
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

/// A position in a source file: 1-based line, 0-based column.
export interface SourceLocation {
  file: string;
  line: number;
  column: number;
}

export interface ActionErrorDiagnostic {
  severity: 'error' | 'warning' | 'info';
  /// Diagnostic code like `loop-parallel-not-boolean`. Optional.
  code?: string;
  message: string;
  /// Optional source location. `file` is a full path; the modal
  /// renders its basename ("main.weft:12:5", full path on hover) and
  /// offers click-to-jump.
  location?: SourceLocation;
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
  | { kind: 'cli_running'; verb: ActionVerb; phase: BarPhase; detail?: Record<string, unknown> }
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
  // Ack for `saveLayout` (see its comment): the layout write reached disk.
  | { kind: 'layoutSaved'; requestId: number }
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
  /// The run reached a terminal. A cancel carries WHY (`reason` is the
  /// text, `cause` the structured value), so a run stopped by a sibling
  /// through `ctx.stop_tagged` reads as that, never as a bare failure.
  | { kind: 'execTerminal'; color: string; state: 'completed' | 'failed' | 'cancelled'; reason?: string; cause?: CancelCause; atUnix: number }
  /// The run tagged itself (`ctx.tag_execution`). `tags` is one call's
  /// list; the webview accumulates the run's set.
  | { kind: 'execTags'; color: string; tags: string[] }
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
  /// One metered call's cost record for a firing (a provider meter's
  /// figure). `costId` is the record's stable identity: the same record can
  /// arrive via both the replay and the live stream, so the reducer dedups
  /// on it. `amountUsd` null = the meter could not resolve the figure (an
  /// honest unknown; nothing is added to the row's total). `origin` says
  /// whose key the call spent.
  | { kind: 'execCost'; nodeId: string; frames: LoopIteration[]; costId: string; amountUsd: number | null; origin: CredentialOwner }
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
  /// Reply to `storageCall`, correlated by requestId. `result` is the
  /// dispatcher route's JSON response on success; `error` is the failure
  /// reason (HTTP error body or transport fault) on failure.
  | { kind: 'storageResult'; requestId: number; result?: unknown; error?: string }
  /// Reply to `accessCall`, correlated by requestId. `result` is the
  /// dispatcher route's JSON response on success; `error` the failure
  /// reason. Summaries only: no stored value ever rides this channel.
  | { kind: 'accessResult'; requestId: number; result?: unknown; error?: string }
  /// Reply to `pickAsset`: `paths` are the token paths the field writes
  /// into its `@asset("<path>", <Type>)` refs (a path in place locally,
  /// `assets/<name>` for stored bytes). Empty means the user cancelled;
  /// a single-file field takes the first. `error` on failure.
  | { kind: 'assetPicked'; requestId: number; paths?: string[]; error?: string }
  /// Reply to `listRuntimeFiles`: the project's STORED runtime files (its
  /// `project/` + `asset/` storage scopes), keys tenant-less (the short
  /// address a picked ref writes into source). `error` is the failure
  /// reason when the listing itself failed; an empty `files` with no
  /// `error` genuinely means the project has no stored files.
  | {
      kind: 'runtimeFiles';
      requestId: number;
      files: { key: string; filename: string; mimeType: string; sizeBytes: number }[];
      error?: string;
    };

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
  // `requestId` is acked by `layoutSaved` once the layout is durably on the
  // host's disk. The editor holds off adopting any layout echoed back by a
  // parse while a save is still un-acked: that echo was read from a disk
  // state the save had not reached yet, and adopting it would erase the
  // just-saved positions (a freshly created node snapping to the fallback
  // spot). EVERY host must reply, success only (a failed save is surfaced
  // host-side and deliberately left un-acked: the editor's copy is ahead of
  // disk, so continuing to refuse stale echoes is exactly right).
  | { kind: 'saveLayout'; layoutCode: string; requestId: number }
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
  /// Run the project. `targets` narrows the run to those output nodes'
  /// upstream subgraphs; absent or empty runs every output node, which
  /// is the ordinary case. Only output nodes may appear here: the
  /// dispatcher refuses anything else, so the graph must not offer a
  /// middle node as a target.
  | { kind: 'runProject'; targets?: string[] }
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
  /// Cancel the in-flight build (transition=building).
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
  /// User clicked the "open .weft source" button on the graph, or a
  /// diagnostic's file:line:column in the error details modal. Host
  /// opens the watched document in a side editor; with a `location`
  /// it opens THAT file and puts the cursor on the position.
  | { kind: 'openSource'; location?: SourceLocation }
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
  | { kind: 'downloadStoredFile'; key: string; filename?: string }
  /// Drive one storage-plane verb through the host: the host POSTs `body`
  /// as JSON to the dispatcher's `/storage/<path>` route and replies with a
  /// correlated `storageResult`. This is the ONE channel for storage control
  /// calls (the inline preview's download handshake); bytes never ride it.
  | { kind: 'storageCall'; requestId: number; path: string; body: unknown }
  /// Drive one access-store verb through the host: the host sends
  /// `method` to the dispatcher's `/access/<path>` route (stamping the
  /// active project into a POST body) and replies with a correlated
  /// `accessResult`. The ONE channel for connect flows, grant
  /// summaries, and remote_select lookups; secrets travel INTO it
  /// (pasted fields going editor -> store) and never back out.
  | {
      kind: 'accessCall';
      requestId: number;
      method: 'GET' | 'POST' | 'DELETE';
      path: string;
      body?: unknown;
    }
  /// Open a URL in the user's real browser (the OAuth consent page).
  | { kind: 'openExternalUrl'; url: string }
  /// The file-drop field asks the host to produce an ASSET REF path. With
  /// `dropped` (a drag-dropped file's bytes, base64), the host stores them
  /// as a project file under `assets/<name>` through its normal save path
  /// and replies with that path. Without, the host runs its own picker
  /// (e.g. a native file dialog whose result is referenced IN PLACE, no
  /// copy) and replies with the chosen path. The field then writes `@asset("<path>", <Type>)` into
  /// config; the pre-build asset sync does the rest.
  | {
      kind: 'pickAsset';
      requestId: number;
      accept?: string;
      /// The field takes several files, so the host's dialog lets the
      /// user choose several at once.
      multiple?: boolean;
      /// Files dropped onto the field, in drop order, as base64 bytes.
      dropped?: { name: string; bytesBase64: string }[];
    }
  /// The file-picker modal asks for the project's STORED runtime files
  /// (reply: `runtimeFiles`): what the storage plane holds for this project,
  /// listed through the same door `weft files` uses. Picking one writes
  /// `@asset("<scope-key>", <Type>)` into config.
  | { kind: 'listRuntimeFiles'; requestId: number };

// SYNC: FileValueWire <-> crates/weft-core/src/storage/mod.rs StoredFile
/// The payload INSIDE a concrete file marker: self-describing metadata
/// plus exactly ONE handle saying where the bytes live. A bucket-backed
/// file carries a `key` (bytes fetched via the authenticated handshake
/// above); a file pointing at an external resource carries a `url`
/// (rendered/fetched directly). `data` (inline base64) is deliberately
/// NOT a handle here: the read paths don't resolve it, so it falls
/// through to the raw-JSON rendering instead of a broken preview.
export type FileValueWire = {
  mimeType: string;
  sizeBytes: number;
  filename: string;
} & ({ key: string; url?: undefined } | { url: string; key?: undefined });

// SYNC: STORED_FILE_MARKER_TYPES <-> crates/weft-core/src/weft_type.rs FileKind
/// The per-kind sentinel key -> primitive type. The marker IS the
/// value's concrete type (no `__weft_media__` umbrella); the type is
/// read from the marker, never re-derived from the mime string. A Map
/// so a prototype key can never resolve, matching NAMED_UNIONS.
export const STORED_FILE_MARKER_TYPES: Map<string, WeftPrimitive> = new Map([
  ['__weft_image__', 'Image'],
  ['__weft_video__', 'Video'],
  ['__weft_audio__', 'Audio'],
  ['__weft_blob__', 'Blob'],
]);

// SYNC: parseFileValue <-> crates/weft-core/src/storage/mod.rs FileHandle::from_value
/// The ONE place the webview parses a file value, regardless of which
/// concrete marker (image/video/audio/blob) it carries. Returns the
/// metadata plus its handle (`key` for bucket-backed, `url` for an
/// external resource; `key` wins when both are somehow present, the
/// bucket copy is authoritative), or null for anything that is not a
/// resolvable file value (including data-backed markers, which no read
/// path resolves). Every consumer (inspector card, node preview)
/// routes through here so the shape is validated identically.
export function parseFileValue(value: unknown): FileValueWire | null {
  if (typeof value !== 'object' || value === null) return null;
  const obj = value as Record<string, unknown>;
  const marker = [...STORED_FILE_MARKER_TYPES.keys()].find((m) => m in obj);
  if (marker === undefined) return null;
  const payload = obj[marker];
  if (typeof payload !== 'object' || payload === null) return null;
  const p = payload as Record<string, unknown>;
  if (typeof p.mimeType !== 'string') return null;
  const meta = {
    mimeType: p.mimeType,
    sizeBytes: typeof p.sizeBytes === 'number' ? p.sizeBytes : 0,
    filename: typeof p.filename === 'string' ? p.filename : '',
  };
  if (typeof p.key === 'string' && p.key !== '') return { key: p.key, ...meta };
  if (typeof p.url === 'string' && p.url !== '') return { url: p.url, ...meta };
  return null;
}

// SYNC: EditOp <-> crates/weft-compiler/src/edit.rs EditOp
/// A structured edit intent (serde tag `op`, camelCase fields).
/// The frontend emits these; the Rust edit-server applies
/// them to the source. All graph edits go through `applyEdits` so the language
/// logic lives in Rust only, reusable by any frontend.
export type EditOp =
  // `form` targets the WRITTEN form when a name legally exists in both
  // (a wired-only port's literal next to a same-named config field):
  // 'inline' touches only the braces field, 'connection' only the
  // statement line. Absent auto-routes (prefers the statement).
  | { op: 'setConfig'; node: string; key: string; value: string; form?: 'inline' | 'connection' }
  | { op: 'removeConfig'; node: string; key: string; form?: 'inline' | 'connection' }
  | { op: 'setLabel'; node: string; label: string | null }
  | { op: 'addNode'; id: string; nodeType: string; parentGroup: string | null }
  | { op: 'removeNode'; node: string }
  | { op: 'addEdge'; source: string; sourcePort: string; target: string; targetPort: string; scopeGroup: string | null; path?: string[] }
  | { op: 'removeEdge'; source: string; sourcePort: string; target: string; targetPort: string; scopeGroup: string | null }
  | { op: 'addGroup'; label: string; parentGroup: string | null }
  | { op: 'removeGroup'; group: string }
  | { op: 'renameGroup'; group: string; newLabel: string }
  | { op: 'moveNodeScope'; node: string; targetGroup: string | null }
  | { op: 'moveGroupScope'; group: string; targetGroup: string | null }
  // A NODE's header declares only its CUSTOM/OVERRIDDEN ports (the catalog
  // provides the node type's own), so `inputs`/`outputs` carry that surface
  // and `removedInputs`/`removedOutputs` name the ports the gesture DELETED:
  // only those lose their wires (absence from the header proves nothing).
  // `revertedInputs`/`revertedOutputs` name ports the gesture returned to
  // their provided shape (the header line goes away, the port stays): the
  // server ignores them (its header rewrite already omits them), but the
  // optimistic projection needs them or it would keep showing the
  // pre-revert state until the reparse lands.
  // The removed lists are REQUIRED (empty when nothing was deleted): an
  // omitted list would read as "sweep nothing" and leave a deleted
  // port's wires in source, silently; Rust refuses a missing one too.
  // SYNC: EditOp updateNodePorts <-> crates/weft-compiler/src/edit.rs EditOp::UpdateNodePorts
  | { op: 'updateNodePorts'; node: string; inputs: EditPortSig[]; outputs: EditPortSig[]; removedInputs: string[]; removedOutputs: string[]; revertedInputs: RevertedPortSig[]; revertedOutputs: RevertedPortSig[] }
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
  // Rewrite which SOURCE FORM a body-set port value is written in
  // ('inline' = a `key: value` braces field, 'connection' = a
  // `node.key = value` statement). The form-toggle marker sends this.
  // SYNC: setValueForm.form <-> crates/weft-compiler/src/edit.rs ValueForm, packages/weft-graph/src/webview/lib/types/index.ts portValueForm.form
  | { op: 'setValueForm'; node: string; key: string; form: 'inline' | 'connection' }
  | { op: 'setLoopConfig'; loopId: string; key: string; value: string }
  | { op: 'removeLoopConfig'; loopId: string; key: string };

// SYNC: EditPortSig <-> crates/weft-compiler/src/edit.rs PortSig
// (`rendered` is projection-only and deliberately absent on the Rust side.)
export interface EditPortSig {
  name: string;
  required: boolean;
  /** The spelling the source header writes for this port (a node sig's
   *  declared type, a container sig's full rendered type). Required: a
   *  missing spelling would have the server write a placeholder over
   *  the author's type. */
  portType: string;
  /** The port's RENDERED type (inference-resolved), which can differ
   *  from `portType` when a header declares a generic that inference
   *  instantiates per wire. Only the optimistic projection reads it (it
   *  must keep showing the rendered type, exactly as a reparse would);
   *  the server ignores it. */
  rendered?: string;
}

/** A port a gesture returned to its provided shape (projection-only,
 *  never read by the server): the header line goes, the port stays.
 *  `portType` is the type the projection should now RENDER, present
 *  only when the producer knows it (a gesture revert carries the
 *  rendered value; a deletion revert omits it for a generic provided
 *  spelling, where the current rendered type is the better answer). */
export interface RevertedPortSig {
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
