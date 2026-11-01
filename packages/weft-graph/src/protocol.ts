// Types shared between extension host and webview. Both import from
// this file so any change propagates.

// The connect library by its path, never as `@weft/connect`: the VS Code
// extension host compiles this file with tsc into CommonJS and runs it in
// node, where the package's TypeScript source cannot be required. The path
// makes tsc emit the library beside it. The webview and the browser
// extension, which vite bundles, import the package by name.
import type { AccessSpecWire, AppRegistration, CredentialOwner } from '../../weft-connect/src/core/wire';

// The input vocabulary (ports, their drivers, the control an input draws)
// lives in the connect library, whose instance page draws a program's inputs
// too; the graph protocol names it from there.
export type { AcceptedForm, Accepts, CodeLanguage, MetadataExpr, MetadataPort, PortDefinition, InputDefinition, Span, Widget } from '../../weft-connect/src/core/wire';
import type { Accepts, MetadataPort, PortDefinition, InputDefinition, Span, Widget } from '../../weft-connect/src/core/wire';

/// Source span of one config field plus how it was written. `origin` tells
/// the editor how to rewrite the field in place: an inline field
/// (`n = Type { k: v }`) becomes `k: v`; a connection-line field (`n.k = v`)
/// keeps its `n.k = ` prefix.
// SYNC: ConfigFieldSpan <-> crates/weft-core/src/project.rs ConfigFieldSpan
export interface ConfigFieldSpan {
  span: Span;
  // SYNC: origin <-> crates/weft-core/src/project.rs ConfigOrigin
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
/// One line of the node catalog's warnings (`weft describe-nodes` and
/// `weft validate` JSON): a node or package left out, or a name a loaded
/// node spells off the weft way (that node still loads).
export interface CatalogWarning {
  kind: 'left_out' | 'spelling';
  message: string;
}

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

/** The type names that are not primitives but still mean something to
 *  the type language: no declared type may take one. */
// SYNC: CONTAINER_AND_SPECIAL_TYPES <-> crates/weft-core/src/weft_type.rs CONTAINER_AND_SPECIAL_TYPES
const CONTAINER_AND_SPECIAL_TYPES = ['List', 'Dict', 'JsonDict', 'Json', 'Bus', 'Access', 'Infra', 'Generator', 'Secret'];

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
  // `Dict[String, V]`, holding only the value type: a dict's keys are
  // always text, so the key is not part of the type (the spelling keeps
  // it, and the parser refuses any other key).
  | { kind: 'dict'; value: WeftType }
  | { kind: 'json_dict' }
  // Any value made only of JSON (text, numbers, booleans, null, and
  // lists or objects of those at any depth). Not an Any: a file, bus,
  // stream, access or infra never fits. Every JSON-made type flows in;
  // a Json value flows out only into Json (narrowing is the Cast node's).
  // SYNC: json <-> crates/weft-core/src/weft_type.rs WeftType::Json
  | { kind: 'json' }
  // A message-bus handle: an in-process channel between co-alive nodes.
  // A Bus output connects only to a Bus input; the payloads are not
  // type-checked. Wires only (a live runtime handle takes no literal).
  | { kind: 'bus' }
  // An access handle (a stored credential grant). Access connects only
  // to Access; the grant itself is opaque to the type system.
  // SYNC: access <-> crates/weft-core/src/weft_type.rs WeftType::Access
  | { kind: 'access' }
  // A handle on one endpoint of an infra node, the value an infra node
  // emits so other nodes reach it. Infra connects only to Infra. Wires
  // only (minted by an infra node, never typed as a literal).
  // SYNC: infra <-> crates/weft-core/src/weft_type.rs WeftType::Infra
  | { kind: 'infra' }
  // A typed, one-directional, terminating stream: `Generator[T]`. The
  // port accepts being emitted into repeatedly (each emission one item
  // of T); exactly one producer feeds exactly one consumer. Wires only.
  // SYNC: generator <-> crates/weft-core/src/weft_type.rs WeftType::Generator
  | { kind: 'generator'; inner: WeftType }
  // A value its run's record never holds: `Secret[T]`. What flows is a
  // marker naming a `T` kept sealed beside the record (see
  // `SECRET_MARKER_KEY`), so the inspector only ever sees the marker.
  // Strict both ways: a `Secret[T]` fits only a `Secret[U]` whose `T`
  // fits `U`, never a plain `T`, and nothing plain fits it. Wires only
  // (a node makes one at run time, never a literal or a Cast).
  // SYNC: WeftType 'secret' <-> crates/weft-core/src/weft_type.rs WeftType::Secret
  | { kind: 'secret'; inner: WeftType }
  | { kind: 'union'; types: WeftType[] }
  // Dict with KNOWN field names: `{ role: String, name?: String }`.
  // `open` (written `..` as the last item, `{ role: String, .. }`) takes
  // keys it does not declare; a closed record refuses them.
  // SYNC: record/named <-> crates/weft-core/src/weft_type.rs WeftType::Record/Named
  | { kind: 'record'; fields: RecordField[]; open: boolean }
  // A user-declared NOMINAL type: compatibility is by name. The backend
  // serializes it self-contained (`Name=Body`), so the parser here never
  // needs a registry; display renders the bare name.
  | { kind: 'named'; name: string; body: WeftType }
  // A port written with no type (`(x)`), or with one that does not
  // parse, holds its own variable `T__in__x` (`T__out__x` on an output):
  // what is wired decides it.
  | { kind: 'typevar'; name: string };

/** One declared field of a record type. */
export interface RecordField {
  name: string;
  ty: WeftType;
  optional: boolean;
}

/** Type variable names users can write: T, T1, T2, ... T99.
 *
 *  Also accepted (catalog-internal only, not user-facing):
 *    - `T_Auto`: sentinel used by form-field port specs to request a
 *      per-port-instance TypeVar. Replaced with `T__in__{key}` at enrichment time.
 *    - `T__scope` (e.g. `T__in__hook`): materialized form of a `T_Auto` marker,
 *      and the type of a header port written with no type (`portVar`).
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

/** The type variable of one port and no other: the type of a header
 *  port written with no type (`(photo)`), and of an input a key or a
 *  wire creates, taken from what is wired to it. An input's is
 *  `T__in__photo`, an output's `T__out__photo`: the side leads, so no
 *  two ports share one whatever their names. */
// SYNC: portVar <-> crates/weft-core/src/weft_type.rs WeftType::port_var
export function portVar(side: 'input' | 'output', port: string): string {
  return side === 'input' ? `T__in__${port}` : `T__out__${port}`;
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
/** Whether the bracket opening `s` is the one its last character
 *  closes (`(A | B)` yes, `(A) | (B)`, `(A)(B)` and a mismatched `(A]`
 *  no). */
// SYNC: openingBracketClosesLast <-> crates/weft-core/src/weft_type.rs opening_bracket_closes_last
function openingBracketClosesLast(s: string): boolean {
  const pairs: Record<string, string> = { ']': '[', '}': '{', ')': '(' };
  const open: string[] = [];
  for (let i = 0; i < s.length; i++) {
    const c = s[i];
    if (c === '[' || c === '{' || c === '(') open.push(c);
    else if (c in pairs) {
      if (open.pop() !== pairs[c]) return false;
      if (open.length === 0) return i === s.length - 1;
    }
  }
  return false;
}

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
    case 'secret':
      return anyLeaf(t.inner, leaf);
    case 'dict':
      return anyLeaf(t.value, leaf);
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

/** Whether a type still holds a type variable anywhere (`T`,
 *  `List[T]`, `Dict[String, T]`, a header port's own `T__in__x`), meaning
 *  inference resolves it per instance and its spelling is not what a
 *  reparse renders. */
// SYNC: containsUnresolvedLeaf <-> crates/weft-core/src/weft_type.rs contains_unresolved_leaf
export function containsUnresolvedLeaf(t: WeftType): boolean {
  return anyLeaf(t, kind => kind === 'typevar');
}

/** A type string that is not a type, carrying why (the backend's
 *  `try_parse` message). Thrown inside the parser only; the exported
 *  entry points turn it into a result or a null. */
class TypeParseError extends Error {}

function refuse(message: string): never {
  throw new TypeParseError(message);
}

/** Why `name` cannot name a declared type, or null when it can: an
 *  identifier starting with an uppercase letter that is not something
 *  the type language already means. */
// SYNC: declarableNameProblem <-> crates/weft-core/src/weft_type.rs TypeRegistry::check_declarable_name
function declarableNameProblem(name: string): string | null {
  if (!/^[A-Z][A-Za-z0-9_]*$/.test(name)) {
    return 'a type name starts with an uppercase letter and contains only letters, digits, and underscores';
  }
  const reserved = (ALL_PRIMITIVE_TYPES as string[]).includes(name)
    || CONTAINER_AND_SPECIAL_TYPES.includes(name)
    || NAMED_UNIONS.has(name)
    || isTypeVarName(name);
  return reserved ? `\`${name}\` is reserved by the type language` : null;
}

/** The declarable names one `tryParseWeftType` call met and could not
 *  resolve, in the order it met them. The editor holds no registry of
 *  the project's declared types, so such a name parses as a stand-in
 *  and the parse goes on: a definite refusal later in the string is
 *  still the one reported, and only a string whose every refusal is
 *  such a name is left to the compiler. Set for the length of one
 *  parse (the parser is synchronous and never re-entered). */
let unresolvedNames: string[] = [];

// SYNC: parseSingleType <-> crates/weft-core/src/weft_type.rs parse_single_type
function parseSingleType(s: string): WeftType {
  s = s.trim();
  if (!s) refuse('an empty type');
  // Parenthesized group: `(A | B)` is the type inside. Exists so a
  // named type's union body has an unambiguous wire form
  // (`Kind=(A | B)` vs the union `Kind=A | B`). Only strip when the
  // opening paren closes at the very end: `(A)(B)` is not one group.
  // SYNC: paren group <-> crates/weft-core/src/weft_type.rs parse_single_type
  if (s.startsWith('(') && openingBracketClosesLast(s)) {
    return parseTypeOrThrow(s.slice(1, -1));
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
    const why = declarableNameProblem(name);
    if (why !== null) refuse(`\`${s}\`: \`${name}\` is not a type name: ${why}`);
    const body = parseTypeOrThrow(s.slice(eq + 1));
    // A declared body must be CONCRETE, mirroring the backend's
    // wire-parser gate: a type variable under an alias would let
    // two same-named types carry different bodies, which name-only
    // nominal compatibility must never see.
    // SYNC: named-body concreteness <-> crates/weft-core/src/weft_type.rs parse_single_type (Name=Body arm)
    if (containsUnresolvedLeaf(body)) {
      refuse(`\`${name}\` is a declared type, so its body cannot hold a type variable`);
    }
    return { kind: 'named', name, body };
  }
  // Record: `{ field: Type, field?: Type }`, closed, or with `..` as its
  // last item, open. At least one field (neither `{}` nor `{ .. }` is a
  // type: `JsonDict` is "some object").
  if (s.startsWith('{') && s.endsWith('}')) {
    const inner = s.slice(1, -1);
    const noField = `\`${s}\` declares no field; \`JsonDict\` is the type for any object`;
    if (!inner.trim()) refuse(noField);
    const fields: RecordField[] = [];
    let open = false;
    const parts = splitTopLevel(inner, ',');
    parts.forEach((part, i) => {
      const p = part.trim();
      if (p === '..') {
        if (i !== parts.length - 1) refuse(`\`${s}\`: \`..\` opens a record, so it comes last`);
        open = true;
        return;
      }
      if (!p) refuse(`\`${s}\`: an empty field`);
      const colon = findTopLevel(p, ':');
      if (colon === -1) refuse(`\`${s}\`: the field \`${p}\` has no type (\`name: Type\`)`);
      let name = p.slice(0, colon).trim();
      const optional = name.endsWith('?');
      if (optional) name = name.slice(0, -1).trimEnd();
      if (!/^[A-Za-z0-9_]+$/.test(name)) refuse(`\`${s}\`: \`${name}\` is not a field name`);
      if (fields.some(f => f.name === name)) refuse(`\`${s}\`: the field \`${name}\` is declared twice`);
      fields.push({ name, ty: parseTypeOrThrow(p.slice(colon + 1)), optional });
    });
    if (fields.length === 0) refuse(noField);
    return { kind: 'record', fields, open };
  }
  if (s === 'JsonDict') return { kind: 'json_dict' };
  if (s === 'Json') return { kind: 'json' };
  if (s === 'Bus') return { kind: 'bus' };
  if (s === 'Access') return { kind: 'access' };
  if (s === 'Infra') return { kind: 'infra' };

  // Parameterized: List[T], Generator[T], Secret[T], Dict[String, V]
  const bracketPos = s.indexOf('[');
  if (bracketPos !== -1) {
    if (!s.endsWith(']')) refuse(`\`${s}\` opens a \`[\` it never closes at its end`);
    const name = s.slice(0, bracketPos).trim();
    const inner = s.slice(bracketPos + 1, -1);

    if (name === 'List') return { kind: 'list', inner: parseTypeOrThrow(inner) };
    if (name === 'Generator') return { kind: 'generator', inner: parseTypeOrThrow(inner) };
    if (name === 'Secret') return { kind: 'secret', inner: parseTypeOrThrow(inner) };
    if (name === 'Dict') {
      const parts = splitTopLevel(inner, ',');
      if (parts.length !== 2) refuse(`\`${s}\`: a dict takes two types, \`Dict[String, V]\``);
      const key = parseTypeOrThrow(parts[0].trim());
      const value = parseTypeOrThrow(parts[1].trim());
      // A JSON object's keys are text, so no value has any other key
      // type: `Dict[Number, String]` would compile and then match nothing.
      // SYNC: dict-key rule <-> crates/weft-core/src/weft_type.rs parse_single_type (Dict arm)
      if (!weftTypesEqual(key, { kind: 'primitive', value: 'String' })) {
        const k = weftTypeToString(key);
        refuse(
          `\`${s}\`: a dict's keys are always text, so its key type is \`String\`. `
          + `To key by a ${k}, use \`List[{key: ${k}, value: ${weftTypeToString(value)}}]\``,
        );
      }
      return { kind: 'dict', value };
    }
    refuse(`\`${name}[...]\` is not a type; the types that take one are List, Generator, Secret and Dict`);
  }

  // Primitive
  if ((ALL_PRIMITIVE_TYPES as string[]).includes(s)) {
    return { kind: 'primitive', value: s as WeftPrimitive };
  }

  // Type variable
  if (isTypeVarName(s)) {
    return { kind: 'typevar', name: s };
  }

  // A user-declared type name lands here too: the editor holds no
  // registry, so it cannot tell `Verdict` from a typo. A name no
  // project can declare is refused outright; a declarable one stands in
  // as a named type until the parse ends (see `unresolvedNames`).
  if (declarableNameProblem(s) !== null) refuse(`\`${s}\` is not a type`);
  unresolvedNames.push(s);
  return { kind: 'named', name: s, body: { kind: 'primitive', value: 'Empty' } };
}

/** The union of `types`, normalized like the backend's union builder:
 *  nested unions flattened, members deduped by structural equality,
 *  `Empty` dropped whenever another member remains (`Number | Empty` IS
 *  `Number`; the bottom type adds nothing), a single survivor collapsed. */
// SYNC: unionOf <-> crates/weft-core/src/weft_type.rs WeftType::union
export function unionOf(types: WeftType[]): WeftType {
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

function parseTypeOrThrow(s: string): WeftType {
  const trimmed = s.trim();
  if (!trimmed) refuse('an empty type');

  // Split on top-level | for unions
  const parts = splitTopLevel(trimmed, '|');
  if (parts.length > 1) {
    return unionOf(parts.map(part => parseSingleType(part.trim())));
  }

  return parseSingleType(trimmed);
}

/** Parse a port type string, or say why it is not one (the backend's
 *  message, so the editor shows what the compiler would). `unknownName`
 *  marks a refusal whose only cause is names this parser does not know
 *  but a project could declare (the editor holds no registry): the
 *  compiler is the one to settle it. Any other refusal in the string
 *  wins over such a name, whichever comes first. */
// SYNC: tryParseWeftType <-> crates/weft-core/src/weft_type.rs WeftType::try_parse
export function tryParseWeftType(
  s: string,
): { ok: true; type: WeftType } | { ok: false; error: string; unknownName: boolean } {
  unresolvedNames = [];
  try {
    const type = parseTypeOrThrow(s);
    if (unresolvedNames.length > 0) {
      return { ok: false, error: `\`${unresolvedNames[0]}\` is not a type`, unknownName: true };
    }
    return { ok: true, type };
  } catch (e) {
    if (e instanceof TypeParseError) return { ok: false, error: e.message, unknownName: false };
    throw e;
  } finally {
    unresolvedNames = [];
  }
}

/** Parse a port type string into a structured representation, null
 *  when it is not a type (`tryParseWeftType` says why). */
// SYNC: parseWeftType <-> crates/weft-core/src/weft_type.rs WeftType::parse
export function parseWeftType(s: string): WeftType | null {
  const parsed = tryParseWeftType(s);
  return parsed.ok ? parsed.type : null;
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
      return b.kind === 'dict' && weftTypesEqual(a.value, b.value);
    case 'union':
      return b.kind === 'union' && a.types.length === b.types.length
        && a.types.every(at => (b as Extract<WeftType, { kind: 'union' }>).types.some(bt => weftTypesEqual(at, bt)));
    case 'record':
      return b.kind === 'record' && a.open === b.open && a.fields.length === b.fields.length
        && a.fields.every(af => (b as Extract<WeftType, { kind: 'record' }>).fields.some(
          bf => bf.name === af.name && bf.optional === af.optional && weftTypesEqual(af.ty, bf.ty)));
    case 'generator':
      return b.kind === 'generator' && weftTypesEqual(a.inner, b.inner);
    case 'secret':
      return b.kind === 'secret' && weftTypesEqual(a.inner, b.inner);
    case 'named':
      return b.kind === 'named' && a.name === (b as Extract<WeftType, { kind: 'named' }>).name;
    case 'typevar':
      return b.kind === 'typevar' && a.name === (b as Extract<WeftType, { kind: 'typevar' }>).name;
    default:
      // json_dict / json / bus / access / infra carry no payload.
      return true;
  }
}


/** The alias NAME of a structural union, if its members are exactly one
 *  alias's primitive set (order-insensitive). The rendering half of the
 *  alias round-trip; the type itself stays structural. */
function unionAliasName(types: WeftType[]): string | null {
  const prims: WeftPrimitive[] = [];
  for (const t of types) {
    if (t.kind !== 'primitive') return null;
    prims.push(t.value);
  }
  for (const [name, members] of NAMED_UNIONS) {
    if (members.length === prims.length && members.every((m) => prims.includes(m))) {
      return name;
    }
  }
  return null;
}

/** The AUTHORED rendering of a parsed type: a named type prints its
 *  bare name (what humans read/write; edit ops re-resolve it against
 *  the project registry backend-side), and an alias's member set
 *  renders under its NAME (`File`/`Media` survive a parse -> string
 *  round trip), matching the backend. NOT re-parseable frontend-side
 *  when a named type is present; a string that will be parsed again
 *  must use `weftTypeToWireString`.
 *  SYNC: weftTypeToString/weftTypeToWireString <-> crates/weft-core/src/weft_type.rs WeftType::fmt_with, wire_string */
export function weftTypeToString(t: WeftType): string {
  return renderType(t, false);
}

/** The SELF-CONTAINED rendering: a named type prints `Name=Body`, so
 *  the result re-parses without any registry. Use for every string that
 *  flows back into `parseWeftType`. */
export function weftTypeToWireString(t: WeftType): string {
  return renderType(t, true);
}

function renderType(t: WeftType, wire: boolean): string {
  switch (t.kind) {
    case 'primitive': return t.value;
    case 'list': return `List[${renderType(t.inner, wire)}]`;
    case 'dict': return `Dict[String, ${renderType(t.value, wire)}]`;
    case 'json_dict': return 'JsonDict';
    case 'json': return 'Json';
    case 'bus': return 'Bus';
    case 'access': return 'Access';
    case 'infra': return 'Infra';
    case 'generator': return `Generator[${renderType(t.inner, wire)}]`;
    case 'secret': return `Secret[${renderType(t.inner, wire)}]`;
    case 'union': return unionAliasName(t.types) ?? t.types.map(m => renderType(m, wire)).join(' | ');
    case 'record': {
      const fields = t.fields.map(f => `${f.name}${f.optional ? '?' : ''}: ${renderType(f.ty, wire)}`);
      return `{${[...fields, ...(t.open ? ['..'] : [])].join(', ')}}`;
    }
    case 'named': {
      if (!wire) return t.name;
      // A union body is parenthesized so the rendered string is
      // unambiguous: `Kind=(A | B)` is one named type, `Kind=A | B`
      // re-parses as a union with a named member.
      const body = renderType(t.body, wire);
      return t.body.kind === 'union' ? `${t.name}=(${body})` : `${t.name}=${body}`;
    }
    case 'typevar': return t.name;
  }
}


/// Does a ref's declared type make it a stored-file ref (its value is a
/// stored file, not text content)? Answered structurally over the
/// parsed type, so composite types (`List[Image]`, `Image | Null`)
/// count and a generator element does not.
/// SYNC: typeReferencesFile <-> crates/weft-core/src/weft_type.rs references_file
export function typeReferencesFile(type: string): boolean {
  // STRUCTURAL, arm for arm with the Rust match (a token regex could
  // not say "a generator's element never counts"). A string that does not parse is not provably a
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
    // file to cast or pick (mirrors Rust's Generator arm). A secret's
    // port value is its marker, never the file it may hold, so it sits
    // here too.
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
  addedPortsAreDataKeys?: boolean;
  /// Each added input comes with a same-named output the compiler makes,
  /// so the node offers no output adder of its own: a person adds the
  /// input and the output follows.
  pairsAddedPorts?: boolean;
  isTrigger?: boolean;
  showDebugPreview?: boolean;
  /// Names the endpoint serving the node's `/live` HTTP route the
  /// body panel polls. Unset for a node that speaks only TCP
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
  /// The bounds on this node's type variables: its node type's
  /// `typeParams` and the ones the program wrote after the type name
  /// (`ExecPython[T: Media](...)`, or on the group a boundary halves),
  /// each with what it resolved to once the compiler ran.
  // SYNC: typeParams <-> crates/weft-core/src/project.rs NodeDefinition.type_params
  typeParams?: TypeParamWire[];
  requiresInfra?: boolean;
  /// Set on a node that exists once per instance of the program at every
  /// place it runs.
  // SYNC: perInstance <-> crates/weft-core/src/project.rs NodeDefinition.per_instance
  perInstance?: PerInstance;
  /// The call paths (outermost site first) where a node inside an
  /// included file reads an instance's value, when its places disagree;
  /// absent when `perInstance` holds at every place.
  // SYNC: perInstancePaths <-> crates/weft-core/src/project.rs NodeDefinition.per_instance_paths
  perInstancePaths?: string[][];
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
  /// The node's metadata ports: its node type's, or on an opaque
  /// `@include` node, the file's group's. Never drawn on the canvas.
  // SYNC: NodeDefinition.metadataPorts <-> crates/weft-core/src/project.rs NodeDefinition.metadata_ports
  metadataPorts?: MetadataPort[];
  /// Set on an opaque `@include` node: the included `.weft` file path. The
  /// editor renders this as an expandable group that navigates into the file.
  includePath?: string;
  /// Set on an opaque `@include` node: what the file behind it holds,
  /// reached through its own includes too. The body is not in the graph
  /// in an interface parse, so this is the only way to tell that a
  /// project's only trigger (or only infra node) is inside an include.
  // SYNC: includeContents <-> crates/weft-core/src/project.rs NodeDefinition.include_contents
  includeContents?: IncludedContents;
  /// Set only on a node inside an included file compiled for a build,
  /// whose types differ per place the file is included at: one entry per
  /// place. The editor's interface parse never produces it; carried as is.
  // SYNC: NodeDefinition.placeTypes <-> crates/weft-core/src/project.rs NodeDefinition.place_types
  placeTypes?: PlaceTypes[];
  /// The file this node was written in; absent = the compiled source.
  /// The editor never edits by span (every EditOp addresses by id/key,
  /// and the Rust engine resolves the decl in the buffer it is editing,
  /// failing loudly on an id it cannot find), so this is not consumed
  /// by the editor; the Problems panel routes diagnostics by it.
  sourceFile?: string;
}

/// The types one node holds at one place its included file is included
/// at: `place` names that place, and each port's type and each type
/// variable's resolution are wire type strings.
// SYNC: PlaceTypes <-> crates/weft-core/src/project.rs PlaceTypes
export interface PlaceTypes {
  place: string[];
  inputs: Record<string, string>;
  outputs: Record<string, string>;
  typeParams: TypeParamWire[];
}

/// What an opaque `@include` node stands for: the roles its file plays,
/// and every file reached to find out.
///
/// Kept apart from the node's own `requiresInfra` / `features.isTrigger`
/// because those two drive real per-node work (an infra node gets a
/// provisioned slot and a `/live` poller, a trigger gets a mount URL and
/// a display poller), and the include alias is not the node that does
/// any of it. It only contains them.
// SYNC: IncludedContents <-> crates/weft-core/src/project.rs IncludedContents
export interface IncludedContents {
  /// Some node inside requires infrastructure.
  requiresInfra?: boolean;
  /// Some node inside is a trigger, so the project can be activated.
  hasTrigger?: boolean;
  /// Every `.weft` file reached through this include, nested ones
  /// included, each relative to the PROJECT ROOT. Root-relative and not
  /// as written, because an `@include` path is relative to the file that
  /// wrote it, so a nested one resolved against the top file's directory
  /// would point at nothing. Watch these so editing a deeply included
  /// file re-parses the graph.
  files?: string[];
  /// The node types found inside. The parser has no catalog, so it
  /// records the types and the compiler's enrich pass settles the two
  /// booleans above from them.
  nodeTypes?: string[];
}

/// One wire, end to end: from the node that makes a value to the node
/// that reads it, however many group ports it passes (`via`). Either end
/// can be a group: a wire landing on a group's `_should_flow` /
/// `_should_not_flow` is its gate; a wire whose source is a group starts
/// at one of that group's own ports (`via[0]` is its crossing: a literal
/// or nothing, sent when the group opens); a wire whose target is a group
/// on any other port stops at that group's own port (the last crossing),
/// which nobody on the far side reads. The editor draws each leg between
/// two stops (`host-bridge.ts legsOf`).
// SYNC: Edge <-> crates/weft-core/src/project.rs Edge
export interface Edge {
  id: string;
  source: string;
  target: string;
  sourceHandle: string | null;
  targetHandle: string | null;
  /// The keys read off the source value before it lands
  /// (`w.seconds = x.profile.wpm` carries `["wpm"]`), every leg's in
  /// order. Absent for a plain wire. The graph draws such a wire dotted
  /// with the path at its target end.
  path?: string[];
  /// The group ports the wire passes, source to target. Absent for a
  /// wire between two nodes of one scope.
  via?: Crossing[];
  /// The last leg's line.
  span?: Span;
  /// The file the span lives in; absent = the compiled source.
  sourceFile?: string;
}

/// One group port a wire passes. `path`, `span` and `sourceFile` belong
/// to the leg that ENDS at this port (the line `g.x = a.out` for the leg
/// into `g.x`).
// SYNC: Crossing <-> crates/weft-core/src/project.rs Crossing
export interface Crossing {
  group: string;
  port: string;
  side: CrossingSide;
  path?: string[];
  span?: Span;
  sourceFile?: string;
}

/// Into a group through one of its inputs, or out through one of its
/// outputs.
// SYNC: CrossingSide <-> crates/weft-core/src/project.rs CrossingSide
export type CrossingSide = 'in' | 'out';

// SYNC: GroupDefinition <-> crates/weft-core/src/project.rs GroupDefinition
// SYNC: GroupDefinition (kind + loopConfig) <-> crates/weft-core/src/project.rs GroupKind
export interface GroupDefinition {
  id: string;
  /// `group` or `loop`. The visual editor renders a loop differently
  /// (distinct color/glyph + carry-port double rendering). A loop runs
  /// through its two boundary nodes; every other kind has none.
  /// On the Rust side `kind` + `loopConfig` are ONE flattened tagged
  /// enum (GroupKind), so a loop always carries its config and a
  /// payload missing `kind` fails Rust deserialization.
  kind: 'group' | 'loop' | 'call' | 'body';
  /// Loop config fields (parallel/over/carry/maxIters/trimOnMismatch).
  /// Always present when `kind === 'loop'`, never for the others.
  loopConfig?: Record<string, unknown> | null;
  /// For a `call`: the id of the shared `body` group (the included
  /// file, compiled once) this site calls. Never for the others.
  body?: string;
  /// For a `call` in a run's cut program: the places under the site that
  /// run (`locatedKey`s relative to the site); every place when absent.
  // SYNC: GroupDefinition.runs <-> crates/weft-core/src/project.rs GroupKind::Call.runs
  runs?: string[];
  /// Always null today: no group-like decl carries a user label in the
  /// language; the editor derives the display name from the id's local
  /// segment. The slot exists so a future label syntax needs no wire
  /// change.
  label: string | null;
  /// Each as full an input as a node's, the widget for a value written
  /// on it included (a loop's its `LoopIn`'s).
  inPorts: InputDefinition[];
  outPorts: PortDefinition[];
  /// The header's `*name: Type` ports, each with the value the body
  /// writes (`self.name = ...`).
  // SYNC: GroupDefinition.metadataPorts <-> crates/weft-core/src/project.rs GroupDefinition.metadata_ports
  metadataPorts?: MetadataPort[];
  parentGroupId: string | null;
  childGroupIds: string[];
  nodeIds: string[];
  /// True for an included file's own top-level group, written `Group(...)`
  /// with no `name =`: the only group allowed to go unnamed, so the only
  /// one that is a whole file. The editor opens it by default.
  anonymous: boolean;
  span?: Span;
  headerSpan?: Span;
  /// The group's description: the plain `# ...` comment on the first body
  /// line of the group (text without the `# `).
  description?: string | null;
  /// Literals written on the container's interface ports: `g.x = "hi"` from
  /// outside, or `_should_flow: false` in the braces. A wired value is an
  /// ordinary edge and never appears here.
  portLiterals?: Record<string, unknown>;
  /// Where each `portLiterals` entry was written, and in which form, so an
  /// edit rewrites the value where it already lives.
  portLiteralSpans?: Record<string, ConfigFieldSpan>;
  /// File-backed `portLiterals` entries, keyed by port: the same record a
  /// node keeps for a `@file` / `@asset` field (`NodeDefinition.fileRefs`).
  // SYNC: fileRefs <-> crates/weft-core/src/project.rs GroupDefinition.file_refs
  fileRefs?: Record<string, FileRef>;
  /// `[T: Media]` written after `Group`: bounds on the type variables its
  /// ports use (a loop's sit on its two boundary nodes instead).
  // SYNC: GroupDefinition.typeParams <-> crates/weft-core/src/project.rs GroupDefinition.type_params
  typeParams?: TypeParamWire[];
  /// Set only on a group inside an included file compiled for a build,
  /// whose port types differ per place the file is included at, the way a
  /// node's do (`NodeDefinition.placeTypes`). Carried as is.
  // SYNC: GroupDefinition.placeTypes <-> crates/weft-core/src/project.rs GroupDefinition.place_types
  placeTypes?: PlaceTypes[];
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
  /// The wires that leave a metadata port. The compiler has already
  /// written each one's value into the input it points at (that input's
  /// `portLiterals` entry), so none of them runs and the editor draws
  /// none: it shows each input they fill as filled from that port. Each
  /// stops where the value lands: a node's input, or a group's port seen
  /// from outside.
  // SYNC: ProjectDefinition.metadataWires <-> crates/weft-core/src/project.rs ProjectDefinition.metadata_wires
  metadataWires?: Edge[];
  groups: GroupDefinition[];
  /// Where a run of a cut is given what it does not compute (the values
  /// are the run's own, never the program's). Absent in a program as
  /// written, which is every program the editor parses.
  given?: GivenPlaces;
  /// The parts of the program that do not compile; a cut that needs one is
  /// refused. Absent in a program that compiles whole, which is every
  /// program the editor parses (its own parse reports diagnostics instead).
  broken?: Broken[];
}

/// See `ProjectDefinition.broken`.
// SYNC: Broken <-> crates/weft-core/src/project.rs Broken
export interface Broken {
  /// The node and group ids the problem is about; a cut keeping all of
  /// them needs it.
  parts: string[];
  /// The places (`locatedKey`) it holds at, when only some; absent: every
  /// place its parts run.
  // SYNC: Broken.places <-> crates/weft-core/src/project.rs Broken.places
  places?: string[];
  why: string;
  /// The part may stand for nodes the program cannot see (an included
  /// file that does not compile, a node whose type did not load).
  hides?: boolean;
}

/// See `ProjectDefinition.given`: per place (`locatedKey`), the input
/// ports handed a value, and the places that stand in for their node.
// SYNC: GivenPlaces <-> crates/weft-core/src/project.rs GivenPlaces
export interface GivenPlaces {
  inputs?: Record<string, string[]>;
  outputs?: string[];
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
  // The node and group ids the finding is about (one for a node or a
  // group, the two ends for a wire); absent when it is about the program
  // as a whole.
  subjects?: string[];
  // The places (`locatedKey`) the finding holds at, when only some of
  // its subjects' places are wrong; absent: every place.
  // SYNC: Diagnostic.places <-> crates/weft-core/src/node.rs Diagnostic.places
  places?: string[];
}

/// Derived from the Widget union (never a second hand-kept list).
export type WidgetKind = Widget['kind'];

// The ways a `remote_select` field can be filled, and the list request
// behind a `list` source: the connect library's, since the one control
// that fills such a field lives there.
export type { ResourceSource, Lookup, PageSpec } from '../../weft-connect/src/core/wire';

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
  /// An infra node's output made once when its infra is applied, and
  /// read saved by every fire.
  baked?: boolean;
  /// The program may declare what this output holds (`-> (response:
  /// Verdict)` on the call): `true` takes anything that fits in `type`,
  /// a type string is a wider bound (`type` stays what the port holds
  /// when nothing is declared). Absent: the output takes no other type
  /// than its own.
  // SYNC: OutputSpec.narrowable <-> crates/weft-core/src/node.rs OutputSpec.narrowable
  narrowable?: true | string;
}

/// The type a program's declaration on this output must fit in, or null
/// when the output takes no other type than its own.
// SYNC: narrowingBound <-> crates/weft-core/src/node.rs OutputSpec::narrowing_bound
export function narrowingBound(output: Pick<OutputSpec, 'type' | 'narrowable'>): string | null {
  if (output.narrowable === true) return output.type;
  if (typeof output.narrowable === 'string') return output.narrowable;
  return null;
}

/// Whether a closed value on this input skips the node, given whether a
/// wire feeds it: always for a required input, never for an optional one
/// (the program wrote `x?:`), and for one that is neither (the node's
/// metadata calls it optional and the program wrote nothing) only while
/// a wire feeds it.
// SYNC: inputIsRequired <-> crates/weft-core/src/project.rs InputDefinition::is_required
export function inputIsRequired(input: Pick<InputDefinition, 'required' | 'optional'>, wired: boolean): boolean {
  return input.required || (wired && !input.optional);
}

/// One type variable a node or a group bounds: `T` in `Group[T: Media]`,
/// or a node type's `typeParams` entry. `bound` and `resolved` are wire
/// type strings; `resolved` is what the compiler worked the variable out
/// to be from what is connected, absent until then. The bound arrives
/// with the node's other resolved variables already substituted in.
// SYNC: TypeParamWire <-> crates/weft-core/src/weft_type.rs TypeParam
export interface TypeParamWire {
  name: string;
  bound: string;
  resolved?: string;
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
  /** The node type's metadata ports, each with its hand-written value. */
  // SYNC: CatalogEntry.metadataPorts <-> crates/weft-core/src/node.rs NodeMetadata.metadata_ports
  metadataPorts?: MetadataPort[];
  requiresInfra?: boolean;
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
  /** The bound on each type variable the node's ports use, keyed by
   *  the variable (`{ "T": "Media" }`): what the variable may resolve to. */
  // SYNC: CatalogEntry.typeParams <-> crates/weft-core/src/node.rs NodeMetadata.type_params
  typeParams?: Record<string, string>;
}

// The connection vocabulary (recipes, connections, doors) lives in the
// connect library, which the editor's picker and an instance's connect page
// both build on; the graph protocol names it from there. A connection's
// door is not re-exported: here `Door` is a trigger's (`take_down.rs`).
export type {
  AppRegistration,
  GrantCoexistence,
  CredentialFieldWire,
  Permission,
  VerificationRung,
  VerificationCost,
  OwnPageWire,
  AccessSpecWire,
  GrantSummary,
  CredentialOwner,
  CredentialOwnerKind,
} from '../../weft-connect/src/core/wire';
export { credentialOwnerKind } from '../../weft-connect/src/core/wire';

/** The on-wire sentinel key tagging an Access value. */
// SYNC: ACCESS_MARKER_KEY <-> crates/weft-core/src/access/value.rs ACCESS_MARKER_KEY
export const ACCESS_MARKER_KEY = '__weft_access__';

/** The on-wire sentinel key tagging a Secret's marker:
 *  `{"__weft_secret__": {"secretId": "<uuid>"}}`. */
// SYNC: SECRET_MARKER_KEY <-> crates/weft-core/src/secret.rs SECRET_MARKER_KEY
export const SECRET_MARKER_KEY = '__weft_secret__';

/** Whether `value` is a well-formed secret marker: an object holding
 *  the sentinel key alone, whose payload holds a `secretId` and nothing
 *  else. A malformed one is not a secret, just an object. */
// SYNC: isSecretMarker <-> crates/weft-core/src/secret.rs Secret::from_value
export function isSecretMarker(value: unknown): boolean {
  if (typeof value !== 'object' || value === null || Array.isArray(value)) return false;
  const keys = Object.keys(value);
  if (keys.length !== 1 || keys[0] !== SECRET_MARKER_KEY) return false;
  const payload = (value as Record<string, unknown>)[SECRET_MARKER_KEY];
  if (typeof payload !== 'object' || payload === null || Array.isArray(payload)) return false;
  const fields = Object.keys(payload);
  const id = (payload as Record<string, unknown>).secretId;
  return fields.length === 1 && fields[0] === 'secretId' && typeof id === 'string' && UUID.test(id);
}

/** A uuid in the hyphenated form the backend writes a `SecretId` in. */
const UUID = /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i;

/** The on-wire sentinel key tagging an Infra handle value. */
// SYNC: INFRA_MARKER_KEY <-> crates/weft-core/src/infra/handle.rs INFRA_MARKER_KEY
export const INFRA_MARKER_KEY = '__weft_infra__';

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
  | { shape: 'value_list' }
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

/// Why a firing did not run. A DECISION (the author's `_should_flow` said no)
/// reads differently from a CONSEQUENCE (an input it needed never arrived), so
/// the journal carries which one it was.
// SYNC: SkipReason <-> crates/weft-core/src/run/skip.rs SkipReason
/// `failure` on a closure-caused reason is the failure the closure carried:
/// the node skipped because something before it BROKE rather than
/// declined, and its own closures went out with that same failure.
export type SkipReason =
  | { kind: 'did_not_flow' }
  | { kind: 'flow_closed'; failure?: Failure }
  | { kind: 'did_flow' }
  | { kind: 'watched_node_failed'; failure: Failure }
  | { kind: 'required_input_closed'; port: string; failure?: Failure }
  | { kind: 'nothing_joined'; failure?: Failure }
  | { kind: 'every_input_closed'; failure?: Failure }
  | { kind: 'one_of_group_closed'; ports: string[]; failure?: Failure }
  | { kind: 'scope_skipped'; scope: string }
  | { kind: 'wait_skipped' };

/// A node that broke, as the closures it leaves carry it: the node spelled
/// the way the program reads it, and its error. Passed on unchanged through
/// every skip it causes.
// SYNC: Failure <-> crates/weft-core/src/pulse.rs Failure
export interface Failure {
  node: string;
  error: string;
}

/// Why a run was cancelled: a person, a sibling run (naming it, and the
/// tag when it was `ctx.stop_tagged`; none when it took the run's
/// triggers down), the live caller
/// dropping, the runtime itself, or a time limit running out. Rides the `run_cancelled`
/// event beside its text `reason`; absent only on a journal row written
/// before the field existed.
// SYNC: CancelCause <-> crates/weft-core/src/run/cancel.rs CancelCause
export type CancelCause =
  | { kind: 'user' }
  | { kind: 'run'; by: string; tag?: string }
  | { kind: 'caller_gone' }
  | { kind: 'runtime'; detail: string }
  | { kind: 'limit'; detail: string };

/// The port every node carries, deciding whether it runs at all.
// SYNC: SHOULD_FLOW_PORT <-> crates/weft-core/src/run/skip.rs SHOULD_FLOW_PORT,
// docs/src/language/syntax.md (reserved keys),
// packages/weft-syntax/weft.tmLanguage.json (reserved-key rule; see its README)
export const SHOULD_FLOW_PORT = '_should_flow';

/// The same gate read the other way round: the node runs when the thing
/// wired here did NOT happen (a closed input is the yes). A node uses
/// one spelling or the other; wiring both is a compile error, so the
/// editor draws ONE gate port and toggles which name it wires.
// SYNC: SHOULD_NOT_FLOW_PORT <-> crates/weft-core/src/run/skip.rs SHOULD_NOT_FLOW_PORT,
// docs/src/language/syntax.md (reserved keys),
// packages/weft-syntax/weft.tmLanguage.json (reserved-key rule; see its README)
export const SHOULD_NOT_FLOW_PORT = '_should_not_flow';

/// Is this port name the language's gate, either way round?
export function isGatePort(name: string): boolean {
  return name === SHOULD_FLOW_PORT || name === SHOULD_NOT_FLOW_PORT;
}

/// The output the language gives every catalog node, which the runtime
/// fills with why the node failed. Wired, a failure completes the node
/// through it; unwired, a failure stops the run. It is the gate's mirror
/// on the output side, so the editor draws it as a marker on the right
/// edge, never as an output row. Group and loop boundaries have none.
// SYNC: ERROR_PORT <-> crates/weft-core/src/node.rs ERROR_PORT
export const ERROR_PORT = '_error';

/// Is this port one the language gives a node (the gate, either way
/// round, or the failure output)? Such a port is never written into a
/// node's header and never removed with it.
export function isLanguagePort(name: string): boolean {
  return isGatePort(name) || name === ERROR_PORT;
}

// SYNC: NodeFiringStatus <-> crates/weft-core/src/run/firing.rs NodeFiringStatus
/// Adding a state requires adding it on both sides; the UI lookup
/// tables in `webview/lib/utils/status.ts` exhaust this union so
/// drift compiles as an error. The earlier shape had ghost variants
/// (`pending` / `suspended` / `accumulating`) the dispatcher never
/// emitted; they painted states the engine could not produce and
/// `suspended` doubled-up with the real `waiting_for_input` event.
export type NodeFiringStatus =
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
// the dispatcher event when the run follower has it. The modal
// inspector renders them as JSON trees; without these filled in
// it falls back to "(none)" for every node, even when the
// run actually moved data.

/// One level of the dynamic structure a firing is inside: an iteration
/// of a loop (`{index}`), or a call of an included file through the site
/// named (`{site}`, the site's own id: `auth`, or `Auth.billing.inner`
/// for a site inside a body). A firing's frame stack is a list of these,
/// outermost first; empty at the root. A loop body and an included file
/// are each compiled once, and the stack is what tells two runs through
/// the same nodes apart.
// SYNC: Frame <-> crates/weft-core/src/frames.rs Frame
export type Frame = { index: number } | { site: string };

/// The call sites on a frame stack, outermost first: which use of which
/// included file the firing belongs to, loop frames left out.
export function callPathOf(frames: readonly Frame[]): string[] {
  return frames.flatMap((f) => ('site' in f ? [f.site] : []));
}

/// One frame as text: `#3` for an iteration, `@auth` for a call.
export function frameText(frame: Frame): string {
  return 'index' in frame ? `#${frame.index}` : `@${frame.site}`;
}

/// The key of one place of a run: a node under the call sites that
/// reach it, `one/Clean.strip`, or the bare id at the top. What a run's
/// scope and a seed's origins are keyed by.
// SYNC: locatedKey <-> crates/weft-core/src/frames.rs Located
export function locatedKey(id: string, callPath: readonly string[]): string {
  return callPath.length === 0 ? id : `${callPath.join('/')}/${id}`;
}


/// Why a node exists once per instance: `marked` for an infra node the
/// source marks `@per_instance` (a container each), `filled` for a node with
/// a field written `@instance_filled` (each instance gets its own value),
/// `derived` for a node reached from either.
// SYNC: PerInstance <-> crates/weft-core/src/instance.rs PerInstance
export type PerInstance = 'marked' | 'filled' | 'derived';

/// The shape an `@instance_filled` field's value takes in the compiled
/// literals: `{ "__weft_instance_filled__": {} }`, with `fallback` inside
/// when the source gives one (`@instance_filled(<value>)`).
// SYNC: INSTANCE_FILLED_KEY <-> crates/weft-core/src/instance.rs INSTANCE_FILLED_KEY
export const INSTANCE_FILLED_KEY = '__weft_instance_filled__';

export interface InstanceFilledValue {
  [INSTANCE_FILLED_KEY]: { fallback?: unknown };
}

/// `value` read as an `@instance_filled` literal (its fallback, when any),
/// `null` for any other value.
// SYNC: instanceFilled <-> crates/weft-core/src/instance.rs as_instance_filled
export function instanceFilled(value: unknown): { fallback?: unknown } | null {
  if (typeof value !== 'object' || value === null || Array.isArray(value)) return null;
  const keys = Object.keys(value);
  if (keys.length !== 1 || keys[0] !== INSTANCE_FILLED_KEY) return null;
  const inner = (value as Record<string, unknown>)[INSTANCE_FILLED_KEY];
  if (typeof inner !== 'object' || inner === null || Array.isArray(inner)) return null;
  return inner as { fallback?: unknown };
}

/// The literal an access node's connection field compiles to when the
/// install keeps its pick (never written in the source: a connection's id
/// means nothing on another install). The pick itself is read through the
/// host (`picksCall`), keyed by the node's place.
// SYNC: INSTALL_PICKED_KEY <-> crates/weft-core/src/picks.rs INSTALL_PICKED_KEY
export const INSTALL_PICKED_KEY = '__weft_install_picked__';

/// Whether `value` is the install-picked marker.
// SYNC: installPicked <-> crates/weft-core/src/picks.rs is_install_picked
export function installPicked(value: unknown): boolean {
  if (typeof value !== 'object' || value === null || Array.isArray(value)) return false;
  const keys = Object.keys(value);
  if (keys.length !== 1 || keys[0] !== INSTALL_PICKED_KEY) return false;
  const inner = (value as Record<string, unknown>)[INSTALL_PICKED_KEY];
  return typeof inner === 'object' && inner !== null && !Array.isArray(inner);
}

/// A connection as a pick holds it.
export interface ConnectionHandle {
  id: string;
  identity?: string;
}

/// The install's picks for a project: place (`slack`, `one.slack`) ->
/// field -> handle.
// SYNC: Picks <-> crates/weft-core/src/picks.rs Picks
export type Picks = Record<string, Record<string, ConnectionHandle>>;

/// One pick to keep: `connection` for `field` of the node at `node`.
// SYNC: PickInput <-> crates/weft-core/src/picks.rs PickInput
export interface PickInput {
  node: string;
  field: string;
  connection: string;
  service: string;
}

/// A change of the install's picks.
// SYNC: ChangePicks <-> crates/weft-core/src/picks.rs ChangePicks
export interface ChangePicks {
  set?: PickInput[];
  clear?: { node: string; field: string }[];
}

/// The literal `@instance_filled` (or `@instance_filled(<fallback>)`) lowers to.
// SYNC: instanceFilledValue <-> crates/weft-core/src/instance.rs instance_filled_literal
export function instanceFilledValue(fallback?: unknown): InstanceFilledValue {
  return { [INSTANCE_FILLED_KEY]: fallback === undefined ? {} : { fallback } };
}

/// Whose activations or infra copies a lifecycle verb acts on, as the
/// wire carries it: the field absent is the program's shared ones, a list
/// names instances, `"all_instances"` is every instance weft holds
/// anything for, and `"all"` is the shared ones and every instance. The
/// CLI spells the same four as no flag, `--instance <id>` (repeated),
/// `--all-instances` and `--all`.
// SYNC: InstanceTarget <-> crates/weft-core/src/instance.rs InstanceTarget (and its wire words)
export type InstanceTarget = string[] | 'all_instances' | 'all';

/// Which activations a lifecycle verb names: the triggers (every one of
/// each owner's when absent), of the owners `instances` names (the shared
/// ones when absent).
// SYNC: ActivationScope <-> crates/weft-core/src/activation.rs ActivationScope
export interface ActivationScope {
  triggers?: string[];
  instances?: InstanceTarget;
}

/// The aggregate status over the program's shared activations.
// SYNC: LifecycleStatus <-> crates/weft-core/src/projects.rs ProjectStatus
export type LifecycleStatus = 'registered' | 'activating' | 'active' | 'inactive';

/// What happens to work arriving while a trigger is down: `take` holds
/// it (a question stays listed for the people who answer it), `hide`
/// holds it out of sight, `refuse` takes nothing.
// SYNC: Door <-> crates/weft-core/src/take_down.rs Door
export type Door = 'take' | 'hide' | 'refuse';

/// What happens to the work already waiting on a trigger as it goes
/// down (the arrivals it held, the runs it fired that are paused):
/// kept for when it is back, or dropped (arrivals deleted, paused runs
/// cancelled).
// SYNC: Held <-> crates/weft-core/src/take_down.rs Held
export type Held = 'keep' | 'drop';

/// What happens to the runs going right now: `wait` lets them finish on
/// the version they started on, and the command goes on only once they
/// have (an infra copy goes, a resync's new version takes over, a
/// deactivate answers), with no time limit; `cancel` stops them at once.
// SYNC: Running <-> crates/weft-core/src/take_down.rs Running
export type Running = 'wait' | 'cancel';

/// What `weft activate` does with the work its triggers held while they
/// were down: run it (paced, so new arrivals still get through), or drop
/// it.
// SYNC: ReleaseHeld <-> crates/weft-core/src/take_down.rs ReleaseHeld
export type ReleaseHeld = 'run' | 'drop';

/// A take-down, every answer given.
// SYNC: TakeDown <-> crates/weft-core/src/take_down.rs TakeDown
export interface TakeDown {
  door: Door;
  /// With `take` or `hide`: the door refuses once this many minutes have
  /// passed (1 or more). Absent: it stays as it is until the trigger is
  /// back.
  doorForMinutes?: number;
  held: Held;
  running: Running;
}

/// A take-down as a request says it: each answer it gives, the rest left
/// to the defaults every command shares (`TAKE_DOWN_DEFAULT`). The
/// CLI's `--door`, `--door-for`, `--held` and `--running`.
// SYNC: TakeDownChoice <-> crates/weft-core/src/take_down.rs TakeDownChoice
export interface TakeDownChoice {
  door?: Door;
  doorForMinutes?: number;
  held?: Held;
  running?: Running;
}

/// What every command that takes triggers down does when a request says
/// nothing: nothing destructive. The door takes and keeps what it took,
/// and the runs going finish on their own version before the command
/// goes on.
// SYNC: TAKE_DOWN_DEFAULT <-> crates/weft-core/src/take_down.rs TakeDown::DEFAULT
export const TAKE_DOWN_DEFAULT: TakeDown = { door: 'take', held: 'keep', running: 'wait' };

/// Whose a per-project resource is: the program's shared one, or one
/// instance's.
// SYNC: Owner <-> crates/weft-core/src/instance.rs Owner
export type Owner = { kind: 'shared' } | { kind: 'instance'; instance: string };

/// One owner's triggers, by name.
// SYNC: OwnerTriggers <-> crates/weft-core/src/activation.rs OwnerTriggers
export interface OwnerTriggers {
  owner: Owner;
  triggers: string[];
}

/// Owners' triggers as a person reads them, the way the CLI prints
/// them: `'door', 'feed'` for the program's own, `'inbox' of instance
/// 'ada'` for an instance's, owners apart with `; `.
// SYNC: describeOwnerTriggers <-> crates/weft-core/src/activation.rs OwnerTriggers::words, crates/weft-core/src/activation.rs OwnerTriggers::list_words
export function describeOwnerTriggers(owned: OwnerTriggers[]): string {
  return owned
    .map(({ owner, triggers }) => {
      const names = triggers.map((t) => `'${t}'`).join(', ');
      return owner.kind === 'shared' ? names : `${names} of instance '${owner.instance}'`;
    })
    .join('; ');
}

/// The work held on triggers that are down: the arrivals their door held
/// and the runs they fired that are paused on a wait.
// SYNC: HeldWork <-> crates/weft-core/src/projects.rs HeldWork
export interface HeldWork {
  arrivals: number;
  paused: number;
}

/// What a take-down did: the answers it acted on, the triggers it took
/// down, what is held on them, and how many runs going it reached.
// SYNC: TakeDownReport <-> crates/weft-core/src/deactivation.rs TakeDownReport
export interface TakeDownReport {
  takeDown: TakeDown;
  triggers: OwnerTriggers[];
  held: HeldWork;
  runs: number;
}

/// Where one activation stands in detail: never turned on, its setup
/// running, on, or for one taken down, what its door does.
// SYNC: ActivationMode <-> crates/weft-core/src/activation.rs ActivationMode
export type ActivationMode = 'registered' | 'activating' | 'active' | Door;

/// Fires waiting in a trigger's queue on something no clock brings (a
/// value the instance has not given, infra, the install's picks, or a
/// hand-over that kept failing): how many, on what, and why.
// SYNC: WaitingFires <-> crates/weft-core/src/program.rs WaitingFires
export interface WaitingFires {
  fires: number;
  // SYNC: on <-> crates/weft-core/src/door_fire.rs UnclockedWait
  on: 'instance' | 'infra' | 'picks' | 'failure';
  reason: string;
}

/// Where one activation of a trigger stands, as its node's badge reads
/// it: `behind` is on and firing an older program (`weft resync` brings
/// it up to date).
// SYNC: TriggerState <-> crates/weft-core/src/projects.rs TriggerState
export type TriggerState = 'off' | 'turning_on' | 'on' | 'behind';

/// What took a trigger down when it was not a person: the health check
/// (its infra's health rules bring it back once that infra is healthy), an
/// infra verb (it comes back once its infra runs again), or the install
/// going into maintenance (`weft maintenance off` brings it back).
// SYNC: DownWith <-> crates/weft-core/src/projects.rs DownWith
export type DownWith = 'health' | 'infra' | 'maintenance';

/// One owner's activation of a trigger.
// SYNC: TriggerCopy <-> crates/weft-core/src/projects.rs TriggerCopy
export interface TriggerCopy {
  /// Whose: absent for the shared one.
  instance?: string;
  state: TriggerState;
  mode: ActivationMode;
  /// For one taken down by something other than a person, what.
  downWith?: DownWith;
  /// For one taken down by something other than a person whose bring-back
  /// is refused: why it stays down, and the way through.
  heldBack?: string;
  /// What it holds while down. Absent when it holds nothing.
  held?: HeldWork;
  waiting?: WaitingFires;
  /// The version of the source its fires run, once its setup finished.
  version?: string;
  /// While it turns on: since when, and what its activation waits on.
  progress?: ChangeProgress;
  /// The fires and calls dropped in the last minute or two.
  dropped?: Dropped;
}

/// The fires and calls of a trigger dropped in the last minute or two:
/// refused before any run started, which nothing tries again.
// SYNC: Dropped <-> crates/weft-core/src/projects.rs Dropped
export interface Dropped {
  count: number;
  /// Why the last one was dropped, as the door said it.
  lastReason: string;
}

/// One trigger place of the program, with its state and each instance's
/// for one that exists once per instance. The dispatcher computes every
/// state; the editor only draws it.
// SYNC: TriggerEntry <-> crates/weft-core/src/projects.rs TriggerEntry
export interface TriggerEntry {
  /// The trigger, spelled the way the program writes it (`door`, `one.door`).
  node: string;
  perInstance: boolean;
  /// The program has it no more (an activation left from before the
  /// source dropped it).
  gone?: boolean;
  /// The shared activation; absent for one that exists once per instance.
  shared?: TriggerCopy;
  /// Each instance's activation, for one that exists once per instance.
  copies?: TriggerCopy[];
  /// The infra places its cut contains: taking one of them down takes
  /// this trigger down with it.
  dependsOn?: string[];
}

/// One instance's copy of an infra node, as the project status lists it.
// SYNC: InstanceInfraEntry <-> crates/weft-core/src/projects.rs InstanceInfraEntry
export interface InstanceInfraEntry {
  node: string;
  instance: string;
  status: string;
  /// While a start, stop or terminate of the copy is under way: since
  /// when, and what it waits on.
  progress?: ChangeProgress;
  /// Applied from other infra than the source holds now.
  behind?: boolean;
  /// The latest restart of its containers since weft last started it.
  lastRestart?: LastRestart;
}

/// Why a container of an infra unit stopped on its own (and its host
/// brought it back).
// SYNC: RestartCause <-> crates/weft-core/src/infra/status.rs RestartCause
export type RestartCause =
  // A process in it was killed for using more memory than it is
  // allowed: the container's own, or one inside a container that kept
  // running (a database backend).
  | { kind: 'out_of_memory' }
  | { kind: 'exited'; code: number }
  | { kind: 'unknown' };

/// The latest time a container of an infra copy stopped on its own and
/// its host brought it back, since weft last started the copy: when it
/// stopped (unix seconds) and why.
// SYNC: LastRestart <-> crates/weft-core/src/infra/status.rs LastRestart
export interface LastRestart {
  atUnix: number;
  cause: RestartCause;
}

/// One copy of an infra node, as an instance's holdings list it.
// SYNC: InfraCopy <-> crates/weft-core/src/program.rs InfraCopy
export interface InfraCopy {
  node: string;
  instance?: string;
  // SYNC: status <-> crates/weft-core/src/infra/status.rs InfraNodeStatus
  status: 'provisioning' | 'running' | 'stopped' | 'flaky' | 'failed' | 'stopping' | 'terminating';
  failure?: string;
}

/// One instance's activation of a per-instance trigger, as its holdings
/// list it.
// SYNC: InstanceTrigger <-> crates/weft-core/src/program.rs InstanceTrigger
export interface InstanceTrigger {
  trigger: string;
  mode: ActivationMode;
  waiting?: WaitingFires;
}

/// Everything weft holds for one instance of the project: what the
/// editor's instance picker lists, and what `weft instances --json`
/// prints.
// SYNC: InstanceHoldings <-> crates/weft-core/src/program.rs InstanceHoldings
export interface InstanceHoldings {
  instance: string;
  values: number;
  connections: number;
  tokens: number;
  copies: InfraCopy[];
  triggers: InstanceTrigger[];
  lastRunUnix?: number;
}

/// What `weft run --check --json` prints: whether the run of the
/// targets could start, and when not, the refusal the run itself would
/// give. When it could, `infraRead` lists the infra copies it reads (the
/// ones the install refuses it for while one is not running); held
/// against the infra status, they say whether Run can go. With more
/// than one target, `targets` says each target as a run of it alone
/// would stand, so a refused run can name which target stops it. It
/// exits 0 either way.
// SYNC: RunCheck <-> crates/weft-cli/src/commands/run.rs RunCheck
export type RunCheck =
  | { ok: true; infraRead?: InfraCopyRead[]; targets?: TargetCheck[] }
  | { ok: false; refusal: string; targets?: TargetCheck[] };

/// One target of a checked run, as a run of it alone would stand: the
/// triggers an input it needs only comes from (a run does not fire
/// them), any other refusal, and the infra copies it reads.
// SYNC: TargetCheck <-> crates/weft-cli/src/commands/run.rs TargetCheck
export interface TargetCheck {
  target: string;
  unfiredTriggers?: string[];
  refusal?: string;
  infraRead?: InfraCopyRead[];
}

/// One infra copy a run reads: the node spelled the way the program
/// reads it (`one.db`), and whose copy (absent for the shared one).
// SYNC: InfraCopyRead <-> crates/weft-cli/src/commands/run.rs InfraCopyRead
export interface InfraCopyRead {
  node: string;
  instance?: string;
}

/// The parent run and the original run supplying each reused result,
/// per place (a `locatedKey`).
// SYNC: Seed <-> crates/weft-core/src/run_spec.rs Seed
export interface Seed {
  parent: string;
  origins: Record<string, string>;
  /// The places this run runs again that replay the answers their waits
  /// got in `parent`.
  answers?: string[];
}

/// Where a run's record keeps a value an event names, and how to read it
/// back: a value travels once, and the graph reads it when it shows it
/// (`readValues`). `row`: the value one record row carries (a port's
/// emission, a wait's answer); `given`: a value the run was given at its
/// birth; `part`: keys and list items read off another value; `list`: a
/// list made of other values (what a loop gathered); `written`: a value
/// kept in no row, written out (a literal, a loop's index).
// SYNC: ValueRef <-> crates/weft-core/src/record_ref.rs ValueRef
export type ValueRef =
  | { from: 'row'; run: string; seq: number; index: number }
  | { from: 'given'; run: string; place: string; port: string; side: 'input' | 'output' }
  | { from: 'part'; of: ValueRef; path: Array<string | number> }
  | { from: 'list'; items: ValueRef[] }
  | { from: 'written'; value: unknown };

/// One value read back (`readValues`): the value; why the run's record no
/// longer holds it (`gone`: the run was erased); or why the record and the
/// reference disagree (`broken`: a bug, shown as an error).
// SYNC: KeptValue <-> crates/weft-core/src/program.rs KeptValue
export type KeptValue = { value: unknown } | { gone: string } | { broken: string };

/// `POST /runs/{id}/values`: the values some events of the run name, by
/// where the record keeps them.
// SYNC: ValuesRequest <-> crates/weft-core/src/program.rs ValuesRequest
export interface ValuesRequest {
  refs: ValueRef[];
}

/// What each reference of a `ValuesRequest` reads, in its order.
// SYNC: ValuesAnswer <-> crates/weft-core/src/program.rs ValuesAnswer
export interface ValuesAnswer {
  values: KeptValue[];
}

export interface NodeFiringEvent {
  nodeId: string;
  state: NodeFiringStatus;
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
  /// loops extend the array. Used as part of the run-card key so
  /// parallel iterations don't cross-correlate.
  frames: Frame[];
  error?: string;
  /// Each input port's value, by where the run's record keeps it.
  input?: Record<string, ValueRef>;
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
  /// Each output port's value, by where the run's record keeps it.
  output?: Record<string, ValueRef>;
  /// The run this firing was taken from when the run was seeded
  /// (`weft run --seed`) and reused it instead of firing the node
  /// again. The graph marks the node inherited. Absent on the run's
  /// own firings.
  // SYNC: inheritedFrom <-> crates/weft-core/src/live_event.rs DispatcherEvent inherited_from
  inheritedFrom?: string;
  /// Input ports whose value the run was given rather than a node of it
  /// making it: a value a person handed in (a scoped run's
  /// `--from node='{"port":value}'`), or an output a node stood in with
  /// (`--emit`, a baked infra value), on `running`. `inheritedPorts`
  /// names the ports a seed's saved result sent, with the run that made
  /// it.
  // SYNC: input origins <-> crates/weft-core/src/live_event.rs DispatcherEvent, extension-vscode/src/runFollower.ts DispatcherEvent, packages/weft-graph/src/webview/lib/types/index.ts NodeFiring
  providedPorts?: string[];
  inheritedPorts?: Record<string, string>;
}

/// One copy of a scope (a group, an include's call site, an included
/// file's body, a loop's iteration): the scope's id and the frames its
/// direct members fire at. A body's copy ends with the call frame of the
/// site that called it; a loop's with the iteration's.
// SYNC: ScopeCopy <-> crates/weft-core/src/project/scopes.rs ScopeCopy
export interface ScopeCopy {
  scope: string;
  frames: Frame[];
}

/// How a copy of a scope was decided: it opened, or it did not run and why.
// SYNC: ScopeState <-> crates/weft-core/src/run/scope.rs ScopeState
export type ScopeState = { state: 'open' } | { state: 'skipped'; reason: SkipReason };

/// The engine decided one copy of a scope. A group is no node, so this
/// is what the graph paints a group (and an include box) from, per copy;
/// a copy inside one that did not run is decided skipped with it
/// (`scope_skipped`, naming the outermost). `inheritedFrom` as on a
/// firing.
// SYNC: ScopeDecidedEvent <-> crates/weft-core/src/live_event.rs DispatcherEvent::ScopeDecided, extension-vscode/src/runFollower.ts DispatcherEvent
export type ScopeDecidedEvent = ScopeCopy & ScopeState & { inheritedFrom?: string; atUnix: number };

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
  // SYNC: LoopInspectorEvent 'instantiated' <-> crates/weft-core/src/live_event.rs LoopInstantiated, extension-vscode/src/runFollower.ts loop_instantiated
  | {
      kind: 'instantiated';
      groupId: string;
      parentFrames: Frame[];
      /// Effective iteration cap; null for an uncapped loop (done- or
      /// stream-driven with no maxIters), whose count is unknowable
      /// up front.
      iterCap: number | null;
      parallel: boolean;
    }
  | {
      kind: 'iteration_launched';
      groupId: string;
      parentFrames: Frame[];
      index: number;
    }
  | {
      kind: 'out_fired';
      groupId: string;
      parentFrames: Frame[];
      index: number;
      doneVote?: boolean | null;
    }
  | {
      kind: 'terminated';
      groupId: string;
      parentFrames: Frame[];
      reason: LoopTerminationReason;
    };

/// One line in a node's bus inspector panel. IRC-shaped: `joined` /
/// `left` render as `* name joined` / `* name left`; `message`
/// renders as `from: <payload-pretty>` (or a byte count for a binary
/// payload); `window` renders an ephemeral window's rollup ("N frames,
/// M bytes"); `closed` renders an explicit `* the bus closed here`
/// marker, or `* the bus failed here: <error>` when it failed. `busId` groups lines by channel so a node attached to
/// multiple buses gets one scrollable section per bus. Replay orders
/// lines by arrival from the SSE stream (the dispatcher already orders
/// them by their place in the run's record).
/// A journaled message's payload, in its declared kind: a JSON value,
/// or raw bytes carried as base64 (the wire form of a media frame; the
/// panel renders its size, not the base64). The ONE wire payload
/// vocabulary for every journaled exchange (bus window messages and the
/// live caller's events).
// SYNC: WirePayload <-> crates/weft-core/src/bus.rs WirePayload
export type WirePayload =
  | { kind: 'json'; data: unknown }
  | { kind: 'bytes'; data: string };

// SYNC: BusInspectorEvent 'message' <-> crates/weft-core/src/bus.rs WindowedBusMessage, extension-vscode/src/runFollower.ts DispatcherEvent 'bus_window' messages
// SYNC: BusInspectorEvent 'window' totals <-> crates/weft-core/src/bus.rs BusWindowTotal, extension-vscode/src/runFollower.ts DispatcherEvent 'bus_window' totals
export type BusInspectorEvent =
  | { kind: 'joined'; busId: string; offset: number; name: string; atUnix: number }
  | { kind: 'left'; busId: string; offset: number; name: string; atUnix: number }
  | {
      kind: 'message';
      busId: string;
      offset: number;
      from: string;
      msgKind: string;
      /// Absent when the journal kept no copy of the message (raw
      /// bytes are counted, never written down). Present, and cut
      /// down, when `trimmed` is set: a message too big to keep whole
      /// is never dropped, only shortened.
      payload?: WirePayload;
      payloadByteSize: number;
      trimmed?: boolean;
      atUnix: number;
    }
  /// One journal window of an EPHEMERAL bus: the payloads never reach
  /// the journal, so the panel renders the rollup ("N frames, M bytes
  /// from <sender>"). `offset` is the window's first offset (the
  /// dedup key the log store uses). Journaled buses never produce
  /// this: their windows unpack into per-message `message` events.
  // SYNC: BusInspectorEvent 'window' <-> crates/weft-journal/src/events.rs BusWindow, crates/weft-core/src/live_event.rs BusWindow, extension-vscode/src/runFollower.ts DispatcherEvent 'bus_window'
  | {
      kind: 'window';
      busId: string;
      offset: number;
      lastOffset: number;
      totals: Array<{ from: string; msgKind: string; count: number; bytes: number }>;
      atUnix: number;
    }
  // SYNC: BusInspectorEvent 'closed' <-> crates/weft-journal/src/events.rs BusClosed, crates/weft-core/src/live_event.rs BusClosed, extension-vscode/src/runFollower.ts DispatcherEvent 'bus_closed'
  | { kind: 'closed'; busId: string; offset: number; end: StreamEnd; atUnix: number };

/// How a stream (a bus) ended: cleanly, or failed with the producer's
/// error.
// SYNC: StreamEnd <-> crates/weft-core/src/primitive.rs StreamEnd
export type StreamEnd =
  | { kind: 'finished' }
  | { kind: 'failed'; error: string };

/// One live-caller-connection event the inspector replays. One caller
/// per run (no busId; the run id is the identity).
/// `payload` is the same tagged `WirePayload` shape a bus window's
/// messages carry.
/// Which way one message of the conversation went. A bus names its
/// sender because it has any number; a conversation has two parties
/// that never change, so the direction is the whole of who said it.
// SYNC: CallerDirection <-> crates/weft-core/src/stream_journal.rs CallerDirection
export type CallerDirection = 'inbound' | 'outbound';

/// One message inside a caller window. `payload` is absent when the
/// journal kept only the size (an ephemeral conversation, or raw
/// bytes), and shorter than `payloadByteSize` when it was trimmed, so
/// the inspector can say "412 KB, showing the first 100".
export interface WindowedCallerMessage {
  offset: number;
  direction: CallerDirection;
  payload?: WirePayload;
  payloadByteSize: number;
  trimmed?: boolean;
  terminal?: boolean;
  atUnix: number;
}

// SYNC: CallerInspectorEvent 'window' <-> crates/weft-journal/src/events.rs CallerWindow, crates/weft-core/src/live_event.rs CallerWindow, extension-vscode/src/runFollower.ts DispatcherEvent 'caller_window'
export type CallerInspectorEvent =
  | { kind: 'connected'; offset: number; protocol: string; atUnix: number }
  | {
      kind: 'window';
      firstOffset: number;
      lastOffset: number;
      messages: WindowedCallerMessage[];
      totals: Array<{ direction: CallerDirection; count: number; bytes: number }>;
      /// Offsets of the program's messages the caller never received
      /// (it read too slowly and the trigger's `whenCallerIsSlow` let
      /// them go), possibly messages of an earlier window. Absent when
      /// none.
      dropped?: number[];
      atUnix: number;
    }
  | { kind: 'errored'; offset: number; message: string; atUnix: number }
  // SYNC: CallerInspectorEvent 'disconnected' <-> crates/weft-journal/src/events.rs CallerDisconnected, crates/weft-core/src/live_event.rs CallerDisconnected, extension-vscode/src/runFollower.ts DispatcherEvent 'caller_disconnected'
  /// `dropped`: offsets of the program's messages still waiting to go
  /// out when the exchange ended (`reason` says how), which the caller
  /// never received. Absent when none.
  | { kind: 'disconnected'; offset: number; reason: string; dropped?: number[]; atUnix: number };

/// Per-bus metadata derived dispatcher-side from the bus marker JSON
/// (`{"__weft_bus__": {"id":..., "mode":"journaled"|"ephemeral"}}`).
/// The dispatcher attaches `ephemeral` to every `BusParticipant` edge
/// it derives from an emitted pulse, so the webview learns mode the
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
  | 'PortEmitted'
  | 'PortClosed'
  | 'PulsesConsumed'
  | 'NodeLifecycle'
  | 'LoopInstantiated'
  | 'LoopStreamEnded'
  | 'LoopIterationLaunched'
  | 'LoopOutFired'
  | 'LoopTerminated'
  /// The run's birth gives values where its program says nothing is
  /// given, or misses one it says.
  | 'RunGiven'
  | 'UndecodableRow'
  /// Not one row: the whole run. The code it ran is no longer
  /// recorded, so no value can be worked out from the journal. Shown
  /// beside the run, not inside a node.
  | 'MissingProgram';

/// Every kind a node's display can draw, written down once for
/// TypeScript: `LiveDataItem['type']` reads from this array, so a
/// fifth kind cannot be added to one and missed by the other.
///
/// Adding one: the string here, a variant on `LiveItemKind` in Rust,
/// and a render branch in ProjectNode.svelte.
// SYNC: LIVE_DATA_TYPES <-> crates/weft-core/src/live.rs LiveItemKind
export const LIVE_DATA_TYPES = ['text', 'image', 'progress', 'secret'] as const;
export type LiveDataType = (typeof LIVE_DATA_TYPES)[number];

/// One line of a node's display: what it shows about itself while it
/// runs. Two things produce one, and both serve it on `GET /live` in
/// this shape: an infra node's own container, and the listener kind
/// holding a signal (a trigger's, or a wait's in a run). One channel
/// (`nodeDisplay`) carries all of them, keyed by node id.
// SYNC: LiveDataItem <-> crates/weft-core/src/live.rs LiveItem, crates/weft-e2e/src/display.rs
export interface LiveDataItem {
  /// - `text`: plain copyable string in a code-style box.
  /// - `image`: `data` is a data URI; rendered inline.
  /// - `progress`: `data` is a 0..1 number; rendered as a bar.
  /// - `secret`: hidden behind a `••••` mask until the user
  ///   clicks the eye icon to reveal. Copy still works on the
  ///   underlying value. Use for API keys, signed URLs, anything
  ///   that shouldn't sit on screen by default.
  type: LiveDataType;
  label: string;
  data: string | number;
  /// Optional action button rendered next to the item. Click posts a
  /// `displayAction` message, which the host sends to
  /// `/projects/{id}/infra/nodes/{place}/action` for the node at this
  /// view's place: the container behind `/live` serves `/action`, and
  /// it owns the payload schema. Only an INFRA node's display carries
  /// one; the listener refuses to serve a trigger's display with a
  /// button. Generic, so a node author adds buttons without changing
  /// the panel.
  action?: {
    label: string;
    actionKind: string;
    payload?: unknown;
    confirm?: string;
  };
}

/// State of one node's display. The dispatcher pushes one of these
/// each time it changes: `ok` carries the items, `error` carries a
/// short user-facing message the webview shows in their place. There is
/// NO silent fallback: if the display cannot be read, the user sees the
/// error verbatim.
// SYNC: NodeFeedState <-> crates/weft-dispatcher/src/display_feeds.rs (NodeFeed)
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
//     cli_running / run_running / error). Driven by status
//     fetches + CLI NDJSON events.
//   - statusSnapshot: the latest `weft status --json` payload
//     (available actions, every trigger's, infra copy's and
//     instance's state). Used by both bars AND graph decorations.
//
// Adding a new graph-decoration source means folding it into
// statusSnapshot, not adding a new message type.

/// The single-flight transition on the project row, orthogonal to
/// `projectStatus`. While a build is in flight, the only offered action
/// is cancel_build (the master transitional rule); while a resync is, only
/// the cancel of the trigger step it is at.
// SYNC: ProjectTransition <-> crates/weft-core/src/projects.rs ProjectTransition, packages/weft-graph/src/status.ts VALID_TRANSITIONS
export type ProjectTransition = 'none' | 'building' | 'cancelling_build' | 'resyncing';

/// Snapshot of backend state from `weft status --json`. Refreshed
/// on graph open, after every CLI verb, on SSE-triggered events
/// (debounced 500ms), on file-change debounce, and on the Refresh
/// button. Drives the bottom bar's in-flight states, the contextual
/// bar's counts, and the badges on the trigger and infra nodes. The
/// dispatcher computes every state in it; the editor only draws.
// SYNC: ActionAvailability <-> crates/weft-core/src/projects.rs ProjectStatusResponse
export interface ActionAvailability {
  /// `weft rm` marked the project and has not finished removing it: no
  /// verb is offered, the starter ones included.
  removing: boolean;
  /// Verbs the dispatcher will currently accept.
  availableActions: ActionVerb[];
  /// The aggregate status over the program's shared activations.
  // SYNC: projectStatus <-> crates/weft-core/src/projects.rs ProjectStatus
  projectStatus: LifecycleStatus | 'unknown';
  /// The transition axis. 'building'/'cancelling_build' show
  /// "Building... (cancel)" on the bottom bar and gate every other verb;
  /// 'resyncing' leaves only what `availableActions` still offers.
  transition: ProjectTransition;
  /// Live infra rows exist whose node the current source no longer
  /// declares (the user deleted the node while it was deployed).
  orphanedInfra: boolean;
  infraRollup: InfraRollup;
  /// An infra operation is in flight that the rollup cannot see yet.
  /// The dispatcher offers only `infra_cancel` in this window.
  infraBusy: boolean;
  /// Per-placement infra status, the shared copies. One entry per
  /// PLACE: a file included twice provisions its infra twice.
  infraNodes: InfraPlacementStatus[];
  /// Every instance's copy of a per-instance infra node.
  instanceInfra: InstanceInfraEntry[];
  /// Every trigger place of the program, each with its state per owner.
  triggers: TriggerEntry[];
  /// Every instance weft holds anything for: the instance picker's list.
  instances: InstanceHoldings[];
}

/// Every verb the editor shows working. Mirrors the dispatcher's
/// `compute_available_actions` output union AND the CLI's `ActionVerb`
/// enum (snake_case). Some verbs come only from the CLI's progress
/// stream (bake, build, rm); the editor consumes both, so the type is
/// the superset.
// SYNC: ActionVerb <-> crates/weft-dispatcher/src/api/project.rs compute_available_actions, crates/weft-cli/src/progress.rs ActionVerb
export type ActionVerb =
  | 'run'
  | 'activate'
  | 'bake'
  | 'cancel_activate'
  | 'cancel_build'
  | 'deactivate'
  | 'resync'
  | 'build'
  | 'rm'
  | 'infra_start'
  | 'infra_stop'
  | 'infra_terminate'
  | 'infra_upgrade'
  | 'infra_cancel';

/// A lifecycle verb the contextual bar sends for the selected nodes: the
/// triggers or infra places it names (never empty: acting on everything
/// is selecting everything), whose (`instances` absent is the shared
/// ones), and the answers the options gave. The host turns it into the
/// CLI's flags (`lifecycleCommand`).
export type LifecycleAction =
  | { verb: 'activate'; triggers: string[]; instances?: InstanceTarget; held?: ReleaseHeld }
  | { verb: 'deactivate' | 'resync'; triggers: string[]; instances?: InstanceTarget; takeDown: TakeDownChoice }
  | { verb: 'infra_start'; infra: string[]; instances?: InstanceTarget }
  | {
      verb: 'infra_stop' | 'infra_terminate' | 'infra_upgrade';
      infra: string[];
      instances?: InstanceTarget;
      takeDown: TakeDownChoice;
    };

/// CLI progress phase. Closed set so the reducer's match is
/// exhaustive at the type level.
// SYNC: CliPhase <-> crates/weft-cli/src/progress.rs Phase
export type CliPhase =
  /// The install is asked to build the project; detail `project`, its name.
  | 'build_start'
  /// The install built it; detail `project`, and `built`, the image refs
  /// it had to build (empty when every image was already there).
  | 'build_done'
  /// One image is building on the install's builder; detail `image`,
  /// `build` (the builder's id) and `logUrl` when it keeps one.
  | 'build_image'
  /// One image finished building; detail `image` and `seconds`.
  | 'build_image_done'
  /// The build goes on; detail `elapsedSeconds`, `images`, the ones
  /// still building, and `built`, the ones done during this wait.
  | 'build_wait'
  /// The install cannot be reached to see where the build is; the CLI
  /// keeps looking. Detail `elapsedSeconds` and `message`, why the last
  /// look failed. The next `build_wait` or `build_image` says it answers again.
  | 'build_unreachable'
  | 'dispatcher_call_start'
  | 'dispatcher_call_done'
  | 'infra_provision_start'
  | 'infra_provision_done'
  | 'trigger_register_start'
  /// The activation goes on, and what it waits on changed. Detail carries
  /// `elapsedSeconds` and `waiting` ("its setup run <id>").
  // SYNC: the trigger_register_wait detail <-> crates/weft-cli/src/progress.rs Progress::trigger_register_wait
  | 'trigger_register_wait'
  | 'trigger_register_done'
  /// Periodic heartbeat while an infra verb waits on the supervisor
  /// (unbounded: draining runs). Detail carries `elapsedSeconds`.
  | 'infra_wait'
  /// The call about to be made will wait for the project's running
  /// runs to finish before it lands. Detail carries `on` (`infra`,
  /// `resync` or `deactivate`). Sent before the call, because the wait
  /// happens on the dispatcher: without it the command looks hung. A
  /// resync sends it again each time the runs it waits for change, with
  /// `waitsFor`: `{ runId, sinceUnix }` per run on the old version, empty
  /// once every run named before has ended, and `turningOn`
  /// (`ChangeProgress`) while it turns an owner's triggers back on; again
  /// every half minute while neither changes, each time with
  /// `elapsedSeconds`, how long it has been followed. A run a worker
  /// accepted but has not recorded yet is waited for without being
  /// listed, so an empty list can still be waiting.
  // SYNC: the drain_wait detail's waitsFor <-> crates/weft-cli/src/progress.rs Progress::resync_waits_for
  | 'drain_wait'
  /// The run is being prepared, and what its start is doing, said once it
  /// changes and every half minute while it lasts. Detail carries
  /// `elapsedSeconds` and `doing` ("reading the results of seed run ...").
  // SYNC: the run_starting detail <-> crates/weft-cli/src/progress.rs Progress::run_starting, packages/weft-graph/src/webview/lib/components/project/ActionBar.svelte (run_starting)
  | 'run_starting'
  /// Something the person should know that is not a failure; the verb
  /// carries on and still ends `complete`. Detail carries `message`.
  | 'warning'
  | 'complete'
  | 'error';

/// What the action bar's working label can show: every CLI phase, plus
/// the extension-side `preflight` window (the saved-state check that
/// runs between the click and the CLI spawn). Extension-only: the CLI
/// never emits it, so it is NOT part of the CliPhase wire SYNC.
export type BarPhase = CliPhase | 'preflight';

/// One NDJSON line emitted by the CLI in --json mode.
export interface CliEvent {
  tsUnix: number;
  /// Absent on the one event a verb with no progress channel (a reader
  /// such as `events` or `logs`) emits under `--json`: its `error`.
  verb?: ActionVerb;
  phase: CliPhase;
  detail?: Record<string, unknown>;
}

/// Action-bar state. Three orthogonal concerns:
///
///   1. `backend`: at-rest facts from `weft status --json`. Always
///      present (defaults until the first fetch lands): the build
///      transition, the infra rollup, the verbs weft takes now.
///
///   2. `overlay`: what user-action the bar is currently locked
///      into. `idle` when waiting for input, `cli_running` while a
///      CLI verb is in flight, `run_running` when the user is
///      following a live run, `pending` while an HTTP verb
///      (Stop) awaits backend confirmation. While a verb is in flight
///      the contextual bar's actions wait for it.
///
///   3. `error`: sticky banner shown above the bar. Independent of
///      both above. Cleared by user dismiss or by the next
///      successful idle push.
///
/// Transitions:
///   - status fetch result      -> backend updated, overlay unchanged
///   - SSE run_started    -> overlay may flip to run_running
///   - SSE run_finished   -> overlay flips back to idle
///   - CLI start                -> overlay = cli_running
///   - CLI complete             -> overlay = idle
///   - CLI error                -> overlay = idle, error set
///   - user clicks Stop         -> overlay = pending, until SSE confirms
///
///   4. `infraSetup`: an infra setup is running, whoever started it
///      (this editor, a terminal, an assistant). The bottom bar shows
///      "Starting infra..." for it, and it is never a run it offers to
///      stop. A trigger setup needs no such flag: the trigger's own
///      `turning_on` state already says it.
export type ActionBarState = {
  backend: BackendSnapshot;
  overlay: ActionBarOverlay;
  infraSetup: boolean;
  error?: ActionBarError;
  activity?: ActionBarActivity;
};

/// What the verb currently on the bar has printed to the terminal.
///
/// The host mirrors its `Weft` output channel here: the command line it
/// spawned, then everything the CLI wrote to stderr (cargo, docker, the
/// supervisor's progress) and any stdout line that was not an NDJSON
/// event. That is the part of a verb nobody can see from the graph. A
/// run's own log is NOT in here: it arrives over SSE and the graph
/// already paints it.
///
/// Set when a verb starts, appended to as it prints, dropped when the
/// verb completes cleanly. A verb that FAILED keeps its output, so the
/// details modal can show it under the error.
export interface ActionBarActivity {
  verb: ActionVerb;
  /// Oldest line first. Capped at `ACTIVITY_LINE_CAP`, keeping the
  /// tail: a build that failed says why in its last lines.
  lines: string[];
}

/// How many terminal lines the bar keeps per verb. A `cargo --verbose`
/// or a docker build with many layers prints megabytes; the reader wants
/// the end of it.
export const ACTIVITY_LINE_CAP = 500;

/// Longest single line kept. One `docker build` progress line can be
/// tens of kilobytes of carriage-return repainting.
export const ACTIVITY_LINE_CHAR_CAP = 2000;

/// Verbs that can carry an error to the action bar. Includes every
/// CLI verb the user can click PLUS the system-side error sources
/// (parse / catalog) that the graph view raises without any user
/// click. Surfaced as a wider union than `ActionVerb` so the modal
/// renders an honest headline ("Parse failed", "Catalog failed")
/// instead of pretending a CLI verb crashed when none did.
export type ErrorVerb = ActionVerb | 'parse' | 'catalog' | 'status';

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
  /// See `ActionAvailability.removing`.
  removing: boolean;
  /// Verbs the dispatcher will currently accept. Inherited from
  /// the most recent status fetch; stale-but-known is preferable
  /// to blank during overlays so disabled-state derivation works.
  available: ActionVerb[];
  /// Project lifecycle status.
  status: LifecycleStatus | 'unknown';
  /// The build-transition axis (see `ActionAvailability.transition`).
  transition: ProjectTransition;
  /// Live orphaned infra exists (see `ActionAvailability.orphanedInfra`).
  orphanedInfra: boolean;
  infraRollup: InfraRollup;
  /// See `ActionAvailability.infraBusy`.
  infraBusy: boolean;
};

/// One infra PLACEMENT's status, as `weft status` reports it.
// SYNC: InfraPlacementStatus <-> crates/weft-core/src/projects.rs ProjectInfraEntry, packages/weft-graph/src/status.ts RawStatusPayload.infra
export interface InfraPlacementStatus {
  /// The node's placement spelled the way a person writes it (`db`,
  /// `one.db`), which is what a view matches against the node under
  /// the calls it walked into (`addressOf(callPath, id)`) and what the
  /// per-node verbs take.
  node: string;
  nodeType: string;
  /// Possible values:
  ///   "provisioning" | "running" | "stopped" | "flaky" | "failed"
  ///   | "stopping"   | "terminating"
  /// for a copy (a start or stop under way reads as provisioning or
  /// stopping before the supervisor reaches the copy), and for a place
  /// with no shared copy:
  ///   "not_started" (nothing started it yet)
  ///   | "per_instance" (a `@per_instance` node: only instances have copies)
  // SYNC: "not_started" / "per_instance" <-> crates/weft-core/src/infra/wire.rs INFRA_NOT_STARTED, INFRA_PER_INSTANCE
  status: string;
  /// For a `per_instance` place: how many instances have a copy.
  instanceCopyCount?: number;
  /// Set when status=failed: which stage of the apply pipeline
  /// failed (`provision` | `apply` | `execute` | `apply_lifecycle`).
  failureStage?: string;
  failureMessage?: string;
  /// While a start, stop or terminate of the copy is under way: since
  /// when, and what it waits on.
  progress?: ChangeProgress;
  /// Its copy was applied from other infra than the source holds now:
  /// `weft infra upgrade --infra <node>` brings it up to date.
  behind?: boolean;
  /// The triggers whose cut contains it (each instance's too, for a
  /// shared one), by owner: what taking it down takes down with it.
  takesDown?: OwnerTriggers[];
  /// The latest time one of its copy's containers stopped on its own
  /// (ran out of memory, exited) and its host brought it back, since
  /// weft last started the copy (`describeRestart` says it).
  lastRestart?: LastRestart;
}

/// How far a start, stop or terminate of an infra copy got: since when
/// it was asked for, as of the clock `asOfUnix` names. The server's
/// answer carries its own clock; `parseStatusPayload` moves both stamps
/// onto this machine's.
// SYNC: ChangeProgress <-> crates/weft-core/src/infra/wire.rs ChangeProgress
export interface ChangeProgress {
  sinceUnix: number;
  asOfUnix: number;
  /// What it waits on right now, in the host's words; absent before the
  /// host reports anything.
  waiting?: string;
}

/// The project-wide infra state the dispatcher rolls up from its
/// per-node rows. A closed set, named here once so nothing that
/// compares against it can compare against a typo.
// SYNC: InfraRollup <-> crates/weft-dispatcher/src/api/project.rs gather_action_snapshot (infra_rollup)
export type InfraRollup =
  | 'none'
  | 'stopped'
  | 'partial'
  | 'running'
  | 'failed'
  | 'flaky'
  | 'stopping'
  | 'terminating'
  | 'provisioning';

export type ActionBarOverlay =
  | { kind: 'idle' }
  | { kind: 'cli_running'; verb: ActionVerb; phase: BarPhase; detail?: Record<string, unknown> }
  | { kind: 'run_running'; runId: string }
  | { kind: 'pending'; verb: ActionVerb; message: string };

/// The install the graph shows the project on. The local install shows
/// the files on disk, editable. Any other shows the program that install
/// holds, read-only except its connections, and every read, run and verb
/// acts there (`--on <install>`).
// SYNC: InstallView <-> extension-vscode/src/extension.ts postInstallView
export interface InstallView {
  /// Every install, the local one first. `loggedIn` is false where this
  /// person holds no key yet (`weft login <name>`).
  installs: { name: string; loggedIn: boolean }[];
  /// The install shown now.
  active: string;
  /// On another install than the local one: the version of the program it
  /// holds, and the version the files on disk are (null when unknown).
  version: string | null;
  diskVersion: string | null;
  /// The install a switch is on its way to.
  switching: string | null;
  /// Why the last switch failed.
  error: string | null;
}

/// The install every command acts on without `--on`.
// SYNC: LOCAL_INSTALL <-> crates/weft-compiler/src/project.rs LOCAL_TARGET
export const LOCAL_INSTALL = 'local';

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
  /// The installs the graph can show the project on, and which one it
  /// shows (the install switch, top right).
  | { kind: 'installView'; view: InstallView }
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
  /// `callPath` is the chain of call sites descended through, outermost
  /// first (`['c']`, or `['c', 'C.inner']`), each the site's own id. An
  /// included file's nodes carry the file's id (`C.strip`) in the journal
  /// whatever site called them; which call a row belongs to is the call
  /// frames it carries, so the view keeps the rows whose call frames are
  /// this path.
  /// `interactive` is whether this view IS a place in the program: the
  /// entry file, or a file walked into through its includes from there.
  /// A `.weft` opened on its own (an included file picked from the
  /// explorer) is a graph with no place: nothing says which call of it
  /// is meant, so the view shows no runs and no action bar, and the
  /// person walks in from the entry file when they want those.
  | { kind: 'navState'; depth: number; fileName: string; callPath: string[]; interactive: boolean }
  /// The run reached a terminal. A cancel carries WHY (`reason` is the
  /// text, `cause` the structured value), so a run stopped by a sibling
  /// through `ctx.stop_tagged` reads as that, never as a bare failure.
  | { kind: 'runTerminal'; runId: string; state: 'completed' | 'failed' | 'cancelled'; reason?: string; cause?: CancelCause; atUnix: number }
  /// The run tagged itself (`ctx.tag_run`). `tags` is one call's
  /// list; the webview accumulates the run's set.
  | { kind: 'runTags'; runId: string; tags: string[] }
  /// The run's scope, off its birth row: every place its program runs
  /// (`locatedKey`s; a place that stands in for itself is not among
  /// them; `null` when the run's program could not be read), so every
  /// other node paints as not in this run, and the run it was seeded
  /// from with what it re-ran.
  // SYNC: runScope <-> crates/weft-core/src/live_event.rs DispatcherEvent::RunStarted (subgraph, seed)
  | { kind: 'runScope'; runId: string; subgraph: string[] | null; seed: Seed | null }
  /// Which version the followed run ran, and which version the files on
  /// disk are (`null` when the disk matches no recorded version), so the
  /// graph can say "this run is from version X, your code differs".
  | { kind: 'runVersion'; runId: string; version: string | null; diskVersion: string | null }
  /// No run is on screen any more: the person stopped looking at one,
  /// or the one they were looking at was deleted. Everything a run
  /// painted (statuses, values, scope dimming, the version banner)
  /// goes, and the canvas waits for the next run to start.
  | { kind: 'runCleared' }
  /// Reply to `checkRun`: weft's answer for those targets
  /// (`weft run --check`).
  | { kind: 'runChecked'; requestId: number; check: RunCheck }
  | { kind: 'catalogAll'; catalog: Record<string, CatalogEntry> }
  /// The node catalog (full set, from `weft describe-nodes`) failed to
  /// load, or loaded with soft warnings. Distinct from `parseError`:
  /// the source may parse fine while the catalog is unavailable
  /// (weft not on PATH, a project error) or partial (a node mid-rename
  /// with bad metadata.json). Rendered as a non-blocking banner so it
  /// isn't erased by an unrelated successful parse. `error` set means
  /// the whole catalog is missing; `warnings` carries per-node soft
  /// failures when the catalog loaded but some nodes were skipped, and
  /// names a loaded node spells off the weft way.
  | { kind: 'catalogError'; error?: string; warnings?: CatalogWarning[] }
  | { kind: 'firingEvent'; event: NodeFiringEvent }
  /// One copy of a scope decided (live or replay); see `ScopeDecidedEvent`.
  | { kind: 'scopeDecided'; event: ScopeDecidedEvent }
  /// One metered call's cost record for a firing (a provider meter's
  /// figure). `costId` is the record's stable identity: the same record can
  /// arrive via both the replay and the live stream, so the reducer dedups
  /// on it. `amountUsd` null = the meter could not resolve the figure (an
  /// honest unknown; nothing is added to the row's total). `origin` says
  /// whose key the call spent.
  /// One metered call's cost. `service` is who was charged.
  /// `inheritedFrom` is the run this cost was FIRST paid by, set when
  /// this run was seeded from that one and reused its work: the money
  /// is real but it was not spent again, so a total that adds it is
  /// counting it twice.
  // SYNC: runCost <-> extension-vscode/src/runFollower.ts DispatcherEvent 'cost_reported', crates/weft-core/src/live_event.rs CostReported
  | { kind: 'runCost'; nodeId: string; frames: Frame[]; costId: string; service: string; inheritedFrom: string | null; amountUsd: number | null; origin: CredentialOwner }
  /// One change a firing made to a stored file's content in place, for
  /// the inspector's "Files edited" card. The same change can arrive via
  /// both the replay and the live stream; the reducer dedups on the file's
  /// key and the version the change produced (each version is reached
  /// once). `inheritedFrom` as on `runCost`.
  // SYNC: execFileEdit <-> extension-vscode/src/runFollower.ts DispatcherEvent 'file_edited', crates/weft-core/src/live_event.rs FileEdited
  | { kind: 'execFileEdit'; nodeId: string; frames: Frame[]; inheritedFrom: string | null; edit: FileEditWire }
  /// One bus event (live or replay). Carries only what the bus layer
  /// recorded: join / left / message / closed keyed by `busId`.
  /// Routing to node inspector panels is a SEPARATE signal,
  /// `busParticipant`, because participation is a property of the
  /// graph, not of the live bus stream.
  | { kind: 'busEvent'; event: BusInspectorEvent }
  /// One live-caller-connection event (live or replay). One caller per
  /// run, so unlike `busEvent` there is no busId; the inspector
  /// renders a single "caller" panel for the run replaying what the
  /// program said to and heard from the caller.
  | { kind: 'callerEvent'; event: CallerInspectorEvent }
  /// One loop event (live or replay). Routes by groupId + parentFrames
  /// so the inspector card for each LoopOut node groups its
  /// iterations together.
  | { kind: 'loopEvent'; event: LoopInspectorEvent }
  /// "Node N participates in bus B." Derived dispatcher-side from
  /// emitted pulses whose value carries a bus marker. The
  /// webview unions these into a per-bus participant set; the
  /// inspector for each participant node renders the bus's IRC log.
  | { kind: 'busParticipant'; busId: string; nodeId: string; meta: BusMeta }
  /// One journal row the dispatcher could not apply during fold
  /// (replay-time corruption check). The inspector aggregates these
  /// into a muted "N journal rows corrupted" line; not alarming,
  /// not red, just visible if the user looks. Per-run; the
  /// webview groups by run and renders the list
  /// behind a collapsed disclosure.
  | { kind: 'journalCorruption'; site: CorruptionSite; reason: string }
  /// What one node is showing, off its `/live`: the container's for an
  /// infra node, the listener kind's for a trigger, and for any other
  /// node the listener kind's holding its wait in the followed run.
  /// Routed to that node's body panel.
  | ({ kind: 'nodeDisplay'; nodeId: string } & NodeFeedState)
  | { kind: 'followStatus'; status: FollowStatus }
  /// The live run SSE stream ended or broke before the
  /// run reached a terminal state. `reason` is 'closed' (server
  /// cleanly ended the stream) or 'error' (connection/read failure).
  /// The webview stops presenting the run as live (so it isn't
  /// stuck showing "running" forever) WITHOUT falsely marking nodes
  /// completed: the per-node rows keep their last known state, the
  /// run is just no longer being followed. Distinct from runTerminal
  /// (which IS the run finishing) and from runReset (a fresh follow).
  | { kind: 'followLost'; runId: string; reason: 'closed' | 'error' }
  /// The followed run's stream dropped after it had opened, or could not
  /// be opened again since: the host keeps opening it again past the
  /// last event painted, for as long as the follow lasts, and says why
  /// on every failed attempt. `why: null` when that ends (the stream
  /// opened again, or the follow stopped). The run stays live meanwhile.
  | { kind: 'followReconnecting'; runId: string; why: string | null }
  /// A fresh follow of run `runId` starts: the canvas clears and its
  /// events follow; a value they name is read from this run
  /// (`readValues`).
  | { kind: 'runReset'; runId: string }
  /// Pushed from the host whenever the action-bar state machine
  /// transitions. The webview is a pure renderer that reads the
  /// latest state from this message. State transitions come from
  /// either status fetches (idle/run_running) or live CLI
  /// events (cli_running/error/complete).
  | { kind: 'actionBarState'; state: ActionBarState }
  /// Latest `weft status --json` snapshot. Drives the badges on the
  /// trigger and infra nodes and what the contextual bar offers. Stays
  /// current across cli_running so the badges do not flicker while a
  /// verb is in flight.
  | { kind: 'statusSnapshot'; snapshot: ActionAvailability }
  /// Reply to `storageCall`, correlated by requestId. `result` is the
  /// dispatcher route's JSON response on success; `error` is the failure
  /// reason (HTTP error body or transport fault) on failure.
  | { kind: 'storageResult'; requestId: number; result?: unknown; error?: string }
  /// Reply to `readValues`, correlated by requestId: the dispatcher's
  /// answer (each value, in the order asked), or why it could not be read
  /// at all.
  | ({ kind: 'valuesRead'; requestId: number } & (ValuesAnswer | { error: string }))
  /// Reply to `accessCall`, correlated by requestId. `result` is the
  /// dispatcher route's JSON response on success; `error` the failure
  /// reason. Summaries only: no stored value ever rides this channel.
  | { kind: 'accessResult'; requestId: number; result?: unknown; error?: string }
  /// Reply to `picksCall`, correlated by requestId: the install's picks
  /// (`GET`) or what the change re-armed (`PUT`), or the failure reason.
  | { kind: 'picksResult'; requestId: number; result?: unknown; error?: string }
  /// Reply to `pickAsset`: `paths` are the token paths the field writes
  /// into its `@asset("<path>", <Type>)` refs (a path in place locally,
  /// `assets/<name>` for stored bytes). Empty means the user cancelled;
  /// a single-file field takes the first. `error` on failure.
  | { kind: 'assetPicked'; requestId: number; paths?: string[]; error?: string }
  /// Reply to `listRuntimeFiles`: the STORED runtime files this project's
  /// runs can read (its own `project/` and `instance/` files, and the
  /// tenant's `shared/` ones), keys tenant-less (the short address a
  /// picked ref writes into source). `error` is the failure reason when
  /// the listing itself failed; an empty `files` with no `error`
  /// genuinely means there is no such file.
  | {
      kind: 'runtimeFiles';
      requestId: number;
      files: { key: string; filename: string; mimeType: string; sizeBytes: number }[];
      error?: string;
    };

/// Which run the graph shows, and whether a run that starts takes over:
///   - following: every run that starts takes the canvas.
///   - locked: the canvas stays on one run; runs that start are counted.
///   - off: no run on the canvas; runs that start are counted.
export type FollowMode = 'following' | 'locked' | 'off';

/// What a run is for: a run of the graph (`fire`), or the setup
/// an `infra start` (`infra_setup`) or an activation (`trigger_setup`)
/// runs. The action bar shows a setup as its verb working, never as a
/// run to stop.
// SYNC: RUN_PHASES <-> crates/weft-core/src/primitive.rs Phase
export const RUN_PHASES = ['fire', 'trigger_setup', 'infra_setup'] as const;
export type RunPhase = (typeof RUN_PHASES)[number];

export interface FollowStatus {
  mode: FollowMode;
  runId: string | undefined;
  /// Runs that started while not following, so not shown.
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
  /// run-id prefix so journal values for the sub-graph (keyed
  /// `alias.node`) render when navigated in. The host opens that file's graph
  /// in the same panel and pushes the current view onto a back-stack.
  | { kind: 'openInclude'; path: string; alias: string }
  /// Pop the navigation back-stack (Return button), reopening the previous
  /// file's graph in the panel.
  | { kind: 'navigateBack' }
  | { kind: 'log'; level: 'info' | 'warn' | 'error'; message: string }
  /// Run the selected nodes as targets (each spelled as its place): the
  /// walk takes them and what they need. Never empty: the graph has no
  /// gesture that runs the whole program, the CLI keeps that one.
  | { kind: 'runProject'; targets: [string, ...string[]] }
  /// Ask whether a run of these targets would start, without starting
  /// it (`weft run --check`). Answered by `runChecked`.
  | { kind: 'checkRun'; requestId: number; targets: [string, ...string[]] }
  /// Restore the version of a run or a version id (`weft branch <ref>`).
  | { kind: 'branchTo'; reference: string }
  /// A lifecycle verb on the selected triggers or infra, from the
  /// contextual bar. The host turns it into the CLI's verb and flags.
  | { kind: 'lifecycle'; action: LifecycleAction }
  /// Cancel in-flight infra work (provisioning / stopping /
  /// terminating): HALT, per-node partial state stays visible.
  | { kind: 'infraCancel' }
  /// Cancel the in-flight build (transition=building).
  | { kind: 'cancelBuild' }
  /// Cancel a trigger setup under way (`weft cancel-activate`): the
  /// triggers it has not turned on yet stay off.
  | { kind: 'cancelActivate' }
  /// Take up again a resync that stopped with triggers down, where it
  /// stopped (`weft resync --resume`), with the choice and the build it
  /// began with. Never starts a new resync.
  | { kind: 'resumeResync' }
  /// User clicked the Refresh Status button on the graph header.
  /// Forces a `weft status --json` recheck without waiting for
  /// the file-change debounce. Useful after editing source
  /// outside the IDE or when the user wants to confirm state.
  | { kind: 'refreshStatus' }
  /// The person picked a follow mode on the toolbar toggle (or the
  /// "new runs" chip, which picks `following`).
  | { kind: 'followSetMode'; mode: FollowMode }
  /// Replay a PAST run onto the canvas: the host loads that run's
  /// recorded events and feeds them as the editor's run state (so the
  /// graph shows that run's final node statuses + outputs). `runId` is the
  /// run to replay, or `null` to drop the replay and return to live
  /// follow. A host that surfaces past runs another way (the VS Code
  /// extension has its own history) leaves this unhandled.
  | { kind: 'replayRun'; runId: string | null }
  /// User clicked a diagnostic's file:line:column in the error details
  /// modal. Host opens that file as a text tab beside the graph with the
  /// cursor on the position; a tab already showing the file is revealed
  /// wherever it is instead of a second one being opened.
  | { kind: 'openSource'; location: SourceLocation }
  /// User clicked the action bar's Stop / Cancel affordance. The
  /// host inspects the current ActionBarState to decide:
  ///   - cli_running       -> SIGTERM the spawned CLI process group.
  ///   - run_running -> POST /runs/{runId}/cancel.
  ///   - any other state   -> ignored (button shouldn't be shown).
  | { kind: 'stopAction' }
  /// User pressed a button one of a node's display items carries
  /// (an infra container's "disconnect phone", say). The host POSTs to
  /// `/projects/{id}/infra/nodes/{place}/action`, the node spelled at
  /// this view's place. Only an INFRA node's display has buttons, and
  /// the container serving `/live` serves `/action` too, so the action
  /// `kind` strings are that node author's, not weft's. A press on a
  /// node that is not infra is reported to the user, never dropped.
  /// `confirm`, when set, is the host's prompt text for a VS Code
  /// QuickPick; the press happens only on explicit confirmation.
  | { kind: 'displayAction'; nodeId: string; actionKind: string; payload?: unknown; confirm?: string }
  /// User dismissed the action-bar error banner. The host clears
  /// the slot's `error` field; the bar stops rendering the banner.
  /// Errors otherwise survive auto-refreshes so the user has time
  /// to read them.
  | { kind: 'dismissError' }
  /// User clicked Download on a stored-file value in the replay
  /// inspector. The host POSTs `/storage/files/download` (the
  /// dispatcher authenticates and answers a short-lived presigned link
  /// straight to the object store) and opens that link in the browser,
  /// which saves the file under its name: the store answers it as an
  /// attachment, and the bytes never pass through the dispatcher. A 404
  /// surfaces as "expired or deleted" (the metadata in the value
  /// stays readable; the bytes are gone).
  | { kind: 'downloadStoredFile'; key: string; filename?: string }
  /// Drive one storage-plane verb through the host: the host POSTs `body`
  /// as JSON to the dispatcher's `/storage/<path>` route and replies with a
  /// correlated `storageResult`. This is the ONE channel for storage control
  /// calls (the inline preview's download link); bytes never ride it.
  | { kind: 'storageCall'; requestId: number; path: string; body: unknown }
  /// Read back the values some events of run `runId` name (`refs`, as the
  /// events carry them): the host asks the dispatcher's
  /// `POST /runs/{runId}/values` and replies with a correlated
  /// `valuesRead`. What the inspector and a debug preview show a value by.
  | ({ kind: 'readValues'; requestId: number; runId: string } & ValuesRequest)
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
  /// Read (`GET`) or change (`PUT`) the install's picks for the active
  /// project's own connections (`/projects/{id}/picks`); the host replies
  /// with a correlated `picksResult`.
  | { kind: 'picksCall'; requestId: number; method: 'GET' | 'PUT'; body?: ChangePicks }
  /// Show the project on another install (the install switch): `local`,
  /// or a target the project's `weft.toml` names.
  | { kind: 'switchInstall'; install: string }
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
  /// The file-picker modal asks for the STORED runtime files this project
  /// can use (reply: `runtimeFiles`), listed through the same door
  /// `weft files` uses. Picking one writes `@asset("<scope-key>", <Type>)`
  /// into config.
  | { kind: 'listRuntimeFiles'; requestId: number };

// SYNC: FileValueWire <-> crates/weft-core/src/storage/mod.rs StoredFile
//       (`version` is backend-only: the editor never reads it)
/// The payload INSIDE a concrete file marker: self-describing metadata
/// plus exactly ONE handle saying where the bytes live. A bucket-backed
/// file carries a `key` (its bytes load from the presigned store link
/// `/storage/files/download` answers), and may carry `linkSecs`: how
/// long, in seconds, a link handed out for this value lives, when
/// whoever made the value chose. A file pointing at an external
/// resource carries a `url` (rendered/fetched directly). `data` (inline
/// base64) is deliberately NOT a handle here: the read paths don't
/// resolve it, so it falls through to the raw-JSON rendering instead of
/// a broken preview.
export type FileValueWire = {
  mimeType: string;
  sizeBytes: number;
  filename: string;
} & (
  | { key: string; linkSecs?: number; url?: undefined }
  | { url: string; key?: undefined; linkSecs?: undefined }
);

// SYNC: StoredFileDownload <-> crates/weft-core/src/storage/mod.rs PresignResult
/// What `/storage/files/download` answers: a link to a stored file's
/// bytes (the store answers it as an attachment under `filename`), the
/// file's size, the version it was when the link was made, and whether
/// the open internet opens the link (`false`: only this install's own
/// network does).
export type StoredFileDownload = {
  url: string;
  filename: string;
  sizeBytes: number;
  version: number;
  public: boolean;
};

// SYNC: FileEditWire <-> crates/weft-core/src/storage/mod.rs FileEdit
/// One change a firing made to a stored file: which file, the version it
/// was made from (`null` for an overwrite that never read the file) and
/// the one it produced, and a readable diff already cut to a readable
/// size (changed lines prefixed `+`/`-`, context with a space, `@@`
/// block headers; a file that is not text reads as its old and new size).
export type FileEditWire = {
  key: string;
  filename: string;
  fromVersion: number | null;
  toVersion: number;
  diff: string;
};

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
/// path resolves, and a key-backed one whose `linkSecs` is not a whole
/// number of seconds, which the runtime refuses too). Every consumer
/// (inspector card, node preview) routes through here so the shape is
/// validated identically.
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
  if (typeof p.key === 'string' && p.key !== '') {
    if (p.linkSecs === undefined || p.linkSecs === null) return { key: p.key, ...meta };
    if (typeof p.linkSecs !== 'number' || !Number.isInteger(p.linkSecs) || p.linkSecs < 0) return null;
    return { key: p.key, linkSecs: p.linkSecs, ...meta };
  }
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
  | { op: 'set_config'; node: string; key: string; value: string; form?: 'inline' | 'connection' }
  | { op: 'remove_config'; node: string; key: string; form?: 'inline' | 'connection' }
  | { op: 'set_label'; node: string; label: string | null }
  // Add or remove the node's `@per_instance` line: the node then exists
  // once per instance of the program. Offered only on a node that may
  // carry it (`canBePerInstance`).
  | { op: 'set_per_instance'; node: string; perInstance: boolean }
  | { op: 'add_node'; id: string; nodeType: string; parentGroup: string | null }
  | { op: 'remove_node'; node: string }
  | { op: 'add_edge'; source: string; sourcePort: string; target: string; targetPort: string; scopeGroup: string | null; path?: string[] }
  | { op: 'remove_edge'; source: string; sourcePort: string; target: string; targetPort: string; scopeGroup: string | null }
  | { op: 'add_group'; label: string; parentGroup: string | null }
  | { op: 'remove_group'; group: string }
  | { op: 'rename_group'; group: string; newLabel: string }
  | { op: 'move_node_scope'; node: string; targetGroup: string | null }
  | { op: 'move_group_scope'; group: string; targetGroup: string | null }
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
  // SYNC: EditOp update_node_ports <-> crates/weft-compiler/src/edit.rs EditOp::UpdateNodePorts
  | { op: 'update_node_ports'; node: string; inputs: EditPortSig[]; outputs: EditPortSig[]; removedInputs: string[]; removedOutputs: string[]; revertedInputs: RevertedPortSig[]; revertedOutputs: RevertedPortSig[] }
  | { op: 'update_group_ports'; group: string; inputs: EditPortSig[]; outputs: EditPortSig[] }
  // A group's description is the plain `# ...` comment on its first body line
  // (the single description concept; the old single-file `# Project:` header is
  // dropped, a file's identity is its filename). `description: null` clears it.
  | { op: 'set_group_description'; group: string; description: string | null }
  // Loop ops mirror the Rust EditOp variants.
  | { op: 'add_loop'; label: string; parentGroup: string | null }
  | { op: 'remove_loop'; loopId: string }
  | { op: 'rename_loop'; loopId: string; newLabel: string }
  | { op: 'move_loop_scope'; loopId: string; targetGroup: string | null }
  | { op: 'update_loop_ports'; loopId: string; inputs: EditPortSig[]; outputs: EditPortSig[] }
  // Rewrite which SOURCE FORM a body-set port value is written in
  // ('inline' = a `key: value` braces field, 'connection' = a
  // `node.key = value` statement). The form-toggle marker sends this.
  // SYNC: set_value_form.form <-> crates/weft-compiler/src/edit.rs ValueForm, packages/weft-graph/src/webview/lib/types/index.ts portValueForm.form
  | { op: 'set_value_form'; node: string; key: string; form: 'inline' | 'connection' }
  | { op: 'set_loop_config'; loopId: string; key: string; value: string }
  | { op: 'remove_loop_config'; loopId: string; key: string }
  // Write a group's or a loop's metadata port value: the `self.<port> =
  // <value>` line of its body (`group` is either's id), rewritten in place
  // when it exists. `value` is weft literal source text, the encoding
  // `set_config` uses; null removes the line.
  // SYNC: EditOp set_metadata_value <-> crates/weft-compiler/src/edit.rs EditOp::SetMetadataValue
  | { op: 'set_metadata_value'; group: string; port: string; value: string | null };

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
  /** A metadata port (`*name: Type` in a container's header). Only in a
   *  container's `outputs`, which carries them after the ordinary ones,
   *  the order the header writes them in. Required, like `required` and
   *  `portType`: an omitted flag would turn a metadata port into an
   *  ordinary output in the author's header, so the server refuses one. */
  // SYNC: EditPortSig.metadata <-> crates/weft-compiler/src/edit.rs PortSig.metadata
  metadata: boolean;
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
/// replacements and trailing newlines are unambiguous. `expected` is the text
/// the range holds when the edit was made; applying refuses when it differs,
/// so a stale undo never writes over a text edit.
// SYNC: TextEdit <-> crates/weft-compiler/src/edit.rs TextEdit
export interface TextEdit {
  start: number;
  end: number;
  text: string;
  expected: string;
}
