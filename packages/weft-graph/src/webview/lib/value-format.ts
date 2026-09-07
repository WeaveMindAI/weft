// Render a config value to its `.weft` source token.
//
// This is presentation, not grammar: the graph holds a config value (string,
// number, bool, object, or a `@file` marker), and an edit needs that value as
// the source text the Rust edit-server will splice in. The webview owns this
// formatting because it owns the value's in-memory shape; the Rust side owns
// WHERE the token goes (spans). The `@file` marker is reconstructed to its
// `@file("path", Type)` source form (config never carries resolved content).

import { parseWeftType } from '../../protocol';

/** Structural `@file` / `@asset` reference held in a config field. The value
 *  the field resolves to lives elsewhere (host-supplied file content, or the
 *  build's asset resolution); config holds only this marker, so no path can
 *  serialize resolved content into source. `marker` says which directive it
 *  serializes back to (and therefore the edit contract; see protocol FileRef). */
export interface WeftFileRefValue {
  __weftFileRef: { path: string; type: string; marker: 'file' | 'asset' };
}

export function isFileRefValue(v: unknown): v is WeftFileRefValue {
  const r = (v as WeftFileRefValue | null)?.__weftFileRef;
  return typeof r === 'object' && r !== null && typeof r.path === 'string'
    && typeof r.type === 'string' && (r.marker === 'file' || r.marker === 'asset');
}

/** A weft double-quoted string literal. The grammar's string is JSON's, and
 *  the Rust side writes a marker path with `serde_json::to_string` and reads
 *  it back with `from_str`, so `JSON.stringify` is the same function on this
 *  side: a tab, a newline or a non-BMP character survives the round trip
 *  instead of arriving as the two characters `\` `t`.
 *  SYNC: quoteString/unquoteString <-> crates/weft-compiler/src/file_ref.rs (marker_text, unquote) */
function quoteString(s: string): string {
  return JSON.stringify(s);
}

/** Inverse of `quoteString` for the inner (unquoted) body of a literal.
 *  Returns null for a body no weft string literal could have held. */
function unquoteString(inner: string): string | null {
  try {
    const parsed: unknown = JSON.parse(`"${inner}"`);
    return typeof parsed === 'string' ? parsed : null;
  } catch {
    return null;
  }
}

/** The one rule for every text-shaped control (text/textarea/password
 *  boxes, code editors): an emptied box means UNSET (null, routed to a
 *  removeConfig / cleared literal), never a stored empty string. Kept
 *  here, next to the unset-vs-token boundary, so the controls cannot
 *  drift apart on it. */
export function emptyToUnset(value: string): string | null {
  return value === '' ? null : value;
}

/** Split a list body on its TOP-LEVEL commas, so a comma inside a
 *  marker's type (`Dict[String, Number]`) or inside a quoted path does not
 *  cut an element in half. */
function splitTopLevel(inner: string): string[] {
  const parts: string[] = [];
  let depth = 0;
  let inString = false;
  let start = 0;
  for (let i = 0; i < inner.length; i++) {
    const ch = inner[i];
    if (inString) {
      if (ch === '\\') i++;
      else if (ch === '"') inString = false;
      continue;
    }
    if (ch === '"') inString = true;
    else if (ch === '(' || ch === '[' || ch === '{') depth++;
    else if (ch === ')' || ch === ']' || ch === '}') depth--;
    else if (ch === ',' && depth === 0) {
      parts.push(inner.slice(start, i));
      start = i + 1;
    }
  }
  parts.push(inner.slice(start));
  return parts;
}

/** One `@file` / `@asset` marker in its source form. Shared by the single
 *  value and by every element of a list of them. */
function formatFileRef(value: WeftFileRefValue): string {
  const { path, type, marker } = value.__weftFileRef;
  const p = quoteString(path);
  // `@asset` always names its type (the compiler never guesses a kind);
  // `@file` defaults to String and omits it.
  if (marker === 'asset') return `@asset(${p}, ${type})`;
  return type === 'String' ? `@file(${p})` : `@file(${p}, ${type})`;
}

/** The file ref a `@file(...)` / `@asset(...)` token names, or null when
 *  the text is not one. A port's written value arrives as this text (the
 *  parse leaves a marker as its own source), so the control reads its
 *  current files through here. */
export function fileRefFromToken(token: string): WeftFileRefValue | null {
  // The path group accepts escaped chars so a `"` inside the path round-trips.
  // The type runs to the closing paren and is checked by the type parser
  // rather than a character class: a named type's wire form (`Name=Body`)
  // carries `=`, `{`, `:` and parentheses around a union body. Whitespace
  // inside the arg list is tolerated, as the compiler's own parse tolerates it.
  // SYNC: fileRefFromToken <-> crates/weft-compiler/src/file_ref.rs (parse_marker, marker_text)
  const m = token.trim().match(/^@(file|asset)\(\s*"((?:[^"\\]|\\.)*)"\s*(?:,\s*(.+?))?\s*\)$/s);
  if (!m) return null;
  const marker = m[1] as 'file' | 'asset';
  // `@asset` never guesses a kind, so a typeless one is not a marker the
  // compiler would accept; inventing String here showed a value for a token
  // the server rejects. `@file` defaults to String, as the grammar does.
  if (marker === 'asset' && m[3] === undefined) return null;
  const type = m[3]?.trim() ?? 'String';
  if (parseWeftType(type) === null) return null;
  const path = unquoteString(m[2]);
  if (path === null) return null;
  return { __weftFileRef: { path, type, marker } };
}

/** Every file a written value names: one for a lone marker, several for a
 *  list of them, none for anything else. Accepts both the structural ref
 *  shape (what the host bridge hands back for a file-backed field) and the
 *  raw marker text (what a port literal carries). */
export function fileRefsOf(value: unknown): WeftFileRefValue[] {
  const one = (v: unknown): WeftFileRefValue | null =>
    isFileRefValue(v) ? v : typeof v === 'string' ? fileRefFromToken(v) : null;
  const single = one(value);
  if (single) return [single];
  if (!Array.isArray(value)) return [];
  const refs = value.map(one);
  return refs.every((r): r is WeftFileRefValue => r !== null) ? refs : [];
}

/** Format a config value as a `.weft` source token. Single-line scalars become
 *  quoted strings / literals; objects and arrays become pretty-printed JSON;
 *  multi-line strings become triple-backtick heredocs; a `@file` marker becomes
 *  `@file("path"[, Type])` (Type omitted when it's the default String).
 *  SYNC: formatConfigValue <-> crates/weft-compiler/src/edit/ops.rs format_string, crates/weft-compiler/src/weft_compiler.rs unescape_heredoc, crates/weft-compiler/src/cst/lexer.rs heredoc_span */
export function formatConfigValue(value: unknown): string {
  // Unset is a removeConfig, not a token: an emitted `null`/`undefined` would
  // splice garbage into source (or, for undefined, break the string contract).
  // Fail loudly at the source so the caller routes it correctly.
  if (value === null || value === undefined) {
    throw new Error('config value is unset; emit a removeConfig, not a token');
  }
  if (isFileRefValue(value)) {
    return formatFileRef(value);
  }
  // A port that holds several files: one marker per file, in order. A
  // marker is a value, so the list is written like any other list.
  if (Array.isArray(value) && value.length > 0 && value.every(isFileRefValue)) {
    return `[${value.map(formatFileRef).join(', ')}]`;
  }
  if (typeof value === 'string') {
    if (value.includes('\n')) {
      // A multi-line value is a ```...``` heredoc. Content is verbatim
      // between the fences; an inner ``` is escaped as \``` (the one
      // escape the decoder honors), so only the escape's own literal
      // spelling is unencodable. Throws exactly as the Rust edit-server.
      if (value.includes('\\```')) {
        throw new Error('multi-line value cannot contain the sequence \\``` (it is the heredoc\'s fence escape)');
      }
      return `\`\`\`\n${value.replaceAll('```', '\\```')}\n\`\`\``;
    }
    return quoteString(value);
  }
  if (typeof value === 'number') {
    // Non-finite numbers have no weft literal (JSON.stringify would emit a bare
    // `null` token that parseConfigToken rejects); fail loudly here instead.
    if (!Number.isFinite(value)) {
      throw new Error(`config number is not finite (${value}); no weft literal exists`);
    }
    return String(value);
  }
  if (typeof value === 'boolean') {
    return String(value);
  }
  // Objects / arrays: compact when small, pretty multi-line when large. Pure
  // STYLE, not correctness: the grammar parses both forms in every value
  // position (pinned by crates/weft-compiler/tests/parser_multiline_object.rs)
  // and the structured editor accepts both (its containment gate admits
  // newlines inside a balanced brace-run). The proxy keeps a small value (a
  // two-key marker, a short list) on one readable line and lets a big one (a
  // form schema, a stored-file marker with long fields) breathe across lines.
  const compact = JSON.stringify(value);
  return compact.length <= JSON_COMPACT_MAX_CHARS ? compact : JSON.stringify(value, null, 2);
}

/// Above this many characters, a JSON config value is written pretty
/// (multi-line) instead of compact: roughly "does it still read as one line".
const JSON_COMPACT_MAX_CHARS = 60;

/** Parse a `.weft` source token back to its in-memory config value: the exact
 *  inverse of `formatConfigValue`, kept next to it so the two can't drift.
 *  Used by the optimistic projection to display a pending `setConfig` op's
 *  value before the host round-trip lands. Throws on a token this module
 *  could not have produced (the projection drops the op loudly).
 *  SYNC: parseConfigToken <-> crates/weft-compiler/src/edit/ops.rs format_string (it inverts what format_string emits), crates/weft-compiler/src/weft_compiler.rs unescape_heredoc, crates/weft-compiler/src/cst/lexer.rs heredoc_span */
export function parseConfigToken(token: string): unknown {
  const fileRef = fileRefFromToken(token);
  if (fileRef) return fileRef;
  // A list of markers: what a port holding several files writes.
  if (token.startsWith('[') && token.includes('@')) {
    const inner = token.slice(1, -1).trim();
    const parts = inner ? splitTopLevel(inner) : [];
    const refs = parts.map((part) => fileRefFromToken(part.trim()));
    if (refs.length > 0 && refs.every((r): r is WeftFileRefValue => r !== null)) {
      return refs;
    }
  }
  if (token.startsWith('```')) {
    // Require a real closing fence: an unterminated heredoc is not a token
    // formatConfigValue can produce.
    if (!token.endsWith('```') || token.length < 6) {
      throw new Error(`unterminated heredoc config token: ${token.slice(0, 40)}`);
    }
    // Body is verbatim between the fences (one syntax newline stripped
    // each side); the one escape is \``` for an inner fence.
    return token.replace(/^```\n?/, '').replace(/\n?```$/, '').replaceAll('\\```', '```');
  }
  if (token.startsWith('"') && token.endsWith('"') && token.length >= 2) {
    const text = unquoteString(token.slice(1, -1));
    if (text === null) throw new Error(`not a string literal: ${token.slice(0, 40)}`);
    return text;
  }
  if (token === 'true') return true;
  if (token === 'false') return false;
  if (/^-?\d+(\.\d+)?([eE][+-]?\d+)?$/.test(token)) return Number(token);
  if (token.startsWith('{') || token.startsWith('[')) return JSON.parse(token);
  throw new Error(`not a config value token: ${token.slice(0, 40)}`);
}
