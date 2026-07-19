// Render a config value to its `.weft` source token.
//
// This is presentation, not grammar: the graph holds a config value (string,
// number, bool, object, or a `@file` marker), and an edit needs that value as
// the source text the Rust edit-server will splice in. The webview owns this
// formatting because it owns the value's in-memory shape; the Rust side owns
// WHERE the token goes (spans). The `@file` marker is reconstructed to its
// `@file("path", Type)` source form (config never carries resolved content).

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

/** A weft double-quoted string literal: backslash and quote escaped. Used for
 *  both plain string values and the path inside an `@file(...)` marker, so a
 *  path with a `"` can't produce a malformed literal. */
function quoteString(s: string): string {
  return `"${s.replace(/\\/g, '\\\\').replace(/"/g, '\\"')}"`;
}

/** Inverse of `quoteString` for the inner (unquoted) body of a literal. */
function unquoteString(inner: string): string {
  return inner.replace(/\\(["\\])/g, '$1');
}

/** Format a config value as a `.weft` source token. Single-line scalars become
 *  quoted strings / literals; objects and arrays become pretty-printed JSON;
 *  multi-line strings become triple-backtick heredocs; a `@file` marker becomes
 *  `@file("path"[, Type])` (Type omitted when it's the default String).
 *  SYNC: formatConfigValue <-> crates/weft-compiler/src/edit/ops.rs format_string */
export function formatConfigValue(value: unknown): string {
  // Unset is a removeConfig, not a token: an emitted `null`/`undefined` would
  // splice garbage into source (or, for undefined, break the string contract).
  // Fail loudly at the source so the caller routes it correctly.
  if (value === null || value === undefined) {
    throw new Error('config value is unset; emit a removeConfig, not a token');
  }
  if (isFileRefValue(value)) {
    const { path, type, marker } = value.__weftFileRef;
    const p = quoteString(path);
    const directive = marker === 'asset' ? '@asset' : '@file';
    return type === 'String' ? `${directive}(${p})` : `${directive}(${p}, ${type})`;
  }
  if (typeof value === 'string') {
    if (value.includes('\n')) {
      // A multi-line value is a ```...``` heredoc. The weft lexer has NO escape
      // for an inner fence (it ends the heredoc at the first ```), so a value
      // containing ``` cannot be encoded faithfully: throw, exactly as the Rust
      // edit-server does, rather than emit source that re-parses wrong.
      if (value.includes('```')) {
        throw new Error('multi-line value cannot contain ``` (no heredoc fence escape)');
      }
      return `\`\`\`\n${value}\n\`\`\``;
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
 *  SYNC: parseConfigToken <-> crates/weft-compiler/src/edit/ops.rs format_string (it inverts what format_string emits) */
export function parseConfigToken(token: string): unknown {
  // The path group accepts escaped chars so a `"` inside the path round-trips.
  const fileRef = token.match(/^@(file|asset)\("((?:[^"\\]|\\.)*)"(?:,\s*([A-Za-z][A-Za-z0-9_[\],| ]*))?\)$/);
  if (fileRef) {
    return {
      __weftFileRef: {
        path: unquoteString(fileRef[2]),
        type: fileRef[3] ?? 'String',
        marker: fileRef[1] as 'file' | 'asset',
      },
    } satisfies WeftFileRefValue;
  }
  if (token.startsWith('```')) {
    // Require a real closing fence: an unterminated heredoc is not a token
    // formatConfigValue can produce.
    if (!token.endsWith('```') || token.length < 6) {
      throw new Error(`unterminated heredoc config token: ${token.slice(0, 40)}`);
    }
    // No fence escape exists (formatConfigValue throws on an inner ```), so the
    // body is taken verbatim between the fences.
    return token.replace(/^```\n?/, '').replace(/\n?```$/, '');
  }
  if (token.startsWith('"') && token.endsWith('"') && token.length >= 2) {
    return unquoteString(token.slice(1, -1));
  }
  if (token === 'true') return true;
  if (token === 'false') return false;
  if (/^-?\d+(\.\d+)?([eE][+-]?\d+)?$/.test(token)) return Number(token);
  if (token.startsWith('{') || token.startsWith('[')) return JSON.parse(token);
  throw new Error(`not a config value token: ${token.slice(0, 40)}`);
}
