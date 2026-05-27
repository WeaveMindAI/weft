// Render a config value to its `.weft` source token.
//
// This is presentation, not grammar: the graph holds a config value (string,
// number, bool, object, or a `@file` marker), and an edit needs that value as
// the source text the Rust edit-server will splice in. The webview owns this
// formatting because it owns the value's in-memory shape; the Rust side owns
// WHERE the token goes (spans). The `@file` marker is reconstructed to its
// `@file("path", Type)` source form (config never carries resolved content).

/** Structural `@file` reference held in a config field. The value the field
 *  resolves to lives elsewhere (host-supplied file content); config holds only
 *  this marker, so no path can serialize resolved content into source. */
export interface WeftFileRefValue {
  __weftFileRef: { path: string; type: string };
}

export function isFileRefValue(v: unknown): v is WeftFileRefValue {
  const r = (v as WeftFileRefValue | null)?.__weftFileRef;
  return typeof r === 'object' && r !== null && typeof r.path === 'string' && typeof r.type === 'string';
}

/** Format a config value as a `.weft` source token. Single-line scalars become
 *  quoted strings / literals; objects and arrays become pretty-printed JSON;
 *  multi-line strings become triple-backtick heredocs; a `@file` marker becomes
 *  `@file("path"[, Type])` (Type omitted when it's the default String). */
export function formatConfigValue(value: unknown): string {
  if (isFileRefValue(value)) {
    const { path, type } = value.__weftFileRef;
    return type === 'String' ? `@file("${path}")` : `@file("${path}", ${type})`;
  }
  if (typeof value === 'string') {
    if (value.includes('\n')) {
      const escaped = value.replace(/```/g, '\\```');
      return `\`\`\`\n${escaped}\n\`\`\``;
    }
    return `"${value.replace(/\\/g, '\\\\').replace(/"/g, '\\"')}"`;
  }
  if (typeof value === 'boolean' || typeof value === 'number') {
    return String(value);
  }
  // Objects / arrays: pretty-printed multi-line JSON. Multi-line form parses in
  // every context (canonical nodes, anons after expansion); compact JSON does
  // not parse inside one-liner anon bodies.
  return JSON.stringify(value, null, 2);
}
