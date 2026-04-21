// Ported from dashboard-v1/src/lib/types/index.ts (relevant subset).
// Parses Weft port type strings into a structured form, enough to
// pick a display color. Handles recursive syntax: List[T],
// Dict[K, V], unions with |, type variables, MustOverride, Media
// alias.

export type WeftPrimitive =
  | 'String'
  | 'Number'
  | 'Boolean'
  | 'Null'
  | 'Image'
  | 'Video'
  | 'Audio'
  | 'Document'
  | 'Empty';

export const ALL_PRIMITIVE_TYPES: WeftPrimitive[] = [
  'String',
  'Number',
  'Boolean',
  'Null',
  'Image',
  'Video',
  'Audio',
  'Document',
  'Empty',
];

export const MEDIA_TYPES: WeftPrimitive[] = ['Image', 'Video', 'Audio', 'Document'];

export type WeftType =
  | { kind: 'primitive'; value: WeftPrimitive }
  | { kind: 'list'; inner: WeftType }
  | { kind: 'dict'; key: WeftType; value: WeftType }
  | { kind: 'json_dict' }
  | { kind: 'union'; types: WeftType[] }
  | { kind: 'typevar'; name: string }
  | { kind: 'must_override' };

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

function splitTopLevel(s: string, delimiter: string): string[] {
  const parts: string[] = [];
  let depth = 0;
  let start = 0;
  for (let i = 0; i < s.length; i++) {
    if (s[i] === '[') depth++;
    else if (s[i] === ']') depth--;
    else if (s[i] === delimiter && depth === 0) {
      parts.push(s.slice(start, i));
      start = i + 1;
    }
  }
  parts.push(s.slice(start));
  return parts;
}

function parseSingleType(s: string): WeftType | null {
  s = s.trim();
  if (s === 'Media') {
    return {
      kind: 'union',
      types: MEDIA_TYPES.map((t) => ({ kind: 'primitive', value: t })),
    };
  }
  if (s === 'JsonDict') return { kind: 'json_dict' };
  if (s === 'MustOverride') return { kind: 'must_override' };

  const bracketPos = s.indexOf('[');
  if (bracketPos !== -1) {
    if (!s.endsWith(']')) return null;
    const name = s.slice(0, bracketPos).trim();
    const inner = s.slice(bracketPos + 1, -1);

    if (name === 'List') {
      const innerType = parseWeftType(inner);
      return innerType ? { kind: 'list', inner: innerType } : null;
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

  if ((ALL_PRIMITIVE_TYPES as string[]).includes(s)) {
    return { kind: 'primitive', value: s as WeftPrimitive };
  }

  if (isTypeVarName(s)) {
    return { kind: 'typevar', name: s };
  }

  return null;
}

export function parseWeftType(s: string): WeftType | null {
  const trimmed = s.trim();
  if (!trimmed) return null;

  const parts = splitTopLevel(trimmed, '|');
  if (parts.length > 1) {
    const types: WeftType[] = [];
    for (const part of parts) {
      const parsed = parseSingleType(part.trim());
      if (!parsed) return null;
      types.push(parsed);
    }
    const flat: WeftType[] = [];
    for (const t of types) {
      if (t.kind === 'union') flat.push(...t.types);
      else flat.push(t);
    }
    const seen = new Set<string>();
    const deduped: WeftType[] = [];
    for (const t of flat) {
      const key = weftTypeToString(t);
      if (!seen.has(key)) {
        seen.add(key);
        deduped.push(t);
      }
    }
    return deduped.length === 1 ? deduped[0] : { kind: 'union', types: deduped };
  }

  return parseSingleType(trimmed);
}

export function weftTypeToString(t: WeftType): string {
  switch (t.kind) {
    case 'primitive':
      return t.value;
    case 'list':
      return `List[${weftTypeToString(t.inner)}]`;
    case 'dict':
      return `Dict[${weftTypeToString(t.key)}, ${weftTypeToString(t.value)}]`;
    case 'json_dict':
      return 'JsonDict';
    case 'union':
      return t.types.map(weftTypeToString).join(' | ');
    case 'typevar':
      return t.name;
    case 'must_override':
      return 'MustOverride';
  }
}
