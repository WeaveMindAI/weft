// Port type → display color, ported from
// dashboard-v1/src/lib/constants/colors.ts. Used by port-marker for
// handle fill/outline, and by ProjectNode headers/accent bars.

import { parseWeftType, type WeftType } from './weft-type';

export const PORT_TYPE_COLORS: Record<string, string> = {
  String: '#6b7280',
  Number: '#5a9eb8',
  Boolean: '#b05574',
  Null: '#a1a1aa',
  Image: '#c4a35a',
  Video: '#8b6fc0',
  Audio: '#4a9e6f',
  Document: '#9e7c5a',
  List: '#5a8a8a',
  Dict: '#7c6f9f',
  TypeVar: '#6366f1',
  MustOverride: '#ef4444',
};

export const FALLBACK_COLOR = '#52525b';

function colorForParsed(t: WeftType): string {
  switch (t.kind) {
    case 'primitive':
      return PORT_TYPE_COLORS[t.value] ?? FALLBACK_COLOR;
    case 'list':
      return PORT_TYPE_COLORS.List;
    case 'dict':
      return PORT_TYPE_COLORS.Dict;
    case 'json_dict':
      return PORT_TYPE_COLORS.Dict;
    case 'union':
      return colorForParsed(t.types[0]);
    case 'typevar':
      return PORT_TYPE_COLORS.TypeVar;
    case 'must_override':
      return PORT_TYPE_COLORS.MustOverride;
  }
}

export function getPortTypeColor(portType: string): string {
  if (!portType) return FALLBACK_COLOR;
  if (PORT_TYPE_COLORS[portType]) return PORT_TYPE_COLORS[portType];
  if (portType === 'Media') return PORT_TYPE_COLORS.Image;
  const parsed = parseWeftType(portType);
  return parsed ? colorForParsed(parsed) : FALLBACK_COLOR;
}
