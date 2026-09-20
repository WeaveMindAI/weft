// The guard on a node's display, shared by every host so they cannot
// fork on what a renderable item is.
//
// Both producers (an infra node's container, the listener kind holding
// a trigger's signal) serve the same shape on `/live`, and the
// dispatcher already reads it into `weft_core::live::LiveFeed` on the
// way through. This is the last check before the renderer, over JSON
// that crossed a process boundary.

import type { LiveDataItem, LiveDataType } from './protocol';
import { LIVE_DATA_TYPES } from './protocol';

function isLiveDataType(v: unknown): v is LiveDataType {
  return typeof v === 'string' && (LIVE_DATA_TYPES as readonly string[]).includes(v);
}

/// Validate one item of a node's display before it reaches the
/// renderer. A malformed item is never trusted as is: the host turns
/// one this refuses into a visible "Unreadable item" line, so nothing
/// a node meant to show goes missing quietly.
export function isLiveDataItem(v: unknown): v is LiveDataItem {
  if (!v || typeof v !== 'object') return false;
  const o = v as Record<string, unknown>;
  if (typeof o.label !== 'string') return false;
  if (typeof o.data !== 'string' && typeof o.data !== 'number') return false;
  if (!isLiveDataType(o.type)) return false;
  if (o.action !== undefined) {
    if (!o.action || typeof o.action !== 'object') return false;
    const a = o.action as Record<string, unknown>;
    if (typeof a.label !== 'string') return false;
    if (typeof a.actionKind !== 'string') return false;
  }
  return true;
}
