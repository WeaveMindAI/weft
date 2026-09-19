// Shared transforms + guards for the node body feeds (`infraLive` /
// `signalDisplay`). Pure functions over untyped backend JSON, so BOTH hosts
// (the VS Code extension and a browser-based host) render the exact same items
// and can never fork on the validation or the display mapping.

import type { LiveDataItem } from './protocol';

/// Allowed live-data kinds. New kinds: add the string here AND a render branch
/// in ProjectNode.svelte. The type guard rejects anything else so a malformed
/// payload never reaches the renderer.
const LIVE_DATA_TYPES = ['text', 'image', 'progress', 'secret'] as const;
type LiveDataType = (typeof LIVE_DATA_TYPES)[number];

function isLiveDataType(v: unknown): v is LiveDataType {
  return typeof v === 'string' && (LIVE_DATA_TYPES as readonly string[]).includes(v);
}

/// Validate one item from an infra `/live` payload before it reaches the
/// renderer. A backend that returns a malformed item is dropped, never trusted.
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

/// Convert the listener's signal `/display` JSON into the `LiveDataItem[]` the
/// trigger node body panel renders. The listener returns a free-form blob; the
/// inspector knows a few standard fields:
///   - surface.kind      -> "public_entry" / "task_callback"
///   - surface.path      -> for public_entry, the route pattern (`chat/{room}`)
///   - surface.methods   -> for public_entry, the HTTP methods served (empty = any)
///   - auth.kind         -> "none" / "connection" (a stored connection the
///                          gateway checks callers against; the key material
///                          lives on the connection, never here)
export function signalDisplayToLiveItems(body: Record<string, unknown>): LiveDataItem[] {
  const items: LiveDataItem[] = [];
  const surface = body.surface as Record<string, unknown> | undefined;
  if (!surface || surface.kind !== 'public_entry') {
    // Only a public entry has anything worth a body panel: a URL an
    // outside caller hits, and the auth that gates it. Task-callback
    // and internal signals (timers, provider pushes, bridge webhooks)
    // carry `auth: none` in their routing too, but there is no caller
    // to gate, so "open" would be noise. No items: the renderer hides
    // the panel entirely.
    return items;
  }
  const path = typeof surface.path === 'string' ? surface.path : '';
  const methods = Array.isArray(surface.methods)
    ? surface.methods.filter((m): m is string => typeof m === 'string')
    : [];
  const route = path === '' ? '/' : `/${path.replace(/^\//, '')}`;
  items.push({
    type: 'text',
    label: 'Route',
    data: methods.length === 0 ? route : `${methods.join('/')} ${route}`,
  });
  const auth = body.auth as Record<string, unknown> | undefined;
  if (auth && auth.kind === 'connection') {
    items.push({ type: 'text', label: 'Auth', data: 'gated by the wired auth connection' });
  } else if (auth && auth.kind === 'none') {
    items.push({ type: 'text', label: 'Auth', data: 'open (anyone with the URL)' });
  }
  return items;
}
