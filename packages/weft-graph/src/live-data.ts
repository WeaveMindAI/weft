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
///   - surface.path      -> for public_entry, the mount path
///   - auth.kind         -> "none" / "api_key"
///   - auth.header_name  -> for api_key
///   - secret            -> plaintext, only while the listener still holds a
///                          freshly-minted key
export function signalDisplayToLiveItems(body: Record<string, unknown>): LiveDataItem[] {
  const items: LiveDataItem[] = [];
  const surface = body.surface as Record<string, unknown> | undefined;
  if (surface && surface.kind === 'public_entry') {
    const path = typeof surface.path === 'string' ? surface.path : '';
    items.push({
      type: 'text',
      label: 'Path',
      data: path === '' ? '/' : `/${path.replace(/^\//, '')}`,
    });
  }
  const auth = body.auth as Record<string, unknown> | undefined;
  if (auth && auth.kind === 'api_key') {
    const header = typeof auth.header_name === 'string' ? auth.header_name : 'X-Api-Key';
    items.push({ type: 'text', label: 'Auth header', data: header });
  } else if (auth && auth.kind === 'none') {
    items.push({ type: 'text', label: 'Auth', data: 'public (no key)' });
  }
  if (typeof body.secret === 'string' && body.secret.length > 0) {
    items.push({
      type: 'secret',
      label: 'API key',
      data: body.secret,
      action: {
        label: 'Regenerate',
        actionKind: 'regenerate_api_key',
        confirm: 'Regenerate the API key? The current key will stop working.',
      },
    });
  } else if (auth && auth.kind === 'api_key') {
    // Auth is api_key but the listener doesn't hold plaintext (pod restarted,
    // original mint dropped). Show a placeholder with the regenerate button so
    // the user can recover.
    items.push({
      type: 'text',
      label: 'API key',
      data: '(hidden by listener restart; click Regenerate to mint a new one)',
      action: {
        label: 'Regenerate',
        actionKind: 'regenerate_api_key',
        confirm: 'Mint a new API key? Replaces any current key.',
      },
    });
  }
  return items;
}
