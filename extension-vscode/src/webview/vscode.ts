// Thin wrapper around acquireVsCodeApi(). Singleton; the API may
// only be acquired once per webview.

import type { HostMessage, WebviewMessage } from '../shared/protocol';

interface VsCodeApi {
  postMessage(msg: WebviewMessage): void;
  getState<T = unknown>(): T | undefined;
  setState<T>(state: T): void;
}

declare function acquireVsCodeApi(): VsCodeApi;

const api = acquireVsCodeApi();

export function send(msg: WebviewMessage): void {
  // Svelte 5 $state values are Proxies. structured-clone (the
  // algorithm VS Code's webview MessagePort uses) can't cross-boundary
  // those, so we deep-clone the payload via JSON round-trip. All our
  // messages are plain data (no Dates, Maps, functions), so this is
  // both safe and cheap.
  api.postMessage(JSON.parse(JSON.stringify(msg)) as WebviewMessage);
}

type Listener = (msg: HostMessage) => void;
const listeners = new Set<Listener>();

window.addEventListener('message', (event: MessageEvent) => {
  const msg = event.data as HostMessage;
  for (const l of listeners) l(msg);
});

export function onMessage(l: Listener): () => void {
  listeners.add(l);
  return () => listeners.delete(l);
}

// ---- stored-file URL resolution (inline image preview) ----
//
// The image preview can't carry an auth header on an <img src>, so it
// asks the host to run the brokered handshake and hand back a public
// URL carrying a short-lived capability. The browser then fetches the
// bytes directly from the storage box, exactly like a user download.
// Correlated by requestId; resolves to a URL or null (expired/deleted).

let nextStoredFileRequestId = 1;
const pendingStoredFileUrls = new Map<
  number,
  (result: { url: string } | { error: string }) => void
>();

window.addEventListener('message', (event: MessageEvent) => {
  const msg = event.data as HostMessage;
  if (msg.kind === 'storedFileUrl') {
    const pending = pendingStoredFileUrls.get(msg.requestId);
    if (pending) {
      pendingStoredFileUrls.delete(msg.requestId);
      pending(msg.url !== undefined ? { url: msg.url } : { error: msg.error ?? 'unavailable' });
    }
  }
});

/// Resolve a stored image to the box's public URL for inline
/// rendering (an <img> streams directly from the box; the CSP admits
/// the storage origin). The host runs the brokered handshake. Rejects
/// with the host's reason (e.g. expired/deleted) so the caller can
/// show a fallback.
export function resolveStoredFileUrl(key: string): Promise<string> {
  const requestId = nextStoredFileRequestId++;
  return new Promise((resolve, reject) => {
    pendingStoredFileUrls.set(requestId, (result) => {
      if ('url' in result) resolve(result.url);
      else reject(new Error(result.error));
    });
    send({ kind: 'resolveStoredFileUrl', key, requestId });
  });
}
