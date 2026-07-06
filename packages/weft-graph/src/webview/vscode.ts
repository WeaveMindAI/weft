// The graph's host bridge: how the editor sends messages to its host and
// receives messages back. Lives in the shared package so any host can reuse one
// graph renderer; the editor talks to whatever host is injected.
//
// The module keeps its original `send` / `onMessage` / `resolveStoredFileUrl`
// surface (so the many components that import them are unchanged), but routes
// through an injectable `HostTransport`. The default transport is the VS Code
// webview API when `acquireVsCodeApi` exists; a non-VS-Code host calls
// `setHostTransport(...)` once, before mount, with a transport backed by the
// dispatcher HTTP API.

import type { HostMessage, WebviewMessage } from '../shared/protocol';

/// The seam each consumer implements: deliver a `WebviewMessage` to the host,
/// and call `receive` for every `HostMessage` the host emits. `dispose` tears
/// down any listeners.
export interface HostTransport {
  post(msg: WebviewMessage): void;
  subscribe(receive: (msg: HostMessage) => void): () => void;
}

let transport: HostTransport | null = null;
// Teardown for the current transport's fan-out subscription, so a transport
// swap can unbind the old one before binding the new.
let transportUnsub: (() => void) | null = null;

/// Inject the host transport. A host outside VS Code calls this once before the
/// editor mounts. Inside VS Code the default is used.
///
/// A single-page web host remounts the editor (and injects a fresh
/// dispatcher-backed transport) every time the user opens a different project,
/// WITHOUT a full page reload. The `send`/`onMessage` fan-out below is module
/// global, so swapping the transport must re-point that fan-out at the new
/// transport: unbind the old subscription and, if the fan-out is already live,
/// rebind it to `t`. Otherwise the editor's listeners stay wired to the previous
/// (now-dead) transport and never see the new project's `parseResult` / action
/// bar / source-toggle echoes (the graph hangs on "loading..." until a reload).
export function setHostTransport(t: HostTransport): void {
  if (transportUnsub) {
    transportUnsub();
    transportUnsub = null;
  }
  transport = t;
  if (subscribed) {
    transportUnsub = t.subscribe(fanOut);
  }
}

interface VsCodeApi {
  postMessage(msg: WebviewMessage): void;
  getState<T = unknown>(): T | undefined;
  setState<T>(state: T): void;
}

declare function acquireVsCodeApi(): VsCodeApi;

/// Lazily build the default VS Code transport. Only acquired when no transport
/// was injected AND `acquireVsCodeApi` exists (the extension webview). The
/// `getState`/`setState` halves are unused by the editor today; only messaging
/// is needed.
function vscodeTransport(): HostTransport {
  const api = acquireVsCodeApi();
  const listeners = new Set<(msg: HostMessage) => void>();
  window.addEventListener('message', (event: MessageEvent) => {
    const msg = event.data as HostMessage;
    for (const l of listeners) l(msg);
  });
  return {
    post(msg) {
      // Svelte 5 $state values are Proxies; structured-clone (the algorithm VS
      // Code's MessagePort uses) can't cross-boundary those, so deep-clone via
      // JSON round-trip. All messages are plain data, so this is safe + cheap.
      api.postMessage(JSON.parse(JSON.stringify(msg)) as WebviewMessage);
    },
    subscribe(receive) {
      listeners.add(receive);
      return () => listeners.delete(receive);
    },
  };
}

function host(): HostTransport {
  if (!transport) {
    transport = vscodeTransport();
  }
  return transport;
}

export function send(msg: WebviewMessage): void {
  host().post(msg);
}

type Listener = (msg: HostMessage) => void;
const listeners = new Set<Listener>();
let subscribed = false;

// The single subscription the fan-out registers on the current transport: it
// dispatches each host message to every `onMessage` listener. Named (not an
// inline closure) so `setHostTransport` can rebind it to a new transport.
function fanOut(msg: HostMessage): void {
  for (const l of listeners) l(msg);
}

function ensureSubscribed(): void {
  if (subscribed) return;
  subscribed = true;
  transportUnsub = host().subscribe(fanOut);
}

export function onMessage(l: Listener): () => void {
  ensureSubscribed();
  listeners.add(l);
  return () => listeners.delete(l);
}

/// Detach the fan-out from the current transport, driving its receiver count to
/// zero so a host that tears down on "no receivers" (the dispatcher HTTP host
/// closes its SSE + pull timer) actually does. The editor App calls this on
/// unmount when no `onMessage` listeners remain: without it the module-global
/// `fanOut` stays subscribed forever, pinning the last-opened project's live
/// connection open after the user navigates away. A later `onMessage`
/// re-subscribes cleanly (`ensureSubscribed` re-arms).
export function teardownTransport(): void {
  if (transportUnsub) {
    transportUnsub();
    transportUnsub = null;
  }
  subscribed = false;
}

// ---- stored-file URL resolution (inline image preview) ----
//
// The image preview can't carry an auth header on an <img src>, so it asks the
// host to run the brokered handshake and hand back a public URL carrying a
// short-lived capability. Correlated by requestId; resolves to a URL or rejects.

let nextStoredFileRequestId = 1;
const pendingStoredFileUrls = new Map<
  number,
  (result: { url: string } | { error: string }) => void
>();

// Stored-file URL replies ride the SAME fan-out as every other host message
// (registered as a normal `onMessage` listener), so they rebind to a new
// transport on swap like everything else. No separate subscription.
let storedFileSubscribed = false;
function ensureStoredFileSubscribed(): void {
  if (storedFileSubscribed) return;
  storedFileSubscribed = true;
  onMessage((msg) => {
    if (msg.kind === 'storedFileUrl') {
      const pending = pendingStoredFileUrls.get(msg.requestId);
      if (pending) {
        pendingStoredFileUrls.delete(msg.requestId);
        pending(msg.url !== undefined ? { url: msg.url } : { error: msg.error ?? 'unavailable' });
      }
    }
  });
}

/// Resolve a stored image to the box's public URL for inline rendering. The host
/// runs the brokered handshake. Rejects with the host's reason (expired/deleted)
/// so the caller can show a fallback.
export function resolveStoredFileUrl(key: string): Promise<string> {
  ensureStoredFileSubscribed();
  const requestId = nextStoredFileRequestId++;
  return new Promise((resolve, reject) => {
    pendingStoredFileUrls.set(requestId, (result) => {
      if ('url' in result) resolve(result.url);
      else reject(new Error(result.error));
    });
    send({ kind: 'resolveStoredFileUrl', key, requestId });
  });
}
