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

// ---- host request/reply calls ----
//
// Several webview features ask the host something and await a correlated
// reply (the storage handshake, the asset picker, the project-file listing).
// ONE registry correlates them all: a reply is matched by (kind, requestId),
// riding the same fan-out as every other host message so it rebinds on a
// transport swap like everything else.

let nextRequestId = 1;
const pendingReplies = new Map<string, (msg: HostMessage) => void>();
let repliesSubscribed = false;

function ensureRepliesSubscribed(): void {
  if (repliesSubscribed) return;
  repliesSubscribed = true;
  onMessage((msg) => {
    const requestId = (msg as { requestId?: number }).requestId;
    if (requestId === undefined) return;
    const key = `${msg.kind}:${requestId}`;
    const pending = pendingReplies.get(key);
    if (pending) {
      pendingReplies.delete(key);
      pending(msg);
    }
  });
}

/// Send one request-shaped message and await its correlated reply of
/// `replyKind`. The single primitive every host request/reply pair builds on.
function hostRequest<K extends HostMessage['kind']>(
  replyKind: K,
  build: (requestId: number) => WebviewMessage,
): Promise<Extract<HostMessage, { kind: K }>> {
  ensureRepliesSubscribed();
  const requestId = nextRequestId++;
  return new Promise((resolve) => {
    pendingReplies.set(`${replyKind}:${requestId}`, (msg) => {
      resolve(msg as Extract<HostMessage, { kind: K }>);
    });
    send(build(requestId));
  });
}

/// Drive one dispatcher storage verb through the host. `path` is the route
/// under `/storage/` (e.g. `files/download`); `body` is the JSON payload.
/// Resolves to the route's parsed response, or rejects with the host's
/// failure reason. Module-private: consumers go through the specific
/// wrappers (`resolveStoredFileUrl`), which name their contract.
async function storageCall<T = unknown>(path: string, body: unknown): Promise<T> {
  const reply = await hostRequest('storageResult', (requestId) => ({
    kind: 'storageCall',
    requestId,
    path,
    body,
  }));
  if (reply.error !== undefined) throw new Error(reply.error);
  return reply.result as T;
}

/// Resolve a stored image to the box's public URL for inline rendering. Runs
/// the download handshake through `storageCall`. Rejects with the host's reason
/// (expired/deleted) so the caller can show a fallback.
export async function resolveStoredFileUrl(key: string): Promise<string> {
  const r = await storageCall<{ url?: string }>('files/download', { key });
  if (!r?.url) throw new Error('stored file unavailable');
  return r.url;
}

/// Ask the host to produce an asset-ref path for the file-drop field: with
/// `dropped` the host stores the bytes as `assets/<name>` and returns that
/// path; without, it runs its own picker (VS Code: the native dialog, whose
/// pick is referenced in place). Resolves to the path, `null` on user cancel,
/// rejects on failure.
export async function pickAsset(
  accept: string | undefined,
  dropped?: { name: string; bytesBase64: string },
): Promise<string | null> {
  const reply = await hostRequest('assetPicked', (requestId) => ({
    kind: 'pickAsset',
    requestId,
    accept,
    dropped,
  }));
  if (reply.error !== undefined) throw new Error(reply.error);
  return reply.path ?? null;
}

/// The project's STORED runtime files (for the "pick a stored file" picker):
/// tenant-less keys + display metadata.
export async function listRuntimeFiles(): Promise<
  { key: string; filename: string; mimeType: string; sizeBytes: number }[]
> {
  const reply = await hostRequest('runtimeFiles', (requestId) => ({
    kind: 'listRuntimeFiles',
    requestId,
  }));
  if (reply.error !== undefined) throw new Error(reply.error);
  return reply.files;
}
