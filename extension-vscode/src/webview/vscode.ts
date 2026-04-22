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
