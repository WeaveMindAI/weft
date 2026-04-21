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
  api.postMessage(msg);
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
