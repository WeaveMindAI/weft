// Thin HTTP client for the dispatcher.

import type { Diagnostic, ParseResponse } from './shared/protocol';

/** Minimal SSE subscription handle. `EventSource` is a browser global
 *  that VS Code's Node-based extension host doesn't ship, so we roll a
 *  tiny client on top of `fetch` + ReadableStream (Node 18+). We only
 *  parse `data:` lines (no `event:` / `id:` / `retry:` yet) because
 *  the dispatcher only emits `data:` and blank-line delimiters. */
export interface SseSubscription {
    close: () => void;
}

function subscribeSse(
    url: string,
    onData: (data: string) => void,
    onError?: (err: unknown) => void,
): SseSubscription {
    const controller = new AbortController();
    let closed = false;
    (async () => {
        try {
            const res = await fetch(url, {
                headers: { accept: 'text/event-stream' },
                signal: controller.signal,
            });
            if (!res.ok || !res.body) {
                throw new Error(`SSE ${url}: ${res.status}`);
            }
            const reader = res.body.getReader();
            const decoder = new TextDecoder();
            let buf = '';
            while (!closed) {
                const { value, done } = await reader.read();
                if (done) break;
                buf += decoder.decode(value, { stream: true });
                // Dispatch every complete event (terminated by blank line).
                let sep: number;
                while ((sep = buf.indexOf('\n\n')) !== -1) {
                    const raw = buf.slice(0, sep);
                    buf = buf.slice(sep + 2);
                    const dataLines = raw
                        .split('\n')
                        .filter((l) => l.startsWith('data:'))
                        .map((l) => l.slice(5).replace(/^ /, ''));
                    if (dataLines.length > 0) onData(dataLines.join('\n'));
                }
            }
        } catch (err) {
            if (!closed) onError?.(err);
        }
    })();
    return {
        close: () => {
            closed = true;
            controller.abort();
        },
    };
}

export class DispatcherClient {
  constructor(private baseUrl: string) {}

  setBaseUrl(url: string) {
    this.baseUrl = url;
  }

  async get<T>(path: string): Promise<T> {
    const res = await fetch(`${this.baseUrl}${path}`);
    if (!res.ok) throw new Error(`GET ${path}: ${res.status}`);
    return (await res.json()) as T;
  }

  async post<T>(path: string, body: unknown): Promise<T> {
    const res = await fetch(`${this.baseUrl}${path}`, {
      method: 'POST',
      headers: { 'content-type': 'application/json' },
      body: JSON.stringify(body),
    });
    if (!res.ok) throw new Error(`POST ${path}: ${res.status}`);
    const text = await res.text();
    return (text ? JSON.parse(text) : ({} as unknown)) as T;
  }

  async del(path: string): Promise<void> {
    const res = await fetch(`${this.baseUrl}${path}`, { method: 'DELETE' });
    if (!res.ok && res.status !== 204) throw new Error(`DELETE ${path}: ${res.status}`);
  }

  subscribe(path: string, onEvent: (ev: { data: string }) => void): SseSubscription {
    return subscribeSse(`${this.baseUrl}${path}`, (data) => onEvent({ data }), (err) => {
      console.warn('[weft/dispatcher] SSE subscription failed:', err);
    });
  }

  async parse(source: string, projectId?: string): Promise<ParseResponse> {
    return this.post<ParseResponse>('/parse', {
      source,
      project_id: projectId,
    });
  }

  async validate(source: string, projectId?: string): Promise<{ diagnostics: Diagnostic[] }> {
    return this.post<{ diagnostics: Diagnostic[] }>('/validate', {
      source,
      project_id: projectId,
    });
  }
}
