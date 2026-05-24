// Thin HTTP client for the dispatcher.

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

/// Thrown by `DispatcherClient` for any non-2xx response. Carries
/// the status code (so callers can match, e.g. a 404 hint) AND the
/// response body, which is where the dispatcher puts its actual
/// reason (e.g. "project is already activating; wait or weft
/// deactivate"). The message surfaces the body when present so the
/// user sees the reason, not just "POST /path: 409".
export class HttpError extends Error {
  constructor(
    public readonly method: string,
    public readonly path: string,
    public readonly status: number,
    public readonly body?: string,
  ) {
    const reason = body && body.trim() ? body.trim() : `${status}`;
    super(`${method} ${path}: ${reason}`);
    this.name = 'HttpError';
  }
}

/// Read an errored response's body (best-effort) and build an
/// HttpError. One place so every verb surfaces the dispatcher's
/// reason identically.
async function httpError(method: string, path: string, res: Response): Promise<HttpError> {
  const body = await res.text().catch(() => '');
  return new HttpError(method, path, res.status, body);
}

// TODO(weft): the generic get<T>/post<T> surface lets callers
// declare inline shapes that drift from the Rust wire structs
// without `tsc` catching it (a renamed field on the backend keeps
// compiling because `<{status?: string}>` is partial by design).
// The structural answer is generated TypeScript types from the
// Rust protocol structs (ts-rs / typeshare) wired into setup.sh.
// Tracked separately from this slice; each new endpoint should
// still go through this client for the moment.
export class DispatcherClient {
  constructor(private baseUrl: string) {}

  setBaseUrl(url: string) {
    this.baseUrl = url;
  }

  async get<T>(path: string): Promise<T> {
    const res = await fetch(`${this.baseUrl}${path}`);
    if (!res.ok) throw await httpError('GET', path, res);
    return (await res.json()) as T;
  }

  async post<T>(path: string, body: unknown): Promise<T> {
    const res = await fetch(`${this.baseUrl}${path}`, {
      method: 'POST',
      headers: { 'content-type': 'application/json' },
      body: JSON.stringify(body),
    });
    if (!res.ok) throw await httpError('POST', path, res);
    const text = await res.text();
    return (text ? JSON.parse(text) : ({} as unknown)) as T;
  }

  async del(path: string): Promise<void> {
    const res = await fetch(`${this.baseUrl}${path}`, { method: 'DELETE' });
    if (!res.ok && res.status !== 204) throw await httpError('DELETE', path, res);
  }

  subscribe(path: string, onEvent: (ev: { data: string }) => void): SseSubscription {
    return subscribeSse(`${this.baseUrl}${path}`, (data) => onEvent({ data }), (err) => {
      console.warn('[weft/dispatcher] SSE subscription failed:', err);
    });
  }
}
