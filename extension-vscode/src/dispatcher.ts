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
    headers: Record<string, string>,
    onData: (data: string) => void,
    onError?: (err: unknown) => void,
    onClosed?: () => void,
    onOpen?: () => void,
): SseSubscription {
    const controller = new AbortController();
    let closed = false;
    (async () => {
        try {
            const res = await fetch(url, {
                headers: { ...headers, accept: 'text/event-stream' },
                signal: controller.signal,
            });
            if (!res.ok || !res.body) {
                throw new Error(`SSE ${url}: ${res.status}`);
            }
            if (!closed) onOpen?.();
            const reader = res.body.getReader();
            const decoder = new TextDecoder();
            let buf = '';
            while (!closed) {
                const { value, done } = await reader.read();
                // Server cleanly ended the stream. Distinct from an
                // error: report it so a caller can tell "stream ended"
                // from "stream broke" (only the latter is alarming).
                if (done) {
                    if (!closed) onClosed?.();
                    break;
                }
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
  /// The operator key a request to a remote install carries; the local
  /// install needs none.
  private operatorKey: string | undefined;
  /// Where requests go, or `null` while there is no address to go to (a
  /// named install never started); then every request fails with
  /// `unavailable`, and the extension stays up until an address appears.
  private baseUrl: string | null = null;
  private unavailable = 'weft: no install to talk to yet';
  /// Added to a connection failure: where this address came from, so the
  /// person knows what to fix.
  private unreachableHint: string | undefined;
  private readonly installListeners: Array<() => void> = [];

  /// Point every request, and every stream, at another install: the
  /// local one (no key) or a target of the project with its operator
  /// key. Streams reconnect there through `onInstallChange`.
  setInstall(url: string, operatorKey: string | undefined, unreachableHint?: string) {
    this.unreachableHint = unreachableHint;
    if (url === this.baseUrl && operatorKey === this.operatorKey) return;
    this.baseUrl = url;
    this.operatorKey = operatorKey;
    for (const listener of this.installListeners) listener();
  }

  /// There is no install to talk to, for `reason`: requests fail with it
  /// until the next `setInstall`.
  setUnavailable(reason: string) {
    this.unavailable = reason;
    if (this.baseUrl === null) return;
    this.baseUrl = null;
    this.operatorKey = undefined;
    for (const listener of this.installListeners) listener();
  }

  private url(path: string): string {
    if (this.baseUrl === null) throw new Error(this.unavailable);
    return `${this.baseUrl}${path}`;
  }

  /// `fetch`, with a connection failure named: the address, and where
  /// it came from.
  private async send(path: string, init: RequestInit): Promise<Response> {
    const url = this.url(path);
    try {
      return await fetch(url, init);
    } catch (e) {
      if ((e as Error).name === 'AbortError') throw e;
      const hint = this.unreachableHint ? `; ${this.unreachableHint}` : '';
      throw new Error(`cannot reach weft at ${this.baseUrl}: ${e instanceof Error ? e.message : e}${hint}`, { cause: e });
    }
  }

  /// Called after every `setInstall` that changed the install.
  onInstallChange(listener: () => void): void {
    this.installListeners.push(listener);
  }

  private headers(extra: Record<string, string> = {}): Record<string, string> {
    return this.operatorKey ? { ...extra, authorization: `Bearer ${this.operatorKey}` } : extra;
  }

  /// The address this client reaches the dispatcher at. The webview's
  /// CSP needs it: a minted file link comes back on whichever host the
  /// request went out on, so the origin to allow is this one.
  getBaseUrl(): string | null {
    return this.baseUrl;
  }

  async get<T>(path: string, signal?: AbortSignal): Promise<T> {
    const res = await this.send(path, { signal, headers: this.headers() });
    if (!res.ok) throw await httpError('GET', path, res);
    return (await res.json()) as T;
  }

  async post<T>(path: string, body: unknown): Promise<T> {
    const res = await this.send(path, {
      method: 'POST',
      headers: this.headers({ 'content-type': 'application/json' }),
      body: JSON.stringify(body),
    });
    if (!res.ok) throw await httpError('POST', path, res);
    const text = await res.text();
    return (text ? JSON.parse(text) : ({} as unknown)) as T;
  }

  async put<T>(path: string, body: unknown): Promise<T> {
    const res = await this.send(path, {
      method: 'PUT',
      headers: this.headers({ 'content-type': 'application/json' }),
      body: JSON.stringify(body),
    });
    if (!res.ok) throw await httpError('PUT', path, res);
    const text = await res.text();
    return (text ? JSON.parse(text) : ({} as unknown)) as T;
  }

  async del(path: string): Promise<void> {
    const res = await this.send(path, { method: 'DELETE', headers: this.headers() });
    if (!res.ok && res.status !== 204) throw await httpError('DELETE', path, res);
  }

  /** Subscribe to an SSE stream. `onOpen` fires once the server
   *  accepted the stream (2xx with a body); `onError` fires when the
   *  stream fails (connection refused, non-2xx, mid-stream read
   *  error); `onClosed` fires when the server cleanly ends the
   *  stream. All are optional; a caller that passes no error handler
   *  falls back to a console.warn so a dropped stream is never fully
   *  silent. A caller that DOES pass one owns surfacing the dead
   *  stream (reconnect loop, "live follow lost" state) instead of
   *  leaving the UI stuck forever. */
  subscribe(
    path: string,
    onEvent: (ev: { data: string }) => void,
    handlers?: {
      onError?: (err: unknown) => void;
      onClosed?: () => void;
      onOpen?: () => void;
    },
  ): SseSubscription {
    const onError = (err: unknown) => {
      if (handlers?.onError) handlers.onError(err);
      else console.warn('[weft/dispatcher] SSE subscription failed:', err);
    };
    // No install to stream from: the stream fails the way a refused one
    // does, after this returns, so every caller's error path runs as usual.
    if (this.baseUrl === null) {
      const err = new Error(this.unavailable);
      let closed = false;
      queueMicrotask(() => { if (!closed) onError(err); });
      return { close: () => { closed = true; } };
    }
    return subscribeSse(
      this.url(path),
      this.headers(),
      (data) => onEvent({ data }),
      onError,
      handlers?.onClosed,
      handlers?.onOpen,
    );
  }
}
