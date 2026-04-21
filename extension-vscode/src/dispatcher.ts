// Thin HTTP client for the dispatcher. Mirrors weft-cli's client in
// shape so behavior stays consistent across surfaces.

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
    return (await res.json()) as T;
  }

  subscribe(path: string, onEvent: (ev: MessageEvent) => void): EventSource {
    const es = new EventSource(`${this.baseUrl}${path}`);
    es.onmessage = onEvent;
    return es;
  }

  async runCurrentProject(): Promise<void> {
    // Phase A2: read the project id from the open workspace's
    // weft.toml, POST /projects/{id}/run.
    throw new Error('runCurrentProject not yet implemented');
  }
}
