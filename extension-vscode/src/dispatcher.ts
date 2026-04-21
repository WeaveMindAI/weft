// Thin HTTP client for the dispatcher.

import type { Diagnostic, ParseResponse } from './shared/protocol';

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
