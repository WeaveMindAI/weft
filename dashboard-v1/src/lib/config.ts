import { browser } from '$app/environment';
import { STORAGE_KEYS } from '$lib/utils';

// URL configuration for the three backend services.
//
// Standalone mode (3 separate services):
//   - weft-api (Axum, port 3000): /api/* routes (triggers, force-retry, webhooks)
//   - Restate ingress (port 8180): InfrastructureManager/*, TaskRegistry/*
//   - Executor (Axum, port 9081): ProjectExecutor/* (start, status, cancel, etc.)
//
// Embedded mode (dashboard loaded inside a host app via iframe):
//   The host passes a single api_url via query param, stored in sessionStorage.
//   All three getters fall back to that URL when present, since the host
//   is expected to proxy all backend services behind one endpoint.

// When embedded, the host app sets a single unified backend URL in sessionStorage.
function getSessionUrl(): string | null {
    if (!browser) return null;
    return sessionStorage.getItem(STORAGE_KEYS.apiUrl) || null;
}

// weft-api (Axum): /api/* routes (triggers, force-retry, webhooks, usage)
const DEFAULT_API_URL = 'http://localhost:3000';

export function getApiUrl(): string {
    const session = getSessionUrl();
    if (session) return session;
    if (browser) {
        const stored = localStorage.getItem(STORAGE_KEYS.apiUrl);
        if (stored) return stored;
    }
    return DEFAULT_API_URL;
}

export function setApiUrl(url: string): void {
    if (browser) {
        localStorage.setItem(STORAGE_KEYS.apiUrl, url);
    }
}

// Restate ingress: InfrastructureManager/*, TaskRegistry/*
const DEFAULT_RESTATE_URL = 'http://localhost:8080';

export function getRestateUrl(): string {
    const session = getSessionUrl();
    if (session) return session;
    if (browser) {
        const stored = localStorage.getItem(STORAGE_KEYS.restateUrl);
        if (stored) return stored;
    }
    return DEFAULT_RESTATE_URL;
}

export function setRestateUrl(url: string): void {
    if (browser) {
        localStorage.setItem(STORAGE_KEYS.restateUrl, url);
    }
}

// Executor (Axum): ProjectExecutor/* (start, status, cancel, outputs, provide_input)
const DEFAULT_EXECUTOR_URL = 'http://localhost:9081';

export function getExecutorUrl(): string {
    const session = getSessionUrl();
    if (session) return session;
    if (browser) {
        const stored = localStorage.getItem(STORAGE_KEYS.executorUrl);
        if (stored) return stored;
    }
    return DEFAULT_EXECUTOR_URL;
}

export function setExecutorUrl(url: string): void {
    if (browser) {
        localStorage.setItem(STORAGE_KEYS.executorUrl, url);
    }
}

// Get auth headers for API calls.
// If a token exists in sessionStorage (set by website iframe auth), attach it.
// In standalone local mode no token is stored, so this returns {}.
// Restate ignores unknown headers, so sending the token to it is harmless.
export function getAuthHeaders(): Record<string, string> {
    if (!browser) return {};
    const token = sessionStorage.getItem(STORAGE_KEYS.authToken);
    if (!token) return {};
    return { 'Authorization': `Bearer ${token}` };
}

// Fetch wrapper that automatically injects auth headers for cloud mode
export function authFetch(input: RequestInfo | URL, init?: RequestInit): Promise<Response> {
    const headers = new Headers(init?.headers);
    const authHeaders = getAuthHeaders();
    for (const [key, value] of Object.entries(authHeaders)) {
        if (!headers.has(key)) {
            headers.set(key, value);
        }
    }
    return fetch(input, { ...init, headers });
}

// API endpoints
export const api = {
    // Project execution (Axum executor)
    startExecution: (executionId: string) => `${getExecutorUrl()}/ProjectExecutor/${executionId}/start/send`,
    getStatus: (executionId: string) => `${getExecutorUrl()}/ProjectExecutor/${executionId}/get_status`,
    getNodeStatuses: (executionId: string) => `${getExecutorUrl()}/ProjectExecutor/${executionId}/get_node_statuses`,
    getAllOutputs: (executionId: string) => `${getExecutorUrl()}/ProjectExecutor/${executionId}/get_all_outputs`,
    getNodeExecutions: (executionId: string) => `${getExecutorUrl()}/ProjectExecutor/${executionId}/get_node_executions`,
    provideInput: (executionId: string) => `${getExecutorUrl()}/ProjectExecutor/${executionId}/provide_input`,
    cancelExecution: (executionId: string) => `${getExecutorUrl()}/ProjectExecutor/${executionId}/cancel`,
    
    // Task registry (Restate virtual object)
    listTasks: () => `${getRestateUrl()}/TaskRegistry/global/list_tasks`,
    
    // Trigger management (weft-api)
    listTriggers: () => `${getApiUrl()}/api/v1/triggers`,
    registerTrigger: () => `${getApiUrl()}/api/v1/triggers`,
    unregisterProjectTriggers: (projectId: string) => `${getApiUrl()}/api/v1/triggers/project/${projectId}`,
    
    // Usage and credits (weft-api)
    getUsage: (userId: string, from: string, to: string) => `${getApiUrl()}/api/v1/usage/${userId}?from=${from}&to=${to}`,
    getCredits: (userId: string) => `${getApiUrl()}/api/v1/credits?userId=${userId}`,
    getExecutionCost: (executionId: string) => `${getApiUrl()}/api/v1/usage/execution-cost?executionId=${executionId}`,
    
    // File storage (works in both local and cloud mode)
    createFile: () => `${getApiUrl()}/api/v1/files`,
    getFile: (fileId: string) => `${getApiUrl()}/api/v1/files/${fileId}`,
    listFiles: () => `${getApiUrl()}/api/v1/files`,
    deleteFile: (fileId: string) => `${getApiUrl()}/api/v1/files/${fileId}`,
    getStorageUsage: () => `${getApiUrl()}/api/v1/files/usage`,

    // Infrastructure management (via weft-api for compile+enrich)
    startInfra: (projectId: string) => `${getApiUrl()}/api/infra/${projectId}/start`,
    stopInfra: (projectId: string) => `${getApiUrl()}/api/infra/${projectId}/stop`,
    terminateInfra: (projectId: string) => `${getApiUrl()}/api/infra/${projectId}/terminate`,
    getInfraStatus: (projectId: string) => `${getApiUrl()}/api/infra/${projectId}/status`,
    forceRetryInfra: (projectId: string) => `${getApiUrl()}/api/infra/${projectId}/force-retry`,
    getInfraLiveData: (projectId: string, nodeId: string) => `${getApiUrl()}/api/infra/${projectId}/nodes/${nodeId}/live`,

    // Publish (deploy-as-a-page). Works against weft-api locally and
    // cloud-api in cloud mode; both expose the same CRUD surface at
    // /api/v1/publish. Cloud additionally exposes session + run endpoints.
    listPublications: () => `${getApiUrl()}/api/v1/publish`,
    publishProject: () => `${getApiUrl()}/api/v1/publish`,
    updatePublication: (slug: string) => `${getApiUrl()}/api/v1/publish/${encodeURIComponent(slug)}`,
    deletePublication: (slug: string) => `${getApiUrl()}/api/v1/publish/${encodeURIComponent(slug)}`,
    // Public endpoints are scoped on (username, slug) so two users can own
    // the same slug. Owner-scoped mutation endpoints (update/delete) stay
    // slug-only because the authenticated user_id uniquely scopes the slug.
    getPublicationByUserSlug: (username: string, slug: string) =>
        `${getApiUrl()}/api/v1/publish/by-user/${encodeURIComponent(username)}/${encodeURIComponent(slug)}`,
    publicationSession: (username: string, slug: string) =>
        `${getApiUrl()}/api/v1/publish/by-user/${encodeURIComponent(username)}/${encodeURIComponent(slug)}/session`,
    publicationRun: (username: string, slug: string) =>
        `${getApiUrl()}/api/v1/publish/by-user/${encodeURIComponent(username)}/${encodeURIComponent(slug)}/run`,
    publicationLatestTriggerRun: (username: string, slug: string) =>
        `${getApiUrl()}/api/v1/publish/by-user/${encodeURIComponent(username)}/${encodeURIComponent(slug)}/latest-trigger-run`,
};
