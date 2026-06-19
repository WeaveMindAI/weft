// API client for the WeaveMind dispatcher's general signal surface.
//
// Architecture-4: the dispatcher exposes ONE generic enumeration
// route per api_token (`GET /api-token/{tk}/signals`) and ONE
// generic per-signal route (`POST /signal/{signal_token}`,
// `DELETE /signal/{signal_token}`). Per-kind rendering happens
// inside the listener. The browser extension is the consumer that
// renders Form-kind signals; future consumers (Slack bot, etc)
// would render their kinds the same way against the same routes.
//
// Each token in storage is scoped server-side (kinds + projects +
// tags). The extension just uses the token; scope checks happen on
// the dispatcher.

export interface PendingTask {
  /// Per-signal token. Identifies the signal end-to-end:
  /// fire = `POST /signal/{token}`, cancel = `DELETE /signal/{token}`.
  /// Replaces the v1 `executionId` field (the dispatcher's `color`
  /// is no longer surfaced to consumers; signal token alone is
  /// sufficient routing).
  token: string;
  nodeId: string;
  /// Wake-signal kind tag (e.g. `form`, `timer`, `api_endpoint`).
  /// Useful for the extension to skip kinds it can't render.
  kind: string;
  /// Free-form consumer label (e.g. `human_in_the_loop`). Set by
  /// the registering node; the extension's allowed_kinds must
  /// include it for the dispatcher to surface this signal.
  consumerKind?: string;
  /// Display text. Listener picks a sensible default if the node
  /// didn't set a title.
  title: string;
  description?: string;
  /// Form schema, present for Form-kind signals.
  formSchema?: unknown;
}

export interface ApiToken {
  /// The api_token string (e.g. `wm_tk_swift-falcon-23`).
  token: string;
  /// User-facing name shown in the popup.
  name: string;
  /// Base URL of the dispatcher this token belongs to. Allows the
  /// extension to point at multiple dispatchers (e.g. local + cloud)
  /// from one browser.
  dispatcherUrl: string;
}

export async function getTokens(): Promise<ApiToken[]> {
  const result = await browser.storage.local.get('apiTokens');
  return (result.apiTokens as ApiToken[]) || [];
}

export async function setTokens(tokens: ApiToken[]): Promise<void> {
  await browser.storage.local.set({ apiTokens: tokens });
}

export async function addToken(token: ApiToken): Promise<void> {
  const tokens = await getTokens();
  if (!tokens.find(t => t.token === token.token)) {
    tokens.push(token);
    await setTokens(tokens);
  }
}

export async function removeToken(tokenId: string): Promise<void> {
  const tokens = await getTokens();
  await setTokens(tokens.filter(t => t.token !== tokenId));
}

export interface FetchTasksResult {
  tasks: PendingTask[];
  /// True iff at least one configured token successfully reached
  /// its dispatcher. Drives the popup's "connected" indicator.
  anyReachable: boolean;
  tokenCount: number;
  successCount: number;
}

/// Fetch pending tasks from every configured api_token IN PARALLEL.
/// One round-trip per token; failures are isolated.
export async function fetchPendingTasks(
  { timeoutMs }: { timeoutMs?: number } = {},
): Promise<FetchTasksResult> {
  const tokens = await getTokens();
  if (tokens.length === 0) {
    return { tasks: [], anyReachable: false, tokenCount: 0, successCount: 0 };
  }

  const fetchOne = async (tokenConfig: ApiToken): Promise<PendingTask[]> => {
    const url = `${tokenConfig.dispatcherUrl}/api-token/${tokenConfig.token}/signals`;
    const opts: RequestInit = { method: 'GET' };
    if (timeoutMs) opts.signal = AbortSignal.timeout(timeoutMs);
    const resp = await fetch(url, opts);
    if (!resp.ok) throw new Error(`HTTP ${resp.status}`);
    const data = await resp.json();
    const tasks = (Array.isArray(data) ? data : []) as PendingTask[];
    for (const task of tasks) {
      (task as PendingTask & { _tokenConfig: ApiToken })._tokenConfig = tokenConfig;
    }
    return tasks;
  };

  const results = await Promise.allSettled(tokens.map(fetchOne));
  const allTasks: PendingTask[] = [];
  let successCount = 0;
  results.forEach((res, i) => {
    if (res.status === 'fulfilled') {
      allTasks.push(...res.value);
      successCount += 1;
    } else {
      console.warn(
        `[WeaveMind] Failed to fetch tasks for token ${tokens[i].name}:`,
        res.reason,
      );
    }
  });
  return {
    tasks: allTasks,
    anyReachable: successCount > 0,
    tokenCount: tokens.length,
    successCount,
  };
}

/// Submit a form payload (HumanQuery completion / human trigger
/// fire). The signal token alone is sufficient routing; no extra
/// auth header is needed because possessing the signal token is the
/// authorization. (Cancel/dismiss requires the api_token because
/// they're destructive across the whole signal pool.)
export async function submitTask(
  task: PendingTask & { _tokenConfig?: ApiToken },
  input: Record<string, unknown>,
): Promise<void> {
  const tokenConfig = task._tokenConfig;
  if (!tokenConfig) throw new Error('Task missing token configuration');
  const url = `${tokenConfig.dispatcherUrl}/signal/${task.token}`;
  const resp = await fetch(url, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify(input),
  });
  if (!resp.ok) {
    const text = await resp.text();
    throw new Error(`HTTP ${resp.status}: ${text}`);
  }
}

/// Skip ONE task: resume its lane with null. Sibling lanes of
/// the same execution keep going. Most upstream code patterns
/// auto-skip on null inputs (downstream null-propagation), so
/// this is the "I don't want to answer this one, do whatever"
/// action. Auth: signal token alone (knowing it = permission).
export async function skipTask(
  task: PendingTask & { _tokenConfig?: ApiToken },
): Promise<void> {
  const tokenConfig = task._tokenConfig;
  if (!tokenConfig) throw new Error('Task missing token configuration');
  const url = `${tokenConfig.dispatcherUrl}/signal/${task.token}/skip`;
  const resp = await fetch(url, { method: 'POST' });
  if (!resp.ok) {
    const text = await resp.text();
    throw new Error(`HTTP ${resp.status}: ${text}`);
  }
}

/// Cancel the WHOLE RUN this task belongs to. Every sibling task
/// of the same execution dies (5 parallel HumanQueries → all 5
/// dropped). Worker stops, NodeCancelled + ExecutionFailed
/// journaled. The user can still inspect the run in the journal
/// afterward to debug why they cancelled.
///
/// Auth: api_token via Authorization header. Token must be ≥
/// project-scoped (no kind / tag restrictions). Tag-scoped tokens
/// are rejected; they can only skip their visible signals.
export async function cancelRun(
  task: PendingTask & { _tokenConfig?: ApiToken },
): Promise<void> {
  const tokenConfig = task._tokenConfig;
  if (!tokenConfig) throw new Error('Task missing token configuration');
  const url = `${tokenConfig.dispatcherUrl}/signal/${task.token}`;
  const resp = await fetch(url, {
    method: 'DELETE',
    headers: { Authorization: `Bearer ${tokenConfig.token}` },
  });
  if (!resp.ok) {
    const text = await resp.text();
    throw new Error(`HTTP ${resp.status}: ${text}`);
  }
}

/// Clear all visible tasks for one api_token. Cancels every
/// distinct execution this token sees (one cancel per color, not
/// per task). Same scope rule as cancelRun: token must be
/// ≥ project-scoped.
export async function clearAll(token: ApiToken): Promise<{
  colorsCancelled: number;
  entrySignalsDropped: number;
}> {
  const url = `${token.dispatcherUrl}/api-token/${token.token}/signals`;
  const resp = await fetch(url, { method: 'DELETE' });
  if (!resp.ok) {
    const text = await resp.text();
    throw new Error(`HTTP ${resp.status}: ${text}`);
  }
  const body = (await resp.json()) as {
    colors_cancelled?: number;
    entry_signals_dropped?: number;
  };
  return {
    colorsCancelled: body.colors_cancelled ?? 0,
    entrySignalsDropped: body.entry_signals_dropped ?? 0,
  };
}

