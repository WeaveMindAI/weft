// Extension token authentication.
//
// v2: the extension talks directly to the weft dispatcher's /ext/*
// surface. No dashboard proxy. For local dev, `dispatcherUrl`
// defaults to http://localhost:9999 (the weft start daemon).
//
// Related:
// - crates/weft-dispatcher/src/api/extension.rs

const DEFAULT_DISPATCHER_URL = 'http://localhost:9999';

export type TaskType = 'Task' | 'Action' | 'Trigger';

export interface PendingTask {
  executionId: string;
  nodeId: string;
  title: string;
  description?: string;
  createdAt: string;
  taskType?: TaskType;
  actionUrl?: string;
  formSchema?: unknown;
  metadata?: Record<string, unknown>;
}

export interface ExtensionToken {
  token: string;
  name: string;
  dispatcherUrl: string; // Base URL for the cloud API
}

export async function getTokens(): Promise<ExtensionToken[]> {
  const result = await browser.storage.local.get('extensionTokens');
  return (result.extensionTokens as ExtensionToken[]) || [];
}

export async function setTokens(tokens: ExtensionToken[]): Promise<void> {
  await browser.storage.local.set({ extensionTokens: tokens });
}

export async function addToken(token: ExtensionToken): Promise<void> {
  const tokens = await getTokens();
  // Avoid duplicates
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
  /// Tasks pulled across every token, in arbitrary order.
  tasks: PendingTask[];
  /// True iff at least one token's `/tasks` fetch returned 2xx.
  /// Callers use this to flip a "connected" indicator without a
  /// second `/health` round-trip.
  anyReachable: boolean;
  /// Per-token success/failure counts so callers can render
  /// granular status.
  tokenCount: number;
  successCount: number;
}

/// Fetch pending tasks from every configured token IN PARALLEL.
/// Sequential awaits in a for-loop multiplied total latency by N
/// (one round-trip per token); `Promise.allSettled` lets each
/// token round-trip in its own task and we collect once everything
/// settles. Failures don't poison the others — a single token
/// returning 500 still surfaces the others' tasks.
export async function fetchPendingTasks(
  { timeoutMs }: { timeoutMs?: number } = {},
): Promise<FetchTasksResult> {
  const tokens = await getTokens();
  if (tokens.length === 0) {
    return { tasks: [], anyReachable: false, tokenCount: 0, successCount: 0 };
  }

  const fetchOne = async (tokenConfig: ExtensionToken): Promise<PendingTask[]> => {
    const url = `${tokenConfig.dispatcherUrl}/ext/${tokenConfig.token}/tasks`;
    const fetchOptions: RequestInit = { method: 'GET' };
    if (timeoutMs) fetchOptions.signal = AbortSignal.timeout(timeoutMs);
    const response = await fetch(url, fetchOptions);
    if (!response.ok) {
      throw new Error(`HTTP ${response.status}`);
    }
    const data = await response.json();
    // Dispatcher returns a flat array; v1's dashboard proxy
    // wrapped it in { tasks: [...] }. Accept either shape.
    const tasks = (Array.isArray(data) ? data : (data.tasks ?? [])) as PendingTask[];
    for (const task of tasks) {
      (task as PendingTask & { _tokenConfig: ExtensionToken })._tokenConfig = tokenConfig;
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

/// Dismiss an action (just removes from list, no project interaction)
export async function dismissAction(
  action: PendingTask & { _tokenConfig?: ExtensionToken }
): Promise<void> {
  const tokenConfig = action._tokenConfig;
  
  if (!tokenConfig) {
    throw new Error('Action missing token configuration');
  }
  
  // Use the full executionId (includes -action suffix) for dismissal
  const actionId = encodeURIComponent(action.executionId);
  
  // Use dashboard proxy: /ext/{token}/actions/{actionId}/dismiss
  const url = `${tokenConfig.dispatcherUrl}/ext/${tokenConfig.token}/actions/${actionId}/dismiss`;
  
  console.log('[WeaveMind] Dismissing action:', { url, actionId: action.executionId });
  
  const response = await fetch(url, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
  });
  
  if (!response.ok) {
    const text = await response.text();
    throw new Error(`HTTP ${response.status}: ${text}`);
  }
  
  console.log('[WeaveMind] Action dismissed successfully');
}

/// Cancel a task (skip downstream execution, remove from list)
export async function cancelTask(
  task: PendingTask & { _tokenConfig?: ExtensionToken }
): Promise<void> {
  const tokenConfig = task._tokenConfig;
  
  if (!tokenConfig) {
    throw new Error('Task missing token configuration');
  }
  
  const executionId = encodeURIComponent(task.executionId);
  const url = `${tokenConfig.dispatcherUrl}/ext/${tokenConfig.token}/tasks/${executionId}/cancel`;
  
  console.log('[WeaveMind] Cancelling task:', { url, executionId: task.executionId });
  
  const response = await fetch(url, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
  });
  
  if (!response.ok) {
    const text = await response.text();
    throw new Error(`HTTP ${response.status}: ${text}`);
  }
  
  console.log('[WeaveMind] Task cancelled successfully');
}

/// Submit a trigger form (fires the trigger with form data)
export async function submitTrigger(
  trigger: PendingTask & { _tokenConfig?: ExtensionToken },
  input: Record<string, unknown>,
): Promise<void> {
  const tokenConfig = trigger._tokenConfig;

  if (!tokenConfig) {
    throw new Error('Trigger missing token configuration');
  }

  const triggerTaskId = encodeURIComponent(trigger.executionId);
  const url = `${tokenConfig.dispatcherUrl}/ext/${tokenConfig.token}/triggers/${triggerTaskId}/submit`;

  console.log('[WeaveMind] Submitting trigger:', { url, triggerTaskId: trigger.executionId });

  const response = await fetch(url, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    // Send the form payload directly. The dispatcher journals it
    // as the suspension's resolved value; the worker's
    // HumanTrigger node maps it to output ports per its
    // form_field_specs.
    body: JSON.stringify(input),
  });

  if (!response.ok) {
    const text = await response.text();
    throw new Error(`HTTP ${response.status}: ${text}`);
  }

  console.log('[WeaveMind] Trigger submitted successfully');
}

/// Delete every pending task owned by this token (orphans from cancelled runs, etc).
/// Returns the total number of tasks removed across all tokens.
export async function clearAllTasks(): Promise<number> {
  const tokens = await getTokens();
  let totalRemoved = 0;
  for (const tokenConfig of tokens) {
    const url = `${tokenConfig.dispatcherUrl}/ext/${tokenConfig.token}/cleanup/all`;
    try {
      const response = await fetch(url, { method: 'POST' });
      if (!response.ok) {
        console.warn(`[WeaveMind] cleanup/all failed for ${tokenConfig.name}: ${response.status}`);
        continue;
      }
      const body = await response.json() as { removed?: number };
      totalRemoved += body.removed ?? 0;
    } catch (e) {
      console.error(`[WeaveMind] cleanup/all error for ${tokenConfig.name}:`, e);
    }
  }
  return totalRemoved;
}

/// Delete every pending task whose callback_id is scoped to a specific execution.
/// Use this when one run got stuck with dozens of orphan form requests.
export async function clearTasksForExecution(
  tokenConfig: ExtensionToken,
  executionId: string,
): Promise<number> {
  const url = `${tokenConfig.dispatcherUrl}/ext/${tokenConfig.token}/cleanup/execution/${encodeURIComponent(executionId)}`;
  const response = await fetch(url, { method: 'POST' });
  if (!response.ok) {
    const text = await response.text();
    throw new Error(`HTTP ${response.status}: ${text}`);
  }
  const body = await response.json() as { removed?: number };
  return body.removed ?? 0;
}

/// Check if any token is reachable. Parallel, returns on first
/// success. Callers that ALSO do a tasks fetch should NOT call
/// this — infer connectivity from the tasks fetch result and save
/// the round-trip.
export async function checkConnection(): Promise<boolean> {
  const tokens = await getTokens();
  if (tokens.length === 0) return false;
  const probes = tokens.map(async (tokenConfig) => {
    const response = await fetch(
      `${tokenConfig.dispatcherUrl}/ext/${tokenConfig.token}/health`,
      { method: 'GET', signal: AbortSignal.timeout(5000) },
    );
    if (!response.ok) throw new Error(`HTTP ${response.status}`);
    return true;
  });
  try {
    await Promise.any(probes);
    return true;
  } catch {
    return false;
  }
}
