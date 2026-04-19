// Extension Token Authentication
//
// This extension uses token-based authentication to connect to WeaveMind.
// All token logic is handled by weft-api (extension_tokens.rs, extension_api.rs).
// In cloud mode, cloud-api proxies requests to weft-api.
//
// Related code:
// - Backend: crates/weft-api/src/extension_tokens.rs
// - Dashboard UI: dashboard/src/routes/(app)/extension/+page.svelte

const DEFAULT_CLOUD_URL = 'https://cloud.weavemind.ai';

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
  cloudUrl: string; // Base URL for the cloud API
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

/// Fetch pending tasks from all configured tokens
export async function fetchPendingTasks({ timeoutMs }: { timeoutMs?: number } = {}): Promise<PendingTask[]> {
  const tokens = await getTokens();
  
  if (tokens.length === 0) {
    console.log('[WeaveMind] No tokens configured');
    return [];
  }
  
  const allTasks: PendingTask[] = [];
  
  for (const tokenConfig of tokens) {
    // Use dashboard proxy: /api/ext/{token}/tasks
    const url = `${tokenConfig.cloudUrl}/api/ext/${tokenConfig.token}/tasks`;
    console.log('[WeaveMind] Fetching tasks from:', url);
    
    try {
      const fetchOptions: RequestInit = { method: 'GET' };
      if (timeoutMs) fetchOptions.signal = AbortSignal.timeout(timeoutMs);
      const response = await fetch(url, fetchOptions);
      
      if (!response.ok) {
        console.warn(`[WeaveMind] Failed to fetch tasks for token ${tokenConfig.name}:`, response.status);
        continue;
      }
      
      const data = await response.json();
      const tasks = (data.tasks || []) as PendingTask[];
      console.log(`[WeaveMind] Got ${tasks.length} tasks from ${tokenConfig.name}`);
      
      // Add token info to tasks for later use when completing
      for (const task of tasks) {
        (task as PendingTask & { _tokenConfig: ExtensionToken })._tokenConfig = tokenConfig;
        allTasks.push(task);
      }
    } catch (error) {
      console.error(`[WeaveMind] Failed to fetch tasks for token ${tokenConfig.name}:`, error);
    }
  }
  
  console.log(`[WeaveMind] Total tasks: ${allTasks.length}`);
  return allTasks;
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
  
  // Use dashboard proxy: /api/ext/{token}/actions/{actionId}/dismiss
  const url = `${tokenConfig.cloudUrl}/api/ext/${tokenConfig.token}/actions/${actionId}/dismiss`;
  
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
  const url = `${tokenConfig.cloudUrl}/api/ext/${tokenConfig.token}/tasks/${executionId}/cancel`;
  
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
  const url = `${tokenConfig.cloudUrl}/api/ext/${tokenConfig.token}/triggers/${triggerTaskId}/submit`;

  console.log('[WeaveMind] Submitting trigger:', { url, triggerTaskId: trigger.executionId });

  const response = await fetch(url, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ nodeId: trigger.nodeId, input }),
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
    const url = `${tokenConfig.cloudUrl}/api/ext/${tokenConfig.token}/cleanup/all`;
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
  const url = `${tokenConfig.cloudUrl}/api/ext/${tokenConfig.token}/cleanup/execution/${encodeURIComponent(executionId)}`;
  const response = await fetch(url, { method: 'POST' });
  if (!response.ok) {
    const text = await response.text();
    throw new Error(`HTTP ${response.status}: ${text}`);
  }
  const body = await response.json() as { removed?: number };
  return body.removed ?? 0;
}

/// Check if any token is connected
export async function checkConnection(): Promise<boolean> {
  const tokens = await getTokens();
  
  if (tokens.length === 0) {
    return false;
  }
  
  // Check if at least one token is valid via dashboard proxy
  for (const tokenConfig of tokens) {
    try {
      const response = await fetch(`${tokenConfig.cloudUrl}/api/ext/${tokenConfig.token}/health`, {
        method: 'GET',
        signal: AbortSignal.timeout(5000),
      });
      if (response.ok) {
        return true;
      }
    } catch (e) {
      console.error(`[WeaveMind] Health check failed for ${tokenConfig.name}:`, e);
    }
  }
  
  return false;
}
