<script lang="ts">
  import { onMount } from 'svelte';
  import { fetchPendingTasks, dismissAction, cancelTask, clearAllTasks, clearTasksForExecution, checkConnection, getTokens, addToken, removeToken, type PendingTask, type ExtensionToken } from '../../lib/api';

  let allItems = $state<PendingTask[]>([]);
  let loading = $state(true);
  let connected = $state(false);
  let error = $state<string | null>(null);
  let selectedTask = $state<PendingTask | null>(null);
  let selectedAction = $state<PendingTask | null>(null);
  let showSettings = $state(false);
  let tokens = $state<ExtensionToken[]>([]);
  
  // Separate tasks, triggers, and actions
  const tasks = $derived(allItems.filter(t => t.taskType === 'Task' || (!t.taskType && t.taskType !== 'Action' && t.taskType !== 'Trigger')));
  const triggers = $derived(allItems.filter(t => t.taskType === 'Trigger'));
  const actions = $derived(allItems.filter(t => t.taskType === 'Action'));
  
  // New token form
  let newTokenUrl = $state('');
  let newTokenName = $state('');
  let addingToken = $state(false);

  // Settings
  let notificationsEnabled = $state(true);

  async function loadSettings() {
    try {
      const result = await browser.storage.local.get('settings');
      if (result.settings && typeof result.settings === 'object') {
        const settings = result.settings as { notificationsEnabled?: boolean };
        notificationsEnabled = settings.notificationsEnabled ?? true;
      }
    } catch {
      // Use defaults
    }
  }

  async function toggleNotifications() {
    notificationsEnabled = !notificationsEnabled;
    await browser.storage.local.set({ 
      settings: { notificationsEnabled } 
    });
  }

  onMount(async () => {
    await loadSettings();
    tokens = await getTokens();
    await refresh();
  });

  async function refresh() {
    loading = true;
    error = null;
    selectedTask = null;
    selectedAction = null;
    
    try {
      tokens = await getTokens();
      connected = await checkConnection();
      if (connected) {
        allItems = await fetchPendingTasks({ timeoutMs: 10000 });
      } else {
        allItems = [];
      }
    } catch (e) {
      error = e instanceof Error ? e.message : 'Failed to connect';
      connected = false;
    } finally {
      loading = false;
    }
  }

  async function handleCancelTask(task: PendingTask) {
    try {
      await cancelTask(task as PendingTask & { _tokenConfig?: ExtensionToken });
      selectedTask = null;
      await refresh();
    } catch (e) {
      error = e instanceof Error ? e.message : 'Failed to cancel task';
    }
  }

  async function handleClearAll() {
    // Bypass confirmation in browser extension context? Popups block window.confirm.
    // Use a tiny custom state instead for now: one click removes everything.
    try {
      await clearAllTasks();
      await refresh();
    } catch (e) {
      error = e instanceof Error ? e.message : 'Failed to clear tasks';
    }
  }

  /// Extract the execution UUID from a task's callback_id. The TaskRegistry
  /// stores callback_id in the `executionId` field in one of three shapes:
  ///   - WaitingForInput: "{execUuid(36)}-{nodeId}-{pulseUuid}-{seq}"
  ///   - NotifyAction:    "{execUuid(36)}-{nodeId}-action"  (or overridden)
  ///   - Trigger:         "trigger-{triggerId}"
  /// Returns null for shapes where no clean execution UUID can be pulled out,
  /// so the caller hides the "clear run" affordance instead of sending a
  /// malformed request.
  const UUID_RE = /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i;
  function extractExecutionUuid(task: PendingTask): string | null {
    const prefix = task.executionId.slice(0, 36);
    return UUID_RE.test(prefix) ? prefix : null;
  }

  async function handleClearForTask(task: PendingTask) {
    const tokenConfig = (task as PendingTask & { _tokenConfig?: ExtensionToken })._tokenConfig;
    if (!tokenConfig) {
      error = 'Task missing token configuration';
      return;
    }
    const execUuid = extractExecutionUuid(task);
    if (!execUuid) {
      error = 'This task does not belong to a runnable execution';
      return;
    }
    try {
      await clearTasksForExecution(tokenConfig, execUuid);
      await refresh();
    } catch (e) {
      error = e instanceof Error ? e.message : 'Failed to clear tasks for this run';
    }
  }

  async function handleDismissAction(action: PendingTask) {
    // Dismiss action using dedicated endpoint (just removes from list)
    try {
      await dismissAction(action as PendingTask & { _tokenConfig?: ExtensionToken });
      selectedAction = null;
      await refresh();
    } catch (e) {
      error = e instanceof Error ? e.message : 'Failed to dismiss action';
    }
  }

  function openActionUrl(action: PendingTask) {
    if (action.actionUrl) {
      window.open(action.actionUrl, '_blank');
    }
  }

  async function handleAddToken() {
    if (!newTokenUrl.trim()) return;

    addingToken = true;
    error = null;

    try {
      // Accepted formats:
      //   http://host:port/ext/TOKEN   (full dispatcher URL)
      //   TOKEN                        (uses http://localhost:9999)
      let token: string;
      let dispatcherUrl: string;

      if (newTokenUrl.startsWith('http')) {
        const url = new URL(newTokenUrl);
        dispatcherUrl = `${url.protocol}//${url.host}`;
        const pathParts = url.pathname.split('/').filter(Boolean);
        const extIndex = pathParts.indexOf('ext');
        if (extIndex >= 0 && pathParts[extIndex + 1]) {
          token = pathParts[extIndex + 1];
        } else {
          throw new Error('Invalid URL format. Expected: http://localhost:9999/ext/TOKEN');
        }
      } else {
        token = newTokenUrl.trim();
        dispatcherUrl = 'http://localhost:9999';
      }

      const response = await fetch(`${dispatcherUrl}/ext/${token}/health`, {
        method: 'GET',
        signal: AbortSignal.timeout(5000),
      });

      if (!response.ok) {
        throw new Error('Invalid token or dispatcher unreachable');
      }

      await addToken({
        token,
        name: newTokenName.trim() || `Token ${tokens.length + 1}`,
        dispatcherUrl,
      });

      newTokenUrl = '';
      newTokenName = '';
      await refresh();
    } catch (e) {
      error = e instanceof Error ? e.message : 'Failed to add token';
    } finally {
      addingToken = false;
    }
  }

  async function handleRemoveToken(tokenId: string) {
    await removeToken(tokenId);
    await refresh();
  }

  function formatTime(dateStr: string): string {
    if (!dateStr) return '';
    const date = new Date(dateStr);
    const now = new Date();
    const diff = now.getTime() - date.getTime();
    
    if (diff < 60000) return 'Just now';
    if (diff < 3600000) return `${Math.floor(diff / 60000)}m ago`;
    if (diff < 86400000) return `${Math.floor(diff / 3600000)}h ago`;
    return date.toLocaleDateString();
  }

  // Smart data formatting helpers
  function getDataType(value: unknown): string {
    if (value === null) return 'null';
    if (value === undefined) return 'undefined';
    if (Array.isArray(value)) return 'array';
    if (typeof value === 'object') return 'object';
    return typeof value;
  }

  function isSimpleValue(value: unknown): boolean {
    const type = getDataType(value);
    return ['null', 'undefined', 'string', 'number', 'boolean'].includes(type);
  }

  function truncateString(str: string, maxLength: number = 200): string {
    if (str.length <= maxLength) return str;
    return str.slice(0, maxLength) + '...';
  }

  function getTaskReviewUrl(task: PendingTask & { _tokenConfig?: ExtensionToken }): string | null {
    if (!task._tokenConfig) return null;
    const { dispatcherUrl } = task._tokenConfig;
    // Phase A: the dispatcher doesn't yet serve a per-task runner
    // page. Point at the ops dashboard so users land somewhere
    // useful. Phase B wires a /tasks/{execId} runner UI.
    return `${dispatcherUrl}/dashboard`;
  }

  function openTaskInDashboard(task: PendingTask) {
    const url = getTaskReviewUrl(task as PendingTask & { _tokenConfig?: ExtensionToken });
    if (url) {
      window.open(url, '_blank');
    }
  }
</script>

<!-- Extension container with dot pattern background -->
<div class="extension-root">
  <div class="dot-pattern"></div>
  
  <div class="extension-content">
    <!-- Header Card -->
    <div class="card header-card">
      <div class="card-header">
        <div class="header-dot" class:loading class:connected={!loading && connected} class:disconnected={!loading && !connected}></div>
        <span class="header-title">WeaveMind</span>
        {#if loading}
          <span class="status-badge loading">Connecting</span>
        {:else if connected}
          <span class="status-badge connected">Connected</span>
        {:else}
          <span class="status-badge disconnected">Offline</span>
        {/if}
      </div>
      <div class="header-actions">
        <button class="action-btn" onclick={refresh} title="Refresh" aria-label="Refresh">
          <svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2">
            <path d="M23 4v6h-6M1 20v-6h6M3.51 9a9 9 0 0114.85-3.36L23 10M1 14l4.64 4.36A9 9 0 0020.49 15"/>
          </svg>
        </button>
        <button class="action-btn" class:active={showSettings} onclick={() => showSettings = !showSettings} title="Settings" aria-label="Settings">
          <svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2">
            <circle cx="12" cy="12" r="3"/><path d="M19.4 15a1.65 1.65 0 00.33 1.82l.06.06a2 2 0 010 2.83 2 2 0 01-2.83 0l-.06-.06a1.65 1.65 0 00-1.82-.33 1.65 1.65 0 00-1 1.51V21a2 2 0 01-2 2 2 2 0 01-2-2v-.09A1.65 1.65 0 009 19.4a1.65 1.65 0 00-1.82.33l-.06.06a2 2 0 01-2.83 0 2 2 0 010-2.83l.06-.06a1.65 1.65 0 00.33-1.82 1.65 1.65 0 00-1.51-1H3a2 2 0 01-2-2 2 2 0 012-2h.09A1.65 1.65 0 004.6 9a1.65 1.65 0 00-.33-1.82l-.06-.06a2 2 0 010-2.83 2 2 0 012.83 0l.06.06a1.65 1.65 0 001.82.33H9a1.65 1.65 0 001-1.51V3a2 2 0 012-2 2 2 0 012 2v.09a1.65 1.65 0 001 1.51 1.65 1.65 0 001.82-.33l.06-.06a2 2 0 012.83 0 2 2 0 010 2.83l-.06.06a1.65 1.65 0 00-.33 1.82V9a1.65 1.65 0 001.51 1H21a2 2 0 012 2 2 2 0 01-2 2h-.09a1.65 1.65 0 00-1.51 1z"/>
          </svg>
        </button>
      </div>
    </div>

    {#if showSettings}
      <!-- Settings Card -->
      <div class="card">
        <div class="card-header">
          <div class="header-dot"></div>
          <span class="header-title">Settings</span>
        </div>
        <div class="card-body">
          <!-- Notifications Toggle -->
          <div class="form-group">
            <div class="toggle-row">
              <div>
                <span class="label">Toast Notifications</span>
                <p class="hint">Show in-browser alerts for new tasks</p>
              </div>
              <button 
                class="toggle-btn" 
                class:active={notificationsEnabled}
                onclick={toggleNotifications}
                aria-pressed={notificationsEnabled}
                aria-label="Toggle notifications"
              >
                <span class="toggle-slider"></span>
              </button>
            </div>
          </div>

          <div class="divider"></div>

          <!-- Tokens Section -->
          <div class="form-group">
            <span class="label">Connected Tokens</span>
            <p class="hint">Tokens link this extension to your projects</p>
          </div>
          
          {#if tokens.length > 0}
            <div class="token-list">
              {#each tokens as tokenConfig}
                <div class="token-item">
                  <div class="token-info">
                    <span class="token-name">{tokenConfig.name}</span>
                    <span class="token-url">{tokenConfig.dispatcherUrl}</span>
                  </div>
                  <button class="remove-btn" onclick={() => handleRemoveToken(tokenConfig.token)} title="Remove" aria-label="Remove token">×</button>
                </div>
              {/each}
            </div>
          {:else}
            <p class="empty-text">No tokens configured</p>
          {/if}
          
          <div class="add-token-form">
            <input 
              type="text" 
              bind:value={newTokenUrl} 
              placeholder="Paste token URL" 
              disabled={addingToken}
              class="input"
            />
            <input 
              type="text" 
              bind:value={newTokenName} 
              placeholder="Name (optional)" 
              disabled={addingToken}
              class="input"
            />
            <button class="btn btn-primary" onclick={handleAddToken} disabled={addingToken || !newTokenUrl.trim()}>
              {#if addingToken}
                <span class="spinner-small"></span>
              {:else}
                Add Token
              {/if}
            </button>
          </div>
        </div>
      </div>
    {:else if loading}
      <!-- Loading State -->
      <div class="card">
        <div class="card-header">
          <div class="header-dot"></div>
          <span class="header-title">Loading</span>
        </div>
        <div class="card-body center-content">
          <div class="spinner"></div>
          <p class="hint">Fetching tasks...</p>
        </div>
      </div>
    {:else if !connected}
      <!-- Disconnected State -->
      <div class="card">
        <div class="card-header">
          <div class="header-dot disconnected"></div>
          <span class="header-title">Not Connected</span>
        </div>
        <div class="card-body center-content">
          <div class="disconnected-icon">
            <svg width="24" height="24" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2">
              <line x1="1" y1="1" x2="23" y2="23"/>
              <path d="M16.72 11.06A10.94 10.94 0 0119 12.55"/>
              <path d="M5 12.55a10.94 10.94 0 015.17-2.39"/>
              <path d="M10.71 5.05A16 16 0 0122.56 9"/>
              <path d="M1.42 9a15.91 15.91 0 014.7-2.88"/>
              <path d="M8.53 16.11a6 6 0 016.95 0"/>
              <line x1="12" y1="20" x2="12.01" y2="20"/>
            </svg>
          </div>
          {#if tokens.length === 0}
            <p class="disconnected-title">No tokens configured</p>
            <p class="hint">Add a token to connect this extension to your WeaveMind projects.</p>
            <button class="btn btn-primary" style="margin-top: 10px" onclick={() => showSettings = true}>
              Open Settings
            </button>
          {:else}
            <p class="disconnected-title">Connection failed</p>
            <p class="hint">Could not reach the server. Check that WeaveMind is running and your tokens are valid.</p>
            <button class="btn btn-secondary" style="margin-top: 10px" onclick={refresh}>
              Retry
            </button>
          {/if}
        </div>
      </div>
    {:else if error}
      <!-- Error State -->
      <div class="card">
        <div class="card-header">
          <div class="header-dot error"></div>
          <span class="header-title">Error</span>
        </div>
        <div class="card-body">
          <div class="error-box">{error}</div>
          <button class="btn btn-secondary" onclick={refresh}>Retry</button>
        </div>
      </div>
    {:else if tasks.length === 0 && triggers.length === 0 && actions.length === 0}
      <!-- Empty State -->
      <div class="card">
        <div class="card-header">
          <div class="header-dot success"></div>
          <span class="header-title">All Clear</span>
        </div>
        <div class="card-body center-content">
          <div class="empty-icon">
            <svg width="24" height="24" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2">
              <path d="M22 11.08V12a10 10 0 11-5.93-9.14"/>
              <path d="M22 4L12 14.01l-3-3"/>
            </svg>
          </div>
          <p class="empty-title">No pending items</p>
          <p class="hint">Tasks and actions from your projects will appear here</p>
        </div>
      </div>
    {:else}
      <!-- Tasks Section -->
      {#if tasks.length > 0}
        <div class="section-header">
          <span class="section-title">Tasks</span>
          <span class="section-count">{tasks.length}</span>
          <button class="clear-all-btn" onclick={handleClearAll} title="Delete every pending task (use this to flush orphan requests from cancelled runs)">
            Clear all
          </button>
        </div>
        <div class="tasks-container">
          {#each tasks as task}
            <div class="task-card-wrapper">
              <button class="task-card" onclick={() => openTaskInDashboard(task)}>
                <div class="task-card-header">
                  <div class="task-dot"></div>
                  <span class="task-title">{task.title}</span>
                  <span class="task-time">{formatTime(task.createdAt)}</span>
                </div>
                {#if task.description}
                  <p class="task-preview">{task.description}</p>
                {/if}
              </button>
              {#if extractExecutionUuid(task)}
                <button class="task-clear-run" onclick={() => handleClearForTask(task)} title="Clear every pending task for this run">
                  <svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2">
                    <polyline points="3 6 5 6 21 6"/><path d="M19 6l-1 14a2 2 0 01-2 2H8a2 2 0 01-2-2L5 6"/><path d="M10 11v6M14 11v6"/>
                  </svg>
                </button>
              {/if}
              <button class="task-cancel" onclick={() => handleCancelTask(task)} title="Cancel task (skip downstream)">
                <svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2">
                  <path d="M18 6L6 18M6 6l12 12"/>
                </svg>
              </button>
            </div>
          {/each}
        </div>
      {/if}
      
      <!-- Triggers Section (persistent forms) -->
      {#if triggers.length > 0}
        <div class="section-header">
          <span class="section-title">Triggers</span>
          <span class="section-count">{triggers.length}</span>
        </div>
        <div class="tasks-container">
          {#each triggers as trigger}
            <button class="task-card trigger-card" onclick={() => openTaskInDashboard(trigger)}>
              <div class="task-card-header">
                <div class="trigger-dot"></div>
                <span class="task-title">{trigger.title}</span>
              </div>
              {#if trigger.description}
                <p class="task-preview">{trigger.description}</p>
              {/if}
            </button>
          {/each}
        </div>
      {/if}

      <!-- Actions Section (URLs to open) -->
      {#if actions.length > 0}
        <div class="section-header">
          <span class="section-title">Links</span>
          <span class="section-count">{actions.length}</span>
        </div>
        <div class="tasks-container">
          {#each actions as action}
            <div class="action-card">
              <div class="action-card-row">
                <button class="action-link" onclick={() => { openActionUrl(action); handleDismissAction(action); }}>
                  <span class="action-dot"></span>
                  <span class="action-url">{action.actionUrl || 'Open link'}</span>
                </button>
                <button class="action-dismiss" onclick={() => handleDismissAction(action)} title="Dismiss">
                  <svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2">
                    <path d="M18 6L6 18M6 6l12 12"/>
                  </svg>
                </button>
              </div>
            </div>
          {/each}
        </div>
      {/if}
    {/if}
  </div>
</div>

<style>
  * {
    margin: 0;
    padding: 0;
    box-sizing: border-box;
  }

  /* Root container with dot pattern */
  .extension-root {
    font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif;
    width: 340px;
    min-height: 420px;
    max-height: 500px;
    background: #fafafa;
    position: relative;
    overflow: hidden;
  }

  .dot-pattern {
    position: absolute;
    inset: 0;
    pointer-events: none;
    background-image: radial-gradient(circle, #d4d4d8 1px, transparent 1px);
    background-size: 20px 20px;
  }

  .extension-content {
    position: relative;
    z-index: 1;
    padding: 12px;
    display: flex;
    flex-direction: column;
    gap: 10px;
    max-height: 500px;
    overflow-y: auto;
  }

  /* Card styles (node-like) */
  .card {
    background: white;
    border-radius: 8px;
    box-shadow: 0 2px 8px rgba(0, 0, 0, 0.08);
    border: 1px solid #e4e4e7;
    overflow: hidden;
  }

  .card-header {
    display: flex;
    align-items: center;
    gap: 8px;
    padding: 10px 12px;
    border-bottom: 1px solid #f4f4f5;
  }

  .header-dot {
    width: 8px;
    height: 8px;
    border-radius: 50%;
    background: #f59e0b;
    flex-shrink: 0;
  }

  .header-dot.loading { background: #f59e0b; }
  .header-dot.connected { background: #22c55e; }
  .header-dot.disconnected { background: #ef4444; }
  .header-dot.error { background: #ef4444; }
  .header-dot.success { background: #22c55e; }

  .header-title {
    font-size: 13px;
    font-weight: 600;
    color: #3f3f46;
    flex: 1;
    white-space: nowrap;
    overflow: hidden;
    text-overflow: ellipsis;
  }

  .card-body {
    padding: 14px;
  }

  .card-body.center-content {
    display: flex;
    flex-direction: column;
    align-items: center;
    justify-content: center;
    padding: 28px 14px;
    text-align: center;
  }

  /* Header card with actions */
  .header-card .card-header {
    border-bottom: none;
  }

  .header-card {
    display: flex;
    flex-direction: row;
    align-items: center;
    padding: 8px 10px;
    flex-shrink: 0;
  }

  .header-card .card-header {
    flex: 1;
    padding: 0;
    border: none;
  }

  .status-badge {
    font-size: 10px;
    font-weight: 500;
    padding: 1px 7px;
    border-radius: 10px;
    white-space: nowrap;
  }

  .status-badge.connected {
    background: #f0fdf4;
    color: #16a34a;
  }

  .status-badge.disconnected {
    background: #fef2f2;
    color: #dc2626;
  }

  .status-badge.loading {
    background: #fffbeb;
    color: #d97706;
  }

  .header-actions {
    display: flex;
    gap: 4px;
  }

  .action-btn {
    width: 28px;
    height: 28px;
    border: none;
    background: #f4f4f5;
    border-radius: 6px;
    cursor: pointer;
    display: flex;
    align-items: center;
    justify-content: center;
    color: #71717a;
    transition: all 0.15s;
  }

  .action-btn:hover {
    background: #e4e4e7;
    color: #3f3f46;
  }

  .action-btn.active {
    background: #f59e0b;
    color: white;
  }

  /* Form elements */
  .form-group {
    margin-bottom: 12px;
  }

  .form-group:last-child {
    margin-bottom: 0;
  }

  .label {
    display: block;
    font-size: 11px;
    font-weight: 500;
    color: #71717a;
    margin-bottom: 6px;
  }

  .hint {
    font-size: 11px;
    color: #a1a1aa;
    margin: 0;
  }

  .input {
    width: 100%;
    padding: 8px 10px;
    font-size: 12px;
    border: 1px solid #e4e4e7;
    border-radius: 6px;
    background: #fafafa;
    color: #3f3f46;
    transition: all 0.15s;
  }

  .input:focus {
    outline: none;
    border-color: #f59e0b;
    box-shadow: 0 0 0 2px rgba(245, 158, 11, 0.15);
  }

  .input:disabled {
    background: #f4f4f5;
    color: #a1a1aa;
  }

  /* Toggle */
  .toggle-row {
    display: flex;
    align-items: center;
    justify-content: space-between;
    gap: 12px;
  }

  .toggle-btn {
    position: relative;
    width: 36px;
    height: 20px;
    background: #d4d4d8;
    border: none;
    border-radius: 10px;
    cursor: pointer;
    transition: background 0.2s;
    padding: 0;
    flex-shrink: 0;
  }

  .toggle-btn.active {
    background: #f59e0b;
  }

  .toggle-slider {
    position: absolute;
    top: 2px;
    left: 2px;
    width: 16px;
    height: 16px;
    background: white;
    border-radius: 50%;
    transition: transform 0.2s;
    box-shadow: 0 1px 2px rgba(0, 0, 0, 0.15);
  }

  .toggle-btn.active .toggle-slider {
    transform: translateX(16px);
  }

  /* Divider */
  .divider {
    height: 1px;
    background: #e4e4e7;
    margin: 14px 0;
  }

  /* Buttons */
  .btn {
    padding: 8px 14px;
    font-size: 12px;
    font-weight: 500;
    border: none;
    border-radius: 6px;
    cursor: pointer;
    transition: all 0.15s;
    display: inline-flex;
    align-items: center;
    justify-content: center;
    gap: 6px;
  }

  .btn:disabled {
    opacity: 0.5;
    cursor: not-allowed;
  }

  .btn-primary {
    background: #27272a;
    color: white;
  }

  .btn-primary:hover:not(:disabled) {
    background: #3f3f46;
  }

  .btn-secondary {
    background: #f4f4f5;
    color: #3f3f46;
  }

  .btn-secondary:hover:not(:disabled) {
    background: #e4e4e7;
  }

  /* Token list */
  .token-list {
    display: flex;
    flex-direction: column;
    gap: 6px;
    margin-bottom: 12px;
  }

  .token-item {
    display: flex;
    align-items: center;
    justify-content: space-between;
    padding: 8px 10px;
    background: #fafafa;
    border: 1px solid #e4e4e7;
    border-radius: 6px;
  }

  .token-info {
    display: flex;
    flex-direction: column;
    gap: 2px;
    min-width: 0;
  }

  .token-name {
    font-size: 12px;
    font-weight: 500;
    color: #3f3f46;
  }

  .token-url {
    font-size: 10px;
    color: #a1a1aa;
    white-space: nowrap;
    overflow: hidden;
    text-overflow: ellipsis;
  }

  .remove-btn {
    width: 24px;
    height: 24px;
    border: none;
    background: none;
    color: #a1a1aa;
    cursor: pointer;
    border-radius: 4px;
    font-size: 16px;
    display: flex;
    align-items: center;
    justify-content: center;
    transition: all 0.15s;
    flex-shrink: 0;
  }

  .remove-btn:hover {
    background: #fef2f2;
    color: #ef4444;
  }

  .add-token-form {
    display: flex;
    flex-direction: column;
    gap: 8px;
  }

  .empty-text {
    font-size: 11px;
    color: #a1a1aa;
    text-align: center;
    padding: 12px;
  }

  /* Spinner */
  .spinner {
    width: 24px;
    height: 24px;
    border: 2px solid #e4e4e7;
    border-top-color: #f59e0b;
    border-radius: 50%;
    animation: spin 0.7s linear infinite;
    margin-bottom: 10px;
  }

  .spinner-small {
    width: 14px;
    height: 14px;
    border: 2px solid rgba(255,255,255,0.3);
    border-top-color: white;
    border-radius: 50%;
    animation: spin 0.7s linear infinite;
  }

  @keyframes spin {
    to { transform: rotate(360deg); }
  }

  /* Error box */
  .error-box {
    padding: 10px 12px;
    background: #fef2f2;
    border: 1px solid #fecaca;
    border-radius: 6px;
    font-size: 12px;
    color: #dc2626;
    margin-bottom: 12px;
  }

  /* Empty state */
  /* Disconnected state */
  .disconnected-icon {
    width: 40px;
    height: 40px;
    background: #fef2f2;
    border-radius: 50%;
    display: flex;
    align-items: center;
    justify-content: center;
    color: #ef4444;
    margin-bottom: 10px;
  }

  .disconnected-title {
    font-size: 13px;
    font-weight: 500;
    color: #3f3f46;
    margin-bottom: 4px;
  }

  .empty-icon {
    width: 40px;
    height: 40px;
    background: #f0fdf4;
    border-radius: 50%;
    display: flex;
    align-items: center;
    justify-content: center;
    color: #22c55e;
    margin-bottom: 10px;
  }

  .empty-title {
    font-size: 13px;
    font-weight: 500;
    color: #3f3f46;
    margin-bottom: 4px;
  }

  /* Section headers */
  .section-header {
    display: flex;
    align-items: center;
    gap: 8px;
    padding: 6px 0;
    margin-top: 4px;
  }

  .section-title {
    font-size: 11px;
    font-weight: 600;
    color: #71717a;
    text-transform: uppercase;
    letter-spacing: 0.5px;
  }

  .section-count {
    font-size: 10px;
    background: #f4f4f5;
    color: #71717a;
    padding: 2px 6px;
    border-radius: 10px;
  }

  /* Task cards */
  .tasks-container {
    display: flex;
    flex-direction: column;
    gap: 8px;
  }

  .task-card-wrapper {
    display: flex;
    align-items: stretch;
    gap: 0;
    position: relative;
  }

  .task-card {
    background: white;
    border: 1px solid #e4e4e7;
    border-radius: 8px 0 0 8px;
    padding: 10px 12px;
    text-align: left;
    cursor: pointer;
    transition: all 0.15s;
    flex: 1;
    min-width: 0;
  }

  .task-card:hover {
    border-color: #d4d4d8;
    box-shadow: 0 2px 8px rgba(0, 0, 0, 0.06);
  }

  .task-clear-run,
  .task-cancel {
    display: flex;
    align-items: center;
    justify-content: center;
    width: 32px;
    background: white;
    border: 1px solid #e4e4e7;
    border-left: none;
    cursor: pointer;
    color: #a1a1aa;
    transition: all 0.15s;
    flex-shrink: 0;
  }

  .task-clear-run {
    border-radius: 0;
  }

  .task-cancel {
    border-radius: 0 8px 8px 0;
  }

  .task-clear-run:hover {
    background: #fff7ed;
    color: #f59e0b;
    border-color: #fed7aa;
  }

  .task-cancel:hover {
    background: #fef2f2;
    color: #ef4444;
    border-color: #fecaca;
  }

  .clear-all-btn {
    margin-left: auto;
    font-size: 11px;
    padding: 4px 10px;
    background: #fff7ed;
    border: 1px solid #fed7aa;
    border-radius: 6px;
    color: #b45309;
    cursor: pointer;
    font-weight: 500;
    transition: all 0.15s;
  }

  .clear-all-btn:hover {
    background: #fef3c7;
    border-color: #fcd34d;
  }

  .action-card {
    background: white;
    border: 1px solid #e4e4e7;
    border-radius: 8px;
    padding: 10px 12px;
    text-align: left;
    width: 100%;
  }

  .action-card-row {
    display: flex;
    align-items: center;
    gap: 8px;
  }

  .action-link {
    flex: 1;
    display: flex;
    align-items: center;
    gap: 8px;
    background: none;
    border: none;
    cursor: pointer;
    text-align: left;
    padding: 0;
  }

  .action-link:hover .action-url {
    color: #18181b;
    text-decoration: underline;
  }

  .action-url {
    font-size: 11px;
    color: #52525b;
    overflow: hidden;
    text-overflow: ellipsis;
    white-space: nowrap;
    max-width: 240px;
  }

  .action-dismiss {
    background: none;
    border: none;
    cursor: pointer;
    padding: 4px;
    color: #a1a1aa;
    border-radius: 4px;
    display: flex;
    align-items: center;
    justify-content: center;
  }

  .action-dismiss:hover {
    background: #f4f4f5;
    color: #71717a;
  }

  .task-card-header {
    display: flex;
    align-items: center;
    gap: 8px;
  }

  .task-dot {
    width: 6px;
    height: 6px;
    border-radius: 50%;
    background: #f59e0b;
    flex-shrink: 0;
  }

  .trigger-dot {
    width: 6px;
    height: 6px;
    border-radius: 50%;
    background: #8b5cf6;
    flex-shrink: 0;
  }

  .trigger-card {
    border-radius: 8px;
    border-left: 3px solid #8b5cf6;
  }

  .action-dot {
    width: 6px;
    height: 6px;
    border-radius: 50%;
    background: #22c55e;
    flex-shrink: 0;
  }

  .task-title {
    font-size: 12px;
    font-weight: 500;
    color: #3f3f46;
    flex: 1;
    white-space: nowrap;
    overflow: hidden;
    text-overflow: ellipsis;
  }

  .task-time {
    font-size: 10px;
    color: #a1a1aa;
    flex-shrink: 0;
  }

  .task-preview {
    font-size: 11px;
    color: #71717a;
    margin-top: 6px;
    overflow: hidden;
    text-overflow: ellipsis;
    white-space: nowrap;
  }

</style>
