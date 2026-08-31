<script lang="ts">
  import { onMount } from 'svelte';
  import { fetchPendingTasks, skipTask, cancelRun, clearAll, getTokens, addToken, removeToken, hostPermissionPattern, isTrigger, releaseHostIfUnused, GRANT_DECLINED_MESSAGE, type PendingTask, type ApiToken } from '../../lib/api';
  import { getSettings, saveSettings } from '../../lib/settings';
  import { singleFlight } from '../../lib/single-flight';

  let allItems = $state<PendingTask[]>([]);
  let loading = $state(true);
  let connected = $state(false);
  let error = $state<string | null>(null);
  /// A non-failure outcome worth saying (a clear-all's counts); shares
  /// the banner slot with `error`, styled as information, not alarm.
  let notice = $state<string | null>(null);
  let showSettings = $state(false);
  let tokens = $state<ApiToken[]>([]);

  /// Every token whose tasks could NOT load this refresh, with why:
  /// its host grant is missing (`ungranted`, the actionable one) or
  /// its fetch failed (`unreached`, with the failure's own sentence in
  /// `detail`: a down runtime and one answering garbage are different
  /// problems). One list for both, because they are two reasons for
  /// the same user-visible fact, and a popup that shows "Connected"
  /// while a down runtime's tasks are silently absent is lying by
  /// omission.
  type TokenIssue = { token: ApiToken; reason: 'ungranted' | 'unreached'; detail: string };
  let tokenIssues = $state<TokenIssue[]>([]);

  // The list splits into two sections; `isTrigger` (api.ts) carries
  // the full trigger-vs-resume story.
  const triggerTasks = $derived(allItems.filter(isTrigger));
  const resumeTasks = $derived(allItems.filter((t) => !isTrigger(t)));

  // New token form
  let newTokenUrl = $state('');
  let newTokenName = $state('');
  let addingToken = $state(false);

  // Settings
  let notificationsEnabled = $state(true);
  /// Token strings whose host the browser does NOT currently grant
  /// (revoked in browser settings, or a pre-grant install); drives the
  /// Grant access row in Settings.
  const tokensNeedingGrant = $derived(
    new Set(tokenIssues.filter((i) => i.reason === 'ungranted').map((i) => i.token.token)),
  );

  /// The message banner element, scrolled to whenever a message
  /// lands: the content area scrolls, and with settings open the user
  /// is usually below the fold when the message renders.
  let bannerEl = $state<HTMLElement | undefined>();
  $effect(() => {
    if (error || notice) bannerEl?.scrollIntoView({ block: 'nearest' });
  });

  async function loadSettings() {
    notificationsEnabled = (await getSettings()).notificationsEnabled;
  }

  async function toggleNotifications() {
    notificationsEnabled = !notificationsEnabled;
    await saveSettings({ notificationsEnabled });
  }

  /// The token strings among `current` whose host the browser does
  /// not grant. A fetch to an ungranted host fails exactly like a dead
  /// server, so this is the only way to tell the user the truth about
  /// WHY nothing loads.
  async function tokensWithoutGrant(current: ApiToken[]): Promise<Set<string>> {
    const missing = new Set<string>();
    for (const t of current) {
      try {
        const granted = await browser.permissions.contains({
          origins: [hostPermissionPattern(t.dispatcherUrl)],
        });
        if (!granted) missing.add(t.token);
      } catch {
        missing.add(t.token);
      }
    }
    return missing;
  }

  /// Re-request a stored token's host grant (first await: the prompt
  /// needs the click's user gesture).
  async function handleGrantAccess(t: ApiToken) {
    try {
      const granted = await browser.permissions.request({
        origins: [hostPermissionPattern(t.dispatcherUrl)],
      });
      if (granted) {
        await refreshFlight.afterNow();
      } else {
        error = GRANT_DECLINED_MESSAGE;
      }
    } catch (e) {
      error = e instanceof Error ? e.message : 'Could not request access';
    }
  }

  onMount(async () => {
    await loadSettings();
    // refresh() loads the tokens itself.
    await refresh();
  });

  // Single-flight: a second Refresh click joins the pending run (a
  // staler response landing last would stomp the fresh one), while a
  // refresh AFTER a mutation uses `afterNow`, guaranteeing a fetch
  // that started after the mutation (joining an older in-flight one
  // would render pre-mutation state as if it were the result).
  const refreshFlight = singleFlight(async () => {
    loading = true;
    error = null;
    notice = null;

    try {
      // One storage read: the fetch returns the configured tokens it
      // used, so this list and the results can never disagree.
      const result = await fetchPendingTasks({ timeoutMs: 10000 });
      tokens = result.configured;
      const ungranted = await tokensWithoutGrant(tokens);
      if (tokens.length === 0) {
        connected = false;
        allItems = [];
        tokenIssues = [];
        return;
      }
      allItems = result.tasks;
      connected = result.anyReachable;
      // Missing grant wins as the reason (it is the actionable one,
      // and an ungranted host always also reads as failed).
      const failureDetails = new Map(result.failures.map((f) => [f.token.token, f.detail]));
      tokenIssues = tokens
        .filter((t) => ungranted.has(t.token) || failureDetails.has(t.token))
        .map((t) => ({
          token: t,
          reason: ungranted.has(t.token) ? ('ungranted' as const) : ('unreached' as const),
          detail: failureDetails.get(t.token) ?? '',
        }));
    } catch (e) {
      error = e instanceof Error ? e.message : 'Failed to connect';
      connected = false;
      // The banner must never outlive its evidence: everything that
      // can throw here throws BEFORE anything was learned about the
      // tokens (tokensWithoutGrant swallows per-token errors), so
      // claim nothing.
      tokenIssues = [];
    } finally {
      loading = false;
    }
  });
  const refresh = refreshFlight.join;

  /// Skip = "I don't want to answer this one." Resume the lane
  /// with null; the rest of the run keeps going.
  async function handleSkipTask(task: PendingTask) {
    try {
      await skipTask(task);
      await refreshFlight.afterNow();
    } catch (e) {
      error = e instanceof Error ? e.message : 'Failed to skip task';
    }
  }

  /// Cancel = "kill this whole run." Drops every sibling task
  /// of the same execution. Confirm before doing it because the
  /// blast radius is bigger than the visible task.
  async function handleCancelRun(task: PendingTask) {
    if (!confirm('Cancel the entire run? Every related task will be dropped and the execution will be marked failed.')) {
      return;
    }
    try {
      await cancelRun(task);
      await refreshFlight.afterNow();
    } catch (e) {
      error = e instanceof Error ? e.message : 'Failed to cancel run';
    }
  }

  /// Clear all = cancel every run AND delete every trigger the tokens
  /// see. The trigger half is the sharp edge (a deleted trigger is a
  /// project's entry point, gone until the project re-registers), so
  /// the confirm names it and the result reports the real counts.
  async function handleClearAll() {
    if (!confirm(
      'Cancel every pending run AND delete every trigger these tokens see? '
      + 'Runs are marked failed (still inspectable in the journal); deleted '
      + 'triggers come back only when their project re-registers.',
    )) {
      return;
    }
    // Clear-all calls the per-token endpoint once per token. Each
    // call cancels every distinct execution that token sees, so
    // iterating across configured tokens covers everything. Failures
    // are COLLECTED and reported, never swallowed: a runtime that was
    // down still has its runs pending, and a popup that looks cleared
    // over them would be lying.
    const failed: string[] = [];
    let cancelled = 0;
    let triggersDropped = 0;
    for (const t of tokens) {
      try {
        const counts = await clearAll(t);
        cancelled += counts.colorsCancelled;
        triggersDropped += counts.entrySignalsDropped;
      } catch (e) {
        console.warn('[weft] clearAll failed for', t.name, e);
        failed.push(t.name);
      }
    }
    await refreshFlight.afterNow();
    // The refresh may itself have failed and written `error`; the
    // counts must not vanish behind it (the clear already happened).
    const refreshError = error;
    const summary =
      `Cancelled ${cancelled} run${cancelled === 1 ? '' : 's'}, deleted `
      + `${triggersDropped} trigger${triggersDropped === 1 ? '' : 's'}.`;
    if (failed.length > 0) {
      error =
        `${summary} Could not clear ${failed.join(', ')}: those runtimes did not `
        + 'answer, so their runs are still pending. Retry when they are reachable.';
    } else if (refreshError) {
      error = `${summary} The refresh after it failed: ${refreshError}`;
    } else {
      notice = summary;
    }
  }

  async function handleAddToken() {
    if (!newTokenUrl.trim()) return;

    addingToken = true;
    error = null;

    try {
      // Accepted formats (the address the website / `weft token mint` copies):
      //   http://host:port/signal-token/TOKEN          (server-qualified address)
      //   http://host:port/signal-token/TOKEN/signals  (with the /signals suffix)
      //   TOKEN                                        (uses http://localhost:9999)
      let token: string;
      let dispatcherUrl: string;

      if (newTokenUrl.startsWith('http')) {
        const url = new URL(newTokenUrl);
        dispatcherUrl = `${url.protocol}//${url.host}`;
        const pathParts = url.pathname.split('/').filter(Boolean);
        const idx = pathParts.indexOf('signal-token');
        if (idx >= 0 && pathParts[idx + 1]) {
          token = pathParts[idx + 1];
        } else {
          throw new Error('Invalid URL format. Expected: http://localhost:9999/signal-token/TOKEN');
        }
      } else {
        token = newTokenUrl.trim();
        dispatcherUrl = 'http://localhost:9999';
      }

      // Ask the browser for access to this runtime's host. The manifest
      // grants no host access of its own, so the extension can only ever
      // reach addresses the user granted here, one by one. This must be
      // the first await in the click handler: a permission prompt is
      // only allowed inside a user gesture, and the gesture expires at
      // the first unrelated await.
      const pattern = hostPermissionPattern(dispatcherUrl);
      const granted = await browser.permissions.request({ origins: [pattern] });
      if (!granted) {
        throw new Error(GRANT_DECLINED_MESSAGE);
      }

      try {
        // The pasted string is parsed apart; on the wire the token only
        // ever travels in the Authorization header, never a URL path.
        const response = await fetch(`${dispatcherUrl}/signal-token/health`, {
          method: 'GET',
          headers: { Authorization: `Bearer ${token}` },
          signal: AbortSignal.timeout(5000),
        });
        if (!response.ok) {
          throw new Error('Invalid token or dispatcher unreachable');
        }
        // No name given = keep the existing one on a re-paste, or let
        // the store generate one on a first add.
        await addToken({
          token,
          name: newTokenName.trim() || undefined,
          dispatcherUrl,
        });
      } catch (e) {
        // The grant just landed but no token references it: hand it
        // back, or three typos would leave three invisible standing
        // grants nothing can ever revoke through the UI.
        await releaseHostIfUnused(dispatcherUrl, await getTokens());
        throw e;
      }

      newTokenUrl = '';
      newTokenName = '';
      await refreshFlight.afterNow();
    } catch (e) {
      error = e instanceof Error ? e.message : 'Failed to add token';
    } finally {
      addingToken = false;
    }
  }

  async function handleRemoveToken(tokenId: string) {
    try {
      await removeToken(tokenId);
    } catch (e) {
      error = e instanceof Error ? e.message : 'Failed to remove token';
    } finally {
      // The list must reflect storage even when the removal threw
      // half-way, or the popup shows a token that is already gone.
      await refreshFlight.afterNow();
    }
  }

  /// Open the task in the extension-hosted full-page runner. The
  /// hash carries the signal token so the runner knows which task
  /// to focus among the cross-token list it loads.
  function openTaskInRunner(task: PendingTask) {
    const url = `${browser.runtime.getURL('/tasks.html')}#/${encodeURIComponent(task.token)}`;
    browser.tabs.create({ url });
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
        <button class="action-btn" onclick={refresh} disabled={loading} title="Refresh" aria-label="Refresh">
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

    {#if !loading && (error || notice)}
      <!-- THE one renderer for `error` / `notice`, settings open or
           not: an outcome (a skip refused, a grant declined, a
           clear-all's counts) is a message OVER the view, never a
           view of its own: replacing eight visible tasks with one
           error card would hide everything that still works. Scrolled
           into view on change: with settings open the container
           scrolls, and a message rendered above the fold would look
           like nothing happened. -->
      <div class={error ? 'error-banner' : 'notice-banner'} bind:this={bannerEl}>
        <span>{error ?? notice}</span>
        <button class="dismiss-btn" onclick={() => { error = null; notice = null; }} title="Dismiss" aria-label="Dismiss message">×</button>
      </div>
    {/if}
    {#if !loading && !showSettings && tokenIssues.length > 0}
      <!-- One row per token whose tasks could not load, whatever the
           reason: with one working token the connected view renders,
           and nothing else would say why another token's tasks are
           silently missing. -->
      <div class="grant-banner">
        <div class="issue-rows">
          {#each tokenIssues as issue}
            <div class="issue-row">
              <span class="issue-text">
                <strong>{issue.token.name}</strong>
                {#if issue.reason === 'ungranted'}
                  has no access to its runtime’s address; its tasks cannot load.
                {:else if issue.detail}
                  could not load its tasks: {issue.detail}
                {:else}
                  did not answer; its tasks are not shown.
                {/if}
              </span>
              {#if issue.reason === 'ungranted'}
                <button class="grant-btn" onclick={() => handleGrantAccess(issue.token)}>Grant access</button>
              {:else}
                <button class="grant-btn" onclick={refresh}>Retry</button>
              {/if}
            </div>
          {/each}
        </div>
      </div>
    {/if}
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
                <span class="label">Notifications</span>
                <p class="hint">Notify when a new task arrives</p>
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
                  {#if tokensNeedingGrant.has(tokenConfig.token)}
                    <!-- The browser revoked (or never held) this host's
                         access; without it every poll fails looking
                         exactly like a dead server. -->
                    <button class="grant-btn" onclick={() => handleGrantAccess(tokenConfig)}>Grant access</button>
                  {/if}
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
            <!-- The per-token banner above already names WHO failed
                 and why (a missing grant included, with its Grant
                 access button); this card is the summary. -->
            <p class="disconnected-title">Connection failed</p>
            <p class="hint">Could not reach the server. Check that WeaveMind is running and your tokens are valid.</p>
            <button class="btn btn-secondary" style="margin-top: 10px" onclick={refresh}>
              Retry
            </button>
          {/if}
        </div>
      </div>
    {:else if allItems.length === 0}
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
          <p class="hint">Tasks from your projects will appear here</p>
        </div>
      </div>
    {:else}
      {#if triggerTasks.length > 0}
        <div class="section-header">
          <span class="section-title">Triggers</span>
          <span class="section-count">{triggerTasks.length}</span>
        </div>
        <div class="tasks-container">
          {#each triggerTasks as task}
            {@render taskCard(task)}
          {/each}
        </div>
      {/if}
      <div class="section-header">
        <span class="section-title">Tasks</span>
        <span class="section-count">{resumeTasks.length}</span>
        <button
          class="clear-all-btn"
          onclick={handleClearAll}
          title="Cancel every pending run this token sees. Drops every related task; runs are marked failed (still inspectable in the journal)."
        >
          Clear all
        </button>
      </div>
      <div class="tasks-container">
        {#each resumeTasks as task}
          {@render taskCard(task)}
        {/each}
      </div>
    {/if}
  </div>
</div>

<!-- One card renderer for both sections. `isTrigger` gates the
     skip/cancel buttons (THE shared trigger test, same as the task
     runner's): a RESUME task acts on a paused run (skip = answer null,
     cancel = kill the run), so it shows them; a TRIGGER has no
     in-flight run to skip or cancel, so opening it (to submit its form
     and START a run) is the only action. -->
{#snippet taskCard(task: PendingTask)}
  <div class="task-card-wrapper">
    <button class="task-card" onclick={() => openTaskInRunner(task)}>
      <div class="task-card-header">
        <div class="task-dot"></div>
        <span class="task-title">{task.title}</span>
      </div>
      {#if task.description}
        <p class="task-preview">{task.description}</p>
      {/if}
    </button>
    {#if !isTrigger(task)}
      <button
        class="task-skip"
        onclick={() => handleSkipTask(task)}
        title="Skip: answer this task with null. The rest of the run continues."
        aria-label="Skip task"
      >
        <svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2">
          <polygon points="5 4 15 12 5 20"/><line x1="19" y1="5" x2="19" y2="19"/>
        </svg>
      </button>
      <button
        class="task-cancel"
        onclick={() => handleCancelRun(task)}
        title="Cancel run: kill this entire execution. Every related task is dropped and the run is marked failed (you can still inspect it in the journal)."
        aria-label="Cancel run"
      >
        <svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2">
          <polyline points="3 6 5 6 21 6"/><path d="M19 6l-1 14a2 2 0 01-2 2H8a2 2 0 01-2-2L5 6"/><path d="M10 11v6M14 11v6"/>
        </svg>
      </button>
    {/if}
  </div>
{/snippet}

<style>
  /* Browser popup chrome sizing (the overflow clamp included).
     Scoped via `:global()` so it lands in the popup's per-entry CSS
     chunk only: anything shared between the entries lives in
     lib/reset.css, and a popup-sized rule reaching the shared chunk
     would clamp the full-tab tasks page (vite hoists CSS imported by
     multiple entries into one shared chunk). */
  /* body carries the ONE overflow clamp on purpose (#app inherits its
     box; a second clamp there would just shadow this one). */
  :global(html), :global(body) {
    margin: 0;
    padding: 0;
    width: 340px;
    min-height: 420px;
    overflow: hidden;
  }
  :global(#app) {
    width: 340px;
    min-height: 420px;
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

  .grant-banner {
    margin-bottom: 8px;
    padding: 8px 10px;
    border: 1px solid #f59e0b;
    background: #fffbeb;
    color: #92400e;
    border-radius: 8px;
    font-size: 11px;
  }

  .issue-rows {
    display: flex;
    flex-direction: column;
    gap: 6px;
  }

  .issue-row {
    display: flex;
    align-items: center;
    gap: 8px;
  }

  .issue-text {
    flex: 1;
    min-width: 0;
  }

  .error-banner,
  .notice-banner {
    display: flex;
    align-items: flex-start;
    gap: 8px;
    margin-bottom: 8px;
    padding: 8px 10px;
    border-radius: 8px;
    font-size: 11px;
  }

  .error-banner {
    border: 1px solid #fecaca;
    background: #fef2f2;
    color: #dc2626;
  }

  .notice-banner {
    border: 1px solid #e4e4e7;
    background: #f4f4f5;
    color: #3f3f46;
  }

  .error-banner span,
  .notice-banner span {
    flex: 1;
    min-width: 0;
  }

  .dismiss-btn {
    border: none;
    background: none;
    color: inherit;
    cursor: pointer;
    font-size: 14px;
    line-height: 1;
    padding: 0 2px;
    flex-shrink: 0;
  }

  .grant-btn {
    border: 1px solid #f59e0b;
    background: #fffbeb;
    color: #b45309;
    cursor: pointer;
    border-radius: 4px;
    font-size: 10px;
    padding: 3px 6px;
    white-space: nowrap;
  }

  .grant-btn:hover {
    background: #fef3c7;
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

  .task-skip,
  .task-cancel {
    display: flex;
    align-items: center;
    justify-content: center;
    width: 32px;
    background: white;
    border: 1px solid #e4e4e7;
    border-left: none;
    border-radius: 0;
    cursor: pointer;
    color: #a1a1aa;
    transition: all 0.15s;
    flex-shrink: 0;
  }

  /* Cancel sits on the right edge: rounded right corner. */
  .task-cancel {
    border-radius: 0 8px 8px 0;
  }

  .task-skip:hover {
    background: #eff6ff;
    color: #2563eb;
    border-color: #bfdbfe;
  }

  .task-cancel:hover {
    background: #fef2f2;
    color: #ef4444;
    border-color: #fecaca;
  }

  .clear-all-btn {
    margin-left: auto;
    padding: 3px 9px;
    background: white;
    border: 1px solid #e4e4e7;
    border-radius: 4px;
    color: #71717a;
    font-size: 11px;
    cursor: pointer;
    transition: all 0.15s;
  }

  .clear-all-btn:hover {
    background: #fef2f2;
    color: #ef4444;
    border-color: #fecaca;
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

  .task-title {
    font-size: 12px;
    font-weight: 500;
    color: #3f3f46;
    flex: 1;
    white-space: nowrap;
    overflow: hidden;
    text-overflow: ellipsis;
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
