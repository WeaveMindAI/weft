<script lang="ts">
  import { onMount } from 'svelte';
  import {
    fetchPendingTasks,
    submitTask,
    skipTask,
    cancelRun,
    type PendingTask,
    type ApiToken,
  } from '../../lib/api';

  // ------------- Form schema types (mirror dispatcher wire shape) ----------
  //
  // The dispatcher serializes weft-core's `FormField` directly (camelCase).
  // Each field carries a `fieldType` (the catalog field-type id, e.g.
  // `text_input`, `approve_reject`), a `render` hint the consumer
  // interprets, optional pre-fill `value`, and the source `config`. We
  // render purely off `render.component` plus the spec-driven flags
  // (`source`, `multiple`, `prefilled`); we never branch on `fieldType`
  // here because that would re-bake catalog knowledge into the consumer.

  interface FormFieldRender {
    component: string;
    source?: 'static' | 'input';
    multiple?: boolean;
    prefilled?: boolean;
  }

  interface FormField {
    fieldType: string;
    key: string;
    label?: string;
    render: FormFieldRender;
    value?: unknown;
    config?: Record<string, unknown>;
  }

  interface FormSchema {
    fields: FormField[];
    title?: string;
    description?: string;
  }

  type EnrichedTask = PendingTask & {
    _tokenConfig?: ApiToken;
    formSchema?: FormSchema;
  };

  // ------------- State -------------

  /// Cross-token list of pending tasks. Sorted by createdAt asc so
  /// "next" walks chronologically.
  let allTasks = $state<EnrichedTask[]>([]);
  /// Index into allTasks. -1 = no task selected (initial / all done).
  let currentIndex = $state(-1);
  let loading = $state(true);
  let error = $state<string | null>(null);
  let submitting = $state(false);
  let completed = $state(false);

  let formValues = $state<Record<string, unknown>>({});
  let buttonDecisions = $state<Record<string, boolean | null>>({});

  const currentTask = $derived(
    currentIndex >= 0 && currentIndex < allTasks.length ? allTasks[currentIndex] : null,
  );
  const hasPrev = $derived(currentIndex > 0);
  const hasNext = $derived(currentIndex < allTasks.length - 1);
  const totalCount = $derived(allTasks.length);

  // ------------- Hash routing -------------
  // URL form: tasks.html#/{signalToken}
  // The signal token uniquely identifies a task across all
  // configured api_tokens. Empty hash → land on the first pending
  // task.

  function readHashToken(): string | null {
    const hash = window.location.hash.replace(/^#\/?/, '').trim();
    return hash || null;
  }

  function writeHashToken(token: string): void {
    // Use replaceState so prev/next don't litter browser history.
    history.replaceState(null, '', `#/${encodeURIComponent(token)}`);
  }

  // ------------- Load tasks -------------

  onMount(() => {
    // Kick off the initial fetch (don't await: onMount must
    // return synchronously so its cleanup callback typechecks).
    void refresh();
    window.addEventListener('hashchange', onHashChange);
    return () => window.removeEventListener('hashchange', onHashChange);
  });

  function onHashChange() {
    const tok = readHashToken();
    if (!tok) return;
    const idx = allTasks.findIndex((t) => t.token === tok);
    if (idx >= 0 && idx !== currentIndex) {
      jumpTo(idx);
    }
  }

  async function refresh() {
    loading = true;
    error = null;
    completed = false;
    try {
      const result = await fetchPendingTasks({ timeoutMs: 10000 });
      const tasks = result.tasks as EnrichedTask[];
      allTasks = tasks;

      if (tasks.length === 0) {
        currentIndex = -1;
        loading = false;
        return;
      }

      const wantedToken = readHashToken();
      let idx = wantedToken ? tasks.findIndex((t) => t.token === wantedToken) : -1;
      if (idx < 0) {
        idx = 0;
        writeHashToken(tasks[0].token);
      }
      jumpTo(idx);
    } catch (e) {
      error = e instanceof Error ? e.message : 'Failed to fetch tasks';
    } finally {
      loading = false;
    }
  }

  function jumpTo(idx: number) {
    if (idx < 0 || idx >= allTasks.length) return;
    currentIndex = idx;
    completed = false;
    error = null;
    const task = allTasks[idx];
    writeHashToken(task.token);
    initFormState(task);
  }

  function initFormState(t: EnrichedTask) {
    const fields = t.formSchema?.fields ?? [];
    const vals: Record<string, unknown> = {};
    const decisions: Record<string, boolean | null> = {};
    for (const f of fields) {
      if (!f.key) continue;
      const r = f.render;
      if (!r) continue;
      if (r.component === 'buttons') decisions[f.key] = null;
      else if (r.component === 'select' && r.multiple) vals[f.key] = [];
      else if ((r.component === 'textarea' || r.component === 'text') && r.prefilled)
        vals[f.key] = typeof f.value === 'string' ? f.value : '';
      else if (r.component !== 'readonly') vals[f.key] = '';
    }
    formValues = vals;
    buttonDecisions = decisions;
  }

  function isFormValid(): boolean {
    if (!currentTask?.formSchema) return true;
    for (const f of currentTask.formSchema.fields) {
      if (!f.key || !f.render) continue;
      if (f.render.component === 'buttons' && buttonDecisions[f.key] === null) return false;
    }
    return true;
  }

  // ------------- Submit / cancel / dismiss / advance -------------

  async function submitForm() {
    const t = currentTask;
    if (!t) return;
    if (!t._tokenConfig) {
      error = 'Task missing token configuration';
      return;
    }

    submitting = true;
    error = null;
    try {
      const inputPayload: Record<string, unknown> = { ...formValues };
      for (const [key, decision] of Object.entries(buttonDecisions)) {
        if (decision !== null) inputPayload[key] = decision;
      }

      // Single generic fire path: POST /signal/{token}. The
      // dispatcher relays to the listener's /process which decides
      // whether to resolve a suspension or fire an entry trigger.
      // The runner doesn't branch by taskType anymore.
      await submitTask(t, inputPayload);

      completed = true;
      setTimeout(() => advanceAfterSubmit(), 800);
    } catch (e) {
      error = e instanceof Error ? e.message : 'Failed to submit';
    } finally {
      submitting = false;
    }
  }

  function advanceAfterSubmit() {
    // Remove the submitted task from local state. The next task
    // shifts into our current index naturally.
    const newTasks = allTasks.filter((_, i) => i !== currentIndex);
    allTasks = newTasks;
    if (newTasks.length === 0) {
      currentIndex = -1;
      completed = true;
      return;
    }
    const nextIdx = Math.min(currentIndex, newTasks.length - 1);
    jumpTo(nextIdx);
  }

  /// Skip = resume this lane with null. Sibling tasks of the
  /// same run keep going. No confirmation: low blast radius.
  async function handleSkip() {
    const t = currentTask;
    if (!t) return;
    try {
      await skipTask(t);
      advanceAfterSubmit();
    } catch (e) {
      error = e instanceof Error ? e.message : 'Failed to skip';
    }
  }

  /// Cancel = kill the entire run. Confirm first because the
  /// blast radius is wider than the task on screen.
  async function handleCancelRun() {
    const t = currentTask;
    if (!t) return;
    if (!confirm('Cancel the entire run? Every related task will be dropped and the execution will be marked failed.')) return;
    try {
      await cancelRun(t);
      advanceAfterSubmit();
    } catch (e) {
      error = e instanceof Error ? e.message : 'Failed to cancel run';
    }
  }

  // ------------- UI helpers -------------

  function toggleMultiSelect(key: string, option: string) {
    const current = (formValues[key] as string[]) ?? [];
    formValues = {
      ...formValues,
      [key]: current.includes(option) ? current.filter((o) => o !== option) : [...current, option],
    };
  }

  function getOptions(field: FormField): string[] {
    if (field.render?.source === 'input')
      return Array.isArray(field.value) ? (field.value as string[]) : [];
    return (field.config?.options as string[]) ?? [];
  }

  function fmt(value: unknown): string {
    if (value === null || value === undefined) return '';
    if (typeof value === 'string') return value;
    if (typeof value === 'number' || typeof value === 'boolean') return String(value);
    return JSON.stringify(value, null, 2);
  }

  function isComplex(value: unknown): boolean {
    return typeof value === 'object' && value !== null;
  }

  // Keyboard shortcuts: ← / → to navigate, Ctrl+Enter to submit.
  function onKeyDown(e: KeyboardEvent) {
    if (e.target instanceof HTMLTextAreaElement) return;
    const isInput =
      e.target instanceof HTMLInputElement && e.target.type !== 'submit';
    if (e.key === 'ArrowLeft' && hasPrev && !isInput) {
      jumpTo(currentIndex - 1);
    } else if (e.key === 'ArrowRight' && hasNext && !isInput) {
      jumpTo(currentIndex + 1);
    } else if ((e.ctrlKey || e.metaKey) && e.key === 'Enter') {
      e.preventDefault();
      if (currentTask && !submitting && isFormValid()) submitForm();
    }
  }
</script>

<svelte:window on:keydown={onKeyDown} />
<svelte:head>
  <title>{currentTask?.title || 'WeaveMind Task'}</title>
</svelte:head>

<div class="page">
  <div class="dot-pattern"></div>

  <div class="content">
    <!-- Top nav bar -->
    <div class="topbar">
      <button class="nav-btn" disabled={!hasPrev} onclick={() => jumpTo(currentIndex - 1)} title="Previous task (←)" aria-label="Previous">
        <svg width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><polyline points="15 18 9 12 15 6"/></svg>
      </button>
      <div class="position">
        {#if totalCount > 0 && currentIndex >= 0}
          {currentIndex + 1} / {totalCount}
        {:else}
          0 / 0
        {/if}
      </div>
      <button class="nav-btn" disabled={!hasNext} onclick={() => jumpTo(currentIndex + 1)} title="Next task (→)" aria-label="Next">
        <svg width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><polyline points="9 18 15 12 9 6"/></svg>
      </button>
      <div class="spacer"></div>
      <button class="nav-btn" onclick={refresh} title="Refresh" aria-label="Refresh">
        <svg width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2">
          <path d="M23 4v6h-6M1 20v-6h6M3.51 9a9 9 0 0114.85-3.36L23 10M1 14l4.64 4.36A9 9 0 0020.49 15"/>
        </svg>
      </button>
      <button class="nav-btn" onclick={() => window.close()} title="Close" aria-label="Close">
        <svg width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><path d="M18 6L6 18M6 6l12 12"/></svg>
      </button>
    </div>

    <div class="card-wrap">
      {#if loading}
        <div class="card">
          <div class="card-header"><div class="dot loading"></div><span class="card-title">Loading</span></div>
          <div class="card-body center">
            <div class="spinner"></div>
            <p class="hint">Fetching tasks...</p>
          </div>
        </div>
      {:else if error && !currentTask}
        <div class="card">
          <div class="card-header"><div class="dot error"></div><span class="card-title">Error</span></div>
          <div class="card-body">
            <div class="error-box">{error}</div>
            <button class="btn btn-secondary" onclick={refresh}>Retry</button>
          </div>
        </div>
      {:else if completed && allTasks.length === 0}
        <div class="card">
          <div class="card-header"><div class="dot success"></div><span class="card-title">All Clear</span></div>
          <div class="card-body center">
            <div class="check-icon">
              <svg width="32" height="32" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><path d="M22 11.08V12a10 10 0 11-5.93-9.14"/><path d="M22 4L12 14.01l-3-3"/></svg>
            </div>
            <p class="big-msg">No more pending tasks</p>
            <button class="btn btn-secondary" style="margin-top: 16px" onclick={() => window.close()}>Close</button>
          </div>
        </div>
      {:else if currentTask}
        <div class="card">
          <div class="card-header">
            <div class="dot amber"></div>
            <div class="card-title-block">
              <span class="card-title">{currentTask.title}</span>
              {#if currentTask.description}
                <span class="card-desc">{currentTask.description}</span>
              {/if}
              {#if currentTask._tokenConfig}
                <span class="token-pill">{currentTask._tokenConfig.name}</span>
              {/if}
            </div>
          </div>

          {#if completed}
            <div class="card-body center">
              <div class="check-icon green">
                <svg width="32" height="32" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2.5"><polyline points="20 6 9 17 4 12"/></svg>
              </div>
              <p class="big-msg">Submitted</p>
              <p class="hint">Advancing to the next task...</p>
            </div>
          {:else}
            <div class="card-body">
              {#if error}
                <div class="error-box">{error}</div>
              {/if}

              {#if currentTask.formSchema}
                {#each currentTask.formSchema.fields as field}
                  {@const r = field.render}
                  {#if r?.component === 'readonly'}
                    <div class="field">
                      <p class="field-key">{(field.config?.label as string) || field.label || field.key}</p>
                      {#if isComplex(field.value)}
                        <pre class="readonly-pre">{fmt(field.value)}</pre>
                      {:else}
                        <p class="readonly-line">{fmt(field.value) || '(empty)'}</p>
                      {/if}
                    </div>
                  {:else if r?.component === 'image'}
                    {@const imgSrc = typeof field.value === 'string' ? field.value : ((field.value as Record<string, unknown>)?.url as string | undefined)}
                    <div class="field">
                      <p class="field-key">{(field.config?.label as string) ?? field.key}</p>
                      {#if imgSrc}
                        <img src={imgSrc} alt={(field.config?.label as string) ?? field.key} class="field-image" />
                      {:else}
                        <p class="field-empty">(no image)</p>
                      {/if}
                    </div>
                  {:else if r?.component === 'buttons'}
                    {@const decision = buttonDecisions[field.key]}
                    <div class="field">
                      <p class="field-key">{(field.config?.label as string) || field.label || field.key}</p>
                      <div class="btn-row">
                        <button
                          class="decision-btn {decision === false ? 'reject-active' : 'reject-idle'}"
                          onclick={() => { buttonDecisions = { ...buttonDecisions, [field.key]: false }; }}
                        >{(field.config?.rejectLabel as string) || 'Reject'}</button>
                        <button
                          class="decision-btn {decision === true ? 'approve-active' : 'approve-idle'}"
                          onclick={() => { buttonDecisions = { ...buttonDecisions, [field.key]: true }; }}
                        >{(field.config?.approveLabel as string) || 'Approve'}</button>
                      </div>
                    </div>
                  {:else if r?.component === 'select'}
                    {@const options = getOptions(field)}
                    {#if r.multiple}
                      {@const selected = (formValues[field.key] as string[]) ?? []}
                      <div class="field">
                        <p class="field-key">{(field.config?.label as string) || field.label || field.key}</p>
                        <div class="chip-row">
                          {#each options as option}
                            <button
                              class="chip {selected.includes(option) ? 'chip-active' : 'chip-idle'}"
                              onclick={() => toggleMultiSelect(field.key, option)}
                            >{option}</button>
                          {/each}
                          {#if options.length === 0}<p class="field-empty">No options available</p>{/if}
                        </div>
                      </div>
                    {:else}
                      <div class="field">
                        <p class="field-key">{(field.config?.label as string) || field.label || field.key}</p>
                        <div class="chip-row">
                          {#each options as option}
                            <button
                              class="chip {formValues[field.key] === option ? 'chip-active' : 'chip-idle'}"
                              onclick={() => { formValues = { ...formValues, [field.key]: option }; }}
                            >{option}</button>
                          {/each}
                          {#if options.length === 0}<p class="field-empty">No options available</p>{/if}
                        </div>
                      </div>
                    {/if}
                  {:else if r?.component === 'text'}
                    <div class="field">
                      <p class="field-key">{(field.config?.label as string) || field.label || field.key}</p>
                      <input
                        type="text"
                        class="text-input"
                        placeholder={field.key}
                        value={(formValues[field.key] as string) ?? ''}
                        oninput={(e) => { formValues = { ...formValues, [field.key]: e.currentTarget.value }; }}
                      />
                    </div>
                  {:else if r?.component === 'textarea'}
                    <div class="field">
                      <p class="field-key">{(field.config?.label as string) || field.label || field.key}</p>
                      <textarea
                        class="text-input"
                        rows={r.prefilled ? 6 : 3}
                        value={(formValues[field.key] as string) ?? ''}
                        oninput={(e) => { formValues = { ...formValues, [field.key]: e.currentTarget.value }; }}
                      ></textarea>
                    </div>
                  {/if}
                {/each}

                <div class="action-row">
                  <button
                    class="btn btn-primary"
                    onclick={submitForm}
                    disabled={submitting || !isFormValid()}
                    title="Ctrl+Enter"
                  >
                    {#if submitting}
                      <span class="spinner-small"></span>
                    {:else}
                      Submit
                    {/if}
                  </button>
                  <button
                    class="btn btn-secondary"
                    onclick={handleSkip}
                    title="Skip: answer this task with null. The rest of the run continues."
                  >
                    Skip
                  </button>
                  <button
                    class="btn btn-danger"
                    onclick={handleCancelRun}
                    title="Cancel run: kill this whole execution. Every related task is dropped and the run is marked failed (still inspectable in the journal)."
                  >
                    Cancel run
                  </button>
                </div>
              {:else}
                <p class="hint center-text">No form fields configured for this task.</p>
                <div class="action-row">
                  <button class="btn btn-secondary" onclick={handleSkip}>
                    Skip
                  </button>
                  <button class="btn btn-danger" onclick={handleCancelRun}>
                    Cancel run
                  </button>
                </div>
              {/if}
            </div>
          {/if}
        </div>
        <p class="task-id-foot">Task ID: {currentTask.token.slice(0, 8)}...</p>
      {:else}
        <div class="card">
          <div class="card-header"><div class="dot success"></div><span class="card-title">All Clear</span></div>
          <div class="card-body center">
            <p class="big-msg">No pending tasks</p>
            <button class="btn btn-secondary" style="margin-top: 16px" onclick={refresh}>Refresh</button>
          </div>
        </div>
      {/if}
    </div>
  </div>
</div>

<style>
  /* Full-tab page sizing. Scoped via `:global()` so the rules
     ride along in tasks-*.css (Svelte component CSS), not in
     the shared `app-*.css` chunk vite builds across entries.
     Without this, the popup's own html/body sizing wins in the
     shared chunk and clamps the task page to 340px. */
  :global(html), :global(body) {
    margin: 0;
    padding: 0;
    width: 100%;
    min-height: 100vh;
    background: #fafafa;
  }
  :global(#app) {
    width: 100%;
    min-height: 100vh;
  }

  .page {
    font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif;
    width: 100%;
    min-height: 100vh;
    position: relative;
    background: #fafafa;
    color: #18181b;
    /* Block layout with auto-margin centering on `.content` is more
       robust against host stylesheets than flex `align-items`;
       extension popup-page CSS sometimes overrides flex defaults
       and the form ends up flush-left. `margin: 0 auto` always
       centers a fixed-width child. */
    padding: 64px 24px 48px;
    box-sizing: border-box;
  }
  .dot-pattern {
    position: fixed;
    inset: 0;
    pointer-events: none;
    background-image: radial-gradient(circle, #d4d4d8 1px, transparent 1px);
    background-size: 24px 24px;
    z-index: 0;
  }
  .content {
    position: relative;
    z-index: 1;
    width: 100%;
    max-width: 720px;
    margin: 0 auto;
  }
  .topbar {
    display: flex;
    align-items: center;
    gap: 8px;
    margin-bottom: 16px;
    background: white;
    border: 1px solid #e4e4e7;
    border-radius: 8px;
    padding: 6px 8px;
    box-shadow: 0 1px 3px rgba(0, 0, 0, 0.04);
  }
  .nav-btn {
    display: flex;
    align-items: center;
    justify-content: center;
    width: 32px;
    height: 32px;
    border-radius: 6px;
    border: 1px solid #e4e4e7;
    background: white;
    cursor: pointer;
    color: #52525b;
    transition: background 0.15s;
  }
  .nav-btn:hover:not(:disabled) {
    background: #f4f4f5;
    color: #18181b;
  }
  .nav-btn:disabled {
    opacity: 0.35;
    cursor: not-allowed;
  }
  .position {
    font-size: 13px;
    font-weight: 500;
    color: #71717a;
    min-width: 60px;
    text-align: center;
    user-select: none;
  }
  .spacer { flex: 1; }
  .card-wrap { display: flex; flex-direction: column; align-items: center; }
  .card {
    width: 100%;
    background: white;
    border-radius: 10px;
    border: 1px solid #e4e4e7;
    /* v1 dashboard's shadow-lg: lifts the form off the dot pattern. */
    box-shadow: 0 10px 15px -3px rgba(0, 0, 0, 0.08), 0 4px 6px -4px rgba(0, 0, 0, 0.06);
    overflow: hidden;
  }
  .card-header {
    display: flex;
    align-items: flex-start;
    gap: 12px;
    padding: 14px 18px;
    border-bottom: 1px solid #f4f4f5;
  }
  .dot {
    width: 10px;
    height: 10px;
    border-radius: 50%;
    flex-shrink: 0;
    margin-top: 5px;
  }
  .dot.amber { background: #f59e0b; }
  .dot.loading { background: #f59e0b; }
  .dot.error { background: #ef4444; }
  .dot.success { background: #22c55e; }
  .card-title-block { flex: 1; display: flex; flex-direction: column; gap: 2px; }
  .card-title {
    font-size: 14px;
    font-weight: 600;
    color: #18181b;
    line-height: 1.3;
  }
  .card-desc {
    font-size: 12px;
    color: #71717a;
    line-height: 1.4;
  }
  .token-pill {
    align-self: flex-start;
    margin-top: 4px;
    font-size: 10px;
    font-weight: 500;
    background: #f4f4f5;
    color: #52525b;
    padding: 2px 8px;
    border-radius: 999px;
    text-transform: uppercase;
    letter-spacing: 0.05em;
  }
  .card-body { padding: 20px; }
  .card-body .field + .field { margin-top: 16px; }
  .card-body.center {
    display: flex;
    flex-direction: column;
    align-items: center;
    justify-content: center;
    text-align: center;
    padding: 36px 18px;
  }
  .field { margin-bottom: 16px; }
  .field:last-child { margin-bottom: 0; }
  .field-key {
    font-size: 12px;
    font-weight: 500;
    color: #71717a;
    margin: 0 0 6px;
  }
  .readonly-pre {
    font-size: 12px;
    background: #fafafa;
    border: 1px solid #e4e4e7;
    border-radius: 6px;
    padding: 10px 12px;
    font-family: ui-monospace, SFMono-Regular, monospace;
    white-space: pre-wrap;
    word-break: break-word;
    max-height: 160px;
    overflow: auto;
    margin: 0;
  }
  .readonly-line {
    font-size: 13px;
    background: #fafafa;
    border: 1px solid #e4e4e7;
    border-radius: 6px;
    padding: 8px 12px;
    margin: 0;
    min-height: 36px;
  }
  .field-image {
    max-width: 100%;
    max-height: 320px;
    border-radius: 6px;
    border: 1px solid #e4e4e7;
    background: #fafafa;
    object-fit: contain;
    display: block;
  }
  .field-empty {
    font-size: 12px;
    color: #a1a1aa;
    font-style: italic;
    margin: 0;
  }
  .text-input {
    width: 100%;
    box-sizing: border-box;
    padding: 8px 12px;
    background: #fafafa;
    border: 1px solid #e4e4e7;
    border-radius: 6px;
    font-size: 13px;
    color: #18181b;
    font-family: inherit;
    outline: none;
    transition: border-color 0.15s, background 0.15s;
    resize: vertical;
  }
  .text-input:focus {
    border-color: #f59e0b;
    background: white;
  }
  .btn-row { display: flex; gap: 8px; }
  .decision-btn {
    flex: 1;
    padding: 9px 16px;
    border: 1px solid transparent;
    border-radius: 6px;
    font-size: 13px;
    font-weight: 500;
    cursor: pointer;
    transition: background 0.15s;
    color: #18181b;
  }
  .reject-idle { background: #f4f4f5; }
  .reject-idle:hover { background: #e4e4e7; }
  .reject-active { background: #ef4444; color: white; }
  .approve-idle { background: #f4f4f5; }
  .approve-idle:hover { background: #e4e4e7; }
  .approve-active { background: #22c55e; color: white; }
  .chip-row { display: flex; flex-wrap: wrap; gap: 8px; }
  .chip {
    padding: 6px 12px;
    border: 1px solid transparent;
    border-radius: 6px;
    font-size: 12px;
    font-weight: 500;
    cursor: pointer;
    transition: background 0.15s;
  }
  .chip-idle { background: #f4f4f5; color: #52525b; }
  .chip-idle:hover { background: #e4e4e7; }
  .chip-active { background: #18181b; color: white; }

  .action-row {
    display: flex;
    gap: 8px;
    margin-top: 18px;
  }
  .btn {
    padding: 9px 18px;
    border: 1px solid transparent;
    border-radius: 6px;
    font-size: 13px;
    font-weight: 500;
    cursor: pointer;
    transition: background 0.15s;
    display: inline-flex;
    align-items: center;
    justify-content: center;
    gap: 6px;
  }
  .btn-primary {
    flex: 1;
    background: #18181b;
    color: white;
  }
  .btn-primary:hover:not(:disabled) { background: #27272a; }
  .btn-primary:disabled {
    background: #d4d4d8;
    cursor: not-allowed;
  }
  .btn-secondary {
    background: #f4f4f5;
    color: #52525b;
    border-color: #e4e4e7;
  }
  .btn-secondary:hover { background: #e4e4e7; }
  .btn-danger {
    background: #fef2f2;
    color: #dc2626;
    border-color: #fecaca;
  }
  .btn-danger:hover { background: #fee2e2; }

  .error-box {
    background: #fef2f2;
    border: 1px solid #fecaca;
    color: #dc2626;
    border-radius: 6px;
    padding: 8px 12px;
    font-size: 12px;
    margin-bottom: 12px;
  }
  .hint {
    font-size: 12px;
    color: #71717a;
    margin: 0;
  }
  .hint.center-text { text-align: center; padding: 16px 0; }
  .check-icon {
    color: #71717a;
  }
  .check-icon.green { color: #22c55e; }
  .big-msg {
    font-size: 14px;
    font-weight: 600;
    color: #18181b;
    margin: 12px 0 4px;
  }
  .task-id-foot {
    text-align: center;
    color: #a1a1aa;
    font-size: 11px;
    margin-top: 12px;
  }
  .spinner {
    width: 24px;
    height: 24px;
    border: 2px solid #e4e4e7;
    border-top-color: #f59e0b;
    border-radius: 50%;
    animation: spin 0.8s linear infinite;
  }
  .spinner-small {
    width: 12px;
    height: 12px;
    border: 2px solid rgba(255, 255, 255, 0.3);
    border-top-color: white;
    border-radius: 50%;
    animation: spin 0.6s linear infinite;
  }
  @keyframes spin {
    to { transform: rotate(360deg); }
  }
</style>
