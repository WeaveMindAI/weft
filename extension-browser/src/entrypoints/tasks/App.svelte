<script lang="ts">
  import { onMount } from 'svelte';
  import {
    fetchPendingTasks,
    fetchTaskFile,
    imageSourceOf,
    isStoredFileValue,
    isTrigger,
    submitTask,
    skipTask,
    cancelRun,
    type FormField,
    type PendingTask,
  } from '../../lib/api';
  import { singleFlight } from '../../lib/single-flight';

  // ------------- State -------------

  /// Cross-token list of pending tasks, in the dispatcher's order:
  /// triggers first, then resume tasks, each group oldest-first. So
  /// "next" walks chronologically within a group and crosses from the
  /// trigger group into the resume group (the trigger badge on the
  /// card marks the crossing).
  let allTasks = $state<PendingTask[]>([]);
  /// Index into allTasks. -1 = no task selected (initial / all done).
  let currentIndex = $state(-1);
  let loading = $state(true);
  let error = $state<string | null>(null);
  let submitting = $state(false);
  /// The task whose submit just landed, waiting out its 800ms advance
  /// window. TOKEN-scoped, never a bare boolean: the user can navigate
  /// mid-window, and a boolean would paint the "Submitted" body over
  /// whatever task happens to be on screen when the POST resolves.
  let settlingToken = $state<string | undefined>();
  /// Whether the card ON SCREEN is the just-submitted one.
  const settled = $derived(
    currentIndex >= 0 && allTasks[currentIndex]?.token === settlingToken,
  );
  /// The last settle emptied the list: the "no more pending tasks"
  /// variant of the All Clear card.
  let justFinished = $state(false);
  /// The URL named a task that is no longer pending (a stale
  /// notification, an old tab); the page says so instead of quietly
  /// substituting an unrelated task.
  let staleTaskRequested = $state(false);
  /// Every configured runtime answered the last fetch. When false, an
  /// absent task is NOT proven gone (its runtime may just be down or
  /// ungranted), so the done/stale claims soften accordingly.
  let allReached = $state(true);
  /// How many tokens are configured at all: zero must never read as
  /// "All Clear" (nothing was even asked).
  let configuredCount = $state(0);
  /// The pending advance-after-submit timer, cleared on unmount and on
  /// any manual navigation so it can never fire against a moved list.
  let advanceTimer: ReturnType<typeof setTimeout> | undefined;
  /// The answered task the pending advance will drop; carried beside
  /// the handle so a flush can run the drop it stands for.
  let advanceToken: string | undefined;

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
    return () => {
      window.removeEventListener('hashchange', onHashChange);
      cancelAdvance();
    };
  });

  function onHashChange() {
    focusToken(readHashToken());
  }

  /// THE one place "focus the task the URL names" lives, so a miss
  /// behaves the same whether it arrives through a refresh or a hash
  /// edit: no token focuses the first task; a listed token jumps to
  /// it; a missing one (a stale notification, an old tab) shows the
  /// Already-handled / Unreachable card instead of quietly keeping or
  /// substituting an unrelated task the user might answer by mistake
  /// (with an empty list the same card offers Close).
  function focusToken(tok: string | null) {
    if (!tok) {
      // jumpTo no-ops on an empty list, leaving no task focused.
      jumpTo(0);
      return;
    }
    const idx = allTasks.findIndex((t) => t.token === tok);
    if (idx < 0) {
      currentIndex = -1;
      staleTaskRequested = true;
      return;
    }
    // Already focused on this task: re-jumping would re-init the form
    // and wipe what the user typed.
    if (idx === currentIndex) return;
    jumpTo(idx);
  }

  // Single-flight: a second Refresh click joins the pending fetch
  // instead of racing it (the slower, staler response would land last
  // and re-init the form state the user is typing into).
  const refreshFlight = singleFlight(async () => {
    loading = true;
    error = null;
    settlingToken = undefined;
    justFinished = false;
    staleTaskRequested = false;
    // An answered task still inside its advance window is flushed out
    // NOW: replacing the list around a pending drop would cancel it,
    // leaving the answered task listed, re-focusable and
    // re-submittable (on a trigger, a second run).
    flushAdvance();
    const focusedToken = currentTask?.token ?? null;
    try {
      const result = await fetchPendingTasks({ timeoutMs: 10000 });
      allTasks = result.tasks;
      configuredCount = result.configured.length;
      // A task absent from the list is only PROVEN gone when every
      // configured runtime actually answered; a failed one (down, or
      // its host grant revoked, or answering garbage) hides its tasks
      // without ending them, and claiming "answered elsewhere" over
      // that would be a confident false statement about live work.
      allReached = result.failures.length === 0;
      // Focus by TOKEN identity across the swap: the task the user was
      // on keeps its focus (and their typed input) wherever it landed
      // in the new list; positional state would either wipe the form
      // or focus a stranger at the old index.
      currentIndex = focusedToken
        ? allTasks.findIndex((t) => t.token === focusedToken)
        : -1;
      focusToken(readHashToken());
    } catch (e) {
      error = e instanceof Error ? e.message : 'Failed to fetch tasks';
    } finally {
      loading = false;
    }
  });
  const refresh = refreshFlight.join;

  function jumpTo(idx: number) {
    if (idx < 0 || idx >= allTasks.length) return;
    // The target is held by TOKEN across the flush: a pending
    // post-submit drop must land before the list is walked (or
    // navigating during the 800ms window would cancel it, leaving the
    // answered task listed and re-submittable), and the drop shifts
    // every index after it. A target that WAS the flushed task comes
    // back -1, and the drop's own advance already focused its
    // neighbour, so nothing more to do.
    const target = allTasks[idx].token;
    flushAdvance();
    const at = allTasks.findIndex((t) => t.token === target);
    if (at < 0) return;
    currentIndex = at;
    justFinished = false;
    error = null;
    staleTaskRequested = false;
    writeHashToken(target);
    initFormState(allTasks[at]);
  }

  function initFormState(t: PendingTask) {
    const fields = t.formSchema?.fields ?? [];
    const vals: Record<string, unknown> = {};
    const decisions: Record<string, boolean | null> = {};
    for (const f of fields) {
      if (!f.key) continue;
      // `render` is non-nullable on the wire (the runtime refuses a
      // field without one at build time).
      const r = f.render;
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
      if (!f.key) continue;
      if (f.render.component === 'buttons' && buttonDecisions[f.key] === null) return false;
    }
    return true;
  }

  /// THE one answer to "may submit fire right now", for the button
  /// and the Ctrl+Enter path alike: two copies once let held-down
  /// key auto-repeat re-submit an already-settled task (a second
  /// POST, which for a trigger STARTS A SECOND RUN) because only the
  /// button's copy checked the settled state.
  const canSubmit = $derived(
    !!currentTask && !submitting && !settled && isFormValid(),
  );

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

      // Identity captured NOW: the user can navigate during the 800ms
      // pause, and dropping "whatever is at the current index by then"
      // once deleted an unanswered neighbour instead.
      settlingToken = t.token;
      armAdvance(t.token);
    } catch (e) {
      // Named, because the user may have navigated while the POST was
      // in flight: a bare "Failed to submit" would read as being
      // about whatever card is on screen now.
      const detail = e instanceof Error ? e.message : String(e);
      error = `Failed to submit '${t.title}': ${detail}`;
    } finally {
      submitting = false;
    }
  }

  /// The ONLY writer of the advance state, with `flushAdvance` and
  /// `cancelAdvance`: arming always flushes any pending one first, so
  /// there can never be two live timers (an overwritten handle once
  /// left a stray timer firing 800ms later against a list that had
  /// moved).
  function armAdvance(token: string) {
    flushAdvance();
    advanceTimer = setTimeout(() => settleTask(token), 800);
    advanceToken = token;
  }

  /// A pending advance runs NOW instead of in 800ms: the answered
  /// task's removal is a fact, and anything about to move the list
  /// (navigation, a refresh) must see it landed, never cancel it.
  function flushAdvance() {
    if (advanceTimer === undefined) return;
    clearTimeout(advanceTimer);
    advanceTimer = undefined;
    const token = advanceToken;
    advanceToken = undefined;
    if (token !== undefined) settleTask(token);
  }

  /// Unmount only: the page is going away, nothing left to keep true.
  function cancelAdvance() {
    if (advanceTimer !== undefined) {
      clearTimeout(advanceTimer);
      advanceTimer = undefined;
      advanceToken = undefined;
    }
  }

  /// The task is settled here (answered, skipped, or its run
  /// cancelled): a resume task leaves the local list (by token, never
  /// by position); a fired TRIGGER stays, back to a ready-to-fire
  /// card, because it can be fired again (the dispatcher keeps
  /// listing it, and a runner claiming it is gone would contradict
  /// the popup). Clears any advance armed for it, so a natural timer
  /// fire leaves no stale record behind. Focus only moves when the
  /// settled task WAS the focused one; a user who navigated away
  /// mid-window keeps their place (and their typed input).
  function settleTask(token: string) {
    // Only THIS task's pending advance is cleared: settling B while
    // A's timer runs (skip B inside A's 800ms window) must leave A's
    // advance live, or A stays listed and re-submittable.
    if (advanceToken === token) {
      if (advanceTimer !== undefined) clearTimeout(advanceTimer);
      advanceTimer = undefined;
      advanceToken = undefined;
    }
    if (settlingToken === token) settlingToken = undefined;
    const idx = allTasks.findIndex((t) => t.token === token);
    if (idx < 0) return;
    const task = allTasks[idx];
    if (isTrigger(task)) {
      if (currentIndex === idx) initFormState(task);
      return;
    }
    const wasFocused = currentIndex === idx;
    const remaining = allTasks.filter((t) => t.token !== token);
    allTasks = remaining;
    if (remaining.length === 0) {
      currentIndex = -1;
      justFinished = true;
      // The hash still names the settled task; left in place, the
      // next refresh would read it, miss, and claim "answered
      // elsewhere" over a plain all-done.
      history.replaceState(null, '', '#/');
      return;
    }
    if (wasFocused) {
      jumpTo(Math.min(idx, remaining.length - 1));
    } else if (currentIndex > idx) {
      // The removal shifted everything after it down one; keep the
      // SAME task focused without re-initing its form.
      currentIndex -= 1;
    }
  }

  /// Skip = resume this lane with null. Sibling tasks of the
  /// same run keep going. No confirmation: low blast radius.
  async function handleSkip() {
    const t = currentTask;
    if (!t) return;
    if (isTrigger(t)) {
      // "Skipping" a trigger would FIRE it (a null answer starts a
      // run); the button is hidden for triggers, and this guard keeps
      // any other path equally unable to do it by accident.
      error = 'A trigger has no run to skip; use Fire to start one.';
      return;
    }
    try {
      await skipTask(t);
      settleTask(t.token);
    } catch (e) {
      error = e instanceof Error ? e.message : 'Failed to skip';
    }
  }

  /// Cancel = kill the entire run. Confirm first because the
  /// blast radius is wider than the task on screen.
  async function handleCancelRun() {
    const t = currentTask;
    if (!t) return;
    if (isTrigger(t)) {
      // "Cancelling" a trigger would delete the project's ENTRY POINT
      // (the signal itself), not stop a run; same gate as the button.
      error = 'A trigger has no run to cancel.';
      return;
    }
    if (!confirm('Cancel the entire run? Every related task will be dropped and the execution will be marked failed.')) return;
    try {
      await cancelRun(t);
      settleTask(t.token);
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
    if (field.render.source === 'input')
      return Array.isArray(field.value) ? (field.value as string[]) : [];
    return (field.config.options as string[]) ?? [];
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
      if (canSubmit) submitForm();
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
      <button class="nav-btn" onclick={refresh} disabled={loading} title="Refresh" aria-label="Refresh">
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
      {:else if configuredCount === 0}
        <!-- Nothing was even asked: an "All Clear" here (from a
             notification on a since-removed token, or a bookmarked
             tab) would claim everything is answered when nothing is
             connected. -->
        <div class="card">
          <div class="card-header"><div class="dot error"></div><span class="card-title">Not connected</span></div>
          <div class="card-body center">
            <p class="big-msg">No tokens configured</p>
            <p class="hint">Add a token in the extension popup to connect this page to your WeaveMind projects.</p>
            <button class="btn btn-secondary" style="margin-top: 16px" onclick={refresh}>Refresh</button>
          </div>
        </div>
      {:else if staleTaskRequested}
        <div class="card">
          {#if allReached}
            <div class="card-header"><div class="dot success"></div><span class="card-title">Already handled</span></div>
            <div class="card-body center">
              <p class="big-msg">That task is no longer pending</p>
              <p class="hint">It was answered elsewhere, or its run ended.</p>
              {#if allTasks.length > 0}
                {@render showPendingButton()}
              {:else}
                <button class="btn btn-secondary" style="margin-top: 16px" onclick={() => window.close()}>Close</button>
              {/if}
            </div>
          {:else}
            <!-- An unreached runtime hides its tasks without ending
                 them; claiming "answered" here would be a confident
                 false statement about live work. -->
            <div class="card-header"><div class="dot error"></div><span class="card-title">Runtime unreachable</span></div>
            <div class="card-body center">
              <p class="big-msg">Could not reach every runtime</p>
              <p class="hint">This task may still be pending; its runtime did not answer (down, or its host access was turned off in the popup's settings).</p>
              <button class="btn btn-secondary" style="margin-top: 16px" onclick={refresh}>Retry</button>
              {#if allTasks.length > 0}
                <!-- Retry can stay futile forever (a decommissioned
                     runtime); the tasks that DID load must stay
                     reachable from here. -->
                {@render showPendingButton()}
              {/if}
            </div>
          {/if}
        </div>
      {:else if justFinished && allTasks.length === 0}
        {@render allClearCard(true)}
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
              {#if isTrigger(currentTask)}
                <!-- The walk crosses from resume tasks into triggers;
                     the badge is what says "firing this STARTS a run"
                     rather than answering one. -->
                <span class="trigger-pill">Trigger: firing starts a new run</span>
              {/if}
            </div>
          </div>

          {#if settled}
            <div class="card-body center">
              <div class="check-icon green">
                <svg width="32" height="32" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2.5"><polyline points="20 6 9 17 4 12"/></svg>
              </div>
              {#if isTrigger(currentTask)}
                <p class="big-msg">Fired</p>
                <p class="hint">A run started; the trigger stays and can be fired again.</p>
              {:else}
                <p class="big-msg">Submitted</p>
                <p class="hint">Advancing to the next task...</p>
              {/if}
            </div>
          {:else}
            <div class="card-body">
              {#if error}
                <div class="error-box">{error}</div>
              {/if}

              {#if currentTask.formSchema}
                {#each currentTask.formSchema.fields as field}
                  {@const r = field.render}
                  {#if r.component === 'readonly'}
                    <div class="field">
                      <p class="field-key">{field.label || field.key}</p>
                      {#if isComplex(field.value)}
                        <pre class="readonly-pre">{fmt(field.value)}</pre>
                      {:else}
                        <p class="readonly-line">{fmt(field.value) || '(empty)'}</p>
                      {/if}
                    </div>
                  {:else if r.component === 'image'}
                    {@const imgSrc = imageSourceOf(field.value)}
                    {@const storedFile = !imgSrc && isStoredFileValue(field.value)}
                    <div class="field">
                      <p class="field-key">{field.label || field.key}</p>
                      {#if imgSrc}
                        <img src={imgSrc} alt={field.label || field.key} class="field-image" />
                      {:else if storedFile}
                        <!-- A stored file carries no link: ask the files door
                             each time this renders, so the link is always fresh
                             and an expired file says so in the image's place. -->
                        {#await fetchTaskFile(currentTask, field.key)}
                          <p class="field-empty">Loading image...</p>
                        {:then link}
                          <img src={link.url} alt={field.label || field.key} class="field-image" />
                        {:catch e}
                          <div class="error-box">{e instanceof Error ? e.message : String(e)}</div>
                        {/await}
                      {:else if field.value != null && field.value !== ''}
                        <!-- Something is there and it is neither an address a
                             browser can open nor a file weft stored, so say so
                             rather than showing an empty space. -->
                        <div class="error-box">This field does not hold an image address.</div>
                      {:else}
                        <p class="field-empty">(no image)</p>
                      {/if}
                    </div>
                  {:else if r.component === 'buttons'}
                    {@const decision = buttonDecisions[field.key]}
                    <div class="field">
                      <p class="field-key">{field.label || field.key}</p>
                      <div class="btn-row">
                        <button
                          class="decision-btn {decision === false ? 'reject-active' : 'reject-idle'}"
                          onclick={() => { buttonDecisions = { ...buttonDecisions, [field.key]: false }; }}
                        >{(field.config.rejectLabel as string) || 'Reject'}</button>
                        <button
                          class="decision-btn {decision === true ? 'approve-active' : 'approve-idle'}"
                          onclick={() => { buttonDecisions = { ...buttonDecisions, [field.key]: true }; }}
                        >{(field.config.approveLabel as string) || 'Approve'}</button>
                      </div>
                    </div>
                  {:else if r.component === 'select'}
                    {@const options = getOptions(field)}
                    {#if r.multiple}
                      {@const selected = (formValues[field.key] as string[]) ?? []}
                      <div class="field">
                        <p class="field-key">{field.label || field.key}</p>
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
                        <p class="field-key">{field.label || field.key}</p>
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
                  {:else if r.component === 'text'}
                    <div class="field">
                      <p class="field-key">{field.label || field.key}</p>
                      <input
                        type="text"
                        class="text-input"
                        placeholder={(field.config.placeholder as string) ?? ''}
                        value={(formValues[field.key] as string) ?? ''}
                        oninput={(e) => { formValues = { ...formValues, [field.key]: e.currentTarget.value }; }}
                      />
                    </div>
                  {:else if r.component === 'textarea'}
                    <div class="field">
                      <p class="field-key">{field.label || field.key}</p>
                      <textarea
                        class="text-input"
                        rows={r.prefilled ? 6 : 3}
                        placeholder={(field.config.placeholder as string) ?? ''}
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
                    disabled={!canSubmit}
                    title="Ctrl+Enter"
                  >
                    {#if submitting}
                      <span class="spinner-small"></span>
                    {:else}
                      {isTrigger(currentTask) ? 'Fire' : 'Submit'}
                    {/if}
                  </button>
                  <!-- Skip and Cancel-run act on an IN-FLIGHT run,
                       which a trigger does not have: "skipping" a
                       trigger would fire it (starting a run) and
                       "cancelling" it would delete the project's
                       entry point. Same gate as the popup's. -->
                  {#if !isTrigger(currentTask)}
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
                  {/if}
                </div>
              {:else}
                <p class="hint center-text">No form fields configured for this task.</p>
                <div class="action-row">
                  {#if isTrigger(currentTask)}
                    <!-- A form-less trigger still fires (an empty
                         payload starts the run). -->
                    <button class="btn btn-primary" onclick={submitForm} disabled={!canSubmit}>
                      Fire
                    </button>
                  {:else}
                    <button class="btn btn-secondary" onclick={handleSkip}>
                      Skip
                    </button>
                    <button class="btn btn-danger" onclick={handleCancelRun}>
                      Cancel run
                    </button>
                  {/if}
                </div>
              {/if}
            </div>
          {/if}
        </div>
        <p class="task-id-foot">Task ID: {currentTask.token.slice(0, 8)}...</p>
      {:else}
        {@render allClearCard(false)}
      {/if}
    </div>
  </div>
</div>

<!-- The escape hatch to the tasks that DID load, shared by both stale
     cards so the two cannot drift. -->
{#snippet showPendingButton()}
  <button class="btn btn-primary" style="margin-top: 16px" onclick={() => { staleTaskRequested = false; jumpTo(0); }}>
    Show the {allTasks.length === 1 ? 'pending task' : `first of ${allTasks.length} pending tasks`}
  </button>
{/snippet}

<!-- The one All Clear card. `afterSettle` (a distinct name from the
     `justFinished` state that FEEDS it, so neither shadows the other)
     is the post-submit variant: checkmark, "no more", Close; the
     plain variant is the empty landing (Refresh). -->
{#snippet allClearCard(afterSettle: boolean)}
  <div class="card">
    <div class="card-header"><div class="dot success"></div><span class="card-title">All Clear</span></div>
    <div class="card-body center">
      {#if afterSettle}
        <div class="check-icon">
          <svg width="32" height="32" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><path d="M22 11.08V12a10 10 0 11-5.93-9.14"/><path d="M22 4L12 14.01l-3-3"/></svg>
        </div>
      {/if}
      <p class="big-msg">{afterSettle ? 'No more pending tasks' : 'No pending tasks'}</p>
      {#if !allReached}
        <p class="hint">Some runtimes did not answer, so more may be waiting there.</p>
      {/if}
      {#if afterSettle}
        <button class="btn btn-secondary" style="margin-top: 16px" onclick={() => window.close()}>Close</button>
      {:else}
        <button class="btn btn-secondary" style="margin-top: 16px" onclick={refresh}>Refresh</button>
      {/if}
    </div>
  </div>
{/snippet}

<style>
  /* Full-tab page sizing. Scoped via `:global()` so the rules
     ride along in tasks-*.css (Svelte component CSS), not in
     the shared `app-*.css` chunk vite builds across entries.
     Without this, the popup's own html/body sizing wins in the
     shared chunk and clamps the task page to 340px. */
  /* Background comes from lib/reset.css (shared by both entries). */
  :global(html), :global(body) {
    margin: 0;
    padding: 0;
    width: 100%;
    min-height: 100vh;
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

  .trigger-pill {
    align-self: flex-start;
    margin-top: 4px;
    font-size: 10px;
    font-weight: 500;
    background: #fffbeb;
    border: 1px solid #f59e0b;
    color: #92400e;
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
