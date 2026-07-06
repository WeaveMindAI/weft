<script lang="ts">
  // A resizable + collapsible container for a panel injected into one of the
  // editor's side slots (`leftPanel` / `rightPanel`). The MECHANICS (drag to
  // resize, width persistence, collapse) live here once; the RULES are declared
  // by the consumer, so the editor never bakes in side-specific behavior:
  //
  //   - `side`:        which edge the drag handle sits on ('left' panel -> handle
  //                    on its RIGHT edge; 'right' panel -> handle on its LEFT edge).
  //   - min/max/default width, in px (max may also be a viewport fraction).
  //   - `collapseMode`: 'full'  -> collapsing hides the panel entirely (0 width),
  //                                for a code view the consumer wants gone when off.
  //                     'rail'  -> collapsing leaves a minimal icon rail visible,
  //                                for a replay bar that should always be reachable.
  //   - `collapsed`:    bindable open/closed state the consumer owns.
  //   - `rail`:         snippet for the icon rail shown when collapsed in 'rail'
  //                     mode (the consumer draws its own icons + click-to-expand).
  //   - `children`:     the panel body, shown when not collapsed.
  //
  // Width persists per `storageKey` in localStorage so it survives reloads.

  import type { Snippet } from 'svelte';

  let {
    side,
    collapseMode = 'full',
    collapsed = $bindable(false),
    minWidth = 240,
    maxWidth = 900,
    defaultWidth = 360,
    railWidth = 36,
    storageKey,
    onCollapse,
    rail,
    header,
    children,
  }: {
    side: 'left' | 'right';
    collapseMode?: 'full' | 'rail';
    collapsed?: boolean;
    minWidth?: number;
    maxWidth?: number;
    defaultWidth?: number;
    railWidth?: number;
    storageKey?: string;
    /// Optional: when the consumer drives `collapsed` from its OWN state (a value,
    /// not a `bind:`), it passes `onCollapse` so the in-panel collapse button asks
    /// the consumer to close instead of mutating the (read-only) `collapsed` prop.
    /// When absent, the collapse button toggles the bound `collapsed` directly.
    onCollapse?: () => void;
    rail?: Snippet;
    /// The panel's HEADER content (its label + any actions). SlotPanel draws the
    /// header BAR itself (fixed height, bottom border) with the collapse chevron
    /// as an in-flow part of that bar at the inner edge (the edge facing the
    /// canvas), so the chevron is flush in the bar, never floating over the body
    /// and never needing the consumer to reserve a gutter. Absent = no header bar.
    header?: Snippet;
    /// The panel body (below the header bar).
    children: Snippet;
  } = $props();

  // Close the panel: defer to the consumer's `onCollapse` if given (consumer owns
  // the state), else flip the locally-bound `collapsed`.
  function doCollapse() {
    if (onCollapse) onCollapse();
    else collapsed = true;
  }

  // Persisted width (px). Scoped by `storageKey` so two panels don't collide.
  // `$derived` so it tracks `storageKey` reactively (props can change if a
  // consumer re-keys the panel) rather than capturing only its initial value.
  const widthKey = $derived(storageKey ? `slotpanel:${storageKey}:width` : null);
  function loadWidthFor(key: string | null): number {
    if (!key) return defaultWidth;
    try {
      const v = localStorage.getItem(key);
      return v === null ? defaultWidth : Math.max(minWidth, Math.min(maxWidth, Number(v)));
    } catch {
      return defaultWidth;
    }
  }
  let panelWidth = $state(loadWidthFor(widthKey));
  // Re-LOAD the width when the key changes (a consumer re-keyed the panel):
  // read the NEW key's stored value rather than carrying the old key's width
  // and clobbering the new key's stored value on the next persist. Tracked by
  // key only (untrack panelWidth) so a drag doesn't retrigger a reload.
  let lastWidthKey: string | null = widthKey;
  $effect(() => {
    if (widthKey !== lastWidthKey) {
      lastWidthKey = widthKey;
      panelWidth = loadWidthFor(widthKey);
    }
  });
  // Persist on width change (a user drag). Safe now that a key change reloads
  // rather than writing the stale width to the new key.
  $effect(() => {
    if (!widthKey) return;
    try {
      localStorage.setItem(widthKey, String(panelWidth));
    } catch {
      // localStorage unavailable (private mode): width just won't persist.
    }
  });

  let isDragging = $state(false);
  let containerEl: HTMLElement | null = $state(null);

  // Drag the handle to resize. The new width is computed from the cursor vs the
  // panel's FIXED edge (the edge opposite the handle), so dragging tracks the
  // cursor exactly regardless of side.
  function onDragStart(e: MouseEvent) {
    e.preventDefault();
    isDragging = true;
    document.body.style.userSelect = 'none';
    document.body.style.cursor = 'col-resize';

    function onMove(ev: MouseEvent) {
      if (!containerEl) return;
      const rect = containerEl.getBoundingClientRect();
      // Handle on the right edge (left panel): width = cursor - left edge.
      // Handle on the left edge (right panel): width = right edge - cursor.
      const raw = side === 'left' ? ev.clientX - rect.left : rect.right - ev.clientX;
      panelWidth = Math.max(minWidth, Math.min(maxWidth, raw));
    }
    function onUp() {
      isDragging = false;
      document.body.style.userSelect = '';
      document.body.style.cursor = '';
      window.removeEventListener('mousemove', onMove);
      window.removeEventListener('mouseup', onUp);
    }
    window.addEventListener('mousemove', onMove);
    window.addEventListener('mouseup', onUp);
  }

  // When collapsed: full mode renders nothing (0 width); rail mode renders the
  // consumer's icon rail at `railWidth`.
  const showFull = $derived(!collapsed);
  const showRail = $derived(collapsed && collapseMode === 'rail');
  const effectiveWidth = $derived(collapsed ? (collapseMode === 'rail' ? railWidth : 0) : panelWidth);
  const borderClass = $derived(side === 'left' ? 'border-r' : 'border-l');
  // The drag handle sits on the panel's INNER edge (the edge facing the canvas):
  // a left panel's handle is on its right, a right panel's on its left.
  const handleEdge = $derived(side === 'left' ? 'right-0' : 'left-0');
</script>

{#if !collapsed || collapseMode === 'rail'}
  <div
    bind:this={containerEl}
    class="relative flex h-full shrink-0 flex-col overflow-hidden bg-background {borderClass} border-border"
    style="width: {effectiveWidth}px; transition: {isDragging ? 'none' : 'width 150ms ease'};"
  >
    {#if showRail}
      {@render rail?.()}
    {:else if showFull}
      <!-- Header BAR: the collapse chevron is an in-flow part of this bar (flush,
           no shadow), sitting at the panel's INNER edge (facing the canvas): for
           a LEFT panel the chevron is the LAST item (right edge, after the
           consumer's header content); for a RIGHT panel it's the FIRST item
           (left edge, before the content). The consumer fills the middle. -->
      {#if header}
        <div class="flex h-9 shrink-0 items-stretch border-b border-border bg-background">
          {#if side === 'right'}
            {@render collapseChevron()}
          {/if}
          <div class="flex min-w-0 flex-1 items-center">
            {@render header()}
          </div>
          {#if side === 'left'}
            {@render collapseChevron()}
          {/if}
        </div>
      {/if}
      <div class="min-h-0 flex-1 overflow-hidden">
        {@render children()}
      </div>
      <!-- Drag handle on the inner edge. 4px hit area, highlights on hover/drag. -->
      <!-- svelte-ignore a11y_no_static_element_interactions -->
      <div
        class="absolute top-0 z-10 h-full w-1 cursor-col-resize {handleEdge} {isDragging
          ? 'bg-zinc-400'
          : 'bg-transparent hover:bg-zinc-300'}"
        onmousedown={onDragStart}
      ></div>
    {/if}
  </div>
{/if}

{#snippet collapseChevron()}
  <button
    class="flex w-9 shrink-0 items-center justify-center text-zinc-400 transition-colors hover:bg-zinc-100 hover:text-zinc-700"
    title="Collapse panel"
    aria-label="Collapse panel"
    onclick={doCollapse}
  >
    <svg class="h-3.5 w-3.5" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round">
      {#if side === 'left'}
        <polyline points="15 18 9 12 15 6" />
      {:else}
        <polyline points="9 18 15 12 9 6" />
      {/if}
    </svg>
  </button>
{/snippet}
