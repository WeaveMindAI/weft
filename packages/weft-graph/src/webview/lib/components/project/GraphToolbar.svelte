<script lang="ts">
  import { ArrowLeft, Code, EyeOff, Pin, PinOff } from '@lucide/svelte';
  import type { Snippet } from 'svelte';

  let {
    mode,
    color,
    pendingCount,
    notPainted = undefined,
    onTogglePin,
    onCatchUp,
    onClearFollow,
    onOpenSource,
    sourceOpen = false,
    navDepth = 0,
    navFileName = '',
    onNavigateBack,
    leading,
    trailing,
    below,
  }: {
    mode: 'latest' | 'pinned';
    color: string | undefined;
    pendingCount: number;
    /// Set when the run on screen cannot be painted from its journal,
    /// with the reason. Shown as its own pill: the canvas is empty or
    /// half-empty and the person deserves to be told why rather than
    /// left to guess at the viewer.
    notPainted?: string;
    onTogglePin: () => void;
    onCatchUp: () => void;
    /// Take the run off the canvas without leaving live mode: the eye
    /// button. Before it, the only way out of a replayed run's
    /// highlight was closing the graph.
    onClearFollow: () => void;
    onOpenSource?: () => void;
    /// True when the .weft source is currently visible in some
    /// editor tab. Drives the Source button's active styling so
    /// the user sees at a glance that clicking will reveal an
    /// existing tab (vs creating a new one).
    sourceOpen?: boolean;
    /// Include-navigation depth: > 0 means viewing an @include'd file, so a
    /// Return button shows. navFileName labels the current file.
    navDepth?: number;
    navFileName?: string;
    onNavigateBack?: () => void;
    /// Host-injected controls placed at the START of the toolbar, before the
    /// editor's own buttons. A web host can put a "back to projects" button
    /// here so it sits inline with Live/Source instead of floating separately;
    /// VS Code injects nothing (its native chrome owns navigation). Absent =
    /// nothing rendered.
    leading?: Snippet;
    /// Host-injected controls placed at the END of the toolbar, after the
    /// editor's own buttons. Symmetric with `leading`. Absent = nothing.
    trailing?: Snippet;
    /// Controls for the RUN itself, on their own row under the toolbar.
    ///
    /// The row above is about the view (which run is followed, whether the
    /// source is open, where in an include you are). This one is about
    /// what happened inside the run, and those are not the same question,
    /// so they do not share a line. A run's caller conversation goes here.
    ///
    /// The row is not rendered at all when the snippet is absent or draws
    /// nothing, so a run with nothing to say about it costs no space.
    below?: Snippet;
  } = $props();

  const shortColor = $derived(color ? color.slice(0, 8) : '');
</script>

<div class="absolute top-3 left-3 z-30 flex flex-col items-start gap-2 pointer-events-auto">
<div class="flex items-center gap-2">
  {#if leading}{@render leading()}{/if}
  {#if navDepth > 0}
    <button
      type="button"
      onclick={() => onNavigateBack?.()}
      class="flex items-center gap-1.5 px-2.5 py-1.5 rounded-md border border-violet-300 bg-white text-violet-700 shadow-sm text-xs font-medium hover:bg-violet-50 transition"
      title="Return to the file you came from"
    >
      <ArrowLeft class="w-3 h-3" />
      Return{navFileName ? ` · ${navFileName}` : ''}
    </button>
  {/if}

  {#if notPainted}
    <div
      class="flex items-center gap-1.5 px-3 py-1.5 rounded-md bg-amber-50 text-amber-800 border border-amber-300 shadow-sm text-xs font-medium max-w-lg"
      title={notPainted}
    >
      <span class="w-1.5 h-1.5 rounded-full bg-amber-500"></span>
      <span class="truncate">{notPainted}</span>
    </div>
  {/if}

  {#if mode === 'pinned' && pendingCount > 0}
    <button
      type="button"
      onclick={onCatchUp}
      class="flex items-center gap-1.5 px-3 py-1.5 rounded-md bg-amber-100 text-amber-800 border border-amber-300 shadow-sm text-xs font-medium hover:bg-amber-200 transition"
      title="Jump to the newest execution"
    >
      <span class="w-1.5 h-1.5 rounded-full bg-amber-500 animate-pulse"></span>
      {pendingCount} new {pendingCount === 1 ? 'execution' : 'executions'} &middot; Catch up
    </button>
  {/if}

  <button
    type="button"
    onclick={onTogglePin}
    class="flex items-center gap-1.5 px-2.5 py-1.5 rounded-md border shadow-sm text-xs font-medium transition
      {mode === 'pinned'
        ? 'bg-zinc-900 text-white border-zinc-900 hover:bg-zinc-800'
        : 'bg-white text-zinc-700 border-zinc-200 hover:bg-zinc-50'}"
    title={mode === 'pinned'
      ? 'Pinned to this execution. Click to unpin and jump to the latest.'
      : 'Live: following the latest execution. Click to pin to this one.'}
    disabled={!color}
  >
    {#if mode === 'pinned'}
      <Pin class="w-3 h-3" />
      Pinned{shortColor ? ` · ${shortColor}` : ''}
    {:else}
      <PinOff class="w-3 h-3" />
      Live{shortColor ? ` · ${shortColor}` : ''}
    {/if}
  </button>

  {#if color}
    <button
      type="button"
      onclick={onClearFollow}
      class="flex items-center justify-center w-7 h-7 rounded-md border border-zinc-200 bg-white text-zinc-600 shadow-sm hover:bg-zinc-50 hover:text-zinc-900 transition"
      title="Stop showing this run. The graph goes blank and follows the next run that starts."
      aria-label="Stop showing this run"
    >
      <EyeOff class="w-3.5 h-3.5" />
    </button>
  {/if}

  {#if onOpenSource}
    <button
      type="button"
      onclick={onOpenSource}
      class="flex items-center gap-1.5 px-2.5 py-1.5 rounded-md border shadow-sm text-xs font-medium transition
        {sourceOpen
          ? 'bg-zinc-900 text-white border-zinc-900 hover:bg-zinc-800'
          : 'bg-white text-zinc-700 border-zinc-200 hover:bg-zinc-50'}"
      title={sourceOpen
        ? 'Source is open. Click to focus the existing tab.'
        : 'Open the .weft source in a side editor.'}
    >
      <Code class="w-3 h-3" />
      Source
    </button>
  {/if}
  {#if trailing}{@render trailing()}{/if}
</div>
{#if below}
  <div class="flex items-center gap-2">{@render below()}</div>
{/if}
</div>
