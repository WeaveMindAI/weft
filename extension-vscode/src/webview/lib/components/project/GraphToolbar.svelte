<script lang="ts">
  import { FileText, Pin, PinOff } from '@lucide/svelte';

  let {
    mode,
    color,
    pendingCount,
    onTogglePin,
    onCatchUp,
    onOpenSource,
    sourceOpen = false,
  }: {
    mode: 'latest' | 'pinned';
    color: string | undefined;
    pendingCount: number;
    onTogglePin: () => void;
    onCatchUp: () => void;
    onOpenSource?: () => void;
    /// True when the .weft source is currently visible in some
    /// editor tab. Drives the Source button's active styling so
    /// the user sees at a glance that clicking will reveal an
    /// existing tab (vs creating a new one).
    sourceOpen?: boolean;
  } = $props();

  const shortColor = $derived(color ? color.slice(0, 8) : '');
</script>

<div class="absolute top-3 left-3 z-30 flex items-center gap-2 pointer-events-auto">
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
      <FileText class="w-3 h-3" />
      Source
    </button>
  {/if}
</div>
