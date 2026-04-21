<script lang="ts">
  // Cmd/Ctrl+K node picker. v1 had a full command palette with 10
  // categories and search; v2 ships the core: filter by type name or
  // tags, group by category, press Enter to insert at cursor.

  import { cn } from '../utils/cn';
  import { resolveIcon } from '../utils/icon';
  import type { CatalogEntry } from '../../shared/protocol';
  import { onMount } from 'svelte';

  interface Props {
    open: boolean;
    catalog: Record<string, CatalogEntry>;
    onPick: (nodeType: string) => void;
    onClose: () => void;
  }

  let { open, catalog, onPick, onClose }: Props = $props();

  let filter = $state('');
  let index = $state(0);
  let input: HTMLInputElement | undefined = $state();

  const entries = $derived(Object.values(catalog));

  const filtered = $derived.by(() => {
    const q = filter.trim().toLowerCase();
    if (!q) return entries;
    return entries.filter((e) => {
      const hay = [
        e.type,
        e.label,
        e.category,
        ...(e.tags ?? []),
        e.description,
      ]
        .join(' ')
        .toLowerCase();
      return hay.includes(q);
    });
  });

  const grouped = $derived.by(() => {
    const out: Record<string, CatalogEntry[]> = {};
    for (const e of filtered) {
      (out[e.category] ??= []).push(e);
    }
    return Object.entries(out).sort(([a], [b]) => a.localeCompare(b));
  });

  $effect(() => {
    if (open) {
      filter = '';
      index = 0;
      setTimeout(() => input?.focus(), 0);
    }
  });

  function onKey(e: KeyboardEvent) {
    if (!open) return;
    if (e.key === 'Escape') {
      onClose();
    } else if (e.key === 'ArrowDown') {
      index = Math.min(index + 1, filtered.length - 1);
      e.preventDefault();
    } else if (e.key === 'ArrowUp') {
      index = Math.max(0, index - 1);
      e.preventDefault();
    } else if (e.key === 'Enter') {
      const picked = filtered[index];
      if (picked) onPick(picked.type);
      e.preventDefault();
    }
  }

  onMount(() => {
    window.addEventListener('keydown', onKey);
    return () => window.removeEventListener('keydown', onKey);
  });
</script>

{#if open}
  <!-- svelte-ignore a11y_click_events_have_key_events -->
  <!-- svelte-ignore a11y_no_static_element_interactions -->
  <div
    class="fixed inset-0 z-50 bg-black/30 backdrop-blur-[2px] flex items-start justify-center pt-24"
    onclick={onClose}
  >
    <div
      class="w-[500px] max-h-[60vh] flex flex-col rounded-lg bg-popover text-popover-foreground border border-border shadow-xl overflow-hidden"
      onclick={(e) => e.stopPropagation()}
    >
      <input
        bind:this={input}
        bind:value={filter}
        placeholder="Search nodes..."
        class="w-full bg-transparent border-b border-border/60 px-3 py-2 text-[13px] focus:outline-none"
      />
      <div class="flex-1 overflow-y-auto py-1">
        {#if filtered.length === 0}
          <div class="px-3 py-2 text-[11px] text-muted-foreground">no matches</div>
        {:else}
          {#each grouped as [category, items]}
            <div class="px-3 pt-1 pb-0.5 text-[10px] uppercase tracking-wide text-muted-foreground">
              {category}
            </div>
            {#each items as entry}
              {@const Icon = resolveIcon(entry.icon)}
              {@const globalIdx = filtered.indexOf(entry)}
              <button
                type="button"
                class={cn(
                  'w-full text-left px-3 py-1.5 flex items-center gap-2',
                  globalIdx === index && 'bg-accent text-accent-foreground',
                )}
                onmouseenter={() => (index = globalIdx)}
                onclick={() => onPick(entry.type)}
              >
                <Icon class="size-3.5" style={`color: ${entry.color ?? 'currentColor'}`} />
                <span class="text-[12px]">{entry.label}</span>
                <span class="text-[10px] text-muted-foreground ml-1">{entry.type}</span>
                <span class="ml-auto text-[10px] text-muted-foreground/60 line-clamp-1 max-w-[200px]"
                  >{entry.description}</span
                >
              </button>
            {/each}
          {/each}
        {/if}
      </div>
      <div class="border-t border-border/60 px-3 py-1.5 text-[10px] text-muted-foreground flex gap-3">
        <span>↑↓ navigate</span>
        <span>↵ insert</span>
        <span>esc close</span>
      </div>
    </div>
  </div>
{/if}
