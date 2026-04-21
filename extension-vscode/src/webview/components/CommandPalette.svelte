<script lang="ts">
  // Ported from dashboard-v1/src/lib/components/project/CommandPalette.svelte.
  // Cmd/Ctrl+K picker. Two panels: 420px main with search + grouped
  // results, right side preview for the currently-highlighted node.
  // Keyboard: ArrowUp/Down navigate, Enter picks, Escape closes.

  import {
    Search,
    BrainCircuit,
    ChartBar,
    GitFork,
    Server,
    Wrench,
    Bug,
    Zap,
  } from 'lucide-svelte';
  import { cn } from '../utils/cn';
  import { resolveIcon } from '../utils/icon';
  import type { CatalogEntry } from '../../shared/protocol';
  import type { Component } from 'svelte';

  interface Props {
    open: boolean;
    catalog: Record<string, CatalogEntry>;
    onPick: (nodeType: string) => void;
    onClose: () => void;
  }

  let { open = $bindable(false), catalog, onPick, onClose }: Props = $props();

  let searchValue = $state('');
  let selectedIndex = $state(0);
  let inputRef: HTMLInputElement | null = $state(null);

  // Known category → icon + order. v1 hardcodes this map; we do the
  // same. Unknown categories fall into "Other".
  const CATEGORY_ICONS: Record<string, { icon: Component<any>; order: number }> = {
    Triggers: { icon: Zap as unknown as Component<any>, order: 0 },
    AI: { icon: BrainCircuit as unknown as Component<any>, order: 1 },
    Data: { icon: ChartBar as unknown as Component<any>, order: 2 },
    Flow: { icon: GitFork as unknown as Component<any>, order: 3 },
    Infrastructure: { icon: Server as unknown as Component<any>, order: 4 },
    Utility: { icon: Wrench as unknown as Component<any>, order: 5 },
    Debug: { icon: Bug as unknown as Component<any>, order: 6 },
  };

  const entries = $derived(Object.values(catalog));

  function scoreEntry(entry: CatalogEntry, q: string): number {
    const label = entry.label.toLowerCase();
    if (label === q) return 0;
    if (label.startsWith(q)) return 1;
    if (label.split(/\s+/).some((w) => w.startsWith(q))) return 2;
    if (label.includes(q)) return 3;
    if ((entry.tags ?? []).some((t) => t.toLowerCase().includes(q))) return 4;
    if (entry.description.toLowerCase().includes(q)) return 5;
    return -1;
  }

  const filtered: CatalogEntry[] = $derived.by(() => {
    const q = searchValue.toLowerCase().trim();
    if (!q) return entries;
    const scored: { e: CatalogEntry; s: number }[] = [];
    for (const e of entries) {
      const s = scoreEntry(e, q);
      if (s >= 0) scored.push({ e, s });
    }
    scored.sort((a, b) => a.s - b.s);
    return scored.map((x) => x.e);
  });

  // Group by category in default view (no search). With search, render flat.
  const groups: { name: string; icon: Component<any> | null; items: CatalogEntry[] }[] = $derived.by(() => {
    if (searchValue.trim()) {
      return [{ name: '', icon: null, items: filtered }];
    }
    const byCategory = new Map<string, CatalogEntry[]>();
    for (const e of filtered) {
      const cat = e.category || 'Other';
      (byCategory.get(cat) ?? byCategory.set(cat, []).get(cat)!).push(e);
    }
    return Array.from(byCategory.entries())
      .map(([name, items]) => ({
        name,
        icon: CATEGORY_ICONS[name]?.icon ?? null,
        items,
        order: CATEGORY_ICONS[name]?.order ?? 99,
      }))
      .sort((a, b) => a.order - b.order)
      .map(({ name, icon, items }) => ({ name, icon, items }));
  });

  // Currently-highlighted preview item (for right panel).
  const previewed = $derived(filtered[selectedIndex] ?? null);

  $effect(() => {
    if (open && inputRef) {
      setTimeout(() => inputRef?.focus(), 10);
      selectedIndex = 0;
      searchValue = '';
    }
  });

  // Reset selection when filter changes.
  $effect(() => {
    filtered;
    selectedIndex = 0;
  });

  function onKey(e: KeyboardEvent) {
    if (!open) return;
    if (e.key === 'Escape') {
      e.preventDefault();
      onClose();
    } else if (e.key === 'ArrowDown') {
      e.preventDefault();
      selectedIndex = Math.min(selectedIndex + 1, filtered.length - 1);
    } else if (e.key === 'ArrowUp') {
      e.preventDefault();
      selectedIndex = Math.max(0, selectedIndex - 1);
    } else if (e.key === 'Enter' && filtered.length > 0) {
      e.preventDefault();
      pick(filtered[selectedIndex]);
    }
  }

  function pick(entry: CatalogEntry) {
    onPick(entry.type);
    searchValue = '';
  }

  function globalIdx(entry: CatalogEntry): number {
    return filtered.indexOf(entry);
  }
</script>

{#if open}
  <!-- svelte-ignore a11y_no_static_element_interactions -->
  <!-- svelte-ignore a11y_click_events_have_key_events -->
  <div class="fixed inset-0 z-[100] bg-black/50" onclick={onClose}></div>

  <!-- svelte-ignore a11y_no_static_element_interactions -->
  <div class="fixed top-[15%] left-1/2 -translate-x-1/2 z-[101] flex gap-3" onkeydown={onKey}>
    <!-- Main panel -->
    <div class="w-[420px] bg-white border border-zinc-200 rounded-xl shadow-2xl overflow-hidden flex flex-col max-h-[60vh]">
      <div class="flex items-center border-b border-zinc-200 px-3">
        <Search class="w-4 h-4 text-zinc-400 mr-2" />
        <input
          bind:this={inputRef}
          bind:value={searchValue}
          type="text"
          placeholder="Search nodes..."
          class="flex-1 py-3 bg-transparent outline-none text-sm text-zinc-800 placeholder-zinc-400"
        />
        <kbd class="text-[10px] text-zinc-400 border border-zinc-200 rounded px-1.5 py-0.5">esc</kbd>
      </div>

      <div class="flex-1 overflow-y-auto p-2">
        {#if filtered.length === 0}
          <div class="px-3 py-4 text-xs text-zinc-400 text-center">no matches</div>
        {:else}
          {#each groups as group}
            {#if group.name}
              <div class="px-3 pt-2 pb-1 flex items-center gap-1.5 text-[10px] uppercase tracking-wider text-zinc-400">
                {#if group.icon}
                  <group.icon class="w-3 h-3" />
                {/if}
                <span>{group.name}</span>
              </div>
            {/if}
            {#each group.items as entry}
              {@const idx = globalIdx(entry)}
              {@const Icon = resolveIcon(entry.icon)}
              <button
                type="button"
                data-selected={idx === selectedIndex}
                class={cn(
                  'w-full flex items-center gap-2 px-3 py-1.5 rounded-lg text-sm text-left transition-colors',
                  idx === selectedIndex ? 'bg-zinc-900 text-white' : 'hover:bg-zinc-100 text-zinc-800',
                )}
                onmouseenter={() => (selectedIndex = idx)}
                onclick={() => pick(entry)}
              >
                <Icon class="w-4 h-4 shrink-0" style={`color: ${entry.color ?? 'currentColor'}`} />
                <span class="flex-1 truncate">{entry.label}</span>
                <span class={cn('text-[10px]', idx === selectedIndex ? 'text-white/60' : 'text-zinc-400')}>{entry.type}</span>
              </button>
            {/each}
          {/each}
        {/if}
      </div>

      <div class="border-t border-zinc-200 px-3 py-1.5 text-[10px] text-zinc-400 flex gap-3">
        <span>↑↓ navigate</span>
        <span>↵ insert</span>
        <span>esc close</span>
      </div>
    </div>

    <!-- Preview -->
    {#if previewed}
      {@const PrevIcon = resolveIcon(previewed.icon)}
      <div class="w-64 bg-white border border-zinc-200 rounded-xl shadow-2xl p-4 self-start">
        <div class="flex items-center gap-2 mb-2">
          <PrevIcon class="w-4 h-4" style={`color: ${previewed.color ?? '#52525b'}`} />
          <span class="text-sm font-semibold text-zinc-800">{previewed.label}</span>
        </div>
        <div class="text-[10px] text-zinc-400 mb-2 font-mono">{previewed.type}</div>
        <div class="text-xs text-zinc-600 mb-3 line-clamp-4">{previewed.description}</div>

        {#if previewed.inputs.length > 0}
          <div class="text-[10px] font-medium text-green-600 mb-1">Inputs</div>
          <div class="flex flex-wrap gap-1 mb-3">
            {#each previewed.inputs as p}
              <span class="text-[10px] px-1.5 py-0.5 bg-green-50 text-green-700 rounded">{p.name}</span>
            {/each}
          </div>
        {/if}
        {#if previewed.outputs.length > 0}
          <div class="text-[10px] font-medium text-blue-600 mb-1">Outputs</div>
          <div class="flex flex-wrap gap-1 mb-3">
            {#each previewed.outputs as p}
              <span class="text-[10px] px-1.5 py-0.5 bg-blue-50 text-blue-700 rounded">{p.name}</span>
            {/each}
          </div>
        {/if}
        {#if previewed.tags && previewed.tags.length > 0}
          <div class="text-[10px] font-medium text-zinc-500 mb-1">Tags</div>
          <div class="flex flex-wrap gap-1">
            {#each previewed.tags as tag}
              <span class="text-[10px] px-1.5 py-0.5 bg-zinc-100 text-zinc-600 rounded">{tag}</span>
            {/each}
          </div>
        {/if}
      </div>
    {/if}
  </div>
{/if}
