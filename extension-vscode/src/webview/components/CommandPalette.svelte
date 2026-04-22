<script lang="ts">
  // Ported from dashboard-v1/src/lib/components/project/CommandPalette.svelte.
  //
  // Two panels side-by-side centered at top 15%:
  //   • 420px main: search input + flat Actions list + grouped node
  //     results. When search is non-empty, actions + nodes mix.
  //   • 256px preview panel: shown when a NODE is highlighted. Header
  //     (icon + label + type), description, input chips (green),
  //     output chips (blue), tag chips (zinc).
  //
  // Keyboard: ArrowUp/Down navigate, Enter picks, Escape closes.
  // data-selected attribute is set on the highlighted row so the
  // scroll-into-view effect can query for it.

  import {
    Search,
    BrainCircuit,
    ChartBar,
    GitFork,
    Server,
    Wrench,
    Bug,
    Zap,
    Play,
    Save,
    Undo2,
    Redo2,
    Copy,
    Trash2,
    CheckSquare,
    Maximize2,
    LayoutDashboard,
    Upload,
    Download,
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
    onAction?: (action: string) => void;
    playground?: boolean;
  }

  let { open = $bindable(false), catalog, onPick, onClose, onAction, playground = false }: Props = $props();

  let searchValue = $state('');
  let selectedIndex = $state(0);
  let inputRef: HTMLInputElement | null = $state(null);
  let listRef: HTMLDivElement | null = $state(null);

  // ─── Actions ───────────────────────────────────────────────────

  interface ActionItem {
    kind: 'action';
    id: string;
    label: string;
    icon: Component<any>;
    shortcut?: string;
  }
  interface NodeItem {
    kind: 'node';
    entry: CatalogEntry;
  }
  type Item = ActionItem | NodeItem;

  const ACTIONS: ActionItem[] = [
    { kind: 'action', id: 'save', label: 'Save Project', icon: Save as unknown as Component<any>, shortcut: 'Ctrl+S' },
    { kind: 'action', id: 'run', label: 'Run Project', icon: Play as unknown as Component<any>, shortcut: 'Ctrl+Enter' },
    { kind: 'action', id: 'export_json', label: 'Export as JSON', icon: Upload as unknown as Component<any> },
    { kind: 'action', id: 'export_weft', label: 'Export as Weft', icon: Upload as unknown as Component<any> },
    { kind: 'action', id: 'import', label: 'Import from JSON/Weft', icon: Download as unknown as Component<any> },
    { kind: 'action', id: 'undo', label: 'Undo', icon: Undo2 as unknown as Component<any>, shortcut: 'Ctrl+Z' },
    { kind: 'action', id: 'redo', label: 'Redo', icon: Redo2 as unknown as Component<any>, shortcut: 'Ctrl+Shift+Z' },
    { kind: 'action', id: 'duplicate', label: 'Duplicate Selected', icon: Copy as unknown as Component<any>, shortcut: 'Ctrl+D' },
    { kind: 'action', id: 'delete', label: 'Delete Selected', icon: Trash2 as unknown as Component<any>, shortcut: 'Del' },
    { kind: 'action', id: 'selectAll', label: 'Select All Nodes', icon: CheckSquare as unknown as Component<any>, shortcut: 'Ctrl+A' },
    { kind: 'action', id: 'fitView', label: 'Fit View', icon: Maximize2 as unknown as Component<any> },
    { kind: 'action', id: 'autoOrganize', label: 'Auto Organize Layout', icon: LayoutDashboard as unknown as Component<any> },
  ];
  const PLAYGROUND_HIDDEN = new Set(['save', 'run', 'export_json', 'export_weft', 'import']);
  const availableActions = $derived.by(() =>
    playground ? ACTIONS.filter((a) => !PLAYGROUND_HIDDEN.has(a.id)) : ACTIONS,
  );

  // ─── Categories ────────────────────────────────────────────────

  const CATEGORY_ICONS: Record<string, { icon: Component<any>; order: number }> = {
    Triggers: { icon: Zap as unknown as Component<any>, order: 0 },
    AI: { icon: BrainCircuit as unknown as Component<any>, order: 1 },
    Data: { icon: ChartBar as unknown as Component<any>, order: 2 },
    Flow: { icon: GitFork as unknown as Component<any>, order: 3 },
    Infrastructure: { icon: Server as unknown as Component<any>, order: 4 },
    Utility: { icon: Wrench as unknown as Component<any>, order: 5 },
    Debug: { icon: Bug as unknown as Component<any>, order: 6 },
  };

  const catalogEntries = $derived(Object.values(catalog).filter((e) => !e.features?.hidden));

  // ─── Scoring ───────────────────────────────────────────────────

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

  function scoreAction(action: ActionItem, q: string): number {
    const label = action.label.toLowerCase();
    if (label.startsWith(q)) return 1.5;
    if (label.includes(q)) return 3.5;
    return -1;
  }

  // Flat filtered items (used for keyboard navigation).
  const filteredFlat: Item[] = $derived.by(() => {
    const q = searchValue.toLowerCase().trim();
    if (!q) {
      return [
        ...availableActions.map<ActionItem>((a) => ({ ...a })),
        ...catalogEntries.map<NodeItem>((e) => ({ kind: 'node', entry: e })),
      ];
    }
    const scoredActions = availableActions
      .map((a) => ({ item: { ...a } as ActionItem, score: scoreAction(a, q) }))
      .filter((x) => x.score >= 0);
    const scoredNodes = catalogEntries
      .map((e) => ({ item: { kind: 'node', entry: e } as NodeItem, score: scoreEntry(e, q) }))
      .filter((x) => x.score >= 0);
    return [...scoredActions, ...scoredNodes]
      .sort((a, b) => a.score - b.score)
      .map((x) => x.item);
  });

  // Rendered sections (actions flat + grouped-by-category nodes when
  // no search; everything flat when searching).
  interface Section {
    name: string;
    icon: Component<any> | null;
    items: Item[];
  }

  const sections: Section[] = $derived.by(() => {
    const q = searchValue.trim();
    if (q) {
      return [{ name: '', icon: null, items: filteredFlat }];
    }
    const out: Section[] = [];
    out.push({ name: 'Actions', icon: null, items: availableActions as unknown as Item[] });
    const byCategory = new Map<string, CatalogEntry[]>();
    for (const e of catalogEntries) {
      const cat = e.category || 'Other';
      if (!byCategory.has(cat)) byCategory.set(cat, []);
      byCategory.get(cat)!.push(e);
    }
    const orderedCategories = Array.from(byCategory.entries()).sort(
      (a, b) =>
        (CATEGORY_ICONS[a[0]]?.order ?? 99) - (CATEGORY_ICONS[b[0]]?.order ?? 99),
    );
    for (const [name, items] of orderedCategories) {
      out.push({
        name,
        icon: CATEGORY_ICONS[name]?.icon ?? null,
        items: items.map<NodeItem>((e) => ({ kind: 'node', entry: e })),
      });
    }
    return out;
  });

  const previewed = $derived.by(() => {
    const item = filteredFlat[selectedIndex];
    return item?.kind === 'node' ? item.entry : null;
  });

  // Global flat index is what selectedIndex tracks; look up a given
  // item's position in filteredFlat for the `data-selected` match.
  function globalIdx(it: Item): number {
    return filteredFlat.indexOf(it);
  }

  $effect(() => {
    if (open && inputRef) {
      setTimeout(() => inputRef?.focus(), 10);
      selectedIndex = 0;
      searchValue = '';
    }
  });

  $effect(() => {
    // Reset selection when the filter changes.
    void filteredFlat.length;
    selectedIndex = 0;
  });

  // Scroll the highlighted row into view.
  $effect(() => {
    void selectedIndex;
    if (!listRef) return;
    const row = listRef.querySelector('[data-selected="true"]') as HTMLElement | null;
    row?.scrollIntoView({ block: 'nearest' });
  });

  function onKey(e: KeyboardEvent) {
    if (!open) return;
    if (e.key === 'Escape') {
      e.preventDefault();
      onClose();
    } else if (e.key === 'ArrowDown') {
      e.preventDefault();
      selectedIndex = Math.min(selectedIndex + 1, filteredFlat.length - 1);
    } else if (e.key === 'ArrowUp') {
      e.preventDefault();
      selectedIndex = Math.max(0, selectedIndex - 1);
    } else if (e.key === 'Enter' && filteredFlat.length > 0) {
      e.preventDefault();
      pick(filteredFlat[selectedIndex]);
    }
  }

  function pick(item: Item) {
    if (item.kind === 'action') {
      onAction?.(item.id);
    } else {
      onPick(item.entry.type);
    }
    searchValue = '';
  }
</script>

{#if open}
  <!-- svelte-ignore a11y_no_static_element_interactions -->
  <!-- svelte-ignore a11y_click_events_have_key_events -->
  <div class="fixed inset-0 z-[100] bg-black/50" onclick={onClose}></div>

  <!-- svelte-ignore a11y_no_static_element_interactions -->
  <div class="fixed top-[15%] left-1/2 -translate-x-1/2 z-[101] flex gap-3" onkeydown={onKey}>
    <!-- Main panel -->
    <div class="w-[420px] bg-white border border-zinc-200 rounded-xl shadow-2xl overflow-hidden flex flex-col max-h-[70vh]">
      <div class="flex items-center border-b border-zinc-200 px-3">
        <Search class="w-4 h-4 text-zinc-400 mr-2" />
        <input
          bind:this={inputRef}
          bind:value={searchValue}
          type="text"
          placeholder="Search nodes and actions..."
          class="flex-1 py-3 bg-transparent outline-none text-sm text-zinc-800 placeholder-zinc-400"
        />
        <kbd class="text-[10px] text-zinc-400 border border-zinc-200 rounded px-1.5 py-0.5">esc</kbd>
      </div>

      <div bind:this={listRef} class="flex-1 overflow-y-auto p-2">
        {#if filteredFlat.length === 0}
          <div class="px-3 py-4 text-xs text-zinc-400 text-center">no matches</div>
        {:else}
          {#each sections as section}
            {#if section.name}
              <div class="px-3 pt-2 pb-1 flex items-center gap-1.5 text-[10px] uppercase tracking-wider text-zinc-400">
                {#if section.icon}
                  <section.icon class="w-3 h-3" />
                {/if}
                <span>{section.name}</span>
              </div>
            {/if}
            {#each section.items as item}
              {@const idx = globalIdx(item)}
              {#if item.kind === 'action'}
                <button
                  type="button"
                  data-selected={idx === selectedIndex}
                  class={cn(
                    'w-full flex items-center gap-2 px-3 py-1.5 rounded-lg text-sm text-left transition-colors',
                    idx === selectedIndex ? 'bg-zinc-900 text-white' : 'hover:bg-zinc-100 text-zinc-800',
                  )}
                  onmouseenter={() => (selectedIndex = idx)}
                  onclick={() => pick(item)}
                >
                  <item.icon class="w-4 h-4 shrink-0" />
                  <span class="flex-1 truncate">{item.label}</span>
                  {#if item.shortcut}
                    <kbd class={cn('text-[10px] border rounded px-1 py-0', idx === selectedIndex ? 'text-white/80 border-white/30' : 'text-zinc-400 border-zinc-200')}>{item.shortcut}</kbd>
                  {/if}
                </button>
              {:else}
                {@const Icon = resolveIcon(item.entry.icon)}
                <button
                  type="button"
                  data-selected={idx === selectedIndex}
                  class={cn(
                    'w-full flex items-center gap-2 px-3 py-1.5 rounded-lg text-sm text-left transition-colors',
                    idx === selectedIndex ? 'bg-zinc-900 text-white' : 'hover:bg-zinc-100 text-zinc-800',
                  )}
                  onmouseenter={() => (selectedIndex = idx)}
                  onclick={() => pick(item)}
                >
                  <Icon class="w-4 h-4 shrink-0" style={`color: ${item.entry.color ?? 'currentColor'}`} />
                  <span class="flex-1 truncate">{item.entry.label}</span>
                  <span class={cn('text-[10px]', idx === selectedIndex ? 'text-white/60' : 'text-zinc-400')}>{item.entry.type}</span>
                </button>
              {/if}
            {/each}
          {/each}
        {/if}
      </div>

      <div class="border-t border-zinc-200 px-3 py-1.5 text-[10px] text-zinc-400 flex gap-3">
        <span>↑↓ navigate</span>
        <span>↵ select</span>
        <span>esc close</span>
      </div>
    </div>

    <!-- Preview -->
    {#if previewed}
      {@const PrevIcon = resolveIcon(previewed.icon)}
      <div class="w-64 bg-white border border-zinc-200 rounded-xl shadow-2xl p-4 self-start">
        <div class="flex items-center gap-2 mb-2">
          <PrevIcon class="w-5 h-5" style={`color: ${previewed.color ?? '#52525b'}`} />
          <span class="text-sm font-semibold text-zinc-800">{previewed.label}</span>
        </div>
        <div class="text-[10px] text-zinc-400 mb-2 font-mono">{previewed.type}</div>
        <div class="text-xs text-zinc-600 mb-3 line-clamp-4">{previewed.description}</div>

        {#if previewed.inputs.length > 0}
          <div class="text-[10px] font-medium text-green-600 mb-1">Inputs</div>
          <div class="flex flex-wrap gap-1 mb-3">
            {#each previewed.inputs as p}
              <span class="text-[10px] px-1.5 py-0.5 bg-green-100 text-green-700 rounded">{p.name}</span>
            {/each}
          </div>
        {/if}
        {#if previewed.outputs.length > 0}
          <div class="text-[10px] font-medium text-blue-600 mb-1">Outputs</div>
          <div class="flex flex-wrap gap-1 mb-3">
            {#each previewed.outputs as p}
              <span class="text-[10px] px-1.5 py-0.5 bg-blue-100 text-blue-700 rounded">{p.name}</span>
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
