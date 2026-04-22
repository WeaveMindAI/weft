<script lang="ts">
  // Ported from dashboard-v1/src/lib/components/project/ExecutionInspector.svelte.
  // Accepts an executions[] history; renders an inline pager ‹ N/M ›
  // (when count > 1) + magnifier that opens the modal. The modal
  // has three columns (Input / Details / Output) each with a copy
  // button + a full-text copy in the header.

  import { Search } from 'lucide-svelte';
  import { formatDuration, formatCost, getStatusIcon } from '../utils/status';
  import type { NodeExecution } from './exec-types';
  import JsonTree from './JsonTree.svelte';

  interface Props {
    executions: NodeExecution[];
    label?: string;
  }

  let { executions, label = 'Node' }: Props = $props();
  let selectedIndex = $state(0);
  let open = $state(false);

  const count = $derived(executions.length);

  $effect(() => {
    // New execution appended → jump to it (v1 execution-inspector.md
    // line 22-23).
    if (count > 0) selectedIndex = count - 1;
  });

  const selected = $derived(executions[selectedIndex] ?? null);
  const statusColor = $derived.by(() => {
    switch (selected?.status) {
      case 'failed':
        return 'text-red-500';
      case 'completed':
        return 'text-green-600';
      case 'running':
      case 'waiting_for_input':
        return 'text-blue-500';
      default:
        return 'text-muted-foreground';
    }
  });

  function close() {
    open = false;
  }
  function stop(e: Event) {
    e.stopPropagation();
  }
  async function copyText(text: string) {
    try {
      await navigator.clipboard.writeText(text);
    } catch {
      // clipboard may be blocked in the webview
    }
  }

  const inputEntries: [string, unknown][] = $derived(
    selected?.input && typeof selected.input === 'object' && !Array.isArray(selected.input)
      ? Object.entries(selected.input as Record<string, unknown>)
      : [],
  );
  const outputEntries: [string, unknown][] = $derived(
    selected?.output && typeof selected.output === 'object' && !Array.isArray(selected.output)
      ? Object.entries(selected.output as Record<string, unknown>)
      : [],
  );

  const inputJson = $derived(
    selected?.input != null ? JSON.stringify(selected.input, null, 2) : null,
  );
  const outputJson = $derived(
    selected?.output != null ? JSON.stringify(selected.output, null, 2) : null,
  );
  const detailsText = $derived(
    selected?.error ?? (selected?.status === 'completed' ? 'Completed successfully' : selected?.status ?? ''),
  );
  const fullCopyText = $derived(
    !selected
      ? ''
      : [
          `--- Input ---`,
          inputJson ?? '(none)',
          ``,
          `--- Details ---`,
          detailsText,
          ``,
          `--- Output ---`,
          outputJson ?? '(none)',
          ``,
          `Status: ${selected.status} | Duration: ${formatDuration(
            selected.startedAt,
            selected.completedAt,
          )}${selected.costUsd > 0 ? ` | Cost: ${formatCost(selected.costUsd)}` : ''} | ${new Date(
            selected.startedAt,
          ).toLocaleString()} | ${selected.id}`,
        ].join('\n'),
  );
</script>

{#if count > 1}
  <div class={`inline-flex items-center gap-0.5 ml-1.5 text-[9px] select-none ${statusColor}`}>
    <button
      disabled={selectedIndex === 0}
      class="w-4 h-4 flex items-center justify-center rounded hover:bg-black/5 disabled:opacity-30 nodrag"
      onclick={(e) => {
        e.stopPropagation();
        if (selectedIndex > 0) selectedIndex--;
      }}
      aria-label="Previous execution"
    >‹</button>
    <span class="font-mono tabular-nums">{selectedIndex + 1}/{count}</span>
    <button
      disabled={selectedIndex >= count - 1}
      class="w-4 h-4 flex items-center justify-center rounded hover:bg-black/5 disabled:opacity-30 nodrag"
      onclick={(e) => {
        e.stopPropagation();
        if (selectedIndex < count - 1) selectedIndex++;
      }}
      aria-label="Next execution"
    >›</button>
  </div>
{/if}

{#if count > 0}
  <button
    class="w-5 h-5 flex items-center justify-center rounded hover:bg-black/5 cursor-pointer transition-colors text-zinc-400 nodrag"
    onclick={(e) => {
      e.stopPropagation();
      open = true;
    }}
    title="Inspect execution"
    aria-label="Inspect execution"
  >
    <Search class="w-3 h-3" />
  </button>
{/if}

{#if open && selected}
  <!-- svelte-ignore a11y_click_events_have_key_events -->
  <!-- svelte-ignore a11y_no_static_element_interactions -->
  <div
    class="fixed inset-0 z-[100] bg-black/30 flex items-center justify-center p-6 nodrag nopan"
    onclick={close}
    oncontextmenu={stop}
  >
    <div
      class="bg-white text-zinc-900 w-[92vw] max-h-[85vh] overflow-hidden rounded-lg shadow-2xl flex flex-col"
      onclick={stop}
      role="dialog"
      aria-modal="true"
      tabindex={-1}
    >
      <!-- Header -->
      <div class="flex items-center justify-between px-4 py-2.5 border-b border-zinc-200 shrink-0">
        <div class="flex items-center gap-3">
          <span class={statusColor}>{getStatusIcon(selected.status)}</span>
          <span class="text-sm font-semibold text-zinc-800">{label}</span>
          {#if count > 1}
            <div class={`inline-flex items-center gap-0.5 text-[10px] select-none ${statusColor}`}>
              <button
                disabled={selectedIndex === 0}
                class="w-5 h-5 flex items-center justify-center rounded hover:bg-zinc-100 disabled:opacity-30"
                onclick={() => selectedIndex > 0 && selectedIndex--}
              >‹</button>
              <span class="font-mono tabular-nums">{selectedIndex + 1}/{count}</span>
              <button
                disabled={selectedIndex >= count - 1}
                class="w-5 h-5 flex items-center justify-center rounded hover:bg-zinc-100 disabled:opacity-30"
                onclick={() => selectedIndex < count - 1 && selectedIndex++}
              >›</button>
            </div>
          {/if}
        </div>
        <div class="flex items-center gap-2">
          <button
            class="w-6 h-6 flex items-center justify-center rounded hover:bg-zinc-100 text-zinc-400 hover:text-zinc-700 transition-colors"
            onclick={() => copyText(fullCopyText)}
            title="Copy all"
            aria-label="Copy all"
          >
            <svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round">
              <rect x="9" y="9" width="13" height="13" rx="2" ry="2" />
              <path d="M5 15H4a2 2 0 0 1-2-2V4a2 2 0 0 1 2-2h9a2 2 0 0 1 2 2v1" />
            </svg>
          </button>
          <button
            class="w-6 h-6 flex items-center justify-center rounded hover:bg-zinc-100 text-zinc-400 hover:text-zinc-700 transition-colors"
            onclick={close}
            aria-label="Close"
          >✕</button>
        </div>
      </div>

      <!-- 3-column body -->
      <div class="grid grid-cols-3 min-h-0 flex-1 overflow-hidden" style="height: calc(85vh - 80px);">
        <!-- Input -->
        <div class="flex flex-col min-h-0 border-r border-zinc-200">
          <div class="flex items-center justify-between px-3 py-1.5 bg-zinc-50 border-b border-zinc-200 shrink-0">
            <span class="text-[10px] font-medium text-zinc-400 uppercase tracking-wider">Input</span>
            {#if inputJson}
              <button class="text-[10px] text-zinc-400 hover:text-zinc-700" onclick={() => copyText(inputJson)} title="Copy input">copy</button>
            {/if}
          </div>
          <div class="overflow-auto flex-1 p-2">
            {#if inputEntries.length > 0}
              {#each inputEntries as entry}
                <JsonTree data={entry[1]} label={entry[0]} defaultExpanded={true} />
              {/each}
            {:else}
              <div class="p-1 text-xs text-zinc-400 italic">No input data</div>
            {/if}
          </div>
        </div>

        <!-- Details -->
        <div class="flex flex-col min-h-0 border-r border-zinc-200">
          <div class="flex items-center justify-between px-3 py-1.5 bg-zinc-50 border-b border-zinc-200 shrink-0">
            <span class="text-[10px] font-medium text-zinc-400 uppercase tracking-wider">Details</span>
            <button class="text-[10px] text-zinc-400 hover:text-zinc-700" onclick={() => copyText(detailsText)}>copy</button>
          </div>
          <div class="overflow-auto flex-1 p-3 space-y-3">
            {#if selected.error}
              <div class="rounded border border-red-200 bg-red-50 p-2.5">
                <div class="text-[10px] font-semibold text-red-700 mb-1">Error</div>
                <pre class="text-[11px] text-red-600 whitespace-pre-wrap break-words font-mono">{selected.error}</pre>
              </div>
            {:else if selected.status === 'completed'}
              <div class="text-[11px] text-green-600">Completed successfully</div>
            {:else if selected.status === 'running'}
              <div class="text-[11px] text-blue-600 animate-pulse">Running...</div>
            {:else if selected.status === 'waiting_for_input'}
              <div class="text-[11px] text-blue-600 animate-pulse">Waiting for input...</div>
            {:else if selected.status === 'skipped'}
              <div class="text-[11px] text-zinc-500">Skipped (null input on required port)</div>
            {:else if selected.status === 'cancelled'}
              <div class="text-[11px] text-orange-500">Cancelled</div>
            {/if}
          </div>
        </div>

        <!-- Output -->
        <div class="flex flex-col min-h-0">
          <div class="flex items-center justify-between px-3 py-1.5 bg-zinc-50 border-b border-zinc-200 shrink-0">
            <span class="text-[10px] font-medium text-zinc-400 uppercase tracking-wider">Output</span>
            {#if outputJson}
              <button class="text-[10px] text-zinc-400 hover:text-zinc-700" onclick={() => copyText(outputJson)}>copy</button>
            {/if}
          </div>
          <div class="overflow-auto flex-1 p-2">
            {#if outputEntries.length > 0}
              {#each outputEntries as entry}
                <JsonTree data={entry[1]} label={entry[0]} defaultExpanded={true} />
              {/each}
            {:else if selected.output !== null && selected.output !== undefined}
              <div class="p-1 text-[11px] font-mono text-zinc-700">{JSON.stringify(selected.output)}</div>
            {:else}
              <div class="p-1 text-xs text-zinc-400 italic">No output</div>
            {/if}
          </div>
        </div>
      </div>

      <!-- Footer -->
      <div class="flex items-center gap-4 px-4 py-1.5 border-t border-zinc-200 bg-zinc-50 text-[10px] text-zinc-500 shrink-0">
        <span class={`font-medium ${statusColor}`}>{selected.status}</span>
        <span class="font-mono">{formatDuration(selected.startedAt, selected.completedAt)}</span>
        {#if selected.costUsd > 0}
          <span class="font-mono">{formatCost(selected.costUsd)}</span>
        {/if}
        <span>{new Date(selected.startedAt).toLocaleString()}</span>
      </div>
    </div>
  </div>
{/if}
