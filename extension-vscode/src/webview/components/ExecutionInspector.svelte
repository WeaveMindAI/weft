<script lang="ts">
  import type { NodeExecStatus } from './exec-types';
  // Ported from dashboard-v1/src/lib/components/project/ExecutionInspector.svelte.
  // Inline pager + magnifying glass button in the node header; modal
  // opens to show Input / Details / Output columns plus a footer
  // (status, duration, cost, timestamp).

  import { Search } from 'lucide-svelte';
  import { getStatusIcon, formatDuration, formatCost } from '../utils/status';
  import JsonTree from './JsonTree.svelte';

  // The webview receives a single live exec state per node. We
  // wrap it into a 1-element array so the UI matches v1's
  // paginated shape; the history list will grow once the host
  // forwards a per-node execution journal.
  interface Props {
    status: NodeExecStatus;
    input?: unknown;
    output?: unknown;
    error?: string;
    startedAt?: number;
    completedAt?: number;
    costUsd?: number;
    label?: string;
  }

  let {
    status,
    input,
    output,
    error,
    startedAt,
    completedAt,
    costUsd = 0,
    label = 'Node',
  }: Props = $props();

  let open = $state(false);

  const hasData = $derived(status !== 'idle');

  const statusStr = $derived(status === 'started' ? 'running' : status);

  function close() {
    open = false;
  }

  // Block node drag/pan while modal is open.
  function stop(e: Event) {
    e.stopPropagation();
  }

  async function copyText(text: string) {
    try {
      await navigator.clipboard.writeText(text);
    } catch {
      // ignore, clipboard might be blocked in webview
    }
  }

  const inputJson = $derived(input ? JSON.stringify(input, null, 2) : null);
  const outputJson = $derived(output ? JSON.stringify(output, null, 2) : null);
  const detailsText = $derived(error ?? (statusStr === 'completed' ? 'Completed successfully' : statusStr));
  const inputEntries: [string, unknown][] = $derived(
    input && typeof input === 'object' && !Array.isArray(input)
      ? Object.entries(input as Record<string, unknown>)
      : [],
  );
  const outputEntries: [string, unknown][] = $derived(
    output && typeof output === 'object' && !Array.isArray(output)
      ? Object.entries(output as Record<string, unknown>)
      : [],
  );

  const fullCopyText = $derived(
    [
      `--- Input ---`,
      inputJson ?? '(none)',
      ``,
      `--- Details ---`,
      detailsText,
      ``,
      `--- Output ---`,
      outputJson ?? '(none)',
      ``,
      `Status: ${statusStr}${startedAt && completedAt ? ` | Duration: ${formatDuration(startedAt, completedAt)}` : ''}${costUsd > 0 ? ` | Cost: ${formatCost(costUsd)}` : ''}${startedAt ? ` | ${new Date(startedAt).toLocaleString()}` : ''}`,
    ].join('\n'),
  );
</script>

<!-- Inline: magnifying glass button in the node header. -->
{#if hasData}
  <button
    class="w-5 h-5 flex items-center justify-center rounded hover:bg-black/5 cursor-pointer transition-colors text-zinc-400 nodrag"
    onclick={(e) => {
      e.stopPropagation();
      open = true;
    }}
    title="Inspect execution"
  >
    <Search class="w-3 h-3" />
  </button>
{/if}

<!-- Modal backdrop + dialog. -->
{#if open && hasData}
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
          <span class={status === 'failed' ? 'text-red-600' : status === 'completed' ? 'text-green-600' : 'text-zinc-500'}>
            {getStatusIcon(statusStr)}
          </span>
          <span class="text-sm font-semibold text-zinc-800">{label}</span>
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

      <!-- 3-col body -->
      <div class="grid grid-cols-3 min-h-0 flex-1 overflow-hidden">
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

        <div class="flex flex-col min-h-0 border-r border-zinc-200">
          <div class="flex items-center justify-between px-3 py-1.5 bg-zinc-50 border-b border-zinc-200 shrink-0">
            <span class="text-[10px] font-medium text-zinc-400 uppercase tracking-wider">Details</span>
            <button class="text-[10px] text-zinc-400 hover:text-zinc-700" onclick={() => copyText(detailsText)}>copy</button>
          </div>
          <div class="overflow-auto flex-1 p-3 space-y-3">
            {#if error}
              <div class="rounded border border-red-200 bg-red-50 p-2.5">
                <div class="text-[10px] font-semibold text-red-700 mb-1">Error</div>
                <pre class="text-[11px] text-red-600 whitespace-pre-wrap break-words font-mono">{error}</pre>
              </div>
            {:else if status === 'completed'}
              <div class="text-[11px] text-green-600">Completed successfully</div>
            {:else if status === 'started'}
              <div class="text-[11px] text-blue-600 animate-pulse">Running...</div>
            {:else if status === 'skipped'}
              <div class="text-[11px] text-zinc-500">Skipped</div>
            {/if}
          </div>
        </div>

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
            {:else if output !== null && output !== undefined}
              <div class="p-1 text-[11px] font-mono text-zinc-700">{JSON.stringify(output)}</div>
            {:else}
              <div class="p-1 text-xs text-zinc-400 italic">No output</div>
            {/if}
          </div>
        </div>
      </div>

      <!-- Footer -->
      <div class="flex items-center gap-4 px-4 py-1.5 border-t border-zinc-200 bg-zinc-50 text-[10px] text-zinc-500 shrink-0">
        <span class={`font-medium ${status === 'failed' ? 'text-red-600' : status === 'completed' ? 'text-green-600' : ''}`}>
          {statusStr}
        </span>
        {#if startedAt}
          <span class="font-mono">{formatDuration(startedAt, completedAt)}</span>
        {/if}
        {#if costUsd > 0}
          <span class="font-mono">{formatCost(costUsd)}</span>
        {/if}
        {#if startedAt}
          <span>{new Date(startedAt).toLocaleString()}</span>
        {/if}
      </div>
    </div>
  </div>
{/if}
