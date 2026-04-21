<script lang="ts">
  import { cn } from '../utils/cn';
  import JsonTree from './JsonTree.svelte';

  export type NodeExecStatus =
    | 'idle'
    | 'started'
    | 'completed'
    | 'failed'
    | 'skipped';

  interface Props {
    status: NodeExecStatus;
    input?: unknown;
    output?: unknown;
    error?: string;
  }

  let { status, input, output, error }: Props = $props();
</script>

<div class="flex flex-col gap-1 border-t border-border/40 pt-1">
  <div class="flex items-center gap-2">
    <span
      class={cn(
        'text-[10px] uppercase tracking-wide px-1.5 py-0.5 rounded',
        status === 'started' && 'bg-primary/20 text-primary',
        status === 'completed' && 'bg-[oklch(45%_0.12_140/0.3)] text-[oklch(70%_0.14_140)]',
        status === 'failed' && 'bg-destructive/20 text-destructive',
        status === 'skipped' && 'bg-muted text-muted-foreground',
        status === 'idle' && 'bg-muted text-muted-foreground',
      )}>{status}</span
    >
  </div>

  {#if error}
    <div
      class="text-[11px] text-destructive bg-destructive/10 px-2 py-1 rounded break-words"
    >
      {error}
    </div>
  {/if}

  {#if status !== 'idle' && status !== 'skipped'}
    {#if input !== undefined}
      <div class="text-[10px] text-muted-foreground uppercase tracking-wide">input</div>
      <JsonTree value={input} />
    {/if}
    {#if output !== undefined && status === 'completed'}
      <div class="text-[10px] text-muted-foreground uppercase tracking-wide">output</div>
      <JsonTree value={output} />
    {/if}
    {#if status === 'started'}
      <div class="text-[11px] italic text-muted-foreground">running...</div>
    {/if}
  {/if}
</div>
