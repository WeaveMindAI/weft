<script lang="ts">
  import { cn } from '../utils/cn';
  import SelfImport from './JsonTree.svelte';
  // Recursive self-import: cast to any so the TS language server
  // doesn't collapse the circular type to `never`.
  const Self = SelfImport as any;

  interface Props {
    value: unknown;
    depth?: number;
    label?: string;
  }

  let { value, depth = 0, label = '' }: Props = $props();

  let open = $state(true);

  const kind = $derived(
    value === null
      ? 'null'
      : Array.isArray(value)
        ? 'array'
        : typeof value,
  );

  const arrayItems: unknown[] = $derived(Array.isArray(value) ? value : []);
  const objectEntries: [string, unknown][] = $derived(
    value && typeof value === 'object' && !Array.isArray(value)
      ? Object.entries(value as Record<string, unknown>)
      : [],
  );

  function preview(v: unknown): string {
    if (v === null) return 'null';
    if (typeof v === 'string') {
      const s = v.length > 80 ? v.slice(0, 77) + '...' : v;
      return JSON.stringify(s);
    }
    if (typeof v === 'number' || typeof v === 'boolean') return String(v);
    if (Array.isArray(v)) return `[… ${v.length} items]`;
    return `{… ${Object.keys(v as object).length} keys}`;
  }
</script>

<div class={cn('font-mono text-[11px]', depth === 0 && 'py-1')}>
  {#if kind === 'object' || kind === 'array'}
    <button
      type="button"
      class="text-left w-full hover:bg-muted/40 rounded px-1"
      onclick={() => (open = !open)}
    >
      <span class="inline-block w-3 text-muted-foreground">
        {open ? '▾' : '▸'}
      </span>
      {#if label}<span class="text-foreground">{label}:</span>{/if}
      <span class="text-muted-foreground ml-1">{preview(value)}</span>
    </button>
    {#if open}
      <div class="pl-3 border-l border-border/40 ml-1">
        {#if kind === 'array'}
          {#each arrayItems as item, i}
            <Self value={item} depth={depth + 1} label={`[${i}]`} />
          {/each}
        {:else}
          {#each objectEntries as entry}
            <Self value={entry[1]} depth={depth + 1} label={entry[0]} />
          {/each}
        {/if}
      </div>
    {/if}
  {:else}
    <div class="px-1">
      {#if label}<span class="text-foreground">{label}:</span>{/if}
      <span
        class={cn(
          'ml-1',
          kind === 'string' && 'text-[oklch(70%_0.14_120)]',
          kind === 'number' && 'text-[oklch(70%_0.14_30)]',
          kind === 'boolean' && 'text-[oklch(70%_0.14_270)]',
          kind === 'null' && 'text-muted-foreground',
        )}>{preview(value)}</span
      >
    </div>
  {/if}
</div>
