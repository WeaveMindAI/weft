<script lang="ts">
  // Collapsible group container. The /parse response flattens groups
  // into passthrough nodes (see NodeDefinition.scope + groupBoundary).
  // We reconstruct a visual group by rendering a dashed-border frame
  // behind the members. xyflow routes child nodes into this frame via
  // a `parentId` relationship we set at Graph.svelte composition time.

  import { cn } from '../utils/cn';
  import { ChevronDown, ChevronRight } from 'lucide-svelte';

  interface Props {
    data: {
      groupId: string;
      label: string;
      collapsed: boolean;
      onToggle: () => void;
    };
    id: string;
    selected?: boolean;
  }

  let { data, selected }: Props = $props();
</script>

<div
  class={cn(
    'border-2 border-dashed rounded-lg',
    selected ? 'border-ring' : 'border-border/60',
    'bg-background/30',
    'w-full h-full relative',
  )}
>
  <div class="absolute top-1 left-2 flex items-center gap-1">
    <button
      type="button"
      onclick={data.onToggle}
      class="text-muted-foreground hover:text-foreground"
      aria-label={data.collapsed ? 'expand' : 'collapse'}
    >
      {#if data.collapsed}
        <ChevronRight class="size-3.5" />
      {:else}
        <ChevronDown class="size-3.5" />
      {/if}
    </button>
    <span class="text-[11px] uppercase tracking-wide">{data.label}</span>
    <span class="text-[10px] text-muted-foreground/80">({data.groupId})</span>
  </div>
</div>
