<script lang="ts">
  import { Handle, Position } from '@xyflow/svelte';
  import { cn } from '../utils/cn';

  type Side = 'input' | 'output';

  interface Props {
    name: string;
    portType: string;
    side: Side;
    required?: boolean;
    wired?: boolean;
    configFilled?: boolean;
    description?: string;
  }

  let {
    name,
    portType,
    side,
    required = false,
    wired = false,
    configFilled = false,
    description = '',
  }: Props = $props();

  const position = $derived(side === 'input' ? Position.Left : Position.Right);
  const handleType = $derived(side === 'input' ? 'target' : 'source');
  const state = $derived(
    wired ? 'wired' : configFilled ? 'config' : required ? 'required' : 'idle',
  );
</script>

<div
  class={cn(
    'relative flex items-center gap-1 text-[11px] py-0.5',
    side === 'output' ? 'justify-end' : 'justify-start',
  )}
  title={description}
>
  {#if side === 'input'}
    <Handle
      id={name}
      type={handleType}
      position={position}
      class={cn(
        '!w-2 !h-2 !border !rounded-full',
        state === 'wired' && '!bg-primary !border-primary',
        state === 'config' && '!bg-transparent !border-dashed !border-foreground',
        state === 'required' && '!bg-destructive !border-destructive',
        state === 'idle' && '!bg-muted !border-foreground/40',
      )}
    />
  {/if}
  <span class="truncate max-w-[120px] text-muted-foreground">
    <span class="text-foreground">{name}</span>
    <span class="opacity-60">: {portType}</span>
    {#if required}
      <span class="text-destructive" aria-label="required">*</span>
    {/if}
  </span>
  {#if side === 'output'}
    <Handle
      id={name}
      type={handleType}
      position={position}
      class={cn(
        '!w-2 !h-2 !border !rounded-full',
        '!bg-primary !border-primary',
      )}
    />
  {/if}
</div>
