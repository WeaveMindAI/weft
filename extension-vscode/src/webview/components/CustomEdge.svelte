<script lang="ts">
  import { BaseEdge, getBezierPath, type EdgeProps } from '@xyflow/svelte';

  let {
    id,
    sourceX,
    sourceY,
    targetX,
    targetY,
    sourcePosition,
    targetPosition,
    sourceHandleId,
    targetHandleId,
    style,
    markerEnd,
  }: EdgeProps = $props();

  const pathResult = $derived(
    getBezierPath({
      sourceX,
      sourceY,
      targetX,
      targetY,
      sourcePosition,
      targetPosition,
    }),
  );
  const path = $derived(pathResult[0]);
  const labelX = $derived(pathResult[1]);
  const labelY = $derived(pathResult[2]);

  const label = $derived(
    sourceHandleId || targetHandleId
      ? `${sourceHandleId ?? ''} → ${targetHandleId ?? ''}`
      : '',
  );
</script>

<BaseEdge {id} path={path} style={style} markerEnd={markerEnd} />

{#if label}
  <text
    x={labelX}
    y={labelY}
    text-anchor="middle"
    dominant-baseline="central"
    class="fill-muted-foreground text-[9px] pointer-events-none select-none"
  >
    {label}
  </text>
{/if}
