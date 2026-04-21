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
    data,
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

  // Data from Graph.svelte: showLabel true only when sourceHandle
  // != targetHandle. v1 hides labels when they'd be redundant.
  const showLabel = $derived(Boolean((data as any)?.showLabel));
  const sourcePort = $derived(String((data as any)?.sourcePort ?? ''));
  const targetPort = $derived(String((data as any)?.targetPort ?? ''));
</script>

<BaseEdge {id} path={path} style={style} markerEnd={markerEnd} />

{#if showLabel}
  <text
    x={labelX}
    y={labelY}
    text-anchor="middle"
    dominant-baseline="central"
    class="fill-zinc-500 text-[9px] pointer-events-none select-none"
    style="paint-order: stroke; stroke: #fafafa; stroke-width: 3px;"
  >
    {sourcePort} → {targetPort}
  </text>
{/if}
