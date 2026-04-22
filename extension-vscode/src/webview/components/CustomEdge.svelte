<script lang="ts">
  // Ported verbatim from dashboard-v1/src/lib/components/project/CustomEdge.svelte.
  // Bezier path, no label (v1 never renders edge labels: the port
  // colors + handle visuals carry the meaning). EdgeReconnectAnchor
  // at the target end binds a `reconnecting` flag; while dragging
  // the target endpoint we hide the static path so the live preview
  // is the only visible line.

  import {
    BaseEdge,
    EdgeReconnectAnchor,
    getBezierPath,
    type EdgeProps,
  } from '@xyflow/svelte';

  let {
    id,
    sourceX,
    sourceY,
    targetX,
    targetY,
    sourcePosition,
    targetPosition,
    style,
    markerEnd,
  }: EdgeProps = $props();

  let reconnecting = $state(false);

  const edgePath = $derived(
    getBezierPath({
      sourceX,
      sourceY,
      targetX,
      targetY,
      sourcePosition,
      targetPosition,
    })[0],
  );
</script>

{#if !reconnecting}
  <BaseEdge {id} path={edgePath} {style} {markerEnd} />
{/if}

<EdgeReconnectAnchor
  bind:reconnecting
  type="target"
  position={{ x: targetX, y: targetY }}
  size={20}
/>
