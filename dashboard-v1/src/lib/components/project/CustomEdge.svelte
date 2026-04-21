<script lang="ts">
	import { BaseEdge, EdgeReconnectAnchor, getBezierPath, type EdgeProps } from '@xyflow/svelte';
	
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
		targetHandleId,
	}: EdgeProps = $props();
	
	// Track reconnection state - hide edge while reconnecting
	let reconnecting = $state(false);
	
	// Smooth bezier curve
	let edgePath = $derived(getBezierPath({ sourceX, sourceY, targetX, targetY, sourcePosition, targetPosition })[0]);
</script>

<!-- Hide edge while reconnecting -->
{#if !reconnecting}
	<!-- All edges: straight lines, no arrowheads -->
	<BaseEdge {id} path={edgePath} {style} />
{/if}

<!-- EdgeReconnectAnchor at target end - larger grab zone overlapping the handle -->
<EdgeReconnectAnchor 
	bind:reconnecting 
	type="target" 
	position={{ x: targetX, y: targetY }} 
	size={20}
/>
