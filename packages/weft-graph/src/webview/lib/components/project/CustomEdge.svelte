<script lang="ts">
	import { BaseEdge, EdgeLabel, EdgeReconnectAnchor, getBezierPath, type EdgeProps } from '@xyflow/svelte';
	
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
		data,
	}: EdgeProps = $props();

	// Simplified view: an edge is a MERGE of many real connections, so it is
	// non-interactive (no reconnect grab zone). Editing one would be ambiguous.
	const simplified = $derived(!!(data as { simplified?: boolean } | undefined)?.simplified);

	// A wire that reads keys off its value (`t.n = s.out.profile.wpm`) is
	// drawn dotted, with the path written at the end where the value lands.
	const path = $derived((data as { path?: string[] } | undefined)?.path ?? []);
	const dotted = $derived(path.length > 0);
	const pathLabel = $derived(path.length > 0 ? '.' + path.join('.') : '');

	// Track reconnection state - hide edge while reconnecting
	let reconnecting = $state(false);

	// Smooth bezier curve
	let edgePath = $derived(getBezierPath({ sourceX, sourceY, targetX, targetY, sourcePosition, targetPosition })[0]);
</script>

<!-- Hide edge while reconnecting -->
{#if !reconnecting}
	<!-- All edges: straight lines, no arrowheads; a dereferencing wire is dotted -->
	<BaseEdge {id} path={edgePath} style={dotted ? `${style ?? ''} stroke-dasharray: 4 4;` : style} />
{/if}

{#if dotted}
	<EdgeLabel x={targetX - 14} y={targetY - 14} transparent>
		<span
			class="rounded px-1 text-[10px] font-mono bg-popover border text-muted-foreground whitespace-nowrap"
			style="transform: translate(-100%, -100%); display: inline-block;"
			title="reads {pathLabel} off the value"
		>{pathLabel}</span>
	</EdgeLabel>
{/if}

<!-- EdgeReconnectAnchor at target end - larger grab zone overlapping the handle.
     Omitted in simplified view: collapsed edges are not reconnectable. -->
{#if !simplified}
	<EdgeReconnectAnchor
		bind:reconnecting
		type="target"
		position={{ x: targetX, y: targetY }}
		size={20}
	/>
{/if}
