<script lang="ts">
	/// The `_should_flow` dock: the port that decides whether this thing
	/// runs at all. It sits in the top-left corner, apart from the port
	/// rail, because it answers a different question from the node's own
	/// inputs. An arrow pointing into the node, hollow while nothing
	/// answers it (the node runs) and filled once something does.
	import { Handle, Position } from '@xyflow/svelte';
	import { SHOULD_FLOW_PORT } from '../../../../protocol';

	let { top, subject, connected }: {
		/// Distance from the top of the node, in pixels.
		top: number;
		/// What the tooltip calls the thing this dock belongs to.
		subject: 'node' | 'group' | 'loop';
		/// True when a wire or a written value answers the port.
		connected: boolean;
	} = $props();

	/// Amber: the colour the graph already uses for "the runtime is
	/// deciding something here", and never a port type's colour, so this
	/// dock cannot be mistaken for a data port.
	const COLOR = '#f59e0b';
</script>

<Handle
	type="target"
	position={Position.Left}
	id={SHOULD_FLOW_PORT}
	title="_should_flow: whether this {subject} runs at all"
	style="top: {top}px; z-index: 6; background: none; border: none; width: 10px; height: 10px;"
>
	<svg
		width="10"
		height="10"
		viewBox="0 0 10 10"
		style="pointer-events: none; position: absolute; left: 0; top: 0;"
	>
		<polygon
			points="1.6,0.9 9.1,5 1.6,9.1"
			fill={connected ? COLOR : 'white'}
			stroke={COLOR}
			stroke-width="1.5"
			stroke-linejoin="round"
		/>
	</svg>
</Handle>
