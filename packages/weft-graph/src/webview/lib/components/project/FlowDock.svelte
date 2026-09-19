<script lang="ts">
	/// The gate dock: the port that decides whether this thing runs at
	/// all. It sits in the top-left corner, apart from the port rail,
	/// because it answers a different question from the node's own
	/// inputs. An arrow pointing into the node, hollow while nothing
	/// answers it (the node runs) and filled once something does.
	///
	/// The gate has two spellings and a node carries one of them. The
	/// inverted one keeps the arrow exactly where it is and adds a small
	/// hollow circle between its tip and the node, which is how a
	/// negated input is drawn in a logic diagram and reads as "not"
	/// without a legend. The arrow never moves, resizes or flips: the
	/// mark has to sit in the same place whichever way the gate reads,
	/// or a graph full of them is unreadable.
	///
	/// Right-clicking opens the same port menu every other port has. The
	/// handler sits on a wrapper because `Handle` renders its own
	/// element and does not forward DOM events; the wrapper is
	/// `display: contents`, so it adds no box and changes no layout, and
	/// the event reaches it by bubbling.
	import { Handle, Position } from '@xyflow/svelte';
	import { SHOULD_FLOW_PORT, SHOULD_NOT_FLOW_PORT } from '../../../../protocol';
	import { openPortMenu } from '../../utils/port-context-menu';

	let { top, subject, connected, inverted = false, onToggle }: {
		/// Distance from the top of the node, in pixels.
		top: number;
		/// What the tooltip calls the thing this dock belongs to.
		subject: 'node' | 'group' | 'loop';
		/// True when a wire or a written value answers the port.
		connected: boolean;
		/// True when this is `_should_not_flow`: the same decision read
		/// the other way round, where a closure is what runs it.
		inverted?: boolean;
		/// Flip the polarity. Absent on a read-only view, which leaves
		/// the menu with nothing to offer rather than a dead item.
		onToggle?: () => void;
	} = $props();

	/// Amber: the colour the graph already uses for "the runtime is
	/// deciding something here", and never a port type's colour, so this
	/// dock cannot be mistaken for a data port.
	const COLOR = '#f59e0b';

	const port = $derived(inverted ? SHOULD_NOT_FLOW_PORT : SHOULD_FLOW_PORT);
	const title = $derived(
		inverted
			? `${SHOULD_NOT_FLOW_PORT}: runs this ${subject} when what is wired here did NOT happen`
			: `${SHOULD_FLOW_PORT}: whether this ${subject} runs at all`,
	);

	/// The mark the flip would LEAVE BEHIND, drawn at menu size. Showing
	/// the destination says what the button does with no words at all,
	/// which is why the row needs only one line of text beside it.
	function markOf(withCircle: boolean): string {
		const bubble = withCircle
			? `<circle cx="12.4" cy="6" r="2.4" fill="none" stroke="${COLOR}" stroke-width="1.6" />`
			: '';
		return `<svg width="16" height="12" viewBox="0 0 16 12">
			<polygon points="2,1.2 10,6 2,10.8" fill="${COLOR}" stroke="${COLOR}"
				stroke-width="1.6" stroke-linejoin="round" />${bubble}</svg>`;
	}

	let menuAt = $state<{ x: number; y: number } | null>(null);

	// Same shape as every other port menu on the canvas: the effect
	// tracks only open/close, and the items are built untracked as a
	// snapshot of the gesture.
	$effect(() => {
		if (!menuAt) return;
		const { x, y } = menuAt;
		return openPortMenu({ x, y }, () => {
			// Nothing drives the gate means there is no decision to read
			// the other way round, so there is no menu: an undriven gate
			// of either spelling lets the node run, and a row that
			// changed nothing would be worse than no row.
			if (!onToggle || !connected) return null;
			return [{
				label: inverted ? 'Run when this arrives' : 'Run when this does not arrive',
				icon: markOf(!inverted),
				onClick: onToggle,
			}];
		}, () => { menuAt = null; });
	});
</script>

<!-- svelte-ignore a11y_no_static_element_interactions -->
<div
	style="display: contents;"
	oncontextmenu={(e) => {
		if (!onToggle) return;
		e.preventDefault();
		e.stopPropagation();
		menuAt = { x: e.clientX, y: e.clientY };
	}}
>
	<Handle
		type="target"
		position={Position.Left}
		id={port}
		{title}
		style="top: {top}px; z-index: 6; background: none; border: none; width: 10px; height: 10px;"
	>
		<!-- The box stays 10x10 and the triangle keeps its exact
		     coordinates, so the mark does not move when the gate is
		     flipped. The circle is drawn OUTSIDE that box, between the
		     tip and the node, which `overflow: visible` allows. -->
		<svg
			width="10"
			height="10"
			viewBox="0 0 10 10"
			style="pointer-events: none; position: absolute; left: 0; top: 0; overflow: visible;"
		>
			<polygon
				points="1.6,0.9 9.1,5 1.6,9.1"
				fill={connected ? COLOR : 'white'}
				stroke={COLOR}
				stroke-width="1.5"
				stroke-linejoin="round"
			/>
			{#if inverted}
				<circle cx="11.9" cy="5" r="2.1" fill="white" stroke={COLOR} stroke-width="1.5" />
			{/if}
		</svg>
	</Handle>
</div>
