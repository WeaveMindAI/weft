/**
 * Shared port context menu utility.
 * Creates a floating menu attached to document.body (avoids CSS transform issues in xyflow nodes).
 * Returns a cleanup function for use in Svelte $effect.
 */

import type { PortDefinition } from '$lib/types';

export interface PortMenuItem {
	label: string;
	onClick: () => void;
	color?: string;
}

/** Parameters for the shared port menu builder. Both ProjectNode and GroupNode
 *  call `buildPortMenuItems` with their own port + callbacks so the menu
 *  content stays identical across all port surfaces in the graph. */
export interface BuildPortMenuOptions {
	port: PortDefinition;
	side: 'input' | 'output';
	/** True if the port is user-added (not in the node's catalog default).
	 *  For groups, every interface port is user-added, pass true. */
	isCustom: boolean;
	/** Whether the underlying node type accepts user-added ports on this side.
	 *  For groups both are true. For regular nodes, read from the catalog
	 *  features.canAddInputPorts / canAddOutputPorts. */
	canAddPorts: boolean;
	onToggleRequired: () => void;
	onSetType: (newType: string) => void;
	onRemove: () => void;
}

/** Build the standard port context menu items. Exactly one definition to
 *  keep every port surface (regular node, group expanded, group collapsed)
 *  identical. */
export function buildPortMenuItems(opts: BuildPortMenuOptions): PortMenuItem[] {
	const { port, side, isCustom, canAddPorts, onToggleRequired, onSetType, onRemove } = opts;
	const items: PortMenuItem[] = [];

	// Required toggle (inputs only; outputs do not have runtime required semantics).
	if (side === 'input') {
		items.push({
			label: port.required ? '☐ Make optional' : '☑ Make required',
			onClick: onToggleRequired,
		});
	}

	// Type edit (prompt for now; port.portType is the current value).
	items.push({
		label: `✎ Type: ${port.portType || 'MustOverride'}`,
		onClick: () => {
			const newType = prompt('Enter port type:', port.portType || '');
			if (newType !== null && newType.trim() && newType.trim() !== port.portType) {
				onSetType(newType.trim());
			}
		},
	});

	// Remove (only when the port is removable: user-added + the node type
	// accepts custom ports on this side).
	if (isCustom && canAddPorts) {
		items.push({
			label: 'Remove port',
			onClick: onRemove,
			color: '#ef4444',
		});
	}

	return items;
}

export function createPortContextMenu(
	x: number,
	y: number,
	items: PortMenuItem[],
	onClose: () => void,
): () => void {
	if (items.length === 0) {
		onClose();
		return () => {};
	}

	const backdrop = document.createElement('div');
	backdrop.style.cssText = 'position:fixed;inset:0;z-index:9998;';
	backdrop.addEventListener('click', onClose);
	backdrop.addEventListener('contextmenu', (e) => { e.preventDefault(); onClose(); });

	const menu = document.createElement('div');
	menu.style.cssText = `position:fixed;left:${x}px;top:${y}px;z-index:9999;background:white;border:1px solid #e4e4e7;border-radius:8px;box-shadow:0 4px 12px rgba(0,0,0,0.15);padding:4px 0;min-width:180px;`;

	for (const item of items) {
		const btn = document.createElement('button');
		const color = item.color ?? '#18181b';
		btn.style.cssText = `width:100%;display:flex;align-items:center;gap:8px;padding:6px 12px;font-size:12px;text-align:left;border:none;background:none;cursor:pointer;color:${color};`;
		btn.addEventListener('mouseenter', () => { btn.style.background = '#f4f4f5'; });
		btn.addEventListener('mouseleave', () => { btn.style.background = 'none'; });
		btn.innerHTML = `<span>${item.label}</span>`;
		btn.addEventListener('click', () => { item.onClick(); onClose(); });
		menu.appendChild(btn);
	}

	document.body.appendChild(backdrop);
	document.body.appendChild(menu);

	return () => {
		backdrop.remove();
		menu.remove();
	};
}
