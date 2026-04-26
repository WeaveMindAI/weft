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
	/// When set, clicking the row swaps it for an inline text input
	/// pre-populated with `value`. Pressing Enter or blurring fires
	/// `onCommit(newValue)`; Escape cancels. Used for "Type: X" row
	/// because VS Code webviews block the browser `prompt()` API.
	editable?: {
		value: string;
		onCommit: (newValue: string) => void;
	};
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

	// Type edit. Click swaps the row in place for an `<input>`; Enter
	// commits, Escape cancels, blur commits. We can't use the browser
	// `prompt()` here because VS Code webviews block it.
	items.push({
		label: `✎ Type: ${port.portType || 'MustOverride'}`,
		onClick: () => {/* handled by the editable path */},
		editable: {
			value: port.portType || '',
			onCommit: (newValue) => {
				const trimmed = newValue.trim();
				if (trimmed && trimmed !== port.portType) {
					onSetType(trimmed);
				}
			},
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
		const row = document.createElement('div');
		row.style.cssText = 'width:100%;';
		menu.appendChild(row);

		const renderButton = () => {
			row.innerHTML = '';
			const btn = document.createElement('button');
			const color = item.color ?? '#18181b';
			btn.style.cssText = `width:100%;display:flex;align-items:center;gap:8px;padding:6px 12px;font-size:12px;text-align:left;border:none;background:none;cursor:pointer;color:${color};`;
			btn.addEventListener('mouseenter', () => { btn.style.background = '#f4f4f5'; });
			btn.addEventListener('mouseleave', () => { btn.style.background = 'none'; });
			btn.textContent = item.label;
			btn.addEventListener('click', (e) => {
				if (item.editable) {
					// Don't let the click bubble to the backdrop's
					// click-to-close handler; we want to keep the
					// menu mounted while the user types.
					e.stopPropagation();
					renderInput(item.editable);
				} else {
					item.onClick();
					onClose();
				}
			});
			row.appendChild(btn);
		};

		const renderInput = (edit: { value: string; onCommit: (newValue: string) => void }) => {
			row.innerHTML = '';
			const wrap = document.createElement('div');
			wrap.style.cssText = 'padding:4px 8px;background:#f4f4f5;';
			// Clicking the wrap or the input itself must not
			// bubble to the backdrop (which would close the menu).
			wrap.addEventListener('click', (e) => { e.stopPropagation(); });
			const input = document.createElement('input');
			input.type = 'text';
			input.value = edit.value;
			input.placeholder = 'Type name (e.g. String, List[Number])';
			input.style.cssText = 'width:100%;box-sizing:border-box;font-size:12px;font-family:inherit;padding:4px 6px;border:1px solid #d4d4d8;border-radius:4px;background:white;color:#18181b;outline:none;';
			input.addEventListener('focus', () => { input.style.borderColor = '#71717a'; });
			input.addEventListener('blur', commit);
			input.addEventListener('keydown', (e) => {
				if (e.key === 'Enter') {
					e.preventDefault();
					commit();
				} else if (e.key === 'Escape') {
					e.preventDefault();
					onClose();
				}
			});
			// Stop right-clicks from bubbling to the backdrop and
			// prematurely closing the menu while the user is editing.
			input.addEventListener('contextmenu', (e) => { e.stopPropagation(); });
			let committed = false;
			function commit() {
				if (committed) return;
				committed = true;
				edit.onCommit(input.value);
				onClose();
			}
			wrap.appendChild(input);
			row.appendChild(wrap);
			// Defer focus + select to next tick so the click that
			// triggered the swap doesn't immediately blur the input.
			setTimeout(() => {
				input.focus();
				input.select();
			}, 0);
		};

		renderButton();
	}

	document.body.appendChild(backdrop);
	document.body.appendChild(menu);

	return () => {
		backdrop.remove();
		menu.remove();
	};
}
