/**
 * Shared port context menu utility.
 * Creates a floating menu attached to document.body (avoids CSS transform issues in xyflow nodes).
 * Returns a cleanup function for use in Svelte $effect.
 */

import type { PortDefinition } from '../types';

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
	/** Loop-specific role context. Set ONLY when the menu is being built for
	 *  a port on a Loop container; the strip surfaces the four derived roles
	 *  (broadcast / iter / gather / carry) and lets the user cycle the port
	 *  to the legal alternatives. The handlers translate the chosen role to
	 *  the right cascade of setConfig / updateGroupPorts ops. */
	loopRole?: LoopPortRoleContext;
}

export type LoopPortRole =
	| 'broadcast'
	| 'iter'
	| 'gather'
	| 'carry'
	| 'synthesized_carry_input'
	// User-declared input whose name shadows a carry output (or vice versa).
	// Surfaced with a clear remediation; the user keeps the delete handle
	// (the prior shape misclassified this as a ghost and stripped Remove).
	| 'name_collision';

export interface LoopPortRoleContext {
	/** The role this port currently has, derived from over / carry config. */
	currentRole: LoopPortRole;
	/** A blocking reason that prevents toggling the role (e.g. a same-named
	 *  input with a conflicting type would prevent making an output a carry).
	 *  When set, the toggle button is disabled and the reason renders as a
	 *  muted line so the user knows what to fix. */
	conflictReason?: string;
	/** Toggle to the other role (broadcast ↔ iter, or gather ↔ carry). Only
	 *  called when there is no conflictReason. */
	onToggleRole: () => void;
}

/** Build the standard port context menu items. Exactly one definition to
 *  keep every port surface (regular node, group expanded, group collapsed)
 *  identical. */
export function buildPortMenuItems(opts: BuildPortMenuOptions): PortMenuItem[] {
	const { port, side, isCustom, canAddPorts, onToggleRequired, onSetType, onRemove, loopRole } = opts;
	const items: PortMenuItem[] = [];

	// Synthesized carry inputs are ghost mirrors of the carry output. The
	// user can't edit them directly; the menu just explains and offers a
	// no-op header.
	if (loopRole?.currentRole === 'synthesized_carry_input') {
		items.push({
			label: `Carry input (auto from output \`${port.name}\`)`,
			onClick: () => {},
			color: '#8b5cf6',
		});
		items.push({
			label: 'Edit the carry output to change this port.',
			onClick: () => {},
			color: '#71717a',
		});
		return items;
	}

	// Loop role header + cycle button. `name_collision` keeps the Remove
	// option (handled below) so the user can delete their colliding port;
	// the header surfaces the conflictReason directly because oppositeRole
	// returns null and the cycle branch is skipped.
	if (loopRole) {
		const roleLabel = humanRole(loopRole.currentRole);
		items.push({
			label: `Role: ${roleLabel}`,
			onClick: () => {},
			color: '#8b5cf6',
		});
		const target = oppositeRole(loopRole.currentRole);
		if (target) {
			if (loopRole.conflictReason) {
				items.push({
					label: `Cannot switch to ${humanRole(target)}`,
					onClick: () => {},
					color: '#71717a',
				});
				items.push({
					label: loopRole.conflictReason,
					onClick: () => {},
					color: '#71717a',
				});
			} else {
				items.push({
					label: `↻ Make ${humanRole(target)}`,
					onClick: loopRole.onToggleRole,
				});
			}
		} else if (loopRole.conflictReason) {
			// Roles with no toggle target still surface the reason
			// (name_collision is the current case): the user needs to
			// see WHY the port is flagged before they decide to rename
			// or delete.
			items.push({
				label: loopRole.conflictReason,
				onClick: () => {},
				color: '#71717a',
			});
		}
	}

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

function humanRole(role: LoopPortRole): string {
	switch (role) {
		case 'broadcast': return 'Broadcast input';
		case 'iter': return 'Iter input (List[T])';
		case 'gather': return 'Gather output (List[T | Null])';
		case 'carry': return 'Carry port (threaded across iterations)';
		case 'synthesized_carry_input': return 'Synthesized carry input';
		case 'name_collision': return 'Name conflict';
	}
}

/// The role a port cycles to when the user clicks "Make X". Inputs cycle
/// between broadcast and iter; outputs cycle between gather and carry.
/// Returns null for roles with no toggle target (synthesized carry input is
/// handled by the early-return above).
function oppositeRole(role: LoopPortRole): LoopPortRole | null {
	switch (role) {
		case 'broadcast': return 'iter';
		case 'iter': return 'broadcast';
		case 'gather': return 'carry';
		case 'carry': return 'gather';
		case 'synthesized_carry_input': return null;
		case 'name_collision': return null;
	}
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
