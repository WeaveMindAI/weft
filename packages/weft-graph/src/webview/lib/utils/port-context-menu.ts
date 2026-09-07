/**
 * Shared port context menu utility.
 * Creates a floating menu attached to document.body (avoids CSS transform issues in xyflow nodes).
 * Returns a cleanup function for use in Svelte $effect.
 */

import { untrack } from 'svelte';
import type { PortDefinition } from '../types';

/// An informational row (a role header, an explainer, a conflict
/// reason): plain text, no click handler, not in the tab order, and a
/// click neither acts nor dismisses the menu.
export interface PortMenuNote {
	note: true;
	label: string;
	color?: string;
}

/// An actionable row: a button that runs `onClick` and closes the
/// menu, or, when `editable` is set, swaps itself for an inline text
/// input pre-populated with `value` (Enter or blur fires
/// `onCommit(newValue)`; Escape discards the edit and closes the
/// menu; the "Type: X" row uses it because VS Code webviews block the
/// browser `prompt()` API).
export interface PortMenuAction {
	note?: false;
	label: string;
	onClick: () => void;
	color?: string;
	editable?: {
		value: string;
		onCommit: (newValue: string) => void;
	};
}

export type PortMenuItem = PortMenuNote | PortMenuAction;

/** Parameters for the shared port menu builder. Both ProjectNode and GroupNode
 *  call `buildPortMenuItems` with their own port + callbacks so the menu
 *  content stays identical across all port surfaces in the graph. */
/** The side of the port, and with it the ONE writer only an input
 *  has: requiredness has no runtime meaning on an output, so an output
 *  caller cannot be handed the writer (the type forbids it) and an input
 *  caller cannot forget it (the type requires it). The writer sets the
 *  port's requiredness to exactly the value the menu row promised; the
 *  menu is a frozen snapshot, so a toggle against LIVE state could
 *  write the opposite after a reparse landed under the open menu. */
export type PortMenuSide =
	| { side: 'input'; onSetRequired: (required: boolean) => void }
	| { side: 'output'; onSetRequired?: never };

export interface BasePortMenuOptions {
	port: PortDefinition;
	/** What the delete gesture does: `portDeleteAction` from
	 *  projection/header-ports, the ONE rule every delete surface
	 *  shares. 'remove' kills the port and its wires; 'revert' drops an
	 *  override back to the catalog shape (wires kept); null offers no
	 *  delete item. For groups every interface port is 'remove'. */
	deleteAction: 'remove' | 'revert' | null;
	/** True when this side's ports are DERIVED from a config list (a
	 *  form's `fields`, a switch's `cases`): type and requiredness live
	 *  in that list, so the menu explains instead of offering edits the
	 *  emitter must not write into the header. */
	configDerived?: boolean;
	onSetType: (newType: string) => void;
	onRemove: () => void;
	/** Loop-specific role context. Set ONLY when the menu is being built for
	 *  a port on a Loop container; the strip surfaces the four derived roles
	 *  (broadcast / iter / gather / carry) and lets the user cycle the port
	 *  to the legal alternatives. The handlers translate the chosen role to
	 *  the right cascade of setConfig / updateGroupPorts ops. */
	loopRole?: LoopPortRoleContext;
}

export type BuildPortMenuOptions = BasePortMenuOptions & PortMenuSide;

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
	const { port, side, deleteAction, configDerived, onSetType, onRemove, loopRole } = opts;
	const items: PortMenuItem[] = [];

	// Synthesized carry inputs are ghost mirrors of the carry output. The
	// user can't edit them directly; the menu just explains and offers a
	// no-op header.
	if (loopRole?.currentRole === 'synthesized_carry_input') {
		items.push({
			label: `Carry input (auto from output \`${port.name}\`)`,
			note: true,
			color: '#8b5cf6',
		});
		items.push({
			label: 'Edit the carry output to change this port.',
			note: true,
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
			note: true,
			color: '#8b5cf6',
		});
		const target = oppositeRole(loopRole.currentRole);
		if (target) {
			if (loopRole.conflictReason) {
				items.push({
					label: `Cannot switch to ${humanRole(target)}`,
					note: true,
					color: '#71717a',
				});
				items.push({
					label: loopRole.conflictReason,
					note: true,
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
				note: true,
				color: '#71717a',
			});
		}
	}

	// Config-derived ports (a form's `fields`, a switch's `cases`):
	// their type and requiredness live in the config list, and the
	// emitter never writes them into the header, so offering those
	// edits here would silently swallow them. Explain instead. (A
	// DECLARED shadow line over a derived name is not flagged derived
	// by the caller, so it keeps the full menu below, delete included.)
	if (configDerived) {
		items.push({
			label: 'This port comes from the node’s field list; edit that instead.',
			note: true,
			color: '#71717a',
		});
		return items;
	}

	// Required toggle (inputs only; outputs do not have runtime required
	// semantics, and only an input caller hands over the writer).
	if (opts.side === 'input') {
		const setRequired = opts.onSetRequired;
		items.push({
			label: port.required ? '☐ Make optional' : '☑ Make required',
			onClick: () => setRequired(!port.required),
		});
	}

	// Type edit. Click swaps the row in place for an `<input>`; Enter
	// commits, Escape cancels, blur commits. We can't use the browser
	// `prompt()` here because VS Code webviews block it.
	items.push({
		label: `✎ Type: ${port.portType}`,
		onClick: () => {/* handled by the editable path */},
		editable: {
			value: port.portType,
			onCommit: (newValue) => {
				const trimmed = newValue.trim();
				if (trimmed && trimmed !== port.portType) {
					onSetType(trimmed);
				}
			},
		},
	});

	// Delete, labeled by what it will DO (see portDeleteAction): a
	// custom port truly goes; an override drops back to its catalog
	// shape with wires kept.
	if (deleteAction) {
		items.push({
			label: deleteAction === 'revert' ? 'Reset to default' : 'Remove port',
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

/** Open a port menu from inside a Svelte `$effect`. The menu is a
 *  SNAPSHOT of the gesture: `build` runs UNTRACKED, so the effect that
 *  calls this depends only on the open/close state it read before
 *  calling, never on the port lists `build` reads. Tracking those would
 *  rebuild the menu on every incoming reparse, tearing an open type
 *  editor out from under the user mid-typing. `build` returns null when
 *  the port is gone (nothing opens). Returns the disposer for the
 *  effect to hand back. The ONE shape every port surface uses, so a
 *  third surface cannot re-fork the tracking rule. */
export function openPortMenu(
	anchor: { x: number; y: number },
	build: () => PortMenuItem[] | null,
	onClose: () => void,
): (() => void) | undefined {
	const items = untrack(build);
	if (!items) return undefined;
	return createPortContextMenu(anchor.x, anchor.y, items, onClose);
}

function createPortContextMenu(
	x: number,
	y: number,
	items: PortMenuItem[],
	onClose: () => void,
): () => void {
	// Set by the disposer FIRST: removing the menu while the inline type
	// editor holds focus may fire `blur` on the input, and a blur commit
	// after disposal would write an abandoned edit into source.
	let disposed = false;

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
			const color = item.color ?? '#18181b';
			if (item.note) {
				// Plain text: no button semantics, no tab stop, no handler
				// (the menu sits beside the backdrop, not inside it, so a
				// click here reaches nothing that would close it).
				const text = document.createElement('div');
				text.style.cssText = `padding:6px 12px;font-size:12px;color:${color};cursor:default;`;
				text.textContent = item.label;
				row.appendChild(text);
				return;
			}
			const btn = document.createElement('button');
			btn.style.cssText = `width:100%;display:flex;align-items:center;gap:8px;padding:6px 12px;font-size:12px;text-align:left;border:none;background:none;cursor:pointer;color:${color};`;
			btn.addEventListener('mouseenter', () => { btn.style.background = '#f4f4f5'; });
			btn.addEventListener('mouseleave', () => { btn.style.background = 'none'; });
			btn.textContent = item.label;
			btn.addEventListener('click', () => {
				if (item.editable) {
					renderInput(item.editable);
				} else {
					item.onClick();
					onClose();
				}
			});
			row.appendChild(btn);
		};

		const renderInput = (edit: { value: string; onCommit: (newValue: string) => void }) => {
			// Once settled (committed or discarded), nothing commits again.
			let settled = false;
			row.innerHTML = '';
			const wrap = document.createElement('div');
			wrap.style.cssText = 'padding:4px 8px;background:#f4f4f5;';
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
					// Handled here; the menu's document-level Escape
					// listener must not close a second time.
					e.stopPropagation();
					// Escape DISCARDS: closing removes the focused input,
					// and a browser may fire `blur` on removal, which
					// would otherwise commit the abandoned text.
					settled = true;
					onClose();
				}
			});
			function commit() {
				if (settled || disposed) return;
				settled = true;
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

	// Escape dismisses from anywhere: a menu made of notes alone has no
	// focusable row, so without this a keyboard user could not close the
	// overlay. (The inline type editor's own Escape handler also closes.)
	const onEsc = (e: KeyboardEvent) => {
		if (e.key === 'Escape') onClose();
	};
	document.addEventListener('keydown', onEsc);

	return () => {
		disposed = true;
		document.removeEventListener('keydown', onEsc);
		backdrop.remove();
		menu.remove();
	};
}
