// Ported from dashboard-v1. Right-click port menu, rendered on
// document.body so xyflow's CSS transforms on the node don't skew
// position. Returns a cleanup function; Svelte $effect hands that
// back on unmount.

import type { PortDefinition } from '../../shared/protocol';

export interface PortMenuItem {
  label: string;
  onClick: () => void;
  color?: string;
}

export interface BuildPortMenuOptions {
  port: PortDefinition;
  side: 'input' | 'output';
  isCustom: boolean;
  canAddPorts: boolean;
  onToggleRequired: () => void;
  onSetType: (newType: string) => void;
  onRemove: () => void;
}

export function buildPortMenuItems(opts: BuildPortMenuOptions): PortMenuItem[] {
  const { port, side, isCustom, canAddPorts, onToggleRequired, onSetType, onRemove } = opts;
  const items: PortMenuItem[] = [];

  if (side === 'input') {
    items.push({
      label: port.required ? '☐ Make optional' : '☑ Make required',
      onClick: onToggleRequired,
    });
  }

  items.push({
    label: `✎ Type: ${port.portType || 'MustOverride'}`,
    onClick: () => {
      const newType = prompt('Enter port type:', port.portType || '');
      if (newType !== null && newType.trim() && newType.trim() !== port.portType) {
        onSetType(newType.trim());
      }
    },
  });

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
  backdrop.addEventListener('contextmenu', (e) => {
    e.preventDefault();
    onClose();
  });

  const menu = document.createElement('div');
  menu.style.cssText = `position:fixed;left:${x}px;top:${y}px;z-index:9999;background:white;border:1px solid #e4e4e7;border-radius:8px;box-shadow:0 4px 12px rgba(0,0,0,0.15);padding:4px 0;min-width:180px;`;

  for (const item of items) {
    const btn = document.createElement('button');
    const color = item.color ?? '#18181b';
    btn.style.cssText = `width:100%;display:flex;align-items:center;gap:8px;padding:6px 12px;font-size:12px;text-align:left;border:none;background:none;cursor:pointer;color:${color};`;
    btn.addEventListener('mouseenter', () => {
      btn.style.background = '#f4f4f5';
    });
    btn.addEventListener('mouseleave', () => {
      btn.style.background = 'none';
    });
    btn.innerHTML = `<span>${item.label}</span>`;
    btn.addEventListener('click', () => {
      item.onClick();
      onClose();
    });
    menu.appendChild(btn);
  }

  document.body.appendChild(backdrop);
  document.body.appendChild(menu);

  return () => {
    backdrop.remove();
    menu.remove();
  };
}
