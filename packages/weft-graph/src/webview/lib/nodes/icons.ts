/**
 * Node icon resolution: a metadata `icon` names ANY icon the installed
 * `@lucide/svelte` ships (PascalCase, e.g. "BrainCircuit"; the full set
 * is lucide.dev/icons). The lookup is dynamic against the library's
 * whole namespace, so a new icon name needs no editor change at all;
 * the cost is that the webview bundle carries every lucide icon
 * (namespace import, no tree-shaking), which a locally-served editor
 * absorbs. A name the library does not ship falls back to the generic
 * Square and logs loudly; `icons.test.ts` pins every catalog
 * metadata's icon to this same lookup so a deleted or misspelled icon
 * fails the suite instead of shipping as a square.
 */

import type { Component } from 'svelte';
import * as lucide from '@lucide/svelte/icons';

// The icons entry also exports the base component and its plumbing;
// those are not icons a node may name.
const NON_ICON_EXPORTS = new Set(['Icon', 'defaultAttributes']);

/** The icons the EDITOR itself asks for (the editor-only Group and
 * Loop builtins), by node type. Named here, next to the lookup, so
 * `icons.test.ts` pins them alongside the catalog's: a lucide upgrade
 * that drops one fails the suite instead of shipping as a square. */
export const BUILTIN_NODE_ICONS = {
	Group: 'GitFork',
	Loop: 'Repeat',
} as const;

/** The lucide component for `name`, or undefined when the installed
 * lucide does not ship it. */
export function iconComponent(name: string): Component | undefined {
	if (!/^[A-Z][A-Za-z0-9]*$/.test(name) || NON_ICON_EXPORTS.has(name)) return undefined;
	return (lucide as unknown as Record<string, Component | undefined>)[name];
}

/** The component a node renders: its declared icon, or the generic
 * Square (absent name, or a name the installed lucide does not ship,
 * surfaced loudly so it gets fixed instead of shipping as a square). */
export function resolveIcon(name: string | undefined): Component {
	if (!name) return lucide.Square as Component;
	const icon = iconComponent(name);
	if (!icon) {
		console.error(
			`resolveIcon: '${name}' is not an icon the installed @lucide/svelte ships; ` +
				`pick one from lucide.dev/icons. Falling back to Square.`,
		);
		return lucide.Square as Component;
	}
	return icon;
}
