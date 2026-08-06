/**
 * Every catalog node's declared icon must be one the installed
 * `@lucide/svelte` actually ships: an unknown name renders as the
 * generic Square in the editor (with a console error nobody watches in
 * production), which is exactly how deleted or misspelled icons have
 * shipped before. This walks the repo's whole `catalog/` and pins each
 * metadata `icon` to the same lookup the editor renders with, so a
 * lucide upgrade that drops an icon (the brand icons already went) or
 * a typo in a new node fails here instead of on screen.
 */

import { describe, it, expect } from 'vitest';
import * as fs from 'node:fs';
import * as path from 'node:path';
import { BUILTIN_NODE_ICONS, iconComponent } from './icons';

const CATALOG_ROOT = path.resolve(import.meta.dirname, '../../../../../..', 'catalog');

/** Every metadata.json under the catalog, relative path + parsed body. */
function catalogMetadata(): Array<{ file: string; meta: Record<string, unknown> }> {
	return fs
		.readdirSync(CATALOG_ROOT, { recursive: true, encoding: 'utf8' })
		.filter((p) => path.basename(p) === 'metadata.json')
		.map((p) => ({
			file: p,
			meta: JSON.parse(fs.readFileSync(path.join(CATALOG_ROOT, p), 'utf8')) as Record<
				string,
				unknown
			>,
		}));
}

describe('catalog icons', () => {
	it('finds the catalog (the layout moved if this trips)', () => {
		expect(catalogMetadata().length).toBeGreaterThan(0);
	});

	it('every declared icon is one the installed lucide ships', () => {
		const broken = catalogMetadata()
			.filter(({ meta }) => typeof meta.icon === 'string')
			.filter(({ meta }) => iconComponent(meta.icon as string) === undefined)
			.map(({ file, meta }) => `${meta.icon} (catalog/${file})`);
		expect(broken).toEqual([]);
	});

	it('the editor builtins (Group, Loop) name icons lucide ships', () => {
		const broken = Object.entries(BUILTIN_NODE_ICONS)
			.filter(([, icon]) => iconComponent(icon) === undefined)
			.map(([type, icon]) => `${icon} (builtin ${type})`);
		expect(broken).toEqual([]);
	});
});
