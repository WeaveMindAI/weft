import { callPathOf, type Frame } from '../../../protocol';

/** A run's rows sorted by where they sit relative to the view's call
 *  path. `here` holds the rows of the calls this view descended through
 *  (a row's call sites, in order, equal `path`), keyed by node id: at the
 *  top level every row with no call frame, inside an included file the
 *  rows of the one call on screen. `inside` holds the rows that ran
 *  UNDER a call site of this level (their call sites extend `path` by at
 *  least one), keyed by that site: what an include box at this level
 *  stands for, however deep the calls below it go. */
export type RowsByCallPath<R> = {
	here: Record<string, R[]>;
	inside: Record<string, R[]>;
};

/** Split `rows` (by node id) around `path`; see `RowsByCallPath`. The same
 *  file reached through another site keeps its own rows: they are under
 *  neither key. */
export function rowsByCallPath<R extends { frames: readonly Frame[] }>(
	rows: Record<string, R[]>,
	path: readonly string[],
): RowsByCallPath<R> {
	const here: Record<string, R[]> = Object.create(null);
	const inside: Record<string, R[]> = Object.create(null);
	for (const [id, list] of Object.entries(rows)) {
		for (const row of list) {
			const sites = callPathOf(row.frames);
			if (sites.length < path.length || !path.every((s, i) => s === sites[i])) continue;
			if (sites.length === path.length) {
				(here[id] ??= []).push(row);
			} else {
				(inside[sites[path.length]] ??= []).push(row);
			}
		}
	}
	return { here, inside };
}
