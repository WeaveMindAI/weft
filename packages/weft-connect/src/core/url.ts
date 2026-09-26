/** `base` without its trailing slashes. A loop rather than `/\/+$/`:
 *  that pattern backtracks on a long run of slashes anywhere in the
 *  string, and `base` can come from a caller. */
export function trimTrailingSlashes(base: string): string {
	let end = base.length;
	while (end > 0 && base[end - 1] === '/') end--;
	return base.slice(0, end);
}
