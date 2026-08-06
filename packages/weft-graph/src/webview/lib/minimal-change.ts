/** The smallest single splice turning `oldValue` into `newValue`
 *  (shared prefix and suffix left untouched), or null when they are
 *  already equal. Shaped as a CodeMirror change spec so an editor can
 *  apply an external value without discarding cursor, selection, or
 *  undo history outside the changed span. */
export function minimalChange(
	oldValue: string,
	newValue: string,
): { from: number; to: number; insert: string } | null {
	if (oldValue === newValue) return null;
	let prefixLen = 0;
	const minLen = Math.min(oldValue.length, newValue.length);
	while (prefixLen < minLen && oldValue[prefixLen] === newValue[prefixLen]) prefixLen++;
	let oldSuffix = oldValue.length;
	let newSuffix = newValue.length;
	while (
		oldSuffix > prefixLen &&
		newSuffix > prefixLen &&
		oldValue[oldSuffix - 1] === newValue[newSuffix - 1]
	) {
		oldSuffix--;
		newSuffix--;
	}
	return { from: prefixLen, to: oldSuffix, insert: newValue.slice(prefixLen, newSuffix) };
}
