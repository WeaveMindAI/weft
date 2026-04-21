/**
 * Applies SEARCH/REPLACE block patches to a Weft string.
 *
 * Since @layout is no longer in weftCode, matching is simple string matching.
 * All offsets are in the RAW source string, no normalization of the source.
 */

export interface PatchApplyResult {
	patched: string;
	errors: string[];
}

interface SearchReplaceBlock {
	search: string;
	replace: string;
}

function parseBlocks(patchBody: string): SearchReplaceBlock[] {
	const blocks: SearchReplaceBlock[] = [];
	const pattern = /<<<<<<< SEARCH\n([\s\S]*?)\n?=======\n([\s\S]*?)\n?>>>>>>> REPLACE/g;
	let match: RegExpExecArray | null;
	while ((match = pattern.exec(patchBody)) !== null) {
		blocks.push({ search: match[1], replace: match[2] });
	}
	return blocks;
}

/** Trim trailing whitespace from each line (but not leading). */
function trimTrailing(s: string): string {
	return s.split('\n').map(l => l.trimEnd()).join('\n');
}

/**
 * Find `needle` in `haystack`. Returns offset and length in haystack, or null.
 * Tries exact match first, then with trailing-whitespace-trimmed needle.
 */
function findInRaw(haystack: string, needle: string): { offset: number; length: number } | null {
	// Exact match
	const idx = haystack.indexOf(needle);
	if (idx !== -1) return { offset: idx, length: needle.length };

	// Trim trailing whitespace from needle lines (AI might not match editor's trailing spaces)
	const trimmed = trimTrailing(needle);
	if (trimmed !== needle) {
		const tidx = haystack.indexOf(trimmed);
		if (tidx !== -1) return { offset: tidx, length: trimmed.length };
	}

	return null;
}

/** Splice replacement into source at a match, preserving newline boundaries.
 *  The key insight: if the source has a newline right after the match, include it
 *  in what gets replaced, and ensure the replacement ends with a newline too. */
function spliceAt(source: string, offset: number, length: number, replacement: string): string {
	// Extend match to include trailing newline if present (the regex strips it)
	let actualLength = length;
	const charAfterMatch = source[offset + length];
	if (charAfterMatch === '\n') {
		actualLength += 1; // consume the newline as part of the replaced region
	}

	const before = source.slice(0, offset);
	const after = source.slice(offset + actualLength);

	// Ensure replacement ends with newline if we consumed one
	let finalReplace = replacement;
	if (actualLength > length && !replacement.endsWith('\n')) {
		finalReplace += '\n';
	}

	return before + finalReplace + after;
}

function applyBlock(source: string, block: SearchReplaceBlock): { result: string; error?: string } {
	const replace = trimTrailing(block.replace);

	// 1. Find search text in source
	const match = findInRaw(source, block.search);
	if (match) {
		return { result: spliceAt(source, match.offset, match.length, replace) };
	}

	// 2. Try with trimmed search (strip leading/trailing blank lines)
	const trimmedSearch = block.search.trim();
	if (trimmedSearch !== block.search) {
		const trimMatch = findInRaw(source, trimmedSearch);
		if (trimMatch) {
			return { result: spliceAt(source, trimMatch.offset, trimMatch.length, replace) };
		}
	}

	const preview = trimmedSearch.split('\n')[0]?.trim().slice(0, 80) || trimmedSearch.slice(0, 80);
	return { result: source, error: `SEARCH block not found in current Weft: "${preview}"` };
}

/**
 * Find where a SEARCH block matches in the source.
 * Returns offset and length in the RAW source string (not a normalized copy).
 * Used by the streaming patch system for positional insertion.
 */
export function findSearchMatch(source: string, searchText: string): { offset: number; length: number } | { error: string } {
	// 1. Find in raw source
	const match = findInRaw(source, searchText);
	if (match) return match;

	// 2. Try trimmed (strip leading/trailing blank lines)
	const trimmed = searchText.trim();
	if (trimmed !== searchText) {
		const trimMatch = findInRaw(source, trimmed);
		if (trimMatch) return trimMatch;
	}

	const preview = trimmed.split('\n')[0]?.trim().slice(0, 80) || trimmed.slice(0, 80);
	return { error: `SEARCH block not found in current Weft: "${preview}"` };
}

export function applyWeftPatch(currentWeft: string, patchBody: string): PatchApplyResult {
	const blocks = parseBlocks(patchBody);
	if (blocks.length === 0) {
		return { patched: currentWeft, errors: ['No SEARCH/REPLACE blocks found in patch'] };
	}

	let source = currentWeft;
	const errors: string[] = [];

	for (const block of blocks) {
		const { result, error } = applyBlock(source, block);
		if (error) {
			errors.push(error);
		} else {
			source = result;
		}
	}

	return { patched: source, errors };
}

export function extractWeftPatchBlock(rawResponse: string): string | null {
	const match = rawResponse.match(/````weft-patch\s*\n([\s\S]*?)\n````/);
	return match ? match[1] : null;
}

export function hasWeftPatchMarker(rawResponse: string): boolean {
	return /````weft-patch\s*\n/.test(rawResponse);
}

export function extractLoomPatchBlock(rawResponse: string): string | null {
	const match = rawResponse.match(/````loom-patch\s*\n([\s\S]*?)\n````/);
	return match ? match[1] : null;
}

export function hasLoomPatchMarker(rawResponse: string): boolean {
	return /````loom-patch\s*\n/.test(rawResponse);
}

/** Apply a loom-patch block to a Loom DSL string. Same algorithm as weft-patch. */
export function applyLoomPatchText(currentLoom: string, patchBody: string): PatchApplyResult {
	return applyWeftPatch(currentLoom, patchBody);
}
