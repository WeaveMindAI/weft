/**
 * Field adapters: the translation layer between a Loom `as:` variant and the
 * underlying node field type.
 *
 * The rule: the variant picks the UI widget, the underlying field type
 * decides how the value is stored. Adapters are the thin code that bridges
 * the two. A Boolean node with `as:toggle` is trivial; a Text node with
 * `as:toggle` stores "true"/"false"; a List node with `as:multiselect`
 * stores a string[]; a Text node with `as:multiselect` stores JSON-encoded.
 *
 * The UI renderer asks the adapter:
 *   1. "What's the display value for this raw value?" → `read`
 *   2. "The user changed the UI to X, what should I store?" → `write`
 *
 * Adapters are pure functions, no Svelte state.
 */

import type { SetupItem, FieldDefinition } from '$lib/types';

// ── Option resolution ───────────────────────────────────────────────────────

/** Resolve the effective option list for a picker variant. */
export function resolveOptions(item: SetupItem, field: FieldDefinition | null): string[] {
	if (item.options && item.options.length > 0) return item.options;
	if (field?.options && field.options.length > 0) return field.options;
	return [];
}

// ── Boolean encode/decode ───────────────────────────────────────────────────

/**
 * Decode an arbitrary raw value into a boolean.
 * Accepts: literal bool, "true"/"false"/"yes"/"y"/"1"/"0", 0/1 numbers.
 * Falsy default for anything else.
 */
export function decodeBool(raw: unknown): boolean {
	if (typeof raw === 'boolean') return raw;
	if (typeof raw === 'number') return raw !== 0;
	if (typeof raw === 'string') {
		const s = raw.trim().toLowerCase();
		return s === 'true' || s === '1' || s === 'yes' || s === 'y' || s === 'on';
	}
	return false;
}

/**
 * Encode a boolean for storage, given the underlying field type.
 * - 'checkbox' native Boolean node → literal bool
 * - 'text' / 'textarea' → the string "true" / "false"
 * - 'number' → 1 / 0
 */
export function encodeBool(value: boolean, fieldType: string): unknown {
	if (fieldType === 'checkbox') return value;
	if (fieldType === 'number') return value ? 1 : 0;
	// Text, textarea, password, anything else: stringified.
	return value ? 'true' : 'false';
}

// ── List encode/decode ──────────────────────────────────────────────────────

/**
 * Decode a raw value into a string array. Handles:
 * - native arrays
 * - JSON-encoded strings ('["a","b"]')
 * - comma-separated ("a,b,c")
 */
export function decodeList(raw: unknown): string[] {
	if (Array.isArray(raw)) return raw.map(v => String(v));
	if (typeof raw !== 'string') return [];
	const s = raw.trim();
	if (!s) return [];
	// Try JSON first, otherwise comma split.
	if (s.startsWith('[') && s.endsWith(']')) {
		try {
			const parsed = JSON.parse(s);
			if (Array.isArray(parsed)) return parsed.map(v => String(v));
		} catch {
			// fall through to comma split
		}
	}
	return s.split(',').map(x => x.trim()).filter(x => x.length > 0);
}

/**
 * Encode a list back to the field's native storage format.
 * - list field type → native array
 * - text/textarea → JSON-encoded string
 */
export function encodeList(value: string[], fieldType: string): unknown {
	// The list catalog field stores arrays directly.
	if (fieldType === 'multiselect') return value;
	// Text-shaped fields get JSON (per Quentin: "JSON is the best" for Text).
	return JSON.stringify(value);
}

// ── String encode/decode ────────────────────────────────────────────────────

/** Coerce any raw value into a string for display. */
export function decodeString(raw: unknown): string {
	if (raw === null || raw === undefined) return '';
	if (typeof raw === 'string') return raw;
	return String(raw);
}

// ── Number encode/decode ────────────────────────────────────────────────────

export function decodeNumber(raw: unknown): number {
	if (typeof raw === 'number') return raw;
	if (typeof raw === 'string') {
		const n = Number(raw);
		return Number.isFinite(n) ? n : 0;
	}
	if (typeof raw === 'boolean') return raw ? 1 : 0;
	return 0;
}

export function encodeNumber(value: number, fieldType: string): unknown {
	if (fieldType === 'number') return value;
	// Text-shaped storage: store as string so it round-trips cleanly.
	return String(value);
}

// ── Variant compatibility matrix ────────────────────────────────────────────

/**
 * Which field types each variant can render over. The first entry is the
 * "native" type; any other entry is supported via an adapter. This is used
 * by the loom-builder prompt and the Loom validator (future work).
 */
export const VARIANT_COMPATIBILITY: Record<string, string[]> = {
	// Text inputs
	text:       ['text', 'textarea', 'number', 'password'],
	textarea:   ['textarea', 'text'],
	password:   ['password', 'text'],
	email:      ['text', 'textarea'],
	url:        ['text', 'textarea'],
	// Numeric
	number:     ['number', 'text'],
	slider:     ['number', 'text'],
	// Boolean
	toggle:     ['checkbox', 'text', 'number'],
	checkbox:   ['checkbox', 'text', 'number'],
	// Single-select pickers
	radio:      ['select', 'text', 'multiselect'],
	select:     ['select', 'text', 'multiselect'],
	cards:      ['select', 'text', 'multiselect'],
	// Multi-select pickers
	multiselect:['multiselect', 'text', 'textarea'],
	tags:       ['multiselect', 'text', 'textarea'],
	multicards: ['multiselect', 'text', 'textarea'],
	// Specialized
	date:       ['text', 'number'],
	time:       ['text'],
	datetime:   ['text', 'number'],
	color:      ['text'],
	file:       ['blob'],
};

/**
 * What's the "natural" field type for a given variant? Used in the Loom
 * prompt to guide Tangle toward the right primitive node.
 */
export function naturalFieldType(variant: string): string {
	return VARIANT_COMPATIBILITY[variant]?.[0] ?? 'text';
}
