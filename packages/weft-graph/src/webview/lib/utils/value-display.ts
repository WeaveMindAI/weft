// How one execution value is shown in the inspector: what its badge
// says, and the tokens its JSON renders as.
//
// The tokens are built by WALKING the value, never by regex over
// `JSON.stringify` output. A regex cannot tell a brace inside a string
// from a brace in the structure, so a value carrying JSON-looking text
// (an LLM reply, a prompt, a log line) would colour as garbage. Walking
// gets it right by construction and is what the tests pin.

export type JsonTokenKind = 'key' | 'string' | 'number' | 'boolean' | 'null' | 'punct';

export interface JsonToken {
	text: string;
	kind: JsonTokenKind;
}

const INDENT = '  ';

/** A string value is shown as itself, not as a quoted JSON literal.
 *  A caption or a prompt is text a person reads, and `"...\n..."` with
 *  escaped newlines is the one rendering that makes it unreadable. */
export function isPlainText(value: unknown): value is string {
	return typeof value === 'string';
}

/** What the card's badge says: the value's type and how much of it
 *  there is, so a person can tell a 3-key object from a 300-key one
 *  without opening it. */
export function valueSummary(value: unknown): string {
	if (value === null) return 'null';
	if (Array.isArray(value)) {
		return `array · ${value.length} ${value.length === 1 ? 'item' : 'items'}`;
	}
	switch (typeof value) {
		case 'string':
			return `text · ${countLabel(value.length, 'char')}`;
		case 'number':
			return 'number';
		case 'boolean':
			return 'boolean';
		case 'undefined':
			return 'empty';
		case 'object': {
			const keys = Object.keys(value as Record<string, unknown>).length;
			return `object · ${countLabel(keys, 'key')}`;
		}
		default:
			return typeof value;
	}
}

function countLabel(n: number, unit: string): string {
	return `${n.toLocaleString('en-US')} ${n === 1 ? unit : `${unit}s`}`;
}

/** Pretty-printed JSON, as tokens a renderer can colour. Indentation
 *  and newlines ride along as `punct`, so the caller only has to paint
 *  each token and keep whitespace. */
export function jsonTokens(value: unknown): JsonToken[] {
	const out: JsonToken[] = [];
	write(out, value, 0);
	return out;
}

function write(out: JsonToken[], value: unknown, depth: number): void {
	if (value === null) return push(out, 'null', 'null');
	if (value === undefined) return push(out, 'undefined', 'null');
	if (Array.isArray(value)) return writeArray(out, value, depth);
	switch (typeof value) {
		case 'string':
			return push(out, JSON.stringify(value), 'string');
		case 'number':
			// `String`, not `JSON.stringify`: a non-finite number has no
			// JSON spelling and would print as `null`, losing which one
			// it was. `String(NaN)` says `NaN`.
			return push(out, String(value), 'number');
		case 'boolean':
			return push(out, String(value), 'boolean');
		case 'object':
			return writeObject(out, value as Record<string, unknown>, depth);
		default:
			// A function or a symbol cannot reach here from parsed wire
			// JSON; render it visibly rather than dropping it silently.
			return push(out, String(value), 'null');
	}
}

function writeArray(out: JsonToken[], items: unknown[], depth: number): void {
	if (items.length === 0) return push(out, '[]', 'punct');
	push(out, '[\n', 'punct');
	items.forEach((item, i) => {
		push(out, INDENT.repeat(depth + 1), 'punct');
		write(out, item, depth + 1);
		push(out, i === items.length - 1 ? '\n' : ',\n', 'punct');
	});
	push(out, `${INDENT.repeat(depth)}]`, 'punct');
}

function writeObject(out: JsonToken[], obj: Record<string, unknown>, depth: number): void {
	const entries = Object.entries(obj);
	if (entries.length === 0) return push(out, '{}', 'punct');
	push(out, '{\n', 'punct');
	entries.forEach(([key, val], i) => {
		push(out, INDENT.repeat(depth + 1), 'punct');
		push(out, JSON.stringify(key), 'key');
		push(out, ': ', 'punct');
		write(out, val, depth + 1);
		push(out, i === entries.length - 1 ? '\n' : ',\n', 'punct');
	});
	push(out, `${INDENT.repeat(depth)}}`, 'punct');
}

function push(out: JsonToken[], text: string, kind: JsonTokenKind): void {
	out.push({ text, kind });
}

/** The plain text behind a value, for the copy button and for the
 *  "select all" a double-click performs: a string copies as itself,
 *  anything else as its pretty JSON. */
export function valueText(value: unknown): string {
	if (isPlainText(value)) return value;
	return jsonTokens(value)
		.map((t) => t.text)
		.join('');
}
