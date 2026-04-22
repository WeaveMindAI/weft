/**
 * Shared field editing utility. Prevents race conditions where reactive store
 * updates overwrite input values mid-keystroke.
 *
 * Pattern: on focus, snapshot value to local state. On input, update local
 * state only (no store round-trip). After a debounce delay (user stops typing),
 * flush to store. On blur, flush immediately.
 *
 * Usage:
 *   const editor = createFieldEditor(2000);
 *   // In template: value={editor.display(key, storeValue)}
 *   //              onfocus={() => editor.focus(key, storeValue)}
 *   //              oninput={(e) => editor.input(e.currentTarget.value)}
 *   //              onblur={() => editor.blur(key, saveFn)}
 */

const DEFAULT_DEBOUNCE_MS = 2000;

export interface FieldEditor {
	/** Get the display value: local value if editing this key, otherwise the store value. */
	display: (key: string, storeValue: string) => string;
	/** Call on focus: snapshots the current store value into local state. */
	focus: (key: string, currentValue: string) => void;
	/** Call on input: updates local state only. Schedules a debounced save. */
	input: (value: string, key: string, saveFn: (value: string) => void) => void;
	/** Call on blur: flushes local value to store immediately. */
	blur: (key: string, saveFn: (value: string) => void) => void;
	/** Flush any pending debounced save immediately. Call before actions like Run Project. */
	flush: () => void;
	/** The current editing key (reactive, for use in templates). */
	readonly activeKey: string | null;
	/** The current local value (reactive, for use in templates). */
	readonly activeValue: string;
}

export function createFieldEditor(debounceMs: number = DEFAULT_DEBOUNCE_MS): FieldEditor {
	let _activeKey: string | null = $state(null);
	let _activeValue: string = $state('');
	let _timer: ReturnType<typeof setTimeout> | null = null;
	let _pendingSaveFn: ((value: string) => void) | null = null;

	function clearTimer() {
		if (_timer !== null) {
			clearTimeout(_timer);
			_timer = null;
		}
	}

	function display(key: string, storeValue: string): string {
		if (_activeKey === key) return _activeValue;
		return storeValue;
	}

	function focus(key: string, currentValue: string) {
		clearTimer();
		_activeKey = key;
		_activeValue = currentValue;
	}

	function input(value: string, key: string, saveFn: (value: string) => void) {
		_activeValue = value;
		_pendingSaveFn = saveFn;
		clearTimer();
		_timer = setTimeout(() => {
			if (_activeKey === key) {
				saveFn(_activeValue);
				_pendingSaveFn = null;
			}
		}, debounceMs);
	}

	function blur(key: string, saveFn: (value: string) => void) {
		clearTimer();
		if (_activeKey === key) {
			saveFn(_activeValue);
			_pendingSaveFn = null;
			_activeKey = null;
			_activeValue = '';
		}
	}

	function flush() {
		if (_activeKey !== null && _pendingSaveFn !== null) {
			clearTimer();
			_pendingSaveFn(_activeValue);
			_pendingSaveFn = null;
		}
	}

	return {
		display,
		focus,
		input,
		blur,
		flush,
		get activeKey() { return _activeKey; },
		get activeValue() { return _activeValue; },
	};
}
