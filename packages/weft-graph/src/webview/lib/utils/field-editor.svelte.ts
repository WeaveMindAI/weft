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

// Snappy enough that edits autosave shortly after you stop typing (no need to
// click away to commit), long enough not to hammer the surgical editor / file
// writes on every keystroke.
const DEFAULT_DEBOUNCE_MS = 700;

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
	// The value as last WRITTEN (at focus time, or by the latest
	// debounced/flushed save): a blur whose value matches it must not
	// emit a write (a click into a field and out again would otherwise
	// WRITE the rendered value back, and for a port literal an empty
	// rendering writes a delete). Tracked against the last save, not
	// the focus snapshot: the debounce saves DURING the focus, so
	// typing, autosaving, then deleting back to the focus value must
	// still write on blur or the file keeps the autosaved text.
	let _savedValue: string = '';
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
		_savedValue = currentValue;
	}

	/// THE one write path: every save (debounce fire, blur, flush)
	/// goes through here, so the "no write when the value matches the
	/// last write" guard cannot be forgotten at one of them (it was,
	/// twice).
	function commit(saveFn: (value: string) => void) {
		if (_activeValue === _savedValue) return;
		saveFn(_activeValue);
		_savedValue = _activeValue;
	}

	function input(value: string, key: string, saveFn: (value: string) => void) {
		// Typing into a field that was never focused has no baseline to
		// diff against; a silent drop here would throw the user's text
		// away, so refuse loudly (every control pairs onfocus with
		// oninput; this is the tripwire for one that forgets).
		if (_activeKey !== key) {
			throw new Error(
				`field editor: input() for '${key}' without focus() (active: ${_activeKey})`,
			);
		}
		_activeValue = value;
		_pendingSaveFn = saveFn;
		clearTimer();
		_timer = setTimeout(() => {
			commit(saveFn);
			_pendingSaveFn = null;
		}, debounceMs);
	}

	function blur(key: string, saveFn: (value: string) => void) {
		clearTimer();
		if (_activeKey === key) {
			commit(saveFn);
			_pendingSaveFn = null;
			_activeKey = null;
			_activeValue = '';
		}
	}

	function flush() {
		if (_activeKey !== null && _pendingSaveFn !== null) {
			clearTimer();
			commit(_pendingSaveFn);
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
