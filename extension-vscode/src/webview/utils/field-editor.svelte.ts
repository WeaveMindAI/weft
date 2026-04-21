// Ported from dashboard-v1/src/lib/utils/field-editor.svelte.ts.
// Prevents reactive update race conditions where a store value
// overwrites the user's in-progress keystrokes. Pattern: on focus,
// snapshot to local state. On input, update local state only. After
// a debounce (default 2s), flush to store. On blur, flush
// immediately.

const DEFAULT_DEBOUNCE_MS = 2000;

export interface FieldEditor {
  display: (key: string, storeValue: string) => string;
  focus: (key: string, currentValue: string) => void;
  input: (value: string, key: string, saveFn: (value: string) => void) => void;
  blur: (key: string, saveFn: (value: string) => void) => void;
  flush: () => void;
  readonly activeKey: string | null;
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
    get activeKey() {
      return _activeKey;
    },
    get activeValue() {
      return _activeValue;
    },
  };
}
