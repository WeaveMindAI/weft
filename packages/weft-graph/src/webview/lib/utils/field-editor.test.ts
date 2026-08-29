// The field editor's write discipline: every save (debounce fire,
// blur, flush) goes through one commit that writes only when the value
// differs from the LAST WRITE. The orderings here are the ones that
// shipped bugs: a click-in-click-out must write nothing (an empty
// rendering would write a delete for a port literal), and
// type-autosave-revert must still write the revert on blur.

import { describe, it, expect, vi, beforeEach, afterEach } from 'vitest';
import { createFieldEditor } from './field-editor.svelte';

describe('createFieldEditor', () => {
	beforeEach(() => vi.useFakeTimers());
	afterEach(() => vi.useRealTimers());

	function rig() {
		const writes: string[] = [];
		const save = (v: string) => writes.push(v);
		const ed = createFieldEditor(100);
		return { writes, save, ed };
	}

	it('a click in and out writes nothing', () => {
		const { writes, save, ed } = rig();
		ed.focus('k', 'hello');
		ed.blur('k', save);
		expect(writes).toEqual([]);
	});

	it('type then blur writes once', () => {
		const { writes, save, ed } = rig();
		ed.focus('k', '');
		ed.input('a', 'k', save);
		ed.blur('k', save);
		expect(writes).toEqual(['a']);
	});

	it('type then revert before the debounce fires writes nothing', () => {
		// The debounce timer is a write path too; without the shared
		// guard it wrote the reverted (often empty) value.
		const { writes, save, ed } = rig();
		ed.focus('k', '');
		ed.input('a', 'k', save);
		ed.input('', 'k', save);
		vi.advanceTimersByTime(200);
		expect(writes).toEqual([]);
		ed.blur('k', save);
		expect(writes).toEqual([]);
	});

	it('type, autosave, then revert to the focus value still writes the revert', () => {
		// The guard compares against the last WRITE, not the focus
		// snapshot: after the autosave the store holds "hello world",
		// so deleting back to "hello" must write.
		const { writes, save, ed } = rig();
		ed.focus('k', 'hello');
		ed.input('hello world', 'k', save);
		vi.advanceTimersByTime(200);
		expect(writes).toEqual(['hello world']);
		ed.input('hello', 'k', save);
		ed.blur('k', save);
		expect(writes).toEqual(['hello world', 'hello']);
	});

	it('flush writes a pending change once and skips a reverted one', () => {
		const { writes, save, ed } = rig();
		ed.focus('k', 'x');
		ed.input('xy', 'k', save);
		ed.flush();
		expect(writes).toEqual(['xy']);
		ed.input('xy', 'k', save);
		ed.flush();
		expect(writes).toEqual(['xy']);
	});
});
