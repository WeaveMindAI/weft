// The search/replace block locator: the replaced range must be exactly the
// text that matched, never more.
import { describe, it, expect } from 'vitest';
import { findBlockRange } from './streamingEdits';

describe('findBlockRange', () => {
  it('an exact match replaces exactly the search text', () => {
    expect(findBlockRange('abc def ghi', 'def')).toEqual({ start: 4, end: 7 });
  });

  it('a trimmed fallback match covers only the trimmed text, never past it', () => {
    const text = 'x = 1\ny = 2\nz = 3\n';
    const range = findBlockRange(text, '   y = 2   \n\n')!;
    expect(text.slice(range.start, range.end)).toBe('y = 2');
  });

  it('no match, or an all-whitespace search, finds nothing', () => {
    expect(findBlockRange('abc', 'zzz')).toBeNull();
    expect(findBlockRange('abc', '   ')).toBeNull();
  });
});
