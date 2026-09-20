import { describe, it, expect } from 'vitest';
import { isLiveDataItem } from './live-data';

describe('isLiveDataItem', () => {
  it('accepts a well-formed item', () => {
    expect(isLiveDataItem({ type: 'text', label: 'Path', data: '/hook' })).toBe(true);
    expect(isLiveDataItem({ type: 'progress', label: 'p', data: 42 })).toBe(true);
  });

  it('rejects non-objects and missing/mistyped fields', () => {
    expect(isLiveDataItem(null)).toBe(false);
    expect(isLiveDataItem('nope')).toBe(false);
    expect(isLiveDataItem({ label: 'x', data: 'y' })).toBe(false); // no type
    expect(isLiveDataItem({ type: 'nope', label: 'x', data: 'y' })).toBe(false); // bad type
    expect(isLiveDataItem({ type: 'text', label: 1, data: 'y' })).toBe(false); // label not string
    expect(isLiveDataItem({ type: 'text', label: 'x', data: {} })).toBe(false); // data not string|number
  });

  it('validates an optional action shape', () => {
    expect(
      isLiveDataItem({ type: 'text', label: 'x', data: 'y', action: { label: 'go', actionKind: 'k' } }),
    ).toBe(true);
    expect(isLiveDataItem({ type: 'text', label: 'x', data: 'y', action: { label: 'go' } })).toBe(
      false,
    );
  });
});
