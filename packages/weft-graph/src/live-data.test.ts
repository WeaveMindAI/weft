import { describe, it, expect } from 'vitest';
import { isLiveDataItem, signalDisplayToLiveItems } from './live-data';

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

describe('signalDisplayToLiveItems', () => {
  it('renders a public-entry route (root when empty, leading slash normalized, methods in front)', () => {
    expect(signalDisplayToLiveItems({ surface: { kind: 'public_entry', path: '' } })[0]).toEqual({
      type: 'text',
      label: 'Route',
      data: '/',
    });
    expect(signalDisplayToLiveItems({ surface: { kind: 'public_entry', path: '/hook' } })[0]).toEqual(
      { type: 'text', label: 'Route', data: '/hook' },
    );
    expect(
      signalDisplayToLiveItems({
        surface: { kind: 'public_entry', path: 'chat/{room}', methods: ['POST'] },
      })[0],
    ).toEqual({ type: 'text', label: 'Route', data: 'POST /chat/{room}' });
  });

  it('renders auth mode on a public entry (open vs gated by a connection)', () => {
    const entry = { kind: 'public_entry', path: '/hook' };
    const none = signalDisplayToLiveItems({ surface: entry, auth: { kind: 'none' } });
    expect(none).toContainEqual({ type: 'text', label: 'Auth', data: 'open (anyone with the URL)' });
    const gated = signalDisplayToLiveItems({ surface: entry, auth: { kind: 'connection' } });
    expect(gated).toContainEqual({
      type: 'text',
      label: 'Auth',
      data: 'gated by the wired auth connection',
    });
  });

  it('emits nothing without a public entry (internal/task-callback routing is not news)', () => {
    expect(signalDisplayToLiveItems({})).toEqual([]);
    expect(signalDisplayToLiveItems({ auth: { kind: 'none' } })).toEqual([]);
    expect(
      signalDisplayToLiveItems({ surface: { kind: 'internal' }, auth: { kind: 'none' } }),
    ).toEqual([]);
    expect(
      signalDisplayToLiveItems({ surface: { kind: 'task_callback' }, auth: { kind: 'connection' } }),
    ).toEqual([]);
  });
});
