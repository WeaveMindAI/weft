import { describe, expect, it } from 'vitest';

import { fileFieldDisplay, fileFieldNotice, fileFieldReadonly, fileFieldSave, fileFieldState, flipFileMarker, textFileRefOf } from './file-field';

const marker = (path: string, type = 'String', kind: 'file' | 'asset' = 'file') => ({ __weftFileRef: { path, type, marker: kind } });

describe('file-backed fields', () => {
  it('shows and edits a loaded @file through its content', () => {
    const literals = { prompt: marker('prompt.txt') };
    const state = fileFieldState(literals, { 'prompt.txt': { content: 'hello' } }, 'prompt');
    expect(fileFieldDisplay(state)).toBe('hello');
    expect(fileFieldReadonly(state)).toBe(false);
    expect(fileFieldSave(state!, 'moved')).toEqual({ path: 'prompt.txt', content: 'moved' });
    expect(fileFieldSave(state!, null)).toEqual({ path: 'prompt.txt', content: '' });
  });

  it('is read-only while loading, and replaces the control on a read error', () => {
    const literals = { prompt: marker('prompt.txt') };
    const loading = fileFieldState(literals, {}, 'prompt');
    expect(fileFieldDisplay(loading)).toBe('loading prompt.txt...');
    expect(fileFieldReadonly(loading)).toBe(true);
    expect(fileFieldSave(loading!, 'x')).toHaveProperty('error');
    const failed = fileFieldState(literals, { 'prompt.txt': { error: 'not found' } }, 'prompt');
    expect(fileFieldNotice(failed)).toBe('cannot read prompt.txt: not found');
  });

  it('never writes back through an @asset', () => {
    const state = fileFieldState({ p: marker('p.txt', 'String', 'asset') }, { 'p.txt': { content: 'a' } }, 'p');
    expect(fileFieldReadonly(state)).toBe(true);
    expect(fileFieldSave(state!, 'b')).toHaveProperty('error');
  });

  it('leaves a file-typed asset and a plain value to the field itself', () => {
    expect(textFileRefOf({ img: marker('logo.png', 'Image', 'asset') }, 'img')).toBeNull();
    expect(fileFieldState({ tone: 'formal' }, {}, 'tone')).toBeNull();
  });

  it('flips @file and @asset, but not a remote source', () => {
    const flipped = flipFileMarker({ p: marker('p.txt') }, 'p');
    expect(flipped).toEqual({ portLiterals: { p: marker('p.txt', 'String', 'asset') } });
    expect(flipFileMarker({ p: marker('https://example.com/a.txt', 'String', 'asset') }, 'p')).toHaveProperty('error');
    expect(flipFileMarker({ p: 'plain' }, 'p')).toBeNull();
  });
});
