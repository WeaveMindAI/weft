import { describe, expect, it } from 'vitest';
import {
  exampleNameProblem,
  addressOf,
  groupOfCallPath,
  isFrozen,
  orderSpecsForMenu,
  parseRunSpec,
  parseSuppliedJson,
  specForAction,
  specScopedTo,
  specSummary,
  specToRunArgs,
  type RunSpec,
} from './run-spec';

describe('run spec', () => {
  it('refuses duplicate supplied JSON keys without confusing quoted braces or sibling objects', () => {
    for (const text of ['{"x":1,"x":2}', '{"rows":[{"x":1,"x":2}]}', '{"x":1,"\\u0078":2}']) {
      expect(() => parseSuppliedJson(text)).toThrow("duplicate key 'x'");
    }
    for (const value of [{ text: '"x":1, {[]}' }, [{ x: 1 }, { x: 2 }], { a: { x: 1 }, b: { x: 2 } }, null, 'hello', 42]) {
      expect(parseSuppliedJson(JSON.stringify(value))).toEqual(value);
    }
    expect(() => parseSuppliedJson('{"unfinished":')).toThrow();
  });
  it('rejects obsolete and malformed file shapes while preserving evidence', () => {
    for (const field of ['scope', 'kicks', 'provided', 'emitted', 'input']) {
      expect(() => parseRunSpec({ name: 'x', [field]: {} })).toThrow('unknown field');
    }
    expect(() => parseRunSpec({ name: 'x', from: { a: [] } })).toThrow('expected an object');
    expect(() => parseRunSpec({ name: 'x', from: ['a'] })).toThrow('expected an object');
    expect(() => parseRunSpec({ name: 'x', fire: ['a'] })).toThrow('expected [trigger, payload]');
    const spec = { name: 'x', before: ['send'], emit: { a: { rows: [1, 2] } },
      answers: [{ node: 'review', payload: 'yes', question: 'Continue?' }], caller: [{ text: 'hello' }] };
    expect(parseRunSpec(spec)).toEqual(spec);
  });
  it('turns a spec into the same flags a person would type', () => {
    const spec: RunSpec = {
      name: 'x',
      from: { classify: { text: 'hi' }, bare: {} }, target: ['reply'], before: ['send'],
      fire: ['inbound', { a: 1 }],
      emit: { bridge: { url: 'http://x' } },
    };
    expect(specToRunArgs(spec)).toEqual([
      '--from', 'classify={"text":"hi"}', '--from', 'bare',
      '--target', 'reply',
      '--before', 'send',
      '--fire', 'inbound={"a":1}',
      '--emit', 'bridge={"url":"http://x"}',
    ]);
    expect(specToRunArgs({ name: 'plain' })).toEqual([]);
    expect(specToRunArgs({ name: 'batch', group: ['batch', { items: [1, 2] }] })).toEqual(['--group', 'batch={"items":[1,2]}']);
    expect(specToRunArgs({ name: 'batch', group: ['batch', {}] })).toEqual(['--group', 'batch']);
    expect(specToRunArgs({ name: 'fed', from: { 'hear.note': {} }, feed: ['hear.note'] }))
      .toEqual(['--from', 'hear.note', '--feed', 'hear.note']);
    // The seed checkbox has to travel: the dialog resolves the spec WITH
    // seeding to decide it is runnable, so a run without `--seed` is
    // refused for an input the preview said was covered.
    expect(specToRunArgs({ name: 'plain' }, true)).toEqual(['--seed']);
  });

  it('summarises a spec for a menu', () => {
    expect(specSummary({ name: 'plain' })).toBe('whole graph');
    expect(specSummary({ name: 'g', from: { a: { p: 1 } }, expected: { wires: [] } }))
      .toBe('from a, 1 input backups, frozen');
    expect(isFrozen({ name: 'a', expected: { wires: [] } })).toBe(true);
  });

  it('reads the group the user stands in off the include prefix', () => {
    expect(groupOfCallPath([])).toBeNull();
    expect(groupOfCallPath(['c'])).toBe('c');
    expect(groupOfCallPath(['c', 'C.inner'])).toBe('c.inner');
    expect(groupOfCallPath(['c', 'C.billing.inner'])).toBe('c.billing.inner');
    expect(addressOf([], 'plain')).toBe('plain');
    expect(addressOf(['c'], 'C.strip')).toBe('c.strip');
    expect(addressOf(['c', 'C.inner'], 'Inner.deep')).toBe('c.inner.deep');
  });

  it('puts the specs scoped to the focused group first', () => {
    const specs: RunSpec[] = [
      { name: 'whole' },
      { name: 'b-inner', from: { 'triage.parse': {} } },
      { name: 'a-group', group: ['triage', {}] },
      { name: 'other', group: ['billing', {}] },
    ];
    expect(specScopedTo(specs[1], 'triage')).toBe(true);
    expect(specScopedTo(specs[3], 'triage')).toBe(false);
    expect(orderSpecsForMenu(specs, 'triage').map((s) => s.name)).toEqual(['a-group', 'b-inner', 'other', 'whole']);
    expect(orderSpecsForMenu(specs, null).map((s) => s.name)).toEqual(['a-group', 'b-inner', 'other', 'whole']);
  });

  it('opens the dialog with the node action filled in', () => {
    expect(specForAction('from', 'g.inner')).toEqual({ name: 'from-g-inner', from: { 'g.inner': {} } });
    expect(specForAction('group', 'triage')).toEqual({ name: 'group-triage', group: ['triage', {}] });
  });

  it('refuses an example name that is a path, and suggests the name meant', () => {
    // The rule the CLI applies, so the dialog can say no before it
    // closes: closing destroys every field the person typed.
    expect(exampleNameProblem('happy-path')).toBeUndefined();
    expect(exampleNameProblem('a name with spaces')).toBeUndefined();
    expect(exampleNameProblem('flows/happy')).toBe('an example is named, not a path: use `happy`');
    expect(exampleNameProblem('happy.json')).toBe('an example is named, not a path: use `happy`');
    // The suggestion survives the same rule: `...json` strips to `..`,
    // which is not a name either, so there is nothing to suggest.
    expect(exampleNameProblem('...json')).toBe('an example needs a name (a word, not a path or a directory)');
    expect(exampleNameProblem('')).toBe('an example needs a name (a word, not a path or a directory)');
    // A dash-leading name is read as a flag by `weft run <name>`, so it
    // is refused here rather than written and then unrunnable.
    expect(exampleNameProblem('-x')).toBe('an example cannot start with a dash');
  });
});
