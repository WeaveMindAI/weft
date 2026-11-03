import { describe, it, expect } from 'vitest';
import { runProgress, runCheckKey, runTargetsOf, whyNothingToRun } from './run-targets';

describe('runTargetsOf', () => {
  const door = { place: 'door', isTrigger: true, isInfra: false };
  const clean = { place: 'clean', isTrigger: false, isInfra: false };
  const sub = { place: 'c.door', isTrigger: true, isInfra: false };
  const pg = { place: 'pg', isTrigger: false, isInfra: true };
  const included = { place: 's.db', isTrigger: false, isInfra: true };

  it('offers no Run for triggers alone: a trigger runs when it fires', () => {
    expect(runTargetsOf([door])).toEqual([]);
    expect(runTargetsOf([door, sub])).toEqual([]);
  });

  it('offers no Run for infra alone: infra is started, never run', () => {
    expect(runTargetsOf([pg])).toEqual([]);
    expect(runTargetsOf([pg, included, door])).toEqual([]);
  });

  it('runs the rest of a mixed selection, in the order selected', () => {
    expect(runTargetsOf([door, clean, sub])).toEqual([clean]);
    expect(runTargetsOf([pg, clean, included])).toEqual([clean]);
  });
});

describe('whyNothingToRun', () => {
  const trigger = { isTrigger: true, isInfra: false };
  const infra = { isTrigger: false, isInfra: true };

  it('says what to select when nothing is', () => {
    expect(whyNothingToRun([])).toMatch(/^Select the nodes to run first/);
  });

  it('says why each kind is left out', () => {
    expect(whyNothingToRun([trigger])).toMatch(/^A trigger runs when it fires/);
    expect(whyNothingToRun([infra])).toMatch(/^Infra is started, not run/);
    expect(whyNothingToRun([trigger, infra])).toMatch(/^Run leaves out triggers and infra/);
  });
});

describe('runProgress', () => {
  it('counts the places whose latest firing ended, out of the run\'s places', () => {
    const firings = {
      a: [{ status: 'completed' as const }],
      b: [{ status: 'completed' as const }, { status: 'running' as const }],
      c: [{ status: 'skipped' as const }],
      d: [{ status: 'waiting' as const }],
      e: [{ status: 'paused' as const }],
    };
    expect(runProgress(firings, ['a', 'b', 'c', 'd', 'e', 'f'])).toEqual({ done: 2, total: 6 });
  });

  it('has no total when the run\'s program could not be read', () => {
    expect(runProgress({}, null)).toEqual({ done: 0, total: null });
  });
});

describe('runCheckKey', () => {
  const src = 'a = Text\n';
  it('gives the same key for the same selection in a new array', () => {
    const selection = ['b', 'a.x'];
    expect(runCheckKey([...selection], src)).toBe(runCheckKey(selection, src));
    expect(runCheckKey(['a.x', 'b'], src)).toBe(runCheckKey(['b', 'a.x'], src));
  });

  it('gives another key when the selection changes', () => {
    expect(runCheckKey(['a', 'b'], src)).not.toBe(runCheckKey(['a'], src));
    expect(runCheckKey(['a', 'b'], src)).not.toBe(runCheckKey(['a', 'c'], src));
  });

  it('gives another key when the program changes under the same selection', () => {
    expect(runCheckKey(['a'], src)).not.toBe(runCheckKey(['a'], 'a = Text\nb = Text\n'));
  });

  it('cannot confuse two places with one whose name holds the separator', () => {
    expect(runCheckKey(['a,b'], src)).not.toBe(runCheckKey(['a', 'b'], src));
  });
});
