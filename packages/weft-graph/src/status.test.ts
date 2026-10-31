import { describe, it, expect, vi, afterEach } from 'vitest';
import {
  backendFromSnapshot,
  describeProgress,
  describeRestart,
  emptyActionAvailability,
  parseRunning,
  parseStatusPayload,
  progressOnLocalClock,
} from './status';

describe('parseStatusPayload', () => {
  it('reads the project-wide fields', () => {
    const snap = parseStatusPayload({
      status: 'active',
      transition: 'building',
      infraRollup: 'running',
      orphanedInfra: true,
      availableActions: ['deactivate'],
      infra: [{ node: 'n1', nodeType: 'pg', status: 'running' }],
    });
    expect(snap.projectStatus).toBe('active');
    expect(snap.transition).toBe('building');
    expect(snap.infraRollup).toBe('running');
    expect(snap.orphanedInfra).toBe(true);
    expect(snap.availableActions).toEqual(['deactivate']);
    expect(snap.infraNodes).toEqual([{ node: 'n1', nodeType: 'pg', status: 'running' }]);
  });

  it('carries every trigger\'s, infra copy\'s and instance\'s state as the dispatcher computed it', () => {
    const snap = parseStatusPayload({
      status: 'active',
      infra: [
        {
          node: 'db',
          nodeType: 'pg',
          status: 'running',
          behind: true,
          takesDown: [
            { owner: { kind: 'shared' }, triggers: ['door'] },
            { owner: { kind: 'instance', instance: 'ada' }, triggers: ['inbox'] },
          ],
        },
        { node: 'box', nodeType: 'pg', status: 'per_instance', instanceCopyCount: 1 },
      ],
      instanceInfra: [{ node: 'box', instance: 'ada', status: 'stopped', behind: true }],
      triggers: [
        { node: 'door', perInstance: false, shared: { state: 'behind', mode: 'active', version: 'v4' }, dependsOn: ['db'] },
        {
          node: 'inbox',
          perInstance: true,
          copies: [{ instance: 'ada', state: 'off', mode: 'take', held: { arrivals: 3, paused: 1 } }],
        },
      ],
      instances: [{ instance: 'ada', values: 2, connections: 1, tokens: 0, copies: [], triggers: [], lastRunUnix: 99 }],
    });
    expect(snap.infraNodes[0]).toEqual({
      node: 'db',
      nodeType: 'pg',
      status: 'running',
      behind: true,
      takesDown: [
        { owner: { kind: 'shared' }, triggers: ['door'] },
        { owner: { kind: 'instance', instance: 'ada' }, triggers: ['inbox'] },
      ],
    });
    expect(snap.instanceInfra).toEqual([{ node: 'box', instance: 'ada', status: 'stopped', behind: true }]);
    expect(snap.triggers.map((t) => t.node)).toEqual(['door', 'inbox']);
    expect(snap.triggers[0].shared?.state).toBe('behind');
    expect(snap.triggers[1].copies?.[0].held).toEqual({ arrivals: 3, paused: 1 });
    expect(snap.instances.map((i) => i.instance)).toEqual(['ada']);
  });

  it('refuses a trigger state it does not know rather than drawing it', () => {
    expect(() => parseStatusPayload({
      triggers: [{ node: 'door', perInstance: false, shared: { state: 'mystery' as never, mode: 'active' } }],
    })).toThrow(/cannot read/);
  });

  it('collapses unknown enum strings to their resting value (version skew, no crash)', () => {
    const snap = parseStatusPayload({
      status: 'some-future-status',
      transition: 'some-future-transition',
      infraRollup: 'some-future-rollup',
    });
    expect(snap.projectStatus).toBe('unknown');
    expect(snap.transition).toBe('none');
    expect(snap.infraRollup).toBe('none');
  });

  it('carries a project being removed onto the bar', () => {
    const snap = parseStatusPayload({ removing: true, status: 'inactive', availableActions: [] });
    expect(snap.removing).toBe(true);
    expect(backendFromSnapshot(snap).removing).toBe(true);
    expect(parseStatusPayload({}).removing).toBe(false);
  });

  it('fills defaults for an empty payload without throwing', () => {
    const snap = parseStatusPayload({});
    const empty = emptyActionAvailability();
    expect(snap.projectStatus).toBe(empty.projectStatus);
    expect(snap.transition).toBe(empty.transition);
    expect(snap.infraRollup).toBe(empty.infraRollup);
    expect(snap.infraNodes).toEqual([]);
    expect(snap.triggers).toEqual([]);
    expect(snap.instances).toEqual([]);
  });
});

describe('parseRunning', () => {
  it('keeps the dispatcher order and each phase', () => {
    const running = parseRunning({
      runs: { running: [{ runId: 'a', phase: 'infra_setup' }, { runId: 'b', phase: 'fire' }] },
    });
    expect(running).toEqual([{ runId: 'a', phase: 'infra_setup' }, { runId: 'b', phase: 'fire' }]);
    expect(parseRunning({})).toEqual([]);
  });

  it('refuses a phase it does not know rather than showing it as a run', () => {
    expect(() => parseRunning({
      runs: { running: [{ runId: 'a', phase: 'mystery' as never }] },
    })).toThrow(/cannot read/);
  });
});

describe('infra progress', () => {
  afterEach(() => {
    vi.useRealTimers();
  });

  it('counts the server\'s duration, then ticks on this machine\'s clock', () => {
    // The server says 20s had passed; this machine's clock runs 500s ahead.
    const local = progressOnLocalClock({ sinceUnix: 1_000, asOfUnix: 1_020, waiting: 'db' }, 1_520);
    expect(local).toEqual({ sinceUnix: 1_500, asOfUnix: 1_520, waiting: 'db' });
    expect(describeProgress(local, 1_520)).toBe('for 20s, waiting on: db');
    expect(describeProgress(local, 1_565)).toBe('for 1m05s, waiting on: db');
  });

  it('moves a status answer\'s progress onto this machine\'s clock as it arrives', () => {
    vi.useFakeTimers();
    vi.setSystemTime(9_000_000);
    const snap = parseStatusPayload({
      infra: [{ node: 'db', nodeType: 'pg', status: 'provisioning', progress: { sinceUnix: 100, asOfUnix: 130 } }],
    });
    const progress = snap.infraNodes[0].progress!;
    expect(progress).toEqual({ sinceUnix: 8_970, asOfUnix: 9_000 });
    expect(describeProgress(progress, Date.now() / 1000)).toBe('for 30s');
  });

  it('moves a trigger turning on onto this machine\'s clock too', () => {
    vi.useFakeTimers();
    vi.setSystemTime(9_000_000);
    const turningOn = { state: 'turning_on' as const, mode: 'registered' as const, progress: { sinceUnix: 100, asOfUnix: 340, waiting: 'its setup run r1' } };
    const snap = parseStatusPayload({ triggers: [{ node: 'door', perInstance: false, shared: turningOn }] });
    const progress = snap.triggers[0].shared!.progress!;
    expect(progress).toEqual({ sinceUnix: 8_760, asOfUnix: 9_000, waiting: 'its setup run r1' });
    expect(describeProgress(progress, progress.asOfUnix)).toBe('for 4m00s, waiting on: its setup run r1');
  });
});

describe('infra restarts', () => {
  // 2026-10-27 14:03:20 UTC; the same words `LastRestart::describe` writes.
  const atUnix = 1_793_109_800;

  it('says what happened, when, and what to do about it', () => {
    expect(describeRestart({ atUnix, cause: { kind: 'out_of_memory' } }, 'one.db')).toBe(
      'one.db ran out of memory at 2026-10-27 14:03 UTC: a process in it was killed because it needs more memory than its container may use; give it more',
    );
    expect(describeRestart({ atUnix, cause: { kind: 'exited', code: 137 } }, 'one.db')).toBe(
      'one.db stopped (exit code 137) and was restarted at 2026-10-27 14:03 UTC',
    );
    expect(describeRestart({ atUnix, cause: { kind: 'unknown' } }, 'one.db')).toBe(
      'one.db stopped and was restarted at 2026-10-27 14:03 UTC, for a reason its host no longer knows',
    );
  });

  it('carries a copy\'s last restart from the status answer', () => {
    const lastRestart = { atUnix, cause: { kind: 'out_of_memory' as const } };
    const snap = parseStatusPayload({ infra: [{ node: 'db', nodeType: 'pg', status: 'running', lastRestart }] });
    expect(snap.infraNodes[0].lastRestart).toEqual(lastRestart);
  });
});
