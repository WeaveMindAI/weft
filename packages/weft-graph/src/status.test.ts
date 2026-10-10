import { describe, it, expect, vi, afterEach } from 'vitest';
import {
  describeProgress,
  emptyActionAvailability,
  parseRunning,
  parseStatusPayload,
  progressOnLocalClock,
} from './status';

describe('parseStatusPayload', () => {
  it('remaps snake_case wire fields to the camelCase snapshot', () => {
    const snap = parseStatusPayload({
      status: 'active',
      transition: 'building',
      infra_rollup: 'running',
      drift: { binary_drift: true, definition_drift: false, infra_drift: true },
      orphaned_infra: true,
      mode: 'active',
      running_count: 3,
      available_actions: ['deactivate'],
      fires_deadline_unix: 123,
      infra: [{ node: 'n1', node_type: 'pg', status: 'running' }],
      preservation: { parked: 2, suspended: 1 },
    });
    expect(snap.projectStatus).toBe('active');
    expect(snap.transition).toBe('building');
    expect(snap.infraRollup).toBe('running');
    expect(snap.binaryDrift).toBe(true);
    expect(snap.definitionDrift).toBe(false);
    expect(snap.infraDrift).toBe(true);
    expect(snap.orphanedInfra).toBe(true);
    expect(snap.runningCount).toBe(3);
    expect(snap.firesDeadlineUnix).toBe(123);
    expect(snap.infraNodes).toEqual([{ node: 'n1', nodeType: 'pg', status: 'running' }]);
    expect(snap.preservation).toEqual({ parked: 2, suspended: 1 });
  });

  it('names the program\'s own triggers that are off, never an instance\'s', () => {
    const snap = parseStatusPayload({
      status: 'active',
      activations: [
        { trigger: 'gmail.watch', status: 'inactive' },
        { trigger: 'daily', status: 'active' },
        { trigger: 'fresh', status: 'registered' },
        { trigger: 'gmail.watch', instance: 'ada', status: 'inactive' },
      ],
    });
    expect(snap.triggersOff).toEqual(['gmail.watch', 'fresh']);
    expect(parseStatusPayload({}).triggersOff).toEqual([]);
  });

  it('collapses unknown enum strings to their resting value (version skew, no crash)', () => {
    const snap = parseStatusPayload({
      status: 'some-future-status',
      transition: 'some-future-transition',
      infra_rollup: 'some-future-rollup',
    });
    expect(snap.projectStatus).toBe('unknown');
    expect(snap.transition).toBe('none');
    expect(snap.infraRollup).toBe('none');
  });

  it('fills defaults for an empty payload without throwing', () => {
    const snap = parseStatusPayload({});
    const empty = emptyActionAvailability();
    expect(snap.projectStatus).toBe(empty.projectStatus);
    expect(snap.transition).toBe(empty.transition);
    expect(snap.infraRollup).toBe(empty.infraRollup);
    expect(snap.runningCount).toBe(0);
    expect(snap.infraNodes).toEqual([]);
    expect(snap.firesDeadlineUnix).toBeUndefined();
  });
});

describe('parseRunning', () => {
  it('keeps the dispatcher order and each phase', () => {
    const running = parseRunning({
      executions: { running: [{ execution_id: 'a', phase: 'infra_setup' }, { execution_id: 'b', phase: 'fire' }] },
    });
    expect(running).toEqual([{ execution_id: 'a', phase: 'infra_setup' }, { execution_id: 'b', phase: 'fire' }]);
    expect(parseRunning({})).toEqual([]);
  });

  it('refuses a phase it does not know rather than showing it as a run', () => {
    expect(() => parseRunning({
      executions: { running: [{ execution_id: 'a', phase: 'mystery' as never }] },
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
      infra: [{ node: 'db', node_type: 'pg', status: 'provisioning', progress: { sinceUnix: 100, asOfUnix: 130 } }],
    });
    const progress = snap.infraNodes[0].progress!;
    expect(progress).toEqual({ sinceUnix: 8_970, asOfUnix: 9_000 });
    expect(describeProgress(progress, Date.now() / 1000)).toBe('for 30s');
  });
});
