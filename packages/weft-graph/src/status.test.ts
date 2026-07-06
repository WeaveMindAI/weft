import { describe, it, expect } from 'vitest';
import { parseStatusPayload, emptyActionAvailability } from './status';

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
      infra: [{ node_id: 'n1', node_type: 'pg', status: 'running' }],
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
    expect(snap.infraNodes).toEqual([{ nodeId: 'n1', nodeType: 'pg', status: 'running' }]);
    expect(snap.preservation).toEqual({ parked: 2, suspended: 1 });
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
