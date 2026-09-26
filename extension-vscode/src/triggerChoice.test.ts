import { describe, expect, it } from 'vitest';
import { isTriggerChoiceRefusal, triggerChoiceIntent } from './triggerChoice';

describe('isTriggerChoiceRefusal', () => {
  it('recognizes the flagged error event', () => {
    expect(isTriggerChoiceRefusal({
      ts_unix: 1,
      verb: 'infra_stop',
      phase: 'error',
      detail: { message: 'triggers are on', daemonUnreachable: false, needsTriggerChoice: true },
    })).toBe(true);
  });

  it('ignores any other failure, and the flag on a non-error phase', () => {
    expect(isTriggerChoiceRefusal({
      ts_unix: 1,
      phase: 'error',
      detail: { message: 'project not registered', needsTriggerChoice: false },
    })).toBe(false);
    expect(isTriggerChoiceRefusal({ ts_unix: 1, phase: 'error' })).toBe(false);
    expect(isTriggerChoiceRefusal({
      ts_unix: 1,
      phase: 'warning',
      detail: { needsTriggerChoice: true },
    })).toBe(false);
  });
});

describe('triggerChoiceIntent', () => {
  it('maps each verb with a picker to its intent', () => {
    expect(triggerChoiceIntent('resync')).toBe('resync');
    expect(triggerChoiceIntent('infra_stop')).toBe('infraStop');
    expect(triggerChoiceIntent('infra_terminate')).toBe('infraTerminate');
    expect(triggerChoiceIntent('infra_upgrade')).toBe('infraUpgrade');
  });

  it('refuses a verb with no picker', () => {
    expect(() => triggerChoiceIntent('run')).toThrow(/no trigger-choice picker/);
  });
});
