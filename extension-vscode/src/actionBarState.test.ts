// The action bar's running set is what puts a Stop button on the bar
// and what a pending Stop waits on. These tests pin the reconciliation:
// a status fetch is the truth about what runs, so a run the fetch no
// longer names is gone even when its terminal event never arrived,
// and a Stop waiting on it is over.

import { describe, it, expect } from 'vitest';
import { ActionBarStore } from './actionBarState';
import type { ActionAvailability } from '../../packages/weft-graph/src/protocol';

function snapshot(): ActionAvailability {
  return {
    availableActions: ['run'],
    binaryDrift: false,
    definitionDrift: false,
    infraDrift: false,
    projectStatus: 'active',
    transition: 'none',
    orphanedInfra: false,
    mode: 'active',
    runningCount: 0,
    infraRollup: 'none',
    infraNodes: [],
    preservation: { parked: 0, suspended: 0 },
  };
}

describe('the running set follows the status fetch', () => {
  it('a refresh that no longer lists the pending color ends the Stop', () => {
    const store = new ActionBarStore();
    store.setPinnedProject('p');
    store.pushStatus('p', snapshot(), ['c1']);
    expect(store.watchedRunningColor('p')).toBe('c1');
    store.setPending('p', 'run', 'Cancelling...', 'c1');
    expect(store.current().overlay.kind).toBe('pending');
    // The terminal event was lost; the next fetch says nothing runs.
    store.pushStatus('p', snapshot(), []);
    expect(store.current().overlay.kind).toBe('idle');
    expect(store.watchedRunningColor('p')).toBeUndefined();
  });

  it('a refresh replaces the set and keeps the order of colors it still lists', () => {
    const store = new ActionBarStore();
    store.setPinnedProject('p');
    store.markExecutionStarted('p', 'old');
    store.markExecutionStarted('p', 'new');
    // Between fetches the set is built from arriving events, so the
    // last one added is genuinely the newest.
    expect(store.watchedRunningColor('p')).toBe('new');
    // `old` finished (event lost); `born` started while the stream was
    // down and is the newest, so the dispatcher lists it last.
    store.pushStatus('p', snapshot(), ['new', 'born']);
    // The fetch's order is the dispatcher's own, oldest first, so the
    // last one it names is the newest run. Note `new` was already known
    // here and `born` was not: taking the fetch's order wholesale is
    // what makes this right, where keeping the known ones in place
    // would have answered `new`.
    expect(store.watchedRunningColor('p')).toBe('born');
    store.markExecutionFinished('p', 'born');
    expect(store.watchedRunningColor('p')).toBe('new');
    store.pushStatus('p', snapshot(), []);
    expect(store.watchedRunningColor('p')).toBeUndefined();
  });

  it('a pending Stop on a color the fetch still lists keeps waiting', () => {
    const store = new ActionBarStore();
    store.setPinnedProject('p');
    store.pushStatus('p', snapshot(), ['c1']);
    store.setPending('p', 'run', 'Cancelling...', 'c1');
    store.pushStatus('p', snapshot(), ['c1']);
    expect(store.current().overlay.kind).toBe('pending');
    store.markExecutionFinished('p', 'c1');
    expect(store.current().overlay.kind).toBe('idle');
  });
});
