// The action bar's running set is what puts a Stop button on the bar
// and what a pending Stop waits on. These tests pin the reconciliation:
// a status fetch is the truth about what runs, so a run the fetch no
// longer names is gone even when its terminal event never arrived,
// and a Stop waiting on it is over.

import { describe, it, expect } from 'vitest';
import { ActionBarStore } from './actionBarState';
import {
  ACTIVITY_LINE_CAP,
  type ActionAvailability,
} from '../../packages/weft-graph/src/protocol';

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

const PROJECT = 'p1';

function pinnedStore(): ActionBarStore {
  const store = new ActionBarStore();
  store.setPinnedProject(PROJECT);
  return store;
}

describe('the terminal output the bar carries', () => {
  it('collects what the running verb prints, oldest line first', () => {
    const store = pinnedStore();
    store.cliStart(PROJECT, 'activate');
    store.cliLog(PROJECT, 'activate', '> weft --json activate  (/p)\n');
    store.cliLog(PROJECT, 'activate', 'Compiling weft-core\nCompi');
    store.cliLog(PROJECT, 'activate', 'ling weft-engine\n');
    expect(store.current().activity).toEqual({
      verb: 'activate',
      lines: [
        '> weft --json activate  (/p)',
        'Compiling weft-core',
        // The chunk boundary fell mid-line; the tail arrives as its own
        // line, which is what a terminal shows too.
        'Compi',
        'ling weft-engine',
      ],
    });
  });

  it('cuts a docker-style carriage-return repaint into lines', () => {
    const store = pinnedStore();
    store.cliStart(PROJECT, 'infra_start');
    store.cliLog(PROJECT, 'infra_start', '#1 [1/4] FROM node\r#1 [2/4] COPY .\r\n');
    expect(store.current().activity?.lines).toEqual([
      '#1 [1/4] FROM node',
      '#1 [2/4] COPY .',
    ]);
  });

  it('keeps the tail once a build has printed more than the cap', () => {
    const store = pinnedStore();
    store.cliStart(PROJECT, 'run');
    for (let i = 0; i < ACTIVITY_LINE_CAP + 50; i += 1) {
      store.cliLog(PROJECT, 'run', `line ${i}\n`);
    }
    const lines = store.current().activity!.lines;
    expect(lines).toHaveLength(ACTIVITY_LINE_CAP);
    expect(lines[0]).toBe('line 50');
    expect(lines[lines.length - 1]).toBe(`line ${ACTIVITY_LINE_CAP + 49}`);
  });

  it('ignores a completion or an error from a verb that is no longer the one on the bar', () => {
    // Every event is verb-gated now (the host stamps the verb it
    // spawned onto verb-less ones), so a late word from a verb the
    // bar moved on from cannot stop the running verb's spinner or
    // hang an error under it.
    const store = pinnedStore();
    store.cliStart(PROJECT, 'activate');
    store.cliStart(PROJECT, 'run');
    store.cliEvent(PROJECT, { ts_unix: 0, verb: 'activate', phase: 'complete' });
    expect(store.current().overlay.kind).toBe('cli_running');
    store.cliEvent(PROJECT, {
      ts_unix: 0,
      verb: 'activate',
      phase: 'error',
      detail: { message: 'too late' },
    });
    expect(store.current().error).toBeUndefined();
    expect(store.current().overlay).toMatchObject({ kind: 'cli_running', verb: 'run' });
    // The running verb's own completion still lands.
    store.cliEvent(PROJECT, { ts_unix: 0, verb: 'run', phase: 'complete' });
    expect(store.current().overlay.kind).toBe('idle');
  });

  it('ignores a chunk from a verb that is no longer the one on the bar', () => {
    const store = pinnedStore();
    store.cliStart(PROJECT, 'activate');
    store.cliStart(PROJECT, 'run');
    store.cliLog(PROJECT, 'activate', 'late chunk from the killed child\n');
    expect(store.current().activity).toBeUndefined();
  });

  it('drops the output when the verb completes cleanly', () => {
    const store = pinnedStore();
    store.cliStart(PROJECT, 'activate');
    store.cliLog(PROJECT, 'activate', 'Compiling weft-core\n');
    store.cliEvent(PROJECT, { ts_unix: 0, verb: 'activate', phase: 'complete' });
    expect(store.current().activity).toBeUndefined();
  });

  it('keeps the output under the error when the verb fails', () => {
    const store = pinnedStore();
    store.cliStart(PROJECT, 'activate');
    store.cliLog(PROJECT, 'activate', 'error[E0432]: unresolved import\n');
    store.cliEvent(PROJECT, {
      ts_unix: 0,
      verb: 'activate',
      phase: 'error',
      detail: { message: 'the worker image did not build' },
    });
    const state = store.current();
    expect(state.error?.message).toBe('the worker image did not build');
    expect(state.activity?.lines).toEqual(['error[E0432]: unresolved import']);
  });

  it('drops the output with the error the user dismisses', () => {
    const store = pinnedStore();
    store.cliStart(PROJECT, 'activate');
    store.cliLog(PROJECT, 'activate', 'error[E0432]: unresolved import\n');
    store.cliCrashed(PROJECT, 'activate', 'weft activate exited 1');
    store.clearError(PROJECT);
    const state = store.current();
    expect(state.error).toBeUndefined();
    expect(state.activity).toBeUndefined();
  });

  it('drops the output when the user stops the verb', () => {
    const store = pinnedStore();
    store.cliStart(PROJECT, 'run');
    store.cliLog(PROJECT, 'run', 'Compiling weft-core\n');
    store.cliKilled(PROJECT, 'run');
    expect(store.current().activity).toBeUndefined();
  });

  it('keeps the running verb when a stop lands for the one before it', () => {
    // The user stopped `activate`, then started `run` before the
    // killed child's rejection made it back. The stop belongs to the
    // verb it was aimed at, so the run keeps its spinner and its log.
    const store = pinnedStore();
    store.cliStart(PROJECT, 'activate');
    store.cliStart(PROJECT, 'run');
    store.cliLog(PROJECT, 'run', 'Compiling weft-core\n');
    store.cliKilled(PROJECT, 'activate');
    const state = store.current();
    expect(state.overlay.kind).toBe('cli_running');
    expect(state.activity?.verb).toBe('run');
  });

  it('keeps the running verb when the one before it crashes', () => {
    const store = pinnedStore();
    store.cliStart(PROJECT, 'activate');
    store.cliStart(PROJECT, 'run');
    store.cliLog(PROJECT, 'run', 'Compiling weft-core\n');
    store.cliCrashed(PROJECT, 'activate', 'weft activate exited 1');
    const state = store.current();
    expect(state.overlay.kind).toBe('cli_running');
    expect(state.error).toBeUndefined();
    expect(state.activity?.verb).toBe('run');
  });

  it('keeps a running verb\'s output when an unrelated banner is dismissed', () => {
    // A parse error goes up on a half-typed edit WHILE a build runs,
    // and the user waves it away. The build is still going, so its log
    // is still the thing the details modal is for.
    const store = pinnedStore();
    store.cliStart(PROJECT, 'activate');
    store.cliLog(PROJECT, 'activate', 'Compiling weft-core\n');
    store.setError(PROJECT, 'parse', 'unexpected token');
    store.clearError(PROJECT);
    const state = store.current();
    expect(state.error).toBeUndefined();
    expect(state.activity?.lines).toEqual(['Compiling weft-core']);
    // And it keeps collecting.
    store.cliLog(PROJECT, 'activate', 'Compiling weft-engine\n');
    expect(store.current().activity?.lines).toHaveLength(2);
  });

  it('carries nothing while a verb has printed only blank lines', () => {
    const store = pinnedStore();
    store.cliStart(PROJECT, 'run');
    store.cliLog(PROJECT, 'run', '\n   \n');
    expect(store.current().activity).toBeUndefined();
  });
});
