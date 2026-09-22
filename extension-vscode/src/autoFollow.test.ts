// AutoFollowController decides which execution the graph view is streaming.
// Its rules are all about ordering (a run the user started, versus one a
// trigger opened while they were reading an old one), so the tests drive it
// through event sequences and read back what it told the webview.
//
// The follower is faked with an append-only call log: the controller's only
// outputs are the calls it makes on the follower and the status messages it
// posts, so those two logs are the whole observable surface.

import { describe, it, expect } from 'vitest';
import { AutoFollowController, type FollowStatus } from './autoFollow';
import type { DispatcherEvent, ExecutionFollower } from './execFollower';
import type { HostMessage } from '../../packages/weft-graph/src/protocol';

type Call = { verb: 'replay' | 'stop'; color?: string };

/// The workspace state, as a plain map the tests can read back.
function memoryOf(seed: Record<string, unknown> = {}) {
  const map = new Map<string, unknown>(Object.entries(seed));
  return {
    map,
    get: (key: string) => map.get(key),
    update: async (key: string, value: unknown) => { map.set(key, value); },
  };
}

function rig(seed: Record<string, unknown> = {}) {
  const calls: Call[] = [];
  const posted: FollowStatus[] = [];
  // Every other message the controller posts to the webview, by kind.
  const otherPosts: string[] = [];

  // Typed as the surface the controller actually uses, so a signature
  // change on the real follower fails this file at compile time; the
  // one cast is at the constructor below.
  const follower: Pick<ExecutionFollower, 'replay' | 'stop'> = {
    replay: async (color: string) => { calls.push({ verb: 'replay', color }); },
    stop: () => { calls.push({ verb: 'stop' }); },
  };

  const post = (msg: HostMessage) => {
    if (msg.kind === 'followStatus') posted.push(msg.status as FollowStatus);
    else otherPosts.push(msg.kind);
  };

  const memory = memoryOf(seed);
  const actionable: DispatcherEvent[] = [];
  const c = new AutoFollowController(
    follower as ExecutionFollower,
    post,
    memory,
    (ev) => actionable.push(ev),
  );
  return { c, calls, posted, otherPosts, actionable, memory, latest: () => posted[posted.length - 1] };
}

function started(color: string): DispatcherEvent {
  return { kind: 'execution_started', color, entry_node: 'n', project_id: 'p' };
}

describe('following', () => {
  it('is the mode a project starts in', () => {
    const { c, latest } = rig();
    c.setProject('p');
    expect(latest()).toEqual({ mode: 'following', color: undefined, pendingCount: 0 });
  });

  it('jumps to a new execution and replays it', () => {
    const { c, calls, latest } = rig();
    c.handleEvent(started('a'));
    expect(calls).toEqual([{ verb: 'replay', color: 'a' }]);
    expect(latest()).toEqual({ mode: 'following', color: 'a', pendingCount: 0 });
  });

  it('does not replay twice when the run command already followed the color', () => {
    const { c, calls } = rig();
    c.followStartedByUser('a');    // the /run response
    c.handleEvent(started('a'));   // the same start, echoed on the project stream
    expect(calls).toEqual([{ verb: 'replay', color: 'a' }]);
  });

  it('picks up the next execution when the caller did not know the color', () => {
    const { c, calls, latest } = rig();
    c.followStartedByUser(undefined); // activate: the color arrives over SSE
    c.handleEvent(started('a'));
    expect(calls).toEqual([{ verb: 'replay', color: 'a' }]);
    expect(latest().color).toBe('a');
  });
});

describe('locked', () => {
  it('queues background executions instead of stealing focus', () => {
    const { c, calls, latest } = rig();
    c.lockTo('old');
    c.handleEvent(started('a'));
    c.handleEvent(started('b'));

    expect(calls).toEqual([{ verb: 'replay', color: 'old' }]);
    expect(latest()).toEqual({ mode: 'locked', color: 'old', pendingCount: 2 });
  });

  it('locks the execution on screen from the toggle', () => {
    const { c, latest } = rig();
    c.handleEvent(started('a'));
    c.setMode('locked');
    expect(latest()).toEqual({ mode: 'locked', color: 'a', pendingCount: 0 });
  });

  it('cannot be picked with nothing on screen to hold', () => {
    const { c, posted } = rig();
    c.setMode('locked');
    expect(posted).toEqual([]);
  });

  it('becomes off when the locked run is deleted', () => {
    const { c, calls, otherPosts, latest } = rig();
    c.lockTo('old');
    c.stopShowing();
    expect(calls.at(-1)).toEqual({ verb: 'stop' });
    expect(otherPosts).toEqual(['execCleared']);
    expect(latest()).toEqual({ mode: 'off', color: undefined, pendingCount: 0 });
  });
});

describe('off', () => {
  it('clears the canvas and counts runs that start without showing them', () => {
    const { c, calls, otherPosts, latest } = rig();
    c.handleEvent(started('a'));
    c.setMode('off');
    c.handleEvent(started('b'));
    c.handleEvent(started('c'));
    expect(calls).toEqual([{ verb: 'replay', color: 'a' }, { verb: 'stop' }]);
    expect(otherPosts).toEqual(['execCleared']);
    expect(latest()).toEqual({ mode: 'off', color: undefined, pendingCount: 2 });
  });

  it('re-picking off keeps the count', () => {
    const { c, latest } = rig();
    c.setMode('off');
    c.handleEvent(started('a'));
    c.setMode('off');
    expect(latest().pendingCount).toBe(1);
  });

  it('a run the person starts from the editor follows again', () => {
    const { c, calls, latest } = rig();
    c.setMode('off');
    c.followStartedByUser('mine');
    expect(calls.at(-1)).toEqual({ verb: 'replay', color: 'mine' });
    expect(latest()).toEqual({ mode: 'following', color: 'mine', pendingCount: 0 });
  });
});

describe('following again', () => {
  it('jumps to the newest run that started meanwhile', () => {
    const { c, calls, latest } = rig();
    c.lockTo('old');
    c.handleEvent(started('a'));
    c.handleEvent(started('b'));
    c.setMode('following');

    expect(calls.at(-1)).toEqual({ verb: 'replay', color: 'b' });
    expect(latest()).toEqual({ mode: 'following', color: 'b', pendingCount: 0 });
  });

  it('with nothing queued keeps the run on screen', () => {
    const { c, latest } = rig();
    c.lockTo('old');
    c.setMode('following');
    expect(latest()).toEqual({ mode: 'following', color: 'old', pendingCount: 0 });
  });

  it('from off with nothing queued waits for the next run', () => {
    const { c, calls, latest } = rig();
    c.setMode('off');
    c.setMode('following');
    expect(latest()).toEqual({ mode: 'following', color: undefined, pendingCount: 0 });
    c.handleEvent(started('fresh'));
    expect(calls.at(-1)).toEqual({ verb: 'replay', color: 'fresh' });
  });

  it('keeps following when the run on screen is deleted', () => {
    const { c, calls, latest } = rig();
    c.handleEvent(started('a'));
    c.stopShowing();
    expect(latest()).toEqual({ mode: 'following', color: undefined, pendingCount: 0 });
    c.handleEvent(started('b'));
    expect(calls.at(-1)).toEqual({ verb: 'replay', color: 'b' });
  });
});

describe('remembering the choice per project', () => {
  it('a project left off opens off', () => {
    const first = rig();
    first.c.setProject('p');
    first.c.setMode('off');
    const again = rig(Object.fromEntries(first.memory.map));
    again.c.setProject('p');
    expect(again.latest().mode).toBe('off');
  });

  it('a project left locked opens off: the run it held is not on screen', () => {
    const first = rig();
    first.c.setProject('p');
    first.c.lockTo('old');
    const again = rig(Object.fromEntries(first.memory.map));
    again.c.setProject('p');
    expect(again.latest().mode).toBe('off');
  });

  it('another project keeps its own choice', () => {
    const { c, latest } = rig();
    c.setProject('p');
    c.setMode('off');
    c.setProject('q');
    expect(latest().mode).toBe('following');
    c.setProject('p');
    expect(latest().mode).toBe('off');
  });

  it('switching project drops what was on screen', () => {
    const { c, calls, latest } = rig();
    c.setProject('p');
    c.lockTo('old');
    c.handleEvent(started('a'));
    c.setProject('q');
    expect(calls.at(-1)).toEqual({ verb: 'stop' });
    expect(latest()).toEqual({ mode: 'following', color: undefined, pendingCount: 0 });
    expect(c.currentColor()).toBeUndefined();
  });
});

describe('reconnect', () => {
  it('jumps to the running execution it missed', () => {
    const { c, calls, latest } = rig();
    c.handleReconnect('a');
    expect(calls).toEqual([{ verb: 'replay', color: 'a' }]);
    expect(latest().color).toBe('a');
  });

  it('says nothing when already following that execution', () => {
    const { c, posted } = rig();
    c.handleEvent(started('a'));
    const before = posted.length;
    c.handleReconnect('a');
    expect(posted.length).toBe(before);
  });

  it('queues it while locked', () => {
    const { c, latest } = rig();
    c.lockTo('old');
    c.handleReconnect('a');
    expect(latest()).toEqual({ mode: 'locked', color: 'old', pendingCount: 1 });
  });

  it('does not queue the same execution twice', () => {
    const { c, latest } = rig();
    c.lockTo('old');
    c.handleReconnect('a');
    c.handleReconnect('a');
    expect(latest().pendingCount).toBe(1);
  });

  it('does not double-count a reconnect resync and its backlogged start', () => {
    // The reconnect names the running color, then the reconnected
    // stream delivers the same color's execution_started off the
    // backlog: one run, one pending entry.
    const { c, latest } = rig();
    c.lockTo('old');
    c.handleReconnect('a');
    c.handleEvent(started('a'));
    expect(latest().pendingCount).toBe(1);
  });
});

describe('actionable events', () => {
  it('forwards the events that change what the action bar can offer', () => {
    const { c, actionable } = rig();
    c.handleEvent(started('a'));
    c.handleEvent({ kind: 'execution_completed', color: 'a', project_id: 'p' } as DispatcherEvent);
    c.handleEvent({ kind: 'infra_flaky', project_id: 'p' } as unknown as DispatcherEvent);
    expect(actionable.map((e) => e.kind)).toEqual([
      'execution_started',
      'execution_completed',
      'infra_flaky',
    ]);
  });

  it('ignores per-node traffic', () => {
    const { c, actionable } = rig();
    c.handleEvent({
      kind: 'node_started', color: 'a', node: 'n', frames: [], input: null,
      closed_ports: [], project_id: 'p',
    });
    expect(actionable).toEqual([]);
  });
});
