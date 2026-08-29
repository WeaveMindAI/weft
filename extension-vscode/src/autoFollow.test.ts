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

function rig() {
  const calls: Call[] = [];
  const posted: FollowStatus[] = [];

  // Typed as the surface the controller actually uses, so a signature
  // change on the real follower fails this file at compile time; the
  // one cast is at the constructor below.
  const follower: Pick<ExecutionFollower, 'replay' | 'stop'> = {
    replay: async (color: string) => { calls.push({ verb: 'replay', color }); },
    stop: () => { calls.push({ verb: 'stop' }); },
  };

  const post = (msg: HostMessage) => {
    if (msg.kind === 'followStatus') posted.push(msg.status as FollowStatus);
  };

  const actionable: DispatcherEvent[] = [];
  const c = new AutoFollowController(
    follower as ExecutionFollower,
    post,
    (ev) => actionable.push(ev),
  );
  return { c, calls, posted, actionable, latest: () => posted[posted.length - 1] };
}

function started(color: string): DispatcherEvent {
  return { kind: 'execution_started', color, entry_node: 'n', project_id: 'p' };
}

describe('latest mode', () => {
  it('jumps to a new execution and replays it', () => {
    const { c, calls, latest } = rig();
    c.handleEvent(started('a'));
    expect(calls).toEqual([{ verb: 'replay', color: 'a' }]);
    expect(latest()).toEqual({ mode: 'latest', color: 'a', pendingCount: 0 });
  });

  it('does not replay twice when the run command already followed the color', () => {
    const { c, calls } = rig();
    c.pinAndFollow('a');           // the /run response
    c.handleEvent(started('a'));   // the same start, echoed on the project stream
    expect(calls).toEqual([{ verb: 'replay', color: 'a' }]);
  });

  it('picks up the next execution when the caller did not know the color', () => {
    const { c, calls, latest } = rig();
    c.pinAndFollow(undefined);     // activate: the color arrives over SSE
    c.handleEvent(started('a'));
    expect(calls).toEqual([{ verb: 'replay', color: 'a' }]);
    expect(latest().color).toBe('a');
  });
});

describe('pinned mode', () => {
  it('queues background executions instead of stealing focus', () => {
    const { c, calls, latest } = rig();
    c.pinToExecution('old');
    c.handleEvent(started('a'));
    c.handleEvent(started('b'));

    expect(calls).toEqual([{ verb: 'replay', color: 'old' }]);
    expect(latest()).toEqual({ mode: 'pinned', color: 'old', pendingCount: 2 });
  });

  it('catching up jumps to the newest queued execution', () => {
    const { c, calls, latest } = rig();
    c.pinToExecution('old');
    c.handleEvent(started('a'));
    c.handleEvent(started('b'));
    c.catchUpToLatest();

    expect(calls.at(-1)).toEqual({ verb: 'replay', color: 'b' });
    expect(latest()).toEqual({ mode: 'latest', color: 'b', pendingCount: 0 });
  });

  it('catching up with nothing queued keeps the current color', () => {
    const { c, latest } = rig();
    c.pinToExecution('old');
    c.catchUpToLatest();
    expect(latest()).toEqual({ mode: 'latest', color: 'old', pendingCount: 0 });
  });
});

describe('the pin toggle', () => {
  it('pins the execution currently on screen', () => {
    const { c, latest } = rig();
    c.handleEvent(started('a'));
    c.togglePin();
    expect(latest()).toEqual({ mode: 'pinned', color: 'a', pendingCount: 0 });
  });

  it('does nothing when there is nothing on screen to pin', () => {
    const { c, posted } = rig();
    c.togglePin();
    expect(posted).toEqual([]);
  });

  it('unpinning catches up', () => {
    const { c, latest } = rig();
    c.pinToExecution('old');
    c.handleEvent(started('a'));
    c.togglePin();
    expect(latest()).toEqual({ mode: 'latest', color: 'a', pendingCount: 0 });
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

  it('queues it while pinned', () => {
    const { c, latest } = rig();
    c.pinToExecution('old');
    c.handleReconnect('a');
    expect(latest()).toEqual({ mode: 'pinned', color: 'old', pendingCount: 1 });
  });

  it('does not queue the same execution twice', () => {
    const { c, latest } = rig();
    c.pinToExecution('old');
    c.handleReconnect('a');
    c.handleReconnect('a');
    expect(latest().pendingCount).toBe(1);
  });

  it('does not double-count a reconnect resync and its backlogged start', () => {
    // The reconnect names the running color, then the reconnected
    // stream delivers the same color's execution_started off the
    // backlog: one run, one pending entry.
    const { c, latest } = rig();
    c.pinToExecution('old');
    c.handleReconnect('a');
    c.handleEvent(started('a'));
    expect(latest().pendingCount).toBe(1);
  });
});

describe('clearing and switching', () => {
  it('clearing a deleted execution stops the stream and reverts to latest', () => {
    const { c, calls, latest } = rig();
    c.pinToExecution('old');
    c.clearFollow();
    expect(calls.at(-1)).toEqual({ verb: 'stop' });
    expect(latest()).toEqual({ mode: 'latest', color: undefined, pendingCount: 0 });
  });

  it('switching project drops everything', () => {
    const { c, calls, latest } = rig();
    c.pinToExecution('old');
    c.handleEvent(started('a'));
    c.setProject();
    expect(calls.at(-1)).toEqual({ verb: 'stop' });
    expect(latest()).toEqual({ mode: 'latest', color: undefined, pendingCount: 0 });
    expect(c.currentColor()).toBeUndefined();
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
