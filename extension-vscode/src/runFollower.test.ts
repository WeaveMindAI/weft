import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';
import { DispatcherClient, SseRefused } from './dispatcher';
import { RunFollower, CAUGHT_UP, REOPEN_AFTER_MS, REOPEN_CEILING_MS, type DispatcherEvent, type LiveRun } from './runFollower';
import type { HostMessage } from '../../packages/weft-graph/src/protocol';
import type { LivePause } from './presence';

type Handlers = NonNullable<Parameters<DispatcherClient['subscribe']>[2]>;

class FakeDispatcher extends DispatcherClient {
  subscriptions: Array<{ path: string; handlers: Handlers; send: (data: string) => void; caughtUp: () => void; closed: boolean }> = [];

  constructor() { super('unused'); }

  override subscribe(path: string, onEvent: (ev: { data: string; event?: string }) => void, handlers: Handlers = {}) {
    const entry = {
      path,
      handlers,
      send: (data: string) => onEvent({ data }),
      caughtUp: () => onEvent({ data: CAUGHT_UP, event: CAUGHT_UP }),
      closed: false,
    };
    this.subscriptions.push(entry);
    return { close: () => { entry.closed = true; } };
  }
}

function completed(node: string, seq = 1): DispatcherEvent {
  return { eventId: `journal:${seq}:0:0`, kind: 'node_completed', runId: 'a', projectId: 'p', node, frames: [], output: {}, atUnix: 1 };
}

/** A pause the test flips by hand. */
class ManualPause implements LivePause {
  private value = false;
  private readonly listeners: Array<(paused: boolean) => void> = [];
  paused(): boolean { return this.value; }
  onChange(listener: (paused: boolean) => void): void { this.listeners.push(listener); }
  set(value: boolean): void {
    this.value = value;
    for (const listener of this.listeners) listener(value);
  }
}

function rig() {
  const client = new FakeDispatcher();
  const posted: HostMessage[] = [];
  const pause = new ManualPause();
  const follower = new RunFollower(client, (event) => posted.push(event), pause);
  return { client, follower, posted, pause };
}

function painted(posted: HostMessage[]): string[] {
  return posted.flatMap((e) => (e.kind === 'firingEvent' ? [e.event.nodeId] : []));
}

beforeEach(() => { vi.useFakeTimers(); });
afterEach(() => { vi.useRealTimers(); });

describe('one stream from the run\'s start', () => {
  it('opens the run\'s stream from its start and paints what it sends, history and live alike', async () => {
    const { client, follower, posted } = rig();
    await follower.replay('a');
    expect(client.subscriptions.map((s) => s.path)).toEqual(['/events/run/a']);
    expect(posted).toEqual([{ kind: 'runReset', runId: 'a' }]);
    client.subscriptions[0].handlers.onOpen?.();
    client.subscriptions[0].send(JSON.stringify(completed('history', 1)));
    client.subscriptions[0].caughtUp();
    client.subscriptions[0].send(JSON.stringify(completed('live', 2)));
    expect(painted(posted)).toEqual(['history', 'live']);
    follower.stop();
  });

  it('opens a dropped stream again past the last event it painted', async () => {
    const { client, follower, posted } = rig();
    await follower.replay('a');
    client.subscriptions[0].handlers.onOpen?.();
    client.subscriptions[0].send(JSON.stringify(completed('one', 3)));
    client.subscriptions[0].send(JSON.stringify({ eventId: 'live:00000000-0000-0000-0000-000000000001', kind: 'run_parked', runId: 'a', projectId: 'p' }));
    client.subscriptions[0].handlers.onError?.(new Error('the connection dropped'));
    expect(client.subscriptions[0].closed).toBe(true);
    vi.advanceTimersByTime(REOPEN_AFTER_MS);
    expect(client.subscriptions.map((s) => s.path)).toEqual(['/events/run/a', '/events/run/a?after=3%3A0%3A0']);
    client.subscriptions[1].handlers.onOpen?.();
    client.subscriptions[1].send(JSON.stringify(completed('two', 4)));
    expect(painted(posted)).toEqual(['one', 'two']);
    expect(posted.some((e) => e.kind === 'followLost')).toBe(false);
    // A stream opened again paints on; it does not start over.
    expect(posted.filter((e) => e.kind === 'runReset')).toHaveLength(1);
    follower.stop();
  });

  it('opens nothing again once the run ended and its stream dropped', async () => {
    const { client, follower, posted } = rig();
    await follower.replay('a');
    client.subscriptions[0].handlers.onOpen?.();
    client.subscriptions[0].send(JSON.stringify({ eventId: 'journal:5:0:0', kind: 'run_completed', runId: 'a', projectId: 'p', outputs: {}, atUnix: 2 }));
    client.subscriptions[0].handlers.onClosed?.();
    vi.advanceTimersByTime(REOPEN_AFTER_MS * 10);
    expect(client.subscriptions).toHaveLength(1);
    expect(posted.some((e) => e.kind === 'followLost')).toBe(false);
    follower.stop();
  });

  it('loses a follow whose stream could not be opened at all', async () => {
    const { client, follower, posted } = rig();
    await follower.replay('a');
    client.subscriptions[0].handlers.onError?.(new Error('refused'));
    vi.advanceTimersByTime(REOPEN_AFTER_MS * 10);
    expect(client.subscriptions).toHaveLength(1);
    expect(posted.at(-1)).toEqual({ kind: 'followLost', runId: 'a', reason: 'error' });
  });

  it('keeps opening a dropped stream again while each reopen fails, waiting longer each time, and says so until one opens', async () => {
    const { client, follower, posted } = rig();
    await follower.replay('a');
    client.subscriptions[0].handlers.onOpen?.();
    client.subscriptions[0].send(JSON.stringify(completed('one', 3)));
    client.subscriptions[0].handlers.onClosed?.();
    for (let attempt = 1; attempt <= 3; attempt++) {
      const wait = REOPEN_AFTER_MS * 2 ** (attempt - 1);
      vi.advanceTimersByTime(wait - 1);
      expect(client.subscriptions).toHaveLength(attempt);
      vi.advanceTimersByTime(1);
      expect(client.subscriptions).toHaveLength(attempt + 1);
      expect(client.subscriptions[attempt].path).toBe('/events/run/a?after=3%3A0%3A0');
      client.subscriptions[attempt].handlers.onError?.(new Error(`refused ${attempt}`));
    }
    expect(posted.some((e) => e.kind === 'followLost')).toBe(false);
    expect(posted.filter((e) => e.kind === 'followReconnecting')).toEqual([
      { kind: 'followReconnecting', runId: 'a', why: 'the stream ended' },
      { kind: 'followReconnecting', runId: 'a', why: 'refused 1' },
      { kind: 'followReconnecting', runId: 'a', why: 'refused 2' },
      { kind: 'followReconnecting', runId: 'a', why: 'refused 3' },
    ]);
    vi.advanceTimersByTime(REOPEN_AFTER_MS * 8);
    client.subscriptions[4].handlers.onOpen?.();
    expect(posted.at(-1)).toEqual({ kind: 'followReconnecting', runId: 'a', why: null });
    client.subscriptions[4].send(JSON.stringify(completed('two', 4)));
    expect(painted(posted)).toEqual(['one', 'two']);
    follower.stop();
  });

  it('loses the follow when the install itself refuses to open the stream again', async () => {
    const { client, follower, posted } = rig();
    await follower.replay('a');
    client.subscriptions[0].handlers.onOpen?.();
    client.subscriptions[0].handlers.onClosed?.();
    vi.advanceTimersByTime(REOPEN_AFTER_MS);
    client.subscriptions[1].handlers.onError?.(new SseRefused('/events/run/a', 404, true));
    expect(posted.at(-1)).toEqual({ kind: 'followLost', runId: 'a', reason: 'error' });
    vi.advanceTimersByTime(REOPEN_CEILING_MS * 2);
    expect(client.subscriptions).toHaveLength(2);
  });

  it('tries again when the install says nothing ran and to ask again', async () => {
    const { client, follower, posted } = rig();
    await follower.replay('a');
    client.subscriptions[0].handlers.onOpen?.();
    client.subscriptions[0].handlers.onClosed?.();
    vi.advanceTimersByTime(REOPEN_AFTER_MS);
    client.subscriptions[1].handlers.onError?.(new SseRefused('/events/run/a', 503, true));
    expect(posted.some((e) => e.kind === 'followLost')).toBe(false);
    vi.advanceTimersByTime(REOPEN_AFTER_MS * 2);
    expect(client.subscriptions).toHaveLength(3);
    follower.stop();
  });

  it('tries again when the platform in front of the install says it is starting or rate limiting', async () => {
    const { client, follower, posted } = rig();
    await follower.replay('a');
    client.subscriptions[0].handlers.onOpen?.();
    client.subscriptions[0].handlers.onClosed?.();
    vi.advanceTimersByTime(REOPEN_AFTER_MS);
    client.subscriptions[1].handlers.onError?.(new SseRefused('/events/run/a', 503, false));
    vi.advanceTimersByTime(REOPEN_AFTER_MS * 2);
    client.subscriptions[2].handlers.onError?.(new SseRefused('/events/run/a', 429, false));
    expect(posted.some((e) => e.kind === 'followLost')).toBe(false);
    vi.advanceTimersByTime(REOPEN_AFTER_MS * 4);
    expect(client.subscriptions).toHaveLength(4);
    follower.stop();
  });

  it('ends the reconnecting notice when the follow stops mid-reconnect', async () => {
    const { client, follower, posted } = rig();
    await follower.replay('a');
    client.subscriptions[0].handlers.onOpen?.();
    client.subscriptions[0].handlers.onError?.(new Error('dropped'));
    follower.stop();
    expect(posted.at(-1)).toEqual({ kind: 'followReconnecting', runId: 'a', why: null });
    vi.advanceTimersByTime(REOPEN_AFTER_MS * 10);
    expect(client.subscriptions).toHaveLength(1);
  });

  it('stopping while a dropped stream waits to open again opens nothing', async () => {
    const { client, follower } = rig();
    await follower.replay('a');
    client.subscriptions[0].handlers.onOpen?.();
    client.subscriptions[0].handlers.onClosed?.();
    follower.stop();
    vi.advanceTimersByTime(REOPEN_AFTER_MS * 10);
    expect(client.subscriptions).toHaveLength(1);
  });

  it('ignores an old stream once the run is followed again', async () => {
    const { client, follower, posted } = rig();
    await follower.replay('a');
    client.subscriptions[0].handlers.onOpen?.();
    await follower.replay('a');
    client.subscriptions[1].handlers.onOpen?.();
    client.subscriptions[1].send(JSON.stringify(completed('current')));
    client.subscriptions[0].send(JSON.stringify(completed('stale')));
    client.subscriptions[0].handlers.onClosed?.();
    vi.advanceTimersByTime(REOPEN_AFTER_MS * 10);
    expect(painted(posted)).toEqual(['current']);
    expect(client.subscriptions).toHaveLength(2);
    follower.stop();
  });

  it('passes a bus close through with how it ended, a failure and its error included', async () => {
    const { client, follower, posted } = rig();
    await follower.replay('a');
    client.subscriptions[0].handlers.onOpen?.();
    const closed = (seq: number, end: Extract<DispatcherEvent, { kind: 'bus_closed' }>['end']): DispatcherEvent =>
      ({ eventId: `journal:${seq}:0:0`, kind: 'bus_closed', runId: 'a', projectId: 'p', busId: 'b', offset: 3, end, atUnix: 1 });
    client.subscriptions[0].send(JSON.stringify(closed(1, { kind: 'finished' })));
    client.subscriptions[0].send(JSON.stringify(closed(2, { kind: 'failed', error: 'producer crashed' })));
    expect(posted.filter((e) => e.kind === 'busEvent')).toEqual([
      { kind: 'busEvent', event: { kind: 'closed', busId: 'b', offset: 3, end: { kind: 'finished' }, atUnix: 1 } },
      { kind: 'busEvent', event: { kind: 'closed', busId: 'b', offset: 3, end: { kind: 'failed', error: 'producer crashed' }, atUnix: 1 } },
    ]);
    follower.stop();
  });
});

describe('pausing while the person is away', () => {
  it('closes the stream on pause and opens it again past what it painted on resume', async () => {
    const { client, follower, posted, pause } = rig();
    await follower.replay('a');
    client.subscriptions[0].handlers.onOpen?.();
    client.subscriptions[0].send(JSON.stringify(completed('before', 2)));

    pause.set(true);
    expect(client.subscriptions[0].closed).toBe(true);
    expect(posted.some((e) => e.kind === 'followLost')).toBe(false);

    pause.set(false);
    expect(client.subscriptions.map((s) => s.path)).toEqual(['/events/run/a', '/events/run/a?after=2%3A0%3A0']);
    client.subscriptions[1].handlers.onOpen?.();
    client.subscriptions[1].send(JSON.stringify(completed('meanwhile', 3)));
    expect(posted.filter((e) => e.kind === 'runReset')).toHaveLength(1);
    expect(painted(posted)).toEqual(['before', 'meanwhile']);
    follower.stop();
  });

  it('opens nothing for a run picked while paused until the person is back', async () => {
    const { client, follower, pause } = rig();
    pause.set(true);
    await follower.replay('a');
    expect(client.subscriptions).toHaveLength(0);
    pause.set(false);
    expect(client.subscriptions).toHaveLength(1);
    follower.stop();
  });

  it('does not revive a follow that was stopped or lost', async () => {
    const { client, follower, pause } = rig();
    await follower.replay('a');
    client.subscriptions[0].handlers.onClosed?.();
    pause.set(true);
    pause.set(false);
    expect(client.subscriptions).toHaveLength(1);
    follower.stop();
    pause.set(true);
    pause.set(false);
    expect(client.subscriptions).toHaveLength(1);
  });
});

describe('scope decisions', () => {
  it('reach the graph as the copy and its state, the skip reason with them', async () => {
    const { client, follower, posted } = rig();
    await follower.replay('a');
    client.subscriptions[0].handlers.onOpen?.();
    client.subscriptions[0].send(JSON.stringify({ eventId: 'journal:1:0:0', kind: 'scope_decided', runId: 'a', projectId: 'p', scope: 'g', frames: [{ index: 0 }], state: 'open', atUnix: 4 }));
    client.subscriptions[0].send(JSON.stringify({ eventId: 'journal:2:0:0', kind: 'scope_decided', runId: 'a', projectId: 'p', scope: 'inc', frames: [], state: 'skipped', reason: { kind: 'did_not_flow' }, inheritedFrom: 'r0', atUnix: 5 }));
    expect(posted.filter((e) => e.kind === 'scopeDecided')).toEqual([
      { kind: 'scopeDecided', event: { scope: 'g', frames: [{ index: 0 }], state: 'open', inheritedFrom: undefined, atUnix: 4 } },
      { kind: 'scopeDecided', event: { scope: 'inc', frames: [], state: 'skipped', reason: { kind: 'did_not_flow' }, inheritedFrom: 'r0', atUnix: 5 } },
    ]);
    follower.stop();
  });
});

describe('which followed run has not ended', () => {
  const live: LiveRun = { runId: 'a', projectId: 'p' };

  function ended(runId: string): DispatcherEvent {
    return { eventId: 'journal:9:0:0', kind: 'run_completed', runId, projectId: 'p', outputs: {}, atUnix: 2 };
  }

  /** Follow `runId` whose stream sends `history` then says it caught up,
   *  recording what the live-run listeners hear. */
  async function follow(history: DispatcherEvent[]) {
    const r = rig();
    const heard: Array<LiveRun | undefined> = [];
    r.follower.onLiveRun((run) => heard.push(run));
    await r.follower.replay('a');
    r.client.subscriptions[0].handlers.onOpen?.();
    for (const event of history) r.client.subscriptions[0].send(JSON.stringify(event));
    expect(heard).toEqual([]);
    r.client.subscriptions[0].caughtUp();
    return { ...r, heard };
  }

  it('names a run once its history was sent, and drops it when the run ends', async () => {
    const { client, follower, heard } = await follow([completed('n')]);
    expect(heard).toEqual([live]);
    client.subscriptions[0].send(JSON.stringify(ended('a')));
    expect(heard).toEqual([live, undefined]);
    follower.stop();
    expect(heard).toEqual([live, undefined]);
  });

  it('names a run whose history held no event once a live one says its project', async () => {
    const { client, follower, heard } = await follow([]);
    expect(heard).toEqual([]);
    client.subscriptions[0].send(JSON.stringify(completed('n')));
    expect(heard).toEqual([live]);
    follower.stop();
  });

  it('never names a run whose history already holds its end', async () => {
    const { follower, heard } = await follow([completed('n'), ended('a')]);
    expect(heard).toEqual([]);
    follower.stop();
  });

  it('drops the run when the follow is lost or stopped, keeps it while a dropped stream opens again', async () => {
    const dropped = await follow([completed('n')]);
    dropped.client.subscriptions[0].handlers.onClosed?.();
    expect(dropped.heard).toEqual([live]);
    dropped.follower.stop();
    expect(dropped.heard).toEqual([live, undefined]);
    const stopped = await follow([completed('n')]);
    stopped.follower.stop();
    expect(stopped.heard).toEqual([live, undefined]);
  });

  it('drops the run the moment another one is followed', async () => {
    const { client, follower, heard } = await follow([completed('n')]);
    void follower.replay('b');
    expect(heard).toEqual([live, undefined]);
    client.subscriptions[1].handlers.onOpen?.();
    expect(heard).toEqual([live, undefined]);
    follower.stop();
  });

  it('paints nothing for a parked run', async () => {
    const { client, follower, posted, heard } = await follow([completed('n')]);
    const before = posted.length;
    client.subscriptions[0].send(JSON.stringify({ eventId: 'live:00000000-0000-0000-0000-000000000002', kind: 'run_parked', runId: 'a', projectId: 'p' }));
    expect(posted).toHaveLength(before);
    expect(heard).toEqual([live]);
    follower.stop();
  });
});
