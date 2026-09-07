import { describe, expect, it } from 'vitest';
import { DispatcherClient } from './dispatcher';
import { ExecutionFollower, MAX_REPLAY_BUFFER_BYTES, type DispatcherEvent } from './execFollower';
import type { HostMessage } from '../../packages/weft-graph/src/protocol';

type Handlers = NonNullable<Parameters<DispatcherClient['subscribe']>[2]>;

class FakeDispatcher extends DispatcherClient {
  subscriptions: Array<{ handlers: Handlers; send: (data: string) => void; closed: boolean }> = [];
  reads: Array<{ path: string; signal?: AbortSignal; resolve: (events: DispatcherEvent[]) => void; reject: (err: Error) => void }> = [];

  constructor() { super('unused'); }

  override subscribe(_path: string, onEvent: (ev: { data: string }) => void, handlers: Handlers = {}) {
    const entry = { handlers, send: (data: string) => onEvent({ data }), closed: false };
    this.subscriptions.push(entry);
    return { close: () => { entry.closed = true; } };
  }

  override get<T>(path: string, signal?: AbortSignal): Promise<T> {
    return new Promise<T>((resolve, reject) => {
      signal?.addEventListener('abort', () => reject(signal.reason), { once: true });
      this.reads.push({ path, signal, resolve: (events) => resolve(events as T), reject });
    });
  }
}

function completed(node: string): DispatcherEvent {
  return { event_id: `event:${node}`, kind: 'node_completed', color: 'a', project_id: 'p', node, frames: [], output: {}, at_unix: 1 };
}

function rig() {
  const client = new FakeDispatcher();
  const posted: HostMessage[] = [];
  const follower = new ExecutionFollower(client, (event) => posted.push(event));
  return { client, follower, posted };
}

describe('execution history and live follow', () => {
  it('applies overlapping history once and preserves identical but distinct events', async () => {
    const { client, follower, posted } = rig();
    const following = follower.replay('a');
    client.subscriptions[0].handlers.onOpen?.();
    await Promise.resolve();
    const event = completed('n');
    client.subscriptions[0].send(JSON.stringify(event));
    client.reads[0].resolve([event]);
    await following;
    client.subscriptions[0].send(JSON.stringify(event));
    client.subscriptions[0].send(JSON.stringify({ ...event, event_id: 'distinct' }));
    expect(posted.filter((e) => e.kind === 'execEvent')).toHaveLength(2);
    follower.stop();
  });

  it('cancels an abandoned history request without waiting for its server', async () => {
    const { client, follower } = rig();
    const following = follower.replay('a');
    client.subscriptions[0].handlers.onOpen?.();
    await Promise.resolve();
    follower.stop();
    expect(client.reads[0].signal?.aborted).toBe(true);
    await following;
  });

  it('ends a stalled replay when waiting live data exceeds its memory budget', async () => {
    const { client, follower, posted } = rig();
    const following = follower.replay('a');
    client.subscriptions[0].handlers.onOpen?.();
    await Promise.resolve();
    client.subscriptions[0].send(JSON.stringify({ ...completed('large'), output: { text: 'x'.repeat(MAX_REPLAY_BUFFER_BYTES) } }));
    await following;
    expect(client.reads[0].signal?.aborted).toBe(true);
    expect(client.subscriptions[0].closed).toBe(true);
    expect(posted.at(-1)).toEqual({ kind: 'followLost', color: 'a', reason: 'error' });
    expect(posted.some((e) => e.kind === 'execEvent')).toBe(false);
  });

  it('waits for acceptance before reading history, then drains live events', async () => {
    const { client, follower, posted } = rig();
    const following = follower.replay('a');
    expect(client.reads).toHaveLength(0);
    client.subscriptions[0].handlers.onOpen?.();
    await Promise.resolve();
    expect(client.reads.map((read) => read.path)).toEqual(['/executions/a/replay']);
    client.subscriptions[0].send(JSON.stringify(completed('live')));
    expect(posted).toEqual([{ kind: 'execReset' }]);
    client.reads[0].resolve([completed('history')]);
    await following;
    expect(posted.filter((e) => e.kind === 'execEvent').map((e) => e.event.nodeId))
      .toEqual(['history', 'live']);
    follower.stop();
  });

  it('settles a failed connection without fetching incomplete history', async () => {
    const { client, follower, posted } = rig();
    const following = follower.replay('a');
    client.subscriptions[0].handlers.onClosed?.();
    await following;
    expect(client.reads).toHaveLength(0);
    expect(client.subscriptions[0].closed).toBe(true);
    expect(posted.at(-1)).toEqual({ kind: 'followLost', color: 'a', reason: 'closed' });
  });

  it('stopping while connecting settles the pending follow', async () => {
    const { client, follower, posted } = rig();
    const following = follower.replay('a');
    follower.stop();
    await following;
    client.subscriptions[0].handlers.onOpen?.();
    client.subscriptions[0].send(JSON.stringify(completed('stale')));
    expect(client.reads).toHaveLength(0);
    expect(posted).toEqual([{ kind: 'execReset' }]);
  });

  it('ignores old history and callbacks when reopening the same execution', async () => {
    const { client, follower, posted } = rig();
    const first = follower.replay('a');
    client.subscriptions[0].handlers.onOpen?.();
    await Promise.resolve();
    const second = follower.replay('a');
    client.subscriptions[1].handlers.onOpen?.();
    await Promise.resolve();
    client.reads[1].resolve([completed('current')]);
    await second;
    client.reads[0].resolve([completed('stale')]);
    await first;
    client.subscriptions[0].send(JSON.stringify(completed('stale-live')));
    client.subscriptions[0].handlers.onClosed?.();
    expect(posted.filter((e) => e.kind === 'execEvent').map((e) => e.event.nodeId))
      .toEqual(['current']);
    expect(posted.some((e) => e.kind === 'followLost')).toBe(false);
    follower.stop();
  });

  it('does not revive a broken stream when its history finally arrives', async () => {
    const { client, follower, posted } = rig();
    const following = follower.replay('a');
    client.subscriptions[0].handlers.onOpen?.();
    await Promise.resolve();
    client.subscriptions[0].handlers.onClosed?.();
    client.reads[0].resolve([completed('stale')]);
    await following;
    expect(posted.some((e) => e.kind === 'execEvent')).toBe(false);
    expect(posted.at(-1)).toEqual({ kind: 'followLost', color: 'a', reason: 'closed' });
  });
});
