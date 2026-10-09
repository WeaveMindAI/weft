import { describe, expect, it } from 'vitest';
import { DispatcherClient } from './dispatcher';
import { ReconnectingStream } from './projectEvents';
import type { LivePause } from './presence';

type Handlers = NonNullable<Parameters<DispatcherClient['subscribe']>[2]>;

class FakeDispatcher extends DispatcherClient {
  subscriptions: Array<{ path: string; handlers: Handlers; closed: boolean }> = [];
  constructor() { super(); }
  override subscribe(path: string, _onEvent: (ev: { data: string }) => void, handlers: Handlers = {}) {
    const entry = { path, handlers, closed: false };
    this.subscriptions.push(entry);
    return { close: () => { entry.closed = true; } };
  }
}

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
  const pause = new ManualPause();
  const stream = new ReconnectingStream<unknown>(client, 'test', pause);
  let connects = 0;
  stream.onConnect(() => connects++);
  return { client, pause, stream, connects: () => connects };
}

describe('a reconnecting stream while the person is away', () => {
  it('closes without retrying on pause, and reconnects and resyncs on resume', () => {
    const { client, pause, stream, connects } = rig();
    stream.setPath('/events/project/p');
    client.subscriptions[0].handlers.onOpen?.();
    expect(connects()).toBe(1);

    pause.set(true);
    expect(client.subscriptions[0].closed).toBe(true);
    // The closed connection's late failure must not schedule a retry.
    client.subscriptions[0].handlers.onError?.(new Error('aborted'));
    expect(client.subscriptions).toHaveLength(1);

    pause.set(false);
    expect(client.subscriptions).toHaveLength(2);
    client.subscriptions[1].handlers.onOpen?.();
    expect(connects()).toBe(2);
    stream.dispose();
  });

  it('opens nothing for a path set while paused until the person is back', () => {
    const { client, pause, stream } = rig();
    pause.set(true);
    stream.setPath('/events/project/p');
    client.setInstall('https://elsewhere.example.com', 'key');
    expect(client.subscriptions).toHaveLength(0);
    pause.set(false);
    expect(client.subscriptions.map((s) => s.path)).toEqual(['/events/project/p']);
    stream.dispose();
  });

  it('opens nothing on resume when it points at nothing', () => {
    const { client, pause } = rig();
    pause.set(true);
    pause.set(false);
    expect(client.subscriptions).toHaveLength(0);
  });
});
