// A persistent, self-reconnecting SSE subscription, and the two the
// editor holds: the project-level stream (`/events/project/{id}`) that
// every consumer (auto-follow, executions sidebar, action bar) listens
// on, and the display stream a graph view opens for the nodes it draws.
// A single dropped connection must never silence the editor for the
// rest of the session: the dispatcher may not be up yet when the graph
// opens (the first Run boots it), and it can restart under us, so the
// stream retries with backoff forever until it is pointed elsewhere.
//
// Consumers register two kinds of listeners:
//   - message listeners: every parsed message, live.
//   - connect listeners: fired on EVERY successful (re)connection.
//     Messages sent while the stream was down are gone, so a connect is
//     the signal to resync from authoritative state (refetch status,
//     refetch the executions list, catch up to a run that started
//     meanwhile).

import type { DispatcherClient, SseSubscription } from './dispatcher';
import type { DispatcherEvent } from './execFollower';

const INITIAL_BACKOFF_MS = 500;
const MAX_BACKOFF_MS = 5000;

export class ReconnectingStream<T> {
  // Bumped on every setPath/dispose; callbacks from a previous path's
  // connection compare against it and go inert, so a slow failing
  // connect can never revive after the stream moved on.
  private generation = 0;
  private path: string | undefined;
  private subscription: SseSubscription | undefined;
  private retryTimer: NodeJS.Timeout | undefined;
  private backoffMs = INITIAL_BACKOFF_MS;

  private readonly messageListeners: Array<(msg: T) => void> = [];
  private readonly connectListeners: Array<() => void> = [];

  /** `name` labels this stream's log lines. */
  constructor(
    private readonly client: DispatcherClient,
    private readonly name: string,
  ) {}

  onMessage(listener: (msg: T) => void): void {
    this.messageListeners.push(listener);
  }

  onConnect(listener: () => void): void {
    this.connectListeners.push(listener);
  }

  /** Point the stream at a path (or nothing). Closes the previous
   *  connection and, for a path, starts the persistent connect loop
   *  immediately. */
  setPath(path: string | undefined): void {
    this.generation += 1;
    this.stopCurrent();
    this.path = path;
    this.backoffMs = INITIAL_BACKOFF_MS;
    if (path) this.connect(this.generation);
  }

  dispose(): void {
    this.generation += 1;
    this.stopCurrent();
  }

  private stopCurrent(): void {
    this.subscription?.close();
    this.subscription = undefined;
    if (this.retryTimer) {
      clearTimeout(this.retryTimer);
      this.retryTimer = undefined;
    }
  }

  private connect(generation: number): void {
    if (generation !== this.generation || !this.path) return;
    // Own exactly one live subscription: close whatever is there
    // before opening the next, so a reconnect can never stack a
    // still-running reader under the new one.
    this.subscription?.close();
    console.log(`[weft/${this.name}] connecting (gen ${generation}) ${this.path}`);
    this.subscription = this.client.subscribe(
      this.path,
      (ev) => {
        if (generation !== this.generation) return;
        let parsed: T;
        try {
          parsed = JSON.parse(ev.data) as T;
        } catch (err) {
          console.warn(`[weft/${this.name}] bad SSE payload`, err);
          return;
        }
        for (const listener of this.messageListeners) listener(parsed);
      },
      {
        onOpen: () => {
          if (generation !== this.generation) return;
          console.log(`[weft/${this.name}] connected (gen ${generation})`);
          this.backoffMs = INITIAL_BACKOFF_MS;
          for (const listener of this.connectListeners) listener();
        },
        onError: (err) => {
          this.scheduleReconnect(generation, `stream failed: ${err}`);
        },
        onClosed: () => {
          this.scheduleReconnect(generation, 'stream ended by server');
        },
      },
    );
  }

  /** Logged here, after both guards, so the message reports the delay
   *  actually scheduled (a stale generation or an already-pending
   *  timer schedules nothing and stays silent). */
  private scheduleReconnect(generation: number, why: string): void {
    if (generation !== this.generation) return;
    if (this.retryTimer) return;
    const delay = this.backoffMs;
    this.backoffMs = Math.min(this.backoffMs * 2, MAX_BACKOFF_MS);
    console.warn(`[weft/${this.name}] ${why} (gen ${generation}), retry in ${delay}ms`);
    this.retryTimer = setTimeout(() => {
      this.retryTimer = undefined;
      this.connect(generation);
    }, delay);
  }
}

/** The ONE project-level stream every consumer listens on. */
export class ProjectEventStream extends ReconnectingStream<DispatcherEvent> {
  constructor(client: DispatcherClient) {
    super(client, 'projectEvents');
  }

  /** Point the stream at a project (or nothing). */
  setProject(projectId: string | undefined): void {
    this.setPath(projectId ? `/events/project/${projectId}` : undefined);
  }

  onEvent(listener: (ev: DispatcherEvent) => void): void {
    this.onMessage(listener);
  }
}
