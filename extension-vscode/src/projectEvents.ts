// The ONE project-level SSE stream: a persistent, self-reconnecting
// subscription to `/events/project/{id}` that every consumer
// (auto-follow, executions sidebar, action bar) listens on. A single
// dropped connection must never silence the editor for the rest of
// the session: the dispatcher may not be up yet when the graph opens
// (the first Run boots it), and it can restart under us, so the
// stream retries with backoff forever until the project is unpinned.
//
// Consumers register two kinds of listeners:
//   - event listeners: every parsed DispatcherEvent, live.
//   - connect listeners: fired on EVERY successful (re)connection.
//     Events that happened while the stream was down are gone, so a
//     connect is the signal to resync from authoritative state
//     (refetch status, refetch the executions list, catch up to a
//     run that started meanwhile).

import type { DispatcherClient, SseSubscription } from './dispatcher';
import type { DispatcherEvent } from './execFollower';

const INITIAL_BACKOFF_MS = 500;
const MAX_BACKOFF_MS = 5000;

export class ProjectEventStream {
  // Bumped on every setProject/dispose; callbacks from a previous
  // project's connection compare against it and go inert, so a slow
  // failing connect can never revive after a project switch.
  private generation = 0;
  private projectId: string | undefined;
  private subscription: SseSubscription | undefined;
  private retryTimer: NodeJS.Timeout | undefined;
  private backoffMs = INITIAL_BACKOFF_MS;

  private readonly eventListeners: Array<(ev: DispatcherEvent) => void> = [];
  private readonly connectListeners: Array<() => void> = [];

  constructor(private readonly client: DispatcherClient) {}

  onEvent(listener: (ev: DispatcherEvent) => void): void {
    this.eventListeners.push(listener);
  }

  onConnect(listener: () => void): void {
    this.connectListeners.push(listener);
  }

  /** Point the stream at a project (or nothing). Closes the previous
   *  connection and, for a project, starts the persistent connect
   *  loop immediately. */
  setProject(projectId: string | undefined): void {
    this.generation += 1;
    this.stopCurrent();
    this.projectId = projectId;
    this.backoffMs = INITIAL_BACKOFF_MS;
    if (projectId) this.connect(this.generation);
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
    if (generation !== this.generation || !this.projectId) return;
    // Own exactly one live subscription: close whatever is there
    // before opening the next, so a reconnect can never stack a
    // still-running reader under the new one.
    this.subscription?.close();
    console.log(`[weft/projectEvents] connecting (gen ${generation}) project ${this.projectId}`);
    this.subscription = this.client.subscribe(
      `/events/project/${this.projectId}`,
      (ev) => {
        if (generation !== this.generation) return;
        let parsed: DispatcherEvent;
        try {
          parsed = JSON.parse(ev.data) as DispatcherEvent;
        } catch (err) {
          console.warn('[weft/projectEvents] bad SSE payload', err);
          return;
        }
        for (const listener of this.eventListeners) listener(parsed);
      },
      {
        onOpen: () => {
          if (generation !== this.generation) return;
          console.log(`[weft/projectEvents] connected (gen ${generation})`);
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
    console.warn(`[weft/projectEvents] ${why} (gen ${generation}), retry in ${delay}ms`);
    this.retryTimer = setTimeout(() => {
      this.retryTimer = undefined;
      this.connect(generation);
    }, delay);
  }
}
