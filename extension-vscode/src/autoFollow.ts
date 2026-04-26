// Auto-follow controller: decides which execution the graph
// view is currently streaming, using the rules the user spec'd:
//
//   1. Default mode is 'latest': any new ExecutionStarted for the
//      pinned project gets followed automatically.
//   2. "User-initiated" execs (Run, Activate, Infra Start) call
//      pinAndFollow(color) to force-follow regardless of mode.
//      They also flip mode back to 'latest' since the user just
//      expressed "I want to see what happens now."
//   3. Clicking view-in-graph on a past exec → pinToExecution,
//      which flips mode to 'pinned'. Subsequent background events
//      (trigger fires) don't steal focus; they bump a counter.
//   4. The webview's "Catch up" / "Unpin" buttons call
//      catchUpToLatest, which goes back to 'latest' + follows the
//      newest currently-running or most-recent completed exec.
//
// The controller owns the project-level SSE subscription. It has
// no UI of its own — it tells the webview what to show via
// `kind: followStatus` messages and defers the actual
// execution-event stream to ExecutionFollower.

import type { DispatcherClient, SseSubscription } from './dispatcher';
import type { ExecutionFollower, DispatcherEvent } from './execFollower';
import type { HostMessage } from './shared/protocol';

export type FollowMode = 'latest' | 'pinned';

/** State the webview renders in the pin pill + banner. */
export interface FollowStatus {
  mode: FollowMode;
  color: string | undefined;
  // Count of new execs that started while pinned and weren't
  // followed. Reset whenever the user catches up.
  pendingCount: number;
}

export type PostFn = (msg: HostMessage) => void;

export class AutoFollowController {
  private mode: FollowMode = 'latest';
  private color: string | undefined;
  private pendingCount = 0;
  // Execs that started while we were pinned; newest last so we
  // can pop the most-recent when the user catches up.
  private pendingQueue: string[] = [];

  private projectId: string | undefined;
  private subscription: SseSubscription | undefined;

  constructor(
    private readonly client: DispatcherClient,
    private readonly follower: ExecutionFollower,
    private readonly post: PostFn,
  ) {}

  /** Called when the extension pins a different project. Resets
   *  everything and re-subscribes to the new project's SSE. */
  setProject(projectId: string | undefined): void {
    this.subscription?.close();
    this.subscription = undefined;
    this.projectId = projectId;
    this.mode = 'latest';
    this.color = undefined;
    this.pendingCount = 0;
    this.pendingQueue = [];
    this.follower.stop();
    if (projectId) {
      this.subscription = this.client.subscribe(
        `/events/project/${projectId}`,
        (ev) => {
          try {
            this.onEvent(JSON.parse(ev.data) as DispatcherEvent);
          } catch (err) {
            console.warn('[weft/autoFollow] bad SSE payload', err);
          }
        },
      );
    }
    this.emitStatus();
  }

  /** User clicked Run, Activate, or Infra Start. We know the
   *  color the dispatcher handed back (Run) or we don't (Activate
   *  spawns an internal exec whose color we learn via SSE). In
   *  the latter case the caller passes `undefined` and we pick up
   *  the next ExecutionStarted. */
  pinAndFollow(color: string | undefined): void {
    this.mode = 'latest';
    this.pendingCount = 0;
    this.pendingQueue = [];
    if (color) {
      // Race window: the dispatcher's /run response arrives on the
      // CLI's HTTP path AND the same ExecutionStarted is broadcast
      // on the project SSE. Whichever arrives first triggers a
      // replay. If the SSE-driven `onEvent` already started a
      // replay for this exact color, don't kick a second one
      // here (and vice-versa via onEvent's same guard). Two
      // concurrent replays would double-deliver journaled events
      // to the webview as the journal fills mid-execution.
      if (this.color === color) {
        this.emitStatus();
        return;
      }
      this.color = color;
      // `replay` handles the "already completed" and "mid-run"
      // cases uniformly: pulls journaled events, then continues
      // live-following. `follow` only streams new events, so if
      // the exec has already emitted some frames (or finished)
      // we miss them.
      void this.follower.replay(color);
    }
    this.emitStatus();
  }

  /** User clicked "view in graph" on a past exec in the sidebar
   *  or asked us to pin the current one from the graph pill. */
  pinToExecution(color: string): void {
    this.mode = 'pinned';
    this.color = color;
    this.pendingCount = 0;
    this.pendingQueue = [];
    void this.follower.replay(color);
    this.emitStatus();
  }

  /** Pin pill toggle from the graph: switches mode, keeps color
   *  unchanged on pin→unpin (so the user's looking at the same
   *  thing, but new execs will now auto-jump). */
  togglePin(): void {
    if (this.mode === 'pinned') {
      this.catchUpToLatest();
    } else {
      if (this.color) {
        this.mode = 'pinned';
        this.emitStatus();
      }
    }
  }

  /** Banner "Catch up" or pin pill "Unpin → jump to latest". */
  catchUpToLatest(): void {
    const newest = this.pendingQueue[this.pendingQueue.length - 1];
    this.mode = 'latest';
    this.pendingCount = 0;
    this.pendingQueue = [];
    if (newest) {
      this.color = newest;
      // Replay, not follow: the queued exec may have already
      // finished while we were pinned, so we need the journaled
      // events to paint the graph.
      void this.follower.replay(newest);
    }
    this.emitStatus();
  }

  dispose(): void {
    this.subscription?.close();
  }

  private onEvent(ev: DispatcherEvent): void {
    if (ev.kind !== 'execution_started') return;
    if (this.mode === 'latest') {
      // If we're already following this color (the run command
      // called `pinAndFollow(color)` synchronously and the
      // project SSE is now echoing the same ExecutionStarted),
      // don't re-replay: the live stream is already delivering
      // events and a second replay would re-apply whatever's
      // landed in the journal so far, double-rendering pulses.
      if (this.color === ev.color) {
        return;
      }
      // Auto-jump for a NEW color (Activate / Infra Start path
      // where `pinAndFollow(undefined)` was just called, or a
      // trigger fire opening a fresh execution).
      this.color = ev.color;
      void this.follower.replay(ev.color);
      this.emitStatus();
    } else {
      this.pendingQueue.push(ev.color);
      this.pendingCount += 1;
      this.emitStatus();
    }
  }

  private emitStatus(): void {
    const status: FollowStatus = {
      mode: this.mode,
      color: this.color,
      pendingCount: this.pendingCount,
    };
    this.post({ kind: 'followStatus', status });
  }
}
