// Auto-follow controller: decides which execution the graph view is
// currently streaming. Three modes, picked on the graph's toggle:
//
//   - following: every run that starts takes the canvas. The default.
//   - locked: the canvas stays on one run; runs that start are counted
//     in a "new runs" chip instead of taking over.
//   - off: no run on the canvas; runs that start are counted the same
//     way. For reading the graph without a run painted over it.
//
// Besides the toggle, two gestures move the mode on their own:
//   - a run the person started from the editor (Run, Activate, Infra
//     Start) goes back to following: clicking Run is asking to watch
//     it. A run started anywhere else (a terminal, an assistant
//     running the CLI) is only an execution_started on the stream,
//     so it follows the mode like any trigger fire.
//   - "View in Graph" on a past run locks onto it.
//
// Whether runs that start take the canvas is remembered per project,
// so a person who turned following off finds it off after a restart.
// A lock is not: the run it held is not on screen after a restart,
// so it comes back as off (neither jumps to new runs).
//
// The controller consumes the shared project event stream
// (ProjectEventStream: the extension feeds `handleEvent` /
// `handleReconnect`). It has no UI of its own; it tells the webview
// what to show via `kind: followStatus` messages and defers the
// actual execution-event stream to ExecutionFollower.

import type { ExecutionFollower, DispatcherEvent } from './execFollower';
import type { FollowMode, FollowStatus, HostMessage } from '../../packages/weft-graph/src/protocol';

export type { FollowMode, FollowStatus };

export type PostFn = (msg: HostMessage) => void;

/// Fired on every project-SSE event that could change the
/// dispatcher's `available_actions` list (executions starting /
/// finishing, project status changes). The extension uses this to
/// debounce-trigger a `weft status --json` refetch.
export type ActionableEventHandler = (ev: DispatcherEvent) => void;

/// Where the per-project "do runs that start take the canvas" choice
/// is kept. The extension passes its workspace state.
export interface FollowMemory {
  get(key: string): unknown;
  update(key: string, value: unknown): Thenable<void>;
}

const memoryKey = (projectId: string) => `weft.follow.jumpsToNewRuns.${projectId}`;

export class AutoFollowController {
  private mode: FollowMode = 'following';
  private color: string | undefined;
  private projectId: string | undefined;
  // Runs that started while not following; newest last so catching up
  // can take the most recent. The pending COUNT shown in the chip is
  // derived from it, never stored twice.
  private pendingQueue: string[] = [];

  constructor(
    private readonly follower: ExecutionFollower,
    private readonly post: PostFn,
    private readonly memory: FollowMemory,
    /// Notified on events that shift available_actions (start /
    /// completion / failure). The extension debounces 500ms before
    /// re-fetching status, so a flurry of node events during a run
    /// doesn't hammer the dispatcher.
    private readonly onActionable: ActionableEventHandler = () => {},
  ) {}

  /** Called when the extension pins a different project (the shared
   *  project event stream re-points in the same breath). Nothing is on
   *  screen for it yet; the mode is the one remembered for it. */
  setProject(projectId: string): void {
    this.projectId = projectId;
    this.mode = this.memory.get(memoryKey(projectId)) === false ? 'off' : 'following';
    this.color = undefined;
    this.pendingQueue = [];
    this.follower.stop();
    this.emitStatus();
  }

  /** The execution the graph is currently streaming, if any. The ONE
   *  place that answers "which color is on screen": consumers (e.g.
   *  the sidebar's delete action) read it here instead of shadowing
   *  it, since every follow path updates it. */
  currentColor(): string | undefined {
    return this.color;
  }

  /** The person picked a mode on the toggle, or clicked the "new runs"
   *  chip (which picks following). */
  setMode(mode: FollowMode): void {
    // Re-picking the current lock or off changes nothing (and must not
    // reset the count of runs waiting). Re-picking following still
    // catches up, which is what the chip relies on.
    if (mode === this.mode && mode !== 'following') return;
    switch (mode) {
      case 'following': {
        // Catch up: the newest run that started while not following,
        // or else whatever is already on screen stays.
        const newest = this.pendingQueue[this.pendingQueue.length - 1];
        this.enter('following');
        if (newest) this.show(newest);
        break;
      }
      case 'locked':
        // Locking holds the run on screen; with none there is nothing
        // to hold (the toggle disables the choice).
        if (!this.color) return;
        this.enter('locked');
        break;
      case 'off':
        this.enter('off');
        this.clearCanvas();
        break;
    }
    this.emitStatus();
  }

  /** The person started something from the editor: Run, Activate, or
   *  Infra Start. We know the color the dispatcher handed back (Run)
   *  or we don't (Activate spawns an internal exec whose color we
   *  learn via SSE). In the latter case the caller passes `undefined`
   *  and we pick up the next ExecutionStarted. */
  followStartedByUser(color: string | undefined): void {
    console.log(`[weft/autoFollow] followStartedByUser(${color}): mode=${this.mode} color=${this.color}`);
    this.enter('following');
    // Race window: the dispatcher's /run response arrives on the CLI's
    // HTTP path AND the same ExecutionStarted is broadcast on the
    // project SSE. Whichever arrives first starts the replay; two
    // concurrent replays would double-deliver journaled events to the
    // webview as the journal fills mid-execution.
    if (color && this.color !== color) this.show(color);
    this.emitStatus();
  }

  /** "View in Graph" on a past run: lock onto it. */
  lockTo(color: string): void {
    this.enter('locked');
    this.show(color);
    this.emitStatus();
  }

  /** The run on screen is gone (deleted, here or in another window).
   *  A lock has nothing left to hold, so it becomes off; following
   *  keeps following and shows the next run that starts. */
  stopShowing(): void {
    if (this.mode === 'locked') this.enter('off');
    this.clearCanvas();
    this.emitStatus();
  }

  /** The project stream (re)connected: events emitted while it was
   *  down are gone, so the extension hands us the freshly fetched
   *  newest RUNNING execution (if any) to catch up on. It is treated
   *  exactly as if its ExecutionStarted had arrived live. */
  handleReconnect(runningColor: string | undefined): void {
    console.log(`[weft/autoFollow] reconnect resync: running=${runningColor} mode=${this.mode} color=${this.color}`);
    if (runningColor) this.runStarted(runningColor);
  }

  /** Every parsed event from the shared project stream. */
  handleEvent(ev: DispatcherEvent): void {
    // Anything that shifts the dispatcher's `available_actions` list
    // routes to the actionable-event handler so the action bar
    // refetches `weft status --json` and re-renders. Includes:
    //   - execution lifecycle (run / cancel / complete change run state)
    //   - infra lifecycle (status flips, transients, flaky/recovered);
    //     this is what makes Stop / Terminate's `stopping` /
    //     `terminating` transients visible in the UI. Without it the
    //     bar only re-reads on user action.
    if (
      ev.kind === 'execution_started' ||
      ev.kind === 'execution_completed' ||
      ev.kind === 'execution_failed' ||
      ev.kind === 'execution_cancelled' ||
      // A deleted run was preserved state until now (a run parked on a
      // question counts), so the verb set moves with it.
      ev.kind === 'execution_deleted' ||
      ev.kind === 'infra_status_changed' ||
      ev.kind === 'infra_flaky' ||
      ev.kind === 'infra_recovered' ||
      ev.kind === 'infra_terminated' ||
      ev.kind === 'project_registered' ||
      ev.kind === 'project_activated' ||
      ev.kind === 'project_deactivated' ||
      ev.kind === 'infra_config_error' ||
      ev.kind === 'trigger_url_changed' ||
      // Build/verb lifecycle: the dispatcher announces transition flips
      // (building, activating, ...) so the bar shows "Building... (cancel)"
      // without waiting for the verb round-trip to finish.
      ev.kind === 'project_transition_changed'
    ) {
      this.onActionable(ev);
    }

    if (ev.kind !== 'execution_started') return;
    console.log(`[weft/autoFollow] execution_started ${ev.color}: mode=${this.mode} color=${this.color}`);
    this.runStarted(ev.color);
  }

  /** A run started (live, or found running on reconnect). Following
   *  shows it; locked and off count it. */
  private runStarted(color: string): void {
    // Already on screen: the run command followed it from its own
    // reply and the stream is echoing the same start. A second replay
    // would re-apply the journal so far and double-render pulses.
    if (this.color === color) return;
    if (this.mode === 'following') {
      this.show(color);
      this.emitStatus();
    } else if (!this.pendingQueue.includes(color)) {
      // Deduplicated, so a reconnect resync and the backlog's own
      // execution_started for the same run never count twice.
      this.pendingQueue.push(color);
      this.emitStatus();
    }
  }

  /** Switch mode. Every switch empties the pending queue: following
   *  has just caught up (or is about to), and a fresh lock or off
   *  counts from now. Remembers whether runs that start take over. */
  private enter(mode: FollowMode): void {
    this.mode = mode;
    this.pendingQueue = [];
    if (this.projectId) void this.memory.update(memoryKey(this.projectId), mode === 'following');
  }

  /** Put `color` on the canvas. Replay, not follow: the run may have
   *  emitted frames already or finished, and replay paints the
   *  journaled events before continuing live. */
  private show(color: string): void {
    this.color = color;
    void this.follower.replay(color);
  }

  private clearCanvas(): void {
    this.color = undefined;
    this.follower.stop();
    this.post({ kind: 'execCleared' });
  }

  /** Post the current follow status to the webview. Public because a
   *  webview remount loses its copy of the status while the follow
   *  itself lives on extension-side: the ready handler re-seeds it. */
  emitStatus(): void {
    const status: FollowStatus = {
      mode: this.mode,
      color: this.color,
      pendingCount: this.pendingQueue.length,
    };
    this.post({ kind: 'followStatus', status });
  }
}
