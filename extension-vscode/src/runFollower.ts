// Subscribes to the dispatcher's run SSE stream and forwards
// node lifecycle events into the graph webview.
//
// The dispatcher exposes /events/run/{runId} as SSE. Each
// event arrives with kind + payload, tagged to match the Rust
// enum. We translate
// each event into one `firingEvent` (graph state: running /
// completed / failed / ...).
//
// The follower does NOT post node-body panel content. That panel is
// a node's display, fed by graphView's `/live` pollers, which run
// independently of run.
//
// A single follower tracks a single run at a time. Switching
// follows (the user picks a different past run in the
// sidebar) closes the current event stream and opens a new one.
//
// The stream gives the run's history first, then its live events, each
// once: the dispatcher reads the history off the run's record and goes on
// with what happens live, catching up from the record by itself when it
// falls behind. A stream that drops is opened again past the last event
// held (`after=`), so nothing is missed or painted twice, and again after
// every failed attempt for as long as the follow lasts; only a first open
// that fails loses the follow.
//
// While the person is away from a remote install (see `presence.ts`)
// the stream is closed, and coming back opens the same run
// again the way a click does.

import type * as vscode from 'vscode';

import { worthTryingAgain, type DispatcherClient } from './dispatcher';
import type { LivePause } from './presence';
import type {
  WirePayload,
  CancelCause,
  CredentialOwner,
  CorruptionSite,
  FileEditWire,
  RunPhase,
  ValueRef,
  HostMessage,
  Frame,
  LoopTerminationReason,
  NodeFiringEvent,
  Seed,
  ScopeDecidedEvent,
  ScopeState,
  SkipReason,
  StreamEnd,
} from '../../packages/weft-graph/src/protocol';

/** How long a follow whose stream dropped waits before opening it again,
 *  doubling after each try that fails, up to [`REOPEN_CEILING_MS`]. */
export const REOPEN_AFTER_MS = 1000;
/** The longest wait between two tries at opening a dropped stream again:
 *  an install that stays away is not asked every second. */
export const REOPEN_CEILING_MS = 30_000;

/** The SSE event a run's stream sends once the run's history is sent:
 *  what follows is live. */
// SYNC: CAUGHT_UP <-> crates/weft-dispatcher/src/api/events.rs CAUGHT_UP, crates/weft-cli/src/commands/follow.rs CAUGHT_UP
export const CAUGHT_UP = 'caught_up';

/** How a wait ended: its answer (where the run's record keeps it), a
 *  person skipping it, or the run giving it up (the waiting node failed
 *  with `error`). */
// SYNC: AwaitEnd <-> crates/weft-core/src/primitive.rs AwaitEnd
export type AwaitEnd = { type: 'answered'; value: ValueRef } | { type: 'skipped' } | { type: 'gave_up'; error: string };

// SYNC: DispatcherEvent <-> crates/weft-core/src/live_event.rs DispatcherEvent, weavemind/website/src/lib/graph/dispatcher-host.ts translateDispatcherEvent
// SYNC: eventId <-> crates/weft-core/src/live_event.rs IdentifiedEvent
export type DispatcherEvent = { eventId: string } & (
  // `phase`: a run of the graph (`fire`) or the setup an `infra start`
  // (`infra_setup`) or an activation (`trigger_setup`) runs.
  | { kind: 'run_started'; runId: string; entryNode: string; phase: RunPhase; subgraph?: string[]; seed?: Seed; projectId: string; atUnix: number }
  // `inheritedFrom` is set when the firing was taken from the run this
  // one was seeded from (`weft run --seed`); `providedPorts` names the
  // input ports the run was given (a value handed in at a `--from` or
  // `--group` start, or an output a node stood in with), `inheritedPorts`
  // the ones a seed's saved result sent.
  // SYNC: input origins <-> crates/weft-core/src/live_event.rs DispatcherEvent, packages/weft-graph/src/protocol.ts NodeFiringEvent, packages/weft-graph/src/webview/lib/types/index.ts NodeFiring
  // Every value an event names (a firing's input and output, the run's
  // outputs, a wait's answer) comes as where the run's record keeps it;
  // the graph reads a value when it shows it (`POST /runs/{id}/values`).
  | { kind: 'node_started'; runId: string; node: string; frames: Frame[]; input: Record<string, ValueRef>; closedPorts: string[]; providedPorts?: string[]; inheritedPorts?: Record<string, string>; inheritedFrom?: string; projectId: string; atUnix: number }
  | { kind: 'node_suspended'; runId: string; node: string; frames: Frame[]; token: string; inheritedFrom?: string; projectId: string; atUnix: number }
  | { kind: 'node_resumed'; runId: string; node: string; frames: Frame[]; token: string | null; inheritedFrom?: string; projectId: string; atUnix: number }
  // A wait of the firing ended, the first ending on record. A wait the run
  // holds in its worker never suspends or resumes the firing, so this is
  // the one event that sees every wait's ending.
  | { kind: 'wait_ended'; runId: string; node: string; frames: Frame[]; token: string; ended: AwaitEnd; inheritedFrom?: string; projectId: string; atUnix: number }
  | { kind: 'node_cancelled'; runId: string; node: string; frames: Frame[]; reason: string; inheritedFrom?: string; projectId: string; atUnix: number }
  | { kind: 'node_completed'; runId: string; node: string; frames: Frame[]; output: Record<string, ValueRef> | null; inheritedFrom?: string; projectId: string; atUnix: number }
  | { kind: 'node_failed'; runId: string; node: string; frames: Frame[]; error: string; inheritedFrom?: string; projectId: string; atUnix: number }
  | { kind: 'node_skipped'; runId: string; node: string; frames: Frame[]; closedPorts: string[]; reason: SkipReason; inheritedFrom?: string; projectId: string; atUnix: number }
  // A copy of a scope (a group, an include's call site, an included
  // body, a loop's iteration, at `frames`) was decided: open, or skipped
  // with why. A group is no node, so the graph paints it from these.
  // SYNC: DispatcherEvent scope_decided <-> crates/weft-core/src/live_event.rs DispatcherEvent::ScopeDecided, packages/weft-graph/src/protocol.ts ScopeDecidedEvent
  | ({ kind: 'scope_decided'; runId: string; projectId: string; scope: string; frames: Frame[]; inheritedFrom?: string; atUnix: number } & ScopeState)
  | { kind: 'run_completed'; runId: string; projectId: string; outputs: Record<string, Record<string, ValueRef>> | null; atUnix: number }
  | { kind: 'run_failed'; runId: string; projectId: string; error: string; atUnix: number }
  | { kind: 'run_cancelled'; runId: string; projectId: string; reason: string; cause?: CancelCause; atUnix: number }
  | { kind: 'run_tagged'; runId: string; projectId: string; tags: string[]; atUnix: number }
  // The run was erased (weft clean, a prune, the editor's delete), its
  // journal and the questions it was parked on with it. No atUnix:
  // nothing was journaled, the journal is what went.
  | { kind: 'run_deleted'; runId: string; projectId: string }
  // The run parked: every firing still open waits on a signal (a
  // person, an outside job, a timer) and no worker drives it until an
  // answer comes. Parking writes no journal row: a replay ends with it
  // when the run is parked at the time of the read.
  | { kind: 'run_parked'; runId: string; projectId: string }
  // Infra lifecycle. Emitted by the dispatcher's infra_event_bridge
  // from supervisor-written rows; drive action-bar refresh so
  // transient `stopping` / `terminating` states show up in the UI.
  | { kind: 'infra_status_changed'; projectId: string; nodeId: string; status: string }
  | { kind: 'infra_flaky'; projectId: string; nodeId: string; reason: string }
  | { kind: 'infra_recovered'; projectId: string; nodeId: string }
  | { kind: 'infra_terminated'; projectId: string; nodeId: string }
  // Project lifecycle. Emitted by the dispatcher when a project's
  // overall lifecycle state changes (independent of any single
  // run). Drive action-bar refresh so the UI sees the new
  // verb set without polling.
  | { kind: 'project_registered'; projectId: string; name: string }
  | { kind: 'project_activated'; projectId: string }
  | { kind: 'project_deactivated'; projectId: string }
  // A lifecycle axis flipped (entering/leaving activating / building /
  // cancelling_build / resyncing, or landing at rest).
  // Carries both axes; the host treats it as a status-refresh signal
  // like every other project_* event.
  | { kind: 'project_transition_changed'; projectId: string; status: string; transition: string }
  // A working copy of the project moved: its files (a save the runtime
  // synced, a program writing code) or its head (a stamp, a run, a
  // switch). The editor reads the status and the version tree again.
  // SYNC: working_copy_changed <-> crates/weft-core/src/live_event.rs DispatcherEvent::WorkingCopyChanged
  | { kind: 'working_copy_changed'; projectId: string; workingCopy: string }
  // External-URL invalidation. Trigger nodes that mint a tenant-
  // public URL emit this when the URL changes (re-activate, new
  // mount path, etc.). UI invalidates any cached chip.
  | { kind: 'trigger_url_changed'; projectId: string; nodeId: string; url: string }
  // One metered call's cost record (a provider meter's figure), attributed
  // to the exact firing. amountUsd null = the meter could not resolve the
  // figure. costId is the record's stable identity (the webview dedups on
  // it: the same journal row can arrive via both replay and live streams).
  // SYNC: cost_reported <-> crates/weft-core/src/live_event.rs CostReported, packages/weft-graph/src/protocol.ts runCost
  | { kind: 'cost_reported'; runId: string; inheritedFrom?: string; projectId: string; nodeId: string; frames: Frame[]; costId: string; service: string; amountUsd: number | null; origin: CredentialOwner; atUnix: number }
  // A firing changed a stored file's content in place: the file, the
  // versions and a readable diff, for the inspector's "Files edited" card.
  // SYNC: file_edited <-> crates/weft-core/src/live_event.rs FileEdited, packages/weft-graph/src/protocol.ts execFileEdit
  | { kind: 'file_edited'; runId: string; inheritedFrom?: string; projectId: string; nodeId: string; frames: Frame[]; edit: FileEditWire; atUnix: number }
  // Bus events: live + replay. The inspector renders one IRC-style
  // log per node per bus. `busId` is the channel's uuid (matches
  // the uuid embedded in the bus marker JSON that flows on pulses);
  // `from` on a message is the sender's registered name (stamped by
  // the bus on the producer side, never spoofed). A journaled bus's
  // window carries its `messages`; an ephemeral bus's window carries
  // only the `totals` rollup.
  // SYNC: DispatcherEvent 'bus_window' messages <-> crates/weft-core/src/bus.rs WindowedBusMessage, packages/weft-graph/src/protocol.ts BusInspectorEvent 'message'
  // SYNC: DispatcherEvent 'bus_window' totals <-> crates/weft-core/src/bus.rs BusWindowTotal, packages/weft-graph/src/protocol.ts BusInspectorEvent 'window' totals
  | { kind: 'bus_joined'; runId: string; projectId: string; busId: string; offset: number; name: string; atUnix: number }
  | { kind: 'bus_left'; runId: string; projectId: string; busId: string; offset: number; name: string; atUnix: number }
  // SYNC: DispatcherEvent 'bus_window' <-> crates/weft-journal/src/events.rs BusWindow, crates/weft-core/src/live_event.rs BusWindow, packages/weft-graph/src/protocol.ts BusInspectorEvent 'window'
  | { kind: 'bus_window'; runId: string; projectId: string; busId: string; firstOffset: number; lastOffset: number; messages: Array<{ offset: number; from: string; msgKind: string; payload?: WirePayload; payloadByteSize: number; trimmed?: boolean; atUnix: number }>; totals: Array<{ from: string; msgKind: string; count: number; bytes: number }>; atUnix: number }
  // SYNC: DispatcherEvent 'bus_closed' <-> crates/weft-journal/src/events.rs BusClosed, crates/weft-core/src/live_event.rs BusClosed, packages/weft-graph/src/protocol.ts BusInspectorEvent 'closed'
  | { kind: 'bus_closed'; runId: string; projectId: string; busId: string; offset: number; end: StreamEnd; atUnix: number }
  // Live caller connection events. One caller per run (keyed by
  // run, no busId). The webview replays the caller exchange the same
  // way it replays a bus; `payload` is the same tagged WirePayload a
  // bus window's messages carry.
  // SYNC: DispatcherEvent 'caller_window' <-> crates/weft-journal/src/events.rs CallerWindow, crates/weft-core/src/live_event.rs CallerWindow, packages/weft-graph/src/protocol.ts CallerInspectorEvent 'window'
  | { kind: 'caller_connected'; runId: string; projectId: string; offset: number; protocol: string; atUnix: number }
  | { kind: 'caller_window'; runId: string; projectId: string; firstOffset: number; lastOffset: number; messages: Array<{ offset: number; direction: 'inbound' | 'outbound'; payload?: WirePayload; payloadByteSize: number; trimmed?: boolean; terminal?: boolean; atUnix: number }>; totals: Array<{ direction: 'inbound' | 'outbound'; count: number; bytes: number }>; dropped?: number[]; atUnix: number }
  | { kind: 'caller_errored'; runId: string; projectId: string; offset: number; message: string; atUnix: number }
  // SYNC: DispatcherEvent 'caller_disconnected' <-> crates/weft-journal/src/events.rs CallerDisconnected, crates/weft-core/src/live_event.rs CallerDisconnected, packages/weft-graph/src/protocol.ts CallerInspectorEvent 'disconnected'
  | { kind: 'caller_disconnected'; runId: string; projectId: string; offset: number; reason: string; dropped?: number[]; atUnix: number }
  // Loop events. Carry the inspector groupId + parentFrames so
  // nested loops and parallel sibling iterations route to distinct
  // inspector cards.
  // SYNC: loop_instantiated <-> crates/weft-core/src/live_event.rs LoopInstantiated, packages/weft-graph/src/protocol.ts LoopInspectorEvent 'instantiated'
  | { kind: 'loop_instantiated'; runId: string; projectId: string; groupId: string; parentFrames: Frame[]; iterCap: number | null; parallel: boolean; atUnix: number }
  | { kind: 'loop_iteration_launched'; runId: string; projectId: string; groupId: string; parentFrames: Frame[]; index: number; atUnix: number }
  | { kind: 'loop_out_fired'; runId: string; projectId: string; groupId: string; parentFrames: Frame[]; index: number; doneVote?: boolean | null; atUnix: number }
  | { kind: 'loop_terminated'; runId: string; projectId: string; groupId: string; parentFrames: Frame[]; reason: LoopTerminationReason; atUnix: number }
  // Graph-level participation: a node is wired to a bus. Derived
  // dispatcher-side from emitted pulses carrying a bus marker,
  // so source AND target nodes get one BusParticipant edge each.
  // `ephemeral` is sniffed from the marker JSON, so the webview learns
  // mode the same time it learns about the bus and renders the panel
  // header badge without a separate event.
  | { kind: 'bus_participant'; runId: string; projectId: string; busId: string; nodeId: string; ephemeral: boolean }
  // One journal row the dispatcher could not apply during fold.
  // Emitted one-shot at replay time per affected row. The webview
  // groups by run and renders a muted "N journal rows corrupted"
  // collapsed disclosure in the inspector; not a banner, not red.
  | { kind: 'journal_corruption'; runId: string; projectId: string; site: CorruptionSite; reason: string });

/** The place in its run's record of the event `eventId` names
 *  (`journal:{seq}:{index}:{k}`), as a stream takes it back in `after`;
 *  undefined for an event about where the run stands (`live:...`). */
export function recordedAt(eventId: string): string | undefined {
  return eventId.startsWith('journal:') ? eventId.slice('journal:'.length) : undefined;
}

function identifiedEvent(value: unknown): DispatcherEvent {
  if (!value || typeof value !== 'object' || !('eventId' in value)
      || typeof value.eventId !== 'string' || value.eventId.length === 0) {
    throw new Error('Run event has no delivery identity');
  }
  return value as DispatcherEvent;
}

export type PostFn = (msg: HostMessage) => void;

/** A followed run that has not ended, with the project it belongs to:
 *  a display stream asks for its waits only on that project's stream. */
export interface LiveRun {
  runId: string;
  projectId: string;
}

export class RunFollower implements vscode.Disposable {
  private eventSource: { close: () => void } | undefined;
  private generation = 0;
  private reopenTimer: ReturnType<typeof setTimeout> | undefined;
  /// The run being followed, kept across a pause so coming back
  /// reopens it; cleared when the follow stops or is lost.
  private runId: string | undefined;
  /// The followed run while it has not ended: its history was sent
  /// without its end, and no event since said it ended. What `onLiveRun`
  /// listeners hear about.
  private liveRun: LiveRun | undefined;
  /// The project the followed run belongs to, from the first of its
  /// events (every run event carries it); undefined until one arrived.
  private runProject: string | undefined;
  /// The followed run's stream sent its history ([`CAUGHT_UP`]): what
  /// comes now is live.
  private caughtUp = false;
  /// The followed run's stream carried its end.
  private ended = false;
  /// The place in the run's record of the last event painted
  /// (`{seq}:{index}:{k}`): a stream opened again goes on past it.
  private after: string | undefined;
  /// The followed run's stream opened once. Only a first open that fails
  /// loses the follow; from then on a drop is opened again, as long as
  /// the follow lasts.
  private opened = false;
  /// The stream dropped and has not opened again yet: the graph shows
  /// that it is reconnecting until it does.
  private reconnecting = false;
  /// The tries at opening it again that failed since it last opened.
  private failedReopens = 0;
  private readonly liveRunListeners: Array<(run: LiveRun | undefined) => void> = [];

  constructor(
    private readonly client: DispatcherClient,
    private readonly post: PostFn,
    private readonly pause: LivePause,
  ) {
    pause.onChange((paused) => {
      if (paused) {
        // Nothing tries while paused, so nothing says it is reconnecting.
        this.close();
        this.endReconnecting();
      } else if (this.runId) this.start(this.runId);
    });
  }

  /** Hear which followed run has not ended yet, every time that
   *  changes: a run once its history was sent without an end in it, and
   *  once one of its events said which project it belongs to, then
   *  undefined when it ends, the follow is lost or stopped, or another
   *  run is followed. The graph asks for the displays of that run's
   *  waits while it is set. */
  onLiveRun(listener: (run: LiveRun | undefined) => void): void {
    this.liveRunListeners.push(listener);
  }

  private setLiveRun(run: LiveRun | undefined): void {
    // A run's project never changes, so its id says whether this changed.
    if (run?.runId === this.liveRun?.runId) return;
    this.liveRun = run;
    for (const listener of this.liveRunListeners) listener(run);
  }

  /** Name the followed run live once its history was sent without an
   *  end and its project is known. */
  private announceLive(): void {
    if (this.caughtUp && !this.ended && this.runId && this.runProject) {
      this.setLiveRun({ runId: this.runId, projectId: this.runProject });
    }
  }

  /** Paint a run from its start and keep following it, so a still-running
   *  run stays live. Called when the user clicks a run in the sidebar. */
  async replay(runId: string): Promise<void> {
    this.close();
    this.endReconnecting();
    this.runId = runId;
    this.runProject = undefined;
    this.ended = false;
    this.caughtUp = false;
    this.after = undefined;
    this.opened = false;
    this.failedReopens = 0;
    this.setLiveRun(undefined);
    this.post({ kind: 'runReset', runId });
    // Coming back opens it.
    if (this.pause.paused()) return;
    this.start(runId);
  }

  /** Open the run's stream past what was painted (from its start when
   *  nothing was). */
  private start(runId: string): void {
    this.close();
    if (this.ended) return;
    const generation = this.generation;
    const isCurrent = () => this.generation === generation;
    const lost = (reason: 'closed' | 'error') => {
      if (!isCurrent()) return;
      this.stop();
      this.post({ kind: 'followLost', runId, reason });
    };
    /** The stream dropped, or could not be opened again after a drop:
     *  open it again past the last event painted, unless the run ended
     *  (what its record gets after its end, a meter settling a call, is
     *  not worth a stream of its own). A first open that fails loses the
     *  follow instead, and so does an open the install refuses for good
     *  (the run is gone, the key refused): only an install out of reach
     *  or busy for now is tried again ([`worthTryingAgain`]), waiting
     *  longer after each try. */
    const dropped = (why: string, reason: 'closed' | 'error', err?: unknown) => {
      if (!isCurrent()) return;
      if (this.ended) {
        this.close();
        return;
      }
      if (!this.opened || (err !== undefined && !worthTryingAgain(err))) {
        lost(reason);
        return;
      }
      this.close();
      this.reconnecting = true;
      this.post({ kind: 'followReconnecting', runId, why });
      const wait = Math.min(REOPEN_AFTER_MS * 2 ** this.failedReopens, REOPEN_CEILING_MS);
      this.failedReopens++;
      this.reopenTimer = setTimeout(() => {
        this.reopenTimer = undefined;
        if (this.runId === runId && !this.pause.paused()) this.start(runId);
      }, wait);
    };
    const query = this.after === undefined ? '' : `?after=${encodeURIComponent(this.after)}`;
    this.eventSource = this.client.subscribe(
      `/events/run/${runId}${query}`,
      (ev) => {
        if (!isCurrent()) return;
        if (ev.event === CAUGHT_UP) {
          this.caughtUp = true;
          this.announceLive();
          return;
        }
        let event: DispatcherEvent;
        try {
          event = identifiedEvent(JSON.parse(ev.data));
        } catch (err) {
          console.warn('[weft/runFollower] bad SSE payload', err);
          lost('error');
          return;
        }
        const at = recordedAt(event.eventId);
        if (at !== undefined) this.after = at;
        this.apply(event);
      },
      {
        onOpen: () => {
          if (!isCurrent()) return;
          this.opened = true;
          this.failedReopens = 0;
          this.endReconnecting();
        },
        onClosed: () => dropped('the stream ended', 'closed'),
        onError: (err) => {
          console.warn('[weft/runFollower] live follow dropped', err);
          dropped(err instanceof Error ? err.message : String(err), 'error', err);
        },
      },
    );
  }

  /** Stop following: nothing reopens it. */
  stop(): void {
    this.endReconnecting();
    this.runId = undefined;
    this.setLiveRun(undefined);
    this.close();
  }

  /** Tell the graph the followed run is no longer reconnecting, if it
   *  was. */
  private endReconnecting(): void {
    if (!this.reconnecting) return;
    this.reconnecting = false;
    if (this.runId) this.post({ kind: 'followReconnecting', runId: this.runId, why: null });
  }

  /** Close the stream, keeping which run it was and how far it was
   *  painted (a pause, or a dropped stream, opens it again). */
  private close(): void {
    this.generation++;
    if (this.reopenTimer !== undefined) clearTimeout(this.reopenTimer);
    this.reopenTimer = undefined;
    this.eventSource?.close();
    this.eventSource = undefined;
  }

  dispose(): void {
    this.stop();
  }

  private apply(e: DispatcherEvent): void {
    this.runProject ??= e.projectId;
    switch (e.kind) {
      case 'node_started': {
        const firingEvent: NodeFiringEvent = {
          nodeId: e.node,
          atUnix: e.atUnix,
          state: 'running',
          frames: e.frames,
          input: e.input,
          closedPorts: e.closedPorts,
          providedPorts: e.providedPorts,
          inheritedPorts: e.inheritedPorts,
          inheritedFrom: e.inheritedFrom,
        };
        this.post({ kind: 'firingEvent', event: firingEvent });
        break;
      }
      case 'node_suspended': {
        // SSE 'node_suspended' = engine 'WaitingForInput'. Use the
        // Rust-side label directly so the inspector renders one
        // canonical state instead of two names (suspended-via-SSE
        // vs waiting_for_input-via-fold).
        const firingEvent: NodeFiringEvent = {
          nodeId: e.node,
          atUnix: e.atUnix,
          state: 'waiting_for_input',
          frames: e.frames,
          inheritedFrom: e.inheritedFrom,
        };
        this.post({ kind: 'firingEvent', event: firingEvent });
        break;
      }
      case 'node_resumed': {
        // A resume is rendered as a re-dispatch (the row returns to
        // `running`), flagged `resumed` so the reducer keeps the
        // firing's accumulated per-attempt state (port warnings)
        // instead of resetting it like a fresh start. The delivered
        // value shows up in the run replay itself, so the event
        // carries no resume payload.
        const firingEvent: NodeFiringEvent = {
          nodeId: e.node,
          atUnix: e.atUnix,
          state: 'running',
          resumed: true,
          frames: e.frames,
          inheritedFrom: e.inheritedFrom,
        };
        this.post({ kind: 'firingEvent', event: firingEvent });
        break;
      }
      case 'node_cancelled': {
        const firingEvent: NodeFiringEvent = {
          nodeId: e.node,
          atUnix: e.atUnix,
          state: 'cancelled',
          frames: e.frames,
          error: e.reason,
          inheritedFrom: e.inheritedFrom,
        };
        this.post({ kind: 'firingEvent', event: firingEvent });
        break;
      }
      case 'node_completed': {
        const firingEvent: NodeFiringEvent = {
          nodeId: e.node,
          atUnix: e.atUnix,
          state: 'completed',
          frames: e.frames,
          output: e.output ?? undefined,
          inheritedFrom: e.inheritedFrom,
        };
        this.post({ kind: 'firingEvent', event: firingEvent });
        break;
      }
      case 'node_failed': {
        const firingEvent: NodeFiringEvent = {
          nodeId: e.node,
          atUnix: e.atUnix,
          state: 'failed',
          frames: e.frames,
          error: e.error,
          inheritedFrom: e.inheritedFrom,
        };
        this.post({ kind: 'firingEvent', event: firingEvent });
        break;
      }
      case 'node_skipped': {
        const firingEvent: NodeFiringEvent = {
          nodeId: e.node,
          atUnix: e.atUnix,
          state: 'skipped',
          frames: e.frames,
          closedPorts: e.closedPorts,
          skipReason: e.reason,
          inheritedFrom: e.inheritedFrom,
        };
        this.post({ kind: 'firingEvent', event: firingEvent });
        break;
      }
      case 'scope_decided': {
        const copy = { scope: e.scope, frames: e.frames, inheritedFrom: e.inheritedFrom, atUnix: e.atUnix };
        const event: ScopeDecidedEvent = e.state === 'open'
          ? { ...copy, state: 'open' }
          : { ...copy, state: 'skipped', reason: e.reason };
        this.post({ kind: 'scopeDecided', event });
        break;
      }
      case 'run_completed':
      case 'run_failed':
        this.ended = true;
        this.setLiveRun(undefined);
        this.post({
          kind: 'runTerminal',
          runId: e.runId,
          state: e.kind === 'run_completed' ? 'completed' : 'failed',
          atUnix: e.atUnix,
        });
        break;
      case 'run_cancelled':
        this.ended = true;
        this.setLiveRun(undefined);
        // A cancel carries why: the text and, on every row written since
        // the cause existed, the structured value (a sibling run's stop
        // names the run and the tag).
        this.post({
          kind: 'runTerminal',
          runId: e.runId,
          state: 'cancelled',
          reason: e.reason,
          cause: e.cause,
          atUnix: e.atUnix,
        });
        break;
      case 'run_tagged':
        this.post({ kind: 'runTags', runId: e.runId, tags: e.tags });
        break;
      case 'run_deleted':
        // Nothing to paint: the host drops the follow if this was the
        // run on screen, and the tree and the bar re-read.
        break;
      case 'run_parked':
        // Nothing to paint: every open firing already shows waiting
        // from its own `node_suspended`, and the waits keep their
        // displays. The host re-reads the bar and the runs tree, where
        // the run now reads waiting.
        break;
      case 'bus_joined':
        // Forward `offset` on every bus event so the inspector can
        // dedupe replay-vs-live, render a stable row id for
        // debugging, and (in future) request gaps after SSE
        // reconnect.
        this.post({
          kind: 'busEvent',
          event: { kind: 'joined', busId: e.busId, offset: e.offset, name: e.name, atUnix: e.atUnix },
        });
        break;
      case 'bus_left':
        this.post({
          kind: 'busEvent',
          event: { kind: 'left', busId: e.busId, offset: e.offset, name: e.name, atUnix: e.atUnix },
        });
        break;
      case 'bus_window':
        // One journal row per aggregation window. A journaled bus's
        // window unpacks into per-message panel lines (boundaries and
        // senders kept); an ephemeral window carries only the rollup
        // and renders as one summary line.
        for (const m of e.messages) {
          this.post({
            kind: 'busEvent',
            event: {
              kind: 'message',
              busId: e.busId,
              offset: m.offset,
              from: m.from,
              msgKind: m.msgKind,
              payload: m.payload,
              payloadByteSize: m.payloadByteSize,
              trimmed: m.trimmed,
              atUnix: m.atUnix,
            },
          });
        }
        // An empty `messages` list IS the ephemeral discriminator: the
        // backend never emits an empty window row, so a window without
        // messages always carries the rollup.
        if (e.messages.length === 0) {
          this.post({
            kind: 'busEvent',
            event: {
              kind: 'window',
              busId: e.busId,
              offset: e.firstOffset,
              lastOffset: e.lastOffset,
              totals: e.totals.map((t) => ({
                from: t.from,
                msgKind: t.msgKind,
                count: t.count,
                bytes: t.bytes,
              })),
              atUnix: e.atUnix,
            },
          });
        }
        break;
      case 'bus_closed':
        this.post({
          kind: 'busEvent',
          event: { kind: 'closed', busId: e.busId, offset: e.offset, end: e.end, atUnix: e.atUnix },
        });
        break;
      case 'bus_participant':
        this.post({
          kind: 'busParticipant',
          busId: e.busId,
          nodeId: e.nodeId,
          meta: { ephemeral: e.ephemeral },
        });
        break;
      // Live caller connection: forwarded as one `callerEvent` stream
      // (one caller per run, keyed by run, so no busId). The
      // webview replays it like a bus panel.
      case 'caller_connected':
        this.post({
          kind: 'callerEvent',
          event: { kind: 'connected', offset: e.offset, protocol: e.protocol, atUnix: e.atUnix },
        });
        break;
      case 'caller_window':
        this.post({
          kind: 'callerEvent',
          event: {
            kind: 'window',
            firstOffset: e.firstOffset,
            lastOffset: e.lastOffset,
            messages: e.messages.map((m) => ({
              offset: m.offset,
              direction: m.direction,
              payload: m.payload,
              payloadByteSize: m.payloadByteSize,
              trimmed: m.trimmed,
              terminal: m.terminal,
              atUnix: m.atUnix,
            })),
            totals: e.totals,
            dropped: e.dropped,
            atUnix: e.atUnix,
          },
        });
        break;
      case 'caller_errored':
        this.post({
          kind: 'callerEvent',
          event: { kind: 'errored', offset: e.offset, message: e.message, atUnix: e.atUnix },
        });
        break;
      case 'caller_disconnected':
        this.post({
          kind: 'callerEvent',
          event: { kind: 'disconnected', offset: e.offset, reason: e.reason, dropped: e.dropped, atUnix: e.atUnix },
        });
        break;
      case 'loop_instantiated':
        this.post({
          kind: 'loopEvent',
          event: {
            kind: 'instantiated',
            groupId: e.groupId,
            parentFrames: e.parentFrames,
            iterCap: e.iterCap,
            parallel: e.parallel,
          },
        });
        break;
      case 'loop_iteration_launched':
        this.post({
          kind: 'loopEvent',
          event: {
            kind: 'iteration_launched',
            groupId: e.groupId,
            parentFrames: e.parentFrames,
            index: e.index,
          },
        });
        break;
      case 'loop_out_fired':
        this.post({
          kind: 'loopEvent',
          event: {
            kind: 'out_fired',
            groupId: e.groupId,
            parentFrames: e.parentFrames,
            index: e.index,
            doneVote: e.doneVote ?? null,
          },
        });
        break;
      case 'loop_terminated':
        this.post({
          kind: 'loopEvent',
          event: {
            kind: 'terminated',
            groupId: e.groupId,
            parentFrames: e.parentFrames,
            reason: e.reason,
          },
        });
        break;
      case 'journal_corruption':
        this.post({
          kind: 'journalCorruption',
          site: e.site,
          reason: e.reason,
        });
        break;
      case 'file_edited':
        // One change a firing made to a stored file: fold it onto the
        // firing's row (the webview dedups across replay/live overlap).
        this.post({
          kind: 'execFileEdit',
          nodeId: e.nodeId,
          frames: e.frames,
          inheritedFrom: e.inheritedFrom ?? null,
          edit: e.edit,
        });
        break;
      case 'cost_reported':
        // One metered call's cost record: fold it onto the firing's row
        // (the webview dedups by costId across replay/live overlap).
        this.post({
          kind: 'runCost',
          nodeId: e.nodeId,
          frames: e.frames,
          costId: e.costId,
          service: e.service,
          // A seeded run inherits the costs of the run it was seeded
          // from. Dropping this made those read as money this run spent,
          // so a seeded run looked as expensive as the one it reused.
          inheritedFrom: e.inheritedFrom ?? null,
          amountUsd: e.amountUsd,
          origin: e.origin,
        });
        break;
      // Events handled by `autoFollow` (action-bar refresh) or
      // user-facing banners; this run-scoped follower
      // intentionally no-ops on them. Listing them keeps the
      // switch exhaustive so a new variant fails to compile here
      // until a reviewer routes it explicitly.
      case 'run_started':
        this.post({ kind: 'runScope', runId: e.runId, subgraph: e.subgraph ?? null, seed: e.seed ?? null });
        break;
      case 'infra_status_changed':
      case 'infra_flaky':
      case 'infra_recovered':
      case 'infra_terminated':
      case 'project_registered':
      case 'project_activated':
      case 'project_deactivated':
      case 'project_transition_changed':
      case 'trigger_url_changed':
      case 'working_copy_changed':
        break;
      // How a wait ended is what `weft freeze` records; the graph paints
      // the firing's own lifecycle, which the node events already carry.
      case 'wait_ended':
        break;
      default: {
        // Exhaustiveness: if a new DispatcherEvent variant is
        // added to the union without a route here, TypeScript
        // will narrow `e` to the new shape (not `never`) and
        // this assignment will fail to compile.
        const _exhaustive: never = e;
        return _exhaustive;
      }
    }
    // A run whose history held no event learns its project here.
    this.announceLive();
  }
}
