// Per-project action-bar state machine.
//
// One store, one slot per project, one pinned-project pointer that
// selects which slot drives webview emissions. Three orthogonal
// concerns flow into each slot:
//
//   - Backend status (`ActionAvailability` from `weft status --json`):
//     the project's at-rest facts. Available verbs, drift bits,
//     trigger/infra rollups, per-node infra status.
//
//   - Live executions: a Set of currently-running colors on the
//     project. Seeded from status fetch's `last_color` + `last_status`,
//     mutated by SSE `execution_started/completed/failed`.
//
//   - Follow state: which color the user is watching, and in what
//     mode (latest tracks the newest live exec; pinned holds on a
//     specific color the user picked from the right sidebar).
//
// CLI in-flight + error overlay sit on top of all three.
//
// The reducer computes "watched-live color" by intersecting follow
// state with running colors. When that intersection is non-empty,
// the bar shows Stop and cancels that color; otherwise the bar
// shows Run/Activate. The infra and trigger sections are
// independent: they read the backend status directly and never
// look at follow or running.

import type {
  ActionBarState,
  ActionBarOverlay,
  ActionAvailability,
  ActionVerb,
  BackendSnapshot,
  CliEvent,
} from './shared/protocol';

interface FollowState {
  mode: 'latest' | 'pinned';
  color: string | undefined;
}

interface Slot {
  backend: ActionAvailability | undefined;
  runningColors: Set<string>;
  follow: FollowState;
  cli: {
    verb: ActionVerb;
    phase: CliEvent['phase'];
    detail?: Record<string, unknown>;
  } | undefined;
  /// HTTP-driven verb awaiting confirmation. Currently only used
  /// for Stop on a running execution: set when the user clicks
  /// Stop, cleared when an SSE terminal event arrives for the
  /// targeted color (or when a status fetch reveals the color is
  /// no longer running).
  pendingAction: {
    verb: ActionVerb;
    message: string;
    /// Color the verb targets. The pending state clears as soon
    /// as `markExecutionFinished(color)` matches.
    color: string;
  } | undefined;
  error: { verb: ActionVerb; message: string } | undefined;
}

function emptySlot(): Slot {
  return {
    backend: undefined,
    runningColors: new Set(),
    follow: { mode: 'latest', color: undefined },
    cli: undefined,
    pendingAction: undefined,
    error: undefined,
  };
}

type Listener = (state: ActionBarState) => void;

export class ActionBarStore {
  private slots = new Map<string, Slot>();
  private pinnedProjectId: string | undefined;
  private listeners: Listener[] = [];

  setPinnedProject(projectId: string | undefined): void {
    this.pinnedProjectId = projectId;
    this.notify();
  }

  subscribe(fn: Listener): () => void {
    this.listeners.push(fn);
    fn(this.derive());
    return () => {
      this.listeners = this.listeners.filter(l => l !== fn);
    };
  }

  current(): ActionBarState {
    return this.derive();
  }

  /// Color the user is watching live, or undefined if they're
  /// looking at a finished execution / nothing running. Stop button
  /// uses this; cancel POSTs against this color.
  watchedLiveColor(projectId?: string): string | undefined {
    const id = projectId ?? this.pinnedProjectId;
    if (!id) return undefined;
    const slot = this.slots.get(id);
    if (!slot) return undefined;
    return computeWatchedLiveColor(slot);
  }

  pushStatus(
    projectId: string,
    snapshot: ActionAvailability,
    seedRunningColor: string | undefined,
  ): void {
    const slot = this.ensureSlot(projectId);
    slot.backend = snapshot;
    // Seed running colors from status fetch's most-recent
    // execution. SSE is the authoritative source for transitions
    // during a live session; this seed only matters on graph open
    // / project pin (before SSE has any history). Older parallel
    // execs aren't covered by `last_*`, but they'll appear via
    // SSE if any events fire while we're connected.
    if (seedRunningColor !== undefined) {
      slot.runningColors.add(seedRunningColor);
    }
    this.notifyIfPinned(projectId);
  }

  /// SSE `execution_started` arrived: that color is now live.
  markExecutionStarted(projectId: string, color: string): void {
    const slot = this.ensureSlot(projectId);
    slot.runningColors.add(color);
    this.notifyIfPinned(projectId);
  }

  /// SSE `execution_completed` / `execution_failed` arrived: that
  /// color is no longer live.
  markExecutionFinished(projectId: string, color: string): void {
    const slot = this.slots.get(projectId);
    if (!slot) return;
    slot.runningColors.delete(color);
    // If a pending action was targeting this color, the backend
    // has confirmed it: clear the pending state. The bar exits
    // "Cancelling..." into whatever the next derived state is.
    if (slot.pendingAction?.color === color) {
      slot.pendingAction = undefined;
    }
    this.notifyIfPinned(projectId);
  }

  /// User clicked Stop (or another HTTP-driven action). Lock the
  /// bar into a transient "waiting for backend" state until SSE
  /// confirms the action took effect.
  setPending(projectId: string, verb: ActionVerb, message: string, color: string): void {
    const slot = this.ensureSlot(projectId);
    slot.pendingAction = { verb, message, color };
    this.notifyIfPinned(projectId);
  }

  /// Clear pending action (e.g. POST failed and we want to revert
  /// to the previous derived state). On success, prefer letting
  /// `markExecutionFinished` clear it via the color match.
  clearPending(projectId: string): void {
    const slot = this.slots.get(projectId);
    if (!slot || !slot.pendingAction) return;
    slot.pendingAction = undefined;
    this.notifyIfPinned(projectId);
  }

  /// User dismissed the error banner. The error survives an
  /// auto-refresh so the user has time to read it; this is the
  /// explicit user-acked exit path.
  clearError(projectId: string): void {
    const slot = this.slots.get(projectId);
    if (!slot || !slot.error) return;
    slot.error = undefined;
    this.notifyIfPinned(projectId);
  }

  /// AutoFollow emitted a follow-state change for projectId.
  /// Mirrors mode + color into the slot so the reducer can compute
  /// the watched-live color.
  setFollow(projectId: string, mode: 'latest' | 'pinned', color: string | undefined): void {
    const slot = this.ensureSlot(projectId);
    slot.follow = { mode, color };
    this.notifyIfPinned(projectId);
  }

  cliStart(projectId: string, verb: ActionVerb): void {
    const slot = this.ensureSlot(projectId);
    slot.error = undefined;
    slot.cli = { verb, phase: 'build_start' };
    this.notifyIfPinned(projectId);
  }

  cliEvent(projectId: string, ev: CliEvent): void {
    const slot = this.slots.get(projectId);
    if (!slot || !slot.cli || slot.cli.verb !== ev.verb) return;
    if (ev.phase === 'complete') {
      slot.cli = undefined;
      this.notifyIfPinned(projectId);
      return;
    }
    if (ev.phase === 'error') {
      const msg = (ev.detail?.message as string | undefined) ?? 'unknown error';
      slot.error = { verb: ev.verb, message: msg };
      slot.cli = undefined;
      this.notifyIfPinned(projectId);
      return;
    }
    slot.cli = { verb: ev.verb, phase: ev.phase, detail: ev.detail };
    this.notifyIfPinned(projectId);
  }

  cliCrashed(projectId: string, verb: ActionVerb, message: string): void {
    const slot = this.ensureSlot(projectId);
    slot.error = { verb, message };
    slot.cli = undefined;
    this.notifyIfPinned(projectId);
  }

  cliKilled(projectId: string): void {
    const slot = this.slots.get(projectId);
    if (!slot) return;
    slot.cli = undefined;
    slot.error = undefined;
    this.notifyIfPinned(projectId);
  }

  private ensureSlot(projectId: string): Slot {
    let slot = this.slots.get(projectId);
    if (!slot) {
      slot = emptySlot();
      this.slots.set(projectId, slot);
    }
    return slot;
  }

  private notify(): void {
    const s = this.derive();
    for (const l of this.listeners) l(s);
  }

  private notifyIfPinned(projectId: string): void {
    if (this.pinnedProjectId === projectId) {
      this.notify();
    }
  }

  /// Project the pinned project's slot to a public ActionBarState.
  /// Always emits a `backend` snapshot (defaulted to "unknown" when
  /// no fetch has landed yet); overlay carries the current user-
  /// action layer; error sits alongside as a sticky banner.
  ///
  /// Overlay precedence (top wins):
  ///   1. cli set            -> cli_running    (CLI verb in flight)
  ///   2. pendingAction set  -> pending        (HTTP verb awaiting SSE)
  ///   3. watched-live color -> execution_running (live exec)
  ///   4. otherwise          -> idle
  ///
  /// Backend stays present in every overlay so the section that
  /// doesn't own the spinner can still render the live state.
  private derive(): ActionBarState {
    const slot = this.pinnedProjectId
      ? this.slots.get(this.pinnedProjectId)
      : undefined;
    return {
      backend: snapshotFromSlot(slot),
      overlay: overlayFromSlot(slot),
      ...(slot?.error ? { error: slot.error } : {}),
    };
  }
}

const DEFAULT_BACKEND: BackendSnapshot = {
  available: [],
  status: 'unknown',
  mode: 'unknown',
  infraRollup: 'none',
  runningCount: 0,
};

function snapshotFromSlot(slot: Slot | undefined): BackendSnapshot {
  const b = slot?.backend;
  if (!b) return DEFAULT_BACKEND;
  return {
    available: b.availableActions,
    status: b.projectStatus,
    mode: b.mode,
    infraRollup: b.infraRollup,
    runningCount: b.runningCount,
    ...(b.firesDeadlineUnix !== undefined
      ? { firesDeadlineUnix: b.firesDeadlineUnix }
      : {}),
  };
}

function overlayFromSlot(slot: Slot | undefined): ActionBarOverlay {
  if (!slot) return { kind: 'idle' };
  if (slot.cli) {
    return {
      kind: 'cli_running',
      verb: slot.cli.verb,
      phase: slot.cli.phase,
      ...(slot.cli.detail !== undefined ? { detail: slot.cli.detail } : {}),
    };
  }
  if (slot.pendingAction) {
    return {
      kind: 'pending',
      verb: slot.pendingAction.verb,
      message: slot.pendingAction.message,
    };
  }
  const watchedLive = computeWatchedLiveColor(slot);
  if (watchedLive && slot.backend) {
    return { kind: 'execution_running', color: watchedLive };
  }
  return { kind: 'idle' };
}

/// Pure function: which color is the user watching live on this slot?
///
///   pinned mode: the user's pinned color, only if it's currently running.
///   latest mode: any currently-running color (newest if multiple).
///                Returns undefined when nothing is running.
///
/// Returns undefined when the user is looking at a finished
/// execution (so the bar shows Run, not Stop), even if a different
/// execution is running on the same project.
function computeWatchedLiveColor(slot: Slot): string | undefined {
  if (slot.runningColors.size === 0) return undefined;
  if (slot.follow.mode === 'pinned') {
    return slot.follow.color && slot.runningColors.has(slot.follow.color)
      ? slot.follow.color
      : undefined;
  }
  // Latest mode: pick any running color. Set iteration is
  // insertion-order; the most recently added (newest exec) is
  // last. Iterating to grab the last one gives us "newest live".
  let last: string | undefined;
  for (const c of slot.runningColors) last = c;
  return last;
}
