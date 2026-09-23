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
//   - Live executions: the currently-running colors on the project,
//     each with its phase (a run, or an infra / trigger setup).
//     REPLACED by every status fetch's `running` (the
//     reconciliation), mutated between fetches by SSE
//     `execution_started/completed/failed/cancelled` (the fast path).
//     A terminal event lost to a dropped stream is caught by the next
//     fetch, so a Stop button never outlives its run.
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
// shows Run/Activate. Only runs of the graph count there: a setup is
// never a run to stop. The trigger section reads the backend status
// alone; the infra section also reads whether an infra setup is
// running (`infraSetup`), so an `infra start` typed in a terminal shows
// as that verb working.

import type {
  ActionBarState,
  ActionBarActivity,
  ActionBarError,
  ActionBarOverlay,
  ActionAvailability,
  ActionErrorDetails,
  ActionErrorDiagnostic,
  ActionVerb,
  BackendSnapshot,
  BarPhase,
  CliEvent,
  ErrorVerb,
  ExecutionPhase,
  FollowMode,
} from '../../packages/weft-graph/src/protocol';
import {
  ACTIVITY_LINE_CAP,
  ACTIVITY_LINE_CHAR_CAP,
} from '../../packages/weft-graph/src/protocol';
import { backendFromSnapshot, emptyActionAvailability, parseTransition } from '../../packages/weft-graph/src/status';
import type { RunningExecution } from '../../packages/weft-graph/src/status';

interface FollowState {
  mode: FollowMode;
  color: string | undefined;
}

interface Slot {
  backend: ActionAvailability | undefined;
  /// Running colors in start order (a Map iterates in insertion
  /// order), each with what it is for. Only `fire` ones are runs the
  /// bar can stop; the setups drive the infra and trigger slots.
  running: Map<string, ExecutionPhase>;
  follow: FollowState;
  cli: {
    verb: ActionVerb;
    phase: BarPhase;
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
  error: ActionBarError | undefined;
  /// Terminal output of the verb in `cli`, or of the verb that just
  /// failed. Reset by `cliStart`, dropped on a clean `complete`, kept
  /// through an error so the details modal can show it under the
  /// failure.
  activity: ActionBarActivity | undefined;
}

function emptySlot(): Slot {
  return {
    backend: undefined,
    running: new Map(),
    follow: { mode: 'following', color: undefined },
    cli: undefined,
    pendingAction: undefined,
    error: undefined,
    activity: undefined,
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

  /// A color that is running right now on this project, or undefined
  /// if the user is looking at a finished execution or nothing runs.
  /// Stop button uses this; cancel POSTs against this color.
  watchedRunningColor(projectId?: string): string | undefined {
    const id = projectId ?? this.pinnedProjectId;
    if (!id) return undefined;
    const slot = this.slots.get(id);
    if (!slot) return undefined;
    return computeWatchedRunningColor(slot);
  }

  pushStatus(
    projectId: string,
    snapshot: ActionAvailability,
    running: readonly RunningExecution[],
  ): void {
    const slot = this.ensureSlot(projectId);
    slot.backend = snapshot;
    // The fetch is the truth about what runs: colors it does not name
    // are gone (their terminal event may have been lost to a dropped
    // stream), colors it names and the set lacks are live.
    //
    // The fetch's ORDER is the truth too, and it replaces what was
    // here rather than being merged into it. The dispatcher sends them
    // oldest first (see `ProjectExecutionsSummary::running`), so
    // taking the last is taking the newest. Keeping the old positions
    // and appending, which is what this used to do, mixed one ordering
    // into another and left "latest" following an arbitrary run.
    slot.running = new Map(running.map((r) => [r.color, r.phase]));
    const next = slot.running;
    // A Stop waiting on a run the fetch no longer lists is over: the
    // run ended, whether or not its terminal event ever arrived.
    if (slot.pendingAction && !next.has(slot.pendingAction.color)) {
      slot.pendingAction = undefined;
    }
    this.notifyIfPinned(projectId);
  }

  /// SSE `project_transition_changed` arrived: the event already
  /// carries the new transition, so the bar flips NOW ("Building...
  /// (cancel)") instead of waiting for the debounced status refetch
  /// (which still runs afterwards, as reconciliation for everything
  /// the event does not carry). No backend snapshot yet means nothing
  /// to flip; the refetch seeds the whole snapshot then.
  markTransition(projectId: string, transition: unknown): void {
    const slot = this.slots.get(projectId);
    if (!slot?.backend) return;
    slot.backend = { ...slot.backend, transition: parseTransition(transition) };
    this.notifyIfPinned(projectId);
  }

  /// SSE `execution_started` arrived: that color is now live.
  markExecutionStarted(projectId: string, color: string, phase: ExecutionPhase): void {
    const slot = this.ensureSlot(projectId);
    slot.running.set(color, phase);
    this.notifyIfPinned(projectId);
  }

  /// SSE `execution_completed` / `execution_failed` arrived: that
  /// color is no longer live.
  markExecutionFinished(projectId: string, color: string): void {
    const slot = this.slots.get(projectId);
    if (!slot) return;
    slot.running.delete(color);
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
    // The terminal output that survived the failure was kept FOR the
    // failure, so it goes with it. Unless a verb is RUNNING: a parse
    // banner can go up (and be dismissed) in the middle of a build,
    // and that build's log belongs to the build, not to the banner.
    if (!slot.cli) slot.activity = undefined;
    this.notifyIfPinned(projectId);
  }

  /// Clear the error ONLY if it was raised by `verb`. A system-side
  /// source (parse-on-keystroke, catalog load) raises a sticky error
  /// that the user never dismisses by hand: every half-typed edit fails
  /// to parse, so the banner must clear itself on that source's next
  /// SUCCESS. Scoped to the verb so a parse success doesn't wipe an
  /// unrelated error (a failed run, say) sitting in the same slot.
  clearErrorIfVerb(projectId: string, verb: ErrorVerb): void {
    const slot = this.slots.get(projectId);
    if (!slot || slot.error?.verb !== verb) return;
    slot.error = undefined;
    this.notifyIfPinned(projectId);
  }

  /// AutoFollow emitted a follow-state change for projectId.
  /// Mirrors mode + color into the slot so the reducer can compute
  /// the watched-live color.
  setFollow(projectId: string, mode: FollowMode, color: string | undefined): void {
    const slot = this.ensureSlot(projectId);
    slot.follow = { mode, color };
    this.notifyIfPinned(projectId);
  }

  /// A verb is in flight from the moment of the CLICK: the gated verbs
  /// start in the extension-side 'preflight' phase (the saved-state
  /// check before any CLI spawn), everything else at 'build_start'.
  cliStart(projectId: string, verb: ActionVerb, phase: BarPhase = 'build_start'): void {
    const slot = this.ensureSlot(projectId);
    slot.error = undefined;
    slot.cli = { verb, phase };
    // The previous verb's terminal output belongs to the previous verb.
    slot.activity = { verb, lines: [] };
    this.notifyIfPinned(projectId);
  }

  /// The running verb printed to the terminal. `text` is a raw chunk
  /// off the child's stderr (or a stdout line that was not an event),
  /// so it may hold any number of newlines and may end mid-line; the
  /// store splits it and keeps the last `ACTIVITY_LINE_CAP` lines.
  ///
  /// Ignored when it does not belong to the verb the bar is showing:
  /// a late chunk from a killed child must not scribble on the next
  /// verb's log.
  cliLog(projectId: string, verb: ActionVerb, text: string): void {
    const slot = this.slots.get(projectId);
    if (!slot?.activity || slot.activity.verb !== verb) return;
    const added = splitLogChunk(text);
    if (added.length === 0) return;
    const lines = [...slot.activity.lines, ...added];
    slot.activity = {
      verb,
      lines: lines.length > ACTIVITY_LINE_CAP
        ? lines.slice(lines.length - ACTIVITY_LINE_CAP)
        : lines,
    };
    this.notifyIfPinned(projectId);
  }

  cliEvent(projectId: string, ev: CliEvent): void {
    const slot = this.slots.get(projectId);
    if (!slot || !slot.cli) return;
    // A failure that reached the CLI's main unreported (a refused
    // spec, a bad flag) is emitted with no verb. The HOST stamps the
    // verb it spawned onto those before they get here (it is the only
    // party that knows which child wrote the line), so an event whose
    // verb is not this slot's is a late word from a verb the bar has
    // already moved on from.
    if (slot.cli.verb !== ev.verb) return;
    if (ev.phase === 'complete') {
      slot.cli = undefined;
      // Nothing went wrong, so nobody needs the build log.
      slot.activity = undefined;
      this.notifyIfPinned(projectId);
      return;
    }
    if (ev.phase === 'error') {
      slot.error = errorFromCliEvent(slot.cli.verb, ev);
      slot.cli = undefined;
      this.notifyIfPinned(projectId);
      return;
    }
    slot.cli = { verb: slot.cli.verb, phase: ev.phase, detail: ev.detail };
    this.notifyIfPinned(projectId);
  }

  cliCrashed(
    projectId: string,
    verb: ActionVerb,
    message: string,
    details?: ActionErrorDetails,
  ): void {
    const slot = this.ensureSlot(projectId);
    // A verb that died after the user started a NEXT one: its exit is
    // real, but the bar has moved on. Reporting it here would kill the
    // running verb's spinner and hang its output under somebody else's
    // error.
    if (slot.cli && slot.cli.verb !== verb) return;
    // The process exiting non-zero after its own `error` event is the
    // same failure twice; the event said what went wrong (`unknown
    // node 'one.trim'`), the exit only says "exited 1". Keep the
    // event's unless the crash brought a headline of its own.
    const said = slot.error?.verb === verb;
    const crashSays = details?.diagnostics.length;
    if (said && !crashSays) {
      slot.cli = undefined;
      this.notifyIfPinned(projectId);
      return;
    }
    slot.error = { verb, message, ...(details ? { details } : {}) };
    slot.cli = undefined;
    this.notifyIfPinned(projectId);
  }

  /// Generic error setter for non-CLI failures (edit failures, parse
  /// failures, anything else that wants to surface a problem to the
  /// action bar). Same shape as cliCrashed. `verb` is `ErrorVerb`
  /// (the CLI verb superset) so parse/catalog/etc. land here without
  /// being forced to pretend they're a 'run'.
  setError(
    projectId: string,
    verb: ErrorVerb,
    message: string,
    details?: ActionErrorDetails,
  ): void {
    const slot = this.ensureSlot(projectId);
    slot.error = { verb, message, ...(details ? { details } : {}) };
    this.notifyIfPinned(projectId);
  }

  /// The user stopped `verb` (or abandoned it before it spawned): the
  /// bar goes back to rest and its output goes with it.
  ///
  /// Ignored unless `verb` is the one ON the bar. A stop belongs to the
  /// verb it was aimed at, so it must not wipe the next verb's state,
  /// and it must not wipe a FAILURE either: the CLI exits non-zero
  /// after emitting its error, so a Stop pressed while that banner is
  /// up arrives here with nothing running and would take the message
  /// the user was reading with it.
  cliKilled(projectId: string, verb: ActionVerb): void {
    const slot = this.slots.get(projectId);
    if (!slot) return;
    if (!slot.cli || slot.cli.verb !== verb) return;
    slot.cli = undefined;
    slot.error = undefined;
    slot.activity = undefined;
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
      infraSetup: [...(slot?.running.values() ?? [])].includes('infra_setup'),
      ...(slot?.error ? { error: slot.error } : {}),
      ...(slot?.activity && slot.activity.lines.length > 0
        ? { activity: slot.activity }
        : {}),
    };
  }
}

function snapshotFromSlot(slot: Slot | undefined): BackendSnapshot {
  // The shared projection: one derivation for both hosts, and the
  // same empty snapshot before any status has arrived.
  return backendFromSnapshot(slot?.backend ?? emptyActionAvailability());
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
  const watchedLive = computeWatchedRunningColor(slot);
  if (watchedLive && slot.backend) {
    return { kind: 'execution_running', color: watchedLive };
  }
  return { kind: 'idle' };
}

/// Pure function: which running color does the bar act on for this slot?
///
///   locked: the locked color, only if it's currently running.
///   off:    undefined; no run is on screen to stop.
///   following: the most recently started of the running colors.
///              Returns undefined when nothing runs.
///
/// "Most recently started" is the last element of an insertion-ordered set, and that
/// really is the newest from both directions: a run arriving on the
/// live stream is appended as it starts, and a status fetch replaces
/// the whole set with the dispatcher's own oldest-first list.
///
/// Returns undefined when the user is looking at a finished
/// execution (so the bar shows Run, not Stop), even if a different
/// execution is running on the same project.
function computeWatchedRunningColor(slot: Slot): string | undefined {
  // Only runs: a setup is its verb working, shown in its own slot.
  const runs = [...slot.running].filter(([, phase]) => phase === 'fire').map(([color]) => color);
  if (runs.length === 0) return undefined;
  if (slot.follow.mode === 'off') return undefined;
  if (slot.follow.mode === 'locked') {
    return slot.follow.color && runs.includes(slot.follow.color)
      ? slot.follow.color
      : undefined;
  }
  // Following: the newest running run. Map iteration is
  // insertion-order, and both things that fill it put the newest
  // last (see the note above), so the last one is the answer.
  return runs[runs.length - 1];
}


/// Split a raw output chunk into the lines the bar keeps.
///
/// A chunk off a pipe ends wherever the OS split it, and a tool that
/// repaints a progress line (docker, cargo) separates its frames with
/// carriage returns rather than newlines. Both are cut here, empty
/// pieces dropped, and each surviving line capped so one runaway line
/// cannot fill the modal.
function splitLogChunk(text: string): string[] {
  const out: string[] = [];
  for (const piece of text.split(/\r\n|\r|\n/)) {
    const line = piece.trimEnd();
    if (!line.trim()) continue;
    out.push(
      line.length <= ACTIVITY_LINE_CHAR_CAP
        ? line
        : `${line.slice(0, ACTIVITY_LINE_CHAR_CAP)}...`,
    );
  }
  return out;
}

/// Truncate a string at `maxLen` characters with an explicit suffix
/// so the modal can render a wire-drift dump without blowing up on a
/// runaway diagnostics array.
function truncateForModal(s: string, maxLen: number): string {
  return s.length <= maxLen ? s : `${s.slice(0, maxLen)}... [truncated ${s.length - maxLen} chars]`;
}

/// Build an ActionBarError from a CliEvent error phase. The CLI's
/// JSON detail field is a free-form record; pull the conventional
/// fields out and fold anything else into raw. When required fields
/// (message / what / stage) are missing, fall back to placeholders
/// AND console.error so wire drift surfaces, and stamp the raw event
/// into details.raw so the modal shows what actually arrived.
/// `verb` is the slot's, not the event's: the guard in `cliEvent`
/// already matched them, and an event's verb is optional on the wire.
function errorFromCliEvent(verb: ActionVerb, ev: CliEvent): ActionBarError {
  const d = ev.detail ?? {};
  const messageRaw = d.message as string | undefined;
  const whatRaw = d.what as string | undefined;
  const stageRaw = d.stage as string | undefined;
  // Only `message` is required by the Rust contract: the plain
  // `progress.error(message)` path (every ordinary CLI failure) emits
  // just `message`, while `what`/`stage` come only from the richer
  // `structured_error` path. So a MISSING `message` is real wire-shape
  // drift; a missing `what`/`stage` is normal and just falls back to a
  // sensible default. Treating the latter as drift fired a false alarm
  // on every ordinary failure.
  if (!messageRaw) {
    console.error('errorFromCliEvent: CLI error event missing required `message`', ev);
  }
  const message = messageRaw ?? 'unknown error';
  const what = whatRaw ?? `Running '${ev.verb}'`;
  const stage = stageRaw ?? 'cli';
  const rawField = (d.raw as string | undefined) ?? (d.stderr as string | undefined);
  // Fold the full event into raw ONLY on real drift (missing message)
  // so the user can see the actual payload. Cap the JSON dump at 4KB
  // so a runaway diagnostics array can't blow up the modal.
  const raw = !messageRaw
    ? `${rawField ? `${rawField}\n\n` : ''}wire-shape drift: full event = ${truncateForModal(JSON.stringify(ev), 4096)}`
    : rawField;
  const exitCode = typeof d.exit_code === 'number' ? d.exit_code : undefined;
  const command = (d.command as string | undefined);
  // The message is what the modal has to show when the verb packed no
  // per-item diagnostics: "unknown node 'one.trim'" is the whole story,
  // and a modal saying "no further details" under it hid it.
  const parsed = parseDiagnostics(d.diagnostics);
  const diagnostics = parsed.length > 0 ? parsed : [{ severity: 'error' as const, message }];
  const details: ActionErrorDetails = {
    what,
    stage,
    diagnostics,
    ...(raw ? { raw } : {}),
    ...(exitCode !== undefined ? { exitCode } : {}),
    ...(command ? { command } : {}),
  };
  return { verb, message, details };
}

function parseDiagnostics(value: unknown): ActionErrorDiagnostic[] {
  if (!Array.isArray(value)) return [];
  const out: ActionErrorDiagnostic[] = [];
  for (const raw of value) {
    if (!raw || typeof raw !== 'object') continue;
    const r = raw as Record<string, unknown>;
    const message = typeof r.message === 'string' ? r.message : undefined;
    if (!message) continue;
    // Closed severity set: warning | info | error. Anything else is
    // wire drift; log it and keep the unknown string in the message
    // so it's visible (don't silently relabel as 'error').
    const severityIn = typeof r.severity === 'string' ? r.severity : 'error';
    const severity: ActionErrorDiagnostic['severity'] =
      severityIn === 'warning' || severityIn === 'info' || severityIn === 'error'
        ? severityIn
        : 'error';
    let messageOut = message;
    if (severity !== severityIn) {
      console.error('parseDiagnostics: unknown severity coerced to error', severityIn, r);
      messageOut = `[unknown severity: ${severityIn}] ${message}`;
    }
    const code = typeof r.code === 'string' ? r.code : undefined;
    const hint = typeof r.hint === 'string' ? r.hint : undefined;
    const locRaw = r.location;
    let location: ActionErrorDiagnostic['location'] | undefined;
    if (locRaw && typeof locRaw === 'object') {
      const lr = locRaw as Record<string, unknown>;
      const file = typeof lr.file === 'string' ? lr.file : undefined;
      const line = typeof lr.line === 'number' ? lr.line : undefined;
      const column = typeof lr.column === 'number' ? lr.column : undefined;
      if (file && line !== undefined && column !== undefined) {
        location = { file, line, column };
      }
    }
    out.push({
      severity,
      ...(code ? { code } : {}),
      message: messageOut,
      ...(hint ? { hint } : {}),
      ...(location ? { location } : {}),
    });
  }
  return out;
}
