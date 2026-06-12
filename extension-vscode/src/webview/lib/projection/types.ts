// The optimistic-projection model. The visible graph is a pure function of
// (truth, pendingOps, layoutCode): truth is the last parse the host confirmed,
// pendingOps is the FIFO queue of edits not yet confirmed, and the projection
// re-derives whenever either changes. One queue, one rollback path: a rejected
// or invalidated op leaves the queue and the projection snaps back on its own.

import type { ProjectDefinition } from '$lib/types';
import type { EditOp, TextEdit } from '../../../shared/protocol';
import type { LayoutOp } from '$lib/layout';

/** The last host-confirmed parse: the structural source of truth the
 *  projection layers pending ops onto. Replaced wholesale on every
 *  `editApplied`, `sourceResynced`, and external `parseResult`. */
export interface Truth {
  project: ProjectDefinition;
  weftCode: string;
}

/** One optimistic edit: the op batch of a single gesture, applied to the
 *  visible projection immediately and sent to the host in the background.
 *  The batch is the unit (not the single op) because the host applies an
 *  `applyEdits` batch atomically: one request, one inverse, one rejection. */
export interface PendingOp {
  /** Local id correlating the queue entry with its undo-stack entry. */
  id: string;
  ops: EditOp[];
  /** The INVERSE of this gesture's layout change, captured once at record time
   *  (`diffLayoutOps(after, before)`). The FORWARD ops live in the engine's
   *  layout log (keyed by this op's id), which owns the visible fold and the
   *  reject-drop; this inverse is only for the confirmed UNDO entry, so undoing
   *  a confirmed edit reverts its layout regardless of later rebases. */
  layoutUndo?: LayoutOp[];
  /** `pending` = not yet sent (debounced config typing); `sending` = the
   *  applyEdits RPC is in flight. Confirmation removes the op entirely. */
  state: 'pending' | 'sending';
  /** Set on config-typing ops: `${opKind}:${nodeId}:${key}`. Each keystroke
   *  replaces the value of the existing typing op for the same field instead
   *  of appending a new queue entry, so the queue stays one-op-per-field. */
  typingKey?: string;
  /** The redo stack snapshot to restore if THIS op is REFUSED (a refused edit
   *  branched nothing, so the redo branch its record cleared must come back).
   *  Stored on the op (not threaded as a send param) so a TYPING op flushed
   *  later by `flushTypingOps` also restores correctly. Epoch-guarded at use. */
  redoRestore?: { saved: HistoryEntry[]; epoch: number };
}

/** One undo (or redo) step. Three shapes:
 *  - `pending`: the action is still an unconfirmed PendingOp; undo peels it
 *    from the queue locally (no host round-trip).
 *  - `confirmed`: the host applied it; undo replays the inverse TextEdit
 *    through the edit-server and the layout ops locally.
 *  - `reapply`: a previously-undone pending op; redo re-records its forward
 *    ops + layout as a fresh gesture. */
export type HistoryEntry =
  | { kind: 'pending'; opId: string }
  | { kind: 'confirmed'; source?: TextEdit; layout?: LayoutOp[] }
  | { kind: 'reapply'; ops: EditOp[]; layout?: LayoutOp[] };

/** What a successful edit RPC resolves with: the inverse text edit (this
 *  action's undo) and, normally, the post-edit truth (already translated to
 *  the editor's project shape). `project` is null when the host applied the
 *  edit but has no truth for THIS view to adopt: a mid-edit `.weft` doc switch
 *  (truth belongs to a graph no longer shown) or a translation failure on the
 *  reply (the edit is on disk but the webview can't render it). The engine
 *  then records the undo entry WITHOUT advancing truth. */
export interface EditRpcResult {
  inverse: TextEdit | null;
  project: ProjectDefinition | null;
  weftCode: string;
}

/** Result of folding pendingOps over a truth project: the projected visible
 *  project plus the partition of ops that applied vs failed. A `dropped`
 *  entry's reason is the apply error verbatim (the same wording a server
 *  rejection would produce), shown to the user as the rollback toast. */
export interface FoldResult {
  project: ProjectDefinition;
  kept: PendingOp[];
  dropped: Array<{ op: PendingOp; reason: string }>;
}

/** Lock state gating source-mutating graph edits while the `.weft` code is
 *  being edited (by the user in the text tab, or by an AI streaming edits).
 *  Two gates feed one effective state; layout-only gestures bypass it. */
export interface LockState {
  /** Auto-lock: engaged until this epoch-ms deadline. Every external code
   *  keystroke pushes it 1s forward; it expires on its own. Null = never
   *  engaged. */
  codeEditLockUntil: number | null;
  /** Explicit lock set via the `setGraphLogicLock` message (AI assistant)
   *  or a UI toggle. */
  lockGraphLogic: boolean;
  lockReason?: string;
}

export function isLogicLocked(lock: LockState, now: number): boolean {
  return (lock.codeEditLockUntil !== null && now < lock.codeEditLockUntil) || lock.lockGraphLogic;
}

/** The user-facing reason for a lock rejection. */
export function lockReasonText(lock: LockState, now: number): string {
  if (lock.lockGraphLogic) {
    return lock.lockReason
      ? `Graph logic locked (${lock.lockReason})`
      : 'Graph logic locked';
  }
  if (lock.codeEditLockUntil !== null && now < lock.codeEditLockUntil) {
    return 'Graph logic locked (Weft code is being edited)';
  }
  return 'Graph logic locked';
}
