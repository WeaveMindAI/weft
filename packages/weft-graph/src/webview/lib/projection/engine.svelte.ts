// The projection edit engine: owns truth, the pending-op queue, the layout
// code, the history stacks, and the graph-logic lock. Framework-agnostic
// orchestration behind a small host interface (the I/O ports), so the whole
// gesture-to-confirmation lifecycle is testable against a fake host. The
// Svelte component is a thin binding: it renders the projection and routes
// gestures here.
//
// Reactive by construction: the state fields are runes ($state), so a
// component's $derived over `engine.truth` / `engine.pendingOps` /
// `engine.layoutCode` re-runs when those are REASSIGNED (every transition that
// changes what's visible does so). The one exception is the internal `p.state`
// pending->sending flip, which mutates an op in place without reassigning the
// array; nothing visible depends on it, so it intentionally triggers no
// re-derive.

import type { ProjectDefinition } from '../types';
import type { EditOp, TextEdit } from '../../../protocol';
import { applyLayoutOps, diffLayoutOps, LAYOUT_VERB, type LayoutOp } from '../layout';
import { applyOpsToProject, foldOps, type ProjectionCatalog } from './apply';
import { runPreflight } from './preflight';
import type { EditRpcResult, HistoryEntry, LockState, PendingOp, Truth } from './types';

/** The engine's I/O ports. Production wires the host RPCs + toasts; tests
 *  wire a fake that records calls and resolves on command. */
export interface EngineHost {
	/** Send an op batch to the edit-server. Resolves with the inverse + the
	 *  post-edit truth; rejects with the server's reason. */
	applyEdits(ops: EditOp[]): Promise<EditRpcResult>;
	/** Replay a raw text edit (undo/redo). Same reply shape. */
	applyTextEdit(edit: TextEdit): Promise<EditRpcResult>;
	/** Fetch the host's current truth after a rejection. Null = source
	 *  doesn't parse right now (keep the previous truth). */
	resyncSource(): Promise<{ project: ProjectDefinition; weftCode: string } | null>;
	/** Persist the layout file. Resolves once the layout is durably on the
	 *  host's disk (the host acks the write): while any persist is
	 *  outstanding, the engine refuses to adopt a layout echoed back by a
	 *  parse, because that echo was read from a disk state the write had
	 *  not reached yet and adopting it would erase the just-saved
	 *  positions. */
	persistLayout(layoutCode: string): Promise<void>;
	/** Surface a user-facing notice (rejection/rollback/undo failures). */
	notify(title: string, description: string): void;
	/** A preflight rejection may leave stale gesture visuals (xyflow already
	 *  moved nodes mid-drag); the binding re-derives the render here. */
	snapBack(): void;
	/** A typing flush went out (the binding flashes its save indicator). */
	flashSave(): void;
	/** Clock, injected for lock-window tests. */
	now(): number;
}

const MAX_HISTORY = 100;
const TYPING_FLUSH_MS = 250;

/** A gesture's layout half: a PURE transform of the current layout text to the
 *  new layout text. The engine runs it against the right layout (the durable
 *  base for a layout-only gesture, the visible layout for a source gesture)
 *  and captures the diff, so the binding never reaches into engine state. */
export type LayoutMutator = (layout: string) => string;

/** One layer in the layout log: a chronological set of forward layout ops and
 *  who owns them. `owner` is a pending source op's id (dropped if that op is
 *  rejected) or the sentinel `COMMITTED` for a layout-only gesture (durable,
 *  never dropped, lives in the log only until the next rebase). `routed`
 *  distinguishes WHY an op-owned layer exists: the op's OWN forward layout
 *  (part of the gesture; rolls back wholesale with a rejection) versus ops
 *  ROUTED to it from an independent gesture (a reflow that touched the op's
 *  optimistic node; on rejection those are re-judged, not blindly dropped:
 *  a position of a node that still exists is rescued). */
interface LayoutLayer {
	owner: string;
	ops: LayoutOp[];
	routed?: boolean;
}
const COMMITTED = '__committed__';

export class ProjectionEngine {
	// ── Reactive state ───────────────────────────────────────────────────
	truth = $state.raw<Truth>({ project: { id: '', nodes: [], edges: [], createdAt: '', updatedAt: '' }, weftCode: '' });
	pendingOps = $state.raw<PendingOp[]>([]);
	// Layout as a base + an ordered LOG of layers, mirroring how the project is
	// truth + a queue of ops. `layoutBase` is the rebased durable layout; the
	// log holds every layout change still "in flight" (a pending source op's
	// forward layout, OR a layout-only gesture made while source ops are in
	// flight). The VISIBLE layout (`layoutCode`) folds the log over the base in
	// CHRONOLOGICAL order, so the latest change to any key wins, and dropping
	// one layer (a rejected source op) leaves every LATER layer intact. The log
	// rebases into the base the moment no source op is pending (see
	// `maybeRebaseLayout`), so it stays short and base==visible at rest.
	layoutBase = $state('');
	layoutLog = $state.raw<LayoutLayer[]>([]);
	undoStack = $state.raw<HistoryEntry[]>([]);
	redoStack = $state.raw<HistoryEntry[]>([]);
	// Graph-logic lock. Gate 1: sliding auto-lock deadline (external code
	// keystrokes). Gate 2: explicit lock (AI assistant / UI toggle).
	codeEditLockUntil = $state<number | null>(null);
	lockGraphLogic = $state(false);
	lockReason = $state<string | undefined>(undefined);

	// ── Internals ────────────────────────────────────────────────────────
	private readonly host: EngineHost;
	private readonly catalog: ProjectionCatalog;
	// All mutations (gestures + undo/redo replay) run through one serialized
	// chain: they share the queue/stacks/layout and each awaits a round-trip.
	private chain: Promise<void> = Promise.resolve();
	// Bumped per forward action; an in-flight undo's redo-push checks it so a
	// dead redo branch can't resurrect.
	private redoEpoch = 0;
	private nextOpId = 0;
	// Mints each history entry's stable identity (see HistoryEntry.seq).
	private nextHistorySeq = 0;
	// Persists sent to the host whose disk write has not yet been acked.
	// While any is outstanding, a parse's layout echo predates our write and
	// must not be adopted (see applyExternalSource).
	private outstandingPersists = 0;
	private typingFlushTimer: ReturnType<typeof setTimeout> | null = null;
	// Gesture transaction buffer: recordEdit calls inside `transaction(fn)`
	// coalesce into ONE pending op + ONE history entry.
	private txBuffer: Array<{ ops: EditOp[]; mutateLayout: LayoutMutator }> | null = null;

	constructor(host: EngineHost, catalog: ProjectionCatalog, initial: Truth, layoutCode: string) {
		this.host = host;
		this.catalog = catalog;
		this.truth = initial;
		this.layoutBase = layoutCode;
	}

	/** Write the durable base to the host, tracking the ack. The count (not
	 *  a boolean) survives overlapping persists; a rejection (the request
	 *  swept on view teardown) still settles the counter. */
	private persistBase(): void {
		this.outstandingPersists++;
		void this.host.persistLayout(this.layoutBase).finally(() => {
			this.outstandingPersists--;
		});
	}

	/** The projected visible project: truth + pending ops. Recomputed on
	 *  demand; the binding caches it in a $derived. */
	visibleProject(): ProjectDefinition {
		return foldOps(this.truth.project, this.pendingOps, this.catalog).project;
	}

	/** The VISIBLE layout: the durable base + the log, folded in chronological
	 *  order. A pure function of (layoutBase, layoutLog), so dropping a layer (a
	 *  rejected source op) re-derives the layout without it while every later
	 *  layer survives, with no absolute inverse that could clobber a later
	 *  same-key change. Read by the binding as `engine.layoutCode`; reactive
	 *  because it reads the $state fields. */
	get layoutCode(): string {
		if (this.layoutLog.length === 0) return this.layoutBase;
		return applyLayoutOps(this.layoutBase, this.layoutLog.flatMap(l => l.ops));
	}

	/** Append a layer to the log. Does NOT persist: only DURABLE layout (the
	 *  base) is ever written to disk, so an optimistic (still-unconfirmed) layer
	 *  can't leave an orphan entry on disk if the webview is torn down before
	 *  the op resolves. The layer's contribution reaches disk when it rebases
	 *  into the base (`maybeRebaseLayout`). The trade-off: a COMMITTED (durable
	 *  layout-only) layer made WHILE a source op is in flight lives only in RAM
	 *  until that op resolves; a teardown in that one-round-trip window loses it.
	 *  That's accepted over the alternative (persist the optimistic fold), which
	 *  re-introduces the on-disk orphan when the in-flight op is rejected. */
	private pushLayoutLayer(owner: string, ops: LayoutOp[], routed = false): void {
		if (ops.length === 0) return;
		this.layoutLog = [...this.layoutLog, routed ? { owner, ops, routed } : { owner, ops }];
	}

	/** Drop every layer owned by `owner` (a rejected source op). The visible
	 *  layout re-derives without them; later layers are untouched. No persist:
	 *  the base never held this layer, so disk is already correct. */
	private dropLayoutLayers(owner: string): void {
		this.layoutLog = this.layoutLog.filter(l => l.owner !== owner);
	}

	/** Apply a layout op batch as a durable change (an undo/redo replay, or a
	 *  redo of an undone layout-only gesture): commit to the base when quiescent,
	 *  else push a COMMITTED layer so it orders after pending work and rebases
	 *  later. Returns the INVERSE (for the opposite history stack), or undefined
	 *  if nothing changed. The ONE durable-layout primitive, so gesture and
	 *  history replay can't drift. */
	private applyLayoutChange(ops: LayoutOp[]): LayoutOp[] | undefined {
		if (ops.length === 0) return undefined;
		const before = this.layoutCode;
		const after = applyLayoutOps(before, ops);
		if (after === before) return undefined;
		if (this.pendingOps.length === 0) {
			this.layoutBase = after;
			this.persistBase();
		} else {
			// A durable (COMMITTED) layer must never absorb a key whose NODE
			// exists only through a still-unconfirmed op: the diff below is
			// taken against the VISIBLE layout, which includes optimistic
			// entries (a just-added node's position, a scope-move's re-keyed
			// id), and committing one of those would survive the op's
			// rejection as a permanent orphan line in the saved file. Split
			// the gesture by ownership: keys of nodes TRUTH knows commit as
			// usual (a drag of a real node survives an unrelated rejection,
			// the fold's core guarantee); keys of optimistic-only nodes ride
			// a layer OWNED by the op that introduced them, so they drop
			// with it on rejection and turn durable with it at the rebase.
			const committed: LayoutOp[] = [];
			const byOwner = new Map<string, LayoutOp[]>();
			for (const op of diffLayoutOps(before, after)) {
				const owner = op.op === 'setView' ? null : this.pendingOwnerOfKey(op.id);
				if (owner === null) committed.push(op);
				else {
					const list = byOwner.get(owner);
					if (list) list.push(op);
					else byOwner.set(owner, [op]);
				}
			}
			this.pushLayoutLayer(COMMITTED, committed);
			for (const [owner, ownedOps] of byOwner) this.pushLayoutLayer(owner, ownedOps, true);
		}
		const inv = diffLayoutOps(after, before);
		return inv.length > 0 ? inv : undefined;
	}

	/** The pending op id whose fold FLIPS the existence of the node behind
	 *  layout key `id` relative to truth, or null when no pending op changes
	 *  it (the fact is durable; commit as usual). Ownership is decided by
	 *  the projection itself, folding the pending ops over truth one op at a
	 *  time, and it is SYMMETRIC: a key APPEARING optimistically (an added
	 *  node, a scope-move's new scoped id) is owned by the op that makes the
	 *  node exist, and a key DISAPPEARING optimistically (the scope-move's
	 *  OLD id, a pending delete) is owned by the op that makes it stop.
	 *  Both halves of a rename therefore ride ONE owner and drop or turn
	 *  durable together; judging only the appearing half made a rejected
	 *  scope-move keep the old key's REMOVAL durably, erasing the node's
	 *  saved position. Total by construction: an op with no layout half of
	 *  its own still owns its node's keys. */
	private pendingOwnerOfKey(id: string): string | null {
		let present = this.truth.project.nodes.some(n => n.id === id);
		let working = this.truth.project;
		let owner: string | null = null;
		for (const p of this.pendingOps) {
			try {
				working = applyOpsToProject(working, p.ops, this.catalog);
			} catch {
				// This op no longer applies (it will be dropped at the next
				// refold); it cannot flip anything.
				continue;
			}
			const now = working.nodes.some(n => n.id === id);
			if (now !== present) {
				// The LAST flip owns: it is the op that decides whether the
				// node finally exists. First-flip ownership orphaned a
				// delete-then-readd chain (the delete confirming while the
				// readd was rejected kept a position line for a dead node).
				owner = p.id;
				present = now;
			}
		}
		return owner;
	}

	/** Fold the whole log into the base and clear it, but ONLY when no source
	 *  op is still pending (so no layer can yet be dropped by a rejection).
	 *  Keeps the log short and restores `base == visible` at rest, and is the
	 *  ONE place a log layer's contribution becomes durable (persisted): a
	 *  confirmed source op's position and a layout-only drag made while ops were
	 *  in flight both reach disk here, never while still optimistic. Idempotent. */
	private maybeRebaseLayout(): void {
		if (this.layoutLog.length === 0) return;
		if (this.pendingOps.length > 0) return;
		this.layoutBase = applyLayoutOps(this.layoutBase, this.layoutLog.flatMap(l => l.ops));
		this.layoutLog = [];
		this.persistBase();
	}

	/** Settle of every queued task, including tasks a running task appends
	 *  (a redo's re-send). Loops until the chain stops growing. */
	async settled(): Promise<void> {
		let current: Promise<void>;
		do {
			current = this.chain;
			await current;
		} while (current !== this.chain);
	}

	private lock(): LockState {
		return { codeEditLockUntil: this.codeEditLockUntil, lockGraphLogic: this.lockGraphLogic, lockReason: this.lockReason };
	}

	setCodeEditTouched(): void {
		this.codeEditLockUntil = this.host.now() + 1000;
	}

	setGraphLogicLock(locked: boolean, reason?: string): void {
		this.lockGraphLogic = locked;
		this.lockReason = locked ? reason : undefined;
	}

	// ── Recording gestures ───────────────────────────────────────────────

	/** Coalesce several recordEdit calls into one gesture (one pending op,
	 *  one undo unit). Nested transactions flatten into the outermost. */
	transaction(fn: () => void): void {
		if (this.txBuffer) {
			fn();
			return;
		}
		const buffer: Array<{ ops: EditOp[]; mutateLayout: LayoutMutator }> = [];
		this.txBuffer = buffer;
		try {
			fn();
		} finally {
			this.txBuffer = null;
		}
		if (buffer.length === 0) return;
		// Compose the buffered layout mutators left-to-right into one.
		const composed: LayoutMutator = (layout) => buffer.reduce((l, e) => e.mutateLayout(l), layout);
		this.recordGesture(buffer.flatMap(e => e.ops), composed, undefined);
	}

	/** Record a user gesture. `ops` is the source half (empty = layout-only);
	 *  `mutateLayout` is the PURE layout half: `(currentLayout) => newLayout`,
	 *  run by the engine against the right layout (the durable base for a
	 *  layout-only gesture, the visible layout for a source gesture).
	 *  `typingKey` coalesces config typing into one op per field. */
	recordEdit(ops: EditOp[], mutateLayout: LayoutMutator = (l) => l, typingKey?: string): void {
		if (this.txBuffer) {
			this.txBuffer.push({ ops, mutateLayout });
			return;
		}
		this.recordGesture(ops, mutateLayout, typingKey);
	}

	/** Persist a layout-only change WITHOUT an undo entry. For positions the user
	 *  did not author: an automatic re-flow (auto-organize triggered by a node
	 *  resizing when live-display content arrives) is not a user action, so it must
	 *  not pollute the undo stack (a streaming execution would otherwise bury real
	 *  edits under dozens of reflow frames). Routes through the SAME durable-layout
	 *  primitive as `recordEdit`'s layout path, just skipping `pushHistory`. */
	persistLayoutEdit(mutateLayout: LayoutMutator): void {
		const before = this.layoutCode;
		this.applyLayoutChange(diffLayoutOps(before, mutateLayout(before)));
	}

	private recordGesture(ops: EditOp[], mutateLayout: LayoutMutator, typingKey: string | undefined): void {
		// Layout-only gesture (drag, resize, collapse): no source op, no
		// preflight (the logic lock gates source mutations only). It is durable
		// (no round-trip can reject it). With no source op in flight it commits
		// straight to the base; with ops in flight it joins the log as a
		// COMMITTED layer (so it lands chronologically AFTER them and the latest
		// change to a shared key wins, then rebases durable). One confirmed
		// history entry holds its inverse for undo.
		if (ops.length === 0) {
			const before = this.layoutCode;
			// Route through the ONE durable-layout primitive (commit-to-base when
			// quiescent, else a COMMITTED layer) so this gesture and history
			// replay can't drift. The returned inverse is the undo entry.
			const inverse = this.applyLayoutChange(diffLayoutOps(before, mutateLayout(before)));
			if (inverse) this.pushHistory({ seq: this.nextHistorySeq++, kind: 'confirmed', layout: inverse });
			return;
		}

		// Preflight produces the same {ok:false, reason} shape a server
		// rejection does, so both flow through one rollback path. It includes
		// a dry-run apply, so a gesture that passes is guaranteed to project.
		const pf = runPreflight(ops, this.visibleProject(), this.lock(), this.catalog, this.host.now());
		if (!pf.ok) {
			this.host.notify('Edit rejected', pf.reason);
			this.host.snapBack();
			return;
		}

		// Config typing: replace the existing typing op's batch in place (the
		// projection repaints) and restart the debounce; no new history entry.
		if (typingKey) {
			const existing = this.pendingOps.find(p => p.typingKey === typingKey && p.state === 'pending');
			if (existing) {
				this.pendingOps = this.pendingOps.map(p => (p === existing ? { ...p, ops } : p));
				this.armTypingFlush();
				return;
			}
		}

		// Source gesture: its forward layout joins the log as a layer OWNED by
		// the op (dropped if the op is rejected), captured as a diff against the
		// current visible layout so the fold stays chronological.
		const p: PendingOp = { id: `op-${++this.nextOpId}`, ops, state: 'pending', typingKey };
		const beforeVisible = this.layoutCode;
		const afterVisible = mutateLayout(beforeVisible);
		const forward = diffLayoutOps(beforeVisible, afterVisible);
		const layoutUndo = diffLayoutOps(afterVisible, beforeVisible);
		if (layoutUndo.length > 0) p.layoutUndo = layoutUndo;
		// A new forward action branches history (pushHistory clears redo and
		// bumps the epoch). A REFUSED edit changes nothing, so its send restores
		// the redo snapshot. Stored ON THE OP (epoch captured AFTER pushHistory)
		// so a typing op flushed later by flushTypingOps restores correctly too.
		const savedRedo = this.redoStack;
		this.pendingOps = [...this.pendingOps, p];
		this.pushLayoutLayer(p.id, forward);
		this.pushHistory({ seq: this.nextHistorySeq++, kind: 'pending', opId: p.id });
		p.redoRestore = { saved: savedRedo, epoch: this.redoEpoch };
		if (typingKey) this.armTypingFlush();
		else {
			// Sends must leave in QUEUE order. A typing op still waiting on its
			// debounce was recorded BEFORE this structural op; letting the
			// structural op jump the queue makes the host apply them reversed
			// (e.g. a config edit arriving AFTER the delete of its node, which
			// the edit-server rejects with "node not found" and rolls back a
			// gesture the user already saw succeed). Flush first.
			this.flushTypingOps();
			this.sendPendingOp(p);
		}
	}

	private pushHistory(entry: HistoryEntry): void {
		this.undoStack = [...this.undoStack, entry].slice(-MAX_HISTORY);
		// Clear redo SYNCHRONOUSLY (a redo pressed before the async
		// confirmation must already see it gone) and bump the epoch so an
		// in-flight undo's redo-push can't resurrect the dead branch.
		this.redoStack = [];
		this.redoEpoch++;
	}

	// ── The one rollback path ────────────────────────────────────────────

	/** Remove a failed/invalidated pending op: drop it from the queue (BOTH the
	 *  projection AND the visible layout re-derive without it on their own, so
	 *  there is no absolute inverse to apply and a rejected op can never clobber
	 *  a later same-key layout change), drop its history entry (the user never
	 *  saw it succeed), notify. Idempotent: an op already gone is a no-op. The
	 *  durable base is untouched (the op's forward layout never reached it), so
	 *  no persist is needed. */
	failPendingOp(p: PendingOp, reason: string): void {
		if (!this.pendingOps.some(x => x.id === p.id)) return;
		this.discardPendingOp(p);
		this.maybeRebaseLayout();      // folds surviving layers if nothing pends
		this.host.notify('Edit failed', `${reason}. Rolled back to last good state.`);
	}

	/** Remove an op, its layout layers, and its history entries, with no
	 *  side effects beyond that. `failPendingOp` adds the rebase + toast for
	 *  a rejection; the cancellation path in `sendPendingOp` uses this bare
	 *  (a dying view must not persist or toast). */
	/** Undo an UNCONFIRMED op locally (the revoke and the unsent-typing
	 *  peel): capture its full forward layout (all its layers flattened in
	 *  log order, so the redo restores the LAST position the user saw, not
	 *  the first one recorded), then remove it through the SAME re-judging
	 *  cleanup a rejection uses. Undo and rejection remove the same op, so
	 *  they must clean up identically: the blunt drop this replaced also
	 *  erased a reflow's move of a NEIGHBOR node that had been routed to
	 *  the op. Returns the reapply entry's layout half. */
	private peelOp(p: PendingOp): LayoutOp[] | undefined {
		const flattened = this.layoutLog.filter(l => l.owner === p.id).flatMap(l => l.ops);
		const dead = this.discardPendingOp(p);
		this.maybeRebaseLayout();
		// The redo restores exactly what this undo made disappear: the LAST
		// value of each key that died with the op (a routed reflow may have
		// moved the node after the gesture's own layout wrote it). Keys the
		// discard rescued still live in the layout; re-carrying them would
		// wrongly tie a surviving node's position to the redone op's fate.
		// Only setEntry ops qualify: a removeEntry is not a position (a
		// reflow that DROPPED the dead node's line would otherwise ride the
		// redo as "delete this key" and erase a saved position on replay),
		// and the key died with the op anyway, so the redo carries nothing
		// for it: the same treatment the history scrub gives dead keys.
		const lastByKey = new Map<string, LayoutOp>();
		for (const o of flattened) {
			if (o.op !== 'setEntry') continue;
			const key = `${o.verb ?? LAYOUT_VERB}:${o.id}`;
			if (dead.has(key)) lastByKey.set(key, o);
		}
		const ops = [...lastByKey.values()];
		return ops.length > 0 ? ops : undefined;
	}

	/** Insert a redo entry at its PRESS-ORDERED slot. A queued undo press
	 *  completes after a later sync revoke already pushed its entry, and
	 *  redo consumes products in reverse press order (LIFO by press), so
	 *  the stack orders by press stamp, never by which path finished
	 *  first. Invariant: EVERY redo-stack entry is stamped (both push
	 *  sites stamp, and every re-push restores an already-stamped entry). */
	private pushRedoEntry(entry: HistoryEntry & { pressStamp: number }): void {
		const at = this.redoStack.findIndex(e => e.pressStamp! > entry.pressStamp);
		this.redoStack = at < 0
			? [...this.redoStack, entry]
			: [...this.redoStack.slice(0, at), entry, ...this.redoStack.slice(at)];
	}

	/** Returns the set of layout keys (`verb:id`) that DIED with the op, so
	 *  a peel can hand redo exactly what the removal took away. */
	private discardPendingOp(p: PendingOp): Set<string> {
		this.pendingOps = this.pendingOps.filter(x => x.id !== p.id);
		// The op's owned layer ops are re-judged now that the op is gone,
		// each by the fate of ITS key's node:
		// - node dead (not in truth, no remaining pending op flips it): the
		//   key exists only through the discarded op. Its ops drop, and any
		//   HISTORY entry still holding layout ops for it (a drag of the
		//   optimistic node produced a confirmed entry whose inverse would
		//   put the dead key back) loses them too, or a single undo would
		//   write the dead node's line durably into the saved file.
		// - node still flipped by a REMAINING pending op: re-own the ops to
		//   that op (they drop or turn durable with it).
		// - node alive in truth: a setEntry is a REAL position the user saw
		//   (a reflow placed a node whose delete-then-readd chain collapsed):
		//   rescue it as COMMITTED, don't erase it with the dead op. A
		//   removeEntry, though, was CONTINGENT on the op (a rejected delete
		//   must not keep the removal of the live node's position): void it.
		// The transformation is IN PLACE: a rescued op keeps its layer's
		// position in the chronological log (pushing it at the end would
		// make the dead op's old write win over a LATER drag of the same
		// key, clobbering the fold's latest-write-wins guarantee).
		const deadKeys = new Set<string>();
		this.layoutLog = this.layoutLog.flatMap((layer): LayoutLayer[] => {
			if (layer.owner !== p.id) return [layer];
			if (!layer.routed) {
				// The op's OWN forward layout: part of the rejected gesture,
				// rolls back wholesale (an unconfirmed op's optimistic layout
				// must never outlive it). Its dead-node keys still feed the
				// history scrub.
				for (const o of layer.ops) {
					if (o.op === 'setView') continue;
					if (!this.truth.project.nodes.some(n => n.id === o.id) && this.pendingOwnerOfKey(o.id) === null) {
						deadKeys.add(`${o.verb ?? LAYOUT_VERB}:${o.id}`);
					}
				}
				return [];
			}
			const parts: LayoutLayer[] = [];
			const emit = (owner: string, o: LayoutOp): void => {
				const last = parts[parts.length - 1];
				if (last && last.owner === owner) last.ops.push(o);
				else parts.push({ owner, ops: [o], routed: true });
			};
			for (const o of layer.ops) {
				if (o.op === 'setView') {
					emit(COMMITTED, o);
					continue;
				}
				const inTruth = this.truth.project.nodes.some(n => n.id === o.id);
				const owner = this.pendingOwnerOfKey(o.id);
				if (!inTruth && owner === null) {
					deadKeys.add(`${o.verb ?? LAYOUT_VERB}:${o.id}`);
					continue;
				}
				if (o.op === 'removeEntry') continue; // contingent on p: void
				emit(owner ?? COMMITTED, o);
			}
			return parts;
		});
		const scrub = (entries: HistoryEntry[]): HistoryEntry[] =>
			entries.flatMap((e): HistoryEntry[] => {
				if (e.kind === 'pending') return e.opId === p.id ? [] : [e];
				if (!e.layout || deadKeys.size === 0) return [e];
				const kept = e.layout.filter(
					o => o.op === 'setView' || !deadKeys.has(`${o.verb ?? LAYOUT_VERB}:${o.id}`),
				);
				if (kept.length === e.layout.length) return [e];
				const layout = kept.length > 0 ? kept : undefined;
				if (e.kind === 'confirmed' && !e.source && !layout) return [];
				return [{ ...e, layout }];
			});
		this.undoStack = scrub(this.undoStack);
		this.redoStack = scrub(this.redoStack);
		return deadKeys;
	}

	/** Advance truth and re-validate the queue against it: pending ops that
	 *  no longer apply drop through `failPendingOp`. THE one truth-advance
	 *  path: edit confirmations, rejection resyncs, and external parses. */
	adoptTruth(newProject: ProjectDefinition, newWeftCode: string): void {
		const partition = foldOps(newProject, this.pendingOps, this.catalog);
		this.truth = { project: newProject, weftCode: newWeftCode };
		for (const d of partition.dropped) this.failPendingOp(d.op, d.reason);
	}

	/** A host parseResult (text-tab edit, focus change): truth replacement
	 *  always wins; pending ops re-apply on top. The host's layout is adopted
	 *  only when the editor has no unconfirmed work (otherwise the in-memory
	 *  copy already holds the ops' re-keys and the echo lags). */
	applyExternalSource(newProject: ProjectDefinition, newWeftCode: string, newLayoutCode: string): void {
		// The host's layout IS the durable base (it persisted it). Adopt it only
		// when there is no unconfirmed work; with pending ops in flight, the
		// in-memory base already holds their re-keys and the echo lags.
		if (this.pendingOps.length === 0) {
			// Invariant: no pending ops implies the log already rebased into the
			// base (every queue-removal site calls maybeRebaseLayout). If a layer
			// somehow lingers, surface it LOUDLY (the notice port, not a throw
			// that would crash the message handler and wedge truth-adoption
			// forever) and DON'T overwrite the base: our in-memory layout (base +
			// the stray layer) is ahead of the host's echo, so keep + fold ours
			// rather than dropping the user's change.
			if (this.layoutLog.length > 0) {
				this.host.notify('Layout engine warning', 'recovered a stray layout layer (please report)');
				this.maybeRebaseLayout();
			} else if (this.outstandingPersists === 0 && newLayoutCode !== this.layoutBase) {
				// Adopt the host's layout only when nothing we wrote is still
				// in transit: an echo read from disk BEFORE our un-acked save
				// landed predates our base, and adopting it would erase the
				// just-saved positions (a freshly created node then falls back
				// to the x=0 stack placement, which the anchor pass would
				// persist as if deliberate).
				this.layoutBase = newLayoutCode;
			}
		}
		this.adoptTruth(newProject, newWeftCode);
	}

	// ── Sending ──────────────────────────────────────────────────────────

	private humanEditError(reason: string): string {
		return reason === 'code-was-edited'
			? 'the Weft code was edited during the round-trip'
			: reason;
	}

	private sendPendingOp(p: PendingOp): void {
		p.state = 'sending';
		this.enqueue(async () => {
			// The op may have left the queue while this task waited (an undo
			// peeled it, or a truth advance invalidated it). A revoke that
			// beat this task marked the op for post-reply cleanup, but with
			// the request never dispatched there is nothing to invert:
			// consume the mark here or it leaks forever.
			if (!this.pendingOps.some(x => x.id === p.id)) {
				this.revokedOps.delete(p.id);
				return;
			}
			try {
				const r = await this.host.applyEdits(p.ops);
				// Revoked while in flight (undo pressed before the reply): the
				// projection already dropped the op at press time. Apply the
				// inverse NOW, still inside this chain slot, so every send
				// queued after the press lands after the revert, exactly as
				// the projection showed it. A refusal needs nothing (the op
				// never landed and the projection already agrees); a
				// cancellation is the dying-view case, nothing either.
				if (this.revokedOps.delete(p.id)) {
					if (!r.cancelled && r.inverse) {
						const reverted = await this.host.applyTextEdit(r.inverse);
						if (!reverted.cancelled && reverted.project) this.adoptTruth(reverted.project, reverted.weftCode);
					}
					return;
				}
				// A synchronous truth-advance (external parseResult) may have
				// dropped this op while the RPC was in flight. If so, its slot
				// is gone and the newer truth already won: do NOT regress truth
				// to this older reply, and do NOT stash a confirmed inverse no
				// undo will ever consume.
				if (!this.pendingOps.some(x => x.id === p.id)) return;
				// Cancelled: the view switched away and the reply will never
				// come. Stand down QUIETLY: no confirmation, no rebase (a
				// rebase would PERSIST this dying view's layout, which the
				// host would write onto the file it now watches), no toast.
				if (r.cancelled) {
					this.discardPendingOp(p);
					return;
				}
				// The op confirmed: its forward layout layer STAYS in the log and
				// becomes durable at the rebase below. Op ids are monotonic and
				// never reused, and dropLayoutLayers only ever targets an op still
				// in the queue, so a confirmed op's layer can never be dropped;
				// no owner conversion is needed.
				this.pendingOps = this.pendingOps.filter(x => x.id !== p.id);
				this.confirmHistoryEntry(p.id, { kind: 'confirmed', source: r.inverse ?? undefined, layout: p.layoutUndo });
				this.maybeRebaseLayout();
				// project===null: the host applied the edit but has no truth for
				// this view. Keep the undo entry; do NOT advance truth. The op
				// leaves the queue, so the projection falls back to pre-edit
				// truth, which is correct for the two callers: a doc switch (the
				// webview now shows a different graph, whose own parse is its
				// truth) and a translation failure (the host raised an error
				// banner; the new truth is un-renderable here anyway).
				if (r.project) this.adoptTruth(r.project, r.weftCode);
			} catch (err) {
				// A revoked op that the server then REFUSED: the projection
				// already agrees (the op is gone on both sides), so there is
				// nothing to revert, roll back, or toast. Consume the mark.
				if (this.revokedOps.delete(p.id)) return;
				// Same race on the failure side: a truth-advance already dropped
				// and rolled this op back. failPendingOp is idempotent, but bail
				// before the redo-restore + resync so a stale rejection can't
				// disturb state the newer truth already settled.
				if (!this.pendingOps.some(x => x.id === p.id)) return;
				const reason = err instanceof Error ? err.message : String(err);
				this.failPendingOp(p, this.humanEditError(reason));
				// A refused edit branched nothing: restore the redo stack its
				// record cleared. Epoch-guarded so a later forward edit that
				// legitimately re-cleared redo stays cleared. PREPEND rather
				// than overwrite: an undo that completed while this send was in
				// flight may have pushed its own redo entry onto the (then
				// empty) stack; overwriting would destroy it. Same epoch means
				// no forward edit branched, so anything now on the stack can
				// only be that undo's entry.
				if (p.redoRestore && this.redoEpoch === p.redoRestore.epoch) {
					this.redoStack = [...p.redoRestore.saved, ...this.redoStack];
				}
				// Snap to the host's authoritative post-rejection state. We do
				// NOT mirror server rejection semantics locally; one extra
				// round-trip per failure is the price of staying honest. A null
				// resync (source doesn't parse right now) keeps the previous
				// truth; the parse path delivers a fresh one once it parses.
				const t = await this.host.resyncSource();
				if (t) this.adoptTruth(t.project, t.weftCode);
			}
		});
	}

	/** The chain invariant: it is ALWAYS a resolved promise, so the next
	 *  `enqueue` runs and `settled()` never throws. A task body that rejects
	 *  (e.g. a `resyncSource` RPC dying mid-flight, which the per-task
	 *  try/catch doesn't cover) is caught here and surfaced loudly via the
	 *  notice port; the chain stays alive. */
	private enqueue(fn: () => Promise<void>): void {
		this.chain = this.chain
			.then(fn, fn)
			.catch(err => this.host.notify('Edit engine error', err instanceof Error ? err.message : String(err)));
	}

	/** Swap an op's `pending` history entry to its confirmed inverse. The
	 *  entry is always still on a stack here: undo pops inside the chain, so
	 *  it cannot outrun this confirmation (the send's own task), and a
	 *  rolled-back op never confirms (failPendingOp removed both op and
	 *  entry, and the send guard skips it). */
	private confirmHistoryEntry(opId: string, entry: Omit<HistoryEntry & { kind: 'confirmed' }, 'seq'>): void {
		const idx = this.undoStack.findIndex(e => e.kind === 'pending' && e.opId === opId);
		if (idx >= 0) {
			const next = [...this.undoStack];
			// The swap INHERITS the pending entry's seq (an entry's identity
			// survives its transformations), so a queued undo press that
			// captured the pending entry still finds its target.
			if (entry.source || entry.layout) next[idx] = { ...entry, seq: next[idx].seq };
			else next.splice(idx, 1); // nothing reversible came back: drop the entry
			this.undoStack = next;
			return;
		}
		// idx < 0: the entry aged out of MAX_HISTORY; nothing to swap.
	}

	// ── Config typing ────────────────────────────────────────────────────

	private armTypingFlush(): void {
		if (this.typingFlushTimer) clearTimeout(this.typingFlushTimer);
		this.typingFlushTimer = setTimeout(() => {
			this.typingFlushTimer = null;
			this.flushTypingOps();
		}, TYPING_FLUSH_MS);
	}

	/** Send every still-pending typing op now (debounce fired, or a verb like
	 *  Run/Activate needs the freshest source on the host's write chain). */
	flushTypingOps(): void {
		if (this.typingFlushTimer) {
			clearTimeout(this.typingFlushTimer);
			this.typingFlushTimer = null;
		}
		let sent = false;
		for (const p of this.pendingOps) {
			if (p.typingKey && p.state === 'pending') {
				this.sendPendingOp(p);
				sent = true;
			}
		}
		if (sent) this.host.flashSave();
	}

	// ── Undo / redo ──────────────────────────────────────────────────────

	// Undo and redo RESOLVE INSIDE their chain task, not at press time. The
	// stacks only reach their settled state once the tasks ahead on the chain
	// have run (a confirmation swaps the op's `pending` entry to its inverse;
	// an undo pushes its redo entry), so acting on the press-time stack reads
	// state that hasn't caught up: undo pressed during a round-trip missed the
	// inverse, and redo pressed right after undo found an empty redo stack and
	// silently dropped the press.
	//
	// An undo press captures its TARGET (the entry the user meant, which is
	// the top entry not already claimed by an earlier queued press) and undoes
	// that entry at run time wherever it then sits. Gestures recorded between
	// press and run do NOT cancel the press: a layout-only gesture above the
	// target commutes (no source text involved) and the undo proceeds; a
	// SOURCE gesture above it does not (the target's inverse TextEdit was
	// minted against a source that gesture has since changed, and replaying it
	// out of order would edit at stale offsets), so that undo refuses LOUDLY
	// instead of silently vanishing.
	// No typing flush is needed in either: the top history entry IS the newest
	// gesture, so an unsent typing op can only be undone as itself (the local
	// peel in applyHistoryEntry), never bypassed by an older entry's replay.

	/** Presses queued but not yet run, so a rapid second press targets the
	 *  entry BELOW the first press's target instead of double-claiming it. */
	private queuedUndoPresses = 0;

	/** Redo presses queued but not yet run (see redo's press-stamp guard). */
	private queuedRedoPresses = 0;

	/** Global press order across undo AND redo: stamps each redo-stack entry
	 *  with the undo press that produced it, so a queued redo press can tell
	 *  earlier-born entries (its to consume) from later-born ones (not). */
	private pressSeq = 0;

	/** Ops REVOKED while their send was in flight (undo pressed before the
	 *  reply): the projection already dropped them at press time, and their
	 *  send task, when the reply lands, applies the inverse immediately
	 *  (still inside its own chain slot, so anything recorded after the
	 *  press lands AFTER the inverse, exactly as the projection showed it). */
	private revokedOps = new Set<string>();

	/** Outstanding revoke marks. Every mark is consumed by the op's send task,
	 *  so this is 0 at rest; a non-zero count at quiescence means a mark
	 *  outlived its op. Read by the contract tests as the leak assertion. */
	get revokedOpCount(): number {
		return this.revokedOps.size;
	}

	undo(): void {
		const press = ++this.pressSeq;
		const target = this.undoStack[this.undoStack.length - 1 - this.queuedUndoPresses];
		if (!target) return;
		// An UNCONFIRMED op undoes AT PRESS TIME, not after its round-trip:
		// the projection drops it instantly, so a long optimistic chain
		// (edit, undo, edit, undo, redo...) reads coherently even while the
		// server is still at step 0, and every LATER gesture is recorded
		// against the world the user actually sees. An unsent op is simply
		// never sent; an in-flight one is marked revoked and its send task
		// applies the inverse the moment the confirmation lands, before any
		// later-queued send.
		if (target.kind === 'pending') {
			const p = this.pendingOps.find(x => x.id === target.opId);
			if (p) {
				if (p.state === 'sending') this.revokedOps.add(p.id);
				this.pushRedoEntry({ seq: this.nextHistorySeq++, pressStamp: press, kind: 'reapply', ops: p.ops, layout: this.peelOp(p) });
				return;
			}
			// The op left the queue in the same tick (confirmed: the entry
			// was swapped; rolled back: the entry is gone). Either way this
			// captured entry object is stale; fall through to the task path,
			// whose locator resolves by opId against the CURRENT stack.
		}
		this.queuedUndoPresses++;
		const epoch = this.redoEpoch;
		this.enqueue(async () => {
			this.queuedUndoPresses--;
			// Find the target now BY SEQ: rewrites (the pending->confirmed
			// swap, a rollback's layout scrub) clone the entry but inherit
			// its seq, so the press survives them all.
			const idx = this.undoStack.findIndex(e => e.seq === target.seq);
			// Gone entirely: the gesture itself was rolled back (its rejection
			// already toasted) or aged past the history cap. Nothing to undo.
			if (idx < 0) return;
			// Entries recorded above the target: pure-layout ones commute;
			// anything touching source makes the target's inverse stale.
			for (const above of this.undoStack.slice(idx + 1)) {
				if (!(above.kind === 'confirmed' && !above.source)) {
					this.host.notify('Undo failed', 'a newer edit landed before the undo could run');
					return;
				}
			}
			const entry = this.undoStack[idx];
			this.undoStack = [...this.undoStack.slice(0, idx), ...this.undoStack.slice(idx + 1)];
			try {
				const redoEntry = await this.applyHistoryEntry(entry);
				// Only repopulate redo if no new forward edit branched history
				// meanwhile (which cleared redo and bumped the epoch).
				if (redoEntry && this.redoEpoch === epoch) {
					this.pushRedoEntry({ ...redoEntry, pressStamp: press });
				}
			} catch (e) {
				// Replay failed: restore the entry where it sat.
				this.undoStack = [...this.undoStack.slice(0, idx), entry, ...this.undoStack.slice(idx)];
				this.host.notify('Undo failed', e instanceof Error ? e.message : String(e));
			}
		});
	}

	redo(): void {
		const press = ++this.pressSeq;
		// OPTIMISTIC redo, symmetric with undo's revoke: a `reapply` entry on
		// top re-records RIGHT NOW (the projection shows it instantly, and
		// anything the user does next is recorded on top of it, in press
		// order). Only possible when no earlier redo press is still queued
		// (LIFO order must not skip around a queued claim) AND no undo press
		// is still queued: a queued undo's product is this press's rightful
		// target, and acting early would both grab an older entry and plant
		// a fresh pending entry above the queued undo's target, defeating it.
		if (this.queuedRedoPresses === 0 && this.queuedUndoPresses === 0) {
			const top = this.redoStack[this.redoStack.length - 1];
			if (top && top.kind === 'reapply') {
				this.redoStack = this.redoStack.slice(0, -1);
				try {
					const undoEntry = this.reapplyEntry(top);
					this.undoStack = [...this.undoStack, undoEntry].slice(-MAX_HISTORY);
				} catch (e) {
					this.redoStack = [...this.redoStack, top]; // refused: keep replayable
					this.host.notify('Redo failed', e instanceof Error ? e.message : String(e));
				}
				return;
			}
		}
		// Queued redo: pop at RUN time (the entry a just-completed undo
		// pushed is here by then; a forward gesture recorded since the press
		// cleared the stack, so an empty stack is a natural no-op). The
		// press-stamp guard keeps press ORDER: an entry born of an undo
		// pressed AFTER this redo press is not this press's to consume.
		this.queuedRedoPresses++;
		this.enqueue(async () => {
			this.queuedRedoPresses--;
			const entry = this.redoStack[this.redoStack.length - 1];
			if (!entry) return;
			if (entry.pressStamp! > press) return;
			this.redoStack = this.redoStack.slice(0, -1);
			try {
				const undoEntry = await this.applyHistoryEntry(entry);
				if (undoEntry) this.undoStack = [...this.undoStack, undoEntry].slice(-MAX_HISTORY);
			} catch (e) {
				this.redoStack = [...this.redoStack, entry]; // replay failed: restore
				this.host.notify('Redo failed', e instanceof Error ? e.message : String(e));
			}
		});
	}

	/** Resolve one history entry (already popped by the caller); returns the
	 *  entry for the opposite stack (null = nothing to push). Runs inside the
	 *  serialized chain. */
	private async applyHistoryEntry(entry: HistoryEntry): Promise<HistoryEntry | null> {
		if (entry.kind === 'pending') {
			const p = this.pendingOps.find(x => x.id === entry.opId);
			if (p) {
				// Still unconfirmed. Sends queued ahead of this task have
				// settled (the chain serializes), so this is an unsent typing
				// op: peel it locally. The reapply entry carries the gesture's
				// forward layout so redo restores both source and layout.
				return { seq: this.nextHistorySeq++, kind: 'reapply', ops: p.ops, layout: this.peelOp(p) };
			}
			// Unreachable by construction: a confirmed op's entry was swapped
			// to 'confirmed' before this task ran (undo resolves inside the
			// chain), and a rolled-back op's entry left the stack with it. If
			// this ever fires the invariant broke; say so instead of quietly
			// doing nothing.
			this.host.notify('Undo engine error', `a history entry points at a missing op (${entry.opId}); please report`);
			return null;
		}
		if (entry.kind === 'confirmed') {
			let source: TextEdit | undefined;
			if (entry.source) {
				const r = await this.host.applyTextEdit(entry.source);
				// Cancelled mid-replay (the view switched away): the replay
				// never ran. Throw so the caller restores the popped entry;
				// applying the LAYOUT half anyway would persist a dying
				// view's layout onto the newly watched file.
				if (r.cancelled) throw new Error('the view changed before the replay could run');
				if (r.project) this.adoptTruth(r.project, r.weftCode);
				source = r.inverse ?? undefined;
			}
			const layout = entry.layout ? this.applyLayoutChange(entry.layout) : undefined;
			return source || layout ? { seq: this.nextHistorySeq++, kind: 'confirmed', source, layout } : null;
		}
		// 'reapply' (redo of an undone pending op): re-record as a fresh
		// gesture, synchronously (see reapplyEntry).
		return this.reapplyEntry(entry);
	}

	/** Re-record an undone gesture (`reapply` entry) as a fresh pending op:
	 *  preflight, rebuild the op with its forward layout, send. Returns the
	 *  new `pending` history entry; the caller pushes it (no pushHistory:
	 *  the redo branch must NOT be cleared, we're walking it). Fully
	 *  synchronous, so redo can apply a reapply OPTIMISTICALLY at press
	 *  time. Throws on a preflight refusal (don't return null): the
	 *  caller's catch restores the popped entry to its stack and notifies,
	 *  so a transient rejection (e.g. the 1s code-edit lock) leaves the
	 *  redo replayable once it clears, instead of silently destroying the
	 *  branch. */
	private reapplyEntry(entry: HistoryEntry & { kind: 'reapply' }): HistoryEntry {
		const pf = runPreflight(entry.ops, this.visibleProject(), this.lock(), this.catalog, this.host.now());
		if (!pf.ok) throw new Error(pf.reason);
		const p: PendingOp = { id: `op-${++this.nextOpId}`, ops: entry.ops, state: 'pending' };
		// If the SERVER refuses this reapply (a transient race, not just the
		// preflight lock above), the redo entry must come BACK onto the redo
		// stack, exactly as the preflight throw leaves it replayable. Carry the
		// reapply entry on the op; sendPendingOp's catch restores it (epoch-
		// guarded, so a forward edit that re-branched since stays cleared).
		p.redoRestore = { saved: [entry], epoch: this.redoEpoch };
		if (entry.layout && entry.layout.length > 0) {
			// Re-attach the gesture's forward layout as the op's OWNED layer
			// (droppable if it's rejected), and recompute its undo inverse
			// against the current visible layout for a later confirmed undo.
			const before = this.layoutCode;
			const after = applyLayoutOps(before, entry.layout);
			this.pushLayoutLayer(p.id, diffLayoutOps(before, after));
			const inv = diffLayoutOps(after, before);
			if (inv.length > 0) p.layoutUndo = inv;
		}
		this.pendingOps = [...this.pendingOps, p];
		this.sendPendingOp(p);
		return { seq: this.nextHistorySeq++, kind: 'pending', opId: p.id };
	}
}
