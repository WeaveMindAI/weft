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

import type { ProjectDefinition } from '$lib/types';
import type { EditOp, TextEdit } from '../../../shared/protocol';
import { applyLayoutOps, diffLayoutOps, type LayoutOp } from '$lib/layout';
import { foldOps, type ProjectionCatalog } from './apply';
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
	/** Persist the layout file (fire-and-forget outward save). */
	persistLayout(layoutCode: string): void;
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
 *  never dropped, lives in the log only until the next rebase). */
interface LayoutLayer {
	owner: string;
	ops: LayoutOp[];
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
	// Confirmed inverses for ops whose `pending` history entry an undo popped
	// before the confirmation landed (the undo task queues behind the send).
	private confirmedByOpId = new Map<string, HistoryEntry & { kind: 'confirmed' }>();
	private nextOpId = 0;
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
	private pushLayoutLayer(owner: string, ops: LayoutOp[]): void {
		if (ops.length === 0) return;
		this.layoutLog = [...this.layoutLog, { owner, ops }];
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
			this.host.persistLayout(this.layoutBase);
		} else {
			this.pushLayoutLayer(COMMITTED, diffLayoutOps(before, after));
		}
		const inv = diffLayoutOps(after, before);
		return inv.length > 0 ? inv : undefined;
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
		this.host.persistLayout(this.layoutBase);
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
			if (inverse) this.pushHistory({ kind: 'confirmed', layout: inverse });
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
		this.pushHistory({ kind: 'pending', opId: p.id });
		p.redoRestore = { saved: savedRedo, epoch: this.redoEpoch };
		if (typingKey) this.armTypingFlush();
		else this.sendPendingOp(p);
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
		this.pendingOps = this.pendingOps.filter(x => x.id !== p.id);
		this.dropLayoutLayers(p.id);   // its forward layout leaves the fold
		this.maybeRebaseLayout();      // folds surviving layers if nothing pends
		this.undoStack = this.undoStack.filter(e => !(e.kind === 'pending' && e.opId === p.id));
		this.redoStack = this.redoStack.filter(e => !(e.kind === 'pending' && e.opId === p.id));
		this.host.notify('Edit failed', `${reason}. Rolled back to last good state.`);
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
			} else if (newLayoutCode !== this.layoutBase) {
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
			// peeled it, or a truth advance invalidated it).
			if (!this.pendingOps.some(x => x.id === p.id)) return;
			try {
				const r = await this.host.applyEdits(p.ops);
				// A synchronous truth-advance (external parseResult) may have
				// dropped this op while the RPC was in flight. If so, its slot
				// is gone and the newer truth already won: do NOT regress truth
				// to this older reply, and do NOT stash a confirmed inverse no
				// undo will ever consume.
				if (!this.pendingOps.some(x => x.id === p.id)) return;
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

	/** Swap an op's `pending` history entry to its confirmed inverse. When an
	 *  undo already popped the entry (its task queues behind this confirmation
	 *  on the chain), stash the inverse for that task to consume. */
	private confirmHistoryEntry(opId: string, entry: HistoryEntry & { kind: 'confirmed' }): void {
		const keep = entry.source || entry.layout ? entry : null;
		const idx = this.undoStack.findIndex(e => e.kind === 'pending' && e.opId === opId);
		if (idx < 0) {
			if (keep) this.confirmedByOpId.set(opId, keep);
			return;
		}
		const next = [...this.undoStack];
		if (keep) next[idx] = keep;
		else next.splice(idx, 1); // nothing reversible came back: drop the entry
		this.undoStack = next;
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

	undo(): void {
		const entry = this.undoStack[this.undoStack.length - 1];
		if (!entry) return;
		this.undoStack = this.undoStack.slice(0, -1);
		const epoch = this.redoEpoch;
		this.enqueue(async () => {
			try {
				const redoEntry = await this.applyHistoryEntry(entry);
				// Only repopulate redo if no new forward edit branched history
				// meanwhile (which cleared redo and bumped the epoch).
				if (redoEntry && this.redoEpoch === epoch) this.redoStack = [...this.redoStack, redoEntry];
			} catch (e) {
				this.undoStack = [...this.undoStack, entry]; // replay failed: restore
				this.host.notify('Undo failed', e instanceof Error ? e.message : String(e));
			}
		});
	}

	redo(): void {
		const entry = this.redoStack[this.redoStack.length - 1];
		if (!entry) return;
		this.redoStack = this.redoStack.slice(0, -1);
		this.enqueue(async () => {
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
				// Still unconfirmed. Structural sends queued ahead of this task
				// have settled (the chain serializes), so this is an unsent
				// typing op or a queued-but-not-yet-sent op: peel it locally.
				// Its forward layout layer leaves the fold; the reapply entry
				// carries that forward so redo restores both source and layout.
				const layer = this.layoutLog.find(l => l.owner === p.id);
				const forwardLayout = layer?.ops;
				this.pendingOps = this.pendingOps.filter(x => x.id !== p.id);
				this.dropLayoutLayers(p.id);
				this.maybeRebaseLayout();
				return { kind: 'reapply', ops: p.ops, layout: forwardLayout && forwardLayout.length > 0 ? forwardLayout : undefined };
			}
			// The op confirmed while this undo was queued: consume the stashed
			// inverse. A rejected op stashed nothing (its rollback already
			// happened), so the undo is a no-op.
			const confirmed = this.confirmedByOpId.get(entry.opId);
			this.confirmedByOpId.delete(entry.opId);
			return confirmed ? this.applyHistoryEntry(confirmed) : null;
		}
		if (entry.kind === 'confirmed') {
			let source: TextEdit | undefined;
			if (entry.source) {
				const r = await this.host.applyTextEdit(entry.source);
				if (r.project) this.adoptTruth(r.project, r.weftCode);
				source = r.inverse ?? undefined;
			}
			const layout = entry.layout ? this.applyLayoutChange(entry.layout) : undefined;
			return source || layout ? { kind: 'confirmed', source, layout } : null;
		}
		// 'reapply' (redo of an undone pending op): re-record as a fresh
		// gesture. The caller pushes the returned `pending` entry, so no
		// pushHistory here (and the redo branch must NOT be cleared: we're
		// walking it).
		const pf = runPreflight(entry.ops, this.visibleProject(), this.lock(), this.catalog, this.host.now());
		if (!pf.ok) {
			// THROW (don't return null): the caller's catch restores the popped
			// entry to its stack and notifies, so a transient rejection (e.g.
			// the 1s code-edit lock) leaves the redo replayable once it clears,
			// instead of silently destroying the branch.
			throw new Error(pf.reason);
		}
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
		return { kind: 'pending', opId: p.id };
	}
}
