/** Collapse overlapping runs of one async job, with the two joining
 *  verbs callers actually mean:
 *
 *  - `join()`: the pending run, or a fresh one. For plain reads (a
 *    Refresh click): two interleaved runs would let the slower, staler
 *    response land last and stomp what the fresh one rendered.
 *  - `afterNow()`: a run that STARTS after this call. For reads that
 *    follow a mutation (skip, cancel, grant): joining a run that
 *    started BEFORE the mutation would render pre-mutation state as if
 *    it were the result. Concurrent callers coalesce into one trailing
 *    run.
 *
 *  `fn` must handle its own failures (every current fn try/catches its
 *  whole body): a rejecting fn reaches whichever caller started that
 *  run, and a caller that fires without awaiting (a background arming
 *  path) would turn it into an unhandled rejection.
 */
export function singleFlight(fn: () => Promise<void>): {
  join: () => Promise<void>;
  afterNow: () => Promise<void>;
} {
  let pending: Promise<void> | null = null;
  let trailing: Promise<void> | null = null;
  const start = () => {
    const p = fn().finally(() => {
      if (pending === p) pending = null;
    });
    pending = p;
    return p;
  };
  return {
    join() {
      return pending ?? start();
    },
    afterNow() {
      if (!pending) return start();
      if (!trailing) {
        trailing = pending.catch(() => {}).then(() => {
          trailing = null;
          return start();
        });
        // The trailing chain OWNS the slot from now on: without this,
        // the microtask gap between the finished run's finally
        // (pending = null) and the trailing's start() would let a
        // join() land on an empty slot and run in parallel with the
        // trailing, the exact overlap this primitive exists to
        // prevent. start() re-points the slot at the real run when it
        // begins, and the finished run's finally no longer matches.
        pending = trailing;
      }
      return trailing;
    },
  };
}
