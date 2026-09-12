# Where this is going

What is being built next, and the design behind each piece.

## Language

**Function callbacks.** A node declares named entry points it can call, with
typed arguments and returns, as part of its declared interface. At fire time
the runtime injects a callable; the node calls it, the pulse flows through the
connected subgraph, and the result comes back. From the node's side it feels
synchronous. This is what makes higher-order nodes (run this subgraph per
element) and first-class agent loops fall out of the existing vocabulary.

The architectural decision still open is whether a callback rides the journal
and replay machinery as a suspension whose resolver is an internal subgraph, or
gets its own path.

**A reason on a closed port.** A closure today says "nothing will arrive here"
and nothing more, so three situations produce the same signal: the producer
failed, the producer declined, or the producer was never going to run because a
different trigger fired. Attaching the reason is straightforward. The design
work is deciding what a downstream node may do with it without reinventing
exceptions.

**A concurrency bound on parallel loops.** `max_parallel`, so a loop over ten
thousand items runs at the width you choose.

**Suspendable live channels.** Buses and streams surviving a suspension, which
is what lets a stream consumer's body `await_signal`.

## Execution

**Held suspensions.** The durable model kills the worker on every suspension,
which is what makes parking thousands of cheap flows free. A node holding
in-process state too expensive to rebuild (a browser session with thousands of
cookies, a loaded local model, a warm connection pool) gets an opt-in primitive
where the future awaits in place and the worker stays alive. Die-and-resume
stays the default, and the language makes the cost of holding visible, since
holding pins a pod for the whole duration.

**One concept for journal holes.** A write that failed and a stored row that
cannot be read are the same thing: the journal cannot give a correct event at
some position. One "hole" concept replaces the two mechanisms handling them
today, with severity decided by **where** the hole sits: cosmetic in dead
history, where the replay view degrades and nothing else does, and fatal when
it intersects the state a resume depends on, where it refuses rather than
resume on state it had to guess at.

Defining that resume frontier precisely is the hard part. See
[The journal](../running/the-journal.md#when-records-go-missing).

**Stream journaling volume.** Two journal rows per stream item
carries real workloads today and does not carry a stream of ten million items.
Buses already solve this with windowing, and streams take the same route.

## Operations

**One surface for degraded state.** When something goes wrong that the runtime
cannot fix on its own (a health action that keeps failing, an infra node stuck,
a trigger setup that errored, a worker crash-looping), one place answers "what
is wrong with my project right now, and what do I do about it".

**A project-scoped meta log.** Observability for the things that belong to no
single execution and therefore have no journal to live in.

**Per-node infrastructure drift detection.** Noticing, per unit, that what is
running no longer matches what the spec says.

## Catalog

**A GitHub package, done properly.** Covering what people actually automate:
issues, pull requests, and repository triggers. The old package covered issue
creation alone and was removed. Both credential doors already exist in the
access system.

**Model-list filtering by capability.** A model picker offers only the models
that can do what the node needs.

## What the rest of it is for

Most of this list clears the way for
[Sequential Diffusion Programming](../thinking/sdp.md), building a program by
refining it against real examples pass after pass, which works now that a pass
is cheap. What is in use today and what is being built next is on that page.
