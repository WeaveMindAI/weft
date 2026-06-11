# Loops

A `Loop` is a built-in language construct in Weft. Like `Group`, it's
authored as a box in the visual editor and as a keyword in `.weft`
source. Unlike `Group`, a `Loop` runs its body multiple times: once
per element of a list, or until the body says "stop", with full
parallelism control and accumulator support.

This document covers:

- The four kinds of ports a loop has, and what each one means.
- The config knobs (`parallel`, `over`, `carry`, `max_iters`,
  `trim_on_mismatch`) and which combinations cover which loop shapes
  (parallel-map, sequential-map, fold, while, count-driven, agent
  swarm).
- The "loop is a wrapper, not a lifecycle owner" semantic. This is the
  most important thing about loops in Weft and what makes them
  different from loops in mainstream languages.

## Syntax

A loop looks like a group with a mixed body (config fields + decls +
connections):

```
my_loop = Loop(
    images: List[Image],
    threshold: Number,
) -> (
    results: List[String | Null],
    state: String,
) {
    parallel: false
    over: ["images"]
    carry: ["state"]
    max_iters: 100

    process = ProcessImage {}
    process.image = self.images
    process.threshold = self.threshold
    process.prev_state = self.state
    self.results = process.result
    self.state = process.next_state
}
```

The header is the same shape as a group's header: input signature,
arrow, output signature, body. The body is mixed: top-level config
fields (`key: value`) AND nested node decls + connections, in any
order.

## The four port roles

Every port on a loop is in exactly one of four roles, derived from
the config:

1. **Iter input** (named in `over`). Outside type is `List[T]`,
   inside type is `T`. The body sees one element per iteration via
   `self.<port>`. Multiple ports in `over` zip together in lockstep.

2. **Carry port** (named in `carry`). The user declares it on the
   OUTPUT side of the signature. The compiler auto-creates a matching
   input port with the same name and same type, used as the initial
   value. Inside the body, `self.<port>` on the right-hand side reads
   the current value (the previous iteration's update, or the initial
   value); on the left-hand side it writes the next-iteration value.
   At loop termination, the final value is emitted on the outward
   output port. Same name appears on both sides of the loop box.

3. **Gather output** (declared in the output signature, not in
   `carry`). Outside type MUST be `List[T | Null]` (explicit; the
   compiler rejects `List[T]` with `gather-output-must-be-nullable`).
   Inside, the write port is `T?`. The body writes one value per
   iteration; if the body fails to write at a particular iteration
   (the port closes), the assembled outward list has `null` at that
   slot. Parallel ordering is preserved by iteration index.

4. **Broadcast input** (declared in the input signature, not in
   `over`). Outside type and inside type are the same. The value is
   available unchanged to every iteration.

## Implicit body API

Two ports exist on every loop body, not declared in the signature:

- `self.index: Number` (read-only). The 0-indexed iteration number.
  Available in every drive mode, in parallel and sequential.
- `self.done: Boolean` (write-only, optional). The body writes
  `true` to vote "stop launching new iterations after this one
  completes." Only meaningful in sequential mode; in parallel mode
  the compiler rejects any `self.done = ...` write with
  `parallel-with-done`.

`index` and `done` are reserved port names. The compiler rejects a
user-declared input port named `index` or an output port named `done`
with `reserved-port-name`.

## Drive modes (how the loop knows when to stop)

`parallel` defaults to `false` (sequential, the safer mode: carry and
`self.done` work, no ordering surprises); set `parallel: true` to opt
into the parallel drive mode. A non-boolean value (`parallel: "yes"`)
is a compile error (`loop-parallel-not-boolean`), never a silent
coercion. The other knobs default by absence: no `max_iters` means no
cap, and `over` / `carry` default to empty lists. Unknown config keys
are rejected (`loop-unknown-config-field`), so a typo'd knob cannot
silently change the loop's behavior.

A loop launches iterations and terminates whichever of these comes
first:

- `over` lists exhausted (one iteration per element, zipped if
  multiple lists are listed).
- Body wrote `self.done = true` (sequential only).
- `max_iters` reached (if set).

An empty `over` AND empty `carry` is allowed: the loop runs purely
until `self.done` (or `max_iters`). This is the pure side-effect
loop shape.

`parallel: true` + `carry` non-empty is a compile error: carry implies
sequential (iteration N+1 reads what iteration N wrote).

`parallel: true` + `over` empty is a compile error: without an `over`
list, the iteration count is not known upfront, so parallel makes no
sense.

`parallel: true` + any `self.done = ...` connection is a compile
error.

## The five base shapes

| Shape | `parallel` | `over` | `carry` | Terminated by |
|--|--|--|--|--|
| Parallel map | `true` | `[...]` | `[]` | Over exhausted |
| Sequential map | `false` | `[...]` | `[]` | Over exhausted |
| Fold (sequential reduce) | `false` | `[...]` | `[acc]` | Over exhausted |
| While | `false` | `[]` | `[acc?]` | `self.done = true` |
| Side-effect | `false` | `[]` | `[]` | `self.done = true` |

For a count-based loop ("run N times"), wire the `Range` catalog node
into a parallel-map or sequential-map: declare a `values:
List[Number]` input on the loop, list it in `over`, and feed it from
a `Range`:

```
r = Range { to: 10 }
my_loop = Loop(values: List[Number]) -> () {
    parallel: false
    over: ["values"]
    # body sees self.values as one Number per iteration
}
my_loop.values = r.values
```

## Loop is a wrapper, not a lifecycle owner

This is the most important thing about loops in Weft. In every
mainstream language, a `for` loop OWNS the lifetime of its body:
the body has no existence outside the iteration, and when the loop
ends the body is gone. In Weft the loop does NOT own its body's
lifetime. The loop is just two things:

1. A LAUNCHER that decides how/when/how-many iterations to start.
2. A SINGLE OUTWARD EMITTER that, at termination, assembles gathers
   and carries and emits them at the parent frame stack.

The body itself is a SCOPE addressed by frame stacks. Body work at
each iteration exists in the runtime as soon as the iteration is
launched, and continues to exist until that work naturally drains.
The runtime does NOT tear it down when the loop emits outwardly.

Concrete consequences:

- Body work can keep running PAST the loop's outward emit. A node
  inside an iteration that launched a sub-agent or opened a bus
  continues running, pulsing, branching, even hours after the loop
  has emitted outwardly. The runtime knows which iteration each
  post-emit pulse belongs to because the frame stack uniquely
  addresses it.

- A body node can have multiple output branches: one that writes to
  `LoopOut` (contributing to a gather), and others that fire to
  sibling body nodes that run independently. Only the LoopOut
  branch gates the loop's outward emit. The other branches keep
  going.

- Nested loops can be instantiated INSIDE a post-emit body branch.
  They spin up at the outer iteration's frame stack and run
  normally; the outer loop has long since emitted outwardly.

- Body work is bounded in SPACE (the loop's scope: edges cannot
  cross out of the loop's body to non-boundary nodes) but unbounded
  in TIME.

- Communication outward post-emit happens through artifacts the
  body emitted at emit time: typically a `List[Bus | Null]` carrying
  the bus handles that body nodes still hold. Outside-the-loop
  coordinators wire to that list and exchange messages with the
  body work that is still alive.

## Worked example: spawn N agents in parallel, keep them alive

```
swarm = Loop(
    specs: List[String],
) -> (
    channels: List[Bus | Null],
) {
    parallel: true
    over: ["specs"]

    agent = LaunchAgent {}
    agent.spec = self.specs
    self.channels = agent.bus_marker
    # agent.process, agent.tick, etc. wire to OTHER body nodes here,
    # forming a sub-graph that keeps running after self.channels is
    # written.
}

coordinator = Coordinator {}
coordinator.channels = swarm.channels
```

Sequence of events:

1. The graph fires `swarm` with `specs = ["agent-a", "agent-b",
   "agent-c"]`.
2. `LoopIn` launches three body iterations at frames `[{0}]`,
   `[{1}]`, `[{2}]`.
3. At each iteration's frame, `LaunchAgent` opens a bus, emits the
   bus marker on `self.channels`, and emits other pulses to follow-up
   body nodes that process input/respond on the bus.
4. `LoopOut` at each iteration fires once `self.channels` has
   arrived. The agent's other branches inside the body are NOT
   gated by `LoopOut`; they continue running.
5. Once all three iterations have fired `LoopOut`, the loop emits
   outwardly: `swarm.channels` carries `[Bus_a, Bus_b, Bus_c]`.
6. `Coordinator` outside the loop sends messages on each bus and
   expects replies.
7. The agent body work INSIDE the loop (alive at frames `[{0}]`,
   `[{1}]`, `[{2}]`) reads messages, replies, processes. The agents
   may run for minutes or hours.

The execution as a whole terminates only when all body work has
drained.

This pattern is the headline reason loops exist in Weft. It is not
expressible cleanly in any mainstream language (a Python `for` loop
that "leaves bodies running" requires the user to hand-roll thread
pools, futures, and signal them). In Weft it is the natural shape:
write the loop, wire `self.channels` to the bus marker, and the
runtime does the rest.

## Per-iteration failure handling

Loops are NOT a kill-switch. If a body iteration's pre-LoopOut work
fails (a required input arrives closed), the failure cascades only
through that body branch, not through the loop. At the LoopOut
boundary:

- A gather port that received a CLOSURE at this iteration's frame:
  the assembled outward list has `null` at that index. Downstream
  must handle `List[T | Null]` (the type system forces this).
- A carry port whose write received a closure: the runtime KEEPS the
  previous carry value for the next iteration.
- The `self.done` port closed: treated as `false`. The loop
  continues.

A side-branch failure inside the body (not wired to any LoopOut
input) does not affect the loop's outward emit. The loop's emit is
gated by "every launched iteration has fired LoopOut", not by "every
launched iteration's body has fully drained."

## Nested loops

A loop inside a loop adds one frame to the iteration frame stack.
Inner-loop bodies fire at `[outer_iter, inner_iter]`. The inner
loop terminates and emits at the outer body's frame stack (one frame
less); the outer loop collects those emissions as normal body pulses.

Outer-loop termination does NOT cascade to inner loops. Each inner
loop runs to its own natural end.
