# How a weft program runs

Most people arrive already holding a model, and it is nearly right, which is
the hard case.

The model is: it is a DAG. Nodes are tasks, edges are dependencies, something
walks the graph in topological order and runs each task when its dependencies
are done. Airflow, Prefect, a build system.

That model gets you a long way and then breaks in three places. It cannot
express a node that produces nothing. It cannot express the same node running
twice at once on different data. And it cannot express a task that pauses for a
week.

All three come from the same difference: in weft, nothing walks the graph.
Values move, and nodes react to them arriving.

## Pulses

When a node finishes its work it **emits** values on its output ports. Each
emission becomes a **pulse**: a small object carrying a value, addressed to
exactly one input port on exactly one node.

```mermaid
flowchart LR
    A["classify<br/><i>LlmInference</i>"] -- "pulse: {response: &quot;critical&quot;}" --> B["reply<br/><i>SlackSendMessage</i>"]
```

A pulse carries more than its value:

| Field | What it is |
|---|---|
| `value` | the JSON value being delivered |
| `target_node` / `target_port` | where it is going |
| `color` | which execution it belongs to |
| `frames` | which loop iteration it belongs to |
| `closed` | whether it carries a value at all |

A pulse sits at its destination port and waits. **A node fires when every one
of its required input ports is holding a pulse**, and all of those pulses
agree on their color and their frames. Then it fires, consuming them.

A node with no inputs has nothing to wait for, so it fires immediately. That is
where an execution starts.

## What a firing is

One firing is one call to the node's `run` body, with those pulses as its
inputs. It produces zero or more emissions and then returns.

A firing is not a process. It is an execution id and an iteration number, so
the same node can be firing four times at once inside a parallel loop, told
apart by those numbers. That is the second place the DAG model breaks, and it
is why weft talks about firings rather than about running a task.

## The closed pulse

A node does not have to emit on every output port. When it emits on some ports
and not others, the ports it left out get **closed**: a pulse is sent down
each of those wires carrying no value, meaning "nothing will ever arrive here,
at this color, at these frames. Stop waiting."

What the receiving node does with it depends on one thing:

- If the closed pulse lands on a **required** input, the node cannot run. It is
  **skipped**, and it closes all of its own outputs in turn.
- If it lands on an **optional** input (declared with a trailing `?`), the node
  fires anyway, with that input simply absent.
- If it lands on `_should_flow`, the node is skipped whatever else arrived:
  that port is the one that decides whether the node runs at all, and a
  closure on it means nothing ever said yes.

So a skip cascades forward, closing everything downstream, until it reaches a
node that opted into handling absence.

```mermaid
flowchart LR
    G["triage"] -- "closed" --> S1["send_alert<br/>(skipped)"]
    S1 -- "closed" --> S2["log_alert<br/>(skipped)"]
    S2 -- "closed" --> S3["notify<br/>(skipped)"]
    style S1 stroke-dasharray: 4 4
    style S2 stroke-dasharray: 4 4
    style S3 stroke-dasharray: 4 4
```

## Branching is just that

There is no `if`, no `try`/`catch`, and no conditional edge anywhere in the
language. A branch is a node that did not run, and everything behind it
closing in turn.

Every node has a `_should_flow` input deciding whether it runs at all. Leave it
alone and the node runs. Wire it, and a `false` (or a closure, meaning whatever
decides never spoke) skips that node, which closes its outputs, which skips
everything behind it.

```weft
reply = SlackSendMessage {
  _should_flow: review.escalate_approved
  text: classify.response
}
```

Both branches of a conditional exist in the graph; the one that was not taken
goes dark. Two nodes turn that into a shape you can read:

- `Switch` tests a value against its cases and emits `true` on the one port
  the winning case names, closing the rest. Each case's kind is its test
  (`equals`, `in`, `between`, `otherwise`, and the rest). Wire a case's port
  into the `_should_flow` of whatever that branch runs.
- `FirstInOrder` takes the branches back to one wire: it emits the first of its
  inputs that carried a value, in the order they are written, so the answer a
  person approved can sit above the automatic one and never be overtaken.

A `Group` or a `Loop` takes `_should_flow` too, so one line turns off a whole
subgraph: it closes the group's outputs, which closes everything inside it,
however deeply nested.

The same rule explains the rest:

- A node that fails closes its outputs, so a failure propagates exactly like an
  absent value, and a downstream node with an optional input is the recovery
  path.

- A trigger that did not fire closes its outputs, so the branches belonging to
  other triggers go dark and the branch belonging to the one that fired runs.
- A loop iteration that failed to write its gather port leaves `null` at that
  index, which is why a gather output is typed `List[T | Null]` and the
  compiler makes you say so.
- A node whose every input is optional would run even when everything upstream
  is dead, which is a bug you almost never want, so the compiler warns and
  suggests `@require_one_of`.

## Groups and loops are compile-time only

Neither exists at run time. The compiler flattens a group into two boundary
nodes and a loop into a pair of them, and their children become ordinary nodes.

So what the executor ever sees is one flat graph of nodes and pulses. Nesting,
folding and iteration were all resolved by the compiler before it started.

## Frames

A `frames` value is a stack of loop iteration indices. Top level is the empty
stack. Inside a loop's third iteration it is `[2]`. Inside the fifth iteration
of a loop nested in that one it is `[2, 4]`.

Two pulses only meet at a node if their frames match, so iteration 2's data can
never combine with iteration 4's. Nothing copies the graph per iteration: there
is one graph, and pulses that know which iteration they belong to.

## What actually runs

A weft program has no `main`, and nothing declares an entry point. Every
node the run reaches runs. What differs between the kinds of run is where
the first pulses are put.

**A manual run** kicks every root: a node at the top level that no wire
feeds. From there pulses go wherever the wiring takes them, and a node runs
the moment its inputs are settled. A branch nobody reaches stays blank.

You can start narrower:

```bash
weft run --target daily_report --target alert
```

which runs those two nodes and what they need, and nothing else: the run
is held to the targets' upstream, so a root they share with another branch
(a database the whole file reads) never drags that branch in, and nothing
past a target runs either. Several targets run the union of what each
needs, independent branches side by side. Any node can be a target. A
target inside a group brings the whole group, and whatever feeds the
group's inputs runs first, the same as when the group starts in a full
run. In the graph, right-click a node and choose **Set as target**; the
Run button then says how many targets it is aimed at.

An aimed run answers to what it would execute, reading "what it would
execute" as the joined upstream walk from every target at once:

- **If no trigger sits anywhere in that walk**, the run is an ordinary
  one-shot, so the Run button appears next to Activate / Deactivate even in a
  project full of triggers. This is how a maintenance branch works: a chain
  the triggers cannot reach, fired by hand whenever you need it, for example
  the enrollment door in the
  [telegram example](https://github.com/WeaveMindAI/weft/tree/main/examples/telegram-image-bot).
- **Only the infra inside that walk gates it.** A run cannot start while
  an infra node it would touch is not running, the same rule as a plain run;
  but infra elsewhere in the project has no say, since this run never touches
  it. The Run button greys out accordingly, and `weft run --target ...`
  refuses with the same message until you `weft infra start`.

**A trigger fire** runs one program: the trigger that fired, everything
downstream of it, and everything upstream of that, stopping at other
triggers on the way up. At fire time a trigger's outputs are the event, not
a function of its inputs, which were read once at activation, so a node that
only feeds a trigger has nothing to contribute. Every other trigger in that
set is kicked with no payload, which closes its outputs, and
[the skip cascade](#the-closed-pulse) prunes the branches that belong to it.

A node the fired trigger cannot reach belongs to another program in the same
file, and a value that spills into it from a shared node (one database feeding
two programs) is dropped with no row. One file can hold several programs, one
per trigger, and a middle section both of them need is picked up by whichever
one fired without you saying so. What that buys you when laying a project out:
[one file, several programs](../start/reading-the-graph.md#one-file-several-programs).

A group or a loop is reached as a whole. When a scope's boundary settles, the
launcher kicks every root inside it at the scope's frames, and a run aimed at
a node inside a group brings the whole group.

## When it ends

An execution is finished when no pulse is in flight and no node is waiting for
one. There is no terminal node and nothing declares completion.

Two ends that are not completion:

- **Suspended.** Every live firing is parked on a wait for an external event. The
  worker exits. The execution is alive and costs nothing.
- **Stuck.** The engine can prove no remaining node can ever proceed, because
  every one of them is waiting on one of the others. That is a graph-shape bug
  and it fails loudly rather than hanging: the failure names each node left
  holding a value and the wired inputs it never received.

And one end that is a decision: **cancelled**. A person pressed Stop, or
another run of the same project stopped this one. A run can tag itself, and
any sibling carrying that tag can be stopped, even one parked on a person or
a timer. The journal records who did it. For how a node asks for that, go
and read [Stopping other runs](../nodes/steering-executions.md).

## The journal, and why waiting is free

Everything above happens in a worker process, in RAM: pulses are values in
memory and the drive loop is an ordinary loop.

Alongside it, the worker writes an append-only **journal**, one row per event,
as it goes. Nothing reads that journal back during a normal run. It is read
only when a worker has to rebuild an execution it did not run: it folds the
rows in order, reconstructs the pulse table and which nodes completed or
suspended, and carries on from there.

That buys:

- **A human pause costs a database row.** `HumanQuery` parks a firing, and when
  the last live firing parks, the worker exits. Ten thousand executions waiting
  on ten thousand people are ten thousand rows and no processes.
- **Crashes are survivable.** A worker that dies mid-execution is replaced, and
  the replacement folds the journal and continues, without re-running the nodes
  that had already completed.
- **A failure is readable.** A run from last Tuesday is still legible node by
  node, with the actual values on the actual wires.

Weft's execution guarantee is at-least-once. A crash can lose the write that
recorded a node's completion, so a node that had already finished when its
worker died **is re-run** by the replacement. When a node's work must not
happen twice, `ctx.run` makes it happen once and replays the recorded result
afterwards. See [Surviving a restart](../nodes/durable-execution.md).

## The compiler's half

None of the above is checked at run time. Before an execution exists, the compiler has already read the whole
graph and refused it if:

- any connection's types do not match,
- any required input is unwired,
- there is a cycle in the wire graph,
- a type variable was never pinned to anything concrete,
- a node's own config validation failed,
- a loop's configuration is internally contradictory,
- a trigger sits somewhere a trigger cannot sit.

The full list is in [What the compiler refuses](diagnostics.md).

What is left after a successful compile is external: a service is down, an API
errors, a person never answers.

## The shape of the whole thing

```mermaid
flowchart TD
    S[".weft source"] --> P["parse<br/><i>lossless syntax tree</i>"]
    P --> F["flatten<br/><i>groups and loops become<br/>boundary nodes</i>"]
    F --> E["enrich<br/><i>attach each node's declared<br/>ports from its metadata</i>"]
    E --> V["validate<br/><i>types, completeness,<br/>graph shape</i>"]
    V --> C["codegen<br/><i>emit a Rust crate</i>"]
    C --> B["cargo build<br/><i>a native binary</i>"]
    B --> R["run<br/><i>pulses, journal,<br/>suspend, resume</i>"]
```

Read the next chapters in whatever order you need. [Syntax](syntax.md) is the
surface, [Types](types.md) is what the checker checks, and
[Groups](groups.md) and [Loops](loops.md) are the two structures that make
large programs stay readable.
