# Where this is going



**Held suspensions.** Today the durable model kills a worker whenever every lane
parks on a wait, and a fresh worker folds the journal to resume. That is what
makes parking thousands of cheap flows free. It fails a node that holds
in-process state too expensive to rebuild: a browser session with thousands of
cookies, a loaded local model. The plan is an opt-in
primitive, `ctx.hold_signal`, where the await happens in place and the worker
stays alive. Die-and-resume stays the default, and the language makes the cost
visible, because holding keeps a worker up for the whole wait.

**Should a `recorded: false` run be able to pause?** Today it cannot, so its
waits hold in its worker. It could: such a run writes nothing while it works,
but at a pause everything it holds in memory is plain data, so it could write
that once, as one snapshot, and nothing else. That needs the snapshot's
format, a place to keep it, a resume that starts from it and then reads what
was written after it (the answer that woke the run), the run's secrets and
the costs it has not settled carried inside it, and cancelling and
`weft node-state` taught to read it. Whether that is
worth it is still open.

**Type rules in metadata.** `FirstInOrder` wants an output whose type metadata
cannot write down as one type: the union of its created inputs. Today every
input it creates shares one type variable, so it cannot join a `String` branch
with a `Number` one. The plan is a
small rule language in `metadata.json`, read the way `portsFromConfig` is, for
"the union of these inputs". It is the whole feature or nothing, because a half rule would
send the editor, the validator and the runtime three different answers about one
port.

## More the compiler can prove

Today the compiler checks how the pieces connect: types, required inputs, graph
shape. The ambition is for it to prove things about how a program behaves,
through richer node metadata. A node that acts in the world says so and says how
big the action is. A node that verifies says what strength it gives. An
autonomous node says so. Then the compiler can check a claim across the graph:
every path from an autonomous node to a high-stakes action passes through a
verification of at least this strength, or a critical action has three separate
human approvals in front of it, and a program that does not hold up does not
build. None of it works if the labels are decoration, so the other half is
vetted sets of nodes, signed off by whoever is doing the regulating, with
safety-critical programs required to build out of those. Read
[How I think about AI safety](../thinking/safety.md) for why this matters.

## Running it for real

**Other clouds.** Weft installs on GCP today ([deploying to your
cloud](../running/cloud.md)). AWS and Azure come next, installed on your
own account the same way.

## The builders

**Tangle's own CLI.** Tangle today is a set of instructions on top of whatever
coding assistant you use. A proof-of-concept CLI wrote a working program from a
brief in under five minutes. The released version is slower because existing
assistants spend time on fluff weft does not need, and our own CLI fixes that.

**Coordinated assistants in parallel.** Several agents building branches of the
weft codebase under one plan, the same shape weft gives a program.

## The editor

**Branch lanes in the version tree.** The **Runs** view can already list runs
by version (**Flat list / by version** on its title bar): every version as a tree, its runs under
it, the head of each working copy marked, and **Load this version** on a version or a run. The plan is to draw it the way a
git client does, with a lane per branch, and to pick a run's seed from the same
view.
