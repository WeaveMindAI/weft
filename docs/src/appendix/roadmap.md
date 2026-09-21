# Where this is going



**Held suspensions.** Today the durable model kills a worker whenever every lane
parks on a wait, and a fresh worker folds the journal to resume. That is what
makes parking thousands of cheap flows free. It fails a node that holds
in-process state too expensive to rebuild: a browser session with thousands of
cookies, a loaded local model, a warm connection pool. The plan is an opt-in
primitive, `ctx.hold_signal`, where the await happens in place and the worker
stays alive. Die-and-resume stays the default, and the language makes the cost
visible, because holding pins a pod for the whole wait.

**Suspendable live channels.** A bus is pinned to one worker, both ends have to
stay alive, and `await_signal` is forbidden while it is open. The plan is to
journal enough channel state that a fresh worker resumes both ends mid-stream.
That needs a replay story for a partially-consumed stream, which items were
delivered and which pulls answered, so it comes after the rest of the durable
work.

**Type rules in metadata.** Two nodes want an output whose type metadata cannot
write down as one type: `FirstInOrder` emits the union of its created inputs, and
`LlmInference.response` is a `String` without `parseJson` and a record or
`JsonDict` with it. Both make the author restate the type in the signature every
time. The plan is a small rule language in `metadata.json`, read the way
`portsFromConfig` is, for "the union of these inputs" or "this type when that
input is true". It is the whole feature or nothing, because a half rule would
send the editor, the validator and the runtime three different answers about one
port.

## More the compiler can prove

Today the compiler checks how the pieces connect: types, required inputs, graph
shape. The ambition is for it to prove things about how a program behaves,
through richer node metadata. A node that acts in the world says so and says how
big the action is. A node that verifies says what strength it gives. An
autonomous node says so. Then the compiler can check a claim across the graph:
every path from an autonomous step to a high-stakes action passes through a
verification of at least this strength, or a critical action has three separate
human approvals in front of it, and a program that does not hold up does not
build. None of it works if the labels are decoration, so the other half is
vetted sets of nodes, signed off by whoever is doing the regulating, with
safety-critical programs required to build out of those. Read
[How I think about AI safety](../thinking/safety.md) for why this matters.

## Connections

**Delegated end-customer connections.** An operator builds a product on weft and
serves it from their own site. Each end customer needs to connect their own
Google or Slack and run the workflow on their own data. Today connections belong
to the tenant who owns the project, so there is no way to mint a connection for
one end customer, keep two customers' grants apart, or point a run at "customer
X's connection". The plan: the operator registers their OAuth app once, their
backend asks weft to mint a consent link scoped to an opaque subject id, the
customer approves at the provider, and the grant lands in weft tagged with that
subject. Credentials stay in weft the whole way. The operator can list and revoke
by subject but never read, and a run bound to one subject can never resolve
another's grant. The existing single-tenant flow is untouched.

## Running it for real

**Cloud deployment, and routes that reach the internet.** Everything today runs
on one machine: a kind cluster, a local Postgres, and a quick tunnel whose proxy
allowlists only the provider-events receiver, the per-signal fire door, the file
relay and the OAuth callback. A `Route` or `Socket` is reached through
`/connect/<tenant>/<path>`, and the dispatcher answers with a redirect to the
worker's own address on the gateway, which only resolves locally; the tunnel does
not forward `/connect/` at all. Routes stay local until this changes. The open
work is the whole deployment story: where the control plane runs and who owns it,
how workers are reached from outside, secrets and identity, images and a
registry, upgrades against a database that is not disposable, and cost and
isolation. A guide to deploying on your own cloud provider lands with it.

**Running outside Kubernetes.** A local LLM, or an AWS, GCP or Azure service,
connected to the rest of the runtime.

## The builders

**Tangle's own CLI.** Tangle today is a set of instructions on top of whatever
coding assistant you use. A proof-of-concept CLI wrote a working program from a
brief in under five minutes. The released version is slower because existing
assistants spend time on fluff weft does not need, and our own CLI fixes that.

**Coordinated assistants in parallel.** Several agents building branches of the
weft codebase under one plan, the same shape weft gives a program.

**Multiple logins for one hosted program.** Letting whoever hosts a program
handle several users of it natively, instead of each install serving one tenant.

## The editor

**A version tree in the sidebar.** `weft tree` already knows every version, what
changed, the runs beneath each, and head. The sidebar shows none of it, so you
cannot see that two runs sit on different branches or that head moved back. The
plan is a git-client-shaped history: a row per version, a lane per branch, the
runs nested under their version, with branching and seed selection on the same
view.
