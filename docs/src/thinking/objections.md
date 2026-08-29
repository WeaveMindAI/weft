# Things people say to me

Collected as I hear them, with what I actually think.

## "This is just Python with extra steps"

No. Three things below cannot be done in Python at all, and "awkward in" is not
what I mean.

**A Python program waiting three days for an approval is a process that exists
for three days.** You can hide that behind a queue and a state machine, which
is what everyone does, but then the thing that waited is your infrastructure,
and the thing that resumed is a different invocation rebuilding its own context
by hand. In weft the process exits and the execution is rows in a table. That
is not an optimisation of the Python version, it is a different object.

**Nothing can check your orchestration, because there is no orchestration to
check.** There is control flow, spread across a dozen files and two frameworks,
and no artifact any tool could read. In weft the wires are the source, so a
type mismatch, an unwired input, or a cycle is refused before anything runs.

**Adding a service means writing the auth.** The author of the S3 node wrote a
JSON block declaring SigV4 and got AWS request signing. Not a helper that made
it easier: they wrote no auth code, and neither will you.

What you *can* do in Python is build all of that yourself, which is exactly
what everyone is doing and where the several hundred lines of plumbing came
from. The question was never whether Python is capable. It is whether you want
to be the person maintaining the durable executor you wrote by accident.

## "The compiler checks the wiring, not whether the program is right"

True today, and it is the interesting half of what comes next rather than a
limit.

What a compiler can check scales with what is legible to it. Because the
orchestration is data, it can be asked to prove properties about the program
itself: flags that turn a policy into a property of compilation.

None of it is shipped, and all of it is reachable. What each flag would buy,
with a worked example, is in
[our approach to AI safety](safety.md#what-becomes-provable).

## "Nobody adopts new languages"

New languages die because somebody has to learn them, and that cost is gone
here. Nobody learns weft. A model writes it and you read the graph, the way
nobody learns SQL's grammar to look at a query and see what it selects.

## "Visual programming always fails"

This one has a graveyard behind it. It fails for three reasons.

**It becomes unreadable past about fifty boxes.** Groups collapse recursively,
and a group is a typed contract you can reason about without opening it, so a
two-hundred-node program is five boxes at the top level. It costs nothing at
run time either, because groups are compiled away before anything executes.

**You cannot diff or merge it.** The source is text: the `.weft` file lives in
git and merges like any file. The picture is a view, and a GUI gesture goes
through the compiler, which rewrites the source and hands it back, so your
comments and formatting survive.

**The boxes eventually cannot express what you need.** The boxes are typed
nodes whose insides are Rust. When the graph cannot express something you write
a node, in minutes, and it is vocabulary forever.

## "You will never keep up with the integrations"

I am not trying to.

The design goal is that adding a service is a JSON file and adding a node is a
folder with two files. While that holds, whoever needs an integration builds it
in an afternoon and it is vocabulary for everyone afterwards. When it stops
holding, that is a bug in the language and gets fixed as one.

## "Rust is a barrier"

Nobody writing weft writes the Rust by hand.

The premise of the whole project is that models write the code. A node body is
usually under a hundred lines because everything hard sits behind the ctx, and
a small self-contained typed unit with its own test rig is the single thing
models are best at producing. Nodes are the easiest part of weft to generate,
not the hardest. And you only reach for one at all when what you need is not
already vocabulary.

The version of this objection that lands is about *reading* Rust when something
goes wrong, which is fair, and is why node bodies are kept small enough to read
in one sitting.

## "Kubernetes on my laptop is absurd"

It sounds absurd right up until you want a Postgres.

Weft can provision a database, a headless browser, or a model server as a node
you drop on the graph, and something has to manage containers, networks,
storage, health and lifecycle for that.

Using the real one means the manifests that work on your laptop work in
production, so there is no separate production setup quietly drifting from your
development one. You write no YAML and you will not think about the cluster
again after installing it.

## "It is a graph, so it cannot do X"

Two of the three things people mean by this are deliberate.

**Cycles are refused.** You iterate with a `Loop` and exchange feedback over a
bus. Refusing cycles is why the compiler can prove things about the rest.

**Two nodes talking while both run** is not expressible with pulses alone,
which is why buses exist. A parallel loop can launch fifty agents, gather their
channels, and have a coordinator talking to all fifty while they keep
working.

**A synchronous call and return between nodes** is the real one. Weft is
structurally a process network rather than a call graph, and node-to-node
function callbacks are designed, with one architectural question still open
about how they ride the replay machinery: see
[the roadmap](../appendix/roadmap.md#language). Until they land, agent loops
work but are less elegant than they will be.

## "The docs claim things the code does not do"

If you find one, report it and I will fix it.

Every claim in this book is grounded: read in the source, run, or written by
the person who built the thing. Where something is a direction rather than
shipped, the page says so.
