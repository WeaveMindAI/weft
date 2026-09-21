# The commandments of plumbing

A node does its own job and nothing else. Everything underneath it, the
credentials, the listening, the storage, the record of what happened, belongs to
weft.

Here is where that line sits today, and what it means in practice.

**The test underneath all of them:** if every node that needs a mechanism would
need the same answer, the mechanism belongs in weft. If two good nodes would do
it completely differently, it belongs in your program.

---

**I. Thou shalt not handle credentials.**

Signing in, signing requests, refreshing, revoking, deciding whose account to
use. You get handed a client that already works.

**II. Thou shalt not listen for anything.**

A trigger says what should wake it up and the runtime does the listening:
holding the socket open, taking the push, polling on a timer, checking the event
is real, renewing the subscription.

**III. Thou shalt not build the channel.**

Two nodes alive at the same time talk over a channel weft opens and holds for
them. Sockets, reconnects, acks and backpressure are already written.

**IV. Thou shalt not build control flow in Rust.**

Looping, branching, retrying, fanning out, gathering results: you decide all of
it, in the graph, where it is journaled and somebody can read it. Not buried in
one node's body where nobody can.

**V. Thou shalt not save your own state.**

No file on the side, no table of your own, no "I'll just keep this in memory
between runs". If the worker dies, a fresh one rebuilds the execution from the
journal and keeps going, and `ctx.run` is how a step reuses its recorded result
instead of running again.

**VI. Thou shalt not carry files around.**

You say what a file is for and how long it should stick around, and you get all
of that control. Fetching it, storing it, giving it a public address, expiring
it, turning it into whatever shape a provider wants: that happens underneath
you.

**VII. Thou shalt not keep the books.**

A call on a metered connection gets its cost recorded as it happens, as far as
the provider's evidence allows. You never total anything up, carry a running
cost between nodes, or work out whose spend a call was.

**VIII. Thou shalt not run infrastructure.**

Containers, health checks, volumes, lifecycle. You describe what should be
running and the runtime keeps it that way.

**IX. Thou shalt not police your inputs.**

No checking that an input is really a string, no unwrapping something you were
already promised, no defensive parse at the top of your body. Every wire was
checked before anything ran, so the value is the thing it says it is. A wrong
one fails before it reaches your code.

**X. Thou shalt not write down what happened.**

No log file, no print so you can work out later what went on, no audit trail of
your own. Every event is journaled as it happens, and that journal is what the
runtime replays to bring an execution back, so a record you keep on the side is
one the runtime cannot restore from anyway.

**XI. Thou shalt not report to the human.**

Every value on every wire is already in the inspector, live and afterwards. If a
firing makes something worth looking at, declare a `display` and the editor
renders it on the node. You never hand-deliver information to whoever is
watching.

---

These are also aspirational. A commandment can be right and weft can still not
cover your case yet, or be missing an option you need. If you think the line
should move, come and tell us about that one on
[Discord](https://discord.com/invite/FGwNu6mDkU).

## What is left is yours

Your own logic: building this call's request body, reading this reply, knowing
what this provider's errors mean, doing the actual work.

It is a small job on purpose. Everything hard sits behind the ctx, and when
something back there goes wrong, it fails loudly.

## Moving the line

The line sits where it does because of the nodes people have written so far.

If you are about to write something you think is plumbing and belongs in weft,
come and say so on [Discord](https://discord.com/invite/FGwNu6mDkU) or in an
issue. Bring the node you are building and the code you would otherwise have to
stuff in its body. Or build the general version yourself and send a PR, which is
usually faster and always welcome; ask on Discord first so you do not waste your
time.

Same if a commandment holds but what weft gives you falls short: socket handling
that does not fit your protocol, a display that cannot show your kind of result,
a scope that does not last long enough. Tell us what you hit.
