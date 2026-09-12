# The commandments of plumbing

The docs keep telling you a node does no plumbing, and then never tell you what
plumbing actually is. So here it is.

This is where the line sits today. It can move, if you think it should, tell us.

They're also a bit aspirational. A commandment can be completely right and the
thing weft hands you can still not cover your case today, or be missing an option you
need, tell us about that one too.

**The test underneath all of them:** it's weft's job when every node that needs
the mechanism needs the same or a similar answer. If two good nodes would reasonably do it completely differently, it's the job of your program.

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
it, in the graph, where it's journaled and someone can actually read it. Not
buried in one node's body where nobody can.

**V. Thou shalt not save your own state.**

No file on the side, no table of your own, no "I'll just keep this in memory
between runs". If the worker dies, a fresh one rebuilds the execution from the
journal and keeps going, and `ctx.run` is how you say a step must not happen
twice.

**VI. Thou shalt not carry files around.**

You say what a file is for and how long it should stick around, and you get all
of that control. Fetching it, storing it, giving it a public address, expiring
it, turning it into whatever shape a provider wants: that happens underneath
you.

**VII. Thou shalt not keep the books.**

Every paid call gets priced and attributed as it happens. You never total
anything up, carry a running cost between nodes, or work out whose spend a call
was.

**VIII. Thou shalt not run infrastructure.**

Containers, health checks, volumes, lifecycle. You describe what should be
running and the runtime keeps it that way.

**IX. Thou shalt not police your inputs.**

No checking that an input is really a string, no unwrapping something you were
already promised, no defensive parse at the top of your body. Every wire got
checked before anything ran, so the value is the thing it says it is or it would have crashed before reaching your code.

**X. Thou shalt not write down what happened.**

No log file, no print so you can work out later what went on, no audit trail of
your own. Every event is journaled as it happens, and that journal is what the
runtime replays to bring an execution back, so a record you keep on the side is
one the runtime can't restore from anyway.

**XI. Thou shalt not report to the human.**

Every value on every wire is already in the inspector, live and afterwards. If a
firing makes something worth looking at, declare a `display` and the editor
renders it on the node. You never hand-deliver information to whoever is
watching.
---

## What's left is yours

Your own logic and nothing else: building this call's request body, reading this
reply, knowing what this provider's errors mean, doing the actual work.

It's a small job on purpose. Everything hard sits behind the ctx, and when
something back there goes wrong it fails loudly and tells you what to do next.

This also allow the language to be smarter because there is a single mechanism for e.g. "two nodes are talking together".

## Moving the line

The line sits where it does because of the nodes people have written so far.

If you're about to write something that you think is plumbing and should be integrated 
in the commandments, come say so on [Discord](https://discord.com/invite/FGwNu6mDkU) or in an issue. 
Bring the node you're building and the code you'd otherwise have to stuff in its body. 
Or build the general version yourself and send a PR, which is usually faster and always welcome (ask on discord first to not waste your time).

Same if a commandment holds but what weft gives you falls short: socket handling
that doesn't fit your protocol, a display that can't show your kind of result, a
scope that doesn't last long enough. Tell us what you hit