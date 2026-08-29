# The commandments of plumbing

The docs keep telling you that a node does no plumbing. None of them say what
plumbing is. 

Here is where it stops today. It is a line that can move, and asking us to move 
it is a normal thing to do. 

They are also aspirations. A commandment can be right and the thing weft
actually hands you can still not cover your case, or be missing an option you
need. That is worth raising too, and it is the easier fix of the two: the line
is fine, our application of it is not.

**The test underneath all of them:** something is weft's job when every node
that needs it needs the same answer. If two good nodes would reasonably do it
differently, it is yours.

---

**I. Thou shalt not handle credentials.**

Signing in, signing requests, refreshing, revoking, deciding whose account to
use. Your node is handed a client that already works.

**II. Thou shalt not listen for anything.**

A trigger declares what should wake it, and the runtime does the listening:
holding the socket open, taking the push, polling on a timer, checking the
event is genuine, renewing the subscription. Your body runs once per event that
has already arrived.

**III. Thou shalt not build the channel.**

Two nodes alive at the same time exchange messages over a channel weft opens
and holds for them. Sockets, reconnects, acknowledgements and backpressure are
already written.

**IV. Thou shalt not build control flow in Rust.**

Looping, branching, retrying, fanning out, gathering results: you decide all of
it, in the graph, where it is journaled and where somebody can read it. Not
inside one node's body, where nobody can.

**V. Thou shalt not save your own state.**

If the worker dies, a fresh one rebuilds the execution from the journal and
carries on. You keep nothing of your own between runs, and `ctx.run` is how you
say a step must not happen twice.

**VI. Thou shalt not shift bytes around yourself.**

You say what a file is for and how long it should last, and you get all of that
control. Fetching it, storing it, giving it a public address, expiring it,
turning it into whatever a provider wants: that happens underneath you.

**VII. Thou shalt not keep the books.**

Every paid call is priced and attributed as it happens. You never total
anything up, carry a running cost between nodes, or work out whose spend a
call was.

**VIII. Thou shalt not run infrastructure.**

Containers, health checks, volumes, lifecycle. You describe what should be
running and the runtime keeps it that way.

**IX. Thou shalt not police your inputs.**

Every wire was checked before anything ran, so a value arrives as what it was
declared to be.

**X. Thou shalt not keep records.**

Every event is written down as it happens, so there is nothing for you to log
to a file or a table of your own.

**XI. Thou shalt not hand-deliver information to whoever is watching.**

Every value on every wire is already there in the inspector, live and
afterwards. If a firing makes something worth looking at, declare a `display`
and the editor renders it on the node. You never print, and you never add a
port whose only job is to tell a person what happened.

---

## And what is left is yours

Your own logic and nothing else: building this call's request body, reading
this reply, knowing what this provider's errors mean, doing the actual work.

It is a small job on purpose. Everything hard sits behind the ctx, and when
something behind there goes wrong it fails loudly and names what to do next.

## Moving the line

The line sits where it does because of the nodes people have written so far.
Yours might be the one that shows it is in the wrong place.

If you are about to write something the commandments say is ours, come and say
so in [Discord](https://discord.com/invite/FGwNu6mDkU). Bring the node you are
building and the code you would otherwise have to put in its body, because that
code makes the case better than any description of it. Or build the general
version yourself and send a PR, which is usually faster and always welcome.

Same if a commandment holds but what weft gives you falls short: the socket
handling that does not fit your protocol, the display that cannot show your
kind of result, the scope that does not last long enough. Tell us what you hit,
because a commandment we cannot deliver on is worse than one we never made.

One question decides most of these. Can you name another case the mechanism
would serve, besides your own? If the only answer is your provider, what you
have is a hook for it, and we will probably end up talking you out of that. If
you can name others, the line is in the wrong place, and these conversations
are how it gets moved.
