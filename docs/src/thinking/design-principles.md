# Design principles

These are the rules we use when deciding whether something belongs in weft. They're here so you can hold us to them, and so you can argue with them.

## Put the coordination where you can read it

A model call and a database query are both just steps with named inputs and outputs. The graph says how their work fits together, what happens inside each one is that step's own business. So you can change who approves an answer without reading the model code, and swap a step for another one that does the same job.

The point isn't that models are special. A step can finish in a millisecond or stay alive for a week swapping messages with other steps, and plenty of weft programs have no model in them at all. What they have in common is that the thing deciding what happens next is the graph, which was written down and checked before anything ran.

## Make the shape readable before the details

You should be able to read what a program does before reading how any of it works. Groups do that in the source as much as in the picture: read the interface, open the body only when you have to work inside it.

The test we apply to a feature is whether a decision stays visible in the program. If whether a message needs approval ends up settled somewhere inside a step, we got it wrong.

## Add the mechanism, not the special case

A new service should need its own declaration and its own code. What we try hard to avoid is a special branch for it inside weft's runtime. A service says how to get hold of a credential and how to sign a request, using mechanisms weft already has, and when a protocol needs something genuinely new, the job is to build that mechanism once so every other service gets it too.

So the question to ask isn't how many catalog entries there are. It's whether you can add the thing your program needs without rebuilding the machinery underneath it. For where that line sits today, read [the commandments of plumbing](plumbing.md).

## Refuse a mistake as soon as anything can see it

The compiler sees a badly typed connection before anything runs. A service can refuse a missing credential the moment you connect it. Each check belongs wherever there's finally enough information to make it, and not once you reach the place where the issue will break something worse.

When something does fail it should say what went wrong and what you can do next to fix it.

## One implementation of anything shared

If every step has to repeat the same dance to open a connection, that dance belongs in weft. A step should ask for the connection and get on with the request it wanted to make. This is opinionated on purpose: there is one way a file is stored and one way a credential is held, so each piece gets hardened once instead of half-written again in every node.

For people contributing to weft's development, the same goes for weft's own insides.
