# What a node is

A node is a step you can drop in a weft graph. It might count words, call a
model, or wait for somebody to reply. Its declaration says what it takes in and
what it can send out, and its Rust does the work.

You do not learn another language for the work itself. weft is a framework for
the inside of a node, and a language for arranging nodes into a program.

## Decide what the node is for first

Say you are building a support assistant. Investigating the problem, checking
the proposed answer, and sending it are three good nodes, because somebody
might want to change the checker without touching the investigator, or put a
person in front of the send.

Inside the investigator, a loop reading fields off an API response is just
Rust. It belongs in the graph when the person writing the program needs to wire
it, look at its result, or swap it out.

That is also what makes a node a job you can hand to an assistant: give it the
inputs, the outputs, and some examples of a good answer, and it can build and
test that on its own. The port types say how the piece fits back in. Your
instructions and tests are the only thing saying what good work looks like.

## The files

A node lives under your project's `nodes/`:

```text
nodes/word_count/
  metadata.json       name, inputs, outputs, how it looks in the editor
  mod.rs              the Rust
  tests.rs            its tests, if it has any
  deps.toml           extra crates, if it needs any
```

The compiler reads `metadata.json` to check the graph without compiling any
Rust, and pulls in the implementation of whatever you actually used when it
builds the program.

Several nodes can share helpers and dependencies in a package. For that, read
[packaging](packaging.md).

## Inside

You implement the `Node` trait's `run`. It gets an `ExecutionContext`, called
`ctx` everywhere, which is your inputs and everything weft provides.

Read the inputs, do the work, emit through the ctx. Returning `Ok(())` just
ends the body: it does not send anything back to the graph, so a word counter
has to emit its number on its `count` output explicitly.

A body can also stay alive and swap messages with other nodes. Ordinary outputs
emit once per firing; generator ports emit a sequence. For those, read
[streams and buses](streams-and-buses.md).

Two kinds of node have a second method:

- A **trigger** implements `setup_trigger` to register what should start a run,
  and its `run` handles each event that arrives. See
  [writing a trigger](writing-triggers.md).
- An **infrastructure node** implements `provision_infra` to describe the
  service it needs, and weft has that running before it calls `run`. See
  [infrastructure nodes](infrastructure.md).

## Use what is already there

A node that posts a message has to build the request and check the provider
took it. Opening the account connection and signing the request is not its
job, so use the ctx's client for that part.

The same ctx gives you storage, durable waits and the rest. Before building one
of those yourself, check [the ctx](the-ctx.md). If something genuinely is
missing, propose it or bring it to
[Discord](https://discord.com/invite/FGwNu6mDkU). For where that line sits,
read [the commandments of plumbing](../thinking/plumbing.md).

The compiler will catch a connection with the wrong type. It has no idea
whether your word counter counts correctly. That is what the tests are for.

Next, [write your first node](your-first-node.md).
