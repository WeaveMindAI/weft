# What a node is

A node does one thing: calls an API, transcribes a piece of audio, writes a
row, renders a PDF.

It takes its inputs, does that one thing, emits its result, and returns.

## The node does not orchestrate

Looping, retrying, branching, fanning out, gathering results, waiting for a
person between two steps: all of that is the graph's job, expressed
declaratively, and the engine gives you per-iteration journaling,
resumability, and cancellation for free.

When you feel the urge to write one of these in Rust, stop and reach for the
graph instead:

| The urge | The graph's answer |
|---|---|
| a `for` loop driving a multi-step process | a weft `Loop`, firing your node once per iteration |
| retry with backoff around a call | express the retry in the graph |
| "call A, then depending on the result call B or C" | wire A's outputs to both, and let each branch's `_should_flow` decide. A node that is told not to run closes its outputs, which skips everything behind it: [the closure rule](../language/mental-model.md#the-closed-pulse) |
| "work, then wait for a human, then more work" | three nodes |

A node that owns a loop with a wait inside it is a small workflow engine hiding
inside a node, and it has to solve by hand every problem weft already solved.
[Durable execution](durable-execution.md#a-loop-with-a-wait-inside-a-node) has
the survival guide for when a genuine constraint forces that shape on you.

## The node does not do plumbing

A node body contains its own logic and nothing else. No transport choice, no
credential handling, no acknowledgement protocol, no subscription lifecycle, no
retry bookkeeping. Those belong to the language, implemented once and hardened
once, which is why a real node body is usually under a hundred lines. The full
list of what falls on which side is
[the commandments of plumbing](../thinking/plumbing.md).

A node that sends a Slack message reads its inputs, builds the request body,
posts it on the client it was handed, and emits the message id. It has no way
to find out how the token was acquired, whether the call was measured, or whose
money paid for it.

Everything it needs arrives through one object: [the ctx](the-ctx.md).

## When the ctx does not have what you need

**First, check it is missing.** The surface is large, and things are named for
what the caller receives rather than for how they work, so what you want is
often sitting there under a name you would not have guessed. Skim
[The ctx](the-ctx.md), or just ask in Discord and save yourself the reading.

**If it really is missing, you are still not blocked.** A node in your own
project's `nodes/` is nobody's business but yours, so write the workaround
there and ship it today. The higher bar is for the shared catalog, because that is vocabulary
everyone inherits.

**Then come and argue for the mechanism.** Whether a thing is weft's job or
yours is a line we drew and are willing to move, and
[the commandments of plumbing](../thinking/plumbing.md) is where that line
is written down, along with how to argue with it.

If it can be expressed as declared data, it becomes declared data. If it
cannot, it becomes a closed typed variant, added deliberately and
shared by every service.

`StreamListen` is the example worth knowing. Someone needed a trigger that
watches a mailbox, and IMAP speaks neither HTTP nor WebSocket. Instead of an
IMAP branch in the listener, what shipped describes the conversation as data,
and it now serves IMAP, MQTT, Redis and XMPP alike:
[the signal kinds](writing-triggers.md#outbound-event-sources).

## One node, one process

A node embodies exactly one user expectation. When one capability answers two
different questions, build two nodes, even when the machinery underneath is
identical. The test is what the **user** expects, not what the code does:

- `GoogleSheetsRead` signed in, versus reading a public share link, is **one
  node**. The expectation is "read this sheet's rows" either way; only the
  mechanics differ.
- `SlackReceiveMessage` (your bot, your workspace, a channel you picked)
  versus `SlackAppMessages` (you own the app; every workspace that installed
  it) are **two nodes**. Same event stream, different questions, different
  inputs, different outputs.

Which **transport** serves a capability is never a node split, because the
expectation is the same either way and the environment decides. Which **scope**
it operates at always is.

## Why this shape

**It saves the plumbing.** Credential handling, retry logic and cost
accounting are the bulk of what an integration usually costs to write, and none
of it is in the node.

**It makes node-building an independent task.** One job, a small context, a
[test rig](testing.md) that forces the author to prove the node works before it
ships. That is exactly the task shape a model is good at.

Once a node exists, composition cannot misuse it: the compiler enforces its
declared contract, and the API call lives inside it.

## The anatomy

A node is a directory:

```
nodes/my_node/
  mod.rs              the Rust: a `Node` trait impl
  metadata.json       the declared surface: ports, config, presentation
  deps.toml           optional: extra cargo crates this node needs
  tests.rs            optional: the node's own tests
```

The trait has three bodies, and the engine picks which to call from the
manifest. A node never inspects the lifecycle phase itself.

```rust
#[async_trait]
pub trait Node: NodeManifest + Send + Sync {
    /// Infra nodes only. The desired shape of the long-running service.
    async fn provision_infra(&self, ctx: InfraProvisionContext, input: ValueBag)
        -> WeftResult<InfraSpec> { /* default: error */ }

    /// Triggers only. Register the wake signal. Called INSTEAD of `run`
    /// at registration time.
    async fn setup_trigger(&self, ctx: ExecutionContext)
        -> WeftResult<()> { /* default: error */ }

    /// The normal body. The only way to fire downstream is
    /// `ctx.pulse_downstream(output)`.
    async fn run(&self, ctx: ExecutionContext) -> WeftResult<()>;
}
```

Most nodes implement only `run`.

Next: [your first node](your-first-node.md).
