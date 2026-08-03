# Weft Design Principles

> **Note.** This document was written fast to ship the open source release. It may sound a bit AI-generated in places. The principles themselves are real, the prose around them could be tightened. If you have the time to rewrite it more cleanly, a PR that improves the writing is as welcome as one that fixes a bug.

A reference for contributors. These are the opinions that guide every decision in the language. If a feature fights one of these, it does not ship. If you want to argue with some of those, please do so!

## Coordination, not replacement

Weft coordinates things rather than replacing them. LLMs, databases, APIs, humans, code execution: these are primitives in the language, not libraries. Each node is implemented in Rust. Weft handles how they connect, type-check, and execute.

The surface area of the language is small on purpose. You learn the core once, then you compose. Everything interesting is in the nodes.

## If it compiles, the architecture is sound

Rust's memory-safety philosophy, applied to system architecture. The compiler validates:

- **Connections.** Types match at every edge. String into Number is a compile error. Generics, unions, and type variables are all resolved before the program runs.
- **Completeness.** Every required input is wired. Orphan nodes are flagged. A dangling LLM call is a compile error, not a runtime surprise.
- **Node self-validation.** Each node checks its own config (required fields, credential shape, schema validity). If the node compiles, its config is sane.

The only failures left after compilation are external: a service goes down, an API returns an error, a human never responds. Your logic is not in question at that point, the world is.

## No special cases

When a node needs a new capability, the language gets a general feature and the node implements it. No node ever requires custom compiler support.

"Wait for a payment" is not a payment-specific feature. It is "wait for an external signal" at the language level. The Stripe payment node plugs into that. "Collect human input" is not a form-specific feature. It is a generic form schema that HumanQuery happens to use.

Two things fall out of this. First, the language stays tiny. Second, when the AI builder learns a pattern, it works everywhere. No node-specific escape hatches to memorize.

## Recursive composability

Any set of nodes becomes a group. Groups have typed inputs and outputs. From outside, a group looks like a single node. Groups contain groups, arbitrarily deep.

A 100-node system still looks like 5 blocks at the top level. Each group is self-contained: its children can only talk to each other and to the group's own interface ports. No hidden coupling, no global scope.

Previous graph-based languages turned into spaghetti at scale because they had no recursive scoping. Weft does. This is the single most important structural decision in the language.

## Null propagation

Every required input refuses to run on null. When upstream produces nothing, downstream skips, and that skip cascades through the graph until it hits something that can handle it. There is no exception machinery, no try/catch ceremony for the common case. Empty data just flows past.

Optional ports (`?`) opt into receiving null. That is the explicit signal that a node knows how to deal with absence. Everything else stops at the boundary.

This is why branching in Weft is just "the router outputs null on the inactive branch". It is why a skipped LLM call does not crash a 50-node pipeline. It is why parallel processing handles partial failures without special casing.

## Graph-native

The code describes a graph, so there is a native way to view it, interact with it, and analyze it. Edit either view, the other updates. The source of truth is the code.

The graph is not a visualization bolted on. It is the other half of the program.

Why this matters:

- Humans see architecture at a glance.
- Debugging is visual: click a node, see inputs, outputs, errors.
- Tooling can analyze the architecture algorithmically. Find unfiltered user input flowing into an LLM. Find chains of LLMs with no verification step. Find permission boundaries.

## Durable by default

Programs run on [Restate](https://restate.dev). A Weft execution is a durable workflow: every node output is persisted, every suspension point survives crashes, every in-flight program can be resumed exactly where it left off.

This means "wait three days for a human to approve" is the same code as "wait three seconds for an API response". No queue infrastructure to glue together. No background job framework. No state machine to hand-write. The language treats time and failure as first-class, because AI systems live in environments where both are everywhere.

**Note:** This part of the code was done very early and might not be completely wired properly. Feel free to open a PR if you see any issues here.

## Infrastructure as nodes, sidecars as the bridge

Weft systems need real stateful services: databases, caches, browser pools, messaging bridges, vector stores. The design decision is that these are **nodes in the graph**, not environment variables or external config. You drop them in, wire them up, the platform provisions the real resource on Kubernetes. Dependencies and lifecycle are both expressed in the same language as the rest of your project.

The trick that makes this work without welding Weft to every possible backend is the **sidecar pattern**. An infrastructure node in the catalog bundles three things:

1. **Raw Kubernetes manifests** (typically `Deployment`, `PVC`, `Service`), written once in the node's Rust source with placeholders like `__INSTANCE_ID__` and `__SIDECAR_IMAGE__` that the platform fills at provision time.
2. **A sidecar image**: a small Docker image that implements an HTTP protocol the platform knows how to call. Any language, any runtime, as long as it exposes the three endpoints:
    - `POST /action` accepts `{ action, payload }` and returns `{ result }`
    - `GET /health` liveness check
    - `GET /outputs` runtime-computed values the sidecar wants to expose as node output ports (endpoint URL, instance ID, anything else)
3. **An action endpoint** (port + path) telling consumer nodes where to reach the sidecar.

When you start an infrastructure node, the platform generates a unique instance ID, fills placeholders, injects ownership labels (`weavemind.ai/managed-by`, `weavemind.ai/user`, etc.), applies the manifests, waits for `/health`, queries `/outputs`, and emits the returned values on the node's output ports. Consumer nodes (Memory Store, Memory Query, Send WhatsApp Message, etc.) take the endpoint URL as an input and use `InfraClient` (a small retry-enabled HTTP wrapper) to call `/action` with typed payloads. They never talk to the underlying service directly.

**Why this separation matters.** It is tempting to let consumer nodes hold connections to databases, open sockets, speak native protocols. We do not, for a stack of reasons.

- **Capabilities, not drivers.** Consumer nodes talk to "durable KV" or "send a message", not to "Postgres" or "WhatsApp". A different sidecar implementing the same capability (Redis for KV, some other message broker) swaps in without touching any Weft code. The interface is the contract.
- **Security surface.** The sidecar is the only thing with a real connection. It enforces input validation, rate limiting, schema. Users of the consumer nodes cannot write destructive queries because there is no query language exposed, only typed actions.
- **Language freedom.** The sidecar can be in whatever language has the best library for the job. The WhatsApp bridge uses a Node.js lib, so its sidecar is JavaScript. Postgres has a good Rust crate, so its sidecar is Rust. Weft does not care.
- **Isolation.** The sidecar runs in its own container, its own process, with its own resource limits. A buggy database client cannot take down the orchestrator.
- **Lifecycle hooks.** The sidecar can do startup migrations, graceful shutdown, health checks, backups. A raw connection from a consumer node has none of that.

The relationship is this: Weft is the graph and the scheduler, the sidecar is the adapter, and the real service (Postgres, WhatsApp, whatever) is behind the adapter. Weft can add support for any new stateful service by writing a new sidecar and a thin infrastructure node, without touching the core.

The reference implementations live in `sidecars/` (`postgres-database`, `whatsapp-bridge`) along with minimal Rust and JavaScript starter templates under `sidecars/examples/`. Adding a new one is the recipe in `CONTRIBUTING.md`.

## Opinionated by design

The language pushes toward patterns that work. Some things are enforced (type validation, connection completeness, scoped permissions). Some things are warnings (all-optional inputs without `@require_one_of`). Some things are defaults that you can override when you really mean it.

The opinion evolves. Wrong patterns get removed. New good patterns get added. Weft is not neutral, and it should not be. Neutral tools produce neutral code, and neutral code in AI systems is how you end up with 100k lines of patches on patches.

## Dense for AI generation

Weft is designed to be written by AI builders. Dense syntax means fewer tokens. Compile-time validation catches mistakes before the user sees them, so the AI wastes fewer iterations. The grammar is constrained enough that the AI writes correct code more often on the first try.

The language and the AI builder co-evolve. If the AI keeps making the same mistake, the language changes to make that mistake harder to express. If a pattern keeps showing up, it becomes sugar.

This is a feedback loop we take seriously. The goal is not "nice for humans to type by hand". The goal is "the AI ships correct systems in one shot".

## Top-down building

The language supports building from architecture down to implementation. Define the top-level blocks and their interfaces. Validate. Zoom into one block at a time. Mock unfinished pieces. Test progressively. By the time every block is real, each piece is already validated.

Mocking is native: any node or group can be replaced with "return this data instead". The mock is type-checked against the real port signatures, so it cannot silently drift.

This is how you build systems that do not collapse under their own weight. You never have to hold the whole thing in your head at once.
