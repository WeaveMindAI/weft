# Contributing to Weft

> **Note.** This document was written fast to ship the open source release. It may sound a bit AI-generated in places. If you have the time to rewrite it more cleanly, a PR that improves the writing is as welcome as one that fixes a bug.

Thanks for even considering it. Weft is early, opinionated, and moves fast, and every external eye on it makes the language better. This document tells you how to get set up, what the repo looks like, and the rules of engagement when you send code.

If anything in here is wrong, unclear, or out of date, that is itself a bug. Open an issue.

---

## Before you start

- **Read [DESIGN.md](./DESIGN.md).** The design principles are not decoration. They are the filter every pull request runs through. If a change fights one of them, it either gets reshaped or dropped. Knowing them up front saves everybody time.
- **Check the [roadmap](./ROADMAP.md) and open issues.** Someone might already be working on the thing you want to build. Ask in [Discord](https://discord.com/invite/FGwNu6mDkU) before starting a large change.
- **Small changes: just open a PR.** Typos, doc fixes, obvious bugs, adding a missing error message. Go for it.
- **Medium or large changes: open an issue first.** Describe what you want to build and why. Wait for a thumbs up. This protects your time: no one enjoys closing a 500-line PR because the approach does not fit.

---

## Getting set up

### Prerequisites

- [Docker](https://docs.docker.com/get-docker/) for PostgreSQL.
- [Node.js](https://nodejs.org/) 18+.
- macOS only: `brew install bash` (Bash 4+ required).

Rust, Restate, and pnpm are installed automatically on first run.

### Clone and run

```bash
git clone https://github.com/WeaveMindAI/weft.git
cd weft
cp .env.example .env
# Edit .env to add your API keys (OpenRouter, Tavily, etc.)

# Terminal 1: backend
./dev.sh server

# Terminal 2: dashboard
./dev.sh dashboard
```

Open http://localhost:5173. If anything crashes or refuses to start, that is a bug and we want to hear about it.

### Useful commands

```bash
./dev.sh server       # Backend only
./dev.sh dashboard    # Frontend only
./dev.sh all          # Both, server in background
./dev.sh extension    # Build the browser extension

./cleanup.sh          # Stop everything and reset state
./cleanup.sh --no-db  # Keep the database

cargo build           # Compile all Rust crates
cargo test            # Run the Rust test suite (no DB needed, .sqlx is committed)
cargo clippy          # Lint
pnpm -C dashboard check  # Svelte type check
```

---

## Repo layout

```
weft/
├── catalog/                # Node definitions (source of truth, see below)
├── crates/
│   ├── weft-core/          # Type system, compiler, executor, Restate objects
│   ├── weft-nodes/         # Node trait, registry, node runner binary
│   ├── weft-api/           # REST API (triggers, files, infra, usage)
│   └── weft-orchestrator/  # Restate services and Axum project executor
├── dashboard/              # Web UI (SvelteKit + Svelte 5)
├── extension/              # Browser extension (WXT)
├── scripts/                # Dev helpers (catalog-link, etc.)
├── DESIGN.md               # Design principles
├── ROADMAP.md              # What's coming
└── dev.sh                  # Development entry point
```

The `catalog/` directory is the source of truth for every node. `scripts/catalog-link.sh` (run by `dev.sh`) symlinks it into the Rust crate and the dashboard. Do not duplicate node files. Always edit the originals in `catalog/`.

---

## How to add a node

A node is one folder under `catalog/<category>/<node_name>/` with two files.

**`backend.rs`**: the Rust implementation.

```rust
//! Greeting Node - says hi.

use async_trait::async_trait;
use crate::node::{Node, NodeMetadata, NodeFeatures, PortDef, ExecutionContext, FieldDef};
use crate::{NodeResult, register_node};

#[derive(Default)]
pub struct GreetingNode;

#[async_trait]
impl Node for GreetingNode {
    fn node_type(&self) -> &'static str {
        "Greeting"
    }

    fn metadata(&self) -> NodeMetadata {
        NodeMetadata {
            label: "Greeting",
            inputs: vec![
                PortDef::new("name", "String", false),
            ],
            outputs: vec![
                PortDef::new("message", "String", false),
            ],
            features: NodeFeatures { ..Default::default() },
            fields: vec![],
        }
    }

    async fn execute(&self, ctx: ExecutionContext) -> NodeResult {
        let name = ctx.inputs.get("name")
            .and_then(|v| v.as_str())
            .unwrap_or("stranger");
        NodeResult::completed(serde_json::json!({
            "message": format!("Hi, {}!", name)
        }))
    }
}

register_node!(GreetingNode);
```

**`frontend.ts`**: the dashboard UI definition.

```typescript
import type { NodeTemplate } from '$lib/types';
import { Hand } from '@lucide/svelte';

export const GreetingNode: NodeTemplate = {
  type: 'Greeting',
  label: 'Greeting',
  description: 'Generates a greeting for a name.',
  isBase: false,
  icon: Hand,
  color: '#6b7280',
  category: 'Utility',
  tags: ['greeting', 'hello', 'text'],
  fields: [],
  defaultInputs: [
    { name: 'name', portType: 'String', required: true, description: 'Person to greet' },
  ],
  defaultOutputs: [
    { name: 'message', portType: 'String', required: false, description: 'Greeting message' },
  ],
  features: {},
};
```

That is it. The `inventory` crate auto-discovers the new node at startup on the backend, and the dashboard picks up the new template on the next reload.

### Checklist before you open the PR

- [ ] The backend and frontend port names and types match exactly.
- [ ] Every input and output has a clear one-line description.
- [ ] The node has a sensible icon and category.
- [ ] If the node needs credentials, it uses an existing `*Config` node or you added a new `*Config` alongside it.
- [ ] You added the node to `catalog-tree.json` if it applies.
- [ ] You tried building a tiny project that uses the node, end to end, from the dashboard.
- [ ] `cargo test`, `cargo clippy`, and `pnpm -C dashboard check` all pass.

### Node design rules

These come straight from [DESIGN.md](./DESIGN.md), do not skip them.

- **No special cases.** If your node needs a new capability, propose the capability as a general language feature first. Do not bolt it into a single node.
- **Typed end to end.** Every port has a concrete type. No `Any`. No untyped dicts except `JsonDict` for genuinely opaque JSON.
- **One thing per node.** If your node is doing five different things based on config flags, it is five nodes.
- **Surface errors loudly.** Nodes either work or fail with a clear error message, and should handle all the types they are declaring they can handle. No silent fallbacks, no guessed defaults for values the user was supposed to provide.

---

## The compiler and the language

Core language work (parser, type system, edge resolution, groups, parallel processing) lives in `crates/weft-core/`. This is the most opinionated part of the codebase. Before you touch it:

1. Read the relevant doc page in the [language reference](https://weavemind.ai/docs).
2. Open an issue describing the change.
3. Wait for a thumbs up.

Reason: changes to the compiler affect every single project written in Weft. A "small improvement" to the type checker can silently break a user's production pipeline. We are not paranoid, we are careful.

Tests for the compiler live alongside the code in `crates/weft-core/src/tests/`. Any change to parsing or type resolution needs a test. Any bug fix needs a test that fails before the fix and passes after.

---

## Infrastructure nodes and sidecars

A regular node (see "How to add a node" above) is code that runs during execution. An **infrastructure node** is different: it provisions a real Kubernetes workload on Start and tears it down on Stop. Think Postgres databases, WhatsApp bridges, browser pools, vector stores: anything stateful that needs to outlive a single execution.

Infrastructure nodes are the pattern Weft uses to plug stateful services into the graph. They are more work than a regular node (you are also writing a sidecar service), but they buy you a clean abstraction, language freedom, and real isolation. The design rationale is in [DESIGN.md](./DESIGN.md) under "Infrastructure as nodes, sidecars as the bridge". Read that first.

### The two pieces

Every infrastructure node is actually two things shipped together:

1. **The infrastructure node itself** in `catalog/<category>/<name>/`. Same two-file layout as a regular node (`backend.rs` + `frontend.ts`), but the backend returns an `InfrastructureSpec` in its `NodeFeatures` instead of just executing. The spec contains raw Kubernetes manifests (as JSON) with placeholders like `__INSTANCE_ID__` and `__SIDECAR_IMAGE__` that the platform fills at provision time.
2. **A sidecar service** in `sidecars/<name>/`. A small HTTP service (any language, any runtime) that exposes three endpoints:
    - `POST /action` accepts `{ action, payload }` and returns `{ result }`
    - `GET /health` liveness check for the K8s readiness probe
    - `GET /outputs` runtime-computed values the sidecar wants to expose as the node's output ports

The reference implementations are `sidecars/postgres-database/` (Rust) and `sidecars/whatsapp-bridge/` (Node.js). Minimal starter templates live in `sidecars/examples/rust/` and `sidecars/examples/javascript/`. Copy whichever matches your language, and work from there.

### Consumer nodes

On top of the infrastructure node you usually ship a family of **consumer nodes** (e.g. `MemoryStoreAdd`, `MemoryQuery`, `MemoryDelete` for the Postgres case). Consumer nodes are regular nodes, they take an `endpointUrl` as input, build an `InfraClient` from it, and call `/action` with typed payloads. They never touch the underlying service directly.

This is the point of the whole pattern: the consumer nodes talk to a **capability** (durable KV, send-message, whatever) through a typed action API. The sidecar is the only thing that knows about Postgres (or WhatsApp, or whatever) and that is the only place you would change if you wanted to swap out the backend.

### The full checklist

- [ ] Write the sidecar. Docker-ize it. Make sure `/health` and `/outputs` work.
- [ ] Push the image to a registry Weft can pull from. For the reference nodes this is `ghcr.io/weavemindai/sidecar-<name>:latest`. The platform uses `SIDECAR_IMAGE_REGISTRY` (or its default) plus the `sidecarName` from your `InfrastructureSpec` to build the image reference at provision time.
- [ ] Write the infrastructure node in `catalog/<category>/<name>/backend.rs`. Return an `InfrastructureSpec` with your manifests, your `sidecarName`, and your `ActionEndpoint`.
- [ ] Write the matching `frontend.ts` with `features: { isInfrastructure: true }`, output ports `instanceId` and `endpointUrl` (plus whatever else your sidecar exposes via `/outputs`).
- [ ] Write at least one consumer node that wires into the infrastructure node and calls an action.
- [ ] Test the whole thing against a local kind cluster: `INFRASTRUCTURE_TARGET=local ./dev.sh server`, then build a tiny project, click Start on the infra node, watch it provision, verify the consumer node can talk to it.
- [ ] Test Stop and Terminate: stop keeps the PVC and data, terminate destroys both.

### Design rules for infrastructure nodes

- **One capability per sidecar.** If your sidecar is doing two unrelated things, it is two sidecars.
- **Do not leak implementation details into consumer nodes.** Consumer nodes should call `put`, not "insert into a Postgres table". If a consumer node needs to know it is talking to Postgres specifically, the abstraction is wrong.
- **`/outputs` is authoritative.** Anything the node exposes as an output port must come from `/outputs` at provision time. Do not hardcode URLs or IDs in the node's Rust file.
- **Manifests use placeholders.** `__INSTANCE_ID__` and `__SIDECAR_IMAGE__` are the standard ones. Do not hardcode names: every pod has to be unique per (user, project, node) or you get collisions.
- **Label everything.** The platform injects ownership labels (`weavemind.ai/managed-by`, `weavemind.ai/user`, `weavemind.ai/project`, `weavemind.ai/node`) into every resource you ship. Do not strip them.
- **Readiness probes matter.** The platform polls pod readiness before calling `/outputs`. Ship realistic `readinessProbe` values for every container in your `Deployment` or the platform will think your infra is ready before it actually is.

---

## The dashboard

`dashboard/` is a SvelteKit + Svelte 5 app. It is the graph view, the code view, and the AI builder UI wrapped into one.

- Svelte 5 runes (`$state`, `$derived`, `$effect`). No legacy reactive statements.
- Types come from `$lib/types`. Do not duplicate interfaces.
- The parser lives in `$lib/parser`. Long term, we are moving parsing into Rust (see [ROADMAP.md](./ROADMAP.md)). In the meantime, keep the frontend parser and the backend parser in lockstep.

---

## Commit, branch, PR

- **Branch naming**: `fix/short-description`, `feat/short-description`, `docs/short-description`. One branch per logical change.
- **Commit messages**: short summary on line 1, blank line, body explaining the "why". Imperative mood ("fix parser crash on empty groups", not "fixed").
- **One thing per PR.** A refactor and a feature in the same PR is two PRs.
- **Link the issue.** `Closes #123` in the PR body if applicable.
- **No AI-generated slop.** If an AI wrote your PR, read it yourself first. We will notice if there are issues, and it wastes everyone's time.

### PR checklist

- [ ] Code compiles and all tests pass locally.
- [ ] New code has tests.
- [ ] Public functions and types have one-line docs where useful. Do not write essays.
- [ ] No unrelated formatting churn.
- [ ] No commented-out code.
- [ ] No `TODO` or `FIXME` without a linked issue.

---

## What not to do

- Do not add a new primitive type to the language without discussion.
- Do not add a "quick fix" that bypasses the type checker.
- Do not add libraries for things we can do in 20 lines.
- Do not add silent fallbacks. Fail loud.

---

## Getting help

- **Discord**: [join here](https://discord.com/invite/FGwNu6mDkU). Fastest for questions.
- **GitHub Discussions**: for longer-form proposals and design conversations.
- **GitHub Issues**: for bugs and concrete feature requests.
- **Email**: contact@weavemind.ai.

## Ground rules

Weft runs on **constructive confrontation**. We would rather have a 30-minute argument that ends in alignment than three weeks of polite avoidance that ends in a shipped disaster. This project is not a "nice" culture, it is a **respectful** one. That distinction matters, so read this section carefully before you send your first PR.

**What is welcome:**

- Strong disagreement with a design, a decision, a PR, a commit, a pattern, a blocker. With real heat behind it if that is how you feel.
- Calling out a bug, a broken approach, or a regression bluntly. Swear words at a problem are fine if they are scoped and in service of fixing something.
- Pushing back on a maintainer when you think they are wrong. Including Quentin. Especially Quentin.
- Arguing until both sides understand each other. The goal of an argument here is not "someone wins", it is "we both leave knowing more".

**What is not welcome:**

- Anger or insults aimed at a **person**. Attacks on someone's character, intelligence, background, identity, or experience. Hard no, zero tolerance.
- Sarcasm or condescension in code review. Say what you mean directly.
- "You are lazy", "you clearly did not read", "this is amateur work". Those describe a person. Replace them with "this PR misses X", "this approach breaks Y", "this does not match the convention in Z". Same point, aimed at the work.
- Piling on. One maintainer's pushback is enough. If you see a contributor getting dogpiled, step in.

**The test.** After an argument, does everyone involved understand the problem better, or does one side just feel worse? If the first, we are doing it right. If the second, somebody crossed the line and we will step in.

**Calibrate to context.** A design argument between people who have been working together for a year can run hot. A first-time contributor's PR review should stay warm. Same respect, different temperature, because the trust bandwidth is different. If you are new, you will find us friendly and careful. If you stick around, you will find us blunt and fast. Both are intentional.

We follow the [Contributor Covenant](./CODE_OF_CONDUCT.md) as the floor. The rules above describe the ceiling of what kind of heat is welcome above that floor. If someone is making the project worse to be around, email contact@weavemind.ai and we will handle it.

---

Thanks for contributing. The project is better because you showed up.
