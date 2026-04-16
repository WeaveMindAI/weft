# Weft

> **Building in public, two months in.** Weft is young. The language, the type system, and the durable executor are the stable parts. The node catalog is small and intentionally opinionated (a few dozen nodes across LLM, code, communication, flow, storage, and triggers). The long-term vision is to let projects define their own nodes fluently in the language itself, but that is still ahead. If you are evaluating it for production, treat it as a foundation to build on, not a finished product. Breaking changes are expected while the shape is still settling; they will be announced, and migration notes will come with them.
>
> **Note on the docs.** This README, `DESIGN.md`, `CONTRIBUTING.md`, `CODE_OF_CONDUCT.md`, and most of the prose around the project were written fast to get the open source release out the door. They may sound a bit AI-generated in places. If you have the time and taste to rewrite any of them more cleanly, please do. A PR that improves the writing is as welcome as one that fixes a bug.

**A programming language for AI systems.**

In 2026, real software calls LLMs, spins up databases, waits for humans, browses the web, coordinates agents. Where are those primitives? You are still importing libraries and writing plumbing for things that should be one line.

Weft is a language where LLMs, humans, APIs, and infrastructure are base ingredients. You wire them together, the compiler checks the architecture, and you get a visual graph of your program automatically. No plumbing.

- **First-class humans.** Pause mid-program, send a form to a human, wait days, resume exactly where you left off. One node. No webhooks, no polling, no state management.
- **Recursively foldable.** Any group of nodes collapses into a single node with a described interface. A 100-node system still looks like 5 blocks at the top level.
- **Typed end to end.** Generics, unions, type variables, null propagation. The compiler catches missing connections, type mismatches, and broken architecture before anything runs.
- **Durable execution.** Programs survive crashes and restarts via [Restate](https://restate.dev). A human approval that takes three days is the same code as one that takes three seconds.
- **Built-in nodes.** LLM, Code, HTTP, Human Query, Gate, Template, Discord, Slack, Telegram, WhatsApp, Email, X, Postgres, Memory, Apollo, Web Search, and more, the end goal will be to allow AI to build custom node on the fly using the langauge features (this is not the case right now).
- **Two native views.** Same program, rendered as dense code for AI builders and as a graph for humans. Edit either, the other updates. Nothing is bolted on.

Read the full story: [The Future of Programming (and Why I'm Building a New Language)](https://weavemind.ai/blog/announcement).

Full documentation: [weavemind.ai/docs](https://weavemind.ai/docs).

Join the community on [Discord](https://discord.com/invite/FGwNu6mDkU).

## Star History

<a href="https://www.star-history.com/?repos=WeaveMindAI%2Fweft&type=date&legend=top-left">
 <picture>
   <source media="(prefers-color-scheme: dark)" srcset="https://api.star-history.com/chart?repos=WeaveMindAI/weft&type=date&theme=dark&legend=top-left" />
   <source media="(prefers-color-scheme: light)" srcset="https://api.star-history.com/chart?repos=WeaveMindAI/weft&type=date&legend=top-left" />
   <img alt="Star History Chart" src="https://api.star-history.com/chart?repos=WeaveMindAI/weft&type=date&legend=top-left" />
 </picture>
</a>

---

## Quick start

### Prerequisites

- [Docker](https://docs.docker.com/get-docker/) for PostgreSQL
- [Node.js](https://nodejs.org/)
- macOS only: `brew install bash` (Bash 4+ required).

Rust, Restate, and pnpm are installed automatically if missing.

### Run

```bash
git clone https://github.com/WeaveMindAI/weft.git
cd weft
cp .env.example .env
# Edit .env to add your API keys (OpenRouter, Tavily, etc.)

# Terminal 1: backend (auto-installs deps, starts PostgreSQL, Restate, all services)
./dev.sh server

# Terminal 2: dashboard
./dev.sh dashboard
```

Open http://localhost:5173 and build your first project. A tour of the dashboard and a walkthrough of the language live in the [Getting Started](https://weavemind.ai/docs/hello-world) guide.

### VS Code

From the parent workspace, run the **Dev Local All** task to start both the server and the dashboard in split terminals.

### API keys

All keys are optional. Nodes that need one show a clear error at run time if the key is missing.

```bash
OPENROUTER_API_KEY=     # LLM nodes (OpenRouter)
TAVILY_API_KEY=         # Web Search nodes
ELEVENLABS_API_KEY=     # Speech-to-Text nodes
APOLLO_API_KEY=         # Apollo enrichment nodes
DISCORD_BOT_TOKEN=      # Discord nodes
```

PostgreSQL is auto-provisioned via Docker. No manual database setup.

---

## A tiny Weft program

```weft
# Project: Poem Generator
# Description: Writes a short poem about any topic

topic = Text {
  label: "Topic"
  value: "the silence between stars"
}

llm_config = LlmConfig {
  label: "Config"
  model: "anthropic/claude-sonnet-4.6"
  systemPrompt: "Write a short, beautiful poem (4-6 lines) about the given topic."
  temperature: "0.8"
}

poet = LlmInference -> (response: String) {
  label: "Poet"
}
poet.prompt = topic.value
poet.config = llm_config.config

output = Debug { label: "Poem" }
output.data = poet.response
```

Four nodes, four edges. The LLM takes a topic and a config, writes a poem, the Debug node displays it. The compiler already validated every connection and every type before this paragraph finished loading.

For the full language, start with [Nodes](https://weavemind.ai/docs/nodes), [Connections](https://weavemind.ai/docs/connections), and [Types](https://weavemind.ai/docs/types).

---

## Project layout

```
weft/
├── catalog/                # Node definitions (source of truth)
│   ├── ai/                 #   LLM config and inference
│   ├── code/               #   Python execution
│   ├── communication/      #   Discord, Slack, Telegram, WhatsApp, Email, X
│   ├── data/               #   Text, Number, Dict, List, Pack/Unpack
│   ├── enrichment/         #   Apollo, Web Search, Speech-to-Text
│   ├── flow/               #   Gate, Human Query/Trigger
│   ├── storage/            #   Postgres
│   └── triggers/           #   Cron, webhooks, polling
├── crates/
│   ├── weft-core/          # Type system, Weft compiler, executor, Restate objects
│   ├── weft-nodes/         # Node trait, registry, sandbox, node runner binary
│   ├── weft-api/           # REST API (triggers, files, infra, usage)
│   └── weft-orchestrator/  # Restate services and Axum project executor
├── dashboard/              # Web UI (SvelteKit + Svelte 5)
├── extension/              # Browser extension for human-in-the-loop (WXT)
├── scripts/
│   └── catalog-link.sh     # Links catalog/ into crates and dashboard (run by dev.sh)
├── init-db.sh              # PostgreSQL container setup
├── cleanup.sh              # Stop services and reset state
└── dev.sh                  # Development entry point
```

### How nodes work

The `catalog/` directory is the source of truth for every node. Each node is a folder with two files:

- `backend.rs`: the Rust implementation (the `Node` trait).
- `frontend.ts`: the dashboard UI definition (ports, config fields, icon).

`scripts/catalog-link.sh` (run automatically by `dev.sh`) symlinks these into the Rust crate and the dashboard. The `inventory` crate auto-discovers every node at startup. Adding a new node is two files in one folder. The walkthrough is in [CONTRIBUTING.md](./CONTRIBUTING.md).

---

## Development

```bash
./dev.sh server       # Backend (Restate + orchestrator + API + node runner)
./dev.sh dashboard    # Frontend (SvelteKit dev server)
./dev.sh all          # Both (server in background, dashboard in foreground)
./dev.sh extension    # Build browser extension

./cleanup.sh                # Stop everything, clean Restate data + DB
./cleanup.sh --no-db        # Stop everything but keep database
./cleanup.sh --services     # Just stop services
./cleanup.sh --db-destroy   # Remove PostgreSQL container entirely
```

### Infrastructure nodes

Nodes like Postgres Database provision real Kubernetes resources. For local development with infrastructure nodes:

```bash
# Install kind (local K8s)
curl -Lo ./kind https://kind.sigs.k8s.io/dl/v0.31.0/kind-$(uname -s | tr '[:upper:]' '[:lower:]')-amd64
chmod +x ./kind && sudo mv ./kind /usr/local/bin/kind

# Run with local K8s
INFRASTRUCTURE_TARGET=local ./dev.sh server
```

Infrastructure is optional. Projects that do not use infrastructure nodes do not need Kubernetes at all.

### Building without Docker

The `.sqlx` directory is committed, so `cargo build` and `cargo test` work without a running database:

```bash
cargo build          # works without PostgreSQL
cargo test           # works without PostgreSQL
```

---

## Where to go next

- **Use Weft.** Build your first project: [Getting Started](https://weavemind.ai/docs/hello-world).
- **Learn the language.** Start with [Nodes](https://weavemind.ai/docs/nodes), [Connections](https://weavemind.ai/docs/connections), [Types](https://weavemind.ai/docs/types), [Groups](https://weavemind.ai/docs/groups), [Parallel Processing](https://weavemind.ai/docs/parallel).
- **Understand the architecture.** [DESIGN.md](./DESIGN.md) lays out the principles that guide every decision in the language.
- **See what's coming.** [ROADMAP.md](./ROADMAP.md) lists the directions we are exploring.
- **Contribute.** [CONTRIBUTING.md](./CONTRIBUTING.md) walks through the dev loop, the repo layout, and how to add a node.
- **Report a security issue.** [SECURITY.md](./SECURITY.md).
- **Ask a question, share a project, argue with me.** [Discord](https://discord.com/invite/FGwNu6mDkU).

---

## License

[O'Saasy License](./LICENSE). MIT with a SaaS restriction: you can use, modify, and self-host freely, but you cannot offer it as a competing hosted service. See [osaasy.dev](https://osaasy.dev/).

Copyright © 2026 Quentin Feuillade--Montixi.
