# Weft

**Agents improvise. Weft orchestrates.**

The bet behind this language: production-ready AI won't be one giant agent left to figure everything out. It'll be a fast, reliable *orchestration* of intelligent pieces, and Weft is the language for writing it.

Right now you have three bad options for building AI software. Agents are flexible but you can't trust them on long work. Custom code can do anything, but no language can actually reason about a system stitched together from LLMs, humans, and APIs, so it turns fragile fast. Zapier-style tools are predictable until step five, then they're spaghetti, you're managing infrastructure by hand, and you're running on a graph some engine walks at runtime, so it crawls and falls over at scale. Every option makes you trade away something you needed.

Weft is the fourth option. It's a language where LLMs, humans, APIs, databases, and infrastructure are not libraries you import, they're *primitives you wire together*. The compiler reads the whole system, checks every connection and type, then transpiles the whole thing to Rust: your program is a native binary, not a graph being interpreted. No glue code. No plumbing. If it compiles, the architecture holds, and it runs at Rust speed.

**You own the units of computation. Weft owns the coordination between them, types, time, failure, live messaging, and infrastructure.** That's the whole idea.

Here's a real one. A support ticket comes in over a webhook, an LLM triages it, and anything it flags as critical waits for a human before it gets escalated:

````weft
ticket = ApiPost -> (subject: String, body: String) {}

triage = LlmConfig {
  systemPrompt: "Classify this support ticket. Reply with JSON: {severity, summary}."
}

classify = LlmInference -> (response: String) {}
classify.prompt = ticket.body
classify.config = triage.config

route = ExecPython(raw: String) -> (severity: String, is_critical: Boolean) {
  code: ```
import json
r = json.loads(raw)
return {"severity": r["severity"], "is_critical": r["severity"] == "critical"}
```
}
route.raw = classify.response

review = HumanQuery(context: String) -> (escalate_approved: Boolean?) {
  fields: [{ "fieldType": "approve_reject", "key": "escalate" }]
}
review.context = classify.response

escalate = Gate(pass: Boolean, value: String) -> (value: String?) {}
escalate.pass = review.escalate_approved
escalate.value = classify.response

alert = Debug { label: "Escalated" }
alert.data = escalate.value
````

Read it top to bottom: ticket in, LLM classifies, Python pulls out the severity, a human approves the critical ones, the gate only lets approved tickets through to the alert. Every edge and every type was checked before a single node ran. The human pause is one node (`HumanQuery`): the program can wait minutes or days for that approval and resume exactly where it left off. Open the same file in the editor and it's a graph you click through and watch execute live.

> **Building in public, early days.** The language, the type system, and the durable executor are the stable core. The node catalog is small and opinionated on purpose. Breaking changes will happen while the shape settles, and they'll come with migration notes. Treat this as a foundation to build on, not a finished product.

## Two layers, one cheap seam

Here's the thing that makes Weft different from everything else, and it's worth slowing down for.

Most tools pick a side. Zapier lets you *compose* pre-built blocks but you can't make new ones. A library lets you *make* primitives but composing them is just... more raw code, with all the plumbing back. Weft is built so the seam between those two worlds is cheap to cross.

**The lower layer is vocabulary.** Someone wraps a capability (an LLM call, a Postgres store, a WhatsApp bridge, a NeRF, a niche model, a custom agent) into a *node*: a typed, self-contained building block with clean input and output ports. Hard tech that was painful to use becomes a drop-in. The node carries its own dependencies and infrastructure, so when someone else imports it, it just works.

**The upper layer is composition.** You snap that vocabulary into programs. If the node you need already exists, you use it or import someone else's. If it doesn't, you write one in a few minutes, and now it's vocabulary forever. The catalog compounds: every node added pulls in more builders, who add more nodes.

That's the whole model. Build the words once, write sentences forever.

## Built to be written by AI, not learned by humans

People hear "new language" and flinch: nobody wants to learn another syntax. But you don't learn Weft. **It's designed from the ground up to be written by AI and read by you as a graph.**

And the syntax isn't AI-friendly by accident, it's AI-friendly because it's *strict*. Strong typing, top-down construction, and connection-completeness aren't ergonomics, they're a cage. The compiler won't let the AI wire a String into a Number, leave a required input dangling, or send unfiltered user input straight into a model. The AI builds *inside* a structure that's guaranteed sound, instead of improvising the whole thing and hoping. That's the difference between an agent and orchestration: you don't trust the model, you trust the architecture.

The payoff shows up in build time. In our testing, an AI builds the equivalent system in Weft about **20x faster** than writing it in Python with a coding agent (a customer-feedback triage pipeline went from ~1 hour to ~3 minutes), and the result is a graph you can read, edit, and watch execute live.

## What the compiler buys you

Because the whole orchestration is legible (not buried in glue code), the machine can do things no framework can:

- **Guarantees before it runs.** The compiler reads the entire architecture. It can flag user input reaching a model with no filter, an output hitting a destructive action with no human review, and it's the place to enforce things like compliance or jailbreak protection, before a single node fires.
- **Reliable systems from unpredictable parts.** LLMs are unpredictable by nature. You choose how tightly each one is contained, from "acts freely, fast to prototype" to "output bounded and checked." Prototype loose, then lock down the parts that need to be reliable, without losing the intelligence where it matters.
- **Everything is mockable.** Any node or group can be swapped for "pretend it returns this." Test one step, benchmark it, compare two prompts in isolation. The mock is type-checked against the real ports, so it can't silently drift.
- **First-class humans.** Pause mid-program, send a form to a person, wait three days, resume exactly where you left off. One node. No webhooks, no polling, no hand-rolled state machine.
- **Durable by default.** Programs survive crashes and restarts. "Wait three days for an approval" is the same code as "wait three seconds for an API response."
- **The full power of Kubernetes, none of operating it.** Kubernetes already won at coordinating real infrastructure (pods, networking, storage, health, lifecycle). The only thing wrong with it is that wielding it means YAML, operators, and an ops priesthood. Weft puts a tiny typed DSL in front of all that power: a database, a WhatsApp bridge, a headless browser is just a node you drop on the graph and wire up. Hit start and the platform provisions the real pod, waits for it to be healthy, and hands the rest of your program a URL. The same code runs on a local cluster on your laptop and on real Kubernetes in any cloud: one model, no "local vs prod" split. And the defaults are sane, not a ceiling: an expert can drop down to the actual cluster config and tighten it for their use case, because every node's full vocabulary is always there to tweak, no expertise required to start, none lost when you have it.
- **Recursively foldable.** Any group of nodes collapses into a single node with a typed interface. A 100-node system still reads as 5 blocks at the top level.
- **Compiles to native code.** Weft transpiles to Rust, so you get memory safety and real performance, not a slow interpreted graph (the Zapier-clone failure mode) that buckles at scale. The graph is how you read and edit it; the thing that runs is a compiled binary.

## Quick start

You need [Docker](https://docs.docker.com/get-docker/) (for Postgres) and [Rust](https://rustup.rs/). On macOS, `brew install bash` (the script needs Bash 4+).

```bash
git clone https://github.com/WeaveMindAI/weft.git
cd weft
./setup.sh
```

One script. It builds and links three binaries into `~/.local/bin`:

- `weft` (the CLI)
- `weft-dispatcher` (the local runtime daemon)
- `weft-runner` (the worker, launched by the dispatcher)

It also builds and installs the VS Code extension, which is where the graph editor and live execution view live. If `~/.local/bin` isn't on your `PATH`, the script prints the exact line to add.

Then build your first project:

```bash
weft daemon start      # launch the local runtime in the background
weft new hello         # scaffold a project
cd hello
weft run               # compile, register, fire an execution, stream live events
```

Open the project folder in VS Code to see the graph, click nodes, and watch execution flow through in real time. The full walkthrough (webhooks, human-in-the-loop, infrastructure nodes) is in [docs/getting-started.md](./docs/getting-started.md).

### API keys

All optional. A node that needs a key fails loudly at run time if it's missing, never silently.

```bash
OPENROUTER_API_KEY=     # LLM nodes (via OpenRouter)
TAVILY_API_KEY=         # Web Search nodes
ELEVENLABS_API_KEY=     # Speech-to-Text nodes
```

Copy `.env.example` to `.env` and fill in what you need.

## Repo layout

```
weft/
├── catalog/            # The node catalog (source of truth for every built-in node)
│   ├── ai/             #   LLM config + inference
│   ├── basic/          #   Text, Debug, Python execution
│   ├── http/           #   HTTP request
│   ├── human/          #   Human Query, Human Trigger (forms, approvals)
│   ├── logic/          #   Gate (conditional routing)
│   ├── triggers/       #   Cron, webhooks
│   └── whatsapp/       #   WhatsApp bridge + send/receive
├── crates/
│   ├── weft-core/      #   Type system, pulse model, the Node trait
│   ├── weft-compiler/  #   Lex, parse (lossless CST), enrich, validate, codegen
│   ├── weft-engine/    #   The execution loop (durable, resumable)
│   ├── weft-dispatcher/#   Routing + lifecycle, coordinates through Postgres
│   ├── weft-listener/  #   Wake events (timers, webhooks, forms)
│   └── ...             #   journal, catalog, broker, infra, CLI
├── extension-vscode/   # The editor: graph view + live execution
├── extension-browser/  # Browser extension for human-in-the-loop tasks
├── docs/               # Getting started, authoring nodes, use cases
└── setup.sh            # Build + install everything
```

### How a node works

Every node is a folder in `catalog/` with:

- `mod.rs`: the Rust implementation (the `Node` trait: declare metadata, implement `execute`).
- `metadata.json`: ports, config fields, and UI hints, as data.
- `deps.toml` (optional): the cargo crates and system packages this node needs.

The compiler discovers nodes by walking the catalog. Adding one is a folder with two files. The full guide is in [docs/authoring-nodes.md](./docs/authoring-nodes.md).

## The common objections

**"Isn't this just Python with some libraries?"** A library is more code on top of a language that still can't see your system. Weft is a coordination language, so the orchestration itself is something the compiler can read and prove sound. The syntax is also shaped to be token-efficient, written and reasoned about by AI.

**"Do I have to rebuild my stack?"** No. Weft coordinates your existing stack and drops down to real code wherever you need it. It can even expose itself as an API. You adopt it incrementally.

**"Nobody adopts new languages."** What kills a new language is the cost of getting people to learn it. Nobody has to learn Weft. The AI writes it, you read it as a graph.

**"Why now?"** Software is being rebuilt around AI, and people are still gluing these systems together with primitive tooling. The language is always the foundation of what gets built on top of it. The window to set that foundation is open now.

**"Why should I bet my stack on something this young?"** Because the foundation is deliberately small. Weft stands on two technologies that aren't going anywhere: Rust (the runtime it compiles to) and Kubernetes (how it provisions and runs infrastructure). Everything else, we keep the dependency surface as thin as we can, so there's little to rot. And because Kubernetes is the substrate, the runtime isn't welded to one cloud: the same manifests run on a local cluster or any provider's Kubernetes, so porting is a config change, not a rewrite.

## Where to go next

- **Build something.** [docs/getting-started.md](./docs/getting-started.md).
- **Author a node.** [docs/authoring-nodes.md](./docs/authoring-nodes.md).
- **See what it's for.** [docs/target-use-cases.md](./docs/target-use-cases.md).
- **Contribute.** [CONTRIBUTING.md](./CONTRIBUTING.md).
- **Join in, share a project, argue with me.** [Discord](https://discord.com/invite/FGwNu6mDkU).

Read the longer story: [The Future of Programming (and Why I'm Building a New Language)](https://weavemind.ai/blog/future-of-programming).

## Star history

<a href="https://www.star-history.com/?repos=WeaveMindAI%2Fweft&type=date&legend=top-left">
 <picture>
   <source media="(prefers-color-scheme: dark)" srcset="https://api.star-history.com/chart?repos=WeaveMindAI/weft&type=date&theme=dark&legend=top-left" />
   <source media="(prefers-color-scheme: light)" srcset="https://api.star-history.com/chart?repos=WeaveMindAI/weft&type=date&legend=top-left" />
   <img alt="Star History Chart" src="https://api.star-history.com/chart?repos=WeaveMindAI/weft&type=date&legend=top-left" />
 </picture>
</a>

## License

[O'Saasy License](./LICENSE). MIT with a SaaS restriction: use, modify, and self-host freely, but you can't offer it as a competing hosted service. See [osaasy.dev](https://osaasy.dev/).

Copyright © 2026 Quentin Feuillade--Montixi.
