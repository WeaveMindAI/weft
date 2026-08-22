# Weft

**A programming language where an LLM call, a human approval, a database, and a WhatsApp line are the same kind of thing: typed nodes a compiler can check. An AI writes it, you read it as a graph, and it runs as a native Rust binary.**

```bash
git clone https://github.com/WeaveMindAI/weft.git && cd weft && ./setup.sh
```

Why can't any compiler see the parts of your system that matter most, the LLM calls, the human review steps, the API glue?

Why does "wait for a person to approve this" take a webhook, a queue, and a state machine instead of one line?

Why did your AI assistant just write ten thousand lines of Python that neither of you can hold in your head?

Every language you can use today was designed before programs had intelligence inside them. Weft is designed after. LLMs, humans, APIs, databases, and infrastructure are its primitives, the way numbers and operators are primitives elsewhere. The compiler reads the whole system, checks every connection and every type, and transpiles it to Rust: a native binary, not a graph crawling through an interpreter.

Here's a real one. A support ticket comes in by email, an LLM triages it, and anything it flags as critical waits for a human before it gets escalated:

````weft
mailbox = EmailAccess

ticket = ReceiveEmail
ticket.account = mailbox.access

llm = OpenRouterProvider { model: "openai/gpt-4.1-nano" }

triage = LlmParams {
  systemPrompt: "Classify this support ticket. Reply with JSON: {severity, summary}."
}

classify = LlmInference -> (response: String) {}
classify.prompt = ticket.body
classify.provider = llm.provider
classify.params = triage.params

review = HumanQuery {
  title: "Escalate this ticket?"
  fields: [{ "fieldType": "approve_reject", "key": "escalate" }]
}

escalate = Gate(pass: Boolean, value: String) -> (value: String?) {}
escalate.pass = review.escalate_approved
escalate.value = classify.response

alert = Debug
alert.data = escalate.value
````

Read it top to bottom: ticket in, LLM classifies, a human approves the escalation, the gate only lets approved tickets through to the alert. Every edge and every type was checked before a single node ran. The human pause is one node (`HumanQuery`): the program can wait minutes or days for that approval and resume exactly where it left off. Open the same file in the editor and it's a graph you click through and watch execute live.

<!-- CAPTURE: hero image or short GIF right here: the support-ticket example
     above, shown side by side as code and as its rendered graph in VS Code,
     ideally mid-execution with one node lit. This is the front door's one
     visual; it carries the "code for the AI, graph for you" claim. -->

> **Building in public, early days.** The language, the type system, and the durable executor are the stable core. The node catalog is small and opinionated on purpose. Breaking changes will happen while the shape settles, and they'll come with migration notes. Treat this as a foundation to build on, not a finished product.

## Two layers, one cheap seam

Most tools pick a side. Zapier lets you *compose* pre-built blocks but you can't make new ones. A library lets you *make* primitives but composing them is just more raw code, with all the plumbing back. Weft is built so the seam between those two worlds is cheap to cross, and that seam is what makes it different from everything else.

**The lower layer is vocabulary.** Someone wraps a capability (an LLM call, a Postgres store, a WhatsApp bridge, a NeRF, a niche model, a custom agent) into a *node*: a typed, self-contained building block with clean input and output ports. Hard tech that was painful to use becomes a drop-in. The node carries its own dependencies and infrastructure, so when someone else imports it, it just works.

**The upper layer is composition.** You snap that vocabulary into programs. If the node you need already exists, you use it or import someone else's. If it doesn't, you write one in a few minutes, and now it's vocabulary forever. The catalog compounds: every node added pulls in more builders, who add more nodes. Build a word once and every sentence after that gets to use it.

## Built to be written by AI, not learned by humans

People hear "new language" and flinch: nobody wants to learn another syntax. But you don't learn Weft. **It's designed from the ground up to be written by AI and read by you as a graph.**

What makes the syntax AI-friendly is that it is *strict*. Strong typing, top-down construction, and connection-completeness form a cage around the model: the compiler won't let it wire a String into a Number, leave a required input dangling, or send unfiltered user input straight into an LLM. The AI builds *inside* a structure that's guaranteed sound, instead of improvising the whole thing and hoping. That's the difference between an agent and orchestration: your trust goes to the architecture, which the compiler checked, rather than to the model's good behavior.

The payoff shows up in build time. In our testing, an AI builds the equivalent system in Weft about **20x faster** than writing it in Python with a coding agent (a customer-feedback triage pipeline went from ~1 hour to ~3 minutes), and the result is a graph you can read, edit, and watch execute live.

## How you actually build with it

You grow the system against a real example instead of writing it from a spec. Take one input that matters to you, build the first step, run it, click the node, and look at the value that actually came out. When that step produces what you want, grow the next one. When the whole chain works end to end, feed it a second example and fix whichever steps break, while the earlier examples keep passing. A few examples in, new inputs just work, and at no point were you guessing: every decision was made looking at a real value on a real run.

We call this way of working **Sequential Diffusion Programming**: the program sharpens pass after pass, the way an image sharpens out of noise, and each pass is anchored to a concrete case. It only became viable now, because an AI pass over a Weft program is fast and cheap enough that refining beats up-front design. The graph view exists for exactly this loop, and when something breaks in production later, the same motion works in reverse: the journal keeps every execution, so you open the failed run, descend the folded groups to the step whose value went wrong, and iterate on that step with the failing case as your new example.

## What the compiler buys you

Because the whole orchestration is legible (not buried in glue code), the machine can do things no framework can:

- **Guarantees before it runs.** The compiler reads the entire architecture. It can flag user input reaching a model with no filter, an output hitting a destructive action with no human review, and it's the place to enforce things like compliance or jailbreak protection, before a single node fires.
- **Reliable systems from unpredictable parts.** LLMs are unpredictable by nature. You choose how tightly each one is contained, from "acts freely, fast to prototype" to "output bounded and checked." Prototype loose, then lock down the parts that need to be reliable, without losing the intelligence where it matters.
- **Everything is mockable.** Any node or group can be swapped for "pretend it returns this." Test one step, benchmark it, compare two prompts in isolation. The mock is type-checked against the real ports, so it can't silently drift.
- **First-class humans.** Pause mid-program, send a form to a person, wait three days, resume exactly where you left off. All of that is one node, with no webhooks or polling loops to hand-roll around it.
- **Durable by default.** Programs survive crashes and restarts. "Wait three days for an approval" is the same code as "wait three seconds for an API response."
- **The full power of Kubernetes without operating it.** Kubernetes already won at coordinating real infrastructure (pods, networking, storage, health, lifecycle). The only thing wrong with it is that wielding it means YAML, operators, and an ops priesthood. Weft puts a tiny typed DSL in front of all that power: a database, a WhatsApp bridge, a headless browser is just a node you drop on the graph and wire up. Hit start and the platform provisions the real pod, waits for it to be healthy, and hands the rest of your program a URL. The same code runs on a local cluster on your laptop and on real Kubernetes in any cloud, with no separate "prod" setup to maintain. The defaults are sane without being a ceiling: an expert can always drop down to the actual cluster config and tighten it, because every node's full vocabulary stays reachable.
- **Recursively foldable.** Any group of nodes collapses into a single node with a typed interface. A 100-node system still reads as 5 blocks at the top level.
- **Compiles to native code.** Weft transpiles to Rust, so you get memory safety and real performance, not a slow interpreted graph (the Zapier-clone failure mode) that buckles at scale. The graph is how you read and edit it; the thing that runs is a compiled binary.

## Quick start

You need [Docker](https://docs.docker.com/get-docker/) (for Postgres) and [Rust](https://rustup.rs/). On macOS, `brew install bash` (the script needs Bash 4+).

```bash
git clone https://github.com/WeaveMindAI/weft.git
cd weft
./setup.sh
```

That one script builds and links three binaries into `~/.local/bin`:

- `weft` (the CLI)
- `weft-dispatcher` (the local runtime daemon)
- `weft-runner` (the worker, launched by the dispatcher)

It also builds and installs the VS Code extension, which is where the graph editor and live execution view live. If `~/.local/bin` isn't on your `PATH`, the script prints the exact line to add.

The script also leaves the local runtime daemon up and running, so you can go
straight to your first project:

```bash
weft new hello         # scaffold a project
cd hello
weft run               # compile, register, fire an execution, stream live events
```

Open the project folder in VS Code to see the graph, click nodes, and watch execution flow through in real time. The full walkthrough (webhooks, human-in-the-loop, infrastructure nodes) is in [docs/getting-started.md](./docs/getting-started.md).

### API keys and connections

Nodes that call a third party (an LLM provider, email, Slack) take a **connection**, picked on the node in the editor. Your key never enters the graph or the source file; the node's config stores only a handle to the stored connection. Two ways to connect:

- **Your own key**: paste it once in the editor's connection flow for that service.
- **One click on this weft's own key**: add an `api_key` entry for the service in the shared-credentials file (`access-apps.json`) and the editor offers it as a ready-made connection.

Copy `access-apps.example.json` to `access-apps.json` and fill in the services you want to offer that way. All optional: a node whose connection is missing fails loudly at run time, never silently.

## Repo layout

```
weft/
├── catalog/            # The node catalog (source of truth for every built-in node)
│   ├── ai/             #   LLM providers, params + inference; speech-to-text
│   ├── basic/          #   Text, Debug, Python execution
│   ├── email/          #   Email receive/send
│   ├── http/           #   HTTP request
│   ├── human/          #   Human Query, Human Trigger (forms, approvals)
│   ├── live/           #   HTTP endpoints, websockets
│   ├── logic/          #   Gate (conditional routing)
│   ├── triggers/       #   Cron
│   ├── bailey/         #   WhatsApp bridge + send/receive
│   └── ...             #   Slack, GitHub, Google, storage, Telegram
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
