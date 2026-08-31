<div align="center">

<img src="docs/src/img/logo.png" alt="Weft" width="120" />

# Weft

**A programming language and framework for AI orchestration.**

*As flexible as an agent, as reliable as code.*

[Try it](#try-it) · [The book](https://weavemindai.github.io/weft/) · [Discord](https://discord.com/invite/FGwNu6mDkU) · [Blog](https://weavemind.ai/blog/future-of-programming)

</div>

---

## Why weft?

Weft is a high-level language and a low-level Rust framework made to effortlessly create complex and reliable orchestrations of AIs, humans and tools:

- **Readable.** The language describes how components interact, not what happens inside them, so a whole system fits in a few lines of code that are extremely fast to produce and hard to get wrong. The internal representation of the program is natively a graph, so even a non developer can follow what is happening and fully interact with it through a GUI.
- **Compiled.** The compiler proves things about your orchestration before it runs: today it proves the whole graph is wired soundly and will execute properly at runtime; it will grow toward compiler flags that pin down what the system is allowed to do at runtime. The final step is Rust transpilation, so you also get Rust's memory safety, and high-speed performance for free.
- **Alive.** Execution is not one pass through a graph. Parts can stay up, wait for answers, and keep talking to each other while the rest of the program carries on.
- **Durable.** A program can put itself in hibernation and wait for any kind of signal at virtually 0 computational cost, then resume from exactly where it stopped. Every run is written down as it happens, so you can open any of them and see what happened, or is happening, interactively through the graph.
- **Infrastructure included.** Write `pg = PostgresDatabase` and the program gets a Postgres of its own. The node says which container it needs; the runtime starts that container with a disk that survives restarts, and the rest of the program reaches it through one output, `pg.access`. The WhatsApp bridge in the program below is an infrastructure node too: it puts its login QR code in the graph view, so you connect the bot by scanning it. Anything that runs in a container can be an infrastructure node; if you want to write your own, go and read [infrastructure nodes](https://weavemindai.github.io/weft/nodes/infrastructure.html).
- **Dynamic vocabulary.** A node is two files and a few dozen lines of Rust, because weft has already taken on every piece of plumbing you would have to do manually. It is opinionated on purpose: there is one way a file is stored and one way a credential is held, and you get every parameter but never the mechanism, so each piece is hardened once instead of half-written again in every node. That makes a node cost a fraction of the tokens it would in another framework, and come out with far less to go wrong. Where weft's job stops and yours begins is written down in [the commandments of plumbing](https://weavemindai.github.io/weft/thinking/plumbing.html).

## Your first weft program

```weft
whatsapp = BaileyBridge

ask = BaileyReceive { endpointUrl: whatsapp.endpointUrl }

draft = LlmInference -> (answer: String, sensitivity: String) {
  parseJson: true
  prompt: ask.content
  provider: OpenRouterProvider { model: "z-ai/glm-5.3" }.provider
  params: LlmParams { systemPrompt: @file("prompts/support.md"), temperature: 0.75 }.params
}

# Exactly one of these two says yes, the other stays quiet
route = Switch {
  value: draft.sensitivity
  cases: [
    { "kind": "equals", "value": "high", "port": "needsAPerson" },
    { "kind": "otherwise", "port": "goAhead" }
  ]
}

review = HumanQuery {
  _should_flow: route.needsAPerson
  title: "Send this answer?"
  fields: [
    { "kind": "display", "key": "from" },
    { "kind": "display", "key": "question" },
    { "kind": "display", "key": "answer" },
    { "kind": "approve_reject", "key": "send" }
  ]
  from: ask.pushName
  question: ask.content
  answer: draft.answer
}

allowed = FirstInOrder {
  approved: review.send_approved
  automatic: route.goAhead
}

reply = BaileySend {
  _is_output: true
  _should_flow: allowed.value
  endpointUrl: whatsapp.endpointUrl
  to: ask.chatId
  message: draft.answer
}
```

[Watch the demo](docs/src/img/readme_demo.mp4)

This is a complete runnable program: a support bot on WhatsApp. A customer messages in, a model writes an answer and rates how sensitive the question was, and if the question is sensitive the programs waits for a person to approve the answer before sending it.

It is about forty lines, so let me walk you through it:

The first line is the WhatsApp integration. **It is only one line** because `BaileyBridge` is an infrastructure node: it ships with its own container and handle it's own infrastructure. When you open the graph view, you only have to scan the QR code with your phone that owns the whatsapp account, and the bot is connected. There is nothing else to set up. You can check the source code of the BailyBridge in the [`catalog/`](catalog/bailey/bridge/) if you want to see how it works.

Then `ask` is a trigger that waits for a message to come in. When a message is detected, it start an executions with the message payload. Then `draft` call and LLM and draft a reply. `parseJson: true` means that the prompt asks for a JSON reply and it extract the payload into two typed outputs: the answer, and the sensitivity rating. The system prompt lives in its own file, and the model is whatever OpenRouter is serving, here `z-ai/glm-5.3`.

After that there is a `Switch`. It looks at `draft.sensitivity` and opens exactly one of its two ports: `needsAPerson` if the rating is `"high"`, `goAhead` otherwise. `review` reads that port through `_should_flow`, so the person is only asked when the answer was sensitive; if not, the review node is skipped.

When the `review` fires, the execution pause, and ask for a human to review the answer through the integrated [Weft browser extension](extension-browser/). **While they decide, nothing is running.** The process that runs the program exit, and the execution is a row in a table. When the human reply, a fresh worker rebuilds the state and picks up from that exact line. **Whether the wait is three seconds or three weeks, there is no compute spent** (other than the infra running in the background, but for a program with no infra there is nothing running)

If the user refuse, this fires true on "send_rejected" and close the other "send_approved" which propagate through the rest of the program and stop the execution. If the user approve, the "send_approved" is set to True and the other is closed. ("send_rejected" and "send_approved" are auto infered ports from the field of kind "approve_reject")

Then `FirstInOrder` takes whichever input arrived first in the config order, `review.send_approved` or `route.goAhead`, and ouput it as ".value", and `reply` uses it as its own `_should_flow`. So either the LLM decided it wasn't highly sensitive and it skiped direclty to this node and then sent the reply, or the human approved the reply and it gets sent through the same path.

The whole thing is a runnable project in [`examples/`](examples/whatsapp-support-bot/). There are other more complex example next to it.

## Try it

Weft runs your programs in a Kubernetes cluster, and that cluster is local:
`setup.sh` builds it on your machine with `kind`. You will need Docker,
kubectl, and kind; on a clean checkout the CLI, the extension and the images
are downloaded from the latest published build, so no Rust or Node toolchain
is needed. Once you change anything, the script builds from source instead,
which additionally needs Rust, Node 20 or newer, and pnpm. If anything is
missing the script names all of it before it starts, and if you rebuild
later, only what changed is redone.

```bash
git clone https://github.com/WeaveMindAI/weft.git && cd weft
./setup.sh                               # the runtime, the CLI, the VS Code ext
weft new hello && cd hello && weft run   # scaffold, compile, fire an execution
```

If `weft` is not found, `setup.sh` printed the `export PATH=...` line to add to
your shell rc: run it and try again.

If a VS Code window was open while `./setup.sh` ran, it installed the editor
extension into it; otherwise it printed a `code --install-extension` command to
run from a VS Code terminal. Either way, open the `hello` folder in VS Code and
you get your program as a graph, lighting up node by node as it runs, with a
"Source" button that puts the text back beside it. If it went into a window you
already had open, run CTRL+Shift+P `Developer: Reload Window` once first.

When a program hits a "HumanQuery" or a "HumanTrigger", the question turns up in the
weft browser extension, which the default install does not build. If you want
it, `./setup.sh --browser --no-sign` builds it (`--no-sign` skips the
Firefox add-on signing you do not need locally). For how to load it into your
browser and point it at your runtime, go and read
[the browser extension](https://weavemindai.github.io/weft/running/browser-extension.html).

If you only want part of it built, or you want to uninstall the whole thing, go
and read
[the install page](https://weavemindai.github.io/weft/start/install.html)
for the flags. For what a weft runtime is actually made of (four tiers plus a
broker), and why a crash mid-execution loses nothing, go and read
[how the runtime is built](https://weavemindai.github.io/weft/running/architecture.html).

## Nobody is supposed to learn this

No serious developer writes code by hand any more.

It became the slow way to get from an idea to a system that works, and the
people who noticed first are shipping while everyone else is still defending
the craft.

What replaces it is a person with real field experience, an AI that writes the
code, and a medium between them that **both sides can trust and understand**.

Field experience: having walked into the walls yourself, many times, across
enough real cases to know which ones matter. A model has read about the walls.
The other half of what you bring is taste: knowing which of five working
designs is the one to keep. Weft is built so those two can be all you bring.

Today the medium between the two is Python, and neither side trusts it. The
human cannot read ten thousand lines to check the model's work. The model has
no structure holding it to anything, so it improvises and you find out later.

Weft is aiming at that medium. You are not meant to sit down and learn it the
way you learned Python: it is written by AI and read by you, as a graph you
**watch run**. When you do write some yourself, for a node or a tricky step,
you are writing your own logic and nothing around it.

A model writing weft cannot invent a control flow nobody checked: control flow
is wires, and you have already seen the compiler go over them.

If you want to hand a piece of the work to an agent or another person, the same
strictness is what makes that safe. You can give out a job as small as "turn a
raw email into a normalised ticket, here are its input and output types",
because a group is a typed contract. Check it against the types on its boundary
and merge it, without anyone re-reading the whole program.

## The best way to use Weft

Vibe coding is one-shot generation. You describe the thing, a model produces
it, and you hope. If you want more than a demo, build against a real example.

Take one input that actually matters to you and build the first step, then run
it and click the node to see the value that came out. When that step does what
you want, add the next one and run again. Once the chain works end to end, feed
in a second real example and fix whichever steps break while the earlier ones
keep coming out right. After a few examples the only steps that still break are
usually the ones parsing messy input.

We call it
[Sequential Diffusion Programming](https://weavemindai.github.io/weft/thinking/sdp.html):
the program sharpens pass after pass, the way an image sharpens out of noise.

When something breaks in production six months later, you run the same loop
again with the failing case as your example. Every execution was recorded step
by step, with the value on every wire. Weft calls that the journal, and for how
to read one, go and read
[the journal](https://weavemindai.github.io/weft/running/the-journal.html). You
open the run that failed, descend the folded groups to the step whose value
went wrong, and iterate there.

## Building blocks, and programs made of them

Most tools hands you blocks somebody else built and a wall
the moment you need one they did not imagine. A library lets you build your own
and then leaves you wiring them together in raw code.

In weft you write the node you were missing and use it on the next line.

**The lower layer is where the vocabulary comes from.** Someone wraps a
capability into a node: an LLM call, a Postgres store, a WhatsApp bridge, a
long-lived agent. Something that was painful to stand up becomes a line in a
file, and it works the same way for the next person who drops that folder into
their project.

**The upper layer is where you snap that vocabulary together**, and you can
drop the Python you already have straight into it. `ExecPython` runs it as a
node: `py = ExecPython(x: Number) -> (result: String) { code: "..." }`. Each
input arrives as a variable named after its port, and the dict your code
returns goes out keyed by port name.

A node is a folder with a `metadata.json` saying what goes in and out and a
`mod.rs` doing the work. If it needs a crate, name it in a `deps.toml`, and if
you want it to have its own tests, drop a `tests.rs` in the same folder. For
the walkthrough, go and read
[what a node is](https://weavemindai.github.io/weft/nodes/what-a-node-is.html).

## Models are not required

The program above uses a model, because that is what everyone is building right
now. Nothing in weft needs one.

If your slow step is a person or an API, weft sees the same thing it sees in a
model call: a node with typed ports that takes a while to answer.

## Where things stand

Weft's own development runs every day on the language, the type system, the
compiler, the executor that survives restarts, the journal, the node framework
and the login system. They change rarely now.

The catalog is small on purpose: it exists to prove the language can express
things, and it grows wherever somebody needs it to. To see what is in it today,
go and look in
[`catalog/`](catalog/).

Breaking changes will happen while the shape settles. Each one comes with its
migration notes.

For the pushback we get most often and what we think about it, go and read
[Things people say to me](https://weavemindai.github.io/weft/thinking/objections.html).

## Repo layout

```
weft/
├── catalog/            every built-in node, and the best place to read before writing your own
│   ├── ai/             LLM providers and inference, speech, image and video, documents
│   ├── basic/          Text, Debug, Python execution, Cast, Range
│   ├── human/          forms and approvals
│   ├── live/           HTTP endpoints, websockets
│   ├── logic/          Switch, FirstInOrder
│   └── ...             Slack, Google, Notion, Airtable, email, S3, storage, Postgres,
│                       Telegram, WhatsApp (`bailey/`), HTTP, RSS, web, triggers
├── crates/
│   ├── weft-core/       the type system, how values travel between nodes
│   ├── weft-compiler/   lex, parse, enrich, validate, codegen
│   ├── weft-engine/     the execution loop: durable, resumable
│   ├── weft-dispatcher/ routing and lifecycle, coordinated through Postgres
│   ├── weft-listener/   wake events: timers, forms, feeds, provider events
│   └── ...              journal, task store, catalog, broker, infra, CLI
├── packages/
│   └── weft-graph/      the graph renderer and editor the VS Code extension embeds
├── extension-vscode/   the editor: graph view and live execution
├── extension-browser/  where people answer the tasks programs send them
├── docs/               the book
├── deploy/             the container images, the cluster manifests, the public page
├── scripts/            the test runners and the worktree helper
└── setup.sh            build and install everything
```

## Contributing

If you want to send a patch, or you are wondering how we review them, go and
read [CONTRIBUTING.md](CONTRIBUTING.md).

## License

[O'Saasy](LICENSE): do what you like, including selling your programs, as long
as you keep the notice and do not run it as a competing hosted service.

---

<sub>Weft is built by [Weavemind](https://weavemind.ai). Come and find us on
[Discord](https://discord.com/invite/FGwNu6mDkU).</sub>
