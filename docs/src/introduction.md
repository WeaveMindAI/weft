# The weft book

Weft is a language and a runtime for programs that combine AI models, people
and tools.

Running an agent puts one model in charge of the whole job, and you wait while
it works. Weft goes the other way. You break the job into a graph of small
steps, called nodes. A node can run a model, open a webhook, ask a person, call
an API or keep a database. Each one sees only what you wired to it, and the
graph does the coordinating. The compiler checks every connection before the
run, and every value that passes between nodes is written down where you can
read it afterwards.

![A support answer waiting for human approval, with the draft visible in the graph](img/intro.png)

## Let Tangle show you

Two things are yours to do, and both are quick. [Install weft](start/install.md),
which is mostly waiting, and decide which services you want weft to keep keys
for. Then make a project:

```bash
weft new hello --assistant claude-code
```

That puts Tangle in the project. Tangle is our weft specialist: it knows the
language, reads the catalog on your disk before it wires anything, writes any
step you are missing, and will build you a web frontend if you ask for one.

Open the project in your assistant and ask it to show you around, or just say
what you want built. It walks you through everything below, pointing at your
own screen while it does. You do not need the rest of this book.

## Or read it yourself

If you would rather build with your own hands, or you just prefer reading, two
sections get you to the same place.

**[Set it up](start/install.md)** gets you to a running program: install, your
first project, the three things in a weft project that are alive on their own
clocks, and how to talk to Tangle.

**[Build something real](build/the-graph.md)** is where you take the wheel:
editing the graph with a mouse, running one part of it on its own, connecting
an account, giving your machine an address the internet can reach, and putting
a person in the middle of a run.

After that the book stops being a path and becomes a reference. The language,
writing your own nodes, connections, and what the runtime is doing underneath.
Read a section when you need it.

The last part, [why it is shaped this way](thinking/objections.md), is the
thinking behind the design: what weft handles for you, and how we decide what
belongs in the language.

## If something here is wrong

These docs are AI-written and the code moves fast, so mistakes get through. If
you find one, or an explanation you cannot follow,
[tell us](https://github.com/WeaveMindAI/weft/blob/mvp/CONTRIBUTING.md#found-something-wrong-in-the-docs)
or come and say so on
[Discord](https://discord.com/invite/FGwNu6mDkU). You do not need the
correction worked out before you raise it.
