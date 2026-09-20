# Introduction

Weft is a language and runtime for programs that combine AI models, people and tools.

Running an agent puts one model in charge of the whole job, and you wait while it works. Weft goes the other way: you break the job into a graph of small steps, called nodes. A node can run a model, open a webhook trigger, ask a person, call an API, or keep a database. Each one sees only the context it needs, and the graph handles the coordination, so no node spends compute on it. The compiler checks every connection before the run, and every value that passes between nodes is written to a journal you can read afterwards.

You view and edit a weft project through a code IDE extension. Today that means VS Code and its forks (we are working on extending it to other IDE). The installer also puts a `weft` CLI on your machine, and you (or your ai assistant) can handle everything without any graphical interface. 

![A support answer waiting for human approval, with the draft visible in the graph](img/intro.png)

## Getting started

First, [install weft](start/install.md). The installer runs a small Kubernetes cluster on your machine, and sets up the `weft` CLI and the VS Code extension.

Then [build your first program](start/first-program.md).

The rest of the book is for when you want to understand how weft works underneath, or write a piece of it yourself.

If you find a mistake, or an explanation you cannot follow, [tell us](https://github.com/WeaveMindAI/weft/blob/mvp/CONTRIBUTING.md#found-something-wrong-in-the-docs).

## You do not have to read this book

Weft is built to be written by an AI assistant and steered by you. Pass `--assistant` when you create a project and weft installs Tangle, our in-house weft specialist:

```
weft new hello --assistant claude-code
```

Other assistants are available too, and the flag is remembered for your next project.

Tangle knows the language and the runtime, and it has the tools to run and debug a project. Describe what you want in plain words and it builds it for you.
