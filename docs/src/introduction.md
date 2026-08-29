# Introduction

Ask any team to draw their AI system on a whiteboard and you get boxes and
arrows in a minute or two. Input arrives here, the model sees it there,
this branch needs a human, that one writes to the database, and if the model
says "delete" then something had better check first.

Now ask their codebase the same question. Nothing answers. The boxes and
arrows are the program, and they live in the whiteboard photo and in the Python
that happens to implement them, nowhere a machine can read.

Weft is a language for the boxes and arrows.

```weft
mailbox = EmailAccess

ticket = ReceiveEmail
ticket.account = mailbox.access

llm = OpenRouterProvider { model: "openai/gpt-4.1-nano" }

classify = LlmInference -> (response: String) {}
classify.prompt = ticket.body
classify.provider = llm.provider

review = HumanQuery {
  title: "Escalate this ticket?"
  fields: [{ "kind": "approve_reject", "key": "escalate" }]
}

alert = Debug {
  _should_flow: review.escalate_approved
  data: classify.response
}
```

An email arrives, a model reads it, a person approves the escalation, and only
approved tickets reach the alert. Nothing above is an excerpt: that is the file
the runtime compiles, and by the time it runs it is a Rust binary with your
nodes built into the engine.

## What is actually different

**The orchestration is the source.** The wires in that file are the program's
structure. So the compiler holds you to it: every type has to line up and every
required input has to be connected, and it says so before anything runs.

**Waiting is free.** `HumanQuery` suspends the execution and lets the worker
process **exit** rather than block. The execution becomes rows in a table, and
when the answer arrives a fresh worker rebuilds the state and continues from
where it stopped. How that works: [The journal](running/the-journal.md).

**A node's job is small enough to fit in one head.** Everything a node needs
from the outside world (an authenticated HTTP client, file storage, a channel
to another node, the ability to pause) arrives through one object, already
built. A node that calls Slack contains the Slack call and nothing else.

And a node's ports say nothing about what is behind them, so `HumanQuery` and
`LlmInference` are the same kind of thing here. A program built only out of
people and services is an ordinary weft program.

## Who writes it

Mostly an AI, in practice. The syntax is strict, so a model writing weft cannot
wire a String into a Number, leave a required input dangling, or invent a
control flow nobody checked.

You read the result as a graph, click a node, and look at the value that
actually came out of it. This book is the full reference, so you should be able
to read any program in it without help.

## How to read this book

If you have never run weft, start at [Install](start/install.md) and follow
the five short pages after it. They end with a program on a URL that pauses
for a human.

If you want to understand the model before touching it,
[How a weft program runs](language/mental-model.md) is the chapter everything
else rests on.

If you are here to write a node, [What a node is](nodes/what-a-node-is.md)
and [The ctx](nodes/the-ctx.md) are the two pages that matter, in that order.

If you are here to argue, [Things people say to me](thinking/objections.md) is
where that is collected.

## Where things stand

For what is solid today, what the catalog holds, and what is being built next,
go and read the [roadmap](appendix/roadmap.md).
