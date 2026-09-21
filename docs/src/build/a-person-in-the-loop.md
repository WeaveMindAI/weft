# Putting a person in the loop

A step can stop and ask somebody a question. The run parks, the worker shuts
down, and when the answer arrives a fresh worker picks the run back up exactly
where it was. Waiting is free, so a program can sit on a question for a week
without costing you anything.

This is `HumanQuery`, and it looks like this:

```weft
review = HumanQuery {
  title: "Send this answer?"
  fields: [
    { "kind": "display", "key": "question" },
    { "kind": "display", "key": "answer" },
    { "kind": "approve_reject", "key": "send" }
  ]
  question: ask.content
  answer: draft.answer
}
```

The `fields` list builds both the form and the step's ports. A `display` field
takes a value in and shows it to the reader. An `approve_reject` field gives
you two boolean outputs, `send_approved` and `send_rejected`, so you gate what
happens next on the one you want:

```weft
reply = SendMessage {
  _should_flow: review.send_approved
  text: draft.answer
}
```

## Where the question shows up

Not in VS Code. The graph shows you that something is waiting, with a cyan ring
round the box and the word `Suspended` in its body, and the inspector says
`Waiting for input...`. Answering happens in the **Weft tasks** browser
extension you installed earlier.

Give the extension a way in by minting a token from your project folder:

```bash
weft token mint --name "my laptop"
```

A signal token lets an outside client list a project's waiting steps and answer
them. The full value is printed once, at mint, and never again, so copy it now.
`--projects` and `--tags` narrow what it can reach, and `weft token ls` and
`weft token revoke <id>` handle the rest.

Open the extension, go to its settings, and add your runtime: its address and
that token. The browser will ask you to allow access to that host, because the
extension holds no permission to reach anything until you give it one.

## What the person sees

One card at a time. Your title, the values you wired into the `display` fields,
and the buttons or boxes your other fields asked for. The primary button says
**Submit** for a question inside a running program, and **Fire** for a trigger
that starts one.

Beside it, **Skip** answers with nothing and lets the run carry on, and
**Cancel run** ends the whole execution. Neither shows up on a trigger, because
skipping a trigger would fire it.

After a submit the card says `Submitted` and moves to the next one. Back in the
graph, the cyan ring goes green and the rest of your program runs.

![A question waiting in the extension, and the same run parked in the graph](../img/extension-task.png)

## Starting a run from a person

`HumanTrigger` is the same form pointed the other way: instead of parking a run
that already exists, it starts a new one when somebody fills it in. It shows up
in the extension under trigger tasks rather than resume tasks, and it only
appears once you have run `weft activate`, because a trigger is only listening
after you arm it.

That is one of [the three lifecycles](../start/lifecycles.md), and it is the
step people forget.

## When nobody answers

A parked run waits, with no deadline. `HumanQuery` has no timeout setting and
weft will not invent one, because a question that expires on its own leaves you
with a run that ended for a reason nobody wrote down.

So ending it is a decision somebody makes. The person looking at the card can
press **Cancel run**, and you can run `weft stop <color>`. To find the ones
still sitting there:

```bash
weft executions --status running
```

Parked runs come back in that list, with `waiting_for_input` in the status
column rather than `running`, so you can tell at a glance which ones are
working and which ones are waiting on a person.
