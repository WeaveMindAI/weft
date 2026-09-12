# Putting a person in the loop

Sometimes a program should stop and ask you before it goes on, and that is
just another step: you drop a `HumanQuery` in and everything after it waits
until somebody answers. The question turns up in your browser.

You can try this now without connecting a model or a messaging account.

## Get the questions on your screen

Install **Weft tasks** from
[Firefox Add-ons](https://addons.mozilla.org/en-US/firefox/addon/weft-tasks/)
or the
[Chrome Web Store](https://chromewebstore.google.com/detail/weavemind/mddobmalhoelphnmhbenmbmeibfpoppm).

Now the extension needs a key so it can reach your weft:

```bash
weft token mint --name "my laptop"
```

`--name` is only a label, so you can tell your tokens apart later. The command
prints a connect URL once and never again.

Make sure your runtime is up, then open the extension, click the gear for
**Settings**, paste the URL into **Paste token URL** and click **Add Token**.
Your browser will ask whether the extension may talk to that address: say yes,
or it can never reach your weft. It then checks it can actually get through
before it saves anything, so this will fail if your runtime is stopped.

Treat that URL like a password. This one sees every question in your whole
weft, in every project, until you revoke it. If you want to hand somebody a
key to one project only, or you lost the URL and need a new one, go and read
[The browser extension](../running/browser-extension.md#connect-it).

## Ask before continuing

Put this in `main.weft`:

```weft
draft = Text { value: "We can replace the damaged item." }

review = HumanQuery {
  title: "Send this answer?"
  fields: [
    { "kind": "display", "key": "answer" },
    { "kind": "approve_reject", "key": "send" }
  ]
  answer: draft.value
}

approved = Debug {
  _should_flow: review.send_approved
  data: draft.value
}
```

`_should_flow` is a step's on switch. Give it a true or false value and the
step only runs when it is true.

Run it, and before you answer anything, put the graph beside the extension.
`review` wears its cyan waiting ring, and everything after it is sitting
still.

![The answer awaiting approval beside the same waiting execution in the graph](../img/extension-task.png)

<!-- IMAGE: extension-task.png. Two panels. Left: the task tab showing
"Send this answer?" and the draft text with Approve / Reject. Right: the
same execution in the graph, review waiting in cyan, approved not yet run. -->

Open the extension and **Send this answer?** is waiting in the list. Click it
and the form opens in its own tab, with the draft text and an Approve and a
Reject button. Click Approve and `approved` runs. Then run the program a
second time and click Reject: this time `approved` never runs.

## Where those two outputs came from

You never declared `send_approved`. The form did. A field of kind
`approve_reject` named `send` gives the step two outputs, `send_approved` and
`send_rejected`, and only the one you chose fires. The other one closes, which
means no value will ever come out of it, and any step waiting on that value is
skipped.

That is why `approved` has `_should_flow: review.send_approved`. So approving
switches it on and rejecting switches it off. If you want something to happen
on a rejection instead, wire `send_rejected` into it.

You describe the form once, and that description is what makes the ports.
Change the fields and the ports change with them. For the rest of that, go and
read [Triggers](../language/triggers.md#form-derived-ports).

## Answer it tomorrow

Close your laptop and answer the question in the morning. The run will still
be there.

weft writes the paused run down and lets the worker exit, so a question can sit
open for a week without holding a process open. When you answer, it picks the
run back up where it stopped. All you need is your weft running and reachable
at the moment you answer.

If you want to know what else survives a restart, go and read
[The journal](../running/the-journal.md). For a form that *starts* a run
rather than pausing one, go and read [Triggers](../language/triggers.md) and
look at `HumanTrigger`.

Next, open [a complete Telegram bot](a-bigger-example.md), which puts a model,
a database and a person in the same program.
