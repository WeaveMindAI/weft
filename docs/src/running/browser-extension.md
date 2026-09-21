# The browser extension

**Weft tasks** is where a person answers a program. When a step asks a question,
the run parks and the question shows up here as a card.

Install it from
[Firefox Add-ons](https://addons.mozilla.org/en-US/firefox/addon/weft-tasks/) or the
[Chrome Web Store](https://chromewebstore.google.com/detail/weavemind/mddobmalhoelphnmhbenmbmeibfpoppm).
To build it from your checkout instead, `./setup.sh --browser --no-sign` writes
an unpacked build per browser under `extension-browser/build/`, and Chrome loads
it from `chrome://extensions` with Developer mode on.

## Point it at your runtime

The extension ships with **no access to anything**. Not your runtime, not any
website. You give it one address at a time.

Mint a token from your project folder:

```bash
weft token mint --name "my laptop"
```

Then open the extension's settings, add your runtime's address and paste the
token. The browser will ask whether to allow access to that host, because until
you say yes the extension cannot reach it. Remove the last token for a host and
it hands that permission back.

The token's full value is printed once, at mint, and never again. `weft token
ls` shows you which tokens exist by a short prefix, and `weft token revoke <id>`
ends one.

### Scoping a token

A token with no restrictions sees every waiting step in the tenant, and no node
displays at all.

| Flag | What it narrows to |
|---|---|
| `--projects <uuid>` | Only these projects. Repeatable |
| `--tags <tag>` | Only steps carrying one of these tags. Repeatable |
| `--display <node>` | Also lets it read one node's display, and press the buttons on it |
| `--displays` | Every node display in the token's projects |

The display scope is the one that grants nothing by default, because a display
can be a credential: a bridge showing a QR code to scan, a database showing the
password it just minted.

## What you see

**The popup** is the list. It shows whether each runtime is reachable, and
splits what is waiting into triggers, which start a new run, and questions from
runs already going.

It distinguishes two kinds of failure, and says which: a host you have not
granted, and a host it could not reach. A popup that said "Connected" while a
runtime was down would be hiding tasks from you.

**The task page** is one card at a time, with the count, and Previous and Next
to move. The card shows whatever the step declared: values to read, text boxes,
option buttons, approve and reject pairs, images and files.

| Button | What it does |
|---|---|
| **Submit** | Answers a question inside a run already going |
| **Fire** | Starts a new run, on a trigger |
| **Skip** | Answers with nothing and lets the run carry on. Not shown on a trigger, because skipping a trigger would fire it |
| **Cancel run** | Ends the whole execution. Also not shown on a trigger |

Approve and reject are labelled by whoever wrote the step, so they can read
**Send it** and **Hold it** rather than the defaults.

After you submit, the card says `Submitted` for a moment and moves to the next.
Back in the editor, the cyan ring on that box goes green and the rest of the
program runs.

## What it can see

It holds three permissions: storage for your tokens, notifications, and alarms
for the poller behind them. No access to any site until you add a runtime, and
the Firefox build declares that it collects nothing.

It talks to two addresses on your runtime: one that lists what is waiting, and
one that answers. The token rides in the `Authorization` header rather than in
the URL, so it does not land in anybody's logs.

## Writing your own instead

The extension is one consumer, not the only possible one. It reads the same two
addresses any client can, and it renders a form from what the step declared
rather than knowing anything about particular steps. A Slack bot or an internal
page would work the same way against the same addresses.

If you want to build one, the token is the whole authentication story, and
`weft token mint --display <node>` is how it gets to read what a node is
showing.
