---
name: weft-gaps
description: "What to do with a genuine gap, a capability the language, compiler, or runtime cannot express at all, and with a node worth sharing. Read when no node, group, or node-smith contract can honestly deliver what the request needs, or when a node you just built belongs in the shared catalog: the tracker to search first, the issue templates and their exact field ids, the pre-filled issue URL to hand the user, and the hand-over words. A missing node is not a gap; it is a node-smith dispatch, and a broadly useful node is a contribution offer."
---

# A genuine gap

A capability the catalog lacks is a `node-smith` dispatch, not a gap. A node
lives in the project's own `nodes/` folder and works today, so a missing node
never blocks the user and never reaches the tracker.

A genuine gap is different: a capability the language, the compiler, or the
runtime cannot express at all. A node needs a mechanism the `ctx` does not
give it. The engine cannot model a shape the program needs. An integration's
API cannot be honestly wrapped with what the language has. When the shape is
genuinely out of reach, Tangle does not shrug, and Tangle does not route
around it to an outside service. Tangle writes the issue and hands the user
the link.

## The one page Tangle cannot submit itself

Filing an issue needs the user's own GitHub account. So Tangle prepares and
the user submits, and the preparing is the work: the URL below opens the
right issue form with every field Tangle can fill already filled in. The user
clicks it, completes what is theirs (their words, a screenshot), and hits
submit. Nothing else is typing.

## Search before filing

The repo: `https://github.com/WeaveMindAI/weft`

Search the tracker first, so the user adds their case to a real issue instead
of opening a duplicate:

`https://github.com/WeaveMindAI/weft/issues?q=<keywords>`

When an open issue already covers the gap, hand the user that issue's URL and
stop: "someone already asked for this, add your case to it." Only when none
covers it do you build a new-issue URL.

## The templates and their fields

GitHub issue forms are prefilled through URL query parameters, one per field
`id` in the template, URL-encoded. `title` works too. The templates in this
repo and the fields each carries:

| Gap | Template | Field ids |
|---|---|---|
| a language, compiler, or editor capability | `feature_request.yml` | `problem`, `proposal`, `alternatives`, `design-fit` |
| a node you built that belongs in the shared catalog (a contribution, not a gap) | `node_request.yml` | `name`, `what-it-does`, `inputs-outputs`, `use-case`, `workarounds` |
| a provider on the shared keys | `provider_request.yml` | `name`, `base-url`, `pricing`, `existing-meter`, `use-case` |
| something in weft is broken | `bug_report.yml` | `what-happened`, `expected`, `repro`, `weft-code`, `logs`, `version`, `platform`, `extra` |

Blank issues are off, so a template is required. When none of the four fits
the gap, use `feature_request.yml` and say so in the issue.

The shape of the URL:

```
https://github.com/WeaveMindAI/weft/issues/new?template=feature_request.yml&title=<title>&problem=<problem>&proposal=<proposal>
```

## Filling it

Fill every field Tangle can, from the real program it was building:

- `title`: the capability, in one line, in the user's words where possible.
- `problem` (or the template's equivalent): what the user was trying to do,
  the exact point the language or the runtime stopped, in plain words.
- `proposal`: the smallest thing that would close the gap, and the smallest
  program that shows it (real weft, not a sketch).
- `alternatives`: what was tried, including the node dispatch that came back
  impossible, so the reader knows the gap is real.

Leave a field for the user rather than invent content, and say which ones are
theirs. A prefilled issue carrying the real program is worth ten sentences of
guessing.

## The hand-over

In plain words, with the URL on its own line:

"Everything you asked for is expressible in weft. This one piece is not there
yet, so I wrote it up for the weft team. Click this link, fill the blanks that
are yours, and submit:

<url>

Filling it in and hitting submit is how you let us know and tell us to work on
it. We read that tracker."

Then say what still works today without the gap, so the user knows where they
stand while it waits. A genuine gap is reported with the same honesty as any
other finding: named exactly, with the evidence, and with the path that gets
it built.

## A node worth sharing is not a gap

A node you just built is not a gap either. After a `node-smith` lands a node,
judge it on one question: would a lot of projects want this? A node that
wraps a public service, does a general job, and carries no assumptions about
this particular program is a candidate for the shared catalog. It already
works in the project's own `nodes/`; proposing it only asks whether it should
ship for everyone. When it is a candidate, say so once, plainly, as an offer
and not a warning:

"By the way, this node looks useful beyond your project. If you want it in the
shared catalog, propose it here, and put the node's folder, the one we just
built (`nodes/<folder>/`), in the issue:

<url>

We take it from there."

The folder is the thing to attach: its `metadata.json`, its code, and its
tests, in the issue body or as a pasted tree. A user who is not interested has
answered, and the node keeps working where it is; never press the point.

The prepared `node_request.yml` URL:

```
https://github.com/WeaveMindAI/weft/issues/new?template=node_request.yml&title=<name>&name=<name>&what-it-does=<one%20paragraph>&inputs-outputs=<ports>&use-case=<what%20it%20was%20built%20for>
```

Fill `name`, `what-it-does`, `inputs-outputs` (the real ports), and `use-case`
from the node you just built; leave the rest as the issue's own prompts. Tangle
offers this, and the user files it.
