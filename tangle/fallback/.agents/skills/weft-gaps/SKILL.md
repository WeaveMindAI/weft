---
name: weft-gaps
description: "Read when no node, group or node-smith contract can honestly deliver what the request needs, or when a node just built belongs in the shared catalog: the tracker to search first, the issue templates with their exact field ids, and the pre-filled issue URL to hand the user."
---


# A genuine gap

Two things reach the tracker, and a missing node is neither:

- [a gap] is a capability the language, the compiler, or the runtime cannot express at all: a node needs a mechanism the `ctx` does not give it, the engine cannot model a shape the program needs, an integration's API cannot be honestly wrapped with what the language has.
- [a contribution] is a node a `node-smith` just built that a lot of projects would want: it wraps a public service, does a general job, and carries no assumptions about this particular program.

A capability the catalog lacks is a `node-smith` dispatch: the node lives in the project's own `nodes/` folder and works today, so it never blocks the user. If you catch yourself writing an issue for a missing node, stop and write: "Wait. A missing node is a dispatch." Then design [the contract] and dispatch.

For [a gap], Tangle does not shrug and does not route around it to an outside service: it writes the issue and hands the user the link. Filing needs the user's own GitHub account, so Tangle prepares and the user submits: the URL Tangle builds opens the right issue form with every field Tangle can fill already filled in, and the user completes what is theirs (their words, a screenshot).

## Search before filing

The repo: `https://github.com/WeaveMindAI/weft`

You search the tracker first, so the user adds their case to an existing issue instead of opening a duplicate:

`https://github.com/WeaveMindAI/weft/issues?q=<keywords>`

If an open issue already covers [a gap], you hand the user that issue's URL and stop: "someone already asked for this, add your case to it." Only when none covers it do you build a new-issue URL.

## The templates and their fields

GitHub issue forms are prefilled through URL query parameters, one per field `id` in the template, URL-encoded; `title` works too. The templates and their fields:

| Gap | Template | Field ids |
|---|---|---|
| a language, compiler, or editor capability | `feature_request.yml` | `problem`, `proposal`, `alternatives`, `design-fit` |
| [a contribution] | `node_request.yml` | `name`, `what-it-does`, `inputs-outputs`, `use-case`, `workarounds` |
| a provider on the shared keys | `provider_request.yml` | `name`, `base-url`, `pricing`, `existing-meter`, `use-case` |
| something in weft is broken | `bug_report.yml` | `what-happened`, `expected`, `repro`, `weft-code`, `logs`, `version`, `platform`, `extra` |

Blank issues are off, so a template is required. When none of the four fits, you use `feature_request.yml` and say so in the issue.

The shape of the URL:

```
https://github.com/WeaveMindAI/weft/issues/new?template=feature_request.yml&title=<title>&problem=<problem>&proposal=<proposal>
```

## Filling it

You fill every field you can, from the real program you were building:

- `title`: the capability, in one line, in the user's words where possible.
- `problem` (or the template's equivalent): what the user was trying to do and the exact point the language or the runtime stopped, in plain words.
- `proposal`: the smallest thing that would close [a gap], and the smallest program that shows it (real weft, never a sketch).
- `alternatives`: what was tried, including the node dispatch that came back impossible, so the reader knows the gap is real.

A field you cannot fill from the real program stays empty for the user, and you say which ones are theirs.

## The hand-over

In plain words, with the URL on its own line:

"Everything you asked for is expressible in weft. This one piece is not there yet, so I wrote it up for the weft team. Click this link, fill the blanks that are yours, and submit:

<url>

Filling it in and hitting submit is how you let us know and tell us to work on it. We read that tracker."

Then you say what still works today without [a gap], so the user knows where they stand while it waits. [a gap] is reported like any other finding: named exactly, with the evidence, and with the path that gets it built.

## A node worth sharing

After a `node-smith` lands a node, you ask one question: would a lot of projects want this? The node already works in the project's own `nodes/`; proposing it only asks whether it ships for everyone. When it is [a contribution], you say so once, as an offer and never a warning:

"By the way, this node looks useful beyond your project. If you want it in the shared catalog, propose it here, and put the node's folder, the one we just built (`nodes/<folder>/`), in the issue:

<url>

We take it from there."

The folder is what the user attaches: its `metadata.json`, its code, and its tests, in the issue body or as a pasted tree. A user who is not interested has answered, and the node keeps working where it is; you never press the point.

The prepared `node_request.yml` URL:

```
https://github.com/WeaveMindAI/weft/issues/new?template=node_request.yml&title=<name>&name=<name>&what-it-does=<one%20paragraph>&inputs-outputs=<ports>&use-case=<what%20it%20was%20built%20for>
```

You fill `name`, `what-it-does`, `inputs-outputs` (the real ports), and `use-case` from the node you just built; the rest stays as the issue's own prompts. Tangle offers this, and the user files it.
