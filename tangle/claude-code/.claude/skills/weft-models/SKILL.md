---
name: weft-models
description: "Read when a program calls a language model and you need it to behave: whether it reasons before answering and how hard, what bounds a reply's length and why one comes back empty, what a class of model costs in time and money, and how prompt caching works across a chain of calls."
---

# Models: reasoning, budgets, and caching

Three things decide how a model call runs, and they sit in different
places. The provider node says WHICH model. The LLM node itself says
what it is given and what comes back. And [params], the node wired into
an LLM node's `params` input, says how it thinks: everything below that
is not about the prompt is a setting on it.

Everything here is about the model's behaviour, which no metadata file
can tell you: what a setting DOES to an answer, what it costs, and how
the pieces interact across a chain of calls.

## Reasoning is off unless you turn it on

`reasoning` on [params] has two positions: off (unset, or `false`),
or on with the `reasoningEffort` you picked, `low` (the fast, cheap
effort) when you picked none.

- **Off** sends no reasoning request; a plain model runs as it always
  does. A model that cannot switch reasoning off refuses on the providers
  that spell the off position, and the failure reads ``This model always
  reasons; set `reasoning: true` on its params (low effort unless you pick
  one)``. On the OpenAI and Anthropic wires nothing is sent for off, so
  such a model reasons at its own default and bills you for it: the call
  that takes four seconds takes forty-five.
- **On** sends the effort. A model that has no reasoning fails with its
  provider's own raw error, or ignores the setting; when the model says it
  cannot reason, you set `false`.

There is no retry behind either failure: the fix is one edit on [params].
Most modern models reason, so on a model you do not know you start with
`true` and raise the effort only for the one step that needs
it.

A prompt that asks the model for its thinking inside the answer (a
`reasoning` key in a JSON reply, a "think first" section) runs with the
model's own reasoning off. If you catch yourself wiring `reasoning: true`
under such a prompt, stop and write: "Wait. Paying for thinking twice."
Then set `reasoning: false` on [params].

## `maxTokens` bounds the whole reply, thinking included

Unset, nothing is sent and the provider's default applies. Set, it is the
most tokens the reply may hold, and on a reasoning model the thinking
counts against it, so a reasoning model with a small `maxTokens` spends
the whole budget thinking and answers nothing. That failure reads `the
model spent N reasoning tokens and answered 0 text tokens: raise maxTokens
so the answer fits after the thinking, or set reasoning off`. Those are the two fixes, and you
raise `maxTokens` first.

A reply that is empty for another reason fails as `the provider returned
an empty response (no text, tool calls, or media)`: the model answered
nothing, and you look at the prompt.

## What a class of model costs

Rough magnitudes for choosing, never for billing:

- A small fast model answers in a second or two and costs cents per
  thousand calls. You use it for routing, extraction, classification,
  anything that forks the graph on a value it computes. It does not
  follow an elaborate manipulation,
  so when a screen or a gate is worth its extra call, the small model
  plays it (the `weft-safety` skill says when to offer that).
- A large model answers in several seconds and costs ten to fifty times
  more per token. You use it where the answer is the product.
- A reasoning model at high effort takes tens of seconds to minutes and
  bills every reasoning token. You use it for one hard step. If you catch
  yourself wiring it into every turn of a conversation, stop and write:
  "Wait. One hard step." Then give that one step its own [params] node.

Two nodes wired to one [params] share every setting; a step that
needs a different model or effort gets its own [params] node, with a
comment saying why.

## Prompt caching

Providers that cache reuse the prefix of a conversation they have already
seen, which is where most of the cost of a chain of calls sits. A [mark]
is the place on a message where caching is requested; weft puts [mark]s
on the messages themselves and the library translates them per provider,
so you write nothing provider-specific.

A node with `autoCache` on (the default wherever the input exists) places
[mark]s for you: with a wired history that carries no [mark], the request
marks the system message and the last history message before this call's
new turn (a history with no system message marks the last one alone). The
[mark]s live on the request alone; the emitted `history` is the
conversation as written, so each call in a chain marks the prefix it sees.
You chain the calls through `history`, and the shared prefix is cached
from the second call on.

To place [mark]s yourself, set `cacheBreakpoint: true` on a Chat Message
node: everything up to and including that message is cached. A history
that carries any [mark] of yours turns the automatic ones off, so you
place them all or none. The wires that translate [mark]s (the Anthropic
family, and OpenRouter where it fronts one) keep at most four, the last
ones win, so marking freely never fails a call; the wires with no marker
concept drop the [mark]s with a warning in the log, and a call never fails
on one.

Whichever way the [mark]s land, the shape that caches is:

- the stable persona and reference material first, in the system message;
- the volatile values (the time, the user's name, today's notecard) last,
  in the newest user turn, never inside the persona;
- the conversation as real turns appended with Chat Message, never one
  blob of text rebuilt every call: a rebuilt blob is a new prefix every
  time and caches nothing.
