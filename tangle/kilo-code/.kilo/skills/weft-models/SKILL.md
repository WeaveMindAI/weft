---
name: weft-models
description: "Choosing and tuning a language model in a weft program. Read before wiring an LlmInference or LlmStream: reasoning off by default and on at low, what each sends and what a refusal means; what maxTokens bounds; why a reply comes back empty; what a class of model costs in time and money; and how prompt caching is placed across a chain of calls."
---

# Models: reasoning, budgets, and caching

Everything here is a knob on `LlmParams` (wired into the LLM node's
`params`) or on the LLM node itself. The provider node says which model;
these say how it runs.

## Reasoning is off unless you turn it on

`reasoning` on `LlmParams` is a switch with two positions, and the params
always carry one: off (unset, or `false`), or on with the `reasoningEffort`
you picked, `low` when you picked none: the fast, cheap effort.

- **Off** sends no reasoning request. A plain model runs as it always does.
  A model that cannot switch reasoning off refuses on the providers that
  spell the off position, and the failure reads
  ``This model always reasons; set `reasoning: true` on its params (low
  effort unless you pick one)``. On the OpenAI and Anthropic wires nothing
  is sent for off, so such a model reasons at its own default and bills
  you for it: the call that should take four seconds takes forty-five.
- **On** sends the effort. A model that has no reasoning fails with its
  provider's own raw error, or ignores the setting; when the model says it
  cannot reason, set `false`.

There is no retry behind either failure: the fix is one edit on the params
node. Most modern models reason, so on a model you do not know, start with
`true` and raise the effort only for the one step that needs it. Either way
the first run tells you, and the second run is right.

If the prompt already asks the model for its thinking in a field of the
answer (a `reasoning` key in a JSON reply, a "think first" section), turn
the model's own reasoning off: paying for both is the commonest way a
call gets slow and expensive at once.

## `maxTokens` bounds the whole reply, thinking included

Unset, nothing is sent and the provider's default applies. Set, it is the
most tokens the reply may hold, and on a reasoning model the thinking
counts against it. So a reasoning model with a small `maxTokens` can spend
the whole budget thinking and answer nothing. That failure reads
`the model spent N reasoning tokens and answered 0 text tokens: raise
maxTokens so the answer fits after the thinking, or set reasoning off`,
and those are the two fixes, in that order of preference.

A reply that is empty for another reason fails as `the provider returned
an empty response (no text, tool calls, or media)`: the model answered
nothing, and the prompt is where to look.

## What a class of model costs

Rough magnitudes, for choosing, not for billing:

- A small fast model answers in a second or two and costs cents per
  thousand calls. Use it for routing, extraction, classification, anything
  a `Switch` reads. Its dumbness is a safety feature too: it does not
  follow an elaborate manipulation, so when a screen or a gate is worth
  its extra call, the small model is the one that plays it (the
  `weft-safety` skill says when that is worth asking for).
- A large model answers in several seconds and costs ten to fifty times
  more per token. Use it where the answer is the product.
- A reasoning model at high effort can take tens of seconds to minutes and
  bills every reasoning token. Use it for one hard step, never for every
  turn of a conversation.

Two nodes wired to one `LlmParams` share every setting; when one step
needs a different model or effort, give it its own params node and say
why in a comment.

## Prompt caching

Providers that cache reuse the prefix of a conversation they have already
seen, which is where most of the cost of a chain of calls sits. weft marks
the places to cache on the messages themselves and the library translates
them per provider, so there is nothing provider-specific to write.

`LlmInference` and `LlmStream` do it for you when `autoCache` is on (the
default): with a wired history that carries no mark, the request marks the
system message and the last history message before this call's new turn (a
history with no system message marks the last one alone). The marks live on
the request alone; the emitted `history` is the conversation as written, so
each call in a chain marks the prefix it sees. Chain the calls through
`history` and the shared prefix is cached from the second call on.

To place the marks yourself, set `cacheBreakpoint: true` on a Chat Message
node: everything up to and including that message is cached. A history
that carries any mark of yours turns the automatic ones off, so place them
all or none. The wires that translate marks (the Anthropic family, and
OpenRouter where it fronts one) keep at most four, the last ones win, so
marking freely never fails a call; the wires with no marker concept drop
the marks with a warning in the log, and a call never fails on one.

Whichever way the marks land, the shape that caches well is the same:

- the stable persona and reference material first, in the system message;
- the volatile values (the time, the user's name, today's notecard) last,
  in the newest user turn, never inside the persona;
- the conversation as real turns appended with Chat Message, never one
  blob of text rebuilt every call, because a rebuilt blob is a new prefix
  every time and caches nothing.
