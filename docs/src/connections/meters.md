# Measuring what a call costs

A node never states a cost. It never totals anything, never carries a running
figure from one call to the next, and never works out whose spend a call was.

A **meter** does that, once per provider, wrapped around the client your node
already uses. All the care lives in one place so a node author can be careless:
a node cannot produce a wrong bill, because it is never asked.

That only holds if your calls go through `ctx.open` or `ctx.client`. A client
you built yourself is outside the meter.

## What a meter is

One implementation of `ProviderMeter` per service, registered with a macro at
the bottom of its file. Its existence **is** the declaration that this
service's calls are measured. There is no flag anywhere else.

| Method | Required | What it does |
|---|---|---|
| `service()` | Yes | The service name, matching the access node's |
| `base_url()` | Yes | Where the provider really lives. The single authority, so no caller ever takes a host from a request |
| `classify(method, path)` | Yes | What kind of route this is |
| `observe(path, query, body)` | Yes | A fresh tap for one response |
| `resolve(path, observed, follow_up)` | Yes | Price one observed call |
| `prepare(path, body)` | No | Rewrite an outgoing body so the cost comes back reportable |
| `priceable(path, follow_up)` | No | Asked before sending: can this be priced at all when it returns? |
| `opens_charge`, `charge_reported_on`, `fold_report` | No | For a provider that reports the cost later, on a separate response |
| `observe_session(path, query)` | For a session route | A fresh tap for one long-lived session, priced by its frames |

## Routes

| Class | Meaning |
|---|---|
| `Billable` | Spends money. Prepared, observed, priced |
| `Free` | A known route that costs nothing: a model list, a health check, a cost lookup |
| `Reports` | Not a charge itself. It speaks for one that is already open |
| `BillableSession` | A session priced by its own frames as they flow |
| `Unknown` | Not a route this meter knows |

`Free` is why a node asking what something cost is harmless, and why the
meter's own follow-up query does not bill.

## Route matching is a security boundary

`classify` gets the path **relative to the base URL**, with no leading slash
and no query, and matches it exactly.

Anything that does not match lands in `Unknown`. That is deliberate: a
traversal, an encoded traversal, a backslash or a host smuggled into the
userinfo all simply fail to match, rather than sliding into a cheaper route's
price.

## What gets recorded

`MeasuredCost` carries an amount, a model and some details.

The amount is **optional**, and `None` means genuinely unknown. It is recorded
as unknown, and never booked as zero.

That distinction is the whole discipline. `None` is for a figure the provider
will not give you. It is not for one nobody went looking for, and it is
certainly not a licence to re-derive the quantity from the request: a token
count you worked out yourself is unmetered spend wearing the shape of a
measurement.

`resolve` never errors, for the same reason. An unresolvable cost is an honest
unknown, not a failed call.

## What the meter never touches

A credential. It gets a signed side-channel for its own follow-up queries and
nothing else, which is what lets one meter work identically on a pasted key and
on a sign-in.

Each record also says whose credential paid. For what each value means, go and
read
[who paid for a call](../running/instances.md#who-paid-for-a-call).

## Where the number surfaces

You see the number in the inspector beside the firing's duration, with whose
key paid next to it. It also appears in the run's events as soon as the call
happens.

This is weft's own record of what it observed, not the provider's bill. They
should agree, and when they do not, the provider is right.

## Reading it from a program

If your program needs the numbers (to bill somebody, or to stop spending), the
`InstanceCosts` node reads them for one instance. Give it `instance`, and
narrow it if you want with `run` (a run id, the one `GetRunId` gives),
`service`, `node`, `paidBy` (whose credential paid: `platform` for the runtime's
credit, `author` for your own connection, `instance` for the instance's own) or `since` (unix seconds). It answers with three
outputs: `records`, one entry per call; `totalUsd`, the
sum; and `unpricedCalls`, how many of those calls have no known price. `totalUsd` adds up only the known ones, so it is the whole cost
only when `unpricedCalls` is `0`.

## Capping what one instance spends

weft puts no cap on spending. If you want one, check the total before the paid
node and gate that node on the answer:

```weft
question = Route -> (body: { text: String }) { path: "ask", method: "POST" }
openrouter = OpenRouterAccess
model = OpenRouterModel { access: openrouter.access, model: "z-ai/glm-5.3" }

ask = Group(text: String, provider: LlmProvider) -> (answer: String) {
  # Answer only while this instance has spent less than five dollars in all
  who = CurrentInstance
  spent = InstanceCosts { instance: who.instance }
  budget = Switch {
    value: spent.totalUsd
    cases: [
      { "kind": "lt", "value": 5, "port": "underCap" },
      { "kind": "otherwise", "port": "overCap" }
    ]
  }

  answer = LlmInference {
    _should_flow: budget.underCap
    prompt: self.text
    provider: self.provider
  }
  self.answer = answer.response

  refuse = Close {
    _should_flow: budget.overCap
    status: 402
    reason: "the budget is spent"
  }
}
ask._should_flow = question.fired
ask.text = question.body.text
ask.provider = model.provider

reply = Reply { body: ask.answer }
```

Each caller sends its instance token (minted with `weft token mint
--instance`) in the `Weft-Instance-Token` header. A
call without one is for no instance, so `who` fires `nobody`, which nothing
here is wired to: the run ends without answering, and the caller gets a `500`
saying `Something went wrong`. If you want a clear refusal instead, gate a
`Close` on `who.nobody`. For the other ways a call becomes an
instance's, go and read [programs with instances](../running/instances.md).

The check can let a little through. A call's cost is written down when its
answer ends, and a job the provider runs in the background is written down
when a read of the job says it finished. So a call still going is not in the total
yet. And two runs of the same instance that check at the same moment both see
the same total, both pass, and together can go over.

If your cap covers a long stretch, know that a run's costs are deleted with the
run, so the total only counts runs weft still keeps. The example above has no
`since`, so it really caps the last week of spending, because ended runs are
kept for a week by default. If you want a cap over all time, keep the runs
`forever` (the trigger's `keepRunsFor`, or `keep_for` in `weft.toml`). If your
cap uses `since`, keep runs at least as long as that window. For how to set
either, go and read [how long a run is kept](../running/the-journal.md#how-long-a-run-is-kept).

## A provider that states no price

ElevenLabs is the example today. Its image and video generations (`flows/image`, `flows/video`) come
back with no cost in any answer. weft prices them from the request instead:
the model, the settings that change its price, and how many images or seconds
of video it asks for. That is what ElevenLabs bills these by, so the request
holds the whole bill. The rates are ElevenLabs' own pricing table, captured by
hand on 2026-10-16 for the account tier `grant_tier_2_2025_07_23`. If
ElevenLabs changes its prices, the rates to update are in
`crates/weft-providers/src/providers/elevenlabs_prices.json`.

| Model | What moves the price | At its defaults |
|---|---|---|
| `gemini-2.5-flash-image` | Nothing | 4.98¢ per image |
| `gemini-3-pro-image` | `resolution` (default `1K`) | 24.12¢ per image |
| `gemini-3.1-flash-image` | `resolution` (default `1K`) | 8.04¢ per image |
| `gemini-3.1-flash-lite-image` | `resolution` (default `1K`) | 4.08¢ per image |
| `bytedance-seedream-5-lite` | Nothing | 4.2¢ per image |
| `bytedance-seedream-5-pro` | `resolution` (default `2K`) | 16.2¢ per image |
| `gpt-image-1` | `quality` (default `medium`), `aspect_ratio` (default `1:1`) | 6.84¢ per image |
| `gpt-image-1.5` | `quality` (default `medium`), `aspect_ratio` (default `1:1`) | 5.88¢ per image |
| `gpt-image-2` | `quality` (default `medium`) | 6.48¢ per image |
| `gpt-image-2.5-sunburst`, `gpt-image-2.5-flare` | `quality` (default `high`) | 6.68¢ per image |
| `veo-3.1-generate-001` | `resolution` (only `4K` changes it, default `720p`), `generate_audio` (default on) | 48¢ per second |
| `veo-3.1-fast-generate-001` | the same | 18¢ per second |
| `bytedance-seedance-v2` | `resolution` (default `720p`), `aspect_ratio` (default `16:9`) | 36.29¢ per second |
| `bytedance-seedance-v2-fast` | the same | 29.04¢ per second |
| `bytedance-seedance-v2-mini` | the same | 18.14¢ per second |
| `bytedance-seedance-v2.5` | the same, except that with a `start_frame` the frame decides the aspect, and the table has no rate for that | 55.46¢ per second |

A video costs its rate times `duration_secs`, which is 8 by default on Veo and
5 on Seedance.

Any other model, a setting the table has no rate for, or a video whose length
the request does not say (a lip-sync video lasts as long as its audio) is
written down as an unknown price, never at the nearest rate. Starting a
generation only queues it, and ElevenLabs does not charge for one that fails.
So the price is written down when your node reads the job back and finds it
`completed`, and a `failed` one is written down at $0. If the run ends before
the job is read back, its price is written down as unknown.

## Writing one

Copy the nearest existing meter. What it has to get right:

Say the base URL once and never take a host from anywhere else. Classify
exactly, and let anything unrecognised be `Unknown`. Tap the response as the
bytes flow rather than buffering it. Return `None` when the provider will not
tell you, rather than guessing. And if a route could never be priced when it
comes back, say so in `priceable` **before** the call, so the caller finds out
before spending rather than after.
