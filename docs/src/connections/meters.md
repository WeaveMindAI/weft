# Measuring what a call costs

A node never states a cost. It never totals anything, never carries a running
figure between steps, and never works out whose spend a call was.

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
| `ceiling_usd(...)` | No | The worst case, from the request bytes alone, before the call |
| `opens_charge`, `charge_reported_on`, `fold_report` | No | For a provider that reports the cost later, on a separate response |
| `observe_session`, `session_slice_usd`, `session_max_frame_bytes` | No | For a long-lived session priced by its frames |

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

## Where the number surfaces

On the firing, in the inspector, beside the duration. And in the run's events,
as the call happens rather than at the end.

This is weft's own record of what it observed, not the provider's bill. They
should agree, and when they do not, the provider is right.

## Writing one

Copy the nearest existing meter. What it has to get right:

Say the base URL once and never take a host from anywhere else. Classify
exactly, and let anything unrecognised be `Unknown`. Tap the response as the
bytes flow rather than buffering it. Return `None` when the provider will not
tell you, rather than guessing. And if a route could never be priced when it
comes back, say so in `priceable` **before** the call, so the caller finds out
before spending rather than after.
