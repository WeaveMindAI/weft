# Measuring what a call costs

A provider meter turns a request and its observed response into a cost
record. For an LLM call, it might read token usage; another API might
report a charge that the meter retrieves afterwards.

The record is only as complete as the provider's evidence and the meter's
pricing logic. If a response is interrupted before usage arrives and no
lookup can recover it, the cost is unknown.

## Add a meter

Implement `ProviderMeter` in a Rust module and register its instance:

```rust
weft_providers::register_meter!(MY_METER);
```

`MY_METER` is your meter instance. Put the module in a package's shared
Rust files or in a bare node's `mod.rs`, so it gets compiled into the
worker. For a built-in meter, add its module under
`crates/weft-providers/src/providers/`, declare it in that directory's
`mod.rs`, and register its instance with `crate::register_meter!`.

The meter's `service()` must match the connection recipe's service name.
That name connects the two; a processing node does not import the meter.

For a worked implementation, read
[OpenRouter's meter](https://github.com/WeavemindAI/weft/blob/mvp/crates/weft-providers/src/providers/openrouter.rs).

## What happens around a call

On a direct connection, the client checks whether the request falls under
the meter's `base_url`, then calls `classify` with the HTTP method and
relative path.

For a billable route, it runs `prepare` on the request body, sends the
request, and gives an observer the response status and chunks. The
consumer receives those chunks as they arrive. When the response ends or
is dropped, `resolve` turns the observation into a cost record.

Billable requests need a buffered body. Even a request with no payload
must send an explicit empty body, such as `.body(Vec::<u8>::new())`.
Streaming bodies and absent bodies are refused before sending.
The response can stream.

## Classify routes explicitly

| Class | Meaning |
|---|---|
| `Billable(Fixed)` | A fixed-price operation whose outcome still needs checking |
| `Billable(Metered)` | Cost depends on measured usage |
| `BillableSession` | A WebSocket session measured from its frames |
| `Free` | A route declared to have no charge |
| `Unknown` | This meter does not recognize the operation |

A successful HTTP status does not always prove that a fixed-price
operation was charged. Inspect the provider's outcome before recording
the fixed amount.

Cost-lookup and status routes should be classified according to their own
price. A free lookup stays `Free` even though it returns the cost of
another request.

`classify` receives the parsed relative path without a leading slash or
query string. Match the path segments you support; the original URL
spelling and its query parameters are unavailable here.

### Runtime-owned credentials

On a direct HTTP connection using a runtime-owned credential, weft checks
the initial request URL before authentication. URLs outside the meter's
base or classified `Unknown` are refused. A meter must be registered even
if every allowed route is free.

Redirect destinations are not checked again. Custom authentication headers
can follow a redirect to another origin.

Direct WebSocket connections currently apply handshake authentication
without this route check. Use a trusted provider URL; an unrelated
destination can receive the authentication too.

On the user's own direct HTTP connection, an unknown route passes through
unmeasured. A service without a registered meter can also be used with the
user's credential.

The registry does not distinguish a built-in meter from one supplied by
the project. Installing a project meter is therefore a trust decision
about code, not a proof that weft's maintainers reviewed its prices.

A relayed connection sends measurement work to the relay. Its client
requires a meter to identify the service's base URL and refuses URLs
outside that base.

## Implement the observation

The main methods are:

| Method | Job |
|---|---|
| `service` | Name the connection service |
| `base_url` | Name its API base |
| `classify` | Recognize the method and relative route |
| `prepare` | Adjust a billable request so its usage can be measured |
| `observe` | Create an observer using the path, query, and prepared request body |
| `resolve` | Produce a `MeasuredCost` from the observation |

For the exact signatures, read
[ProviderMeter](https://github.com/WeavemindAI/weft/blob/mvp/crates/weft-providers/src/lib.rs).

`prepare` can enable a provider's usage reporting or remove internal
estimation fields before sending. Return `Ok(None)` to keep the body
unchanged. If a required rewrite cannot parse the request, return an error.

A `CallObservation` receives `on_status`, followed by `on_chunk`
calls, then `end(interrupted)`. Keep the state needed to interpret
chunk boundaries and extract usage. An observer may retain a bounded copy
of response data; it must not accumulate an unlimited response in memory
or hold up the consumer until the whole response arrives.

`resolve` returns:

```rust
MeasuredCost {
    amount_usd: Some(amount),
    model: Some(model),
    metadata: details,
}
```

Here `amount` is an `f64`, `model` is a `String`, and `details`
is JSON containing the evidence useful for inspecting the cost.

Return `amount_usd: None` when you cannot establish the charge.
Use zero only when the evidence establishes no charge.

### Look up a charge after the response

Some providers report an identifier before they make usage available.
Use the supplied `FollowUp` client to query their ledger or price API
from `resolve`.

This client carries the connection's authentication. In the local
implementation, it has a 30-second timeout, disables redirects, and does
not include the metering middleware. Its requests therefore do not
recursively create another measurement.

Choose a provider endpoint that does not itself incur an unreported
charge.

### Measure a session

A `BillableSession` route needs `observe_session`.
Its observer receives frames in both directions, exposes accumulated cost
through `accrued_usd`, and returns a `MeasuredCost` when it ends.

Keep frame parsing separate from the arithmetic so you can test both
partial exchanges and complete sessions.

## Estimates and spending limits

The trait also exposes `ceiling_usd`, `session_slice_usd`, and
`session_max_frame_bytes`. `ceiling_usd` estimates a call's upper cost
before sending it. `session_slice_usd` supplies the amount to reserve at
a time for a prepaid session, and `session_max_frame_bytes` supplies a
frame-size limit.

The local runtime does not use those methods to reserve a prepaid balance
or enforce a spending cap. Implementing them does not give an installation
that behavior. The local cost record measures calls; the provider still
charges the account associated with the credential.

## Keep pricing current

When a provider publishes a machine-readable rate catalog, use it with a
bounded cache instead of copying a model price list into constants.
Cover the choices the processing node offers.

If a provider introduces an unsupported billing unit, report unknown cost
until you can interpret it. Treating an unfamiliar unit as one image or
one second would produce a plausible but wrong number.

For rate lookup and handling different billing units, read the
[fal meter](https://github.com/WeavemindAI/weft/blob/mvp/crates/weft-providers/src/providers/fal.rs).

## Test with recorded exchanges

Test route classification, including malformed paths and unsupported
methods. Test request preparation with both valid and unreadable bodies.

For observation, use recorded provider responses with known expected
costs. Split streaming responses at awkward byte boundaries, and include
an interrupted response. The interrupted case should remain unknown
unless the available evidence or a follow-up lookup establishes the cost.

For a session meter, feed recorded frames in both directions and check
the accumulated and final amounts. A test that only repeats the meter's
own arithmetic will not catch a wrong interpretation of the provider's
usage fields.
