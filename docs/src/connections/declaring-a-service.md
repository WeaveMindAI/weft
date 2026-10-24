# Declaring a service

A service recipe is the `service` block in an access node's `metadata.json`. It
is pure declaration: how a credential is obtained, how a request is signed,
what the permissions are, how events arrive. There is no Rust per service.

The node itself is one line:

```rust
weft::access_node!(SlackAccessNode);
```

## The top level

| Key | Default | What it says |
|---|---|---|
| `service` | **required** | The name, lowercase letters, digits and underscores |
| `label` | the service name | What people see, like "Slack" |
| `acquisition` | **required** | How a credential is obtained |
| `auth` | `[]` | How a request through it is signed. Empty for something that signs nothing, like a database |
| `doors` | `["own"]` | Which connect doors to offer |
| `test` | none | A call made at connect time to check it works, and where identity usually comes from. Whatever permission it needs must be granted on every connection (see `always` below) |
| `identity` | none | The display name, assembled from stored values, like `"{team} / {user}"` |
| `permissions` | `[]` | The permission catalogue people tick from |
| `allPermissionsUrl` | none | Where the provider's full list is, when yours is a subset |
| `verification` | silent, free | What the check can learn and what it costs |
| `connectionOptional` | `false` | The node runs with nothing picked |
| `callback` | `any` | What address the provider accepts for its OAuth callback: `any`, `https` (it refuses plain http, like Slack), or `domain` (https on a domain name, never an IP address, like Google) |
| `ownPage` | none | What the "your own" door shows beyond its paste fields |
| `events` | `{}` | How this service reports events, by topic |
| `verify` | none | How weft checks the callers of a route gated by this service's connections. For every key, go and read [checking a signed request](verifying-requests.md) |
| `capabilities` | `[]` | Named groups of optional fields, each unlocking one thing |
| `grants` | `coexisting` | Whether two grants of this service can coexist |

## Acquisition

| `kind` | For | Keys |
|---|---|---|
| `static` | Paste a key | `fields` |
| `oauth2` | Sign in | `grant`, `tokenUrl`, and the keys below |
| `mint_jwt` | Sign your own assertion | `fields`, `algorithm`, `claims`, `jwtTtlSecs`, `exchange` |

There is a fourth, `runtime`, and **you never write it.** It is the stored form
of a shared connect, written by the store, and declaring one is refused.

### Signing in

An `oauth2` acquisition needs two keys. `grant` is either
`authorization_code`, a consent page in the browser, with an `authUrl` and PKCE
on unless you set `"pkce": false`, or `client_credentials`, where weft asks the
token endpoint directly with the app's id and secret. `tokenUrl` is where the
code is exchanged and the token renewed.

Everything else is for a provider that does something differently from the
OAuth standard:

| Key | Default | Set it when |
|---|---|---|
| `scopeDelimiter` | a space | the consent URL joins scopes with something else. Slack and TikTok want `","` |
| `clientParams` | `{ "id": "client_id", "secret": "client_secret" }` | the provider calls the app's id or secret something else. TikTok wants `{ "id": "client_key" }`, which is then the name on the consent URL and on every token call that sends the id in its form. Templates still say `{client_id}` and `{client_secret}` |
| `authParams` | none | the consent page needs more parameters, like Google's `"access_type": "offline"`. Each value is a template over the app's values, never a secret one, because the consent URL is shown in the browser |
| `extraParams` | none | the provider wants more parameters on every token call: the exchange, the refresh, and client credentials. Each value is a template over the app's values |
| `registrationFields` | none | the app has more to paste than an id and a secret |
| `captures` | none | you want to keep values from the token answer, like the team's name. They run on every token answer. One missing from the first answer fails the connect unless it is `optional`, and one missing from a refresh answer keeps what it held |
| `tokenAuth` | `body` | the token endpoint wants the id and secret as HTTP Basic rather than in the form: set `basic` (Notion, Airtable) |
| `afterExchange` | none | the code exchange hands out a short-lived token that has to be swapped right away, like Instagram's. See [your own token calls](#your-own-token-calls) |
| `refresh` | the standard refresh | renewing is not the standard `refresh_token` POST. See [your own token calls](#your-own-token-calls) |

The answer keys in the next section go on the acquisition too.

### Reading the token answer

Every answer that hands out a token is read the same way, and the defaults are
the names the OAuth standard uses. If your provider answers somewhere else, say
where with a dotted path, where a number picks an item from a list:
`data.0.access_token` is the `access_token` of the first item under `data`.

| Key | Default | What it points at |
|---|---|---|
| `tokenPath` | `access_token` | The token, stored as `token`. An answer without one fails |
| `refreshTokenPath` | `refresh_token` | The refresh token, stored as `refresh_token` when there is one |
| `expiresInPath` | `expires_in` | How many seconds the token lives, as a number or a string of digits |
| `expiresPath` | none | When the token dies instead, as an RFC 3339 time or epoch seconds |
| `neverExpires` | `false` | Set it to `true` when the token does not expire, so no expiry is read |
| `scopePath` | `scope` | The scopes the provider says it granted, as one string split on spaces and commas, or a list |
| `errorPath` | `error` | Where a refusal sits when the provider answers it with a 200. Anything there but `null`, `false` or an empty string fails the call. Instagram puts it at `error_type` |
| `okPath` | none | Where the provider says whether the call worked, like Slack's `ok`. Anything there but `true` or `null` fails the call, and an answer with nothing there passes |

An expiry path you declare has to be in the answer, while the default
`expires_in` may simply be missing. `expiresInPath`, `expiresPath` and
`neverExpires` exclude each other.

The code found at `errorPath` also decides what a refusal means: a code saying
the provider is busy (`temporarily_unavailable`, `ratelimited`) is tried again
later, and one saying the token is bad asks for a reconnect.

If weft can renew a connection (a refresh token came back, a `refresh` call is
declared, or it is a `client_credentials` grant) and the answer names no
expiry, the connect is refused, because nothing would ever renew it:

```text
the provider's answer names no expiry at 'expires_in' (expiresInPath), so weft
would never know when to renew this connection and it would stop working
without a word. If the provider sends the lifetime under another name, declare
it on the acquisition (`expiresInPath` for seconds, `expiresPath` for a time);
if the token never expires, declare `neverExpires: true` there
```

A renewal answer is allowed to leave the expiry out, as the OAuth standard
lets a provider do when its tokens always last the same time. weft then gives
the new token the lifetime the last one had. It does the same when a renewal
answer names an expiry it cannot read (a declared path missing from it, a
value that is not a time, or a time already past when the answer arrives), so
the connection is never renewed again on every use.

### Your own token calls

`afterExchange` and `refresh` are calls you write out. Each takes a `url`, a
`method` (`GET` unless you say `POST`), a `form` or a JSON `body`, `auth`
steps, `captures`, and any of the answer keys above, which read this call's
answer and start from the same defaults.

`afterExchange` runs once, right after the code exchange, and its answer
replaces the token, its expiry and the refresh token. If it carries no refresh
token, the connection has none: the one from the exchange belonged to the
short-lived token that was just swapped away.
`refresh` runs instead of the standard refresh whenever the token is about to
expire.

Templates in them can use what the connection stores (`token`,
`refresh_token`, every capture) and the app's values (`client_id`,
`client_secret`, the registration fields). A name nothing provides is refused
when the node loads. A value that lands in the path or the query of the `url`
is percent-encoded, so a secret holding `&` arrives as one value, while in a
header, a form or a body it goes in as it is.

Instagram uses both. Its code exchange answers a one-hour token nested under
`data`, which it swaps for a 60-day one, and renews with a GET:

```json
"acquisition": {
  "kind": "oauth2",
  "grant": { "kind": "authorization_code",
             "authUrl": "https://www.instagram.com/oauth/authorize", "pkce": false },
  "tokenUrl": "https://api.instagram.com/oauth/access_token",
  "tokenPath": "data.0.access_token",
  "scopePath": "data.0.permissions",
  "errorPath": "error_type",
  "afterExchange": {
    "url": "https://graph.instagram.com/access_token?grant_type=ig_exchange_token&client_secret={client_secret}&access_token={token}"
  },
  "refresh": {
    "url": "https://graph.instagram.com/refresh_access_token?grant_type=ig_refresh_token&access_token={token}"
  }
}
```

Both calls answer `access_token` and `expires_in` at the top, so they declare
no answer keys.

### Refusals answered with a 200

Some providers answer a failed call with a 200 and say inside the body that it
failed. Slack answers `{"ok": false, "error": "invalid_auth"}`. weft only sees
that where the recipe says to look, with `errorPath` and `okPath`.

A token answer looks at `error` unless told otherwise. Every other call looks
nowhere until you declare it, because some providers put an `error` field in
answers that worked (TikTok's say `"error": {"code": "ok"}`). That covers the
`test` call, a socket's `connect`, a `subscribe` or `unsubscribe`, and a
[lookup](../nodes/metadata.md#remote_select-sources) on a node's input. Slack
declares both keys on each of its calls, like its test call:

```json
"test": {
  "url": "https://slack.com/api/auth.test",
  "method": "POST",
  "okPath": "ok",
  "errorPath": "error"
}
```

### Minting your own

A `mint_jwt` acquisition signs a JWT from the pasted key with the `claims` you
declare, valid for `jwtTtlSecs` (600 by default). Without an `exchange`, that
JWT is the token. With one, weft POSTs it as a bearer token to the exchange's
`url`, and reads the answer with `tokenPath` and `expiresPath` from the table
above. An answer naming no expiry is minted again once the JWT's own lifetime
runs out.

## Auth steps

How a request gets signed, applied in order:

| `kind` | What it does |
|---|---|
| `header` | Adds a header |
| `query` | Adds a query parameter |
| `basic` | HTTP basic auth |
| `path_prefix` | Puts a prefix on the path |
| `base_url` | Replaces the scheme, host and port, keeping the node's path |
| `sign` | Signs the request: SigV4, or OAuth 1.0a |

Every value is a template interpolating stored values by name, like
`"Bearer {token}"`. If you need a literal brace, write it twice: `{{` for `{`
and `}}` for `}`. That is how a JSON body carries a GraphQL query, so
`"query": "{{ viewer {{ login }} }}"` sends `{ viewer { login } }`.

A `header` step's `name` can be a template too, for a connection where the
person types which header their key goes in (a generic API key):
`{ "kind": "header", "name": "{header}", "value": "{key}" }`. It may only name
a declared field that is not `secret`, and if what the person typed is not a
legal header name, the call is refused before anything is sent, telling them
to reconnect.

## Permissions

```json
{ "id": "chat:write", "label": "Send messages",
  "description": "Post messages as the app.", "default": true }
```

A curated catalogue, not the provider's whole list. On a provider with hundreds
of scopes, list the ones your nodes actually need and point
`allPermissionsUrl` at the rest.

`default: true` starts it ticked. `ownOnly: true` means the runtime's own
credential can never serve it, which greys out the shared option for anyone who
ticks it.

If your `test` call needs a permission of its own to learn who the account is,
mark that permission `always: true`. It is then asked for on every connection,
whatever the person ticks, and never shown to tick. Google does this with
`openid` and `userinfo.email`, so a connection made for Gmail alone still
passes its check against Google's "who is this" endpoint. Without it, a test
call that reads Drive fails every connection that did not tick Drive. Only
mark a permission `always` when the provider grants it without a review and an
app allows it without being set up for it, because every app and every
connection asks for it. An `always` permission cannot also be `default` or
`ownOnly`.

## Verification

Say honestly what your check can learn:

| `rung` | Meaning |
|---|---|
| `reports_permissions` | The provider states what it granted |
| `self_introspect` | The credential can describe itself |
| `reports_validity` | Only "alive", never "what" |
| `probe` | A real call whose refusal would be read back. No check uses it yet |
| `silent` | Nothing knowable |

Only the first two make the recorded permissions authoritative, and only those
let weft block a node before it calls.

`cost` is `free`, `ambiguous` or `paid`, and **only `free` runs on its own**. A
check that might bill is never run without being asked.

Today the shipped catalog has 21 services. Fourteen of them declare a
verification, all `free`: three `reports_permissions`, ten `reports_validity`
and one `silent`. The seven that declare none take the default, `silent` and
`free`. So in practice the only way a connection's *permissions* are verified
right now is the OAuth scope echo, where the provider tells weft what it
granted. A `probe` would only learn about the one call it makes, so like
`reports_validity` it leaves the permissions recorded as claimed.

## Your own door

`ownPage` is what that door shows:

| Key | What it is |
|---|---|
| `mint` | A button that creates the application for them, if the provider has an API for it |
| `guide` | Numbered steps, and a pre-filled creation link |
| `paste` | The fields, when the acquisition is not already a paste form |

A `paste` block on a `static` acquisition is refused as redundant: those fields
already render.

## What it refuses

The validator is strict, and the messages name the fix. A service name with a
capital in it. A header name that is not a legal header name. A capability
listing a field the service does not declare, or listing a required field,
since a required field can never be the thing that is missing. A permission
with no description. A service that signs its requests offering the shared
door. A `token_echo` caller check, which compares a token weft minted when
subscribing and a route's caller never has.

## Events

Each topic under `events` says which facts to pull out of a payload, which
account it belongs to, and how it arrives. Go and read
[events from a service](events.md).

## Where it goes

A package root's `metadata.json`, so every node in the package inherits it, or
the access node's own file.

Exactly one input in the node needs the access widget. That is the connect
control, and a recipe without one is refused.
