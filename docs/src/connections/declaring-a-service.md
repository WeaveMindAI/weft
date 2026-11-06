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
| `capabilities` | `[]` | Named groups of optional fields, each unlocking one thing |
| `grants` | `coexisting` | Whether two grants of this service can coexist |

## Acquisition

| `kind` | For | Keys |
|---|---|---|
| `static` | Paste a key | `ways` |
| `oauth2` | Sign in | `grant`, `tokenUrl`, and the keys below |
| `mint_jwt` | Sign your own assertion | `fields`, `algorithm`, `claims`, `jwtTtlSecs`, `exchange` |

There is a fourth, `runtime`, and **you never write it.** It is the stored form
of a shared connect, written by the store, and declaring one is refused.

### Pasting a key

A `static` acquisition lists the ways somebody can sign in. Most services have
one, and for one way you write only its fields and, if it helps, a `guide` for
getting them:

```json
"acquisition": {
  "kind": "static",
  "ways": [{
    "fields": [{ "name": "key", "label": "API key", "placeholder": "sk-ant-..." }],
    "guide": {
      "link": "https://console.anthropic.com/settings/keys",
      "steps": ["Create an API key.", "Paste it below and name the connection."]
    }
  }]
}
```

Some services take more than one kind of credential. Claude Code runs on a
Claude subscription's token or on an API key, so its recipe offers both, and
the connect form asks which one before it shows any field:

```json
"acquisition": {
  "kind": "static",
  "ways": [
    { "id": "subscription", "label": "Claude subscription", "localOnly": true,
      "fields": [{ "name": "token", "label": "Token", "placeholder": "sk-ant-oat01-..." }],
      "guide": { "steps": ["Run `claude setup-token` in a terminal.", "Paste the token it prints below."] } },
    { "id": "apiKey", "label": "API key",
      "fields": [{ "name": "key", "label": "API key", "placeholder": "sk-ant-..." }],
      "guide": { "link": "https://console.anthropic.com/settings/keys", "steps": ["Create an API key."] } }
  ]
}
```

| Key | Default | What it says |
|---|---|---|
| `id` | `default` | What the connection records, and what your node compares to tell the credentials apart. camelCase, and unique within the service. Only a service with one way leaves it out |
| `label` | none | What the choice shows. Every way needs one when there are several |
| `fields` | **required** | The fields the person fills in for this way. A connection made this way stores only these, plus whatever the `test` call captures |
| `guide` | none | How to get those fields, with a `link` and `steps`, shown above them |
| `localOnly` | `false` | Offer this way only on a local install, so the credential stays on the person's own machine |
| `renewal` | none | What is pasted is a sign-in that expires, and weft keeps it alive. See [a pasted login weft keeps alive](#a-pasted-login-weft-keeps-alive) |

The connection remembers which way it was made with, and your node reads it as
[`way()`](using-one-in-a-node.md#what-an-opened-connection-knows). Set
`localOnly` when the provider does not let a third party hold its consumer
sign-in, like a claude.ai login. A cloud install then never offers that way.
For everything a cloud install does with such a connection, go and read
[how connections work](how-they-work.md#a-way-that-stays-on-your-machine).

`auth`, `test` and `identity` sit outside `ways`, so they are the same
whichever way somebody picks. Every way has to collect each value they name.
The one exception is a value the `test` call captures, which an auth step or
the identity may use. If Claude Code's recipe had an `x-api-key: {key}` auth
step, the subscription way would not collect `key`, and loading the node
refuses it:

```text
way 'subscription' does not collect 'key': the service's auth steps interpolate
it, so every request through such a connection would fail. Add the field to that
way, or leave 'key' out of what the service declares for all its ways
```

If your ways sign differently, leave `auth` empty. `client()` then signs
nothing, and your node reads each value with `value(name)` and hands it on
itself, as in
[telling the credentials apart](using-one-in-a-node.md#what-an-opened-connection-knows).
A `capabilities` group may only name fields of one way, because a connection
holds one way's fields and a group is filled all at once.

### A pasted login weft keeps alive

Some logins can only be pasted, yet expire and renew like a sign-in. Codex's
ChatGPT login is a file, `auth.json`, holding a token that lasts ten days and a
refresh token that rotates: each renewal retires the refresh token before it.
If two programs each renew their own copy, the second one's refresh token is
already dead and it is logged out.

If your service has a login like that, give its way a `renewal`, and weft
becomes the only one who ever renews it. The paste is read once into a token
set, and the connection keeps renewing it the way a sign-in renews. Every node
opening the connection is handed the same short-lived token, never the refresh
token. Here is Codex's:

```json
{ "id": "subscription", "label": "ChatGPT login", "localOnly": true,
  "fields": [{ "name": "authJson", "label": "auth.json" }],
  "renewal": {
    "paste": { "field": "authJson",
               "tokenPath": "tokens.access_token", "refreshTokenPath": "tokens.refresh_token",
               "jwtExpiry": true,
               "captures": [{ "name": "accountId", "path": "tokens.account_id" }] },
    "call": { "url": "https://auth.openai.com/oauth/token", "method": "POST",
              "body": { "client_id": "app_EMoamEEZ73f0CkXaXp7hrann", "grant_type": "refresh_token",
                        "refresh_token": "{refresh_token}" },
              "jwtExpiry": true } } }
```

`paste` names the field holding the pasted JSON, and reads it with the keys of
[the token answer](#reading-the-token-answer): the token is stored as `token`,
the refresh token as `refresh_token`, and each capture under its own name. The
pasted field itself is not stored, since it holds the refresh token. If the
token in it has already expired, the paste is still taken, and the first open
renews it.

`call` is the renewal, written like [your own token calls](#your-own-token-calls),
over what the paste stored. It runs whenever the token is about to expire, and
whenever a node says the token was refused
([`ctx.renew`](using-one-in-a-node.md#when-the-service-refuses-the-values)).

A paste that leaves out what the call needs is refused, so you find out at the
paste rather than ten days later:

```text
'authJson' holds no refresh token (the renewal needs 'refresh_token'); paste the
whole file, from a machine signed in
```

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
| `jwtExpiry` | `false` | Set it to `true` when the token is a JWT and only its own `exp` claim says when it dies, as OpenAI's tokens do |
| `scopePath` | `scope` | The scopes the provider says it granted, as one string split on spaces and commas, or a list |
| `errorPath` | `error` | Where a refusal sits when the provider answers it with a 200. Anything there but `null`, `false` or an empty string fails the call. Instagram puts it at `error_type` |
| `okPath` | none | Where the provider says whether the call worked, like Slack's `ok`. Anything there but `true` or `null` fails the call, and an answer with nothing there passes |

An expiry path you declare has to be in the answer, while the default
`expires_in` may simply be missing. `expiresInPath`, `expiresPath`,
`neverExpires` and `jwtExpiry` exclude each other.

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

### A status other than 200

The `test` call passes when it answers 200. If the provider checks the key
before it reads the request, set `expectStatus` to the status you get once the
key is accepted. `ExaAccess` sends a POST with no body and expects 400: a good
key reaches the missing-body error, and a bad one is refused earlier with 401.

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

A service that declares no verification takes the default, `silent` and `free`.
A `probe` would only learn about the one call it makes, so like
`reports_validity` it leaves the permissions recorded as claimed.

## Your own door

`ownPage` is what that door shows:

| Key | What it is |
|---|---|
| `mint` | A button that creates the application for them, if the provider has an API for it |
| `guide` | Numbered steps for creating the application, and a pre-filled creation link |
| `paste` | The fields, when the acquisition is not already a paste form |

A `paste` block on a `static` acquisition is refused as redundant: those fields
already render. A `guide` there is refused too: for a pasted key, the steps
for getting it go in each way's own `guide`.

## What it refuses

The validator is strict, and the messages name the fix. A service name with a
capital in it. A header name that is not a legal header name. A `static`
acquisition with no way, two ways sharing an id or a label, a way id that is
not camelCase, a way with no fields, an empty `label` or one missing when there
are several, or a way's `guide` with no steps. A way's `renewal` whose
`paste.field` is not one of the way's fields, that declares `neverExpires`, or
whose call uses a value the paste does not store. A capability
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
