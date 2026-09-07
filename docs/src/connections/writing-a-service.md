# Declaring a service

The whole of a service is the `service` block in its access node's
`metadata.json`. There is never per-service Rust.

The pieces the JSON is built from are protocols weft implements once (an HMAC,
SigV4, OIDC), and a service picks them and fills in the details. When a
mechanism genuinely cannot be described as data, it becomes a new one of those,
available to every service rather than to the one that needed it.

## The whole node

```jsonc
{
  "type": "SlackAccess",
  "service": {
    "service": "slack",
    "doors": ["shared", "own"],
    "acquisition": { "kind": "oauth2", /* or "static", "mint_jwt" */ },
    "auth": [ { "kind": "header", "name": "Authorization", "value": "Bearer {token}" } ],
    "permissions": [ /* the catalogue, one human sentence each */ ],
    "test": { "url": "https://slack.com/api/auth.test", "method": "POST" },
    "identity": "{team}"
  },
  "inputs": [
    { "name": "account", "type": "JsonDict",
      "widget": { "kind": "access" }, "label": "Workspace" }
  ],
  "outputs": [ { "name": "access", "type": "Access" } ]
}
```

And the entire Rust:

```rust
weft::access_node!(SlackAccessNode);
```

The compiler finds the picker input by its `access` widget, never by name.
The `access_node!` macro requires it to be named `account`; a node writing its
own body (the LLM providers name theirs `connection`) picks its own name.

Every macro access node's body is the same pass-through, so it cannot drift.

Declaring a `service` block also means the node requires a connection: the
language synthesizes the runtime "no connection picked" rule for it, and the
editor keeps the unconnected node expanded until one is picked. You never
write that rule yourself. If the node genuinely runs without a connection (a
custom endpoint that may be unauthenticated), declare
`"connection_optional": true` in the `service` block and both behaviors turn
off. Such a node cannot use `access_node!` (its pass-through body has nothing
to pass through when no connection is picked, so the macro refuses the
combination at run time): write your own body and read the pick with
`ctx.inputs.access("<your picker input's name>")?`, which answers `None` when
nothing is picked. The LLM providers do exactly this, with an input named
`connection`.

## Acquisition: how a credential is obtained

| Kind | What happens |
|---|---|
| `static` | the user pastes the declared `fields` |
| `oauth2` + `authorization_code` | browser consent, then code, then token. PKCE by default. |
| `oauth2` + `client_credentials` | a server-to-server token from the app's credentials, re-requested on expiry |
| `mint_jwt` | paste a private key; weft mints a short-lived JWT and optionally exchanges it for a working token, lazily. GitHub Apps, service accounts. |

`captures` pull values off token and test responses by dotted JSON path, and
become stored values that templates can interpolate.

An `oauth2` acquisition may declare `token_auth` for how every token call
authenticates the client: `body` (default) sends the client id and secret as
form fields, `basic` sends them as HTTP Basic, for providers that ignore body
credentials.

It may also declare `refresh`, its own renewal call, for providers whose
renewal is not the standard refresh-token POST. It is a declared call like any
other, running at refresh time over the stored values plus the app's, with its
captures writing back and a top-level `expires_in` setting the next expiry. On
a registered app it may only address the app's pinned sign-in origin.

A test capture named `granted_permissions` records the credential's
introspected permission set as **verified** on a `self_introspect` service.

## Auth steps: how a request is signed

Applied by the client in the worker, in declaration order, with signing steps
last so the signature covers the final request. Templates interpolate stored
values by name, and an unknown name is a loud error rather than an empty
substitution.

| Kind | Example |
|---|---|
| `header` | `Authorization: Bearer {token}`, or any header |
| `query` | `?key={token}` |
| `basic` | Stripe's `key:`, Twilio's `sid:token` |
| `path_prefix` | Telegram's `/bot{token}/` |
| `base_url` | per-account API bases: an S3 endpoint, a captured instance URL |
| `sign` | per-request cryptographic signing: `sigv4` for S3-compatible stores, `oauth1a` for X |

The S3 access node gets AWS SigV4 request signing from that one `sign` block in
its JSON, and its entire Rust is the `access_node!` macro line. The signing
code belongs to the protocol, shared by every S3-compatible store.

A service whose credential is not a request transform at all declares **no**
auth steps, and consumers read the stored values by name instead. That is how
anything that is not an HTTP API gets a connection: `PostgresAccess` collects a
host, port, database, user and password, seals them, and the query nodes open
the connection and read what they need. Nothing about it is an ordinary node
input, so the password is never in config and never on a wire.

## The own page

- **`own_page.mint`**: the provider's create-an-app-by-API recipe, a URL plus a
  manifest payload. `"{permissions}"` in it is replaced by the ticked ids.
  Renders as "create it for me".
- **`own_page.guide`**: a pre-filled creation link plus ordered steps.
  `{permissions}` in a step interpolates the ticked labels. Renders foldable,
  unfolded.
- **`own_page.paste`**: "I already have a credential", pasted directly with no
  app. The fields have to cover every value the auth steps interpolate, which
  is validated at load. Refused on a natively static service.

## Permissions

The catalogue: a provider id, a human label, and one sentence each.

On a provider with hundreds of permissions, list the ones the shipped nodes
use and declare `all_permissions_url` so the picker can point at where the rest
live.

### Own-account-only capabilities

A catalogue entry may declare `own_only: true` plus its own guide.

Reach for it if the capability creates or reads durable things **inside** the
connected account: minted voices, configured agents, phone numbers. A
runtime-supplied credential can never serve one, because the result would land
in the runtime's account, so resolution refuses the shared credential for any
node requiring it and the "your own" page shows that entry's guide.

A capability like this never appears in the tick list and never rides a consent
URL, so it works the same on a pasted-key service.

## Capabilities: when optional fields decide what a connection can do

For a service where filling different optional fields unlocks different
abilities, declare named all-or-nothing groups.

```jsonc
"capabilities": [
  { "label": "receive mail", "fields": ["imap_host", "imap_port"] },
  { "label": "send mail",    "fields": ["smtp_host", "smtp_port"] }
]
```

Two rules, both enforced at connect: each group is filled completely or not at
all, and at least one group must be complete. Consumers then declare which
values **they** need with `requiresValues`, and a shortfall always refuses,
because a value is stored or it is not.

Do not build two access nodes for one account.

## Verification and test

`verification` names the ladder rung and the cost. See
[the ladder](overview.md#the-verification-ladder).

`test` is the declarative connect-time check: a URL, a method, an expected
status, captures. Identity captures usually hang here, and `identity` is a
template over them such as `"{team}"`.

`callback_https` says the provider refuses plain-http OAuth callbacks, as Slack
does. Consents then use an https address, failing loudly when this weft has
none.

## Events

`events` is a map of named topics, because one provider really does report along
independent topologies at once. Slack declares `messages`, `reactions`,
`interactions` and `files`.

Each topic declares:

- **`fields`**: named facts, and where each lives in the push (a dotted body
  path, or `header:X-Some-Header`). Subscriptions filter on the **names** and
  trigger nodes fan them out, so nobody downstream ever writes a provider path.
- **`account`**: which stored value identifies the account on a connection, and
  where the same identifier sits on an incoming event. That pair is what routes
  a push to the right connection.
- **`socket`** (dial out): an authenticated `connect` call answers a single-use
  socket address; weft connects out and holds the line. `replies` declare
  acknowledgement rules as data, and `event_path` and `account_path` say where
  the event and its account sit in a frame. Works from a laptop and needs no
  public address.
- **`webhook`** (dial in): the provider posts to a published address. Declares
  the `handshake` (echo a body field, echo a query param, or visit a
  confirmation URL), `verify`, `route_by` (`account` or `subscription`), an
  optional `decode` for an event wrapped in a queue envelope, and optional
  subscribe and unsubscribe calls with a renewal margin.

Subscribe calls interpolate weft-minted values by reserved name:
`{subscription_id}`, `{subscription_token}`, `{receiver_url}`, and capture
`expires_at` when they renew.

### Verification

`verify` names a **protocol**, parameterized by data. Never a provider.

```jsonc
// An HMAC over a declared concatenation (the Slack shape: a signed
// timestamp, replay-checked).
"verify": { "kind": "hmac",
            "signature_header": "X-Slack-Signature",
            "timestamp_header": "X-Slack-Request-Timestamp",
            "concat": "v0:{timestamp}:{body}",
            "prefix": "v0=" }

// Body-only (the GitHub shape; Shopify is the same with base64 encoding
// and no prefix). A declared timestamp and {timestamp} in the concat come
// and go together: a timestamp the signature does not cover is refused at
// load.
"verify": { "kind": "hmac",
            "signature_header": "X-Hub-Signature-256",
            "concat": "{body}",
            "prefix": "sha256=" }

// Packed header (the Stripe shape): the timestamp and digest ride the
// signature header's own k=v pairs. Any pair under the signature key that
// matches verifies, which covers a rolled secret's overlap.
"verify": { "kind": "hmac",
            "signature_header": "Stripe-Signature",
            "packed": { "timestamp": "t", "signature": "v1" },
            "concat": "{timestamp}.{body}" }

// Address signing (the Twilio shape): SHA1, base64, over the posted address
// plus the form params sorted by name. The concat vocabulary is {body},
// {timestamp}, {url}, {method}, {sorted_form_params}.
"verify": { "kind": "hmac",
            "signature_header": "X-Twilio-Signature",
            "concat": "{url}{sorted_form_params}",
            "algorithm": "sha1",
            "encoding": "base64" }

// A public-key signature (Discord's ed25519; SendGrid's ecdsa_p256 with
// base64 encoding). The public key is configured on the receiving app in
// the apps file, never in the recipe.
"verify": { "kind": "signature",
            "scheme": "ed25519",
            "signature_header": "X-Signature-Ed25519",
            "timestamp_header": "X-Signature-Timestamp",
            "concat": "{timestamp}{body}" }

// The provider echoes the token weft minted at subscribe time,
// per-subscription and unguessable (Google's watch channels).
"verify": { "kind": "token_echo" }

// A signed OIDC identity token, checked against the issuer's published keys.
"verify": { "kind": "oidc",
            "issuers": ["https://accounts.google.com"],
            "jwks_url": "https://www.googleapis.com/oauth2/v3/certs" }
```

The secret material is never in the recipe. An HMAC's signing secret, and a
signature scheme's public key, belong to the registered app that receives.
A `token_echo` compares against what weft minted.

Verification always runs **before** the handshake is answered, and its refusals
are flat: which part failed is logged operator-side, never echoed back.

## Which transport actually serves a trigger

Decided at activation, from what the service declares and what the environment
can do. A trigger that cannot be served fails loudly right there, naming what
is missing.

The user-facing side of that is [Events from a service](events.md).

## The rule to remember

Per-service knowledge lives in declared data. When a mechanism genuinely cannot
be data (a signature algorithm, a browser picker handshake), it becomes a typed
variant every service can reach for.

So the question to ask about a new capability is whether the next service
needing it can express it without touching the engine.
