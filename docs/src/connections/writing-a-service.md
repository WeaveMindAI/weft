# Declaring a service

Put a service's connection recipe in its access node's `metadata.json`.
The recipe tells weft how to collect credentials and authenticate requests.
Your processing nodes use that connection through the framework.

## A complete access node

This example collects an API key and sends it as an `x-api-key` header.
It follows the shape used by the catalog's Exa connection:

```json
{
  "type": "ExampleAccess",
  "label": "Example access",
  "description": "Connect an account for the example service.",
  "service": {
    "service": "example",
    "label": "Example",
    "doors": ["own"],
    "acquisition": {
      "kind": "static",
      "fields": [{ "name": "key", "label": "API key" }]
    },
    "auth": [{
      "kind": "header",
      "name": "x-api-key",
      "value": "{key}"
    }]
  },
  "inputs": [{
    "name": "account",
    "type": "Access",
    "label": "Account",
    "widget": { "kind": "access" }
  }],
  "outputs": [{ "name": "access", "type": "Access" }]
}
```

Its `mod.rs` contains:

```rust
weft::access_node!(ExampleAccessNode);
```

The macro reads the selected connection from `account` and emits
`access`. Nodes consuming that output can open it and call the service.
For that Rust API, read [Using a connection in a node](using-a-connection.md).

Replace the example names and authentication details with your service's.
Add a verification request if the provider offers a suitable check.
For a catalog implementation with a setup guide and verification, read
[Exa Access](https://github.com/WeavemindAI/weft/blob/mvp/catalog/web/exa_access/metadata.json).

### When the connection is optional

Declaring a service normally makes a connection mandatory. An unconnected
node stays expanded in the editor, and running it reports the missing
connection.

If the node also supports an unauthenticated endpoint, set
`"connection_optional": true` in the service block. Write its body
yourself and read the selection with
`ctx.inputs.access("connection")?`, using your input's name.
It returns `None` when nothing is selected.

The `access_node!` macro requires an `account` input and a connection
to pass through. It cannot implement this optional case.

## Acquire the credentials

| Acquisition | Use it when |
|---|---|
| `static` | The user supplies the declared fields |
| `oauth2` with `authorization_code` | The user grants access in a browser; PKCE is enabled by default |
| `oauth2` with `client_credentials` | The app exchanges its own credentials for a token |
| `mint_jwt` | weft must sign a short-lived JWT, optionally exchanging it for another token |

A `captures` entry reads a dotted JSON path from a token or test
response and stores the result. Authentication templates can then use it.

For OAuth, `token_auth` defaults to `body`: client credentials go in
the token request's form fields. Set it to `basic` when the provider
expects HTTP Basic authentication.

A provider with a nonstandard renewal API can declare a `refresh` call.
Its captures update the stored values, and the response's top-level
`expires_in` sets the next expiry. For a registered app, the call must
stay on the app's pinned sign-in origin.

For the full recipe types and validation rules, read
[access/spec.rs](https://github.com/WeavemindAI/weft/blob/mvp/crates/weft-core/src/access/spec.rs).

## Authenticate requests

The `auth` array declares request transformations. Templates substitute
stored values by name; an unknown name returns an error.

| Step | What it changes |
|---|---|
| `header` | A request header, such as `Authorization: Bearer {token}` |
| `query` | A query parameter |
| `basic` | HTTP Basic credentials |
| `path_prefix` | A prefix containing credentials or account information |
| `base_url` | The API base for this connection |
| `sign` | A cryptographic signature using `sigv4` or `oauth1a` |

Steps run in declaration order, with signing steps last so they cover the
completed request.

A database connection can declare no HTTP authentication steps.
Its consumers read the stored host and credentials and pass them to the
database client. The password still belongs in the connection store,
where it does not become a config value in the execution journal.

## Offer setup for the user's own account

The `own_page` can contain:

- `mint`: an API call that creates an app for the user. Its manifest
  payload can include `"{permissions}"`, replaced by the selected
  permission IDs.
- `guide`: a creation link and instructions. In its steps,
  `{permissions}` expands to the selected permission labels.
- `paste`: fields for an existing credential, as an alternative to the
  normal acquisition flow. This is redundant and rejected when the
  service already uses `static` or `mint_jwt`, which already collect fields.

A `shared` option uses an operator-configured app or credential. For the
configuration it requires, read [The apps file](the-apps-file.md).

## Describe permissions

Each permission has a provider ID and a human-readable label and
description. If the provider has a large catalog, describe the permissions
your nodes use and set `all_permissions_url` to its full reference.

Consumers declare their requirements on the `Access` input.
For how those checks treat verified and unknown permissions, read
[What weft can verify](overview.md#what-weft-can-check-about-a-credential).

### Own-account-only capabilities

Some calls create lasting resources in the connected account: a saved
voice or a configured agent, for example. Using a shared runtime credential
would put those resources in the operator's account.

Mark the corresponding permission entry `own_only: true` and give it a
setup guide. A node requiring that capability refuses a runtime-owned
credential and directs the user to connect their own.

These capabilities are not OAuth scopes. They do not appear in the consent
permission list or get added to a consent URL.

## Capabilities: when optional fields decide what a connection can do

One mailbox connection can contain incoming settings, outgoing settings,
or both. Group the optional fields that must be supplied together:

```json
"capabilities": [
  { "label": "receive mail", "fields": ["imap_host", "imap_port"] },
  { "label": "send mail", "fields": ["smtp_host", "smtp_port"] }
]
```

Each listed field must be declared optional in the acquisition.
At connect time, every group must be either complete or empty, and at
least one group must be complete. A partially filled group reports the
missing fields.

Consumers use `requiresValues` to request the settings their work needs.
The user can keep both halves of the mailbox in one connection.

## Verify the connection

A `test` declares a request, its expected status, and any response
captures. An `identity` template can use those captures to display the
connected account's name.

The `verification` block says what the check establishes and whether it
can cost money. Only a check declared `free` runs automatically.
With `self_introspect`, a test capture named `granted_permissions` records
the reported permissions as verified for `static`, `mint_jwt`, and
client-credentials acquisition. Browser-consent OAuth uses the token
response's `scope` field; its test captures do not update the recorded
permissions.

Use `callback_https` when the provider requires an HTTPS OAuth callback.
If the installation has no HTTPS public address, connecting fails. Follow
[A public address](public-address.md) to configure one.

## Declare events

The `events` map describes the service's topics. Each topic defines
named fields extracted from an incoming event and an `account` mapping
that matches the event to a stored connection.

A topic can declare a `socket` transport that connects out, or a
`webhook` transport that receives requests at a public address.

For a socket, the recipe describes the connection request, event and
account paths in frames, and acknowledgement replies. For a webhook, it
describes verification and any handshake, subscription, renewal, or
unsubscribe calls.

Subscription requests can use these values supplied by weft:
`{subscription_id}`, `{subscription_token}`, and `{receiver_url}`.
For subscriptions that expire, make the `subscribe` call capture
`expires_at` and set `renew_margin_secs` to how many seconds before expiry
weft should subscribe again.

### Verify incoming webhooks

Choose the verification mechanism the provider implements:

| `verify.kind` | What is checked |
|---|---|
| `hmac` | A signature over the declared combination of request fields |
| `signature` | An `ed25519` or `ecdsa_p256` signature |
| `token_echo` | The token weft generated for the subscription |
| `oidc` | An identity token against the issuer's published keys |

For example, this HMAC declaration signs both a timestamp and the body:

```json
"verify": {
  "kind": "hmac",
  "signature_header": "X-Slack-Signature",
  "timestamp_header": "X-Slack-Request-Timestamp",
  "concat": "v0:{timestamp}:{body}",
  "prefix": "v0="
}
```

The timestamp must be covered by the signature. Declaring an unsigned
timestamp is rejected when the recipe loads.

An HMAC signing secret or signature public key belongs to the receiving
app's configuration. Keep that material out of the recipe.
Verification runs before the handshake response. Failure details go to
the operator's logs, while the caller receives a generic refusal.

For all supported fields, including packed signatures and queue envelopes,
read [access/events.rs](https://github.com/WeavemindAI/weft/blob/mvp/crates/weft-core/src/access/events.rs).
For activating a trigger and choosing an available transport, follow
[Events from a service](events.md).

Authentication and event protocols belong to the framework. If a new
service needs a mechanism the recipe cannot express, extend that shared
protocol vocabulary. For the design behind this, read
[Design principles](../thinking/design-principles.md).
