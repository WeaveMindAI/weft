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
| `test` | none | A call made at connect time to check it works, and where identity usually comes from |
| `identity` | none | The display name, assembled from stored values, like `"{team} / {user}"` |
| `permissions` | `[]` | The permission catalogue people tick from |
| `all_permissions_url` | none | Where the provider's full list is, when yours is a subset |
| `verification` | silent, free | What the check can learn and what it costs |
| `connection_optional` | `false` | The node runs with nothing picked |
| `callback_https` | `false` | The provider refuses plain http callbacks |
| `own_page` | none | What the "your own" door shows beyond its paste fields |
| `events` | `{}` | How this service reports events, by topic |
| `verify` | none | How a caller presenting this connection is checked, on a gated route |
| `capabilities` | `[]` | Named groups of optional fields, each unlocking one thing |
| `grants` | `coexisting` | Whether two grants of this service can coexist |

## Acquisition

| `kind` | For | Keys |
|---|---|---|
| `static` | Paste a key | `fields` |
| `oauth2` | Sign in | `grant`, `token_url`, `scope_delimiter`, `auth_params`, `extra_params`, `registration_fields`, `captures`, `token_auth`, `refresh` |
| `mint_jwt` | Sign your own assertion | `fields`, `algorithm`, `claims`, `jwt_ttl_secs`, `exchange` |

The `grant` is `authorization_code` with an `auth_url` and PKCE on by default,
or `client_credentials`.

There is a fourth, `runtime`, and **you never write it.** It is the stored form
of a shared connect, written by the store, and declaring one is refused.

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
`"Bearer {token}"`.

## Permissions

```json
{ "id": "chat:write", "label": "Send messages",
  "description": "Post messages as the app.", "default": true }
```

A curated catalogue, not the provider's whole list. On a provider with hundreds
of scopes, list the ones your nodes actually need and point
`all_permissions_url` at the rest.

`default: true` starts it ticked. `own_only: true` means the runtime's own
credential can never serve it, which greys out the shared option for anyone who
ticks it.

## Verification

Say honestly what your check can learn:

| `rung` | Meaning |
|---|---|
| `reports_permissions` | The provider states what it granted |
| `self_introspect` | The credential can describe itself |
| `reports_validity` | Only "alive", never "what" |
| `silent` | Nothing knowable |

Only the first two make the recorded permissions authoritative, and only those
let weft block a node before it calls.

`cost` is `free`, `ambiguous` or `paid`, and **only `free` runs on its own**. A
check that might bill is never run without being asked.

Today every service in the shipped catalog declares `free`, and nine of the
thirteen declare `reports_validity`. So in practice the only way a connection
is genuinely verified right now is the OAuth scope echo, where the provider
tells weft what it granted. A `probe` rung exists in the type and nothing
implements it.

## Your own door

`own_page` is what that door shows:

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
