# The access system

How weft connects to outside services: one mental model, three
objects, and a declaration language that makes a new service a JSON
file instead of a code change.

This is the reference. The short version for node authors is in
[authoring-nodes.md](authoring-nodes.md#connections-the-one-way-a-node-calls-a-third-party);
how events from services wake projects is in
[event-triggers.md](event-triggers.md); how call costs are measured is
in [authoring-provider-meters.md](authoring-provider-meters.md).

## The one idea

A **connection** is an account someone hooked up to an outside
service. It lives in the access store and holds
everything secret. A project's source holds a bare id and nothing
else, so a credential can never end up in git history.

Every kind of credential is the same concept: an OAuth sign-in, a
pasted API key, a GitHub App's private key, a mail server's
host + user + password. One store, one connect screen, one way for a
node to use it.

## The three objects

Three different objects carry the system, and confusing them is the
main way to misread it:

```
 RECIPE (AccessSpec)          REGISTERED APP                 CONNECTION (grant row)
 in the access node's         in the operator's apps file    in the access store (Postgres)
 metadata.json                (WEFT_ACCESS_APPS_FILE)
 written by: the node         written by: the operator       created by: a user connecting;
 author (any user;            running this weft (trusted;    one row per connected account
 untrusted input)             holds real secrets)
──────────────────────────   ────────────────────────────   ───────────────────────────────
 describes the SERVICE,       one OAuth app weft itself      one account, hooked up:
 true for everybody:          signs users in with:            · who (identity)
 · how a credential is        · label ("Google Drive")        · through which app (label)
   acquired                   · client_id + client_secret     · granted permissions, and
 · how a request is signed    · covers: the FIXED               whether they are verified
 · the permission catalogue     permission set it asks for    · the stored values (token,
 · how it is verified         · pinned sign-in addresses        captures, pasted fields)
 · how events arrive          · event-receiving secrets       · snapshots of the recipe and
 · which doors exist            (signing key, audience)         the app it was made with
```

How they link: **by the service name string, and nothing else.** A
recipe saying `"service": "slack"` reaches the apps filed under
`"slack"`. The recipe travels with every connect request; the server
looks the apps up itself. A connection snapshots both at creation, so
using it later needs no catalog and no file lookup.

Because the recipe is user-authored and the apps file is
operator-owned, the rules below all follow one principle: **the recipe
may use a registered app, and can never extract from it.**

- **Pinned addresses.** Each registered app writes its own `auth_url`
  and `token_url` beside its secret. A shared-door connect whose
  recipe names any other address is refused outright, so the app's
  credentials are only ever sent where the operator wrote.
- **Fixed permissions.** A shared-door connect's permission set is
  replaced by the chosen app's `covers` from the trusted file. Nothing
  a client sends widens it.
- **Secrets stay compartmentalized.** App credentials are snapshotted
  in their own field on the connection row; the values handed to
  workers come from a different field that structurally cannot
  contain them.
- **Event material joins own-door connections only.** An app's
  event-serving values (an app-level socket token, a signing secret)
  are merged into event resolution only when the app is the USER'S
  own. A registered app's material serves connecting, never event
  serving, so no connection made through it can hold the app-wide
  event line.
- **The server resolves the app.** A connect request names a door and
  (for the shared door) an app label. It never carries an app; the
  client is not the security boundary.

## The fourth object: the `Access` value

What actually flows through a graph is a small reference:

```json
{ "__weft_access__": { "accessId": "…uuid…", "service": "slack",
                       "identity": "Acme Corp" } }
```

An access node emits it; action nodes consume it. It is not a secret:
resolving it requires being authenticated as the tenant that owns the
row (a missing row and another tenant's row give the same "not found",
so existence never leaks). When the consumer's input bag is built, the
runtime also stamps the input's declared `requiresScopes` /
`requiresValues` onto the marker, so resolution can hold the
connection to what the node needs.

## Life of a connection

```
        the user clicks the access node's field
                        │
                        ▼
          doors probe ("what can this weft offer?")
          one option per registered app (label + its
          fixed covers), plus "your own" when offered
                        │
        ┌───────────────┴───────────────┐
        ▼                               ▼
   SHARED door                      OWN door: one page with up to
   one click; app resolved          three parts, whichever exist:
   by label from the file;          · mint  ("Create it for me")
   permissions = its covers         · guide (generated from ticks)
                        │           · fields (paste; always there)
        └───────────────┬───────────────┘
                        ▼
          the acquisition runs (consent, paste, jwt mint,
          server-to-server exchange), on the broker, whose
          network egress reaches the public internet only
                        │
                        ▼
          verification ladder (see below), identity capture
                        │
                        ▼
          one grant row: values, permissions (+verified flag),
          owner, door, label, identity, snapshots
```

After that the connection appears in the picker for every project of
the tenant; picking it stores only its id on the node.

**Doors.** At most two kinds, and hidden when not offered, never
greyed: `shared` (a credential this weft holds: a registered app for a
consent service, the runtime's own key for a key service, whose calls
spend its credit and say so) and `own` (the user brings or creates
their own). A service whose `auth` contains a `sign` step can never
offer `shared`: the shared lane substitutes the secret in the request
bytes, and a signed request carries a hash computed FROM the secret
instead, so there is nothing to substitute. That combination is a
parse error, not a runtime surprise.

**The verification ladder.** What connect-time checking can learn
about a fresh credential, top rung first:

| rung | what the provider tells us | permissions recorded as |
|---|---|---|
| `reports_permissions` | what it granted, explicitly | **verified** |
| `self_introspect` | the credential describes itself | **verified** |
| `reports_validity` | only "alive", never "what" | claimed |
| `probe` | a real call's refusal, read back | claimed |
| `silent` | nothing knowable | claimed |

`verification.cost` is `free`, `ambiguous`, or `paid`; anything but
`free` means the check is NEVER auto-run (a check that might bill is
one weft never runs on its own; the first real call surfaces the
truth). A later shortfall hard-fails only on a VERIFIED set; a claimed
set passes with a warning, because nobody actually knows, and refusing
would block every pasted key on a service that reports nothing.

**Coexistence.** `grants` on the recipe is a provider property:
`coexisting` (Google-class: one token per consent, grants are
per-project) or `exclusive` (Slack-bot / GitHub-App class:
structurally ONE grant per app + account; re-consent rotates it in
place, reuse inherits it, a permission upgrade unions onto it, and
every referencing project follows).

## Life of a call

```
 node body                     runtime                          store / broker
─────────────────────────────────────────────────────────────────────────────────
 ctx.open(&access) ─────────▶ resolve request ────────────────▶ tenant wall
                              (id, service, requires*)          lazy single-flight
                                                                refresh, drift
                                                                backstop
                              ◀──────────────────────────────── values + auth steps
                                                                + owner (+ relay)
                              build ONE client:
                                base (hygiene)
                              + auth middleware   (always)
                              + metering          (iff a meter is
                                                   registered for
                                                   the service)
                              + relay routing     (iff the resolved
                                                   credential says so)
 conn.client() ◀───────────── the signed-in client
 …the node makes calls…
 body finishes (any way) ───▶ release the lease
```

What the worker receives is everything the connection stores EXCEPT
the store's own keep-alive material (the refresh token); an app secret
is not a stored value and cannot travel at all. Refresh is lazy, at
resolution, single-flight through the row lock; a revoked credential
is a loud "needs reconnecting" error naming the fix, never a silent
retry.

**At rest, every stored secret is sealed** (AES-256-GCM under
`CREDENTIAL_ENCRYPTION_KEY`, base64 of 32 bytes): a grant's values, an
app snapshot, a pending consent's app + PKCE verifier, a
subscription's echo token. A database dump alone carries no usable
credential; only non-secret row data (service names, permission sets,
value NAMES, the public client id) stays plain so queries work on it.
Unset, a built-in development key is used with a logged warning; set a
real key (`openssl rand -base64 32`) before storing credentials you
care about, and know that changing it strands every sealed row (the
fix is reconnecting each connection).

**Measured vs billed are separate switches.** A registered meter for
the service = its calls are measured, whoever pays. `owner` on the
connection row = whose money: the user's own credential is measured
and not billed; the runtime's is measured and billed. A node declares
neither, and its code cannot tell which combination it is running in.

**Sessions (WebSockets) follow the same shape.** `conn.socket(url)`
opens the service's realtime API with the credential on the handshake
only (never inside a frame). A meter may classify a route as a
SESSION: cost then accrues from the frames both directions (an audio
second sent, a token streamed back), observed live by the meter's
session observation. On the user's own credential the accrued figure
is measured and recorded when the session ends. On the runtime's
credential the session is admitted in pre-carved SLICES (e.g. one
minute of audio worth): a slice is reserved up front, another whenever
the accrued cost approaches the reserved total, and when no more can
be reserved the session is closed with the reason in the close frame
and settled at exactly what accrued.

**The node-facing surface**, in full:

```rust
let conn = ctx.open(&access).await?;      // one resolve, one lease per firing
conn.client()          // &ClientWithMiddleware: signed in, measured when a meter exists
conn.credential()?     // &str: ONLY when the sign-in is one step over one value
conn.value("imap_host")?    // one stored value by name (loud when absent)
conn.opt_value("alias")     // Option<&str> for genuinely optional values
conn.identity()        // display identity, if recorded
ctx.client(&access)    // sugar: open + clone the client (accepts None → plain client)
ctx.open_within(&access, window)   // a longer lease for genuinely long work
```

`credential()` is DERIVED, never declared: it exists exactly when the
resolved auth is one step whose template interpolates one stored value
(a bearer key, a bot token). A Basic pair, a signing step, or two
steps have no single string that means anything, and the call fails
loudly naming the service.

## The recipe language (`AccessSpec`)

The whole of a service is the `service` block in its access node's
`metadata.json`. There is never per-service Rust: the closed parts of
the vocabulary are protocols (an HMAC, SigV4, OIDC), parameterized by
data, shared by every service.

### Acquisition: how a credential is obtained

| kind | what happens |
|---|---|
| `static` | the user pastes the declared `fields` |
| `oauth2` + `authorization_code` | browser consent → code → token; PKCE by default |
| `oauth2` + `client_credentials` | server-to-server token from the app's credentials, re-requested on expiry |
| `mint_jwt` | paste a private key; weft mints a short-lived JWT and optionally exchanges it for a working token, lazily (GitHub App, service accounts) |

`captures` pull values off token/test responses by dotted JSON path
and become stored values templates can interpolate.

An `oauth2` acquisition may declare `token_auth` for how every token
call (exchange, refresh, client credentials) authenticates the CLIENT:
`body` (default) sends client_id + client_secret as form fields;
`basic` sends them as HTTP Basic, for providers that ignore body
credentials (Notion, Airtable).

An `oauth2` acquisition may also declare `refresh`, its own renewal
call, for providers whose renewal is not the standard refresh-token
POST (Meta's long-lived-token exchange is a GET interpolating the
current token). It is a declared call like a socket mint or a
subscribe (url template, method, optional body and auth, captures);
it runs at refresh time over the stored values plus the app's, its
captures write back (capture the fresh credential under the name the
auth steps interpolate), and a top-level `expires_in` in the answer
sets the next expiry. On a registered app the renewal may only
address the app's pinned sign-in origin, checked at every shared
connect. A test capture
named `granted_permissions` records the credential's introspected
permission set as VERIFIED on a `self_introspect` service. A
`runtime` acquisition also exists but is never authored: it is the
stored form the shared door of a key service writes (nothing stored;
the runtime's credential source answers per call).

### Auth steps: how a request is signed

Applied by the client in the worker, in declaration order (signing
steps run last so the signature covers the final request). Templates
(`"Bearer {token}"`) interpolate stored values by name; an unknown
name is a loud error, never an empty substitution.

| kind | example |
|---|---|
| `header` | `Authorization: Bearer {token}`, any header |
| `query` | `?key={token}` |
| `basic` | Stripe `key:`, Twilio `sid:token` |
| `path_prefix` | Telegram's `/bot{token}/` |
| `base_url` | per-account API bases: an S3 endpoint, a captured `instance_url` |
| `sign` | per-request cryptographic signing: `sigv4` (S3-compatible stores), `oauth1a` (X/Twitter) |

A service whose credential is not a request transform at all (a mail
server's settings) declares NO auth steps; consumers read the values
by name (`conn.value("imap_host")`) instead.

### The own page, permissions, verification

- `doors`: `["shared", "own"]` or a subset; defaults to `["own"]`.
- `own_page.mint`: the provider's create-an-app-by-API recipe (URL +
  manifest payload; `"{permissions}"` in it is replaced by the ticked
  ids). Renders "Create it for me".
- `own_page.guide`: a pre-filled creation `link` plus ordered `steps`;
  `{permissions}` in a step interpolates the ticked labels. Renders
  foldable, unfolded.
- `own_page.paste`: "I already have a credential": paste it directly,
  no app. Stores a connection whose snapshot is a `static` acquisition
  over these fields, so they must cover every value the auth steps
  interpolate (validated at load). Redundant (and refused) on a
  natively static service.
- `permissions`: the catalogue: provider id + human label + one
  sentence each. Deliberately not exhaustive on huge-scope providers:
  list what shipped nodes use, declare `all_permissions_url` so the
  picker can say where the rest live.
- A catalogue entry may declare `own_only: true` plus its own `guide`
  (same shape as `own_page.guide`): an OWN-ACCOUNT-ONLY capability.
  Use it when the capability creates or reads durable things INSIDE
  the connected account (minted voices, configured agents, phone
  numbers): a runtime-supplied credential can never serve it (the
  result would land in the runtime's account), so resolution refuses
  the shared credential for any node requiring it, the editor marks
  the consumer node live, and the "Your own" page shows the entry's
  guide as its own foldable set-up section. `own_only` entries are
  capability declarations, not consent asks: they never appear in the
  tick list and never ride a consent URL, so they work equally on a
  static (pasted-key) service, where nodes name them via
  `requiresScopes` like any permission.
- `verification`: the ladder rung + cost, above.
- `test`: the declarative connect-time check (URL, method, expected
  status, captures). Also where identity captures usually hang;
  `identity` is a template over captured values (`"{team}"`).
- `callback_https`: the provider refuses plain-http OAuth callbacks
  (Slack does; Google takes a loopback). Consents then use an https
  address, failing loudly when this weft has none.

### Capabilities: when optional fields decide what a connection can do

For a service where filling different optional fields unlocks
different abilities (a mailbox: the incoming server reads, the
outgoing one sends), declare named all-or-nothing groups:

```jsonc
"capabilities": [
  { "label": "receive mail", "fields": ["imap_host", "imap_port"] },
  { "label": "send mail",    "fields": ["smtp_host", "smtp_port"] }
]
```

Exactly two rules, both enforced at connect: each group is filled
completely or not at all, and at least one group must be complete.
Consumers then declare which values THEY need with `requiresValues`
on their access input; a shortfall always refuses (a value is stored
or it is not; there is no unknown case). Do not build two access
nodes for one account.

### Events: how the service reports

`events` is a map of named topics, because one provider genuinely
reports along independent topologies at once. Each topic declares:

- `fields`: named facts → where each lives in the push (a dotted body
  path, or `header:X-Some-Header`). Subscriptions filter on the NAMES
  and trigger nodes fan them out; nobody downstream writes a provider
  path.
- `account`: which stored value identifies the account on a
  connection, and where the same identifier sits on an incoming
  event. The pair is what routes a push to the right connection.
- `socket` (dial-out): an authenticated `connect` call answers a
  single-use socket address; weft connects out and holds the line.
  `replies` declare acknowledgement rules as data; `event_path` /
  `account_path` say where the event and its account sit in a frame.
  Works from a laptop; needs no public address.
- `webhook` (dial-in): the provider posts to a published address.
  Declares the `handshake` (echo a body field, echo a query param, or
  visit a confirmation URL), `verify` (below), `route_by` (`account`
  or `subscription`), an optional `decode` (an event base64-wrapped in
  a queue envelope), and optional `subscribe`/`unsubscribe` calls with
  a renewal margin for providers that must be told where to send.
  Subscribe calls interpolate weft-minted values by reserved name:
  `{subscription_id}`, `{subscription_token}`, `{receiver_url}`, and
  capture `expires_at` when they renew.

`verify` names a protocol, parameterized by data (never a provider):

```jsonc
// An HMAC over a declared concatenation (Slack shape: a signed
// timestamp, replay-checked)
"verify": { "kind": "hmac",
            "signature_header": "X-Slack-Signature",
            "timestamp_header": "X-Slack-Request-Timestamp",
            "concat": "v0:{timestamp}:{body}",
            "prefix": "v0=" }

// the body-only flavor (GitHub shape; Shopify is the same with
// "encoding": "base64" and no prefix). A declared timestamp and
// {timestamp} in the concat come and go together: a timestamp the
// signature does not cover is refused at load.
"verify": { "kind": "hmac",
            "signature_header": "X-Hub-Signature-256",
            "concat": "{body}",
            "prefix": "sha256=" }

// the packed-header flavor (Stripe shape): the timestamp and the
// digest ride the signature header's own k=v pairs; any pair under
// the signature key matching verifies (a rolled secret's overlap)
"verify": { "kind": "hmac",
            "signature_header": "Stripe-Signature",
            "packed": { "timestamp": "t", "signature": "v1" },
            "concat": "{timestamp}.{body}" }

// the address-signing flavor (Twilio shape): SHA1, base64, over the
// posted address plus the form params sorted by name. The concat
// vocabulary is {body}, {timestamp}, {url}, {method},
// {sorted_form_params}.
"verify": { "kind": "hmac",
            "signature_header": "X-Twilio-Signature",
            "concat": "{url}{sorted_form_params}",
            "algorithm": "sha1",
            "encoding": "base64" }

// a public-key signature (Discord's ed25519; SendGrid's ecdsa_p256
// with "encoding": "base64"). The public key is configured on the
// receiving app in the apps file (`events.public_key`), never in
// the recipe.
"verify": { "kind": "signature",
            "scheme": "ed25519",
            "signature_header": "X-Signature-Ed25519",
            "timestamp_header": "X-Signature-Timestamp",
            "concat": "{timestamp}{body}" }

// the provider echoes the token weft minted at subscribe time
// (per-subscription and unguessable; Google's watch channels)
"verify": { "kind": "token_echo" }

// a signed OIDC identity token in the Authorization header,
// checked against the issuer's published keys (queue relays)
"verify": { "kind": "oidc",
            "issuers": ["https://accounts.google.com"],
            "jwks_url": "https://www.googleapis.com/oauth2/v3/certs" }
```

The secret material is never in the recipe: an HMAC's signing secret
(and a signature scheme's public key) belongs to the registered app
that receives (its `events` block in the apps file); a `token_echo`
compares against what weft minted.
Verification always runs BEFORE the handshake is answered, and its
refusals are flat (which part failed is logged operator-side, never
echoed).

Which transport serves a given trigger is decided at activation from
what the service declares and what the environment can do, and a
trigger that cannot be served fails loudly right there naming what is
missing. The user-facing story is
[event-triggers.md](event-triggers.md).

## The shared-credentials file

`WEFT_ACCESS_APPS_FILE` names one JSON file (start from
`access-apps.example.json`): every credential the runtime offers as a
SHARED connection, whatever its shape. Keyed by service; each service
holds a LIST of entries, always a list, even for one, and every entry
declares its `kind`:

```jsonc
{
  "_readme": "keys starting with _ are comments and are ignored",
  "google": [
    {
      "kind": "oauth_app",
      "_setup": "console.cloud.google.com → credentials → add the callback URL",
      "label": "Google Drive",                       // mandatory, unique per service
      "covers": [                                     // mandatory: exactly what it asks for
        "https://www.googleapis.com/auth/drive.file"
      ],
      "auth_url": "https://accounts.google.com/o/oauth2/v2/auth",   // pinned
      "token_url": "https://oauth2.googleapis.com/token",           // pinned
      "client_id": "…",
      "client_secret": "…",
      "events": {                                     // only if this app RECEIVES pushes
        "signing_secret": "…"
      }
    }
  ],
  "openrouter": [
    { "kind": "api_key", "label": "Runtime key", "key": "sk-or-…" }
  ]
}
```

An `api_key` entry is the runtime's own key for a pasted-key service:
what the "use the runtime's key" door hands the credential source. At
most one per service. How the key is handed out (directly to the
worker, or swapped for a stand-in behind a relay) is decided by the
deployment's credential source, deliberately never by a field in this
file: a trust decision must not be one typo away from a leak.

Rules, all checked loudly (a typo must never silently remove the
one-click door):

- every entry is a list; every entry declares its `kind` and has a
  non-empty `label`, unique within its service;
- `covers` is mandatory; empty means "may ask for nothing", never
  "everything"; every entry must exist in the service's permission
  catalogue (checked at the door probe and at every shared connect);
- an `events` block on a service whose recipe declares no webhook
  transport is refused (configured receiving for pushes that can
  never arrive);
- the file is read per lookup, so edits take effect without a
  restart; a missing file means "no apps" (shared doors hidden), a
  malformed file is a loud error on every lookup.

Each registered app is its own one-click option in the connect panel,
labelled, with its `covers` shown as a fixed set. Several apps per
service is how a cheap permission tier avoids a provider's review
process, and how one service gets product-shaped options ("Google
Drive", "Google Calendar") while staying one account.

A project may also ship a PUBLIC (PKCE, secretless) app under
`accessApps` in its metadata, inherited from the package root; an
entry carrying a `client_secret` fails the metadata load, because
project metadata is source and source never holds secrets.

## Resource fields: `remote_select`

A connection says WHICH ACCOUNT; most nodes then need WHICH THING in
it. One field, a declared list of sources in preference order; the
editor uses the richest one the picked connection supports and
silently drops each source whose requirement is not met:

| kind | options come from | needs |
|---|---|---|
| `granted` | recorded on the connection at sign-in | nothing, no call |
| `list` | a declarative authenticated lookup | its `requires` permissions |
| `picker` | the provider's own chooser, declared by the node author as a `script` URL + browser-JS `code` glue, run on a weft-served page with `weft.token` / `weft.done` / `weft.cancel` / `weft.fail` in scope | a connection |
| `from_url` | paste a link; the pattern's first capture group is the id | nothing at all |

`from_url` needing nothing is what leaves it standing with no
connection at all: the works-with-a-share-link path. The `picker` is
the node's own end to end (adapt the provider's embed sample, no weft
change ever needed); the full glue contract is in
[authoring-nodes.md](authoring-nodes.md#picking-resources-the-remote_select-widget),
and `catalog/google/sheets_read` is the worked example declaring
`list` + `picker` + `from_url` on one field.

A picked resource is as person-scoped as the connection itself: both
are re-chosen when a project changes hands, and an unresolvable one is
a loud node error, never a silent pointer.

## Where things run

- **Editor** (the graph webview): renders the picker and the doors,
  sends pasted values straight to the store, runs the live
  `requiresScopes` / `requiresValues` check when a connection is
  picked. Never holds an app, never decides permissions.
- **Dispatcher**: the authenticated front door; forwards connect verbs,
  serves the OAuth callback and the picker page, answers pure-DB reads
  (the grant list).
- **Broker**: every verb whose work makes an OUTBOUND call to a URL
  the tenant influences (test calls, token exchanges, lookups, app
  mints, event subscribe calls) runs here, and its network egress
  reaches the public internet only, so a crafted URL pointing at
  internal addresses dies at the network layer. Also resolves
  registered apps and serves worker/listener resolution.
- **Access store** (Postgres): the rows, the tenant wall, the lazy
  refresh, the permission/value backstops.
- **Worker**: opens connections per firing, builds the one signed-in
  (and possibly measured) client, runs node code.
- **Listener**: holds event sources (sockets, polls, streams),
  resolving the connection freshly through the broker on every
  (re)connect so credentials are never frozen into a loop.
