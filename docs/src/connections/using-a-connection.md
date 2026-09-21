# Using a connection in a node

If your node needs to call a service, take an `Access` input, open it, and use
the connection's client. Authentication is already set up. Add
`use weft::{Access, NodeErrExt};` at module scope, then put this fragment in
the node's `run` method:

```rust
let account: Access = ctx.inputs.get("account")?;
let conn = ctx.open(&account).await?;
let response = conn.client()
    .get(url)
    .send()
    .await
    .node_err("calling the service")?;
```

Here `url` is the service API address your node calls. If you need an account
to connect in the editor first, follow [Connect an account](connect-an-account.md).

## Make calls through the connection

The connection's client authenticates requests and applies any registered
meter that recognizes them. A client you build yourself bypasses that work.
Call the service's normal API address; the connection handles routing for a
runtime-supplied credential.

If you only need the client, use:

```rust
let client = ctx.client(&account).await?;
```

When a node refuses a runtime credential, that refusal comes back through this
call so the user sees why they must connect their own.

### Realtime connections

Use `socket` for a provider's WebSocket API. In this fragment, `url`
and `payload` follow that provider's protocol:

```rust
let mut session = conn.socket(&url).await?;
session.send(weft::access::socket::SocketMessage::Text(payload)).await?;
while let Some(frame) = session.recv().await? {
    // Handle the provider's response.
}
session.close().await?;
```

The connection signs the handshake and handles routing. A registered
session meter can measure the exchange. Your node still implements the
provider's message protocol.

Use a trusted provider URL. What happens next depends on whose credential
signs the socket. With the user's own credential, direct sockets attach
authentication even outside the meter's declared API base. With a
runtime-owned credential, the same allowlist that guards HTTP calls applies,
so the route must be classified by the meter or the dial is refused.
For that distinction, read [Runtime-owned credentials](meters.md#the-runtime-credential-allowlist).

### Longer calls

For a runtime-supplied credential, the default lease window is 15 minutes.
The runtime releases the lease when the node finishes; the window bounds
its lifetime if the worker crashes before cleanup.

Declare a longer window when the provider's work needs one:

```rust
let conn = ctx.open_within(
    &account,
    std::time::Duration::from_secs(3600),
).await?;
```

This window does not change the lifetime of the user's own credentials.

## Read the stored values

The connection also hands you what the user stored when they connected. If
your node reads mail, it can pick up the incoming server settings from here.

| Method | Returns |
|---|---|
| `client()` | The authenticated HTTP client |
| `value("imap_host")?` | A stored value, or an error naming the missing field |
| `opt_value("alias")` | An optional stored value |
| `identity()` | The recorded display identity, if present |
| `credential()?` | A single credential string, when the authentication recipe has one |

`credential()` works when authentication consists of one step using one
stored value, such as a bearer token. Basic authentication with a username
and password has no single credential string, so this call fails.

Reach for the raw credential only when an SDK demands it. If the SDK accepts
an HTTP client too, give it the connection's client so its calls keep the
authentication and measurement behavior.

Keep the values out of outputs, logs, and error messages. Whatever lands in
config or a port value is recorded in the journal and shown in the inspector,
and a password placed there becomes part of that record. That is also why
credentials live in an access node's store rather than in your node: the
store gives the graph a connection reference, not a secret. For a new
service, follow [Declaring a service](writing-a-service.md).

## Declare the requirements on the input

Say your node reads mail and needs the connection's incoming server settings.
Declare them on the input:

```json
{
  "name": "account",
  "type": "Access",
  "required": true,
  "requiresValues": ["imap_host", "imap_port"]
}
```

`requiresValues` checks that the named values exist. A missing value
refuses resolution and tells the user what to add.

Use `requiresScopes` for permissions needed by the node's own calls:

```json
{
  "name": "account",
  "type": "Access",
  "required": true,
  "requiresScopes": ["chat:write"]
}
```

A verified permission shortfall refuses the connection. Claimed or unknown
permissions are allowed through, because weft cannot establish that they
are missing; the provider can still refuse the request.

A permission needed only to browse a resource list belongs on that picker
source's `requires` field. Putting it on the input would also block
people who can supply the resource through another source.

A service can declare capabilities that require the user's own account.
Nodes request those through `requiresScopes` too. For declaring them,
read [Own-account-only capabilities](writing-a-service.md#own-account-only-capabilities).

These checks run against the stored connection record when the connection
resolves, not against the provider's live state: weft cannot see permissions
the provider granted after the record was written.

## Allow an absent connection where the API supports it

If the provider serves the resource without authentication, an optional
access input lets users reach it without connecting an account.
Declare `"required": false`, then read it as optional:

```rust
let account: Option<Access> = ctx.inputs.opt("account")?;
let client = ctx.client(account.as_ref()).await?;
```

With no account, this returns a plain client. Check which API address
supports anonymous access: a provider may use a different route for public
resources, or refuse private resources on that route even with credentials.

## Pick a resource inside the account

A connection selects an account. A `remote_select` field selects a thing
inside it, such as a spreadsheet or channel.

Declare the ways users can supply that resource. The editor can combine
supported sources, such as a list, a provider chooser, and a pasted link:

| Source | How it fills the field |
|---|---|
| `granted` | Resources recorded during sign-in |
| `list` | A provider API request, with its own required permissions |
| `picker` | The provider's browser chooser |
| `from_url` | The first capture group in a pasted URL |

This field accepts a spreadsheet link without making an API request:

```json
{
  "name": "spreadsheet",
  "type": "String",
  "required": true,
  "widget": {
    "kind": "remote_select",
    "access": "account",
    "sources": [{
      "kind": "from_url",
      "pattern": "/spreadsheets/d/([a-zA-Z0-9_-]+)"
    }]
  }
}
```

The node also needs an `account` access input. Make that input optional
if the node supports using the resource without signing in.

The editor stores the selection as `{"id": "...", "label": "..."}` in
the source. The runtime unwraps it, so your node receives the ID as a
`String`. Set the widget's `free_text` to `true` if users should also
be able to type values the sources did not list.

A value wired into the input bypasses the picker.

### Add a provider's browser chooser

A `picker` source carries browser JavaScript in its metadata.
Its `script` URL loads the provider's chooser library, then `code`
runs inside an async function on a page served by weft.

| Source field | Purpose |
|---|---|
| `script` | HTTPS URL of the chooser library |
| `code` | JavaScript that opens it and handles the result |
| `grants` | Permissions the choice grants on the selected resource |
| `mime_types` | Resource types passed to the chooser code |

The code receives a `weft` object:

| Property or method | Purpose |
|---|---|
| `weft.token` | Connection token to pass to the chooser |
| `weft.clientId` | Public app client ID, or `null` |
| `weft.mimeTypes` | The declared resource types |
| `weft.done({id, label})` | Return the selection |
| `weft.cancel()` | Report cancellation; the user can then close the tab |
| `weft.fail(message)` | Show an error on the field |

Finish with one of `done`, `cancel`, or `fail`. The first call wins.
An exception from the supplied code or a rejected `await` reports a failure
too. Catch errors inside later provider callbacks and pass them to
`weft.fail(message)`.

Start from the provider's current embed example and connect its callbacks
to these methods. For a complete metadata example, read
[Google Sheets Read](https://github.com/WeaveMindAI/weft/blob/mvp/catalog/google/sheets_read/metadata.json).

## Publish a connection to a service your node runs

An infrastructure node can publish credentials for its own database or
bridge. Downstream nodes receive an ordinary `Access` value.

Declare the service and output in metadata:

```json
"publishes": "postgres",
"outputs": [{ "name": "access", "type": "Access" }]
```

Then publish the service's declared fields from `run`:

```rust
let access = ctx.publish_access(values).await?;
ctx.pulse_downstream(weft::NodeOutput::new().set("access", access)).await
```

Here `values` is a `BTreeMap<String, String>` containing the service's
connection fields. Missing required fields and undeclared names are
rejected.

The compiler finds the service recipe in the catalog and attaches it to
the built project. A `publishes` name with no matching service fails
the build.

Publishing again updates the node's existing connection. Infrastructure
termination removes it as part of cleanup. A published connection uses the
node's supplied values; it does not grant access to a runtime credential.

### Preserve a secret before retiring it

The [Postgres database node](https://github.com/WeaveMindAI/weft/blob/mvp/catalog/postgres/database/mod.rs)
gets its password from the service on first use, publishes it, then tells
the service to stop exposing it. Later runs use `ctx.published_access()`
to retrieve the connection and confirm that its password still matches
the database. A replaced disk may have generated a new one.

Those operations are safe to repeat. If the worker dies after publishing
but before retiring the password, the next run finishes that job.

This example does not use `ctx.run` to record either branch.
A lookup such as `published_access()` reads mutable state and can answer
differently after a restart. If you add recorded steps around such a
decision, follow [Keep the replayed path stable](../nodes/durable-execution.md#keep-the-replayed-path-stable).
