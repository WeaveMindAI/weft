# Using a connection in a node

Declare an `Access` input with the connect widget:

```json
"inputs": [
  { "name": "account", "type": "Access", "required": true,
    "widget": { "kind": "access" },
    "requiresScopes": ["chat:write"] }
]
```

Write just `{"kind": "access"}`. The service and whether it is optional are
filled in from your node's `service` recipe.

Then open it:

```rust
let account: Access = ctx.inputs.get("account")?;
let client = ctx.open(&account).await?.client();

let reply = client.post("https://slack.com/api/chat.postMessage")
    .json(&body).send().await.node_err("posting the message")?;
```

That client already has the authentication, the routing and the metering on it.

## The one rule about HTTP

A call **on a connection** goes through `ctx.open` or `ctx.client`. Everything
else goes through `ctx.http()`.

A `reqwest` client you build yourself skips both the signing and the cost
recording, and the call will either fail unauthenticated or succeed while
nobody writes down what it cost.

| Call | Gives you |
|---|---|
| `ctx.open(&access).await?` | The connection, leased for this firing |
| `ctx.client(access).await?` | Straight to the signed-in client. Takes `None` and gives a plain one |
| `ctx.http()` | The shared plain client, no credentials |

`ctx.client` accepting `None` is for a node whose connection is genuinely
optional, like fetching a link that is usually public.

## What an opened connection knows

| Call | Gives you |
|---|---|
| `client()` | The HTTP client, authenticated and metered |
| `credential()` | The single credential, for a service that has one |
| `value(name)` | One stored value, like `imap_host` |
| `identity()` | Who it is, for a log line |
| `owner()` | Whether it is theirs or the runtime's |

Keep those out of your outputs and out of your logs. Anything you emit is
journaled and shows in the inspector.

## Saying what you need

`requiresScopes` and `requiresValues` on your `Access` input are the important
part, and they are per consuming node.

```json
{ "name": "account", "type": "Access", "requiresScopes": ["channels:history"],
  "requiresValues": ["imap_host"] }
```

Those get stamped onto the value as it reaches you, and checked when you open
it. So a node that needs to read history refuses a connection that can only
write, **before** it makes the call, and the person is told to reconnect rather
than getting a provider error they have to decode.

The list is all-of, and it is fixed by the node's one job. When the permission
would depend on which input arrives, like a post that needs one scope for a
person and another for a company page, that is two nodes, each declaring its
own. One node with the rule written in prose protects nobody.

A missing stored value always refuses. A missing permission only refuses when
weft actually knows it is missing: on a service that reports nothing, the call
goes through and the provider decides.

## A long job

A lease covers the firing. For something that genuinely runs a long time, a
multi-hour generation, ask for a window:

```rust
let conn = ctx.open_within(&account, Duration::from_secs(3 * 3600)).await?;
```

The default is fifteen minutes, which is generous for an API call including its
stream and its cost resolution, and short enough that a lease left by a crashed
worker goes stale the same quarter hour.

## A connection your own node hands out

If your node runs the service, it publishes the connection rather than
consuming one:

```rust
let access = ctx.publish_access(BTreeMap::from([
    ("host".into(), host), ("password".into(), password),
])).await?;
ctx.pulse_downstream(NodeOutput::new().set("access", access)).await
```

Declare it in metadata with `"publishes": "postgres"` and an `Access` output.

Publishing again updates rather than duplicating, and terminating the node's
infrastructure deletes it. `ctx.published_access()` reads back what you
published before, which is how a container that mints its own password on first
boot is asked for it exactly once.

## In a test

```rust
let access = rig.access("slack");
rig.connection_value("slack", "team_id", "T123");
rig.connection_permissions("slack", ["chat:write"]);

let outcome = rig.run(&SendNode, json!({ "account": access, "text": "hi" })).await.ok()?;
rig.assert_sent("POST", "/api/chat.postMessage");
```

No real credential, no real call, no cost.
