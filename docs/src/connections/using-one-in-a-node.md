# Using a connection in a node

Declare a connection input, typed with its service, with the connect widget:

```json
"inputs": [
  { "name": "account", "type": "Access[slack]", "required": true,
    "widget": { "kind": "access" },
    "requiresScopes": ["chat:write"] }
]
```

Write just `{"kind": "access"}`. The service and whether it is optional are
filled in from your node's `service` recipe. The `[slack]` makes the compiler
refuse any other service's connection on this input; for a node that works
with several, go and read [which connection a node takes](../nodes/metadata.md#which-connection-a-node-takes).

Then open it:

```rust
let account: Access = ctx.inputs.get("account")?;
let client = ctx.open(&account).await?.client();

let reply = client.post("https://slack.com/api/chat.postMessage")
    .json(&body).send().await.node_err("posting the message")?;
```

That client already has the authentication, the routing and the metering on it.

## Sending the call and reading the answer

Every API node does the same three things after building a request: send it,
fail with the provider's own words when the status is not a success, and read
the body. `weft::access::client` has that once, so your node does not write it
again:

| Call | Use it when |
|---|---|
| `get_json(&client, url, what)` / `post_json(&client, url, &body, what)` | A plain GET, or a POST of a JSON body, that answers JSON |
| `json_call(request, what)` | The request needs its own preparation (a content type, a multipart body) but answers JSON |
| `checked_send(request, what)` | The answer is not JSON (a download, raw text). You get the response back unread |
| `require_ok_flag(answer, "ok", "error", what)` | The service answers 200 to everything and says inside the body whether it worked (Slack's `ok`, Telegram's `ok` and `description`) |
| `cursor_paged(paging, what, request, check, visit)` | A list that comes a page at a time with a cursor. `CursorPaging` says where the items and the next cursor sit and which query parameter carries it back; `past_cap_hint` is what the user is told to narrow when the list runs past the page cap |
| `required_str(&answer, what, "id")` | A field the rest of the node cannot do without, failing by name when it is missing |

`what` names the attempt, so a failure reads "the service answered 403 trying
to post the message" instead of a bare status code.

If your node waits on a job the provider runs, go and read
[waiting on a job a service runs](../nodes/durable-execution.md#waiting-on-a-job-a-service-runs).

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
| `value(name)` | One stored value, like `imapHost` |
| `identity()` | Who it is, for a log line |
| `owner()` | Whether it is theirs or the runtime's |
| `way()` | Which [way to sign in](declaring-a-service.md#pasting-a-key) the connection was made with, like `"subscription"`, or `"default"` when its service offers only one. `None` for a connection signed in through a consent flow, a minted key or the runtime's own key; a key pasted on a consent service's own page reads `"default"` |

If your service offers several ways, `way()` is how you tell the credentials
apart. A Claude subscription's token and an API key go into different
variables:

```rust
let conn = ctx.open(&account).await?;
match conn.way() {
    Some("subscription") => env.insert("CLAUDE_CODE_OAUTH_TOKEN", conn.value("token")?),
    _ => env.insert("ANTHROPIC_API_KEY", conn.value("key")?),
};
```

Keep the values out of your outputs and out of your logs. Anything you emit is
journaled and shows in the inspector.

## Saying what you need

`requiresScopes` and `requiresValues` on your connection input are the important
part, and they are per consuming node.

```json
{ "name": "account", "type": "Access[slack]", "requiresScopes": ["channels:history"],
  "requiresValues": ["teamId"] }
```

Those get stamped onto the value as it reaches you, and checked when you open
it. So a node that needs to read history refuses a connection that can only
write, **before** it makes the call, and the person is told to reconnect rather
than getting a provider error they have to decode.

The list is all-of, and it is fixed by the node's one job. When the permission
would depend on which input arrives, like a post that needs one scope for a
person and another for a company page, that is two nodes, each declaring its
own. One node with the rule written in prose protects nobody.

If an optional input needs one more permission only when somebody uses it,
leave that permission out of `requiresScopes`, which every program using the
node is asked for, and add it in `run` before you open the connection:

```rust
if reply_to.is_some() {
    let mut needed = account.required_permissions().to_vec();
    needed.push("https://www.googleapis.com/auth/gmail.readonly".to_string());
    account = account.with_required_permissions(needed);
}
let http = ctx.client(&account).await?;
```

Extend the list rather than replace it: it already holds what `requiresScopes`
declared. Opening then refuses a connection that lacks the permission, naming
it, the same way it refuses one missing a declared one.

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

## When the service refuses the values

If the service refuses the token your node was handed, or something you handed
it to says it is about to die, ask weft to renew the connection:

```rust
let conn = ctx.open(&account).await?;
let token = match call_with(conn.value("token")?).await {
    Err(Refused::Unauthorized) => ctx.renew(&account).await?.value("token")?.to_string(),
    answer => answer?,
};
```

`ctx.renew` hands back the connection opened again, renewed. weft renews it
only while the connection still holds exactly what your firing was handed. If
another node renewed it in the meantime, you get its new values and the
provider is not asked again. If ten nodes ask at the same moment, the provider
sees one renewal and all ten get its result. That matters for a provider whose
refresh token rotates, where each renewal retires the refresh token before it:
weft is the only one ever holding it, so no node can spend it under another.

Open the connection in the same firing first, because the renewal is of what
you were handed. Without an open, the node fails:

```text
renew the 'codex' connection: this node has not opened it in this firing, so
nothing says which values stopped working; open it (`ctx.open`) first
```

A connection weft has nothing to renew with, like a pasted API key, fails the
node with a reconnect:

```text
the service refused what the 'my key' connection holds, and weft holds nothing
to renew it with; connect the account again (Connect on its access node in the
editor, or `weft connect` in the project's folder)
```

So does a renewal the provider refuses (a revoked sign-in, say). For a pasted
login weft can renew, the recipe declares how, and for that, go and read
[declaring a service](declaring-a-service.md#a-pasted-login-weft-keeps-alive).

## A connection your own node hands out

If your node runs the service, it publishes the connection rather than
consuming one:

```rust
let access = ctx.publish_access(BTreeMap::from([
    ("host".into(), host), ("password".into(), password),
])).await?;
ctx.pulse_downstream(NodeOutput::new().set("access", access)).await
```

Declare it in metadata with `"publishes": "postgres"` and an `Access[postgres]` output.

Publishing again updates rather than duplicating, and terminating the node's
infrastructure deletes it. `ctx.published_access()` reads back what you
published before, which is how a container that mints its own password on first
boot is asked for it exactly once.

## In a test

```rust
rig.connection_permissions("slack", &["chat:write"]);
rig.respond("POST", "/api/chat.postMessage", json!({ "ok": true, "ts": "111.222", "channel": "C42" }));
rig.respond("GET", "/api/chat.getPermalink", json!({ "ok": true, "permalink": "https://acme.slack.com/archives/C42/p111222" }));

let inputs = json!({ "account": rig.access("slack"), "channel": "C42", "text": "hi" });
rig.run(&SlackSendMessageNode, inputs).await.ok()?;
rig.assert_sent("POST", "/api/chat.postMessage");
```

`rig.access("slack")` makes a connection marker to put on the access input.
`rig.connection_permissions` says what that connection was granted, so opening
it checks the node's `requiresScopes`; leave it out and the permission check is
skipped. `rig.connection_value("slack", "teamId", "T123")` gives the connection
a stored value, for a node that reads one. `rig.respond` sets up a canned answer
for a route, so no request leaves the test.

If your node reads `way()`, `rig.connection_way("codex", "subscription")` makes
it answer `"subscription"`. If your node renews a connection,
`rig.connection_renewal("codex", Ok(&[("token", "t2")]))` says what a renewal
hands, and `Err("why")` makes the provider refuse it. `rig.connection_renewals("codex")`
counts the renewals that reached the provider. The rig renews the way a real
run does: it refuses a renewal before an open, and a node handed values the
connection no longer holds gets the newer ones with no renewal counted.

The test uses no real credential and makes no real call, so it costs nothing.
