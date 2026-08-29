# Using a connection

## Opening one

```rust
let account: Access = ctx.inputs.get("account")?;
let conn = ctx.open(&account).await?;   // one resolve, one lease, this firing

conn.client()          // &ClientWithMiddleware: signed in, measured if a meter exists
conn.credential()?     // &str: ONLY when the sign-in is one string
conn.value("imap_host")?    // one stored value by name, loud when absent
conn.opt_value("alias")     // Option<&str>, for genuinely optional values
conn.identity()             // the display identity, if recorded
```

For the common case there is sugar:

```rust
let gh = ctx.client(&access).await?;
gh.post(format!("https://api.github.com/repos/{repo}/issues"))
  .json(&body)
  .send().await.node_err("creating the issue")?;
```

Those four lines are the integration, and they are byte-identical across a
pasted token, an OAuth grant, a minted installation token, and the runtime's
own credential.

When the body finishes, any way at all, the runtime releases the lease.

## Secrets never go through config

**Never add a config field asking the user to paste an API key, a token, or a
password.**

Node config and port values travel the execution journal and render in the
inspector in plaintext. A connection's values are sealed in the store and only
exist in the worker's memory while your node runs.

If your service needs a pasted key, declare it as an access node with a paste
acquisition, and the value is sealed in the store instead of the journal. See
[Declaring a service](writing-a-service.md).

## When `credential()` exists

You never declare it. It is there exactly when the resolved auth is **one**
step interpolating **one** stored value: a bearer key, a bot token.

A Basic pair, a signing step, or two steps have no single string that means
anything, and the call fails loudly naming the service.

It is there for libraries that insist on a raw string. Do not stash it
anywhere: not an output port, not a log, not an error, not a struct that
outlives the call.

```rust
// A library that insists on a string: hand it both.
let generator = GeneratorInfo::openrouter(model)
    .with_api_key(conn.credential()?)
    .with_http_client(conn.client().clone());
```

## The rules that matter

**Never construct your own HTTP client for a connection's calls.** Always take
it from the opened connection. A hand-rolled client is invisible to the cost
trail, and a runtime-supplied credential only works through the connection
client's routing.

**Address the service's real API.** The client does whatever routing a
runtime-supplied credential needs, so your code never rewrites a URL.

**Do not paper over a refusal.** When the runtime declines to supply its own
credential, that is a loud error naming the fix ("connect your own"), and
passing it on is the correct behavior.

Redirects behave like any ordinary HTTP client's: followed, with authorization
and cookie headers dropped across a host change.

When you send media on a measured call, declare what you know about it
(duration, dimensions). It sharpens the pre-flight estimate and never changes
what is billed.

## Sockets

If the service has a realtime API, the same connection opens it:

```rust
let mut session = conn.socket("wss://api.example.com/v1/realtime?model=m").await?;
session.send(SocketMessage::Text(payload)).await?;
while let Some(frame) = session.recv().await? { /* ... */ }
session.close().await?;
```

Same rules as the client. The runtime signs the handshake (the credential rides
the handshake only, never a frame), routes the session, and measures it when
the service's meter prices sessions. Never hand-roll a socket client for a
provider.

## The lease window

Every connection call assumes your provider work fits the default window of 15
minutes, which is how long a runtime-supplied credential stays usable if your
node crashes without finishing. On the user's own connected account it changes
nothing.

If your node wraps something that takes longer, say so:

```rust
let conn = ctx.open_within(&access, Duration::from_secs(3600)).await?;
```

## Declaring what your node needs

Say it on the access **input**, and the checks then run where the answer
lives:
live in the editor when a connection is picked, at connect time, and at
run-time resolution.

```json
{ "name": "account", "type": "Access", "required": true,
  "requiresScopes": ["chat:write"],
  "requiresValues": ["imap_host", "imap_port"] }
```

There is deliberately no compile-time check, because source holds only a
connection id.

### `requiresScopes`

Declare **only** what your node's own runtime calls need on every path.

A permission that only one picker source needs (a browse-everything scope
backing a list) belongs on that source's own `requires`, never here. Putting it
here locks out every connection that would have used the picker or a pasted
link.

A **verified** shortfall is a hard error. A claimed or unknown one is let
through, because nobody actually knows, and a pasted key on a service that
reports nothing must not be refused.

A required permission may be an **own-account-only** capability, which the
service's catalogue marks as such: the node's work creates or reads durable
things **inside** the connected account (a minted voice, a configured agent),
so a shared runtime credential can never serve it, because the result would
land in the runtime's account. Declare it through the same `requiresScopes`.
Resolution refuses the shared credential with that capability's setup guide,
and the editor marks the node the moment a shared connection is picked.

### `requiresValues`

For services whose optional fields decide what a connection can **do**. A
mailbox holding the incoming half, the outgoing half, or both.

Unlike a permission set, the answer here is never unknown: a value is stored or
it is not. So a shortfall **always** refuses, naming the value to add.

## Working without a connection at all

Some providers serve a link-shared resource with no sign-in: Google's
spreadsheet CSV export, GitHub's normal API.

**If a provider offers that, support it.** It costs the author little and it
turns "connect your Google account" into "paste the link" for the many people
whose file is already shared.

Say the two limits when you say it works: only for a resource you already have
the link to, and only where the provider really does serve anonymously.

Best case, one address serves both worlds and the body has no branch at all,
which is what GitHub's API does. Verify that before assuming it: Google's CSV
export is anonymous-**only**, ignores a bearer token, and answers 404 for a
private sheet, while its Sheets API is signed-in-only. So
`catalog/google/sheets_read` branches on whether an account is connected, and
both branches share the parsing so they answer identically.

An access input declared `"required": false` **is** the declaration, and no new
metadata is needed. `ctx.client` accepts the absent connection and answers a
plain client:

```rust
let account: Option<Access> = ctx.inputs.opt("account")?;
let client = ctx.client(account.as_ref()).await?;
```

On a `required: true` input an absent value is an ordinary missing-input error,
never a quiet bare request.

## Picking a resource

A connection says **which account**. Almost every real node then needs **which
thing in it**: a spreadsheet, a channel, a repo.

One field, and a list of sources you declare best-first. The editor uses the
best one the picked connection actually supports and quietly drops the rest.

| Kind | Where options come from | Needs |
|---|---|---|
| `granted` | recorded on the connection during sign-in | nothing, no call |
| `list` | call the service and enumerate | its `requires` permissions |
| `picker` | the provider's own chooser, declared entirely by you. Choosing **grants** the picked resource. | a connection |
| `from_url` | paste a link; the pattern's first capture group is the id | nothing at all |

`from_url` needing nothing is what leaves the field standing with **no**
connection: the works-without-signing-in path.

```jsonc
{ "name": "spreadsheet", "type": "String", "required": true,
  "widget": { "kind": "remote_select", "access": "account", "sources": [
    { "kind": "list",
      "requires": ["https://www.googleapis.com/auth/drive.readonly"],
      "get": "https://www.googleapis.com/drive/v3/files?...",
      "items": "files", "label": "name", "value": "id",
      "page": { "cursor_param": "pageToken", "cursor_path": "nextPageToken" } },
    { "kind": "picker",
      "script": "https://apis.google.com/js/api.js",
      "code": "await new Promise((r) => gapi.load('picker', r)); ...",
      "grants": ["https://www.googleapis.com/auth/drive.file"],
      "mime_types": ["application/vnd.google-apps.spreadsheet"] },
    { "kind": "from_url", "pattern": "/spreadsheets/d/([a-zA-Z0-9_-]+)" } ] } }
```

The stored value is the bare id, which is what your node reads and what the
field's `String` type holds. The human label the editor shows is a display
cache, never source. Pasting a raw id fills the field just as well.
A value arriving on the wire at run time skips the picker entirely, since there
is nobody there to pick.

### Writing a picker

A `picker` is yours end to end, and it is the one place a node carries
**browser JavaScript**, the same way an `ExecPython` node carries Python. Your
`mod.rs` stays pure Rust and never sees any of it.

| Field | What it is |
|---|---|
| `script` | the https address of the provider's own chooser library, the one their embed docs tell every web developer to load |
| `code` | plain browser JavaScript, usually adapted straight from the provider's sample. It runs on a small page weft serves, opened in the user's real browser, after `script` loaded, inside an async function, so top-level `await` works. |
| `grants` | the permissions that choosing through this chooser grants on the picked resource, recorded when the pick lands |
| `mime_types` | narrows the chooser, threaded to your glue |

Your code talks to weft through one object, `weft`, already in scope:

| | |
|---|---|
| `weft.token` | the connection's access token: the string you hand the chooser where its docs say "your OAuth token" |
| `weft.clientId` | the **public** client id of the app behind the connection, or `null` when no app made it. Some choosers require an app identifier. |
| `weft.mimeTypes` | your declared types, for choosers that filter |
| `weft.done({id, label})` | the user picked this: the field fills with it |
| `weft.cancel()` | the user closed it without picking: the field closes quietly |
| `weft.fail(message)` | the chooser could not work: the message shows on the field in red |

Call exactly one of the three enders. The first call wins, and a thrown
exception or rejected await becomes `weft.fail` automatically, so a provider
error surfaces on the field instead of hanging it.

So the recipe for any new provider is: open their "picker embed" docs, take
their sample, replace their token slot with `weft.token`, and route their
picked and cancelled callbacks into `weft.done` and `weft.cancel`. The page
opens in the user's real browser, so a chooser that leans on the provider's own
session finds it already there.

A picked resource is as person-scoped as the connection itself, so both are
re-chosen when a project changes hands, and an unresolvable one is a loud node
error.

## Handing out a connection to something your node runs

If your node **runs** a service itself, a database it provisions say, it can
hand out a connection to that service, and downstream nodes then reach it
exactly like one a person connected.

```json
"publishes": "postgres",
"outputs": [{ "name": "access", "type": "Access", "required": false }]
```

```rust
let values = BTreeMap::from([
    ("host".into(), host), ("database".into(), db),
    ("user".into(), user), ("password".into(), password),
]);
let access = ctx.publish_access(values).await?;
ctx.pulse_downstream(NodeOutput::new().set("access", access)).await
```

The call names no service: your metadata already did, and saying it twice is a
way for the two to disagree. The values are the service's own declared fields,
the same ones a person would have filled in, and anything else is refused right
there.

The service's recipe stays where it always lives, on that service's access
node. The compiler looks it up and attaches it at build time, which is why the
name has to be declared rather than passed at run time: a built project carries
only the node types its graph uses, so the access node is usually not in there,
while the compiler sees the whole catalog. A name nothing declares fails the
build.

The connection belongs to your node. Publishing again updates it, and
terminating the node's infrastructure deletes it, so it lives exactly as long
as the thing it opens. It is always the user's
own credential; nothing a node publishes can resolve to the runtime's.

### Ask for a once-only secret once

A well-built service mints its password on first boot and refuses to say it
twice, so on later runs read it back from your own connection rather than
asking the service again:

```rust
let password = match ctx.published_access().await? {
    Some(mine) => ctx.open(&mine).await?.value("password")?.to_string(),
    None => ask_the_container(&ctx).await?,
};
```

The two arms run on different runs, and a replay walks back through the steps
it recorded, so the arms have to record the same ones. Give them the same sequence of
`ctx.run` and `ctx.await_signal` calls, or none at all, and do not assume a body
with no suspension point is exempt. Both rules and why they bite here:
[the replay rule](../nodes/durable-execution.md#the-replay-rule).

`catalog/postgres/database` is the worked example. It retires the password only
once a connection holds it, and asks again on every run rather than only the
one that read it, so a run that dies in between cannot leave a service nothing
can sign in to.
