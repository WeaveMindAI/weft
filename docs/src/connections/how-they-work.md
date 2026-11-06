# How connections work

A credential never appears in your program. It lives in weft's own store, and
what travels a wire is a reference with nothing secret in it.

## Four things that sound alike

| | What it is | Who writes it |
|---|---|---|
| **The recipe** | How a credential is obtained, how a request is signed, what the permissions are, how events arrive. Pure declaration, no code | Whoever wrote the access node |
| **A registered app** | One OAuth application: its client id, its secret, the permissions it asks for | The operator, in the apps file |
| **A connection** | One account somebody connected. Its sealed values, its permissions, who it belongs to | The person who connected it |
| **The `Access` value** | The reference your program passes around | The runtime, when it builds a node's inputs |

For OAuth the first two are different things: the app is the software asking,
and the connection is the account that said yes. A pasted API key needs no app
and is still a connection, and so is a hostname, a username and a password for
a mail server.

## What actually travels

```json
{
  "__weft_access__": {
    "accessId": "11111111-2222-3333-4444-555555555555",
    "service": "slack",
    "identity": "Acme",
    "requiresPermissions": ["chat:write"]
  }
}
```

The runtime stamps the `requires` lists on **per consuming node**, from what
that node's input said it needed: its `requiresScopes` becomes
`requiresPermissions`, and its `requiresValues` stays `requiresValues`, each
there only when the node asked for some. So the same connection
carries different requirements depending on which node is about to use it.
When that node opens it, a missing stored value is refused, and so is a missing
permission on a verified connection (one whose provider said what it granted),
rather than failing halfway through a call.

## Shared, or your own

| Door | What it is |
|---|---|
| `shared` | One click on a credential weft holds: a registered application, or the runtime's own key, spending its credit |
| `own` | You bring your own. A page with the paste fields, and sometimes a guide or a button that creates the application for you |

`own` is the default. Offering `shared` is something a service's recipe has to
declare, and what sits behind it is whatever the operator put in
[the apps file](the-apps-file.md).

A permission a recipe marks as only working on your own account can never be
served by the runtime's credential. The editor greys that option out and
resolution refuses it.

## A way that stays on your machine

A service whose key is pasted can offer more than one way to sign in, and a
recipe can keep one of them to a local install, so the credential never leaves
your own machine. A Claude subscription's token is one: Anthropic does not let
another product hold a claude.ai login.

On a cloud install, the connect form leaves that way out and `weft connect`
refuses it. `weft migrate connections` skips a connection made with it and
says why. If one is on a cloud install some other way, opening it fails before
its credential is handed to anything:

```text
the 'claude_code' access needs reconnecting: the 'my sub' connection was made on
a local install; signing in to Claude Code with 'Claude subscription' works only
on a local install, so that the credential stays on the person's own machine,
and this install runs on a cloud; connect it with 'API key' instead
```

To declare one, go and read
[declaring a service](declaring-a-service.md#pasting-a-key).

## An instance's own connections

In a program with instances, an access node whose connection is written
`account: @instance_filled` holds no connection in the source: each instance
gets its own, and a run for that instance uses it. For how that works, go and
read
[programs with instances](../running/instances.md#what-an-instance-fills-instance_filled).

## What weft can actually tell you about a credential

Providers differ in how much they admit, and weft records which kind of answer
it got.

| What the provider does | Is it authoritative? |
|---|---|
| States exactly what it granted | **Yes** |
| Lets weft ask the credential to describe itself | **Yes** |
| Confirms only that the credential is alive | No |
| Says nothing at all | No |

That distinction decides what gets blocked. A permission weft **knows** is
missing blocks the node that needs it. A permission that is only claimed, or
unknown, lets the call through, because refusing would block every pasted key
on every service that reports nothing.

Stored values are simpler and have no unknown case: if a node needs
`imapHost` and the connection has not got one, the node never starts.

Connecting also runs the service's own check, but **only when that check is
free**. Anything paid, or ambiguous because it might bill depending on the
account, is skipped, so a bad credential sometimes shows up for the first time
when a node tries to use it.

One thing that is always checked: if you tick permissions and the provider
hands back fewer, the connect is refused naming the one you did not get.

## Refreshing

Lazily, when a node opens the connection, and only if the token expires within
the next minute. There is no background keep-alive.

Two things opening the same connection at once queue behind each other, so it
refreshes once and both get the fresh values.

A node can also ask for a refresh itself, when the service refused the token
it was handed (`ctx.renew`). weft refreshes only while the connection still
holds exactly what that node was handed. So if ten nodes ask at once, there is
one refresh and all ten get its result, and a node that asks after another
node's refresh landed gets the new token without weft calling the provider
again. A provider whose refresh token rotates (each refresh retires the one
before) is safe to share for that reason: weft alone ever holds the refresh
token, so no node can use it up under another.

How it refreshes depends on the recipe. A sign-in sends its refresh token to
the token endpoint, a `client_credentials` app asks for a new token, and a
recipe that declares its own `refresh` call runs that instead. A pasted login
whose way declares a `renewal` (Codex's ChatGPT login) runs that renewal. Whichever it
is, the answer is read the way the recipe says, the new token and expiry are
stored, a new refresh token replaces the old one when the provider rotates
them, and the recipe's captures update what they find. For those keys, go and
read [declaring a service](declaring-a-service.md#reading-the-token-answer).

Only a token with a known expiry is ever refreshed, which is why a connect is
refused when the provider's answer names none and the connection could be
renewed. A recipe whose provider says its tokens never expire declares
`neverExpires`.

If a refresh answer names no expiry, or one weft cannot read (a lifetime of
`0` included), the new token is given the lifetime the provider stated for
the one before it, so the connection is not refreshed again every time a node
opens it.

When a refresh cannot happen, it fails loudly rather than retrying into
nothing:

```text
the 'google' access needs reconnecting: the provider issued no refresh token,
so the expired sign-in cannot be renewed
```

A pasted credential that expired says so too, because there is nothing to
refresh it with.

If the provider is busy or down when weft refreshes (it answers 408, 429 or
a 5xx, or does not answer at all), nothing is wrong with the connection, so
it never asks you to reconnect: the node fails with the provider's own
answer (`token endpoint answered 503 ...`, or `token endpoint unreachable:
...`), and the refresh is tried again the next time the connection is
opened.

## Where the secrets are

Sealed with AES-256-GCM, under `CREDENTIAL_ENCRYPTION_KEY`, which is 32 bytes
in base64:

```bash
openssl rand -base64 32
```

Service names, client ids, which permissions were granted and who owns what
stay readable, so weft can query them. Only the credentials themselves are
sealed.

**Without that key set**, weft uses a public development key built into its own
source and warns you once. That key is in the repository, so anyone with a copy
of your database can read every credential in it. Set a real one before you
store anything you care about.

**There is no rotation.** Change the key and everything sealed under the old
one is unreadable:

```text
stored credential data does not open under the current CREDENTIAL_ENCRYPTION_KEY;
if the key changed, the sealed rows it wrote are gone: reconnect the affected
connections
```

Putting the old key back reads them again. Losing it means reconnecting
everything.

## The rest

For connecting one, go and read
[connecting an account](../build/connections.md). For using one inside a node,
[using a connection in a node](using-one-in-a-node.md). For writing a recipe,
[declaring a service](declaring-a-service.md).
