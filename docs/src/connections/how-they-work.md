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
    "identity": "Q @ Acme",
    "requiresPermissions": ["chat:write"],
    "requiresValues": []
  }
}
```

The two `requires` lists are stamped on **per consuming node**, from what that
node said it needed. So the same connection carries different requirements
depending on which step is about to use it, and one that falls short is refused
when that step opens it rather than halfway through a call.

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
`imap_host` and the connection has not got one, the node never starts.

Connecting also runs the service's own check, but **only when that check is
free**. Anything paid, or ambiguous because it might bill depending on the
account, is skipped, so a bad credential sometimes shows up for the first time
when a step tries to use it.

One thing that is always checked: if you tick permissions and the provider
hands back fewer, the connect is refused naming the one you did not get.

## Refreshing

Lazily, when a step opens the connection, and only if the token expires within
the next minute. There is no background keep-alive.

Two things opening the same connection at once queue behind each other, so it
refreshes once and both get the fresh values.

When a refresh cannot happen, it fails loudly rather than retrying into
nothing:

```text
the 'slack' access needs reconnecting: the provider issued no refresh token,
so the expired sign-in cannot be renewed
```

A pasted credential that expired says so too, because there is nothing to
refresh it with.

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
