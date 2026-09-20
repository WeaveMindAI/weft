# How connections work

Credentials live in weft's own store, never in your program. An access step
holds a reference to one. Any step that needs to call that service takes that
reference as an input.

If you just want to connect something, read [Connect an
account](connect-an-account.md). This page is about what happens behind that.

## Four things that sound alike

| Thing | What it is | Where it lives |
|---|---|---|
| Service recipe | How to get a credential, sign a request, check permissions and receive events. The glossary calls this the recipe, and so does the rest of the book | The access step's metadata |
| Registered app | An OAuth application the operator configured, with its secret and the redirect URLs it is allowed to use | The runtime's apps file |
| Connection | One connected account: its values, permissions and identity | The access store |
| `Access` value | The reference your program passes around | On the wire |

For OAuth, the app and the account are different things: the app is the
software asking for permission, and the connection is the account that granted
it. A pasted API key needs no app and still becomes a connection, and so does
a hostname, username and password for a mail server.

Here is what actually moves between steps. It is small, and there is nothing
secret in it:

```json
{
  "__weft_access__": {
    "accessId": "connection-id",
    "service": "slack",
    "identity": "Acme Corp",
    "requiresPermissions": ["chat:write"],
    "requiresValues": []
  }
}
```

The two `requires` lists are stamped on per consuming step, from what that
step said it needed, and a connection that falls short of them is refused when
it resolves. For where the tenant wall stops and what you still have to keep
private yourself, go and read the [security
policy](https://github.com/WeavemindAI/weft/blob/mvp/SECURITY.md).

## Shared credentials, or your own

A service can offer shared, own, or both. The picker shows whichever of them
your installation actually has.

**Shared** uses an app or credential the operator configured. For OAuth, their
configuration fixes which permissions get asked for and pins the redirect URLs
the provider sends people back to. Nothing a recipe says can widen
either.

**Own** means you bring or create the credentials. Depending on the service,
that is a guided walk through creating an app, a button that creates one for
you, or a form asking for the values.

For configuring what is on offer, read [the apps file](the-apps-file.md). For
declaring a service's options, read [declaring a
service](writing-a-service.md).

## What weft can check about a credential

Providers differ. A few state exactly what they granted, or let weft ask the
credential to describe itself, and those two answers are the only ones weft
treats as authoritative. Others confirm only that the credential is alive, or
say nothing at all until a real call comes back refused.

weft records which of those answers it got. A permission it knows is missing
blocks a step that needs it. A permission set that is only claimed, or
unknown, is not enough to block on, so the call goes through and the provider
may refuse it.

Stored values are simpler. If a step needs `imap_host` and the connection does
not have one, the step never starts.

A service also says what its check costs to run, and only a `free` one runs on
its own. Anything paid, or ambiguous because it might bill depending on the
account, is skipped. So a bad credential sometimes shows up for the first time
when a step tries to use it.

## When a step makes a call

The step opens its `Access` with `ctx.open`. The access store checks what that
step declared it needs, and refreshes the credential if it has expired. What
comes back is a client with the authentication already set up.

If a meter recognises the request, weft records what it cost. That is weft's
own record, not the provider's bill. For the rules, read [measuring what a
call costs](meters.md).

For using the connection from inside a step without leaking secrets into
outputs and logs, read [using a connection](using-a-connection.md).

## Where the secrets are

Secret values are encrypted at rest with AES-256-GCM, keyed by
`CREDENTIAL_ENCRYPTION_KEY`. Public metadata such as service names stays
readable so weft can query it.

If you have not set that key, weft falls back to a development key built into
its own source, and warns you the first time it seals or opens anything. That
key is public, so anyone holding a copy of your database can read every
credential in it. Set a real one before you store a credential you care about,
and keep it: lose it and every connection you had is unreadable. For setup and
recovery, read [encryption](the-apps-file.md#encryption).

For the event path, read [events from a service](events.md).
