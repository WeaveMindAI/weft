# The apps file

`access-apps.json` is where the operator puts the applications and keys this
installation offers as one-click connections. Whatever is in it shows up in
everybody's connect panel.

Start from the example and keep what you want:

```bash
cp access-apps.example.json access-apps.json
```

Fill in the services you expect to use often. A service you set up here is one
click forever after, and one you skip asks each person to bring their own the
first time they need it, which is fine for something you touch once.

## The shape

A service name maps to a **list**, always, even with one entry. Keys starting
with `_` are ignored, which is how the example file carries its own notes.

```json
{
  "openrouter": [{
    "kind": "api_key",
    "label": "Runtime key",
    "key": "sk-or-..."
  }]
}
```

### A pasted key

| Key | What it is |
|---|---|
| `kind` | `"api_key"` |
| `label` | The name the connection list shows |
| `key` | The credential. At most one `api_key` per service |

This is what "use ours" hands out. Calls on it spend money on that account, so
keep the file private.

### An OAuth application

```json
{
  "slack": [{
    "kind": "oauth_app",
    "label": "Slack",
    "covers": ["chat:write", "channels:read", "channels:history"],
    "auth_url": "https://slack.com/oauth/v2/authorize",
    "token_url": "https://slack.com/api/oauth.v2.access",
    "client_id": "...",
    "client_secret": "...",
    "events": { "signing_secret": "..." }
  }]
}
```

| Key | What it is |
|---|---|
| `label` | Unique within the service |
| `covers` | Exactly the permissions this app asks for, by their ids from the service's catalogue |
| `auth_url`, `token_url` | The provider addresses this app signs in against, copied from the recipe |
| `client_id`, `client_secret` | From the provider. `client_secret` is absent for a public PKCE app |
| `events` | Only if this app receives pushes. Whatever the recipe needs to verify them, like a signing secret |

**Permissions are fixed per app.** People pick an application rather than
ticking boxes, which is why you can list several for one service: a cheap tier
that avoids a provider's review process, and a fuller one beside it. Each shows
up as its own labelled choice with its permissions listed.

The addresses are pinned here, and a recipe that tries to substitute a
different sign-in address is refused. A connection through a registered app
only ever talks to the addresses you wrote.

You also have to register the callback URL the connect panel shows you as the
app's redirect URL, and re-register it whenever your public address changes.
Some providers, Slack among them, only accept https callbacks.

## Making it take effect

The installer hands the file to the runtime, so `./setup.sh` picks it up. After
that:

```bash
weft daemon start
```

That re-applies it and restarts the piece holding it, in a few seconds.

If the file is gone, the runtime keeps the keys it already has and tells you
so. To actually empty them, `weft daemon start --clear-access-apps`.

Nothing reads your local disk at run time. The file is copied in, so an edit
does nothing until you re-apply it.

To keep it somewhere else, point `WEFT_ACCESS_APPS_FILE` at it. A file named
that way and unreadable is a hard error rather than a silent nothing, because
shipping no apps quietly shows up later as a service nobody can connect on a
node the operator thought was configured.

## What it refuses

- A service whose value is not a list
- An entry with no `kind`
- Two entries with the same label in one service
- More than one `api_key` for a service
- An OAuth app with no `covers`
- A permission in `covers` that the service's catalogue does not have
- An `events` block on a service with no webhook transport
- Invalid JSON, or a file you configured that is not there

## Apps a node ships

A node's own `metadata.json` can declare `accessApps` for public applications
with no secret. Those need PKCE, and metadata carrying a `client_secret` is
refused: node metadata is source, and source never holds secrets.

An application with a secret belongs in this file.

## Encryption

Everything stored is sealed under `CREDENTIAL_ENCRYPTION_KEY`, which is 32
bytes in base64:

```bash
openssl rand -base64 32
```

Set it before you store a credential you care about. Without it weft uses a
public development key from its own source and warns you once, which protects a
database dump from nobody.

Keep it. There is no rotation: change the key and everything sealed under the
old one is unreadable until you put it back. Lose it and you reconnect
everything.
