# The apps file

`access-apps.json` is where an operator puts the apps and API keys their
installation offers as shared connections. Whatever is in there shows up in
everyone's connection panel.

It maps a service name to a list of credentials. A runtime OpenRouter key:

```json
{
  "openrouter": [{
    "kind": "api_key",
    "label": "Runtime key",
    "key": "REPLACE_WITH_YOUR_KEY"
  }]
}
```

Keep the file private: calls on that key spend money on its account. For what
weft records about that, read [measuring what a call costs](meters.md).

Start the daemon from the directory holding the file, or point at it:

```bash
export WEFT_ACCESS_APPS_FILE='/path/to/access-apps.json'
weft daemon start
```

The daemon copies it into the cluster, so an edit only takes effect after
another `weft daemon start`. Nothing reads your local disk at run time.

For more services, start from
[access-apps.example.json](https://github.com/WeavemindAI/weft/blob/mvp/access-apps.example.json)
and keep the entries you want. If there is no file and no
`WEFT_ACCESS_APPS_FILE`, you simply have no operator-configured apps.

## OAuth apps

An `oauth_app` entry is the application users grant access to. It needs a
`label`, a `client_id`, a `client_secret` if there is one, and a pinned
`token_url`. Browser-consent apps need a pinned `auth_url` too.

Its `covers` list is the permission set weft asks for, and a recipe that tries
to substitute a different sign-in address is refused.

You can configure several apps for one service. Each shows up as its own
labelled choice with its permissions listed, so people can pick the one that
suits what their program does.

If the app receives webhooks, its `events` block carries whatever the recipe
needs to verify them, such as a signing secret. For that side, read
[events from a service](events.md).

## What it refuses

Every service value has to be a list, even with one entry. Every entry needs a
`kind` and a label that is unique within that service, and there can be only
one `api_key` per service.

An OAuth app has to declare `covers`, and an empty list means it asks for
nothing. Those entries are checked against the service's permission catalog.
An `events` block on a service with no webhook transport is refused. So are
invalid JSON and a configured file that is not there.

## Apps that ship with a step

Step metadata can declare `accessApps`, including through package defaults.
That is for public OAuth apps with no client secret; metadata carrying a
`client_secret` is rejected. Browser consent uses PKCE according to the
recipe's `pkce` setting, which is on unless it says otherwise.

## Encryption

Stored secrets are encrypted with `CREDENTIAL_ENCRYPTION_KEY`, which is 32
bytes in base64:

```bash
openssl rand -base64 32
```

Keep it somewhere safe and supply it every time you start the daemon:

```bash
export CREDENTIAL_ENCRYPTION_KEY='paste-the-generated-key'
weft daemon start
```

Without one, weft uses a public development key and warns you. It protects a
database dump from nobody.

Changing the key does not re-encrypt what is already stored. Those records
simply fail to open under the new key, and putting the old one back reads them
again. Lose it and you reconnect the accounts. There is no rotation procedure.
