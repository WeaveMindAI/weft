# The shared-credentials file

`WEFT_ACCESS_APPS_FILE` names one JSON file holding every credential this weft
offers as a **shared** connection, whatever its shape. Start from
`access-apps.example.json`.

It is keyed by service, and each service holds a **list** of entries, even for
one. Every entry declares its `kind`.

```jsonc
{
  "_readme": "keys starting with _ are comments and are ignored",

  "google": [
    {
      "kind": "oauth_app",
      "_setup": "console.cloud.google.com -> credentials -> add the callback URL",
      "label": "Google Drive",
      "covers": [
        "https://www.googleapis.com/auth/drive.file"
      ],
      "auth_url": "https://accounts.google.com/o/oauth2/v2/auth",
      "token_url": "https://oauth2.googleapis.com/token",
      "client_id": "…",
      "client_secret": "…",
      "events": {
        "signing_secret": "…"
      }
    }
  ],

  "openrouter": [
    { "kind": "api_key", "label": "Runtime key", "key": "sk-or-…" }
  ]
}
```

## The two kinds

**`oauth_app`** is one OAuth application this weft signs users in with. Its
`auth_url` and `token_url` are **pinned**: a shared connect whose recipe names
any other address is refused, so these credentials only ever go where you wrote.

**`api_key`** is the runtime's own key for a pasted-key service, and it is what
the "use the runtime's key" door hands the credential source. At most one per
service.

How that key is handed out, directly to the worker or swapped for a stand-in
behind a relay, is decided by the deployment's credential source and
**deliberately never by a field in this file**. A typo in here must not be able
to change who gets the raw key.

## The rules

Every one is checked loudly, because a typo must never silently remove a
one-click door.

- Every service's value is a list, and every entry declares its `kind`.
- Every entry has a non-empty `label`, unique within its service.
- On an `oauth_app`, **`covers` is mandatory.** Empty means "may ask for
  nothing", never "everything". Every entry in it must exist in that service's
  permission catalogue, which is checked at the door probe and at every shared
  connect. An `api_key` entry carries only a `label` and a `key`.
- An `events` block on a service whose recipe declares no webhook transport is
  refused. That is configured receiving for pushes that can never arrive.
- A malformed file is a loud error on every lookup.
- If `WEFT_ACCESS_APPS_FILE` names a file that is not there, that is an error
  too. It is only when the variable is unset, and the default
  `access-apps.json` is absent, that this quietly means "no apps" and hides the
  shared doors.

The broker rereads the file on every lookup, but it reads a copy packed into
the cluster at daemon-apply time, so editing `access-apps.json` on your machine
changes nothing until you re-run `weft daemon start`.

## Several apps per service

Each registered app is its own one-click option in the connect panel, labelled,
with its `covers` shown as a fixed set.

That is how a cheap permission tier avoids a provider's review process, and how
one service gets product-shaped options ("Google Drive", "Google Calendar")
while staying one account underneath.

## Apps a project ships

A project may ship a **public** app, meaning PKCE and secretless, under
`accessApps` in its metadata, inherited from the package root like any other
key.

An entry carrying a `client_secret` fails the metadata load, because project
metadata is source and source never holds secrets.

## Encryption

Everything sealed at rest goes through `CREDENTIAL_ENCRYPTION_KEY`: base64 of
32 bytes.

```bash
openssl rand -base64 32
```

Unset, a built-in development key is used and a warning is logged. Set a real
one before you store a credential you care about.

**Set it once and do not change it.** Every sealed row is opened with the key
that sealed it, so a different key means every connection you have stops
opening, and the way back is reconnecting each account by hand. There is no
rotation ceremony, and the failure surfaces the first time something tries to
use a credential rather than at boot.
