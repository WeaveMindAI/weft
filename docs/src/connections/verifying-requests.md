# Checking a signed request

A `verify` block tells weft how to check that a request really comes from who
it claims to. You write it in one of two places:

- in a topic's `webhook`, to check a provider's pushes. The secret or public
  key comes from the receiving app's `events` entry in
  [the apps file](the-apps-file.md);
- in a `Route`'s or `Socket`'s `auth` setting, so that it checks its callers
  (a gated route). The secret or key comes from a connection of the route's
  own (see [on a route](#on-a-route) below).

The block only describes the request: where each part sits, what gets signed,
which issuer to trust.

## The shortest one

If a provider signs the raw body with a shared secret and sends the hex digest
in a header, this is the whole block (GitHub's, which writes `sha256=<hex>`):

```json
"verify": {
  "kind": "hmac",
  "signature": { "in": "header:X-Hub-Signature-256", "pair": "=", "key": "sha256" },
  "concat": "{body}"
}
```

weft computes the HMAC-SHA256 of the exact bytes it received, keyed by the
stored secret, decodes the hex after `sha256=`, and compares the two as bytes,
in constant time.

## On a route

A `Route` or a `Socket` answers anybody who has its address until you set its
`auth`:

| `auth` | What the caller sends |
|---|---|
| `"none"` (the default) | Nothing |
| `"api_keys"` | One of the keys you hand out, in `X-Api-Key` or as `Authorization: Bearer <key>` |
| `"hmac_sha256"` | A hex HMAC-SHA256 of `<timestamp>.<body>` in `X-Signature`, with the unix-seconds timestamp in `X-Timestamp`, refused when it is more than five minutes off |
| `"hmac_sha1"`, `"hmac_sha512"` | The same layout, with HMAC-SHA1 or HMAC-SHA512 |
| `"ed25519"` | The same layout, signed with Ed25519 instead of an HMAC |
| `"ecdsa_p256"` | The same layout, signed with ECDSA P-256, the signature in base64 DER |
| `"oidc"` | A signed identity token in `Authorization: Bearer`, from the issuer the connection names |
| a block | Whatever the block says, written the way the rest of this page describes |

If your sender uses another scheme (GitHub's signature, signed identity
tokens from your sign-in provider), write the whole block:

```weft
hook = Route {
  path: "github"
  method: "POST"
  auth: { "kind": "hmac", "signature": { "in": "header:X-Hub-Signature-256", "pair": "=", "key": "sha256" }, "concat": "{body}" }
}
```

Then give the route its connection, which holds the keys or the secret: the
editor shows a Connect panel under `auth`, and in the terminal it is
`weft connect hook`. Each scheme's connection has its own fields:

| `auth` | Its connection's service | Fields |
|---|---|---|
| `api_keys` | `api_key_auth` | `keys` |
| `hmac_sha256`, `hmac_sha1`, `hmac_sha512` | `hmac_auth` | `signingSecret` |
| `ed25519`, `ecdsa_p256` | `signature_auth` | `publicKey`: hex for `ed25519`, base64 DER for `ecdsa_p256` |
| `oidc` | `oidc_auth` | `issuer`, `jwksUrl`, `audience` |

If you are scripting it, `weft connect hook --door own --set-env
signingSecret=HOOK_SECRET --label "github hook"` stores a new one, and `weft
connect hook --grant <id>` picks one already stored. When a field is missing,
the command prints the fields the scheme wants.

If you turn on a route whose `auth` names a scheme and no connection is
picked, preparing it fails with ``'hook' checks callers with hmac but has no
connection; run `weft connect hook` ``. `token_echo` cannot check a caller, so
`auth` refuses it.

## The schemes

| `kind` | Checks | Secret or key |
|---|---|---|
| `hmac` | An HMAC over parts of the request | `signingSecret` |
| `signature` | An Ed25519 or ECDSA P-256 signature over parts of the request | `publicKey` |
| `oidc` | A signed identity token in `Authorization: Bearer`, against an issuer's published keys | the issuer's keys; see below for the audience |
| `api_keys` | A key from a list. Routes only | `keys` |
| `token_echo` | The token weft minted when it subscribed, sent back on every push. Pushes only | the subscription |

If you use `oidc`, write `issuers` (the `iss` values the token may carry) and
`jwksUrl` (where the issuer publishes its signing keys). The token must be
signed RS256, and `jwksUrl` has to be an `https://` address, since keys
fetched over plain `http` could be swapped by anyone on the way. weft keeps
the keys it fetched for an hour, and an issuer that is slow to answer holds
up only the tokens it signed. On a push, write `issuers` and `jwksUrl` out
in full, with no `{...}` placeholders, or the metadata fails to load. Each
push's token must then name the `audience` of one of the receiving apps (the
receiver's own address for an app that names none) and carry
`email_verified: true`, and when that app sets `pushEmail`, its `email` must
be that one.

On a gated route, `issuers` and `jwksUrl` can name the connection's stored
values, like `"{jwksUrl}"`, and every token must name the connection's
`audience`. One issuer usually signs tokens for every app it serves, so
without the audience a token meant for another of those apps would get in.
If you want a route that admits the users of your own sign-in provider
(BetterAuth, Clerk, Auth0, Firebase, any OIDC issuer), this `auth` does it,
and the route's connection then holds `issuer`, `jwksUrl` and `audience`:

```json
{ "kind": "oidc", "issuers": ["{issuer}"], "jwksUrl": "{jwksUrl}" }
```

The caller sends `Authorization: Bearer <token>`, and the run's `caller`
carries the token's claims (`sub`, `email`, whatever the issuer put there).

If you use `api_keys`, the caller sends the key in the header that `header`
names (`X-Api-Key` unless you set it), and with `bearer` (on unless you set it
to `false`) weft also takes `Authorization: Bearer <key>`. When both are sent,
the header wins. The accepted keys are the connection's `keys` value,
separated by commas or line breaks.

`token_echo` has no keys of its own. The topic's `tokenPath` says where a push
carries the token back, and its `idPath` where it names the subscription,
which is how weft finds the token to compare against. The subscribe call has
to send the token, as `{subscriptionToken}`.

Everything below is about `hmac` and `signature`. They read the request the
same way and differ only in the check itself, which
[how the check is made](#how-the-check-is-made) covers.

## Where a value sits

Every place a value is read from is written the same way:

| Written | Reads |
|---|---|
| `header:<Name>` | A header, whatever its case. If the request sends it twice with two different values, it is refused, since there is no telling which one was signed. The signature's own header is the exception: every copy is read |
| `body:<path>` | A field of the JSON body, by dotted path. A string is read as it is, and a whole number or `true`/`false` as written. A number with a fraction or an exponent, or a whole number too large for 64 bits, is written back out the way weft's JSON reader prints it, so it matches only if the provider wrote it that way too. A field that is `null`, an object or an array counts as missing |
| `signature:<key>` | One entry of the signature's own value, like Stripe's `t`. Only for a timestamp |

## `signature`: where the signature is and how it is laid out

| Key | Default | Set it when |
|---|---|---|
| `in` | **required** | Always: `header:<Name>`, or `body:<path>` when the provider puts the signature in the body (Mailgun) |
| `list` | the whole value is one entry | The value holds several entries: `,` for Stripe, `;` for Paddle, a space for Standard Webhooks |
| `pair` | each entry is a bare signature | Each entry is a key and a value: `=` in `v1=<sig>`, `,` in `v1,<sig>`. An entry is cut at the first one |
| `key` | none | You set `pair`: the key whose entries hold a signature (`v1`, `h1`, `v0`). Entries under other keys are skipped |

When the request carries several signatures (several entries under the key, or
the header sent more than once), one match is enough. That is how a provider
rolls a secret: it signs with the old and the new one for a while.

## `timestamp`: refusing an old request sent again

Leave it out when the provider signs no timestamp (GitHub, Shopify, Twilio).

| Key | Default | Set it when |
|---|---|---|
| `in` | **required** | `header:<Name>`, `body:<path>`, or `signature:<key>` for one entry of the signature's value |
| `format` | `seconds` | The provider writes `milliseconds` (Linear) or `rfc3339` (`2024-05-01T12:00:00Z`) |
| `toleranceSecs` | `300` | The provider documents another window: Paddle's is 5, Linear's 60 |

A timestamp further from now than the tolerance, in either direction, is
refused.

## `concat`: what is signed

`concat` is a template: literal text stays as it is, and each placeholder is
replaced by a part of the request:

| Placeholder | Becomes |
|---|---|
| `{body}` | The raw body, byte for byte, never parsed and rewritten |
| `{timestamp}` | The declared timestamp, exactly as the request wrote it |
| `{header:<Name>}` | One header's value |
| `{body:<path>}` | One field of the JSON body |
| `{url}` | The address the request was sent to (see below) |
| `{method}` | `POST`, `GET`, and so on (always `POST` on a push) |
| `{sorted_form_params}` | The form body's pairs sorted by name, then value, each written name then value with nothing between (Twilio's rule) |

On a gated route, `{url}` is the address the caller used, with the path and
query exactly as they sent them. The host is the `Host` the request arrived
at weft with (a proxy's `X-Forwarded-Host` wins over `Host`), lowercased and
without the scheme's default port (`:443` for `https`, `:80` for `http`), so
register the address with the provider in that form. The scheme comes from
`X-Weft-Forwarded-Proto` when the request carries it (weft's own doors set
it to the scheme the caller used), otherwise from `X-Forwarded-Proto` (a
proxy in front of weft that terminated TLS says `https` there), and is
`http` when neither says `http` or `https`. If the proxy mounts weft under a
path, it has to send that path in `X-Forwarded-Prefix`. For a push, `{url}`
is the receiver's address, `<your public address>/events/<service>/<topic>`,
with no query.

## How the check is made

`secret` and `algorithm` only apply to `hmac`, and `encoding` to both:

| Key | Default | Set it when |
|---|---|---|
| `secret.stripPrefix` | none | The provider's secrets start with a marker that is not part of the key, like `whsec_`. A stored secret without that marker is refused |
| `secret.decode` | `raw` (the secret's bytes) | The key is written `base64` (Standard Webhooks) or `hex` |
| `algorithm` | `sha256` | The provider uses `sha1` (Twilio) or `sha512` |
| `encoding` | `hex` | The digest or signature is written `base64`. Hex is read in either case, base64 with or without its `=` padding |

A `signature` block also needs `scheme`, which has no default: `ed25519` (the
public key written as 64 hex characters, the way Discord shows it) or
`ecdsa_p256` (the public key as base64 DER, the way SendGrid shows it). A
`signature` block that carries `secret` or `algorithm` is refused.

## What weft refuses when it loads the block

The rules are the same for a push and for a gated route. They run when the node's
`metadata.json` is loaded, and a broken block fails there with an error naming
the service and the rule it broke:

- **The concat has to cover the request**: `{body}`, `{sorted_form_params}`
  or at least one `{body:<path>}`. A signature over the method and the address
  alone would verify any body.
- **A timestamp is signed, or not declared at all.** One the concat never
  covers proves nothing, since anyone can send any timestamp, and a concat
  that contains `{timestamp}` with no `timestamp` declared could never be
  filled in. A timestamp counts as covered when the concat contains
  `{timestamp}`, its own header, its own body field, or the whole body it sits
  in (Linear's case).
- **A concat that covers only fields of the body has to sign a timestamp.**
  The rest of the body is then outside the signature, so what stops somebody
  sending a captured signature again with other contents is the tolerance.
  That is Mailgun's design: it signs a timestamp and a random token, not the
  event.
- **The concat cannot contain the signature itself**, since the provider signs
  before the signature exists.
- Smaller ones: `pair` needs `key` and `key` needs `pair`; `list` and `pair`
  cannot be the same separator, and neither can be empty; `toleranceSecs`
  cannot be 0; a `signature:<key>` timestamp needs `pair` set and a different
  key from the signature's; and the signature sits in a header or a body
  field, never in `signature:<key>`.

## Blocks for common providers

Standard Webhooks (Svix, Resend, Whop):

```json
{
  "kind": "hmac",
  "signature": { "in": "header:webhook-signature", "list": " ", "pair": ",", "key": "v1" },
  "timestamp": { "in": "header:webhook-timestamp" },
  "concat": "{header:webhook-id}.{timestamp}.{body}",
  "secret": { "stripPrefix": "whsec_", "decode": "base64" },
  "encoding": "base64"
}
```

Stripe:

```json
{
  "kind": "hmac",
  "signature": { "in": "header:Stripe-Signature", "list": ",", "pair": "=", "key": "v1" },
  "timestamp": { "in": "signature:t" },
  "concat": "{timestamp}.{body}"
}
```

Paddle:

```json
{
  "kind": "hmac",
  "signature": { "in": "header:Paddle-Signature", "list": ";", "pair": "=", "key": "h1" },
  "timestamp": { "in": "signature:ts", "toleranceSecs": 5 },
  "concat": "{timestamp}:{body}"
}
```

Slack:

```json
{
  "kind": "hmac",
  "signature": { "in": "header:X-Slack-Signature", "pair": "=", "key": "v0" },
  "timestamp": { "in": "header:X-Slack-Request-Timestamp" },
  "concat": "v0:{timestamp}:{body}"
}
```

Twilio:

```json
{
  "kind": "hmac",
  "signature": { "in": "header:X-Twilio-Signature" },
  "concat": "{url}{sorted_form_params}",
  "algorithm": "sha1",
  "encoding": "base64"
}
```

Mailgun:

```json
{
  "kind": "hmac",
  "signature": { "in": "body:signature.signature" },
  "timestamp": { "in": "body:signature.timestamp" },
  "concat": "{timestamp}{body:signature.token}"
}
```

Linear:

```json
{
  "kind": "hmac",
  "signature": { "in": "header:Linear-Signature" },
  "timestamp": { "in": "body:webhookTimestamp", "format": "milliseconds", "toleranceSecs": 60 },
  "concat": "{body}"
}
```

Shopify's block is GitHub's with no `pair` or `key`, plus `"encoding":
"base64"` and the header `X-Shopify-Hmac-SHA256`.

Discord:

```json
{
  "kind": "signature",
  "scheme": "ed25519",
  "signature": { "in": "header:X-Signature-Ed25519" },
  "timestamp": { "in": "header:X-Signature-Timestamp" },
  "concat": "{timestamp}{body}"
}
```

SendGrid's block is Discord's with `"scheme": "ecdsa_p256"`, `"encoding":
"base64"`, and the headers `X-Twilio-Email-Event-Webhook-Signature` and
`X-Twilio-Email-Event-Webhook-Timestamp`.

## When a request is refused

If the check fails, the sender sees a flat `401` with the body `refused`,
whichever part failed, because a checker that says which part failed tells a
forger what to fix next. The reason goes to the runtime's log (`weft daemon
logs` on your machine): look for `caller refused` (target
`weft_broker::caller_auth`) on a gated route, `push refused` (target
`weft_broker::events`) on a push.

| Reason in the log | What to look at |
|---|---|
| `the request carries no header:X-Signature (the signature)` | The sender is not signing, or the block names the wrong header |
| `the request carries no signature under header:X-Signature` | The header arrived, but no entry in it sits under `key`: check `list`, `pair` and `key` against a real header value |
| `the request's signature does not match` | The wrong secret or key is stored, or the concat differs from what the provider signs. On a gated route that signs `{url}`, also check the address weft rebuilt (above). For `api_keys`, the key the caller sent is not in `keys`. For `token_echo`, the token at `tokenPath` is not the one weft minted for the subscription `idPath` names |
| `the request's timestamp is 412s away from now, outside the replay window` | An old request sent again, or a clock that is off. If the provider documents a wider window, set `toleranceSecs` to it |
| `the request's header:X-Timestamp is not a timestamp in the format the scheme declares` | `format` does not match how the provider writes it |
| `the request carries the webhook-timestamp header more than once with different values, ...` | The sender, or a proxy in front of weft, adds the header twice. The same goes for any header a topic routes by (its `fields`, its account, `idPath` or `tokenPath`) |
| `no signing secret is configured for this service, ...` | No secret is stored: the connection has no `signingSecret`, or the app's `events` entry has none |
| `no usable signing secret (it lacks the prefix ...) is configured ...` | The secret was pasted without its `whsec_`: paste it whole |
| `no public key is configured for this service, ...`, or `no usable public key (...) is configured ...` | No public key is stored, or it is not in the form `scheme` expects |
| `the request's identity token is not usable: ...` | The text after the colon says which check failed: the token expired, came from another issuer or for another audience, or is not signed RS256; on a push, it lacks `email_verified: true` or its `email` is not the app's `pushEmail`. `unknown signing key` or `key fetch failed` means weft could not get the token's key from `jwksUrl`: the text after it says whether the address did not answer in time, answered an error, or answered no key set. A failed fetch is not tried again for a minute, so after you fix the address, tokens keep failing with the same reason for up to that minute |
| `the issuer's signing keys cannot be fetched: the jwksUrl '...' is not an https:// address, ...` (or `names no host`) | The connection's (or the recipe's) `jwksUrl` is plain `http`, or not an address at all: set it to the issuer's `https://` address |
| `no push audience is configured for this service, ...` | An `oidc` push where the receiving apps name no `audience` and this install has no public address to use instead: set `audience` in an app's `events` entry, or turn on a public address with `./setup.sh --public-url` |
| `no api key list is configured for this service, ...` | The connection's `keys` value holds no key (it is empty, or only commas and blank lines): reconnect it with at least one key |
| `no receiving app is configured for this service, ...` | A push arrived for a service with no app in the apps file that has an `events` entry |
| `no receiving app verifies it (app '...': ...; app '...': ...)` | With `hmac` or `signature`, every receiving app was tried and none verified the push. Each app's own reason follows its client id: a missing or malformed secret on one app is only that app's, so check the one the provider signs with |
| `no public receiver address is configured ...` | The concat signs `{url}`. On a gated route, the request named no host to rebuild the address from. On a push, this install has no public address: turn one on with `./setup.sh --public-url` |

A push for a service no project here has connected yet, or for a topic its
`events` block does not declare, gets `404` instead. On a gated route, if weft
cannot reach the broker to make the check, the caller gets `503` with `the
caller could not be checked; try again in a moment`.
