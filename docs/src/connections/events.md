# Events from a service

Some triggers fire when something happens at an outside service: a Slack
message lands, a Drive file changes, an email arrives.

## The two directions

Every service delivers events in one of two directions, and some offer both.

**Your weft calls the service and keeps a line open.** Weft signs in, asks for
a private connection address, connects out, and holds that line while the
service sends each event down it. Because your weft made the call, this works
anywhere with ordinary internet access, including a laptop behind a home
router.

**The service sends each event to your weft.** The service is given an address
and posts every event to it, the way any webhook works. That address has to be
reachable from the internet, which a laptop normally is not.

## A trigger never asks you to pick

It looks at the connection you wired in and uses what that connection can do.

| Trigger | What it uses |
|---|---|
| **Slack Receive Message** (your bot, your workspace) | if your connection is through your own Slack app and you pasted its app-level token, weft holds a line open and nothing is exposed. Otherwise Slack must send events in. |
| **Slack App Messages** (you own the app; every workspace that installed it) | always the held line, always through your app's app-level token, never a public address |
| **Google Drive changes** | Google only sends. There is no line to hold. This always needs your weft reachable. |
| **Email arrivals** | the mailbox is watched over IMAP, a line weft dials out, so no public address is ever needed |

Slack App Messages is a separate node from Slack Receive Message because it
answers a different question: not "what lands in my channel" but "what lands
anywhere my app is installed". See
[one node, one process](../nodes/what-a-node-is.md#one-node-one-process).

<!-- SYNC: this section <-> crates/weft-access-store/src/subscriptions.rs (no_public_url_error) -->
## "requires your weft to be reachable from the internet"

When you activate a project whose trigger needs events sent in, and your weft
has no public address, activation stops with that message.

**Use the direction that needs no public address**, where the service offers
one. For Slack: connect through your own app and paste its app-level token on
the connection.

**Or give your weft a public address**, covering exactly its trigger
surface:

```bash
./setup.sh --public-url
```

Then activate the project again.

## What `--public-url` actually does

Two small pieces start inside your local cluster.

An **outbound tunnel**: your machine dials out to a relay service and receives a
random public `https://` address. The relay forwards what arrives there back
down the same connection. Nothing on your machine starts listening for inbound
connections, and no router configuration is involved.

A **filtering proxy** between the tunnel and weft. Four addresses pass through
it, and nothing else does:

- `/events/...`, where services deliver event pushes. Every push is verified
  (a signature, a secret only the real service and your weft share) before
  anything acts on it.
- `/signal/...`, the per-signal fire links weft mints, each protected by its
  own unguessable token.
- `/public/files/...`, the share links you mint yourself for stored files. A
  link carries its own unguessable token, expires, and only ever serves the one
  file it names. See [Files](../running/files.md).
- `/access/oauth/callback`, exactly that path, where a provider hands back the
  code at the end of a sign-in you started.

Plus the root, which shows a small weft page, and the icon on it. Every other
address answers "not found", so your projects, executions, and settings are not
reachable through the tunnel, and it cannot be used to browse or operate your
weft.

The address is printed when the install finishes, and `weft daemon status`
prints it any time after.

## Two things to know about the address

**It is random, and it changes whenever the tunnel reconnects.** A reboot, a
cluster restart, a dropped connection. Anything registered against the old
address at a provider (Slack's event request URL, an OAuth redirect URL) stops
working until you re-register it.

When it changes, run `./setup.sh --public-url` again so active triggers are
re-pointed at the new address, then update the provider-side registrations. If
you own any domain, give your weft a permanent hostname instead and the
registrations never rot: [A public address](public-address.md).

**Anyone who knows the address can send requests at those four addresses**,
which is what they are built for: every push is signature-verified and every
fire link carries its own unguessable token. When you no longer need triggers
delivered from outside, close it:

```bash
./setup.sh --no-public-url
```

## Per-service setup notes

**Slack, through your own app** (no public address needed): in your app's
settings, enable Socket Mode and mint an app-level token with the
`connections:write` scope, under Basic Information then App-Level Tokens. Paste
it in the connection's app token field.

**Slack, events sent to your weft**: under Event Subscriptions, set the Request
URL to `<your public address>/events/slack/messages`, subscribe the bot to
`message.channels`, and put the app's Signing Secret in your `access-apps.json`
under that app's `events` block.

**Google Drive watch**: Google only pushes to addresses on domains verified in
the app's Google console, so register your public address's domain there. Weft
renews the watch automatically while the trigger is active.

**Email**: nothing to set up. The `email` package watches the inbox over IMAP,
a connection weft dials out, so it works with no public address on any provider
serving IMAP. With Gmail or Outlook, use an app password.
