# Event triggers: how outside services wake your projects

Some trigger nodes fire when something happens at an outside service:
a Slack message lands, a Drive file changes, an email arrives. This
page explains how those events reach your weft, why some triggers need
your weft to be reachable from the internet, and what to do when weft
tells you it is not.

## The two ways events travel

Every service delivers events in one of two directions, and sometimes
offers both:

**Your weft calls the service and keeps a line open.** Weft signs in,
asks the service for a private connection address, connects out, and
holds that line; the service sends each event down it. Because your
weft made the call, this works from anywhere: a laptop behind a home
router, an office network, anywhere with ordinary internet access.
Nothing about your machine is exposed.

**The service sends each event to your weft.** The service is given an
address and posts every event to it, the way any webhook works. That
address has to be reachable from the internet, which a laptop normally
is not. Most services only offer this direction.

A trigger node never asks you to pick. It looks at the connection you
wired in and uses what that connection can do:

- **Slack Receive Message** (your bot, your workspace): if your
  connection is through your own Slack app and you pasted its
  app-level token (the `xapp-...` one), weft keeps a line open to
  Slack and nothing needs to be exposed. Otherwise Slack must send
  events to your weft.
- **Slack App Messages** (you own the app; every workspace that
  installed it): always the held line, always through your own app's
  app-level token, never a public address. This is a separate node
  because it answers a different question: not "what lands in my
  channel" but "what lands anywhere my app is installed".
- **Google Drive changes**: Google only sends; there is no line to
  hold open. This always needs your weft reachable.
- **Email arrivals** (the `email` package): the mailbox is watched
  over IMAP, a line weft dials out and holds, so no public address is
  ever needed.

## "This trigger needs your weft to be reachable from the internet"

When you activate a project whose trigger needs the service to send
events in, and your weft has no public address, activation stops with
that message. Nothing is silently half-working: the trigger either
serves or tells you why not.

You have two ways forward:

1. **Use the direction that needs no public address**, where the
   service offers one. For Slack: connect through your own app and
   paste its app-level token on the connection.
2. **Give your weft a public address for exactly its trigger surface**:

   ```
   ./setup.sh --public-url
   ```

   then activate the project again.

## What `--public-url` does, precisely

It starts two small pieces inside your local weft cluster:

- an **outbound tunnel**: your machine dials out to a relay service
  (Cloudflare) and receives a random public `https://` address; the
  relay forwards what arrives there back down the same connection.
  Nothing on your machine starts listening for inbound connections,
  and no router configuration is involved.
- a **filtering proxy** between the tunnel and weft. Only two kinds of
  address pass through it:
  - `/events/...`: where services deliver event pushes. Every push is
    verified (a signature, a secret only the real service and your
    weft share) before anything acts on it.
  - `/signal/...`: the per-signal fire links weft mints, each
    protected by its own unguessable token.

  The bare root shows a small weft page; everything else (your
  projects, executions, files, settings) answers "not found". The
  tunnel cannot be used to browse or operate your weft.

The address is printed when the install finishes, and
`weft daemon status` prints it any time after.

Two honest caveats:

- The address is **random and changes when the tunnel reconnects**
  (a reboot, a cluster restart, even a dropped connection). Anything
  registered against the old address at a provider (Slack's event
  request URL, an OAuth redirect URL) stops working until you
  re-register it. If that happens, run `./setup.sh --public-url`
  again so active triggers are re-pointed at the new address, and
  update the provider-side registrations. This gets annoying fast for
  services like Slack; if you have any domain, use a stable
  hostname instead (next section).
- A public address is a public address: anyone who knows it can send
  requests at those two surfaces. They are built for that (verified
  pushes, unguessable tokens), but if you no longer need triggers
  delivered from outside, close it:

  ```
  ./setup.sh --no-public-url
  ```

## A stable public address (recommended)

If you own any domain on Cloudflare, a named tunnel gives your weft a
permanent address instead of the rotating random one, so provider-side
registrations never rot. The walkthrough lives in
[stable-public-address.md](stable-public-address.md).

**Slack, through your own app** (no public address needed): in your
app's settings, enable Socket Mode and mint an app-level token with
the `connections:write` scope (Basic Information, App-Level Tokens).
Paste it in the connection's app token field.

**Slack, events sent to your weft**: in the app's settings under Event
Subscriptions, set the Request URL to
`<your public address>/events/slack/messages`, subscribe the bot to
`message.channels`, and put the app's Signing Secret in your
`access-apps.json` under the app's `events` block.

**Google Drive watch**: Google only pushes to addresses on domains
verified in the app's Google console; register your public address's
domain there. Weft renews the watch automatically while the trigger is
active.

**Email**: nothing to set up. The `email` package watches the inbox
over IMAP, a connection weft dials out, so it works with no public
address on any provider that serves IMAP (with Gmail or Outlook, use
an app password).
