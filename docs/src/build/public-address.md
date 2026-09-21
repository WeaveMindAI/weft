# A public address

If you want Slack, Stripe or Telegram to push events into a program running on
your laptop, they need somewhere to push them. Your laptop is behind a router
and has no address they can reach, so weft opens one for you:

```bash
./setup.sh --public-url
```

That builds an outbound tunnel from your machine and puts a filtering proxy in
front of it. The choice sticks, so later installs keep it open until you say
otherwise.

## What it exposes, and what it does not

Two paths, and nothing else. Everything else on that address answers 404.

| Path | What arrives there |
|---|---|
| `/events/<service>/<topic>` | A provider pushing you an event |
| `/signal/<token>` | Somebody answering a waiting step through its link |

Your dispatcher's real API, your projects, your journal and your runs are not
on that address at all. They stay on `127.0.0.1:9999`, which has no
authentication of its own, so keep it on an interface you control.

## Find your address

```bash
weft daemon status
```

```text
daemon: running (cluster 'weft', system ns 'weft-system'); 1 project(s)
public trigger surface: https://something.example/ (events + signal fire routes only)
```

The installer prints it too, when it is open.

Hand a provider the full path for what it is sending, not the bare address:
`https://something.example/events/slack/messages`. Each trigger's connect panel
shows you the exact one it wants.

## It changes when the tunnel restarts

This is the thing that bites. The address is not yours to keep. When the tunnel
comes back up with a different one, anything you registered against the old one
is pointing at nothing, and events stop arriving without an error anywhere:
the provider is posting into the void.

So after a restart, check `weft daemon status` and re-register the address with
any provider you configured by hand. A Slack app's Request URL and a webhook
you pasted into somebody's dashboard both need it.

## Close it

```bash
./setup.sh --no-public-url
```

The tunnel comes down and the address stops answering.

## When you do not need it

Triggers that go and fetch rather than wait to be pushed work with no public
address at all: a timer, a poller, anything weft opens an outgoing connection
for. So do runs you start yourself, and so does a person answering through the
browser extension, because that talks to your runtime directly.

You need this page when a provider insists on pushing. For which triggers those
are and how a service declares its event transport, go and read
[events from a service](../connections/events.md).
