# A public address

You need a public address when a provider has to send events in, or when
somebody outside needs a signal or a file link.

For a throwaway address:

```bash
./setup.sh --public-url
weft daemon status
```

That starts a Cloudflare quick tunnel and reports a random hostname, which can
change whenever the tunnel restarts. For a hostname that stays put, keep
reading.

The tunnel dials out from the cluster, so you do not need to forward a port on
your router.

## What the outside world can reach

Requests go through weft's filtering proxy, which allows exactly these:

| Route | For |
|---|---|
| `/events/...` | Providers posting events in, each one checked against the signature the service declares |
| `/signal/...` | Firing a signal with its token |
| `/signal-token/...` | Reading what a signal token may see, for a caller holding that token |
| `/public/files/...` | Reading a file with an expiring share link |
| `/access/oauth/callback` | Finishing an OAuth connection |

The landing page and its logo are public too. Everything else answers 404, and
the management API is not behind the proxy at all.

Signal tokens and file links are credentials: whoever holds one has the access
it grants.

<!-- SYNC: weft-system <-> crates/weft-core/src/infra/mod.rs (SYSTEM_NAMESPACE) -->

## A hostname that stays put

You need a domain on Cloudflare and a named tunnel. In the [Cloudflare
dashboard](https://dash.cloudflare.com/), under **Networking → Tunnels**,
create a tunnel and copy its connector token. weft runs the connector for you,
inside your own weft cluster, so there is nothing to install on your machine.

Add a **Published application** route to that tunnel:

| Field | Value |
|---|---|
| Hostname | Your subdomain, such as `weft.example.com` |
| Service URL | `http://weft-public-proxy.weft-system.svc.cluster.local:8080` |

Cloudflare's own [tunnel
setup](https://developers.cloudflare.com/tunnel/setup/) covers their side.

Then set both of these where you run the daemon:

```bash
export WEFT_PUBLIC_TUNNEL_TOKEN='paste-your-tunnel-token'
export WEFT_PUBLIC_TUNNEL_HOSTNAME='https://weft.example.com'
./setup.sh --public-url
```

Both have to be there, and the hostname has to be HTTPS with no port, path or
query. A `.env` file works too, with the same two names and no `export`. If a
name is in both places, what you exported is what weft uses. weft remembers
that
you want a public URL, so later starts keep one, but it does not remember your
token. Start without those two set and you get a quick tunnel again, on a new
random hostname, so keep them somewhere you can find them.

Open the address in a browser afterwards. weft's landing page means requests
are reaching the proxy. It says nothing about whether a provider's credentials
or subscriptions are right.

Then register that hostname wherever your connection needs it, such as an
OAuth callback or a webhook URL. For the event routes, read [events from a
service](events.md).

## If Cloudflare refuses a program's request

File links get fetched by programs, not just browsers. Cloudflare's Browser
Integrity Check can challenge an unfamiliar user agent, and it does that at
the edge, before the request ever reaches weft.

If Cloudflare's security events name that check, add a configuration rule for
your weft hostname and turn **Browser Integrity Check** off for it, following
[their
instructions](https://developers.cloudflare.com/waf/tools/browser-integrity-check/).
Then try again. If the 403 survives that, something else is causing it, and
Cloudflare's security event log will name what.

## Changing or closing the tunnel

To go back to a quick tunnel, take both named-tunnel settings out of your
environment and any `.env`, then `./setup.sh --public-url` again. Check
the new address and update anything registered against the old one.

To close the tunnel entirely:

```bash
./setup.sh --no-public-url
```
