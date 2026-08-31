# A public address

If you want services to be able to reach your weft, `./setup.sh --public-url`
gives it a public https address. By default that address comes from a free
quick tunnel and is **random**: it changes whenever the tunnel reconnects, and
everything you registered at a provider against the old one silently stops
working until you go and re-register it.

And if you own a domain on Cloudflare, the setup below gives your weft a
**permanent** address instead, so those registrations never rot.

## What you need

A domain managed by Cloudflare. Any plan; the free one works.

A subdomain name for this weft. Pick something unguessable, for example
`weft-dev-amber-comet.example.com`. Nothing publishes it, so treat it as a
secret you happen to have typed into DNS, and do not rely on it staying
unknown: what actually protects the surface is that only four addresses pass
through it, each with its own check.

What it exposes is weft's filtered trigger surface, which is built to face the
internet. See
[what the proxy passes](events.md#what---public-url-actually-does).

## Create the tunnel

1. Open the Zero Trust dashboard at `one.dash.cloudflare.com`.

2. **Networks** then **Tunnels** then **Create a tunnel**, connector type
   **Cloudflared**. Name it anything, such as `weft-local`. Save.

3. The next step shows connector install commands. **Ignore them**: weft runs
   the connector inside its own cluster. You only need the **token**, the long
   `eyJ...` string after `--token`. Copy it.

   You can get it again any time by opening the tunnel and clicking Edit, where
   a refresh option also rotates it if it ever leaks.

4. **Public Hostname** tab, then **Add a public hostname**:

   | Field | Value |
   |---|---|
   | Subdomain | your chosen name |
   | Domain | your domain |
   | Path | leave empty |
   | Type | `HTTP` |
   <!-- SYNC: weft-system <-> crates/weft-core/src/infra/mod.rs (SYSTEM_NAMESPACE) -->
   | URL | `weft-public-proxy.weft-system.svc.cluster.local:8080` |

   That tells Cloudflare "whatever arrives at that subdomain, hand it down the
   tunnel to weft's filtering proxy". It creates the DNS record itself.

## Point weft at it

In your shell, or the repo's `.env`:

```bash
WEFT_PUBLIC_TUNNEL_TOKEN=eyJ...
WEFT_PUBLIC_TUNNEL_HOSTNAME=https://weft-dev-amber-comet.example.com
```

Both must be set together. Setting one without the other fails loudly.

```bash
./setup.sh --public-url      # first time; persists the choice
weft daemon start            # any later restart
```

## Two signs it worked

The daemon prints `public trigger surface reachable at https://<your hostname>`.

The tunnel's page in the Cloudflare dashboard flips to **Healthy**.

To check from the outside, open `https://<your-host>/` in a browser. You should
see a small weft page. That page is served by weft's filtering proxy inside
your cluster, so seeing it proves the whole chain: DNS, Cloudflare, the tunnel,
into your cluster.

Every other path answers "not found" on purpose. If something were broken you
would see a Cloudflare error page or a timeout instead.

## Register the address at providers, once

**Slack**: OAuth redirect URL `https://<your-host>/access/oauth/callback`.
Event Subscriptions request URL `https://<your-host>/events/slack/messages`.
Interactivity, for button approvals,
`https://<your-host>/events/slack/interactions`.

Everything else in [Events from a service](events.md) that says "your public
address" means this hostname now.

## Going back

Unset the two variables and rerun the daemon to fall back to the random quick
tunnel. `./setup.sh --no-public-url` closes the surface entirely.
