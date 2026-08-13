# A stable public address for your weft

`./setup.sh --public-url` gives your weft a public https address for
its trigger surface (see [event-triggers.md](event-triggers.md) for
what is and is not exposed). By default that address comes from a free
Cloudflare quick tunnel and is **random**: it changes whenever the
tunnel reconnects (a reboot, a cluster restart, even a dropped
connection), and everything registered against the old address at a
provider (Slack's event request URL, an OAuth redirect URL) silently
stops working until you re-register it.

If you own a domain on Cloudflare, ten minutes of setup gives your
weft a **permanent** address instead, and provider registrations never
rot. This guide walks through it.

## What you need

- A domain managed by Cloudflare (any plan, the free one works).
- A subdomain name for this weft. Pick something unguessable, e.g.
  `weft-dev-amber-comet.example.com`. It stays effectively
  private: Cloudflare's wildcard certificate covers it (so the name
  never appears in public certificate-transparency logs) and DNS
  names cannot be enumerated. Even so, what it exposes is only weft's
  filtered trigger surface, which is built to face the internet.

## Create the tunnel (Cloudflare dashboard)

1. Open the Zero Trust dashboard: `one.dash.cloudflare.com`.
2. **Networks → Tunnels** → **Create a tunnel** → connector type
   **Cloudflared**. Name it anything (`weft-local`). Save.
3. The next step shows connector install commands. **Ignore the
   install instructions** (weft runs the connector inside its own
   cluster); you only need the **token**: the long `eyJ...` string
   after `--token` (Docker tab) or after `service install`. Copy it.
   You can retrieve it again any time: open the tunnel → Edit; a
   "Refresh token" option there rotates it if it ever leaks.
4. **Public Hostname** tab → **Add a public hostname**:
   - **Subdomain**: your chosen name (e.g. `weft-dev-amber-comet`)
   - **Domain**: your domain
   - **Path**: leave empty
   - **Type**: `HTTP`
   - **URL**: `weft-public-proxy.weft-system.svc.cluster.local:8080`

   This tells Cloudflare "whatever arrives at that subdomain, hand it
   down the tunnel to weft's filtering proxy". Cloudflare creates the
   DNS record automatically; you never touch DNS yourself.

## Point weft at it

In your shell or the repo `.env`:

```
WEFT_PUBLIC_TUNNEL_TOKEN=eyJ...
WEFT_PUBLIC_TUNNEL_HOSTNAME=https://weft-dev-amber-comet.example.com
```

Both must be set together; setting one without the other fails loudly.
Then:

```
./setup.sh --public-url      # first time (persists the choice)
weft daemon start            # any later restart
```

Two signs it worked:

- the daemon prints
  `public trigger surface reachable at https://<your hostname>`
- the tunnel's page in the Cloudflare dashboard flips to **Healthy**

To double-check from the outside, open `https://<your-host>/` in a
browser: you should see a small weft page. That page is served by
weft's filtering proxy inside your cluster, so seeing it proves the
whole chain works (DNS, Cloudflare, the tunnel, into your cluster).
Every other path answers "not found" on purpose: only the trigger
paths are exposed. If something were broken you would see a Cloudflare
error page or a timeout instead.

## Register the address at providers, once

- **Slack**: OAuth redirect URL
  `https://<your-host>/access/oauth/callback`; Event Subscriptions
  request URL `https://<your-host>/events/slack/messages`;
  Interactivity (button approvals) `https://<your-host>/events/slack/interactions`.
- Everything else in [event-triggers.md](event-triggers.md)'s
  per-service notes that says "your public address" means this
  hostname now.

Unset the two variables (and rerun the daemon) to fall back to the
random quick tunnel.
