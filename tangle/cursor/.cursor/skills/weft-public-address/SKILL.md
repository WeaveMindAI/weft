---
name: weft-public-address
description: "Read when a trigger needs the internet to reach weft (a route, a provider that pushes its events), when the user asks about a public URL or a tunnel, or when activation fails with \"needs your weft to be reachable from the internet\": what the error means, walking the user through ./setup.sh --public-url, the dial-out alternative, named tunnels, and the refusals that come from Cloudflare or Google rather than weft."
---

# A public address

Webhook-style triggers (a `Route`, a Drive watch, a Slack app
installed in other workspaces) need the runtime reachable from the
internet. Polling triggers (Telegram, email, sheets, RSS, cron) and
everything local work without one.

**The error, verbatim.** When a project with a dial-in trigger is
activated on a weft the internet cannot reach, activation refuses with:

> this trigger needs '<service>' to deliver events TO your weft, which
> requires your weft to be reachable from the internet, and it is not. If
> you accept making its public trigger surface reachable, rerun
> `./setup.sh --public-url` and re-activate; or use a connection that
> supports dialing out (your own provider app), where the service offers
> one.

If you see that, or a user pastes it, this is the cause and nothing else.
It is not a broken credential, a wrong path, or a bad subscription. Say so
in one sentence before doing anything else, because the wording sends
people hunting through their provider settings.

**Walking a user through it.** The command is `./setup.sh --public-url`,
always. Never `weft daemon start --public-url`: it exists, but every
message weft prints names setup.sh, and the user should be typing the same
thing the tool told them. Steps:

1. Say what it will do: open a Cloudflare quick tunnel so the provider can
   reach their machine, and expose only the event, signal, file-link and
   OAuth-callback routes. The management API stays unreachable. The
   book's [public address page](https://weavemindai.github.io/weft/build/public-address.html)
   lists the exact routes; offer it, do not paste it.
2. Get their consent before running it, because it makes a surface on
   their machine reachable from the internet. This is one of the
   `ask`-first moves.
3. Run `./setup.sh --public-url`, then `weft daemon status` to read back
   the hostname it minted.
4. Re-activate the project. If the same error comes back, the transport
   was never the problem; go and read the trigger's own metadata for
   which transports its connection actually supports.

**The alternative, and prefer it when it exists.** Several services can
dial out instead. For Slack, pasting an app-level token with
`connections:write` on the connection switches it to Socket Mode and drops
the public-address requirement entirely. Offer that first: it needs no
tunnel and nothing on the user's machine becomes reachable.

**A quick tunnel's hostname changes** whenever the tunnel restarts, so any
URL the user registered by hand at a provider goes stale. If they are
registering webhook URLs anywhere, they want the named-tunnel setup
(`WEFT_PUBLIC_TUNNEL_TOKEN` plus `WEFT_PUBLIC_TUNNEL_HOSTNAME`, both
required, hostname HTTPS with no port or path), which is on the same book
page.

**Google Drive has a second requirement** on top of the address: it must
be on a domain verified in the app's Google console, or Google refuses to
create the watch. That refusal comes from Google, not weft, and no amount
of tunnel work fixes it.

**A `403` carrying `error code: 1010`** on a file link is Cloudflare's
Browser Integrity Check refusing the client (Python's `urllib` is one it
refuses), never weft. The fix is a Cloudflare configuration rule on the
user's side, and the book's public address page walks through it.
