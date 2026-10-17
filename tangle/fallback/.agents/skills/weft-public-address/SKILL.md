---
name: weft-public-address
description: "Read when a trigger needs the internet to reach weft (a route, a provider that pushes its events), when the user asks about a public URL or a tunnel, or when activation fails with \"needs your weft to be reachable from the internet\": what the error means, walking the user through ./setup.sh --public-url, the dial-out alternative, named tunnels, and the refusals that come from Cloudflare or Google rather than weft; and when the user wants other people to reach the project's site, hosting it from this computer behind an address of theirs: the Cloudflare login, the tunnel, starting the site and the tunnel, restarting them after a reboot, and listing and stopping what this machine is hosting."
---

# A public address

Webhook-style triggers (a `Route`, a Drive watch, a Slack app
installed in other workspaces) need the runtime reachable from the
internet. Polling triggers (Telegram, email, sheets, RSS, cron) and
everything local work without one.

**The error, verbatim.** When a project with a dial-in trigger is
activated on a weft the internet cannot reach, activation refuses with
this, and a trigger already on stops with it when it next has to make its
subscription again:

> this trigger needs '<service>' to deliver events TO your weft, which
> requires your weft to be reachable from the internet, and it is not. If
> you accept making its public trigger surface reachable, rerun
> `./setup.sh --public-url`, then `weft activate` (or `weft resync`, when
> the trigger is already on); or use a connection that supports dialing
> out (your own provider app), where the service offers one. See
> https://weavemindai.github.io/weft/connections/events.html

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
4. Run `weft activate`, or `weft resync` if the trigger is already on
   (`weft activate` refuses a trigger that is on). If the same error comes back, the transport
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

## Hosting the site from this computer

When the user wants other people to reach the project's site and there is
no cloud install yet, you offer this before the cloud, in words like these:
"Putting it on the cloud is a bigger step. Do you want to try it from your
computer first? It stays online while your computer is on, at an address of
yours that I set up, and it is fine for a handful of people at once. When
that gets tight, we move it to the cloud." Then add how many people it can
really carry, worked out from the program's heaviest job: a program that
renders one video at a time serves a few people working at once and many
more just browsing. With their yes, you do every step yourself. Their part
is installing `cloudflared` (`cloudflared --version` says whether it is
there) and having a Cloudflare account with a domain on it.

This is the site's own address. The site's server still calls weft on this
machine, so weft itself needs no public address for it. The exception is a
page that holds a socket open or puts a link straight at weft in the
browser: then the browser has to reach weft too, which is the public address
at the top of this page.

1. **Log in.** Run `cloudflared tunnel login` in the background. It prints a
   link: hand the user that link and nothing else, no tutorial, and carry
   on once `~/.cloudflared/cert.pem` exists. That goes for any tool that
   signs in through a link or a code: you run the command, the user only
   clicks. Never read a token out of `cert.pem` to call Cloudflare yourself;
   the `cloudflared` commands below are all you need.
2. **Ask which address.** The subdomain, and which domain if their account
   holds several.
3. **Make the tunnel and its address.** `cloudflared tunnel create <project
   name>` prints the tunnel's id and writes its credentials to
   `~/.cloudflared/<id>.json`. Then `cloudflared tunnel route dns <project
   name> <sub>.<domain>`.
4. **Write its config** into `<project>/.hosting/tunnel.yml` (`<project>` is
   the project folder's absolute path, here and in step 6, so the running
   tunnel's command line names its project), and add
   `.hosting/` to the project's `.gitignore`:

   ```yaml
   tunnel: <id>
   credentials-file: <home>/.cloudflared/<id>.json
   ingress:
     - hostname: <sub>.<domain>
       service: http://localhost:<port>
     - service: http_status:404
   ```

   `<port>` is a free port you pick for the site. `cloudflared tunnel
   --config .hosting/tunnel.yml ingress validate` answers `OK` when the
   file is right.
5. **Build the site and start it, detached.** `pnpm run build` in `front/`,
   then from `front/`: `PORT=<port> ORIGIN=https://<sub>.<domain> nohup node
   --env-file=.env build > ../.hosting/site.log 2>&1 & echo $! >
   ../.hosting/site.pid`. `ORIGIN` tells the server the address people reach
   it at; without it, a form post through the tunnel is refused as
   cross-site. A site on BetterAuth also takes `BETTER_AUTH_URL=https://<sub>.<domain>`
   on the same line, so BetterAuth takes the public address as its own
   (`baseURL`, which it otherwise guesses from each request). A site whose
   pages hold a socket or link straight at weft also takes
   `WEFT_PUBLIC_URL=<weft's public address>` there, once `./setup.sh
   --public-url` has given weft one: `front/.env` says
   `http://127.0.0.1:14111`, which no other computer reaches.
6. **Start the tunnel, detached.** From the project folder: `nohup cloudflared
   tunnel --config <project>/.hosting/tunnel.yml --pidfile
   <project>/.hosting/tunnel.pid run > .hosting/tunnel.log 2>&1 &` (the two
   flags go before `run`, which refuses them after it).
7. **Check it answers.** `curl -sS -I https://<sub>.<domain>/` shows the
   status line. A fresh DNS record can take a minute; `cloudflared tunnel
   info <project name>` says whether the tunnel is connected, and the two
   logs in `.hosting/` say what went wrong if it is not.
8. **Tell the user** the address, that it is online only while this computer
   is on and awake, and what to do after a restart: weft comes back on its
   own (`weft daemon status` says so), the tunnel, its address and its
   config stay, and only the site and the tunnel are started again, with the
   two commands of steps 5 and 6. Offer to do it for them when they ask.

**What this machine is hosting.** If the user asks what is running, or
before you start another site, look: `pgrep -a cloudflared` on Linux
(`pgrep -lf cloudflared` on macOS) lists every tunnel running here with the
config it was started with, whose path names its project; `ss -ltnp` on Linux (`lsof -iTCP -sTCP:LISTEN -n -P` on macOS)
lists the ports listening and the process behind each; each project hosted
this way has its pid files in `.hosting/`. `cloudflared tunnel list` shows
every tunnel of the account and whether it is connected. To stop one, `kill`
the pid from its project's `.hosting/site.pid` and `.hosting/tunnel.pid`, or
the one the listing showed, never `pkill -f`, whose pattern matches your own
shell too. Stopping a site that belongs to another project is the user's
call: ask first, naming it.
