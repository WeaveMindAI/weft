# A public address

If you want Slack, Stripe or Telegram to push events into a program running on
your laptop, they need somewhere to push them. Your laptop is behind a router
and has no address they can reach, so weft opens one for you:

```bash
./setup.sh --public-url
```

That opens a tunnel from a public address to a port of its own on your machine
(14112 on the default install), where weft serves a short list of paths. The
choice sticks: later runs of `./setup.sh` keep the tunnel open until you pass
`--no-public-url`.

## What it exposes, and what it does not

These paths, and nothing else. Everything else on that address answers 404.

| Path | What arrives there |
|---|---|
| `POST /events/<service>/<topic>` | A provider pushing you an event. A GET there answers 404 |
| `POST /signal/<token>` | Somebody answering a waiting node through its link |
| `DELETE /signal/<token>` | A client cancelling the run waiting behind that link, and every other wait of that run with it. It needs a signal token minted without `--tags`: either a token for whole projects, or an instance token (`weft token mint --instance`), which can only cancel its own instance's waits. On a trigger's link, it drops that trigger's signal instead |
| `POST /signal/<token>/skip` | Somebody skipping that waiting node |
| `GET /signal-token/signals` | A client holding a signal token, listing the nodes waiting on it |
| `DELETE /signal-token/signals` | That client cancelling every run it can see waiting on anything (a form, a timer, a poll), and dropping every trigger signal it can see, listed or not: a webhook's or a schedule's too. It needs a token that covers whole projects (minted with `weft token mint` without `--tags`, not an instance token) |
| `GET /signal-token/signals/<signal>/files/<field>` | That client asking for a fresh link to a file a listed form shows |
| `GET /signal-token/health` | That client checking its token still works |
| `GET /signal-token/displays` | That client listing what your nodes are showing (a QR code, say) |
| `GET /signal-token/displays/<project>/<node>` | That client reading one node's display |
| `POST /signal-token/displays/<project>/<node>/action` | That client pressing a button on it |
| `/instance/...` | Somebody holding an instance token for one of your programs: that instance's fields, its connections, its runs (a failed run shows only `Something went wrong`; the reason stays on the run's record, where `weft logs <run id>` shows it to you) |
| `GET`, `HEAD` or `PUT /files/runtime/...` | Somebody opening a file link your program handed out, or a page sending a part of an upload. The link's own signature is what lets it in |
| `POST /files/upload/<step>` | A page uploading a file through an upload slot your program handed it |
| `/connect/<tenant>/<project id>/<path>` | A caller of one of your routes or sockets |
| `/infra/<project>/<copy>/...` | A caller of an infrastructure endpoint your node opened to the public |
| `GET /access/oauth/callback` | A provider sending you back after you approve a connection |
| `/access/picker/<state>` | The page where you pick files for a connection, and its answer |
| `POST /<tenant>/<project id>/<path>` | A trigger of yours that fires on a plain POST to its own path |
| `/`, `/index.html`, `/logo.png` | A small page saying this address is a weft install |

Every `/signal-token/...` request, and a `DELETE /signal/<token>`, carries the
signal token in its `Authorization: Bearer` header, so it stays out of proxy
logs. The `<token>` in a `/signal/` path is a different one: the link's own
token, which is made to be pasted.

Your dispatcher's real API, your projects, your journal and your runs are not
on that port at all, so nothing arriving through the tunnel can reach them.
They stay on `127.0.0.1:14111` (the default install's address; `weft daemon
which` gives another's), which listens on your machine only, asks
every caller for an operator key and sends no CORS headers. For
how that key works, go and read [the key the install's API asks
for](../running/architecture.md#the-key-the-installs-api-asks-for).

## Find your address

```bash
weft daemon status
```

```text
weft: running at http://127.0.0.1:14111 (install 'default'); 1 project(s)
public address: https://something.example (./setup.sh --no-public-url closes it)
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

## A stable address of your own

If you want an address that never changes, run the tunnel under a hostname you
own. You need a Cloudflare account with that domain on it.

1. In the Cloudflare dashboard, create a tunnel (Zero Trust, then Networks,
   then Tunnels) and copy its token.
2. Add a public hostname to that tunnel, say `weft.example.com`, and set its
   service to the port weft serves those paths on: on Linux,
   `http://127.0.0.1:14112`, because the tunnel shares your machine's
   network; on macOS, `http://host.docker.internal:14112`. 14112 is the
   default install's port. A branch's install has its own: the `outside` value
   in `ports.json`, in the folder `weft daemon which` prints.
3. Install with both set:

   ```bash
   WEFT_PUBLIC_TUNNEL_TOKEN=<token> WEFT_PUBLIC_TUNNEL_HOSTNAME=https://weft.example.com ./setup.sh --public-url
   ```

   The hostname is the bare `https://` address, with no port and no path.

Once the tunnel is up, `weft daemon start` fetches the page at your address
through it. If the service in step 2 is wrong, the start fails and says so,
naming the value it expects.

The install remembers that tunnel, so a later `weft daemon start` or `weft
daemon restart` from a shell that sets neither variable opens the same one
again. If you set both, they win, and the install remembers the new ones. To
go back to a quick tunnel, run `./setup.sh --no-public-url`, then
`./setup.sh --public-url` with neither variable set.

## Close it

```bash
./setup.sh --no-public-url
```

The tunnel comes down and the address stops answering.

## Devices on your own network

If you want a phone or another computer on your wifi to open what your program
hands out, without the internet in between, give weft this machine's address
on that network:

```bash
WEFT_NETWORK_ADDRESS=192.168.1.20 weft daemon start
```

```text
network address: http://192.168.1.20:14112 (the doors outside callers use, for devices on this machine's network; a start with WEFT_NETWORK_ADDRESS=off closes it)
```

weft then answers the paths in the table above on that address too, on the
port the tunnel uses (14112 on the default install), and nothing else: your projects and runs stay on `127.0.0.1`. weft
remembers the address for every later start. If you want to close it, start
once with `WEFT_NETWORK_ADDRESS=off`.

It takes the bare IP (`ip -brief address` lists yours), and weft adds the
scheme and the port itself. The start refuses a value with a scheme, a port or
a path. It also refuses a loopback address (no other device can reach
it), `0.0.0.0` or `::` (that would open these paths on every network this
machine is on), a link-local IPv6 one starting `fe80:`, and one this machine
does not have. Each refusal names
what to give instead.

## Your own site in front

If your project has its own site and you want links to its stored files to
go through that site, tell weft the address where the site passes its
visitors' calls on to weft (the `weftPassThrough` helper from `@weft/connect/server`,
mounted at `/weft` on the site):

```bash
weft frontend visitor-address shop https://shop.example/weft
```

`shop` is the name you gave the frontend with `weft frontend add`. If you are
making the frontend now, `weft frontend add shop --visitor-address
https://shop.example/weft` sets it in the same step. To clear it, give `off` in
place of the address. One
frontend per project can hold it: asking for a second is refused, naming the
frontend that has it.

## Which address a file link gets

When your program hands somebody outside a link to a stored file (a file in a
route's answer, say), the link goes through this tunnel only on a local install
whose project has no visitor address. With the tunnel closed too, it goes to
the install's own address: the network address if you set one, which only
devices on your network can open, otherwise `127.0.0.1`, which only this
machine can open. For the full order and what a node sees, go and read
[handing a file out](../nodes/storage.md#handing-a-file-out).

If your tunnel is the one whose address changes on restart (`./setup.sh
--public-url` without a hostname of your own), weft asks it for its address each time it makes a
link, so a new link always carries the current address. A link handed out
before the tunnel restarted still points at the old one. If the tunnel cannot say its address at that
moment, the link is refused:

```text
the install's tunnel does not say its address right now; `weft daemon start` opens it again
```

## When you do not need it

Triggers that go and fetch rather than wait to be pushed work with no public
address at all: a timer, a poller, anything weft opens an outgoing connection
for. So do runs you start yourself, and so does a person answering through the
browser extension, because that talks to your runtime directly.

You need this page when a provider insists on pushing. For which triggers those
are and how a service declares its event transport, go and read
[events from a service](../connections/events.md).
