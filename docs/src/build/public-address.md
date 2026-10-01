# A public address

If you want Slack, Stripe or Telegram to push events into a program running on
your laptop, they need somewhere to push them. Your laptop is behind a router
and has no address they can reach, so weft opens one for you:

```bash
./setup.sh --public-url
```

That starts a tunnel from your machine to a separate port (14112) where weft
serves a short list of paths and nothing else. The choice sticks, so later
installs
keep it open until you say otherwise.

## What it exposes, and what it does not

These paths, and nothing else. Everything else on that address answers 404.

| Path | What arrives there |
|---|---|
| `POST /events/<service>/<topic>` | A provider pushing you an event. A GET there answers 404 |
| `POST /signal/<token>` | Somebody answering a waiting step through its link |
| `DELETE /signal/<token>` | Somebody cancelling that waiting step through the same link |
| `POST /signal/<token>/skip` | Somebody skipping that waiting step |
| `GET /signal-token/signals` | A client holding a signal token, listing the steps waiting on it |
| `DELETE /signal-token/signals` | That client clearing every step it listed |
| `GET /signal-token/signals/<signal>/files/<field>` | That client asking for a fresh link to a file a listed form shows |
| `GET /signal-token/health` | That client checking its token still works |
| `GET /signal-token/displays` | That client listing what your nodes are showing (a QR code, say) |
| `GET /signal-token/displays/<project>/<node>` | That client reading one node's display |
| `POST /signal-token/displays/<project>/<node>/action` | That client pressing a button on it |
| `/instance/...` | Somebody holding an instance token for one of your programs: that instance's fields, its connections, its runs |
| `GET /public/files/<token>` | Somebody fetching a file link your program handed out |
| `/connect/<tenant>/<path>`, `/live/<project>/...` | A caller of one of your routes or sockets |
| `/infra/<project>/<copy>/...` | A caller of an infrastructure endpoint your node opened to the public |
| `GET /access/oauth/callback` | A provider sending you back after you approve a connection |
| `/access/picker/<state>` | The page where you pick files for a connection, and its answer |
| `POST /<path>` | A trigger of yours that fires on a plain POST to its own path |
| `/`, `/index.html`, `/logo.png` | A small page saying this address is a weft install |

Every `/signal-token/...` request carries the signal token in its
`Authorization: Bearer` header, never in the path, so it stays out of proxy
logs.

Your dispatcher's real API, your projects, your journal and your runs are not
on that port at all, so nothing arriving through the tunnel can reach them.
They stay on `127.0.0.1:14111`, which has no authentication of its own and
listens on your machine only.

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
   network; on macOS, `http://host.docker.internal:14112`.
3. Install with both set:

   ```bash
   WEFT_PUBLIC_TUNNEL_TOKEN=<token> WEFT_PUBLIC_TUNNEL_HOSTNAME=https://weft.example.com ./setup.sh --public-url
   ```

   The hostname is the bare `https://` address, with no port and no path.

Once the tunnel is up, `weft daemon start` fetches the page at your address
through it. If the service in step 2 is wrong, the start fails and says so,
naming the value it expects.

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
