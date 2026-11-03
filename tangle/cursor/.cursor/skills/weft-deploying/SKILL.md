---
name: weft-deploying
description: "Read when the user wants weft or a program live for real people on their cloud, asks about prod, a target, `--on`, a domain or GCP, or a deploy failed: targets and logging in, domains, the deploy workflow `weft ci add` writes, handing it its settings with `weft target export`, a project's frontends (`weft frontend`), what a cloud build is and how it fails, rolling back, and connections on a cloud install. The deployer specialist reads it; you read it before dispatching one."
---

# Deploying

A [target] is one weft install, named in the project's `weft.toml`:

```toml
[targets.prod]
url = "https://weft-role-dispatcher-123456789.us-central1.run.app"
```

`local` is always a target: the install on this machine, at
`http://127.0.0.1:14111` unless it was started on another port (the
`public` field of `~/.local/share/weft/ports.json` says). Every `weft` command acts on `local` unless it is
given `--on <target>` (or `WEFT_DISPATCHER_URL` is set in its environment,
which you never do), and no setting makes another target the default.

The `deployer` specialist runs the deploy procedure: a deploy, a rollback,
setting up the deploy workflow, a failed cloud build. You dispatch it with
what the user asked, and relay its report. The rest you may do on a cloud
install yourself, with `--on <target>` on every command: read what is there
(`weft status`, `weft runs`, `weft logs`, `weft events`), run, resync, start
infra. Before anything that stops or turns off something on a cloud install
(stopping a run, `weft deactivate`, `weft infra stop` or `terminate`, `weft
cancel-activate`), ask the user first, in one plain sentence that says what
will stop. When a program there is failing, stuck or slow for the people
using it, read the weft-incidents skill.

If weft is not on the user's cloud yet, or they want to upgrade or resize
it, Tangle reads the weft-cloud-install skill and does it in
conversation: the user has to log in to `gh` and `gcloud` along the way,
so it is never handed to a specialist, which cannot talk to the user.

## A name instead of the install's own address

The install answers at its own Cloud Run address (`https://...run.app`) from
the start, over HTTPS, for free. That address is enough for the CLI,
frontends and providers' webhooks; a domain gives the install a name people
see.

A domain costs money: it needs a Google load balancer in front of the
install, about $18 a month plus $0.008 per GB through it, for as long as the
install has any domain. weft makes it with the first domain and takes it
down with the last. So before the first domain, tell the user that price
and go on only once they agree. With their yes, the deployer runs `weft domain add <name> --on prod
--accept-cost --no-wait` (without `--accept-cost` the first domain is refused, naming
the price) and hands back the DNS record it prints: type `A` (or `AAAA`),
the name, and the load balancer's IP.

You set that record yourself whenever a CLI or an API can.
`dig NS <zone> +short` says who serves the zone:

- **Cloudflare** (the servers end in `ns.cloudflare.com`): the Cloudflare
  API, with `curl` and a token that can edit DNS on that zone.
  `cloudflared` only makes tunnel CNAMEs, never an `A` record, so it is not
  the tool here. Use `$CLOUDFLARE_API_TOKEN` when it is set. Otherwise ask
  the user once: "In the Cloudflare dashboard, open My Profile, then API
  Tokens, create one from the "Edit zone DNS" template for `<zone>`, and add
  `CLOUDFLARE_API_TOKEN=<the token>` to this project's `.env`." Then load
  it without printing it (`set -a; . ./.env; set +a;` in front of each
  call). Every call carries `-H "Authorization: Bearer $CLOUDFLARE_API_TOKEN"`:
  1. the zone's id: `GET https://api.cloudflare.com/client/v4/zones?name=<zone>`;
  2. what is already on the name: `GET .../zones/<zone id>/dns_records?name=<fqdn>`,
     and `DELETE .../zones/<zone id>/dns_records/<record id>` for each one that
     is not the wanted `A` record (a name that pointed at the user's machine
     through a tunnel has a CNAME to `<tunnel id>.cfargotunnel.com` there);
  3. the record: `POST .../zones/<zone id>/dns_records` with
     `{"type":"A","name":"<fqdn>","content":"<ip>","proxied":false,"ttl":1}`
     (`"AAAA"` when that is the type it printed).

  `"proxied": false` (DNS only, the grey cloud) is required: Google issues
  the certificate only when the record points straight at the load
  balancer's IP, with no proxy or CDN in between.
- **Google Cloud DNS** (servers like `ns-cloud-*.googledomains.com`):
  `gcloud dns managed-zones list --project <project>` names the zone, then
  `gcloud dns record-sets create <fqdn>. --zone=<zone> --type=A --ttl=300
  --rrdatas=<ip> --project <project>`.
- **Anyone else**: one line to the user, "At <registrar>, add an A record:
  name `<sub>`, value `<ip>`, and remove any other record on that name."

Once the record is set, dispatch the deployer to watch the certificate.
`weft domain list --on prod` shows each domain's `certificate: issued`,
`waiting: <why>`, or `failed: <why> (remove the domain and add it again to
retry)`, and `--json` gives the same as `domains[].certificate.state`
(`issued`, `pending` or `failed`, with `why`). It usually takes a few minutes
once Google sees the record. When it is issued, check it yourself with
`curl -sI https://<name>/`.

From then on the domain is the install's address: every link weft hands
out uses it (sign-in callbacks, webhook addresses, file links, `weft
target export`), and the `run.app` address keeps answering. `weft status
--on prod` says so: `install address: <url> (it still answers at <run.app
address> too)`. Two things follow, both from what the deployer reports out
of `weft domain list --on prod --json` (`liveAddress`, `callbackUrl`,
`callbackServices`):

- For each service in `callbackServices`, one line to the user: "In your
  <service> app's dashboard, add `<callbackUrl>` to the allowed redirect
  URLs." No CLI does this for Google, Meta, TikTok, Whop and the like, so it
  is always the user's.
- Run `weft target export prod ...` again, the way it was run before, for
  any site that talks to the install: `WEFT_PUBLIC_URL` changed.

A domain can serve one project instead of the whole install: `--for api`
serves that project's routes at the root of the domain
(`https://api.shop.com/users/42`, on top of
`<install address>/connect/local/<project id>/users/42`), and `--for frontend --to <its
https address>` passes the domain on to the project's frontend (`weft
frontend ls --on prod` shows that address).

## Workers and run length

`weft workers` shows a project's worker settings and which come from the install: copies kept warm (`min_instances`), the most copies (`max_instances`), how many calls Cloud Run sends one copy at once (`concurrency`, 80), CPU (unset is one CPU on a cloud install), memory, how long a copy keeps a pool its runs share once nothing uses it (`shared_idle_seconds`), how many runs one copy takes at once (`max_runs_at_once`, unset is one per MiB of memory and never fewer than 64), and how long a call waits for room before it is turned away (`max_queue_wait_seconds`, 30). Before you run `weft workers set --min-instances 1`, tell the user: it keeps
one copy warm so the first call never waits for a start, and on a cloud
install that copy is billed all the time. On a cloud install one stretch of a run lasts an hour at most (Cloud Run cuts a request there, and the run stops itself a minute before): a run that pauses (every node still running waits, a bus read or a stream pull counting, and at least one waits on a timer, a form or another answer from outside, and no node is reading with `ctx.hold()`) starts a fresh hour when it picks back up, so split work that can take longer with a pause. A pause does not help while a caller is still on the line: such a run's wait holds its copy, and at 59 minutes the client is told to reconnect. With `outlivesCaller` off the run is cancelled then; with it on the caller is dropped and the run goes on, pausing if everything in it waits on a timer or a form, else moving off that copy with a fresh hour. Open buses between its nodes do not stop it pausing either. A run whose trigger leaves `durable` off (the default) ends cancelled if its copy dies mid-run, and is not run again; if a trigger's runs have to carry on after that (they move money, say), set `durable: true` on it.

A trigger that holds a connection open (a stream or a socket it listens to,
an event subscription that dials out) runs on a holder, which weft starts
only while such a trigger is on and which is billed while it runs. Its node
shows "waiting for a holder to take it" until one has. A holder does not
keep the database awake: it hears about changes over Pub/Sub and keeps a
Cloud Tasks check pushed ahead of itself, and neither touches the database.
The database wakes when an event arrives on that connection, when the
connection drops or comes back, and when a holder starts or is told the
triggers changed. Infrastructure that runs fine keeps
nothing awake: its machine reports only when its state changes. Everything
else (a route, a form, a timer, a poll, a provider that pushes its events)
needs nothing running between events.

While the user's VS Code editor is pointed at the cloud install, it keeps a
live connection to it, which keeps the install and its database awake. After
5 minutes away from the window it pauses that connection and resumes when
they come back; switching the editor back to the local install ends it at
once.

## Setting a project up for its cloud

Somebody installed weft on their cloud once (above), and its summary printed
the install's address.
What a project needs from there, in order, each run from the project's
folder:

1. `weft target add prod <address>`. It writes `[targets.prod]` into
   `weft.toml`, which is committed, so the team shares the name.
2. `weft login prod`: takes an operator key, checks it against the install,
   and stores it in `~/.config/weft/credentials.toml`, which only the user
   can read. If the install is on the user's GCP and its first key is still
   in Secret Manager, you log in yourself by piping it straight across:
   `gcloud secrets versions access latest --secret <secret> --project
   <project> | weft login prod --key-stdin` (the install workflow's log
   prints that exact line). The key goes from Secret Manager to weft and
   nowhere else: never print it, put it in a file, or ask the user to paste
   it into the chat. For any other key, the user runs `weft login prod`
   themselves and types it into its hidden prompt.
3. `weft ci add --cloud gcp`: writes `.github/workflows/deploy.yml`. The
   workflow, run by hand from the repository's Actions tab, asks the
   install which weft it runs and builds that CLI, then deploys the
   program: `weft infra upgrade --all`, then `weft resync --all`, which
   bring onto the new version only the [infra] already running and the
   triggers already on, the program's own and every instance's. It never
   turns anything on, so a trigger or a piece of [infra] somebody took down
   stays down, and a first deploy needs steps 6 and 8 below. The
   workflow's `RUNNING: wait` setting makes both commands let the runs
   going finish on their old version, however long they take. If the user
   would rather stop them at once, they set it to `cancel`. Then, when the install hosts a frontend for the
   repository, it builds `front/` with Docker and runs it on that
   frontend's Cloud Run service, which calls the install at its address
   and reaches the program's infrastructure over the install's private
   network.
   `weft new --ci gcp` writes the same file when the project is made. If it
   refuses because the file "was changed since weft wrote it (or weft never
   wrote it)", the file is somebody's own and weft never overwrites it: tell
   the user, and leave it.
4. If the project has a frontend (`front/Dockerfile`), register it on the
   install now (next section). A project with no frontend skips this: the
   workflow deploys the program and stops.
5. Pick the program's connections on prod, every access node
   ([Connections](#connections) below says how), the program's own keys
   included. A node connected on this machine has nothing picked on prod.
   When this machine already holds them, you copy them across first with
   `weft migrate connections` (the weft-cloud-install skill, "Moving an app
   from this machine"): the user never makes them again.
6. If the program has infrastructure (a database, a bridge), size it before
   its first start on this target. Each infra node has its size settings
   (a database node's `cpu` and `memory`, 0.25 CPU and `1Gi` when unset,
   which on a cloud install is the smallest machine): ask the user what the
   database will carry (how many people, how much data, how busy), propose
   a size, and set it on the node. It can change later: the setting, then
   `weft infra upgrade --on prod`. Then `weft infra start --on prod`. It builds the program for prod first, on the
   install's builder (the worker and every piece's image, about 2 minutes
   the first time, nothing when they are built already), then brings each
   piece up: on GCP each boots a machine of its own, about 2 to 3 minutes,
   and several pieces boot side by side.
7. `weft target export prod --github`: needs step 2 first, and the
   repository already on GitHub. It sets the variables and secrets the
   workflow reads, with `gh` (logged in, run inside the repo): it mints
   an operator key for CI. If the install hosts a frontend for this repository, it also gives that frontend a new token and names its Cloud Run service; the old token keeps working until the workflow's next run deploys the new one and retires it. Every run mints new ones, so it runs once per setup, or again
   when a CI credential was lost. Without `--github` it prints the variables and writes the secrets to a file only you can read, which the user copies into the repository's secrets and then deletes. It also lists every connection the program needs that prod still has no pick for (or says every one is picked): `weft activate --on prod` (step 8) refuses until those are picked (step 5).
   If the project has a frontend, its server's own settings go out with
   this same export: the user writes them into `front/.env.prod` first
   ([The frontend on a cloud install](#the-frontend-on-a-cloud-install)
   says what goes in it), and the export runs as `weft target export prod
   --github --front-env front/.env.prod`.
8. Run `weft activate --on prod` once to turn the program's triggers on:
   every run of the workflow after that brings them up to date.

Then the user runs the workflow from the repository's Actions tab: about a
minute and a half for the program, plus however long the frontend's build
takes, plus however long the runs going take to end while `RUNNING` is
`wait`.

If the cloud build of the project's worker fails, or the cloud refuses a
program for a missing need, look at the `[build.worker]` section of
`weft.toml`, which the worker's image on the cloud is built from:

- `base_image`: the image it starts from, `debian:trixie-slim` when unset.
  On any Debian 13 (trixie) image the build starts from weft's prebuilt
  compiler image; on any other, an older Debian included, it installs Rust
  and compiles the worker inside that image, which takes longer.
- `dockerfile_template`: the path, from the project folder, to a Dockerfile
  used instead of weft's. A missing file fails the build naming the path.
  Without the `{{install_runtime_system_packages}}` placeholder in it, every
  program whose nodes have a `run` need is refused.
- `package_manager` (`apt`, `dnf`, `apk` or `brew`): how the nodes' needs
  are installed. It is required when the image's name before its tag
  (ignoring case) contains none of `debian`, `ubuntu`, `alpine`, `rocky`,
  `almalinux`, `centos`, `fedora`, `amazonlinux`, `oraclelinux`, `rhel`,
  `homebrew` or `/brew`, and does not itself start with `python` (so
  `docker.io/library/python:3.12` needs it and `python:3.12` does not);
  without it the build fails with `base_image ... is of no family weft
  knows`. When set, it wins over the family weft reads from the name.

A build that fails with `none matches the project's base image` means a
node's need has packages for this manager but not for this distro: add a
`default` (or that distro key) under `[needs.install.<manager>]` in the
node's `deps.toml`. If any of the three is set, the cloud builds an image
for this project alone. A local run never builds this image, so a project
proven on this machine can still fail its cloud build on this section.

## A project's frontend: `weft frontend`

A frontend is a website that calls the install on its visitors' behalf.
The install knows each one by a name you pick, and gives it a token of its
own that can only act on this project. Where the site runs decides how you
add it.

**If the install should host it** (Cloud Run, deployed by the project's
GitHub workflow), name the repository whose workflow deploys it:

```bash
weft frontend add front --repo <owner/name> --on prod
weft target export prod --github
```

The first command reads the repository's id with `gh` (logged in; access
is granted to that id, so the name changing hands later gives nobody
anything), makes the frontend's Cloud Run service (empty until the first
deploy) and lets that repository's workflow deploy to that service and
nothing else on the install. It prints the service's name and the
address visitors will reach. It makes no token: the export makes the
frontend's first one and hands the repository the service's name and that
token, and the next run of the deploy workflow builds `front/`, puts it there
and puts the token in place. The order matters: an
export run before the frontend exists hands over no frontend, and the
workflow skips it.

Every hosted frontend deploys as the install's one deploy account, so a
repository you add here could deploy to another project's frontend too.
Only add repositories the user trusts with all of them, and ask before
adding one the user did not name.

**If it runs somewhere else** (Vercel, the user's own server, this
machine), leave out `--repo`:

```bash
weft frontend add shop --on prod
```

The install makes nothing. The command writes the frontend's token, with
the install's address, as `WEFT_TOKEN`, `WEFT_DISPATCHER_URL` and
`WEFT_PUBLIC_URL`, to a file under `~/.local/share/weft/exports/` that only
the user can read. Tell the user that path: they copy the three into the
site's environment. Never print the token or put it in the chat. A local
install hosts nothing, so locally this is the only form, and `--repo` is
refused.

**Afterwards:**

- `weft frontend ls --on prod` lists the project's frontends: each one's
  name, where it runs, the repository that deploys it, and its address.
- A new token never breaks the running site: the old one keeps working
  until the new one is in place. For a hosted frontend, `weft target export
  prod --github` makes the new one and hands it to the workflow, and the
  workflow's next run deploys it and then retires the old one itself. For
  one that runs elsewhere, `weft frontend token <name> --on prod` writes a
  new one to the private file and prints its id; once the user has put it
  in the site's environment, `weft frontend token <name> --done <id> --on
  prod` puts it in place and retires every other token of the frontend.
- `weft frontend rm <name> --on prod` removes one: its tokens stop working,
  and a hosted one's service is deleted with whatever runs on it. Ask the
  user before running it. If the service cannot be removed, it stops and
  says why; `--force` forgets the frontend anyway and names what stays on
  the cloud. `weft rm` on the project removes its frontends first, and
  `weft rm --force` does the same with `--force`.

A name is 1 to 20 lower-case letters, digits or inner hyphens, starting
with a letter, and is taken once per project.

## What a deploy does

`weft activate --on prod` compiles the project here first (a compile
error fails fast, on this machine), then uploads a snapshot of the
project's sources, only the files the install does not have yet. The
install compiles that snapshot itself, builds the images it needs, and
activates. So a deploy of an unchanged project builds nothing.

Activate never starts infrastructure, so on a first deploy of a program
with infra (a database, a bridge), run `weft infra start --on prod` first
(step 6 above, which builds the program too). The `weft-running` skill has
what activate says when infra is down. On GCP each piece boots a machine, so it takes a
few minutes; `weft status --on prod` shows each one's state.

`weft activate` turns on what is off. A change to a program whose triggers
are on goes live with `weft infra upgrade --on prod --all`, then `weft
resync --on prod --all` (the workflow's own two steps), never a plain
`weft resync`, which reaches only the program's own triggers. While the
resync has a trigger down, what arrives is held and runs on the new
version, and the runs still going finish on the old one first; for the three answers every take-down gives, and why a
new trigger cannot be turned on before that resync, go and read the
`weft-running` skill, "Taking triggers down, and bringing them back". Pass
`--held drop` or `--door refuse` on prod only when the user says so: both
throw away work.

After a deploy, if the program has a setup step to run against the
target (a `weft run --on prod --target <group>` the project uses for its
database schema, say), run it.

Every target keeps its own versions: `weft tree --on prod` lists what was
deployed there. To roll back, put an older one's files back in the folder
with `weft branch <id> --on prod`, then `weft infra upgrade --on prod --all`
and `weft resync --on prod --all` (or `weft activate --on prod` if the
program is off). Its images are already built, so nothing rebuilds. `weft branch` records
what the folder held as a version first, and prints the `weft branch <id>`
that comes back to it.

## The frontend on a cloud install

The frontend's server reads three variables, set by the workflow on Cloud
Run and by `front/.env` on this machine:

| Variable | On a cloud install | On this machine |
|---|---|---|
| `WEFT_DISPATCHER_URL` | the install's address | `http://127.0.0.1:14111` |
| `WEFT_TOKEN` | the frontend's own token, which `weft target export` makes (the first one too) and hands the workflow | a token from `weft frontend add <name>` (no `--repo`), written to a private file |
| `WEFT_PUBLIC_URL` | the install's public address | `http://127.0.0.1:14111` |

On this machine, 14111 is the default port; if the install was started on
another one, `~/.local/share/weft/ports.json` has it (`public`).

What else the server reads (the program's database from `weft infra env`,
`BETTER_AUTH_SECRET`) goes to Cloud Run as one dotenv file: the user writes
it on their machine with `weft infra env <node> --on prod --into
front/.env.prod --set NAME=Label` (one `--set` per value, each label as
`weft infra show <node> --on prod` prints it) plus the door's address from `weft infra list-doors --on
prod`, and `weft target export prod --github --front-env front/.env.prod`
stores it as the `WEFT_FRONT_ENV` secret. The deployer only runs the
export; the file holds secrets the user writes.

`front/Dockerfile` builds a server that listens on `$PORT`. If the user's
frontend is not a SvelteKit app, only the workflow's "build the frontend"
step changes.

## Connections

Each install keeps its own connections and its own picks of them: which
connection each access node uses is picked on the install, never written in
the source. A node connected on this machine has no pick on prod until one
is made there: `weft connect <node> --on prod --list` shows prod's
stored connections, `--grant <id>` picks one (a live trigger reading it is
set up again).

A connection prod does not hold yet is added one of three ways:

- **One this machine already holds**: you copy it with `weft migrate
  connections --to prod --all` (or a narrower pick) from the project folder (the weft-cloud-install
  skill, "Moving an app from this machine"), never by asking the user for
  it again. Check this first, with `--dry-run`.
- **A key the project issued itself**, kept in the project's own `.env`
  (the key that guards its own API, a webhook signing secret): the
  deployer stores it without ever reading it, by naming the variable:
  weft loads the project's `.env` into its own environment when it starts,
  so the value goes from the file to the install and never through the
  chat: `weft connect <node> --on prod --door own --set-env <field>=<VARIABLE>`,
  run from the project folder.
- **A person's own account** (a sign-in, a key a provider gave the user):
  the user adds it, with the command you hand them (`weft connect <node> --on prod`).

A run on prod that reaches a node with nothing picked there is refused
before it starts, naming that command.

An instance's connection (a field written `@instance_filled`) lives in the
install it was made on, so instances connect on prod through the
site, as they do here.

## When a deploy fails

- **A compile error**: printed before anything leaves this machine. The
  same fix as on `local`.
- **An image build failed**: the install's builder log is quoted in the
  error, the `cargo` or `apt` line included. Fix the node it names; the
  next deploy rebuilds only what changed.
- **401 from the install**: the key is wrong or was revoked. The user runs
  `weft login prod` again; in CI, `weft target export prod --github` again.
- **429 on a route**: one of the route's limits turned the call away.
  `weft status --on prod` lists, under "refused calls", which node refused,
  how often in the last minute or two, and by which limit. The per-minute
  and at-once limits are settings on the route's node (`callsPerMinutePerCaller`,
  `callsPerMinute`, `callsAtOnce`); "refused tokens from this address" is the
  install's `invalidTokensPerMinute` (30 by default), which blocks an address
  after that many bad tokens in a minute.
- **503 `This worker is busy right now; try again in a moment.`**: a copy was
  full, or its memory was over 90%. It is not under "refused calls". Raise
  `--max-instances` or `--memory`, or lower `--concurrency` (`weft workers
  set`); a lower `--max-runs-at-once` only helps if it is below `--concurrency`.
