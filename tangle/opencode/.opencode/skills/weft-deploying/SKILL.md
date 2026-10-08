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

Only the `deployer` specialist passes `--on`.
You never do: when the user asks for anything on a cloud install, you
dispatch the deployer with what they asked, and relay its report.

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
--accept-cost` (without `--accept-cost` the first domain is refused, naming
the price). It prints the DNS record to set (type `A`, the name, the load
balancer's address), which the deployer hands back for the user to set at
their registrar; Google then issues the certificate on its own once it sees
the record. `weft domain list --on prod`
shows every domain and its record.

A domain can serve one project instead of the whole install: `--for api`
serves that project's routes at the root of the domain
(`https://api.shop.com/users/42`, on top of
`<install address>/connect/local/<project id>/users/42`), and `--for frontend --to <its
https address>` passes the domain on to the project's frontend (`weft
frontend ls --on prod` shows that address).

## Workers and run length

`weft workers` shows a project's worker settings and which come from the install: copies kept warm (`min_instances`), the most copies (`max_instances`), how many calls Cloud Run sends one copy at once (`concurrency`, 80), CPU (unset is one CPU on a cloud install), memory, how long a copy keeps a pool its runs share once nothing uses it (`shared_idle_seconds`), how many runs one copy takes at once (`max_runs_at_once`, unset is one per MiB of memory and never fewer than 64), and how long a call waits for room before it is turned away (`max_queue_wait_seconds`, 30). Before you run `weft workers set --min-instances 1`, tell the user: it keeps
one copy warm so the first call never waits for a start, and on a cloud
install that copy is billed all the time. On a cloud install one stretch of a run lasts an hour at most (Cloud Run cuts a request there, and the run stops itself a minute before): a run that pauses whole (every branch waiting on a timer or a form) starts a fresh hour when it picks back up, so split work that can take longer with a pause. A pause does not help while a caller is still on the line (a route with `outlivesCaller` off) or while a bus between its nodes is open: such a run's wait holds its copy, and it stops at 59 minutes, telling the client to reconnect. A route with `outlivesCaller` whose caller has gone moves its run off the caller's copy once the steps it was running end, and carries it on with a fresh hour; a run with a bus open between its nodes cannot move, so it keeps running on that copy for as long as the copy stays up. A run whose trigger leaves `durable` off (the default) ends cancelled if its copy dies mid-run, and is not run again; if a trigger's runs have to carry on after that (they move money, say), set `durable: true` on it.

A trigger that holds a connection open (a stream or a socket it listens to,
an event subscription that dials out) runs on a holder, which weft starts
only while such a trigger is on and which is billed while it runs. Its node
shows "waiting for a holder to take it" until one has. While a holder runs,
it checks in through weft every 10 seconds, which also keeps weft's broker
and the database up. Infrastructure that is up also keeps the broker and the
database awake: the supervisor checks its health every 30 seconds, through the
broker. Everything else (a route, a form, a timer, a poll, a
provider that pushes its events) needs nothing running between events. If
the install's database is on a plan that sleeps and has limited hours, tell
the user before you deploy such a trigger, or infrastructure, that it keeps
the database awake around the clock.

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
   install which weft it runs and builds that CLI, deploys the program (`weft activate
   --on <target>` when it is off, `weft resync --on <target> --mode park`
   when it is on), then, when the install hosts a frontend for the
   repository, builds `front/` with Docker and runs it on that frontend's
   Cloud Run service, which calls the install at its address and reaches the
   program's infrastructure over the install's private network.
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
6. If the program has infrastructure (a database, a bridge), `weft infra
   start --on prod`. It builds the program for prod first, on the
   install's builder (the worker and every piece's image, about 2 minutes
   the first time, nothing when they are built already), then brings each
   piece up: on GCP each boots a machine of its own, about 2 to 3 minutes,
   and several pieces boot side by side. The workflow's activate refuses
   while a trigger reads infra that is not running.
7. `weft target export prod --github`: needs step 2 first, and the
   repository already on GitHub. It sets the variables and secrets the
   workflow reads, with `gh` (logged in, run inside the repo): it mints
   an operator key for CI. If the install hosts a frontend for this repository, it also gives that frontend a new token and names its Cloud Run service; the old token keeps working until the workflow's next run deploys the new one and retires it. Every run mints new ones, so it runs once per setup, or again
   when a CI credential was lost. Without `--github` it prints the variables and writes the secrets to a file only you can read, which the user copies into the repository's secrets and then deletes. It also lists every connection the program needs that prod still has no pick for (or says every one is picked): the workflow cannot turn
   the program on until those are picked (step 5).
   If the project has a frontend, its server's own settings go out with
   this same export: the user writes them into `front/.env.prod` first
   ([The frontend on a cloud install](#the-frontend-on-a-cloud-install)
   says what goes in it), and the export runs as `weft target export prod
   --github --front-env front/.env.prod`.

Then the user runs the workflow from the repository's Actions tab: about a
minute and a half for the program, plus however long the frontend's build
takes.

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

Activate never starts infrastructure. On a first deploy of a program with
infra (a database, a bridge), run `weft infra start --on prod` first (step 6
above, which builds the program too): if a trigger reads infra that is not
running, activate refuses with "these triggers' infra is not running:
<node>", and while it is still starting, with "this program's infra is
changing (infra provisioning)". On GCP each piece boots a machine, so it
takes a few minutes; `weft status --on prod` shows each one's state.

`weft activate` is for a program that is off. Once its triggers are on, it
refuses, and a change goes live with `weft resync --on prod --mode <mode>`,
which takes the triggers down and brings them back on the new version. On
prod, pick `park` (events that arrive meanwhile wait and run on the new
version once the triggers are back, while a caller on a route is told to
try again in a moment) or `hibernate` (the same for a grace window),
because those events can be real people's. `wipe` drops them and
cancels the work waiting on the triggers: fine on a dev install, and on
prod only when the user says so.

Every target keeps its own versions: `weft tree --on prod` lists what was
deployed there. To roll back, put an older one's files back in the folder
with `weft branch <id> --on prod`, then `weft resync --on prod --mode park`
(or `weft activate --on prod` if the program is off). Its
images are already built, so nothing rebuilds. `weft branch` refuses while
the folder holds changes no version records; `--discard` throws those
changes away, and you pass it only when the user said to.

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
is made there: `weft connect --on prod --node <node> --list` shows prod's
stored connections, `--grant <id>` picks one (a live trigger reading it is
set up again).

A connection prod does not hold yet is added one of two ways:

- **A key the project issued itself**, kept in the project's own `.env`
  (the key that guards its own API, a webhook signing secret): the
  deployer stores it without ever reading it, by loading `.env` into the
  command's environment and naming the variable, so the value goes from
  the file to the install and never through the chat:
  `set -a; . ./.env; set +a; weft connect --on prod --node <node> --door own --set-env <field>=<VARIABLE>`.
- **A person's own account** (a sign-in, a key a provider gave the user):
  the user adds it, with the command you hand them (`weft connect --on prod
  --node <node>`).

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
