# Deploying to your cloud

If you want a program to serve real people, you put weft on your own GCP
project once, and then send each project there with the commands you
already use on your machine, adding `--on prod`. `prod` is a name you give your cloud install in the project's `weft.toml`, and
weft calls it a target.

You do two jobs, from two repositories:

- **The install**, once, by a workflow in your own fork of
  [weft on GitHub](https://github.com/WeaveMindAI/weft).
- **Each project**, from its own repository: `weft activate --on prod` sends
  the program, and a workflow written by `weft ci add` sends its frontend to
  Cloud Run.

## Install weft on GCP

If you work with Tangle, it can do this whole section for you. It first gets
`gh` and `gcloud` installed and logged in on your machine (the sign-ups, the
billing card and the two logins are the parts only you can do, in your
browser), then runs every step below itself.

### Once, by hand

Before the install workflow can create anything, it needs permission to act
on your GCP project, and a bucket where Terraform (the tool that creates
the cloud pieces) keeps its state. You need `gcloud` logged in as an owner
of a GCP project with billing on.

Create a new GCP project just for weft, with nothing else in it. Weft's own
accounts get broad rights inside the project they run in: they create and
delete machines, services and service accounts there, and can act as any
service account in it. In a project of its own, that reach stops at weft.
The programs you deploy never get those rights; each one runs as an account
of its own that can only reach what it needs.

The commands below turn on the services
weft uses, make the bucket, and create a robot account (a service account)
that your fork's workflows may act as. You store no key in GitHub:
GitHub tells Google which repository a workflow runs in, and Google lets only
`FORK` act as the account.

Replace the three values on the first line. `FORK` is your fork's
`owner/repo`, with the same capitals as on GitHub, because Google compares
it letter for letter.

If you are on the free tier and have no reason to be anywhere in particular,
keep `us-west1` and put the database (below) in Neon's `aws-us-west-2`: both
are in Oregon, so the install's calls to its database stay short. Google's
always-free machine and 5 GB of storage exist only in `us-west1`,
`us-central1` and `us-east1`, and Neon runs on AWS and Azure rather than
Google, so that pair is the one place both free tiers sit side by side.

```bash
PROJECT=my-project REGION=us-west1 FORK=me/weft
NUMBER=$(gcloud projects describe $PROJECT --format='value(projectNumber)')
# On a new project IAM can take a minute to catch up (with the services just
# enabled, with the account just made), so each IAM step below is tried
# again, quietly, for up to three minutes; a last try shows its own error.
retry() {
  for _ in $(seq 18); do
    "$@" >/dev/null 2>&1 && return 0
    echo "waiting for IAM to catch up, trying again in 10 seconds"
    sleep 10
  done
  "$@"
}

gcloud services enable --project $PROJECT \
  iam.googleapis.com iamcredentials.googleapis.com sts.googleapis.com \
  cloudresourcemanager.googleapis.com serviceusage.googleapis.com

gcloud storage buckets create gs://$PROJECT-weft-state --project $PROJECT \
  --location $REGION --uniform-bucket-level-access

gcloud iam service-accounts create weft-installer --project $PROJECT
retry gcloud projects add-iam-policy-binding $PROJECT --role roles/owner \
  --member serviceAccount:weft-installer@$PROJECT.iam.gserviceaccount.com

pool() {
  gcloud iam workload-identity-pools describe weft-install --project $PROJECT \
    --location global >/dev/null 2>&1 \
  || gcloud iam workload-identity-pools create weft-install --project $PROJECT \
    --location global
}
provider() {
  gcloud iam workload-identity-pools providers describe github --project $PROJECT \
    --location global --workload-identity-pool weft-install >/dev/null 2>&1 \
  || gcloud iam workload-identity-pools providers create-oidc github \
    --project $PROJECT --location global --workload-identity-pool weft-install \
    --issuer-uri https://token.actions.githubusercontent.com \
    --attribute-mapping google.subject=assertion.sub,attribute.repository=assertion.repository \
    --attribute-condition "assertion.repository=='$FORK'"
}
retry pool
retry provider
retry gcloud iam service-accounts add-iam-policy-binding \
  weft-installer@$PROJECT.iam.gserviceaccount.com --project $PROJECT \
  --role roles/iam.workloadIdentityUser \
  --member principalSet://iam.googleapis.com/projects/$NUMBER/locations/global/workloadIdentityPools/weft-install/attribute.repository/$FORK
```

The robot account owns the project, because Terraform turns on the rest of
the Google APIs weft uses and creates the network, weft's Cloud Run services
and the accounts everything else runs as.

Then, in your fork's settings on GitHub, add these repository variables:

| Variable | Value |
|---|---|
| `GCP_PROJECT_ID` | `$PROJECT` |
| `GCP_REGION` | `$REGION` |
| `GCP_ZONE` | a zone inside it, like `us-west1-b` |
| `TF_STATE_BUCKET` | `$PROJECT-weft-state`, the bucket you just made |
| `GCP_WORKLOAD_IDENTITY_PROVIDER` | `projects/$NUMBER/locations/global/workloadIdentityPools/weft-install/providers/github` (`echo $NUMBER` prints the number) |
| `GCP_INSTALL_SERVICE_ACCOUNT` | `weft-installer@$PROJECT.iam.gserviceaccount.com` |

### A database

weft keeps everything in a Postgres database you bring, and only needs its
address. Any Postgres the internet can reach works. If you want the install to cost next to nothing while
nobody uses it, pick one that scales to zero: once weft's services
have scaled to zero they hold no connection to it, so the database can sleep (for
what keeps it awake, see [what you get](#what-you-get)). If you work with Tangle, it
can create one that scales to zero and set the secrets below for you.

Put its address in a repository secret named `WEFT_DATABASE_URL`, as a
connection URL (`postgres://user:password@host/db?sslmode=require`).

If that address goes through a pooler that hands out a connection per
transaction, also add a direct address of the same database as
`WEFT_DATABASE_LISTEN_URL`. weft waits on changes to its rows with a Postgres
`LISTEN`, which needs one connection that stays open, and such a pooler hands
out a different connection for each transaction. If you leave it out and the
address cannot listen, weft refuses to start and its log names this secret.

### OAuth apps of your own (optional)

If your programs sign in to services through OAuth apps of your own
([the apps file](../connections/the-apps-file.md)), put that file's contents
in a repository secret named `WEFT_ACCESS_APPS`. Otherwise skip it.

### Run the workflow

Open the Actions tab of your fork. GitHub turns workflows off in a new fork, so if the tab asks, enable them first. Then pick **install on GCP** and run it. It
takes weft's CLI and images from the release when your fork holds the same
source (and builds the ones it changed), pushes the images to your project,
and creates everything listed under [what you get](#what-you-get). If you
run it right after weft's main moved and your fork caught up, the release
for that source may still be building: the workflow waits for it rather
than compiling the same thing, and says so in its log. When it does have to
build, it keeps the compiled dependencies for the next run, so after an
upgrade only what changed in weft compiles again.

When it finishes, its last step (in the run's summary, and in its log for
`gh run view <id> --log`) gives you the install's address, a Cloud Run
address like `https://weft-role-dispatcher-123456789.us-west1.run.app`, and
the line that logs you in with the first operator key: the password your CLI
uses to act on the install. The key is kept in Secret Manager and never
printed in the workflow's logs; that line reads it on your machine (it needs
`gcloud`) and hands it straight to `weft login`, below.

The address works at once, over HTTPS, with no extra charge. If you want a
name of your own instead, see [your own domain](#your-own-domain), which
costs money.

If you want to upgrade weft, merge upstream into your fork and run the
workflow again; Terraform changes only what is different from last time.
Then rebuild your CLI from that commit, or `activate` refuses to deploy
([when something goes wrong](#when-something-goes-wrong)).

If you want weft gone, delete the GCP project you made for it: weft and
everything it ever started live in that project and nowhere else, so that is
the whole uninstall. Unlink its billing first, so it stops counting against
your billing account at once:

```bash
gcloud billing projects unlink $PROJECT
gcloud projects delete $PROJECT
```

Google keeps a deleted project for 30 days, during which `gcloud projects
undelete $PROJECT` brings it back. The database is yours and stays where it
is: delete it from its own provider if you no longer want it.

### What you get

While nobody is using the install, no trigger keeps a connection open, and
no program's infrastructure is up, none of weft runs, apart from a check every
few hours that wakes each part for a moment.

| Piece | What it is |
|---|---|
| Cloud Run | weft's dispatcher, broker, listener and supervisor, a service each, and your programs' workers, one service per program. Each scales on its own load, and to zero between calls. A service at zero starts again when it is called: by the part of weft that just wrote work for it, by a Cloud Tasks wake, or by a call from outside (your CLI, a route, a provider's webhook) |
| The holders | a Cloud Run worker pool for the triggers that keep a connection open between events (a stream, a socket, an event subscription that dials out). weft runs one holder per 200 such triggers and none when there are none |
| Your database | everything weft keeps, at the address you gave it |
| Cloud Build | builds your programs' images when you deploy |
| Pub/Sub | the `cloud-builds` topic, on which Cloud Build announces each build's end, so the dispatcher hears it at once, and one carrying Compute Engine's record of an infrastructure machine stopping or failing to the supervisor |
| Cloud Tasks | every timer, schedule and poll your programs set, and the wakes weft schedules for itself to check on its own pending work |
| Compute Engine | one machine per infrastructure unit your programs start (a database, a GPU model) |
| A storage bucket | weft's files, reached as weft's own service account: no key is made for it, so an organization that forbids service-account keys runs weft as is |
| Artifact Registry | two image repositories: one for the runtime, workers and infra nodes, which only the install writes, and one for frontends |
| Secret Manager | every secret weft uses |

Most triggers need nothing running between events: a route or a form is
called, a timer or a poll is woken by Cloud Tasks, and a service that pushes
its events (a provider's webhook) calls the install. If you want to
know when an event subscription takes a push instead of holding a
connection, go and read
[how a trigger picks its road](../connections/events.md#the-two-roads).
You pay for a holder while it runs. A running holder also checks in with weft
every 10 seconds, so weft's broker and your database never get to sleep
while such a trigger is on. A program's infrastructure does the same while it
is up: the supervisor checks its health every 30 seconds, through the broker,
so the database stays awake until you stop it. If a
holder crashes, its triggers hear nothing for up to about 40 seconds: its
claims run out 30 seconds after it last renewed them, and another holder, or
its restarted copy, takes them at its next look. A
holder that is stopped normally hands its triggers over at once.

If you want to change one of the install's defaults (how many triggers a
holder takes, a holder's CPU and memory, how many builds run at once, how
many bad tokens a minute one address may present), edit it in
`deploy/terraform/gcp/variables.tf` in your fork and run the workflow again.

weft generates the rest of its secrets itself: the key your saved
connections are encrypted with, the key live callers' tickets are signed
with, and the first operator key. The encryption key is made once and kept
in the Terraform state, in the `$PROJECT-weft-state` bucket, and copied
into Secret Manager, which is where weft's services read it. If it is ever
lost or replaced, every saved connection stops working and has to be made
again, so keep the bucket and never run `terraform destroy`.

A cloud install belongs to one person. Every project on it shares one
private network, so a project's code can reach another project's
infrastructure there; keep projects you would not trust with each other on
installs of their own.

### Your own domain

If you want the install, or one of your projects, at a name like
`weft.example.com`, buy the domain from any registrar.

A domain needs a load balancer in front of the install, which holds the
domain's certificate. Google bills it by the hour while it exists: about $18
a month, plus $0.008 per GB that goes through it. weft makes one with your
first domain, every later domain shares it, and removing the last one takes
it down. For your first domain, pass `--accept-cost` to say you accept
that charge; without it, `weft domain add` stops and prints the cost. Run
this from any project that has the install as a target:

```bash
weft domain add weft.example.com --on prod --accept-cost
```

It prints the DNS record to set at your registrar (type `A`, the name, and
the load balancer's address) and waits until the name points there. Google
then issues the domain's certificate once it sees the record, and
`https://weft.example.com` works. `weft domain list --on prod` shows
every domain with its record.

A domain can also serve one project instead of the whole install:
`--for api` answers that project's routes at the root of the domain
(`https://api.example.com/users/42`, as well as
`https://<the install's address>/connect/local/users/42`), sending its calls
straight to the project's own Cloud Run service, and
`--for frontend --to <address>` passes visitors on to the project's
frontend on Cloud Run, at the address `weft frontend ls --on prod` shows for
it.

## Deploy a project

In the project's folder, give your cloud install a name, log in to it, and
deploy:

```bash
weft target add prod https://weft-role-dispatcher-123456789.us-west1.run.app
gcloud secrets versions access latest --secret <the secret> --project <project> | weft login prod --key-stdin
weft infra start --on prod   # only if the program has infrastructure
weft activate --on prod
```

If your program has infrastructure (a database, a bridge), start it before
the first activate: activate never starts it for you, and if a trigger reads
infrastructure that is not running, it refuses with `these triggers' infra is
not running: db`, naming each piece. `weft infra start --on prod` builds the
program and brings its infrastructure up, which on GCP means a machine per
piece booting, so give it a few minutes; `weft status --on prod` shows how
far each one got.

`weft target add` records the install's name and address in `weft.toml`.
Commit it, and your team gets the same `prod`:

```toml
[targets.prod]
url = "https://weft-role-dispatcher-123456789.us-west1.run.app"
```

`weft login prod` takes an operator key (from its hidden prompt, or piped in
with `--key-stdin`), checks it against the install, and keeps it in `~/.config/weft/credentials.toml`, which only you
can read. If a teammate needs their own key, run
`weft token mint --operator --on prod` and send them what it prints. To see
the keys you have handed out, run `weft token ls --on prod`, and to cancel
one right away, `weft token revoke <id> --on prod`.

Every command takes `--on` the same way: `weft status --on prod`,
`weft executions --on prod`, `weft logs <execution-id> --on prod`. Without it, a
command acts on your own machine; nothing makes prod the default.

### What `activate --on prod` does

Your machine compiles the project first, so a mistake shows up on your
screen before anything is uploaded. Then it uploads the project's sources,
only the files the install does not have yet, and the install does the
rest: it compiles them again itself, builds the images the program needs,
and activates.

### Deploy a change

Once the program is on, `weft activate` refuses (`these triggers are already
on`). If you want a change to go live, resync instead: it takes the triggers
down and brings them back up on the new version.

```bash
weft resync --on prod --mode park
```

On prod, pick `park` or `hibernate`, because the events arriving while the
triggers are down can be real people's: `park` keeps them and runs them on
the new version once the triggers are back, `hibernate` does the same for a
grace window (`--grace`, in minutes). A caller on a route is not kept: it is
answered `503` with a `Retry-After`, and its client tries again. `wipe` drops them and cancels the work waiting on the triggers,
which is fine on a dev install.

Only the change is uploaded, and only the image it touches is rebuilt. If
nothing changed, nothing is built.

### Roll back

Each install keeps its own history of what was deployed to it. If you want
to go back, find the version in `weft tree --on prod`, then let
`weft branch` put its files back in your folder and deploy them:

```bash
weft branch <version> --on prod
weft resync --on prod --mode park   # or `weft activate --on prod` if it is off
```

`weft branch` stops if your folder holds changes no version records yet;
`--discard` throws those away. That version's images still exist from its
first deploy, so nothing is rebuilt.

### Connections

Each install keeps its own connections and its own picks of them: which
connection a step uses is picked on the install, never written in your
files ([picked on each install](../build/connections.md#picked-on-each-install)).
A step you connected on your machine has no pick on prod until you make one
there, with the same walkthrough:

```bash
weft connect --on prod
```

If a run on prod reaches a step with nothing picked there, it is refused
before it starts, and the message names the step and
`weft connect --node <step>`; add `--on prod`. An instance's own connection (a
field written `@instance_filled`) lives in the install it was made on, so
your customers connect through your frontend on prod, the same way they do
locally.

### Look at prod from the editor

If your project names a target, the graph has a switch in its top right
corner: **local**, then each target. If you want to see what prod is
running, click **prod**. The graph then shows the program as it was last
deployed there, along with prod's runs, status and connections. From then
on, **Run**, **Activate**, the Executions list and the version tree all go
to prod, as if you had typed `--on prod` on each command.

You cannot edit the program there, because it is a copy of what prod runs.
You can change its connections, though: **Connect** and **Disconnect** on a
step change which connection prod uses, and prod sets up again any trigger
that relies on it. If the files in your folder are a different version from
prod's, a banner at the top says so. **Activate** deploys the files in your
folder, exactly like `weft activate --on prod`, not the copy you see on
screen.

If you want to edit again, click **local**; closing the graph does the same.
If a target shows faded, you have not logged in to it yet: log in to it as
in [deploy a project](#deploy-a-project). And if you want prod's program as files you can read,
`weft running-source <folder> --on prod` writes them into a new folder and
leaves your own files alone.

## Deploy the frontend

If you want the project's frontend (in `front/`) on the cloud, it runs on
Cloud Run, calls weft at the install's address with a token of its own,
and reaches the program's infrastructure (its database) over the install's
private network. In the project's
folder (the repository must already be on GitHub, and you must have run
`weft login prod`):

```bash
weft frontend add front --repo me/shop --on prod
weft ci add --cloud gcp
weft target export prod --github
```

`weft frontend add` reads the repository's id with `gh` (access is granted
to that id, so nobody who takes the name later gets it), makes the frontend
a Cloud Run service of its own,
public from the start, and lets that repository's workflows sign in to your
GCP project and change that one service: they cannot touch weft's own
services or any other frontend's. Every frontend deploys as the install's one
deploy account, so a repository you add could deploy to another project's
frontend too; add only repositories you trust with all of them. The name
(`front` here) is yours to pick: lower-case letters, digits and hyphens.
`weft frontend ls --on prod` lists a project's frontends, and
`weft frontend rm front --on prod` removes one, its service included.

If your frontend runs somewhere else (Vercel, a server of your own), leave
out `--repo`: the install makes nothing, and the command writes the
frontend's token, with the install's address, to a file only you can read.
Put those in the frontend's environment.

`weft ci add` writes `.github/workflows/deploy.yml`, a workflow you run by
hand from the Actions tab. It takes the weft CLI the release built from the
source your install runs (waiting for it while that release is still
building, or building it when the release has none), deploys
the program (`weft activate --on prod` when it is off,
`weft resync --on prod --mode park` when it is on: edit the mode in the file
if `hibernate` or `wipe` suits the program better), then
builds `front/` with Docker and deploys it to the repository's service. If you run
`weft ci add` again, it replaces the file only if you have not edited it;
otherwise it stops and leaves your edits alone. If you are starting a new
project, `weft new <name> --ci gcp` writes the workflow for you.

`weft target export prod --github` uses the GitHub CLI (`gh`) to set the
variables and secrets the workflow reads, so run it with `gh` logged in. It
mints an operator key for the workflow, and gives the frontend the install
hosts for this repository a new token, with the name of its service (its
first one: `weft frontend add` makes none for a hosted frontend). Once it has
been deployed, its old token keeps working until the workflow's next run has
deployed the new one, and then the workflow retires it. If you would rather paste them yourself, leave out
`--github` and it prints everything. If you use the printed version, copy the
secrets straight away: they are never shown again.

The frontend's server reads three variables, which the workflow sets on
Cloud Run:

| Variable | What it is | Value on your machine |
|---|---|---|
| `WEFT_DISPATCHER_URL` | where the server calls weft: the install's address, over HTTPS | `http://127.0.0.1:14111` |
| `WEFT_TOKEN` | the token the server calls with; never send it to a browser (on Cloud Run, the one `weft target export` made) | a token from `weft frontend add <name>`, written to a file only you can read |
| `WEFT_PUBLIC_URL` | the start of any link a browser follows | `http://127.0.0.1:14111` |

If the frontend's server needs more than those three (the program's
database, a sign-in secret), put them in one file on your machine and hand it
over with the export. Keep that file out of git.

1. `weft infra env <node> --on prod --into front/.env.prod --set ...` writes
   a database's values into it, the same way it does locally.
2. `weft infra list-doors --on prod` prints the database's private address.
   Only what runs inside the install's network can reach it, and your Cloud
   Run frontend is one of them.
3. `weft target export prod --github --front-env front/.env.prod` stores the
   file as the `WEFT_FRONT_ENV` secret, and the workflow puts it on Cloud Run
   beside weft's own variables.

Cloud Run tells the container which port to use in `$PORT`, so
`front/Dockerfile` has to build a server that listens on it. If your
frontend builds differently, edit the workflow's "build the frontend" step.

## Routes on the internet

A `Route` answers at `https://<the install's address>/connect/local/<path>`,
where `<path>` is the route's `path`. The `local` in that path is the same on every install, prod included, so a
route's address on prod differs from your machine's only in the host. The
project also answers at its own Cloud Run address (`weft status --on prod`
shows it), where the call reaches your program with nothing of weft's in
between: give that one, or a `--for api` domain, to a client that cares how
fast it is answered.
A browser opening a socket on a route that checks its callers is handed an
address under the same path on the same host, carrying a ticket, so one
address and certificate cover it.

If you want to stop a flood of calls from costing you money, each route has
its own limits, set on the node:
`callsPerMinutePerCaller` (60 by default), `callsPerMinute` for everybody
together (none by default), and `callsAtOnce` (100 by default); set any of
them to `0` to turn it off. A call past a limit gets `429` with
`Retry-After` before any run starts. `weft status --on prod` shows the refusals
of the last minute or two under "refused calls": which route, how often, and by
which limit. If the
program runs on several copies of its worker, each copy counts calls itself
and hears the others' counts once a second, so a per-minute limit can let
through about one second's worth of extra calls, and `callsAtOnce` is shared
out between the copies that are up.

## When something goes wrong

- **`401` on every command**: the key is wrong or was revoked. Run
  `weft login prod` again. In CI, run `weft target export prod --github`
  again.
- **If you want weft's logs**, the services' logs are in Cloud Logging:
  `gcloud logging read 'resource.type="cloud_run_revision"' --project <project> --freshness 1h`.
  The holders' logs are under the worker pool `weft-holder` in the Cloud Run
  console.
- **HTTPS fails at a domain right after `weft domain add`**: Google has not
  issued its certificate yet. Check that its record points at the address
  `weft domain list --on prod` prints; the certificate follows once Google
  sees it.
- **`weft domain list` says the door in front of your domains refuses to
  follow them**: Google refused to change the load balancer, and the
  message says why. weft tries again on its own, waiting longer each time,
  up to six hours. Once you have fixed the cause, any `weft domain add` or
  `weft domain rm` tries again at once.
- **weft does not start, and its log says the database session "cannot
  LISTEN"**: `WEFT_DATABASE_URL` goes through a pooler. Add a direct address
  of the same database as the `WEFT_DATABASE_LISTEN_URL` secret ([a
  database](#a-database)) and run the install workflow again.
- **A trigger that keeps a connection open shows "waiting for a holder to
  take it" on its node**: a running holder with room takes it within about 10 seconds; otherwise weft
  is starting one. If it stays
  that way, the holders' logs say why.
- **A run on prod is refused with "has no ... connection picked on this
  install"**: the step was connected on your machine, not on prod. Run the
  `weft connect --node <step>` it names, with `--on prod`.
- **An image build fails**: the error quotes the builder's log, with the
  `cargo` or `apt` line that failed. Fix what it names and deploy again;
  only what changed rebuilds.
- **The frontend deploy fails at its `google-github-actions/auth` step, or
  its `gcloud run deploy` is refused `run.services.get`**: the repository is
  not the one its frontend was added with (`weft frontend ls --on prod` shows
  it, spelled as GitHub spells it, `owner/name`). Remove the frontend and add
  it again with the right `--repo`, then run `weft target export` again.
- **The install workflow stops in "create the cloud"**: Terraform's output
  in that step names the resource it could not make and why. If it names a quota (Compute Engine addresses or CPUs, say), raise
  it in the Google Cloud console and run the workflow again.
