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

```bash
PROJECT=my-project REGION=us-central1 FORK=me/weft
NUMBER=$(gcloud projects describe $PROJECT --format='value(projectNumber)')

gcloud services enable --project $PROJECT \
  iam.googleapis.com iamcredentials.googleapis.com sts.googleapis.com \
  cloudresourcemanager.googleapis.com serviceusage.googleapis.com

gcloud storage buckets create gs://$PROJECT-weft-state --project $PROJECT \
  --location $REGION --uniform-bucket-level-access

gcloud iam service-accounts create weft-installer --project $PROJECT
gcloud projects add-iam-policy-binding $PROJECT --role roles/owner \
  --member serviceAccount:weft-installer@$PROJECT.iam.gserviceaccount.com

# On a new project IAM can take a minute to catch up with the services
# just enabled, so each IAM step below is retried until it holds.
retry() { until "$@"; do echo "waiting for IAM to catch up, trying again in 10 seconds"; sleep 10; done; }
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
the services weft uses and creates the network, the machine and the accounts
everything else runs as.

Then, in your fork's settings on GitHub, add these repository variables:

| Variable | Value |
|---|---|
| `GCP_PROJECT_ID` | `$PROJECT` |
| `GCP_REGION` | `$REGION` |
| `GCP_ZONE` | a zone inside it, like `us-central1-a` |
| `TF_STATE_BUCKET` | `$PROJECT-weft-state`, the bucket you just made |
| `GCP_WORKLOAD_IDENTITY_PROVIDER` | `projects/$NUMBER/locations/global/workloadIdentityPools/weft-install/providers/github` (`echo $NUMBER` prints the number) |
| `GCP_INSTALL_SERVICE_ACCOUNT` | `weft-installer@$PROJECT.iam.gserviceaccount.com` |
| `WEFT_FRONTEND_REPOS` | optional: the project repositories allowed to deploy a frontend, as JSON, like `["me/shop"]`. You can add these later ([deploy the frontend](#deploy-the-frontend)) |
| `WEFT_MACHINE_TYPE` | optional: the machine's size, `e2-micro` when unset ([what you get](#what-you-get)) |
| `WEFT_SERVERLESS_ROLES` | optional: the parts of weft that run on Cloud Run, as JSON; `["dispatcher", "broker", "supervisor"]` when unset ([what you get](#what-you-get)) |
| `WEFT_LISTENER_MACHINE` | optional: `true` gives the listener a machine of its own ([what you get](#what-you-get)) |

If your programs sign in to services through OAuth apps of your own
([the apps file](../connections/the-apps-file.md)), put that file's contents
in a repository secret named `WEFT_ACCESS_APPS`. Otherwise skip it.

### Run the workflow

Open the Actions tab of your fork. GitHub turns workflows off in a new fork, so if the tab asks, enable them first. Then pick **install on GCP** and run it. It
takes weft's images from the release when your fork matches it (and
builds the ones it changed), pushes them to your project, and creates
everything listed under [what you get](#what-you-get).

When it finishes, the run's summary gives you the install's address,
`https://<an IP address>`, and the command that reads the first operator
key: the password your CLI uses to act on the install, which you paste into
`weft login prod` below. Run that command on your machine (it needs
`gcloud`): the key is kept in Secret Manager and never printed in the
workflow's logs.

The address works a minute or two after the machine boots: the machine gets
its own certificate for that IP address from Let's Encrypt. Such a
certificate lasts about six days, and the machine renews it on its own
before it runs out. If you want a name instead of an IP, see
[your own domain](#your-own-domain).

If you want to upgrade weft, merge upstream into your fork and run the
workflow again; Terraform changes only what is different from last time.
Then rebuild your CLI from that commit, or `activate` refuses to deploy
([when something goes wrong](#when-something-goes-wrong)).

### What you get

| Piece | What it is |
|---|---|
| One machine | an `e2-micro` running Postgres, the listener and the front door, with a static public address. Its database and certificates live on a disk of their own (20 GB of standard disk by default, beside a 10 GB boot disk) |
| Cloud Run | weft's dispatcher, broker and supervisor, a service each, and your programs' workers, one service per program. Each scales on its own load and to zero between calls |
| Cloud Build | builds your programs' images when you deploy |
| Cloud Tasks | every timer, schedule and poll your programs set |
| Compute Engine | one machine per infrastructure unit your programs start (a database, a GPU model) |
| A storage bucket | weft's files, reached with a key that opens only this bucket |
| Artifact Registry | two image repositories: one for the runtime, workers and infra nodes, which only the install writes, and one for frontends |
| Secret Manager | every secret weft uses |

`e2-micro` is in Google's free tier in `us-west1`, `us-central1` and
`us-east1`, and so is 30 GB of standard disk, which is what the two disks
add up to. `WEFT_LISTENER_MACHINE` adds a third, the listener machine's own
10 GB boot disk, which takes you past it. The machine has 1 GB of
memory for Postgres and the listener together, so a busy install outgrows it. If
you want more room, set `WEFT_MACHINE_TYPE` (say `e2-small`) and run the
workflow again: the machine stops, grows and starts again with its disk.
If you want to change anything else (the data disk's size or type, how many
builds run at once, how many bad tokens a minute the install tolerates), edit
its default in `deploy/terraform/gcp/variables.tf` in your fork and run the
workflow again. A disk can grow but never shrink.

The listener stays on a machine because a trigger that listens to a stream
or a socket needs a process holding the connection open between events,
and a service that scales to zero holds nothing. If those connections load
the machine, set `WEFT_LISTENER_MACHINE` to `true`: the listener then gets an
`e2-small` of its own that stays up, and the machine keeps only Postgres and
the front door. If your programs hold no connections at all, you can put the
listener on Cloud Run too by naming it in `WEFT_SERVERLESS_ROLES`, and weft
then refuses such a trigger when you activate it. Naming roles there replaces
the default list, so list every role you want on Cloud Run.

weft generates the rest of its secrets itself: the database password, the storage
key, the key your saved connections are encrypted with, and the key live
callers' tickets are signed with. The encryption key is made once and kept
in the Terraform state, in the `$PROJECT-weft-state` bucket, and copied
into Secret Manager, which is where the machine reads it. If it is ever
lost or replaced, every saved connection stops working and has to be made again, so keep the
bucket and never run `terraform destroy`.

A cloud install belongs to one person. Every project on it shares one
private network, so a project's code can reach another project's
infrastructure there; keep projects you would not trust with each other on
installs of their own.

### Your own domain

If you want the install at a name like `weft.example.com`, buy the domain
from any registrar. Then run this from any project that has the install as a
target:

```bash
weft domain add weft.example.com --on prod
```

It prints the DNS record to set at your registrar (type `A`, the name, and
the machine's address) and waits until the name points at the install. The
machine then gets the domain's certificate on its own, and
`https://weft.example.com` works. `weft domain list --on prod` shows every
domain with its record.

A domain can also serve one project instead of the whole install:
`--for api` answers that project's routes at the root of the domain
(`https://api.example.com/users/42`), and `--for frontend --to <address>`
passes visitors on to the project's frontend on Cloud Run.

If your programs connect to Google, give the install a domain first: Google
will not send a person back to a bare IP address after they sign in. Until
the install has one, connecting says so and names `weft domain add`.

## Deploy a project

In the project's folder, give your cloud install a name, log in to it, and
deploy:

```bash
weft target add prod https://weft.example.com
weft login prod
weft activate --on prod
```

`weft target add` records the install's name and address in `weft.toml`.
Commit it, and your team gets the same `prod`:

```toml
[targets.prod]
url = "https://weft.example.com"
```

`weft login prod` asks you to paste an operator key, checks it against the
install, and keeps it in `~/.config/weft/credentials.toml`, which only you
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

If you deploy again after a small change, only that change is uploaded, and
only the image it touches is rebuilt. If nothing changed, nothing is built.

### Roll back

Each install keeps its own history of what was deployed to it. If you want
to go back, find the version in `weft tree --on prod`, then let
`weft branch` put its files back in your folder and deploy them:

```bash
weft branch <version> --on prod
weft activate --on prod
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
If a target shows faded, you have not logged in to it yet: run
`weft login <name>` and paste its operator key. And if you want prod's program as files you can read,
`weft running-source <folder> --on prod` writes them into a new folder and
leaves your own files alone.

## Deploy the frontend

If you want the project's frontend (in `front/`) on the cloud, it runs on
Cloud Run and talks to weft over your private network. First add the
project's repository to `WEFT_FRONTEND_REPOS` in your fork's settings and
run **install on GCP** again. That makes the repository a Cloud Run service
of its own, public from the start, and lets its workflows sign in to your
GCP project and change that one service: they cannot touch weft's own
services or any other repository's. Then, in the project's folder (the repository must
already be on GitHub, and you must have run `weft login prod`):

```bash
weft ci add --cloud gcp
weft target export prod --github
```

`weft ci add` writes `.github/workflows/deploy.yml`, a workflow you run by
hand from the Actions tab. It builds the weft CLI from the commit your
install runs, deploys the program with `weft activate --on prod`, then
builds `front/` with Docker and deploys it to the repository's service. If you run
`weft ci add` again, it replaces the file only if you have not edited it;
otherwise it stops and leaves your edits alone. If you are starting a new
project, `weft new <name> --ci gcp` writes the workflow for you.

`weft target export prod --github` uses the GitHub CLI (`gh`) to set the
variables and secrets the workflow reads, so run it with `gh` logged in. It
mints two credentials on the install: an operator key for the workflow, and
a token for the frontend's server, scoped to this project. If you would
rather paste them yourself, leave out `--github` and it prints everything.
If you use the printed version, copy the secrets straight away: the two it
mints are never shown again.

The frontend's server reads three variables, which the workflow sets on
Cloud Run:

| Variable | What it is | Value on your machine |
|---|---|---|
| `WEFT_DISPATCHER_URL` | where the server calls weft: the machine's private address on its internal port, `http://10.10.0.2:14113`, which answers the same API as the public address and never leaves Google's network | `http://127.0.0.1:14111` |
| `WEFT_TOKEN` | the token the server calls with; never send it to a browser | a token from `weft token mint` |
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
route's address on prod differs from your machine's only in the host.
If a route keeps a connection open, the caller is moved to a
`/live/...` address on the same host, so one address and certificate cover it.

If you want to stop a flood of calls from costing you money, each route has
its own limits, set on the node:
`callsPerMinutePerCaller` (60 by default), `callsPerMinute` for everybody
together (none by default), and `callsAtOnce` (100 by default); set any of
them to `0` to turn it off. A call past a limit gets `429` with
`Retry-After` before any run starts, and `weft status --on prod` lists, under
"refused calls", which route refused, how often, and by which limit.

## When something goes wrong

- **`401` on every command**: the key is wrong or was revoked. Run
  `weft login prod` again. In CI, run `weft target export prod --github`
  again.
- **HTTPS fails right after the install, or right after `weft domain add`**:
  the certificate is not issued yet. For a domain, check that its record
  points at the address `weft domain list --on prod` prints. The machine's
  log says what it is waiting for:
  `gcloud compute ssh weft-machine --project <project> --zone <zone> --tunnel-through-iap -- sudo journalctl -u weft-runtime -f`.
- **A run on prod is refused with "has no ... connection picked on this
  install"**: the step was connected on your machine, not on prod. Run the
  `weft connect --node <step>` it names, with `--on prod`.
- **An image build fails**: the error quotes the builder's log, with the
  `cargo` or `apt` line that failed. Fix what it names and deploy again;
  only what changed rebuilds.
- **The frontend deploy fails at its `google-github-actions/auth` step, or
  its `gcloud run deploy` is refused `run.services.get`**: the repository is
  not in `WEFT_FRONTEND_REPOS` (spelled as GitHub spells it, `owner/name`),
  or the install has not run since you added it. Add it and run **install
  on GCP** again.
- **The install workflow stops in "create the cloud"**: Terraform's output
  in that step names the resource it could not make and why. If it names a quota (Compute Engine addresses or CPUs, say), raise
  it in the Google Cloud console and run the workflow again.
