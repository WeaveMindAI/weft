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
nobody uses it, pick one that scales to zero: once nobody uses the install, weft
stops querying it, so the database can sleep (for what keeps it awake, see
[what you get](#what-you-get)). If you work with Tangle, it
can create one that scales to zero and set the secrets below for you.

Put its address in a repository secret named `WEFT_DATABASE_URL`, as a
connection URL (`postgres://user:password@host/db?sslmode=require`).

If that address goes through a pooler that hands out a connection per
transaction, also add a direct address of the same database as
`WEFT_DATABASE_LISTEN_URL`. weft waits on changes to its rows with a Postgres
`LISTEN`, which needs one connection that stays open, and such a pooler hands
out a different connection for each transaction. If you leave it out and the
address cannot listen, weft refuses to start and its log names this secret.

If your program will be busy (schedules every few minutes, a connection held
open all day, a site calling it constantly), its database rarely gets to
sleep, and a hosted free plan runs out: Neon's caps the data sent out of the
database at 5 GB a month, and every query weft makes crosses the internet to
reach it. For that, the install can have a Postgres of its own instead, on
Cloud SQL in its own region and network. Set the repository variable
`WEFT_DATABASE` to `cloud_sql` and leave `WEFT_DATABASE_URL` out. It bills
every hour it runs, even with nobody using the install, and Google's Cloud
SQL pricing page has the price for your region. Its size is the repository
variable `WEFT_CLOUD_SQL_TIER` (`db-g1-small`, a small shared machine, when
unset).

You can switch to it later. Run `weft maintenance on --on prod`, set
`WEFT_DATABASE` to `cloud_sql`, and run the workflow again: it makes the
install's own database, copies everything from the one it used, and only then
points the install at it. Until the install uses the new database, every run
of the workflow copies again from scratch, so if anything fails on the way,
the install stays on the old database and running the workflow again starts
over. Then `weft maintenance off --on prod`. Once you are happy with it, you
can delete the `WEFT_DATABASE_URL` secret and the old database.

#### How many connections the install takes

A Postgres database only takes so many connections at once (its
`max_connections`: 50 on Cloud SQL's `db-g1-small`). Each copy of weft's
dispatcher and broker asks the database for that number when it starts. It
leaves out the connections the database keeps for its superusers, and 3
more for anyone else (your `psql`, say), then splits the rest evenly among
the most copies the install may run: 2 dispatchers and 4 brokers unless you
change it. On `db-g1-small` that leaves 44, split six ways: 7 each, with
the 2 left over unused. All the copies together therefore never ask for
more than the database takes.
When every connection of a copy is in use, its next request waits its turn,
and `weft status` says so (see [when something goes
wrong](#when-something-goes-wrong)). Each copy logs `sized the database
pools to this copy's share` when it starts, with its numbers.

If you want the install to handle more at once, raise `WEFT_CLOUD_SQL_TIER`
to a larger machine, which takes more connections. If you would rather pick
the number yourself, set `WEFT_CLOUD_SQL_MAX_CONNECTIONS`. The workflow
refuses a number below 25, and with the default copy limits a copy does not
start below 42. Unset, Cloud SQL uses its own number for the tier; once you
set it, it stays at your number whatever the tier, so raise it along with the tier. Setting
or changing it restarts the database once, the next time the workflow runs.

If you want more copies of the dispatcher or the broker running at once,
raise `WEFT_DISPATCHER_MAX_COPIES` (2 when unset) or
`WEFT_BROKER_MAX_COPIES` (4 when unset). Every copy then gets a smaller
share, so give the database more connections first: a broker copy needs a
share of at least 6 and a dispatcher copy 5, and one whose share is smaller
refuses to start, with a log line that names the numbers and these
variables.

If you want a role to have more memory, set `WEFT_DISPATCHER_MEMORY` (1Gi
when unset), `WEFT_BROKER_MEMORY`, `WEFT_LISTENER_MEMORY` or
`WEFT_SUPERVISOR_MEMORY` (512Mi each when unset), a whole number of `Mi` or
`Gi` from 512Mi to 4Gi, or `WEFT_HOLDER_MEMORY` for each copy that holds a
trigger's open connection (512Mi when unset, at least 512Mi). The install sets each one on every run, so a value you
change by hand in the Cloud Run console is put back the next time the workflow
runs. The dispatcher gets more because every version is compiled there, and on
Cloud Run the files a build writes are held in memory, beside the programs the
dispatcher keeps read (about 90 MiB at most).

These are repository variables on your fork, like the ones above, so
merging a new weft into the fork keeps them. After you change one, run the
install workflow again.

If you bring your own database, each share comes from that database's
`max_connections`, which a pooler's address (Neon's, say) reports too. If
the install waits on it too often, move to a plan of your provider's that
takes more connections, or lower the two copy limits.

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
upgrade only what changed in weft compiles again. Among the images is the
standard worker, which every project made only of the standard nodes runs on,
so the first of them to run after an upgrade starts at once instead of
waiting a few minutes for that build. It also keeps the CLI it used in your
install, so your projects' deploys download it instead of compiling it.

When it finishes, its last step (in the run's summary, and in its log for
`gh run view <id> --log`) gives you the install's address, a Cloud Run
address like `https://weft-role-dispatcher-123456789.us-west1.run.app`, and
the line that logs you in with the install's own operator key: the
password your CLI uses to act on the install. The key is kept in Secret
Manager and never printed in the workflow's logs; that line reads it on your
machine (it needs `gcloud`) and hands it straight to `weft login`, below.

The address works at once, over HTTPS, with no extra charge. If you want a
name of your own instead, see [your own domain](#your-own-domain), which
costs money.

If you want to upgrade weft, run `weft maintenance on --on prod` from a
project, merge upstream into your fork, run the workflow again, rebuild your
CLI from that commit (`./setup.sh --cli`), then run `weft maintenance off
--on prod`. The first takes down every trigger that is on and stops the
infra that runs, so the old weft and the new one never run side by side, and
the workflow refuses to replace anything while something is on. Terraform
changes only what is different from last time. The CLI goes before `off`
because an older CLI cannot read what `off` answers, and `activate` refuses
to deploy from one ([when something goes wrong](#when-something-goes-wrong)).
The last builds those projects again on the new weft and brings back exactly
what the first took down. For what each step
does, go and read [updating weft](cli.md#updating-weft).

This works from an install of any age, however long ago you last ran the
workflow.

Once the new weft is rolled out, the workflow deletes the older revisions of
each of weft's own services (the dispatcher, the broker, the listener and the
supervisor), and that stops every copy of the old weft still running, even one
the editor or a worker was holding a connection to. Each finishes what it was
answering, closes what it held open and gives back the work it was in the
middle of, all within the 10 seconds Cloud Run gives it, and the editor and the
workers open again on the new copies by themselves. So the old weft and the new
one never take work side by side. If a service's new revision is not ready, or
does not get all of its requests (traffic you pinned or split by hand), that
step stops with an error naming the service and deletes nothing. The holders
are not on that list: they run in a worker pool, which moves every one of its
copies onto its newest revision as soon as that revision is rolled out, so the
old holders stop by themselves. That holds while the pool sends its copies to
its latest revision, which is how the install sets it up. If you split its
copies between revisions by hand, that split stays through every later
update and the old holders keep running; send every copy to the latest
revision again with `gcloud run worker-pools update-instance-split weft-holder
--to-latest --region <your region>`.

If you roll a revision out by hand, outside the workflow, the copies of the
older revisions keep running until the next run of the workflow deletes them,
or until you delete them yourself with `gcloud run revisions delete`.

Once in a while, weft's database starts its history over, and a database an
older weft built cannot come across (it last happened when executions were
renamed runs). Before it deploys anything, the workflow checks your install's
database. If it is one of those, the workflow stops with an error that names
the `reset_database` input, and your install stays as it was. If you want to
go ahead, run the workflow again with `reset_database` ticked (from a terminal:
`gh workflow run install-gcp.yml -f reset_database=true`). It wipes the
database, builds the new weft's in its place, and deploys. That deletes every
project's run history, versions and stored connections, and the maintenance
with them, so there is nothing for `weft maintenance off` to bring back:
deploy each project again with `weft activate --on prod`, and make its
connections again with `weft connect --on prod`. Your project folders are
untouched. If the database did not need it, the workflow stops without
touching anything, and you run it again with the box unticked.

If something in a project does not come back with `weft maintenance off --on
prod`, everything else is back anyway, and `off` says what went wrong. If the
message says the project's copy of the standard nodes is older than this
weft, run `weft catalog update` in that folder once your weft checkout and CLI
are on the new commit, since it copies the standard nodes from the checkout
on your machine (it replaces `nodes/base_catalog/`, so any edits you made in
there are lost); otherwise fix what it says. Then run `weft resync --on prod`
there, which builds the project and brings back what the upgrade took down.

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

While nobody is using the install and no trigger keeps a connection open,
none of weft runs. Your database wakes only when something happens: a run
doing work, a timer or schedule coming due, a poll that finds something new,
something arriving from outside (a call to a route, a webhook, an event on a
connection a trigger holds), something weft itself has due (a door you gave a `--door-for` time limit
starting to refuse, a subscription to renew), or an editor or `weft follow` watching the
install (more on the editor below). A poll that finds nothing new wakes weft's
listener and leaves the database asleep. A program's infrastructure that
runs fine keeps nothing awake: each machine says so itself when its state
changes.

If a run cannot pause (its caller is still on the line, or it is
unrecorded) and it waits, on a person's answer or on its caller's next
message over a socket, it keeps its worker and one copy of the broker up for
as long as it waits, which is cheap, and leaves your database asleep for all
of it. The answer or the cancel that ends the wait reaches
the broker over Pub/Sub, not through the database, and is pushed to the
worker on its connection. The database wakes when the answer, the cancel
or the caller's next message arrives, or the run ends. If you want a wait longer than an hour, make the
run one that can pause: Cloud Run cuts a request after an hour, so when a
worker's runs are all only waiting and its connection is a minute from that
cut, the worker ends them with a message naming the limit, and stops. A
caller that reaches that worker in the few seconds it takes to stop is told
to try again. Cloud Run may send the retry to the same stopping worker for
those few seconds, which turns it away again; once the worker is gone, the
retry reaches a fresh one.

| Piece | What it is |
|---|---|
| Cloud Run | weft's dispatcher, broker, listener and supervisor, a service each, and your programs' workers, one service per program. Each scales on its own load, and to zero between calls. A service at zero starts again when it is called: by the part of weft that just wrote work for it, by a Cloud Tasks wake, or by a call from outside (your CLI, a route, a provider's webhook) |
| The holders | a Cloud Run worker pool for the triggers that keep a connection open between events (a stream, a socket, an event subscription that dials out). weft runs at least one holder per 200 such triggers, more while the holders are busy, and none when there are none |
| Your database | everything weft keeps, at the address you gave it |
| Cloud Build | builds your programs' images when you deploy |
| Pub/Sub | three topics, which let weft's parts tell each other things without going through your database: `weft-machine-events` carries Compute Engine's record of an infrastructure machine stopping or failing to the supervisor, `weft-news` is where weft announces that a run a worker drives got an answer or was cancelled, and `weft-holders` tells the holders that the triggers they should hold changed, or that one of your connections was reconnected, rotated or removed, so they drop the credentials they kept for it (a token refresh is not announced here) |
| Cloud Tasks | two queues. `weft-wakes` delivers every timer, schedule and poll your programs set, each subscription renewal, and each holder's check; if the install does not answer one, it keeps retrying, at most five minutes apart. For a trigger's wake, the listener gives the trigger up after `wake_attempts` failed deliveries it answered (20, about an hour; a delivery that never reached it uses none) and says why on its node. For how to bring it back, go and read [the listener](architecture.md#the-listener). `weft-ticks` delivers the wakes weft sets for itself (a background job's next look) and gives one up after ten minutes |
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
You pay for a holder while it runs, but it wakes your database only when it
calls weft: when it starts, when an event arrives on one of its connections,
when a connection drops or comes back (once per drop, however many reconnect
attempts follow), when its CPU has moved enough to say so (below), and when
the `weft-holders` topic tells it something changed. If a holder crashes, weft frees its triggers within three
minutes for another holder, or its restarted copy, to take (for how it
notices, go and read [the listener](architecture.md#the-listener)). A holder that is
stopped normally hands its triggers over at once.

Some triggers keep a holder far busier than others: a socket that talks all
day costs more than a subscription that hears one event an hour. So each
holder also watches its own CPU, sampled every ten seconds and averaged over
the last minute, and weft runs as many holders as that load needs at about
50% each. If two holders say 60% and 55%, that is 115, and weft starts a
third. That third holder starts with nothing, so once it has said its own
number, the first busy holder to notice hands it a few triggers, keeping
what brings it nearest 50%, and the spare holds work from then on. Once
three say 20% each, that is 60, and weft stops one. It never runs
fewer than one holder per 200 triggers, and never more holders than
triggers, because a trigger is never split between two holders. A holder
tells weft its number when it has a first minute measured, once more a
minute after it took triggers or handed some over, and otherwise only once
it has moved 10 points from the last one it said, so while the load holds
steady it writes nothing and your database can sleep. When a holder's
average reaches 70%, it hands part of its triggers over whether or not a
spare is up, keeping as many as should bring it back to about 50%. A single
trigger that keeps a holder busy on its own stays where it is.

If you have VS Code pointed at the install, the editor keeps a live
connection open to it, and the install and your database stay awake while
it does. Once the window has been out of focus or untouched for 5 minutes,
the editor closes that connection, and opens it again when you come back.
Switching the editor back to your local install closes it at once.

If you want to change one of the install's defaults (how many triggers a
holder takes, the CPU holders aim for and the CPU at which one hands
triggers over, a holder's
CPU and memory, how many builds run at once, how
many bad tokens a minute one address may present, how many times a trigger's
failing wake is delivered before it is given up), edit it in
`deploy/terraform/gcp/variables.tf` in your fork and run the workflow again.

If weft's own work keeps failing, like waking one of its services, handing
a run to your project's workers, or acting on your infrastructure, the
install sends an email. A run of your program that fails sends no email.
`weft status --on prod` lists these failures under `waiting on weft:`. You
get an email about one of them in two cases. If weft has given up retrying,
you get the email at once. If weft is still retrying, you get it on the
first failed try after the failure has been going on for 5 minutes
(`alert_after_minutes` changes that, see below). weft asks Google to email you once per
problem, and once more each time it clears and then comes back. Google also
sends at most one of these emails every 5 minutes for the whole install, a
limit separate from the wait above, so a problem that comes up soon after
an email may get no email of its own. It still shows in `weft status`. If
you run weft on your own machine, there is no email: `weft status`, without
`--on prod`, is where these failures show.

When an email arrives, run `weft status --on prod` in each of your
projects' folders:
a problem with the install as a whole, like waking one of its services,
shows in all of them, and a problem with one project, like weft failing to
hand a run to its workers or to act on its infrastructure, shows only in
that project's status.

The emails go to a person with the Owner role on your Google Cloud project,
usually the account you made the project with. Only a person granted Owner
on the project itself counts: groups, service accounts, and an Owner role
inherited from a folder or organization are skipped. If several people hold
Owner directly, the emails go to the one whose address comes first in
alphabetical order. If you want them sent somewhere else, add a repository
secret named `WEFT_ALERT_EMAIL` holding that address (a secret, so a public
fork's run logs never show it). The alert that sends these
emails is on by default and costs about $0.35 a month on Google's bill. If
you want it off, set `alerts_enabled` to `false` in
`deploy/terraform/gcp/variables.tf`. If you want weft to wait a different
number of minutes before emailing about a failure it is still retrying,
change `alert_after_minutes` in the same file (set it to 0 to be emailed at
the first failure). Each of these changes takes effect the next time you run
the install workflow.

weft generates the rest of its secrets itself: the key your saved
connections are encrypted with, the key live callers' tickets are signed
with, and the install's own operator key. The encryption key is made once and kept
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
the load balancer's address) and waits until the name points there, then
until Google has issued the domain's certificate, which usually takes a few
minutes once the record is seen. The record has to point straight at that
address: on Cloudflare, set it to "DNS only" (the grey cloud), because a
proxied record hides the load balancer and Google never issues the
certificate. If the name already points somewhere else (a tunnel to your
machine, say), remove that record first. `weft domain list --on prod` shows
every domain with its record and where its certificate stands, and says why
when it is stuck.

Once the certificate is issued, the domain becomes the install's address.
Every link it hands out from then on uses it: sign-in callbacks, webhook
addresses, file links, `weft target export`. The `run.app` address keeps
working, so nothing you already handed out breaks. Two things are yours to do
then, and `weft domain add` (or `weft domain list`) prints both:

- Each provider you sign in through (Google, Meta, TikTok...) only sends
  people back to addresses its app lists. Add the new callback,
  `https://weft.example.com/access/oauth/callback`, to each app's allowed
  redirect URLs.
- A site that talks to the install was given its address by `weft target
  export`; run it again so the site uses the domain.

A domain can also serve one project instead of the whole install:
`--for api` answers that project's routes at the root of the domain
(`https://api.example.com/users/42`, as well as
`https://<the install's address>/connect/local/<project id>/users/42`), sending its calls
straight to the project's own Cloud Run service, and
`--for frontend --to <address>` passes visitors on to the project's
frontend on Cloud Run, at the address `weft frontend ls --on prod` shows for
it.

### Moving from your machine to the cloud

If you built an app on your machine and now run it on a cloud install, you do
not have to make every connection again or upload every file by hand. `weft
migrate` copies them from the install a command acts on to the one `--to`
names, and the install they come from keeps everything:

```bash
weft migrate connections --to prod --all
weft migrate files --to prod --prefix project/<project id>/
```

You always say what to copy: `--all`, or any mix of `--id <id>` (repeat it
for several), `--since`/`--until` (a date like `2026-10-01`, or a time), and
`--service`/`--project` for connections or `--prefix` for files. Add
`--dry-run` to see what would be copied first. Each line says what happened
to one item: copied, already there, or skipped and why.

A connection's secrets are never in the clear on the way: the cloud install
makes a key pair, your machine seals the connections to it, and only the
cloud install opens them. Each connection keeps its id, so a program that
names one finds it. The connections your nodes use are picked per install,
so the command ends by printing the `weft connect --grant` lines to pick them
on the cloud too. A connection that spends one of the install's own keys
copies only when the cloud install holds that key. A copied sign-in (Google,
Slack and the like) renews itself with a refresh token that both installs
now hold. Most providers accept that, but one that replaces its token on
every renewal breaks whichever install renews second; the command names the
sign-ins it copied, and if one stops working on the cloud, connect it again
there with `weft connect`.

A file keeps its key, its version and its lifetime, so a value that names it
still finds it. A run's file stays behind with its run, unless you kept it.
Run `weft migrate` from the project's folder, where `--to prod` means the
target the project names.

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
one right away, `weft token revoke <id> --on prod`. The install's own key,
the one the workflow gave you, is not among them: it is no row in the
install's database but its secret in Secret Manager (`<name>-install-key`),
so it works until that secret changes, and `weft token revoke` cannot retire
it. An install set up before that also lists it under the name `bootstrap`,
a row an older weft wrote for the same key: revoking that row is up to you,
and the key keeps working through its secret either way.

Every command takes `--on` the same way: `weft status --on prod`,
`weft runs --on prod`, `weft logs <run-id> --on prod`. Without it, a
command acts on your own machine; nothing makes prod the default.

### What `activate --on prod` does

Your machine compiles the project first, so a mistake shows up on your
screen before anything is uploaded. Then it uploads the project's sources,
only the files the install does not have yet, and the install does the
rest: it compiles them again itself, builds the images the program needs,
and activates.

### Deploy a change

If you want a change to go live on a program that is already on, run these
(`weft activate` would only turn on what is off):

```bash
weft infra upgrade --on prod --all
weft resync --on prod --all
```

The upgrade puts the new build on the infrastructure that is running (with
none, it does nothing), and the resync brings onto it every trigger that is
on and behind. `--all` includes
every instance's (for those, go and read [programs with
instances](instances.md)). By default nothing that arrives meanwhile is lost:
the triggers' door holds what arrives and runs it on the new version once
they are back, a caller on a route is answered `503` with a `Retry-After` and
its client tries again, and the runs going finish on the old version first.
For the other answers (`--door`, `--held`, `--running`), go and read [taking
triggers down](lifecycles.md#taking-triggers-down-three-questions).

Only the change is uploaded, and only the image it touches is rebuilt. If
nothing changed, nothing is built.

### Roll back

Each install keeps its own history of what was deployed to it. If you want
to go back, find the version in `weft tree --on prod`, then let
`weft branch` put its files back in your folder and deploy them:

```bash
weft branch <version> --on prod
weft infra upgrade --on prod --all
weft resync --on prod --all          # and `weft activate --on prod` for what is off
```

If your folder holds changes no version records yet, `weft branch` records
them on prod as a version first and prints the `weft branch` that brings them
back. Unless `weft clean --images` removed them since, the old version's
images are still there from its first deploy, so nothing is rebuilt.

### Connections

Each install keeps its own connections and its own picks of them: which
connection a node uses is picked on the install, never written in your
files ([picked on each install](../build/connections.md#picked-on-each-install)).
A node you connected on your machine has no pick on prod until you make one
there, with the same walkthrough:

```bash
weft connect --on prod
```

If a run on prod reaches a node with nothing picked there, it is refused
before it starts, and the message names the node and
`weft connect --node <node>`; add `--on prod`. An instance's own connection (a
field written `@instance_filled`) lives in the install it was made on, so
your customers connect through your frontend on prod, the same way they do
locally.

### Look at prod from the editor

If your project names a target, the graph has a switch in its top right
corner: **local**, then each target. If you want to see what prod is
running, click **prod**. The graph then shows the program as it was last
deployed there, along with prod's runs, status and connections. From then
on, **Run**, **Activate**, the Runs list and the version tree all go
to prod, as if you had typed `--on prod` on each command.

You cannot edit the program there, because it is a copy of what prod runs.
You can change its connections, though: **Connect** and **Disconnect** on a
node change which connection prod uses, and prod sets up again any trigger
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
hand from the Actions tab. It takes the weft CLI built from the source your
install runs: first the one the install workflow stored in your install
(signed in with `WEFT_OPERATOR_KEY`, its sha256 checked), which is how a fork
gets its own CLI without compiling it, then the one weft's release built
(waiting for it while that release is still building), and it compiles the
CLI only when neither has one. Then it deploys
the program with `weft infra upgrade --all` then `weft resync --all`, then
builds `front/` with Docker and deploys it to the repository's service. If
you run `weft ci add` again, it replaces the file only if you have not edited
it; otherwise it stops and leaves your edits alone.

The workflow never turns anything on: the upgrade replaces only the
infrastructure that is running, and the resync moves only the triggers that
are on, so anything somebody took down stays down. Before the first deploy,
run `weft infra start --on prod` and `weft activate --on prod` once by hand.

The runs going when the deploy replaces what they run on finish first, and
the deploy waits for them however long that takes. If you would rather they
stop at once, set `RUNNING: cancel` in the workflow's `env:` block, where it
says `RUNNING: wait` (that counts as an edit). If GitHub cuts the job off,
the upgrade or resync it had started carries on by itself on the install. If
it was cut off before the resync began, wait until `weft infra status --on
prod` shows the upgrade done, then run `weft resync --on prod --all` by hand.
If the resync had already begun, it finishes on its own, and `weft status --on
prod` shows how far it is.

If you are starting a new project, `weft new <name> --ci gcp` writes the
workflow for you.

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

A `Route` answers at `https://<the install's address>/connect/local/<project id>/<path>`,
where `<project id>` is the `id` under `[package]` in your `weft.toml` and
`<path>` is the route's `path`. The `local` and the project id are the same on
every install, prod included, so a route's address on prod differs from your
machine's only in the host. The
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
out between the copies that are up. A copy with nothing to do stops listening
so the install can sleep. When a call wakes it, that call is decided at once on
what the copy heard before it went quiet, and so is every call until the copy
hears back from weft's broker: under a second normally, a few seconds when the
broker was asleep too and has to start. During that window the copy counts
itself as the only copy, so if several copies wake at the same moment, each
can let in up to the full `callsAtOnce`. The per-minute limits barely move:
they can only overshoot by the calls that reach the copy in those few
seconds. We chose this on purpose: the other way would make the first call
after every pause wait for the broker, and on a sleeping install that means
waiting for it to start.

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
  message says why. weft tries again on its own twice, five and then ten
  minutes later, and after the third refusal it stops and leaves the
  message in `weft domain list`. Once you have fixed the cause, any `weft
  domain add` or `weft domain rm` tries again at once.
- **weft does not start, and its log says the database session "cannot
  LISTEN"**: `WEFT_DATABASE_URL` goes through a pooler. Add a direct address
  of the same database as the `WEFT_DATABASE_LISTEN_URL` secret ([a
  database](#a-database)) and run the install workflow again.
- **`weft status` shows a `database: busy` line**: in the last 15 minutes,
  requests waited more than a second for a database connection because
  every connection of a copy was in use. The line says how many times, when, what the
  database takes and each copy's share, for example `database: busy. Since
  14:02 UTC, requests waited 42 times for a database connection, the last at
  14:20 UTC (the database takes 50 connections, and each copy of the broker
  holds at most 7)`, followed by what gives the install more. The waiting
  copy's log says `requests are waiting for a database connection` at the
  first wait, then at most once a minute with the count. The CLI may print
  `warning: the install is busy, so this command waits and asks again`
  (also when Cloud Run has no free copy of a service), at most every 30
  seconds. If the command only reads, or Cloud Run had no free copy, it
keeps asking until the install answers, and Ctrl+C stops it. If
  you see the line often, give the install more connections ([how many
  connections the install takes](#how-many-connections-the-install-takes)).
- **A copy of the dispatcher or broker does not start, and its log says
  `this copy's share is ... and it needs at least ...`**: the copy limits
  split the database's connections too thin. On Cloud SQL, raise the tier
  (and `WEFT_CLOUD_SQL_MAX_CONNECTIONS` with it, if you set one); on a
  database you brought, move to a plan that takes more connections. Either
  way you can instead lower `WEFT_DISPATCHER_MAX_COPIES` or
  `WEFT_BROKER_MAX_COPIES`. Then run the install workflow again.
- **`weft resync` or `weft status` says `the new version takes over once 2
  runs on the old one end`**: the resync waits for the runs still going on
  the old version, however long they take, and names them. `weft stop
  <run>` ends one now; `weft cancel-activate` gives the resync up and leaves
  its triggers down. Ctrl+C only stops following: the resync goes on.
- **`weft status` says `resync: stopped with triggers it took down still
  down`**: the line gives why, then `Next:` with what to do. If the
  database was busy or out of reach, or the resync waits on infra or an
  activation still landing, it goes on by itself. If its build cannot run
  the triggers, `Next:` says to fix the source, give the resync up with
  `weft cancel-activate`, and run `weft activate`. Otherwise fix the cause
  and run `weft resync --resume`, or give it up with `weft cancel-activate`.
  For how a resync retries, go and read
  [resync](lifecycles.md#resync).
- **A trigger that keeps a connection open shows "waiting for a holder to
  take it" on its node**: a running holder with room takes it as soon as
  weft tells the holders the trigger is there; otherwise weft is starting
  one. If it stays that way, the holders' logs say why.
- **A run on prod is refused with "has no ... connection picked on this
  install"**: the node was connected on your machine, not on prod. Run the
  `weft connect --node <node>` it names, with `--on prod`.
- **An image build fails**: the error quotes the builder's log, with the
  `cargo` or `apt` line that failed. Fix what it names and deploy again;
  only what changed rebuilds.
- **The frontend deploy fails at its `google-github-actions/auth` step, or
  its `gcloud run deploy` is refused `run.services.get`**: the repository is
  not the one its frontend was added with (`weft frontend ls --on prod` shows
  it, spelled as GitHub spells it, `owner/name`). Remove the frontend and add
  it again with the right `--repo`, then run `weft target export` again.
- **The install workflow stops in "create the cloud" with "no email to send
  the alerts to was found"**: `WEFT_ALERT_EMAIL` is not set, and no person
  holds the Owner role directly on your Google Cloud project, the only kind
  of owner the workflow emails. If you want the alerts, add a repository
  secret named `WEFT_ALERT_EMAIL` holding the address that should get them; if you
  don't, set `alerts_enabled` to `false` in
  `deploy/terraform/gcp/variables.tf`. Then run the workflow again.
- **The install workflow stops in "create the cloud"**: Terraform's output
  in that step names the resource it could not make and why. If it names a quota (Compute Engine addresses or CPUs, say), raise
  it in the Google Cloud console and run the workflow again.
