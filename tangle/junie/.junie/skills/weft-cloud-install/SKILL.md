---
name: weft-cloud-install
description: "Read when the user wants weft itself on their Google Cloud for the first time, or to upgrade or resize the weft already there: getting the user's GitHub and Google Cloud accounts and CLIs ready, the fork, the one-time gcloud block, the database, the fork's variables and secrets, running the install workflow, the first operator key, upgrading, and sizing. Deploying a project to an install that already exists is weft-deploying, not this."
---

# Putting weft on GCP

A cloud install has no machine of its own. weft's parts and programs' workers
are Cloud Run services that scale to zero. Triggers that keep a connection
open run on holders, which weft starts only while at least one such trigger is
on. Builds run on Cloud Build, timers on Cloud Tasks, and infrastructure on
Compute Engine machines.
Everything weft keeps lives in a Postgres database the user brings, by its
address. So an install with no trigger that keeps a connection open and no
infrastructure up runs nothing between calls and, within Google's and the database's free tiers,
costs next to nothing: what it does pay for is storing weft's own images. It is made by the "install on GCP"
workflow in the user's own fork of weft. The user owns the GCP project, the
billing, the database and the fork; you do the work, through their `gh` and
`gcloud`.

## First: the two CLIs, logged in

Everything after this step you run yourself, so this step comes first and
you do not move on until both work. Check before asking anything:
`gh auth status` and `gcloud auth list` (plus `gcloud config get project`).
Whatever already works, you skip.

- **GitHub.** No account: send them to `https://github.com/signup` and wait.
  No `gh`: install it for their system (the instructions are at
  `https://cli.github.com`). Then they run `gh auth login` themselves
  (it opens a browser; GitHub.com, HTTPS, log in with a web browser), since
  only they can sign in. Check with `gh auth status`.
- **Google Cloud.** No account: send them to
  `https://console.cloud.google.com`, where they sign in with a Google
  account, accept the terms and add a billing account (a card is required
  even for the free tier). No `gcloud`: install it for their system
  (`https://cloud.google.com/sdk/docs/install`). Then they run
  `gcloud auth login`. Check with `gcloud auth list`.

The only things the user does by hand are what needs them in a browser:
signing up, the card, and the two logins. Say each step in plain words and
wait for them to say it is done.

## Then you do the install

The steps are in the cloud guide
(`https://weavemindai.github.io/weft/running/cloud.html`, section "Install
weft on GCP"). Run them with the user's CLIs, telling them what you are about
to do before anything that creates or costs something:

1. **The project.** A new project made just for weft (weft's own accounts get
   broad rights inside it, so nothing else should live there):
   `gcloud projects create <id>`, then link billing
   (`gcloud billing accounts list`, `gcloud billing projects link <id>
   --billing-account <account>`). If they already made one for weft, use it.
   A billing account links only a few projects (five on a new one): if the
   link fails with `Cloud billing quota exceeded`, offer to reuse a project
   made for weft before, or to unlink one they no longer use
   (`gcloud billing projects unlink <id>`), and let them choose.
2. **The region.** If the user is on the free tier and has no reason to be
   anywhere in particular, the install goes in `us-west1` (zone
   `us-west1-b`) and its database in Neon's `aws-us-west-2`: both are in
   Oregon, so every call the install makes to its database stays short.
   Google's always-free machine and 5 GB of storage exist only in
   `us-west1`, `us-central1` and `us-east1`, and Neon runs on AWS and Azure,
   not Google, so that pair is the one place both free tiers sit side by
   side. If they need another region, take theirs, and the Neon region
   nearest it.
3. **The fork.** `gh repo fork WeaveMindAI/weft --clone=false`. A new fork
   has its workflows turned off: turn them on with
   `gh api -X PUT repos/<fork>/actions/permissions -F enabled=true`.
4. **The `gcloud` block** below, as written, with the three values on its
   first line filled in (`FORK` is the fork's `owner/repo`, with GitHub's
   capitals, because Google compares it letter for letter); never invent a
   flag. Its IAM steps retry on their own while IAM catches up on a
   brand new project, so a few "waiting for IAM" lines are normal.

   ```bash
   PROJECT=my-project REGION=us-west1 FORK=me/weft
   NUMBER=$(gcloud projects describe $PROJECT --format='value(projectNumber)')
   # On a new project IAM can take a minute to catch up (with the services
   # just enabled, with the account just made), so each IAM step below is
   # retried until it holds.
   retry() { until "$@"; do echo "waiting for IAM to catch up, trying again in 10 seconds"; sleep 10; done; }

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

5. **The fork's variables**, with `gh variable set <NAME> --repo <fork>
   --body <value>`: `GCP_PROJECT_ID`, `GCP_REGION`, `GCP_ZONE`,
   `TF_STATE_BUCKET`, `GCP_WORKLOAD_IDENTITY_PROVIDER` and
   `GCP_INSTALL_SERVICE_ACCOUNT`. A project's frontend is added later, from
   the project (read weft-deploying), never here.
6. **The database.** If the user has no Postgres, read weft-database. If
   they already have a
   Postgres they want, put its address in the `WEFT_DATABASE_URL` secret
   with `gh secret set WEFT_DATABASE_URL --repo <fork>` (piping it in, never
   echoing it into the chat), and when that address goes through a pooler,
   a direct address of the same database in `WEFT_DATABASE_LISTEN_URL`.
7. **The workflow.** `gh workflow run "install on GCP" --repo <fork>`, then
   follow it with `gh run watch` in the background. It runs for a while.

Its last step prints the install's address (a Cloud Run address,
`https://weft-role-dispatcher-<number>.<region>.run.app`, working at once)
and the line that logs in with the first operator key, which you read with
`gh run view <id> --log` (plain `gh run view` leaves it out). Run that line
from the project, after `weft target add` (read weft-deploying): it reads the
key from Secret Manager straight into `weft login --key-stdin`, so the key
never passes through the chat or a file. Never print the key itself.

The install needs no domain: its own address is HTTPS and free. A domain
costs money, so it is never part of the install; read weft-deploying when
the user asks for one.

Upgrading weft on the cloud is merging upstream into the fork
(`gh repo sync <fork>`) and running the workflow again, then rebuilding the
user's CLI from that commit (`./setup.sh --cli`). Sizing (how many triggers
one holder takes, a holder's CPU and memory, how many builds run at once,
the machine builds compile on) is a default in
`deploy/terraform/gcp/variables.tf` in the fork: change it there, commit,
and run the workflow again. The build machine defaults to the one Cloud
Build's free minutes cover; a bigger one (`build_machine = "E2_HIGHCPU_8"`)
compiles a program's image faster and is paid by the minute, once per
version of the program, never per worker.

Removing weft from GCP is deleting the project made for it, since weft and
everything it started live there and nowhere else: `gcloud billing projects
unlink <id>`, then `gcloud projects delete <id>` (Google keeps it 30 days,
`gcloud projects undelete <id>` brings it back). It deletes everything in
it, so say so and wait for the user's yes. The database is theirs, at its
own provider, and stays.

For deploying projects to the install once the key is in `weft login`,
read weft-deploying.
