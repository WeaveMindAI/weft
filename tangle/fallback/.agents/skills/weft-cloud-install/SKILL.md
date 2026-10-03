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
2. **The fork.** `gh repo fork WeaveMindAI/weft --clone=false`. A new fork
   has its workflows turned off: turn them on with
   `gh api -X PUT repos/<fork>/actions/permissions -F enabled=true`.
3. **The `gcloud` block** below, as written, with the three values on its
   first line filled in (`FORK` is the fork's `owner/repo`, with GitHub's
   capitals, because Google compares it letter for letter); never invent a
   flag. Its IAM steps retry on their own while IAM catches up on a
   brand new project, so a few "waiting for IAM" lines are normal.

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

4. **The fork's variables**, with `gh variable set <NAME> --repo <fork>
   --body <value>`: `GCP_PROJECT_ID`, `GCP_REGION`, `GCP_ZONE`,
   `TF_STATE_BUCKET`, `GCP_WORKLOAD_IDENTITY_PROVIDER` and
   `GCP_INSTALL_SERVICE_ACCOUNT`. A project's frontend is added later, from
   the project (read weft-deploying), never here.
5. **The database.** If the user has no Postgres, read weft-database. If
   they already have a
   Postgres they want, put its address in the `WEFT_DATABASE_URL` secret
   with `gh secret set WEFT_DATABASE_URL --repo <fork>` (piping it in, never
   echoing it into the chat), and when that address goes through a pooler,
   a direct address of the same database in `WEFT_DATABASE_LISTEN_URL`.
6. **The workflow.** `gh workflow run "install on GCP" --repo <fork>`, then
   follow it with `gh run watch` in the background. It runs for a while.

Its summary (`gh run view <id>`) prints the install's address (a Cloud Run
address, `https://weft-role-dispatcher-<number>.<region>.run.app`, working at
once) and the command that reads the first operator key from Secret Manager.
Run that command yourself, then the user pastes the key into `weft login`
(it is a secret: never echo it into the chat).

The install needs no domain: its own address is HTTPS and free. A domain
costs money, so it is never part of the install; read weft-deploying when
the user asks for one.

Upgrading weft on the cloud is merging upstream into the fork
(`gh repo sync <fork>`) and running the workflow again, then rebuilding the
user's CLI from that commit (`./setup.sh --cli`). Sizing (how many triggers
one holder takes, a holder's CPU and memory, how many builds run at once)
is a default in `deploy/terraform/gcp/variables.tf` in the fork: change it
there, commit, and run the workflow again.

For deploying projects to the install once the key is in `weft login`,
read weft-deploying.
