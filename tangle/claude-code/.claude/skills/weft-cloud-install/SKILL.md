---
name: weft-cloud-install
description: "Read when the user wants weft itself on their Google Cloud for the first time, or to upgrade or resize the weft already there: the fork, the one-time gcloud block, the fork's variables, running the install workflow, the first operator key, upgrading, and growing the machine. Deploying a project to an install that already exists is weft-deploying, not this."
---

# Putting weft on GCP

A cloud install is one small machine (an `e2-micro`, in Google's free tier in
`us-west1`, `us-central1` and `us-east1`) running Postgres and weft's
listener, with weft's other parts and programs' workers on Cloud Run, builds on Cloud Build, timers on Cloud Tasks
and infrastructure on Compute Engine machines. It is made by the "install on
GCP" workflow in the user's own fork of weft, and every step of it is the
user's: they own the GCP project, the billing and the fork. You walk them
through it; you run none of it.

The steps, all in the cloud guide
(`https://weavemindai.github.io/weft/running/cloud.html`, section "Install
weft on GCP"): fork weft on GitHub; run the one-time `gcloud` block the guide
gives (it makes the Terraform state bucket and the robot account the fork's
workflows act as), as an owner of a GCP project with billing on (a new project made just for weft: weft's own accounts get broad rights inside it, so you tell the user to keep nothing else there); set the
fork's repository variables (`GCP_PROJECT_ID`, `GCP_REGION`, `GCP_ZONE`,
`TF_STATE_BUCKET`, `GCP_WORKLOAD_IDENTITY_PROVIDER`,
`GCP_INSTALL_SERVICE_ACCOUNT`, and optionally `WEFT_FRONTEND_REPOS`,
`WEFT_MACHINE_TYPE`, `WEFT_SERVERLESS_ROLES`, `WEFT_LISTENER_MACHINE`); then run the workflow from the
fork's Actions tab. Hand them the guide's commands as written, with their
values filled in; never invent a flag.

Its summary prints the install's address (`https://<an IP address>`, working
a minute or two after the machine boots) and the command that reads the
first operator key from Secret Manager. The user runs that command on their
own machine and keeps the key to paste into `weft login`.

Upgrading weft on the cloud is merging upstream into the fork and running
the workflow again, then rebuilding the user's CLI from that commit
(`./setup.sh --cli`), or `weft activate --on prod` refuses with "this
version was written by weft X". If the machine is too small, the user sets
`WEFT_MACHINE_TYPE` in the fork (say `e2-small`, which is not free) and runs
the workflow again; the machine stops, grows and starts with its disk.

For deploying projects to the install once the key is in `weft login`,
read weft-deploying.
