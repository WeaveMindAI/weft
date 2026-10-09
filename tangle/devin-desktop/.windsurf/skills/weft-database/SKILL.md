---
name: weft-database
description: "Read when a cloud install needs its database: the user has no Postgres for it, or asks which to use. Gets one that scales to zero (Neon), with the user only signing up and pasting one key, and puts its addresses in the fork's secrets. Part of weft-cloud-install."
---

# A database for a cloud install

A cloud install keeps everything in a Postgres it reaches by address, read
from the fork's `WEFT_DATABASE_URL` secret. Any Postgres works; if its
address goes through a pooler, the install also needs a direct address of the
same database in `WEFT_DATABASE_LISTEN_URL`.
When the user has none, offer Neon: a hosted Postgres that sleeps while
nothing uses it, with a free plan. Before they sign up, tell them what the
free plan's limits mean for weft (What it costs, below). If they would rather use one they already
pay for, take its address instead and skip the rest of this.

## What the user does

Two things, in their browser:

1. Sign up at `https://console.neon.tech/signup`. They need no project
   there: you make it.
2. Make an API key for their organization: in the Neon console, the
   organization's **Settings**, **API keys**, **Create new API key**, and
   paste it to you once. A key made there names the organization by itself,
   which creating a project needs.

The key is a secret. Put it straight into `NEON_API_KEY` in the same command
that uses it, never into a file, and never repeat it in the chat. Say both
steps plainly, and wait until they are done.

## What you do

Make the project through Neon's API. The region is `aws-us-west-2` when the
install is in GCP's `us-west1` (the free-tier default, read weft-cloud-install
for why); otherwise the Neon region nearest the install's GCP region, by its
id (Neon lists them at `https://neon.com/docs/introduction/regions`). Run all of this as one Bash call:
each call starts a fresh shell, so the key and the file name would not
survive into a second one. The `trap` deletes the file holding Neon's answer,
which contains the database's password, when that shell ends:

```bash
NEON_API_KEY='<the key>'
neon_json=$(mktemp)
trap 'rm -f "$neon_json"' EXIT
curl -sS https://console.neon.tech/api/v2/projects \
  -H "Authorization: Bearer $NEON_API_KEY" -H "Content-Type: application/json" \
  -d '{"project": {"name": "weft", "region_id": "<a region id>", "default_endpoint_settings": {"autoscaling_limit_min_cu": 0.25, "autoscaling_limit_max_cu": 1, "suspend_timeout_seconds": 300}}}' > "$neon_json"
jq -e '.connection_uris[0]' "$neon_json" >/dev/null || { cat "$neon_json"; exit 1; }
direct=$(jq -r '.connection_uris[0].connection_uri' "$neon_json")
pooler_host=$(jq -r '.connection_uris[0].connection_parameters.pooler_host' "$neon_json")
printf '%s' "$direct" | sed "s/@[^/:]*/@$pooler_host/" | gh secret set WEFT_DATABASE_URL --repo <fork> &&
  printf '%s' "$direct" | gh secret set WEFT_DATABASE_LISTEN_URL --repo <fork>
```

The `default_endpoint_settings` start the database at the smallest size (0.25
compute unit), let it grow to 1 under load, and put it to sleep after 300
seconds with no queries. Without them Neon picks the size itself, on some
accounts a fixed 1 compute unit, which bills about $77 a month if it never
sleeps.

If it printed Neon's answer and stopped, that is Neon refusing, and the answer
says why (a wrong region id, a limit of the plan). `WEFT_DATABASE_URL` gets the
pooled address (the direct one with its host swapped for `pooler_host`), which
weft does its work on. `WEFT_DATABASE_LISTEN_URL` gets the direct one, because
a pooler cannot hold a `LISTEN`. If `gh` printed an error, the project exists
but one or both secrets are missing (when `WEFT_DATABASE_URL` failed, the
second was never tried): do not run the block again, which would make a second
project. Check which are set with `gh secret list --repo <fork>`, have the user
open the project's **Connect** dialog in the Neon console (the pooled address,
or the direct one with pooling off), and set each missing one with
`gh secret set <NAME> --repo <fork>`, piping in its address. If neither printed anything, both secrets are set: tell the user the
database is ready, and in one or two plain sentences what they got. It runs at
the smallest size (0.25 compute unit), grows up to 1 under load, and sleeps
after 5 minutes with no queries; Neon bills only the hours it is awake, about
$0.11 per compute-unit-hour on the paid Launch plan, and the free plan gives
100 compute-unit-hours a month. If they want it bigger or never asleep, those
are settings of the compute in the Neon console (**Branches**, the compute,
**Edit**), and either one costs more. Then go back to weft-cloud-install.

## What it costs

Neon's free plan gives a project 100 CU-hours a month, which at 0.25 CU is
about 400 hours awake. The database sleeps 5 minutes after the last thing that
used it, and an idle install wakes nothing: weft books a wake-up only for a
moment something is actually due (a hibernation ending, a queued event's
retry), and its services hold no busy connection to the database once quiet.
Infrastructure that runs fine keeps nothing awake either, because a machine
reports only when its state changes.

Two things do keep it awake. A trigger that keeps a connection open keeps it
awake around the clock, because its holder checks in through weft every 10
seconds; on the free plan that uses up the month's hours, and when they run
out the database stops until the next month or a paid plan, and the install
stops with it. A VS Code editor pointed at the install keeps it awake while
the person uses it; once the window has been out of focus or untouched for 5
minutes, the editor stops its live connection so the install can sleep, and
picks back up when they return.
