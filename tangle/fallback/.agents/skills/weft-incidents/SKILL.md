---
name: weft-incidents
description: "Read when a program on a cloud install is failing, stuck or slow for real users: reading what is failing there, a database infra node that ran out of memory, weft's own database logs, stuck runs, a resync that stopped, weft held back by its own database, and what to tell the user."
---

# When a cloud install is in trouble

People are using this install, so you read before you change anything, and
you ask the user before anything that stops something there, in one plain
sentence that says what will stop. Every command below runs from the
project's folder and names the target with `--on <target>`.

## Read what is failing

1. `weft status --on <target>`: the project's triggers, each infra node's
   state, the calls a route turned away (under "refused calls"), and weft's
   own work that is stuck (under "waiting on weft:").
2. `weft runs --on <target> --project <project id> --status running`: the
   runs still going, each with its start time. One that started long ago and
   should have ended by now is stuck.
3. `weft runs --on <target> --project <project id> --status failed --since
   1h`: what broke in the last hour.
4. For one run, `weft logs <run id> --on <target>`, then `weft events <run
   id> --on <target>`. The weft-debugging skill has how to read them.
5. For an infra node (a database, a bridge), `weft infra logs <node> --on
   <target>` prints what its containers wrote.
6. For weft's own database, when the install has its own on Cloud SQL, its
   logs are in Cloud Logging: `gcloud logging read
   'resource.type="cloudsql_database"' --project <project> --freshness 1h`.
   weft's own services are the same command with
   `resource.type="cloud_run_revision"`.

## What you do about it

- **A database infra node ran out of memory.** weft says so in `weft status`
  and on the node's card in the editor. Raise `memory` on the node (and
  `cpu` with it if the user agrees to the bigger machine), then `weft infra
  upgrade --infra <node> --on <target>`. The upgrade replaces the database,
  and the triggers that reach it go down until it is back, so ask first.
- **A run is stuck.** Read it first (above). If it is waiting on something
  that will never come, ask the user, then `weft stop <run id> --on
  <target>`.
- **A resync stopped partway.** `weft resync --on <target> --resume` takes
  it up where it stopped, with the answers and the build it began with.
- **weft is held back by its own database.** You see it one of two ways.
  `weft status` shows a `database: busy` line: requests waited for a
  database connection, and the line ends with what gives the install more.
  Or a copy of weft's dispatcher or broker will not start, and its log (the
  `cloud_run_revision` read above) says `this copy's share is ... and it
  needs at least ...`. Both mean the database's connections are spread too
  thin. Each copy takes an equal share of them, and the fork's repository
  variables `WEFT_DISPATCHER_MAX_COPIES` (2 when unset) and
  `WEFT_BROKER_MAX_COPIES` (4 when unset) cap how many copies run. When the
  install's database is its own on Cloud SQL, the fix is a bigger tier:
  `WEFT_CLOUD_SQL_TIER`, with `WEFT_CLOUD_SQL_MAX_CONNECTIONS` raised beside
  it if one is set. On a database the user brought, it is a plan with more
  connections at their provider. Either way, lowering one of the two copy
  ceilings gives each copy a bigger share instead. Then run the install
  workflow again. The weft-cloud-install skill has the steps, and the
  restart they cost.
- **A copy of weft runs out of memory.** A request answers `Service
  Unavailable`, often while `weft resync` or `weft maintenance off` brings
  many triggers back, and the role's `cloud_run_revision` log says `Memory
  limit of ... exceeded`. Raise that role's memory with the fork's
  repository variable (`WEFT_DISPATCHER_MEMORY`, 1Gi when unset, or
  `WEFT_BROKER_MEMORY`, `WEFT_LISTENER_MEMORY`, `WEFT_SUPERVISOR_MEMORY`,
  512Mi each), then run the install workflow again. A value set by hand in
  the Cloud Run console does not last: the next install run puts the
  variable's back.

Then tell the user plainly what happened, what you did, and what is back:
"The database ran out of memory at about 3pm and stopped answering, so I
gave it twice the memory and restarted it. Orders go through again."
