---
name: weft-deploying
description: "Read when the user wants weft or a program live for real people on their cloud, asks about prod, a target, `--on`, a domain or GCP, or a deploy failed: targets and logging in, domains, the deploy workflow `weft ci add` writes, handing it its settings with `weft target export`, what a cloud build is and how it fails, rolling back, and connections on a cloud install. The deployer specialist reads it; you read it before dispatching one."
---

# Deploying

A [target] is one weft install, named in the project's `weft.toml`:

```toml
[targets.prod]
url = "https://weft.example.com"
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
it, Tangle reads the weft-cloud-install skill and walks them through it
in conversation. It is never handed to a specialist, which cannot talk to
the user.

## A name instead of an IP

The install answers at its IP address from the start. If the user wants a
domain (`weft.example.com`), they buy it anywhere, then the deployer runs
`weft domain add weft.example.com --on prod`: it prints the DNS record to
set (type `A`, the name, the machine's address) and waits until it resolves,
and the machine then gets the certificate itself. `--for api` serves one
project's routes at the root of a domain, and `--for frontend --to <its
https address>` passes a domain on to the project's frontend.
`weft domain list --on prod` shows every domain and its record.

A connection to Google needs the install to have a domain: Google refuses a
bare IP as the place to send a person back after they sign in, and weft's connect flow says so. Set up the domain before any Google
connection.

## Workers and long runs

`weft workers` shows a project's worker settings (copies kept warm, the most
copies, runs per copy, CPU, memory) and which come from the install. Before you run `weft workers set --min-instances 1`, tell the user: it keeps
one copy warm so the first call never waits for a start, and on a cloud
install that copy is billed all the time. A run that may take longer
than an hour on a cloud install (Cloud Run cuts a request there) runs with
`weft run --long`, or with `longRuns` on the trigger that starts it, and gets
a worker of its own for up to seven days.

A trigger that holds a connection open (a stream or a socket it listens to)
needs the install's listener on the machine. If the install moved `listener`
to `WEFT_SERVERLESS_ROLES`, such a trigger is refused with a message naming
that; the fix is the user's, in the fork's variables.

## Setting a project up for its cloud

Somebody installed weft on their cloud once (above), and its summary printed
the install's address.
What a project needs from there, in order, each run from the project's
folder:

1. `weft target add prod <address>`. It writes `[targets.prod]` into
   `weft.toml`, which is committed, so the team shares the name.
2. `weft login prod`: asks for an operator key in a hidden prompt, and
   checks it against the install before storing it in
   `~/.config/weft/credentials.toml`, which only the user can read. The
   user types the key into their own terminal; you never ask them for it,
   and never pipe one in (`--key-stdin` is for the user's own scripts).
3. `weft ci add --cloud gcp`: writes `.github/workflows/deploy.yml`. The
   workflow, run by hand from the repository's Actions tab, builds the CLI
   of the exact weft the install runs, deploys the program with
   `weft activate --on <target>`, then builds `front/` with Docker and runs
   it on Cloud Run, reaching the install over its private network.
   `weft new --ci gcp` writes the same file when the project is made. If it
   refuses because the file "was changed since weft wrote it (or weft never
   wrote it)", the file is somebody's own and weft never overwrites it: tell
   the user, and leave it.
4. `weft target export prod --github`: needs step 2 first, and the
   repository already on GitHub. It sets the variables and secrets the
   workflow reads, with `gh` (logged in, run inside the repo), and mints
   two credentials on the install: an operator key for CI, and a caller
   token for the frontend's server, scoped to this project. Every run
   mints new ones, so it runs once per setup, or again when a CI
   credential was lost. Without `--github` it prints them, and the
   secrets are shown only that once.

If the project has a frontend (`front/Dockerfile`), the repository also
has to be listed in `WEFT_FRONTEND_REPOS` of the weft fork, with the install
workflow run again since, or the workflow cannot sign in to GCP to deploy
it. That is the user's to do in the fork. A project with no `front/` needs
none of that: the workflow deploys the program and stops.

## What a deploy does

`weft activate --on prod` compiles the project here first (a compile
error fails fast, on this machine), then uploads a snapshot of the
project's sources, only the files the install does not have yet. The
install compiles that snapshot itself, builds the images it needs, and
activates. So a deploy of an unchanged project builds nothing.

Every target keeps its own versions: `weft tree --on prod` lists what was
deployed there. To roll back, put an older one's files back in the folder
with `weft branch <id> --on prod`, then `weft activate --on prod`. Its
images are already built, so nothing rebuilds. `weft branch` refuses while
the folder holds changes no version records; `--discard` throws those
changes away, and you pass it only when the user said to.

## The frontend on a cloud install

The frontend's server reads three variables, set by the workflow on Cloud
Run and by `front/.env` on this machine:

| Variable | On a cloud install | On this machine |
|---|---|---|
| `WEFT_DISPATCHER_URL` | the install's private address | `http://127.0.0.1:14111` |
| `WEFT_TOKEN` | the caller token `weft target export` minted | a token from `weft token mint` |
| `WEFT_PUBLIC_URL` | the install's public address | `http://127.0.0.1:14111` |

On this machine, 14111 is the default port; if the install was started on
another one, `~/.local/share/weft/ports.json` has it (`public`).

What else the server reads (the program's database from `weft infra env`,
`BETTER_AUTH_SECRET`) goes to Cloud Run as one dotenv file: the user writes
it on their machine with `weft infra env <node> --on prod --into
front/.env.prod` plus the door's address from `weft infra list-doors --on
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
set up again), and a connection prod does not hold yet is the user's to
add, with the command you hand them (`weft connect --on prod --node <node>`).
A run on prod that reaches a node with nothing picked there is refused
before it starts, naming that command.

A member's connection (a field written `@member_filled`) lives in the
install the member made it on, so members connect on prod through the
site, as they do here.

## When a deploy fails

- **A compile error**: printed before anything leaves this machine. The
  same fix as on `local`.
- **"this version was written by weft X, and this install runs weft Y"**:
  the CLI and the install are different weft versions. In CI the workflow
  already builds the right CLI. On this machine, tell the user the two
  ways out the error names: the CLI of the install's version (built from
  the fork's commit the install ran), or updating the install (merging
  upstream into the fork and running its install workflow again).
- **"this version's `nodes/base_catalog/` is not the one weft X ships"**:
  the catalog was edited in place or seeded by another build of weft. Run
  `weft catalog update` in the project, then deploy again.
- **An image build failed**: the install's builder log is quoted in the
  error, the `cargo` or `apt` line included. Fix the node it names; the
  next deploy rebuilds only what changed.
- **401 from the install**: the key is wrong or was revoked. The user runs
  `weft login prod` again; in CI, `weft target export prod --github` again.
- **429 on a route**: one of the route's limits turned the call away.
  `weft status --on prod` lists, under "refused calls", which node refused,
  how often in the last two minutes, and by which limit; each limit is a
  setting on the route's node.
