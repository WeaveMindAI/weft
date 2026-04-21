# Weft: Local Getting Started

How to boot a dispatcher, scaffold a project, run it, wire a
webhook, and see live events. Everything here runs against the
subprocess worker backend and sqlite journal, no cloud, no docker.

## Build

```bash
git clone <this repo>
cd weft
cargo build -p weft-dispatcher -p weft-runner -p weft-cli
```

The three binaries land in `target/debug/`:

- `weft` — the CLI.
- `weft-dispatcher` — the always-on daemon.
- `weft-runner` — spawned per execution by the dispatcher; never
  invoked directly.

Add `target/debug` to your `PATH` for the rest of this guide, or
prefix everything with the full path.

## Start the dispatcher

The dispatcher listens on port 9999 by default. Override with
`WEFT_HTTP_PORT` if it clashes. The dispatcher needs to know where
`weft-runner` lives; set `WEFT_RUNNER_PATH`.

```bash
# one-shot foreground (useful for tailing logs):
WEFT_RUNNER_PATH="$PWD/target/debug/weft-runner" \
  ./target/debug/weft-dispatcher

# or use the CLI daemon lifecycle:
weft start       # forks a dispatcher in the background
weft status      # confirm reachability
weft daemon-stop # when you're done
```

The dispatcher's ops dashboard lives at `http://localhost:9999/`.

## Your first project

```bash
weft new hello
cd hello
```

You get:

```
hello/
├── weft.toml          # project id + manifest
├── main.weft          # starter graph: Text -> Debug
├── nodes/             # place your own rust nodes here (phase B)
├── .weft/             # local state
└── .gitignore
```

`main.weft` starts as:

```weft
# Project: hello

greeting = Text { value: "hello world" }
out = Debug

out.value = greeting.value
```

Run it:

```bash
weft run
```

`weft run` compiles, registers the project with the dispatcher,
kicks off a fresh execution, then streams SSE events until the
execution completes. Use `--detach` if you don't want to watch.

Look at the ops dashboard in a browser: `http://localhost:9999/`.
You should see the `hello` project listed.

## Webhook entry

Make a webhook-style project:

```bash
cat > main.weft <<'EOF'
# Project: webhook-demo
receive = ApiPost
print = Debug { label: "webhook" }
print.value = receive.body
EOF

weft run --detach   # registers the project
weft activate $(weft ps | awk 'NR==2 {print $1}')
# response contains the minted /w/{token} URL
```

Fire the webhook from anywhere:

```bash
curl -X POST "http://localhost:9999/w/<TOKEN>" \
     -H "content-type: application/json" \
     -d '{"message":"hi"}'
```

Look at the logs:

```bash
weft logs <color>
# or live:
weft follow <project-id>
```

## Human-in-the-loop (`@weft` pending tasks)

1. Mint an extension token:

   ```bash
   weft token mint --name "my laptop"
   # prints the wm_ext_xxxxx token + the URL to paste in the extension
   ```

2. Build + load the browser extension (WXT-based):

   ```bash
   cd extension-browser
   pnpm install
   pnpm dev
   ```

   This launches a browser with the extension auto-installed. Paste
   the URL from step 1 into the extension's popup.

3. Write a weft program with a HumanQuery node. The node calls
   `ctx.await_form(...)`; the dispatcher mints a form URL; the
   extension sees the task in its pending list.

4. Complete the task from the extension. The dispatcher resumes
   the suspended execution with the form submission.

## Infra nodes (kind cluster)

Infra nodes (Slack bridge, Postgres sidecar, WhatsApp, etc) run
inside a local `kind` kubernetes cluster. The dispatcher provisions
it lazily on first `weft infra up`. Prereqs:

```bash
# Once per machine:
#   https://kind.sigs.k8s.io/docs/user/quick-start/
#   https://kubernetes.io/docs/tasks/tools/
```

Phase A ships the KindInfraBackend scaffolding; full sidecar
wiring (event streaming, credential injection) lands as specific
infra-backed nodes are ported.

## CLI cheat sheet

```
weft new <name>           Scaffold a new project directory.
weft build                Compile; also runs under the hood for weft run.
weft run [--detach]       Compile, register, fire an execution.
weft follow <id|color>    Live SSE events.
weft logs <color>         Historical logs for one execution.
weft stop <color>         Cancel an execution.
weft ps                   List registered projects.
weft activate <project>   Mint webhook/cron entry URLs.
weft deactivate <project> Drop all entry tokens for the project.
weft rm <project>         Remove project (+ its binary) from the dispatcher.
weft status               Check dispatcher reachability.
weft start / daemon-stop  Manage the local dispatcher daemon.
weft token mint/ls/revoke Manage browser-extension tokens.
weft describe-nodes       Print the per-project catalog as JSON.
weft infra up/down        Provision/tear down infra pods (kind).
weft add <git-url>        Install an external node package (phase B).
```

## Troubleshooting

- `dispatcher unreachable`: the daemon isn't running. `weft start`
  or run `weft-dispatcher` in a terminal.
- `weft-runner not found`: point the dispatcher at your binary with
  `WEFT_RUNNER_PATH`.
- Port 9999 in use: `WEFT_HTTP_PORT=19999 weft-dispatcher ...`, and
  pass `--dispatcher http://localhost:19999` to every `weft`
  command (or set `WEFT_DISPATCHER_URL`).
- `kind` not found: install it. The dispatcher logs an actionable
  error when the first infra node is provisioned.
