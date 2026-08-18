# Getting started

By the end of this page you will have a Weft program running on your machine,
reachable over HTTP, with a graph view showing you every value as it flows
through. Everything runs locally, and nothing asks you to sign up.

## Install

```bash
git clone <this repo>
cd weft
./setup.sh
```

The script builds three binaries and symlinks them into `~/.local/bin`:
`weft` (the CLI you'll use for everything), `weft-dispatcher` (the local
daemon that runs your programs), and `weft-runner` (the worker the dispatcher
spawns). If `~/.local/bin` isn't on your `PATH`, the script prints the exact
line to add to your shell rc.

Re-run `./setup.sh` anytime to rebuild. Useful flags: `--debug` for faster
incremental builds, `--vscode` / `--browser` / `--cli` / `--daemon` to build
a subset, `--uninstall` to remove everything (`--purge` also deletes local
state).

## The runtime

Weft programs are run by a small daemon on your machine, and `./setup.sh`
already started it: by the time the script finishes, the runtime is up and
waiting on port 9999 (override with `WEFT_HTTP_PORT=…` if that's taken).

If you ever need to manage it by hand: `weft daemon status` says whether it's
up, `weft daemon logs -f` tails it, `weft daemon stop` and `weft daemon start`
do what they say.

## Your first program

```bash
weft new hello
cd hello
weft run
```

That's a complete cycle: `weft new` scaffolds a project, `weft run` compiles
it, registers it with the daemon, fires an execution, and streams the events
live until it finishes. The program it just ran is `main.weft`:

```weft
greeting = Text { value: "hello world" }
out = Debug

out.data = greeting.value
```

Read it as a graph: a `Text` node holds a string, a `Debug` node prints
whatever reaches it, and the one connection hands the string over. Before
anything ran, the compiler checked that connection: the types match, the
ports exist, nothing required is left unwired. Everything you build in Weft
works this way, whatever the size.

## See it as a graph

Open the project folder in VS Code (with the Weft extension installed, which
`./setup.sh` does by default). Open `main.weft` and the graph appears next to
the code: the same program drawn as boxes and wires, refreshed every time
you save.

<!-- CAPTURE: VS Code split view, main.weft source on the left, the two-node
     graph rendered on the right. -->

Now hit run from the editor (or `weft run` again in the terminal) and watch
the graph light up: each node flashes as it executes, and clicking a node
shows the exact values that went in and came out. You will spend most of your
Weft life in this view. Iterating on a program means running it, clicking the
step that looks wrong, seeing the actual value, and fixing that step.

<!-- CAPTURE: the same graph mid-run, one node highlighted, the inspector
     panel open showing the value on the edge. -->

## Put it on a URL

So far the program only runs when you ask it to. The `ApiEndpoint` node
makes it run when the outside world calls: every HTTP request that hits its
path fires a fresh execution.

Replace `main.weft` with:

```weft
api = ApiEndpoint { path: "hello" }
reply = Reply

reply.started = api.started
```

`Reply` doesn't exist yet; you're about to write it, and writing your own
nodes is routine in Weft. A node is a folder under `nodes/` with two files:
a small JSON manifest describing its ports, and the Rust that does the work.

```bash
mkdir -p nodes/reply
```

`nodes/reply/metadata.json`:

```json
{
  "type": "Reply",
  "label": "Reply",
  "description": "Echo the caller's request body back as JSON.",
  "tags": ["live", "http"],
  "icon": "Send",
  "color": "#06b6d4",
  "inputs": [
    { "name": "started", "type": "Boolean", "required": false,
      "description": "Kick from the ApiEndpoint trigger." }
  ],
  "outputs": [
    { "name": "done", "type": "Boolean", "required": false,
      "description": "Fires true once the response has been sent." }
  ],
  "requires_infra": false
}
```

`nodes/reply/mod.rs`:

```rust
use async_trait::async_trait;
use serde_json::{json, Value};

use weft::caller::{InboundMessage, OutboundChunk};
use weft::node::NodeOutput;
use weft::{ExecutionContext, Node, NodeManifest, WeftResult};

#[derive(NodeManifest)]
pub struct ReplyNode;

#[async_trait]
impl Node for ReplyNode {
    async fn run(&self, ctx: ExecutionContext) -> WeftResult<()> {
        // The connected HTTP caller handle; fails loud on a non-HTTP run.
        let http = ctx.http_caller().await?;
        let req = http.request_parts()?;
        let echoed = match &req.body {
            InboundMessage::Json(v) => v.clone(),
            InboundMessage::Text(s) => Value::String(s.clone()),
            InboundMessage::Bytes(b) => json!({ "bytes": b.len() }),
        };
        http.respond(OutboundChunk::Json(json!({ "you_sent": echoed }))).await?;
        ctx.pulse_downstream(NodeOutput::new().set("done", true)).await
    }
}
```

The node contains no server code and no routing. The `ctx` hands it a live
caller handle and it answers. Everything a node needs from the outside world
(callers, storage, messaging between nodes, secrets) comes through the `ctx`
the same way, already built and already hardened, so the code you write is
only your own logic.

Activate it:

```bash
weft activate    # compiles, registers, prints the live URL
```

And fire it from anywhere:

```bash
curl -X POST "<the URL activate printed>" \
     -H "content-type: application/json" \
     -d '{"message":"hi"}'
```

Each request is a full execution you can inspect: `weft follow <project-id>`
streams them live, and in the editor you'll see them appear in the executions
list as they happen.

## Add a human

Some steps of a real workflow belong to a person, an approval or a judgment
call. In Weft a human is a node like any other. A `HumanQuery` node
suspends the execution, the pending task shows up in the Weft browser
extension, and when the person answers, the execution resumes exactly where
it stopped, whether that took a minute or a week.

The extension takes a few minutes to set up (build it, load it in your
browser, connect it with a token); the walkthrough is in
[docs/browser-extension.md](./browser-extension.md). Once it's connected, add
a `HumanQuery` node to any program: the task appears in the extension, and
answering it wakes the program up.

<!-- CAPTURE: the browser extension popup showing one pending task, next to
     the graph with the HumanQuery node in its waiting state. -->

## Give it infrastructure

Some capabilities need a process that's always there: a WhatsApp bridge
holding a phone session, a local LLM server, a database, a headless browser.
In Weft these are infra nodes, and they sit on the graph like everything
else. You drop the node in, run `weft infra start`, and Weft provisions a
real container for it (locally in a `kind` cluster, which the daemon sets up
lazily; install [kind](https://kind.sigs.k8s.io/docs/user/quick-start/) and
[kubectl](https://kubernetes.io/docs/tasks/tools/) once per machine). You
write no YAML and manage no containers; the rest of your graph just gets a
URL to talk to.

The lifecycle is four verbs:

```bash
weft infra start        # bring everything up, wait for ready
weft infra stop         # scale to zero, keep disks and addresses
weft infra upgrade      # roll a new image or spec
weft infra terminate    # delete it all, disks included
```

`weft infra status` shows per-node health and endpoints, and the graph shows
each infra node's status right on the node.

## Where to go next

- [The Weft language](./weft-lang-guide.md): the full syntax, the type
  system, groups, loops, triggers.
- [Authoring nodes](./authoring-nodes.md): everything the `ctx` gives you
  (buses, storage, secrets, callers, suspensions) and the test rig that
  proves your node works.
- [The browser extension](./browser-extension.md): build it, load it, and
  connect it, so programs can hand tasks to people.
- [The access system](./access-system.md): how programs connect to external
  accounts (Slack, Google, email) without secrets ever touching your code.
- [Event triggers](./event-triggers.md): programs that wake on Slack
  messages, emails, and schedules instead of HTTP.

## CLI cheat sheet

```
weft new <name>            Scaffold a new project directory.
weft build                 Compile; also runs under the hood for weft run.
weft run [--detach]        Compile, register, fire an execution.
weft follow <id|color>     Live SSE events.
weft logs <color>          Historical logs for one execution.
weft stop <color>          Cancel an execution.
weft ps                    List registered projects.
weft activate <project>    Mint webhook/cron entry URLs.
weft deactivate <project>  Drop all entry tokens for the project.
weft rm <project>          Remove project (+ its binary).
weft daemon start          Launch the local dispatcher in the background.
weft daemon stop           Stop it.
weft daemon status         Report whether it's running.
weft daemon restart        Stop + start.
weft daemon logs [-f]      Tail the dispatcher's log.
weft token mint/ls/revoke  Manage browser-extension tokens.
weft describe-nodes        Print the per-project catalog as JSON.
weft executions            List past executions.
weft events <color>        Print one execution's node events.
weft clean [<color>]       Purge journal data (defaults: keep 30 days).
weft infra start/stop      Provision / scale-to-zero the project's infra.
weft infra upgrade         Roll a new image/spec (stop + start).
weft infra terminate       Delete the project's infra (and its disks).
weft infra status          Per-node infra status + endpoint URLs.
weft infra node-stop <id> [--force]   Stop one infra node (--force overrides on_stop).
weft infra node-terminate <id>        Terminate one infra node (delete its resources).
weft add <git-url>         Install an external node package (phase B).
```

## When something goes wrong

- `dispatcher unreachable`: run `weft daemon start`.
- `weft-runner not found`: re-run `./setup.sh`; the daemon expects
  `weft-runner` next to its own binary.
- Port 9999 in use: `WEFT_HTTP_PORT=19999 weft daemon start`, then
  `export WEFT_DISPATCHER_URL=http://localhost:19999` so every `weft`
  command finds it.
- `kind` not found: install it; the daemon prints an actionable error the
  first time an infra node needs provisioning.
