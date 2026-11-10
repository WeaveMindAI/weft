# Infrastructure nodes

Some nodes need something running: a database, a model server, a bridge that
holds a session open. Your node returns a description of it, and the supervisor
makes what runs match it: Docker containers on your machine, or a Compute
Engine machine of its own on a cloud install.

```json
"requiresInfra": true,
"images": ["images/credential"],
"publishes": "postgres"
```

```rust
async fn provision_infra(&self, ctx: InfraProvisionContext, input: ValueBag)
    -> WeftResult<InfraSpec>
{
    let database: String = input.get("database")?;
    Ok(InfraSpec {
        units: vec![Unit {
            name: "db".into(),
            containers: vec![Container { /* image, env, ports, probes */ }],
            ..Default::default()
        }],
        volumes: vec![Volume { /* the disk */ }],
        endpoints: vec![Endpoint { name: "sql".into(), /* ... */ }],
        ..Default::default()
    })
}
```

Declaring `requiresInfra: true` without writing `provision_infra` is an error
naming your node when the infra is provisioned, rather than a silent nothing.

## The spec

| Field | What it holds |
|---|---|
| `units` | What runs. Most nodes have exactly one |
| `volumes` | Disks, kept across stop and upgrade by name, and scratch space emptied at each start |
| `endpoints` | Named ports your node and the project's other nodes reach it on, and whether anything outside the project may |
| `keep_on_terminate` | Disks to keep even through a terminate, for the next `weft infra start` to pick up |
| `health_rules` | What weft does to the project's triggers when a unit of this node stops being ready, and when it undoes it. For the defaults and how to write your own, go and read [when a unit stops being ready](#when-a-unit-stops-being-ready) |
| `notes` | If the copy you declare runs short of something it normally has (an older version kept for its data, say), one plain sentence each saying what and how to fix it. `weft infra status` prints them as warnings for as long as the copy runs. Usually empty |

A unit is a group of containers that run side by side, with optional init
containers that run one after another before them. It also says what machine
it needs (`machine`: `cpu`, `memory`, and `gpu` with a `kind` and a `count`)
and what stop does to it (`onStop`: stop it, or keep it running until
terminate). On a cloud install, weft picks the cheapest machine that holds
those numbers, shared-core ones included: a quarter CPU with `1Gi` is
Compute Engine's `e2-micro`. A number left unset is the sum of the
containers' own limits. If you need one machine type in particular,
`machine.kind` names it (`n2-highmem-8`) and skips the pick. On your machine
the unit's numbers cap nothing: only a container's own `limits.memory` caps
it. weft refuses a unit there that asks for a GPU you do not have. If your
unit has to run a program on the person's own machine instead of containers,
a local install can do that: for how, go and read
[a process on your machine](#a-process-on-your-machine).

Whoever uses your node pays for that machine, so if the right size depends on
their load, let them set it. Declare the size your node needs by default in
its `metadata.json`, next to `requiresInfra`:

```json
"requiresInfra": true,
"features": { "machineInputs": { "cpu": "1", "memory": "2Gi" } }
```

weft gives the node three inputs: `cpu` and `memory`, which default to those
sizes, and `machineType`, which has no default. A `cpu` or a `memory`
written in the program that does not read as a size (`1`, `500m`; `2Gi`,
`512Mi`) is refused at compile time, naming the input, and a wired one fails
the node when it arrives. Read the three into a `MachineShape` in `provision_infra` and put it on
the unit:

```rust
let machine = MachineShape::from_inputs(&input)?;
```

A blank `machineType` comes back as `None`, so the cloud picks the machine
from `cpu` and `memory`. The node fails to load if it declares one of the
three inputs itself, if it sets `machineInputs` without `requiresInfra`, or
if a default is not a size.

On your machine weft cannot pick a GPU by kind, so a unit that
asks for any GPU gets every GPU the machine has, and `weft infra start` and
`weft infra status` warn you about it.

A unit asking for one L4 GPU, with the CPUs and memory beside it:

```rust
Unit {
    name: "model".into(),
    machine: MachineShape {
        cpu: Some("8".into()),
        memory: Some("32Gi".into()),
        gpu: Some(Gpu { kind: "nvidia-l4".into(), count: 1 }),
    },
    ..Default::default()
}
```

The kinds a cloud install attaches are `nvidia-l4` (1, 2, 4 or 8 on one
machine), `nvidia-tesla-t4` and `nvidia-tesla-p4` (1, 2 or 4), and
`nvidia-tesla-v100` (1, 2, 4 or 8). Any other kind or count is refused when
the machine is picked, and the refusal says what is allowed. The CPUs and
memory you ask for must also fit a machine with that many GPUs.

Each container also takes two probes: `readiness` says when it is up, and `liveness` restarts it after
`failureThreshold` failures in a row.

## The spec has to be stable

**Write the same spec every time, for the same inputs.** weft compares what you
return against what is running, and re-applies when they differ.

So never generate a password inside `provision_infra`. Every call would produce
a different spec, and your database would be redeployed on every check.

If your container mints its own credential on first boot, that is what
`ctx.publish_access` is for: the container makes it, your node publishes it
once, and `ctx.published_access()` reads it back afterwards.

## Local images

A Dockerfile in your node's own folder, named in `images`:

```text
postgres/database/
  metadata.json       "images": ["images/credential"]
  images/credential/
    Dockerfile
```

The last path segment is the name you reference in the spec. The path is
relative to your node's directory.

An upstream image reference passes through exactly as you wrote it, and **a
mutable tag is not resolved to a digest**. So `postgres:16` rolling underneath
you produces no change in the spec and no re-apply. Pin by digest if you want
an image change to actually land.

## A process on your machine

Some programs cannot work from a container, because what they need is on the
person's own machine, such as their login to a CLI like `claude` or `codex`,
or a folder of theirs to work in. If your node needs one of those,
give the unit a `process` instead of containers: weft runs that one program
directly on the person's own machine, as them. Only a local install has
such a machine, so a cloud install refuses the node before anything starts,
naming the unit.

```rust
use weft::infra::{Endpoint, EndpointTarget, Expose, InfraSpec, Process, ProcessPort, ProcessReadiness, Unit};
use weft::needs::{Need, NeedStage, PackageManager};

let python3 = Need {
    name: "python3".into(),
    stage: NeedStage::Run,
    check: vec!["python3".into(), "--version".into()],
    install: [(PackageManager::Apt, [("default".to_string(), vec!["python3".to_string()])].into())].into(),
    how: None,
    recipe: None,
};
let tool = Need {
    name: "Tool".into(),
    stage: NeedStage::Run,
    check: vec!["tool".into(), "--version".into()],
    install: Default::default(),
    how: Some("download it from https://tool.example".into()),
    recipe: None,
};
Ok(InfraSpec {
    units: vec![Unit {
        name: "server".into(),
        process: Some(
            Process::new(vec!["python3".into(), "server.py".into()])
                .with_file("server.py", include_str!("server.py"))
                .with_ports(vec![ProcessPort::new("http", "PORT")])
                .with_readiness(ProcessReadiness::http("http", "/health"))
                .with_needs(vec![python3, tool]),
        ),
        ..Default::default()
    }],
    endpoints: vec![Endpoint {
        name: "api".into(),
        target: EndpointTarget::Process { unit: "server".into(), port: "http".into() },
        expose: Expose::Project,
    }],
    ..Default::default()
})
```

| Field | What it holds |
|---|---|
| `command` | The program and its arguments. The first word is looked up on the process's `PATH`, and a start is refused when it is not there |
| `files` | Text files weft writes into the process's folder, by their path in it (`server.py`, `conf/settings.json`). They are not executable, so run a script through its interpreter (`python3 server.py`, `sh run.sh`) |
| `env` | Variables for the process. One with the same name as a variable it gets from the machine wins |
| `ports` | Each port it listens on, as `ProcessPort::new(name, variable)`: endpoints and the readiness check use the name, and weft puts the port's number in the variable. The variable is uppercase letters, digits and `_`, not starting with a digit, and no other port or `env` entry may use it |
| `readiness` | If you leave it out, the unit is ready as soon as the process runs. With `ProcessReadiness::http(port, path)` it is ready once `GET path` on that port answers 2xx or 3xx within a second. For 30 seconds after each start (`.with_startup_seconds(n)` to change it), a failing check still counts as starting |
| `needs` | What it needs from the machine, checked before it starts. Each is a `Need` like the ones in `deps.toml` (for the fields, go and read [what a node needs from the machine](packaging.md#what-a-node-needs-from-the-machine)). It must be a `NeedStage::Run` one: a process is never built, so a build need is refused. One with a `recipe` is installed by weft when it is missing, see [needs weft installs itself](#needs-weft-installs-itself) |

Have the process listen on `127.0.0.1`, at the number in its port's
variable: that is where weft checks it and where every endpoint points. Any
program on the machine can reach that port, whatever `expose` says, so never
serve a secret on it to whoever asks.

The process runs in a folder of its own, which is its working directory:
`infra-processes/<copy>/<unit>/work/` in the install's folder
(`~/.local/share/weft` for the default install; `weft daemon which` prints
it). weft writes `files` there each time it starts the unit. What the process
writes there itself stays until the unit is removed from the spec or the node
is terminated, except changes to one of your `files`, which weft writes over
at the next start. The files are part of the unit, so a changed script is a
changed unit: like a changed container, it lands after a stop and a start
(`weft infra stop --force` for a unit that keeps running through a stop), or
when `weft infra upgrade` restarts the copy on your new build. If the process
crashes or the machine restarts, the start that follows runs the version
already applied.

It gets `HOME` and the `PATH` weft's runtime was started with (the one your
shell had at `weft daemon start`), so a program finds the person's own
installs and logins the way it does when they run it by hand. When weft
installed a need for it, that need's `bin` folders come first on that `PATH`. It also gets each of `USER`, `LOGNAME`, `SHELL`,
`LANG`, `LC_ALL`, `LC_CTYPE`, `TMPDIR` and the `XDG_*` folder variables that
the runtime has, because a program the person runs by hand reads them. Then
come `env` and one variable per port. Nothing else of weft's environment
reaches it. If a value you set in `env` has a line break, the spec is refused:
put text of several lines in one of its `files` instead.

If a need is missing, the start fails and names it. When the need lists a
package for this system, the message includes the command that installs it.
For a program no package manager has, leave `install` empty and put the
command that installs it in `how`: the message gives that command, or says to
install it by hand when `how` is empty. Or give the need a `recipe`, and weft
installs it for the person.

### Needs weft installs itself

If the program your process runs is one weft can download and install
without `sudo` (a CLI shipped as a release archive, an npm package), give its
`Need` a `recipe`, and the person never has to install it:

```rust
use weft::needs::{Os, Recipe};

let tool = Need {
    name: "Tool".into(),
    stage: NeedStage::Run,
    check: vec!["tool".into(), "--version".into()],
    install: Default::default(),
    how: Some("download it from https://tool.example".into()),
    recipe: Some(Recipe {
        folder: "tools/tool".into(),
        script: [(Os::Linux, include_str!("install_linux.sh").into()), (Os::Macos, include_str!("install_macos.sh").into())].into(),
        bin: vec!["bin".into()],
        prelude: String::new(),
    }),
};
```

Before the process first starts, weft looks for the program in three places,
in order:

1. On the process's `PATH`. If the person has their own install, that one is
   used and nothing is installed.
2. In weft's folder for it, `~/.local/share/weft/needs/<folder>/` (here
   `~/.local/share/weft/needs/tools/tool/`), if an earlier install finished there
   and `check` passes with the `bin` folders first on the `PATH`.
3. Nowhere: weft runs the script for this system into that folder. While it
   runs, the unit is starting, and the infra's progress says
   `server: installing Tool (its log: /home/you/.local/share/weft/needs/tools/tool.install.log)`.
   There is no time limit, since a download takes as long as it takes.

The process then starts with the `bin` folders first on its `PATH`.

That folder is the machine's, not the install's: every program of every weft
install on the machine uses the same one, so a need is installed once and
never again. If two units start the same need at once (two nodes, two
projects, two installs), one installs it while the others say
`waiting for another setup to finish installing Tool`, and then they use what
it installed. They take turns through a lock file beside the folder,
`needs/tools/tool.lock`.

The script runs in bash with `set -euo pipefail`, as the person (`sudo` is
never available to it), after sourcing `prelude`: put helpers that several
scripts share there. It gets these variables:

| Variable | What it holds |
|---|---|
| `WEFT_NEED_DIR` | The need's own folder, empty when the script starts. Install into it and nowhere else |
| `WEFT_NEEDS_ROOT` | The group's folder, `~/.local/share/weft/needs/tools/` here: the first half of `folder`. Scripts of one group can keep something they share there, such as a runtime several of them run on |

`folder` is two names, `<group>/<name>`. A `bin` entry is a path inside the
need's folder, or a `../` path into the group's folder.

A folder only counts as installed once the script exited 0 and `check`
passed. weft then writes a `.weft-installed` file into it. If the script is
stopped halfway (a stop, a crash, the machine shutting down), that file is
missing, so the next start empties the folder and runs the script again. If
you want a newer version later, delete the folder, and the next start
installs it again.

If the script needs system packages it cannot install without `sudo`, list
them in the need's `install`. weft checks them with the package manager
before the script runs, and the unit fails naming the command that installs
them:

```text
Tool needs system packages weft cannot install without sudo; install them with:  sudo apt-get install -y git
then start the infra again (`weft deps-install`, in the project's folder, installs what this machine lacks for it, asking for your password where a system package needs it)
```

If the script fails, the unit fails with the last 20 lines of its log and the
need's `how`. `weft deps-install`, run in the project's folder, installs these
needs too, the same way and through the same lock, after asking for the
system packages they lack.

Only an infra process's need can have a recipe. For why a need in a node's
own `deps.toml` cannot, go and read [what a node needs from the
machine](packaging.md#what-a-node-needs-from-the-machine).

A process shares the machine with everything else the person runs, so a unit
that runs one and sets `machine` or `fsGroup` is refused. It mounts none of
the node's `volumes` either: the only storage weft gives it is its own folder.
A unit runs containers (init containers included) or a process, never both.

An endpoint to one of its ports is an `EndpointTarget::Process`, and its
address is `http://127.0.0.1:<port>`. weft picks a free port when the unit
first starts and keeps it until the unit is removed, so the address stays the
same across restarts. With `Expose::SameNetwork`, weft also gives out that
same `127.0.0.1:<port>` as the endpoint's address on the machine's network,
which is the address a frontend asks for.
`Expose::Public` and a [live panel](#a-live-panel) reach it the same way they
reach a container.

Through its life it behaves like a container:

- If the process exits on its own, weft starts it again: after a tenth of a
  second the first time, twice as long each time after that up to a minute,
  and a tenth of a second again once it ran for ten seconds. Its files are not
  written again for that. While it waits, the unit is not ready, and the
  reason weft gives is the exit code and the last 20 lines it printed.
- It keeps running when weft's runtime restarts, and the runtime finds it
  again instead of starting a second one. If the machine restarted, or the
  agent that runs it (below) died, weft starts it again the next time it
  checks on its units, writing its files again first, and kills whatever the
  old one left running before it does. If that start fails, the unit reads as
  failed, with the reason, and weft tries the start again each time it
  checks, so a cause that clears by itself (a program that comes back on the
  `PATH` a little after a reboot) brings the unit back without you.
- If the first start of an apply fails (a program the command names is not
  there, say), what was applied is what is wrong, so weft does not try it
  again: the unit reads as failed, with the reason, until the next apply.
- A stop asks the process to exit (`SIGTERM`) and gives it up to ten seconds.
  Then weft kills whatever is left of it and of everything it started. A unit with
  `on_stop: StopBehavior::KeepRunning` keeps running through a stop, as a
  container unit does.
- `weft infra logs` shows what it printed.

In `ps`, each process unit shows up as `weft-runtime unit-agent process`: a
small agent weft starts for it, which runs your process, starts it again when
it exits, and is what stays up when weft's runtime restarts. If the agent
dies, on Linux your process dies with it, and on macOS weft kills it before
it starts the unit again. What your process
printed is in `process.log` beside its `work/` folder, and if the agent itself
cannot start, the error points at `agent.log` there.

## A server inside another infra node

If your node runs its server inside another infra node (a program it
installs and starts in a computer, which the computer's own server passes a
path on to), it declares no unit, only the endpoint, as an
`EndpointTarget::Within`: the other node's endpoint, as the program wired it
into your node's input, and the path under it.

```rust
let computer: InfraHandle = input.get("computer")?;
Ok(InfraSpec {
    endpoints: vec![Endpoint {
        name: "api".into(),
        target: EndpointTarget::Within { infra: computer, path: "/apps/claude".into() },
        expose: Expose::Project,
    }],
    ..Default::default()
})
```

`provision_infra` gets the handle because your node fires once its input
arrives, after the other node is up. weft keeps the handle and the path, not
an address: every `ctx.endpoint("api")`, `ctx.endpoint_of`, and read of a
[live panel](#a-live-panel) asks where the other node's endpoint answers at
that moment and adds the path, so your endpoint still works after the other
node's address changes (terminated and started again, it gets new ports). If the other node is not running, the
lookup fails saying so and naming it. The path must be a plain path with at
least one segment, the endpoint can only be `Expose::Project`, and the
endpoint it lives inside must have an address of its own, so a `Within`
inside a `Within` is refused when it is looked up. A node with no unit gives
the host nothing to do: a start marks it running, and a stop marks it
stopped (its endpoint stops resolving, and your server keeps running inside
the other node). `weft infra status` shows
`inside endpoint 'api' of 'box' at /apps/claude` where other nodes show their
address.

A terminate has more to do, because your server and its files are inside the
other node, and forgetting the row would leave them there. So before the row
goes, weft sends `DELETE` to your endpoint's own path on the other node
(`DELETE <the computer's address>/apps/claude`, never a path below it), and
the other node answers with a 2xx once nothing of yours is left there, or when
nothing was. If you write a node that other nodes live inside, that is the
request it has to answer. A computer answers it by running the remove command
the app registered beside its start command (`~/.weft/apps/<name>.remove`),
then deleting the app's files in `~/.weft/apps/`.

If the other node is there but not running, nothing can answer, so the
terminate fails before it touches anything and leaves your node as it was:

```text
'claude' lives inside 'box' (endpoint 'api' at /apps/claude), which is stopped, so what 'claude' put there cannot be removed and 'claude' was left as it was. Start 'box' (`weft infra start box`) and terminate 'claude' again, or terminate 'box' too, which removes everything inside it
```

If the other node answers with an error, the terminate fails with
`asking 'box' to remove it failed:` and what it answered, and your node reads
`terminating` until a terminate gets through. If the other node has no copy
at all, nothing of yours can be in it, so weft asks nothing. When you
terminate the whole program, your node goes first if the other node is
running, and after it if it is not, since its disk and your files on it are
deleted with it.

## Reaching it

```rust
let endpoint = ctx.endpoint("sql").await?;
let (host, port) = endpoint.host_and_port()?;
```

`ctx.endpoint(name)` resolves one of your declared endpoints and waits until
something actually answers there, rather than handing you an address the
moment the container reports ready.

| Call | Gives you |
|---|---|
| `url()` | The address, cached, no round trip |
| `host_and_port()` | The two apart, for a client that wants them that way |
| `call(method, path, body)` | An HTTP request against it, retried once through a routing gap |
| `public_url()` | For an endpoint exposed with `Expose::Public`, the address a caller outside the install uses; `None` for any other endpoint |

By default (`Expose::Project`), only the project's own workers and
infrastructure can reach an endpoint. If you want anything else to reach it,
set `expose`:

- `Expose::SameNetwork` opens it to programs on the install's own network:
  anything running on your machine locally, the install's private network on
  a cloud, but never the internet. A frontend uses this to reach a program's
  database. On your machine the address is `127.0.0.1:<port>`, at a port weft
  picks when the unit first starts and keeps until the unit is removed or the
  infra terminated, so a copy of it in a frontend's `.env` keeps working
  through restarts and upgrades.
- `Expose::Public { path: "/hooks" }` opens it to the internet, over HTTP, at
  the install's public address. Hand the caller (a provider delivering
  webhooks, say) `public_url()`.

The install serves every project at that address, so your `/hooks` lives
under a prefix of its own: `public_url()` is
`<address>/infra/<project>/<copy>/hooks` (`<copy>` names this copy of the
node), and the install strips the
prefix again, so your container still sees `/hooks`.
`weft infra status` prints the same address under the node. On a cloud
install it is the install's own address. On your machine it is the tunnel's
address while the tunnel is open
([a public address](../build/public-address.md)), and
`http://127.0.0.1:14111/...` while it is closed, which nothing on the internet
can reach.

If an endpoint hands out a credential, keep it to the project. The Postgres
node shows the split: the port clients sign in on can be `SameNetwork` (its
`reachable` input), because reaching it still takes a password, but the small server that hands out that
password stays reachable only by the project.

It works during provisioning after the apply, and in every later phase once the
infrastructure is running. If the endpoint is not declared, or the
infrastructure is down, the error says which and points at `weft infra status`.

If your unit serves something with a fixed limit (a database that takes only
so many connections), the waiting belongs in your unit, because it is the one
place that sees every client, and the number of clients keeps changing
(workers scale, and a frontend connects too). A PostgresDatabase runs a pooler
in front of Postgres for this, in the same unit: the pooler never opens more
connections than Postgres takes, a client that finds them all busy waits for
one, and Postgres closes a session that sat idle for a minute, so a connection
somebody holds without using it comes back. For what a node that talks to
such a unit should do on its own side, go and read
[sharing something between runs](ctx.md#sharing-something-between-runs).

When a program marks your node `@per_instance`, each instance of the program
gets its own container, and `ctx.endpoint` answers with the copy of the
instance the run is for; your node's code does not change. Each copy has its
own address, so its `public_url()` is its own too. A connection your node
publishes (`ctx.publish_access`) from an instance's copy is recorded as that
instance's. For what an instance is, go and read
[programs with instances](../running/instances.md).

## Outputs saved with the infrastructure: baking

If an output only says where your infrastructure is or how to connect to it
(an address, a database connection), it stays the same from one run to the
next, so weft can save it when the infrastructure is applied and stop running
your node just to hand it out. Mark it `baked` in `metadata.json`:

```json
{ "name": "access", "type": "Access[postgres]", "baked": true }
```

(Baked outputs have nothing to do with `weft bake`, which prepares a
trigger's settings.)

Only an infrastructure node can bake an output: on any other node, the
catalog refuses to load the metadata and names the output.

Every time the infrastructure is applied (`weft infra start`, `weft infra
upgrade`, `weft infra rebake <node>`, or a program's own
`ctx.infra(..).start(&mut state)`), weft runs your node's `run` right after and saves what it sent on its baked outputs
alongside that infrastructure. A baked output your `run` sent nothing on keeps the value saved
last time.

If your `run` does more in a real run than the run after an apply needs (it
keeps a session open and streams what happens on a bus, say), check
`ctx.is_setup()`. It is true in the run right after an apply, and in a
trigger setup. Only the nodes the setup needs run in it, so nothing reads
your other outputs there: send your baked outputs and return. An output you
send nothing on closes, a `Bus` output included, and nothing fails.

While the infrastructure is running, if everything a run reads from your
node comes from baked outputs that have a saved value, your node does not
run: the saved values go down its wires instead.
Nodes that only fed your node do not run either. Your node's log says why:
`did not run: everything this run reads from it is baked, so its saved
<outputs> went out instead`. The Postgres node bakes `access`, so a route that
queries the database gets the saved connection without the Postgres node
running. If you want your node to run anyway (to debug it), name it in the
run: `weft run --target <node>` or `--from <node>` always runs it.

If a run also reads an output that is not baked (a bridge's `status`, which
changes while the bridge runs), your node runs as usual, and if its `run`
sends nothing on a baked output, the saved value goes out on it instead of
the output closing. Only wires into nodes that are part of this run count: a
`status` wired to a node outside the run does not make your node run.

That saved value goes out only once your body returns without an error. So if
your body stays up for the whole run, every node reading the baked output
waits the whole run. Send your baked outputs yourself before the long part
starts.

If your container changes a value itself (a password reset from a button,
say), have it POST the new value to the address in
`WEFT_VALUES_URL`:

```sh
curl -X POST "$WEFT_VALUES_URL" -H 'Content-Type: application/json' \
  -d '{"connection": {"password": "the-new-one"}}'
```

`connection` changes values inside the connection your node published, and
`outputs` replaces saved baked outputs by name (`{"outputs": {"<output>":
<value>}}`).

weft sets `WEFT_VALUES_URL` in every container of your unit, init containers
included, and refuses a spec that sets it. A push only ever changes the copy
whose container sent it. weft does not check a pushed value against the
output's type. A saved value that does not fit its output's type, pushed or
left over from older code, is never handed out: a run that reads it runs
your node instead, and if your `run` sends nothing on that output the run
fails saying `what this node's infra saved for its baked outputs no longer
fits them`.

The request answers `200` once weft has written the value. If you name a key
the connection does not store or an output with nothing saved, or send
`connection` from a node that published none (or several), it answers `422`
with a body that names the problem, and nothing is written. Any other answer
means nothing was written either, and `502` means weft could not be reached
at all.

If you want a button for this, give one of the items on your container's
panel an `action`: `{"label": "Reset password", "actionKind":
"reset_password", "confirm": "<what to ask before pressing>"}`. For serving a
panel, go and read [showing something in the graph](display.md#a-live-panel).
A press reaches your container as a `POST` to `/action` with `{"action":
"<actionKind>", "payload": null}`, or with the button's own `payload` if you
gave it one. For how to answer it, go and read [letting other nodes reach
it](#letting-other-nodes-reach-it).

If the push fails, or a value changed without anything telling weft, the
person running the program fixes it with `weft infra rebake <node>` (add
`--instance <id>` for a `@per_instance` node's copy). It applies your
infrastructure again, restarting nothing that already matches its spec, then
runs your node's `run` and saves what it sends. That only helps if your `run`
reads the current value from your infrastructure instead of trusting what it
published last time. The Postgres node does this: it asks its credential
container whether the password it holds is still the current one, and reads
the new one when it is not.

If your container pushes from a button and the push fails, answer the press
with `{"result": {"error": "..."}}` telling the person to run `weft infra
rebake <node>`, the way the Postgres node's Reset password button does. You
do not need to add that hint anywhere else: when any node fails after
opening the connection your node published, weft adds a line to that node's
error naming `weft infra rebake <node>`.

If you change what your `run` emits on a baked output, runs that skip your
node keep handing out the old saved value until it bakes again. If you want
the new value out, `weft infra upgrade` builds your current source and
restarts each copy it reaches that still runs an older build, which bakes it
again. The triggers that go through that copy are down until it is back. If you would rather
restart nothing, build first
(`weft build`, or any `weft run`) and then run `weft infra rebake <node>`,
which runs whatever weft built last.

Without a flag, the upgrade touches the program's shared infrastructure only.
If you want a `@per_instance` node's copies to bake your new code, add
`--instance <id>` for some instances or `--all-instances` for every one;
neither touches the shared copies. If you want both at once, use `--all`. For what an
upgrade does to running work and to triggers, go and read [infra commands
take down only what reads the
infra](../running/lifecycles.md#infra-commands-take-down-only-what-reads-the-infra).

## Letting other nodes reach it

If other nodes need to talk to your service (say, a WhatsApp bridge that
every send node goes through), pass them a handle instead of `url()`. The
handle names your node, as it is named in the program, and its endpoint, so
it still finds the service if its address changes (a terminated infra
started again gets new ports, for one).

Declare an output typed `Infra` and set it to the endpoint's handle:

```json
{ "name": "bridge", "type": "Infra" }
```

```rust
let api = ctx.endpoint("api").await?;
ctx.pulse_downstream(NodeOutput::new().set("bridge", api.infra_handle())).await
```

A node that uses it declares an input typed `Infra` and resolves it:

```rust
let bridge: InfraHandle = ctx.inputs.get("bridge")?;
let bridge = ctx.endpoint_of(&bridge).await?;
let sent = bridge.action("sendMessage", json!({ "to": to, "text": text })).await?;
```

`ctx.endpoint_of` gives back the same handle `ctx.endpoint` does, so `url()`
and `call(..)` work on it as well. `action(name, payload)` posts
`{"action": name, "payload": payload}` to the service's `/action` and gives
you the `result` of its answer. Your service answers `{"result": ...}`. `action`
returns an error when your service answers non-2xx, when the answer has no
`result`, or when `result` carries an `error` string (the error then reads
`<action>: <error>`). An `error` that is not a string comes back as part of
the result. The buttons of [a live panel](#a-live-panel) post to the same
`/action`, so one handler in your container serves both.

An `Infra` input only takes a handle from an `Infra` output. The compiler
refuses a `String` wired into it, and it refuses a value typed into it by
hand. A handle only works inside its own project. It only reaches
infrastructure the program still declares. For a node marked `@per_instance`,
it reaches the copy that belongs to the run's instance.

## A long job in your container

If your service runs something long (a render, a training run), have it answer
`POST /jobs` with an id at once and a status route like `GET /jobs/<id>`, then
wait on that route the way a node waits on a provider's job: start it inside
a `state.once` and wait with `PollEndpoint` on `format!("{}/jobs/{id}", api.url())`.
For the whole pattern, go and read
[waiting on a job a service runs](durable-execution.md#waiting-on-a-job-a-service-runs).

`api.url()` is the address your workers reach, and the checks are made by the
listener, which on a local install sits on the machine rather than in Docker's
network. Before every check it asks for the same endpoint's address as weft's
own roles reach it, looked up in this project's infra only, so the address you
pass works as it is.

If somebody cancels the run while it waits, the job in your container carries
on. If you want it to stop too, give your container a cancel route and pass it
as the cancel call: `CancelCall::post(format!("{}/jobs/{id}/cancel",
api.url()))`. The listener reaches it the way it reaches the status route. For
what happens when your container refuses the call or does not answer, go and
read [if the run is cancelled while the job
runs](durable-execution.md#if-the-run-is-cancelled-while-the-job-runs).

## A live panel

If your container serves `/live`, name that endpoint and the editor shows the
panel on your node:

```json
"features": { "liveEndpoint": "api" }
```

That is how a bridge shows the QR code you have to scan. Go and read
[showing something in the graph](display.md).

## What the supervisor does

It takes an exclusive lease on the project, applies your spec, and watches
whether what it created is healthy. For what happens when a supervisor dies halfway through a change, go and
read [the supervisor](../running/architecture.md#the-supervisor).

There is no time limit on coming up, because a model server pulling weights can
take a long time. Somebody waiting on yours reads `weft infra logs <node>` and
cancels if it will never come.

## When a unit stops being ready

A unit that was ready and then stays not ready for its whole flaky window
(the unit's `health.flaky_after_seconds` in Rust, `flakyAfterSeconds` in the
spec's JSON; 30 seconds by default) is marked **flaky**. One that stays ready
again for `health.recovery_after_seconds` (`recoveryAfterSeconds`; also 30)
is marked running again. `weft infra status` shows which state each unit
is in.

What happens to the project while a unit is flaky is up to your node's
`health_rules`. If you write none, your node gets these two:

- **take-down-while-flaky**: while a unit of the copy is flaky and the
  project's triggers are on, take down every trigger whose cut contains this
  copy (the cut is the part of the program a trigger's runs go through). Work
  that arrives for those triggers is held until they are back, work they were
  already holding is kept, and the runs reaching the copy are cancelled, since
  they cannot go on without it. A trigger whose cut does not contain the copy
  keeps running.
- **bring-back-once-healthy**: once no unit of the copy is flaky, bring back
  what the health check took down, but never what a person or an infra
  command took down.

Each rule reads one copy at a time, and only that copy's own units, so one
instance's broken copy never takes down another instance's triggers.

If that is not what your node needs, write your own. Say your node runs a
database unit `db` and a small admin panel `panel` beside it. With the two
rules above, the panel going flaky would take down every trigger that uses
the database. Name the unit and only `db` counts:

```rust
use weft::infra::health::{HealthAction, HealthCondition, HealthRule, DEFAULT_RULE_TIMEOUT_SECONDS};

let db_flaky = HealthCondition::UnitFlaky { unit: Some("db".into()) };
let health_rules = vec![
    HealthRule {
        name: "take-down-while-db-flaky".into(),
        when: HealthCondition::All { conditions: vec![db_flaky.clone(), HealthCondition::Listening] },
        action: HealthAction::TakeDownTriggers,
        timeout_seconds: DEFAULT_RULE_TIMEOUT_SECONDS,
    },
    HealthRule {
        name: "bring-back-once-db-healthy".into(),
        when: HealthCondition::All {
            conditions: vec![HealthCondition::Not { condition: Box::new(db_flaky) }, HealthCondition::TakenDownByHealth],
        },
        action: HealthAction::BringBack,
        timeout_seconds: DEFAULT_RULE_TIMEOUT_SECONDS,
    },
];

Ok(InfraSpec { units, health_rules: Some(health_rules), ..Default::default() })
```

A condition is one of these, nested as deep as you like:

| Condition | Holds when |
|---|---|
| `UnitFlaky { unit }` | That unit of the copy is flaky (`unit: None`: any of its units). A unit not reported yet is not flaky |
| `Listening` | Some trigger of the project is on, and none is being turned on |
| `TakenDownByHealth` | Something the health check took down is still down. A person's stop or deactivate clears the mark, so a rule gated on this never undoes what a person did |
| `All { conditions }` | Every one of them holds (an empty list holds) |
| `Any { conditions }` | One of them holds (an empty list does not) |
| `Not { condition }` | It does not hold |

The action is `TakeDownTriggers` or `BringBack`. Give each rule a name no
other rule of this node uses: weft keeps each rule's state under its name,
and `weft status` names it if the rule gives up. A repeated name is refused
when the spec is applied.

Rules are read in the order you write them. A rule fires once for a copy when
its condition starts to hold, and can fire again only after the condition
stopped holding for that copy. When two hold at once for a copy, only the
first fires. The second fires on a later check, once the first has fired or
stopped holding; while the first is retrying or has given up, the second
waits.

Firing a rule means the supervisor asks weft to queue its action, and the
rule counts as fired while that action runs. The action fails if the request
to queue it fails, if weft cannot carry it out, or if it has not finished
within `timeout_seconds` of being queued. Each time weft finishes a health
action, the supervisor takes one more look, so a failure is noticed at once.
A failed action is tried again 5 seconds later, then 10 seconds after that. On
the third failure in a row weft gives up on the rule, and `weft status` names
it with the last error. A rule that gave up tries again when any unit of the
copy changes between ready, not ready and flaky, when the rule stops holding,
when somebody starts, stops, upgrades or terminates the project's infra, or
when somebody runs `weft infra look`.

Set `timeout_seconds` to `DEFAULT_RULE_TIMEOUT_SECONDS` (30 minutes) unless
you want another limit. It is a `NonZeroU32`, so for ten minutes write
`NonZeroU32::new(600).unwrap()`.

When a bring-back fires, a trigger stays down if its cut contains a running
copy that is still broken: one whose node has a bring-back rule that does not
hold right now, or one whose own take-down took down something that is still
down. A healthy copy whose node writes no rules never holds anything down.

If you want a flaky unit to take nothing down, set `health_rules:
Some(vec![])`. A node with take-down rules but no bring-back rule keeps down
what it took down itself until a person runs `weft activate`.

A rule that names a unit the spec does not declare is refused when the spec
is applied:

```text
infra node 'database': health rule 'take-down-while-db-flaky' reads unit 'dbb',
which is not declared; name one of the node's units, or leave `unit` out to
read any of them
```

For what the person running the project sees when the health check took a
trigger down, and how to bring one back by hand, go and read [infra commands
take down only what reads the
infra](../running/lifecycles.md#infra-commands-take-down-only-what-reads-the-infra).

## Two things a user has to do themselves

Nothing starts your infrastructure for them. Not a run, not a build, not
activation. `weft infra start` is a verb they type, because a container costs
money and weft will not spend it on a click that did not mention one.

And `weft infra stop` keeps the disk while `weft infra terminate` deletes it.
If your node has data worth keeping even through a terminate, name its disk in
`keep_on_terminate`. The next `weft infra start` of the same node (the same
instance's copy, for a node marked `@per_instance`) finds that disk and mounts it
again, data and all. If you want a kept disk gone, drop it from
`keep_on_terminate`, start the node once so the copy knows about it, and
terminate.

A kept disk is deleted for you once its copy can never come back: when you
remove the node from your program and run it again, when the node switches
between shared and `@per_instance` (which leaves the old side's copies behind), or
when you remove the project with `weft rm`. This holds even if the copy was already terminated
at the time. The supervisor sweeps for such copies on every ownership tick, so
the disk goes within one tick of the change.

If you wipe an instance (the `WipeInstance` node, or `ctx.infra(node).instance(id).wipe(..)`
from your own node), the kept disks of that instance's copies go right away,
including those of a copy that was already terminated.

## Testing one

```rust
let outcome = rig.run_provision_infra(&PostgresNode, json!({ "database": "app" })).await.ok()?;
let spec = outcome.infra_spec()?;
assert_eq!(spec.units[0].name, "db");
```

`fake` is the top tier here too. For a node that calls its own
infrastructure, declare where each endpoint answers, then what each call gets:

```rust
rig.declare_endpoint("credential", "http://localhost:8080");
rig.refuse_endpoint("credential", EndpointMethod::Get, "/password", 503, "starting");
rig.answer_endpoint("credential", EndpointMethod::Get, "/password", json!({ "password": "minted" }));
let outcome = rig.run(&DatabaseNode, json!({ "database": "app" })).await.ok()?;
assert_eq!(rig.endpoint_calls().len(), 2, "asked twice: refused, then answered");
```

| Call | What it does |
|---|---|
| `declare_endpoint(name, url)` | Says the endpoint `name` answers at `url`. Without it, `ctx.endpoint(name)` fails the way it does when the infrastructure is not running |
| `declare_public_url(name, url)` | Makes a declared endpoint public: `ctx.endpoint(name)?.public_url()` answers `url` |
| `answer_endpoint(endpoint, method, path, answer)` | The next call to `path` on that endpoint answers `answer` (JSON) |
| `refuse_endpoint(endpoint, method, path, status, body)` | The next call to `path` on that endpoint is refused with `status` and `body`, the way a service that is still starting refuses one |
| `declare_infra(place, endpoint, url)` | Use this when your node reads an `Infra` input. It pretends the infrastructure node `place` shares `endpoint` at `url`, and returns the handle to set on that input |
| `answer_infra(place, endpoint, method, path, answer)` | Queues the answer to the next call to `path` on that shared endpoint, one per call, the same way `answer_endpoint` does |
| `endpoint_calls()` | Every call the node made to an endpoint, its own or a shared one, in order: `place`, `endpoint`, `method`, `path`, `body` |
| `stop_after_calls(n)` | Presses stop, as `weft stop` would, once the node has made `n` calls (endpoint calls and web requests, counted together). The call that reaches `n` still gets its answer; `0` stops the run before it starts. This is how you test a node that loops on calls and must end cancelled when a person stops it |

`method` is `EndpointMethod::Get` or `EndpointMethod::Post`.

Each answer and each refusal is used by exactly one call, in the order you
declared them, so declare one per call you expect. That is how a node that
asks twice and acts on the answer changing (refused, then answered) gets
tested. A call with nothing left to answer it fails the run and names
`answer_endpoint`, so a question your node should not have needed to ask shows
up instead of being quietly answered.
