# Infrastructure nodes

If a node needs its own running service, such as a database or a WhatsApp
bridge, declare that service as infrastructure. weft can start it and
track its health alongside the program.

The node returns an `InfraSpec` built from Rust types. weft compiles the
spec into Kubernetes resources, and the supervisor applies them. Your
node then uses the declared endpoints to talk to the service.

## Two methods

Set `requires_infra: true` in the node's metadata and implement:

| Method | Job |
|---|---|
| `provision_infra(ctx, input)` | Return the desired `InfraSpec` |
| `run(ctx)` | Use the service and emit the node's outputs |

Provisioning describes what to run. It does not emit values. Its context
supplies the node and project identity, tenant, and namespace.

For a complete implementation, read the
[Postgres database node](https://github.com/WeavemindAI/weft/tree/mvp/catalog/postgres/database).
It provisions a database, obtains its credentials, and publishes a
connection other nodes can use.

## Return a stable spec

When you start or upgrade infrastructure, weft compares the spec with the
applied state. Build it from the declared inputs and node identity so that
an unchanged configuration produces an unchanged spec.

Generating a fresh password inside `provision_infra`, for example,
changes the desired state on every invocation. If the database keeps the
original password on its volume, the new value will not match it.
The Postgres node instead lets the service generate and retain the password,
then retrieves it in `run`.

This fragment describes a bridge image exposing an HTTP endpoint.
Place the method in your `impl Node` block and the imports at module scope:

```rust
use weft::{
    Container, ContainerPort, Endpoint, EnvEntry, Expose, Image,
    InfraProvisionContext, InfraSpec, Probe, Protocol, Unit,
    UpgradeBehavior, ValueBag, WeftResult,
};

async fn provision_infra(
    &self,
    _ctx: InfraProvisionContext,
    _input: ValueBag,
) -> WeftResult<InfraSpec> {
    const PORT: u16 = 8090;
    Ok(InfraSpec {
        units: vec![Unit {
            name: "bridge".into(),
            on_upgrade: UpgradeBehavior::Recreate,
            containers: vec![
                Container::new("bridge", Image::Local { name: "bridge".into() })
                    .with_env(vec![EnvEntry::Literal {
                        name: "PORT".into(),
                        value: PORT.to_string(),
                    }])
                    .with_ports(vec![ContainerPort {
                        name: "http".into(),
                        port: PORT,
                        protocol: Protocol::Tcp,
                    }])
                    .with_readiness(Probe::http("/health", PORT)),
            ],
            ..Default::default()
        }],
        endpoints: vec![Endpoint {
            name: "api".into(),
            unit: "bridge".into(),
            container: "bridge".into(),
            port: "http".into(),
            expose: Expose::ClusterInternal,
        }],
        ..Default::default()
    })
}
```

Your image must listen on the supplied port and serve `/health`.
For how to include that image, see [Packaging](#packaging) below.

## The spec

| `InfraSpec` field | What it describes |
|---|---|
| `units` | Workloads and their containers |
| `volumes` | Persistent or temporary storage and mounted configuration |
| `config` | Secrets and ConfigMaps to create or reference |
| `endpoints` | Named ports exposed through Services |
| `access` | Ingress and egress rules |
| `lifecycle` | Termination behavior, including volumes to preserve |

All fields have defaults. An empty spec describes no workloads.

### Units and containers

A `Unit` has a name, containers, and a workload kind, which defaults to
`Deployment`. Use `Deployment` or `StatefulSet` for weft's readiness checks
and replica health monitoring. The compiler also accepts `DaemonSet` and
`Job`, but the supervisor does not include them in those checks or ordinary
stop operations.

The unit's scaling policy sets replicas and optional autoscaling where
supported. Stop behavior and health windows also belong to the unit.

Construct a container with `Container::new(name, image)`. Builder methods
add environment variables, ports, mounts, resource settings, and probes.
Use `with_security_context` for its security settings and `with_pre_stop`
for a Kubernetes shutdown hook. weft does not call a Rust node method at
container shutdown.

For all fields and builder methods, read the
[infrastructure types](https://github.com/WeavemindAI/weft/blob/mvp/crates/weft-core/src/infra/types.rs).

### Volumes and network access

A `Volume` has a `name` and a `VolumeKind`. A
`VolumeKind::Persistent` creates persistent storage; the other kinds are
`EmptyDir`, `ConfigMap`, and `Secret`.
Persistent volumes survive ordinary stop and upgrade. Termination removes
them unless the lifecycle policy names them for preservation.

### The rest

**`Image`** is `Image::Local { name }`, built from a directory listed in the
node's `images` and hash-tagged by the CLI, or `Image::Upstream { reference }`
such as `"postgres:18"`.

**`Endpoint`** is `name`, `unit`, `container`, `port` (the named container
port), and `expose`: `ClusterInternal` (default), `TenantPublic{path}`, or
`NodePort{port}`. The unit-container-port chain is validated at compile time.

**`Volume`** is `Persistent { size, storage_class?, access_modes }`, preserved
across stop and upgrade and deleted on terminate unless listed in
`preserve_pvcs`, or `EmptyDir`, `ConfigMap`, `Secret`.

**`Access`** is ingress rules (`FromWorkers` default, `FromInternet`,
`FromCidrs`, `FromLabel`) plus egress (`ToInternet` default, `ToCidrs`),
compiled to one NetworkPolicy on top of the namespace baseline.

A bad spec fails the apply loudly and the node shows `Failed` with the
reason.

## Talking to your infrastructure

Inside the declaring node's `run` method:

```rust
use weft::EndpointMethod;

let api = ctx.endpoint("api").await?;
let result = api.call(EndpointMethod::Get, "/outputs", None).await?;
```

`/outputs` here is a route your service implements. It must return JSON,
which `api.call` parses into `result`. Emit any values you want to put on
the node's output ports; the response is not mapped to them automatically.

`ctx.endpoint` requires the node's aggregate infrastructure status to be
running. Otherwise, it returns an error directing you to `weft infra status`
and the endpoint declarations. For TCP endpoints, it then waits until a TCP
connection succeeds. UDP endpoints skip that connection check.

This closes the startup gap between applying resources and reaching their
address. It does not guarantee that the service will still be available
when your next request arrives, or that an application request will succeed.
Handle errors from the request itself.

The reachability wait has no deadline and can be cancelled by stopping
the execution. Its waiting messages currently reach the node's log only
after the wait ends. To investigate a wait still in progress, use
`weft infra status` and `weft infra logs <node_id>`.

### Only the declaring node

Only the node that declares an endpoint can resolve it through `ctx`.
If another node needs the address, export it and wire it downstream:

```rust
let api = ctx.endpoint("api").await?;
ctx.pulse_downstream(NodeOutput::new().set("apiUrl", api.url())).await?;
```

This fragment assumes an `apiUrl: String` output. A receiving node reads
its wired URL and uses the appropriate client. For clients that take host
and port separately, the endpoint handle also provides `host_and_port()?`.

## Letting a client outside the cluster in

Everything above is for a node talking to its own infrastructure, and by
default that is the only thing that can reach it: an endpoint is a ClusterIP,
and the network policy lets workers in and nobody else.

Sometimes that is not enough. A frontend needs the program's Postgres for its
own sign-in tables. You want a `psql` session to see what a run wrote. A
dashboard wants to read the same database the program writes. All of those are
one thing: a client that is not a weft node, speaking the service's own
protocol.

If your endpoint can serve such a client, say so:

```rust
Endpoint {
    name: "sql".into(),
    unit: "db".into(),
    container: "postgres".into(),
    port: "sql".into(),
    expose: Expose::SameNetwork,
}
```

That is the whole of it. The endpoint says it is reachable and it is, the same
way a volume you declare is a volume you get. Nothing opens a door after the
fact, and nothing in the project's source can open one your node did not
declare, so reading the node tells you what is reachable.

"The same network" means the machine on a local install, where the door binds
to loopback so nothing else on your network can reach it, and the cluster's
own subnet in a deployed one. It never means the internet. No `Expose` reaches
the internet except `TenantPublic`, which is HTTP and goes through the ingress.

### Let the author decide, with an input

A door is rarely something every user of your node wants, so make it theirs to
choose. `provision_infra` runs with your node's inputs already computed, so
you branch on one like any other decision:

```rust
let reachable: bool = input.get("reachable")?;
...
expose: if reachable { Expose::SameNetwork } else { Expose::ClusterInternal },
```

That is how `PostgresDatabase` does it, with a `reachable` input that is off
by default. The lever is an ordinary port with a label and a description, it
shows up in the graph next to the disk size, and whether a database is
reachable reads as part of what the program is.

### Finding the address

The port is not yours and not the author's: the cluster allocates it, so that
two projects can each have a door without colliding. So the address is not in
the source, and asking the runtime is how anyone learns it:

```
$ weft infra list-doors
db.sql  127.0.0.1:30080
```

That command only ever reports. There is nothing to open or close, because the
node already said.

### The rule: never a credential endpoint

If you write only one thing down from this page, write this one.

**An endpoint that hands out a credential is never `SameNetwork`.** Not when
it would be convenient, not when it is guarded by a token, not when it only
answers once.

`PostgresDatabase` is the worked example, because it has one of each. Postgres
itself listens on `sql`, and reaching it still costs you a password, so that
endpoint is `SameNetwork`. Beside it runs a small server that mints that
password, on `credential`, and that one stays `ClusterInternal` for ever. A
door onto it would hand the database away to anything that could reach the
port.

The test on your own node: if reaching this port gives somebody something they
could not already have, it is not `SameNetwork`.

### Which connection the door hands out

A door is half of letting a client in. The other half is what that client
connects AS.

The simple answer, and the one you get for free, is the connection your node
already publishes: the same one the program's own nodes use. That is what a
door carries unless you do something about it, and for plenty of services it
is the only answer there is.

It is worth doing something about it when your service can express a narrower
identity AND the wider one can destroy things the program depends on. A
database is the clear case: the connection your node publishes owns the
tables, and the client coming through the door is usually somebody's frontend,
written fast, which should not be one typo from dropping them. Where the
service supports it, a node can mint a second identity for the door (a
database role with its own schema, a broker user scoped to its own topics) and
publish that instead.

Two things to hold on to. This is a capability, not a rule: a service with no
notion of a second identity has nothing to mint, and a node that publishes one
connection is not wrong. And like the door itself, it is a choice you give the
author rather than one you make for them: a second input, branched on in the
same body, so a program that wants the full connection through the door says
so and gets it.

The rule that IS absolute is the one above: an endpoint that hands out a
credential is never `SameNetwork`. That one holds whatever anybody intends.

## The routes your container serves

| Route | Method | Called by | Contract |
|---|---|---|---|
| `/health`, or any path | GET | the readiness probe | return 2xx when ready. Wire it with `Probe::http("/health", port)`. |
| `/live` | GET | the dispatcher, which proxies the editor's poll and any client that holds a token for this node's display | return `{ "items": [{ "type": ..., "label": "...", "data": "..." }] }`, where the type is `text`, `image`, `progress` or `secret`. This is the node's **display**, the same shape a trigger's kind serves, and [what your node shows in the graph](showing-things-in-the-graph.md#its-display) has the whole of it. The editor asks every three seconds while the graph is open. An item may carry a button: `"action": { "label": "Disconnect phone", "actionKind": "unpair", "confirm": "..." }`, and `payload` if the press carries data. |
| `/action` | POST | the dispatcher, when a `/live` button is pressed | the same envelope sibling nodes use: `{ "action": "<actionKind>", "payload": {...} }` in, `{ "result": {...} }` out. A `result.error` string is your refusal, shown to the user as it is. The editor polls `/live` again right after, so whatever the press changed (a fresh QR code) shows at once. |
| `/outputs` | GET | the declaring node's own `run` | return a flat JSON object; the node folds each key into an output port |
| `/action`, `/events`, ... | any | sibling nodes, through the wired URL | your own convention |

`/live` and its buttons' `/action` are special because the **dispatcher**
calls them, so it has to know which endpoint serves them. An answer that is
not that envelope comes back to the reader as an error naming what you sent,
rather than as an empty panel:

```json
"features": { "liveEndpoint": "api" }
```

An action result containing an `error` string reports the refusal to the
user. The editor requests the live panel again after a successful action.
Add an `action` object to a live item to give it a button:

```json
"action": { "label": "Pair again", "actionKind": "pair", "payload": {} }
```

Clicking it sends the action kind and payload to `/action`.
For item types, read
[What your node shows in the graph](showing-things-in-the-graph.md#its-display).

For readiness, choose a route such as `/health` and declare it with
`Probe::http`. Have the service return HTTP 200 when ready to accept work.

## Lifecycle

Everything below acts on one unit at a time, and every unit belongs to one
INSTANCE: the node at one place in the program. A node written in
`src/main.weft` is one instance. A node inside a file the program includes
runs once per include, so `one = @include("store.weft")` and
`two = @include("store.weft")` provision two databases, `one.db` and
`two.db`, each with its own disks, its own endpoint and its own line in
`weft infra status`. That spelling is how you name an instance to every verb
below.

| Command | Effect |
|---|---|
| `weft infra start` | Bring infrastructure up, preserving units already running |
| `weft infra stop` | Apply each unit's stop behavior |
| `weft infra upgrade` | Stop and start using the current spec |
| `weft infra terminate` | Remove infrastructure, including unpreserved persistent volumes |

The default `on_stop` is `ScaleToZero`. For Deployments and StatefulSets,
it sets replicas to zero while keeping Services and persistent volumes
for a later start.

With `NoOp`, ordinary stop leaves the unit running. An upgrade therefore
leaves that unit at its existing version too. To update it, first
deactivate any triggers that depend on it, then force-stop the node:

```bash
weft infra node-stop <node> --force   # ignores on_stop
weft infra start                       # recreate at the new spec
```

`--force` overrides `NoOp`; it does not bypass the check for active
dependent triggers. After an upgrade, the project remains deactivated.
Activate it again when the infrastructure is ready.

Termination matches `lifecycle.on_terminate.preserve_pvcs` against full
Kubernetes PVC names, formatted as `<instance_id>-<volume_name>`.
A local volume name such as `store` will not preserve the volume.
The provisioning context currently does not expose that generated instance
ID, which limits how a node can declare preservation in advance.

Removing a node from the graph terminates its infrastructure on the next sync.

## Health and recovery

Health is tracked per unit. After the supervisor has observed a unit ready,
it marks the unit flaky if it stays below its desired replica count for
`flaky_after_seconds`. It returns to running
after remaining ready for `recovery_after_seconds`. Both default to
30 seconds and can be set through `Unit.health`.

Containers in the same unit share that unit's health. A failing sidecar
can therefore affect its primary container's unit status.

If a service can get stuck in a state the user can repair, expose that
repair through a live-panel action. A lost pairing should have a way to
pair again without deleting the service's disk. The dispatcher allows
these recovery actions even when the node is not marked running, but the
HTTP service handling the action must still be reachable.

Write failures to stdout or stderr with their cause so they appear in
`weft infra logs <node_id>`. If an API operation only partly succeeded,
return that outcome to the calling node instead of reporting complete
success.

## Security and resources

Set the container's resource requests and limits and choose security
settings its image can run with. The infra compiler does not add a
container security context or resource limits for you.

These are services you choose to run, including their images and code.
For the installation's trust boundaries, read the
[security policy](https://github.com/WeavemindAI/weft/blob/mvp/SECURITY.md).

## Packaging

Local image directories are relative to the directory containing the node's
`metadata.json`. For example:

```text
nodes/my_bridge/
  package.toml
  service/
    metadata.json
    mod.rs
    images/
      bridge/
        Dockerfile
```

The member's metadata includes:

```json
{
  "type": "ExampleBridge",
  "label": "Example bridge",
  "description": "Run the project's bridge service.",
  "requires_infra": true,
  "images": ["images/bridge"],
  "outputs": [{ "name": "apiUrl", "type": "String" }],
  "features": { "liveEndpoint": "api" }
}
```

The directory basename, `bridge`, is the name used by
`Image::Local { name: "bridge".into() }`. The CLI hashes the directory
and builds the image for the cluster.

For an upstream image, use `Image::Upstream { reference }`. A mutable
tag changing at the registry does not change the declared spec's hash.
Use an image digest when the declaration must identify one exact image.
