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

`NetworkAccess` holds ingress and egress rules. Its default allows workers
in and internet access out, on top of the project's baseline policies.
Use the rules to declare the connections your service needs.

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

## The routes your container serves

Your service chooses its own API. Two routes have a defined meaning for
the graph's live panel:

| Route | Method | Contract |
|---|---|---|
| `/live` | GET | Return an object containing an `items` array |
| `/action` | POST | Receive `{ "action": "<actionKind>", "payload": {...} }`; return `{ "result": {...} }` |

Set `features.liveEndpoint` to the name of the endpoint serving those routes:

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
[What your node shows in the graph](showing-things-in-the-graph.md#a-live-feed-from-an-infra-node).

For readiness, choose a route such as `/health` and declare it with
`Probe::http`. Have the service return HTTP 200 when ready to accept work.

## Lifecycle

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
weft infra node-stop <node_id> --force
weft infra start
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
