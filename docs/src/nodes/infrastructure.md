# Infrastructure nodes

Some capabilities need a long-running process the user cannot easily run
themselves: a WhatsApp bridge holding a phone session, a headless browser, a
local model server, a database.

Weft calls those **infra nodes**. The node returns a typed spec describing what
should run, the supervisor compiles it to Kubernetes manifests and applies
them, and the node talks to the running pods over HTTP at fire time.

You never write YAML. You build the spec with typed Rust structs, and the
compiler turns it into Deployments, Services, PVCs, NetworkPolicies, and
autoscalers, stamping every label it needs.

## Two methods

An infra node sets `requires_infra: true` in its metadata and implements two
bodies.

**`provision_infra(ctx, input) -> InfraSpec`** returns the desired shape. It
emits no pulses, it just describes what should run, and its context carries
`project_id`, `node_id`, `namespace` and `tenant_id`.

**`run(ctx)`** is the node's actual logic. By the time it executes, the
infrastructure is applied and it can resolve its endpoints.

## Return the same spec every time

`provision_infra` runs on `weft infra start` and `weft infra upgrade`, and on
nothing else. The supervisor compares what you returned against what is already
applied: identical, it does nothing; different, it changes the cluster.

So build the spec **only** from your inputs and your node's identity. No
clocks, no random values, no generated passwords.

If your service needs a password, the container generates it on first boot and
keeps it on its own volume, and `run` asks the container for it. A password in
the spec would be a different password on every start, while the container
keeps answering to the first one, and no restart would ever fix it.

```rust
async fn provision_infra(&self, _ctx: InfraProvisionContext, _input: ValueBag)
    -> WeftResult<InfraSpec>
{
    const PORT: u16 = 8090;
    Ok(InfraSpec {
        units: vec![Unit {
            name: "bridge".into(),
            on_upgrade: UpgradeBehavior::Recreate,
            containers: vec![
                Container::new("whatsapp", Image::Local { name: "bridge".into() })
                    .with_env(vec![EnvEntry::Literal {
                        name: "PORT".into(), value: PORT.to_string() }])
                    .with_ports(vec![ContainerPort {
                        name: "http".into(), port: PORT, protocol: Protocol::Tcp }])
                    .with_readiness(Probe::http("/health", PORT).with_initial_delay(5)),
            ],
            ..Default::default()
        }],
        endpoints: vec![Endpoint {
            name: "api".into(), unit: "bridge".into(), container: "whatsapp".into(),
            port: "http".into(), expose: Expose::ClusterInternal,
        }],
        ..Default::default()
    })
}
```

## The spec

`InfraSpec` has all-defaulted fields, so `InfraSpec::default()` is valid.

| Field | What it holds |
|---|---|
| `units` | pod templates. Most nodes have one. |
| `volumes` | PVCs, emptyDirs, mounted ConfigMaps and Secrets |
| `config` | Secrets and ConfigMaps to create, inline or by reference |
| `endpoints` | named ports exposed through Services |
| `access` | network policy: ingress and egress. Default is workers in, internet out. |
| `lifecycle` | terminate policy, including which PVCs to preserve |


### `Unit`

One pod template, and the **operational** unit: each has its own status and
its own stop behavior.

| Field | Meaning |
|---|---|
| `name` | required |
| `kind` | `Deployment` (default), `StatefulSet`, `DaemonSet`, `Job` |
| `containers`, `init_containers`, `pod_options` | the pod's contents |
| `scaling` | `replicas`, plus an optional autoscaler. Per unit. |
| `on_upgrade` | `Rolling{...}` (default) or `Recreate`. Honored for Deployments. |
| `on_stop` | `ScaleToZero` (default) or `NoOp`. See lifecycle below. |
| `health` | per-unit flaky and recovery windows. Unset means the supervisor's default of 30 seconds. |

### `Container`

No `Default`, because the image is mandatory. Build with
`Container::new(name, image)` and chain:

`.with_env` `.with_ports` `.with_resources` `.with_mounts` `.with_readiness`
`.with_liveness` `.with_startup` `.with_command` `.with_args`
`.with_security_context` `.with_pre_stop`

`pre_stop` is a Kubernetes preStop hook. **Weft calls no Rust callback at stop
time**; graceful shutdown lives entirely in the container.

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

**`Access`** is ingress rules (`FromWorkers` default, `FromNode`,
`FromInternet`, `FromCidrs`, `FromLabel`) plus egress (`ToInternet` default,
`ToNode`, `ToCidrs`), compiled to one NetworkPolicy on top of the namespace
baseline.

A bad spec fails the apply loudly and the node shows `Failed` with the
reason.

## Talking to your infrastructure

```rust
let api = ctx.endpoint("api").await?;
let out = api.call(EndpointMethod::Get, "/outputs", None).await?;
let url = api.url();                        // the bare service URL
let (host, port) = api.host_and_port()?;    // for a client that stores them apart
```

**`ctx.endpoint` does not return until something answers on that address.**

An address exists as soon as the infrastructure is accepted, which is before
your container has finished starting. So this waits out the gap and your first
call never lands on a refused connection.

There is no time limit, because a first boot takes as long as it takes. A wait that is taking a while
says so in the node's log every few seconds, and `weft stop` ends the run.

The endpoint resolves only when the **whole node** is running, meaning all its
units. An endpoint is a front door to the node, so a request must not land
while a sibling unit is degraded.

### Only the declaring node

`ctx.endpoint(name)` works only for the node that **declared** the endpoint. A
sibling node gets the URL by the declaring node exporting it as an output port
and wiring it downstream:

```rust
// the bridge node, in run:
let api = ctx.endpoint("api").await?;
ctx.pulse_downstream(NodeOutput::new().set("apiUrl", api.url())).await

// the send node, in run:
let base: String = ctx.inputs.get("apiUrl")?;
let resp = post(format!("{}/action", base.trim_end_matches('/')), body).await?;
```

The author chooses what to send downstream, and with several endpoints exports
each by name.

## The routes your container serves

| Route | Method | Called by | Contract |
|---|---|---|---|
| `/health`, or any path | GET | the readiness probe | return 2xx when ready. Wire it with `Probe::http("/health", port)`. |
| `/live` | GET | the dispatcher, which proxies the editor's poll | return `{ "items": [{ "type": ..., "label": "...", "data": "..." }] }`, where the type is `text`, `image`, `progress` or `secret`. The editor asks every three seconds while the graph is open. An item may carry a button: `"action": { "label": "Disconnect phone", "actionKind": "unpair", "confirm": "..." }`, and `payload` if the press carries data. |
| `/action` | POST | the dispatcher, when a `/live` button is pressed | the same envelope sibling nodes use: `{ "action": "<actionKind>", "payload": {...} }` in, `{ "result": {...} }` out. A `result.error` string is your refusal, shown to the user as it is. The dispatcher re-polls `/live` right after, so whatever the press changed (a fresh QR code) shows at once. |
| `/outputs` | GET | the declaring node's own `run` | return a flat JSON object; the node folds each key into an output port |
| `/action`, `/events`, ... | any | sibling nodes, through the wired URL | your own convention |

`/live` and its buttons' `/action` are special because the **dispatcher**
calls them, so it has to know which endpoint serves them:

```json
"features": { "liveEndpoint": "api" }
```

Naming the endpoint **is** opting in. There is no separate flag, and unset
means no live panel.

## Lifecycle

Everything below acts on one unit at a time.

**`weft infra start`** brings down units up to spec, leaving units already up
alone.

**`weft infra stop`** takes units down per their `on_stop`:

- `ScaleToZero` (default): scale the workloads to 0. PVCs and Services are
  kept, so the endpoint URL stays stable and a later start is fast.
- `NoOp`: the unit **stays up**. For a unit expensive or slow to recreate (a
  model that took an hour to download, a license server with live sessions)
  that downstream work depends on. Only terminate, or an explicit force-stop,
  takes a NoOp unit down.

**`weft infra upgrade`** is stop then start. ScaleToZero units cycle onto the
new spec; NoOp units stayed up through the stop, so start leaves them frozen at
their current version. If you want to update a frozen one, force it:

```bash
weft infra node-stop <node_id> --force   # ignores on_stop
weft infra start                          # recreate at the new spec
```

`--force` is the conscious "I accept the downtime", which is why the graph's
per-node right-click stop uses it automatically.

**`weft infra terminate`** deletes everything for the node, PVCs included
unless listed in `lifecycle.on_terminate.preserve_pvcs`. Deleting a node from
the graph terminates it on the next sync; removing a single unit from a spec
terminates that unit's workloads on the next apply.

## Health

The supervisor watches each unit's replicas. A unit continuously below its
readiness threshold for `flaky_after_seconds` is marked flaky; continuously
ready for `recovery_after_seconds` returns it to running. Both default to 30
seconds and are overridable per unit through `Unit.health`.

Health is per unit, so one flaky sidecar does not drag a healthy primary down,
and a project's health protocols can target a specific unit for remediation:
bounce pods, scale, park triggers.

## Security and resources are yours

The compiler stamps labels and namespaces and adds **no security context or
resource limits on your behalf**. Isolation between namespaces comes from the
namespace boundary and the baseline network policies; what happens inside your
own namespace is yours to set.

Set resource requests and limits, and a security context that satisfies the
Kubernetes restricted baseline: run as non-root, read-only root filesystem,
drop capabilities, default seccomp. Your image has to cooperate by running as
the chosen user and tolerating a read-only filesystem, and if it cannot, leave
the security context off. Without limits a runaway container can starve its own
node.

## Every state has a way out from the graph

A service reaches states only its operator can leave: a phone whose pairing
died half way, a password that was handed over once and lost, a session a
provider revoked. If the only way out is `weft infra terminate`, the user
loses the disk to fix a login, and that is a dead end the node put them in.

So for every state your container can sit in, name the action that leaves
it, and put that action on the node's card as a button on a `/live` item
(the contract is in the routes table above). The WhatsApp bridge offers
**Disconnect phone** in every state, which drops the pairing and shows a
fresh QR code; the Postgres node offers **Reset password**, which mints a
new one over the database's own socket and makes it readable again. A
button works whether the service is healthy, stuck, or half way through
something, because the stuck state is the one that needs it. The test:
walk your container's states and ask, for each, what a user does from the
graph to leave it. If the answer is "restart the infra" or "delete the
disk", that state needs a button.

## Nothing fails quietly

Your container is a service other nodes lean on, and its pod log is the only
place anyone can read what it did. Two rules, and they are what makes a
problem inside your image findable at all.

**Every failure writes a line.** Anything that goes wrong writes one line to
the container's stdout or stderr, with the cause, so `weft infra logs <node>`
shows it. A library logger set to silent is the same as no log: set it to
warn or up. A dependency that is optional at install time is a failure
waiting to be silent (a step that quietly skips when the package is
missing), so pin it in the image.

**A success is earned or it is an error.** Answer the node that asked with an
error whenever the thing it asked for did not fully happen, and let the node
fail on it. A message id for a message the recipient will never see, a
partial result with no mention of what is missing, a step that could not run
and was skipped: each of those is an error to the caller, however the
underlying library reports it. When the library reports it only through its
own logger, check the outcome yourself before answering.

The WhatsApp bridge is the example that fixed the rule: a voice note went out
stamped with a mime WhatsApp accepts and never shows, the bridge's Baileys
logger was silent, and the node got a message id for a message that never
arrived. Nothing anywhere said so.

## Two behaviors to know

**Mutable upstream tags do not trigger drift.** `Image::Upstream` with a tag
like `:latest` passes through verbatim and is never resolved to a digest. If
the tag rolls underneath you, the spec hash does not change, so nothing
surfaces an available upgrade. `weft infra upgrade` still re-applies if you
know there is a new version. Use a digest for reproducibility.

**`/outputs` against your declared output ports is a convention**, not
enforced. Keep them in sync by hand.

## Packaging

```
nodes/whatsapp/
  package.toml
  bridge/
    mod.rs
    metadata.json         requires_infra, images, features, ports
    deps.toml
    images/bridge/        a Dockerfile and its source
```

```json
{
  "type": "WhatsAppBridge",
  "requires_infra": true,
  "images": ["images/bridge"],
  "outputs": [{ "name": "apiUrl", "type": "String" }],
  "features": { "liveEndpoint": "api" }
}
```

Each entry in `images` is a directory relative to the package root containing
a `Dockerfile`. Its basename is the `Image::Local { name }`. The CLI hashes the
directory, tags the image, and loads it into the local cluster.
