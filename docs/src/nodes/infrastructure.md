# Infrastructure nodes

Some nodes need something running: a database, a model server, a bridge that
holds a session open. Your node returns a description of it, and the supervisor
makes what runs match it: Docker containers on your machine, or a Compute
Engine machine of its own on a cloud install.

```json
"requires_infra": true,
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

Declaring `requires_infra: true` without writing `provision_infra` is an error
naming your node, rather than a silent nothing.

## The spec

| Field | What it holds |
|---|---|
| `units` | What runs. Most nodes have exactly one |
| `volumes` | Disks, kept across stop and upgrade by name, and scratch space emptied at each start |
| `endpoints` | Named ports your node and the project's other nodes reach it on, and whether anything outside the project may |
| `keepOnTerminate` | Disks to keep even through a terminate, for the next `weft infra start` to pick up |

A unit is a group of containers that run side by side, with optional init
containers that run one after another before them. It also says what machine
it needs (`machine`: `cpu`, `memory`, and `gpu` with a `kind` and a `count`)
and what stop does to it (`onStop`: stop it, or keep it running until
terminate). On a cloud install, weft picks a machine that fits those numbers.
On your machine it only checks them, and refuses a unit that asks for a GPU
you do not have. On your machine weft cannot pick a GPU by kind, so a unit that
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
  database.
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
node shows the split: the port Postgres answers on is `SameNetwork`, because
reaching it still takes a password, but the small server that hands out that
password stays reachable only by the project.

It works during provisioning after the apply, and in every later phase once the
infrastructure is running. If the endpoint is not declared, or the
infrastructure is down, the error says which and points at `weft infra status`.

When a program marks your node `@per_instance`, each instance of the program
gets its own container, and `ctx.endpoint` answers with the copy of the
instance the run is for; your node's code does not change. Each copy has its
own address, so its `public_url()` is its own too. A connection your node
publishes (`ctx.publish_access`) from an instance's copy is recorded as that
instance's. For what an instance is, go and read
[programs with instances](../running/instances.md).

## Letting other nodes reach it

If other nodes need to talk to your service (say, a WhatsApp bridge that
every send node goes through), pass them a handle instead of `url()`. The
address changes every time your service is set up again. The handle names
your node, as it is named in the program, and its endpoint, so it still
finds the service after that.

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
you the `result` of its answer. Your service answers `{"result": ...}`. If
`result` carries an `error`, the node fails with that message, the same as
on a non-2xx. If `result` is missing, the node fails too. The buttons
of [a live panel](#a-live-panel) post to the same `/action`, so one handler in
your container serves both.

An `Infra` input only takes a handle from an `Infra` output. The compiler
refuses a `String` wired into it, and it refuses a value typed into it by
hand. A handle only works inside its own project. It only reaches
infrastructure the program still declares. For a node marked `@per_instance`,
it reaches the copy that belongs to the run's instance.

## A long job in your container

If your service runs something long (a render, a training run), have it answer
`POST /jobs` with an id at once and a status route like `GET /jobs/<id>`, then
wait on that route the way a node waits on a provider's job: start it inside
`ctx.run` and park with `PollEndpoint` on `format!("{}/jobs/{id}", api.url())`.
For the whole pattern, go and read
[waiting on a job a service runs](durable-execution.md#waiting-on-a-job-a-service-runs).

`api.url()` is the address your workers reach, and the checks are made by the
listener, which on a local install sits on the machine rather than in Docker's
network. Before every check it asks for the same endpoint's address as weft's
own roles reach it, looked up in this project's infra only, so the address you
pass works as it is.

If somebody stops the run while it waits, none of your code runs: the run ends
cancelled and the checks stop, but the job in your container carries on. If it
should stop too, have the container give up on a job whose status nobody has
asked for in a few intervals; terminating the infra stops it as well.

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

A unit that was healthy and stays not ready past its flaky window
(`health.flakyAfterSeconds`, 30 seconds by default) is marked flaky, and one
that stays ready again past `health.recoveryAfterSeconds` (also 30) is marked
running again. `weft infra status` shows which.

There is no time limit on coming up, because a model server pulling weights can
take a long time. Somebody waiting on yours reads `weft infra logs <node>` and
cancels if it will never come.

## Two things a user has to do themselves

Nothing starts your infrastructure for them. Not a run, not a build, not
activation. `weft infra start` is a verb they type, because a container costs
money and weft will not spend it on a click that did not mention one.

And `weft infra stop` keeps the disk while `weft infra terminate` deletes it.
If your node has data worth keeping even through a terminate, name its disk in
`keepOnTerminate`. The next `weft infra start` of the same node (the same
instance's copy, for a node marked `@per_instance`) finds that disk and mounts it
again, data and all. If you want a kept disk gone, drop it from
`keepOnTerminate`, start the node once so the copy knows about it, and
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
