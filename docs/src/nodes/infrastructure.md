# Infrastructure nodes

Some nodes need something running: a database, a model server, a bridge that
holds a session open. Your node returns a description of it, and the supervisor
makes the cluster match.

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
| `units` | Pod templates. Most nodes have exactly one |
| `volumes` | Disks, kept across upgrades by name |
| `config` | Secrets and configmaps, inline or by name |
| `endpoints` | Named ports, which is how your node and other nodes reach it |
| `access` | Network rules on top of the project's default-deny |
| `lifecycle` | What stop, upgrade and terminate mean for this node |

Each unit has containers, init containers, pod options, and its own replica
count. The count is on the unit rather than the node, because one node can want
a primary at one replica beside three of something else.

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
moment Kubernetes says the pod is ready.

| Call | Gives you |
|---|---|
| `url()` | The address, cached, no round trip |
| `host_and_port()` | The two apart, for a client that wants them that way |
| `call(method, path, body)` | An HTTP request against it, retried once through a routing gap |

It works during provisioning after the apply, and in every later phase once the
infrastructure is running. If the endpoint is not declared, or the
infrastructure is down, the error says which and points at `weft infra status`.

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
whether what it created is healthy. If it dies between changing the cluster and
recording that it did, the next one works out what to do from what the cluster
looks like rather than trusting the record.

A unit that was healthy and drops below its readiness threshold is marked
flaky, and the supervisor restarts it with a backoff.

There is no time limit on coming up, because a model server pulling weights can
take a long time. Somebody waiting on yours reads `weft infra logs <node>` and
cancels if it will never come.

## Two things a user has to do themselves

Nothing starts your infrastructure for them. Not a run, not a build, not
activation. `weft infra start` is a verb they type, because a container costs
money and weft will not spend it on a click that did not mention one.

And `weft infra stop` keeps the disk while `weft infra terminate` deletes it.
If your node has data worth keeping, say so in the `lifecycle` and the volume,
because that is what decides whether a terminate takes it.

## Testing one

```rust
let outcome = rig.run_provision_infra(&PostgresNode, json!({ "database": "app" })).await.ok()?;
let spec = outcome.infra_spec()?;
assert_eq!(spec.units[0].name, "db");
```

`fake` is the top tier here too, and the rig will answer or refuse an endpoint
so you can test both roads:

```rust
rig.declare_endpoint("sql", "http://localhost:5432");
rig.answer_endpoint("sql", "/health", json!({ "ok": true }));
```
