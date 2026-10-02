---
name: weft-instances
description: "Read whenever part of a program must run as several separate copies, each under its own id, even with ONE user: a session per chat, a sandbox per job, a workspace per document, a bot per customer. `@per_instance` (a container each), `@instance_filled` (a value each, a connection included), which instance a run is for, the route shape that works (shared routes, the instance picked per call), instance tokens, costs, starting and stopping one instance's containers, and giving every instance an end. When the copies belong to several PEOPLE, also read `weft-members`."
---

# Programs with instances

An [instance] is one separate running copy of part of a program, under an id
the program picks (`session-7f3a`, `user-42`). You reach for instances
whenever the program needs several isolated copies of something, whoever uses
them: one sandbox per job, one session per chat, one workspace per document,
one bot per customer. A single user with many sessions is as much a program
with instances as a site with thousands of people. When the copies belong to
people, the `weft-members` skill has how the program keeps who owns which.

weft keeps no list of instances: an id counts as an instance as long as weft
still holds something for it (a container, a value, a token, a run). You pick
one instance per unit of isolation the user needs: ask what must never leak
from one copy to the next (a chat's memory, a job's files, a customer's
account), and that is the instance.

You never copy a program once per instance: you mark what belongs to one
instance, in one of two ways.

## What you mark

`@per_instance` goes on its own line in the braces of an infra node, and gives
each instance its own container. Anywhere else it is refused as
`per-instance-ineligible`.

`@instance_filled` goes where a field's value would, and makes that value each
instance's own: its connection, its spreadsheet, its model, its schedule.
`@instance_filled(<value>)` names what an instance that was given none gets
(it wins over the node's default); bare `@instance_filled` leaves such an
instance's field empty. The fallback may be a file,
`@instance_filled(@file("prompts/default.md"))` (or `@asset(...)`), read the
way a written one is; no other marker goes inside. An empty field then takes
the node's default if it has one, stays empty if the field is optional, and
otherwise the run is refused, naming the field. A wired field cannot be
`@instance_filled` (`instance-filled-wired`), nor a group's, loop's or
included file's own port (`instance-filled-boundary`), nor a key that is not
an input (`instance-filled-not-an-input`). For a group, loop or include port,
mark the input of the node inside that reads it.

You never mark the steps downstream: every node reading an instance's
container or an instance's value follows it and runs in that instance's runs.
A trigger reading an instance's container, or a trigger with an
`@instance_filled` field, is itself per instance.

```weft
bridge = BaileyBridge {
  @per_instance
}
receive = BaileyReceive { bridge: bridge.bridge }

openrouter = OpenRouterProvider {
  connection: @instance_filled
  model: @instance_filled("deepseek/deepseek-v4-flash-0731")
}
digest = Cron { cron: @instance_filled("0 0 8 * * *") }
```

On a connection field, what you write decides whose key pays for an
instance's calls, so ask the user which they want before writing it:

- nothing written: every instance runs on the connection the user picked on
  the install;
- `connection: @instance_filled`: each instance connects its own and pays,
  and an instance with none is refused, naming the field. It takes no
  fallback: a connection id written in the source is refused.

If an instance should run on the user's key, leave the field unmarked: an
instance can never pick the user's connection, or the runtime's shared key,
for itself. The instance door refuses the shared key, and a value saved for an
instance's connection field has to be a connection that instance owns, whoever
saves it (`SetInstanceValues` included). If the user's connection is one made
with the runtime's shared key, the instances it serves spend that key.

An instance's value is held to the node's own rules when it is given (refused
on the spot, with the node's message), and again when a run for that instance
starts; a fallback is checked at compile time like any written value. A run
keeps the values it started with. When an instance's value changes and one of
its live triggers reads it (its own field, or anything its setup goes through,
like its connection), weft sets that trigger up again before the change
returns, so you never reactivate it yourself, and events arriving meanwhile
wait and go through on the new value.

## Routes stay shared

A `Route` or a `Socket` has one public address for every instance, so it can
never be per instance: one that reads a per-instance container or value, even
several steps upstream, is a compile error naming what made it per instance.
Keep every route shared and pick the instance per call:

- a route gated by a credential (`ApiKeyAuth` on its `auth`), called by the
  user's own backend with `Weft-Instance: <id>`; an open route refuses the
  header;
- or an instance token in `Weft-Instance-Token`, for a caller that acts
  inside one instance (a browser page, a script handed that token).

Groups count too. A group that receives a per-instance value on any of its
inputs makes every node reading any of its inputs per instance, and every
node inside it. So the refusal names one of three paths: "it reads 'bridge'",
"it sits inside group 'work', which receives 'bridge'", or "it reads from
group 'work', which receives 'bridge'".

So keep the routes, and anything else that must stay shared, OUTSIDE every
group that receives a per-instance value. Put the per-instance part (the
`@per_instance` node, the `@instance_filled` fields, and what reads them) in
its own group or file, send the route's work into that group through its
inputs, and bring the answer back out through its outputs. The `Reply` of a
request-response route can stay outside too, wired from the group's output.

If several routes each pick an instance, they all sit at the file's top
level, and each one hands what it read to its own `@include` of one file
that holds the per-instance part. A route inside that file would be per
instance itself, and the build refuses it.

```weft
ask = Route -> (body: JsonDict) { path: "ask", method: "POST" }
ask_work = @include("work.weft")
ask_work.request = ask.body
ask_reply = Reply { body: ask_work.answer }

sum = Route -> (body: JsonDict) { path: "summary", method: "POST" }
sum_work = @include("work.weft")
sum_work.request = sum.body
sum_reply = Reply { body: sum_work.answer }
```

`work.weft` is one `Group(request: JsonDict) -> (answer: JsonDict) { ... }`,
and an `@include` takes no braces, so its input goes on its own line.
It is compiled once, so the two includes share one body, and each answers
its own route. Each route still picks its instance one of the two ways
above.

When one per-instance node serves several routes, the shape that compiles
is the node and its routes side by side at the same level, in a group that
receives no per-instance value. Only the groups and includes doing the work
receive the node's output; the routes feed them and read nothing from them
(a `Reply` inside such a group answers its route). A program where each chat
session drives its own Blender container looks like this:

```weft
live = Group(keys: Access) {
  # One session's Blender and the routes that drive it
  blender = BlenderWorkstation {
    @per_instance
  }

  say = Route -> (text: String) { path: "sessions/{id}/messages", method: "POST", auth: self.keys }
  look = Route -> (view: String) { path: "sessions/{id}/preview/{view}", method: "GET", auth: self.keys }
  shoot = Route -> (id: String) { path: "sessions/{id}/render", method: "POST", auth: self.keys }

  agent = @include("live/agent.weft")
  agent.workstation = blender.workstation
  agent.text = say.text

  viewport = Group(workstation: Infra, view: String) {
    frame = BlenderPreview { workstation: self.workstation, view: self.view }
    show = Reply { answerAs: "bytes", body: frame.image }
  }
  viewport.workstation = blender.workstation
  viewport.view = look.view

  output = @include("live/output.weft")
  output.workstation = blender.workstation
  output.session = shoot.id
}
```

Each route is one item at the level, so a node with six routes, its
includes and its groups lands near ten, past the six that [the level rule]
aims for. That is the accepted exception: when routes must stay out of every
group that receives the per-instance value, they cannot be grouped away from
the node they serve, so keep them side by side and do not hunt for a
grouping that would put a route inside one. Everything else at that level
still follows the rule, and fifteen is still the limit the compiler warns at.

Validate the instance on every call. An id a browser sends is a claim, never
proof: a run is for an instance only through a token weft minted for it, or
through the `Weft-Instance` header sent by a server that has already checked
the caller may use that instance. If you catch yourself reading an instance id
out of a request body and acting on it, stop and write: "Wait. That id is a
claim." Then take it from a token or from a gated server call.

## Which instance a run is for

A run is for one instance or for none, stamped by what started it: `weft run
--instance <id>`; an event arriving through an instance's own container; a
live route called with an instance token; a gated live route called with
`Weft-Instance: <id>`. Everything else runs for no instance. A run is refused
before it starts when it reaches something per instance and is for no
instance, when the instance has not filled a field the run needs (or filled
it with a value the node refuses), or when the instance's container is down.
Each refusal names its fix. Hand that message to the user unchanged.

If a page has to show "starting" while an instance's container comes up, give
it two routes. The status route never reaches the container: it reads only
what the program keeps (the state weft holds for that instance's container,
or a row the program writes as it starts and stops it), and it runs for no
instance, so a container that is down cannot get it refused. For example,
reading the container's state with `InstanceInfraStatus`:

```weft
status = Route -> (id: String) { path: "session/status/{id}", method: "GET" }
state = InstanceInfraStatus { node: "sandbox", instance: status.id }
answer = Reply { body: state.status }
```

The page polls it until it reads `running`, and only then calls the second
route, the one that does the real work inside the instance.

An event reaching an instance's trigger before a field it needs is filled is
held, not dropped, and never retried on a timer: it runs when the instance's
values change (or the trigger is activated again). `weft status` shows under
the trigger how many events wait and for which field, and
`ctx.instances().list()` (the `ListInstances` node) hands the same to the
program, so a page can say what is missing. An event held because the
instance's container is down retries on its own until the container is up.

## Starting, stopping and ending an instance

weft never starts an instance's container by itself: the program does, or the
user with `--instance` on the infra verbs. A container runs from images that
`weft activate` builds and records for every node marked `@per_instance`, so
activate the program before it starts any container; without that, the start
fails naming `weft activate`. The instance nodes read the names of infra
nodes and triggers you write in them at compile time (a name that is not an
infra node, or not per instance, is refused then); a name arriving on a wire
is checked only when the run reaches it.

Creating an instance, from a gated route the backend calls:
`MintInstanceToken` (if a browser will act inside it), then `Reply` with the
token so the request ends at once, then `StartInstanceInfra` if the program
has a per-instance infra node (it fires `done` once the container runs), then
`ActivateInstanceTriggers`. A container can take minutes to come up, so the
request never waits for it: whoever shows it reads the `status` of that
instance's displays (the `weft-consumers` skill has the listing) or polls a
status route. If a trigger needs a value with no fallback, turning it on is
refused until that value is saved, so the activate step belongs in a second
call made once the values are saved. When an instance's value is saved, weft
restarts its triggers that are on so they use it, and leaves the ones that
are off alone.

**Every instance needs an end.** weft does not stop an idle instance by
itself, so a program that starts instances on demand also stops them, or
every container it ever started keeps running and costing. The shape:

1. every run for an instance writes a last-used time for it in the program's
   own database (one row per instance id);
2. a `Cron` in the program reads the instances idle past a limit and stops
   them (`StopInstanceInfra`, which keeps the disks) or removes them
   (`TerminateInstanceInfra`, or `WipeInstance` for everything);
3. the limit is the user's choice: ask them how long an idle instance lives
   before it is stopped, and whether it is then deleted, before you write it.

The same cron can also clean up what weft itself can see: containers in
`failed` (`ListInstanceInfra`), instances over a spending limit
(`InstanceCosts`), instances with events waiting on a field never filled
(`ListInstances`).

`WipeInstance` removes the instance's triggers, the containers you name (with
every disk, the ones kept through a terminate too), its values, connections,
tokens, files and runs. `weft rm` of the project takes every instance's
values, connections, tokens and files with it.

## Tokens and costs

An instance token acts inside that one instance of the program: it starts its
runs (the `Weft-Instance-Token` header on a live route), answers its waits,
shows its displays and connects its accounts. It always expires.
`MintInstanceToken` mints one for any instance id at any time, not only when
the instance is created, so a page can drive an instance started elsewhere
(by a cron, by another route). weft keeps only a hash of it, so a replayed run
cannot hand back the value it minted: it mints a new value for the same
token, and the old value stops working.

`InstanceCosts` (or `ctx.costs().instance(id)`) reads what an instance's calls
cost and whose credential paid each one, with the total; that is how a
program shows or bills usage per instance. weft only records the cost.

## The ctx calls

The instance nodes are in the `instances` package. A program that needs
something they do not cover calls the ctx directly in a node of its own:

| Call | What it does |
|---|---|
| `ctx.instance()` | Which instance this run is for (the `CurrentInstance` node fires `instance` or `nobody`) |
| `ctx.infra("bridge").instance(id).start()` / `.stop(spec, stop_self)` / `.terminate(spec, stop_self)` / `.wipe(spec, stop_self)` / `.status()` | One instance's container; `terminate` keeps the disks listed in `keepOnTerminate`, `wipe` deletes them too |
| `ctx.infra("bridge").copies()` | Every copy of the node: the shared one and each instance's (the `ListInstanceInfra` node) |
| `ctx.triggers().instance(id).activate()` / `.deactivate(spec, stop_self)` | An instance's triggers (`.only([..])` narrows) |
| `ctx.values().instance(id).get()` / `.set(step, field, value)` / `.clear(step, field)` / `.apply()` / `.forget()` | What was given for an instance's `@instance_filled` fields: read (the `GetInstanceValues` node), change in one go (the `SetInstanceValues` node; `apply()` returns the triggers it set up again), or forget all |
| `ctx.connections().instance(id).list()` / `.forget()` | An instance's connections (forgetting takes the values naming them) |
| `ctx.instances().list()` | Every instance weft holds anything for, with counts and states and the events waiting on a field not yet filled (the `ListInstances` node) |
| `ctx.costs().instance(id).service(s).since(t).list()` | What an instance cost (the `InstanceCosts` node, with the total) |
| `ctx.runs().instance(id).clean(running, stop_self)` | An instance's runs |
| `ctx.tokens().mint_for_instance(id, expires_in)` / `.instance(id).revoke()` | Instance tokens |
| `ctx.storage(StorageScope::instance())` | The run's instance's own files |

`start()` returns once the container is running, however long that takes
(the run parks between looks and holds no worker), so you can activate the
instance's triggers right after it. If the container never comes up,
`start()` fails with the reason, and so it does if somebody stops the
container while it waits (it never starts it again behind a pause). Every
reader of a container's state (`weft status`, `weft infra status`,
`.status()`, `InstanceInfraStatus`) gives one answer: `none` for a copy never
started (or terminated), and a start or stop on its way reads `provisioning`
or `stopping` at once.

If the run making a take-down call is among the runs it reaches,
`StopSelf::Keep` leaves it running and `StopSelf::Include` cancels it with
the rest.

## Where an instance's values are filled in

An instance's values (connecting its accounts is one of them) are filled in
on a settings page, never in the editor: the browser extension's **Your
settings** page with the instance token, or the `InstanceSettings` component
on the user's own site, copied into the frontend with `weft connect-lib`; for
mounting it, go and read the `weft-frontend` skill. It draws each field with
its own control, and a list field (its spreadsheets, its models) is read
through the instance's own connection. An instance is never offered the
shared key. To try an instance's path yourself before any website exists:
`weft connect --instance <id> --node <step>` connects an account for that
instance (`--door own --set-env key=ENV_VAR`) or picks one it has (`--grant
<id>`), and `weft instance-values --instance <id>` lists its fields, with
`--set step.field=value` / `--clear step.field` changing them. Each command
mints its own one-hour instance token and revokes it when done, so the
instance's own tokens are untouched.

## Checking your work

`weft infra status` lists each instance's container on its own line; `weft
status` lists every infra node the program declares under `infra:` (a
`@per_instance` one with how many instances have a container, one never
started as `not started`), each instance's container under `instance infra:`,
and each instance's triggers under `triggers:`, with the events a trigger
holds for a missing field. A plain `weft activate` turns on the shared
triggers and names the per-instance ones it left off; `weft activate
--instance <id>` (or `ActivateInstanceTriggers`) turns those on. `weft
executions --instance <id>` lists an instance's runs. To try the per-instance
path yourself, start a container (`weft infra start --instance test-1`), turn
on its triggers (`weft activate --instance test-1`), and run with `--instance
test-1`.
