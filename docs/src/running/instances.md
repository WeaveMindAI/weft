# Programs with instances

If you want part of one program to run as several separate copies, each with
its own WhatsApp number, Slack account, spreadsheet or schedule, you mark what
each copy gets for itself: a container of its own (`@per_instance`), or a value
of its own (`@instance_filled`). Each of those copies is an **instance** of the
program.

What an instance stands for is up to you. One per customer is the common case,
but it can as well be one per session, or several per person (a customer with
two shops, each with its own bridge). weft only sees the ids: which person owns
which instances is your program's own data, kept in its own database.

An instance is just an id you choose, such as `user-42`, `session-9f3` or a
shop number. You hand it to weft each time something happens for that
instance: in `--instance`, in an instance token, or in a header from your own
backend. weft keeps no list of instances, so there is nothing to register. Once
you wipe everything weft holds for an id (see
[cleaning up after instances](#cleaning-up-after-instances)), that id is no
longer an instance.

Say you built a WhatsApp assistant and a hundred customers want it. A WhatsApp
bridge signs in to one phone: when it first starts, it shows a QR code on its
display, and whoever scans that code with their phone ties the bridge to their
number. So each customer needs a bridge of their own, and you give each one an
instance:

```weft
bridge = BaileyBridge {
  @per_instance
}
receive = BaileyReceive { bridge: bridge.bridge }
# a real assistant wires an LLM's answer into `message`
reply = BaileySend {
  bridge: bridge.bridge
  to: receive.from
  message: "hi"
}
```

The `@per_instance` line gives each instance its own bridge container, called
its **copy** of `bridge`. `receive` and `reply` read the bridge, so they are
per instance too. A message arriving on instance `user-42`'s phone starts a run
for `user-42`, and `reply` answers through `user-42`'s copy.

## Bringing one instance up

For customer `user-42`, from the project's folder:

0. If you have not activated the program since you last changed it, run
   `weft activate` once: that builds the bridge's image, which every
   instance's copy starts from.
1. `weft infra start --instance user-42` starts their bridge.
2. Wait until `weft infra status` shows that copy as running.
3. `weft activate --instance user-42` turns on its `receive` trigger.
4. `weft token mint --instance user-42 --expires 7d --display bridge` mints an
   instance token that can read that bridge's display.
5. The customer adds the token to the weft browser extension, opens the
   bridge's display, and scans the QR code with their phone.

If you want your own website to do steps 1 to 4 for a customer, a program can do
each of them itself; for how, go and read [the instance nodes](#the-instance-nodes).

## What `@per_instance` marks

`@per_instance` goes on its own line inside the braces of an infra node (a node
that runs its own container, like the bridge). Anywhere else the compiler
refuses it as `per-instance-ineligible`.

If you work in the graph, you can set and remove the mark from a node's
right-click menu; for how, go and read [the graph](../build/the-graph.md).

## What an instance fills: `@instance_filled`

If each instance should get its own value for a field (a spreadsheet, a model,
a schedule, a Slack account), write `@instance_filled` where the value would go:

```weft
google = GoogleAccess { account: @instance_filled }
read = GoogleSheetsRead {
  account: google.access
  spreadsheet: @instance_filled
  tab: @instance_filled("0")
}
digest = Cron { cron: @instance_filled("0 0 8 * * *") }
```

A field holds one of four things:

| In the source | What the field holds |
|---|---|
| a value (`"0 0 3 * * *"`, a picked connection) | that value, for everyone |
| nothing | the node's default; if it has none, nothing when the field is optional, and a refused run when it is required |
| `@instance_filled` | the instance's value; if none was given, the field acts as if nothing were written (the row above) |
| `@instance_filled(<value>)` | the instance's value; an instance with none gets `<value>`, even when the node has a default of its own |
| `@instance_filled(@file("prompts/default.md"))` | the same, with the file's content as the value an instance with none gets (`@asset(...)` works too) |

On a connection field (`account` above), what you write decides whose key pays
for an instance's calls:

| In the source | Whose key an instance's run uses |
|---|---|
| nothing (your pick on the install) | yours, for every instance |
| `account: @instance_filled` | the instance's own; an instance with no connection is refused, naming the field |

Only you can put your key in an instance's run, by leaving the field unmarked
and picking your connection on the install. An instance token can never pick
your connection, or the runtime's shared key: connecting with the shared key
through the instance door is refused, and a value saved for an instance's
connection field has to be a connection that instance owns in this program,
whoever saves it. If your connection is one made with the runtime's shared key,
the instances it serves spend that key, and each of their cost records shows
`Platform` as the payer (for reading those, go to [who paid for a
call](#who-paid-for-a-call)).

A field with a wire into it already gets its value from the wire, so the
compiler refuses `@instance_filled` there (`instance-filled-wired`). A group's,
a loop's or an included file's own port is refused too
(`instance-filled-boundary`); put the mark on the node inside that reads the
value. A key that is not one of the node's inputs cannot take the mark either
(`instance-filled-not-an-input`).

You do not mark the other steps: a step that reads an instance's value or copy,
directly or through steps before it, runs per instance too. Above, `read` needs
Google through the instance's connection, so it runs in that instance's runs.

weft checks an instance's value against the node's own rules at three moments:

- **when the program compiles**: a rule that only needs the field to have a
  value passes, since the value will be given later; a fallback is checked
  like any written value (`@instance_filled("0 3 * * *")` on a `Cron` is
  refused right away);
- **when the value is given**: a value the node refuses is refused on the
  spot, with the node's own message, and nothing is stored;
- **when a run starts**: the instance's values are read once, checked again
  (the program may have changed since), and carried by the run, so every step
  of it sees the same values even if one changes mid-run.

If an instance's value changes and a live trigger of that instance reads it
(the trigger's own field, like `digest.cron`, or anything its setup goes
through, like its connection), weft sets that trigger up again with the new
value, and only then saves the change. An event arriving meanwhile waits and
goes through afterwards, on the new value. If the new setup fails (the new
account refuses the connection, say), the old value and the old trigger stay,
and the change fails with the reason. If you change several fields in one go
(one `apply()`, one `--set` list), each trigger is set up again only once.

## Which instance a run is for

Every run is for one instance or for none. Whatever started the run decides,
and nothing changes it afterwards:

| What started the run | Which instance it is for |
|---|---|
| `weft run --instance user-42` | `user-42` |
| An event arriving through an instance's copy (its bridge, its trigger) | That instance |
| A call to a `Route` with an instance token in the `Weft-Instance-Token` header | The token's instance |
| A call to a gated `Route` with `Weft-Instance: user-42` | `user-42` |
| A timer, a webhook, an open `Route`, anything else | None |

A gated route is one whose `auth` input is wired from an auth node such as
`ApiKeyAuth` (weft's messages call it a route gated by a connection), so the
caller must present a key before a run starts. If your own backend calls your
program for an instance, this is the route it uses. It is also the only place
weft honours the `Weft-Instance` header: on an open route anybody could claim
any instance, so weft refuses the header there and says why. A browser presents
an instance token instead.

If you call a route and want the run to be for an instance, call it at one of
its route addresses: `/connect/local/<project id>/<path>` on the install, the project's own
port on your machine, or an API domain (for where those come from, go and read
[answering on a URL](../language/triggers-and-routes.md#answering-on-a-url)).
A webhook also answers at a plain address without `/connect`, but a run started
there is never for an instance, and weft refuses an instance header sent there.

## What is checked before a run starts

weft refuses a run before it starts, and says why, when:

- it reaches something per instance and is for no instance;
- it reaches a field the node needs and the instance has not filled;
- it reaches a value the instance has that the node's rules refuse;
- it reads an instance's copy that is not running.

If an event reaches an instance's trigger before what it needs is filled, weft
holds it until the instance's values change (from its settings page,
`SetInstanceValues` or `ctx.values()`), then runs it; activating the trigger
again also tries it. Nothing retries it on a timer, since only a new value can
close the gap. `weft status` shows under the trigger how many events wait and
which field they wait for, and so do `ListInstances` and
`ctx.instances().list()`, so your website can tell the customer what is
missing. An event that reaches the trigger while the instance's copy is down is
tried again on a timer (sooner at first, then every five minutes) until the copy
is up. A call to a route gets the refusal instead.

## An instance's own infra

From the terminal, the verbs that start, stop, upgrade or remove infra take
`--instance`:

```bash
weft infra start --instance user-42
weft infra stop --instance user-42                     # the container goes, its disk stays
weft infra node-terminate bridge --instance user-42 --yes    # the container goes, and its disk unless the node keeps it
```

`weft infra status` lists each instance's copy on its own line. `weft status`
lists them under `instance infra:`, and under `infra:` it lists every infra
node your program declares: a `@per_instance` one says how many instances have
a copy, and one that was never started says `not started`.

Every place that reports a copy's state gives the same answer: `weft status`,
`weft infra status`, `ctx.infra(..).status()` and the `InstanceInfraStatus`
node. A start or a stop on its way reads `provisioning` or `stopping` from the
moment it is asked for, before the container itself moves.

From your program, `ctx.infra` names the node and the instance:

```rust
ctx.infra("bridge").instance("user-42").start().await?;
let copy = ctx.infra("bridge").instance("user-42").status().await?; // None: no copy
let every = ctx.infra("bridge").copies().await?;                     // shared + each instance's
```

weft never starts an instance's copy on its own: you do, from the terminal as
above, or your program does. If you skipped step 0, the start fails and names
`weft activate`.

`start()` returns once the copy is running, so the next line can activate the
triggers that read it. A container can take minutes to come up, and the run
waits that long without holding a worker: between looks at the copy it parks,
the way a `Wait` node does. If the copy fails to come up, `start()` fails with
the reason. If somebody stops or terminates the copy while `start()` waits (a
customer pausing right after resuming, say), `start()` fails too, rather than
starting it again behind their back.

## An instance's own triggers

A trigger that reads a per-instance node exists once per instance, and you turn
each instance's version of it on and off separately. `weft activate` with no
flags turns on every shared trigger and skips these, because it cannot know
which instances you mean:

```bash
weft activate --instance user-42
weft activate --trigger receive --instance user-42
weft deactivate --instance user-42 --mode park
```

`weft status` lists every trigger's state under `triggers:`, an instance's with
the instance beside it. A trigger being turned on reads `activating` from the
moment the activate is taken, even while weft is still moving the program's
workers. If you are scripting this, `weft status --json` has the same list
under `activations`, each entry with its `trigger`, `instance` (absent for the
program's own), `status` and `mode`, plus `waiting` (`{ fires, reason }`) when
the instance's trigger holds events until a field is filled.

A plain `weft deactivate` turns off the program's own triggers and leaves the
instances' on, and it tells you how many instances still have triggers on. If
you want every instance's off too, run it again with `--all-instances`:

```bash
weft deactivate --mode park
weft deactivate --all-instances --mode park
```

`weft resync` works the other way round: with no flag it brings every trigger
that is on up to date with your program, the program's own first and then each
instance's, and prints whose it did. `--instance user-42` limits it to that
instance.

From your program:

```rust
ctx.triggers().instance("user-42").activate().await?;
```

If you want to turn one off from your program, the call takes a few choices; for
those, go and read [taking something down from a
run](#taking-something-down-from-a-run).

If an instance's trigger reads its copy and that copy is not running,
activating the trigger is refused, with the command that starts the copy.

If an instance's trigger reads a value it fills (`digest.cron`, say),
activating it before one was given is refused, naming the field, unless the
field has a fallback. If you want an instance's triggers on the moment it is
created, give such fields a fallback: the trigger runs on the fallback until
the instance's own value is saved, and then weft restarts it with that one.
Saving a value never turns on a trigger that is off.

## Where an instance's values are filled in

Each instance's values are filled in on its settings page. It lists every field
your program marks `@instance_filled`, grouped by step, each with the control
the editor shows you: a connection picker for a connection, a searchable list
for a list field (spreadsheets, say, read through the instance's own
connection), and a plain input for anything else.

If you want to show it to your customers, there are two places:

- the browser extension's **Your settings** page, for somebody who has added
  an instance token to the extension;
- the `InstanceSettings` component, for your own website. `weft connect-lib`
  copies the library into your frontend (`front/src/lib/weft-connect` unless
  `--into` names another folder); for how to mount it, go and read the README
  it copies along.

The picker never offers an instance the shared key, because it spends your
credits.

If you want to test that page from the terminal, two commands do what it does
(each mints its own instance token that lasts one hour and revokes it when
done, so the instance's own tokens are untouched):

- `weft connect --instance user-42 --node openrouter --door own --set-env
  key=OPENROUTER_KEY` connects an account as that instance's from a key in
  your environment, and `--grant <id>` picks one it already has; `--list` and
  `--disconnect` work the same way;
- `weft instance-values --instance user-42` lists its fields and values, and
  `--set read.spreadsheet=1AbC --clear digest.cron` changes them. A value that
  reads as JSON (`5`, `true`, `{"id": "..."}`) is passed as JSON, so write a
  value made only of digits as `'read.tab="12"'` to keep it text.

If your program should read or change an instance's values, `ctx.values()` does
it, with the same checks, and sets affected triggers up again:

```rust
let values = ctx.values().instance("user-42").get().await?; // by step, then field
let rearmed = ctx.values()
    .instance("user-42")
    .set("read", "spreadsheet", json!("1AbC..."))
    .set("google", "account", json!({ "id": connection_id }))
    .clear("digest", "cron")
    .apply()
    .await?; // returns the triggers it set up again
let theirs = ctx.connections().instance("user-42").list().await?;
ctx.connections().instance("user-42").forget().await?;    // the values naming them go too
```

## Instance tokens

An instance token acts inside one instance of one program, and nothing else.
With it, a browser or the extension can:

- start that instance's runs, through the program's routes, in the
  `Weft-Instance-Token` header;
- answer that instance's waiting forms;
- read the displays of that instance's copies, such as its bridge's QR code;
- fill in that instance's values, its connections among them.

An instance token always expires. If you mint one from the terminal (step 4
above), it reads a display only when you pass `--display <node>` for that node,
or `--displays` for all of them. A token your program mints reads all of the
instance's displays and lives at most a year. If a customer just logged in to
your site, this is how your program hands their browser a token for one of
their instances (which instances are theirs is up to your own database):

```rust
let minted = ctx.tokens().mint_for_instance("user-42", Duration::from_secs(7 * 86400)).await?;
// minted.token goes to the browser, once
ctx.tokens().instance("user-42").revoke().await?;
```

The token comes out once, because weft keeps only a hash of it. So when a run is
replayed after a restart, weft cannot hand back the token it minted the first
time: it mints a fresh value for the same token, and the old value stops
working. If your program already handed the first value to a browser, that
browser needs the new one.

## An instance's files

`StorageScope::instance()` holds the files of the run's own instance, and
`StorageScope::instance_of(id)` those of any instance of this program:

```rust
ctx.storage(StorageScope::instance())
ctx.storage(StorageScope::instance_of(InstanceId::new("user-42")?))
```

The files stay until your program deletes them, you wipe the instance (see
[cleaning up after instances](#cleaning-up-after-instances)), or you `weft rm`
the project. Using `instance()` in a run for no instance is an error. Another
program never reaches these files, even for the same instance id.

## Who paid for a call

Every cost record says whose credential paid: the runtime's own key
(`Platform`, when a step uses the shared key), yours (`Author`), or the
instance's own (`Instance`). `ctx.costs()` reads them, and you can narrow it by
instance, node, service, run, who paid (`paid_by`) or `since` (unix seconds):

```rust
let spent = ctx.costs().instance("user-42").service("openrouter").since(month_start).list().await?;
```

## Taking something down from a run

A program's `stop`, `terminate` and `deactivate` calls each take a
`DeactivateSpec`, the same choices `weft deactivate` asks you for:

```rust
let park = DeactivateSpec {
    mode: DeactivationMode::Park,          // or Hibernate, or Wipe
    grace_minutes: 0,                      // Hibernate's window before it wipes
    running_policy: RunningPolicy::Wait,   // or Cancel
    drain_timeout_secs: None,              // how long Wait waits; None is 60 seconds
};
ctx.trigger("receive").instance("user-42").deactivate(park, StopSelf::Keep).await?;
```

The mode decides what happens to the triggers. The running policy decides what
happens to runs that were using the thing you took down: taking down an
instance's copy touches only that instance's runs, and taking down an
instance's trigger touches only the runs that trigger started.

If the run making the call is one of the runs the call would take down, the last
argument decides what happens to it:

- `StopSelf::Keep`: it keeps going.
- `StopSelf::Include`: it is cancelled with the rest, once the take-down is
  queued; the call never returns.

If a run is replayed after a restart, a copy it terminated is never terminated
twice (for why, go and read [your project beyond this
run](../nodes/ctx.md#your-project-beyond-this-run)).

## Cleaning up after instances

If you want to see which instances there are, `ctx.instances().list()` (or the
`ListInstances` node) gives one entry per instance weft holds anything for: how
many values, connections and live tokens it has, its infra copies, and its
triggers. It never hands back a value or a secret.

If you want to see an instance's runs, `weft executions --instance user-42`
lists them. `weft clean` takes the same filters (instance, status, the node
that started the run, tag), and so does a program's `ctx.runs()`:

```bash
weft clean --project <id> --instance user-42 --yes
```

```rust
let week = Duration::from_secs(7 * 86400);
ctx.runs().instance("user-42").status("completed").older_than(week).clean(RunningPolicy::Wait, StopSelf::Keep).await?;
```

If a program wants to read its runs rather than delete them, the same filters
end in `.list(limit)` (the newest `limit` runs, 1 to 200, with `total`, how
many match in all) or `.count()`. The `ListRuns` and `CountRuns` nodes do the
same from the graph.

```rust
let failed_today = ctx.runs().instance("user-42").status("failed").count().await?;
```

`clean` never deletes a run that is still going. With
`--cancel-running` (or `RunningPolicy::Cancel`) it cancels it, and the run's
rows go with the next clean; without it (`RunningPolicy::Wait`) the run is left
to finish.

weft has no "delete an instance" command. The `WipeInstance` node removes
everything attached to one: its triggers, the copies of the infra nodes you
name in its `infra` input, its values, its connections, its tokens, its files,
and its runs. If a wipe stops part way, run it again to finish it.

## The instance nodes

If you want a graph to bring an instance up and remove it without writing code,
the `instances` package of the standard catalog has a node for each step:

| Node | What it does |
|---|---|
| `StartInstanceInfra` | Starts an instance's copy of an infra node, and fires once it runs (or, with `waitUntilRunning` off, once weft accepted the start) |
| `StopInstanceInfra` | Stops it, keeping its disk |
| `TerminateInstanceInfra` | Deletes it and its disk |
| `InstanceInfraStatus` | Reads the state of an instance's copy |
| `ListInstanceInfra` | Lists every copy of an infra node, the shared one and each instance's |
| `ListInstances` | Lists every instance weft holds anything for (values, connections, copies, tokens, triggers), with counts and states, and the events waiting on a field not yet filled |
| `CurrentInstance` | Says which instance the run is for: fires `instance` with its id, or `none` |
| `InstanceCosts` | Lists what an instance's runs cost, with the total |
| `GetInstanceValues` | Reads an instance's values for the `@instance_filled` fields, keyed `node.field` |
| `SetInstanceValues` | Sets or clears an instance's `@instance_filled` values, and sets up again any of its triggers that read a changed one |
| `ActivateInstanceTriggers` | Turns on an instance's triggers |
| `DeactivateInstanceTriggers` | Turns them off |
| `MintInstanceToken` | Mints an instance token to hand to a browser |
| `WipeInstance` | Removes everything attached to an instance |

If your backend should create instances, put `MintInstanceToken`,
`StartInstanceInfra` and `ActivateInstanceTriggers` behind a gated route, in
that order. If you want the request to answer at once rather than wait
minutes for the copy, turn `waitUntilRunning` off on `StartInstanceInfra` and
reply with the token after it: the start is accepted by then, so a list read
right after shows the copy as `provisioning`. A reply placed before the start
leaves nothing for that list to show yet. If one of the instance's triggers reads a value with no
fallback, activating it at creation is refused, so leave
`ActivateInstanceTriggers` for a second call your website makes once the
settings are saved. The same route can remove an instance with `WipeInstance`:
weft cannot tell when an instance is no longer needed, so your backend says so.

## When something goes wrong

| What you see | What it means |
|---|---|
| `'bridge' exists once per instance, so this run needs to know which instance it is for` | The run reaches something per instance and nobody named one. Add `--instance`, or start the run through an instance's trigger, with an instance token, or with the `Weft-Instance` header on a gated route |
| `instance 'user-42' has not filled 'read.spreadsheet'` | The run needs a value that instance never got. Fill it in on its settings page, or your program sets it with `SetInstanceValues` or `ctx.values()` |
| `instance 'user-42' at 'digest': ...` | That instance's value breaks the node's own rule, which the rest of the message states. Change it on its settings page, or your program does with `SetInstanceValues` |
| `connect your account at 'google' first: this list is read through it` | A list on the instance's settings page is read through its connection at that step, and none is connected yet. Connect it first, at that step |
| `infra not running for: bridge (instance 'user-42')` | That instance's copy is down. The message names the command that starts it |
| `these triggers' infra is not running: ...` | You activated an instance's trigger before its copy was up. Run the command it names, then activate again |
| `the Weft-Instance header is honoured only on a route gated by a connection` | You sent the header to an open route. Gate the route, or use an instance token |
| `a bare fire runs for no instance` | An instance header reached a webhook's plain address. Call the route at `/connect/...` |
| `this token has expired; ask for a new one` | The instance token's expiry has passed. Mint a new one |
| `this door takes an instance token` | A token that is not an instance token (a plain api token, say) reached an instance's settings page. Mint one with `weft token mint --instance <id>` or `ctx.tokens().mint_for_instance` |
| `an instance connects its own account; the shared key is the program author's` | Somebody tried to connect an instance with the shared key. Connect the instance's own account |
