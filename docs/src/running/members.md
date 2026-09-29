# Programs with members

If you want one program to serve many people, each with their own WhatsApp
number, Slack account, spreadsheet or schedule, you mark what belongs to one
person: a container each of them gets (`@per_member`), or a value each of them
gives (`@member_filled`). Those people are the program's **members**.

A member is just an id you choose, such as `user-42`, a customer number or an
email hash. You hand it to weft each time something happens for that person: in
`--member`, in a member token, or in a header from your own backend. weft keeps
no list of members, so there is nothing to register. Once you wipe everything
weft holds for an id (see [cleaning up after members](#cleaning-up-after-members)),
that id is no longer a member.

Say you built a WhatsApp assistant and a hundred customers want it. A WhatsApp
bridge signs in to one phone: when it first starts, it shows a QR code on its
display, and whoever scans that code with their phone ties the bridge to their
number. So each customer needs a bridge of their own:

```weft
bridge = BaileyBridge {
  @per_member
}
receive = BaileyReceive
receive.endpointUrl = bridge.endpointUrl
# a real assistant wires an LLM's answer into `message`
reply = BaileySend { message: "hi" }
reply.endpointUrl = bridge.endpointUrl
reply.to = receive.from
```

The `@per_member` line gives each member their own bridge container, called
their **copy** of `bridge`. `receive` and `reply` read the bridge, so they are
per member too. A message arriving on member `user-42`'s phone starts a run for
`user-42`, and `reply` answers through `user-42`'s copy.

## Bringing one member on

For customer `user-42`, from the project's folder:

0. If you have not activated the program since you last changed it, run
   `weft activate` once: that builds the bridge's image, which every member's
   copy starts from.
1. `weft infra start --member user-42` starts their bridge.
2. Wait until `weft infra status` shows their copy as running.
3. `weft activate --member user-42` turns on their `receive` trigger.
4. `weft token mint --member user-42 --expires 7d --display bridge` mints them a
   member token that can read their bridge's display.
5. They add the token to the weft browser extension, open their bridge's
   display, and scan the QR code with their phone.

If you want your own website to do steps 1 to 4 for a customer, a program can do
each of them itself; for how, go and read [the member nodes](#the-member-nodes).

## What `@per_member` marks

`@per_member` goes on its own line inside the braces of an infra node (a node
that runs its own container, like the bridge). Anywhere else the compiler
refuses it as `per-member-ineligible`.

If you work in the graph, you can set and remove the mark from a node's
right-click menu; for how, go and read [the graph](../build/the-graph.md).

## What a member fills: `@member_filled`

If each member should give their own value for a field (their spreadsheet,
their model, their schedule, their Slack account), write `@member_filled` where
the value would go:

```weft
google = GoogleAccess { account: @member_filled }
read = GoogleSheetsRead {
  spreadsheet: @member_filled
  tab: @member_filled("0")
}
read.account = google.access
digest = Cron { cron: @member_filled("0 0 8 * * *") }
```

A field holds one of four things:

| In the source | What the field holds |
|---|---|
| a value (`"0 0 3 * * *"`, a picked connection) | that value, for everyone |
| nothing | the node's default; if it has none, nothing when the field is optional, and a refused run when it is required |
| `@member_filled` | the member's value; if they gave none, the field acts as if nothing were written (the row above) |
| `@member_filled(<value>)` | the member's value; a member who gave none gets `<value>`, even when the node has a default of its own |
| `@member_filled(@file("prompts/default.md"))` | the same, with the file's content as the value a member who gave none gets (`@asset(...)` works too) |

On a connection field (`account` above), what you write decides whose key pays
for a member's calls:

| In the source | Whose key a member's run uses |
|---|---|
| nothing (your pick on the install) | yours, for every member |
| `account: @member_filled` | the member's own; a member who connected none is refused, naming the field |

Only you can put your key in a member's run, by leaving the field unmarked and picking your connection on the install. A member can never pick
your connection, or the runtime's shared key, for themselves: connecting with
the shared key through the member door is refused, and a value saved for a
member's connection field has to be a connection that member owns in this
program, whoever saves it. If your connection is one made with the runtime's
shared key, the members it serves spend that key, and each of their cost records shows
`Platform` as the payer (for reading those, go to [who paid for a
call](#who-paid-for-a-call)).

A field with a wire into it already gets its value from the wire, so the
compiler refuses `@member_filled` there (`member-filled-wired`). A group's, a
loop's or an included file's own port is refused too (`member-filled-boundary`);
put the mark on the node inside that reads the value. A key that is not one of
the node's inputs cannot take the mark either (`member-filled-not-an-input`).

You do not mark the other steps: a step that reads a member's value or copy,
directly or through steps before it, runs per member too. Above, `read` needs
Google through the member's connection, so it runs in that member's runs.

weft checks a member's value against the node's own rules at three moments:

- **when the program compiles**: a rule that only needs the field to have a
  value passes, since the member will give one later; a fallback is checked
  like any written value (`@member_filled("0 3 * * *")` on a `Cron` is refused
  right away);
- **when the member gives it**: a value the node refuses is refused on the
  spot, with the node's own message, and nothing is stored;
- **when a run starts**: the member's values are read once, checked again (the
  program may have changed since), and carried by the run, so every step of it
  sees the same values even if the member changes one mid-run.

If a member changes a value a live trigger of theirs reads (the trigger's own
field, like `digest.cron`, or anything its setup goes through, like its
connection), weft sets that trigger up again with the new value, and
only then saves the change. An event arriving meanwhile
waits and goes through afterwards, on the new value. If the new setup fails
(the new account refuses the connection, say), the old value and the old
trigger stay, and the change fails with the reason. If you change several
fields in one go (one `apply()`, one `--set` list), each trigger is set up
again only once.

## Who a run is for

Every run is for one member or for nobody. Whatever started the run decides, and
nothing changes it afterwards:

| What started the run | Who it is for |
|---|---|
| `weft run --member user-42` | `user-42` |
| An event arriving through a member's copy (their bridge, their trigger) | That member |
| A call to a `Route` with a member token in the `Weft-Member-Token` header | The token's member |
| A call to a gated `Route` with `Weft-Member: user-42` | `user-42` |
| A timer, a webhook, an open `Route`, anything else | Nobody |

A gated route is one whose `auth` input is wired from an auth node such as
`ApiKeyAuth` (weft's messages call it a route gated by a connection), so the
caller must present a key before a run starts. If your own backend calls
your program for a member, this is the route it uses. It is also the only place weft honours the `Weft-Member`
header: on an open route anybody could claim to be anybody, so weft refuses the
header there and says why. A browser presents a member token instead.

If you call a route and want the run to be for a member, call it at
`<dispatcher>/connect/<tenant>/<path>` (for where that address comes from, go
and read
[answering on a URL](../language/triggers-and-routes.md#answering-on-a-url)).
A webhook also answers at a plain address without `/connect`, but a run started
there is always for nobody, and weft refuses a member header sent there.

## What is checked before a run starts

weft refuses a run before it starts, and says why, when:

- it reaches something per member and is for nobody;
- it reaches a field the node needs and the member has not filled;
- it reaches a value the member gave that the node's rules refuse;
- it reads a member's copy that is not running.

If an event reaches a member's trigger before they have filled what it needs,
weft holds it until the member's values change (from their settings page,
`SetMemberValues` or `ctx.values()`), then runs it; activating the trigger again
also tries it. Nothing retries it on a timer, since only the member can close
the gap. `weft status` shows under the trigger how many events wait and which
field they wait for, and so do `ListMembers` and `ctx.members().list()`, so your
website can tell the member what is missing. An event that reaches the trigger
while their copy is down is tried again on a timer (sooner at first, then every
five minutes) until the copy is up. A call to a route gets the refusal instead.

## A member's own infra

From the terminal, the verbs that start, stop, upgrade or remove infra take
`--member`:

```bash
weft infra start --member user-42
weft infra stop --member user-42                     # the container goes, its disk stays
weft infra node-terminate bridge --member user-42    # the container goes, and its disk unless the node keeps it
```

`weft infra status` lists each member's copy on its own line. `weft status`
lists them under `member copies:`, and under `infra:` it lists every infra node
your program declares: a `@per_member` one says how many members have a copy,
and one that was never started says `not started`.

Every place that reports a copy's state gives the same answer: `weft status`,
`weft infra status`, `ctx.infra(..).status()` and the `MemberInfraStatus` node.
A start or a stop on its way reads `provisioning` or `stopping` from the moment
it is asked for, before the container itself moves.

From your program, `ctx.infra` names the node and the member:

```rust
ctx.infra("bridge").member("user-42").start().await?;
let copy = ctx.infra("bridge").member("user-42").status().await?; // None: no copy
let every = ctx.infra("bridge").copies().await?;                 // shared + each member's
```

weft never starts a member's copy on its own: you do, from the terminal as
above, or your program does. If you skipped step 0, the start fails and names
`weft activate`.

`start()` returns once the copy is running, so the next line can activate the
triggers that read it. A container can take minutes to come up, and the run
waits that long without holding a worker: between looks at the copy it parks,
the way a `Wait` node does. If the copy fails to come up, `start()` fails with
the reason. If somebody stops or terminates the copy while `start()` waits (a
member pausing right after resuming, say), `start()` fails too, rather than
starting it again behind their back.

## A member's own triggers

A trigger that reads a per-member node exists once per member, and you turn each
member's version of it on and off separately. `weft activate` with no flags turns on
every shared trigger and skips these, because it cannot know which members you
mean:

```bash
weft activate --member user-42
weft activate --trigger receive --member user-42
weft deactivate --member user-42 --mode park
```

`weft status` lists every trigger's state under `triggers:`, a member's with the
member beside it. A trigger being turned on reads `activating` from the moment
the activate is taken, even while weft is still moving the program's workers. If
you are scripting this, `weft status --json` has the same list under
`activations`, each entry with its `trigger`, `member` (absent for the program's
own), `status` and `mode`, plus `waiting` (`{ fires, reason }`) when the member's
trigger holds events until they fill a field.

A plain `weft deactivate` turns off the program's own triggers and leaves the
members' on, and it tells you how many members still have triggers on. If you
want every member's off too, run it again with `--all-members`:

```bash
weft deactivate --mode park
weft deactivate --all-members --mode park
```

`weft resync` works the other way round: with no flag it brings every trigger
that is on up to date with your program, the program's own first and then each
member's, and prints whose it did. `--member user-42` limits it to that member.

From your program:

```rust
ctx.triggers().member("user-42").activate().await?;
```

If you want to turn one off from your program, the call takes a few choices; for
those, go and read [taking something down from a
run](#taking-something-down-from-a-run).

If a member's trigger reads their copy and that copy is not running, activating
the trigger is refused, with the command that starts the copy.

If a member's trigger reads a value they fill (`digest.cron`, say), activating it
before they gave one is refused, naming the field, unless the field has a
fallback. If you want a member's triggers on the moment they join, give such
fields a fallback: the trigger runs on the fallback until the member saves their
own value, and then weft restarts it with theirs. Saving a value never turns on a
trigger that is off.

## Where a member fills in their values

Each member fills in their values on their settings page. It lists every field
your program marks `@member_filled`, grouped by step, each with the control the
editor shows you: a connection picker for a connection, a searchable list for a
list field (their spreadsheets, say, read through their own connection), and a
plain input for anything else.

If you want to show it to them, there are two places:

- the browser extension's **Your settings** page, for a member who has added
  their member token to the extension;
- the `MemberSettings` component, for your own website. `weft connect-lib`
  copies the library into your frontend (`front/src/lib/weft-connect` unless
  `--into` names another folder); for how to mount it, go and read the README
  it copies along.

The picker never offers a member the shared key, because it spends your
credits.

If you want to test what a member sees from the terminal, two commands do what
their page does (each mints its own member token that lasts one hour and revokes it when done, so the
member's own tokens are untouched):

- `weft connect --member user-42 --node openrouter --door own --set-env
  key=OPENROUTER_KEY` connects an account as theirs from a key in your
  environment, and `--grant <id>` picks one they already have; `--list` and
  `--disconnect` work the same way;
- `weft member-values --member user-42` lists their fields and values, and
  `--set read.spreadsheet=1AbC --clear digest.cron` changes them. A value that
  reads as JSON (`5`, `true`, `{"id": "..."}`) is passed as JSON, so write a
  value made only of digits as `'read.tab="12"'` to keep it text.

If your program should read or change a member's values, `ctx.values()` does
it, with the same checks, and sets affected triggers up again:

```rust
let values = ctx.values().member("user-42").get().await?; // by step, then field
let rearmed = ctx.values()
    .member("user-42")
    .set("read", "spreadsheet", json!("1AbC..."))
    .set("google", "account", json!({ "id": connection_id }))
    .clear("digest", "cron")
    .apply()
    .await?; // returns the triggers it set up again
let theirs = ctx.connections().member("user-42").list().await?;
ctx.connections().member("user-42").forget().await?;    // the values naming them go too
```

## Member tokens

A member token acts as one member of one program, and nothing else. With it, a
browser or the extension can:

- start runs as that member, through the program's routes, in the
  `Weft-Member-Token` header;
- answer that member's waiting forms;
- read the displays of that member's copies, such as their bridge's QR code;
- fill in that member's values, their connections among them.

A member token always expires. If you mint one from the terminal (step 4
above), it reads a display only when you pass `--display <node>` for that node,
or `--displays` for all of them. A token your program mints reads all of the
member's displays and lives at most a year. If a member just logged in to your
site, this is how your program hands them a token:

```rust
let minted = ctx.tokens().mint_for_member("user-42", Duration::from_secs(7 * 86400)).await?;
// minted.token goes to that member's browser, once
ctx.tokens().member("user-42").revoke().await?;
```

The token comes out once, because weft keeps only a hash of it. So when a run is
replayed after a restart, weft cannot hand back the token it minted the first
time: it mints a fresh value for the same token, and the old value stops
working. If your program already handed the first value to the member's
browser, that browser needs the new one.

## A member's files

`StorageScope::member()` holds the files of the run's own member, and
`StorageScope::member_of(id)` those of any member of this program:

```rust
ctx.storage(StorageScope::member())
ctx.storage(StorageScope::member_of(MemberId::new("user-42")?))
```

The files stay until your program deletes them, you wipe the member (see
[cleaning up after members](#cleaning-up-after-members)), or you `weft rm` the
project. Using `member()` in a run for nobody is an error. Another program never
reaches these files, even for the same member id.

## Who paid for a call

Every cost record says whose credential paid: the runtime's own key (`Platform`,
when a step uses the shared key), yours (`Author`), or the member's own
(`Member`). `ctx.costs()` reads them, and you can narrow it by member, node,
service, run, who paid (`paid_by`) or `since` (unix seconds):

```rust
let spent = ctx.costs().member("user-42").service("openrouter").since(month_start).list().await?;
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
ctx.trigger("receive").member("user-42").deactivate(park, StopSelf::Keep).await?;
```

The mode decides what happens to the triggers. The running policy decides what
happens to runs that were using the thing you took down: taking down a member's
copy touches only that member's runs, and taking down a member's trigger touches
only the runs that trigger started.

If the run making the call is one of the runs the call would take down, the last
argument decides what happens to it:

- `StopSelf::Keep`: it keeps going.
- `StopSelf::Include`: it is cancelled with the rest, once the take-down is
  queued; the call never returns.

If a run is replayed after a restart, a copy it terminated is never terminated
twice (for why, go and read [your project beyond this
run](../nodes/ctx.md#your-project-beyond-this-run)).

## Cleaning up after members

If you want to see who is there, `ctx.members().list()` (or the `ListMembers`
node) gives one entry per member weft holds anything for: how many values,
connections and live tokens they have, their infra copies, and their triggers.
It never hands back a value or a secret.

If you want to see a member's runs, `weft executions --member user-42` lists
them. `weft clean` takes the same filters (member, status, the node that started
the run, tag), and so does a program's `ctx.runs()`:

```bash
weft clean --project <id> --member user-42 --yes
```

```rust
let week = Duration::from_secs(7 * 86400);
ctx.runs().member("user-42").status("completed").older_than(week).clean(RunningPolicy::Wait, StopSelf::Keep).await?;
```

`clean` never deletes a run that is still going. With
`--cancel-running` (or `RunningPolicy::Cancel`) it cancels it, and the run's
rows go with the next clean; without it (`RunningPolicy::Wait`) the run is left
to finish.

weft has no "delete a member" command. The `WipeMember` node removes everything
attached to one: their triggers, the copies of the infra nodes you name in its
`infra` input, the values they gave, their connections, their tokens, their
files, and their runs. If a wipe stops part way, run it again to finish it.

## The member nodes

If you want a graph to bring a member on and remove them without writing code,
the `members` package of the standard catalog has a node for each step:

| Node | What it does |
|---|---|
| `StartMemberInfra` | Starts a member's copy of an infra node, and fires once it runs |
| `StopMemberInfra` | Stops it, keeping its disk |
| `TerminateMemberInfra` | Deletes it and its disk |
| `MemberInfraStatus` | Reads the state of a member's copy |
| `ListMemberCopies` | Lists every copy of an infra node, the shared one and each member's |
| `ListMembers` | Lists every member weft holds anything for (values, connections, copies, tokens, triggers), with counts and states, and the events waiting on a field they have not filled |
| `CurrentMember` | Says who the run is for: fires `member` with their id, or `nobody` |
| `MemberCosts` | Lists what a member's runs cost, with the total |
| `GetMemberValues` | Reads what a member gave for the `@member_filled` fields, keyed `node.field` |
| `SetMemberValues` | Sets or clears a member's `@member_filled` values, and sets up again any of their triggers that read a changed one |
| `ActivateMemberTriggers` | Turns on a member's triggers |
| `DeactivateMemberTriggers` | Turns them off |
| `MintMemberToken` | Mints a member token to hand to their browser |
| `WipeMember` | Removes everything attached to a member |

If your backend should onboard members, put `MintMemberToken`,
`StartMemberInfra` and `ActivateMemberTriggers` behind a gated route, in that
order, and reply with the token before the copy comes up so the request does
not wait minutes. If one of their triggers reads a value with no fallback,
activating it at onboarding is refused, so leave `ActivateMemberTriggers` for a
second call your website makes once the member has saved their settings. The
same route can offboard with `WipeMember`: weft cannot
tell when a member leaves, so your backend says so.

## When something goes wrong

| What you see | What it means |
|---|---|
| `'bridge' exists once per member, so this run needs to know which member it is for` | The run reaches something per member and nobody named one. Add `--member`, or start the run through a member's trigger, with a member token, or with the `Weft-Member` header on a gated route |
| `member 'user-42' has not filled 'read.spreadsheet'` | The run needs a value that member never gave. They fill it in on their settings page, or your program sets it with `SetMemberValues` or `ctx.values()` |
| `member 'user-42' at 'digest': ...` | That member's value breaks the node's own rule, which the rest of the message states. They change it on their settings page, or your program does with `SetMemberValues` |
| `connect your account at 'google' first: this list is read through it` | A list on the member's page is read through their connection at that step, and they have not connected one yet. They connect it first, at that step |
| `infra not running for: bridge (member 'user-42')` | That member's copy is down. The message names the command that starts it |
| `these triggers' infra is not running: ...` | You activated a member's trigger before their copy was up. Run the command it names, then activate again |
| `the Weft-Member header is honoured only on a route gated by a connection` | You sent the header to an open route. Gate the route, or use a member token |
| `a bare fire runs for no member` | A member header reached a webhook's plain address. Call the route at `/connect/...` |
| `this token has expired; ask for a new one` | The member token's expiry has passed. Mint them a new one |
| `this door takes a member token` | A token that is not a member token (a plain api token, say) reached a member's settings page. Mint one with `weft token mint --member <id>` or `ctx.tokens().mint_for_member` |
| `a member connects their own account; the shared key is the program author's` | A member tried to connect with the shared key. They connect their own account |
