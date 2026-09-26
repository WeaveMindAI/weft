---
name: weft-members
description: "Read when a program must serve many people, each with their own account, number, bot, database, spreadsheet or schedule (a WhatsApp assistant for many customers, a Slack bot per workspace, a report per client): `@per_member` (a container each), `@member_filled` (a value each, a connection included), who a run is for, starting and stopping one member's copies, changing a member's values from the program, member tokens, and the shape such a program takes (a shared part, an admin route, a cron)."
---

# Programs with members

A [member] is one person a program serves, named by an id the program picks
(`user-42`). weft keeps no list of members: an id counts as a member as long
as weft still holds something for it (a copy, a value, a token, a run).

You never copy a program once per person: you mark what belongs to one person,
in one of two ways.

## What you mark

`@per_member` goes on its own line in the braces of an infra node, and gives each
member their own container. Anywhere else it is refused as
`per-member-ineligible`.

`@member_filled` goes where a field's value would, and makes that value each
member's own: their connection, their spreadsheet, their model, their schedule.
`@member_filled(<value>)` names what a member who gave none gets (it wins over
the node's default); bare `@member_filled` leaves such a member's field empty.
The fallback may be a file, `@member_filled(@file("prompts/default.md"))`
(or `@asset(...)`), read the way a written one is; no other marker goes inside.
An empty field then takes the node's default if it has one, stays empty if the
field is optional, and otherwise the run is refused, naming the field. A wired
field cannot be `@member_filled` (`member-filled-wired`), nor a group's, loop's
or included file's own port (`member-filled-boundary`), nor a key that is not an
input (`member-filled-not-an-input`). For a group, loop or include port, mark the input of the
node inside that reads it.

You never mark the steps downstream: every node reading a member's copy or a
member's value follows it and runs in that member's runs. A trigger reading a
member's bridge, or a trigger with a `@member_filled` field, is itself per
member.

```weft
bridge = BaileyBridge {
  @per_member
}
receive = BaileyReceive
receive.endpointUrl = bridge.endpointUrl

openrouter = OpenRouterProvider {
  connection: @member_filled
  model: @member_filled("deepseek/deepseek-v4-flash-0731")
}
digest = Cron { cron: @member_filled("0 0 8 * * *") }
```

On a connection field, what you write decides whose key pays for a member's
calls, so ask the user which they want before writing it:

- `connection: {"id": "<connection id>"}`, not marked: every member runs on the
  user's key;
- `connection: @member_filled`: each member connects their own and pays, and a
  member with none is refused, naming the field;
- `connection: @member_filled({"id": "<connection id>"})`: a member who
  connected nothing runs on the user's connection, one who did on their own.

If a member should run on the user's key, the source has to say so: a member
can never pick the user's connection, or the runtime's shared key, for
themselves. The member door refuses the shared key, and a value saved for a member's connection
field has to be a connection that member owns, whoever saves it (`SetMemberValues`
included). If the user's connection is one made with the runtime's shared key,
the members it serves spend that key.

A member's value is held to the node's own rules when the member gives it
(refused on the spot, with the node's message), and again when a run for them
starts; a fallback is checked at compile time like any written value. A run
keeps the values it started with. When a member changes a value one of their
live triggers reads (its own field, or anything its setup goes through, like its
connection), weft sets that trigger up again before the change returns, so you never
reactivate it yourself, and events arriving meanwhile wait and go through on the new
value.

## Who a run is for

A run is for one member or for nobody, stamped by what started it:
`weft run --member <id>`; an event arriving through a member's own copy; a live
route called with a member token in `Weft-Member-Token`; a live route gated by
a connection called with `Weft-Member: <id>` (the author's own backend; an open
route refuses the header). Everything else runs for nobody. A run is refused
before it starts when it reaches something per member and is for nobody, when
the member has not filled a field the run needs (or filled it with a value the
node refuses), or when the member's infra copy is down. Each refusal names its fix. Hand that message to the user unchanged.

An event reaching a member's trigger before they filled a field it needs is
held, not dropped, and never retried on a timer: it runs when the member's
values change (or the trigger is activated again). `weft status` shows under
the trigger how many events wait and for which field, and `ctx.members().list()`
(the `ListMembers` node) hands the same to the program, so the user's website
can tell the member what is missing. An event held because the member's copy is
down retries on its own until the copy is up.

weft never starts a member's copy by itself: the program does, or the user
with `--member` on the infra verbs. A copy runs from images that `weft
activate` builds and records for every node marked `@per_member`, so
activate the program before it starts any copy; without that, the start
fails naming `weft activate`.

## The shape you build

A program with members needs two parts, and sometimes a third. Propose the
first two every time, and the third only when the user has a rule for it.

1. **The per-member part**: what you mark `@member_filled` (and `@per_member`
   on an infra node), and the steps that follow. When a member's trigger reads
   a value they fill (a schedule, a sheet), give that field a fallback if one
   makes sense. Then you can turn the trigger on the moment the member joins,
   before they have saved anything. Without a fallback,
   turning it on is refused, naming the field, until they have saved it.
   When a member saves a value, weft restarts their triggers that are on so
   they use it, and leaves the ones that are off alone.
2. **An admin route** the user's own backend calls, gated by a credential
   (`ApiKeyAuth` on the `Route`'s `auth`). It onboards a member:
   `MintMemberToken`, then `Reply` with the token so the request ends at once,
   then `StartMemberInfra` if the program has a per-member infra node (it fires
   `done` once the copy runs), then `ActivateMemberTriggers`. A copy can take
   minutes to come up, so the website shows the copy coming up by reading the
   `status` of that member's displays, and the request never waits for it. For
   the displays listing, go and read the `weft-consumers` skill. If a trigger
   needs a value with no fallback, the route's activate step belongs in a
   second call the website makes once the member has saved their settings. The
   same route offboards with `WipeMember`: only the user's backend knows when a
   member leaves, so leaving always comes through here.
3. **A timer**, only for a cleanup rule the user names and weft can see:
   copies in `failed` (`ListMemberCopies`), members over a spending limit
   (`MemberCosts`), or members with events waiting on a field they never filled
   (`ListMembers`), stopped with `StopMemberInfra` or removed with
   `WipeMember`. weft does not track how long a copy sat unused, so do not
   offer "idle copies" as a rule.

The member nodes are in the `members` package. A program that needs something
they do not cover calls the ctx directly in a node of its own:

| Call | What it does |
|---|---|
| `ctx.member()` | Who this run is for (the `CurrentMember` node fires `member` or `nobody`) |
| `ctx.infra("bridge").member(id).start()` / `.stop(spec, stop_self)` / `.terminate(spec, stop_self)` / `.status()` | One member's copy |
| `ctx.infra("bridge").copies()` | Every copy (the `ListMemberCopies` node) |
| `ctx.triggers().member(id).activate()` / `.deactivate(spec, stop_self)` | A member's triggers (`.only([..])` narrows) |
| `ctx.values().member(id).get()` / `.set(step, field, value)` / `.clear(step, field)` / `.apply()` / `.forget()` | What a member gave for the `@member_filled` fields: read (the `GetMemberValues` node), change in one go (the `SetMemberValues` node; `apply()` returns the triggers it set up again), or forget all |
| `ctx.connections().member(id).list()` / `.forget()` | A member's connections (forgetting takes the values naming them) |
| `ctx.members().list()` | Every member weft holds anything for, with counts and states and the events waiting on a field they have not filled (the `ListMembers` node) |
| `ctx.costs().member(id).service(s).since(t).list()` | What a member cost (the `MemberCosts` node, with the total) |
| `ctx.runs().member(id).clean(running, stop_self)` | A member's runs |
| `ctx.tokens().mint_for_member(id, expires_in)` / `.member(id).revoke()` | Member tokens |
| `ctx.storage(StorageScope::member())` | The run's member's own files |

`start()` returns once the copy is running, however long that takes (the run
parks between looks and holds no worker), so you can activate the member's
triggers right after it. If the copy never comes up, `start()` fails with the
reason, and so it does if somebody stops the copy while it waits (it never
starts it again behind a pause). Every reader of a copy's state (`weft status`,
`weft infra status`, `.status()`, `MemberInfraStatus`) gives one answer, and a
start or stop on its way reads `provisioning` or `stopping` at once.

If the run making a take-down call is among the runs it reaches,
`StopSelf::Keep` leaves it running and `StopSelf::Include` cancels it with the
rest.

weft keeps only a hash of a member token, so a replayed run cannot hand back
the value it minted: it mints a new value for the same token, and the old value
stops working.

`WipeMember` removes the member's triggers, the copies you name, their values,
connections, tokens, files and runs. `weft rm` of the project takes every
member's values, connections, tokens and files with it.

## Where the member fills in their values

A member fills in their values (connecting their accounts is one of them) on
their own settings page, never in the editor: the browser extension's **Your
settings** page with their member token, or the `MemberSettings` component on
the user's own site, copied into the frontend with `weft connect-lib`; for mounting it, go and read
the `weft-frontend` skill. It draws each field with its own control, and a list field
(their spreadsheets, their models) is read through their own connection. A member
is never offered the shared key. To try a member's path yourself before any
website exists: `weft connect --member <id> --node <step>` connects an account
as that member (`--door own --set-env key=ENV_VAR`) or picks one they have
(`--grant <id>`), and `weft member-values --member <id>` lists their fields,
with `--set step.field=value` / `--clear step.field` changing them. Each
command mints its own one-hour member token and revokes it when done, so the
member's own tokens are untouched.

## Checking your work

`weft infra status` lists each member's copy on its own line; `weft status`
lists every infra node the program declares under `infra:` (a `@per_member`
one with how many members have a copy, one never started as `not started`),
each member's copy under `member copies:`, and each member's triggers under
`triggers:`, with the events a trigger holds for a missing field. `weft executions --member <id>` lists a
member's runs. To try the per-member path yourself, start a copy
(`weft infra start --member test-1`), turn on its triggers
(`weft activate --member test-1`), and run with `--member test-1`.
