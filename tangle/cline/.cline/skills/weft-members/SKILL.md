---
name: weft-members
description: "Read when a program serves several PEOPLE, each with their own account, number, bot, spreadsheet or schedule (a WhatsApp assistant for many customers, a Slack bot per workspace, a report per client): the table that says which person owns which instances, one instance per person as the simple case, people who own several instances, the admin route that onboards a person, their settings page, and their connections. Read `weft-instances` first: it has the marks, the route shape and the lifecycle this builds on."
---

# Programs for several people

A [member] is one person the program serves. weft knows nothing about
people: it knows [instance]s, separate running copies of part of the program
under their own ids (the `weft-instances` skill). Which person owns which
instances is the program's own data, kept in a table in its own database.
Everything a person does reaches weft through one of their instances.

You never build one project per person, and you never ask each person for a
key in a form field: their accounts are `@instance_filled` connections they
connect on their own settings page.

## Keeping who owns what

The program keeps one table, in the Postgres the program and its frontend
share (the `weft-frontend` skill has the one shared database):

```sql
create table if not exists member_instances (
  user_id     text not null,
  instance_id text not null primary key,
  label       text,
  created_at  timestamptz not null default now(),
  last_used   timestamptz not null default now()
);
create index if not exists member_instances_user on member_instances (user_id);
```

`last_used` is the column the idle cleanup reads (under "Every instance needs
an end" in `weft-instances`); every run for an instance updates it.

**One instance per person** is the simple case and the default: the instance
id is the person's user id (their BetterAuth id on a site you build), so the
table holds one row per person and the lookup is trivial. Use it whenever a
person needs exactly one copy (one WhatsApp number, one workspace).

**Several instances per person** (a session per chat, a bot per workspace
they run, a sandbox per job): the program mints a fresh id per instance
(`<user id>-<random>` reads well in `weft status`), writes a row, and:

- listing a person's instances is a query on `user_id`, run from a gated
  route the frontend's server calls after reading the session;
- opening one is a check that the row belongs to that person, then
  `MintInstanceToken` for that one instance id, and the page uses that token;
- removing one is the same check, then `WipeInstance` and deleting the row.

The check against the table happens on every call that names an instance: a
person can only ever reach an instance whose row carries their user id.

## The shape you build

Propose these parts every time, before any node is written. The routes are
shared (never per instance), as `weft-instances` explains: a route sits
outside every group that receives a per-instance value, and sends its work
into that group through the group's inputs.

1. **The per-instance part**: what you mark `@instance_filled` (and
   `@per_instance` on an infra node), and the steps that follow. When an
   instance's trigger reads a value filled per instance (a schedule, a
   sheet), give that field a fallback if one makes sense, so the trigger can
   be turned on the moment the instance is created.
2. **An admin route** the frontend's server calls, gated by `ApiKeyAuth` on
   the `Route`'s `auth`, its key only in that server's environment. It
   onboards a person: writes the row, `MintInstanceToken`, `Reply` with the
   token so the request ends at once, then `StartInstanceInfra` if there is a
   per-instance infra node, then `ActivateInstanceTriggers`. The same route
   (or siblings on the same gate) lists a person's instances, mints a token
   for the one they open, and removes one with `WipeInstance`.
3. **A settings page** where each person fills in what the program asks of
   their instance: their connections, their spreadsheet, their model, their
   schedule. It is `InstanceSettings` on the frontend with that instance's
   token (the `weft-frontend` skill mounts it), or the browser extension's
   **Your settings** page. If a trigger needs a value with no fallback, the
   page calls the admin route again to turn the triggers on once the values
   are saved.
4. **The cleanup cron** from `weft-instances`, with the idle limit the user
   chose.

A person's own runs go through the program's shared routes: the frontend's
server calls a gated route with `Weft-Instance: <instance id>` after checking
the table, or a page calls a route with that instance's token in
`Weft-Instance-Token`.

## Connections per person

Each person connects their own accounts when the field is written
`connection: @instance_filled`: that person's calls then run on their key and
their bill. A field left unmarked runs every person on the connection the
user picked on the install, on the user's bill. Ask the user which they want
for each paid service before you write it.

To try a person's path before any website exists, give a test instance the
values a person would: `weft connect --instance alice --node <step>` and
`weft instance-values --instance alice --set step.field=value`, then run with
`--instance alice`.

## Billing people

If the site bills its people, a gated route reads `InstanceCosts` for each of
that person's instances (from the table) and the frontend shows or charges
the sum. weft only records the cost.
