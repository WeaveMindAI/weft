---
name: weft-catalog
description: "Read before picking nodes for a job: the three reads that find a node and tell you how it wires, the metadata vocabulary you will meet there, the wiring shapes that recur (a connection, a provider, infrastructure, ports that come from a list), where a project's own nodes live, and what to do when nothing in the catalog fits."
---

# The node catalog

The catalog is everything under `nodes/`, and it is the truth about every node: you never quote a port, config key, or feature from memory, you read it. `nodes/base_catalog/` is the standard library, a managed copy that `weft catalog update` wipes and replaces, so you never edit it and never add to it. Everything else under `nodes/` is this project's own nodes and packages.

## Finding the node for a job

Three reads, in this order, all local and instant (they read `nodes/` on disk, no daemon).

1. **[the listing]**, `weft describe-nodes --list`, through grep. One line per node type: the name, its tags, and what it does. About a hundred and fifty lines with the standard catalog, so you narrow it:

   ```bash
   weft describe-nodes --list | grep -i postgres
   weft describe-nodes --list | grep -i 'image\|photo\|picture'
   ```

   Every search starts here, including the ones where you think you already know the answer. You grep the capability's own words first, then the words a node would use (a picture is `Image`, a webhook is a trigger, a database is `postgres`); the tags sit on the same line, so they match too. When a few tries matched nothing, you read the listing whole: one sentence per node, a few kilobytes, and that is what it is for. Hidden types never appear, in any form of this command.

2. **[the wiring view]**, `weft describe-nodes --node <Type> --compact`. One node's resolved ports, their types, what each accepts (a literal in the source, a wire from another node, or both), the widget kind, and the features that change how it wires. A few kilobytes per node, and it is the view that decides whether your wire compiles, so you read it for every node you are about to use, the familiar ones included. Three candidates is three calls, one per type. You never send a subagent to compare nodes: these calls are the comparison.

3. **[the metadata file]**, `nodes/**/metadata.json`, when the question reaches past wiring: a service recipe, an infra node's images, the exact wording of a validation rule. `--compact` strips `service`, `images`, `label`, `tags`, `icon` and `display`, so those live only in the file. Node folders are snake_case and the `"type"` inside is PascalCase (`exec_python` holds `"type": "ExecPython"`): you glob and read the one whose `"type"` matches.

Two reads you never run. `weft describe-nodes --compact` without `--node` prints every type's wiring as one JSON line, a quarter of a megabyte on the standard catalog, which grep cannot narrow; `weft describe-nodes` with no flags is the same thing with the authoring detail back in, the editor palette's read. And grepping the `nodes/` files to find a node matches only the words you guessed, so a node that does the job under another name stays invisible; those files are for authoring a node and for reading a service recipe once you have picked it. If you catch yourself grepping `nodes/` to find a node, or handing the search to a subagent, stop and write: "Wait. Ask the catalog." Then run [the listing].

**The catalog changes under you, so no page lists it.** Nodes are added, and an existing node gains a capability without announcing it: what a node does is its metadata's to say, and the three reads above are how you ask. A skill names a node where one makes a concept concrete, so treat every name you meet in prose as one example of a thing, never as the set of things that do it. A capability belongs to whichever nodes declare it today, which is a question only the catalog answers.

Two triggers, two different answers:

- If you catch yourself picking a node because a skill named it, stop and write: "Wait. The metadata decides." Then read [the wiring view] for that node and wire from what it says.
- If you catch yourself concluding a node CANNOT do something because no skill said it could, stop and write: "Wait. I have not asked the catalog." Then run [the listing] for the capability's own words, and [the wiring view] on each candidate: the listing finds nodes, and only the wiring view carries `features`, so a question about a flag is always a per-node call.

## When the catalog looks wrong

The copy under `nodes/base_catalog/` does not follow the installed weft on its own, so it can lag it. When a node misbehaves in a way its metadata should not allow, or a diagnostic names the catalog (an enrichment error, an unknown field, "a stale base_catalog copy"), you run `weft catalog update` and re-check before anything else. The update wipes and recopies only `base_catalog`, never the project's own nodes. Only wrongness that survives the update is a real finding.

## Reading a metadata file

Top-level keys: `type`, `label`, `description`, `tags`, `icon`, `color`, `inputs`, `outputs`, `types`, `requires_infra`, `images`, `publishes`, `service`, `portsFromConfig`, `features`, `display`, `validate`.

An input entry: `name`, `type`, `required`, `accepts`, `widget`, `default`, `label`, `placeholder`, `description`, and for `Access`-typed inputs `requiresScopes` / `requiresValues`. An output entry: `name`, `type`, `description`.

`accepts` lists the drivers the port takes: `literal` (a value written in the source, in the braces or on its own line, `@file`/`@asset` included) and `wire` (a value another node produces). Absent means both; `["wire"]` means only a real node fills it (an LLM's `provider`, `params`, `history`, `tools`; a consumer's `Access` handle). The list named in `portsFromConfig` and the access picker are compiler-read: an inline typed value only, never a wire, never a marker. Exactly one driver per port.

`widget` is the editor's control, an object naming its kind:
`"widget": { "kind": "textarea" }`. The kinds are `text`, `textarea`,
`code`, `number`, `checkbox`, `datetime`, `select`, `multiselect`,
`text_list`, `entry_list`, `password`, `access`, `file_drop`,
`remote_select`. Several carry their own settings inside that object, a
select's `options` (the accepted literals) among them.

`features`: `isTrigger` (starts executions from outside), `canAddInputPorts` / `canAddOutputPorts` (the source may add ports of its own, in that direction; a node carries either, both or neither, and this is where you read which), `optionalCustomInputs`, `customInputType`, `oneOfRequired` (skip the node when every port in a group arrives closed), `castPorts`, `liveEndpoint`, `showDebugPreview`.

`portsFromConfig`: ports generated at compile time from a config list. The metadata names the field and what an entry may be, so a node carrying the key tells you which of its ports you get by filling that list (a node that branches on cases derives one port per case this way).

`service`: the connection recipe (acquisition, auth, test URL, identity, event delivery). A node that declares a `service` block is an [access node], and the compiler synthesizes its runtime "no connection picked" rule from that block: no author writes it by hand. `"connection_optional": true` inside the block is the one opt-out, for a node that genuinely runs with nothing picked, whatever the reason (`CustomProvider`, whose endpoint may need no credential at all, is one). The user picks the connection on the node in the editor or with `weft connect` in the terminal (the `weft-connections` skill); what flows on wires is a sealed `Access` handle, never a key.

## What is on disk

`ls nodes/base_catalog/` is the map, and it is one command rather than a list
here that goes stale the day a package lands. The families are named for what
they do (`basic`, `logic`, `api`, `human`, `triggers`, `storage`) or for the
service they reach (`postgres`, `telegram`, `slack`), with `ai` holding one
folder per provider.

What each node takes and emits you read with the three reads above, never
from memory.

## Wiring patterns that recur

**An [access node]**: you declare it (`telegram = TelegramAccess`) and wire its `.access` output into every consumer's `account` input. Consumers check at run time that the connection is picked and has the required scopes or values.

**A provider node**: `provider: OpenRouterProvider { model: "z-ai/glm-5.3" }.provider` inline, or declared long and shared across several LLM nodes.

**Shared sampling**: `params: LlmParams { systemPrompt: @file("assets/prompts/x.md"), temperature: 0.75 }.params` into any LLM node's `params`.

**An [infra node]** (`requires_infra: true`): a node the runtime keeps a
container running for, with a disk that survives restarts. It emits whatever
its consumers need to reach it (a connection, an address), and you wire that
out exactly like an [access node]'s. `weft infra start` / `stop` run them,
`weft infra status` reports them, and a run cannot start while an [infra node]
it touches is not running.

Such a node also carries a **card in the graph**: readouts of its live state,
and a button for every state it can sit in, so a stuck one always has a way out
from the editor. The node's own description says which it offers. You never
tell the user to terminate the infra to get out of a state; a node with no
button for one of its states is the bug you report.

**Ports from a config list** (`portsFromConfig` in its metadata): the entries
you put in one config field decide which ports appear. The node's metadata
names that field and the kinds an entry may be; the compact view shows the
ports a given list produced. A form is the case you meet most, each field of
it an entry that becomes a port.

**Ports you declare on the arrow** (`canAddOutputPorts` in its metadata): a
node that answers with an opaque blob lets you name the pieces you want, and
each arrives typed on a port of its own. Reach for this whenever the next
node needs ONE field rather than the whole answer, because an opaque value
has no keys to read: without a declared port you would be writing out a
record type by hand just to pull one thing out of it. The node's metadata
says what the names mean to it (a column of the answer, a key of a parsed
reply), and the type you declare is what judges the value at run time.

## When the catalog is missing something

The gap is yours to fill by dispatch: you design the typed contract and send a `node-smith` specialist (the `weft-node-authoring` skill holds the protocol and the review checklist). An invented node type is a compile error, and so are invented ports on a real type, so you never fake a capability that way.

When a service has an API, its node is a `node-smith` dispatch, and the credential the service asks for (a developer app, an app review, a token) is the user's to connect, never a reason to refuse. You never point the user at Make, Zapier, Buffer, n8n, or any other automation service, and "weft cannot do this" is not a sentence you write: the only real gap is a capability no API can reach, named exactly, written up for the tracker with the `weft-gaps` skill (the templates, their field ids, and the pre-filled issue URL to hand the user). If you catch yourself reaching for an outside tool, stop and write: "Wait. Everything is expressible in weft." Then design the node the capability needs, or name the thing no API can reach.
