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

   After the node rows, the listing ends with the files a program can include, under the heading `Files a program can @include("<path>"):`. Each line is one `.weft` file under `nodes/`: the path you put in `@include`, already starting with `__catalog/`, and beside it the file's first line when that line is a `#` comment, so the same grep finds them. If one already does the job, read its `Group(...) -> (...)` line for the ports it takes and gives (a `*` port there is a metadata port: a value written in the file, which you wire out like an output), then pull it in as `<name> = @include("<that path>")` instead of rebuilding it by hand. For example, the computer package ships two: `__catalog/base_catalog/computer/agent.weft` is a whole agent that works a computer until its task is done, and `perform.weft` beside it runs one tool call the model asked for. If you write one to go with nodes of your own, put it in the package folder under `nodes/` that holds those nodes (never under `base_catalog/`), and make its first line a `#` comment saying what it does, because that line is all the listing shows. Until `src/main.weft` includes such a file, the validate run never checks it, so include it before you count on the validate run for it. A group that is just part of your program stays under `src/`, even when several modules use it.

   If you want a shipped file but your case differs from it, you can adapt it. When it fits, include it as it is: it was written to be general. When something about your case is specific, copy it into the project's own `src/` and change the copy. Leave the original under `nodes/base_catalog/` alone, because `weft catalog update` replaces that folder and every edit in it. The copy works where it lands: the files it includes and reads are spelled with `__catalog/`, which means the project's `nodes/` folder from any file in the project. Your program then includes the copy by its own path, like any group under `src/`.

2. **[the wiring view]**, `weft describe-nodes --node <Type> --compact`. One node's resolved ports, their types, what each accepts (a literal in the source, a wire from another node, or both), the widget kind, and the features that change how it wires. A few kilobytes per node, and it is the view that decides whether your wire compiles, so you read it for every node you are about to use, the familiar ones included. Three candidates is three calls, one per type. You never send a subagent to compare nodes: these calls are the comparison.

3. **[the metadata file]**, `nodes/**/metadata.json`, when the question reaches past wiring: a service recipe, an infra node's images, the exact wording of a validation rule. `--compact` strips `service`, `images`, `label`, `tags`, `icon`, `color`, `display`, `claimsRoute` and `accessApps`, so those live only in the file. Node folders are snake_case and the `"type"` inside is PascalCase (`exec_python` holds `"type": "ExecPython"`): you glob and read the one whose `"type"` matches.

Two reads you never run. `weft describe-nodes --compact` without `--node` prints every type's wiring as one JSON line, a quarter of a megabyte on the standard catalog, which grep cannot narrow; `weft describe-nodes` with no flags is the same thing with the authoring detail back in, the editor palette's read. And grepping the `nodes/` files to find a node matches only the words you guessed, so a node that does the job under another name stays invisible; those files are for authoring a node and for reading a service recipe once you have picked it. If you catch yourself grepping `nodes/` to find a node, or handing the search to a subagent, stop and write: "Wait. Ask the catalog." Then run [the listing].

**The catalog changes under you, so no page lists it.** Nodes are added, and an existing node gains a capability without announcing it: what a node does is its metadata's to say, and the three reads above are how you ask. A skill names a node where one makes a concept concrete, so treat every name you meet in prose as one example of a thing, never as the set of things that do it. A capability belongs to whichever nodes declare it today, which is a question only the catalog answers.

Two triggers, two different answers:

- If you catch yourself picking a node because a skill named it, stop and write: "Wait. The metadata decides." Then read [the wiring view] for that node and wire from what it says.
- If you catch yourself concluding a node CANNOT do something because no skill said it could, stop and write: "Wait. I have not asked the catalog." Then run [the listing] for the capability's own words, and [the wiring view] on each candidate: the listing finds nodes, and only the wiring view carries `features`, so a question about a flag is always a per-node call.

## When the catalog looks wrong

The copy under `nodes/base_catalog/` does not follow the installed weft on its own, so it can lag it. When a node misbehaves in a way its metadata should not allow, or a diagnostic names the catalog (an enrichment error, an unknown field, "a stale base_catalog copy"), you run `weft catalog update` and re-check before anything else. The update wipes and recopies only `base_catalog`, never the project's own nodes. Only wrongness that survives the update is a real finding.

A broken node or package never breaks the catalog: it is left out, and only a program that uses it fails, with `node 'X' failed to load: <reason>`. Package names, node types and service names are unique across every node folder; when two folders claim one, every claimant is left out and the error names all the folders, so you rename one. A folder with a `package.toml` and no node yet just waits, with a warning.

## Reading a metadata file

Top-level keys: `type`, `label`, `description`, `tags`, `icon`, `color`, `inputs`, `outputs`, `metadataPorts` (ports whose value is written in the file; a wire leaving one carries that value into the input it reaches, as if typed there, before any run), `types`, `requiresInfra`, `images`, `publishes`, `service`, `portsFromConfig`, `features`, `display`, `validate`, `firesWith`, `claimsRoute`, `accessApps`. Every key weft defines is camelCase; a snake_case one (`requires_infra`) fails the node's load, and the error names the camelCase spelling. A value from one of weft's own fixed lists stays snake_case (a widget's `kind`, such as `text_list`). A select's `options` belong to the node and keep its spelling (`Africa/Abidjan`).

An input entry: `name`, `type`, `required`, `accepts`, `widget`, `default`, `label`, `placeholder`, `description`, and for `Access`-typed inputs `requiresScopes` and `requiresValues`. An input without `required` is optional: unwired, the node runs without it; wired, the node waits for it and skips if the wire closes. A program can change this where it uses the node (`x: T` requires the input, `x?: T` runs without it even when wired), but it can never make a `required` input optional; the `weft-language` skill has the table. An output entry: `name`, `type`, `description`, `narrowable` (the program may declare a narrower type on it, `-> (response: Verdict)`), and on an [infra node] `baked` (what that means is under the infra node shape below).

`accepts` lists the drivers the port takes: `literal` (a value written in the source, in the braces or on its own line, `@file`/`@asset` included) and `wire` (a value another node produces). Absent means both; `["wire"]` means only a real node fills it (an LLM's `provider`, `params`, `historyFile`, `tools`; a consumer's `Access` handle). The list named in `portsFromConfig` is compiler-read: an inline typed value only, never a wire, never a marker. The access picker holds nothing in the source at all: its connection is picked on the install (`weft connect`). Exactly one driver per port.

`widget` is the editor's control, an object naming its kind:
`"widget": { "kind": "textarea" }`. The kinds are `text`, `textarea`,
`code`, `number`, `checkbox`, `datetime`, `select`, `multiselect`,
`text_list`, `entry_list`, `password`, `access`, `file_drop`,
`remote_select`. Several carry their own settings inside that object, a
select's `options` (the accepted literals) among them.

`features`: `isTrigger` (starts runs from outside), `canAddInputPorts` / `canAddOutputPorts` (the source may add ports of its own, in that direction; a node carries either, both or neither, and this is where you read which), `join` (the node meets several branches and goes on with whichever arrived, like `FirstInOrder`: the inputs you add are optional, it runs once everything it waits on arrived or closed, closes its output itself when nothing arrived, and needs no `_should_flow`), `customInputType`, `customOutputType` (the type of the outputs you add), `pairsAddedPorts` (each input you add brings an output of the same name, made for you, and one nothing reads fails the build, as on `Dispatch`), `oneOfRequired` (skip the node when every port in a group arrives closed), `castPorts`, `liveEndpoint`, `showDebugPreview`, `addedPortsAreDataKeys` (the ports you add are named after outside data, such as a query's parameters or a path's captures, and keep that data's spelling: the compiler's `not-camel-case` warning skips them).

`portsFromConfig`: ports generated at compile time from a config list. The metadata names the field and what an entry may be, so a node carrying the key tells you which of its ports you get by filling that list (a node that branches on cases derives one port per case this way).

`service`: the connection recipe (acquisition, auth, test URL, identity, event delivery). A node that declares a `service` block is an [access node], and the compiler synthesizes its runtime "no connection picked" rule from that block: no author writes it by hand. `"connectionOptional": true` inside the block is the one opt-out, for a node that genuinely runs with nothing picked, whatever the reason (`CustomProvider`, whose endpoint may need no credential at all, is one). The user picks the connection on the node in the editor or with `weft connect` in the terminal (the `weft-connections` skill); what flows on wires is a sealed `Access` handle, never a key.

## What is on disk

`ls nodes/base_catalog/` is the map, and it is one command rather than a list
here that goes stale the day a package lands. The families are named for what
they do (`basic`, `logic`, `api`, `human`, `triggers`, `storage`) or for the
service they reach (`postgres`, `telegram`, `slack`), with `ai` holding one
folder per provider.

What each node takes and emits you read with the three reads above, never
from memory.

## Wiring patterns that recur

**An [access node]**: you declare it (`telegram = TelegramAccess`) and wire its `.access` output into each consumer's `account` input (`access` on a model node and `LlmModerate`, and `auth` on a `Route` or `Socket` for an auth node such as `ApiKeyAuth`). Consumers check at run time that the connection is picked and has the required scopes or values.

**A provider**: two nodes. The access node holds the connection, once per service (`router = OpenRouterAccess`), and a model node turns it into a `provider` for one model: `provider: OpenRouterModel { access: router.access, model: "z-ai/glm-5.3" }.provider` inline, or declared long and shared across several LLM nodes. One access node serves however many model nodes.

**Shared sampling**: `params: LlmParams { systemPrompt: @file("assets/prompts/x.md"), temperature: 0.75 }.params` into any LLM node's `params`.

**An [infra node]** (`requiresInfra: true`): a node the runtime keeps a
container running for, with a disk that survives restarts. It emits whatever
its consumers need to reach it (a connection, an address), and you wire that
out exactly like an [access node]'s. `weft infra start` / `stop` run them,
`weft infra status` reports them, and a run cannot start while an [infra node] it touches is not running. An output marked `"baked": true` (the Postgres node's `access`, the WhatsApp bridge's `bridge`) is made when the infra is applied and saved, and the infra itself tells weft when a value in it changes (a password reset): a run that reads nothing else from the node uses the saved value instead of running it, and `weft infra rebake <node>` makes it again by hand (`weft bake` is unrelated: it prepares a trigger's settings).

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

**If the next node needs ONE field out of an opaque answer** (a query's row,
a model's parsed JSON), declare the shape on the output the node already
has: a port marked `narrowable` takes a record type on the arrow (`-> (row:
Card)` on `PostgresExecuteQuery`, `-> (response: Verdict)` on
`LlmInferenceJson`), and you read the field off it (`look.row.title`). The
type you declare is what judges the value at run time.

**Ports you declare on the arrow** (`canAddOutputPorts` in its metadata) are
for the few nodes whose outputs are named by your program: `ExecPython` (what
the script returns), `ElevenLabsRunTemplate` (the template's ids) and `Route`
(its path captures). The node's metadata says what the names mean to it.

## When the catalog is missing something

The gap is yours to fill by dispatch: you design the typed contract and send a `node-smith` specialist (the `weft-node-authoring` skill holds the protocol and the review checklist). An invented node type is a compile error, and so are invented ports on a real type, so you never fake a capability that way.

A catalog node that looks like what you need but differs in one thing (a permission its access node does not list, a parameter it lacks, a behavior slightly different from the one your program wants) is a starting point, and you never treat the difference as a limit of weft. You copy its folder from `nodes/base_catalog/` into the project's own `nodes/`, give the copy a type name of its own, and tweak the copy: a `node-smith` dispatch with the catalog node named as where to start. Nobody has to be asked and no weft release has to add it first. For an access node, a missing permission is one more entry in the `permissions` list of its `service` block (an `id`, a `label` and a `description`), and the user ticks it on the "own" door when connecting. If you catch yourself telling the user a catalog node cannot do the thing, stop and write: "Wait. The node is an example. I copy it and tweak my own version." Then dispatch the copy.

When a service has an API, its node is a `node-smith` dispatch, and the credential the service asks for (a developer app, an app review, a token) is the user's to connect, never a reason to refuse. You never point the user at Make, Zapier, Buffer, n8n, or any other automation service, and "weft cannot do this" is not a sentence you write: the only real gap is a capability no API can reach, named exactly, written up for the tracker with the `weft-gaps` skill (the templates, their field ids, and the pre-filled issue URL to hand the user). If you catch yourself reaching for an outside tool, stop and write: "Wait. Everything is expressible in weft." Then design the node the capability needs, or name the thing no API can reach.
