---
name: weft-catalog
description: Finding and reading nodes in this project's catalog (nodes/). Read before picking nodes for a job: how to inspect metadata.json, the exposure and widget vocabulary, access/provider/infra/trigger node families, form field kinds, Switch cases, and where custom nodes go.
---

# The node catalog

The project's complete node vocabulary is on disk under `nodes/`. The catalog
is the truth: never quote a port, config key, or feature from memory, read
the node's interface first (`weft describe-nodes --node <Type> --compact`,
or the `metadata.json` itself when you need the whole file). This page is
how to read it, plus the map.

`nodes/base_catalog/` is the standard library (a managed copy, wiped and
replaced by `weft catalog update`; never edit it, never add to it). Anything
else under `nodes/` is this project's own nodes and packages.

## Discovering nodes

- `weft describe-nodes --list` prints one line per type (type, tags, one-line
  description). That is the cheap first sweep; `--compact` is the next read
  once you have candidates.
- `weft describe-nodes --compact` prints the project catalog as the
  wiring view: resolved ports, exposure, types, features, and config-derived
  port shapes, with labels, icons, connect recipes, and other authoring
  detail stripped (hidden types omitted, as in every form of this command).
  This is the form to sweep when you are picking nodes; it costs a fraction
  of the tokens of the full files.
- `weft describe-nodes --node <Type> --compact` prints one node's wiring
  view. This is the default read before wiring a node you know.
- `weft describe-nodes` (no flags) prints the full resolved metadata as
  JSON; `weft describe-nodes --node <Type>` prints one node's full
  metadata, pretty. These are the reads for authoring or debugging a
  node, and what the editor's palette consumes.
- To read one node's file directly: `Glob` for `nodes/**/metadata.json`,
  then read the file whose `"type"` matches. Node folders are snake_case
  (`exec_python` holds `"type": "ExecPython"`).
- The `catalog-scout` subagent does wide searches for you and reports exact
  specs; use it when several candidate nodes need comparing.

## When the catalog looks wrong

The stdlib under `nodes/base_catalog/` is a managed copy, and it can lag
the installed weft: the stdlib moves, the copy in the project does not
follow on its own. When a node misbehaves in a way its metadata should not
allow, or a diagnostic names the catalog (an enrichment error, an unknown
field, "a stale base_catalog copy"), the first move is `weft catalog
update`, then re-check. The update wipes and recopies only `base_catalog`,
never the project's own nodes. Only if the wrongness survives the update is
it a real finding.

## Reading a metadata.json

Top-level keys: `type`, `label`, `description`, `tags`, `icon`, `color`,
`inputs`, `outputs`, `types`, `requires_infra`, `images`, `publishes`,
`service`, `portsFromConfig`, `features`, `display`, `validate`.

An input entry: `name`, `type`, `required`, `exposure`, `widget`, `default`,
`label`, `placeholder`, `description`, and for `Access`-typed inputs
`requiresScopes` / `requiresValues`. An output entry: `name`, `type`,
`required`, `description`.

`exposure` decides how the port is filled in source: `all` (braces literal,
assignment literal, wire), `assignment` (line literal + wire; the default for
file types), `config` (design-time setting, takes no wire), `wire` (only a
wire, e.g. an LLM's `provider`, `params`, `history`, `tools`). Exactly one
driver per port.

`widget` is the editor's control (`text`, `textarea`, `code`, `number`,
`checkbox`, `datetime`, `select`, `multiselect`, `text_list`, `entry_list`,
`password`, `access`, `file_drop`, `remote_select`). A select widget's `options` are the accepted literals.

`features`: `isTrigger` (starts executions from outside), `isOutputDefault`
(firing is the deliverable; overridable with `_is_output`), `canAddInputPorts`
/ `canAddOutputPorts` (source may add ports, e.g. `ExecPython` both,
`LlmInference` outputs, `FirstInOrder` inputs), `optionalCustomInputs`,
`customInputType`, `oneOfRequired` (skip the node when every port in a group
arrives closed), `castPorts`, `liveEndpoint`, `showDebugPreview`.

`portsFromConfig`: ports generated at compile time from a config list.
`Switch` derives its case ports this way; the human nodes derive their form
ports.

`service`: access nodes only, the connection recipe (acquisition, auth, test
URL, identity, event delivery). Declaring a `service` block is what makes a
node an access node, and the compiler synthesizes the runtime "no connection
picked" rule from it automatically: no author writes that rule by hand.
`"connection_optional": true` inside the service block is the one opt-out,
for a node that can run with no connection picked (an endpoint that may be
public, like `CustomProvider`). The user picks the connection on the node in the
editor or with `weft connect` in the terminal (the `weft-connections`
skill), and what flows on wires is a sealed `Access` handle, never a key.

## Node families

Orientation, not inventory. The inventory is on disk and grows.

- **basic**: `Text` (literal string), `Debug` (inspect a value, isOutputDefault),
  `Cast`, `Range` (number generator for loops), `ExecPython` (author-declared
  ports, Python body), `Format` (a `template` with `{{name}}` holes, filled
  from the input ports you declare inline, emits `text`), and the two timers
  `Wait` (`seconds`) and `WaitUntil` (`when`, a date and time). A timer parks
  the run on a listener clock at no cost, then emits `wokeAt` and passes its
  optional `value` through, so it sits in the middle of a chain: never sleep
  inside `ExecPython`, which holds a worker for the whole wait.
- **ai/llm**: `LlmInference` (buffered completion; `provider` required and
  wire-only; `params`, `history`, `media`, `tools`, `toolCalls`;
  `parseJson: true` plus added output ports extracts JSON keys),
  `LlmStream`, `ChatHistoryAppend`, `LlmParams`, `LlmTool`, `LlmEmbed`,
  `LlmRerank`, `LlmModerate`, and the provider nodes `OpenRouterProvider`,
  `AnthropicProvider`, `OpenAIProvider`, `CustomProvider` (each takes a
  `connection` and emits `.provider`).
- **ai/**: fal (image/video generation and edits), ElevenLabs (voice, music,
  transcription, agents), Mistral (document parsing).
- **human**: `HumanQuery` (pauses for a person to answer a form),
  `HumanTrigger` (a person starts the run). Ports come from the `fields`
  entry list.
- **logic**: `Switch` (cases: `equals`, `in`, `contains`, `matches`,
  `gt`/`gte`/`lt`/`lte`, `between`, `otherwise`; each case names the port it
  opens, `otherwise` last and unique), `FirstInOrder` (its written input
  order decides which branch wins), `TagRun` and `StopTagged` (stopping
  older runs: every input wired onto either node is a tag, `TagRun` puts
  the tags on this run, `StopTagged` stops every older run carrying one,
  `includeSelf: true` stops this run too). Reach for the pair when a new
  event makes work in flight pointless; the wiring is in the
  `weft-language` skill.
- **live**: `ApiEndpoint` (HTTP), `LiveSocket` (WebSocket). Triggers; a fresh
  execution per request or connection.
- **triggers**: `Cron` (`cron` expression, SIX fields with seconds first:
  `0 */5 * * * *` is every five minutes; a five-field expression is refused
  at activation. `timezone` is an IANA name, `UTC` unless set: with
  `Europe/Paris`, `0 0 9 * * *` is nine in Paris all year).
- **postgres**: `PostgresDatabase` (infra: the project's own Postgres,
  emits `.access`), `PostgresAccess` (external one), `PostgresExecuteQuery`
  (its parameters are its own input ports, declared inline and read by name:
  `PostgresExecuteQuery(user_id: String) { query: "... WHERE id = $user_id" }`;
  several statements run as a script, which takes no parameters),
  `PostgresInsertRow`,
  `PostgresUpdateRows`.
- **bailey** (WhatsApp), **telegram**, **slack**, **email**, **google**
  (Drive, Sheets, Docs, Gmail, Calendar), **notion**, **airtable**, **s3**,
  **web** (FetchPage, CrawlSite, WebSearch), **rss**, **storage**
  (FetchToStorage, KeepFile, MediaDisplay, DownloadLink). `FetchToStorage`
  with `scope: "project"` and an `identity` fetches a thing once per
  project: a second fetch of the same identity is the same file, no
  download.
- **http**: `HttpRequest` (`method` is config, `url`/`body`/`headers` wireable).

## Wiring patterns that recur

**Access nodes**: declare the access node (`telegram = TelegramAccess`), wire
its `.access` output into every consumer's `account` input. Consumers
validate at run time that the connection is picked and has the required
scopes or values.

**Provider nodes**: `provider: OpenRouterProvider { model: "z-ai/glm-5.3" }.provider`
inline, or declared long and shared across several LLM nodes.

**Shared sampling**: `params: LlmParams { systemPrompt: @file("prompts/x.md"), temperature: 0.75 }.params`
into any LLM node's `params`.

**Infra nodes** (`requires_infra: true`): `BaileyBridge` brings up the
WhatsApp bridge and emits `endpointUrl` (wire it into every Bailey consumer);
`PostgresDatabase` emits `access`. The runtime starts their containers; start
or stop them with `weft infra start` / `weft infra status`. A run cannot
start while an infra node it touches is not running.

**Forms** (`fields` entry list on `HumanQuery` / `HumanTrigger`): kinds and
their ports: `display` (input `{key}`, read-only), `display_image` (Image
input), `text_input` / `textarea` (output `{key}: String`), `select` /
`multi_select` (output, `options` required), `select_input` /
`multi_select_input` (input `List[String]`, output String or List),
`editable_text_input` / `editable_textarea` (input and output `{key}`),
`approve_reject` (outputs `{key}_approved` and `{key}_rejected`, exactly one
speaks; wire them into `_should_flow` branches).

## When the catalog is missing something

The gap is Tangle's to fill by dispatch: design the typed contract and send a `node-smith` specialist (the `weft-node-authoring` skill holds the protocol and the review checklist). Never fake a capability with an invented node type: an unknown type is a compile error, and inventing ports on a real type is too.
