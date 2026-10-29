---
name: weft-editor
description: "Read when telling the user where to click or what they are looking at in VS Code: the toolbar, running and turning triggers and infra on or off from a selection, the state pill on each trigger and infra node, the action bar, the palette, context menus, editing gestures, groups and loops in the graph, run and replay, the inspector, the connect flow, the problems panel. Labels are verbatim from the extension, except an infra node's live card, whose buttons the running container names."
---

# The editor surface

Everything below is the VS Code extension as the user sees it, and every
quoted label is verbatim. When you tell the user where to click, you quote
the label from this file. If you catch yourself naming a button, menu
entry or toast that is not quoted here, stop and write: "Wait. Labels are
verbatim." Then quote the one that is, or tell the user the extension has
no such control.

`.weft` files open as the graph by default (a stray text tab is closed
automatically); the "Source" button is the text view. The extension talks
to the dispatcher at `http://127.0.0.1:14111` (setting `weft.dispatcherUrl`).

## Where things live

- **Activity bar, icon "Weft"**: two sidebar views.
  - **Projects**: one row per `.weft` file in the workspace (label = folder
    name). Click to pin and open its graph; inline buttons "Open in Editor"
    and "Run Project"; title-bar "Refresh".
  - **Runs**: runs of the pinned project, newest first (paged, "Load
    more (N more)" at the bottom). Row = status icon, entry node, time;
    tooltip = the run id. Inline "View in Graph" and "Delete"; title-bar
    "Refresh Runs" and "Clear All Runs". The title-bar "Flat list / by
    version" shows the runs under the versions they ran on, and there "Load
    this version", on a version or on a run, does what `weft branch` does.
- **The graph panel**, titled "Weft Graph: <project folder name>".
- **The Problems panel** carries the compiler's live diagnostics.

## Reading the graph

- **A node** is a white card: accent bar in the type's color, header with
  the status glyph (✓ completed, ● running, ◉ waiting, ✕ failed, ■
  cancelled, ⊘ skipped) and the type name, body with the label and its
  ports. Left rail: input ports. Right rail: output ports. Each port is a
  dot colored by its type (String gray, Number blue, Boolean rose, files
  gold/purple/green/brown, List teal, Dict purple, Access teal, Bus amber);
  a required input carries a `*`.
- **The small amber arrow top-left of every box** is `_should_flow`, the
  port that decides whether it runs: filled when something answers it,
  hollow when nothing does.
- **Wires** are arrows colored by the source port's type.
- **Groups** are large boxes ("GROUP" header) holding child nodes;
  collapsed they become a chip with the label, an expand button, and the
  description line. A group grows a "Config" strip under its header once
  the source gives one of its ports a value (`g.tone = "formal"`): the
  field edits that line, and the strip is gone when no port has one.
  **Loops** render violet with a rotate icon, their settings in that strip
  always, followed by any port the source gave a value, implicit `index`
  and `done` ports on the rails, carry ports marked ↻. **@include blocks**
  are violet with the filename; their body fields hold the values written
  on their ports (`keep.n = 7`) and edit those lines in place (no `{ }`
  toggle: an alias has no braces); their "Open" button navigates into
  that file and the toolbar grows a "Return · <file>" button.
- **Access nodes** (TelegramAccess and friends) show a "Connect
  <Service>..." button in their body; one with a required, unpicked
  connection pins open (expanded, collapse disabled, "Pick a connection
  first") until a connection is picked. **Trigger nodes** show their display
  (the address a caller sends to and how the door checks them, or a
  timer's schedule; never a key). **Infra nodes** carry a status
  pill (running, stopped, failed...) and a body feed of what the service
  prints.
- **Debug nodes** render their latest value inline; media nodes render the
  image, an audio/video player, or a download card.
- **Status glows**: amber running, cyan waiting for input, green completed
  or skipped, red failed.

## Toolbar and action bar

Top-left floating toolbar: "Return · <file>" (inside an @include), the
[follow toggle], "N new run(s) · Follow" (runs that started while locked
or off; click to show the newest and follow again), and **"Source"**
(opens the text beside the graph; click again to focus it).

The [follow toggle] has three parts, each explained on hover; only the
active one shows its word:
- **"Following · <run-id>"** (eye icon): every run that starts takes over
  the graph. The default.
- **"Locked · <run-id>"** (lock icon): the graph stays on this run; runs
  that start are counted, not shown. Greyed while no run is on screen.
- **"Off"** (crossed eye): no run on the graph, just the program.

Run, Turn on, or Start in the editor switches to
Following. A run started from the terminal (including every `weft run`
you issue) never changes the mode: if the user left the graph Locked or
Off, your run lands in the "N new run(s)" count, not on their screen, so
tell them to click it (or switch to "Following") to watch it. Each project
remembers whether it was left Following.

Top-right: **"Simplified view"** toggle. On: square, read-only nodes (toast
"Switch to the builder view to edit the graph"), one dot per side, for
reading and showing. Builder and simplified keep separate saved positions.

**Everything you act on starts with a selection.** Select nodes (Ctrl+A
selects all; the palette's "Select All Triggers" takes just the triggers)
and a bar appears at the bottom center showing only what that selection
allows, in big buttons: "RUN 1 TARGET" or "RUN N TARGETS", then under
Triggers "Turn off", "Turn on", "Resync", then under Infra "Start", "Stop",
"Terminate", "Upgrade", and "Options". A count, "(N)", follows a label only
when that button covers more than one selected node. [the pre-flight] runs
before a run or a lifecycle verb is sent: a runtime validation through the
warm parse-server, where an unpicked connection and cousins land on the
bar's error banner (the complete list, each entry clickable to its file and
line) and the verb is not sent.

- **RUN N TARGETS** runs the selected nodes as targets (`weft run
  --target`) with what they need. A selected trigger or infra node offers no Run, only its
  own buttons; in a mixed selection Run runs the other nodes. Greyed,
  its tooltip gives weft's reason: the refusal `weft run --check`
  gives, or the infra the run reads that is not running (start it first); with
  several nodes selected it names each blocked node and its reason. Ctrl+Enter with nothing selected says "Select the nodes to run
  first: the run takes them and what they need."; with triggers or infra alone it
  says Run leaves them out. The sidebar's "Run Project" runs every root
  of the program. The editor has no gesture for `--from`, `--group`,
  `--fire` or a saved example: those are CLI.
- **"Options"** ("How triggers go down and come back") holds the same
  three questions as the CLI, prefilled with the defaults: "What arrives
  while it is down" (**Hold it**, **Hold it, hidden**, **Refuse it**, then
  refuse after some minutes), "What already waits" (**Keep it**, **Drop
  it**) and "The runs going now" (**Wait for them**, **Cancel them**), plus,
  for turning on, "What they held while they were down" (**Run it**,
  **Drop it**). Stopping infra lists what "Goes down with it".
- **The instance picker** ("instance: shared") says whose copies the bar
  acts on: "shared (the program's own)", "all instances (N)", an instance
  id, or a new one typed in. Acting on a per-instance node needs an
  instance chosen here.
- Terminate asks first, in a VS Code dialog: "Terminate <infra>? Every
  resource is deleted, stored data included unless the node keeps it."

**Each trigger and infra node shows its state** in a pill. A trigger reads
`on`, `off`, `behind` (on, firing an older version: Resync brings it up to
date), `turning on...`, `held N` (it holds that much for when it is back) or
`held back`; a per-instance one reads `2 of 5 on`. An infra node reads
`running`, `starting...`, `stopping...`, `stopped`, `behind`, `flaky` or
`failed` (`N of M running` for a per-instance one). Hovering the pill says
why: "Taken down with its infra: it comes back once that infra runs
again.", "Taken down by the health check: ...", "Held back: <reason>", and
for infra what taking it down takes with it.

The bottom-center bar is the status bar and the selection bar in one. With
nothing selected it shows only what is happening now (and the eye toggles,
which show only the trigger or the infrastructure subgraph): the run you
are following with its "Stop" button and node count, or the verb at work
("Building...", "Resyncing...", "Turning triggers on...", "Waiting for the
runs still going..."), with "(Cancel)" when it can be called off. A resync
that stopped with triggers down shows "Resync stopped" with "Resync" (take
it up again where it stopped) and "Give up" (leave the triggers it took
down off). Select nodes and their buttons join it, after a divider.

A verb that failed leaves a red banner above the bar; clicking it opens the
full diagnostics.

## The in-graph palette (Ctrl+P or Cmd+P)

"Search nodes and actions...". The **Actions** section: Undo (Ctrl+Z),
Redo, Duplicate Selected, Delete Selected, Select All Nodes, Fit View, Auto
Organize Layout. The **Nodes** section: every type in the project's catalog
with a preview panel (description, input chips, output chips, tags).

## Editing in the builder view

Every GUI edit is a structured edit applied through the compiler, so the
text and the picture cannot drift, and Ctrl+Z undoes in either view.

The graph never writes over a text edit. When the text changes outside the
graph (the user typing, an AI editing the file), the graph drops the undo
steps it held for the old text, and Ctrl+Z in the graph then says "the text
changed since the graph edits, so they can no longer be undone or redone
from the graph; undo them in the text editor instead". And if the file on
disk is newer than the open one, weft refuses to save over it: VS Code
offers to compare or overwrite, and the person picks.
Simplified view refuses structure edits with "Simplified view is
read-only (you can still move, expand, and collapse).".

- **Add a node**: palette, or right-click the canvas "Add Node...
  (Ctrl+P)".
- **Wire**: drag from an output dot to an input dot. Dropping a wire on
  empty space opens the palette and completes the wire in one undo step. A
  port already filled by a literal refuses the wire ("'x' is driven by a
  config assignment; unset it first to drive it with an edge.").
- **Read a key off a wire**: right-click a wire whose value is a record and
  pick a field, one level at a time ("Up one level", "Read the whole value"
  to go back). The wire turns dotted with the path at its end; in source it
  is `t.n = s.out.profile.wpm`. A `JsonDict` or scalar value offers nothing
  and says to declare the shape or Cast first.
- **Metadata ports**: not drawn, and nor are metadata wires: both are
  written in the code. An input a metadata wire fills shows the value,
  read-only, with a link to where it comes from; a group's metadata ports
  show in its Config strip (a fully written value is editable there, a
  gathered one is read-only with each port it names as a link). The graph
  only lets a person follow them.
- **Edit a node's settings**: expand it and edit the fields inline: text,
  selects, checkboxes, code editors, entry lists, file pickers, connection
  pickers. A port-driven field carries a `{ }` / `=` chip toggling whether
  it is written inside the braces or as its own line. An `@file(...)`
  field shows a file chip; editing it writes the referenced file (the
  source keeps one line). The lock chip turns it into read-only `@asset`.
- **Ports**: hover a custom port for its remove ×; "+ input" / "+ output"
  where the type allows. Right-click a port: "Make optional" / "Make
  required", "Type: <type>" (editable), "Remove port". On loop ports the
  right-click also shows and changes the role (broadcast, iter, gather,
  carry).
- **Nodes**: right-click for "Duplicate (Ctrl+D)", "Delete (Del)" and
  "Tags…". On a node each instance can
  have its own copy of (an infra node's container, an access node's
  connection), "One per instance" or "Share across instances". Stopping or
  terminating infra is done by selecting the node (above).
- **An infra node's card**: while its container runs, the node's body shows
  readouts of its live state and a button for each way out of a state it can
  sit in. Those come from the RUNNING container, not from this file, so their
  labels are the node's to name and you read them off the card in front of
  you rather than quoting them from here. A secret it is showing you once
  renders masked, with buttons to reveal and to copy.
- **Rename**: double-click a node's label or a group's header.
- **Groups and loops**: the expand/collapse toggle on the box; "Auto
  Organize Layout" in the palette reflows; positions persist under
  `layouts/` (never in the source).

Keyboard: Ctrl+P palette, **Ctrl+Enter run**, Ctrl+Z/Y undo/redo, Ctrl+A
select, Ctrl+D duplicate, Del delete, Esc closes the palette and drops a
wire you are dragging. Zoom is Ctrl/Cmd+wheel (5% to 200%); the
bottom-left controls carry zoom, fit, and lock.

## Running and watching

Run in the graph runs the selected nodes and what they need; the
sidebar's "Run Project" runs the pinned project's ordinary roots. Triggers
need an explicit fire or supplied outputs.
The run lands in the Runs list. Clicking any node opens [the inspector]: status,
duration, cost ("$0.0123 (own key)"), the exact inputs and outputs of that
firing as one card per port (a long value expands, copies, and selects
whole on a double-click), closed ports greyed out, skip reasons in plain
words ("its `_should_flow` said no", "the required input 'x' closed",
"the scope 'x' it lives in did not run"), error boxes, bus and loop activity
panels, and a firing navigator ("‹ 2/5 iter 5/2 ›") for nodes that fired
several times. The Copy button exports the whole inspection.

Past runs: in the Runs view, "View in Graph" replays the run in the
graph with every value in place and locks the [follow toggle] onto it;
"N new run(s) · Follow" jumps to the newest. From the terminal the same
facts are `weft runs`, `weft events <run-id>`, `weft logs` (the
`weft-debugging` skill).

## Diagnostics and AI edits

As the user types (or as `nodes/` changes), the structural validation runs
on a short debounce and the Problems panel fills with the same
`line:column message` diagnostics the CLI prints. The panel is
structural-only: runtime findings (an unpicked connection) never squiggle
source, because their fix is not in the text; [the pre-flight] shows them.
A diagnostic may name another file than the one being edited (a node
spliced in by `@include` keeps its own file's coordinates). The graph
keeps showing the last good render with a problems pill ("N problems")
until the source compiles again (hint on the pill: stale node copies cause
most catalog errors, run `weft catalog update`). If a node's
`metadata.json` spells a name in the wrong casing, the pill lists it as "node
catalog warning" (a weft key written in snake_case is different: that node fails to
load, and a program that uses it fails to compile, saying the node failed to
load and why). A name in the source that is not camelCase is a `not-camel-case`
warning in the Problems panel, and the program still runs. An AI chat extension can
stream SEARCH/REPLACE edits into the open file and the graph updates as
each block lands (setting `weft.ai.streamingEditsEnabled`).
