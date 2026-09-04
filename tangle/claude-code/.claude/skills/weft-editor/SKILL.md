---
name: weft-editor
description: The VS Code weft extension surface, the graph view and everything in it. Read when telling the user where to click or what they are looking at: toolbar, action bar, palette, context menus, editing gestures, groups and loops in the graph, run and replay, the inspector, connect flow, problems panel. Labels are verbatim from the extension.
---

# The editor surface

Everything below is the VS Code extension as the user sees it, labels
verbatim. `.weft` files open as the graph by default (a stray text tab is
closed automatically); the "Source" button is the intentional text view. The
extension talks to the dispatcher at `http://localhost:9999` (setting
`weft.dispatcherUrl`).

## Where things live

- **Activity bar, icon "Weft"**: two sidebar views.
  - **Projects**: one row per `.weft` file in the workspace (label = folder
    name). Click to pin and open its graph; inline buttons "Open in Editor"
    and "Run"; title-bar "Refresh".
  - **Executions**: runs of the pinned project, newest first (paged, "Load
    more (N more)" at the bottom). Row = status icon, entry node, time;
    tooltip = the color id. Inline "View in Graph" and "Delete"; title-bar
    "Refresh Executions" and "Clear All Executions".
- **The graph panel**, titled "Weft Graph: <project folder name>".
- **The Problems panel** carries the compiler's live diagnostics.

## Reading the graph

- **A node** is a white card: accent bar in the type's color, header with
  the status glyph (✓ completed, ● running, ◉ waiting, ✕ failed, ■
  cancelled, ⊘ skipped) and the type name, body with the label and its
  ports. Left rail: input ports. Right rail: output ports. Each port is a
  dot colored by its type (String gray, Number blue, Boolean rose, files
  gold/purple/green/brown, List teal, Dict purple, Access teal, Bus amber,
  MustOverride red); a required input carries a `*`.
- **The small square top-left of every box** is `_should_flow`, the port
  that decides whether it runs: filled when something answers it, hollow
  when nothing does.
- **Wires** are arrows colored by the source port's type.
- **Groups** are large boxes ("GROUP" header) that hold child nodes;
  collapsed they become a chip with the label and an expand button, and
  their description line shows when collapsed. **Loops** render violet with
  a rotate icon, their config knobs in a strip under the header, implicit
  `index` and `done` ports on the rails, carry ports marked ↻.
  **@include blocks** are violet with the filename; their "Open" button
  navigates into that file and the toolbar grows a "Return · <file>" button.
- **Access nodes** (TelegramAccess and friends) show a "Connect
  <Service>..." button in their body; one with a required, unpicked
  connection pins open (expanded, collapse disabled, "Pick a connection
  first") until a connection is picked. **Trigger nodes** show a live feed
  (mount URLs, minted API keys, progress). **Infra nodes** carry a status
  pill (running, stopped, failed...) and a body feed of what the service
  prints.
- **Debug nodes** render their latest value inline; media nodes render the
  image, an audio/video player, or a download card.
- **Status glows**: amber while running, cyan while waiting for input,
  green when completed or skipped, red when failed.

## Toolbar and action bar

Top-left floating toolbar: "Return · <file>" (inside an @include), "N new
execution(s) · Catch up" (runs started while pinned), the pin pill ("Live ·
<color>" following the newest run, "Pinned · <color>" locked to one; click
to switch), and **"Source"** (opens the text beside the graph; click again
to focus it).

Top-right: **"Simplified view"** toggle. On: square, read-only nodes (toast
"Switch to the builder view to edit the graph"), one dot per side, for
reading and showing rather than building. Builder and simplified keep
separate saved positions.

Bottom-center action bar, contextual slots:

- **Infra slot** (when the project has infra): "Start Infra" / "Stop
  Infra" / "Upgrade Infra" (amber, when source changed since start) /
  terminate (trash). An eye toggle "Show infrastructure subgraph" dims
  everything except the infra closure.
- **Run slot**: "Run Project" (or "Run 1 target" / "Run N targets" once
  targets are set). Before Run, Activate, or Resync is sent, the editor
  runs a runtime validation through the warm parse-server: an unpicked
  connection and cousins land on the bar's error banner (the complete
  list, each entry clickable to its file and line) and the verb is not
  sent. Nothing is wasted on a build that cannot run. While working the
  button becomes its own cancel: "Building...", "Cached, loading...",
  "Provisioning infra...", "Running...". While following a live run it is
  "Stop Execution".
- **Trigger slot** (when the source declares triggers): "Activate" /
  "Deactivate" / "Resync" (amber "Out of sync" when the project changed
  since activation; Resync is gated by the same runtime pre-flight). An
  eye toggle "Show trigger subgraph". Deactivating
  opens the picker "Deactivate: how should triggers come down?": **Park**
  (submissions wait indefinitely), **Hibernate** (grace window, then
  refuse), **Wipe** (drop everything, cancels suspended runs), plus what to
  do with running executions. Reactivating offers "Execute parked + keep
  suspensions", "Keep suspensions only", "Wipe all".

Banners above the bar: a red one for a failed verb (click for full
diagnostics), an amber one for infra drift ("Infrastructure has changed.
Click Upgrade to apply."), an indigo "Graph locked while ..." while a verb
owns the graph.

## The in-graph palette (Ctrl+P or Cmd+P)

"Search nodes and actions...". The **Actions** section: Undo (Ctrl+Z),
Redo, Duplicate Selected, Delete Selected, Select All Nodes, Fit View, Auto
Organize Layout. The **Nodes** section: every type in the project's catalog
with a preview panel (description, input chips, output chips, tags). This
is how nodes are added by hand.

## Editing in the builder view

- **Add a node**: palette, or right-click the canvas "Add Node...
  (Ctrl+P)".
- **Wire**: drag from an output dot to an input dot. Dropping a wire on
  empty space opens the palette and completes the wire in one undo step. A
  port already filled by a literal refuses the wire ("'x' is driven by a
  config assignment; unset it first to drive it with an edge.").
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
- **Nodes**: right-click for "Duplicate (Ctrl+D)", "Delete (Del)", "Tags...",
  and on an output node "Set as target" / "Unset target". On an infra node:
  "Stop this node" (scales to zero, keeps disks) and "Terminate this node"
  (destroys them), both behind a confirmation.
- **Rename**: double-click a node's label or a group's header.
- **Groups and loops**: the expand/collapse toggle on the box; "Auto
  Organize Layout" in the palette reflows; positions persist under
  `layouts/` (never in the source).
- Every GUI edit is a structured edit applied through the compiler, so the
  text and the picture cannot drift, and Ctrl+Z undoes in either view.
- Simplified view refuses structure edits with "Simplified view is
  read-only (you can still move, expand, and collapse).".

Keyboard: Ctrl+P palette, **Ctrl+Enter run**, Ctrl+Z/Y undo/redo, Ctrl+A
select, Ctrl+D duplicate, Del delete, Esc closes the palette and drops a wire you are dragging. Zoom is Ctrl/Cmd+wheel
(5% to 200%); the bottom-left controls carry zoom, fit, and lock.

## Running and watching

The Run button runs the pinned project from its output nodes (or the aimed
targets). Each node glows as it fires, values travel the wires, and the
Executions list gains the run. Click any node to open the **inspector**:
status, duration, cost ("$0.0123 (own key)"), the exact inputs and outputs
of that firing as JSON trees, closed ports shown as "(closed)", skip
reasons in plain words ("its `_should_flow` said no", "the required input
'x' closed", "it is outside the part of the graph this execution runs"),
error boxes, bus and loop activity panels, and a firing navigator
("‹ 2/5 iter 5/2 ›") for nodes that fired several times. The Copy button
exports the whole inspection.

Past runs: the Executions view, "View in Graph" replays the run in the
graph with every value in place; the pin pill says which run is on screen,
and "Catch up" jumps to the newest. From the terminal the same facts are
`weft executions`, `weft events <color>`, `weft logs` (the `weft-running`
skill).

## Diagnostics and AI edits

As the user types (or as `nodes/` changes), the structural validation runs
on a short debounce and the Problems panel fills with the same
`line:column message` diagnostics the CLI prints. The panel is
structural-only by design: runtime findings (an unpicked connection)
never squiggle source, because their fix is not in the text; the action
bar's pre-flight gate is where they are shown. A diagnostic may name
another file than the one being edited (a node spliced in by `@include`
keeps its own file's coordinates). The graph keeps showing
the last good render with a problems pill ("N problems") until the source
compiles again (hint on the pill: stale node copies cause most catalog
errors, run `weft catalog update`). An AI chat extension can stream
SEARCH/REPLACE edits into the open file and the graph updates as each
block lands (setting `weft.ai.streamingEditsEnabled`).
