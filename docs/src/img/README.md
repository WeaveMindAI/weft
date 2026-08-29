# Image checklist

Every visual the book and the README reference, with a brief for each. Nothing
here exists yet; drop the file in at the given path and it renders.

A missing image shows its alt text, so the book is readable while this list is
being worked through.

## Conventions

- **Screenshots**: dark theme, VS Code default dark or the editor's own. Crop
  tight, no desktop chrome, no unrelated panels. Retina scale if you can.
- **Gifs**: 6 to 12 seconds, looping, no cursor unless the cursor is the point.
  Keep them under about 4 MB so a GitHub page stays fast.
- **Video**: GitHub renders `.mp4` uploaded through an issue or release and
  referenced by URL, but a plain `<video>` tag in markdown does not play on
  github.com. Use a gif for anything that must play inline in the README, and
  keep mp4 for the docs site, where it works.
- **Content**: use a program with real-looking data. "hello world" is fine for
  the very first screenshot and nowhere else. A support-ticket triage or an
  email classifier reads as real and is what the docs use as their running
  example.

## The list

| File | Kind | Where it is used | Brief |
|---|---|---|---|
| `readme_demo.mp4` | video (recorded, in place) | README, after the first program | Split view: `main.weft` on the left, the graph on the right, mid-execution. To play inline on github.com it has to be dragged into the README through the web editor (the pasted user-attachments URL is the player); the committed file here is the source of truth and the docs-site copy. |
| `graph-split-view.png` | screenshot | `start/reading-the-graph.md` | Resting state. `main.weft` left, the three-node greeting/shout/out graph right. Nodes as rounded boxes with icon, label, and named ports on the edge rails. Nothing running. |
| `graph-mid-run.png` | gif or png | `start/reading-the-graph.md` | The same graph mid-execution: first node settled, middle node lit and running with a pulse on its incoming edge, third still idle. If a gif, show each node lighting in turn and end with all three settled. |
| `graph-inspector.png` | screenshot | `start/reading-the-graph.md` | One node selected, the execution inspector open on that firing's inputs and outputs as formatted JSON, plus status and timing. Pick a node whose value is interesting: an LLM response, a parsed email body. |
| `graph-groups.png` | screenshot | `start/reading-the-graph.md` | Side by side, or a before/after gif. Left: a group collapsed to one box showing its name, its description line, and its boundary ports. Right: the same graph with it expanded, child nodes visible, the `self` rails on each side. The outside wiring must be identical in both. |
| `executions-list.png` | gif or png | `start/on-a-url.md` | The VS Code sidebar's executions tree filling as curl requests land: colour id, status flipping running to completed, timestamp. Beside it the graph replaying the most recent one. |
| `extension-task.png` | screenshot | `start/a-person-in-the-loop.md` | Two-panel composite. Left: the extension popup with one pending task, "Escalate this ticket?", the classification as context, Approve / Reject. Right: the graph of the same execution with the HumanQuery node visibly waiting and everything downstream idle. Both sides of the handoff at once. |
| `extension-popup.png` | screenshot | `running/browser-extension.md` | The extension popup alone, one task expanded with its form fields, a second task collapsed below so the list nature is visible. |
| `pulse-model.svg` | diagram | README | Hand-drawn or vector, not a screenshot. Four nodes, one wire carrying a pulse labelled with a value, one wire carrying a **closed** pulse drawn distinctly (dashed, greyed, marked "closed"), and the two nodes downstream of it drawn as skipped. One caption: "a closed port is a value, and that is how branching works". |
| `architecture.svg` | diagram | `running/architecture.md` (optional; a mermaid version is already inline) | Only if the mermaid one is not good enough. The four tiers, the broker, Postgres, and which arrows are HTTP versus database rows. |
| `sdp-loop.svg` | diagram | `thinking/sdp.md` | The refinement loop: one real example entering a chain, a stage being iterated, the chain growing, then a second example entering and one stage lighting up as broken. Should read in three seconds. |

## Nice to have, not needed

| File | Kind | Brief |
|---|---|---|
| `node-authoring.gif` | gif | Writing a node folder and having it appear in the editor's node palette without a restart. |
| `infra-start.gif` | gif | `weft infra start` provisioning a container, the node's status going through provisioning to running, the live panel appearing with a QR code. |
| `cost-trail.png` | screenshot | An execution's cost breakdown per node, showing that measurement is real. |
