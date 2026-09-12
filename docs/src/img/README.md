# Images the book is waiting on

Six shots. Drop each file in at the given name and it renders. A missing image
shows its alt text, so the book reads fine until then.

**Conventions.** Crop tight, no desktop chrome, no unrelated panels. Use a
program with real-looking data; `hello world` is fine in `graph-split-view`
and nowhere else. Keep any gif under about 4 MB.

Already in place: `first_program.png` (Tangle building the greeting, used in
[Your first program](../start/first-program.md)), `intro.png`, `logo.png`,
`readme_demo.mp4`.

| File | Used by | What to capture |
|---|---|---|
| `graph-split-view.png` | [Reading and building the graph](../start/reading-the-graph.md) | The three-box greeting program after clicking **Source**. Graph on the left, source on the right. Nothing running, no inspector open. Do not reverse the panes: source always opens on the right, and the page says so. |
| `graph-inspector.png` | same | One box selected with **Inspect execution** open, showing that firing's input and output, its status and its timing. Pick a box whose value is worth reading, so an LLM answer rather than `hello world`. |
| `graph-groups.png` | same | Side by side, or a before and after. Left: a group folded shut, showing its name, its description line and its edge dots. Right: the same graph with it open and the boxes inside visible. The wiring outside the group must be identical in both halves; that is the whole point of the shot. |
| `extension-task.png` | [Putting a person in the loop](../start/a-person-in-the-loop.md) | Two panels. Left: the task tab with **Send this answer?**, the draft text, and the Approve and Reject buttons. Right: the same run in the graph, `review` wearing its cyan waiting ring and `approved` not yet run. Both sides of the handover at once. |
| `extension-popup.png` | [The browser extension](../running/browser-extension.md) | The extension popup on its own, with one task in the list and a second below it so you can see it is a list. This is the popup, not the task tab. |
| `executions-list.png` | [An HTTP endpoint](../start/on-a-url.md) | The Executions list in the sidebar filling up as curl requests land: id, status going from running to completed, timestamp. Beside it, the graph replaying the newest one. |
