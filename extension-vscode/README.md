# Weft VS Code Extension

Primary authoring UX for Weft projects. Phase A1 scaffolds the
structure; phase A2 wires the actual behavior.

Surfaces:

- **Tangle** (left activity bar, webview). Chat with the AI builder.
- **Projects** (left activity bar, tree view). Live list of projects
  registered with the connected dispatcher.
- **Graph view** (editor tab). Opens from `.weft` files. Renders the
  graph using xyflow.
- **Runner view** (editor tab). Opens from `.loom` files. Renders the
  runner UI preview.

Configure the dispatcher URL via `weft.dispatcherUrl` setting. Defaults
to `http://localhost:9999` (local daemon). In hosted workspaces
(Coder + codespaces-equivalent) the setting is pre-populated.

## Local dev

```bash
pnpm install
pnpm run watch
# F5 in VS Code to launch an Extension Development Host.
```

## Phase A2 TODOs

- Wire Tangle panel to the dispatcher's AI endpoint (streaming deltas).
- Implement xyflow-based graph renderer.
- Implement runner view rendering.
- Subscribe to SSE streams for live project/execution state in the
  Projects tree.
- Build into a publishable `.vsix`.
