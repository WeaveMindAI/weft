# Weft VS Code Extension

Ops-side UX for Weft. Connects to the local dispatcher daemon and
gives the user a live view of their projects plus simple previews
for `.weft` and `.loom` files.

## Surfaces

- **Projects** (left activity bar, tree view). Live list of projects
  registered with the connected dispatcher. Polls every 5 seconds.
- **Graph view** (editor tab). Opens from `.weft` files.
- **Runner view** (editor tab). Opens from `.loom` files.
- **Commands**: `Weft: Run Project`, `Weft: Open Graph View`,
  `Weft: Open Runner View`.

## Configuration

`weft.dispatcherUrl` sets the URL of the dispatcher the extension
talks to. Defaults to `http://localhost:9999` (the local daemon
started by `weft start`).

## Local dev

```bash
pnpm install
pnpm run watch
# F5 in VS Code to launch an Extension Development Host.
```

## Phase B TODOs

- Implement xyflow-based graph renderer.
- Implement runner view rendering from `.loom` sources.
- Subscribe to SSE streams for live project/execution state.
- Build into a publishable `.vsix`.
