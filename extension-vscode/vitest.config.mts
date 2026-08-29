import { defineConfig } from 'vitest/config';

// The extension's own tests: the host-side logic that decides what the
// webview is shown, independent of VS Code's API. The graph renderer it
// bundles has its own suite in ../packages/weft-graph.
export default defineConfig({
  resolve: {
    // The real `vscode` module only exists inside the editor process;
    // the stub carries just enough surface for the host-side logic.
    alias: { vscode: new URL('./src/test-support/vscode-stub.ts', import.meta.url).pathname },
  },
  test: {
    include: ['src/**/*.test.ts'],
  },
});
