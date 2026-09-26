import { defineConfig } from 'vitest/config';

// The connect library's own tests: the recipe readers, the member door's
// requests, the consent wait. Nothing here needs a browser.
//
// Like the graph package, this one has no install of its own:
// `node_modules` is a symlink to extension-vscode's, made by setup.sh.
export default defineConfig({
  test: {
    include: ['src/**/*.test.ts'],
  },
});
