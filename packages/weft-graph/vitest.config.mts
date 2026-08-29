import { defineConfig } from 'vitest/config';
import { svelte } from '@sveltejs/vite-plugin-svelte';

// The graph package's own tests: projection, preflight, the edit engine,
// layout, types, the host-bridge protocol. Nothing here knows about a host.
//
// The package has no install of its own; `node_modules` is a symlink to
// extension-vscode's, created by setup.sh, which is where vitest and the
// svelte plugin come from. The plugin is needed to compile the `.svelte.ts`
// runes modules the engine's reactive state lives in.
export default defineConfig({
  plugins: [svelte()],
  test: {
    include: ['src/**/*.test.ts'],
  },
});
