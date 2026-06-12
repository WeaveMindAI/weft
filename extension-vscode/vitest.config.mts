import { defineConfig } from 'vitest/config';
import { svelte } from '@sveltejs/vite-plugin-svelte';
import * as path from 'node:path';

// Unit + engine tests for the webview's modules (projection, preflight,
// the edit engine). The svelte plugin compiles `.svelte.ts` runes modules
// (the engine's reactive state); aliases mirror tsconfig.webview.json so
// test imports resolve exactly like the bundled webview's.
export default defineConfig({
  plugins: [svelte()],
  resolve: {
    alias: [
      { find: '$app/environment', replacement: path.resolve(__dirname, 'src/webview/shims/app-environment.ts') },
      { find: '$lib/nodes', replacement: path.resolve(__dirname, 'src/webview/lib/nodes/index.svelte.ts') },
      { find: '$lib/utils', replacement: path.resolve(__dirname, 'src/webview/lib/utils.ts') },
      { find: '$lib', replacement: path.resolve(__dirname, 'src/webview/lib') },
    ],
  },
  test: {
    include: ['src/**/*.test.ts'],
  },
});
