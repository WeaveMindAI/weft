// Vite config for the VS Code webview bundle. Produces a single IIFE
// that the extension host loads from media/webview/bundle.{js,css}.

import { defineConfig } from 'vite';
import { svelte } from '@sveltejs/vite-plugin-svelte';
import tailwindcss from '@tailwindcss/vite';
import { fileURLToPath } from 'node:url';
import { createRequire } from 'node:module';
import path from 'node:path';

const extRoot = path.dirname(fileURLToPath(import.meta.url));
// The graph webview was extracted into the shared `weft-graph` package so the
// website can share one graph renderer. The extension still builds it into its
// own bundle, just from the package source now.
const webviewRoot = path.join(extRoot, '../packages/weft-graph/src/webview');
const shimRoot = path.join(webviewRoot, 'shims');

// The package's source lives in a sibling dir (`../packages/weft-graph`) whose
// node-resolution walk never reaches THIS app's node_modules, so its bare
// imports (`svelte-sonner`, `@lucide/svelte`, codemirror, svelte, ...) wouldn't
// resolve. `resolve.dedupe` is the documented fix: it forces each listed dep to
// resolve to the single copy in THIS app's node_modules while still honoring the
// dep's own `exports` map (so deep imports like `svelte/internal/...` work).
// Read the list from the package manifest so it stays in lockstep with the
// package's real deps.
const graphPkg = createRequire(import.meta.url)(
  path.join(extRoot, '../packages/weft-graph/package.json'),
);
const graphDeps = Object.keys({
  ...graphPkg.dependencies,
  ...graphPkg.peerDependencies,
});

export default defineConfig({
  plugins: [tailwindcss(), svelte()],
  resolve: {
    // Default Vite list minus the `.mjs` quirks plus `.svelte.ts`
    // so imports of `./lib/nodes` pick up `lib/nodes/index.svelte.ts`.
    extensions: ['.mjs', '.js', '.mts', '.ts', '.svelte.ts', '.jsx', '.tsx', '.json'],
    dedupe: graphDeps,
    alias: [
      // The package's webview code uses SvelteKit's `$app/environment`; in a
      // plain vite build that routes to a tiny shim (browser=true).
      { find: /^\$app\/environment$/, replacement: path.join(shimRoot, 'app-environment.ts') },
      // `@tailwindcss/vite` resolves `tailwindcss` relative to the package's CSS
      // file, which also can't reach this app's node_modules; point it here too.
      { find: /^tailwindcss$/, replacement: path.join(extRoot, 'node_modules/tailwindcss') },
    ],
  },
  build: {
    outDir: 'media/webview',
    emptyOutDir: true,
    target: 'es2020',
    rollupOptions: {
      input: path.join(webviewRoot, 'main.ts'),
      output: {
        entryFileNames: 'bundle.js',
        chunkFileNames: 'bundle-[name].js',
        assetFileNames: 'bundle.[ext]',
        inlineDynamicImports: true,
      },
    },
  },
});
