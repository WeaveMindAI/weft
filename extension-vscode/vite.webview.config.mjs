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
// Plyr's published bundle carries its ads, YouTube and Vimeo plugins
// whether or not a player uses them, and those load and run third-party
// scripts (an ad SDK, an ad server's tag builder, the embed players).
// The graph view only plays stored files, and a marketplace scanner
// reads that code as ad and remote-script injection whatever the
// intent: it is what got the extension refused. So the bundle is built
// from Plyr's own source instead of its dist, with those three modules
// routed to stand-ins that carry none of it. Everything else in Plyr
// (the controls, the html5 backend, the styles) is untouched.
const plyrSource = path.join(extRoot, 'node_modules/plyr/src/js/plyr.js');
const plyrShim = path.join(shimRoot, 'plyr-no-external.js');
const PLYR_EXTERNAL = { './plugins/ads': 'Ads', './plugins/youtube': 'youtube', './plugins/vimeo': 'vimeo' };
const plyrWithoutExternalCode = () => ({
  name: 'plyr-without-external-code',
  enforce: 'pre',
  resolveId(source, importer) {
    if (!importer || !importer.includes(`${path.sep}plyr${path.sep}src${path.sep}js${path.sep}`)) return null;
    return source in PLYR_EXTERNAL ? `\0plyr-shim:${PLYR_EXTERNAL[source]}` : null;
  },
  load(id) {
    if (!id.startsWith('\0plyr-shim:')) return null;
    const name = id.slice('\0plyr-shim:'.length);
    return `export { ${name} as default } from ${JSON.stringify(plyrShim)};`;
  },
  // Plyr's defaults table names the hosts those plugins would load
  // from (the ad SDK, the embed players, its own CDN for the icon
  // sprite and a blank clip). With the plugins gone and the sprite
  // bundled (`FilePreview.svelte`), none is read, and the bundle
  // should not name a host it never talks to.
  transform(code, id) {
    if (!id.replace(/\\/g, '/').endsWith('/plyr/src/js/config/defaults.js')) return null;
    return { code: code.replace(/'https:\/\/[^']*'/g, "''"), map: null };
  },
});

const graphDeps = Object.keys({
  ...graphPkg.dependencies,
  ...graphPkg.peerDependencies,
});

export default defineConfig({
  plugins: [plyrWithoutExternalCode(), tailwindcss(), svelte()],
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
      // Plyr from its source, so the plugin above can route its modules.
      { find: /^plyr$/, replacement: plyrSource },
      // The icon sprite is not in Plyr's exports map, so the bare
      // specifier the component imports (`?raw` and all) is pointed at
      // the file; the prefix match keeps the query.
      { find: /^plyr\/dist\/plyr\.svg/, replacement: path.join(extRoot, 'node_modules/plyr/dist/plyr.svg') },
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
