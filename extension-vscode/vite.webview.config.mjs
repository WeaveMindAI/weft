// Vite config for the VS Code webview bundle. Produces a single IIFE
// that the extension host loads from media/webview/bundle.{js,css}.

import { defineConfig } from 'vite';
import { svelte } from '@sveltejs/vite-plugin-svelte';
import tailwindcss from '@tailwindcss/vite';
import { fileURLToPath } from 'node:url';
import path from 'node:path';

const webviewRoot = path.dirname(fileURLToPath(import.meta.url));
const libRoot = path.join(webviewRoot, 'src/webview/lib');
const shimRoot = path.join(webviewRoot, 'src/webview/shims');

export default defineConfig({
  plugins: [tailwindcss(), svelte()],
  resolve: {
    // Mirror v1's SvelteKit path aliases so copied files resolve
    // without editing a single import statement. $app/... and $env/...
    // route to tiny shims; $lib and $lib/utils route to the copied
    // v1 tree under src/webview/lib.
    alias: [
      { find: /^\$app\/environment$/, replacement: path.join(shimRoot, 'app-environment.ts') },
      { find: /^\$app\/navigation$/, replacement: path.join(shimRoot, 'app-navigation.ts') },
      { find: /^\$lib\/utils$/, replacement: path.join(libRoot, 'utils.ts') },
      { find: /^\$lib\/utils\.js$/, replacement: path.join(libRoot, 'utils.ts') },
      { find: /^\$lib\/utils\//, replacement: path.join(libRoot, 'utils') + '/' },
      { find: /^\$lib\//, replacement: libRoot + '/' },
      { find: /^\$lib$/, replacement: libRoot },
    ],
  },
  build: {
    outDir: 'media/webview',
    emptyOutDir: true,
    target: 'es2020',
    rollupOptions: {
      input: 'src/webview/main.ts',
      output: {
        entryFileNames: 'bundle.js',
        chunkFileNames: 'bundle-[name].js',
        assetFileNames: 'bundle.[ext]',
        inlineDynamicImports: true,
      },
    },
  },
});
