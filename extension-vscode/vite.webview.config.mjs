// Vite config for the VS Code webview bundle. Produces a single IIFE
// that the extension host loads from media/webview/bundle.js.

import { defineConfig } from 'vite';
import { svelte } from '@sveltejs/vite-plugin-svelte';

export default defineConfig({
  plugins: [svelte()],
  build: {
    outDir: 'media/webview',
    emptyOutDir: true,
    target: 'es2020',
    // VS Code webviews accept a single <script src> tag; keep output
    // flat and produce a stable bundle.js + bundle.css so the host's
    // static CSP can list them.
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
