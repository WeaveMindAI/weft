// Vite config for the VS Code webview bundle. Produces a single IIFE
// that the extension host loads from media/webview/bundle.{js,css}.

import { defineConfig } from 'vite';
import { svelte } from '@sveltejs/vite-plugin-svelte';
import tailwindcss from '@tailwindcss/vite';

export default defineConfig({
  plugins: [tailwindcss(), svelte()],
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
