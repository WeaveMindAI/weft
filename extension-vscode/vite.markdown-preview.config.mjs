// Vite config for the markdown preview's highlight.js. Produces a classic
// script (no module) at markdown-preview/hljs.js, which the preview loads
// before markdown-preview/highlight-weft.js.

import { defineConfig } from 'vite';
import { fileURLToPath } from 'node:url';
import path from 'node:path';

const extRoot = path.dirname(fileURLToPath(import.meta.url));

export default defineConfig({
  build: {
    outDir: 'markdown-preview',
    emptyOutDir: false,
    target: 'es2020',
    rollupOptions: {
      input: path.join(extRoot, 'markdown-preview/hljs-entry.js'),
      output: {
        format: 'iife',
        entryFileNames: 'hljs.js',
        inlineDynamicImports: true,
      },
    },
  },
});
