// Bundles the Svelte-based webview (src/webview/main.ts and its
// dependencies) into a single JS + CSS pair under media/webview/.
// The extension host loads these as a VS Code webview.

import { build, context } from 'esbuild';
import esbuildSvelte from 'esbuild-svelte';
import sveltePreprocess from 'svelte-preprocess';

const watch = process.argv.includes('--watch');

const options = {
  entryPoints: ['src/webview/main.ts'],
  bundle: true,
  outfile: 'media/webview/bundle.js',
  format: 'iife',
  target: 'es2020',
  platform: 'browser',
  minify: !watch,
  sourcemap: watch ? 'inline' : false,
  plugins: [
    esbuildSvelte({
      preprocess: sveltePreprocess(),
      compilerOptions: { css: 'injected' },
    }),
  ],
  logLevel: 'info',
};

if (watch) {
  const ctx = await context(options);
  await ctx.watch();
  console.log('[weft] watching webview bundle...');
} else {
  await build(options);
  console.log('[weft] webview bundle built');
}
