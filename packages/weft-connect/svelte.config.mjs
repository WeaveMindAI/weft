// Svelte config for this package's own sources. Vite's svelte plugin picks it
// up automatically, both when the package's tests run and when a host bundles
// the webview from here.
import { vitePreprocess } from '@sveltejs/vite-plugin-svelte';

export default {
  preprocess: vitePreprocess(),
  compilerOptions: {
    css: 'injected',
  },
};
