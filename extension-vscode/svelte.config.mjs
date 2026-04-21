// Svelte config for the webview bundle. Vite's svelte plugin picks
// this up automatically.
import { vitePreprocess } from '@sveltejs/vite-plugin-svelte';

export default {
  preprocess: vitePreprocess(),
  compilerOptions: {
    css: 'injected',
  },
};
