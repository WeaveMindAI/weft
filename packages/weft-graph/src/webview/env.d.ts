/// <reference types="svelte" />
/// <reference types="vite/client" />

// Fallback typing for .svelte imports when the Svelte language
// server isn't running (plain tsc). Keeps the IDE diagnostics
// usable even without the svelte extension.
declare module '*.svelte' {
  import type { Component } from 'svelte';
  const component: Component<Record<string, any>>;
  export default component;
}
