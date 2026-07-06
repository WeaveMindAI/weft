// Barrel re-export of the node registry, which lives in the Svelte-5 rune module
// `index.svelte.ts` (it uses `$state` for the live catalog). A plain `.ts` index
// alongside it lets STANDARD TypeScript module resolution resolve a bare
// `./nodes` import to this file (TS resolves `./nodes/index.ts`, but does NOT try
// `./nodes/index.svelte.ts`). Without this barrel, a consumer that typechecks the
// package through a SYMLINK (a web host can mount the package source via an
// in-src symlink) cannot resolve `./nodes`, because svelte-check's `.svelte.ts`
// resolution does not follow through the symlink. The rune module itself is still
// compiled as a `.svelte.ts` by the svelte plugin; this barrel only forwards its
// exports so resolution works everywhere.
export * from './index.svelte';
