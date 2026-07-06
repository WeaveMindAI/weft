import { defineConfig } from 'vitest/config';
import { svelte } from '@sveltejs/vite-plugin-svelte';
import * as path from 'node:path';

// Unit + engine tests for the SHARED graph editor (projection, preflight, the
// edit engine, layout, types). These live in the extracted `@weft/graph` package
// (`../packages/weft-graph`), reached from the extension through the same source
// the bundled webview compiles. The svelte plugin compiles `.svelte.ts` runes
// modules (the engine's reactive state). The package's tests use relative imports
// (no `$lib`/`$app` aliases), so none are needed here.
const graphSrc = path.resolve(__dirname, '../packages/weft-graph/src');

export default defineConfig({
  plugins: [svelte()],
  test: {
    include: [path.join(graphSrc, '**/*.test.ts')],
  },
});
