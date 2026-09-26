import { readdirSync, readFileSync } from 'node:fs';
import { join } from 'node:path';
import { describe, expect, it } from 'vitest';

// A host builds these components with ITS Svelte, not ours, and some Svelte
// releases break on an optional parameter (`function f(x?: T)`, `(x?: T) =>`): Svelte
// 5.56.1 strips the `: T` but leaves the `?` on the parameter, the esrap
// 2.3 printer writes it out, and the host's `vite build` then fails with
// "Expected `,` or `)` but found `?`" (a `svelte.compile` through `require`
// loads the bundled compiler, whose older printer drops the `?`, so it
// looks fine). A default (`x: T | undefined = undefined`) survives every
// release, so the components use that.
describe('svelte components', () => {
  it('declare no optional function parameter', () => {
    const dir = import.meta.dirname;
    const offenders: string[] = [];
    for (const file of readdirSync(dir).filter((f) => f.endsWith('.svelte'))) {
      const source = readFileSync(join(dir, file), 'utf8');
      // A function declaration or expression, and an arrow function (its
      // return type, if any, sits between the `)` and the `=>`).
      const functions = /function\s*\w*\s*\(([^)]*)\)|\(([^()]*)\)\s*(?::[^=;{}()]*)?=>/g;
      for (const m of source.matchAll(functions)) {
        // An object type's own optional fields (`{ id?: string }`) are fine.
        let params = m[1] ?? m[2];
        while (/\{[^{}]*\}/.test(params)) params = params.replace(/\{[^{}]*\}/g, '');
        if (/\w\?\s*:/.test(params)) offenders.push(`${file}: ${m[0]}`);
      }
    }
    expect(offenders).toEqual([]);
  });
});
