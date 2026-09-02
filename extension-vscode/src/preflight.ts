// Pre-flight gate for the action bar's shipping verbs (PREFLIGHT_VERBS).
//
// Runtime-level findings (an unpicked connection, a missing model) are not
// code errors: the Problems panel skips them so a project can be sketched
// with secrets unfilled. But sending such a project to the dispatcher wastes
// a build and fails mid-execution. So before the extension spawns the CLI
// verb, it validates the saved entry file in `runtime` mode; any
// error-severity finding blocks the verb and lands on the action bar's
// error banner (complete list in the details modal), and nothing is sent.

import * as fs from 'node:fs/promises';
import * as path from 'node:path';
import type { ParseServer } from './parseServer';
import type {
  ActionBarError,
  ActionErrorDetails,
  Diagnostic as WeftDiagnostic,
} from '../../packages/weft-graph/src/protocol';

/// The verbs the gate holds: the ones that ship the project somewhere.
/// The ONE spelling of the set; comments elsewhere point here.
export const PREFLIGHT_VERBS: ReadonlySet<string> = new Set(['run', 'activate', 'resync']);

/// What a blocked verb puts on the action bar: an ActionBarError minus
/// the verb (the caller knows which verb it gated), with the details
/// always present (the modal renders the complete finding list).
export type PreflightBlock = Omit<ActionBarError, 'verb'> & { details: ActionErrorDetails };

/** Decide whether a runtime-mode validation blocks the verb. Pure: takes the
 *  diagnostics, returns the banner + modal content, or null to proceed.
 *  Errors block; warnings ride along in the details so the user sees the
 *  complete picture, but never block on their own. */
export function preflightBlock(
  diagnostics: WeftDiagnostic[],
  entryFile: string,
): PreflightBlock | null {
  const errors = diagnostics.filter((d) => d.severity === 'error');
  if (errors.length === 0) return null;
  const message =
    errors.length === 1
      ? errors[0].message
      : `not ready: ${errors.length} things need fixing on the graph first`;
  return {
    message,
    details: {
      what: `Pre-flight check of ${path.basename(entryFile)}`,
      stage: 'preflight',
      // ActionErrorDiagnostic has no 'hint' rung; hints render as info.
      // A diagnostic's `file` is set when its coordinates live in an
      // @include's file; absent means the compiled source, the entry.
      // SYNC: diagnostic -> ActionErrorDiagnostic mapping <->
      //       crates/weft-cli/src/commands/ensure.rs compile-failure structured_error
      diagnostics: diagnostics.map((d) => ({
        severity:
          d.severity === 'error' || d.severity === 'warning'
            ? d.severity
            : ('info' as const),
        ...(d.code ? { code: d.code } : {}),
        message: d.message,
        location: { file: d.file ?? entryFile, line: d.line, column: d.column },
      })),
    },
  };
}

/** Read the saved entry file (the same source the CLI verb would build) and
 *  validate it in runtime mode. Returns the block to show, or null to
 *  proceed. Throws when the check itself cannot run (entry unreadable, parse
 *  server down); the caller refuses the verb loudly rather than running
 *  unchecked. */
export async function runPreflight(
  parseServer: ParseServer,
  entryPath: string,
  signal?: AbortSignal,
): Promise<PreflightBlock | null> {
  const source = await fs.readFile(entryPath, { encoding: 'utf8', signal });
  const result = await parseServer.request<{ diagnostics: WeftDiagnostic[] }>({
    kind: 'validate',
    source,
    file: entryPath,
    // The verb builds against the catalog as it is ON DISK right now;
    // gating against the warm server's possibly-stale copy could block
    // on a rule the user just fixed, or pass a project the build
    // rejects. One user-initiated check per click, so the reload is
    // cheap where wrong answers are not.
    reloadCatalog: true,
    mode: 'runtime',
  }, signal);
  return preflightBlock(result.diagnostics, entryPath);
}
