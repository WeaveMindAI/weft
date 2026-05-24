// Shell the `weft` CLI and capture its JSON stdout.
//
// Parse, validate, and describe-nodes are node-aware: they need the
// project's `nodes/` catalog, which lives on the user's machine. The
// dispatcher (a remote pod) has no access to it, so these run locally
// through the CLI, which reads `nodes/` directly. This mirrors how the
// extension already shells out for lifecycle verbs.

import { spawn } from 'node:child_process';
import * as path from 'node:path';
import type * as vscode from 'vscode';

/** Thrown when `weft <args>` exits non-zero. Carries the captured
 *  stderr so the caller surfaces the CLI's actual reason. */
export class WeftCliError extends Error {
  constructor(
    public readonly args: string[],
    public readonly code: number | null,
    public readonly stderr: string,
  ) {
    const reason = stderr.trim() ? stderr.trim() : `exited ${code}`;
    super(`weft ${args.join(' ')}: ${reason}`);
    this.name = 'WeftCliError';
  }
}

/** Directory to invoke the `weft` CLI from, and the base for watching
 *  a project's `nodes/`, for a given document. The CLI walks up from
 *  here to find `weft.toml` (project-root resolution lives in the CLI,
 *  authoritatively; the extension does not re-implement it). The nodes
 *  watchers (graphView, diagnostics) treat this as the project root.
 *
 *  Assumption: a `.weft` file sits at its project root, next to
 *  `weft.toml` and `nodes/`. True today (one `main.weft` per project).
 *  When multi-file projects land (`.weft` files in subdirs), the
 *  watchers' nodes-dir base would be wrong; resolve the actual root
 *  then, sharing the CLI's discovery rather than forking a second
 *  walk-up here. `path.dirname` handles the fs-root case (`/x.weft`
 *  -> `/`). One definition so every call site resolves identically. */
export function projectDirOf(doc: vscode.TextDocument): string {
  return path.dirname(doc.uri.fsPath);
}

/** Run `weft <args>` in `cwd`, optionally writing `stdin`, and parse
 *  stdout as JSON. Rejects with `WeftCliError` on a non-zero exit or a
 *  spawn failure. `onStderr` streams the child's stderr (compile
 *  progress, warnings) for callers that want to surface it live. */
export function runWeftJson<T>(
  args: string[],
  cwd: string,
  opts: { stdin?: string; onStderr?: (chunk: string) => void } = {},
): Promise<T> {
  return new Promise((resolve, reject) => {
    const child = spawn('weft', args, { cwd, env: process.env });
    let stdout = '';
    let stderr = '';
    child.stdout?.on('data', (b: Buffer) => {
      stdout += b.toString();
    });
    child.stderr?.on('data', (b: Buffer) => {
      const s = b.toString();
      stderr += s;
      opts.onStderr?.(s);
    });
    child.on('error', (err: NodeJS.ErrnoException) => {
      // ENOENT means `weft` isn't on PATH: the single most likely
      // real-world failure (fresh machine, CLI not installed). The raw
      // `spawn weft ENOENT` is useless, so name the actual fix.
      if (err.code === 'ENOENT') {
        reject(
          new WeftCliError(
            args,
            null,
            'weft CLI not found on PATH. Install it or add it to your PATH.',
          ),
        );
      } else {
        reject(new WeftCliError(args, null, err.message));
      }
    });
    child.on('close', (code) => {
      if (code !== 0) {
        reject(new WeftCliError(args, code, stderr));
        return;
      }
      try {
        resolve(JSON.parse(stdout) as T);
      } catch (err) {
        reject(new Error(`weft ${args.join(' ')}: invalid JSON output: ${String(err)}`));
      }
    });
    // Absorb stdin stream errors: if the child exits and closes the
    // pipe before our write lands (e.g. a fast non-zero exit), the
    // write emits EPIPE on the stdin stream, which with no listener is
    // an UNCAUGHT exception that takes down the extension host. The
    // real failure still surfaces via `close` (exit code + stderr);
    // this only stops a broken pipe from crashing the host.
    child.stdin?.on('error', () => {});
    if (opts.stdin !== undefined) {
      child.stdin?.write(opts.stdin);
    }
    child.stdin?.end();
  });
}
