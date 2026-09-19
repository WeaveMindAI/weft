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

/** The message of a CLI `phase: "error"` event, when `value` is one. */
/// The error event a failing `--json` verb prints, as the fields this
/// side reads off it.
interface CliFailure {
  message?: string;
  /// The request never reached the daemon: it is not running, or was
  /// never installed here. A FLAG rather than a sentence to match on,
  /// because the sentence is wording and wording changes.
  // SYNC: daemonUnreachable <-> crates/weft-cli/src/progress.rs report_plain_error
  daemonUnreachable?: boolean;
}

function jsonFailure(value: unknown): CliFailure | undefined {
  if (typeof value !== 'object' || value === null) return undefined;
  const ev = value as { phase?: unknown; detail?: CliFailure };
  if (ev.phase !== 'error') return undefined;
  const detail = ev.detail;
  return {
    message: typeof detail?.message === 'string' ? detail.message : undefined,
    daemonUnreachable: detail?.daemonUnreachable === true,
  };
}

/** Thrown when `weft <args>` exits non-zero. Carries the captured
 *  stderr so the caller surfaces the CLI's actual reason. */
export class WeftCliError extends Error {
  constructor(
    public readonly args: string[],
    public readonly code: number | null,
    public readonly stderr: string,
    /// True when nothing answered at all: the daemon is not running, or
    /// this machine only has the editor extension. Every verb fails the
    /// same way until that is fixed, which is a different thing to tell
    /// somebody than "that project is not registered", so it travels as
    /// its own fact rather than as words inside `stderr`.
    public readonly daemonUnreachable: boolean = false,
  ) {
    const reason = stderr.trim() ? stderr.trim() : `exited ${code}`;
    super(`weft ${args.join(' ')}: ${reason}`);
    this.name = 'WeftCliError';
  }
}

/** Directory to invoke the `weft` CLI from, for a given document. The
 *  CLI walks up from here to find `weft.toml` (project-root resolution
 *  lives in the CLI, authoritatively; the extension does not
 *  re-implement it).
 *
 *  This is the document's OWN directory, which is only the project root
 *  when the program sits at the top; a program under `src/` gets `src/`.
 *  That is fine for a cwd, because the CLI walks up to `weft.toml`
 *  itself, and it is what every caller here wants it for.
 *
 *  It is NOT the root, so never build a project-relative path on it. A
 *  watcher once did (`nodes/**` against this) and watched `src/nodes/`,
 *  a folder that does not exist, so a node package written while the
 *  window was open never invalidated the warm catalog and every custom
 *  node read as an unknown type. `findProjectRoot` answers that
 *  question. `path.dirname` handles the fs-root case (`/x.weft` -> `/`).
 *  One definition so every call site resolves identically. */
export function docDirOf(doc: vscode.TextDocument): string {
  return path.dirname(doc.uri.fsPath);
}

/** Run `weft <args>` in `cwd`, optionally writing `stdin`, and parse
 *  stdout as JSON. Rejects with `WeftCliError` on a non-zero exit or a
 *  spawn failure. `onStderr` streams the child's stderr (compile
 *  progress, warnings) for callers that want to surface it live. */
export function runWeftJson<T>(
  args: string[],
  cwd: string,
  opts: {
    stdin?: string;
    onStderr?: (chunk: string) => void;
  } = {},
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
            // Nothing answered, for the same reason from the caller's
            // side: weft is not on this machine, so no verb can work.
            true,
          ),
        );
      } else {
        reject(new WeftCliError(args, null, err.message));
      }
    });
    child.on('close', (code) => {
      let parsed: T | undefined;
      try {
        parsed = JSON.parse(stdout) as T;
      } catch {
        parsed = undefined;
      }
      if (parsed !== undefined && code === 0) {
        resolve(parsed);
        return;
      }
      if (code !== 0) {
        // Under `--json` a failing verb prints one `phase: "error"`
        // event on stdout and nothing on stderr; the message lives in
        // its detail. Anything else on a failure is stderr's to tell.
        const failure = jsonFailure(parsed);
        reject(
          new WeftCliError(
            args,
            code,
            failure?.message ?? stderr,
            failure?.daemonUnreachable ?? false,
          ),
        );
        return;
      }
      reject(new Error(`weft ${args.join(' ')}: invalid JSON output`));
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
