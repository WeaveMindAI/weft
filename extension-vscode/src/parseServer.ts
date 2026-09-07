// Long-lived `weft parse-server` client.
//
// The editor parses on every debounced edit. A cold `weft parse` per edit pays
// full catalog discovery (a `nodes/` walk) each time; the parse-server holds
// the catalog warm in memory, so each request is parse-cost. We spawn ONE
// server for the extension, talk to it over stdio (one JSON request per line,
// one JSON response per line), and match responses to requests by id.
//
// Lifecycle: spawned lazily on first request, killed on dispose (deactivate).
// If the child dies unexpectedly, every pending request rejects and the next
// request respawns it: a dead server surfaces loudly (the caller's catch posts
// a parseError) and self-heals, it never silently stops editor feedback.
// The spawned BINARY is watched too: a `weft install` while a window is open
// would otherwise leave a warm server speaking the previous wire shape (the
// webview reads fields the old server never emits), so a binary change kills
// the child and the next request respawns on the new one.

import { type ChildProcessWithoutNullStreams, execFileSync, spawn } from 'node:child_process';
import * as fs from 'node:fs';
import * as readline from 'node:readline';
import { WeftCliError } from './cli';
import type { EditOp, TextEdit } from '../../packages/weft-graph/src/protocol';

/** Which validation tier a `validate` request runs. `structural` = graph
 *  shape only (the Problems panel); `runtime` = additionally the rules
 *  flagged `level: runtime` such as missing credentials (the pre-flight
 *  gate before Run). */
// SYNC: ValidationMode <-> crates/weft-compiler/src/validate.rs ValidationMode
export type ValidationMode = 'structural' | 'runtime';

/** Fields every parse-server request carries. `source` is the buffer text
 *  (parsed as-is, no disk read); `file` gives the `@file`/`@include` base
 *  and the project to resolve; `reloadCatalog` drops the server's warm
 *  catalog for this project first (sent when the host's `nodes/` watcher
 *  fired). */
interface ParseServerRequestBase {
  source: string;
  file?: string;
  reloadCatalog?: boolean;
}

/** A parse-server request. A union on `kind` so each pipeline's required
 *  inputs are required by the type: a `validate` without a mode (which the
 *  server refuses) or an `edit` without ops cannot be written. */
// SYNC: ParseServerRequest <-> crates/weft-cli/src/commands/parse.rs ServerRequest
export type ParseServerRequest =
  | ({ kind: 'parse' } & ParseServerRequestBase)
  | ({ kind: 'validate'; mode: ValidationMode } & ParseServerRequestBase)
  /** Edit ops, applied in order to `source`. */
  | ({ kind: 'edit'; ops: EditOp[] } & ParseServerRequestBase)
  /** A raw text edit to replay (the undo/redo path). */
  | ({ kind: 'applyEdit'; textEdit: TextEdit } & ParseServerRequestBase);

// SYNC: ServerResponseEnvelope <-> crates/weft-cli/src/commands/parse.rs ServerResponse
interface ServerResponseEnvelope {
  id: number;
  payload?: unknown;
  error?: string;
}

interface Pending {
  resolve: (payload: unknown) => void;
  reject: (err: Error) => void;
  timer: ReturnType<typeof setTimeout>;
  /// Detaches the request's abort listener; run on every settle so a
  /// long-lived signal cannot accumulate listeners.
  cleanup?: () => void;
}

/** The server ANSWERED with an error (an invalid edit, a parse/catalog
 *  failure): the message is the server's own words, fit to show the user
 *  verbatim. Distinct from `WeftCliError`, which wraps TRANSPORT failures
 *  (spawn/exit/timeout/wire), where the `weft parse-server: ` framing is the
 *  right thing to say. */
export class ParseServerError extends Error {
  constructor(message: string) {
    super(message);
    this.name = 'ParseServerError';
  }
}

/** A request that gets no response within this window is treated as a wedged
 *  server: reject loudly and tear the child down so the next request respawns.
 *  A dead child already self-heals via the exit handler; this covers the
 *  alive-but-hung case (a hang is otherwise indistinguishable from "feedback
 *  silently stopped").
 *
 *  Sized to comfortably cover the one slow operation a healthy server does: the
 *  first request for a project builds its node catalog (walks `nodes/`, reads
 *  TOMLs), which is milliseconds for realistic projects. 30s leaves ample
 *  headroom so a progressing cold build is never mistaken for a hang (which
 *  would kill+respawn+rebuild in a loop), while still catching a true wedge.
 *  If catalog builds ever genuinely approach this, the right reshape is a
 *  dedicated out-of-band warm-up request, not a bigger timeout. */
const REQUEST_TIMEOUT_MS = 30_000;

export class ParseServer {
  private child: ChildProcessWithoutNullStreams | undefined;
  private reader: readline.Interface | undefined;
  private readonly pending = new Map<number, Pending>();
  private nextId = 1;
  private disposed = false;

  /** cwd to spawn the server in. The server resolves each request's project
   *  from the request `file`, so this only affects where a `file`-less parse
   *  (detached buffer) looks; pass the workspace root. */
  constructor(private readonly cwd: string) {}

  /** Send a request and resolve with its typed payload. Spawns the server on
   *  first use. Rejects with WeftCliError if the server can't be reached,
   *  answers with an error envelope, or doesn't answer within the timeout.
   *  An aborted `signal` rejects immediately (the server's late reply is
   *  dropped by the pending-map miss); the server itself is untouched. */
  request<T>(req: ParseServerRequest, signal?: AbortSignal): Promise<T> {
    if (this.disposed) {
      return Promise.reject(new Error('parse server is disposed'));
    }
    if (signal?.aborted) {
      return Promise.reject(new WeftCliError(['parse-server'], null, 'request aborted'));
    }
    const child = this.ensureChild();
    const id = this.nextId++;
    const line = JSON.stringify({ id, ...req }) + '\n';
    return new Promise<T>((resolve, reject) => {
      const onAbort = () => {
        this.settle(id)?.reject(new WeftCliError(['parse-server'], null, 'request aborted'));
      };
      const timer = setTimeout(() => {
        const p = this.settle(id);
        if (!p) return;
        p.reject(new WeftCliError(['parse-server'], null, `parse server did not respond within ${REQUEST_TIMEOUT_MS}ms (restarting)`));
        // A wedged server won't answer the next request either; tear it down so
        // ensureChild respawns a fresh one. This also rejects siblings pending.
        this.onChildGone(child, new WeftCliError(['parse-server'], null, 'parse server wedged; restarted'));
      }, REQUEST_TIMEOUT_MS);
      this.pending.set(id, {
        resolve: resolve as (p: unknown) => void,
        reject,
        timer,
        cleanup: signal ? () => signal.removeEventListener('abort', onAbort) : undefined,
      });
      signal?.addEventListener('abort', onAbort);
      // stdin write errors (broken pipe on a dying child) surface via the
      // child 'exit' handler, which rejects all pending; absorb here so an
      // EPIPE doesn't crash the extension host.
      child.stdin.write(line, (err) => {
        if (err) {
          this.settle(id)?.reject(new WeftCliError(['parse-server'], null, err.message));
        }
      });
    });
  }

  /** Take one pending request out of flight: clear its timer, detach
   *  its abort listener, and hand it back for the caller to settle.
   *  THE one path out of the pending map; every resolve/reject route
   *  (reply, timeout, abort, write error, child death, dispose) goes
   *  through it, so no route can forget the timer or the listener. */
  private settle(id: number): Pending | undefined {
    const p = this.pending.get(id);
    if (!p) return undefined;
    this.pending.delete(id);
    clearTimeout(p.timer);
    p.cleanup?.();
    return p;
  }

  private ensureChild(): ChildProcessWithoutNullStreams {
    if (this.child) return this.child;

    const child = spawn('weft', ['parse-server'], { cwd: this.cwd, env: process.env });
    this.child = child;
    this.watchBinary(child);

    // One response per line. The handler captures ITS child: a stale
    // reader's late line must never tear down a newer child.
    this.reader = readline.createInterface({ input: child.stdout });
    this.reader.on('line', (line) => this.onLine(child, line));

    // stderr is the server's tracing channel; surface it for debugging but
    // don't treat it as a response.
    child.stderr.on('data', (b: Buffer) => {
      const s = b.toString().trim();
      if (s) console.error(`[weft parse-server] ${s}`);
    });

    const fail = (err: Error) => this.onChildGone(child, err);
    child.on('error', (err: NodeJS.ErrnoException) => {
      // ENOENT: `weft` not on PATH. Name the actual fix, same as runWeftJson.
      const reason =
        err.code === 'ENOENT'
          ? 'weft CLI not found on PATH. Install it or add it to your PATH.'
          : err.message;
      fail(new WeftCliError(['parse-server'], null, reason));
    });
    child.on('exit', (code, signal) => {
      fail(new WeftCliError(['parse-server'], code, `parse server exited (code=${code}, signal=${signal})`));
    });

    return child;
  }

  private onLine(child: ChildProcessWithoutNullStreams, line: string): void {
    if (!line.trim()) return;
    let env: ServerResponseEnvelope;
    try {
      env = JSON.parse(line) as ServerResponseEnvelope;
    } catch {
      // A line that is not JSON must still settle SOMETHING: leaving it
      // on the floor stalls the pending request until the 30s timeout.
      // Salvage the id and fail that request now; an id that is no
      // longer pending (aborted, timed out) has nobody waiting, so the
      // line is simply dropped. Only a line with NO id at all cannot
      // be attributed, and then THIS child is torn down (which rejects
      // every pending request) rather than left silently wedged.
      const id = line.match(/"id"\s*:\s*(\d+)/);
      if (id) {
        this.settle(Number(id[1]))?.reject(
          new WeftCliError(['parse-server'], null, `malformed response line: ${line.slice(0, 200)}`),
        );
        return;
      }
      this.onChildGone(child, new WeftCliError(['parse-server'], null, `unparseable response line: ${line.slice(0, 200)}`));
      return;
    }
    const p = this.settle(env.id);
    if (!p) return;
    if (env.error !== undefined) {
      p.reject(new ParseServerError(env.error));
    } else {
      p.resolve(env.payload);
    }
  }

  /** Watch the `weft` binary the child was spawned from; when it changes (a
   *  `weft install` while this window is open), kill the child so the next
   *  request respawns on the NEW binary. Without this the warm server keeps
   *  answering in the previous wire shape and the webview silently renders
   *  holes (fields the old server never emits). Watch failure (binary not
   *  resolvable, fs.watch unsupported) degrades to the old behavior and is
   *  logged, never fatal: the watch is an invalidation aid, not the spawn. */
  private binaryWatcher: fs.FSWatcher | undefined;
  private watchBinary(child: ChildProcessWithoutNullStreams): void {
    this.binaryWatcher?.close();
    this.binaryWatcher = undefined;
    try {
      const resolver = process.platform === 'win32' ? 'where' : 'which';
      const path = execFileSync(resolver, ['weft'], { encoding: 'utf8' }).split('\n')[0].trim();
      if (!path) return;
      this.binaryWatcher = fs.watch(path, () => {
        console.error('[weft parse-server] weft binary changed; restarting the parse server');
        this.onChildGone(child, new WeftCliError(['parse-server'], null, 'weft binary changed; parse server restarted'));
      });
    } catch (err) {
      console.error(`[weft parse-server] cannot watch the weft binary for changes: ${String(err)}`);
    }
  }

  /** The child died, errored, or wedged (timeout). Reap the process, reject
   *  every pending request (clearing their timeout timers), and clear the child
   *  so the next request respawns. Idempotent across the error+exit pair (only
   *  the first call sees `this.child === child`). `kill()` is a harmless no-op
   *  on an already-dead child, so it's correct for all three callers; it's the
   *  timeout path (child alive-but-hung) that actually needs the reap. */
  private onChildGone(child: ChildProcessWithoutNullStreams, err: Error): void {
    if (this.child !== child) return;
    this.child = undefined;
    this.reader?.close();
    this.reader = undefined;
    this.binaryWatcher?.close();
    this.binaryWatcher = undefined;
    child.kill();
    for (const id of [...this.pending.keys()]) {
      this.settle(id)?.reject(err);
    }
  }

  dispose(): void {
    this.disposed = true;
    const child = this.child;
    this.child = undefined;
    this.reader?.close();
    this.reader = undefined;
    this.binaryWatcher?.close();
    this.binaryWatcher = undefined;
    for (const id of [...this.pending.keys()]) {
      this.settle(id)?.reject(new Error('parse server disposed'));
    }
    child?.kill();
  }
}
