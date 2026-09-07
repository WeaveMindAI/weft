// API client for the weft dispatcher's general signal surface.
//
// The dispatcher exposes ONE generic enumeration route
// (`GET /signal-token/signals`, the signal token in `Authorization:
// Bearer`, never in the URL where logs would capture it) and ONE
// generic per-signal route (`POST /signal/{signal_token}`,
// `DELETE /signal/{signal_token}`, addressed by the per-signal fire
// token, whose whole job is to be a URL). Per-kind rendering happens
// inside the listener. The browser extension is the consumer that
// renders Form-kind signals; future consumers (Slack bot, etc) would
// render their kinds the same way against the same routes.
//
// Each token is scoped by the dispatcher (projects + tags). The extension
// just uses the token; scope + tenant checks happen on the dispatcher.

// ------------- Form schema types (mirror dispatcher wire shape) ----------
//
// The dispatcher serializes weft-core's `FormField` directly (camelCase).
// Each field carries a `fieldType` (the catalog field-type id, e.g.
// `text_input`, `approve_reject`), a `render` hint the consumer
// interprets, optional pre-fill `value`, and the source `config`. The
// task page renders purely off `render.component` plus the spec-driven
// flags (`source`, `multiple`, `prefilled`); it never branches on
// `fieldType`, which would re-bake catalog knowledge into a consumer.
// SYNC: FormFieldRender/FormField/FormSchema <->
//       crates/weft-core/src/node.rs (FormFieldRender),
//       crates/weft-core/src/signal/form.rs (FormSchema/FormField),
//       packages/weft-graph/src/protocol.ts (FormFieldRenderWire; a
//       separate pnpm workspace with no dependency between the two, so
//       the TS shape is restated rather than imported)

export interface FormFieldRender {
  component: string;
  source?: 'static' | 'input';
  multiple?: boolean;
  prefilled?: boolean;
}

export interface FormField {
  fieldType: string;
  key: string;
  /// Always on the wire (the Rust side serializes it unconditionally,
  /// falling back to the field key when the author cleared it);
  /// display still guards with `|| key` for a typed-empty label.
  label: string;
  render: FormFieldRender;
  value?: unknown;
  /// Always on the wire, `{}` when the source set nothing.
  config: Record<string, unknown>;
}

export interface FormSchema {
  fields: FormField[];
}

// SYNC: consumer payload keys <->
//       crates/weft-listener/src/kinds/form.rs (FormHandler::render),
//       crates/weft-dispatcher/src/api/signal.rs (the isResume stamp)
export interface PendingTask {
  /// Per-signal token. Identifies the signal end-to-end:
  /// fire = `POST /signal/{token}`, cancel = `DELETE /signal/{token}`.
  /// Replaces the v1 `executionId` field (the dispatcher's `color`
  /// is no longer surfaced to consumers; signal token alone is
  /// sufficient routing).
  token: string;
  nodeId: string;
  /// Wake-signal kind tag (e.g. `form`, `timer`, `api_endpoint`).
  /// Useful for the extension to skip kinds it can't render.
  kind: string;
  /// Free-form consumer label (e.g. `human_in_the_loop`). Set by the
  /// registering node; descriptive metadata the extension can use to
  /// pick a renderer. (No longer a token filter dimension.)
  consumerKind?: string;
  /// Display text. Listener picks a sensible default if the node
  /// didn't set a title.
  title: string;
  description?: string;
  /// Form schema, present for Form-kind signals.
  formSchema?: FormSchema;
  /// TRIGGER (false) vs RESUME task (true). A trigger is an entry point: it
  /// stays listed while the project is active and can be fired repeatedly to
  /// START a run. A resume is a one-shot reply to a paused execution: it
  /// disappears once answered. The list groups on this. Stamped by the
  /// dispatcher's enumeration; absent on very old payloads (treated as a
  /// resume task, the historical behavior).
  isResume?: boolean;
  /// NOT on the wire: stamped by `fetchPendingTasks` after the fetch
  /// (the underscore marks it), so every consumer knows which token,
  /// and so which dispatcher, a task came through.
  _tokenConfig?: ApiToken;
}

export interface ApiToken {
  /// The signal-token string (e.g. `wft-azure-otter-brave-summit-river-maple`).
  /// Shown ONCE at mint; the extension keeps it locally and presents it via
  /// `Authorization: Bearer`.
  token: string;
  /// User-facing name shown in the popup.
  name: string;
  /// Base URL of the dispatcher this token belongs to. Allows the
  /// extension to point at more than one dispatcher from one browser.
  /// Parsed out of the pasted server-qualified address
  /// (`<base>/signal-token/<token>`).
  dispatcherUrl: string;
}

export async function getTokens(): Promise<ApiToken[]> {
  const result = await browser.storage.local.get('apiTokens');
  return (result.apiTokens as ApiToken[]) || [];
}

export async function setTokens(tokens: ApiToken[]): Promise<void> {
  await browser.storage.local.set({ apiTokens: tokens });
}

/// Store a token: a re-paste of an existing token string UPDATES its
/// address (a runtime that moved hosts is the everyday reason to
/// re-paste), releasing the old host's grant when nothing else needs
/// it. An absent `name` keeps the existing one on an update and
/// defaults to a generated one on an insert, so re-pasting with the
/// name box left empty never renames.
export async function addToken(token: Omit<ApiToken, 'name'> & { name?: string }): Promise<void> {
  const tokens = await getTokens();
  const existing = tokens.find(t => t.token === token.token);
  if (!existing) {
    // Default name: the dispatcher's host, the thing the user actually
    // recognizes (a count-based name collides after a removal: two
    // "Token 2"s).
    const name = token.name || defaultTokenName(token.dispatcherUrl);
    await setTokens([...tokens, { ...token, name }]);
    return;
  }
  const oldUrl = existing.dispatcherUrl;
  if (token.name) {
    existing.name = token.name;
  }
  existing.dispatcherUrl = token.dispatcherUrl;
  await setTokens(tokens);
  await releaseHostIfUnused(oldUrl, tokens);
}

export async function removeToken(tokenId: string): Promise<void> {
  const tokens = await getTokens();
  const removed = tokens.find(t => t.token === tokenId);
  const kept = tokens.filter(t => t.token !== tokenId);
  await setTokens(kept);
  if (removed) {
    await releaseHostIfUnused(removed.dispatcherUrl, kept);
  }
}

/// Hand back the host grant of `dispatcherUrl` when no token in
/// `still` needs it, so the extension's reach shrinks with its
/// configuration. The token change it follows already happened, so a
/// refused revoke must not undo it: log and move on, the grant stays
/// until the next release attempt. Peers are matched through the
/// non-throwing `matchPattern`, so one stored token with a broken URL
/// can never veto every future release.
export async function releaseHostIfUnused(
  dispatcherUrl: string,
  still: ApiToken[],
): Promise<void> {
  const pattern = hostPermissionPatternOrNull(dispatcherUrl);
  if (pattern === null) return;
  const stillNeeded = still.some(t => hostPermissionPatternOrNull(t.dispatcherUrl) === pattern);
  if (stillNeeded) return;
  try {
    await browser.permissions.remove({ origins: [pattern] });
  } catch (error) {
    console.warn('[weft] Could not release a host grant:', error);
  }
}

/// What the user sees when they dismiss the browser's permission
/// prompt, wherever the prompt was raised from (adding a token, the
/// Grant-access button): one sentence, so the same decision never
/// reads two ways.
export const GRANT_DECLINED_MESSAGE =
  'Access to that address was declined. The extension cannot reach the runtime '
  + 'without it; grant access to use this token.';

/// The generated display name for a token the user did not name: the
/// dispatcher's host (port included), the part of the address they
/// recognize. Throws on an unparseable URL, which every caller has
/// already parsed upstream; a throw here is a programming error, not
/// a case to paper over.
function defaultTokenName(dispatcherUrl: string): string {
  return new URL(dispatcherUrl).host;
}

/// Whether a task is a TRIGGER: an entry point that stays listed while
/// its project is active and STARTS a run when fired, as opposed to a
/// resume task (a paused run waiting for an answer). THE one place the
/// distinction is decided, because the two need different verbs
/// everywhere: skip/cancel act on an in-flight run a trigger does not
/// have, and "cancelling" a trigger would delete the entry point
/// itself. A payload with no `isResume` (an old runtime) reads as a
/// resume task, the historical default.
export function isTrigger(task: PendingTask): boolean {
  return task.isResume === false;
}

/// `hostPermissionPattern`, answering `null` instead of throwing: for
/// the paths that compare patterns rather than show the user an error.
function hostPermissionPatternOrNull(dispatcherUrl: string): string | null {
  try {
    return hostPermissionPattern(dispatcherUrl);
  } catch {
    return null;
  }
}

/// The host-permission match pattern for a dispatcher URL. Match patterns
/// carry no port (both Chrome and Firefox refuse one), and a portless
/// pattern matches every port on that host, so one grant covers a runtime
/// wherever it binds on that machine. Throws a sentence the user can act
/// on for addresses no browser can grant (non-http schemes, bracketed
/// IPv6 literals), so the caller shows that instead of the browser's raw
/// pattern error.
export function hostPermissionPattern(dispatcherUrl: string): string {
  const url = new URL(dispatcherUrl);
  if (url.protocol !== 'http:' && url.protocol !== 'https:') {
    throw new Error('The runtime address must start with http:// or https://.');
  }
  if (url.hostname.startsWith('[')) {
    throw new Error(
      'Browser extensions cannot be granted access to an IPv6 literal address. '
      + 'Use a hostname or an IPv4 address for the runtime.',
    );
  }
  return `${url.protocol}//${url.hostname}/*`;
}

export interface TokenFailure {
  token: ApiToken;
  /// Why the fetch for this token failed, in a sentence a user can act
  /// on (unreachable, an HTTP status, a malformed body naming the
  /// field). Rendered by the popup's per-token banner, so a runtime
  /// that answered garbage is never reported as "did not answer".
  /// (`detail`, matching the popup's issue rows, where `reason` is
  /// the category tag.)
  detail: string;
}

export interface FetchTasksResult {
  tasks: PendingTask[];
  /// True iff at least one configured token successfully reached
  /// its dispatcher. Drives the popup's "connected" indicator.
  anyReachable: boolean;
  /// Every configured token, whether its fetch succeeded or not: the
  /// popup's settings list, and the set a consumer carrying per-token
  /// state across polls prunes to (a deleted token's state must not
  /// linger forever). One storage read, here, so no consumer re-reads
  /// and races it.
  configured: ApiToken[];
  /// The tokens whose fetch FAILED this round, each with why. Tasks of
  /// a failed token are absent from `tasks` without being gone, so a
  /// consumer tracking per-task state (e.g. already-notified tasks)
  /// must only refresh the state of non-failed tokens and carry the
  /// rest forward.
  failures: TokenFailure[];
}

/// Fetch pending tasks from every configured api_token IN PARALLEL.
/// One round-trip per token; failures are isolated.
export async function fetchPendingTasks(
  { timeoutMs }: { timeoutMs?: number } = {},
): Promise<FetchTasksResult> {
  const tokens = await getTokens();
  if (tokens.length === 0) {
    return { tasks: [], anyReachable: false, configured: [], failures: [] };
  }

  const fetchOne = async (tokenConfig: ApiToken): Promise<PendingTask[]> => {
    const url = `${tokenConfig.dispatcherUrl}/signal-token/signals`;
    const opts: RequestInit = {
      method: 'GET',
      headers: { Authorization: `Bearer ${tokenConfig.token}` },
    };
    if (timeoutMs) opts.signal = AbortSignal.timeout(timeoutMs);
    const resp = await fetch(url, opts);
    if (!resp.ok) throw new Error(`HTTP ${resp.status}`);
    const data = await resp.json();
    // The boundary check: a runtime answering 200 with the wrong shape
    // must land in the failed bucket with a named reason (the popup's
    // per-token banner shows it), never read as "no tasks" or blow up
    // later inside a render.
    if (!Array.isArray(data)) {
      throw new Error('answered 200 but the body was not a task list (a version mismatch?)');
    }
    const tasks = data as PendingTask[];
    for (const task of tasks) {
      if (typeof task?.token !== 'string' || typeof task?.title !== 'string') {
        throw new Error('a task in the list is missing its token or title (a version mismatch?)');
      }
      // The CONTAINER before the fields: a formSchema of the wrong
      // type (a string, an object with no fields array) would slip
      // past a fields-only loop and die later inside the render.
      const schema = task.formSchema;
      if (
        schema !== undefined
        && (typeof schema !== 'object' || schema === null || !Array.isArray(schema.fields))
      ) {
        throw new Error(
          `task '${task.title}' has a malformed form schema (a version mismatch?)`,
        );
      }
      for (const field of task.formSchema?.fields ?? []) {
        if (
          typeof field?.key !== 'string'
          || typeof field?.label !== 'string'
          || typeof field?.render?.component !== 'string'
          || typeof field?.config !== 'object'
          || field.config === null
        ) {
          throw new Error(
            `form field '${field?.key ?? '?'}' of task '${task.title}' is malformed (a version mismatch?)`,
          );
        }
      }
      task._tokenConfig = tokenConfig;
    }
    return tasks;
  };

  const results = await Promise.allSettled(tokens.map(fetchOne));
  // Deduped by SIGNAL token at the one boundary every consumer reads:
  // two api tokens scoped onto one dispatcher (a re-mint) enumerate
  // the same signals, and without this the badge counts a task twice,
  // the lists show it twice, and its notification fires twice. First
  // token wins the `_tokenConfig` stamp, which is also the CREDENTIAL
  // the destructive verbs use (cancelRun / clearAll send its bearer),
  // so a narrower-scoped token listed first makes those fail loudly
  // where the wider one would have succeeded.
  const byToken = new Map<string, PendingTask>();
  const failures: TokenFailure[] = [];
  results.forEach((res, i) => {
    if (res.status === 'fulfilled') {
      for (const task of res.value) {
        if (!byToken.has(task.token)) byToken.set(task.token, task);
      }
    } else {
      const detail = res.reason instanceof Error ? res.reason.message : String(res.reason);
      failures.push({ token: tokens[i], detail });
    }
  });
  return {
    tasks: [...byToken.values()],
    anyReachable: failures.length < tokens.length,
    configured: tokens,
    failures,
  };
}

/// The link a form's stored file is shown through. A stored file
/// reaches the task as its facts only (`{ mimeType, sizeBytes,
/// filename }`, no `url`): the link is minted by the dispatcher the
/// moment it is asked for and lives an hour, so a task rendered a
/// month after it parked still shows its image, and a refresh of the
/// page asks again. A file that is gone answers 410 and one that could
/// not be reached 502, and the renderer shows either in the image's
/// place.
/// Scoped like the listing: the api token as bearer, the task one it
/// lists, the field one the form declares.
// SYNC: TaskFileLink <-> crates/weft-dispatcher/src/api/signal.rs (SignalFileLink), crates/weft-core/src/signal/form.rs (consumer_file_value, whose URL-backed arm publishes the same four keys)
export interface TaskFileLink {
  url: string;
  mimeType: string;
  sizeBytes: number;
  filename: string;
}

/// The address a browser can put in an `img src`, or undefined. Only a
/// web or data address counts: a field's plain string is text, and a
/// storage key is weft's internal address for a file, which means
/// nothing to a browser and is not a consumer's to see. An empty
/// string is nothing at all, and reads as a field with no value.
///
/// `blob:` is deliberately absent. Such an address only works inside
/// the one page that minted it, and a form's values arrive as JSON off
/// the wire, so one appearing here could only ever draw a broken
/// picture where the page could instead say it has no address to use.
export function imageSourceOf(value: unknown): string | undefined {
  if (value === '') return undefined;
  const web = (s: string): boolean => {
    const scheme = s.slice(0, s.indexOf(':') + 1).toLowerCase();
    return scheme === 'https:' || scheme === 'http:' || scheme === 'data:';
  };
  if (typeof value === 'string') return web(value) ? value : undefined;
  const url = (value as Record<string, unknown> | null | undefined)?.url;
  return typeof url === 'string' && web(url) ? url : undefined;
}

/// Is this field value weft's projection of a stored file: the three
/// facts a stored file leaves behind, and no `url` (a URL-backed file
/// carries its own link and needs no door). All three are required, so
/// ordinary data that happens to carry a `filename` is not mistaken for
/// a file and sent to a door that has none.
// SYNC: isStoredFileValue <-> crates/weft-core/src/signal/form.rs (consumer_file_value, which builds this shape)
export function isStoredFileValue(value: unknown): boolean {
  const v = value as Record<string, unknown> | null | undefined;
  return (
    typeof v === 'object' && v !== null && v.url === undefined
    && typeof v.mimeType === 'string'
    && typeof v.sizeBytes === 'number'
    && typeof v.filename === 'string'
  );
}

export async function fetchTaskFile(task: PendingTask, fieldKey: string): Promise<TaskFileLink> {
  const tokenConfig = task._tokenConfig;
  if (!tokenConfig) throw new Error('Task missing token configuration');
  const url = `${tokenConfig.dispatcherUrl}/signal-token/signals/${encodeURIComponent(task.token)}/files/${encodeURIComponent(fieldKey)}`;
  const resp = await fetch(url, { headers: { Authorization: `Bearer ${tokenConfig.token}` } });
  if (!resp.ok) {
    const text = await resp.text();
    throw new Error(text || `HTTP ${resp.status}`);
  }
  const link = (await resp.json()) as TaskFileLink;
  if (typeof link?.url !== 'string') {
    throw new Error('the files door answered without a url (a version mismatch?)');
  }
  return link;
}

/// Submit a form payload (HumanQuery completion / human trigger
/// fire). The signal token alone is sufficient routing; no extra
/// auth header is needed because possessing the signal token is the
/// authorization. (Cancel/dismiss requires the api_token because
/// they're destructive across the whole signal pool.)
export async function submitTask(
  task: PendingTask,
  input: Record<string, unknown>,
): Promise<void> {
  const tokenConfig = task._tokenConfig;
  if (!tokenConfig) throw new Error('Task missing token configuration');
  const url = `${tokenConfig.dispatcherUrl}/signal/${task.token}`;
  const resp = await fetch(url, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify(input),
  });
  if (!resp.ok) {
    const text = await resp.text();
    throw new Error(`HTTP ${resp.status}: ${text}`);
  }
}

/// Skip ONE task: resume its lane with null. Sibling lanes of
/// the same execution keep going. Most upstream code patterns
/// auto-skip on null inputs (downstream null-propagation), so
/// this is the "I don't want to answer this one, do whatever"
/// action. Auth: signal token alone (knowing it = permission).
export async function skipTask(task: PendingTask): Promise<void> {
  const tokenConfig = task._tokenConfig;
  if (!tokenConfig) throw new Error('Task missing token configuration');
  const url = `${tokenConfig.dispatcherUrl}/signal/${task.token}/skip`;
  const resp = await fetch(url, { method: 'POST' });
  if (!resp.ok) {
    const text = await resp.text();
    throw new Error(`HTTP ${resp.status}: ${text}`);
  }
}

/// Cancel the WHOLE RUN this task belongs to. Every sibling task
/// of the same execution dies (5 parallel HumanQueries → all 5
/// dropped). Worker stops, NodeCancelled + ExecutionFailed
/// journaled. The user can still inspect the run in the journal
/// afterward to debug why they cancelled.
///
/// Auth: api_token via Authorization header. Token must be ≥
/// project-scoped (no kind / tag restrictions). Tag-scoped tokens
/// are rejected; they can only skip their visible signals.
export async function cancelRun(task: PendingTask): Promise<void> {
  const tokenConfig = task._tokenConfig;
  if (!tokenConfig) throw new Error('Task missing token configuration');
  const url = `${tokenConfig.dispatcherUrl}/signal/${task.token}`;
  const resp = await fetch(url, {
    method: 'DELETE',
    headers: { Authorization: `Bearer ${tokenConfig.token}` },
  });
  if (!resp.ok) {
    const text = await resp.text();
    throw new Error(`HTTP ${resp.status}: ${text}`);
  }
}

/// Clear all visible tasks for one api_token. Cancels every
/// distinct execution this token sees (one cancel per color, not
/// per task). Same scope rule as cancelRun: token must be
/// ≥ project-scoped.
export async function clearAll(token: ApiToken): Promise<{
  colorsCancelled: number;
  entrySignalsDropped: number;
}> {
  const url = `${token.dispatcherUrl}/signal-token/signals`;
  const resp = await fetch(url, {
    method: 'DELETE',
    headers: { Authorization: `Bearer ${token.token}` },
  });
  if (!resp.ok) {
    const text = await resp.text();
    throw new Error(`HTTP ${resp.status}: ${text}`);
  }
  const body = (await resp.json()) as {
    colors_cancelled?: number;
    entry_signals_dropped?: number;
  };
  return {
    colorsCancelled: body.colors_cancelled ?? 0,
    entrySignalsDropped: body.entry_signals_dropped ?? 0,
  };
}

