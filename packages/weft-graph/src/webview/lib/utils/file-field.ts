/** A field whose value a `@file` / `@asset` marker names: what it shows,
 *  whether it can be edited, and where an edit goes. One set of rules for
 *  every box that draws port fields (a node's body, a group's or a loop's
 *  config strip), read off the port literals the host bridge puts each
 *  marker into and the host's `fileContents` map. */

import { ownValue } from '../types';
import { isFileRefValue } from '../value-format';
import { isScopeKey } from './storage-key';
import { typeReferencesFile } from '../../../protocol';
import type { FileContent } from '../../../protocol';

export interface TextFileRef {
  path: string;
  type: string;
  marker: 'file' | 'asset';
}

/** Resolved state of a file-backed field. `loading` = content not yet
 *  delivered (brief, transient). `error` = the file couldn't be read
 *  (fail loudly, no fallback). */
export interface FileFieldState {
  path: string;
  marker: 'file' | 'asset';
  content?: string;
  error?: string;
  loading: boolean;
}

/** If the port literal for `key` is a `@file`/`@asset` marker whose
 *  CONTENT is text the host ships (both markers with text types), its
 *  ref. A file-typed `@asset` is NOT: the marker itself is the field's
 *  value (the file-drop field sets/clears it); nothing text-shaped exists
 *  to display, and routing its edits into a file write would clobber the
 *  media file. */
export function textFileRefOf(portLiterals: Record<string, unknown>, key: string): TextFileRef | null {
  const v = ownValue(portLiterals, key);
  if (!isFileRefValue(v)) return null;
  return typeReferencesFile(v.__weftFileRef.type) ? null : v.__weftFileRef;
}

/** A text `@asset` whose source is a URL or a stored-file key has no file
 *  on disk for the host to read: the build fetches it. The field shows
 *  the source itself, read-only. */
export function remoteTextSource(portLiterals: Record<string, unknown>, key: string): string | null {
  const ref = textFileRefOf(portLiterals, key);
  if (!ref || ref.marker !== 'asset') return null;
  return /^https?:\/\//.test(ref.path) || isScopeKey(ref.path) ? ref.path : null;
}

export function fileFieldState(
  portLiterals: Record<string, unknown>,
  fileContents: Record<string, FileContent> | undefined,
  key: string,
): FileFieldState | null {
  const ref = textFileRefOf(portLiterals, key);
  if (!ref) return null;
  // Fetched at build, never by the host: the source is the display.
  const remote = remoteTextSource(portLiterals, key);
  if (remote !== null) return { path: ref.path, marker: 'asset', content: remote, loading: false };
  const entry = ownValue(fileContents, ref.path);
  // Undefined (not delivered) OR an explicit `{loading}` (bytes still being
  // fetched lazily) both render the non-interactive loading state.
  if (entry === undefined || 'loading' in entry) return { path: ref.path, marker: ref.marker, loading: true };
  if ('error' in entry) return { path: ref.path, marker: ref.marker, error: entry.error, loading: false };
  return { path: ref.path, marker: ref.marker, content: entry.content, loading: false };
}

/** Read-only when the content isn't loaded yet (its display is a status
 *  string, not editable content) or when the ref is an `@asset`
 *  (pull-only by contract: nothing ever writes back). */
export function fileFieldReadonly(state: FileFieldState | null): boolean {
  return state ? state.content === undefined || state.marker === 'asset' : false;
}

/** What the field's control shows: the file's content, or a loading
 *  status. `undefined` for a field no file backs. A read error is not a
 *  value: `fileFieldNotice` replaces the control with it. */
export function fileFieldDisplay(state: FileFieldState | null): string | undefined {
  if (!state) return undefined;
  if (state.content !== undefined) return state.content;
  return `loading ${state.path}...`;
}

/** The failure to show IN PLACE OF the field's control when its file
 *  could not be read, so nobody edits an error message as content. */
export function fileFieldNotice(state: FileFieldState | null): string | undefined {
  if (!state || state.error === undefined) return undefined;
  return `cannot read ${state.path}: ${state.error}`;
}

/** Why a read-only file-backed field refuses an edit. */
export function fileFieldReadonlyReason(state: FileFieldState): string {
  return state.marker === 'asset'
    ? `This field pulls its value from @asset(${state.path}); it can't be edited here. Switch it to @file (the badge next to the label) to edit.`
    : `Cannot edit ${state.path}: ${state.error ?? 'still loading'}`;
}

/** Where an edit of a file-backed field goes: the referenced file's new
 *  content, never the weft source (the marker stays as written). An
 *  `error` when the field cannot take the edit. A cleared box (null)
 *  empties the file: there is no "unset" for file-backed content, the
 *  file IS the value. */
export function fileFieldSave(state: FileFieldState, value: unknown): { path: string; content: string } | { error: string } {
  if (state.marker === 'asset') return { error: `${state.path} is an @asset (read-only); use @file for an editable reference` };
  // Editing while loading or on a read error must not write (it would
  // clobber the file with a status string).
  if (state.content === undefined) return { error: `Cannot edit ${state.path}: ${state.error ?? 'still loading'}` };
  const content = value === null || value === undefined ? '' : typeof value === 'string' ? value : JSON.stringify(value, null, 2);
  return { path: state.path, content };
}

/** The port literals with `key`'s text-file marker flipped between
 *  `@file` (editable, writes back) and `@asset` (pull-only), or an error
 *  for a remote source (a URL, a stored key), which has nothing to write
 *  back to. Null when `key` holds no marker. */
export function flipFileMarker(
  portLiterals: Record<string, unknown>,
  key: string,
): { portLiterals: Record<string, unknown> } | { error: string } | null {
  const v = ownValue(portLiterals, key);
  if (!isFileRefValue(v)) return null;
  if (remoteTextSource(portLiterals, key) !== null) {
    return { error: `@asset(${v.__weftFileRef.path}) is fetched at build; a URL or a stored file has no file to edit back, so it stays @asset.` };
  }
  const r = v.__weftFileRef;
  const flipped = { __weftFileRef: { ...r, marker: r.marker === 'file' ? ('asset' as const) : ('file' as const) } };
  return { portLiterals: { ...portLiterals, [key]: flipped } };
}

/** A toast per field at most every 4 seconds, so held-down keys in a
 *  read-only field don't stack them. */
export function throttledPerKey(show: (message: string) => void): (key: string, message: string) => void {
  const shownAt = new Map<string, number>();
  return (key, message) => {
    const now = Date.now();
    if (now - (shownAt.get(key) ?? 0) < 4000) return;
    shownAt.set(key, now);
    show(message);
  };
}
