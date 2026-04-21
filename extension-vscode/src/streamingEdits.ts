// Streaming edit primitive, exposed as a named command other
// extensions (Tangle, future AI tooling) can invoke. The AI streams
// its response into a StreamingEditSession; each complete
// <<<<<<< SEARCH / ======= / >>>>>>> REPLACE block is applied to
// the target document the moment its closing marker arrives.
//
// Ported from v1's weft-patch.ts. Same grammar, same search-and-
// replace-by-text-match semantics. Different host API: v1 owned an
// in-memory string; here we apply to a vscode.TextDocument via
// TextEditor.edit().

import * as vscode from 'vscode';

const SEARCH_MARKER = '<<<<<<< SEARCH';
const DIVIDER = '=======';
const REPLACE_MARKER = '>>>>>>> REPLACE';

export interface StreamingEditHandle {
  /** Append a chunk of raw streamed text. Complete blocks apply
   *  immediately; incomplete blocks stay buffered until the next
   *  chunk closes them. */
  pushChunk(chunk: string): Promise<void>;
  /** Call when the upstream stream is done. Flushes any trailing
   *  buffered (non-applied) text; no-op if nothing pending. */
  end(): Promise<void>;
}

interface SessionState {
  doc: vscode.TextDocument;
  buffer: string;
  disabled: boolean;
}

/** Open a new streaming edit session against the given document.
 *  The session stays live until `end()` is called or the document
 *  is closed. */
export function openStreamingEdit(doc: vscode.TextDocument): StreamingEditHandle {
  const state: SessionState = { doc, buffer: '', disabled: false };
  return {
    pushChunk: (chunk) => handleChunk(state, chunk),
    end: async () => {
      // Buffer contains no complete block; discard. We never emit a
      // partial patch.
      state.buffer = '';
    },
  };
}

async function handleChunk(state: SessionState, chunk: string): Promise<void> {
  if (state.disabled) return;
  state.buffer += chunk;

  // Extract every complete block from the buffer, apply it, slice
  // it out, and loop. Stop when no complete block is found.
  while (true) {
    const block = extractFirstBlock(state.buffer);
    if (!block) return;
    const { searchText, replaceText, consumedThrough } = block;
    state.buffer = state.buffer.slice(consumedThrough);
    const applied = await applyBlock(state.doc, searchText, replaceText);
    if (!applied) {
      // Abort on first failed match. Failing silently would leave
      // the file mid-patched; continuing blindly could corrupt.
      state.disabled = true;
      void vscode.window.showErrorMessage(
        `Weft: streaming edit failed. SEARCH block not found in ${state.doc.fileName}.`,
      );
      return;
    }
  }
}

interface Block {
  searchText: string;
  replaceText: string;
  consumedThrough: number; // exclusive end index into the buffer
}

function extractFirstBlock(buf: string): Block | null {
  const searchIdx = buf.indexOf(SEARCH_MARKER);
  if (searchIdx < 0) return null;
  const dividerIdx = buf.indexOf(DIVIDER, searchIdx + SEARCH_MARKER.length);
  if (dividerIdx < 0) return null;
  const replaceIdx = buf.indexOf(REPLACE_MARKER, dividerIdx + DIVIDER.length);
  if (replaceIdx < 0) return null;

  // Strip one leading newline after each marker if present.
  const searchStart = buf.indexOf('\n', searchIdx + SEARCH_MARKER.length) + 1;
  const dividerStart = dividerIdx;
  const replaceStart = buf.indexOf('\n', dividerIdx + DIVIDER.length) + 1;
  const replaceEnd = replaceIdx;
  const blockEnd = replaceIdx + REPLACE_MARKER.length;

  const searchText = stripTrailingNewline(buf.slice(searchStart, dividerStart));
  const replaceText = stripTrailingNewline(buf.slice(replaceStart, replaceEnd));
  return { searchText, replaceText, consumedThrough: blockEnd };
}

function stripTrailingNewline(s: string): string {
  return s.endsWith('\n') ? s.slice(0, -1) : s;
}

async function applyBlock(
  doc: vscode.TextDocument,
  searchText: string,
  replaceText: string,
): Promise<boolean> {
  const text = doc.getText();
  let idx = text.indexOf(searchText);
  if (idx < 0) {
    // Fall back to trim-tolerant match (both sides trimmed). v1
    // does the same; otherwise whitespace drift breaks patches.
    const trimmed = searchText.trim();
    if (trimmed) {
      idx = text.indexOf(trimmed);
    }
    if (idx < 0) return false;
  }
  const startPos = doc.positionAt(idx);
  const endPos = doc.positionAt(idx + searchText.length);
  const edit = new vscode.WorkspaceEdit();
  edit.replace(doc.uri, new vscode.Range(startPos, endPos), replaceText);
  return vscode.workspace.applyEdit(edit);
}

/** Register the command other extensions can call via
 *  vscode.commands.executeCommand('weft.streamingEdit.open', docUri). */
export function registerStreamingEditApi(): vscode.Disposable {
  return vscode.commands.registerCommand(
    'weft.streamingEdit.open',
    (uri: vscode.Uri): StreamingEditHandle | undefined => {
      const doc = vscode.workspace.textDocuments.find(
        (d) => d.uri.toString() === uri.toString(),
      );
      if (!doc) {
        void vscode.window.showErrorMessage(`weft.streamingEdit: no open document at ${uri}`);
        return undefined;
      }
      const enabled = vscode.workspace
        .getConfiguration('weft.ai')
        .get<boolean>('streamingEditsEnabled', true);
      if (!enabled) return undefined;
      return openStreamingEdit(doc);
    },
  );
}
