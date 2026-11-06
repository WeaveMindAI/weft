// ------------- A conversation a form shows (`chat_history`) -------------
//
// The field's value is a file holding a `ChatHistory`: a list of
// messages in the provider-agnostic shape the LLM package declares.
// This module reads that file's JSON into what the page draws, and
// refuses a file that is not a conversation, naming what is wrong,
// instead of drawing half of it.
// SYNC: ChatMessage/ChatContentPart/ToolCall <->
//       catalog/ai/llm/metadata.json (types.ChatHistory and its members),
//       catalog/ai/llm/chat.rs (the stored form those types mirror)

import { fetchTaskFile, imageSourceOf, isStoredFileValue, type FormField, type PendingTask } from './api';

/// One piece of a message as the page draws it.
export type ChatPart =
  | { kind: 'text'; text: string }
  /// `url` is undefined for a file weft stored: the conversation holds
  /// a reference only weft can open, and no door hands a consumer a link
  /// to a file nested inside another file.
  | { kind: 'image' | 'audio' | 'video'; url: string | undefined; filename: string | undefined };

export interface ChatToolCall {
  id: string;
  name: string;
  /// The arguments as the model wrote them, pretty-printed when they
  /// read as JSON.
  arguments: string;
}

export interface ChatMessageView {
  role: string;
  name: string | undefined;
  parts: ChatPart[];
  toolCalls: ChatToolCall[];
  /// A tool message: the call it answers, its tool's name when an
  /// earlier assistant message made that call.
  answers: { id: string; toolName: string | undefined } | undefined;
}

/// A text longer than this starts collapsed where the page collapses
/// long texts (a tool's result, a system prompt).
export const LONG_TEXT_CHARS = 400;

export function isLongText(text: string): boolean {
  return text.length > LONG_TEXT_CHARS || text.split('\n').length > 8;
}

/// The link a field's conversation file is read through: a file weft
/// stored asks the task's files door (a fresh link each time), a file
/// that lives at a web address carries it.
async function conversationLink(task: PendingTask, field: FormField): Promise<string> {
  if (isStoredFileValue(field.value)) return (await fetchTaskFile(task, field.key)).url;
  const url = (field.value as Record<string, unknown> | null | undefined)?.url;
  if (typeof url === 'string' && url !== '') return url;
  throw new Error('This field does not hold a conversation file.');
}

/// A conversation as it was read: the file's text, kept so the next
/// read can tell whether anything changed, and its messages.
export interface LoadedConversation {
  text: string;
  messages: ChatMessageView[];
}

/// The conversation a `chat_history` field shows, read from its file.
/// The file is fetched each time (a stored file's link is fresh only
/// once), but when its text is the one `previous` was read from, the
/// messages read then are handed back as they are: a long conversation
/// is parsed once, not again every time the task list refreshes.
export async function loadConversation(
  task: PendingTask,
  field: FormField,
  previous: LoadedConversation | undefined,
): Promise<LoadedConversation> {
  const url = await conversationLink(task, field);
  let resp: Response;
  try {
    resp = await fetch(url);
  } catch (e) {
    // A network refusal carries no status; say which file it was.
    const detail = e instanceof Error ? e.message : String(e);
    throw new Error(`Could not read the conversation file: ${detail}`);
  }
  if (!resp.ok) throw new Error(`Could not read the conversation file (HTTP ${resp.status}).`);
  const text = await resp.text();
  if (previous !== undefined && previous.text === text) return previous;
  let json: unknown;
  try {
    json = JSON.parse(text);
  } catch {
    throw new Error('The conversation file is not JSON.');
  }
  return { text, messages: readConversation(json) };
}

/// Whether `next` is `previous` carried on: it still holds, at the same
/// places, the messages `previous` holds from `from` on (the ones on
/// screen) and its first message. A conversation grows only at its end,
/// so any other change means it was started over, whatever its length.
/// Only those messages are compared, never the thousands above them.
export function continues(previous: ChatMessageView[], next: ChatMessageView[], from: number): boolean {
  if (next.length < previous.length) return false;
  const same = (i: number) => JSON.stringify(previous[i]) === JSON.stringify(next[i]);
  if (previous.length > 0 && !same(0)) return false;
  for (let i = from; i < previous.length; i++) {
    if (!same(i)) return false;
  }
  return true;
}

/// A `ChatHistory` value as the page draws it. Throws naming the first
/// message that is not a message.
export function readConversation(value: unknown): ChatMessageView[] {
  if (!Array.isArray(value)) {
    throw new Error('The file does not hold a conversation (a list of messages).');
  }
  const toolNames = new Map<string, string>();
  return value.map((raw, i) => {
    const at = `message ${i + 1}`;
    const m = asRecord(raw, at);
    if (typeof m.role !== 'string') throw new Error(`The conversation's ${at} has no role.`);
    const toolCalls = readToolCalls(m.tool_calls, at);
    for (const call of toolCalls) toolNames.set(call.id, call.name);
    const callId = m.tool_call_id;
    if (callId !== undefined && typeof callId !== 'string') {
      throw new Error(`The conversation's ${at} has a tool_call_id that is not text.`);
    }
    return {
      role: m.role,
      name: typeof m.name === 'string' ? m.name : undefined,
      parts: readContent(m.content, at),
      toolCalls,
      answers: callId === undefined ? undefined : { id: callId, toolName: toolNames.get(callId) },
    };
  });
}

function asRecord(value: unknown, at: string): Record<string, unknown> {
  if (typeof value !== 'object' || value === null || Array.isArray(value)) {
    throw new Error(`The conversation's ${at} is not a record.`);
  }
  return value as Record<string, unknown>;
}

function readToolCalls(value: unknown, at: string): ChatToolCall[] {
  if (value === undefined || value === null) return [];
  if (!Array.isArray(value)) throw new Error(`The conversation's ${at} has tool_calls that are not a list.`);
  return value.map((raw, j) => {
    const c = asRecord(raw, `${at}'s tool call ${j + 1}`);
    if (typeof c.name !== 'string' || typeof c.arguments !== 'string') {
      throw new Error(`The conversation's ${at} has a tool call without a name and arguments.`);
    }
    return { id: typeof c.id === 'string' ? c.id : '', name: c.name, arguments: prettyArguments(c.arguments) };
  });
}

function prettyArguments(args: string): string {
  try {
    return JSON.stringify(JSON.parse(args), null, 2);
  } catch {
    return args;
  }
}

function readContent(value: unknown, at: string): ChatPart[] {
  if (value === undefined || value === null) return [];
  if (typeof value === 'string') return value === '' ? [] : [{ kind: 'text', text: value }];
  if (!Array.isArray(value)) throw new Error(`The conversation's ${at} has content that is neither text nor a list of parts.`);
  return value.map((raw, j): ChatPart => {
    const p = asRecord(raw, `${at}'s part ${j + 1}`);
    switch (p.type) {
      case 'text':
        return { kind: 'text', text: typeof p.text === 'string' ? p.text : '' };
      case 'image_url':
        return media('image', (p.image_url as Record<string, unknown> | undefined)?.url);
      case 'input_audio':
        return media('audio', (p.input_audio as Record<string, unknown> | undefined)?.data);
      case 'video_url':
        return media('video', (p.video_url as Record<string, unknown> | undefined)?.url);
      default:
        throw new Error(`The conversation's ${at} has a part of an unknown type (${String(p.type)}).`);
    }
  });
}

/// The weft file markers a media slot may hold, one per kind.
// SYNC: file marker keys <-> crates/weft-core/src/weft_type.rs (FileKind::marker_key)
const MARKERS = ['__weft_image__', '__weft_audio__', '__weft_video__', '__weft_blob__'];

/// One media slot: a web or data address as is; a weft file value opens
/// through its own `url` when it lives at one, and a stored one names
/// its file (see `ChatPart`).
function media(kind: 'image' | 'audio' | 'video', slot: unknown): ChatPart {
  if (typeof slot === 'string') return { kind, url: imageSourceOf(slot), filename: undefined };
  const obj = slot as Record<string, unknown> | null | undefined;
  const marker = MARKERS.find((m) => typeof obj?.[m] === 'object' && obj?.[m] !== null);
  const payload = (marker ? obj![marker] : obj) as Record<string, unknown> | null | undefined;
  const filename = typeof payload?.filename === 'string' ? payload.filename : undefined;
  return { kind, url: imageSourceOf(payload), filename };
}
