import { readFileSync, writeFileSync, renameSync, existsSync } from 'fs';
import { join } from 'path';

// The store's files in its directory, named here and nowhere else: the
// message history, the ids it has seen, and the temporary each flush
// writes before renaming it into place.
const MESSAGES_FILE = 'messages.json';
const SEEN_FILE = `${MESSAGES_FILE}.seen`;

/** Whether a file in the store's directory is one the store writes. */
export function ownsFile(name) {
  return [MESSAGES_FILE, SEEN_FILE].some((file) => name === file || name === `${file}.tmp`);
}


/**
 * In-memory message store, keyed by chatId, with optional disk persistence.
 *
 * Populated from two sources:
 *   1. Initial history sync (`messaging-history.set`), fires on connection,
 *      includes messages from before the server started.
 *   2. Live messages (`messages.upsert`), new messages as they arrive.
 *
 * Stores raw Baileys WAMessage protobufs so media can be lazy-downloaded
 * at query time via `downloadMediaMessage`. The protobuf is small (few KB);
 * the actual bytes are only fetched when a node asks for them through the
 * `/media/:messageId` route (the receive node for a live message, the
 * fetch-media node for one out of history). The live SSE path and the
 * fetchMessages action carry text and captions only.
 *
 * Each chat keeps at most `maxPerChat` messages, the newest by timestamp.
 * A late delivery older than a full chat is kept when it arrives (the
 * trigger fires for it, and its `/media` has to be there), but it is the
 * oldest message the chat holds, so the chat's next message evicts it.
 * Its media is served until then; after that it is gone like any evicted
 * message.
 *
 * Which ids the store has already seen is kept apart from the messages:
 * evicting a message never forgets its id, so a redelivery of an evicted
 * message never fires the trigger again. The seen ids are bounded too
 * (`maxSeen`, oldest forgotten first), far above what the chats hold.
 *
 * If `persistDir` is provided, the messages are loaded from its
 * `messages.json` on construction and flushed to it (debounced) after
 * mutations, and the seen ids the same way from `messages.json.seen`.
 */
export class MessageStore {
  constructor(maxPerChat = 500, persistDir = null, maxSeen = 200_000) {
    // Eviction keeps the message just added, so a chat holds at least one.
    if (!(maxPerChat >= 1)) throw new RangeError(`maxPerChat must be at least 1, got ${maxPerChat}`);
    if (!(maxSeen >= maxPerChat)) throw new RangeError(`maxSeen must be at least maxPerChat (${maxPerChat}), got ${maxSeen}`);
    this.maxPerChat = maxPerChat;
    this.maxSeen = maxSeen;
    this.persistDir = persistDir;
    this.persistPath = persistDir ? join(persistDir, MESSAGES_FILE) : null;
    /** @type {Map<string, Array<Object>>} chatId -> sorted array of raw WAMessages */
    this.chats = new Map();
    /** @type {Set<string>} every messageId ever added, oldest first, never pruned by eviction */
    this.seen = new Set();
    this._dirty = false;
    this._flushTimer = null;
    /** Resolves when the initial history sync completes (or times out). */
    this._historyReady = null;
    this._resolveHistoryReady = null;

    // Load persisted messages from disk if available
    if (persistDir) {
      this._loadFromDisk();
    }
  }

  /**
   * Ingest one raw Baileys WAMessage. Safe to call with the same message
   * more than once (deduped by messageId).
   *
   * Returns whether this call is the first time the store knows the
   * message's content: true for a new message that has content, or for
   * the real copy of a placeholder; false for a repeat, a placeholder,
   * or a message the store already held with its content (including one
   * loaded from disk or from history sync). The trigger fires on true,
   * so it fires once per message and never for what arrived before.
   *
   * A copy with no content never hides the real one. Baileys announces a
   * message it could not decrypt yet as a placeholder with `message`
   * null, asks the sender's phone to resend, and delivers the real copy
   * later under the SAME id. That copy replaces the placeholder, or the
   * store would serve `/media` an empty message for a message it had.
   */
  add(msg) {
    const chatId = msg.key?.remoteJid;
    const msgId = msg.key?.id;
    if (!chatId || !msgId) return false;
    if (this.seen.has(msgId)) {
      const list = this.chats.get(chatId) ?? [];
      const at = list.findIndex((m) => m.key?.id === msgId);
      if (at < 0 || list[at].message || !msg.message) return false;
      list[at] = msg;
      this._markDirty();
      return true;
    }
    this._see(msgId);

    if (!this.chats.has(chatId)) {
      this.chats.set(chatId, []);
    }

    const list = this.chats.get(chatId);
    list.push(msg);

    // Keep sorted by timestamp ascending
    list.sort((a, b) => toNumber(a.messageTimestamp) - toNumber(b.messageTimestamp));

    // Evict the oldest past capacity, never the message just added: a
    // late delivery older than everything stored is still the message
    // the trigger fires for, and `/media` has to find it.
    while (list.length > this.maxPerChat) {
      list.splice(list[0] === msg ? 1 : 0, 1);
    }

    this._markDirty();
    return !!msg.message;
  }

  /**
   * Bulk-ingest messages (e.g. from messaging-history.set).
   */
  addBatch(messages) {
    for (const msg of messages) {
      this.add(msg);
    }
  }

  /**
   * Get the last `count` raw WAMessages for a chat, oldest first.
   */
  getRawMessages(chatId, count) {
    const list = this.chats.get(chatId);
    if (!list || list.length === 0) return [];
    return list.slice(-count);
  }

  /**
   * Get the oldest raw WAMessage for a chat (needed as cursor for fetchMessageHistory).
   */
  getOldestMessage(chatId) {
    const list = this.chats.get(chatId);
    if (!list || list.length === 0) return null;
    return list[0];
  }

  /**
   * How many messages are stored for a chat.
   */
  count(chatId) {
    return this.chats.get(chatId)?.length || 0;
  }

  /**
   * Get all chatIds that have stored messages.
   */
  getChatIds() {
    return [...this.chats.keys()];
  }

  /**
   * Find a raw WAMessage by its messageId across all chats.
   * Used by the /media/:messageId endpoint to look up media for download.
   */
  findByMessageId(messageId) {
    for (const list of this.chats.values()) {
      const msg = list.find(m => m.key?.id === messageId);
      if (msg) return msg;
    }
    return null;
  }

  /**
   * Total messages across all chats.
   */
  totalCount() {
    let n = 0;
    for (const list of this.chats.values()) n += list.length;
    return n;
  }

  /**
   * Signal that the initial history sync has completed.
   * Called by bridge after the first `messaging-history.set` fires.
   */
  markHistoryReady() {
    if (this._resolveHistoryReady) {
      this._resolveHistoryReady();
      this._resolveHistoryReady = null;
    }
  }

  /**
   * Wait for initial history sync to complete (with timeout).
   * Returns immediately if history is already ready or if there are enough
   * messages for the given chatId.
   */
  async waitForHistory(chatId, needed, timeoutMs = 6000) {
    if (this.count(chatId) >= needed) return;
    if (!this._historyReady) {
      this._historyReady = new Promise((resolve) => {
        this._resolveHistoryReady = resolve;
        // Auto-resolve after timeout
        setTimeout(() => {
          this._resolveHistoryReady = null;
          resolve();
        }, timeoutMs);
      });
    }
    await this._historyReady;
  }

  /** Flush immediately (e.g. on shutdown). */
  flushSync() {
    clearTimeout(this._flushTimer);
    this._flushTimer = null;
    if (!this.persistPath || !this._dirty) return;
    this._flushToDisk();
  }

  // ── Internal ──

  /** Remember an id, forgetting the oldest one past `maxSeen`. */
  _see(msgId) {
    this.seen.add(msgId);
    if (this.seen.size > this.maxSeen) {
      this.seen.delete(this.seen.values().next().value);
    }
  }

  _seenPath() {
    return join(this.persistDir, SEEN_FILE);
  }

  _markDirty() {
    if (!this.persistPath) return;
    this._dirty = true;
    if (!this._flushTimer) {
      this._flushTimer = setTimeout(() => {
        this._flushTimer = null;
        this._flushToDisk();
      }, 5000);
    }
  }

  _flushToDisk() {
    if (!this._dirty) return;
    try {
      const data = {};
      for (const [chatId, msgs] of this.chats.entries()) {
        data[chatId] = msgs;
      }
      writeAtomic(this.persistPath, JSON.stringify(data));
      writeAtomic(this._seenPath(), JSON.stringify([...this.seen]));
      this._dirty = false;
      const total = this.totalCount();
      console.log(`[message-store] Flushed ${total} messages to disk`);
    } catch (err) {
      console.error('[message-store] Failed to flush to disk:', err.message);
    }
  }

  _loadFromDisk() {
    // The seen ids first, oldest first, so what the messages add below
    // lands after them. A store flushed before the ids were persisted has
    // no `.seen` file; its messages' ids are then all it knows.
    // A `.seen` file that is there but does not read stops the store:
    // going on without it would fire again every message still in the
    // chats' history.
    if (existsSync(this._seenPath())) {
      let ids;
      try {
        ids = JSON.parse(readFileSync(this._seenPath(), 'utf-8'));
        if (!Array.isArray(ids)) throw new Error('not a list of ids');
      } catch (err) {
        throw new Error(
          `[message-store] cannot read the seen ids at ${this._seenPath()} (${err.message}); ` + START_AGAIN,
        );
      }
      for (const id of ids) this._see(id);
    }
    if (!existsSync(this.persistPath)) return;
    // An unreadable history stops the store: carrying on empty would
    // let the next flush overwrite it for good.
    let data;
    try {
      data = JSON.parse(readFileSync(this.persistPath, 'utf-8'));
      if (data === null || typeof data !== 'object' || Array.isArray(data) || !Object.values(data).every(Array.isArray)) {
        throw new Error('not a map of chats to message lists');
      }
    } catch (err) {
      throw new Error(`[message-store] cannot read the message history at ${this.persistPath} (${err.message}); ` + START_AGAIN);
    }
    const loaded = new Set();
    let count = 0;
    for (const [chatId, msgs] of Object.entries(data)) {
      for (const msg of msgs) {
        const msgId = msg.key?.id;
        if (!msgId || loaded.has(msgId)) continue;
        loaded.add(msgId);
        this._see(msgId);
        if (!this.chats.has(chatId)) {
          this.chats.set(chatId, []);
        }
        this.chats.get(chatId).push(msg);
        count++;
      }
      // Re-sort after bulk load
      const list = this.chats.get(chatId);
      if (list) {
        list.sort((a, b) => toNumber(a.messageTimestamp) - toNumber(b.messageTimestamp));
        // Trim to capacity
        if (list.length > this.maxPerChat) {
          list.splice(0, list.length - this.maxPerChat);
        }
      }
    }
    console.log(`[message-store] Loaded ${count} messages from disk (${this.chats.size} chats)`);
  }
}

// Both files live on the bridge's own volume, which no weft command
// edits file by file; the way to start again is to drop that volume.
const START_AGAIN =
  'to start again, run `weft infra node-terminate <this bridge node>` (it deletes the bridge volume: this ' +
  'history, the seen ids and the WhatsApp login, so you pair the phone again) and then `weft infra start`';

export function toNumber(ts) {
  if (typeof ts === 'number') return ts;
  // Handle protobuf Long objects (live) and deserialized {low, high, unsigned} (from disk)
  if (ts && typeof ts === 'object' && 'low' in ts) {
    return (ts.high >>> 0) * 0x100000000 + (ts.low >>> 0);
  }
  return Number(ts) || 0;
}

/**
 * Extract text content and message type from a raw Baileys WAMessage:
 * the text of a text message, the caption (or null) of a media one. The
 * bytes are never downloaded here; `/media/:messageId` serves them on
 * demand. A kind this list does not know (a poll, a reaction, a protocol
 * message) and a message with no `message` at all are `unknown` with no
 * content, never an empty text a program would run on as if a person
 * sent it.
 */
// SYNC: the messageTypes returned here <-> the `messageTypes` widget
// options in catalog/bailey/receive/metadata.json.
export function extractTextContent(msg) {
  const m = msg.message;
  if (!m) return { content: null, messageType: 'unknown' };

  if (m.audioMessage) return { content: null, messageType: 'audio' };
  if (m.conversation) return { content: m.conversation, messageType: 'text' };
  if (m.extendedTextMessage?.text) return { content: m.extendedTextMessage.text, messageType: 'text' };
  if (m.imageMessage?.caption) return { content: m.imageMessage.caption, messageType: 'image' };
  if (m.videoMessage?.caption) return { content: m.videoMessage.caption, messageType: 'video' };
  if (m.documentMessage?.caption) return { content: m.documentMessage.caption, messageType: 'document' };
  if (m.imageMessage) return { content: null, messageType: 'image' };
  if (m.videoMessage) return { content: null, messageType: 'video' };
  if (m.documentMessage) return { content: null, messageType: 'document' };
  if (m.stickerMessage) return { content: null, messageType: 'sticker' };
  if (m.contactMessage) return { content: null, messageType: 'contact' };
  if (m.locationMessage) return { content: null, messageType: 'location' };

  return { content: null, messageType: 'unknown' };
}

/**
 * What a media message says about its file before anyone downloads it:
 * `fileSize` in bytes and, for a voice note or a video, its length in
 * `seconds`. A field WhatsApp did not send is left out, never guessed.
 */
export function mediaFacts(msg) {
  const m = msg.message;
  const media = m?.imageMessage || m?.videoMessage || m?.audioMessage || m?.documentMessage || m?.stickerMessage;
  if (!media) return {};
  const facts = {};
  if (media.fileLength != null) facts.fileSize = toNumber(media.fileLength);
  if (media.seconds != null) facts.seconds = toNumber(media.seconds);
  return facts;
}

/**
 * The `Content-Disposition` for a file the sender named: the name as
 * written, in the RFC 5987 `filename*` form a header can carry whatever
 * its characters, plus a plain `filename` for a reader that only knows
 * that form. A name is the sender's choice, so nothing in it may reach
 * the header raw: a quote, a line break or an emoji would break it or
 * make setting it throw.
 */
export function contentDisposition(name) {
  const plain = name.replace(/[^\x20-\x7e]|["\\]/g, '_');
  const encoded = encodeURIComponent(name).replace(/['()*!]/g, (c) => `%${c.charCodeAt(0).toString(16).toUpperCase()}`);
  return `inline; filename="${plain}"; filename*=UTF-8''${encoded}`;
}

/** Write a whole file or nothing: a crash mid-write leaves the old file,
 *  never half of the new one. */
function writeAtomic(path, text) {
  const tmp = `${path}.tmp`;
  writeFileSync(tmp, text);
  renameSync(tmp, path);
}
