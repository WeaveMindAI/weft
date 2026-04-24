import { readFileSync, writeFileSync, existsSync } from 'fs';

/**
 * In-memory message store, keyed by chatId, with optional disk persistence.
 *
 * Populated from two sources:
 *   1. Initial history sync (`messaging-history.set`), fires on connection,
 *      includes messages from before the server started.
 *   2. Live messages (`messages.upsert`), new messages as they arrive.
 *
 * Stores raw Baileys WAMessage protobufs so audio can be lazy-downloaded
 * at query time via `downloadMediaMessage`. The protobuf is small (few KB),
 * the actual audio bytes are only fetched when explicitly requested.
 *
 * Each chat keeps at most `maxPerChat` messages (oldest evicted).
 * If `persistPath` is provided, the store is loaded from disk on construction
 * and flushed to disk (debounced) after mutations.
 */
export class MessageStore {
  constructor(maxPerChat = 500, persistPath = null) {
    this.maxPerChat = maxPerChat;
    this.persistPath = persistPath;
    /** @type {Map<string, Array<Object>>} chatId -> sorted array of raw WAMessages */
    this.chats = new Map();
    /** @type {Set<string>} messageId dedup set */
    this.seen = new Set();
    this._dirty = false;
    this._flushTimer = null;
    /** Resolves when the initial history sync completes (or times out). */
    this._historyReady = null;
    this._resolveHistoryReady = null;

    // Load persisted messages from disk if available
    if (persistPath) {
      this._loadFromDisk();
    }
  }

  /**
   * Ingest one raw Baileys WAMessage.
   * Safe to call multiple times with the same message (deduped by messageId).
   */
  add(msg) {
    const chatId = msg.key?.remoteJid;
    const msgId = msg.key?.id;
    if (!chatId || !msgId) return;
    if (this.seen.has(msgId)) return;
    this.seen.add(msgId);

    if (!this.chats.has(chatId)) {
      this.chats.set(chatId, []);
    }

    const list = this.chats.get(chatId);
    list.push(msg);

    // Keep sorted by timestamp ascending
    list.sort((a, b) => toNumber(a.messageTimestamp) - toNumber(b.messageTimestamp));

    // Evict oldest if over capacity
    if (list.length > this.maxPerChat) {
      const removed = list.splice(0, list.length - this.maxPerChat);
      for (const r of removed) {
        this.seen.delete(r.key?.id);
      }
    }

    this._markDirty();
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
  getRawMessages(chatId, count = 20) {
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
    if (!this.persistPath || !this._dirty) return;
    this._flushToDisk();
  }

  // ── Internal ──

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
      writeFileSync(this.persistPath, JSON.stringify(data));
      this._dirty = false;
      const total = this.totalCount();
      console.log(`[message-store] Flushed ${total} messages to disk`);
    } catch (err) {
      console.error('[message-store] Failed to flush to disk:', err.message);
    }
  }

  _loadFromDisk() {
    if (!this.persistPath || !existsSync(this.persistPath)) return;
    try {
      const raw = readFileSync(this.persistPath, 'utf-8');
      const data = JSON.parse(raw);
      let count = 0;
      for (const [chatId, msgs] of Object.entries(data)) {
        if (!Array.isArray(msgs)) continue;
        for (const msg of msgs) {
          const msgId = msg.key?.id;
          if (!msgId || this.seen.has(msgId)) continue;
          this.seen.add(msgId);
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
            const removed = list.splice(0, list.length - this.maxPerChat);
            for (const r of removed) this.seen.delete(r.key?.id);
          }
        }
      }
      console.log(`[message-store] Loaded ${count} messages from disk (${this.chats.size} chats)`);
    } catch (err) {
      console.error('[message-store] Failed to load from disk:', err.message);
    }
  }
}

function toNumber(ts) {
  if (typeof ts === 'number') return ts;
  // Handle protobuf Long objects (live) and deserialized {low, high, unsigned} (from disk)
  if (ts && typeof ts === 'object' && 'low' in ts) {
    return (ts.high >>> 0) * 0x100000000 + (ts.low >>> 0);
  }
  return Number(ts) || 0;
}

/**
 * Extract text content and message type from a raw Baileys WAMessage.
 * Used when serializing messages for the action response.
 * Does NOT download audio, that's handled separately at query time.
 */
export function extractTextContent(msg) {
  const m = msg.message;
  if (!m) return { content: '', messageType: 'text' };

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

  return { content: '', messageType: 'text' };
}
