import { mkdirSync } from 'fs';
import NodeCache from '@cacheable/node-cache';
import pino from 'pino';
import QRCode from 'qrcode';
import {
  makeWASocket,
  useMultiFileAuthState,
  makeCacheableSignalKeyStore,
  fetchLatestBaileysVersion,
  DisconnectReason,
  proto,
} from 'baileys';
import { toNumber } from './message-store.js';

// Resolve enum values once at module load. If Baileys ever
// renames or moves these (it has done so between major versions),
// the destructure below produces `undefined` and we fail loud
// at boot instead of silently mis-comparing at fire time.
const HISTORY_SYNC_ON_DEMAND = proto?.HistorySync?.HistorySyncType?.ON_DEMAND;
if (HISTORY_SYNC_ON_DEMAND === undefined) {
  throw new Error(
    '[bridge] proto.HistorySync.HistorySyncType.ON_DEMAND is undefined; ' +
      'Baileys may have changed the enum path. Bridge cannot detect ' +
      'on-demand history syncs and will not function correctly.',
  );
}

/**
 * Creates and manages the Baileys WhatsApp connection.
 *
 * State machine:
 *   disconnected -> qr_pending -> connecting -> connected
 *                                             -> disconnected (on close)
 *
 * Aligns with the official Baileys 7.x example.ts:
 *   - auth: { creds, keys: makeCacheableSignalKeyStore(state.keys, logger) }
 *   - version: fetchLatestBaileysVersion() (current WA Web protocol)
 *   - msgRetryCounterCache for unacked-message retransmits
 *   - getMessage callback (placeholder; required for retransmit logic)
 *   - sock.ev.process(events => ...) batched event handler
 */
export async function createBridge(authDir, webhookManager, messageStore) {
  mkdirSync(authDir, { recursive: true });

  const logger = pino({ level: 'silent' });
  const msgRetryCounterCache = new NodeCache();

  let sock = null;
  let currentQrBase64 = null;
  let reconnectAttempts = 0;
  // Guards against stacked reconnects. Two `connection.update`
  // events fired in quick succession (network blip + WhatsApp's
  // own re-handshake) previously spawned two parallel setTimeout
  // -> connect() chains racing on the auth dir.
  let reconnectTimer = null;
  let connecting = false;
  const MAX_RECONNECT_DELAY = 60_000;
  let state = {
    status: 'disconnected',
    phoneNumber: null,
    jid: null,
    pushName: null,
  };

  async function connect() {
    if (connecting) {
      console.log('[bridge] connect() called while already connecting; ignoring');
      return;
    }
    connecting = true;
    if (reconnectTimer !== null) {
      clearTimeout(reconnectTimer);
      reconnectTimer = null;
    }
    try {
      await connectInner();
    } catch (err) {
      // Anything thrown BEFORE the socket emits its first
      // `connection.update` leaves `connecting=true` stuck and
      // blocks every future reconnect. Reset on the throw path
      // so the next reconnect attempt can proceed.
      connecting = false;
      throw err;
    }
  }

  async function connectInner() {
    const { state: authState, saveCreds } = await useMultiFileAuthState(authDir);

    // The version Baileys ships in package.json gets 405'd against the
    // live WhatsApp gateway whenever WA rotates protocol. The
    // documented escape hatch is fetchLatestBaileysVersion(): pulls
    // the current protocol triple from WhatsApp's web client.
    let version;
    try {
      const versionInfo = await fetchLatestBaileysVersion();
      version = versionInfo.version;
      console.log(`[bridge] Using WA Web version: ${version.join('.')}`);
    } catch (err) {
      console.warn('[bridge] Failed to fetch WA version, using default:', err.message);
    }

    sock = makeWASocket({
      logger,
      version,
      // makeCacheableSignalKeyStore wraps the on-disk key store with
      // an in-process LRU. Without it, every Signal cryptography
      // round-trip hits the filesystem. Official example does this.
      auth: {
        creds: authState.creds,
        keys: makeCacheableSignalKeyStore(authState.keys, logger),
      },
      msgRetryCounterCache,
      generateHighQualityLinkPreview: true,
      markOnlineOnConnect: false,
      getMessage,
    });

    sock.ev.on('creds.update', saveCreds);

    // Batched event handler (the 7.x recommended pattern). Each tick
    // delivers a bag of any events that fired since the last drain.
    sock.ev.process(async (events) => {
      if (events['connection.update']) {
        await onConnectionUpdate(events['connection.update']);
      }
      if (events['messages.upsert']) {
        await onMessagesUpsert(events['messages.upsert']);
      }
      if (events['messaging-history.set']) {
        onMessagingHistorySet(events['messaging-history.set']);
      }
      if (events['groups.update']) {
        for (const update of events['groups.update']) {
          webhookManager.emit('group.update', update);
        }
      }
    });
  }

  async function onConnectionUpdate(update) {
    const { connection, lastDisconnect, qr } = update;

    if (qr) {
      try {
        currentQrBase64 = await QRCode.toDataURL(qr, { width: 300 });
        state.status = 'qr_pending';
        console.log('[bridge] QR code generated, waiting for scan...');
      } catch (err) {
        console.error('[bridge] Failed to generate QR:', err);
      }
    }

    if (connection === 'connecting') {
      state.status = 'connecting';
      currentQrBase64 = null;
    }

    if (connection === 'open') {
      state.status = 'connected';
      currentQrBase64 = null;
      reconnectAttempts = 0;
      connecting = false;

      const me = sock.user;
      if (me) {
        const rawId = me.id || '';
        state.phoneNumber = rawId.split(':')[0]?.split('@')[0] || null;
        state.jid = state.phoneNumber ? `${state.phoneNumber}@s.whatsapp.net` : null;
        state.pushName = me.name || null;
      }
      console.log(`[bridge] Connected as ${state.pushName} (${state.phoneNumber})`);
      webhookManager.emit('connection.update', {
        status: 'connected',
        phoneNumber: state.phoneNumber,
      });

      // Fill gaps in persisted chats by requesting history from the
      // oldest stored message per chat. Best-effort; no awaits because
      // history syncs are slow and the connection is already usable.
      const knownChats = messageStore.getChatIds();
      if (knownChats.length > 0) {
        console.log(`[bridge] Requesting history backfill for ${knownChats.length} known chats`);
        for (const chatId of knownChats) {
          const cursor = messageStore.getOldestMessage(chatId);
          if (cursor) {
            sock
              .fetchMessageHistory(50, cursor.key, toNumber(cursor.messageTimestamp))
              .catch((err) => {
                console.warn(`[bridge] History backfill failed for ${chatId}:`, err.message);
              });
          }
        }
      }
    }

    if (connection === 'close') {
      currentQrBase64 = null;
      connecting = false;
      const statusCode = lastDisconnect?.error?.output?.statusCode;
      const shouldReconnect = statusCode !== DisconnectReason.loggedOut;

      console.log(
        `[bridge] Connection closed. statusCode=${statusCode} shouldReconnect=${shouldReconnect}`,
      );

      if (shouldReconnect) {
        state.status = 'disconnected';
        reconnectAttempts++;
        const delay = Math.min(3000 * Math.pow(2, reconnectAttempts - 1), MAX_RECONNECT_DELAY);
        console.log(`[bridge] Reconnecting in ${delay}ms (attempt ${reconnectAttempts})`);
        if (reconnectTimer !== null) {
          clearTimeout(reconnectTimer);
        }
        reconnectTimer = setTimeout(() => {
          reconnectTimer = null;
          // Swallow-and-log: `connect()` re-throws on failure, and a
          // bare un-awaited call here would become an unhandled
          // rejection that terminates the process (Node 15+), defeating
          // the whole reconnect/backoff machinery. The next
          // `connection.update` close event schedules the following
          // attempt, so backoff continues.
          connect().catch((err) => {
            console.error('[bridge] reconnect attempt failed:', err.message);
          });
        }, delay);
      } else {
        state.status = 'disconnected';
        state.phoneNumber = null;
        state.jid = null;
        state.pushName = null;
        console.log('[bridge] Logged out. Need QR re-scan.');
        webhookManager.emit('connection.update', { status: 'logged_out' });
      }
    }
  }

  async function onMessagesUpsert({ type, messages }) {
    if (type !== 'notify') return;

    for (const msg of messages) {
      // Persist every message (including our own) for /media + history.
      messageStore.add(msg);

      if (msg.key.fromMe) continue;

      const { content, messageType } = extractMessageContent(msg);

      // Skip non-actionable noise (reactions, receipts, protocol msgs).
      // Media without caption still goes through so a downstream node
      // can resolve it via /media/:id once media support lands.
      const hasText = content != null && content !== '';
      const isMedia = ['image', 'video', 'document', 'audio', 'sticker'].includes(messageType);
      if (!hasText && !isMedia) continue;

      const from = msg.key.remoteJid;
      const isGroup = from?.endsWith('@g.us') || false;

      webhookManager.emit('message.received', {
        from,
        pushName: msg.pushName || null,
        content,
        messageId: msg.key.id,
        // Baileys deserializes the protobuf int64 as a Long object
        // {low, high} on the live path; coerce to a JS number so the
        // receive node's `timestamp: Number` port type-checks (every
        // other consumer of messageTimestamp does the same).
        timestamp: toNumber(msg.messageTimestamp),
        isGroup,
        chatId: from,
        // messageKey: the handle a future /media/:id resolve needs.
        messageKey: msg.key,
      });
    }
  }

  function onMessagingHistorySet({ messages, syncType }) {
    console.log(`[bridge] History sync: ${messages.length} messages (syncType=${syncType})`);
    messageStore.addBatch(messages);
    messageStore.markHistoryReady();
  }

  // Required by Baileys for transparent retransmit of messages we
  // previously sent that didn't reach the recipient. We don't keep
  // our outbound bodies in memory long enough to replay them, so
  // return a placeholder; WA will treat it as a stub. Per Baileys
  // example.ts, this is acceptable for non-critical receipts.
  async function getMessage(_key) {
    return proto.Message.create({ conversation: '' });
  }

  await connect();

  return {
    getState() {
      return { ...state };
    },
    getQr() {
      return currentQrBase64;
    },
    getSocket() {
      return sock;
    },
    isConnected() {
      return state.status === 'connected';
    },
    /**
     * Request on-demand history sync for a chat. Resolves true when
     * the on-demand sync chunk arrives, false on timeout / error.
     */
    requestHistory(count, cursorMsg, timeoutMs = 8000) {
      if (!sock || !this.isConnected() || !cursorMsg) {
        return Promise.resolve(false);
      }

      return new Promise((resolve) => {
        const timer = setTimeout(() => {
          sock.ev.off('messaging-history.set', handler);
          resolve(false);
        }, timeoutMs);

        const handler = ({ syncType }) => {
          // ON_DEMAND enum value (proto.HistorySync.HistorySyncType).
          if (syncType === HISTORY_SYNC_ON_DEMAND) {
            clearTimeout(timer);
            sock.ev.off('messaging-history.set', handler);
            resolve(true);
          }
        };

        sock.ev.on('messaging-history.set', handler);

        sock
          .fetchMessageHistory(count, cursorMsg.key, toNumber(cursorMsg.messageTimestamp))
          .catch((err) => {
            console.error('[bridge] fetchMessageHistory failed:', err.message);
            clearTimeout(timer);
            sock.ev.off('messaging-history.set', handler);
            resolve(false);
          });
      });
    },
  };
}

/**
 * Extract text content + message type from a WhatsApp message.
 *
 * Returns { content, messageType }. Text messages carry their text in
 * `content`; media messages report their `messageType` and a caption
 * (if any) as `content`. Media payloads (audio/image/video) are NOT
 * downloaded here: the receive node is text-only for now. When media
 * support lands it should be a generic media-fetch path, not the
 * per-type eager download this used to do.
 */
function extractMessageContent(msg) {
  const m = msg.message;
  if (!m) return { content: '', messageType: 'text' };

  if (m.conversation) return { content: m.conversation, messageType: 'text' };
  if (m.extendedTextMessage?.text) {
    return { content: m.extendedTextMessage.text, messageType: 'text' };
  }

  if (m.imageMessage) return { content: m.imageMessage.caption ?? null, messageType: 'image' };
  if (m.videoMessage) return { content: m.videoMessage.caption ?? null, messageType: 'video' };
  if (m.documentMessage) return { content: m.documentMessage.caption ?? null, messageType: 'document' };
  if (m.audioMessage) return { content: null, messageType: 'audio' };
  if (m.stickerMessage) return { content: null, messageType: 'sticker' };
  if (m.contactMessage) return { content: null, messageType: 'contact' };
  if (m.locationMessage) return { content: null, messageType: 'location' };

  return { content: '', messageType: 'text' };
}
