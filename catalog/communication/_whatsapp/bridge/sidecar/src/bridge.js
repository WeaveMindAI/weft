import { makeWASocket, useMultiFileAuthState, DisconnectReason, fetchLatestWaWebVersion, downloadMediaMessage } from 'baileys';
import QRCode from 'qrcode';
import pino from 'pino';
import { mkdirSync } from 'fs';

/**
 * Creates and manages the Baileys WhatsApp connection.
 * 
 * State machine:
 *   disconnected -> qr_pending -> connecting -> connected
 *                                             -> disconnected (on close)
 *
 * The bridge auto-reconnects on non-logout disconnects using saved auth state.
 */
export async function createBridge(authDir, webhookManager, messageStore) {
  mkdirSync(authDir, { recursive: true });

  let sock = null;
  let currentQrBase64 = null;
  let reconnectAttempts = 0;
  const MAX_RECONNECT_DELAY = 60_000;
  let state = {
    status: 'disconnected',
    phoneNumber: null,
    jid: null,
    pushName: null,
  };

  async function connect() {
    const { state: authState, saveCreds } = await useMultiFileAuthState(authDir);

    // Baileys 7.0.0-rc.9 ships a stale WA protocol version that gets 405'd.
    // Fetch the current version from WhatsApp's servers.
    let version;
    try {
      const versionInfo = await fetchLatestWaWebVersion({});
      version = versionInfo.version;
      console.log(`[bridge] Using WA Web version: ${version}`);
    } catch (err) {
      console.warn('[bridge] Failed to fetch WA version, using default:', err.message);
    }

    sock = makeWASocket({
      auth: authState,
      logger: pino({ level: 'silent' }),
      markOnlineOnConnect: false,
      ...(version ? { version } : {}),
    });

    sock.ev.on('creds.update', saveCreds);

    sock.ev.on('connection.update', async (update) => {
      const { connection, lastDisconnect, qr } = update;

      if (qr) {
        // Generate QR as base64 PNG
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

        // Extract phone number, JID, and push name from the socket
        const me = sock.user;
        if (me) {
          const rawId = me.id || '';
          state.phoneNumber = rawId.split(':')[0]?.split('@')[0] || null;
          state.jid = state.phoneNumber ? `${state.phoneNumber}@s.whatsapp.net` : null;
          state.pushName = me.name || null;
        }
        console.log(`[bridge] Connected as ${state.pushName} (${state.phoneNumber})`);
        webhookManager.emit('connection.update', { status: 'connected', phoneNumber: state.phoneNumber });

        // Proactively fetch history for chats we have persisted messages for.
        // This fills gaps between the last persisted message and now.
        const knownChats = messageStore.getChatIds();
        if (knownChats.length > 0) {
          console.log(`[bridge] Requesting history backfill for ${knownChats.length} known chats`);
          for (const chatId of knownChats) {
            const cursor = messageStore.getOldestMessage(chatId);
            if (cursor) {
              sock.fetchMessageHistory(
                50,
                cursor.key,
                typeof cursor.messageTimestamp === 'number'
                  ? cursor.messageTimestamp
                  : Number(cursor.messageTimestamp) || 0,
              ).catch((err) => {
                console.warn(`[bridge] History backfill failed for ${chatId}:`, err.message);
              });
            }
          }
        }
      }

      if (connection === 'close') {
        currentQrBase64 = null;
        const statusCode = lastDisconnect?.error?.output?.statusCode;
        const shouldReconnect = statusCode !== DisconnectReason.loggedOut;

        console.log(`[bridge] Connection closed. statusCode=${statusCode} shouldReconnect=${shouldReconnect}`);

        if (shouldReconnect) {
          state.status = 'disconnected';
          reconnectAttempts++;
          const delay = Math.min(3000 * Math.pow(2, reconnectAttempts - 1), MAX_RECONNECT_DELAY);
          console.log(`[bridge] Reconnecting in ${delay}ms (attempt ${reconnectAttempts})`);
          setTimeout(() => connect(), delay);
        } else {
          state.status = 'disconnected';
          state.phoneNumber = null;
          state.jid = null;
          state.pushName = null;
          console.log('[bridge] Logged out. Need QR re-scan.');
          webhookManager.emit('connection.update', { status: 'logged_out' });
        }
      }
    });

    // Forward incoming messages to registered webhooks
    sock.ev.on('messages.upsert', async ({ type, messages }) => {
      console.log(`[bridge] messages.upsert: type=${type}, count=${messages?.length}`);
      if (type !== 'notify') return;

      for (const msg of messages) {
        // Store every message (including our own) for history retrieval
        messageStore.add(msg);

        // Skip messages sent by us for webhook dispatch
        if (msg.key.fromMe) continue;

        const { content, audio, messageType } = await extractMessageContent(msg);

        // Skip messages with no usable content (protocol messages, reactions, read receipts, etc.)
        // Media messages (image, video, document, audio, sticker) are always forwarded
        // even without text content, the node can download the media via /media/:messageId.
        const hasText = (content != null && content !== '') || (audio != null && audio !== '');
        const isMedia = ['image', 'video', 'document', 'audio', 'sticker'].includes(messageType);
        if (!hasText && !isMedia) {
          console.log(`[bridge] Skipping empty ${messageType} message from ${msg.key.remoteJid}`);
          continue;
        }

        const from = msg.key.remoteJid;
        const isGroup = from?.endsWith('@g.us') || false;

        webhookManager.emit('message.received', {
          from,
          pushName: msg.pushName || null,
          content,
          audio,
          messageType,
          messageId: msg.key.id,
          timestamp: msg.messageTimestamp,
          isGroup,
          chatId: from,
          // Include raw key for reply context
          messageKey: msg.key,
        });
      }
    });

    // Capture messages from history sync (initial + on-demand)
    sock.ev.on('messaging-history.set', ({ messages, syncType }) => {
      console.log(`[bridge] History sync: ${messages.length} messages (syncType=${syncType})`);
      messageStore.addBatch(messages);
      // Signal that the initial sync has delivered data
      messageStore.markHistoryReady();
    });

    // Group events
    sock.ev.on('groups.update', (updates) => {
      for (const update of updates) {
        webhookManager.emit('group.update', update);
      }
    });
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
     * Request on-demand history sync for a chat.
     * Requires the oldest stored message as cursor.
     * Returns a promise that resolves when the history arrives (or times out).
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
          // syncType 4 = ON_DEMAND in the proto enum
          if (syncType === 4) {
            clearTimeout(timer);
            sock.ev.off('messaging-history.set', handler);
            resolve(true);
          }
        };

        sock.ev.on('messaging-history.set', handler);

        sock.fetchMessageHistory(
          count,
          cursorMsg.key,
          typeof cursorMsg.messageTimestamp === 'number'
            ? cursorMsg.messageTimestamp
            : Number(cursorMsg.messageTimestamp) || 0,
        ).catch((err) => {
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
 * Extract content from a WhatsApp message.
 * Returns { content, audio, messageType } where content and audio are
 * mutually exclusive (one is always null).
 *
 * Audio messages are downloaded from WhatsApp and returned as a base64
 * data URL compatible with the SpeechToText node's audio input.
 */
async function extractMessageContent(msg) {
  const m = msg.message;
  if (!m) return { content: '', audio: null, messageType: 'text' };

  // Audio message: download and encode as base64 data URL
  if (m.audioMessage) {
    try {
      const buffer = await downloadMediaMessage(msg, 'buffer', {});
      const mimetype = m.audioMessage.mimetype || 'audio/ogg';
      const base64 = buffer.toString('base64');
      return {
        content: null,
        audio: `data:${mimetype};base64,${base64}`,
        messageType: 'audio',
      };
    } catch (err) {
      console.error('[bridge] Failed to download audio:', err.message);
      return { content: null, audio: null, messageType: 'audio' };
    }
  }

  // Text messages
  if (m.conversation) return { content: m.conversation, audio: null, messageType: 'text' };
  if (m.extendedTextMessage?.text) return { content: m.extendedTextMessage.text, audio: null, messageType: 'text' };

  // Media with captions (text content)
  if (m.imageMessage?.caption) return { content: m.imageMessage.caption, audio: null, messageType: 'image' };
  if (m.videoMessage?.caption) return { content: m.videoMessage.caption, audio: null, messageType: 'video' };
  if (m.documentMessage?.caption) return { content: m.documentMessage.caption, audio: null, messageType: 'document' };

  // Media without caption
  if (m.imageMessage) return { content: null, audio: null, messageType: 'image' };
  if (m.videoMessage) return { content: null, audio: null, messageType: 'video' };
  if (m.documentMessage) return { content: null, audio: null, messageType: 'document' };
  if (m.stickerMessage) return { content: null, audio: null, messageType: 'sticker' };
  if (m.contactMessage) return { content: null, audio: null, messageType: 'contact' };
  if (m.locationMessage) return { content: null, audio: null, messageType: 'location' };

  return { content: '', audio: null, messageType: 'text' };
}
