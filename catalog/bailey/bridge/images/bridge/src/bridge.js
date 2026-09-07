import { mkdirSync, readdirSync, rmSync } from 'fs';
import { join } from 'path';
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
 * Returns as soon as the handle is built, with the connection still
 * being dialled. Read `getState().status` for where it has got to.
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

  // `warn` and up reach the pod log: Baileys swallows a failed media
  // step (a duration or waveform it could not compute) into a warn and
  // sends anyway, and a silent logger made those drops invisible.
  const logger = pino({ level: 'warn' });
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
        // WhatsApp retired this pairing: the user removed the device,
        // a pairing died half way, or `logout` below asked for it. The
        // credentials on disk are dead now, and reconnecting with them
        // only earns the same refusal, so they are dropped and a fresh
        // dial starts, which shows a new QR code on `/live`. The chat
        // history stays: it is the account's, and the same phone can
        // pair again.
        state.status = 'disconnected';
        state.phoneNumber = null;
        state.jid = null;
        state.pushName = null;
        console.log('[bridge] Logged out; dropping the pairing and dialling for a new QR code');
        webhookManager.emit('connection.update', { status: 'logged_out' });
        forgetPairing();
        state.status = 'connecting';
        connect().catch((err) => {
          console.error('[bridge] re-dial after logout failed:', err.message);
        });
      }
    }
  }

  /**
   * Delete the pairing WhatsApp issued (creds.json plus every Signal
   * key file `useMultiFileAuthState` writes) so the next dial starts
   * from nothing and gets a QR code. The message store lives in the
   * same directory and is kept.
   */
  function forgetPairing() {
    for (const name of readdirSync(authDir)) {
      if (name === 'messages.json') continue;
      rmSync(join(authDir, name), { recursive: true, force: true });
    }
    console.log('[bridge] pairing files removed');
  }

  async function onMessagesUpsert({ type, messages }) {
    if (type !== 'notify') return;

    for (const msg of messages) {
      // Persist every message (including our own) for /media + history.
      messageStore.add(msg);

      if (msg.key.fromMe) continue;

      const { content, messageType } = extractMessageContent(msg);

      // Skip non-actionable noise (reactions, receipts, protocol msgs).
      // Media without caption still goes through; the receive node
      // resolves the bytes via /media/:id and stores them.
      const hasText = content != null && content !== '';
      const isMedia = ['image', 'video', 'document', 'audio', 'sticker'].includes(messageType);
      if (!hasText && !isMedia) continue;

      const from = msg.key.remoteJid;
      const isGroup = from?.endsWith('@g.us') || false;

      webhookManager.emit('message.received', {
        from,
        pushName: msg.pushName || null,
        content,
        // The receive node branches on this to fetch media bytes
        // via /media/:id and store them as a Weft media reference.
        messageType,
        messageId: msg.key.id,
        // Baileys deserializes the protobuf int64 as a Long object
        // {low, high} on the live path; coerce to a JS number so the
        // receive node's `timestamp: Number` port type-checks (every
        // other consumer of messageTimestamp does the same).
        timestamp: toNumber(msg.messageTimestamp),
        isGroup,
        chatId: from,
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

  // Dial WhatsApp in the background rather than making the caller wait
  // for it. A connect can sit unfinished for a long time (an unpaired
  // bridge waits for somebody to scan its QR code), and `/live` is what
  // SHOWS that QR code, so the HTTP surface has to be up first.
  //
  // Anything thrown before the socket's first `connection.update` ends
  // the process. The reconnect machinery hangs off those events, so a
  // failure that precedes them has nothing left to retry it, and a
  // bridge that answers `/live` forever while never dialling again is
  // worse than a pod restart.
  state.status = 'connecting';
  connect().catch((err) => {
    console.error('[bridge] initial connect failed:', err.message);
    process.exit(1);
  });

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
     * Detach the paired phone, whatever state the bridge is in. Paired
     * and connected: tell WhatsApp to remove this device, which lands
     * as a logged-out close that drops the pairing and re-dials. Not
     * connected (a pairing that died half way, a session WhatsApp
     * already refused): there is nobody to tell, so the pairing is
     * dropped here and the dial restarted. Either way `/live` shows a
     * QR code next.
     */
    async unpair() {
      if (sock && state.status === 'connected') {
        console.log('[bridge] unpair: logging the device out of WhatsApp');
        await sock.logout('user asked to disconnect the phone');
        return;
      }
      console.log(`[bridge] unpair while ${state.status}: dropping the pairing and re-dialling`);
      if (reconnectTimer !== null) {
        clearTimeout(reconnectTimer);
        reconnectTimer = null;
      }
      if (sock) {
        // Detach every handler (the batched `process` one included)
        // before ending the half-open socket, so its close event
        // cannot race the fresh dial below into a second reconnect.
        try {
          sock.ev.removeAllListeners();
          sock.end(undefined);
        } catch (err) {
          console.warn('[bridge] unpair: ending the old socket failed:', err.message);
        }
        sock = null;
      }
      connecting = false;
      forgetPairing();
      state.status = 'connecting';
      state.phoneNumber = null;
      state.jid = null;
      state.pushName = null;
      currentQrBase64 = null;
      await connect();
    },
    /**
     * Request on-demand history sync for a chat. Resolves true when
     * the on-demand sync chunk arrives, false on timeout / error.
     */
    requestHistory(count, cursorMsg, timeoutMs = 8000) {
      if (!sock || !this.isConnected() || !cursorMsg) {
        return Promise.resolve(false);
      }
      // The socket this request rides. A reconnect swaps the module's
      // `sock` mid-wait; the listener has to come off the socket it went
      // on, or it stays on the retired one and the new one is untouched.
      const asked = sock;

      return new Promise((resolve) => {
        const timer = setTimeout(() => {
          asked.ev.off('messaging-history.set', handler);
          resolve(false);
        }, timeoutMs);

        const handler = ({ syncType }) => {
          // ON_DEMAND enum value (proto.HistorySync.HistorySyncType).
          if (syncType === HISTORY_SYNC_ON_DEMAND) {
            clearTimeout(timer);
            asked.ev.off('messaging-history.set', handler);
            resolve(true);
          }
        };

        asked.ev.on('messaging-history.set', handler);

        asked
          .fetchMessageHistory(count, cursorMsg.key, toNumber(cursorMsg.messageTimestamp))
          .catch((err) => {
            console.error('[bridge] fetchMessageHistory failed:', err.message);
            clearTimeout(timer);
            asked.ev.off('messaging-history.set', handler);
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
 * (if any) as `content`, so an audio message has `content: null`. The
 * bytes are never downloaded here: the message is kept in the store
 * and served on demand by `/media/:messageId`, which is where the
 * receive node (live) and the fetch-media node (history) get them.
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
