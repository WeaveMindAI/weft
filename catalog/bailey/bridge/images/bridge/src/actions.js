import { getAudioWaveform } from 'baileys';
import { extractTextContent } from './message-store.js';

/**
 * Action dispatch router for the standard POST /action contract.
 * 
 * Each action maps to a Baileys socket method. The bridge provides the
 * live socket, and the webhook manager handles webhook registration.
 */
/** The WhatsApp media kind a mime maps to. */
function mediaTypeOf(mime) {
  return mime.startsWith('image/') ? 'image'
    : mime.startsWith('video/') ? 'video'
    : mime.startsWith('audio/') ? 'audio'
    : 'document';
}

/**
 * The mime WhatsApp wants on the wire. Opus in an Ogg container is the
 * only audio a phone renders as a voice note, and it renders it ONLY
 * under the exact string `audio/ogg; codecs=opus`: a message stamped
 * plain `audio/ogg` (what every mime guesser says for a .ogg file, and
 * what an encoder hands back) is accepted by the server, gets a message
 * id, and never shows up on the phone. Baileys' own default for audio
 * is that string; anything else passes through untouched.
 */
function whatsappMime(mime) {
  const bare = mime.split(';')[0].trim().toLowerCase();
  if (bare === 'audio/ogg' || bare === 'audio/opus') return 'audio/ogg; codecs=opus';
  return mime;
}

/** The mime a URL's extension suggests, or '' when it suggests nothing. */
function mimeFromUrl(url) {
  const mimeMap = {
    png: 'image/png', jpg: 'image/jpeg', jpeg: 'image/jpeg',
    gif: 'image/gif', webp: 'image/webp', svg: 'image/svg+xml',
    mp4: 'video/mp4', webm: 'video/webm', mov: 'video/quicktime',
    mp3: 'audio/mpeg', ogg: 'audio/ogg', wav: 'audio/wav',
    opus: 'audio/opus', flac: 'audio/flac',
    pdf: 'application/pdf', doc: 'application/msword',
  };
  try {
    const ext = new URL(url).pathname.toLowerCase().split('.').pop();
    return mimeMap[ext] || '';
  } catch {
    return '';
  }
}

export function createActionRouter(bridge, webhookManager, messageStore) {
  const handlers = {
    async ping() {
      // "ready" means the server can process actions, NOT that the bridge
      // is connected to WhatsApp. The WhatsApp connection is user-initiated
      // (QR scan) and happens after provisioning. As long as the Express
      // server is up and the action router can dispatch, we're ready.
      return { ready: true };
    },

    // The button on `/live`: detach the phone and show a new QR code,
    // from any state (paired and running, or stuck after a pairing
    // that died half way). Nothing else resets a bridge short of
    // tearing its infra down.
    async unpair() {
      await bridge.unpair();
      return { success: true };
    },

    async sendMessage({ to, text }) {
      const sock = bridge.getSocket();
      if (!sock || !bridge.isConnected()) {
        return { error: 'WhatsApp not connected' };
      }
      console.log(`[action] sendMessage: ${(text || '').length} chars`);
      const result = await sock.sendMessage(to, { text });
      return { messageId: result.key.id };
    },

    // `ptt` marks audio as a voice note (WhatsApp renders it as a
    // playable bubble instead of an audio file); ignored for any other
    // media type.
    async sendMedia({ to, mediaUrl, mediaBase64, caption, mimetype, filename, ptt }) {
      const sock = bridge.getSocket();
      if (!sock || !bridge.isConnected()) {
        return { error: 'WhatsApp not connected' };
      }

      // Inline content (weft's storage has no public link to hand out
      // in every install, so the node ships the bytes base64-inline).
      // A data: URL is normalized into the same inline path.
      if (!mediaBase64 && mediaUrl && mediaUrl.startsWith('data:')) {
        const m = mediaUrl.match(/^data:([^;]*);base64,(.*)$/s);
        if (!m) return { error: 'data: URL without a base64 payload' };
        mimetype = mimetype || m[1];
        mediaBase64 = m[2];
        mediaUrl = undefined;
      }
      let buffer = mediaBase64 ? Buffer.from(mediaBase64, 'base64') : undefined;
      const resolvedMime = whatsappMime(mimetype || (buffer ? '' : mimeFromUrl(mediaUrl)));
      const mediaType = mediaTypeOf(resolvedMime);
      const voiceNote = mediaType === 'audio' && ptt === true;

      // A voice note is only a voice note on the phone when the message
      // carries a waveform. Baileys computes one from the decoded audio
      // and, when the decode fails, drops the step with a debug line and
      // sends anyway, so the bytes are decoded HERE first and a voice
      // note that cannot get its waveform is refused before it goes out.
      let waveform;
      if (voiceNote) {
        if (!buffer) {
          const fetched = await fetch(mediaUrl);
          if (!fetched.ok) {
            return { error: `fetching the voice note at ${mediaUrl} answered ${fetched.status}` };
          }
          buffer = Buffer.from(await fetched.arrayBuffer());
        }
        waveform = await getAudioWaveform(buffer);
        if (!waveform?.length) {
          return {
            error: `cannot decode the ${resolvedMime} audio (${buffer.length} bytes) into a waveform, so it would arrive as a plain audio file instead of a voice note; send Ogg Opus, mp3, wav or flac`,
          };
        }
      }

      console.log(`[action] sendMedia: ${mediaType} ${resolvedMime} ${buffer ? `${buffer.length} bytes` : mediaUrl} ptt=${voiceNote}`);
      const result = await sock.sendMessage(to, {
        [mediaType]: buffer ?? { url: mediaUrl },
        caption: caption || undefined,
        mimetype: resolvedMime || undefined,
        fileName: filename || undefined,
        ptt: voiceNote ? true : undefined,
        waveform,
      });
      return { messageId: result.key.id };
    },

    async sendReaction({ chatId, messageId, emoji }) {
      const sock = bridge.getSocket();
      if (!sock || !bridge.isConnected()) {
        return { error: 'WhatsApp not connected' };
      }
      await sock.sendMessage(chatId, {
        react: { text: emoji, key: { remoteJid: chatId, id: messageId } },
      });
      return { success: true };
    },

    // Mark a message as read: the blue ticks on the other phone. The
    // stored message carries the full key WhatsApp wants back (in a
    // group, the `participant` who sent it); a message the store never
    // saw is addressed by chat + id alone, which is enough for a
    // one-to-one chat.
    async readMessages({ chatId, messageId }) {
      const sock = bridge.getSocket();
      if (!sock || !bridge.isConnected()) {
        return { error: 'WhatsApp not connected' };
      }
      if (!chatId || !messageId) {
        return { error: 'chatId and messageId are required' };
      }
      const stored = messageStore.findByMessageId(messageId);
      const key = stored?.key
        ? { ...stored.key }
        : { remoteJid: chatId, id: messageId, fromMe: false };
      await sock.readMessages([key]);
      return { success: true };
    },

    async createGroup({ name, participants }) {
      const sock = bridge.getSocket();
      if (!sock || !bridge.isConnected()) {
        return { error: 'WhatsApp not connected' };
      }
      const result = await sock.groupCreate(name, participants);
      return { groupId: result.id };
    },

    async groupAdd({ groupId, participants }) {
      const sock = bridge.getSocket();
      if (!sock || !bridge.isConnected()) {
        return { error: 'WhatsApp not connected' };
      }
      if (!groupId || !participants || !participants.length) {
        return { error: 'groupId and participants[] are required' };
      }
      const result = await sock.groupParticipantsUpdate(groupId, participants, 'add');
      return { success: true, result };
    },

    async groupKick({ groupId, participants }) {
      const sock = bridge.getSocket();
      if (!sock || !bridge.isConnected()) {
        return { error: 'WhatsApp not connected' };
      }
      if (!groupId || !participants || !participants.length) {
        return { error: 'groupId and participants[] are required' };
      }
      const result = await sock.groupParticipantsUpdate(groupId, participants, 'remove');
      return { success: true, result };
    },

    async groupPromote({ groupId, participants }) {
      const sock = bridge.getSocket();
      if (!sock || !bridge.isConnected()) {
        return { error: 'WhatsApp not connected' };
      }
      if (!groupId || !participants || !participants.length) {
        return { error: 'groupId and participants[] are required' };
      }
      const result = await sock.groupParticipantsUpdate(groupId, participants, 'promote');
      return { success: true, result };
    },

    async groupDemote({ groupId, participants }) {
      const sock = bridge.getSocket();
      if (!sock || !bridge.isConnected()) {
        return { error: 'WhatsApp not connected' };
      }
      if (!groupId || !participants || !participants.length) {
        return { error: 'groupId and participants[] are required' };
      }
      const result = await sock.groupParticipantsUpdate(groupId, participants, 'demote');
      return { success: true, result };
    },

    async groupUpdateSubject({ groupId, subject }) {
      const sock = bridge.getSocket();
      if (!sock || !bridge.isConnected()) {
        return { error: 'WhatsApp not connected' };
      }
      if (!groupId || subject === undefined) {
        return { error: 'groupId and subject are required' };
      }
      await sock.groupUpdateSubject(groupId, subject);
      return { success: true };
    },

    async groupUpdateDescription({ groupId, description }) {
      const sock = bridge.getSocket();
      if (!sock || !bridge.isConnected()) {
        return { error: 'WhatsApp not connected' };
      }
      if (!groupId) {
        return { error: 'groupId is required' };
      }
      await sock.groupUpdateDescription(groupId, description || '');
      return { success: true };
    },

    async sendPresenceUpdate({ chatId, presence }) {
      const sock = bridge.getSocket();
      if (!sock || !bridge.isConnected()) {
        return { error: 'WhatsApp not connected' };
      }
      // presence: 'composing', 'recording', 'paused', 'available', 'unavailable'
      await sock.sendPresenceUpdate(presence || 'composing', chatId);
      return { success: true };
    },

    async deleteMessage({ chatId, messageId, fromMe }) {
      const sock = bridge.getSocket();
      if (!sock || !bridge.isConnected()) {
        return { error: 'WhatsApp not connected' };
      }
      if (!chatId || !messageId) {
        return { error: 'chatId and messageId are required' };
      }
      await sock.sendMessage(chatId, {
        delete: { remoteJid: chatId, id: messageId, fromMe: fromMe !== false },
      });
      return { success: true };
    },

    async getChats() {
      const sock = bridge.getSocket();
      if (!sock || !bridge.isConnected()) {
        return { error: 'WhatsApp not connected' };
      }
      // Baileys stores chats in memory after history sync
      const chats = await sock.groupFetchAllParticipating();
      const chatList = Object.entries(chats).map(([id, meta]) => ({
        id,
        name: meta.subject || id,
        participantCount: meta.participants?.length || 0,
      }));
      return { chats: chatList };
    },

    async registerWebhook({ callbackUrl, events }) {
      if (!callbackUrl) {
        return { error: 'callbackUrl is required' };
      }
      const webhookId = webhookManager.register(callbackUrl, events || ['message.received']);
      return { webhookId };
    },

    async unregisterWebhook({ webhookId }) {
      if (!webhookId) {
        return { error: 'webhookId is required' };
      }
      const success = webhookManager.unregister(webhookId);
      return { success };
    },

    async listWebhooks() {
      return { webhooks: webhookManager.list() };
    },

    async fetchMessages({ chatId, count }) {
      if (!chatId) {
        return { error: 'chatId is required' };
      }
      const requested = count || 20;

      // If the store has no messages for this chat, wait for the initial
      // history sync to arrive (with timeout). This handles the case where
      // the bridge just started and Baileys hasn't delivered history yet.
      if (messageStore.count(chatId) < requested) {
        await messageStore.waitForHistory(chatId, requested);
      }

      // If still not enough, attempt on-demand history sync
      if (messageStore.count(chatId) < requested) {
        const cursor = messageStore.getOldestMessage(chatId);
        if (cursor) {
          console.log(`[action] fetchMessages: store has ${messageStore.count(chatId)}/${requested}, requesting on-demand sync...`);
          const synced = await bridge.requestHistory(requested, cursor);
          if (!synced) {
            console.warn(`[action] fetchMessages: on-demand sync for ${chatId} did not arrive; answering the ${messageStore.count(chatId)} messages the store holds`);
          }
        }
      }

      const rawMessages = messageStore.getRawMessages(chatId, requested);
      const sock = bridge.getSocket();

      // Serialize raw WAMessages, lazy-downloading audio at query time
      // History carries what a message SAYS, never its bytes. A voice
      // bot reading its own history would otherwise download every
      // clip it ever received on every read, and the base64 would ride
      // the pulse into the journal. The entry names the message, and
      // BaileyFetchMedia pulls one message's bytes when something
      // actually needs them.
      const messages = rawMessages.map((msg) => {
        const { content, messageType } = extractTextContent(msg);
        return {
          from: msg.key.remoteJid,
          pushName: msg.pushName || null,
          content,
          messageType,
          messageId: msg.key.id,
          timestamp: typeof msg.messageTimestamp === 'number'
            ? msg.messageTimestamp
            : Number(msg.messageTimestamp) || 0,
          fromMe: !!msg.key.fromMe,
        };
      });

      return { messages };
    },
  };

  return async (req, res) => {
    const { action, payload } = req.body;

    if (!action || typeof action !== 'string') {
      return res.status(400).json({ error: 'Missing or invalid "action" field' });
    }

    const handler = handlers[action];
    if (!handler) {
      return res.status(400).json({ error: `Unknown action: ${action}` });
    }

    // Every outcome leaves one line, the refusals included: a
    // `result.error` answer fails the node that asked, and the pod
    // log is where that failure is read back from.
    try {
      const result = await handler(payload || {});
      if (result?.error) {
        console.error(`[action] ${action} refused: ${result.error}`);
      } else {
        console.log(`[action] ${action} ok`);
      }
      // SYNC: the /action envelope <-> crates/weft-dispatcher/src/api/infra.rs (infra_action_result), catalog/postgres/database/images/credential/bootstrap.py
      res.json({ result });
    } catch (err) {
      console.error(`[action] ${action} failed:`, err);
      res.status(500).json({ error: err.message || 'Action failed' });
    }
  };
}
