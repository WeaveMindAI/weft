import { downloadMediaMessage } from 'baileys';
import { extractTextContent } from './message-store.js';

/**
 * Action dispatch router for the standard sidecar POST /action contract.
 * 
 * Each action maps to a Baileys socket method. The bridge provides the
 * live socket, and the webhook manager handles webhook registration.
 */
export function createActionRouter(bridge, webhookManager, messageStore) {
  const handlers = {
    async ping() {
      // "ready" means the server can process actions, NOT that the bridge
      // is connected to WhatsApp. The WhatsApp connection is user-initiated
      // (QR scan) and happens after provisioning. As long as the Express
      // server is up and the action router can dispatch, we're ready.
      return { ready: true };
    },

    async sendMessage({ to, text }) {
      const sock = bridge.getSocket();
      if (!sock || !bridge.isConnected()) {
        return { error: 'WhatsApp not connected' };
      }
      const result = await sock.sendMessage(to, { text });
      return { messageId: result.key.id };
    },

    async sendMedia({ to, mediaUrl, caption, mimetype, filename }) {
      const sock = bridge.getSocket();
      if (!sock || !bridge.isConnected()) {
        return { error: 'WhatsApp not connected' };
      }

      // Infer mimetype from URL extension if not provided
      let resolvedMime = mimetype || '';
      if (!resolvedMime) {
        try {
          const pathname = new URL(mediaUrl).pathname.toLowerCase();
          const ext = pathname.split('.').pop();
          const mimeMap = {
            'png': 'image/png', 'jpg': 'image/jpeg', 'jpeg': 'image/jpeg',
            'gif': 'image/gif', 'webp': 'image/webp', 'svg': 'image/svg+xml',
            'mp4': 'video/mp4', 'webm': 'video/webm', 'mov': 'video/quicktime',
            'mp3': 'audio/mpeg', 'ogg': 'audio/ogg', 'wav': 'audio/wav',
            'opus': 'audio/opus', 'flac': 'audio/flac',
            'pdf': 'application/pdf', 'doc': 'application/msword',
          };
          resolvedMime = mimeMap[ext] || '';
        } catch { /* invalid URL, leave empty */ }
      }

      const mediaType = resolvedMime.startsWith('image/') ? 'image'
        : resolvedMime.startsWith('video/') ? 'video'
        : resolvedMime.startsWith('audio/') ? 'audio'
        : 'document';

      const msg = {
        [mediaType]: { url: mediaUrl },
        caption: caption || undefined,
        mimetype: resolvedMime || undefined,
        fileName: filename || undefined,
      };
      const result = await sock.sendMessage(to, msg);
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

    async getContacts() {
      const sock = bridge.getSocket();
      if (!sock || !bridge.isConnected()) {
        return { error: 'WhatsApp not connected' };
      }
      // Contacts are populated through history sync events
      // For now, return what's available via the store
      return { contacts: [] };
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
      // the sidecar just started and Baileys hasn't delivered history yet.
      if (messageStore.count(chatId) < requested) {
        await messageStore.waitForHistory(chatId, requested);
      }

      // If still not enough, attempt on-demand history sync
      if (messageStore.count(chatId) < requested) {
        const cursor = messageStore.getOldestMessage(chatId);
        if (cursor) {
          console.log(`[action] fetchMessages: store has ${messageStore.count(chatId)}/${requested}, requesting on-demand sync...`);
          await bridge.requestHistory(requested, cursor);
        }
      }

      const rawMessages = messageStore.getRawMessages(chatId, requested);
      const sock = bridge.getSocket();

      // Serialize raw WAMessages, lazy-downloading audio at query time
      const messages = await Promise.all(rawMessages.map(async (msg) => {
        const { content, messageType } = extractTextContent(msg);
        const entry = {
          from: msg.key.remoteJid,
          pushName: msg.pushName || null,
          content,
          audio: null,
          messageType,
          messageId: msg.key.id,
          timestamp: typeof msg.messageTimestamp === 'number'
            ? msg.messageTimestamp
            : Number(msg.messageTimestamp) || 0,
          fromMe: !!msg.key.fromMe,
        };

        // Lazy-download audio data from protobuf
        if (messageType === 'audio' && msg.message?.audioMessage && sock) {
          try {
            const buffer = await downloadMediaMessage(msg, 'buffer', {}, {
              reuploadRequest: sock.updateMediaMessage,
            });
            const mimetype = msg.message.audioMessage.mimetype || 'audio/ogg';
            entry.audio = `data:${mimetype};base64,${buffer.toString('base64')}`;
          } catch (err) {
            console.warn(`[action] Failed to download audio for ${msg.key.id}:`, err.message);
            entry.content = '[audio message, media unavailable]';
          }
        }

        return entry;
      }));

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

    try {
      const result = await handler(payload || {});
      res.json({ result });
    } catch (err) {
      console.error(`[action] ${action} failed:`, err);
      res.status(500).json({ error: err.message || 'Action failed' });
    }
  };
}
