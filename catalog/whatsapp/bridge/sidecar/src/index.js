import express from 'express';
import { createBridge } from './bridge.js';
import { createActionRouter } from './actions.js';
import { WebhookManager } from './webhooks.js';
import { MessageStore } from './message-store.js';

const PORT = parseInt(process.env.PORT || '8090', 10);
const AUTH_DIR = process.env.AUTH_DIR || '/data/auth';

const app = express();
app.use(express.json({ limit: '10mb' }));

const webhookManager = new WebhookManager();
const MESSAGES_PATH = `${AUTH_DIR}/messages.json`;
const messageStore = new MessageStore(500, MESSAGES_PATH);
const bridge = await createBridge(AUTH_DIR, webhookManager, messageStore);

// Flush message store to disk on shutdown
for (const sig of ['SIGINT', 'SIGTERM']) {
  process.on(sig, () => {
    console.log(`[sidecar-whatsapp-bridge] ${sig} received, flushing message store...`);
    messageStore.flushSync();
    process.exit(0);
  });
}

// Standard sidecar endpoints
app.get('/health', (_req, res) => {
  res.json({ status: 'ok' });
});

app.get('/outputs', (_req, res) => {
  const state = bridge.getState();
  res.json({
    status: state.status,
    phoneNumber: state.phoneNumber || null,
    jid: state.jid || null,
    pushName: state.pushName || null,
  });
});

// WhatsApp-specific endpoints
app.get('/qr', (_req, res) => {
  const qr = bridge.getQr();
  res.json({ qr });
});

app.get('/status', (_req, res) => {
  const state = bridge.getState();
  res.json({ status: state.status });
});

// Live data endpoint (generic pattern for dashboard rendering)
app.get('/live', (_req, res) => {
  const state = bridge.getState();
  const items = [];

  if (state.status === 'qr_pending') {
    const qr = bridge.getQr();
    if (qr) {
      items.push({ type: 'image', label: 'Scan with WhatsApp', data: qr });
    }
    items.push({ type: 'text', label: 'Status', data: 'Waiting for QR scan...' });
  } else if (state.status === 'connecting') {
    items.push({ type: 'text', label: 'Status', data: 'Connecting...' });
  } else if (state.status === 'connected') {
    if (state.pushName) {
      items.push({ type: 'text', label: 'Account', data: state.pushName });
    }
    if (state.phoneNumber) {
      items.push({ type: 'text', label: 'Phone', data: state.phoneNumber });
    }
    items.push({ type: 'text', label: 'Status', data: 'Connected' });
  } else {
    items.push({ type: 'text', label: 'Status', data: state.status });
  }

  res.json({ items });
});

// SSE event stream, clients connect and receive events in real time.
// This avoids the sidecar needing to POST back to a callback URL (which
// fails from inside a k8s pod when the API is on the host).
app.get('/events', (req, res) => {
  res.writeHead(200, {
    'Content-Type': 'text/event-stream',
    'Cache-Control': 'no-cache',
    'Connection': 'keep-alive',
  });
  res.write(':ok\n\n');

  const events = req.query.events
    ? req.query.events.split(',')
    : ['message.received'];

  webhookManager.addSseClient(res, events);
});

// Media download endpoint, serves media from stored Baileys WAMessage protobufs.
// Nodes call this to download image/video/document/audio from received messages.
app.get('/media/:messageId', async (req, res) => {
  const { messageId } = req.params;
  const sock = bridge.getSocket();

  if (!sock) {
    return res.status(503).json({ error: 'WhatsApp not connected' });
  }

  // Find the raw WAMessage across all chats
  const msg = messageStore.findByMessageId(messageId);
  if (!msg) {
    return res.status(404).json({ error: `Message ${messageId} not found in store` });
  }

  const m = msg.message;
  if (!m) {
    return res.status(404).json({ error: 'Message has no content' });
  }

  // Determine which media type is present
  const mediaMessage = m.imageMessage || m.videoMessage || m.audioMessage || m.documentMessage || m.stickerMessage;
  if (!mediaMessage) {
    return res.status(404).json({ error: 'Message does not contain downloadable media' });
  }

  try {
    const { downloadMediaMessage } = await import('baileys');
    const buffer = await downloadMediaMessage(msg, 'buffer', {}, {
      reuploadRequest: sock.updateMediaMessage,
    });

    const mimetype = mediaMessage.mimetype || 'application/octet-stream';
    const filename = mediaMessage.fileName || `media_${messageId}`;

    res.set('Content-Type', mimetype);
    res.set('Content-Disposition', `inline; filename="${filename}"`);
    res.set('X-Mime-Type', mimetype);
    res.set('X-Filename', filename);
    res.send(buffer);
  } catch (err) {
    console.error(`[media] Failed to download media for ${messageId}:`, err.message);
    res.status(500).json({ error: `Failed to download media: ${err.message}` });
  }
});

// Action dispatch (standard sidecar contract)
const actionRouter = createActionRouter(bridge, webhookManager, messageStore);
app.post('/action', actionRouter);

app.listen(PORT, '0.0.0.0', () => {
  console.log(`[sidecar-whatsapp-bridge] listening on port ${PORT}`);
});
