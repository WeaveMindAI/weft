import { pipeline } from 'node:stream';
import express from 'express';
import { createBridge } from './bridge.js';
import { createActionRouter } from './actions.js';
import { WebhookManager } from './webhooks.js';
import { MessageStore, contentDisposition } from './message-store.js';

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
    console.log(`[weft-whatsapp-bridge] ${sig} received, flushing message store...`);
    messageStore.flushSync();
    process.exit(0);
  });
}

// Standard endpoint surface
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
  // The one way out of any pairing state without tearing the infra
  // down: the phone paired to this bridge is detached and a fresh QR
  // code takes its place. Offered in every state, because the state
  // that needs it most is the one where nothing else works.
  items.push({
    type: 'text',
    label: 'Phone',
    data: state.status === 'connected' ? 'paired' : 'not paired',
    action: {
      label: 'Disconnect phone',
      actionKind: 'unpair',
      confirm: 'Detach the paired phone from this bridge and show a new QR code? Messages already stored are kept.',
    },
  });

  res.json({ items });
});

// SSE event stream, clients connect and receive events in real time.
// This avoids the bridge needing to POST back to a callback URL (which
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

  // Streamed through, never held whole: the bytes go from WhatsApp to
  // the caller (the receive node, which streams them into storage) as
  // they decrypt, so a long video costs this pod a buffer, not its size.
  let stream;
  try {
    const { downloadMediaMessage } = await import('baileys');
    stream = await downloadMediaMessage(msg, 'stream', {}, {
      reuploadRequest: sock.updateMediaMessage,
    });
  } catch (err) {
    // The id comes from the URL, so it goes in as an argument, never
    // inside the format string.
    console.error('[media] Failed to download media for %s:', messageId, err.message);
    return res.status(500).json({ error: `Failed to download media: ${err.message}` });
  }
  // The filename and the type are the sender's: a header that cannot be
  // set closes the download it would have described.
  try {
    res.set('Content-Type', mediaMessage.mimetype || 'application/octet-stream');
    res.set('Content-Disposition', contentDisposition(mediaMessage.fileName || `media_${messageId}`));
  } catch (err) {
    stream.destroy();
    console.error('[media] Cannot describe media %s in a header:', messageId, err.message);
    return res.status(500).json({ error: `Cannot serve this media: ${err.message}` });
  }

  // Past this point the status is sent, so a failure mid-file can only
  // cut the response short: the caller sees a truncated body and fails,
  // rather than storing half a file as if it were whole. A caller that
  // hangs up closes the download from WhatsApp with it.
  pipeline(stream, res, (err) => {
    if (err) console.error('[media] Download of %s ended early:', messageId, err.message);
  });
});

// Action dispatch (standard endpoint contract)
const actionRouter = createActionRouter(bridge, webhookManager, messageStore);
app.post('/action', actionRouter);

app.listen(PORT, '0.0.0.0', () => {
  console.log(`[weft-whatsapp-bridge] listening on port ${PORT}`);
});
