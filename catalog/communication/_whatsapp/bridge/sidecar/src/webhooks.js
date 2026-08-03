/**
 * Manages webhook registrations and dispatches events to registered callbacks.
 * 
 * Nodes (like WhatsAppReceive trigger) register a callback URL and a list of
 * events they want. When the bridge emits an event, all matching webhooks
 * get a POST with the event data.
 */
export class WebhookManager {
  constructor() {
    this.webhooks = new Map(); // webhookId -> { callbackUrl, events }
    this.sseClients = new Set(); // { res, events }
    this.counter = 0;
  }

  addSseClient(res, events = ['message.received']) {
    const client = { res, events };
    this.sseClients.add(client);
    console.log(`[webhooks] SSE client connected (events: ${events.join(', ')}), total: ${this.sseClients.size}`);
    res.on('close', () => {
      this.sseClients.delete(client);
      console.log(`[webhooks] SSE client disconnected, total: ${this.sseClients.size}`);
    });
  }

  register(callbackUrl, events = ['message.received']) {
    const webhookId = `wh_${++this.counter}_${Date.now()}`;
    this.webhooks.set(webhookId, { callbackUrl, events });
    console.log(`[webhooks] Registered ${webhookId} -> ${callbackUrl} (events: ${events.join(', ')})`);
    return webhookId;
  }

  unregister(webhookId) {
    const existed = this.webhooks.delete(webhookId);
    if (existed) {
      console.log(`[webhooks] Unregistered ${webhookId}`);
    }
    return existed;
  }

  async emit(event, data) {
    // Push to SSE subscribers
    for (const client of this.sseClients) {
      if (!client.events.includes(event) && !client.events.includes('*')) continue;
      try {
        client.res.write(`data: ${JSON.stringify({ event, data })}\n\n`);
      } catch (err) {
        console.error(`[webhooks] SSE write failed:`, err.message);
        this.sseClients.delete(client);
      }
    }

    // Push to webhook callbacks
    const promises = [];
    for (const [id, wh] of this.webhooks) {
      if (!wh.events.includes(event) && !wh.events.includes('*')) continue;

      promises.push(
        fetch(wh.callbackUrl, {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({ event, data }),
          signal: AbortSignal.timeout(10_000),
        }).catch((err) => {
          console.error(`[webhooks] Failed to deliver ${event} to ${id} (${wh.callbackUrl}):`, err.message);
        })
      );
    }
    await Promise.allSettled(promises);
  }

  list() {
    const result = [];
    for (const [id, wh] of this.webhooks) {
      result.push({ webhookId: id, callbackUrl: wh.callbackUrl, events: wh.events });
    }
    return result;
  }
}
