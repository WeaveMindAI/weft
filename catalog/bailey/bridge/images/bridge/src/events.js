/**
 * Fans the bridge's events out to its `/events` subscribers (Server-Sent
 * Events). The weft listener holds one such stream per BaileyReceive
 * trigger and fires a run for each `message.received` it is sent.
 */
export class EventHub {
  constructor() {
    this.sseClients = new Set(); // { res, events }
  }

  addSseClient(res, events = ['message.received']) {
    const client = { res, events };
    this.sseClients.add(client);
    console.log(`[events] SSE client connected (events: ${events.join(', ')}), total: ${this.sseClients.size}`);
    res.on('close', () => {
      this.sseClients.delete(client);
      console.log(`[events] SSE client disconnected, total: ${this.sseClients.size}`);
    });
  }

  emit(event, data) {
    // Per the SSE spec, the `event:` line sets the event name (defaults
    // to "message" when absent). The weft-listener filters on this name;
    // without an explicit `event:` line every event would be dispatched
    // as "message" and the listener's per-subscription filter
    // (`event_name = "message.received"`) would reject every payload.
    for (const client of this.sseClients) {
      if (!client.events.includes(event) && !client.events.includes('*')) continue;
      try {
        client.res.write(`event: ${event}\ndata: ${JSON.stringify(data)}\n\n`);
      } catch (err) {
        console.error(`[events] SSE write failed:`, err.message);
        this.sseClients.delete(client);
      }
    }
  }
}
