/**
 * WeaveMind Sidecar Example (JavaScript)
 *
 * A minimal self-contained sidecar implementing the full WeaveMind sidecar contract.
 * Clone this directory and extend it to build your own sidecar.
 *
 * Contract endpoints:
 *   GET  /health   liveness probe (required)
 *   POST /action   { action, payload } → { result } (required)
 *   GET  /outputs  runtime values for node output ports (required, return {} if none)
 *   GET  /live     dashboard live data rendering (required, no-op default)
 *   GET  /events   SSE stream for real-time triggers (optional)
 *
 * Required actions (via POST /action):
 *   ping: readiness check. Must return { ready: true } when the sidecar
 *          can process actions. "Ready" means the server is operational and
 *          core dependencies are initialized (e.g., DB pool connected).
 *          Do NOT gate on user-initiated connections (QR scans, OAuth, etc.).
 *          The orchestrator blocks provisioning until ping returns ready.
 *          Returns { ready: true } or { ready: false, reason: "..." }.
 *
 * Environment variables:
 *   PORT: HTTP port (default: 8090)
 */

import express from "express";

const app = express();
app.use(express.json());

// =============================================================================
// YOUR STATE, put your connections, caches, etc. here
// =============================================================================

// const db = await connectToDatabase(process.env.DATABASE_URL);

// =============================================================================
// ACTION DISPATCH, add your actions here
// =============================================================================

async function dispatchAction(action, payload) {
  switch (action) {
    // Required by contract: readiness check.
    // Return ready: true when the server can process actions.
    // Gate on core dependencies (DB pool, etc.), NOT on user-initiated
    // connections (QR scans, OAuth flows).
    // The orchestrator blocks provisioning until this returns ready.
    case "ping": {
      // TODO: add your own checks here, e.g.:
      //   await db.query("SELECT 1");
      return { ready: true };
    }
    default:
      throw new Error(`Unknown action: ${action}`);
  }
}

// =============================================================================
// CONTRACT HANDLERS, you usually don't need to modify these
// =============================================================================

app.get("/health", (_req, res) => {
  res.send("ok");
});

app.post("/action", async (req, res) => {
  const { action, payload } = req.body;
  try {
    const result = await dispatchAction(action, payload ?? {});
    res.json({ result });
  } catch (err) {
    res.status(400).json({ error: err.message });
  }
});

app.get("/outputs", (_req, res) => {
  // Return runtime-computed values exposed as node output ports.
  // Platform values (instanceId, endpointUrl) are added automatically.
  // Add your own here if needed.
  res.json({});
});

app.get("/live", (_req, res) => {
  // Return data for the dashboard live panel.
  // No-op default, override if your sidecar has a live view.
  res.json({});
});

app.get("/events", (req, res) => {
  // SSE stream for real-time trigger events.
  // The orchestrator connects here to receive events like incoming messages.
  // Override this to push events from your sidecar.
  //
  // Example: emit an event with
  //   res.write(`event: message.received\ndata: ${JSON.stringify(payload)}\n\n`);
  //
  // Filter which events the client subscribes to via ?events=event1,event2
  res.writeHead(200, {
    "Content-Type": "text/event-stream",
    "Cache-Control": "no-cache",
    "Connection": "keep-alive",
  });
  res.write(":ok\n\n");

  // Keep connection alive
  const keepAlive = setInterval(() => res.write(":keepalive\n\n"), 15000);
  req.on("close", () => clearInterval(keepAlive));
});

// =============================================================================
// START
// =============================================================================

const port = parseInt(process.env.PORT ?? "8090", 10);
app.listen(port, () => {
  console.log(`Sidecar listening on 0.0.0.0:${port}`);
});
