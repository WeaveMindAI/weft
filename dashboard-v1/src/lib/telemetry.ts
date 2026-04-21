/**
 * Telemetry emitter, sends structured product events to the parent window.
 *
 * This is NOT analytics code. It's a generic event bus. The parent window
 * (website shell) decides what to do with these events. Self-hosted users
 * can ignore them entirely.
 *
 * Events are fire-and-forget postMessages. No external dependencies.
 * Events are buffered until the parent origin is known (set during auth handshake).
 */

const TELEMETRY_MSG_TYPE = 'wm:telemetry';

let parentOrigin: string | null = null;
let buffer: Array<{ eventName: string; properties: Record<string, unknown>; timestamp: string }> = [];

/** Set the expected parent origin (called once from auth-gate after handshake). */
export function setTelemetryParentOrigin(origin: string) {
	parentOrigin = origin;
	// Flush buffered events
	for (const evt of buffer) {
		doEmit(evt.eventName, evt.properties, evt.timestamp);
	}
	buffer = [];
}

/** Emit a telemetry event to the parent window. */
export function emit(eventName: string, properties: Record<string, unknown> = {}) {
	if (!window.parent || window.parent === window) return;

	const timestamp = new Date().toISOString();
	if (!parentOrigin) {
		// Buffer until origin is known
		buffer.push({ eventName, properties, timestamp });
		return;
	}

	doEmit(eventName, properties, timestamp);
}

function doEmit(eventName: string, properties: Record<string, unknown>, timestamp: string) {
	if (!window.parent || window.parent === window || !parentOrigin) return;
	window.parent.postMessage({
		type: TELEMETRY_MSG_TYPE,
		eventName,
		properties,
		timestamp,
	}, parentOrigin);
}
