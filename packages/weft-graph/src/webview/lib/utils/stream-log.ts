/// How one line of a recorded stream reads on screen.
///
/// Two streams are journaled the same way and rendered the same way: a
/// bus's traffic and a run's conversation with its caller. The rules
/// they share live here rather than in each panel, because they are the
/// rules that make a line HONEST (a message whose content was cut says
/// so and says its real size, and raw bytes say they were never written
/// down) and a second copy of those is a second chance to drop one.
import type { WirePayload } from '../../../protocol';

/// Wall-clock time of a line, as a reader scanning a log wants it.
export function formatClockTime(atUnix: number): string {
  const d = new Date(atUnix * 1000);
  const hh = String(d.getHours()).padStart(2, '0');
  const mm = String(d.getMinutes()).padStart(2, '0');
  const ss = String(d.getSeconds()).padStart(2, '0');
  return `${hh}:${mm}:${ss}`;
}

/// A payload as text. A string is itself; anything else is its JSON.
export function prettyPayload(value: unknown): string {
  return typeof value === 'string' ? value : JSON.stringify(value);
}

/// What one recorded message's body reads as.
///
/// `what` names the kind of thing for the binary case ("message",
/// "frame"), since that line can only say what went past, never what it
/// said. A payload the journal cut short is shown WITH its real size, so
/// nobody reads a trimmed message as the whole of what was sent.
export function formatStreamBody(
  payload: WirePayload | undefined,
  byteSize: number,
  trimmed: boolean | undefined,
  what: string,
): string {
  if (!payload || payload.kind === 'bytes') {
    return `sent ${what} of ${byteSize} bytes (binary)`;
  }
  const body = prettyPayload(payload.data);
  return trimmed ? `${body}\n(${byteSize} bytes in all, shown trimmed)` : body;
}
