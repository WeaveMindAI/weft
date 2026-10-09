import { describe, expect, it } from 'vitest';
import { AWAY_PAUSE_MS, Presence, type InstallSource, type WindowPresence } from './presence';

/** A clock the test moves by hand. */
class FakeTimers {
  now = 0;
  private next = 0;
  private readonly pending = new Map<number, { at: number; fn: () => void }>();
  set(fn: () => void, ms: number): unknown {
    const id = this.next++;
    this.pending.set(id, { at: this.now + ms, fn });
    return id;
  }
  clear(handle: unknown): void {
    this.pending.delete(handle as number);
  }
  advance(ms: number): void {
    this.now += ms;
    for (const [id, timer] of [...this.pending]) {
      if (timer.at <= this.now) {
        this.pending.delete(id);
        timer.fn();
      }
    }
  }
}

class FakeWindow {
  state: WindowPresence = { focused: true, active: true };
  private listener: ((state: WindowPresence) => void) | undefined;
  current = () => this.state;
  onDidChange = (listener: (state: WindowPresence) => void) => {
    this.listener = listener;
    return { dispose: () => { this.listener = undefined; } };
  };
  set(state: WindowPresence): void {
    this.state = state;
    this.listener?.(state);
  }
}

class FakeInstall implements InstallSource {
  remote = true;
  private readonly listeners: Array<() => void> = [];
  isRemote = () => this.remote;
  getBaseUrl = () => (this.remote ? 'https://weft.example.com' : 'http://127.0.0.1:9999');
  onInstallChange(listener: () => void): void { this.listeners.push(listener); }
  switchTo(remote: boolean): void {
    this.remote = remote;
    for (const listener of this.listeners) listener();
  }
}

function rig(remote = true) {
  const timers = new FakeTimers();
  const window = new FakeWindow();
  const install = new FakeInstall();
  install.remote = remote;
  const presence = new Presence(install, window, timers);
  const flips: boolean[] = [];
  presence.onChange((paused) => flips.push(paused));
  return { timers, window, install, presence, flips };
}

const away = { focused: false, active: true };
const here = { focused: true, active: true };

describe('presence', () => {
  it('pauses a remote install after five minutes away and resumes at once on return', () => {
    const { timers, window, presence, flips } = rig();
    window.set(away);
    timers.advance(AWAY_PAUSE_MS - 1);
    expect(presence.paused()).toBe(false);
    timers.advance(1);
    expect(presence.paused()).toBe(true);
    window.set(here);
    expect(presence.paused()).toBe(false);
    expect(flips).toEqual([true, false]);
  });

  it('counts an idle focused window as away, and keeps counting across unfocused and idle', () => {
    const { timers, window, presence } = rig();
    window.set({ focused: true, active: false });
    timers.advance(AWAY_PAUSE_MS / 2);
    window.set({ focused: false, active: false });
    timers.advance(AWAY_PAUSE_MS / 2);
    expect(presence.paused()).toBe(true);
  });

  it('starts the count again after a brief return', () => {
    const { timers, window, presence } = rig();
    window.set(away);
    timers.advance(AWAY_PAUSE_MS - 1000);
    window.set(here);
    window.set(away);
    timers.advance(AWAY_PAUSE_MS - 1000);
    expect(presence.paused()).toBe(false);
    timers.advance(1000);
    expect(presence.paused()).toBe(true);
  });

  it('never pauses the local install', () => {
    const { timers, window, presence, flips } = rig(false);
    window.set(away);
    timers.advance(AWAY_PAUSE_MS * 10);
    expect(presence.paused()).toBe(false);
    expect(flips).toEqual([]);
  });

  it('resumes when the install switches to local while paused, and stays paused on another remote', () => {
    const { timers, window, install, presence, flips } = rig();
    window.set(away);
    timers.advance(AWAY_PAUSE_MS);
    install.switchTo(true);
    expect(presence.paused()).toBe(true);
    install.switchTo(false);
    expect(presence.paused()).toBe(false);
    install.switchTo(true);
    expect(presence.paused()).toBe(true);
    expect(flips).toEqual([true, false, true]);
  });

  it('starts counting when the window opens already unfocused', () => {
    const timers = new FakeTimers();
    const window = new FakeWindow();
    window.state = away;
    const presence = new Presence(new FakeInstall(), window, timers);
    timers.advance(AWAY_PAUSE_MS);
    expect(presence.paused()).toBe(true);
  });
});
