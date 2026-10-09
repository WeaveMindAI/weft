// Whether the editor's live connections to the install should be held
// open right now. A cloud install scales its dispatcher, and the
// database behind it, down to nothing when nobody talks to it; an open
// live stream counts as talking, so a window left open on a cloud
// project would keep both awake (and billed) forever. Once the person
// has been away for AWAY_PAUSE_MS, the live streams to a remote install
// close; the moment they come back, they reopen and resync.
//
// "Away" is the window being unfocused or idle (VS Code's window state:
// `focused`, and `active`, which drops after a short spell without
// input). It has to last the whole stretch: coming back for a second
// starts the count again.
//
// The local install is never paused: its dispatcher runs whatever the
// editor does, and a person may be watching a run on a second screen.
//
// Kept free of the `vscode` module (the window state and the timer are
// handed in) so the vitest suite drives it with a fake clock.

/** How long the person must be away before the live streams to a remote
 *  install close. */
export const AWAY_PAUSE_MS = 5 * 60_000;

/** The two halves of VS Code's `WindowState` that say whether somebody
 *  is there. */
export interface WindowPresence {
  focused: boolean;
  active: boolean;
}

export interface WindowStateSource {
  current(): WindowPresence;
  onDidChange(listener: (state: WindowPresence) => void): { dispose(): void };
}

export interface PresenceTimers {
  set(fn: () => void, ms: number): unknown;
  clear(handle: unknown): void;
}

/** What the presence needs from the client: whether the install is a
 *  remote one, its address for the log line, and when it moves. A
 *  `DispatcherClient` is one. */
export interface InstallSource {
  isRemote(): boolean;
  getBaseUrl(): string | null;
  onInstallChange(listener: () => void): void;
}

/** What a live stream consults: paused now, and when that flips. */
export interface LivePause {
  paused(): boolean;
  onChange(listener: (paused: boolean) => void): void;
}

export class Presence implements LivePause {
  private away = false;
  private awayTimer: unknown;
  /** The value last announced, so a listener hears each flip once. */
  private announced = false;
  private readonly listeners: Array<(paused: boolean) => void> = [];
  private readonly windowSubscription: { dispose(): void };

  constructor(
    private readonly install: InstallSource,
    windowState: WindowStateSource,
    private readonly timers: PresenceTimers,
  ) {
    this.windowSubscription = windowState.onDidChange((state) => this.windowChanged(state));
    // Switching installs can flip it on its own: to the local install
    // resumes, to another remote one stays paused.
    install.onInstallChange(() => this.announce());
    this.windowChanged(windowState.current());
  }

  /** Read live, so a stream asking in the middle of an install switch
   *  gets the answer for the install it is about to reach. */
  paused(): boolean {
    return this.away && this.install.isRemote();
  }

  onChange(listener: (paused: boolean) => void): void {
    this.listeners.push(listener);
  }

  dispose(): void {
    this.windowSubscription.dispose();
    this.clearTimer();
  }

  private windowChanged(state: WindowPresence): void {
    if (state.focused && state.active) {
      this.clearTimer();
      this.away = false;
      this.announce();
      return;
    }
    // Still away: moving between unfocused and idle keeps the count.
    if (this.away || this.awayTimer !== undefined) return;
    this.awayTimer = this.timers.set(() => {
      this.awayTimer = undefined;
      this.away = true;
      this.announce();
    }, AWAY_PAUSE_MS);
  }

  private clearTimer(): void {
    if (this.awayTimer === undefined) return;
    this.timers.clear(this.awayTimer);
    this.awayTimer = undefined;
  }

  private announce(): void {
    const paused = this.paused();
    if (paused === this.announced) return;
    this.announced = paused;
    const url = this.install.getBaseUrl();
    console.log(
      paused
        ? `[weft/presence] away for ${AWAY_PAUSE_MS / 60_000} minutes: live updates from ${url} paused so the cloud install can sleep; they resume when you come back`
        : `[weft/presence] live updates from ${url} resumed`,
    );
    for (const listener of this.listeners) listener(paused);
  }
}
