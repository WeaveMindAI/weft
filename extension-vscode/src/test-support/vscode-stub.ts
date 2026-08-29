// Just enough of the `vscode` module for the pure host-side logic the
// vitest suite exercises. Vitest aliases `vscode` here (the real module
// only exists inside the editor process); anything a test needs beyond
// this is added as the test needs it, never speculatively.

export class TabInputTextDiff {
  constructor(
    public readonly original: { toString(): string },
    public readonly modified: { toString(): string },
  ) {}
}

export const window = {
  tabGroups: {
    all: [] as unknown[],
    activeTabGroup: { activeTab: undefined as unknown },
  },
};
