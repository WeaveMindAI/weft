// What replaces Plyr's ads, YouTube and Vimeo plugins in the webview
// bundle. The graph view only ever plays a file the runtime stored (a
// plain `<audio>` / `<video>` element), so those three modules could
// never run here, and each one carries code that loads and executes a
// third party's script (Google's IMA ad SDK, an ad server's tag
// builder, the YouTube and Vimeo player loaders). A marketplace
// scanner reads that code as ad and remote-script injection whether
// or not it is reachable, and it was the shape of the package that
// got the VS Code extension refused. The build routes Plyr's own
// source to these stand-ins (see `vite.webview.config.mjs`), so the
// player keeps its controls and none of that code is in the bundle.

/// `plyr.js` builds one only when `config.ads.enabled` is set, which
/// the graph view never sets, and otherwise reads `enabled` off it.
export class Ads {
  constructor() {
    this.enabled = false;
  }
}

/// `media.js` calls `setup` for a source it detected as an embed. A
/// stored file never is, so reaching this is a bug, and it says so.
function refuse(provider) {
  return {
    setup() {
      throw new Error(`${provider} embeds are not bundled in the graph view; it plays stored files only`);
    },
  };
}

export const youtube = refuse('YouTube');
export const vimeo = refuse('Vimeo');
