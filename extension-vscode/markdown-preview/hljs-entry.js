// The markdown preview renders in a webview that has no highlight.js of its
// own: VS Code highlights fenced code back in the extension host, and it only
// knows the languages its own copy was built with. So the preview gets a
// highlight.js of its own here, and `highlight-weft.js` (loaded right after
// this file) teaches it weft and paints the blocks.
//
// Core only, no bundled languages: weft is the one language this instance ever
// has to know.
import hljs from 'highlight.js/lib/core';

window.hljs = hljs;
