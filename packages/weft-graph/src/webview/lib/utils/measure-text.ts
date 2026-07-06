// Real text width measurement via a shared 2D canvas, instead of estimating by
// character count. Character-count estimates (chars * fixed-px) are wrong for any
// proportional font: `iiii` and `WWWW` get the same width, so wide labels clip
// and narrow ones over-reserve. `measureText` uses the SAME font metrics the
// browser paints with, so the result is "the exact same computation as what is
// rendered". The canvas + per-font measurements are cached: measuring is cheap
// and synchronous, so callers stay synchronous.

let ctx: CanvasRenderingContext2D | null = null;
function context(): CanvasRenderingContext2D | null {
  if (ctx) return ctx;
  if (typeof document === 'undefined') return null; // non-DOM (tests/SSR): caller falls back
  ctx = document.createElement('canvas').getContext('2d');
  return ctx;
}

// Cache by `${font} ${text}` so repeated labels (port names recur across nodes)
// don't re-measure. The key space is bounded by the distinct (font, label) pairs
// in a project; cleared only on reload.
const cache = new Map<string, number>();

/** Measured pixel width of `text` rendered in `font` (a CSS `font` shorthand,
 *  e.g. `"10px ui-sans-serif, system-ui, sans-serif"`). Falls back to a coarse
 *  estimate only when no canvas is available (non-DOM env, e.g. unit tests). */
export function measureTextWidth(text: string, font: string): number {
  const key = `${font} ${text}`;
  const hit = cache.get(key);
  if (hit !== undefined) return hit;
  const c = context();
  let w: number;
  if (c) {
    c.font = font;
    w = c.measureText(text).width;
  } else {
    w = text.length * 6.5;
  }
  cache.set(key, w);
  return w;
}

// The font-family is read LIVE from the rendered document (body inherits the app
// font), so the measurement font tracks whatever the UI actually paints with, no
// hardcoded duplicate of the CSS font stack to drift. Cached after first read.
let familyCache: string | null = null;
function fontFamily(): string {
  if (familyCache) return familyCache;
  if (typeof document === 'undefined') return (familyCache = 'sans-serif');
  return (familyCache = getComputedStyle(document.body).fontFamily || 'sans-serif');
}

/** CSS `font` shorthand for a node's port labels at `sizePx` (the labels render
 *  as `text-[Npx]` in the app's default sans family at normal weight). */
export function nodeLabelFont(sizePx: number): string {
  return `${sizePx}px ${fontFamily()}`;
}
