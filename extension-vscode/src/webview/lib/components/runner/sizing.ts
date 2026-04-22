/**
 * Size + spacing parsing for the runner DSL.
 *
 * The grammar accepts both a named ladder (sm/md/lg/xl/2xl/full/...) and
 * raw CSS values (40vh, 320px, 50%, calc(100vh - 80px), ...). The named
 * ladder is sugar for a common set of pixel/vh values; raw CSS is the escape
 * hatch when the named set isn't enough.
 *
 * All helpers return either `undefined` (no override) or a valid CSS string
 * that can be dropped into a `style="..."` attribute.
 */

// ── Heights ────────────────────────────────────────────────────────────────

const HEIGHT_LADDER: Record<string, string> = {
	xs: '80px',
	sm: '120px',
	md: '200px',
	lg: '320px',
	xl: '480px',
	'2xl': '640px',
	half: '50vh',
	viewport: '100vh',
	full: '100%',
	screen: '100vh',
};

const RAW_CSS_UNIT = /^-?\d+(?:\.\d+)?(?:px|rem|em|vh|vw|svh|lvh|dvh|%|ch|ex|pt)$/;
const CALC_EXPR = /^(?:calc|min|max|clamp)\(.+\)$/;

/**
 * Resolve a named preset or raw CSS string into a valid CSS length.
 * Returns undefined for unknown input so callers can cleanly omit the style.
 */
export function parseSize(raw: string | undefined): string | undefined {
	if (!raw) return undefined;
	const value = raw.trim();
	if (!value) return undefined;
	if (HEIGHT_LADDER[value]) return HEIGHT_LADDER[value];
	if (RAW_CSS_UNIT.test(value)) return value;
	if (CALC_EXPR.test(value)) return value;
	// Accept bare numbers as pixels so "320" works the same as "320px".
	if (/^\d+(?:\.\d+)?$/.test(value)) return `${value}px`;
	return undefined;
}

/**
 * Build a style fragment for height/minHeight/maxHeight/width.
 * Skips any that aren't set. Returns an empty string when nothing applies.
 */
export function sizingStyle(item: {
	height?: string;
	minHeight?: string;
	maxHeight?: string;
	width?: string;
}): string {
	const parts: string[] = [];
	const h = parseSize(item.height);
	const mh = parseSize(item.minHeight);
	const mxh = parseSize(item.maxHeight);
	const w = parseSize(item.width);
	if (h) parts.push(`height: ${h}`);
	if (mh) parts.push(`min-height: ${mh}`);
	if (mxh) parts.push(`max-height: ${mxh}`);
	if (w) parts.push(`width: ${w}`);
	return parts.join('; ');
}

// ── Spacing (padding, gap) ─────────────────────────────────────────────────

const PADDING_LADDER: Record<string, string> = {
	none: '0',
	xs: '0.5rem',
	sm: '1rem',
	md: '1.5rem',
	lg: '2rem',
	xl: '3rem',
	'2xl': '4rem',
	'3xl': '6rem',
};

export function paddingValue(raw: string | undefined, fallback = 'md'): string {
	if (!raw) return PADDING_LADDER[fallback] ?? '1.5rem';
	const v = raw.trim();
	if (PADDING_LADDER[v]) return PADDING_LADDER[v];
	// Allow raw CSS for padding too.
	if (RAW_CSS_UNIT.test(v) || CALC_EXPR.test(v)) return v;
	if (/^\d+(?:\.\d+)?$/.test(v)) return `${v}px`;
	return PADDING_LADDER[fallback] ?? '1.5rem';
}

const GAP_LADDER: Record<string, string> = {
	none: '0',
	xs: '0.5rem',
	sm: '0.75rem',
	md: '1.5rem',
	lg: '2rem',
	xl: '3rem',
	'2xl': '4rem',
};

export function gapValue(raw: string | undefined, fallback = 'md'): string {
	if (!raw) return GAP_LADDER[fallback] ?? '1.5rem';
	const v = raw.trim();
	if (GAP_LADDER[v]) return GAP_LADDER[v];
	if (RAW_CSS_UNIT.test(v) || CALC_EXPR.test(v)) return v;
	if (/^\d+(?:\.\d+)?$/.test(v)) return `${v}px`;
	return GAP_LADDER[fallback] ?? '1.5rem';
}

// ── Column ratios ──────────────────────────────────────────────────────────

/**
 * Parse an asymmetric column ratio. Accepts "1:1", "2:1", "60/40",
 * "1fr 2fr". Returns a valid `grid-template-columns` string, or
 * `undefined` when the caller should fall back to the default
 * `repeat(cols, minmax(0, 1fr))` equal split.
 */
export function parseColumnRatio(raw: string | undefined, cols: number): string | undefined {
	if (!raw) return undefined;
	const v = raw.trim();
	// Already a grid-template-columns value, pass through.
	if (v.includes('fr') || v.includes('minmax')) return v;
	// "a:b:c" or "a:b"
	if (v.includes(':')) {
		const parts = v.split(':').map(p => Number(p.trim())).filter(n => Number.isFinite(n) && n > 0);
		if (parts.length >= 2) {
			return parts.map(p => `${p}fr`).join(' ');
		}
	}
	// "60/40" or "60-40-20" style
	if (v.includes('/') || v.includes('-')) {
		const parts = v.split(/[\/-]/).map(p => Number(p.trim())).filter(n => Number.isFinite(n) && n > 0);
		if (parts.length >= 2) {
			return parts.map(p => `${p}fr`).join(' ');
		}
	}
	// Single number: treat as repeat
	if (/^\d+$/.test(v)) {
		return `repeat(${cols}, minmax(0, 1fr))`;
	}
	return undefined;
}

// ── Radius ─────────────────────────────────────────────────────────────────

const RADIUS_LADDER: Record<string, string> = {
	none: '0',
	sm: '0.25rem',
	md: '0.5rem',
	lg: '0.75rem',
	xl: '1rem',
	'2xl': '1.5rem',
	'3xl': '2rem',
	full: '9999px',
};

export function radiusValue(raw: string | undefined, fallback = 'xl'): string {
	if (!raw) return RADIUS_LADDER[fallback] ?? '1rem';
	const v = raw.trim();
	if (RADIUS_LADDER[v]) return RADIUS_LADDER[v];
	if (RAW_CSS_UNIT.test(v)) return v;
	if (/^\d+(?:\.\d+)?$/.test(v)) return `${v}px`;
	return RADIUS_LADDER[fallback] ?? '1rem';
}

// ── Shadow ─────────────────────────────────────────────────────────────────

const SHADOW_LADDER: Record<string, string> = {
	none: 'none',
	sm: '0 1px 2px 0 rgba(0,0,0,0.05)',
	md: '0 4px 6px -1px rgba(0,0,0,0.08), 0 2px 4px -1px rgba(0,0,0,0.04)',
	lg: '0 10px 15px -3px rgba(0,0,0,0.10), 0 4px 6px -2px rgba(0,0,0,0.04)',
	xl: '0 20px 25px -5px rgba(0,0,0,0.12), 0 10px 10px -5px rgba(0,0,0,0.04)',
	'2xl': '0 25px 50px -12px rgba(0,0,0,0.25)',
	glow: '0 0 40px 0 rgba(124,58,237,0.25)',
};

export function shadowValue(raw: string | undefined, fallback = 'sm'): string {
	if (!raw) return SHADOW_LADDER[fallback] ?? 'none';
	const v = raw.trim();
	if (SHADOW_LADDER[v]) return SHADOW_LADDER[v];
	return v; // Trust raw CSS.
}

// ── Blur ───────────────────────────────────────────────────────────────────

const BLUR_LADDER: Record<string, string> = {
	none: '0',
	sm: '4px',
	md: '8px',
	lg: '16px',
	xl: '24px',
};

export function blurValue(raw: string | undefined, fallback = 'md'): string {
	if (!raw) return BLUR_LADDER[fallback] ?? '8px';
	const v = raw.trim();
	if (BLUR_LADDER[v]) return BLUR_LADDER[v];
	if (RAW_CSS_UNIT.test(v)) return v;
	if (/^\d+(?:\.\d+)?$/.test(v)) return `${v}px`;
	return BLUR_LADDER[fallback] ?? '8px';
}
