/**
 * Skin system for the runner page.
 *
 * A skin bundles: page background treatment, font pairing, default card
 * chrome, text colors, accent, and a few layout defaults. The AI (or user)
 * picks a skin with `theme { skin:"studio" }` and the whole page retunes.
 *
 * Individual theme attributes always override skin defaults, so you can pick
 * "studio" and still change the primary color or layout width.
 *
 * Each skin resolves to a `SkinResolved` which RunnerView applies as CSS
 * variables plus a few body classes. Brick renderers read CSS vars only;
 * they don't depend on the skin identity.
 */

import type { RunnerTheme } from '$lib/types';

export interface SkinResolved {
	/** Inline CSS variables applied to the runner root. */
	vars: Record<string, string>;
	/** Extra Tailwind classes applied to the runner root. */
	rootClass: string;
	/** Font family stack as a raw CSS value. */
	fontFamily: string;
	/** Default card treatment when a card brick doesn't override. */
	defaultCard: {
		background: string;
		border: string;
		shadow: string;
		backdropBlur: string;
		radius: string;
	};
	/** Default hero treatment. */
	defaultHero: {
		titleClass: string;
		subtitleClass: string;
		background: string;
	};
	/** Default runner surface (page background). */
	surface: string;
	/** Default radius for cards/inputs. */
	radius: string;
	/** Default navbar treatment. */
	navbar: {
		background: string;
		border: string;
		textClass: string;
	};
}

const FONT_INTER = "'Inter var', 'Inter', system-ui, -apple-system, 'Segoe UI', sans-serif";
const FONT_SERIF = "'Fraunces', 'Source Serif Pro', Georgia, 'Times New Roman', serif";
const FONT_MONO = "'JetBrains Mono', 'Fira Code', ui-monospace, SFMono-Regular, Menlo, Monaco, Consolas, monospace";
const FONT_DISPLAY = "'Cal Sans', 'Inter var', 'Inter', system-ui, sans-serif";
const FONT_PLAYFUL = "'DM Sans', 'Inter', system-ui, sans-serif";

export const SKINS: Record<string, SkinResolved> = {
	default: {
		vars: {
			'--runner-bg': '#fafafa',
			'--runner-fg': '#09090b',
			'--runner-muted': '#71717a',
			'--runner-card-bg': 'rgba(255,255,255,0.9)',
			'--runner-card-border': 'rgba(0,0,0,0.08)',
			'--runner-input-bg': '#f4f4f5',
			'--runner-input-border': 'transparent',
			'--runner-primary': '#7c3aed',
			'--runner-accent': '#ec4899',
		},
		rootClass: 'runner-skin-default',
		fontFamily: FONT_INTER,
		defaultCard: {
			background: 'rgba(255,255,255,0.9)',
			border: '1px solid rgba(0,0,0,0.08)',
			shadow: '0 1px 2px 0 rgba(0,0,0,0.04)',
			backdropBlur: '8px',
			radius: '1rem',
		},
		defaultHero: {
			titleClass: 'font-bold tracking-tight text-zinc-900',
			subtitleClass: 'text-zinc-500',
			background: 'transparent',
		},
		surface: 'linear-gradient(to bottom right, #fafafa, #ffffff 50%, #fafafa)',
		radius: '1rem',
		navbar: {
			background: 'rgba(255,255,255,0.7)',
			border: '1px solid rgba(0,0,0,0.08)',
			textClass: 'text-zinc-900',
		},
	},

	editorial: {
		vars: {
			'--runner-bg': '#fafaf9',
			'--runner-fg': '#1c1917',
			'--runner-muted': '#78716c',
			'--runner-card-bg': 'transparent',
			'--runner-card-border': 'rgba(0,0,0,0.06)',
			'--runner-input-bg': '#f5f5f4',
			'--runner-input-border': 'transparent',
			'--runner-primary': '#0f172a',
			'--runner-accent': '#b45309',
		},
		rootClass: 'runner-skin-editorial',
		fontFamily: FONT_SERIF,
		defaultCard: {
			background: 'transparent',
			border: 'none',
			shadow: 'none',
			backdropBlur: '0',
			radius: '0',
		},
		defaultHero: {
			titleClass: 'font-bold tracking-tight text-stone-900 font-serif',
			subtitleClass: 'text-stone-600 font-serif italic',
			background: 'transparent',
		},
		surface: '#fafaf9',
		radius: '0.5rem',
		navbar: {
			background: 'transparent',
			border: 'none',
			textClass: 'text-stone-800 font-serif',
		},
	},

	studio: {
		vars: {
			'--runner-bg': '#0f0a1e',
			'--runner-fg': '#f5f3ff',
			'--runner-muted': 'rgba(245,243,255,0.65)',
			'--runner-card-bg': 'rgba(255,255,255,0.06)',
			'--runner-card-border': 'rgba(255,255,255,0.12)',
			'--runner-input-bg': 'rgba(255,255,255,0.04)',
			'--runner-input-border': 'rgba(255,255,255,0.08)',
			'--runner-primary': '#a78bfa',
			'--runner-accent': '#f472b6',
		},
		rootClass: 'runner-skin-studio dark',
		fontFamily: FONT_INTER,
		defaultCard: {
			background: 'rgba(255,255,255,0.06)',
			border: '1px solid rgba(255,255,255,0.12)',
			shadow: '0 20px 60px -15px rgba(124,58,237,0.35)',
			backdropBlur: '16px',
			radius: '1.5rem',
		},
		defaultHero: {
			titleClass: 'font-bold tracking-tight bg-clip-text text-transparent bg-gradient-to-br from-white via-violet-200 to-pink-300',
			subtitleClass: 'text-violet-200/80',
			background: 'transparent',
		},
		surface: 'radial-gradient(ellipse 80% 80% at 50% -20%, rgba(124,58,237,0.35), transparent), radial-gradient(ellipse 60% 50% at 80% 50%, rgba(236,72,153,0.25), transparent), #0f0a1e',
		radius: '1.5rem',
		navbar: {
			background: 'rgba(15,10,30,0.7)',
			border: '1px solid rgba(255,255,255,0.08)',
			textClass: 'text-violet-100',
		},
	},

	brutalist: {
		vars: {
			'--runner-bg': '#fef3c7',
			'--runner-fg': '#0c0a09',
			'--runner-muted': '#57534e',
			'--runner-card-bg': '#ffffff',
			'--runner-card-border': '#0c0a09',
			'--runner-input-bg': '#ffffff',
			'--runner-input-border': '#0c0a09',
			'--runner-primary': '#dc2626',
			'--runner-accent': '#0c0a09',
		},
		rootClass: 'runner-skin-brutalist',
		fontFamily: FONT_MONO,
		defaultCard: {
			background: '#ffffff',
			border: '3px solid #0c0a09',
			shadow: '6px 6px 0 0 #0c0a09',
			backdropBlur: '0',
			radius: '0',
		},
		defaultHero: {
			titleClass: 'font-black tracking-tight text-stone-950 uppercase',
			subtitleClass: 'text-stone-800 font-mono',
			background: 'transparent',
		},
		surface: '#fef3c7',
		radius: '0',
		navbar: {
			background: '#ffffff',
			border: '3px solid #0c0a09',
			textClass: 'text-stone-950 font-mono uppercase font-bold',
		},
	},

	terminal: {
		vars: {
			'--runner-bg': '#0a0f0a',
			'--runner-fg': '#86efac',
			'--runner-muted': '#4ade80',
			'--runner-card-bg': 'rgba(134,239,172,0.04)',
			'--runner-card-border': 'rgba(134,239,172,0.25)',
			'--runner-input-bg': 'rgba(134,239,172,0.06)',
			'--runner-input-border': 'rgba(134,239,172,0.3)',
			'--runner-primary': '#4ade80',
			'--runner-accent': '#22d3ee',
		},
		rootClass: 'runner-skin-terminal dark',
		fontFamily: FONT_MONO,
		defaultCard: {
			background: 'rgba(134,239,172,0.04)',
			border: '1px solid rgba(134,239,172,0.25)',
			shadow: '0 0 20px 0 rgba(74,222,128,0.08)',
			backdropBlur: '0',
			radius: '0.25rem',
		},
		defaultHero: {
			titleClass: 'font-mono font-bold tracking-tight text-green-300',
			subtitleClass: 'text-green-500/80 font-mono',
			background: 'transparent',
		},
		surface: 'radial-gradient(ellipse at top, rgba(34,197,94,0.08), transparent), #0a0f0a',
		radius: '0.25rem',
		navbar: {
			background: 'rgba(10,15,10,0.8)',
			border: '1px solid rgba(134,239,172,0.25)',
			textClass: 'text-green-300 font-mono',
		},
	},

	playful: {
		vars: {
			'--runner-bg': '#fff1f2',
			'--runner-fg': '#18181b',
			'--runner-muted': '#71717a',
			'--runner-card-bg': '#ffffff',
			'--runner-card-border': 'rgba(236,72,153,0.25)',
			'--runner-input-bg': '#ffffff',
			'--runner-input-border': 'rgba(236,72,153,0.3)',
			'--runner-primary': '#ec4899',
			'--runner-accent': '#f59e0b',
		},
		rootClass: 'runner-skin-playful',
		fontFamily: FONT_PLAYFUL,
		defaultCard: {
			background: '#ffffff',
			border: '2px solid rgba(236,72,153,0.25)',
			shadow: '0 12px 40px -8px rgba(236,72,153,0.25)',
			backdropBlur: '0',
			radius: '2rem',
		},
		defaultHero: {
			titleClass: 'font-extrabold tracking-tight bg-clip-text text-transparent bg-gradient-to-br from-pink-500 via-orange-400 to-amber-400',
			subtitleClass: 'text-zinc-600',
			background: 'transparent',
		},
		surface: 'radial-gradient(ellipse at top left, rgba(236,72,153,0.15), transparent), radial-gradient(ellipse at bottom right, rgba(245,158,11,0.12), transparent), #fff1f2',
		radius: '2rem',
		navbar: {
			background: 'rgba(255,255,255,0.85)',
			border: '2px solid rgba(236,72,153,0.2)',
			textClass: 'text-zinc-900 font-semibold',
		},
	},
};

/**
 * Resolve a theme into a concrete skin. Applies theme overrides (primary,
 * accent, layout, etc.) on top of the base skin.
 */
export function resolveSkin(theme: RunnerTheme | undefined): SkinResolved {
	const name = (theme?.skin ?? 'default') as keyof typeof SKINS;
	const base = SKINS[name] ?? SKINS.default;
	const vars = { ...base.vars };

	// Theme-level overrides of the CSS vars.
	if (theme?.primary) vars['--runner-primary'] = theme.primary;
	if (theme?.accent) vars['--runner-accent'] = theme.accent;
	if (theme?.background) {
		vars['--runner-bg'] = theme.background;
	}

	return {
		...base,
		vars,
	};
}

/**
 * Map a theme.surface override to a concrete CSS background value. When the
 * theme explicitly sets `surface`, it overrides the skin's default.
 */
export function resolveSurface(theme: RunnerTheme | undefined, skin: SkinResolved): string {
	if (!theme?.surface) return skin.surface;
	switch (theme.surface) {
		case 'plain': return 'var(--runner-bg)';
		case 'subtle': return 'linear-gradient(to bottom right, var(--runner-bg), rgba(255,255,255,0.5) 50%, var(--runner-bg))';
		case 'gradient': return 'linear-gradient(135deg, color-mix(in srgb, var(--runner-primary) 15%, var(--runner-bg)), var(--runner-bg))';
		case 'glass': return 'radial-gradient(ellipse 80% 60% at 50% 0%, color-mix(in srgb, var(--runner-primary) 25%, transparent), transparent), var(--runner-bg)';
		case 'dark': return '#0a0a0a';
		case 'mesh': return 'radial-gradient(at 20% 10%, color-mix(in srgb, var(--runner-primary) 30%, transparent), transparent 50%), radial-gradient(at 80% 30%, color-mix(in srgb, var(--runner-accent) 25%, transparent), transparent 50%), radial-gradient(at 40% 80%, color-mix(in srgb, var(--runner-primary) 15%, transparent), transparent 50%), var(--runner-bg)';
		default: return skin.surface;
	}
}

/**
 * Map theme.density to a Tailwind `space-y-*` class for the main block stack.
 */
export function densityClass(theme: RunnerTheme | undefined): string {
	switch (theme?.density) {
		case 'compact': return 'space-y-4';
		case 'spacious': return 'space-y-16';
		case 'comfortable':
		default: return 'space-y-10';
	}
}

/**
 * Resolve theme.layout into a Tailwind max-width class (unless
 * theme.contentWidth overrides with a raw CSS value).
 */
export function layoutMaxWidth(theme: RunnerTheme | undefined): string {
	if (theme?.contentWidth) return '';
	switch (theme?.layout) {
		case 'narrow': return 'max-w-2xl';
		case 'centered': return 'max-w-3xl';
		case 'wide': return 'max-w-5xl';
		case 'ultrawide': return 'max-w-7xl';
		case 'full': return 'max-w-none';
		default: return 'max-w-3xl';
	}
}

export function layoutContentWidthStyle(theme: RunnerTheme | undefined): string {
	if (theme?.contentWidth) return `max-width: ${theme.contentWidth}; width: 100%`;
	return '';
}

export function pagePaddingClass(theme: RunnerTheme | undefined): string {
	switch (theme?.padding) {
		case 'sm': return 'px-4 py-6';
		case 'md': return 'px-6 py-8';
		case 'lg': return 'px-8 py-12';
		case 'xl': return 'px-10 py-16';
		case '2xl': return 'px-12 py-24';
		default: return 'px-6 py-10';
	}
}
