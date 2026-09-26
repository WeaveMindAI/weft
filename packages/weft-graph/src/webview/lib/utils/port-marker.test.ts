import { describe, expect, it } from 'vitest';
import { filledMarkerStyle, portMarkerStyle } from './port-marker';
import type { PortDefinition } from '../types';

const port = (over: Partial<PortDefinition> = {}): PortDefinition =>
	({ name: 'value', portType: 'String', required: false, ...over }) as PortDefinition;

const none = new Set<string>();

describe('port markers', () => {
	it('draws an evenly dashed ring on an input already filled from code', () => {
		const m = portMarkerStyle(port({ required: true }), none, new Set(['value']), '#0af', 'input');
		// Painted by a conic gradient, never a CSS `dotted` border, which
		// spaces its dots unevenly round a 12px circle.
		expect(m.style).toContain('border: 2px solid transparent');
		expect(m.style).toContain('repeating-conic-gradient(#0af 0deg 22.5deg, transparent 22.5deg 45deg) border-box');
		expect(m.style).toContain('linear-gradient(white, white) padding-box');
		expect(m.style).not.toContain('dotted');
	});

	it('keeps the ring OUT of the class list', () => {
		// The width used to come from Tailwind's `!border-2`, which in
		// Tailwind 4 also emits an important `border-style`. That beats
		// the inline style, so the filled-from-code marker above rendered solid.
		// Every marker's ring has to stay inline, width included.
		const markers = [
			portMarkerStyle(port(), none, none, '#0af', 'input'),
			portMarkerStyle(port(), none, new Set(['value']), '#0af', 'input'),
			portMarkerStyle(port(), new Set(['value']), none, '#0af', 'input'),
			portMarkerStyle(port(), none, none, '#0af', 'output'),
			filledMarkerStyle('#0af', 'port'),
			filledMarkerStyle('#0af', 'inner'),
		];
		for (const m of markers) {
			expect(m.class).not.toMatch(/border-\d/);
			expect(m.style).toMatch(/border: 2px solid (#0af|transparent)/);
		}
	});

	it('gives the inside of a container port its own smaller size', () => {
		expect(filledMarkerStyle('#0af', 'port').class).toContain('!w-3');
		expect(filledMarkerStyle('#0af', 'inner').class).toContain('!w-2.5');
	});

	it('rings an output in its own colour, so it draws as wide as an input', () => {
		const out = portMarkerStyle(port(), none, none, '#0af', 'output');
		expect(out.style).toBe(filledMarkerStyle('#0af', 'port').style);
	});
});
