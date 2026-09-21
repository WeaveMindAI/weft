import { describe, expect, it } from 'vitest';
import { fullMarkerStyle, innerMarkerStyle, portMarkerStyle } from './port-marker';
import type { PortDefinition } from '../types';

const port = (over: Partial<PortDefinition> = {}): PortDefinition =>
	({ name: 'value', portType: 'String', required: false, ...over }) as PortDefinition;

const none = new Set<string>();

describe('port markers', () => {
	it('draws a dotted ring on an input already filled from code', () => {
		const m = portMarkerStyle(port({ required: true }), none, new Set(['value']), '#0af', 'input');
		expect(m.style).toContain('border: 2px dotted #0af');
		expect(m.style).toContain('background-color: white');
	});

	it('keeps the ring OUT of the class list', () => {
		// The width used to come from Tailwind's `!border-2`, which in
		// Tailwind 4 also emits an important `border-style`. That beats
		// the inline style, so the dotted marker above rendered solid.
		// Every marker's ring has to stay inline, width included.
		const markers = [
			portMarkerStyle(port(), none, none, '#0af', 'input'),
			portMarkerStyle(port(), none, new Set(['value']), '#0af', 'input'),
			portMarkerStyle(port(), new Set(['value']), none, '#0af', 'input'),
			portMarkerStyle(port(), none, none, '#0af', 'output'),
			fullMarkerStyle('#0af'),
			innerMarkerStyle('#0af'),
		];
		for (const m of markers) {
			expect(m.class).not.toMatch(/border-\d/);
			expect(m.style).toMatch(/border: 2px (solid|dotted) #0af/);
		}
	});

	it('gives the inside of a container port its own smaller size', () => {
		expect(fullMarkerStyle('#0af').class).toContain('!w-3');
		expect(innerMarkerStyle('#0af').class).toContain('!w-2.5');
	});

	it('rings an output in its own colour, so it draws as wide as an input', () => {
		const out = portMarkerStyle(port(), none, none, '#0af', 'output');
		expect(out.style).toBe(fullMarkerStyle('#0af').style);
	});
});
