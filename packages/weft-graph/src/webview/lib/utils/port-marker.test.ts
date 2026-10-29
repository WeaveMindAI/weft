import { describe, expect, it } from 'vitest';
import { filledMarkerStyle, portMarkerStyle, requirednessNote } from './port-marker';
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

describe('the marker follows the three requiredness states', () => {
	const full = portMarkerStyle(port({ required: true }), none, none, '#0af', 'input');
	const empty = portMarkerStyle(port({ optional: true }), none, none, '#0af', 'input');

	it('draws a required input full, wired or not', () => {
		for (const wired of [false, true]) {
			expect(portMarkerStyle(port({ required: true }), none, none, '#0af', 'input', '', wired)).toEqual(full);
		}
	});

	it('draws an input the program wrote `x?:` empty, wired or not', () => {
		for (const wired of [false, true]) {
			expect(portMarkerStyle(port({ optional: true }), none, none, '#0af', 'input', '', wired)).toEqual(empty);
		}
	});

	it('draws an input with neither flag empty while unwired and full once wired', () => {
		expect(full).not.toEqual(empty);
		expect(portMarkerStyle(port(), none, none, '#0af', 'input', '', false)).toEqual(empty);
		expect(portMarkerStyle(port(), none, none, '#0af', 'input', '', true)).toEqual(full);
	});
});

describe('requirednessNote', () => {
	it('says nothing for a required input', () => {
		expect(requirednessNote(port({ required: true }), true)).toBeNull();
	});

	it('names each of the other two states for what a missing value does', () => {
		expect(requirednessNote(port({ optional: true }), true)).toContain('runs without it');
		expect(requirednessNote(port(), false)).toMatch(/^value: optional while unwired; once wired, required/);
		expect(requirednessNote(port(), true)).toMatch(/^value: required while wired/);
	});
});

describe("a container's input that passes through", () => {
	const through = port({ required: true, passesThrough: true });

	it('draws a ring with a dot in the middle, apart from every requiredness state', () => {
		const m = portMarkerStyle(through, none, none, '#0af', 'input');
		expect(m.style).toContain('radial-gradient(circle, #0af 0 2px, white 2.5px)');
		expect(m.style).toContain('border: 2px solid #0af');
		expect(m.class).not.toMatch(/border-\d/);
		for (const other of [port({ required: true }), port({ optional: true })]) {
			expect(m).not.toEqual(portMarkerStyle(other, none, none, '#0af', 'input'));
		}
	});

	it('wins over a @require_one_of group but not over a value written there', () => {
		expect(portMarkerStyle(through, new Set(['value']), none, '#0af', 'input'))
			.toEqual(portMarkerStyle(through, none, none, '#0af', 'input'));
		expect(portMarkerStyle(through, none, new Set(['value']), '#0af', 'input'))
			.toEqual(portMarkerStyle(port({ required: true }), none, new Set(['value']), '#0af', 'input'));
	});

	it('says in its hover text that the nodes inside decide', () => {
		expect(requirednessNote(through, false)).toBe(
			'value: passes its value straight to the nodes inside; it has to be connected only when a node inside needs it',
		);
	});
});
