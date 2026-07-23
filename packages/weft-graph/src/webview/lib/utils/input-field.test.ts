import { describe, it, expect } from 'vitest';
import { fieldForInput, clampToRange } from './input-field';
import { inputExposure } from '../types';
import type { PortDefinition } from '../types';

/** The regression class this file pins: an input's resolved widget is
 *  the ONLY source of its editor field, and every widget payload member
 *  must survive the flattening (the old field pipeline dropped `language`
 *  entirely and let `default_value` die unread). */
describe('fieldForInput', () => {
	const base = (over: Partial<PortDefinition>): PortDefinition => ({
		name: 'x',
		portType: 'String',
		required: false,
		exposure: 'all',
		widget: { kind: 'textarea' },
		...over,
	});

	it('maps each widget kind and carries its payload', () => {
		const code = fieldForInput(base({ widget: { kind: 'code', language: 'javascript' } }));
		expect(code.type).toBe('code');
		expect(code.language).toBe('javascript');

		const num = fieldForInput(base({ portType: 'Number', widget: { kind: 'number', min: 0, max: 2, step: 0.1 } }));
		expect(num.type).toBe('number');
		expect([num.min, num.max, num.step]).toEqual([0, 2, 0.1]);

		const sel = fieldForInput(base({ widget: { kind: 'select', options: ['GET', 'POST'] } }));
		expect(sel.type).toBe('select');
		expect(sel.options).toEqual(['GET', 'POST']);

		const key = fieldForInput(base({ widget: { kind: 'api_key', provider: 'openrouter' } }));
		expect(key.type).toBe('api_key');
		expect(key.provider).toBe('openrouter');

		const drop = fieldForInput(base({ portType: 'Image', widget: { kind: 'file_drop', type: 'Image', accept: 'image/png' } }));
		expect(drop.type).toBe('file_drop');
		expect(drop.fileType).toBe('Image');
		expect(drop.accept).toBe('image/png');
	});

	it('routes the value home by exposure', () => {
		expect(fieldForInput(base({ exposure: 'all' })).portDriven).toBe(true);
		expect(fieldForInput(base({ exposure: 'assignment' })).portDriven).toBe(true);
		expect(fieldForInput(base({ exposure: 'config' })).portDriven).toBe(false);
	});

	it('carries default, label, and placeholder', () => {
		const f = fieldForInput(base({ default: 'GET', label: 'Method', placeholder: 'pick one' }));
		expect(f.defaultValue).toBe('GET');
		expect(f.label).toBe('Method');
		expect(f.placeholder).toBe('pick one');
	});

	it('falls back to a textarea for a not-yet-round-tripped port', () => {
		const f = fieldForInput(base({ widget: undefined }));
		expect(f.type).toBe('textarea');
	});
});

describe('inputExposure', () => {
	it('trusts the resolved exposure and only falls back for local placeholders', () => {
		expect(inputExposure({ name: 'a', portType: 'String', required: false, exposure: 'config' })).toBe('config');
		// A locally-added port pre-round-trip: MustOverride implies assignment.
		expect(inputExposure({ name: 'a', portType: 'MustOverride', required: false })).toBe('assignment');
		expect(inputExposure({ name: 'a', portType: 'String', required: false })).toBe('all');
	});
});

describe('clampToRange', () => {
	it('clamps to the declared bounds and passes in-range values through', () => {
		expect(clampToRange(5, 0, 2)).toBe(2);
		expect(clampToRange(-1, 0, 2)).toBe(0);
		expect(clampToRange(1.5, 0, 2)).toBe(1.5);
		expect(clampToRange(99, undefined, undefined)).toBe(99);
		expect(clampToRange(-5, 0, undefined)).toBe(0);
	});
});
