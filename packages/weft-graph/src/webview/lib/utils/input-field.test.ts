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

	it('flattens the access widget with its compiler-stamped service', () => {
		const f = fieldForInput(
			base({ exposure: 'config', widget: { kind: 'access', service: 'slack' } }),
		);
		expect(f.type).toBe('access');
		expect(f.service).toBe('slack');
		expect(f.portDriven).toBe(false);
	});

	it('flattens the remote_select widget with its sources and parents', () => {
		const f = fieldForInput(
			base({
				widget: {
					kind: 'remote_select',
					access: 'account',
					sources: [
						{ kind: 'granted', from: 'repositories' },
						{ kind: 'list', get: 'https://x/list?q={query}', items: 'channels', label: 'name', value: 'id',
							page: { cursor_param: 'cursor', cursor_path: 'meta.next' } },
						{ kind: 'from_url', pattern: 'x\\.com/([^/]+)' },
					],
					depends_on: ['repo'],
				},
			}),
		);
		expect(f.type).toBe('remote_select');
		expect(f.access).toBe('account');
		expect(f.sources?.length).toBe(3);
		expect(f.sources?.[0]).toEqual({ kind: 'granted', from: 'repositories' });
		expect(f.dependsOn).toEqual(['repo']);
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
