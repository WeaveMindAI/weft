import { describe, expect, it } from 'vitest';
import { FIELD_PX, METADATA_EXPR_BOX_MAX_PX, drawsClearButton, METADATA_VALUE_BOX_PX, configStripOpenPx, containerStripFields, expressionBoxPx, stripFieldPx, wrappedLines, type MeasureText, type StripContent, type StripField } from './config-strip';
import { CONFIG_STRIP_BAR_PX } from '../constants/container-layout';
import { LOOP_CONFIG_FIELDS } from './input-field';
import type { MetadataExpr } from '../../../protocol';

/** Every character 6px wide, so wrapping is worked out by hand. */
const measure: MeasureText = (text) => text.length * 6;
const fonts = { label: 'l', checkbox: 'c', expression: 'e', option: 'o' };
const width = () => (s: string) => s.length * 6;

describe('wrappedLines', () => {
	it('keeps a line that fits on one line', () => {
		expect(wrappedLines('one two', 100, width())).toBe(1);
	});

	it('breaks at spaces', () => {
		// "aaaa bbbb cccc" at 6px a character: 24px a word, 6px a space.
		expect(wrappedLines('aaaa bbbb cccc', 60, width())).toBe(2);
		expect(wrappedLines('aaaa bbbb cccc', 30, width())).toBe(3);
	});

	it('breaks inside a word only when the word alone is wider', () => {
		expect(wrappedLines('a'.repeat(25), 60, width())).toBe(3);
	});
});

describe('stripFieldPx', () => {
	const px = (field: StripField, value?: unknown, inner = 1000) => stripFieldPx(field, value, inner, measure, fonts);
	const head = FIELD_PX.labelRow + FIELD_PX.gap;

	it('draws each kind of control at its own height', () => {
		expect(px({ key: 'a', label: 'a', type: 'text' })).toBe(head + FIELD_PX.input);
		expect(px({ key: 'a', label: 'a', type: 'number' })).toBe(head + FIELD_PX.input);
		expect(px({ key: 'a', label: 'a', type: 'select', options: ['x'] })).toBe(head + FIELD_PX.input);
		expect(px({ key: 'a', label: 'a', type: 'checkbox' })).toBe(head + FIELD_PX.checkboxLine);
		expect(px({ key: 'a', label: 'a', type: 'textarea' })).toBe(head + FIELD_PX.textarea);
	});

	it('grows a text list with its values, and a multiselect with its rows of options', () => {
		const list = { key: 'a', label: 'a', type: 'text_list' };
		expect(px(list, ['x', 'y']) - px(list, [])).toBe(2 * (FIELD_PX.listRow + FIELD_PX.gap));
		const many = { key: 'a', label: 'a', type: 'multiselect', options: Array.from({ length: 30 }, (_, i) => `option${i}`) };
		expect(px(many, [], 200)).toBeGreaterThan(px(many, [], 2000));
	});

	it('adds a line for each extra line a label or a checkbox description wraps to', () => {
		const field = { key: 'p', label: 'Parallel', type: 'checkbox', description: 'x '.repeat(60).trim() };
		expect(px(field, undefined, 250)).toBeGreaterThan(px(field, undefined, 1000));
		const label = { key: 'l', label: 'word '.repeat(40).trim(), type: 'text' };
		expect(px(label, undefined, 200) - px(label, undefined, 2000)).toBeGreaterThan(0);
	});
});

describe('configStripOpenPx', () => {
	const content = (over: Partial<StripContent>): StripContent => ({ scope: 'g', fields: [], valueOf: () => undefined, ...over });

	it('is the bar alone when the strip holds nothing', () => {
		expect(configStripOpenPx(content({}), 400, measure, fonts)).toBe(CONFIG_STRIP_BAR_PX);
	});

	it('adds up the fields with the gaps between them', () => {
		const a = { key: 'a', label: 'a', type: 'text' };
		const b = { key: 'b', label: 'b', type: 'checkbox' };
		const one = stripFieldPx(a, undefined, 376, measure, fonts);
		const two = stripFieldPx(b, undefined, 376, measure, fonts);
		expect(configStripOpenPx(content({ fields: [a, b] }), 400, measure, fonts))
			.toBe(CONFIG_STRIP_BAR_PX + FIELD_PX.top + one + two + FIELD_PX.between);
	});

	it('counts each metadata value at its box, and a gathered one with its expression box too', () => {
		const written: MetadataExpr = { kind: 'port', source: 'g.act', port: 'tools' };
		const full = configStripOpenPx(content({ metadataPorts: [{}] }), 400, measure, fonts);
		const gathered = configStripOpenPx(content({ metadataPorts: [{ written }] }), 400, measure, fonts);
		expect(full - CONFIG_STRIP_BAR_PX).toBeGreaterThan(METADATA_VALUE_BOX_PX);
		expect(gathered - full).toBe(FIELD_PX.gap + expressionBoxPx(written, 'g', 400, measure, fonts));
		const filled = configStripOpenPx(content({ metadataInputs: { spec: { from: { kind: 'port', source: 's', port: 'tool' }, value: {} } } }), 400, measure, fonts);
		expect(filled).toBe(full);
	});
});

describe('expressionBoxPx', () => {
	it('is as tall as one line of text when the expression is short', () => {
		// `act.tools`: one line, plus the box's padding.
		expect(expressionBoxPx({ kind: 'port', source: 'g.act', port: 'tools' }, 'g', 400, measure, fonts)).toBe(16 + 12);
	});

	it('grows with the lines it wraps to, up to its cap', () => {
		const items = Array.from({ length: 40 }, (_, i) => ({ spread: true, value: { kind: 'port' as const, source: `g.n${i}`, port: 'tools', path: ['anthropic'] } }));
		const long: MetadataExpr = { kind: 'object', fields: [{ key: 'anthropic', value: { kind: 'list', items } }] };
		expect(expressionBoxPx(long, 'g', 400, measure, fonts)).toBe(METADATA_EXPR_BOX_MAX_PX);
		const two: MetadataExpr = { kind: 'list', items: items.slice(0, 4) };
		const px = expressionBoxPx(two, 'g', 400, measure, fonts);
		expect(px).toBeGreaterThan(16 + 12);
		expect(px).toBeLessThanOrEqual(METADATA_EXPR_BOX_MAX_PX);
	});
});

describe('containerStripFields', () => {
	it("lists a loop's settings, then a field per port the source gives a value", () => {
		const fields = containerStripFields('l', true, { failed: false, _should_flow: true }, [
			{ name: 'failed', portType: 'Boolean', required: true, widget: { kind: 'checkbox' } },
		]);
		expect(fields.map((f) => f.key)).toEqual([...LOOP_CONFIG_FIELDS.map((f) => f.key), 'failed', '_should_flow']);
		expect(fields.find((f) => f.key === 'failed')?.type).toBe('checkbox');
	});

	it('refuses a value on a name that is no port of the container', () => {
		expect(() => containerStripFields('g', false, { nope: 1 }, [])).toThrow("'g' has a value for 'nope'");
	});
});

describe('the clear button beside a label', () => {
	const select = { key: 's', label: 'word '.repeat(12).trim(), type: 'select', portDriven: true, options: ['a'] };

	it('draws for a port checkbox or select holding a value, and only then', () => {
		expect(drawsClearButton(select, 'a')).toBe(true);
		expect(drawsClearButton(select, undefined)).toBe(false);
		expect(drawsClearButton({ ...select, portDriven: false }, 'a')).toBe(false);
		expect(drawsClearButton({ ...select, type: 'text' }, 'a')).toBe(false);
	});

	it('takes its width out of the line the label wraps in', () => {
		// The label is 354 wide at 6px a character: one line in 360, two
		// once the button and its gap take 20 of it.
		expect(stripFieldPx(select, undefined, 360, measure, fonts)).toBe(FIELD_PX.labelRow + FIELD_PX.gap + FIELD_PX.input);
		expect(stripFieldPx(select, 'a', 360, measure, fonts)).toBe(2 * FIELD_PX.labelLine + FIELD_PX.gap + FIELD_PX.input);
	});
});
