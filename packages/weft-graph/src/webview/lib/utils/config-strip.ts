// What a container's open config strip holds and how tall it draws, worked
// out from its content and its width, never read off the screen. The strip
// renderer (GroupNode) and the layout engine both size from it, so the room
// the layout leaves is the strip drawn, from the first render, and nothing
// is measured during a layout pass. Pure: the text measure is handed in.

import type { FieldDefinition, MetadataInput, PortDefinition } from '../types';
import type { MetadataExpr, MetadataPort } from '../../../protocol';
import { CONFIG_STRIP_BAR_PX } from '../constants/container-layout';
import { LOOP_CONFIG_FIELDS, fieldForInput, gateField } from './input-field';
import { isGatePort } from '../../../protocol';
import { metadataExprDisplayText } from './metadata-expr';

/// How wide a run of text draws in a font (`measureTextWidth`).
export type MeasureText = (text: string, font: string) => number;

/// The fonts the strip's wrapping text draws in: a field's label, a
/// checkbox's text, a gathered metadata value's expression, and a
/// multiselect's option buttons.
export interface StripFonts {
	label: string;
	checkbox: string;
	expression: string;
	option: string;
}

/// The heights a field draws at, in px: the ONE place they are written.
/// `FieldStrip.svelte` draws its rows and controls at these (inline
/// styles) and the strip's height is added up from them, so the room the
/// layout leaves is the strip drawn.
export const FIELD_PX = {
	/// A field's label row, tall enough for the clear button beside it.
	labelRow: 18,
	/// The clear button a set checkbox or select draws beside its label
	/// (`drawsClearButton`), and the gap before it.
	clearButton: 16,
	clearGap: 4,
	/// One more line of a label that wraps (10px text).
	labelLine: 15,
	/// Between a field's label row and its control (`space-y-1`).
	gap: 4,
	/// A one-line control: text, number, select, date, password.
	input: 28,
	/// A checkbox's row, and each line of its description (12px text).
	checkboxLine: 16,
	/// A textarea in a container's strip, which does not resize there.
	textarea: 60,
	/// One value of a text list (each followed by `gap`).
	listRow: 24,
	/// One row of a multiselect's option buttons, the gap between rows,
	/// and the box's padding on each side.
	optionRow: 19,
	optionGap: 4,
	optionPad: 6,
	/// Between two fields of a container's strip (`.config-fields`).
	between: 5,
	/// Above the first field of the open strip (`.config-fields`).
	top: 4,
} as const;

/// Where a field's text starts and ends: the strip's padding and the
/// container's border on each side.
const STRIP_SIDE_PX = 10 + 2;
/// The checkbox and the gap before its text.
const CHECKBOX_PX = 16 + 8;
/// An option button's padding, both sides together (`px-1.5`).
const OPTION_PAD_X_PX = 12;

/// The fixed-height box a metadata value draws in (the value of a
/// metadata port, or of an input a metadata wire fills): a JSON box that
/// scrolls, so its height is known before it renders.
export const METADATA_VALUE_BOX_PX = 96;
/// The most a gathered value's expression box takes: as tall as its
/// wrapped text up to this, then it scrolls.
export const METADATA_EXPR_BOX_MAX_PX = 56;
/// The expression box's padding (`px-2 py-1.5`): both sides together, and
/// top and bottom together.
const EXPR_PAD_X_PX = 16;
const EXPR_PAD_Y_PX = 12;

/// The fields of a container's strip a person edits through ordinary
/// controls, in order: a loop's settings, then one per port the source
/// gives a value (the gate included). The strip draws them and the layout
/// engine counts them from this one list.
export function containerStripFields(
	id: string,
	isLoop: boolean,
	portLiterals: Record<string, unknown>,
	inputs: readonly PortDefinition[],
): FieldDefinition[] {
	const portFields = Object.keys(portLiterals).map((key) => {
		if (isGatePort(key)) return gateField(key, 'container');
		const port = inputs.find((p) => p.name === key);
		// A container's ports are its header: the compiler refuses a
		// value on any other name, and the projection throws on one, so a
		// key with no port here is a broken invariant, not a field to
		// improvise.
		if (!port) throw new Error(`'${id}' has a value for '${key}', which is not one of its ports`);
		return fieldForInput(port);
	});
	return [...(isLoop ? LOOP_CONFIG_FIELDS : []), ...portFields];
}

/// The lines `text` wraps to in `width`, breaking at spaces, and inside a
/// word only when the word alone is wider (the strip's `overflow-wrap`).
export function wrappedLines(text: string, width: number, measure: (s: string) => number): number {
	if (width <= 0 || text === '') return 1;
	const space = measure(' ');
	let lines = 1;
	let used = 0;
	for (const word of text.split(/\s+/).filter(Boolean)) {
		const w = measure(word);
		if (used > 0 && used + space + w <= width) {
			used += space + w;
			continue;
		}
		if (used > 0) lines += 1;
		const spill = Math.max(0, Math.ceil(w / width) - 1);
		lines += spill;
		used = w - spill * width;
	}
	return lines;
}

/// The width a field takes in a container `widthPx` wide.
export function stripInnerWidth(widthPx: number): number {
	return widthPx - 2 * STRIP_SIDE_PX;
}

/// The height a gathered value's expression box draws at in a container
/// `widthPx` wide: its wrapped text, padded, up to
/// `METADATA_EXPR_BOX_MAX_PX`.
export function expressionBoxPx(expr: MetadataExpr, scope: string, widthPx: number, measure: MeasureText, fonts: StripFonts): number {
	const text = metadataExprDisplayText(expr, scope);
	const lines = wrappedLines(text, stripInnerWidth(widthPx) - EXPR_PAD_X_PX, (s) => measure(s, fonts.expression));
	return Math.min(METADATA_EXPR_BOX_MAX_PX, lines * FIELD_PX.checkboxLine + EXPR_PAD_Y_PX);
}

/// A field of the strip as its height needs it: what it is, what its
/// text says, and (for a list) the value it holds.
export type StripField = Pick<FieldDefinition, 'key' | 'portDriven' | 'label' | 'type' | 'description' | 'options' | 'freeText'>;

/// The rows `options` take as buttons wrapped in `width`.
function optionRows(options: readonly string[], width: number, measure: (s: string) => number): number {
	let rows = 1;
	let used = 0;
	for (const option of options) {
		const w = measure(option) + OPTION_PAD_X_PX;
		if (used > 0 && used + FIELD_PX.optionGap + w > width) {
			rows += 1;
			used = w;
		} else {
			used += (used > 0 ? FIELD_PX.optionGap : 0) + w;
		}
	}
	return rows;
}

/// Whether a field draws the clear button beside its label: a port's
/// checkbox or select holding a value, which the button removes from the
/// source. The one rule `FieldStrip` draws by and the height wraps by.
export function drawsClearButton(field: Pick<StripField, 'type' | 'portDriven'>, value: unknown): boolean {
	return !!field.portDriven && (field.type === 'checkbox' || field.type === 'select') && value !== undefined && value !== null;
}

/// One field's height in a strip whose fields are `inner` wide, holding
/// `value`: its label row (its label wrapped beside the clear button when
/// it draws one), the gap, and its control as `FieldStrip` draws it.
export function stripFieldPx(field: StripField, value: unknown, inner: number, measure: MeasureText, fonts: StripFonts): number {
	const labelWidth = drawsClearButton(field, value) ? inner - FIELD_PX.clearButton - FIELD_PX.clearGap : inner;
	const labelLines = wrappedLines(field.label, labelWidth, (s) => measure(s, fonts.label));
	const labelRow = Math.max(FIELD_PX.labelRow, labelLines * FIELD_PX.labelLine);
	return labelRow + FIELD_PX.gap + controlPx(field, value, inner, measure, fonts);
}

function controlPx(field: StripField, value: unknown, inner: number, measure: MeasureText, fonts: StripFonts): number {
	switch (field.type) {
		case 'checkbox':
			return field.description
				? wrappedLines(field.description, inner - CHECKBOX_PX, (s) => measure(s, fonts.checkbox)) * FIELD_PX.checkboxLine
				: FIELD_PX.checkboxLine;
		case 'textarea':
			return FIELD_PX.textarea;
		case 'text_list': {
			const items = Array.isArray(value) ? value.length : 0;
			return items * (FIELD_PX.listRow + FIELD_PX.gap) + FIELD_PX.input;
		}
		case 'multiselect': {
			const picked = Array.isArray(value) ? value.filter((v): v is string => typeof v === 'string') : [];
			const options = field.options ?? [];
			const shown = field.freeText ? [...options, ...picked.filter((v) => !options.includes(v))] : options;
			const rows = optionRows(shown, inner - 2 * FIELD_PX.optionPad, (s) => measure(s, fonts.option));
			const buttons = 2 * FIELD_PX.optionPad + rows * FIELD_PX.optionRow + (rows - 1) * FIELD_PX.optionGap;
			return buttons + (field.freeText ? FIELD_PX.gap + FIELD_PX.input : 0);
		}
		default:
			return FIELD_PX.input;
	}
}

/// What the strip holds: its ordinary fields (`containerStripFields`)
/// with the value each holds, its metadata ports, and the inputs a
/// metadata wire fills from outside.
export interface StripContent {
	/// The container's id: a gathered value's names read relative to it.
	scope: string;
	fields: readonly StripField[];
	/// The value a field holds (`declaredHomeValue`): a list's rows.
	valueOf: (field: StripField) => unknown;
	metadataPorts?: readonly Pick<MetadataPort, 'written'>[];
	metadataInputs?: Record<string, MetadataInput>;
}

/// The open strip's height in a container `widthPx` wide: its toggle row,
/// then each field as `FieldStrip` draws it (its text wrapped to the
/// strip's width) and each metadata field's boxes, with the gaps between.
export function configStripOpenPx(content: StripContent, widthPx: number, measure: MeasureText, fonts: StripFonts): number {
	const inner = stripInnerWidth(widthPx);
	const heights = content.fields.map((field) => stripFieldPx(field, content.valueOf(field), inner, measure, fonts));
	const valueField = FIELD_PX.labelRow + FIELD_PX.gap + METADATA_VALUE_BOX_PX;
	for (const port of content.metadataPorts ?? []) {
		heights.push(valueField + (port.written ? FIELD_PX.gap + expressionBoxPx(port.written, content.scope, widthPx, measure, fonts) : 0));
	}
	for (const _ of Object.keys(content.metadataInputs ?? {})) heights.push(valueField);
	if (heights.length === 0) return CONFIG_STRIP_BAR_PX;
	return CONFIG_STRIP_BAR_PX + FIELD_PX.top + heights.reduce((a, b) => a + b, 0) + (heights.length - 1) * FIELD_PX.between;
}
