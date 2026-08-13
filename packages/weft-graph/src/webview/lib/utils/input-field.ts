/// The ONE input -> editor-field mapping. Every input's control comes
/// from its RESOLVED widget (stamped by the compiler/CLI); this module
/// only flattens that widget into the render `FieldDefinition` shape the
/// field components consume. The editor never derives a widget from a
/// type: that rule lives in Rust (`Widget::default_for_type`).
import type { FieldDefinition, PortDefinition } from '../types';
import { inputExposure } from '../types';

/// The render field for one input. `portDriven` follows the exposure: a
/// wireable input's literal lives in `portLiterals` (the port home), a
/// `config`-exposure input's value lives in `config` (the config home).
/// A locally-added port that has not round-tripped yet has no widget;
/// it renders as a textarea until the parse stamps the real one.
export function fieldForInput(input: PortDefinition): FieldDefinition {
	const w = input.widget ?? { kind: 'textarea' };
	const field: FieldDefinition = {
		key: input.name,
		label: input.label ?? input.name,
		type: w.kind,
		portDriven: inputExposure(input) !== 'config',
	};
	if (input.placeholder !== undefined) field.placeholder = input.placeholder;
	if (input.default !== undefined) field.defaultValue = input.default;
	if (input.description !== undefined) field.description = input.description;
	// Per-variant payloads: the Widget union narrows on `kind`, so a
	// variant's required payload (a select's options, a remote_select's
	// sources) cannot be silently absent.
	switch (w.kind) {
		case 'select':
		case 'multiselect':
			field.options = w.options;
			break;
		case 'number':
			if (w.min != null) field.min = w.min;
			if (w.max != null) field.max = w.max;
			if (w.step != null) field.step = w.step;
			break;
		case 'code':
			field.language = w.language;
			break;
		case 'access':
			if (w.service !== undefined) field.service = w.service;
			break;
		case 'remote_select':
			field.access = w.access;
			field.sources = w.sources;
			if (w.depends_on) field.dependsOn = w.depends_on;
			if (w.free_text) field.freeText = true;
			break;
		case 'file_drop':
			if (w.accept) field.accept = w.accept;
			field.fileType = w.type;
			break;
		case 'text':
		case 'textarea':
		case 'checkbox':
		case 'password':
		case 'form_builder':
			break;
	}
	return field;
}

/// The loop container's config knobs. Editor-only UI (a loop is not a
/// catalog node and has no metadata inputs), so the field list lives in
/// the shared field toolbox for every surface that renders loop config.
/// `over`/`carry` are edited through the port context menus, not fields.
export const LOOP_CONFIG_FIELDS: FieldDefinition[] = [
	{
		key: 'parallel',
		label: 'Parallel',
		type: 'checkbox',
		description: 'Run all iterations concurrently (incompatible with carry / self.done).',
	},
	{
		key: 'max_iters',
		label: 'Max iterations',
		type: 'number',
		min: 0,
		description: 'Hard cap on iteration count. Leave blank for no cap.',
	},
	{
		key: 'trim_on_mismatch',
		label: 'Trim on length mismatch',
		type: 'checkbox',
		description: 'Zip iter inputs to the shortest length. Off = crash loud on mismatch.',
	},
];

/// Clamp a number to a field's declared min/max. The widget's range is
/// a contract (the compiler rejects out-of-range literals), so every
/// editor write path routes through this before saving.
export function clampToRange(n: number, min?: number, max?: number): number {
	if (min !== undefined && n < min) return min;
	if (max !== undefined && n > max) return max;
	return n;
}
