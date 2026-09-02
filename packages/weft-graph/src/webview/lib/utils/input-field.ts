/// The ONE input -> editor-field mapping. Every input's control comes
/// from its RESOLVED widget (stamped by the compiler/CLI); this module
/// only flattens that widget into the render `FieldDefinition` shape the
/// field components consume. The editor never derives a widget from a
/// type: that rule lives in Rust (`Widget::default_for_type`).
import type { FieldDefinition, PortDefinition } from '../types';
import type { Widget } from '../../../protocol';
import type { SpecField } from './port-specs';
import { declaredHomeValue, inputExposure } from '../types';
import { SHOULD_FLOW_PORT } from '../../../protocol';

/// A node's rendered input list, read off its data. Every parse and
/// every projection path fills the list (the wire type is a required
/// array), so a missing one is a broken node payload and is refused
/// loudly rather than papered over with the catalog defaults. Every
/// reader of "which inputs does this node show" goes through it, so the
/// unconnected-access pin, the fields, and the port handles can never
/// disagree on the list.
export function inputsOf(inputs: unknown): PortDefinition[] {
	return portList(inputs, 'input');
}

/// The output twin of [`inputsOf`]: same contract, same loud refusal.
export function outputsOf(outputs: unknown): PortDefinition[] {
	return portList(outputs, 'output');
}

function portList(list: unknown, which: 'input' | 'output'): PortDefinition[] {
	if (!Array.isArray(list)) {
		throw new Error(`node data carries no ${which} list`);
	}
	return list as PortDefinition[];
}

/// The render field for one input. `portDriven` follows the exposure: a
/// wireable input's literal lives in `portLiterals` (the port home), a
/// `config`-exposure input's value lives in `config` (the config home).
/// A locally-added port that has not round-tripped yet has no widget;
/// it renders as a textarea until the parse stamps the real one.
export function fieldForInput(input: PortDefinition): FieldDefinition {
	const field = fieldFromWidget(
		input.name,
		input.label ?? input.name,
		input.widget ?? { kind: 'textarea' },
	);
	field.portDriven = inputExposure(input) !== 'config';
	if (input.placeholder !== undefined) field.placeholder = input.placeholder;
	if (input.default !== undefined) field.defaultValue = input.default;
	if (input.description !== undefined) field.description = input.description;
	return field;
}

/// The field for `_should_flow`, the port that decides whether a node
/// runs. Every node has it, and it is answered by a wire, so it gets no
/// field of its own: the corner dock on the node is where it lives. The
/// one exception is a value written straight into the source, which has
/// to be visible to be changed or removed, and this is that field. One
/// definition, so a node and a container show the same control.
export function shouldFlowField(subject: 'node' | 'container'): FieldDefinition {
	return {
		key: SHOULD_FLOW_PORT,
		label: SHOULD_FLOW_PORT,
		type: 'checkbox',
		portDriven: true,
		description: `Whether this ${subject} runs. Off skips it, and everything ${subject === 'container' ? 'inside it ' : ''}downstream closes in turn.`,
	};
}

/// Does this input get a field in the body? A `wire`-exposure input
/// never does, a wired one never does (the edge is the value), and
/// `_should_flow` only does when the source wrote a value for it.
export function inputRendersField(
	input: PortDefinition,
	opts: { wired: boolean; hasWrittenValue: boolean },
): boolean {
	if (input.synthesizedFromCarry) return false; // carry ghost: not editable
	if (inputExposure(input) === 'wire') return false;
	if (opts.wired) return false;
	if (input.name === SHOULD_FLOW_PORT) return opts.hasWrittenValue;
	return true;
}

/// Does this node render an access field with no connection picked?
/// Such a node is pinned open: the Connect button lives in the expanded
/// body, so a collapsed unconnected node would hide the only way to fix
/// it. ONE definition, read by both the node renderer (chevron/toggle)
/// and the projection's build step (which overlays `expanded` from it),
/// so the drawn state and the computed sizing can never disagree.
/// The handle is read from node CONFIG only: an access widget requires
/// `exposure: config` (the metadata validator refuses anything else),
/// so a port-literal home for it cannot exist.
export function hasUnpickedAccess(
	inputs: PortDefinition[],
	config: unknown,
	wiredInputPorts: ReadonlySet<string>,
): boolean {
	for (const input of inputs) {
		if (input.widget?.kind !== 'access') continue;
		// An optional connection (compiler-stamped from the recipe's
		// `connection_optional`) never pins: the node runs without one.
		if (input.widget.optional) continue;
		const rendered = inputRendersField(input, {
			wired: wiredInputPorts.has(input.name),
			hasWrittenValue: false,
		});
		if (!rendered) continue;
		if (declaredHomeValue(undefined, config, fieldForInput(input)) == null) return true;
	}
	return false;
}

/// The render field for one value a config entry kind asks for (a
/// select's options, a case's value). The SAME widget flattening an
/// input goes through, so a node inventing a kind gets a real control
/// without anything here learning the key.
export function fieldForSpecField(spec: SpecField): FieldDefinition {
	const field = fieldFromWidget(spec.key, spec.label || spec.key, spec.widget);
	// An entry's values live in the entry object, never in a port
	// literal: the entry list itself is the one value being edited.
	field.portDriven = false;
	return field;
}

/// Flatten one resolved widget into the render shape the field
/// components consume. The per-variant payloads (a select's options, a
/// number's range) come from the widget itself, so the union narrows on
/// `kind` and a variant's required payload cannot be silently absent.
function fieldFromWidget(key: string, label: string, w: Widget): FieldDefinition {
	const field: FieldDefinition = { key, label, type: w.kind };
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
			if (w.multiple) field.multiple = true;
			break;
		case 'text':
		case 'textarea':
		case 'checkbox':
		case 'password':
		case 'text_list':
		case 'entry_list':
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

/// The next portLiterals map after one field write, pure. THE rule for
/// what "cleared" means (null, undefined, the empty string a text
/// control leaves behind, or the empty array a multiselect/text_list
/// leaves behind): a cleared key is DELETED, so the source goes back to
/// saying nothing about the port, and no control can ever store a
/// phantom "" or [] literal. The same emptiness rule commitEntry
/// applies to entry values. Both hosts of a port strip (ProjectNode,
/// GroupNode) route their writes through this so they cannot disagree.
export function nextPortLiterals(
	literals: Record<string, unknown>,
	key: string,
	value: unknown,
): Record<string, unknown> {
	const next = { ...literals };
	const cleared =
		value === null ||
		value === undefined ||
		value === '' ||
		(Array.isArray(value) && value.length === 0);
	if (cleared) delete next[key];
	else next[key] = value;
	return next;
}

/// Clamp a number to a field's declared min/max. The widget's range is
/// a contract (the compiler rejects out-of-range literals), so every
/// editor write path routes through this before saving.
export function clampToRange(n: number, min?: number, max?: number): number {
	if (min !== undefined && n < min) return min;
	if (max !== undefined && n > max) return max;
	return n;
}
