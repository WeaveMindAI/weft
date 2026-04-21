import type { PortDefinition, PortType } from '$lib/types';
import { parseWeftType, weftTypeToString, type WeftType } from '$lib/types';

/** Generic render descriptor, tells the task review page HOW to render a field
 *  without knowing anything about the field type itself. */
export interface FormFieldRender {
	/** UI primitive: 'readonly' | 'buttons' | 'select' | 'text' | 'textarea' */
	component: string;
	/** Where options come from: 'static' (config.options) or 'input' (field.value) */
	source?: 'static' | 'input';
	/** For select: allow multiple selections */
	multiple?: boolean;
	/** For textarea: pre-fill from input port value */
	prefilled?: boolean;
}

export interface FormFieldSpec {
	fieldType: string;
	label: string;
	render: FormFieldRender;
	requiredConfig: string[];
	optionalConfig: string[];
	addsInputs: PortDefinition[];
	addsOutputs: PortDefinition[];
}

export interface FormFieldDef {
	fieldType: string;
	key: string;
	render?: FormFieldRender;
	config?: Record<string, unknown>;
	required?: boolean;
}

export function port(name: string, portType: PortType): PortDefinition {
	return { name, portType, required: false };
}

function resolvePortName(template: string, key: string): string {
	return template.replace('{key}', key);
}

/** Sentinel TypeVar name used by form-field specs to request an auto-scoped
 *  per-port TypeVar. Mirrors AUTO_TYPE_VAR_MARKER in the Rust enricher.
 *  Catalog authors write `port('{key}', 'T_Auto')` for ports that should
 *  accept any type independently from sibling ports. Explicit TypeVars like
 *  `T`, `T1` are left alone, so authors can still express shared-type
 *  constraints when that is the right semantics. */
const AUTO_TYPE_VAR_MARKER = 'T_Auto';

/** Recursively replace every `T_Auto` marker in a parsed WeftType with a
 *  TypeVar scoped to the field key. */
function materializeAutoTypeVars(t: WeftType, key: string): WeftType {
	switch (t.kind) {
		case 'typevar':
			if (t.name === AUTO_TYPE_VAR_MARKER) {
				return { kind: 'typevar', name: `T__${key}` };
			}
			return t;
		case 'list':
			return { kind: 'list', inner: materializeAutoTypeVars(t.inner, key) };
		case 'dict':
			return {
				kind: 'dict',
				key: materializeAutoTypeVars(t.key, key),
				value: materializeAutoTypeVars(t.value, key),
			};
		case 'union':
			return { kind: 'union', types: t.types.map(x => materializeAutoTypeVars(x, key)) };
		default:
			return t;
	}
}

/** Replace T_Auto markers in a port type string with key-scoped TypeVar names.
 *  Returns the original string if parsing fails or no markers are present. */
function resolveAutoTypeVars(portType: PortType, key: string): PortType {
	const parsed = parseWeftType(portType);
	if (!parsed) return portType;
	const materialized = materializeAutoTypeVars(parsed, key);
	return weftTypeToString(materialized);
}

export function buildSpecMap(specs: FormFieldSpec[]): Record<string, FormFieldSpec> {
	return Object.fromEntries(specs.map(s => [s.fieldType, s]));
}

export function deriveInputsFromFields(
	fields: FormFieldDef[],
	specMap: Record<string, FormFieldSpec>,
): PortDefinition[] {
	const ports: PortDefinition[] = [];
	for (const f of fields) {
		const spec = specMap[f.fieldType];
		if (!spec || !f.key) continue;
		for (const t of spec.addsInputs) {
			ports.push({
				name: resolvePortName(t.name, f.key),
				portType: resolveAutoTypeVars(t.portType, f.key),
				// Form field ports default to required (same as the language default).
				// Set "required": false explicitly to make a port optional.
				required: f.required !== false,
			});
		}
	}
	return ports;
}

export function deriveOutputsFromFields(
	fields: FormFieldDef[],
	specMap: Record<string, FormFieldSpec>,
): PortDefinition[] {
	const ports: PortDefinition[] = [];
	for (const f of fields) {
		const spec = specMap[f.fieldType];
		if (!spec || !f.key) continue;
		for (const t of spec.addsOutputs) {
			ports.push({
				name: resolvePortName(t.name, f.key),
				portType: resolveAutoTypeVars(t.portType, f.key),
				required: false,
			});
		}
	}
	return ports;
}
