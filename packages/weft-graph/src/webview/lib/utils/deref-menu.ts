import { parseWeftType, weftTypeToString, weftTypeToWireString, type WeftType } from '../types';

/// One key the user may read next off a wire's value.
export interface DerefKey {
	name: string;
	optional: boolean;
	/// The key's type, rendered for the menu.
	type: string;
}

/// The keys a wire could read one level below `path`, given the source
/// port's type text: the fields of the record found there, in declared
/// order. `null` when that level has no keys (a `JsonDict`, a scalar, a
/// type that does not parse), which is when the menu says to declare
/// the shape or Cast first.
export function derefKeys(sourceType: string, path: string[]): DerefKey[] | null {
	const fields = recordFields(typeAt(parseWeftType(sourceType), path));
	if (!fields) return null;
	return fields.map((f) => ({ name: f.name, optional: f.optional, type: weftTypeToString(f.ty) }));
}

/// The type a wire carries once it has read `path` off a source port of
/// type `sourceType` (what a dotted wire's target is held to), in the
/// self-contained wire spelling.
///
/// The two ways there is no type are different answers and the caller
/// has to tell them apart: `'unparsed'` means the source type text says
/// nothing (the compiler is the authority, so a gate lets the wire
/// through), while `'absent'` means the type parsed and has no such
/// path, which is a read that cannot work and a gate must stop.
export type Deref =
	| { kind: 'type'; type: string }
	| { kind: 'absent' }
	| { kind: 'unparsed' };

export function derefType(sourceType: string, path: string[]): Deref {
	const parsed = parseWeftType(sourceType);
	if (parsed === null) return { kind: 'unparsed' };
	const ty = typeAt(parsed, path);
	if (!ty) return { kind: 'absent' };
	return { kind: 'type', type: weftTypeToWireString(ty) };
}

/// Walk `path` down a type. A record whose fields cannot be read (a
/// `JsonDict`, a scalar) ends the walk with nothing, and so does a key
/// the record does not have.
function typeAt(ty: WeftType | null, path: string[]): WeftType | null {
	for (const key of path) {
		const fields = recordFields(ty);
		if (!fields) return null;
		ty = fields.find((f) => f.name === key)?.ty ?? null;
	}
	return ty;
}

function recordFields(ty: WeftType | null): { name: string; ty: WeftType; optional: boolean }[] | null {
	while (ty && ty.kind === 'named') ty = ty.body;
	return ty && ty.kind === 'record' ? ty.fields : null;
}
