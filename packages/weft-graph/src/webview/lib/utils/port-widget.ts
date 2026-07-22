/// The ONE WeftType -> inline-widget mapping. A literal-accepting input port
/// renders an inline value field in the node body, and the field's
/// control kind derives from the port's TYPE alone (no field
/// declaration): this function is the single place that derivation
/// lives, so supporting a new weft type in the editor is one arm here.
///
/// Port types arrive as their rendered string form (`String`, `Number`,
/// `List[Number]`, `Image | Video`, the `File`/`Media` union aliases).
import type { FieldType } from '../types';

/// The file kinds a drop/pick control handles, including the union
/// aliases the type system renders (`File` = every stored kind,
/// `Media` = image/video/audio).
const FILE_TYPE_NAMES = new Set(['Image', 'Video', 'Audio', 'Blob', 'File', 'Media']);

/// Whether the rendered type names a file-valued port: a bare file
/// kind, an alias, or a union whose every member is one.
export function isFileValuedType(portType: string): boolean {
	const members = portType.split('|').map((s) => s.trim());
	return members.length > 0 && members.every((m) => FILE_TYPE_NAMES.has(m));
}

/// The inline control kind for a body-settable port of `portType`.
///   - file-valued types -> the drop/pick control (its accept filter
///     derives from the same type via `acceptForFileType`);
///   - Boolean -> checkbox; Number -> number box;
///   - everything else (String, JSON-ish containers, mixed unions) ->
///     a text area (JSON is typed as text, exactly like the source).
export function widgetForPortType(portType: string): FieldType {
	if (isFileValuedType(portType)) return 'file_drop';
	if (portType === 'Boolean') return 'checkbox';
	if (portType === 'Number') return 'number';
	return 'textarea';
}
