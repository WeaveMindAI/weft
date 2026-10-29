// Metadata ports in the editor. A metadata port is declared in code only
// (`*tool: LlmTool` in a group's or a loop's header), and the compiler
// resolves every metadata wire when the program compiles, so the canvas
// draws neither the port nor its wires. What the editor still owes them is
// to keep them through its own header rewrites.

import type { EditPortSig, MetadataPort } from '../../../protocol';

/** The metadata ports' entries in a container's header, written after
 *  its ordinary outputs (`-> (results: String, *tool: LlmTool)`). Every
 *  header rewrite of a group or a loop carries them, or an ordinary port
 *  edit would delete the `*` ports from the header. The header keeps the
 *  type as written; a port whose spelling is unknown (one the projection
 *  made) writes its rendered type. */
export function metadataPortSigs(metadataPorts: readonly MetadataPort[] | undefined): EditPortSig[] {
	return (metadataPorts ?? []).map((m) => ({
		name: m.name,
		required: true,
		portType: m.declaredType ?? m.type,
		rendered: m.type,
		metadata: true,
	}));
}
